/**
 * Unit tests for V2.1 Jupyter Notebook (.ipynb) output mode.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import * as path from 'path';
import { describe, expect, it, vi } from 'vitest';

vi.mock('vscode', () => ({
    workspace: { getConfiguration: vi.fn(), workspaceFolders: [] },
    window: { showErrorMessage: vi.fn(), showInformationMessage: vi.fn(), createOutputChannel: vi.fn() },
    Uri: { file: (p: string) => ({ fsPath: p }) },
}));

import { normalizePipelineParadigm } from '../../../shared/types';
import { CodegenPromptBuilder } from '../../../codegen/CodegenPromptBuilder';
import { getCodegenSkillCatalog } from '../../../codegen/CodegenSkillCatalog';
import { detectPipelineFeatures } from '../../../codegen/PipelineFeatureDetector';
import { SKILL_PARADIGMS, SKILL_RUNNERS } from '../../../codegen/codegenConfig';
import {
    CHECK_IDS,
    ReadOnlyCheckRunner,
    isNotebookGeneratedProject,
    validateNotebookIpynb,
    type CommandExecutor,
} from '../../../codegen/ReadOnlyCheckRunner';
import { describeRunner, runnerScriptCandidates } from '../../../execution/PipelineRunner';
import { isAllowedExecutableScript } from '../../../security/scriptGuard';
import { PYTHON_STRATEGY } from '../../../codeGeneration';
import {
    buildManifest,
    isBuildArtifact,
    parseManifest,
    planCleanup,
} from '../../../generationManifest';

const SKILLS_ROOT = path.resolve(__dirname, '../../../../resources/codegen-skills');

const VALID_NOTEBOOK_JSON = JSON.stringify(
    {
        nbformat: 4,
        nbformat_minor: 5,
        metadata: {
            kernelspec: {
                display_name: 'PySpark (Spark 4.0)',
                language: 'python',
                name: 'python3',
            },
            language_info: { name: 'python', version: '3.11' },
        },
        cells: [
            {
                cell_type: 'markdown',
                id: 'overview',
                metadata: {},
                source: ['# Orders Notebook Pipeline\n'],
            },
            {
                cell_type: 'code',
                id: 'parameters',
                metadata: { tags: ['parameters'] },
                execution_count: null,
                outputs: [],
                source: ['PROJECT_ID = "demo-project"\n'],
            },
        ],
    },
    null,
    2
);

describe('V2.1 Jupyter Notebook (.ipynb) output mode', () => {
    it('normalizes notebook, sdp, imperative, and unknown paradigm values', () => {
        expect(normalizePipelineParadigm('notebook')).toBe('notebook');
        expect(normalizePipelineParadigm(' NOTEBOOK ')).toBe('notebook');
        expect(normalizePipelineParadigm('sdp')).toBe('sdp');
        expect(normalizePipelineParadigm('imperative')).toBe('imperative');
        expect(normalizePipelineParadigm(undefined)).toBe('imperative');
        expect(normalizePipelineParadigm('unknown')).toBe('imperative');
    });

    it('detects notebook paradigm and selects notebook runner skill set', () => {
        const parsed = {
            paradigm: 'notebook',
            nodes: [
                { id: 's1', data: { type: 'SOURCE', spark_transform: 'ParquetSource' } },
                { id: 't1', data: { type: 'STEP', spark_transform: 'Filter' } },
                { id: 'snk1', data: { type: 'SINK', spark_transform: 'BigQuerySink' } },
            ],
            edges: [
                { source: 's1', target: 't1' },
                { source: 't1', target: 'snk1' },
            ],
        };

        const detected = detectPipelineFeatures(parsed, 'PySpark');
        expect(detected.paradigm).toBe(SKILL_PARADIGMS.notebook);
        expect([...detected.runners]).toEqual([SKILL_RUNNERS.notebook]);
    });

    it('builds notebook codegen prompt with notebook-core, runner-notebook, and run_notebook.sh template', () => {
        const catalog = getCodegenSkillCatalog(SKILLS_ROOT);
        const builder = new CodegenPromptBuilder(catalog);
        const parsed = {
            paradigm: 'notebook',
            nodes: [
                { id: 's1', data: { type: 'SOURCE', spark_transform: 'CsvSource' } },
                { id: 'snk1', data: { type: 'SINK', spark_transform: 'ParquetSink' } },
            ],
            edges: [{ source: 's1', target: 'snk1' }],
        };

        const built = builder.build({
            parsedPipeline: parsed,
            redactedYaml: 'paradigm: notebook\nnodes: []\nedges: []\n',
            redactedPaths: [],
            language: 'PySpark',
        });

        expect(built.detection.paradigm).toBe('notebook');
        expect(built.selectedSkills).toContain('codegen-common');
        expect(built.selectedSkills).toContain('notebook-core');
        expect(built.selectedSkills).toContain('runner-notebook');
        expect(built.selectedSkills).toContain('io-csv');
        expect(built.selectedSkills).toContain('io-parquet');
        expect(built.selectedSkills).not.toContain('sdp-core');
        expect(built.selectedSkills).not.toContain('runner-local');
        expect(built.selectedSkills).not.toContain('runner-sdp');
        expect(built.templateFiles).toEqual(['run_notebook.sh']);
        expect(built.prompt).toContain('pipeline.ipynb');
        expect(built.prompt).toContain('find_spec("pyspark")');
    });

    it('validates notebook JSON payloads cleanly for normal and malformed inputs', () => {
        expect(validateNotebookIpynb('pipeline.ipynb', VALID_NOTEBOOK_JSON)).toEqual([]);
        expect(validateNotebookIpynb('pipeline.ipynb', '{not-json')).toHaveLength(1);
        expect(
            validateNotebookIpynb('pipeline.ipynb', JSON.stringify({ nbformat: 3, cells: [] }))
        ).toEqual(
            expect.arrayContaining([
                expect.stringContaining('nbformat must be 4'),
                expect.stringContaining('cells must be a non-empty array'),
            ])
        );
        expect(
            validateNotebookIpynb(
                'pipeline.ipynb',
                JSON.stringify({
                    nbformat: 4,
                    cells: [{ cell_type: 'markdown', source: ['# Only markdown'] }],
                })
            )
        ).toEqual([expect.stringContaining('at least one code cell')]);
    });

    it('runs ReadOnlyCheckRunner in notebook mode and passes for valid generated project', async () => {
        const fakeExec: CommandExecutor = async () => ({
            exitCode: 0,
            stdout: '1 test collected',
            stderr: '',
        });
        const runner = new ReadOnlyCheckRunner(fakeExec);
        const files = [
            { filename: 'pipeline.ipynb', content: VALID_NOTEBOOK_JSON },
            { filename: 'src/transforms.py', content: 'def apply_transforms(dfs, config):\n    return dfs\n' },
            {
                filename: 'tests/test_transforms.py',
                content: 'from src.transforms import apply_transforms\n\ndef test_ok():\n    assert apply_transforms({}, {}) == {}\n',
            },
            { filename: 'requirements.txt', content: 'pyspark==4.0.*\njupyterlab>=4.0.0\nnbconvert>=7.0.0\npytest>=8.0.0\n' },
            { filename: 'run_notebook.sh', content: '#!/usr/bin/env bash\nset -euo pipefail\n' },
        ];

        expect(isNotebookGeneratedProject(files)).toBe(true);
        expect(isNotebookGeneratedProject(files, 'imperative')).toBe(false);

        const results = await runner.run({
            files,
            language: 'python',
            baseName: 'orders',
            runnerFiles: ['run_notebook.sh'],
            paradigm: 'notebook',
        });

        for (const result of results) {
            expect(result.status, `${result.check}: ${result.output}`).toBe('passed');
        }
        expect(results.map((r) => r.check)).toContain(CHECK_IDS.pipelineContract);
    });

    it('routes and allows run_notebook.sh and pipeline.ipynb in runner, scriptGuard, and codeGeneration', () => {
        expect(isAllowedExecutableScript('run_notebook.sh')).toBe(true);
        expect(runnerScriptCandidates('Local', 'notebook')).toEqual(['run_notebook.sh', 'run_local.sh']);
        expect(describeRunner('Local', 'notebook')).toContain('Jupyter Notebook');
        expect(PYTHON_STRATEGY.routeFilename('pipeline.ipynb', 'pipeline.ipynb', '.', 'orders')).toBe('pipeline.ipynb');
        expect(PYTHON_STRATEGY.isMainPipelineFile('pipeline.ipynb', 'orders', '')).toBe(true);
    });

    it('cleans paradigm-switch runtime artifacts (.venv, .spark-ivy, .ipynb_checkpoints, executed_*.ipynb) while preserving user files', () => {
        expect(isBuildArtifact('.spark-ivy')).toBe(true);
        expect(isBuildArtifact('.ipynb_checkpoints')).toBe(true);
        expect(isBuildArtifact('executed_pipeline.ipynb')).toBe(true);
        expect(isBuildArtifact('pipeline.ipynb')).toBe(false);
        expect(isBuildArtifact('user_notes.md')).toBe(false);

        const notebookManifest = buildManifest(
            'sp_1234567890abcdef12345678',
            'PySpark',
            [
                { path: 'pipeline.ipynb', content: VALID_NOTEBOOK_JSON },
                { path: 'src/transforms.py', content: 'def apply_transforms(dfs, cfg): return dfs\n' },
                { path: 'run_notebook.sh', content: '#!/usr/bin/env bash\n' },
            ],
            'notebook'
        );
        const parsed = parseManifest(notebookManifest);
        expect(parsed?.paradigm).toBe('notebook');

        const folderEntries = [
            'pipeline.ipynb',
            'executed_pipeline.ipynb',
            '.ipynb_checkpoints',
            '.spark-ivy',
            '.venv',
            'src',
            'run_notebook.sh',
            'user_notes.md',
        ];

        // Same-paradigm regen keeps .venv / caches and only deletes manifest files
        const sameParadigmPlan = planCleanup(notebookManifest, 'PySpark', folderEntries, {}, 'notebook');
        expect(sameParadigmPlan.isParadigmSwitch).toBe(false);
        expect(sameParadigmPlan.paths).not.toContain('.venv');
        expect(sameParadigmPlan.paths).not.toContain('.ipynb_checkpoints');

        // Paradigm switch (notebook -> imperative) cleans manifest files AND stale runtime artifacts, never user_notes.md
        const switchPlan = planCleanup(notebookManifest, 'PySpark', folderEntries, {}, 'imperative');
        expect(switchPlan.isParadigmSwitch).toBe(true);
        expect(switchPlan.paths).toEqual(
            expect.arrayContaining([
                'pipeline.ipynb',
                'src/transforms.py',
                'run_notebook.sh',
                'executed_pipeline.ipynb',
                '.ipynb_checkpoints',
                '.spark-ivy',
                '.venv',
            ])
        );
        expect(switchPlan.paths).not.toContain('user_notes.md');
    });
});
