/**
 * Unit tests for the reviewer pass wiring (`src/codegen/reviewerPass.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Uses a fake Gemini client, a fixture skills root, and a fake command executor. Covers the
 * setting (default off, on, missing configuration), the review and fix calls (model, schema,
 * token limit), the runner files passed to the checks, and an empty model response.
 */

import { describe, it, expect, vi, beforeAll, afterAll } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

vi.mock('vscode', async () => (await import('../helpers/vscodeMock')).createVscodeMock());

import * as vscode from 'vscode';
import { isReviewerEnabled, runReviewerPass, ReviewerModelClient } from '../../../codegen/reviewerPass';
import { ReadOnlyCheckRunner } from '../../../codegen/ReadOnlyCheckRunner';
import { REVIEW_REPORT_SCHEMA } from '../../../codegen/CodeReviewer';
import { CODEGEN_MODEL_SETTINGS, CODEGEN_REVIEWER_SETTING } from '../../../codegen/codegenConfig';
import { DEFAULT_GEMINI_MODEL } from '../../../constants';
import { clearCodegenSkillCatalogCache } from '../../../codegen/CodegenSkillCatalog';

let skillsRoot: string;

/**
 * Writes one fixture skill.
 *
 * @param name Skill name.
 * @param kind Kind.
 * @param usedBy usedBy list.
 * @param runners runners list.
 * @param paradigms paradigms list.
 */
function writeSkill(
    name: string,
    kind: string,
    usedBy: string,
    runners = '[all]',
    paradigms = '[imperative]'
): void {
    fs.mkdirSync(path.join(skillsRoot, name), { recursive: true });
    fs.writeFileSync(
        path.join(skillsRoot, name, 'SKILL.md'),
        `---\nname: ${name}\ndescription: d.\nmetadata:\n  kind: ${kind}\n  usedBy: ${usedBy}\n  languages: [all]\n  runners: ${runners}\n  paradigms: ${paradigms}\n---\n${name.toUpperCase()} BODY`
    );
}

beforeAll(() => {
    skillsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'reviewer-pass-'));
    writeSkill('codegen-common', 'common', '[generator, reviewer]', '[all]', '[imperative, sdp]');
    writeSkill('codegen-review', 'review', '[reviewer]', '[all]', '[imperative, sdp]');
    writeSkill('runner-local', 'runner', '[generator, reviewer]', '[local]', '[imperative]');
    writeSkill('runner-sdp', 'runner', '[generator, reviewer]', '[sdp]', '[sdp]');
    fs.mkdirSync(path.join(skillsRoot, 'runner-local', 'templates', 'python'), { recursive: true });
    fs.writeFileSync(path.join(skillsRoot, 'runner-local', 'templates', 'python', 'run_local.sh'), '#!/bin/bash');
    fs.mkdirSync(path.join(skillsRoot, 'runner-sdp', 'templates', 'python'), { recursive: true });
    fs.writeFileSync(path.join(skillsRoot, 'runner-sdp', 'templates', 'python', 'run_sdp_local.sh'), '#!/bin/bash');
    clearCodegenSkillCatalogCache();
});

afterAll(() => {
    fs.rmSync(skillsRoot, { recursive: true, force: true });
    clearCodegenSkillCatalogCache();
});

describe('isReviewerEnabled', () => {
    it('is off by default (simple)', () => {
        expect(isReviewerEnabled((_section) => ({ get: <T>(_key: string, defaultValue: T) => defaultValue }))).toBe(false);
    });

    it('is on when the setting is true and reads the right section and key (simple)', () => {
        const get = vi.fn(() => true);
        const getConfiguration = vi.fn(() => ({ get: get as any }));
        expect(isReviewerEnabled(getConfiguration)).toBe(true);
        expect(getConfiguration).toHaveBeenCalledWith(CODEGEN_REVIEWER_SETTING.section);
        expect(get).toHaveBeenCalledWith(CODEGEN_REVIEWER_SETTING.key, false);
    });

    it('is off when the configuration is missing or holds a non-boolean (failure)', () => {
        expect(isReviewerEnabled(() => undefined)).toBe(false);
        expect(isReviewerEnabled(() => ({ get: (() => 'yes') as any }))).toBe(false);
    });

    it('works with the VS Code configuration accessor (simple)', () => {
        // The shared vscode mock returns undefined for every key.
        expect(isReviewerEnabled((section) => vscode.workspace.getConfiguration(section) as any)).toBe(false);
        expect(vscode.workspace.getConfiguration).toHaveBeenCalledWith(CODEGEN_REVIEWER_SETTING.section);
    });
});

describe('runReviewerPass', () => {
    it('reviews with the reviewer schema, fixes with the generator schema, and checks runner files (simple)', async () => {
        const responses = [
            JSON.stringify({
                findings: [{ file: 'src/pipeline.py', severity: 'blocking', problem: 'p', fix: 'f', skillRule: 'core-pyspark' }],
                summary: 's',
            }),
            JSON.stringify({ files: [{ filename: 'src/pipeline.py', content: 'fixed' }] }),
            JSON.stringify({ findings: [], summary: 'clean' }),
        ];
        const generateContent = vi.fn(async () => ({ text: responses.shift() }));
        const ai: ReviewerModelClient = { models: { generateContent } };
        const executor = vi.fn(async () => ({ exitCode: 0, stdout: '', stderr: '' }));
        const progress: string[] = [];

        const outcome = await runReviewerPass({
            ai,
            files: [{ filename: 'src/pipeline.py', content: 'bad' }],
            generationPrompt: 'GEN',
            yamlText: 'nodes: []\n',
            language: 'PySpark',
            baseName: 'orders',
            skillsRoot,
            reviewerTemplate: 'REVIEW TEMPLATE',
            fixTemplate: 'FIX TEMPLATE',
            responseSchema: { generator: true },
            onProgress: (m) => progress.push(m),
            checkRunner: new ReadOnlyCheckRunner(executor),
        });

        expect(outcome.fixRounds).toBe(1);
        expect(outcome.files[0].content).toBe('fixed');
        const calls = generateContent.mock.calls as unknown as Array<[{ model: string; contents: string; config: any }]>;
        expect(calls).toHaveLength(3);
        // Review call: reviewer template + reviewer-only skill + check results naming the missing runner file.
        expect(calls[0][0].model).toBe(DEFAULT_GEMINI_MODEL);
        expect(calls[0][0].config.responseSchema).toBe(REVIEW_REPORT_SCHEMA);
        expect(calls[0][0].config.maxOutputTokens).toBe(CODEGEN_MODEL_SETTINGS.maxOutputTokens);
        expect(calls[0][0].contents).toContain('REVIEW TEMPLATE');
        expect(calls[0][0].contents).toContain('CODEGEN-REVIEW BODY');
        expect(calls[0][0].contents).toContain('run_local.sh');
        // Fix call: generator schema and generation prompt.
        expect(calls[1][0].config.responseSchema).toEqual({ generator: true });
        expect(calls[1][0].contents.startsWith('GEN')).toBe(true);
        expect(progress.length).toBeGreaterThan(0);
    });

    it('records an error when the model returns an empty response (failure)', async () => {
        const ai: ReviewerModelClient = { models: { generateContent: vi.fn(async () => ({ text: '' })) } };
        const outcome = await runReviewerPass({
            ai,
            files: [],
            generationPrompt: 'GEN',
            yamlText: 'nodes: []\n',
            language: 'PySpark',
            baseName: 'orders',
            skillsRoot,
            reviewerTemplate: 'R',
            fixTemplate: 'F',
            responseSchema: {},
            // No checkRunner: the default runner is used. No file exists, so no process starts.
        });
        expect(outcome.error).toBe('Empty response from Gemini during review.');
    });

    it('forwards paradigm sdp and declarative runner files to ReadOnlyCheckRunner when reviewing SDP YAML', async () => {
        const generateContent = vi.fn(async () => ({
            text: JSON.stringify({ findings: [], summary: 'clean sdp review' }),
        }));
        const ai: ReviewerModelClient = { models: { generateContent } };
        const executor = vi.fn(async () => ({ exitCode: 0, stdout: '', stderr: '' }));

        const outcome = await runReviewerPass({
            ai,
            // Deliberately omit spark-pipeline.yml so only explicit paradigm forwarding triggers SDP required_files checks
            files: [{ filename: 'src/pipeline.py', content: 'print(1)\n' }],
            generationPrompt: 'GEN SDP',
            yamlText: 'paradigm: sdp\nnodes: []\nedges: []\njobConfig:\n  runner: ServerlessSpark\n  runtimeVersion: "4.0"\n',
            language: 'PySpark',
            baseName: 'medallion',
            skillsRoot,
            reviewerTemplate: 'REVIEW TEMPLATE',
            fixTemplate: 'FIX TEMPLATE',
            responseSchema: { generator: true },
            checkRunner: new ReadOnlyCheckRunner(executor),
        });

        expect(outcome.fixRounds).toBe(0);
        const calls = generateContent.mock.calls as unknown as Array<[{ contents: string }]>;
        expect(calls).toHaveLength(1);
        expect(calls[0][0].contents).toContain('RUNNER-SDP BODY');
        expect(calls[0][0].contents).not.toContain('RUNNER-LOCAL BODY');
        expect(calls[0][0].contents).toContain('spark-pipeline.yml');
        expect(calls[0][0].contents).toContain('run_sdp_local.sh');
    });

    it('forwards custom Gemini 4 model override distinct from DEFAULT_GEMINI_MODEL to both review and fix calls', async () => {
        const responses = [
            JSON.stringify({
                findings: [{ file: 'src/pipeline.py', severity: 'blocking', problem: 'p', fix: 'f', skillRule: 'core-pyspark' }],
                summary: 'needs fix',
            }),
            JSON.stringify({ files: [{ filename: 'src/pipeline.py', content: 'fixed_v4' }] }),
            JSON.stringify({ findings: [], summary: 'clean' }),
        ];
        const generateContent = vi.fn(async () => ({ text: responses.shift() }));
        const ai: ReviewerModelClient = { models: { generateContent } };
        const executor = vi.fn(async () => ({ exitCode: 0, stdout: '', stderr: '' }));

        expect(DEFAULT_GEMINI_MODEL).toBe('gemini-4-hydrogen');
        const outcome = await runReviewerPass({
            ai,
            files: [{ filename: 'src/pipeline.py', content: 'draft' }],
            generationPrompt: 'GEN',
            yamlText: 'nodes: []\n',
            language: 'PySpark',
            baseName: 'orders',
            model: 'gemini-4-carbon',
            skillsRoot,
            reviewerTemplate: 'REVIEW TEMPLATE',
            fixTemplate: 'FIX TEMPLATE',
            responseSchema: { generator: true },
            checkRunner: new ReadOnlyCheckRunner(executor),
        });

        expect(outcome.fixRounds).toBe(1);
        expect(outcome.files[0].content).toBe('fixed_v4');
        const calls = generateContent.mock.calls as unknown as Array<[{ model: string }]>;
        expect(calls.map((c) => c[0].model)).toEqual([
            'gemini-4-carbon',
            'gemini-4-carbon',
            'gemini-4-carbon',
        ]);
    });

    it('rejects deprecated Gemini 2.5 models before calling the reviewer SDK', async () => {
        const generateContent = vi.fn();
        const ai: ReviewerModelClient = { models: { generateContent } };

        await expect(
            runReviewerPass({
                ai,
                files: [{ filename: 'src/pipeline.py', content: 'draft' }],
                generationPrompt: 'GEN',
                yamlText: 'nodes: []\n',
                language: 'PySpark',
                baseName: 'orders',
                model: 'gemini-2.5-flash',
                skillsRoot,
                reviewerTemplate: 'REVIEW TEMPLATE',
                fixTemplate: 'FIX TEMPLATE',
                responseSchema: {},
            })
        ).rejects.toThrow(/Unsupported or deprecated Gemini code generation model: 'gemini-2\.5-flash'/);

        expect(generateContent).not.toHaveBeenCalled();
    });
});
