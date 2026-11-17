/**
 * Unit tests for Code Generation and Language Strategy Module (`src/codeGeneration.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Tests:
 * - resolveLanguageStrategy: verification of PySpark / Python language strategy resolution.
 * - buildExtensionPrompt (skill-assembled prompt): no builder placeholder is left, the pipeline YAML
 *   and Execution Plan are inserted, and Spark version handling works.
 * - PYTHON_STRATEGY: verification of file filtering, routing (`src/pipeline.py`, `tests/test_pipeline.py`),
 *   and main pipeline file identification.
 * - handleRegenerateCode: DAG pre-flight cycle rejection.
 * - deduplicateGeneratedFiles and ensureSystemCaBundle.
 */

import { describe, it, expect, vi } from 'vitest';
import * as path from 'path';
import * as fs from 'fs';

vi.mock('vscode', async () => (await import('./helpers/vscodeMock')).createVscodeMock({
    workspace: {
        workspaceFolders: []
    }
}));

import https from 'https';
import tls from 'tls';
import {
    resolveLanguageStrategy,
    buildExtensionPrompt,
    deduplicateGeneratedFiles,
    ensureSystemCaBundle,
    handleRegenerateCode,
    PYTHON_STRATEGY,
    JAVA_STRATEGY,
} from '../../codeGeneration';
import { skillsCorpus } from './helpers/codegenSkillsCorpus';
import { DEFAULT_SPARK_VERSION } from '../../shared/types';

describe('A3 — resolveLanguageStrategy (PySpark v1)', () => {
    it('returns PYTHON_STRATEGY for PySpark and Python inputs (simple case)', () => {
        for (const lang of ['PySpark', 'pyspark', 'python', 'Python', 'PYTHON', '']) {
            const strategy = resolveLanguageStrategy(lang);
            expect(strategy).toBe(PYTHON_STRATEGY);
            expect(strategy.langSubfolder).toBe('python');
            expect(strategy.fileExt).toBe('py');
        }
    });

    it('returns PYTHON_STRATEGY for extreme / edge inputs (null, undefined, unknown)', () => {
        expect(resolveLanguageStrategy('scala')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy('java')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy('   ')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy(null as unknown as string)).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy(undefined as unknown as string)).toBe(PYTHON_STRATEGY);
    });
});

/** Extension context that resolves resources from the repository root. */
const REPO_CONTEXT = { asAbsolutePath: (relative: string) => path.join(__dirname, '..', '..', '..', relative) } as any;

describe('Skill-assembled prompt: placeholder handling and Execution Plan', () => {
    const yaml = 'nodes:\n  - id: a\n    data: { type: SOURCE, spark_transform: CreateDataFrameSource, label: Create }\nedges: []\n';

    it('leaves no builder placeholder in the PySpark prompt and injects Execution Plan (simple case)', () => {
        const { prompt, executionPlan } = buildExtensionPrompt(REPO_CONTEXT, yaml, 'PySpark', '4.2.0');
        for (const removed of ['{{language}}', '{{pipeline}}', '{{spark_version}}', '{{beam_version}}']) {
            expect(prompt).not.toContain(removed);
        }
        // Only the model-facing placeholders may remain.
        const tokens = new Set(prompt.match(/\{\{\s*[A-Za-z_][\w]*\s*\}\}/g) ?? []);
        expect([...tokens].filter((t) => !['{{pipeline_name}}', '{{pipeline_class_name}}', '{{gcs_staging_path}}'].includes(t))).toEqual([]);
        expect(prompt).toContain(yaml);
        expect(prompt).toContain('## Execution Plan');
        expect(executionPlan.sortedNodeIds).toEqual(['a']);
    });

    it('enforces the testable 3-phase read_sources, transform, and write_sinks contract in codegen-common (G3)', () => {
        const corpus = skillsCorpus('python');
        expect(corpus).toContain('def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:');
        expect(corpus).toContain('def transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]:');
        expect(corpus).toContain('def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:');
        expect(corpus).toContain('tests/test_pipeline.py');
    });
});

describe('Skill-assembled prompt: literal text ($& and $\' protection)', () => {
    it.each([
        ['$&', 'transform:\n  query: "SELECT $& FROM dataset.table"\n'],
        ["$'", 'transform:\n  regex: "foo$\'bar"\n'],
        ['$` $1 $$ and regex tokens', 'notes: "$`pre $1 group $$100 \\\\w+ \\\\d+ [A-Z]"\n'],
    ])('keeps %s in the pipeline YAML literal', (_label, pipelineYaml) => {
        const { prompt } = buildExtensionPrompt(REPO_CONTEXT, pipelineYaml, 'PySpark', '4.2.0');
        expect(prompt).toContain(pipelineYaml);
        expect(prompt).not.toContain('SELECT {{pipeline}} FROM');
    });
});

describe('A3 — Language Strategies: Python and Java Behaviors', () => {
    describe('PYTHON_STRATEGY', () => {
        it('identifies mismatched Java artifacts', () => {
            expect(PYTHON_STRATEGY.isMismatchedArtifact('pom.xml', 'pom.xml')).toBe(true);
            expect(PYTHON_STRATEGY.isMismatchedArtifact('run_sparkrunner.sh', 'run_sparkrunner.sh')).toBe(true);
            expect(PYTHON_STRATEGY.isMismatchedArtifact('App.class', 'target/classes/App.class')).toBe(true);
            expect(PYTHON_STRATEGY.isMismatchedArtifact('pipeline.py', 'pipeline.py')).toBe(false);
            expect(PYTHON_STRATEGY.isMismatchedArtifact('requirements.txt', 'requirements.txt')).toBe(false);
            expect(PYTHON_STRATEGY.isMismatchedArtifact('Dockerfile', 'Dockerfile')).toBe(false);
        });

        it('routes PySpark pipeline and test files correctly', () => {
            expect(PYTHON_STRATEGY.routeFilename('pipeline.py', 'pipeline.py', '.', 'my_pipeline')).toBe('src/pipeline.py');
            expect(PYTHON_STRATEGY.routeFilename('src/pipeline.py', 'pipeline.py', 'src', 'my_pipeline')).toBe('src/pipeline.py');
            expect(PYTHON_STRATEGY.routeFilename('my_pipeline.py', 'my_pipeline.py', '.', 'my_pipeline')).toBe('src/my_pipeline.py');
            expect(PYTHON_STRATEGY.routeFilename('test_pipeline.py', 'test_pipeline.py', '.', 'my_pipeline')).toBe('tests/test_pipeline.py');
            expect(PYTHON_STRATEGY.routeFilename('tests/test_pipeline.py', 'test_pipeline.py', 'tests', 'my_pipeline')).toBe('tests/test_pipeline.py');
            expect(PYTHON_STRATEGY.routeFilename('pipeline_test.py', 'pipeline_test.py', '.', 'my_pipeline')).toBe('src/my_pipeline_test.py');
            expect(PYTHON_STRATEGY.routeFilename('my_pipeline_test.py', 'my_pipeline_test.py', '.', 'my_pipeline')).toBe('src/my_pipeline_test.py');
            expect(PYTHON_STRATEGY.routeFilename('utils.py', 'utils.py', '.', 'my_pipeline')).toBe('src/utils.py');
            expect(PYTHON_STRATEGY.routeFilename('sub/nested.py', 'nested.py', 'sub', 'my_pipeline')).toBe('sub/nested.py');
        });

        it('has no content post-processing step (regression: postProcessContent removed)', () => {
            expect('postProcessContent' in PYTHON_STRATEGY).toBe(false);
            expect('postProcessContent' in JAVA_STRATEGY).toBe(false);
        });

        it('identifies main Python pipeline files', () => {
            expect(PYTHON_STRATEGY.isMainPipelineFile('src/custom_job.py', 'custom_job', '')).toBe(true);
            expect(PYTHON_STRATEGY.isMainPipelineFile('src/pipeline.py', 'custom_job', '')).toBe(true);
            expect(PYTHON_STRATEGY.isMainPipelineFile('src/helper.py', 'custom_job', '')).toBe(true);
            expect(PYTHON_STRATEGY.isMainPipelineFile('other/helper.py', 'custom_job', '')).toBe(true);
            expect(PYTHON_STRATEGY.isMainPipelineFile('other/helper.py', 'custom_job', 'existing_code')).toBe(false);
            expect(PYTHON_STRATEGY.isMainPipelineFile('src/custom_job_test.py', 'custom_job', '')).toBe(false);
            expect(PYTHON_STRATEGY.isMainPipelineFile('tests/test_pipeline.py', 'custom_job', '')).toBe(false);
            expect(PYTHON_STRATEGY.isMainPipelineFile('src/__init__.py', 'custom_job', '')).toBe(false);
        });
    });

    describe('JAVA_STRATEGY', () => {
        it('identifies mismatched Python artifacts', () => {
            expect(JAVA_STRATEGY.isMismatchedArtifact('requirements.txt', 'requirements.txt')).toBe(true);
            expect(JAVA_STRATEGY.isMismatchedArtifact('pom.xml', 'pom.xml')).toBe(false);
            expect(JAVA_STRATEGY.isMismatchedArtifact('Dockerfile', 'Dockerfile')).toBe(false);
        });

        it('routes Java pipeline and test files correctly', () => {
            expect(JAVA_STRATEGY.routeFilename('pipeline.java', 'pipeline.java', '.', 'myPipeline')).toBe('src/main/java/com/example/MyPipeline.java');
            expect(JAVA_STRATEGY.routeFilename('mypipeline.java', 'mypipeline.java', '.', 'myPipeline')).toBe('src/main/java/com/example/MyPipeline.java');
            expect(JAVA_STRATEGY.routeFilename('pipelinetest.java', 'pipelinetest.java', '.', 'myPipeline')).toBe('src/test/java/com/example/MyPipelineTest.java');
            expect(JAVA_STRATEGY.routeFilename('pipeline_test.java', 'pipeline_test.java', '.', 'myPipeline')).toBe('src/test/java/com/example/MyPipelineTest.java');
            expect(JAVA_STRATEGY.routeFilename('Helper.java', 'Helper.java', '.', 'myPipeline')).toBe('src/main/java/com/example/Helper.java');
            expect(JAVA_STRATEGY.routeFilename('src/main/java/com/example/Custom.java', 'Custom.java', 'src/main/java/com/example', 'myPipeline')).toBe('src/main/java/com/example/Custom.java');
        });

        it('identifies main Java pipeline files', () => {
            expect(JAVA_STRATEGY.isMainPipelineFile('src/main/java/com/example/MyPipeline.java', 'myPipeline', '')).toBe(true);
            expect(JAVA_STRATEGY.isMainPipelineFile('src/main/java/com/example/Pipeline.java', 'myPipeline', '')).toBe(true);
            expect(JAVA_STRATEGY.isMainPipelineFile('src/test/java/com/example/MyPipelineTest.java', 'myPipeline', '')).toBe(false);
        });
    });
});

describe('handleRegenerateCode — DAG pre-flight topology validation', () => {
    it('aborts code generation early and posts failure when YAML DAG contains a cycle', async () => {
        const cyclicYaml = `
nodes:
  - id: n1
    data: { label: 'Node 1', type: 'STEP', spark_transform: 'Select' }
  - id: n2
    data: { label: 'Node 2', type: 'STEP', spark_transform: 'Filter' }
edges:
  - id: e1
    source: n1
    target: n2
  - id: e2
    source: n2
    target: n1
`;
        const postToWebview = vi.fn().mockResolvedValue(true);
        const options: any = {
            message: {
                data: cyclicYaml,
                filename: 'test_pipeline.spark',
                language: 'PySpark',
                outputFolder: 'out',
            },
            context: {} as any,
            uri: { fsPath: '/path/test_pipeline.spark' } as any,
            postToWebview,
            outputChannel: { appendLine: vi.fn() } as any,
        };

        await handleRegenerateCode(options);

        expect(postToWebview).toHaveBeenCalledWith(
            expect.objectContaining({
                command: 'codeRegenerated',
                success: false,
                error: expect.stringContaining('Cyclic dependency detected'),
            })
        );
    });
});

describe('Spark Version Baseline & File Deduplication', () => {
    it('uses DEFAULT_SPARK_VERSION constant as the Spark baseline', () => {
        expect(DEFAULT_SPARK_VERSION).toBe('4.0');
    });

    it('replaces {{spark_version}} in skill text when a version is supplied', () => {
        const { prompt } = buildExtensionPrompt(REPO_CONTEXT, 'nodes: []\n', 'PySpark', '4.2.0');
        expect(prompt).toContain('pyspark>=4.2.0');
        expect(prompt).not.toContain('{{spark_version}}');
    });

    it('does not rewrite version strings inside the user pipeline YAML (F7)', () => {
        const yaml = 'note: "keep 4.0 and {{spark_version}} and {{beam_version}} literally"\n';
        const { prompt } = buildExtensionPrompt(REPO_CONTEXT, yaml, 'PySpark', '4.2.0');
        expect(prompt).toContain('pyspark>=4.2.0');
        expect(prompt).toContain('note: "keep 4.0 and {{spark_version}} and {{beam_version}} literally"');
    });

    it('fallback version substitution also leaves the user pipeline YAML untouched', () => {
        const { prompt } = buildExtensionPrompt(REPO_CONTEXT, 'note: "{{spark_version}}"\n', 'PySpark');
        expect(prompt).toContain(DEFAULT_SPARK_VERSION);
        expect(prompt).toContain('note: "{{spark_version}}"');
    });

    it('deduplicateGeneratedFiles keeps the longest non-truncated entry per filename', () => {
        const rawFiles = [
            {
                filename: 'src/pipeline.py',
                content: 'def read_sources(spark, config):\n    return {}\n\ndef transform(dfs, config):\n    return dfs\n\ndef write_sinks(dfs, config):\n    pass\n',
            },
            { filename: 'requirements.txt', content: 'pyspark>=4.0.0\n' },
            { filename: 'src/pipeline.py', content: 'def read_sources(spark, config):\n    # truncated' },
        ];
        const deduped = deduplicateGeneratedFiles(rawFiles);
        expect(deduped).toHaveLength(2);
        expect(deduped[0].filename).toBe('src/pipeline.py');
        expect(deduped[0].content).toContain('def write_sinks');
    });

    it('ensureSystemCaBundle loads the system CA bundle into https.globalAgent.options.ca and patches tls.createSecureContext', () => {
        const origEnv = process.env.NODE_EXTRA_CA_CERTS;
        const origSslFile = process.env.SSL_CERT_FILE;
        const origCa = https.globalAgent.options.ca;
        const prevCreateSecureContext = tls.createSecureContext;
        delete process.env.NODE_EXTRA_CA_CERTS;
        delete process.env.SSL_CERT_FILE;

        const tempDir = fs.mkdtempSync(path.join(process.env.TMPDIR || '/tmp', 'spark-ca-'));
        const fakeCertPath = path.join(tempDir, 'cert.pem');
        const fakePem = Buffer.from('-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n');
        fs.writeFileSync(fakeCertPath, fakePem);

        try {
            https.globalAgent.options.ca = undefined;
            ensureSystemCaBundle(fakeCertPath);
            expect(https.globalAgent.options.ca).toEqual(fakePem);
            expect(tls.createSecureContext).not.toBe(prevCreateSecureContext);
        } finally {
            fs.rmSync(tempDir, { recursive: true, force: true });
            https.globalAgent.options.ca = origCa;
            tls.createSecureContext = prevCreateSecureContext;
            if (origEnv !== undefined) {
                process.env.NODE_EXTRA_CA_CERTS = origEnv;
            }
            if (origSslFile !== undefined) {
                process.env.SSL_CERT_FILE = origSslFile;
            }
        }
    });

    it('ensureSystemCaBundle still loads certificates when NODE_EXTRA_CA_CERTS is set in process.env after Electron startup', () => {
        const origEnv = process.env.NODE_EXTRA_CA_CERTS;
        const origSslFile = process.env.SSL_CERT_FILE;
        const origCa = https.globalAgent.options.ca;
        const prevCreateSecureContext = tls.createSecureContext;
        delete process.env.SSL_CERT_FILE;

        const tempDir = fs.mkdtempSync(path.join(process.env.TMPDIR || '/tmp', 'spark-ca-'));
        const extraCertPath = path.join(tempDir, 'extra-ca.pem');
        const extraPem = Buffer.from('-----BEGIN CERTIFICATE-----\nEXTRA\n-----END CERTIFICATE-----\n');
        fs.writeFileSync(extraCertPath, extraPem);
        process.env.NODE_EXTRA_CA_CERTS = extraCertPath;

        try {
            https.globalAgent.options.ca = undefined;
            ensureSystemCaBundle(path.join(tempDir, 'nonexistent.pem'));
            expect(https.globalAgent.options.ca).toEqual(extraPem);
            expect(tls.createSecureContext).not.toBe(prevCreateSecureContext);
        } finally {
            fs.rmSync(tempDir, { recursive: true, force: true });
            https.globalAgent.options.ca = origCa;
            tls.createSecureContext = prevCreateSecureContext;
            if (origEnv === undefined) {
                delete process.env.NODE_EXTRA_CA_CERTS;
            } else {
                process.env.NODE_EXTRA_CA_CERTS = origEnv;
            }
            if (origSslFile !== undefined) {
                process.env.SSL_CERT_FILE = origSslFile;
            }
        }
    });
});
