/**
 * Unit tests for Code Generation and Language Strategy Module (A3 Refactor).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Tests:
 * - resolveLanguageStrategy: verification of language strategy resolution (Java, Python, fallbacks, case sensitivity).
 * - buildExtensionPrompt (skill-assembled prompt): no builder placeholder is left, the pipeline YAML
 *   and runner templates are inserted literally (including $& and $'), and Beam version handling.
 * - Prompt-content regressions now read the skill corpus (resources/codegen-skills/**) through
 *   helpers/codegenSkillsCorpus.ts instead of the deleted code-gen-prompt-*.txt files.
 * - PYTHON_STRATEGY and JAVA_STRATEGY: verification of language-specific file filtering, routing,
 *   import adjustment, and pipeline file identification.
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
import { skillsCorpus, skillText, SKILLS_ROOT } from './helpers/codegenSkillsCorpus';
import { DEFAULT_SPARK_VERSION } from '../../shared/types';

describe('A3 — resolveLanguageStrategy', () => {
    it('returns JAVA_STRATEGY for "java" (simple case)', () => {
        const strategy = resolveLanguageStrategy('java');
        expect(strategy).toBe(JAVA_STRATEGY);
        expect(strategy.langSubfolder).toBe('java');
        expect(strategy.fileExt).toBe('java');
    });

    it('returns JAVA_STRATEGY for "Java" and "JAVA" (case-insensitive)', () => {
        expect(resolveLanguageStrategy('Java')).toBe(JAVA_STRATEGY);
        expect(resolveLanguageStrategy('JAVA')).toBe(JAVA_STRATEGY);
        expect(resolveLanguageStrategy('jAvA')).toBe(JAVA_STRATEGY);
    });

    it('returns PYTHON_STRATEGY for "python" (simple case)', () => {
        const strategy = resolveLanguageStrategy('python');
        expect(strategy).toBe(PYTHON_STRATEGY);
        expect(strategy.langSubfolder).toBe('python');
        expect(strategy.fileExt).toBe('py');
    });

    it('returns PYTHON_STRATEGY for "Python" and "PYTHON" (case-insensitive)', () => {
        expect(resolveLanguageStrategy('Python')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy('PYTHON')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy('pYtHon')).toBe(PYTHON_STRATEGY);
    });

    it('returns PYTHON_STRATEGY for empty string (non-Java default)', () => {
        expect(resolveLanguageStrategy('')).toBe(PYTHON_STRATEGY);
    });

    it('returns PYTHON_STRATEGY for unknown language values (non-Java default)', () => {
        expect(resolveLanguageStrategy('scala')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy('go')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy('csharp')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy('unknown')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy('rust')).toBe(PYTHON_STRATEGY);
    });

    it('returns PYTHON_STRATEGY for extreme / edge inputs (partial match, whitespace, null/undefined)', () => {
        // Must not match prefix/substring
        expect(resolveLanguageStrategy('javascript')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy('java17')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy('java ')).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy('   ')).toBe(PYTHON_STRATEGY);
        // Null / undefined type-cast failure cases
        expect(resolveLanguageStrategy(null as unknown as string)).toBe(PYTHON_STRATEGY);
        expect(resolveLanguageStrategy(undefined as unknown as string)).toBe(PYTHON_STRATEGY);
    });
});

/** Extension context that resolves resources from the repository root. */
const REPO_CONTEXT = { asAbsolutePath: (relative: string) => path.join(__dirname, '..', '..', '..', relative) } as any;

describe('Skill-assembled prompt: placeholder handling', () => {
    const yaml = 'nodes:\n  - id: a\n    data: { type: SOURCE, spark_transform: Create, label: Create }\nedges: []\n';

    it.each(['Python', 'Java'])('leaves no builder placeholder in the %s prompt (simple case)', (language) => {
        const { prompt } = buildExtensionPrompt(REPO_CONTEXT, yaml, language, '2.80.0');
        for (const removed of ['{{language}}', '{{pipeline}}', '{{dockerfile_template}}', '{{run_directrunner_template}}', '{{run_dataflowrunner_template}}', '{{run_sparkrunner_template}}', '{{beam_version}}']) {
            expect(prompt).not.toContain(removed);
        }
        // Only the model-facing placeholders may remain.
        const tokens = new Set(prompt.match(/\{\{\s*[A-Za-z_][\w]*\s*\}\}/g) ?? []);
        expect([...tokens].filter((t) => !['{{pipeline_name}}', '{{pipeline_class_name}}', '{{gcs_staging_path}}'].includes(t))).toEqual([]);
        expect(prompt).toContain(yaml);
    });

    it('inlines every runner template of the language verbatim after version resolution (extreme case)', () => {
        const { prompt } = buildExtensionPrompt(REPO_CONTEXT, yaml, 'Java', '2.80.0');
        for (const [skill, file] of [['runner-direct', 'run_directrunner.sh'], ['runner-dataflow', 'Dockerfile'], ['runner-dataflow', 'run_dataflowrunner.sh'], ['runner-spark', 'run_sparkrunner.sh']]) {
            const template = fs.readFileSync(path.join(SKILLS_ROOT, skill, 'templates', 'java', file), 'utf-8').replace(/\{\{beam_version\}\}/g, '2.80.0');
            expect(prompt, file).toContain(template.trimEnd());
        }
    });

    it('omits the Spark template for Python (boundary case)', () => {
        const { prompt, selectedSkills } = buildExtensionPrompt(REPO_CONTEXT, yaml, 'Python', '2.80.0');
        expect(selectedSkills).not.toContain('runner-spark');
        expect(prompt).not.toContain('run_sparkrunner.sh');
    });
});

describe('Skill-assembled prompt: literal text ($& and $\' protection)', () => {
    it.each([
        ['$&', 'transform:\n  query: "SELECT $& FROM dataset.table"\n'],
        ["$'", 'transform:\n  regex: "foo$\'bar"\n'],
        ['$` $1 $$ and regex tokens', 'notes: "$`pre $1 group $$100 \\\\w+ \\\\d+ [A-Z]"\n'],
    ])('keeps %s in the pipeline YAML literal', (_label, pipelineYaml) => {
        const { prompt } = buildExtensionPrompt(REPO_CONTEXT, pipelineYaml, 'Python', '2.80.0');
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

        it('routes Python pipeline and test files correctly', () => {
            expect(PYTHON_STRATEGY.routeFilename('pipeline.py', 'pipeline.py', '.', 'my_pipeline')).toBe('src/my_pipeline.py');
            expect(PYTHON_STRATEGY.routeFilename('my_pipeline.py', 'my_pipeline.py', '.', 'my_pipeline')).toBe('src/my_pipeline.py');
            expect(PYTHON_STRATEGY.routeFilename('pipeline_test.py', 'pipeline_test.py', '.', 'my_pipeline')).toBe('src/my_pipeline_test.py');
            expect(PYTHON_STRATEGY.routeFilename('my_pipeline_test.py', 'my_pipeline_test.py', '.', 'my_pipeline')).toBe('src/my_pipeline_test.py');
            expect(PYTHON_STRATEGY.routeFilename('utils.py', 'utils.py', '.', 'my_pipeline')).toBe('src/utils.py');
            expect(PYTHON_STRATEGY.routeFilename('sub/nested.py', 'nested.py', 'sub', 'my_pipeline')).toBe('sub/nested.py');
        });

        it('has no content post-processing step (regression: postProcessContent removed)', () => {
            // The regex rewrite corrupted comments such as "# Resolve credential from pipeline options".
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
            expect(PYTHON_STRATEGY.isMainPipelineFile('src/__init__.py', 'custom_job', '')).toBe(false);
        });
    });

    describe('JAVA_STRATEGY', () => {
        it('identifies mismatched Python artifacts', () => {
            expect(JAVA_STRATEGY.isMismatchedArtifact('requirements.txt', 'requirements.txt')).toBe(true);
            expect(JAVA_STRATEGY.isMismatchedArtifact('pom.xml', 'pom.xml')).toBe(false);
            expect(JAVA_STRATEGY.isMismatchedArtifact('run_sparkrunner.sh', 'run_sparkrunner.sh')).toBe(false);
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

        it('ships the Spark runner template in the runner-spark skill (Java only)', () => {
            expect(fs.existsSync(path.join(SKILLS_ROOT, 'runner-spark', 'templates', 'java', 'run_sparkrunner.sh'))).toBe(true);
            expect(fs.existsSync(path.join(SKILLS_ROOT, 'runner-spark', 'templates', 'python'))).toBe(false);
        });
    });
});

/**
 * Regression guard for the latent bug this refactor closed.
 *
 * Before A3, the language decision was made TWICE, independently, with different
 * defaults:
 *
 *   const langSubfolder = languageVal.toLowerCase() === 'java' ? 'java' : 'python';
 *   const fileExt       = languageVal.toLowerCase() === 'python' ? 'py' : 'java';
 *
 * For 'Java' and 'Python' these agree. For anything else -- an unknown value, or a
 * hand-edited `.beam` whose pipelineConfig.language is something like 'Go' -- they
 * disagreed: the model was asked for PYTHON via the python prompt, but the response
 * scanner then looked for a file ending in `.java`. It found none, so mainPipelineCode
 * was silently left empty and the user got a successful-looking run with no pipeline
 * code.
 *
 * Collapsing both decisions into a single strategy object fixes this by construction.
 * These tests pin that property so it cannot drift apart again.
 */
describe('A3 — language strategy internal coherence (regression)', () => {
    const ALL_STRATEGIES = [
        { name: 'PYTHON_STRATEGY', strategy: PYTHON_STRATEGY, expectedExt: 'py' },
        { name: 'JAVA_STRATEGY', strategy: JAVA_STRATEGY, expectedExt: 'java' },
    ];

    it.each(ALL_STRATEGIES)(
        '$name targets one language consistently across langSubfolder and fileExt',
        ({ strategy, expectedExt }) => {
            expect(strategy.fileExt).toBe(expectedExt);
            // The language id must describe the SAME language as the extension used to
            // locate the generated pipeline file.
            expect(strategy.langSubfolder).toBe(expectedExt === 'py' ? 'python' : 'java');
        }
    );

    it('resolves a single strategy object, so prompt and extension can never disagree', () => {
        // The specific inputs that used to produce the incoherent python-prompt/.java-ext
        // combination. Each must now yield one internally consistent strategy.
        for (const value of ['', 'go', 'scala', 'rust', 'Kotlin', 'unknown']) {
            const strategy = resolveLanguageStrategy(value);
            expect(strategy.fileExt).toBe('py');
            expect(strategy.langSubfolder).toBe('python');
        }
    });

    it('still routes the two languages the UI can actually produce', () => {
        // webview types pipelineConfig.language as 'Python' | 'Java'; these are the only
        // values reachable without hand-editing the .beam file.
        expect(resolveLanguageStrategy('Python').fileExt).toBe('py');
        expect(resolveLanguageStrategy('Java').fileExt).toBe('java');
    });
});

/**
 * Regression tests for the Java code-generation prompt's dependency & packaging guidance.
 *
 * Verifies that:
 * 1. The Java prompt specifies Maven coordinates, minimum verified baseline versions (`>=`),
 *    and shade plugin requirements (`main.jar`, `ServicesResourceTransformer`) concisely
 *    without embedding a verbose full `<project>` XML block.
 * 2. Sample-specific third-party libraries (such as Datafaker in `generate-mock-data.beam`)
 *    carry their minimum version constraint (`>= 2.7.0`) in the sample pipeline definition
 *    rather than bloating the global system prompt.
 */
describe('Java codegen prompt — dependency & pom.xml specification (regression)', () => {
    const SAMPLE_PATH = path.join(__dirname, '..', '..', '..', 'resources', 'samples', 'generate-mock-data.beam');
    const prompt = skillsCorpus('java');
    const sample = fs.readFileSync(SAMPLE_PATH, 'utf-8');

    it('specifies Maven coordinates and plugins concisely without a full <project> XML block', () => {
        expect(prompt.length).toBeGreaterThan(1000);
        expect(prompt).not.toContain('<project xmlns="http://maven.apache.org/POM/4.0.0"');
        expect(prompt).toContain('beam-sdks-java-core');
        expect(prompt).toContain('maven-shade-plugin');
        expect(prompt).toContain('${project.build.directory}/main.jar');
        expect(prompt).toContain('ServicesResourceTransformer');
    });

    it('instructs the model to treat baseline versions as minimum verified versions (>=) matching called API methods', () => {
        expect(prompt).not.toMatch(/latest version of all libraries/i);
        expect(prompt).toContain('minimum verified baseline versions (`>=`)');
        expect(prompt).toContain('exposes the exact API methods called in your Java code');
    });

    it('keeps sample-specific Datafaker >= 2.7.0 guidance in generate-mock-data.beam rather than the global prompt', () => {
        expect(prompt.toLowerCase()).not.toContain('datafaker');
        expect(sample).toContain('net.datafaker:datafaker >= 2.7.0');
    });
});

/**
 * Regression tests for the Python codegen prompt.
 *
 * Bug: a generated Python mock-data pipeline ran to completion but emitted nothing,
 * while the equivalent Java pipeline worked. Two independent defects, both traced to
 * this prompt:
 *
 *  1. The DoFn yielded `TaggedOutput(TAG_VALID, ...)` for its MAIN output while
 *     declaring `.with_outputs(TAG_ERRORS, main=TAG_VALID)`. Beam routes an
 *     unrecognised tag to a null receiver, so every element was silently discarded --
 *     no error, no warning, zero output. Verified empirically against Beam 2.76.0:
 *     the tagged form produced 0 elements, the bare form produced 3. The prompt gave
 *     no guidance on tagged outputs at all, so the model improvised this.
 *
 *  2. The prompt mandated the Java cross-language `GenerateSequence` for a periodic
 *     trigger. That needs an ~800MB expansion JAR plus a Docker daemon to host the
 *     Java SDK harness. Without Docker it emits confusing `KafkaIOInitializer` noise
 *     (KafkaIO is merely bundled in the IO shadowJar) and produces no elements.
 *     `PeriodicImpulse` is the native Python equivalent and needs neither.
 */
describe('Python codegen prompt — output routing and trigger selection (regression)', () => {
    const prompt = skillsCorpus('python');

    // Non-vacuity guard: every assertion below searches this string, so prove it loaded.
    it('the Python prompt file is readable and non-trivial', () => {
        expect(prompt.length).toBeGreaterThan(1000);
    });

    describe('tagged outputs', () => {
        it('warns that the main output must never be wrapped in TaggedOutput', () => {
            expect(prompt).toMatch(/NEVER wrap the main output in `TaggedOutput`/);
        });

        it('explains the silent-failure mode, which is what makes the rule stick', () => {
            // The danger here is not that it errors -- it is that it does NOT error.
            // A model that does not know this will treat the rule as stylistic.
            expect(prompt).toMatch(/null receiver/i);
            expect(prompt).toMatch(/silently (discards|dropped)/i);
        });

        it('shows the correct bare-yield form for the main output', () => {
            expect(prompt).toMatch(/yield transform\(element\)\s*#\s*CORRECT: main output, bare/);
        });

        it('shows the exact incorrect form that caused the bug', () => {
            expect(prompt).toMatch(/yield TaggedOutput\('valid', record\)\s*#\s*WRONG/);
        });

        it('clarifies that main= only names the read-side accessor', () => {
            expect(prompt).toMatch(/main=.{0,40}only sets the \*attribute name\*/);
        });
    });

    describe('periodic trigger selection', () => {
        it('directs the model to prefer PeriodicImpulse for periodic triggers', () => {
            expect(prompt).toMatch(/PREFER `PeriodicImpulse` FOR PERIODIC TRIGGERS/);
        });

        it('states the Docker requirement that makes cross-language fail locally', () => {
            expect(prompt).toMatch(/Docker daemon/);
        });

        it('names the KafkaIOInitializer symptom so the model can connect cause to effect', () => {
            // This is the exact log line the user saw. Without it the guidance reads as
            // an unexplained preference rather than a diagnosis.
            expect(prompt).toMatch(/KafkaIOInitializer/);
        });

        it('specifies fire_interval in seconds, not milliseconds', () => {
            // The cross-language form immediately below uses milliseconds, so the unit
            // mismatch is a live trap for the model.
            expect(prompt).toMatch(/fire_interval=period_seconds,\s*#\s*seconds, NOT milliseconds/);
        });

        it('still documents the cross-language escape hatch for real sequence consumers', () => {
            // Preferring PeriodicImpulse must not delete the legitimate use case where the
            // pipeline actually consumes the sequence values.
            expect(prompt).toMatch(/beam:external:java:generate_sequence:v1/);
            expect(prompt).toMatch(/consumes the \*\*sequence values themselves\*\*/);
        });

        it('records that None-valued payload keys are dropped rather than sent', () => {
            expect(prompt).toMatch(/silently drops any key whose value is `None`/);
        });
    });

    describe('handleRegenerateCode — DAG pre-flight topology validation', () => {
        it('aborts code generation early and posts failure when YAML DAG contains a cycle', async () => {
            const cyclicYaml = `
nodes:
  - id: n1
    data: { label: 'Node 1', type: 'STEP' }
  - id: n2
    data: { label: 'Node 2', type: 'STEP' }
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
                    filename: 'test_pipeline.beam',
                    language: 'Python',
                    outputFolder: 'out',
                },
                context: {} as any,
                uri: { fsPath: '/path/test_pipeline.beam' } as any,
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
});


/**
 * Regression tests for connector facts added to the codegen prompts after the
 * 2026-09-24 integration run (54/61 passing).
 *
 * Context: the run's auto-generated remediation directives recommended API that does
 * not exist. Two were caught before they reached the prompts and are pinned here so a
 * future edit cannot reintroduce them:
 *
 *  1. XmlIO. The directive told the generator to chain
 *     `.withSuffix(...).withNumShards(...).withShardTemplate(...)` onto `XmlIO.write()`.
 *     Verified against the real Beam 2.53.0 source: `XmlIO.Write` exposes exactly four
 *     builder methods -- `to`, `withRecordClass`, `withRootElement`, `withCharset`. The
 *     `.xml` suffix is hardcoded inside `expand()`. Following that directive would have
 *     turned a passing-but-flagged test into an outright compile failure.
 *
 *  2. ParquetIO codec. The directive offered `'uncompressed'` as a spelling for no
 *     compression. Beam forwards `codec` straight to pyarrow, whose validator accepts
 *     only NONE/SNAPPY/GZIP/LZO/BROTLI/LZ4/ZSTD; `'uncompressed'` raises
 *     `ArrowException: Unsupported compression`.
 */
describe('Codegen prompts — connector API coherence (regression)', () => {
    const JAVA_PROMPT = skillsCorpus('java');
    const PYTHON_PROMPT = skillsCorpus('python');

    // Non-vacuity guard: every assertion below searches these strings, so prove they loaded.
    it('both prompt files are readable and non-trivial', () => {
        expect(JAVA_PROMPT.length).toBeGreaterThan(1000);
        expect(PYTHON_PROMPT.length).toBeGreaterThan(1000);
    });

    describe('XmlIO write surface', () => {
        it('documents XmlIO at all, so the model is not left to improvise it', () => {
            expect(JAVA_PROMPT).toContain('XmlIO');
        });

        it('never recommends the three builder methods XmlIO.Write does not have', () => {
            // Scoped to the XmlIO section: withSuffix/withNumShards are legitimate elsewhere
            // (TextIO, FileIO), so a naive whole-file search would be both wrong and vacuous.
            const section = skillText('io-xml', 'java');
            expect(section, 'io-xml skill not found').not.toBe('');

            // The prompt is allowed to *mention* these names to forbid them; what must never
            // appear is the chained-call form that reads as a recommendation.
            expect(section).not.toMatch(/XmlIO\.<T>write\(\)[^\n]*\.withSuffix\(/);
            expect(section).not.toMatch(/XmlIO\.<T>write\(\)[^\n]*\.withNumShards\(/);
            expect(section).not.toMatch(/XmlIO\.<T>write\(\)[^\n]*\.withShardTemplate\(/);
        });

        it('states plainly that the suffix is not configurable', () => {
            // Without the rationale the model tends to "helpfully" restore the missing call.
            expect(JAVA_PROMPT).toMatch(/no\b[^\n]*`withSuffix`/i);
        });
    });

    describe('ParquetIO codec vocabulary', () => {
        it('maps the UNCOMPRESSED node property to the only spelling pyarrow accepts', () => {
            expect(PYTHON_PROMPT).toMatch(/UNCOMPRESSED[^\n]*codec='none'/);
        });

        it('warns that the intuitive spelling is rejected', () => {
            expect(PYTHON_PROMPT).toContain("`'uncompressed'` is NOT a legal pyarrow value");
        });
    });

    /**
     * Iceberg is reachable only through Managed I/O, whose config map is validated at
     * construction — an unknown key fails the pipeline rather than being ignored. Two keys the
     * sink panel used to collect, `write_mode` and `create_table_if_not_exists`, do not exist
     * anywhere in the Beam tree (verified against `IcebergWriteSchemaTransformProvider`). The
     * panel fields were deleted; these assertions stop the prompts from reintroducing them.
     */
    describe('Iceberg Managed I/O surface', () => {
        it.each([
            ['Java', () => JAVA_PROMPT],
            ['Python', () => PYTHON_PROMPT],
        ])('%s prompt documents Iceberg rather than leaving the model to improvise', (_lang, get) => {
            expect(get()).toContain('IcebergIO');
            expect(get()).toContain('Managed I/O');
        });

        it.each([
            ['Java', () => JAVA_PROMPT],
            ['Python', () => PYTHON_PROMPT],
        ])('%s prompt names the two config keys that do not exist', (_lang, get) => {
            expect(get()).toContain('there is no `write_mode` and no `create_table_if_not_exists`');
            expect(get()).toContain('append-only');
        });

        it('Java prompt distinguishes the read output tag from the write snapshots tag', () => {
            // Both are PCollectionRowTuple, and using "output" on the write result is the
            // single easiest mistake to make here.
            const section = skillText('io-iceberg', 'java');
            expect(section, 'io-iceberg skill not found').not.toBe('');
            expect(section).toContain('.get("output")');
            expect(section).toContain('"snapshots"');
        });

        it('Python prompt forbids the non-existent apache_beam.io.iceberg module', () => {
            // `ReadFromIceberg` / `WriteToIceberg` exist only as Beam YAML transform identifiers;
            // an earlier draft directive tried to import them as Python classes.
            expect(PYTHON_PROMPT).toContain('NO** `apache_beam.io.iceberg` module');
            expect(PYTHON_PROMPT).toContain('from apache_beam.transforms import managed');
        });

        it.each([
            ['Java', () => JAVA_PROMPT],
            ['Python', () => PYTHON_PROMPT],
        ])('%s prompt maps the camelCase panel keys onto snake_case config keys', (_lang, get) => {
            // The panel stores camelCase; Managed's schema is snake_case. Without the explicit
            // mapping the model passes `catalogName` straight through and construction fails.
            expect(get()).toContain('`catalogName` → `catalog_name`');
            expect(get()).toContain('`partition_fields`');
            // catalogType/warehouseLocation/catalogUri are nested, not top-level.
            expect(get()).toContain('NOT top-level keys');
        });
    });

    describe('Tier 2 connector API composition & vocabulary rules', () => {
        it('Java prompt routes TikaIO with* options via FileIO.readMatches -> TikaIO.parseFiles and delegates SINK:TikaIO to TextIO.write', () => {
            expect(JAVA_PROMPT).toContain('`TikaIO.parse().filepattern(filePattern)` has **NO** `.with*` builder methods');
            expect(JAVA_PROMPT).toContain('TikaIO.parseFiles()');
            expect(JAVA_PROMPT).toContain('There is **NO** `TikaIO.write()` class in Apache Beam');
        });

        it('Java prompt documents ThriftIO.readFiles composition and forbids write-only TSimpleJSONProtocol on read', () => {
            expect(JAVA_PROMPT).toContain('ThriftIO.readFiles(T.class).withProtocol(protocolFactory)');
            expect(JAVA_PROMPT).toContain('TSimpleJSONProtocol is a write only protocol');
            expect(JAVA_PROMPT).toContain('Never emit `TSimpleJSONProtocol` on read');
        });

        it('Java prompt warns against the broken ContextualTextIO.Read.watchForNewFiles javadoc and documents resourceId schema', () => {
            expect(JAVA_PROMPT).toContain('`ContextualTextIO.Read.watchForNewFiles(...)` does **NOT** exist');
            expect(JAVA_PROMPT).toContain('`resourceId`');
            expect(JAVA_PROMPT).toContain('`ContextualTextIO` has **NO** `write()` or `sink()` API');
        });

        it('Java prompt documents CsvIO.parseRows composition, mandatory header, and JsonIO write-only fallback to JsonToRow', () => {
            expect(JAVA_PROMPT).toContain('CsvIO.parseRows(schema, csvFormat)');
            expect(JAVA_PROMPT).toContain('`CsvIOParseHelpers.validateCsvFormat` **requires** a non-null, non-empty `.withHeader');
            expect(JAVA_PROMPT).toContain('`JsonIO` in Java is **write-only**');
            expect(JAVA_PROMPT).toContain('JsonToRow.withSchema(schema)');
        });

        it('Python prompt forbids passing compression= to ReadFromCsv/WriteToCsv/ReadFromJson/WriteToJson', () => {
            expect(PYTHON_PROMPT).toContain("raise NotImplementedError('compression')");
            expect(PYTHON_PROMPT).toContain('**Never pass `compression=`** to `ReadFromCsv` or `WriteToCsv`');
        });

        it('Java prompt forbids Compression.AUTO on TFRecordIO.write and Compression.XZ/TAR everywhere', () => {
            expect(JAVA_PROMPT).toContain('`Compression.AUTO.writeCompressed()` throws `UnsupportedOperationException`');
            expect(JAVA_PROMPT).toContain('`Compression` has **NO** `XZ` or `TAR` member');
        });

        it.each([
            ['Java', () => JAVA_PROMPT],
            ['Python', () => PYTHON_PROMPT],
        ])('%s prompt enforces azfs:// for AzureBlobStoreFileSystem and forbids abfss://', (_lang, get) => {
            expect(get()).toContain('ONLY `azfs://{account}/{container}/{blob}`');
            expect(get()).toContain('abfss://');
        });
    });

    describe('property completeness sweep', () => {
        // The single root cause behind 6 of the 7 failures in the run: a configured node
        // property was silently dropped. Both checklists previously enumerated four named
        // properties; every actual failure involved one that was not on that list.
        it.each([
            ['Java', JAVA_PROMPT],
            ['Python', PYTHON_PROMPT],
        ])('%s checklist requires every node property to be accounted for', (_lang, prompt) => {
            expect(prompt).toContain('Walk the `properties` map of EVERY node');
            expect(prompt).toContain('Silently dropping a configured property is a defect');
        });
    });

    /**
     * Beam version floor.
     *
     * The pins should track the newest Beam release; this suite deliberately asserts a
     * FLOOR rather than an exact version so routine upgrades do not require a test edit,
     * while a regression below the floor fails loudly.
     *
     * Why 2.65 and not something lower:
     *   - Managed I/O (`org.apache.beam.sdk.managed`) does not exist before Java 2.58 /
     *     Python 2.61. It is the only supported way to reach Iceberg.
     *   - The Iceberg config keys the UI collects (`partition_fields`, `table_properties`,
     *     `keep`/`drop`/`filter`) landed after 2.60. Managed I/O validates config keys at
     *     construction, so an unknown key is a pipeline construction failure, not a no-op.
     *   2.65 is the first floor that clears both.
     *
     * The prompts must keep pinning an EXPLICIT version. Telling the model to "use the
     * latest version" is what produced the Datafaker 2.1.0 / timeAndDate() build failure
     * guarded above: the model cannot resolve "latest" and invents a plausible number.
     */
    describe('Beam version floor', () => {
        const MIN_BEAM = [2, 65, 0];
        // NOTE: skills hold `{{beam_version}}`. The pin that the model sees is the resolved
        // version in the BUILT prompt, so these assertions read built prompts that use the
        // fallback version (the floor applies to the version the extension ships with).
        const JAVA_PROMPT = buildExtensionPrompt(REPO_CONTEXT, 'nodes: []\n', 'Java').prompt;
        const PYTHON_PROMPT = buildExtensionPrompt(REPO_CONTEXT, 'nodes: []\n', 'Python').prompt;

        const parseVersion = (v: string): number[] => v.split('.').map(n => parseInt(n, 10));

        const gte = (a: number[], b: number[]): boolean => {
            for (let i = 0; i < Math.max(a.length, b.length); i++) {
                const x = a[i] ?? 0;
                const y = b[i] ?? 0;
                if (x > y) { return true; }
                if (x < y) { return false; }
            }
            return true;
        };

        it('the Java prompt pins a Beam version at or above the Managed I/O floor', () => {
            const pin = JAVA_PROMPT.match(/<beam\.version>\s*([0-9][0-9.]*)\s*<\/beam\.version>/);
            expect(pin, 'expected an explicit <beam.version> pin in the Java prompt').not.toBeNull();
            if (!pin) { return; }
            expect(
                gte(parseVersion(pin[1]), MIN_BEAM),
                `Java prompt pins Beam ${pin[1]}, below the ${MIN_BEAM.join('.')} floor required ` +
                `for Managed I/O and its Iceberg config keys`
            ).toBe(true);
        });

        it('the Python prompt pins a Beam version at or above the Managed I/O floor', () => {
            const pin = PYTHON_PROMPT.match(/apache-beam\[gcp\]>=\s*([0-9][0-9.]*)/);
            expect(pin, 'expected an explicit apache-beam[gcp] pin in the Python prompt').not.toBeNull();
            if (!pin) { return; }
            expect(
                gte(parseVersion(pin[1]), MIN_BEAM),
                `Python prompt pins Beam ${pin[1]}, below the ${MIN_BEAM.join('.')} floor required ` +
                `for Managed I/O and its Iceberg config keys`
            ).toBe(true);
        });

        it('the Python requirements pin and the Dockerfile base image agree', () => {
            // A mismatch here means the container ships one SDK while the job requests
            // another, which surfaces as confusing runtime errors rather than a build failure.
            const req = PYTHON_PROMPT.match(/apache-beam\[gcp\]>=\s*([0-9][0-9.]*)/);
            const img = PYTHON_PROMPT.match(/apache\/beam_python3\.11_sdk:([0-9][0-9.]*)/);
            expect(req).not.toBeNull();
            expect(img).not.toBeNull();
            if (!req || !img) { return; }
            expect(img[1]).toBe(req[1]);
        });
    });

    /**
     * Flex Template parameterization. §3A and checklist #17 previously contradicted each
     * other outright -- §3A said never expose non-sensitive properties as options, #17 said
     * every IO endpoint must be one. A generator satisfying either violated the other.
     */
    describe('deployment-varying property carve-out', () => {
        it.each([
            ['Java', JAVA_PROMPT],
            ['Python', PYTHON_PROMPT],
        ])('%s prompt exposes input and output locations as runtime options', (_lang, prompt) => {
            expect(prompt).toContain('DEPLOYMENT-VARYING PROPERTIES MUST BE RUNTIME OPTIONS');
            expect(prompt).toContain('**Input locations**');
            expect(prompt).toContain('**Output locations**');
        });

        it.each([
            ['Java', JAVA_PROMPT],
            ['Python', PYTHON_PROMPT],
        ])('%s prompt no longer blanket-forbids exposing non-sensitive properties', (_lang, prompt) => {
            // The exact sentence that contradicted checklist #17.
            expect(prompt).not.toContain('NEVER expose non-sensitive properties');
        });
    });

    /**
     * Integration test report remediation directives regression suite.
     * Validates Directives P1-P3 (Python), J1-J2 (Java), and runner scaling propagation rules.
     */
    describe('prompt remediation directives from integration test report (regression)', () => {
        // Python GenerateSequence rate-limiting
        it('Python prompt enforces rate-limited sequence generation and forbids silent property drops', () => {
            // NOTE: Must specify PeriodicImpulse with counter DoFn or explicit # NOTE: comment
            expect(PYTHON_PROMPT).toContain('ratePerSec');
            expect(PYTHON_PROMPT).toContain('elementsPerPeriod');
            expect(PYTHON_PROMPT).toContain('PeriodicImpulse');
            expect(PYTHON_PROMPT).toContain('# NOTE: ratePerSec = <val> is not supported directly in in-memory beam.Create batch execution');
            expect(PYTHON_PROMPT).toMatch(/NEVER silently drop or omit `ratePerSec`/);
        });

        // Python entrypoint quote consistency
        it('Python prompt mandates matching quotes in entrypoint guard to prevent SyntaxError', () => {
            // NOTE: Mismatched quotes like "__main__' trigger fatal SyntaxError: unterminated string literal
            expect(PYTHON_PROMPT).toContain("either `if __name__ == '__main__': run()` or `if __name__ == \"__main__\": run()`");
            expect(PYTHON_PROMPT).toContain('never mix quotation styles');
        });

        // Cloud Build tag pinning (Both languages)
        it.each([
            ['Java', JAVA_PROMPT],
            ['Python', PYTHON_PROMPT],
        ])('%s prompt pins docker builder image tag in cloudbuild.yaml', (_lang, prompt) => {
            // NOTE: Builders without tag trigger fatal container image pull failures in Cloud Build
            expect(prompt).toContain("'gcr.io/cloud-builders/docker:latest'");
        });

        // Java KafkaIO Exactly-Once Semantics (withEOS)
        it('Java prompt enforces withEOS on KafkaIO sink when useEOS: true is specified', () => {
            // NOTE: KafkaIO sink must configure withEOS or idempotent producer properties
            expect(JAVA_PROMPT).toContain('Exactly-Once Semantics (`useEOS`)');
            expect(JAVA_PROMPT).toContain('.withEOS(numShards > 0 ? numShards : 1, sinkGroupId != null ? sinkGroupId : "eos-sink-group")');
            expect(JAVA_PROMPT).toMatch(/NEVER silently omit EOS when specified in YAML/);
        });

        // Java Combiner Optimizations after GroupByKey
        it('Java prompt recommends Combine.groupedValues or Combine.perKey after GroupByKey', () => {
            // NOTE: GroupByKey followed by reduction should use Combine.groupedValues rather than manual loop
            expect(JAVA_PROMPT).toContain('Combiner Optimizations after `GroupByKey`');
            expect(JAVA_PROMPT).toContain('Combine.groupedValues');
        });

        // Worker Scaling Propagation (Both languages)
        it.each([
            ['Java', JAVA_PROMPT],
            ['Python', PYTHON_PROMPT],
        ])('%s prompt enforces propagating jobConfig machineType, minInstances, and maxInstances', (_lang, prompt) => {
            // NOTE: Runner script must propagate scaling parameters to Dataflow Flex Template execution
            expect(prompt).toContain('Job Configuration & Scaling Propagation');
            expect(prompt).toContain('--worker-machine-type="${MACHINE_TYPE:-<machineType>}"');
            expect(prompt).toContain('--num-workers="${MIN_INSTANCES:-<minInstances>}"');
            expect(prompt).toContain('--max-workers="${MAX_INSTANCES:-<maxInstances>}"');
        });

        // Truncation prevention: Java SchemaBuilder mandate & Python JSON-safe quoting
        it('Java prompt mandates SchemaBuilder and forbids multi-line JSON schema strings', () => {
            expect(JAVA_PROMPT).toContain('MANDATORY `SchemaBuilder` RULE — NO MULTI-LINE JSON SCHEMA STRINGS');
            expect(JAVA_PROMPT).toContain('SchemaBuilder.record("Record")');
        });

        it('Python prompt enforces JSON-safe quoting and avoids triple-quoted docstrings in _access_secret', () => {
            expect(PYTHON_PROMPT).toContain('# Reads the payload of a Secret Manager secret.');
            expect(PYTHON_PROMPT).not.toContain('"""Reads the payload of a Secret Manager secret."""');
            expect(PYTHON_PROMPT).toContain('JSON-SAFE QUOTING IN GENERATED CODE');
        });

        // ContextualTextIO and ElasticsearchIO Maven artifacts
        it('Java prompt specifies correct Maven artifacts for ContextualTextIO and ElasticsearchIO', () => {
            // NOTE: the old prompt named beam-sdks-java-extensions-contextual-text-io, which is not on Maven Central.
            // The skills now name the doc-verified artifact (conflict resolved per user decision).
            expect(JAVA_PROMPT).toContain('beam-sdks-java-io-contextualtextio');
            expect(JAVA_PROMPT).toContain('NEVER use `beam-sdks-java-extensions-contextual-text-io`');
            expect(JAVA_PROMPT).toContain('org.apache.beam:beam-sdks-java-io-elasticsearch');
            expect(JAVA_PROMPT).toContain('NEVER `beam-sdks-java-io-elasticsearch-tests`');
        });

        // S3Options getter collision & MapElements.as() prohibition
        it('Java prompt forbids redeclaring String getAwsRegion() on S3Options and forbids MapElements.as()', () => {
            expect(JAVA_PROMPT).toContain('CRITICAL `S3Options` GETTER COLLISION RULE');
            expect(JAVA_PROMPT).toContain('No `.as(String)` Method on `MapElements`');
        });

        // Python FileIO prefix, JsonIO beam.Row, MongoDbIO, Pyodbc, and Flatten single-input rules
        it('Python prompt includes FileIO destination_prefix_naming, JsonIO beam.Row, MongoDbIO, Pyodbc, and Flatten rules', () => {
            expect(PYTHON_PROMPT).toContain('fileio.destination_prefix_naming(prefix=..., suffix=...)');
            expect(PYTHON_PROMPT).toContain('CRITICAL SCHEMA REQUIREMENT');
            expect(PYTHON_PROMPT).toContain('ReadFromMongoDB, WriteToMongoDB');
            expect(PYTHON_PROMPT).toContain('ODBC Connection String Braces Rule (CRITICAL)');
            expect(PYTHON_PROMPT).toContain("(upstream_pcoll,) | '<step_name>' >> beam.Flatten()");
        });

        // Anti-bloat guard: neither prompt should contain internal "(Directive J1/P1)" bookkeeping tags
        it.each([
            ['Java', JAVA_PROMPT],
            ['Python', PYTHON_PROMPT],
        ])('%s prompt is free of internal (Directive J/P) bookkeeping tags', (_lang, prompt) => {
            expect(prompt).not.toMatch(/\(Directives?\s+[PJ]\d+/);
        });

        it('Java prompt mandates PAssert.that(...) on every @Test method before p.run().waitUntilFinish()', () => {
            expect(JAVA_PROMPT).toContain('Every `@Test` method in `src/test/java/com/example/<PipelineName>Test.java` MUST validate output elements using `PAssert.that(...)` prior to calling `p.run().waitUntilFinish()`');
            expect(JAVA_PROMPT).toContain('never emit test methods that execute transforms without verifying the resulting `PCollection`');
        });

        it.each([
            ['Java', JAVA_PROMPT],
            ['Python', PYTHON_PROMPT],
        ])('%s prompt forbids YAML sequence syntax (- _TAG: ...) under substitutions in cloudbuild.yaml', (_lang, prompt) => {
            expect(prompt).toContain('NEVER use list/sequence syntax (`- _TAG: ...`) under `substitutions:`');
        });
    });

    describe('Spark Version Baseline & File Deduplication', () => {

        it('uses DEFAULT_SPARK_VERSION constant as the Spark baseline', () => {
            expect(DEFAULT_SPARK_VERSION).toBe('4.0');
        });

        it('replaces {{beam_version}} in skill text and templates when a newer version is supplied', () => {
            const { prompt } = buildExtensionPrompt(REPO_CONTEXT, 'nodes: []\n', 'Python', '2.79.0');
            expect(prompt).toContain('apache-beam[gcp]>=2.79.0');
            expect(prompt).toMatch(/beam_python\$\{IMAGE_PYTHON_VERSION\}_sdk:2\.79\.0/);
            expect(prompt).not.toContain('{{beam_version}}');
        });

        it('does not rewrite version strings inside the user pipeline YAML (F7)', () => {
            const yaml = 'note: "keep 2.76.0 and {{beam_version}} literally"\n';
            const { prompt } = buildExtensionPrompt(REPO_CONTEXT, yaml, 'Python', '2.79.0');
            expect(prompt).toContain('apache-beam[gcp]>=2.79.0');
            expect(prompt).toContain('note: "keep 2.76.0 and {{beam_version}} literally"');
        });

        it('fallback version substitution also leaves the user pipeline YAML untouched', () => {
            const { prompt } = buildExtensionPrompt(REPO_CONTEXT, 'note: "{{beam_version}}"\n', 'Java');
            expect(prompt).toContain(DEFAULT_SPARK_VERSION);
            expect(prompt).toContain('note: "{{beam_version}}"');
        });

        it('deduplicateGeneratedFiles keeps the longest non-truncated entry per filename', () => {
            const rawFiles = [
                { filename: 'src/main/java/com/example/Pipe.java', content: 'package com.example;\npublic class Pipe {\n  public static void main(String[] args) {\n    System.out.println("complete");\n  }\n}\n' },
                { filename: 'pom.xml', content: '<project></project>' },
                { filename: 'src/main/java/com/example/Pipe.java', content: 'package com.example;\npublic class Pipe {\n  // truncated' },
            ];
            const deduped = deduplicateGeneratedFiles(rawFiles);
            expect(deduped).toHaveLength(2);
            expect(deduped[0].filename).toBe('src/main/java/com/example/Pipe.java');
            expect(deduped[0].content).toContain('System.out.println("complete");');
        });

        it('ensureSystemCaBundle loads the system CA bundle into https.globalAgent.options.ca and patches tls.createSecureContext', () => {
            const origEnv = process.env.NODE_EXTRA_CA_CERTS;
            const origSslFile = process.env.SSL_CERT_FILE;
            const origCa = https.globalAgent.options.ca;
            const prevCreateSecureContext = tls.createSecureContext;
            delete process.env.NODE_EXTRA_CA_CERTS;
            delete process.env.SSL_CERT_FILE;

            const tempDir = fs.mkdtempSync(path.join(process.env.TMPDIR || '/tmp', 'beam-ca-'));
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

            const tempDir = fs.mkdtempSync(path.join(process.env.TMPDIR || '/tmp', 'beam-ca-'));
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
});

/**
 * Regression tests for skill fixes found by EXECUTING generated projects (A/B run, Phase 6).
 *
 * - windowing-java: the model imported `org.apache.beam.sdk.coders.AvroCoder` (not in the core SDK)
 *   and left a custom POJO without a coder ("Unable to return a default Coder").
 * - combine-java: `Combine.globally(CombineFnWithContext)` threw "Override this function to provide
 *   the default value" because `withoutDefaults()` was missing.
 * - sample-mockdata-py / periodic-src-py: a `beam.Map` lambda captured a generator, so Beam could
 *   not pickle the function.
 */
describe('Skill fixes from execution failures (regression)', () => {
    it('lang-java forbids the core AvroCoder import and requires a coder for custom classes', () => {
        const text = skillText('lang-java');
        expect(text).toContain('Never import `org.apache.beam.sdk.coders.AvroCoder`');
        expect(text).toContain('@DefaultCoder(SerializableCoder.class)');
    });

    it('transforms-aggregation (Java) requires withoutDefaults() for a global CombineFnWithContext', () => {
        expect(skillText('transforms-aggregation', 'java')).toContain('Combine.globally(fn).withoutDefaults()');
    });

    it('lang-python forbids capturing generators or clients in lambdas and DoFns', () => {
        expect(skillText('lang-python')).toContain("cannot pickle 'generator' object");
    });
});

describe('Skill fix: picklable assert_that matchers (regression)', () => {
    it('testing-python forbids lambdas that capture self inside assert_that', () => {
        expect(skillText('testing-python')).toContain('Never pass a lambda that captures `self`');
    });
});

describe('Skill fix from Phase 3d execution: Beam schema package (regression)', () => {
    it('lang-java names org.apache.beam.sdk.schemas and forbids the singular package', () => {
        // jdbc-source, multidb-yaml-source, and singlestoredb-source failed to compile with
        // `import org.apache.beam.sdk.schema.Schema`.
        const text = skillText('lang-java');
        expect(text).toContain('org.apache.beam.sdk.schemas.Schema');
        expect(text).toContain('The package `org.apache.beam.sdk.schema` does not exist');
    });
});

describe('Skill fixes from Phase 3d execution: Python matchers and FHIR dependency (regression)', () => {
    it('testing-python forbids nested defs that reference self inside assert_that (sinks-python-jms-sink)', () => {
        expect(skillText('testing-python')).toContain('never pass a nested `def` that references `self`');
    });

    it('io-fhir (Java) forbids pinning google-api-services-healthcare (sinks-java-fhir-sink)', () => {
        expect(skillText('io-fhir', 'java')).toContain('Do not declare `com.google.apis:google-api-services-healthcare`');
    });
});

describe('Skill fixes from Phase 3d execution: Java shadowing and test serialization (regression)', () => {
    it('lang-java warns that DoFn nested annotations shadow imported classes (datastore, spanner)', () => {
        expect(skillText('lang-java')).toContain('shadow imported classes with the same simple name');
    });

    it('testing-java forbids anonymous functions inside test methods (sinks-java-influxdb-sink)', () => {
        expect(skillText('testing-java')).toContain('Never create an anonymous `SimpleFunction`, `DoFn`, or `SerializableFunction` inside a test method');
    });

    it('io-datastore (Java) forbids nesting makeValue calls (sinks-java-datastore-sink)', () => {
        expect(skillText('io-datastore', 'java')).toContain('Never pass a `Value.Builder` back into `makeValue`');
    });
});

describe('Skill fix from Phase 3d execution: exact test expectations (regression)', () => {
    it('testing-python requires expected values traced from the code (sources-python-database-source)', () => {
        expect(skillText('testing-python')).toContain('Compute every expected value by tracing the exact pipeline code');
    });
});

describe('Skill fix from Phase 3d execution: tests reuse production helpers (regression)', () => {
    it.each(['testing-java', 'testing-python'])('%s forbids re-implementing production logic in tests (steps-java-pardo-side-inputs)', (skill) => {
        expect(skillText(skill)).toContain('Never re-implement parsing, keying, or formatting logic inside the test');
    });
});

describe('Skill fixes from Phase 3d execution: Java literals and exact expectations (regression)', () => {
    it('lang-java steers away from backslash-heavy string literals (hadoopformat-source, iceberg-source)', () => {
        expect(skillText('lang-java')).toContain('Keep backslashes out of Java string literals');
    });

    it('testing-java requires expected values traced from the code (contextualtextio-source)', () => {
        expect(skillText('testing-java')).toContain('Compute every expected value by tracing the exact pipeline code');
    });
});

describe('Skill fixes from Phase 3d execution: GcpOptions and system libraries (regression)', () => {
    it('credentials-and-options (Java) requires GcpOptions for getProject() (sources-java-googleads-source)', () => {
        expect(skillText('credentials-and-options', 'java')).toContain('`getProject()` exists only on `GcpOptions`');
    });

    it('runner-dataflow (Python) installs connector system libraries in the Dockerfile (pyodbc)', () => {
        expect(skillText('runner-dataflow', 'python')).toContain('If a selected connector needs system libraries');
    });
});
