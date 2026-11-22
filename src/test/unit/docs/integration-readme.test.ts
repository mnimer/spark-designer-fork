/**
 * Integration Suite README Documentation Invariant Tests (DOC6).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Verifies `src/test/integration/README.md` against on-disk E2E suites, dual-paradigm contracts,
 * `SparkArchitectureAgent` checks, and offline `pytest` mechanics. Also enforces single-header
 * author credit, ASD-STE100 prose rules, and banned legacy token guards.
 */

import * as fs from 'fs';
import * as path from 'path';
import { describe, expect, it } from 'vitest';
import {
    REQUIRED_SDP_DELIVERABLES,
    REQUIRED_SPARK_DELIVERABLES,
} from '../../integration/evaluators/deterministic-gate';
import { SPARK_ARCHITECTURE_AGENT_NAME } from '../../integration/evaluators/spark-architecture-evaluator';
import {
    SPARK_SESSION_PROBE_SCRIPT,
    TOOLING_PROBE_SCRIPT,
} from '../../integration/pytest-runner';

const REPO_ROOT = path.resolve(__dirname, '../../../..');
const INTEGRATION_DIR = path.join(REPO_ROOT, 'src', 'test', 'integration');
const INTEGRATION_README_PATH = path.join(INTEGRATION_DIR, 'README.md');
const E2E_DIR = path.join(INTEGRATION_DIR, 'e2e');
const EVALUATORS_DIR = path.join(INTEGRATION_DIR, 'evaluators');
const PACKAGE_JSON_PATH = path.join(REPO_ROOT, 'package.json');

/** Sentence length violation detected by the ASD-STE100 prose checker. */
interface ProseLengthViolation {
    lineNumber: number;
    wordCount: number;
    maxWords: number;
    sentence: string;
}

/** Banned legacy pattern built from split fragments to avoid literal matches in scans. */
interface BannedTokenRule {
    label: string;
    pattern: RegExp;
}

/**
 * Removes fenced Markdown code blocks while preserving surrounding line breaks.
 *
 * @param markdown Raw Markdown document text.
 * @returns Markdown text with fenced code block contents replaced by empty lines.
 */
export function stripFencedCodeBlocks(markdown: string): string {
    const lines = markdown.split('\n');
    const output: string[] = [];
    let inFence = false;

    for (const line of lines) {
        if (/^\s*```/.test(line)) {
            inFence = !inFence;
            output.push('');
            continue;
        }
        output.push(inFence ? '' : line);
    }

    return output.join('\n');
}

/**
 * Audits Markdown prose lines outside fenced code blocks against ASD-STE100 word limits.
 *
 * @param markdown Raw Markdown document text.
 * @returns List of sentences that exceed step (`<= 20`) or prose (`<= 25`) word limits.
 */
export function findSte100WordLimitViolations(markdown: string): ProseLengthViolation[] {
    const proseOnly = stripFencedCodeBlocks(markdown);
    const violations: ProseLengthViolation[] = [];

    proseOnly.split('\n').forEach((rawLine, index) => {
        const trimmed = rawLine.trim();
        if (
            !trimmed ||
            trimmed.startsWith('#') ||
            trimmed.startsWith('---') ||
            trimmed.startsWith('> [!') ||
            /^\|[\s:-]+\|$/.test(trimmed)
        ) {
            return;
        }

        const isNumberedStep = /^\d+\.\s+/.test(trimmed);
        const maxWords = isNumberedStep ? 20 : 25;
        const segments = trimmed.startsWith('|')
            ? trimmed
                  .split('|')
                  .slice(1, -1)
                  .map((cell) => cell.trim())
                  .filter(Boolean)
            : [trimmed];

        for (const segment of segments) {
            const masked = segment.replace(/`[^`]*`/g, (span) => span.replace(/\./g, '_DOT_'));
            const sentences = masked
                .split(/(?<=[.!?])\s+/)
                .map((part) => part.replace(/_DOT_/g, '.').trim())
                .filter(Boolean);

            for (const sentence of sentences) {
                const cleaned = sentence
                    .replace(/^(?:>\s*|\d+\.\s+|[-*]\s+)/, '')
                    .replace(/\*\*/g, '');
                const wordCount = cleaned.split(/\s+/).filter(Boolean).length;
                if (wordCount > maxWords) {
                    violations.push({
                        lineNumber: index + 1,
                        wordCount,
                        maxWords,
                        sentence,
                    });
                }
            }
        }
    });

    return violations;
}

/**
 * Builds banned legacy token regular expressions without embedding literal forbidden strings.
 *
 * @returns Dynamic regular expression rules for legacy framework and model names.
 */
function buildBannedLegacyPatterns(): BannedTokenRule[] {
    const legacyFramework = 'be' + 'am';
    const legacyModelPrefix = 'gem' + 'ini';
    const legacyTitlePrefix = 'Gem' + 'ini';

    return [
        {
            label: 'legacy pipeline extension',
            pattern: new RegExp('\\.' + legacyFramework + '\\b', 'i'),
        },
        {
            label: 'legacy generator folder',
            pattern: new RegExp('\\.' + legacyFramework + 'gen\\b', 'i'),
        },
        {
            label: 'legacy snake_case transform key',
            pattern: new RegExp('\\b' + legacyFramework + '_transform\\b', 'i'),
        },
        {
            label: 'legacy camelCase transform key',
            pattern: new RegExp('\\b' + legacyFramework + 'Transform\\b'),
        },
        {
            label: 'deprecated 2.5 model identifier',
            pattern: new RegExp(legacyModelPrefix + '-2\\.5', 'i'),
        },
        {
            label: 'deprecated 1.x model identifier',
            pattern: new RegExp(legacyModelPrefix + '-1\\.', 'i'),
        },
        {
            label: 'deprecated 2.x model identifier',
            pattern: new RegExp(legacyModelPrefix + '-2\\.', 'i'),
        },
        {
            label: 'stale 3.8 prose model name',
            pattern: new RegExp(legacyTitlePrefix + '\\s+3\\.8', 'i'),
        },
    ];
}

describe('Markdown inspection helpers (normal, boundary, and failure cases)', () => {
    it('strips fenced code blocks while keeping prose lines intact', () => {
        const sample = [
            'Intro prose line.',
            '```bash',
            'A=1; B=2',
            '```',
            'Closing prose line.',
        ].join('\n');

        const stripped = stripFencedCodeBlocks(sample);
        expect(stripped).toContain('Intro prose line.');
        expect(stripped).toContain('Closing prose line.');
        expect(stripped).not.toContain('A=1; B=2');
    });

    it('handles empty documents and unclosed code fences cleanly', () => {
        expect(stripFencedCodeBlocks('')).toBe('');
        const unclosed = 'Lead line.\n```python\nx = 1;\ny = 2;';
        expect(stripFencedCodeBlocks(unclosed)).toBe('Lead line.\n\n\n');
    });

    it('flags numbered steps over 20 words and descriptive prose over 25 words', () => {
        const longStep =
            '1. one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty twentyone.';
        const longProse =
            'alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau upsilon phi chi psi omega extra overflow.';

        const violations = findSte100WordLimitViolations(`${longStep}\n\n${longProse}`);
        expect(violations).toHaveLength(2);
        expect(violations[0]).toMatchObject({ lineNumber: 1, maxWords: 20, wordCount: 21 });
        expect(violations[1]).toMatchObject({ lineNumber: 3, maxWords: 25, wordCount: 26 });
    });
});

describe('src/test/integration/README.md (DOC6)', () => {
    const readme = fs.readFileSync(INTEGRATION_README_PATH, 'utf-8');
    const proseOnly = stripFencedCodeBlocks(readme);
    const packageJson = JSON.parse(fs.readFileSync(PACKAGE_JSON_PATH, 'utf-8')) as {
        scripts?: Record<string, string>;
    };

    it('declares the single-header author credit once at the top of the document', () => {
        expect(readme).toContain('> **Author**: Mike Nimer <mikenimer@google.com>');
        const authorMatches = readme.match(/Author/gi) ?? [];
        expect(authorMatches).toHaveLength(1);
    });

    it('satisfies ASD-STE100 prose rules with zero prose semicolons and bounded sentence lengths', () => {
        expect(proseOnly).not.toContain(';');
        expect(findSte100WordLimitViolations(readme)).toEqual([]);
    });

    it('contains zero banned legacy extensions, transform keys, or deprecated Gemini names', () => {
        for (const rule of buildBannedLegacyPatterns()) {
            expect(readme, `Unexpected match for ${rule.label}`).not.toMatch(rule.pattern);
        }
    });

    it('documents every on-disk E2E pipeline suite folder and removes stale Phase 8 text', () => {
        const e2eFolders = fs
            .readdirSync(E2E_DIR, { withFileTypes: true })
            .filter((entry) => entry.isDirectory())
            .map((entry) => entry.name)
            .sort();

        expect(e2eFolders).toEqual([
            'batch-fanout-auto-cache',
            'multi-input-spark-sql',
            'spark-ml-xgboost-inference',
            'stream-static-join-enrichment',
            'streaming-multi-sink-foreachbatch',
            'zero-transform-csv-to-parquet',
        ]);

        for (const suiteName of e2eFolders) {
            expect(readme, `Missing E2E suite '${suiteName}' in README`).toContain(suiteName);
        }

        expect(readme).not.toMatch(/added in Phase 9/i);
        expect(readme).toContain('integration-suites-topology.test.ts');
        expect(readme).toContain('validateDagTopology');
    });

    it('documents every top-level harness module and evaluator file in src/test/integration/', () => {
        const topLevelTs = fs
            .readdirSync(INTEGRATION_DIR, { withFileTypes: true })
            .filter((entry) => entry.isFile() && entry.name.endsWith('.ts'))
            .map((entry) => entry.name);
        const evaluatorTs = fs
            .readdirSync(EVALUATORS_DIR, { withFileTypes: true })
            .filter((entry) => entry.isFile() && entry.name.endsWith('.ts'))
            .map((entry) => entry.name);

        expect(topLevelTs.length).toBeGreaterThan(0);
        expect(evaluatorTs.length).toBeGreaterThan(0);

        for (const file of [...topLevelTs, ...evaluatorTs]) {
            expect(readme, `Missing integration module '${file}' in README`).toContain(file);
        }
    });

    it('documents both Imperative (Decision D3) and SDP (Spark 4.2+) deliverable contracts', () => {
        for (const deliverable of REQUIRED_SPARK_DELIVERABLES) {
            expect(readme, `Missing Imperative deliverable '${deliverable}'`).toContain(deliverable);
        }
        for (const deliverable of REQUIRED_SDP_DELIVERABLES) {
            expect(readme, `Missing SDP deliverable '${deliverable}'`).toContain(deliverable);
        }

        expect(readme).toContain('read_sources(spark, config)');
        expect(readme).toContain('transform(dfs, config)');
        expect(readme).toContain('write_sinks(dfs, config)');
        expect(readme).toContain('pyspark==4.0.*');
        expect(readme).toContain('1099:1099');
        expect(readme).toContain('transformations/*.py');
        expect(readme).toContain('@dp.table');
        expect(readme).toContain('@dp.materialized_view');
    });

    it('documents live Vertex AI prerequisites, integration npm scripts, and SparkArchitectureAgent rules', () => {
        const requiredScripts = [
            'typecheck:integration',
            'test:integration',
            'test:integration:e2e',
            'test:integration:pytest',
            'test:integration:watch',
        ];
        for (const scriptName of requiredScripts) {
            expect(packageJson.scripts?.[scriptName]).toBeTruthy();
            expect(readme).toContain(`npm run ${scriptName}`);
        }

        expect(readme).toContain('tsconfig.integration.json');
        expect(readme).toContain('vitest.integration.config.mts');
        expect(readme).toContain('gcloud auth application-default login');
        expect(readme).toContain('GOOGLE_CLOUD_PROJECT');
        expect(readme).toContain('gcloud config get-value project');
        expect(readme).toContain("location: 'global'");
        expect(readme).toContain('gemini-4-hydrogen');

        expect(readme).toContain(SPARK_ARCHITECTURE_AGENT_NAME);
        expect(readme).toContain('runSparkArchitectureStaticChecks');
        expect(readme).toContain('runSdpArchitectureStaticChecks');
        expect(readme).toContain('evaluateSparkArchitecture');
        expect(readme).toContain('.collect()');
        expect(readme).toContain('.toPandas()');
        expect(readme).toContain('.cache()');
        expect(readme).toContain('.unpersist()');
        expect(readme).toContain('foreachBatch');
        expect(readme).toContain('txnVersion');
        expect(readme).toContain('txnAppId');
        expect(readme).toContain('withWatermark');
        expect(readme).toContain('_jsc');
        expect(readme).toContain('_jvm');
        expect(readme).toContain('spark-bigquery');
        expect(readme).toContain('iceberg-spark-runtime');
        expect(readme).toContain('gcs-connector');
        expect(readme).toContain('SparkSession.builder');
        expect(readme).toContain('spark.conf.set');
        expect(readme).toContain('stored_as_scd_type=2');
        expect(readme).toContain('cloudFiles');

        expect(readme).toContain('spark-pipelines dry-run');
        expect(readme).toContain('PipelineSpecificationAgent');
        expect(readme).toContain('SecurityAndHardeningAgent');
        expect(readme).toContain('PackagingAndDeployabilityAgent');
        expect(readme).toContain('Lead Arbiter');
        expect(readme).toContain('resources/codegen-skills/');
        expect(readme).toContain('test-report.md');
    });

    it('documents offline pytest-runner JVM setup, probe scripts, conftest injection, and unit suites', () => {
        expect(TOOLING_PROBE_SCRIPT).toContain('pyspark');
        expect(SPARK_SESSION_PROBE_SCRIPT).toContain('SparkSession.builder.master("local[2]")');

        expect(readme).toContain('JAVA_HOME');
        expect(readme).toContain('JDK 17 or JDK 21');
        expect(readme).toContain('getSubject is not supported');
        expect(readme).toContain('SPARK_PYTEST_PYTHON');
        expect(readme).toContain('pyspark>=4');
        expect(readme).toContain(
            'JAVA_HOME=/path/to/jdk-21 SPARK_PYTEST_PYTHON=/path/to/python npm run test:integration:pytest'
        );
        expect(readme).toContain('runGeneratedRepoTests');
        expect(readme).toContain('TOOLING_PROBE_SCRIPT');
        expect(readme).toContain('SPARK_SESSION_PROBE_SCRIPT');
        expect(readme).toContain('SparkSession.builder.master("local[2]")');
        expect(readme).toContain('conftest.py');
        expect(readme).toContain('execFileSafe');
        expect(readme).toContain('envSanitizer');
        expect(readme).toContain('parseJUnitXml');
        expect(readme).toContain("status: 'skipped'");

        const expectedUnitSuites = [
            'src/test/unit/deterministic-gate.test.ts',
            'src/test/unit/spark-architecture-evaluator.test.ts',
            'src/test/unit/pipeline-spec-evaluator.test.ts',
            'src/test/unit/packaging-evaluator.test.ts',
            'src/test/unit/arbiter-evaluator.test.ts',
            'src/test/unit/multi-agent-evaluator.test.ts',
            'src/test/unit/pytest-runner.test.ts',
            'src/test/unit/codegen-runner-pytest-hook.test.ts',
            'src/test/unit/sdp/sdp-evaluators-and-gate.test.ts',
        ];
        for (const relSuite of expectedUnitSuites) {
            expect(fs.existsSync(path.join(REPO_ROOT, relSuite)), `Missing unit suite ${relSuite}`).toBe(true);
            expect(readme).toContain(relSuite);
        }
    });
});
