/**
 * Unit tests verifying README.md accuracy, catalog parity, and ASD-STE100 prose discipline.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates that README.md stays synchronized with bundled sample pipelines, catalog node
 * counts, Gemini model discovery defaults, package.json settings and scripts, and style rules.
 */

import * as fs from 'fs';
import * as path from 'path';
import { describe, expect, it } from 'vitest';
import { DEFAULT_GEMINI_MODEL } from '../../../core/constants';

const REPO_ROOT = path.resolve(__dirname, '../../../..');
const README_PATH = path.join(REPO_ROOT, 'README.md');
const PACKAGE_JSON_PATH = path.join(REPO_ROOT, 'package.json');
const WEBVIEW_PACKAGE_JSON_PATH = path.join(REPO_ROOT, 'webview', 'package.json');
const SAMPLES_DIR = path.join(REPO_ROOT, 'resources', 'samples');
const CATALOG_DIR = path.join(REPO_ROOT, 'webview', 'src', 'catalog', 'nodes');

/**
 * Strips fenced markdown code blocks while preserving prose lines and 1-based line numbers.
 *
 * @param markdown Raw markdown file contents.
 * @returns Prose line entries outside fenced code blocks.
 */
function stripFencedCodeBlocks(markdown: string): Array<{ lineNumber: number; text: string }> {
    const lines = markdown.split(/\r?\n/);
    const proseLines: Array<{ lineNumber: number; text: string }> = [];
    let inFence = false;

    lines.forEach((line, index) => {
        if (/^\s*```/.test(line)) {
            inFence = !inFence;
            return;
        }
        if (!inFence) {
            proseLines.push({ lineNumber: index + 1, text: line });
        }
    });

    return proseLines;
}

/**
 * Counts words in a markdown prose sentence after unwrapping inline links and formatting.
 *
 * @param sentence Single markdown prose sentence.
 * @returns Number of whitespace-separated words.
 */
function countProseWords(sentence: string): number {
    const cleaned = sentence
        .replace(/!\[[^\]]*\]\([^)]*\)/g, '')
        .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
        .replace(/`([^`]+)`/g, '$1')
        .replace(/\*\*([^*]+)\*\*/g, '$1')
        .replace(/\*([^*]+)\*/g, '$1')
        .trim();

    if (!cleaned) {
        return 0;
    }
    return cleaned.split(/\s+/).filter(Boolean).length;
}

/**
 * Counts `sparkTransform` node definitions inside a catalog source file.
 *
 * @param catalogFileName Catalog filename under `webview/src/catalog/nodes/`.
 * @returns Total `sparkTransform` entries in the catalog file.
 */
function countCatalogTransforms(catalogFileName: string): number {
    const filePath = path.join(CATALOG_DIR, catalogFileName);
    const content = fs.readFileSync(filePath, 'utf8');
    const matches = content.match(/sparkTransform:\s*['"][^'"]+['"]/g);
    return matches ? matches.length : 0;
}

/**
 * Builds banned legacy token patterns dynamically so repository literal scans stay clean.
 *
 * @returns Labeled regular expressions for forbidden legacy identifiers and deprecated models.
 */
function buildBannedTokenPatterns(): Array<{ label: string; pattern: RegExp }> {
    const legacyWord = ['be', 'am'].join('');
    const modelFamily = ['gem', 'ini'].join('');
    const legacyEmbedModel = ['textembedding', ['ge', 'cko'].join('')].join('-');
    return [
        { label: 'legacy file extension', pattern: new RegExp(`\\.${legacyWord}\\b`, 'i') },
        { label: 'legacy manifest token', pattern: new RegExp(`\\.?${legacyWord}gen\\b`, 'i') },
        { label: 'legacy snake_case transform key', pattern: new RegExp(`\\b${legacyWord}_transform\\b`, 'i') },
        { label: 'legacy camelCase transform key', pattern: new RegExp(`\\b${legacyWord}Transform\\b`) },
        { label: 'stale Flash badge label', pattern: new RegExp(`${modelFamily}\\s+3\\.8`, 'i') },
        { label: 'deprecated 2.5 model family', pattern: new RegExp(`${modelFamily}-2\\.5`, 'i') },
        { label: 'deprecated 1.x model family', pattern: new RegExp(`${modelFamily}-1\\.`, 'i') },
        { label: 'deprecated 2.x model family', pattern: new RegExp(`${modelFamily}-2\\.`, 'i') },
        { label: 'deprecated legacy embedding family', pattern: new RegExp(legacyEmbedModel, 'i') },
    ];
}

describe('README.md documentation invariants (DOC3)', () => {
    const readme = fs.readFileSync(README_PATH, 'utf8');

    it('contains a single Author credit header near the top of README.md', () => {
        const expectedAuthorCredit = ['Author:', 'Mike Nimer <mikenimer@google.com>'].join(' ');
        const authorPattern = new RegExp('Author:\\s*' + 'Mike Nimer <mikenimer@google\\.com>', 'g');
        const authorMatches = readme.match(authorPattern) ?? [];
        expect(authorMatches).toHaveLength(1);
        const topLines = readme.split(/\r?\n/).slice(0, 5).join('\n');
        expect(topLines).toContain(expectedAuthorCredit);
    });

    it('documents all six bundled .spark samples and rejects stale seven-sample claims', () => {
        const sampleFiles = fs
            .readdirSync(SAMPLES_DIR)
            .filter((entry) => entry.endsWith('.spark'))
            .sort();

        expect(sampleFiles).toEqual([
            'bigquery-cloudsql-broadcast-join.spark',
            'gcs-parquet-etl.spark',
            'generate-mock-data.spark',
            'kafka-streaming-to-iceberg.spark',
            'sdp-auto-cdc-scd1.spark',
            'sdp-biglake-iceberg-medallion.spark',
        ]);

        for (const sampleFile of sampleFiles) {
            expect(readme, `Missing sample ${sampleFile} in README.md`).toContain(`\`${sampleFile}\``);
        }

        const citedSampleMatches = [...readme.matchAll(/`([a-z0-9-]+\.spark)`/g)].map((match) => match[1]);
        const uniqueCitedSamples = [...new Set(citedSampleMatches)].sort();
        expect(uniqueCitedSamples).toEqual(sampleFiles);

        expect(readme).toMatch(/\bsix\b/i);
        expect(readme).not.toMatch(/\bseven\b/i);
    });

    it('cites exact catalog node counts matching sources.ts, steps.ts, and sinks.ts', () => {
        const sourceCount = countCatalogTransforms('sources.ts');
        const stepCount = countCatalogTransforms('steps.ts');
        const sinkCount = countCatalogTransforms('sinks.ts');
        const totalCount = sourceCount + stepCount + sinkCount;

        expect(sourceCount).toBe(40);
        expect(stepCount).toBe(33);
        expect(sinkCount).toBe(38);
        expect(totalCount).toBe(111);

        expect(readme).toContain(`${sourceCount} Sources`);
        expect(readme).toContain(`${stepCount} Steps`);
        expect(readme).toContain(`${sinkCount} Sinks`);
        expect(readme).toContain(`${totalCount} catalog nodes`);
    });

    it('documents Gemini 4 Hydrogen badge and dynamic Vertex AI global model discovery', () => {
        expect(DEFAULT_GEMINI_MODEL).toBe('gemini-4-hydrogen');
        expect(readme).toContain('Gemini%204%20Hydrogen');
        expect(readme).toContain(`\`${DEFAULT_GEMINI_MODEL}\``);
        expect(readme).toContain('DEFAULT_GEMINI_MODEL');
        expect(readme).toContain('ai.models.list');
        expect(readme).toContain('src/codegen/modelDiscovery.ts');
        expect(readme).toContain("location: 'global'");
        expect(readme).toContain('gemini-embedding-001');
    });

    it('covers dual paradigms, canvas badges and handles, security guards, planners, and runners', () => {
        const requiredTokens = [
            'imperative',
            'sdp',
            'SOURCE -> SINK',
            '`BATCH`',
            '`STREAM`',
            '`CACHE`',
            '`foreachBatch`',
            '`BROADCAST`',
            '`N-IN`',
            '`VIEW`',
            '`MV`',
            '`TABLE`',
            '`CDC`',
            '`SINK`',
            '`Join`',
            '`left`',
            '`right`',
            '`ConditionalSplit`',
            '`else`',
            '`error`',
            'redactPipelineDocument',
            '.sparkgen.json',
            'executionPlanner.ts',
            'sdpPlanner.ts',
            'versionMatrix.ts',
            'transform(dfs, config)',
            'src/transforms.py',
            'ReadOnlyCheckRunner',
            'run_local.sh',
            'run_dataproc_cluster.sh',
            'run_serverless_spark.sh',
            '`standard`',
            'run_sdp_local.sh',
            'sdp_stage.pipeline.yaml',
            'run_sdp_dataproc.sh',
            '`dry-run`',
            '`--refresh`',
            '`--full-refresh`',
            '`--full-refresh-all`',
            'SparkExplorer',
        ];

        for (const token of requiredTokens) {
            expect(readme, `Expected README.md to document ${token}`).toContain(token);
        }
    });

    it('documents every sparkDesigner.* configuration setting defined in package.json', () => {
        const pkgJson = JSON.parse(fs.readFileSync(PACKAGE_JSON_PATH, 'utf8')) as {
            contributes?: {
                configuration?: {
                    properties?: Record<string, { default?: unknown }>;
                };
            };
            scripts?: Record<string, string>;
        };
        const properties = pkgJson.contributes?.configuration?.properties ?? {};
        const settingKeys = Object.keys(properties).filter((key) => key.startsWith('sparkDesigner.'));

        expect(settingKeys.length).toBeGreaterThanOrEqual(2);
        for (const key of settingKeys) {
            expect(readme, `Missing extension setting ${key} in README.md`).toContain(`\`${key}\``);
        }
    });

    it('cites valid verification, lint, and integration test commands plus companion docs and license', () => {
        const pkgJson = JSON.parse(fs.readFileSync(PACKAGE_JSON_PATH, 'utf8')) as {
            scripts?: Record<string, string>;
        };
        const webviewPkgJson = JSON.parse(fs.readFileSync(WEBVIEW_PACKAGE_JSON_PATH, 'utf8')) as {
            scripts?: Record<string, string>;
        };
        const rootScripts = pkgJson.scripts ?? {};
        const webviewScripts = webviewPkgJson.scripts ?? {};

        const expectedRootScripts = [
            'compile',
            'typecheck:webview',
            'typecheck:integration',
            'lint',
            'build:webview',
            'test:all',
            'test:integration:e2e',
            'test:integration:pytest',
        ];

        for (const scriptName of expectedRootScripts) {
            expect(rootScripts[scriptName], `Missing script ${scriptName} in package.json`).toBeDefined();
            expect(readme).toContain(`npm run ${scriptName}`);
        }

        expect(webviewScripts.lint).toBeDefined();
        expect(readme).toContain('npm --prefix webview run lint');
        expect(readme).toContain('JAVA_HOME=');
        expect(readme).toContain('SPARK_PYTEST_PYTHON=');

        expect(readme).toContain('ARCHITECTURE.md');
        expect(readme).toContain('DESIGN.md');
        expect(readme).toContain('resources/codegen-skills/README.md');
        expect(readme).toContain('src/test/integration/README.md');
        expect(readme).toContain('[Apache 2.0 License](LICENSE.txt)');
        expect(fs.existsSync(path.join(REPO_ROOT, 'LICENSE.txt'))).toBe(true);
    });

    it('contains zero banned legacy identifiers or deprecated Gemini model tokens', () => {
        for (const { label, pattern } of buildBannedTokenPatterns()) {
            expect(readme, `Found forbidden ${label} in README.md`).not.toMatch(pattern);
        }
    });

    it('enforces ASD-STE100 prose rules: zero prose semicolons and bounded sentence lengths', () => {
        const proseLines = stripFencedCodeBlocks(readme);
        const semicolonLines = proseLines.filter((entry) => entry.text.includes(';'));
        expect(semicolonLines).toEqual([]);

        for (const { lineNumber, text } of proseLines) {
            const trimmed = text.trim();
            if (
                !trimmed ||
                trimmed.startsWith('<!--') ||
                trimmed.startsWith('#') ||
                trimmed === '---' ||
                /^\|[\s:-]+\|$/.test(trimmed) ||
                trimmed.startsWith('[![')
            ) {
                continue;
            }

            if (/^\d+\.\s+/.test(trimmed)) {
                const stepBody = trimmed.replace(/^\d+\.\s+/, '');
                for (const sentence of stepBody.split(/(?<=[.!?])\s+/)) {
                    const wordCount = countProseWords(sentence);
                    expect(
                        wordCount,
                        `Numbered step at line ${lineNumber} exceeds 20 words (${wordCount}): ${sentence}`
                    ).toBeLessThanOrEqual(20);
                }
                continue;
            }

            const cells = trimmed.startsWith('|')
                ? trimmed.split('|').slice(1, -1)
                : [trimmed.replace(/^-\s+/, '')];

            for (const cell of cells) {
                for (const sentence of cell.trim().split(/(?<=[.!?])\s+/)) {
                    const wordCount = countProseWords(sentence);
                    expect(
                        wordCount,
                        `Prose sentence at line ${lineNumber} exceeds 25 words (${wordCount}): ${sentence}`
                    ).toBeLessThanOrEqual(25);
                }
            }
        }
    });

    it('validates helper boundary cases for fenced code stripping and prose word counting', () => {
        const sampleMarkdown = [
            'Intro prose sentence.',
            '```bash',
            'echo "inside fence; semicolon allowed"',
            '```',
            'Trailing prose; with semicolon.',
        ].join('\n');

        const extracted = stripFencedCodeBlocks(sampleMarkdown);
        expect(extracted).toEqual([
            { lineNumber: 1, text: 'Intro prose sentence.' },
            { lineNumber: 5, text: 'Trailing prose; with semicolon.' },
        ]);

        expect(countProseWords('')).toBe(0);
        expect(countProseWords('![badge](https://example.com/b.svg)')).toBe(0);
        expect(countProseWords('Open [DESIGN.md](DESIGN.md) for **canvas** `BATCH` badges.')).toBe(6);
    });
});
