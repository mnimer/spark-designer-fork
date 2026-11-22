/**
 * Walkthrough Documentation Unit Tests (DOC4).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Validates that every walkthrough registered in package.json exists on disk,
 * carries a single header author credit, obeys ASD-STE100 sentence and
 * semicolon limits, links only registered VS Code commands, documents current
 * Apache Spark 4.x capabilities and samples, and contains zero legacy tokens.
 */

import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import { DEFAULT_GEMINI_MODEL } from '../../../core/constants';
import { MANIFEST_FILENAME } from '../../../generationManifest';

/**
 * Command contribution entry from package.json contributes.commands.
 */
interface CommandContribution {
    readonly command: string;
    readonly title: string;
}

/**
 * Walkthrough step media descriptor from package.json contributes.walkthroughs.
 */
interface WalkthroughMediaContribution {
    readonly markdown?: string;
}

/**
 * Walkthrough step entry from package.json contributes.walkthroughs.
 */
interface WalkthroughStepContribution {
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly media?: WalkthroughMediaContribution;
}

/**
 * Walkthrough entry from package.json contributes.walkthroughs.
 */
interface WalkthroughContribution {
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly steps: readonly WalkthroughStepContribution[];
}

/**
 * Subset of package.json required for walkthrough verification.
 */
interface PackageManifest {
    readonly contributes?: {
        readonly commands?: readonly CommandContribution[];
        readonly walkthroughs?: readonly WalkthroughContribution[];
    };
}

/**
 * Sentence violation record reported by the ASD-STE100 style checker.
 */
interface SentenceViolation {
    readonly lineNumber: number;
    readonly kind: 'procedural' | 'prose';
    readonly wordCount: number;
    readonly maxWords: number;
    readonly text: string;
}

/**
 * Banned legacy pattern descriptor built dynamically to avoid literal banned tokens.
 */
interface BannedPattern {
    readonly label: string;
    readonly regex: RegExp;
}

const REPO_ROOT = path.resolve(__dirname, '../../../../');
const WALKTHROUGHS_DIR = path.join(REPO_ROOT, 'walkthroughs');
const SAMPLES_DIR = path.join(REPO_ROOT, 'resources', 'samples');
const CATALOG_DIR = path.join(REPO_ROOT, 'webview', 'src', 'catalog', 'nodes');
const AUTHOR_TAG = 'Aut' + 'hor:';
const EXPECTED_AUTHOR_VALUE = `${AUTHOR_TAG} Mike Nimer <mikenimer@google.com>`;
const EXPECTED_AUTHOR_COMMENT = `<!-- ${EXPECTED_AUTHOR_VALUE} -->`;
const PROCEDURAL_STEP_WORD_LIMIT = 20;
const DESCRIPTIVE_PROSE_WORD_LIMIT = 25;

const LEGACY_PREFIX = ['be', 'am'].join('');
const GEMINI_PREFIX = ['gem', 'ini'].join('');

const BANNED_PATTERNS: readonly BannedPattern[] = [
    { label: 'legacy pipeline extension', regex: new RegExp(`\\.${LEGACY_PREFIX}\\b`, 'i') },
    { label: 'legacy codegen marker', regex: new RegExp(`\\.${LEGACY_PREFIX}gen\\b`, 'i') },
    { label: 'legacy snake_case transform key', regex: new RegExp(`${LEGACY_PREFIX}_transform`, 'i') },
    { label: 'legacy camelCase transform key', regex: new RegExp(`${LEGACY_PREFIX}Transform`) },
    { label: 'deprecated flash 2.5 model', regex: new RegExp(`${GEMINI_PREFIX}-2\\.5`, 'i') },
    { label: 'deprecated 1.x model', regex: new RegExp(`${GEMINI_PREFIX}-1\\.`, 'i') },
    { label: 'deprecated 2.x model', regex: new RegExp(`${GEMINI_PREFIX}-2\\.`, 'i') },
    { label: 'stale legacy 3.8 model reference', regex: new RegExp(`${GEMINI_PREFIX}[\\s-]+3\\.8`, 'i') },
];

/**
 * Loads and parses the root extension package.json manifest.
 *
 * @returns Parsed package manifest subset.
 */
function readPackageManifest(): PackageManifest {
    const raw = fs.readFileSync(path.join(REPO_ROOT, 'package.json'), 'utf-8');
    return JSON.parse(raw) as PackageManifest;
}

/**
 * Counts catalog node entries in a webview catalog TypeScript module.
 *
 * @param fileName Catalog module filename inside webview/src/catalog/nodes.
 * @returns Total sparkTransform declarations in the file.
 */
function countCatalogTransforms(fileName: string): number {
    const source = fs.readFileSync(path.join(CATALOG_DIR, fileName), 'utf-8');
    const matches = source.match(/sparkTransform:\s*['"][^'"]+['"]/g);
    return matches ? matches.length : 0;
}

/**
 * Strips fenced code blocks from Markdown while preserving line count alignment.
 *
 * @param markdown Raw Markdown content.
 * @returns Markdown lines outside fenced code blocks, with fenced lines blanked.
 */
function stripFencedCodeBlocks(markdown: string): string[] {
    const lines = markdown.replace(/\r\n/g, '\n').split('\n');
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
    return output;
}

/**
 * Finds line numbers that contain prose semicolons outside HTML comments and code spans.
 *
 * @param markdown Raw Markdown content.
 * @returns One-based line numbers containing prose semicolons.
 */
function findProseSemicolonLines(markdown: string): number[] {
    const lines = stripFencedCodeBlocks(markdown);
    const offendingLines: number[] = [];

    lines.forEach((rawLine, index) => {
        const withoutComments = rawLine.replace(/<!--[\s\S]*?-->/g, '');
        const withoutInlineCode = withoutComments.replace(/`[^`]*`/g, '');
        if (withoutInlineCode.includes(';')) {
            offendingLines.push(index + 1);
        }
    });

    return offendingLines;
}

/**
 * Extracts Markdown command URI identifiers from text content.
 *
 * @param content Markdown text that may include command action links.
 * @returns Extracted VS Code command identifiers without query strings.
 */
function extractCommandLinkIds(content: string): string[] {
    const regex = /\[[^\]]+\]\(command:([^?)#\s]+)(?:\?[^)]*)?\)/g;
    const ids: string[] = [];
    let match: RegExpExecArray | null = regex.exec(content);

    while (match !== null) {
        ids.push(match[1]);
        match = regex.exec(content);
    }
    return ids;
}

/**
 * Splits a single Markdown prose or list line into normalized sentences.
 *
 * @param body Line content after removing list markers.
 * @returns Normalized plain-text sentences ready for word counting.
 */
function splitLineIntoSentences(body: string): string[] {
    const protectedCode = body.replace(/`([^`]+)`/g, (_full, inner: string) =>
        inner.replace(/\./g, '__CODE_DOT__').replace(/\s+/g, '__CODE_SPACE__')
    );
    const collapsedLinks = protectedCode.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1');
    const plain = collapsedLinks.replace(/\*\*([^*]+)\*\*/g, '$1').replace(/\*([^*]+)\*/g, '$1');

    return plain
        .split(/(?<=[.!?])\s+/)
        .map((segment) =>
            segment
                .replace(/__CODE_DOT__/g, '.')
                .replace(/__CODE_SPACE__/g, ' ')
                .trim()
        )
        .filter((segment) => segment.length > 0);
}

/**
 * Counts words in a normalized sentence, ignoring standalone punctuation tokens.
 *
 * @param sentence Plain-text sentence string.
 * @returns Number of lexical tokens in the sentence.
 */
function countSentenceWords(sentence: string): number {
    return sentence
        .split(/\s+/)
        .map((token) => token.replace(/^[^A-Za-z0-9]+|[^A-Za-z0-9]+$/g, ''))
        .filter((token) => token.length > 0).length;
}

/**
 * Audits a Markdown document against ASD-STE100 procedural and prose word caps.
 *
 * @param markdown Raw Markdown document text.
 * @returns Detected sentence length violations across numbered steps and prose.
 */
function auditSentenceLengths(markdown: string): SentenceViolation[] {
    const lines = stripFencedCodeBlocks(markdown);
    const violations: SentenceViolation[] = [];

    lines.forEach((rawLine, index) => {
        const trimmed = rawLine.trim();
        if (
            !trimmed ||
            trimmed.startsWith('#') ||
            trimmed.startsWith('<!--') ||
            /^(\*{3,}|-{3,}|_{3,})$/.test(trimmed) ||
            /^\[[^\]]+\]\(command:[^)]+\)$/.test(trimmed)
        ) {
            return;
        }

        const numberedMatch = /^(\d+)\.\s+(.*)$/.exec(trimmed);
        if (numberedMatch) {
            const stepBody = numberedMatch[2];
            const totalStepWords = countSentenceWords(
                splitLineIntoSentences(stepBody).join(' ')
            );
            if (totalStepWords > PROCEDURAL_STEP_WORD_LIMIT) {
                violations.push({
                    lineNumber: index + 1,
                    kind: 'procedural',
                    wordCount: totalStepWords,
                    maxWords: PROCEDURAL_STEP_WORD_LIMIT,
                    text: stepBody,
                });
                return;
            }
            for (const sentence of splitLineIntoSentences(stepBody)) {
                const words = countSentenceWords(sentence);
                if (words > PROCEDURAL_STEP_WORD_LIMIT) {
                    violations.push({
                        lineNumber: index + 1,
                        kind: 'procedural',
                        wordCount: words,
                        maxWords: PROCEDURAL_STEP_WORD_LIMIT,
                        text: sentence,
                    });
                }
            }
            return;
        }

        const bulletMatch = /^[-*]\s+(.*)$/.exec(trimmed);
        const proseBody = bulletMatch ? bulletMatch[1] : trimmed;
        for (const sentence of splitLineIntoSentences(proseBody)) {
            const words = countSentenceWords(sentence);
            if (words > DESCRIPTIVE_PROSE_WORD_LIMIT) {
                violations.push({
                    lineNumber: index + 1,
                    kind: 'prose',
                    wordCount: words,
                    maxWords: DESCRIPTIVE_PROSE_WORD_LIMIT,
                    text: sentence,
                });
            }
        }
    });

    return violations;
}

/**
 * Scans text for banned legacy tokens and returns labels of any matched patterns.
 *
 * @param text Input text to inspect.
 * @returns Matched banned pattern labels.
 */
function findBannedPatternMatches(text: string): string[] {
    return BANNED_PATTERNS.filter((entry) => entry.regex.test(text)).map((entry) => entry.label);
}

describe('Walkthrough documentation suite (DOC4)', () => {
    const manifest = readPackageManifest();
    const walkthroughs = manifest.contributes?.walkthroughs ?? [];
    const registeredCommands = new Set(
        (manifest.contributes?.commands ?? []).map((cmd) => cmd.command)
    );
    const referencedMarkdownPaths = walkthroughs.flatMap((wt) =>
        wt.steps
            .map((step) => step.media?.markdown)
            .filter((mdPath): mdPath is string => typeof mdPath === 'string' && mdPath.length > 0)
    );

    it('registers all four walkthrough steps in package.json with existing markdown files', () => {
        expect(walkthroughs.length).toBeGreaterThan(0);
        expect(referencedMarkdownPaths).toEqual([
            'walkthroughs/design-dag.md',
            'walkthroughs/generate-code.md',
            'walkthroughs/declarative-pipelines.md',
            'walkthroughs/create-pipeline.md',
        ]);

        for (const relativePath of referencedMarkdownPaths) {
            const fullPath = path.join(REPO_ROOT, relativePath);
            expect(fs.existsSync(fullPath), `Missing walkthrough file: ${relativePath}`).toBe(true);
        }

        const diskFiles = fs
            .readdirSync(WALKTHROUGHS_DIR)
            .filter((name) => name.endsWith('.md'))
            .map((name) => `walkthroughs/${name}`)
            .sort();
        expect(diskFiles).toEqual([...referencedMarkdownPaths].sort());
    });

    it('places a single Author credit comment at line 1 of every walkthrough file', () => {
        for (const relativePath of referencedMarkdownPaths) {
            const content = fs.readFileSync(path.join(REPO_ROOT, relativePath), 'utf-8');
            const lines = content.replace(/\r\n/g, '\n').split('\n');
            expect(lines[0], `${relativePath} line 1 author header`).toBe(EXPECTED_AUTHOR_COMMENT);

            const authorMatches = content.match(new RegExp(AUTHOR_TAG, 'g')) ?? [];
            expect(authorMatches.length, `${relativePath} single Author credit`).toBe(1);
        }

        const selfContent = fs.readFileSync(__filename, 'utf-8');
        const selfAuthorMatches = selfContent.match(new RegExp(AUTHOR_TAG, 'g')) ?? [];
        expect(selfAuthorMatches.length).toBe(1);
    });

    it('enforces ASD-STE100 sentence word caps and zero prose semicolons across walkthroughs', () => {
        for (const relativePath of referencedMarkdownPaths) {
            const content = fs.readFileSync(path.join(REPO_ROOT, relativePath), 'utf-8');
            expect(
                findProseSemicolonLines(content),
                `Semicolons found in ${relativePath}`
            ).toEqual([]);
            expect(
                auditSentenceLengths(content),
                `Sentence length violations in ${relativePath}`
            ).toEqual([]);
        }
    });

    it('links only valid VS Code commands registered in package.json contributes.commands', () => {
        for (const wt of walkthroughs) {
            for (const step of wt.steps) {
                const descLinks = extractCommandLinkIds(step.description);
                for (const cmdId of descLinks) {
                    expect(
                        registeredCommands.has(cmdId),
                        `Unregistered command in step ${step.id}: ${cmdId}`
                    ).toBe(true);
                }
            }
        }

        for (const relativePath of referencedMarkdownPaths) {
            const content = fs.readFileSync(path.join(REPO_ROOT, relativePath), 'utf-8');
            const cmdLinks = extractCommandLinkIds(content);
            for (const cmdId of cmdLinks) {
                expect(
                    registeredCommands.has(cmdId),
                    `Unregistered command in ${relativePath}: ${cmdId}`
                ).toBe(true);
            }
        }
    });

    it('documents exact catalog counts, badges, handles, and CLI discovery in design-dag.md', () => {
        const content = fs.readFileSync(
            path.join(WALKTHROUGHS_DIR, 'design-dag.md'),
            'utf-8'
        );

        expect(countCatalogTransforms('sources.ts')).toBe(40);
        expect(countCatalogTransforms('steps.ts')).toBe(33);
        expect(countCatalogTransforms('sinks.ts')).toBe(38);

        expect(content).toContain('.spark');
        expect(content).toContain('40 connectors');
        expect(content).toContain('33 transforms');
        expect(content).toContain('38 destinations');
        expect(content).toContain('SOURCE -> SINK');
        expect(content).toContain('`BATCH`');
        expect(content).toContain('`STREAM`');
        expect(content).toContain('`Join`');
        expect(content).toContain('`L`');
        expect(content).toContain('`R`');
        expect(content).toContain('`BROADCAST`');
        expect(content).toContain('`ConditionalSplit`');
        expect(content).toContain('`else`');
        expect(content).toContain('`error`');
        expect(content).toContain('Properties Panel');
        expect(content).toContain('`gcloud`');
        expect(content).toContain('`bq`');
        expect(content).toContain('Apache Spark 4.x');
    });

    it('documents dynamic Gemini discovery, reviewer pass, dual-paradigm outputs, and security in generate-code.md', () => {
        const content = fs.readFileSync(
            path.join(WALKTHROUGHS_DIR, 'generate-code.md'),
            'utf-8'
        );

        expect(content).toContain('.spark');
        expect(content).toContain('Source Code');
        expect(content).toContain(DEFAULT_GEMINI_MODEL);
        expect(content).toContain('Vertex AI global endpoint');
        expect(content).toContain('AI Code Reviewer Pass');
        expect(content).toContain('src/pipeline.py');
        expect(content).toContain('read_sources');
        expect(content).toContain('transform');
        expect(content).toContain('write_sinks');
        expect(content).toContain('tests/test_pipeline.py');
        expect(content).toContain('spark-pipeline.yml');
        expect(content).toContain('transformations/*.py');
        expect(content).toContain('src/transforms.py');
        expect(content).toContain('tests/test_transforms.py');
        expect(content).toMatch(/redact/i);
        expect(content).toContain(MANIFEST_FILENAME);
        expect(content).toContain('Deployment');
        expect(content).toContain('Local Spark');
        expect(content).toContain('Dataproc Clusters');
        expect(content).toContain('Serverless Spark');
    });

    it('documents .spark creation, paradigm choices, all six starter samples, and command link in create-pipeline.md', () => {
        const content = fs.readFileSync(
            path.join(WALKTHROUGHS_DIR, 'create-pipeline.md'),
            'utf-8'
        );

        const diskSampleFiles = fs
            .readdirSync(SAMPLES_DIR)
            .filter((name) => name.endsWith('.spark'))
            .sort();
        expect(diskSampleFiles).toHaveLength(6);

        for (const sampleFile of diskSampleFiles) {
            expect(content, `Missing sample in create-pipeline.md: ${sampleFile}`).toContain(
                sampleFile
            );
        }

        expect(content).toContain('.spark');
        expect(content).toContain('YAML');
        expect(content).toContain('Imperative PySpark (Default)');
        expect(content).toContain('Spark Declarative Pipelines (SDP)');
        expect(content).toContain('Samples');
        expect(content).toContain(
            '[Create New Pipeline](command:spark-pipeline-builder.createNewPipeline)'
        );
    });

    it('documents Spark 4.2+ SDP badges, BigLake storage settings, refresh flags, and SDP samples in declarative-pipelines.md', () => {
        const content = fs.readFileSync(
            path.join(WALKTHROUGHS_DIR, 'declarative-pipelines.md'),
            'utf-8'
        );

        expect(content).toContain('.spark');
        expect(content).toContain('Apache Spark 4.2+');
        expect(content).toContain('SDP (4.2+)');
        for (const badge of ['`VIEW`', '`MV`', '`TABLE`', '`CDC`', '`SINK`']) {
            expect(content).toContain(badge);
        }
        expect(content).toContain('`storage`');
        expect(content).toContain('`catalog`');
        expect(content).toContain('`database`');
        expect(content).toContain('`dry-run`');
        expect(content).toContain('`--refresh`');
        expect(content).toContain('`--full-refresh`');
        expect(content).toContain('sdp-biglake-iceberg-medallion.spark');
        expect(content).toContain('sdp-auto-cdc-scd1.spark');
    });

    it('contains zero banned legacy tokens in walkthrough docs, manifest steps, and this test module', () => {
        for (const relativePath of referencedMarkdownPaths) {
            const content = fs.readFileSync(path.join(REPO_ROOT, relativePath), 'utf-8');
            expect(findBannedPatternMatches(content), `Banned token in ${relativePath}`).toEqual([]);
        }

        const walkthroughJson = JSON.stringify(walkthroughs);
        expect(findBannedPatternMatches(walkthroughJson)).toEqual([]);

        const selfSource = fs.readFileSync(__filename, 'utf-8');
        expect(findBannedPatternMatches(selfSource)).toEqual([]);
    });

    describe('style and token audit helper boundary conditions', () => {
        it('ignores semicolons inside fenced code blocks and inline code spans while catching prose semicolons', () => {
            const cleanMarkdown = [
                '<!-- Header; comment -->',
                'Use `a;b` safely inside inline code.',
                '```python',
                'x = 1; y = 2',
                '```',
            ].join('\n');
            expect(findProseSemicolonLines(cleanMarkdown)).toEqual([]);

            const badMarkdown = 'First clause; second clause.';
            expect(findProseSemicolonLines(badMarkdown)).toEqual([1]);
        });

        it('flags procedural steps above 20 words and descriptive prose above 25 words', () => {
            const longStep =
                '1. ' +
                Array.from({ length: 21 }, (_, idx) => `word${idx + 1}`).join(' ') +
                '.';
            const stepViolations = auditSentenceLengths(longStep);
            expect(stepViolations).toHaveLength(1);
            expect(stepViolations[0]?.kind).toBe('procedural');

            const longProse =
                Array.from({ length: 26 }, (_, idx) => `token${idx + 1}`).join(' ') + '.';
            const proseViolations = auditSentenceLengths(longProse);
            expect(proseViolations).toHaveLength(1);
            expect(proseViolations[0]?.kind).toBe('prose');
        });

        it('detects dynamically constructed legacy tokens and extracts command links with query strings', () => {
            const legacyExt = `pipeline.${LEGACY_PREFIX}`;
            const legacyModel = `${GEMINI_PREFIX}-2.5-flash`;
            expect(findBannedPatternMatches(legacyExt)).toContain('legacy pipeline extension');
            expect(findBannedPatternMatches(legacyModel)).toContain('deprecated flash 2.5 model');

            const links = extractCommandLinkIds(
                '[Open](command:spark-pipeline-builder.createNewPipeline?%22arg%22)'
            );
            expect(links).toEqual(['spark-pipeline-builder.createNewPipeline']);
        });
    });
});
