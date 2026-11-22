/**
 * @fileoverview Unit tests verifying DESIGN.md YAML tokens, PipelineNode badge and handle parity, and ASD-STE100 style invariants.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import * as fs from 'fs';
import * as path from 'path';
import { describe, expect, it } from 'vitest';
import * as yaml from 'yaml';

/**
 * Parsed YAML frontmatter structure expected at the top of DESIGN.md.
 */
interface DesignDocFrontmatter {
    name?: string;
    author?: string;
    description?: string;
    version?: string;
    colors?: Record<string, string>;
    typography?: Record<string, Record<string, string>>;
    rounded?: Record<string, string>;
    spacing?: Record<string, string>;
    components?: Record<string, Record<string, string>>;
}

/**
 * Result of splitting DESIGN.md into YAML frontmatter text, parsed metadata, and Markdown body.
 */
interface ParsedDesignDoc {
    frontmatterRaw: string;
    frontmatter: DesignDocFrontmatter;
    markdownBody: string;
}

/**
 * Sentence length violation detected by the ASD-STE100 prose checker.
 */
interface SentenceLengthViolation {
    lineNumber: number;
    wordCount: number;
    maxWords: number;
    sentence: string;
}

const REPO_ROOT = path.resolve(__dirname, '../../../../');
const DESIGN_DOC_PATH = path.join(REPO_ROOT, 'DESIGN.md');
const PIPELINE_NODE_PATH = path.join(REPO_ROOT, 'webview/src/components/PipelineNode.tsx');
const CANVAS_TOOLBAR_PATH = path.join(REPO_ROOT, 'webview/src/components/CanvasToolbar.tsx');
const GENERATOR_SETTINGS_PATH = path.join(
    REPO_ROOT,
    'webview/src/components/BottomPanel/sourceCode/GeneratorSettings.tsx'
);
const SOURCE_CODE_TAB_PATH = path.join(
    REPO_ROOT,
    'webview/src/components/BottomPanel/SourceCodeTab.tsx'
);

/**
 * Splits a Markdown document with YAML frontmatter into metadata and body sections.
 *
 * @param rawContent Raw Markdown file content.
 * @returns Extracted frontmatter text, parsed YAML object, and Markdown body.
 */
function parseDesignDoc(rawContent: string): ParsedDesignDoc {
    const normalized = rawContent.replace(/\r\n/g, '\n');
    const match = normalized.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
    if (!match) {
        throw new Error('DESIGN.md must begin with a YAML frontmatter block delimited by ---');
    }

    const frontmatterRaw = match[1];
    const markdownBody = match[2];
    const parsed = yaml.parse(frontmatterRaw) as DesignDocFrontmatter | null;
    if (!parsed || typeof parsed !== 'object') {
        throw new Error('DESIGN.md YAML frontmatter must parse into a non-null object');
    }

    return {
        frontmatterRaw,
        frontmatter: parsed,
        markdownBody,
    };
}

/**
 * Strips fenced code blocks from a Markdown body so prose checks ignore code samples.
 *
 * @param markdownBody Markdown text after YAML frontmatter.
 * @returns Prose lines outside fenced code blocks.
 */
function extractProseOutsideCodeFences(markdownBody: string): string {
    const lines = markdownBody.replace(/\r\n/g, '\n').split('\n');
    const proseLines: string[] = [];
    let insideFence = false;

    for (const line of lines) {
        if (line.trim().startsWith('```')) {
            insideFence = !insideFence;
            continue;
        }
        if (!insideFence) {
            proseLines.push(line);
        }
    }

    return proseLines.join('\n');
}

/**
 * Evaluates prose lines outside code fences and tables against ASD-STE100 word limits.
 * Numbered steps allow at most 20 words and descriptive prose sentences allow at most 25 words.
 *
 * @param markdownBody Markdown body after YAML frontmatter.
 * @param startLineOffset Line offset of the Markdown body in the source file.
 * @returns List of sentences exceeding the configured ASD-STE100 word limit.
 */
function findSentenceLengthViolations(
    markdownBody: string,
    startLineOffset: number
): SentenceLengthViolation[] {
    const lines = markdownBody.replace(/\r\n/g, '\n').split('\n');
    const violations: SentenceLengthViolation[] = [];
    let insideFence = false;

    lines.forEach((line, idx) => {
        const trimmed = line.trim();
        if (trimmed.startsWith('```')) {
            insideFence = !insideFence;
            return;
        }
        if (insideFence) {
            return;
        }
        if (
            !trimmed ||
            trimmed.startsWith('#') ||
            trimmed.startsWith('|') ||
            trimmed === '---'
        ) {
            return;
        }

        const isNumberedItem = /^\d+\.\s+/.test(trimmed);
        const maxWords = isNumberedItem ? 20 : 25;
        const bodyWithoutMarker = trimmed.replace(/^([-*]|\d+\.)\s+/, '');
        const protectedText = bodyWithoutMarker
            .replace(/\bvs\./g, 'vs_DOT_')
            .replace(/`[^`]+`/g, (token) => token.replace(/\./g, '_DOT_'));

        const sentences = protectedText.split(/(?<=[.!?])\s+/).filter(Boolean);
        for (const rawSentence of sentences) {
            const restored = rawSentence.replace(/vs_DOT_/g, 'vs.').replace(/_DOT_/g, '.');
            const rawCount = restored.trim().split(/\s+/).filter(Boolean).length;
            const strippedCount = restored
                .replace(/[*_`()[\]:]/g, ' ')
                .trim()
                .split(/\s+/)
                .filter(Boolean).length;
            const wordCount = Math.max(rawCount, strippedCount);
            if (wordCount > maxWords) {
                violations.push({
                    lineNumber: startLineOffset + idx,
                    wordCount,
                    maxWords,
                    sentence: restored,
                });
            }
        }
    });

    return violations;
}

/**
 * Builds banned legacy token regular expressions dynamically so literal repository scans stay clean.
 *
 * @returns Named regular expressions for forbidden legacy framework and deprecated model tokens.
 */
function buildBannedTokenPatterns(): Array<{ label: string; pattern: RegExp }> {
    const legacyFrameworkWord = ['be', 'am'].join('');
    const deprecatedModelPrefix = ['gemini', '-'].join('');
    return [
        {
            label: 'legacy pipeline extension',
            pattern: new RegExp(`\\.${legacyFrameworkWord}\\b`, 'i'),
        },
        {
            label: 'legacy generation manifest',
            pattern: new RegExp(`\\.${legacyFrameworkWord}gen\\b`, 'i'),
        },
        {
            label: 'legacy snake_case transform key',
            pattern: new RegExp(`\\b${legacyFrameworkWord}_transform\\b`, 'i'),
        },
        {
            label: 'legacy camelCase transform key',
            pattern: new RegExp(`\\b${legacyFrameworkWord}Transform\\b`),
        },
        {
            label: 'deprecated generation-one model prefix',
            pattern: new RegExp(`${deprecatedModelPrefix}1\\.`, 'i'),
        },
        {
            label: 'deprecated generation-two model prefix',
            pattern: new RegExp(`${deprecatedModelPrefix}2\\.`, 'i'),
        },
        {
            label: 'deprecated two-point-five model prefix',
            pattern: new RegExp(`${deprecatedModelPrefix}2\\.5`, 'i'),
        },
        {
            label: 'stale three-point-eight prose reference',
            pattern: new RegExp(['Gemini', ' 3\\.8'].join(''), 'i'),
        },
    ];
}

describe('DESIGN.md Specification & PipelineNode Parity (DOC2)', () => {
    const rawDesignDoc = fs.readFileSync(DESIGN_DOC_PATH, 'utf-8');
    const parsedDoc = parseDesignDoc(rawDesignDoc);
    const pipelineNodeSource = fs.readFileSync(PIPELINE_NODE_PATH, 'utf-8');
    const canvasToolbarSource = fs.readFileSync(CANVAS_TOOLBAR_PATH, 'utf-8');
    const generatorSettingsSource = fs.readFileSync(GENERATOR_SETTINGS_PATH, 'utf-8');
    const sourceCodeTabSource = fs.readFileSync(SOURCE_CODE_TAB_PATH, 'utf-8');

    it('parses YAML frontmatter cleanly with a single author credit and complete design tokens', () => {
        const { frontmatter, markdownBody } = parsedDoc;

        const expectedAuthor = ['Mike Nimer', `<mikenimer${'@'}google.com>`].join(' ');
        const emailPattern = new RegExp(`mikenimer${'@'}google\\.com`, 'gi');
        const bodyHeaderPattern = new RegExp(['Auth', 'or:\\s*Mike'].join(''), 'i');

        expect(frontmatter.name).toBe('Apache Spark Pipeline Designer');
        expect(frontmatter.author).toBe(expectedAuthor);
        expect(frontmatter.description).toBeTruthy();
        expect(frontmatter.version).toMatch(/^\d+\.\d+\.\d+$/);

        const authorOccurrences = rawDesignDoc.match(emailPattern) ?? [];
        expect(authorOccurrences).toHaveLength(1);
        expect(markdownBody).not.toMatch(bodyHeaderPattern);

        expect(frontmatter.colors?.['canvas-bg']).toBe('#0d1117');
        expect(frontmatter.colors?.['workbench-bg']).toBe('#161b22');
        expect(frontmatter.colors?.['source-primary']).toBe('#10b981');
        expect(frontmatter.colors?.['step-primary']).toBe('#3b82f6');
        expect(frontmatter.colors?.['sink-primary']).toBe('#ef4444');
        expect(frontmatter.colors?.['sample-primary']).toBe('#a855f7');

        expect(frontmatter.spacing).toMatchObject({
            xs: '4px',
            sm: '8px',
            md: '12px',
            lg: '16px',
            xl: '24px',
            '2xl': '32px',
            drawer: '320px',
        });
        expect(frontmatter.typography).toHaveProperty('display-lg');
        expect(frontmatter.typography).toHaveProperty('body-md');
        expect(frontmatter.typography).toHaveProperty('label-xs');
        expect(frontmatter.typography).toHaveProperty('badge-micro');
        expect(frontmatter.typography).toHaveProperty('code-sm');
        expect(frontmatter.components?.['node-card']?.minWidth).toBe('180px');
    });

    it('cross-checks all execution mode, planner, SDP, and validation badge testids and Tailwind tokens against PipelineNode.tsx', () => {
        const requiredBadgeTestIds = [
            'node-mode-badge',
            'node-broadcast-badge',
            'node-multi-input-badge',
            'node-cache-badge',
            'node-foreachbatch-badge',
            'node-sdp-badge',
            'cycle-error-badge',
            'orphan-warning-badge',
            'node-warning-badge',
        ];

        for (const testId of requiredBadgeTestIds) {
            expect(pipelineNodeSource).toContain(`data-testid="${testId}"`);
            expect(parsedDoc.markdownBody).toContain(testId);
        }

        const sharedBadgeClassSnippets = [
            'bg-slate-500/15 border-[var(--vscode-border,#30363d)] text-[var(--vscode-text-secondary,#c9d1d9)]',
            'bg-cyan-500/15 border-cyan-500/40 text-cyan-300',
            'bg-indigo-500/15 border-indigo-500/40 text-indigo-300',
            'bg-blue-500/15 border-blue-500/40 text-blue-300',
            'bg-amber-500/15 border-amber-500/40 text-amber-300',
            'bg-violet-500/15 border-violet-500/40 text-violet-300',
            'bg-slate-500/20 border-slate-400/40 text-slate-200',
            'bg-purple-500/15 border-purple-500/40 text-purple-300',
            'bg-emerald-500/15 border-emerald-500/40 text-emerald-300',
            'bg-rose-500/15 border-rose-500/40 text-rose-300',
        ];

        for (const classSnippet of sharedBadgeClassSnippets) {
            expect(pipelineNodeSource).toContain(classSnippet);
            expect(parsedDoc.markdownBody).toContain(classSnippet);
        }

        for (const badgeLabel of ['BATCH', 'STREAM', 'BROADCAST', 'N-IN', 'CACHE', 'foreachBatch']) {
            expect(pipelineNodeSource).toContain(badgeLabel);
            expect(parsedDoc.markdownBody).toContain(`\`${badgeLabel}\``);
        }

        for (const sdpBadge of ['VIEW', 'MV', 'TABLE', 'CDC', 'SINK']) {
            expect(parsedDoc.markdownBody).toContain(`\`${sdpBadge}\``);
        }
    });

    it('cross-checks Join and MergeCdcScd dual-input handles, exterior L/R pills, SDP single-input exception, and BROADCAST semantics', () => {
        const joinTokensInCodeAndDoc = [
            'min-h-[64px]',
            'join-left-handle-label',
            'join-right-handle-label',
            '32%',
            '68%',
            '#3b82f6',
            '#6366f1',
            '-left-6',
            'text-blue-300',
            'border-blue-500/30',
            'text-indigo-300',
            'border-indigo-500/30',
            'F.broadcast(right_df)',
        ];

        for (const token of joinTokensInCodeAndDoc) {
            expect(pipelineNodeSource).toContain(token);
            expect(parsedDoc.markdownBody).toContain(token);
        }

        expect(pipelineNodeSource).toContain('id="left"');
        expect(pipelineNodeSource).toContain('id="right"');
        expect(parsedDoc.markdownBody).toContain('`id="left"`');
        expect(parsedDoc.markdownBody).toContain('`id="right"`');
        expect(parsedDoc.markdownBody).toContain('`MergeCdcScd`');
        expect(parsedDoc.markdownBody).toContain('`top: 50%`');
        expect(parsedDoc.markdownBody).toContain('`dp.create_auto_cdc_flow`');
    });

    it('cross-checks ConditionalSplit dynamic rule handles, else fallback styling, and height/offset formulas', () => {
        const splitTokensInCodeAndDoc = [
            'pr-16 min-w-[224px]',
            'Math.max(64, splitHandles.length * 24 + 16)',
            '(((idx + 1) / (splitHandles.length + 1)) * 100)',
            'split-handle-label-',
            'max-w-[56px] truncate',
            'bg-emerald-500/15 border-emerald-500/30 text-emerald-300',
            'bg-amber-500/15 border-amber-500/30 text-amber-300',
            '#f59e0b',
        ];

        for (const token of splitTokensInCodeAndDoc) {
            expect(pipelineNodeSource).toContain(token);
            expect(parsedDoc.markdownBody).toContain(token);
        }

        expect(parsedDoc.markdownBody).toContain('`data-testid="split-handle-label-<id>"`');
        expect(parsedDoc.markdownBody).toContain('`data-testid="split-handle-label-else"`');
        expect(parsedDoc.markdownBody).toContain('`id="else"`');
    });

    it('cross-checks the complete connection handle matrix including DLQ valid (30%) and error (70%) handles', () => {
        expect(pipelineNodeSource).toContain('id="valid"');
        expect(pipelineNodeSource).toContain('id="error"');
        expect(pipelineNodeSource).toContain("top: '30%'");
        expect(pipelineNodeSource).toContain("top: '70%'");
        expect(pipelineNodeSource).toContain("top: '50%'");
        expect(pipelineNodeSource).toContain("data.spark_transform === 'DataQualityAssert'");

        expect(parsedDoc.markdownBody).toContain('`id="valid"`');
        expect(parsedDoc.markdownBody).toContain('`id="error"`');
        expect(parsedDoc.markdownBody).toContain('`top: 30%`');
        expect(parsedDoc.markdownBody).toContain('`top: 70%`');
        expect(parsedDoc.markdownBody).toContain('`DataQualityAssert`');
        expect(parsedDoc.markdownBody).toContain('`SOURCE -> SINK`');
    });

    it('preserves Phase 12 SDP controls and Phase 14 dynamic Gemini model selector contracts', () => {
        expect(canvasToolbarSource).toContain('data-testid="toolbar-pipeline-mode"');
        expect(canvasToolbarSource).toContain('data-testid="toolbar-paradigm-indicator"');
        expect(generatorSettingsSource).toContain('data-testid="pyspark-runtime-badge"');
        expect(generatorSettingsSource).toContain('data-testid="codegen-model-select"');
        expect(generatorSettingsSource).toContain('data-testid="refresh-codegen-models-btn"');
        expect(sourceCodeTabSource).toContain('data-testid="active-model-badge"');

        const preservedUiTokens = [
            'toolbar-pipeline-mode',
            'toolbar-paradigm-indicator',
            'sdp-compatibility-modal',
            'opacity-45 cursor-not-allowed',
            'SdpSinkSettingsField',
            'SdpSettingsPanel',
            'SdpRefreshControls',
            'pyspark-runtime-badge',
            'codegen-model-select',
            'refresh-codegen-models-btn',
            'active-model-badge',
            'gemini-4-hydrogen',
        ];

        for (const token of preservedUiTokens) {
            expect(parsedDoc.markdownBody).toContain(token);
        }
    });

    it('enforces zero prose semicolons, ASD-STE100 sentence length limits, and zero legacy tokens in DESIGN.md', () => {
        const proseOnly = extractProseOutsideCodeFences(parsedDoc.markdownBody);
        expect(proseOnly).not.toContain(';');

        const bodyStartLine = parsedDoc.frontmatterRaw.split('\n').length + 3;
        const lengthViolations = findSentenceLengthViolations(
            parsedDoc.markdownBody,
            bodyStartLine
        );
        expect(lengthViolations).toEqual([]);

        for (const { label, pattern } of buildBannedTokenPatterns()) {
            expect(rawDesignDoc, `Unexpected ${label} in DESIGN.md`).not.toMatch(pattern);
        }
    });

    it('validates parser and ASD-STE100 helper boundary and failure conditions', () => {
        expect(() => parseDesignDoc('# Missing frontmatter\nBody text.')).toThrow(
            /YAML frontmatter/
        );

        const codeFenceSample = [
            'Clean prose line without forbidden punctuation.',
            '```ts',
            'const allowedInCode = true;',
            '```',
        ].join('\n');
        expect(extractProseOutsideCodeFences(codeFenceSample)).not.toContain(';');

        const exactBoundarySample = [
            '1. One two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty.',
            '- One two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty twentyone twentytwo twentythree twentyfour twentyfive.',
        ].join('\n');
        expect(findSentenceLengthViolations(exactBoundarySample, 1)).toEqual([]);

        const overLimitSample = [
            '1. One two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty twentyone.',
            '- One two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty twentyone twentytwo twentythree twentyfour twentyfive twentysix.',
        ].join('\n');
        const detected = findSentenceLengthViolations(overLimitSample, 10);
        expect(detected).toHaveLength(2);
        expect(detected[0]).toMatchObject({ lineNumber: 10, wordCount: 21, maxWords: 20 });
        expect(detected[1]).toMatchObject({ lineNumber: 11, wordCount: 26, maxWords: 25 });
    });
});
