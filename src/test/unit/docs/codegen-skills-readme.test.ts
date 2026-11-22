/**
 * Documentation Contract Tests for `resources/codegen-skills/README.md`
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies that `resources/codegen-skills/README.md` coexists cleanly with
 * `CodegenSkillCatalog.load()`, documents every schema key, kind, feature flag,
 * placeholder, prompt section, and deterministic plan field from `src/codegen/*`,
 * and satisfies single-author, ASD-STE100, and zero-legacy-token invariants.
 */

import { describe, it, expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import {
    CodegenSkillCatalog,
    clearCodegenSkillCatalogCache,
    getCodegenSkillCatalog,
} from '../../../codegen/CodegenSkillCatalog';
import {
    CODEGEN_COMMON_SKILL_NAME,
    CODEGEN_REVIEWER_SETTING,
    MODEL_FACING_PLACEHOLDERS,
    PIPELINE_FEATURES,
    PROMPT_SECTIONS,
    SKILL_CALLERS,
    SKILL_KIND_ORDER,
    SKILL_PARADIGMS,
    SKILL_RUNNERS,
    SKILL_WILDCARD,
    SPARK_VERSION_PLACEHOLDER,
} from '../../../codegen/codegenConfig';
import { SKILL_METADATA_KEYS, SKILL_TOP_LEVEL_KEYS } from '../../../codegen/skillFile';
import { SKILLS_ROOT, listSkillFolders } from '../helpers/codegenSkillsCorpus';

/** Repository root path. */
const REPO_ROOT = path.join(__dirname, '..', '..', '..', '..');

/** Path to the codegen skills authoring guide. */
const README_PATH = path.join(SKILLS_ROOT, 'README.md');

/** Raw Markdown content of `resources/codegen-skills/README.md`. */
const README_TEXT = fs.readFileSync(README_PATH, 'utf-8');

/** Allowed double-brace placeholders inside `resources/codegen-skills/README.md`. */
const ALLOWED_DOC_PLACEHOLDERS = new Set<string>([
    SPARK_VERSION_PLACEHOLDER,
    ...MODEL_FACING_PLACEHOLDERS,
]);

/**
 * Strips fenced code blocks while preserving surrounding Markdown lines.
 *
 * @param markdown Raw Markdown document text.
 * @returns Markdown text with fenced code blocks removed.
 */
export function stripFencedCodeBlocks(markdown: string): string {
    return markdown.replace(/```[\s\S]*?```/g, '');
}

/**
 * Counts whitespace-separated words in a text fragment.
 *
 * @param fragment Candidate sentence or step text.
 * @returns Word count.
 */
function countWords(fragment: string): number {
    return fragment
        .trim()
        .split(/\s+/)
        .filter((token) => token.length > 0).length;
}

/**
 * Splits a Markdown line into sentences without splitting on periods inside inline code spans.
 *
 * @param line Single non-fenced Markdown prose or table cell string.
 * @returns Non-empty sentence strings.
 */
function splitProtectedSentences(line: string): string[] {
    const protectedText = line.replace(/`[^`]*`/g, (match) => match.replace(/\./g, '_'));
    return protectedText
        .split(/(?<=[.!?])\s+/)
        .map((sentence) => sentence.trim())
        .filter((sentence) => sentence.length > 0);
}

/**
 * Collects ASD-STE100 sentence and procedural step length violations from Markdown prose.
 *
 * @param markdown Raw Markdown document text.
 * @returns Human-readable violation messages (empty when compliant).
 */
export function collectSte100Violations(markdown: string): string[] {
    const violations: string[] = [];
    const lines = stripFencedCodeBlocks(markdown).split('\n');

    lines.forEach((rawLine, index) => {
        const lineNumber = index + 1;
        const line = rawLine.trim();
        if (!line || line === '---' || line.startsWith('#') || line.startsWith('>')) {
            return;
        }

        if (line.startsWith('|')) {
            const cells = line
                .replace(/\\\|/g, 'OR')
                .split('|')
                .map((cell) => cell.trim())
                .filter((cell) => cell.length > 0);
            if (cells.every((cell) => /^:?-+:?$/.test(cell))) {
                return;
            }
            for (const cell of cells) {
                for (const sentence of splitProtectedSentences(cell)) {
                    const words = countWords(sentence);
                    if (words > 25) {
                        violations.push(`L${lineNumber} table sentence has ${words} words (>25): ${sentence}`);
                    }
                }
            }
            return;
        }

        const numberedStep = /^(\d+)\.\s+(.*)$/.exec(line);
        if (numberedStep) {
            const words = countWords(numberedStep[2]);
            if (words > 20) {
                violations.push(`L${lineNumber} numbered step has ${words} words (>20): ${line}`);
            }
            return;
        }

        const proseLine = line.replace(/^[-*]\s+/, '');
        for (const sentence of splitProtectedSentences(proseLine)) {
            const words = countWords(sentence);
            if (words > 25) {
                violations.push(`L${lineNumber} prose sentence has ${words} words (>25): ${sentence}`);
            }
        }
    });

    return violations;
}

/**
 * Builds banned legacy token patterns dynamically so repository-wide literal scans stay zero.
 *
 * @returns Labeled regular expressions for forbidden legacy tokens and deprecated models.
 */
function buildBannedTokenPatterns(): Array<{ label: string; pattern: RegExp }> {
    const legacyExt = ['\\.', 'be', 'am\\b'].join('');
    const legacyGenExt = ['\\.', 'be', 'amgen\\b'].join('');
    const legacySnake = ['\\bbe', 'am_transform\\b'].join('');
    const legacyCamel = ['\\bbe', 'amTransform\\b'].join('');
    const model25 = ['gemini-', '2\\.5'].join('');
    const model1x = ['gemini-', '1\\.'].join('');
    const model2x = ['gemini-', '2\\.'].join('');
    const model38 = ['Gemini\\s+', '3\\.8'].join('');
    const deletedPromptPrefix = ['code-gen-', 'prompt-'].join('');

    return [
        { label: 'legacy pipeline extension', pattern: new RegExp(legacyExt, 'i') },
        { label: 'legacy generator extension', pattern: new RegExp(legacyGenExt, 'i') },
        { label: 'legacy snake transform key', pattern: new RegExp(legacySnake) },
        { label: 'legacy camel transform key', pattern: new RegExp(legacyCamel) },
        { label: 'deprecated 2.5 model', pattern: new RegExp(model25, 'i') },
        { label: 'deprecated 1.x model', pattern: new RegExp(model1x, 'i') },
        { label: 'deprecated 2.x model', pattern: new RegExp(model2x, 'i') },
        { label: 'stale 3.8 model prose', pattern: new RegExp(model38) },
        { label: 'deleted monolithic prompt prefix', pattern: new RegExp(deletedPromptPrefix) },
    ];
}

describe('resources/codegen-skills/README.md contract (DOC5)', () => {
    it('coexists with CodegenSkillCatalog.load() without treating README.md as a skill', () => {
        expect(fs.existsSync(README_PATH)).toBe(true);
        expect(README_TEXT.trim().length).toBeGreaterThan(500);

        clearCodegenSkillCatalogCache();
        const catalog = CodegenSkillCatalog.load(SKILLS_ROOT);
        const cached = getCodegenSkillCatalog(SKILLS_ROOT);
        const skillNames = catalog.all().map((skill) => skill.name).sort();

        expect(skillNames).toEqual(listSkillFolders());
        expect(skillNames).not.toContain('README.md');
        expect(skillNames).not.toContain('README');
        expect(cached.all()).toHaveLength(catalog.all().length);
    });

    it('documents every SKILL_TOP_LEVEL_KEYS and SKILL_METADATA_KEYS entry from skillFile.ts', () => {
        expect(SKILL_TOP_LEVEL_KEYS.size).toBe(3);
        for (const topKey of SKILL_TOP_LEVEL_KEYS) {
            expect(README_TEXT, `Missing top-level key ${topKey}`).toContain(`\`${topKey}\``);
        }

        expect(SKILL_METADATA_KEYS.size).toBe(9);
        for (const metaKey of SKILL_METADATA_KEYS) {
            expect(README_TEXT, `Missing metadata key ${metaKey}`).toContain(`\`${metaKey}\``);
        }
    });

    it('documents every SKILL_KIND_ORDER, caller, runner, paradigm, and PIPELINE_FEATURES token', () => {
        for (const kind of SKILL_KIND_ORDER) {
            expect(README_TEXT, `Missing skill kind ${kind}`).toContain(`\`${kind}\``);
        }
        for (const caller of Object.values(SKILL_CALLERS)) {
            expect(README_TEXT, `Missing caller ${caller}`).toContain(`\`${caller}\``);
        }
        for (const runner of Object.values(SKILL_RUNNERS)) {
            expect(README_TEXT, `Missing runner ${runner}`).toContain(`\`${runner}\``);
        }
        for (const paradigm of Object.values(SKILL_PARADIGMS)) {
            expect(README_TEXT, `Missing paradigm ${paradigm}`).toContain(`\`${paradigm}\``);
        }
        expect(README_TEXT).toContain(`\`${SKILL_WILDCARD}\``);

        for (const feature of Object.values(PIPELINE_FEATURES)) {
            expect(README_TEXT, `Missing pipeline feature ${feature}`).toContain(`\`${feature}\``);
        }
    });

    it('documents SPARK_VERSION_PLACEHOLDER and MODEL_FACING_PLACEHOLDERS and introduces zero unauthorized {{...}} tokens', () => {
        expect(README_TEXT).toContain(`\`${SPARK_VERSION_PLACEHOLDER}\``);
        for (const placeholder of MODEL_FACING_PLACEHOLDERS) {
            expect(README_TEXT, `Missing model-facing placeholder ${placeholder}`).toContain(`\`${placeholder}\``);
        }

        const tokensInReadme = [...README_TEXT.matchAll(/\{\{\s*[A-Za-z_][\w.-]*\s*\}\}/g)].map((m) => m[0]);
        expect(tokensInReadme.length).toBeGreaterThanOrEqual(4);
        const unauthorized = tokensInReadme.filter((token) => !ALLOWED_DOC_PLACEHOLDERS.has(token));
        expect(unauthorized).toEqual([]);
    });

    it('documents single-call prompt assembly order, ExecutionPlan and SdpExecutionPlan fields, runner mirrors, and reviewer wiring', () => {
        for (const marker of [
            CODEGEN_COMMON_SKILL_NAME,
            PROMPT_SECTIONS.pipelineYamlHeading,
            PROMPT_SECTIONS.executionPlanHeading,
            PROMPT_SECTIONS.sdpExecutionPlanHeading,
            '## Skill: <name>',
            '### Python specifics',
            PROMPT_SECTIONS.templateHeadingPrefix.trim(),
        ]) {
            expect(README_TEXT, `Missing prompt section marker ${marker}`).toContain(marker);
        }

        for (const executionPlanKey of [
            'sortedNodeIds',
            'pipelineMode',
            'isStreaming',
            'nodes',
            'streamingNodeIds',
            'cacheDirectives',
            'cacheNodes',
            'cachedNodeIds',
            'unpersistPoints',
            'streamingSinkGroups',
            'dlqSplitPoints',
            'dlqRoutes',
            'conditionalSplits',
            'runtimeArtifacts',
        ]) {
            expect(README_TEXT, `Missing ExecutionPlan key ${executionPlanKey}`).toContain(`\`${executionPlanKey}\``);
        }

        for (const sdpPlanKey of [
            'isCompatible',
            'blockingIssues',
            'datasets',
            'datasetsByNodeId',
            'quarantineDatasetName',
            'auto_cdc',
            'usesIceberg',
            'specConfig',
            'staticRunnerProperties',
            'pipPackages',
        ]) {
            expect(README_TEXT, `Missing SdpExecutionPlan key ${sdpPlanKey}`).toContain(`\`${sdpPlanKey}\``);
        }

        const pysparkMirrorDir = path.join(REPO_ROOT, 'resources', 'templates', 'pyspark');
        const mirrorFiles = fs.readdirSync(pysparkMirrorDir).sort();
        expect(README_TEXT).toContain('resources/templates/pyspark/');
        for (const templateFile of mirrorFiles) {
            expect(README_TEXT, `Missing mirrored runner template ${templateFile}`).toContain(templateFile);
        }

        for (const expectedReference of [
            '40',
            '38',
            '33',
            'STREAMING_SOURCE_TRANSFORMS',
            'STREAMING_STEP_TRANSFORMS',
            'PYTHON_DATASOURCE_TRANSFORMS',
            '## Credential Handling Rules',
            `${CODEGEN_REVIEWER_SETTING.section}.${CODEGEN_REVIEWER_SETTING.key}`,
            'resources/codegen-prompts/reviewer.md',
            'resources/codegen-prompts/fix.md',
        ]) {
            expect(README_TEXT, `Missing reference ${expectedReference}`).toContain(expectedReference);
        }
    });

    it('enforces single Author credit, ASD-STE100 sentence limits, zero prose semicolons, and zero banned legacy tokens', () => {
        const authorCredits = README_TEXT.match(/Author\*\*:\s*Mike Nimer <mikenimer@google\.com>|Author:\s*Mike Nimer <mikenimer@google\.com>/g) ?? [];
        expect(authorCredits).toHaveLength(1);
        expect(README_TEXT).toContain('> **Author**: Mike Nimer <mikenimer@google.com>');

        const nonFencedText = stripFencedCodeBlocks(README_TEXT);
        expect(nonFencedText).not.toContain(';');
        expect(collectSte100Violations(README_TEXT)).toEqual([]);

        const selfText = fs.readFileSync(__filename, 'utf-8');
        for (const { label, pattern } of buildBannedTokenPatterns()) {
            expect(README_TEXT, `README matched banned ${label}`).not.toMatch(pattern);
            expect(selfText, `Test file matched banned ${label}`).not.toMatch(pattern);
        }
    });
});

describe('collectSte100Violations helper coverage', () => {
    it('flags >20-word procedural steps and >25-word prose sentences while ignoring fenced code blocks', () => {
        const longStep = `1. ${Array.from({ length: 21 }, (_, idx) => `step${idx}`).join(' ')}.`;
        const longProse = `${Array.from({ length: 26 }, (_, idx) => `word${idx}`).join(' ')}.`;
        const fencedCode = ['```python', 'x = 1; y = 2  # semicolon inside code block is allowed', longProse, '```'].join('\n');

        expect(collectSte100Violations(fencedCode)).toEqual([]);
        expect(collectSte100Violations(longStep)).toHaveLength(1);
        expect(collectSte100Violations(longProse)).toHaveLength(1);
    });
});
