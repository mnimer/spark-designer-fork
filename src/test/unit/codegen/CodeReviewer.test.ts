/**
 * Unit tests for the optional code reviewer (`src/codegen/CodeReviewer.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Uses a fake model client. Covers: no findings, findings fixed in round 1, findings left
 * after the maximum rounds, minor-only findings, malformed review JSON, a fix call that
 * returns an unknown file name, a failed fix call, cancellation (before a call and from the
 * model), progress messages, and the prompt builders and parsers.
 */

import { describe, it, expect, vi } from 'vitest';
import {
    CodeReviewerDeps,
    ReviewRequest,
    buildFixPrompt,
    buildReviewPrompt,
    mergeFiles,
    parseFixFiles,
    parseReviewReport,
    renderChecks,
    reviewAndFix,
} from '../../../codegen/CodeReviewer';
import { CODEGEN_REVIEWER_MAX_FIX_ROUNDS } from '../../../codegen/codegenConfig';
import type { CheckResult, GeneratedFile } from '../../../codegen/ReadOnlyCheckRunner';

/** Files from the generation call. */
const FILES: GeneratedFile[] = [
    { filename: 'src/orders.py', content: 'bad' },
    { filename: 'requirements.txt', content: 'apache-beam' },
];

/** Review request with fixed templates. */
const REQUEST: ReviewRequest = {
    files: FILES,
    generationPrompt: 'GENERATION PROMPT',
    reviewerContext: 'REVIEWER SKILLS AND YAML',
    reviewerTemplate: 'REVIEWER TEMPLATE',
    fixTemplate: 'FIX TEMPLATE',
};

/** One blocking finding on the main module. */
const BLOCKING = { file: 'src/orders.py', severity: 'blocking', problem: 'syntax', fix: 'repair', skillRule: 'lang-python' };

/**
 * Builds fake deps from queued review and fix outputs.
 *
 * @param reviews Review outputs, one per call.
 * @param fixes Fix outputs, one per call.
 * @returns Deps with spies.
 */
function fakeDeps(reviews: unknown[], fixes: unknown[] = []) {
    const checks: CheckResult[] = [{ check: 'python_syntax', status: 'failed', output: 'SyntaxError' }];
    return {
        reviewFn: vi.fn(async (_prompt: string): Promise<unknown> => reviews.shift()),
        fixFn: vi.fn(async (_prompt: string): Promise<unknown> => fixes.shift()),
        runChecks: vi.fn(async (_files: GeneratedFile[]) => checks),
        onProgress: vi.fn(),
    } satisfies CodeReviewerDeps;
}

describe('reviewAndFix', () => {
    it('stops after one review when there are no findings (simple)', async () => {
        const deps = fakeDeps([{ findings: [], summary: 'clean' }]);
        const outcome = await reviewAndFix(REQUEST, deps);
        expect(outcome.files).toBe(FILES);
        expect(outcome.fixRounds).toBe(0);
        expect(outcome.unresolved).toEqual([]);
        expect(outcome.reports).toEqual([{ findings: [], summary: 'clean' }]);
        expect(deps.fixFn).not.toHaveBeenCalled();
        expect(deps.onProgress.mock.calls.map((c) => c[0])).toEqual([
            'Reviewer round 1: running read-only checks...',
            'Reviewer round 1: reviewing 2 generated file(s)...',
        ]);
    });

    it('fixes findings in round 1 and confirms with a second review (simple)', async () => {
        const deps = fakeDeps(
            [JSON.stringify({ findings: [BLOCKING], summary: 'broken' }), { findings: [], summary: 'clean' }],
            [JSON.stringify({ files: [{ filename: 'src/orders.py', content: 'good' }] })]
        );
        const outcome = await reviewAndFix(REQUEST, deps);
        expect(outcome.fixRounds).toBe(1);
        expect(outcome.files.find((f) => f.filename === 'src/orders.py')?.content).toBe('good');
        expect(outcome.files.find((f) => f.filename === 'requirements.txt')?.content).toBe('apache-beam');
        expect(outcome.unresolved).toEqual([]);
        expect(deps.fixFn.mock.calls[0][0]).toContain('GENERATION PROMPT');
        expect(deps.onProgress).toHaveBeenCalledWith('Reviewer round 1: fixing 1 file(s): src/orders.py');
    });

    it('stops after the maximum rounds and reports unresolved findings (extreme)', async () => {
        const reviews = Array.from({ length: CODEGEN_REVIEWER_MAX_FIX_ROUNDS + 1 }, () => ({ findings: [BLOCKING], summary: 'still broken' }));
        const fixes = Array.from({ length: CODEGEN_REVIEWER_MAX_FIX_ROUNDS }, () => ({ files: [{ filename: 'src/orders.py', content: 'still bad' }] }));
        const deps = fakeDeps(reviews, fixes);
        const outcome = await reviewAndFix(REQUEST, deps);
        expect(deps.reviewFn).toHaveBeenCalledTimes(CODEGEN_REVIEWER_MAX_FIX_ROUNDS + 1);
        expect(deps.fixFn).toHaveBeenCalledTimes(CODEGEN_REVIEWER_MAX_FIX_ROUNDS);
        expect(outcome.fixRounds).toBe(CODEGEN_REVIEWER_MAX_FIX_ROUNDS);
        expect(outcome.unresolved).toHaveLength(1);
    });

    it('does not call the fixer for minor findings only (simple)', async () => {
        const deps = fakeDeps([{ findings: [{ ...BLOCKING, severity: 'minor' }], summary: 'nits' }]);
        const outcome = await reviewAndFix(REQUEST, deps);
        expect(deps.fixFn).not.toHaveBeenCalled();
        expect(outcome.unresolved).toEqual([]);
        expect(outcome.reports[0].findings[0].severity).toBe('minor');
    });

    it('keeps the files and records the error on malformed review JSON (failure)', async () => {
        const deps = fakeDeps(['not json at all']);
        const outcome = await reviewAndFix(REQUEST, deps);
        expect(outcome.files).toBe(FILES);
        expect(outcome.error).toMatch(/Reviewer returned non-JSON output/);
    });

    it('records an error when the review has no findings array (failure)', async () => {
        const outcome = await reviewAndFix(REQUEST, fakeDeps([{ summary: 'x' }]));
        expect(outcome.error).toBe('Reviewer output is missing the "findings" array.');
    });

    it('ignores a fixed file with an unknown name but accepts a missing file a finding names (failure)', async () => {
        const missing = { ...BLOCKING, file: 'Dockerfile' };
        const deps = fakeDeps(
            [{ findings: [missing], summary: 'missing' }, { findings: [], summary: 'clean' }],
            [{ files: [{ filename: 'Dockerfile', content: 'FROM x' }, { filename: 'stray/hack.py', content: 'x' }] }]
        );
        const outcome = await reviewAndFix(REQUEST, deps);
        expect(outcome.files.map((f) => f.filename)).toEqual(['src/orders.py', 'requirements.txt', 'Dockerfile']);
        expect(outcome.ignoredFiles).toEqual(['stray/hack.py']);
    });

    it('records an error when the fix call fails (failure)', async () => {
        const deps = fakeDeps([{ findings: [BLOCKING], summary: 'broken' }]);
        deps.fixFn.mockRejectedValueOnce(new Error('quota exceeded'));
        const outcome = await reviewAndFix(REQUEST, deps);
        expect(outcome.error).toBe('quota exceeded');
        expect(outcome.files).toBe(FILES);
    });

    it('records a non-Error rejection as text (failure)', async () => {
        const deps = fakeDeps([]);
        deps.reviewFn.mockRejectedValueOnce('boom');
        expect((await reviewAndFix(REQUEST, deps)).error).toBe('boom');
    });

    it('throws an AbortError when cancelled before a model call (failure)', async () => {
        const controller = new AbortController();
        const deps = { ...fakeDeps([{ findings: [BLOCKING], summary: 'x' }]), abortSignal: controller.signal };
        deps.runChecks.mockImplementationOnce(async () => {
            controller.abort();
            return [];
        });
        await expect(reviewAndFix(REQUEST, deps)).rejects.toMatchObject({ name: 'AbortError' });
        expect(deps.reviewFn).not.toHaveBeenCalled();
    });

    it('re-throws an AbortError raised by the model call (failure)', async () => {
        const deps = fakeDeps([]);
        const abort = new Error('aborted');
        abort.name = 'AbortError';
        deps.reviewFn.mockRejectedValueOnce(abort);
        await expect(reviewAndFix(REQUEST, deps)).rejects.toBe(abort);
    });

    it('converts any error after abort into an AbortError (failure)', async () => {
        const controller = new AbortController();
        const deps = { ...fakeDeps([]), abortSignal: controller.signal };
        deps.reviewFn.mockImplementationOnce(async () => {
            controller.abort();
            throw new Error('network reset');
        });
        await expect(reviewAndFix(REQUEST, deps)).rejects.toMatchObject({ name: 'AbortError', message: 'Code review was cancelled.' });
    });
});

describe('parsers and prompt builders', () => {
    it('parses reports: drops findings without a file, defaults fields, treats unknown severity as blocking (extreme)', () => {
        const report = parseReviewReport({
            findings: [null, { file: '  ' }, { file: ' a.py ', severity: 'urgent' }, { file: 'b.py', severity: 'minor', problem: 1, fix: 'f', skillRule: 's' }],
        });
        expect(report).toEqual({
            findings: [
                { file: 'a.py', severity: 'blocking', problem: '', fix: '', skillRule: '' },
                { file: 'b.py', severity: 'minor', problem: '1', fix: 'f', skillRule: 's' },
            ],
            summary: '',
        });
    });

    it('parses fix files and rejects a missing files array (failure)', () => {
        expect(parseFixFiles({ files: [{ filename: 'a', content: 'x' }, { filename: '', content: 'y' }, { filename: 'b' }, null] })).toEqual([{ filename: 'a', content: 'x' }]);
        expect(() => parseFixFiles({})).toThrow(/missing the "files" array/);
        expect(() => parseFixFiles('nope')).toThrow(/Fix call returned non-JSON output/);
    });

    it('builds the review prompt in order (simple)', () => {
        const prompt = buildReviewPrompt(REQUEST, FILES, [{ check: 'java_braces', status: 'passed', output: '' }]);
        const order = ['REVIEWER TEMPLATE', 'REVIEWER SKILLS AND YAML', '## Generated files', '### File: `src/orders.py`', '## Read-only check results', '### Check: java_braces (PASSED)', '(no output)'];
        const positions = order.map((m) => prompt.indexOf(m));
        expect(positions.every((p) => p >= 0)).toBe(true);
        expect([...positions].sort((a, b) => a - b)).toEqual(positions);
        expect(renderChecks([])).toBe('(no checks run)');
    });

    it('builds the fix prompt with affected files and other paths (simple)', () => {
        const prompt = buildFixPrompt(REQUEST, FILES, [parseReviewReport({ findings: [BLOCKING] }).findings[0]]);
        expect(prompt).toContain('GENERATION PROMPT\n\nFIX TEMPLATE');
        expect(prompt).toContain('1. `src/orders.py` (lang-python): syntax\n   Fix: repair');
        expect(prompt).toContain('### File: `src/orders.py`');
        expect(prompt).toContain('## Other files in the project (paths only)\n\n- requirements.txt');
    });

    it('handles a fix prompt whose affected file is missing and no other files exist (extreme)', () => {
        const prompt = buildFixPrompt({ ...REQUEST }, [], [{ file: 'Dockerfile', severity: 'blocking', problem: 'missing', fix: 'add', skillRule: '' }]);
        expect(prompt).toContain('(none of the affected files exist yet)');
        expect(prompt).toContain('(no skill named)');
        expect(prompt).toContain('## Other files in the project (paths only)\n\n(none)');
    });

    it('merges updates into existing files and keeps executable flags (simple)', () => {
        const merged = mergeFiles([{ filename: 'run.sh', content: 'a', executable: true }], [{ filename: 'run.sh', content: 'b' }], new Set());
        expect(merged).toEqual({ files: [{ filename: 'run.sh', content: 'b', executable: true }], ignored: [] });
    });
});
