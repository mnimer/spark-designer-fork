/**
 * Code Reviewer (optional post-generation pass)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * After the single generation call, optionally reviews the generated files and fixes the
 * defects that it finds:
 * 1. Run the read-only checks (`ReadOnlyCheckRunner`). No model call.
 * 2. Make ONE review call. The prompt holds the reviewer template, the reviewer-selected
 *    skills with the pipeline YAML, the generated files, and the check results. The
 *    response is a structured list of findings (file, severity, problem, fix, skill rule).
 * 3. If there are `blocking` findings, make ONE fix call. It returns only the changed files
 *    in the generator's JSON schema. Those files replace the old versions.
 * 4. Repeat at most `CODEGEN_REVIEWER_MAX_FIX_ROUNDS` times. A final review reports what is
 *    left.
 *
 * Architectural decisions / notes for future sessions:
 * - OFF by default (`CODEGEN_REVIEWER_ENABLED_BY_DEFAULT`). The user turns it on with the
 *   `sparkDesigner.codegen.enableReviewer` setting or the UI toggle.
 * - Files change only through the model, never through scripts (project rule).
 * - Model calls are injected (`reviewFn`, `fixFn`), so this module has no SDK dependency
 *   and is unit-tested with a fake model client.
 * - Prompt text lives in `resources/codegen-prompts/{reviewer,fix}.md` and in the skills.
 * - A reviewer failure (malformed JSON, failed call) never loses the generated files. The
 *   outcome carries the error and the best files so far. Cancellation is re-thrown.
 * - A fix call may only return files that exist or that a finding names. Any other file
 *   name is ignored and reported, so the fix call cannot add stray files.
 */

import { parseOrRepairJson } from './jsonRepair';
import { CODEGEN_REVIEWER_MAX_FIX_ROUNDS, REVIEW_SEVERITIES } from './codegenConfig';
import { fence } from './CodegenPromptBuilder';
import type { CheckResult, GeneratedFile } from './ReadOnlyCheckRunner';

/** Severity of a reviewer finding. */
export type ReviewSeverity = (typeof REVIEW_SEVERITIES)[keyof typeof REVIEW_SEVERITIES];

/** One reviewer finding. */
export interface ReviewFinding {
    /** Project-relative path that must change (or the missing file path). */
    file: string;
    /** `blocking` triggers a fix call. `minor` is reported only. */
    severity: ReviewSeverity;
    /** What is wrong, with evidence. */
    problem: string;
    /** What must change. */
    fix: string;
    /** The skill rule that the file breaks (for example `io-kafka`). */
    skillRule: string;
}

/** Structured reviewer output. */
export interface ReviewReport {
    /** Findings. */
    findings: ReviewFinding[];
    /** One-sentence summary. */
    summary: string;
}

/** Gemini response schema of the review call. */
export const REVIEW_REPORT_SCHEMA = {
    type: 'OBJECT',
    properties: {
        findings: {
            type: 'ARRAY',
            items: {
                type: 'OBJECT',
                properties: {
                    file: { type: 'STRING', description: 'Project-relative path that must change, or the missing file path.' },
                    severity: { type: 'STRING', enum: Object.values(REVIEW_SEVERITIES) },
                    problem: { type: 'STRING', description: 'What is wrong, with evidence.' },
                    fix: { type: 'STRING', description: 'What must change.' },
                    skillRule: { type: 'STRING', description: 'Name of the skill whose rule the file breaks.' },
                },
                required: ['file', 'severity', 'problem', 'fix', 'skillRule'],
            },
        },
        summary: { type: 'STRING', description: 'One-sentence review summary.' },
    },
    required: ['findings', 'summary'],
};

/** Injected collaborators. */
export interface CodeReviewerDeps {
    /** Calls the model with the review prompt. Returns JSON text or an object. */
    reviewFn(prompt: string): Promise<unknown>;
    /** Calls the model with the fix prompt. Returns JSON text or `{ files: [...] }`. */
    fixFn(prompt: string): Promise<unknown>;
    /** Runs the read-only checks over the current files. */
    runChecks(files: GeneratedFile[]): Promise<CheckResult[]>;
    /** Receives one human-readable progress message per step. */
    onProgress?(message: string): void;
    /** Cancels the review between steps. */
    abortSignal?: AbortSignal;
}

/** Input of one review run. */
export interface ReviewRequest {
    /** Files from the generation call. */
    files: GeneratedFile[];
    /** The full generation prompt. Fix calls reuse it so the same skills apply. */
    generationPrompt: string;
    /** Reviewer prompt context: reviewer-selected skills and the pipeline YAML. */
    reviewerContext: string;
    /** Reviewer prompt template (`resources/codegen-prompts/reviewer.md`). */
    reviewerTemplate: string;
    /** Fix prompt template (`resources/codegen-prompts/fix.md`). */
    fixTemplate: string;
}

/** Outcome of a review run. */
export interface ReviewOutcome {
    /** Final files (original files when nothing was fixed). */
    files: GeneratedFile[];
    /** One report per review call. */
    reports: ReviewReport[];
    /** Blocking findings that are left after the last round. */
    unresolved: ReviewFinding[];
    /** Number of fix calls made. */
    fixRounds: number;
    /** File names that a fix call returned but that were not allowed. */
    ignoredFiles: string[];
    /** Set when the review stopped early because of an error. */
    error?: string;
}

/** Separator between prompt sections. */
const SECTION_SEPARATOR = '\n\n';

/**
 * Creates an error that the caller recognizes as a cancellation.
 *
 * @returns An Error named `AbortError`.
 */
function abortError(): Error {
    const err = new Error('Code review was cancelled.');
    err.name = 'AbortError';
    return err;
}

/**
 * Throws an AbortError when the signal is aborted.
 *
 * @param signal Optional cancellation signal.
 */
function throwIfAborted(signal?: AbortSignal): void {
    if (signal?.aborted) {
        throw abortError();
    }
}

/**
 * Checks whether an error is a cancellation.
 *
 * @param err Caught error.
 * @param signal Optional cancellation signal.
 * @returns True for an AbortError or an aborted signal.
 */
function isAbort(err: unknown, signal?: AbortSignal): boolean {
    return signal?.aborted === true || (err instanceof Error && err.name === 'AbortError');
}

/**
 * Converts model output (object or JSON text) into an object.
 *
 * @param output Object or JSON text.
 * @param label Label for error messages.
 * @returns The parsed value.
 * @throws Error when the text is not JSON.
 */
function toObject(output: unknown, label: string): any {
    if (typeof output !== 'string') {
        return output;
    }
    try {
        return parseOrRepairJson(output);
    } catch {
        throw new Error(`${label} returned non-JSON output: ${output.slice(0, 200)}`);
    }
}

/**
 * Parses reviewer output. Fails fast on a malformed report.
 *
 * @param output Object or JSON text.
 * @returns The report. Findings without a file are dropped. Unknown severities count as blocking.
 * @throws Error when the output is not JSON or has no findings array.
 */
export function parseReviewReport(output: unknown): ReviewReport {
    const value = toObject(output, 'Reviewer');
    if (!value || !Array.isArray(value.findings)) {
        throw new Error('Reviewer output is missing the "findings" array.');
    }
    const findings: ReviewFinding[] = value.findings
        .filter((f: any) => f && typeof f.file === 'string' && f.file.trim())
        .map((f: any) => ({
            file: f.file.trim(),
            // NOTE: an unknown severity is treated as blocking (fail safe toward fixing).
            severity: f.severity === REVIEW_SEVERITIES.minor ? REVIEW_SEVERITIES.minor : REVIEW_SEVERITIES.blocking,
            problem: String(f.problem ?? ''),
            fix: String(f.fix ?? ''),
            skillRule: String(f.skillRule ?? ''),
        }));
    return { findings, summary: typeof value.summary === 'string' ? value.summary : '' };
}

/**
 * Parses the fix call output (`{ files: [...] }`).
 *
 * @param output Object or JSON text.
 * @returns The returned files with a filename and string content.
 * @throws Error when the output is not JSON or has no files array.
 */
export function parseFixFiles(output: unknown): GeneratedFile[] {
    const value = toObject(output, 'Fix call');
    if (!value || !Array.isArray(value.files)) {
        throw new Error('Fix call output is missing the "files" array.');
    }
    return value.files.filter((f: any) => f && typeof f.filename === 'string' && f.filename && typeof f.content === 'string');
}

/**
 * Renders files as headed fenced blocks.
 *
 * @param files Files to render.
 * @returns The rendered text.
 */
export function renderFiles(files: GeneratedFile[]): string {
    return files.map((file) => `### File: \`${file.filename}\`\n${fence(file.content)}`).join(SECTION_SEPARATOR);
}

/**
 * Renders check results as headed fenced blocks.
 *
 * @param checks Check results.
 * @returns The rendered text.
 */
export function renderChecks(checks: CheckResult[]): string {
    if (checks.length === 0) {
        return '(no checks run)';
    }
    return checks
        .map((result) => `### Check: ${result.check} (${result.status.toUpperCase()})\n${fence(result.output || '(no output)')}`)
        .join(SECTION_SEPARATOR);
}

/**
 * Merges fixed files into the file list. Only existing files and files that a finding
 * names are accepted.
 *
 * @param files Current files.
 * @param updates Files returned by the fix call.
 * @param allowedNew File names that a finding named (may be missing today).
 * @returns The merged files and the ignored file names.
 */
export function mergeFiles(
    files: GeneratedFile[],
    updates: GeneratedFile[],
    allowedNew: ReadonlySet<string>
): { files: GeneratedFile[]; ignored: string[] } {
    const byName = new Map(files.map((file) => [file.filename, file]));
    const ignored: string[] = [];
    for (const update of updates) {
        if (byName.has(update.filename) || allowedNew.has(update.filename)) {
            byName.set(update.filename, { ...byName.get(update.filename), ...update });
        } else {
            ignored.push(update.filename);
        }
    }
    return { files: [...byName.values()], ignored };
}

/**
 * Builds the review prompt.
 *
 * @param request Review request.
 * @param files Current files.
 * @param checks Read-only check results.
 * @returns The prompt.
 */
export function buildReviewPrompt(request: ReviewRequest, files: GeneratedFile[], checks: CheckResult[]): string {
    return [
        request.reviewerTemplate,
        request.reviewerContext,
        `## Generated files\n\n${renderFiles(files)}`,
        `## Read-only check results\n\n${renderChecks(checks)}`,
    ].join(SECTION_SEPARATOR);
}

/**
 * Builds the fix prompt: the generation prompt, the fix template, the findings, the
 * affected files, and the paths of the other files.
 *
 * @param request Review request.
 * @param files Current files.
 * @param findings Blocking findings to fix.
 * @returns The prompt.
 */
export function buildFixPrompt(request: ReviewRequest, files: GeneratedFile[], findings: ReviewFinding[]): string {
    const affected = new Set(findings.map((finding) => finding.file));
    const findingText = findings
        .map((f, index) => `${index + 1}. \`${f.file}\` (${f.skillRule || 'no skill named'}): ${f.problem}\n   Fix: ${f.fix}`)
        .join('\n');
    const others = files.filter((file) => !affected.has(file.filename)).map((file) => `- ${file.filename}`);
    return [
        request.generationPrompt,
        request.fixTemplate,
        `## Reviewer findings\n\n${findingText}`,
        `## Current versions of the affected files\n\n${renderFiles(files.filter((file) => affected.has(file.filename))) || '(none of the affected files exist yet)'}`,
        `## Other files in the project (paths only)\n\n${others.join('\n') || '(none)'}`,
    ].join(SECTION_SEPARATOR);
}

/**
 * Runs review -> fix rounds over the generated files.
 *
 * @param request Review request.
 * @param deps Injected model calls, checks, progress, and cancellation.
 * @returns The outcome. Never throws for model errors. Throws an AbortError on cancel.
 */
export async function reviewAndFix(request: ReviewRequest, deps: CodeReviewerDeps): Promise<ReviewOutcome> {
    const outcome: ReviewOutcome = { files: request.files, reports: [], unresolved: [], fixRounds: 0, ignoredFiles: [] };

    for (let round = 1; round <= CODEGEN_REVIEWER_MAX_FIX_ROUNDS + 1; round++) {
        try {
            // 1. Evidence: read-only checks.
            throwIfAborted(deps.abortSignal);
            deps.onProgress?.(`Reviewer round ${round}: running read-only checks...`);
            const checks = await deps.runChecks(outcome.files);

            // 2. One review call.
            throwIfAborted(deps.abortSignal);
            deps.onProgress?.(`Reviewer round ${round}: reviewing ${outcome.files.length} generated file(s)...`);
            const report = parseReviewReport(await deps.reviewFn(buildReviewPrompt(request, outcome.files, checks)));
            outcome.reports.push(report);
            const blocking = report.findings.filter((f) => f.severity === REVIEW_SEVERITIES.blocking);
            outcome.unresolved = blocking;

            // 3. Clean, or the fix budget is spent: stop.
            if (blocking.length === 0 || round > CODEGEN_REVIEWER_MAX_FIX_ROUNDS) {
                break;
            }

            // 4. One fix call for the affected files only.
            throwIfAborted(deps.abortSignal);
            const targets = [...new Set(blocking.map((f) => f.file))];
            deps.onProgress?.(`Reviewer round ${round}: fixing ${targets.length} file(s): ${targets.join(', ')}`);
            const updates = parseFixFiles(await deps.fixFn(buildFixPrompt(request, outcome.files, blocking)));
            const merged = mergeFiles(outcome.files, updates, new Set(targets));
            outcome.files = merged.files;
            outcome.ignoredFiles.push(...merged.ignored);
            outcome.fixRounds++;
        } catch (err) {
            // Cancellation propagates. Any other error stops the review and keeps the files.
            if (isAbort(err, deps.abortSignal)) {
                throw err instanceof Error && err.name === 'AbortError' ? err : abortError();
            }
            outcome.error = err instanceof Error ? err.message : String(err);
            break;
        }
    }
    return outcome;
}
