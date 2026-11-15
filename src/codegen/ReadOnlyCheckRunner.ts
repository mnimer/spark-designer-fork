/**
 * Read-Only Check Runner
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Fast checks on generated files with NO model call. The optional reviewer
 * (`CodeReviewer.ts`) sends the results to the review model as evidence:
 * 1. `required_files`: the build file of the language, the main and test sources, and
 *    every runner template file of the selected runner skills exist.
 * 2. `leftover_tokens`: no `{{...}}` placeholder and no redaction sentinel remain.
 * 3. `python_syntax` (Python): `python3 -m py_compile` on every `.py` file. Skipped when no
 *    interpreter is available.
 * 4. `java_braces` (Java): braces, brackets, and parentheses balance in every `.java` file
 *    (string literals, char literals, and comments are skipped).
 * 5. `test_imports` (Python): the test module imports the main module by its real name.
 *
 * Architectural decisions / notes for future sessions:
 * - Checks NEVER modify generated code (project rule: no procedural post-processing). The
 *   review model decides what to fix and the fix call rewrites the file.
 * - A missing tool marks a check `skipped`, not `failed`.
 * - The command executor is injectable, so unit tests never spawn real processes.
 * - The required runner files come from the runner skills' template file names, so the
 *   skills stay the single source of truth for which runner files exist.
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFile } from 'child_process';
import { REDACTION_PLACEHOLDER } from '../credentialRedaction';
import { LANGUAGE_BUILD_FILES, READ_ONLY_CHECK_LIMITS, SkillLanguage } from './codegenConfig';

/** One generated file (same shape as the structured output schema). */
export interface GeneratedFile {
    /** Project-relative path with forward slashes. */
    filename: string;
    /** Complete file text. */
    content: string;
    /** Model-requested executable bit (informational). */
    executable?: boolean;
}

/** Identifiers of the checks. */
export const CHECK_IDS = {
    requiredFiles: 'required_files',
    leftoverTokens: 'leftover_tokens',
    pythonSyntax: 'python_syntax',
    javaBraces: 'java_braces',
    testImports: 'test_imports',
} as const;

/** A check identifier. */
export type CheckId = (typeof CHECK_IDS)[keyof typeof CHECK_IDS];

/** Outcome status of one check. */
export type CheckStatus = 'passed' | 'failed' | 'skipped';

/** Outcome of one check. */
export interface CheckResult {
    /** Which check. */
    check: CheckId;
    /** Status. */
    status: CheckStatus;
    /** Evidence (truncated) or the skip reason. */
    output: string;
}

/** Result of running one command. */
export interface CommandOutcome {
    /** Process exit code (-1 when it could not start). */
    exitCode: number;
    /** Captured stdout. */
    stdout: string;
    /** Captured stderr. */
    stderr: string;
    /** True when the executable could not be started (ENOENT). */
    notFound?: boolean;
}

/** Executes a command. Injectable for tests. */
export type CommandExecutor = (command: string, args: string[], options: { cwd: string; timeoutMs: number }) => Promise<CommandOutcome>;

/** Default executor backed by `child_process.execFile`. */
export const defaultCommandExecutor: CommandExecutor = (command, args, options) =>
    new Promise((resolve) => {
        execFile(command, args, { cwd: options.cwd, timeout: options.timeoutMs }, (error, stdout, stderr) => {
            const err = error as (Error & { code?: unknown }) | null;
            if (err?.code === 'ENOENT') {
                resolve({ exitCode: -1, stdout: '', stderr: String(err.message), notFound: true });
                return;
            }
            const exitCode = err ? (typeof err.code === 'number' ? err.code : 1) : 0;
            resolve({ exitCode, stdout: String(stdout ?? ''), stderr: String(stderr || err?.message || '') });
        });
    });

/** Input of one check run. */
export interface CheckRequest {
    /** Generated files. */
    files: GeneratedFile[];
    /** Pipeline language. */
    language: SkillLanguage;
    /** Pipeline base name (file stem, for example `orders`). */
    baseName: string;
    /** Runner files that must exist (template file names of the selected runner skills). */
    runnerFiles: readonly string[];
}

/** Marker appended to truncated output. */
const TRUNCATION_MARKER = '\n...[truncated]';

/** Pattern of a leftover placeholder. */
const PLACEHOLDER_PATTERN = /\{\{[A-Za-z_][\w]*\}\}/g;

/** Opening to closing bracket pairs for the Java balance check. */
const BRACKET_PAIRS: Readonly<Record<string, string>> = { '{': '}', '(': ')', '[': ']' };

/**
 * Truncates check output to the configured limit.
 *
 * @param text Raw output.
 * @returns The text, cut at `maxOutputChars` with a marker.
 */
export function truncateCheckOutput(text: string): string {
    const limit = READ_ONLY_CHECK_LIMITS.maxOutputChars;
    return text.length <= limit ? text : text.slice(0, limit) + TRUNCATION_MARKER;
}

/**
 * Converts a pipeline base name into the PascalCase Java class name.
 *
 * @param baseName Pipeline base name.
 * @returns The class name.
 */
function toJavaClassName(baseName: string): string {
    return baseName.charAt(0).toUpperCase() + baseName.slice(1);
}

/**
 * Returns the main and test source paths that the extension routes files to.
 *
 * @param language Pipeline language.
 * @param baseName Pipeline base name.
 * @returns `[main, test]` relative paths.
 */
export function expectedSourceFiles(language: SkillLanguage, baseName: string): [string, string] {
    if (language === 'java') {
        const cls = toJavaClassName(baseName);
        return [`src/main/java/com/example/${cls}.java`, `src/test/java/com/example/${cls}Test.java`];
    }
    return [`src/${baseName}.py`, `src/${baseName}_test.py`];
}

/**
 * Finds the first unbalanced bracket in Java source. Skips comments and literals.
 *
 * @param source Java source text.
 * @returns A problem description, or null when balanced.
 */
export function findJavaBracketProblem(source: string): string | null {
    const stack: Array<{ char: string; line: number }> = [];
    let line = 1;
    for (let i = 0; i < source.length; i++) {
        const ch = source[i];
        const next = source[i + 1];
        if (ch === '\n') {
            line++;
            continue;
        }
        // 1. Comments: skip to the end of the line or of the block.
        if (ch === '/' && next === '/') {
            const end = source.indexOf('\n', i);
            i = (end < 0 ? source.length : end) - 1;
            continue;
        }
        if (ch === '/' && next === '*') {
            const end = source.indexOf('*/', i + 2);
            const stop = end < 0 ? source.length : end + 2;
            line += (source.slice(i, stop).match(/\n/g) ?? []).length;
            i = stop - 1;
            continue;
        }
        // 2. Text blocks, string literals, and char literals: skip to the closing quote.
        if (ch === '"' && source.startsWith('"""', i)) {
            const end = source.indexOf('"""', i + 3);
            const stop = end < 0 ? source.length : end + 3;
            line += (source.slice(i, stop).match(/\n/g) ?? []).length;
            i = stop - 1;
            continue;
        }
        if (ch === '"' || ch === "'") {
            let j = i + 1;
            while (j < source.length && source[j] !== ch && source[j] !== '\n') {
                j += source[j] === '\\' ? 2 : 1;
            }
            i = j;
            continue;
        }
        // 3. Brackets.
        if (BRACKET_PAIRS[ch]) {
            stack.push({ char: ch, line });
        } else if (ch === '}' || ch === ')' || ch === ']') {
            const open = stack.pop();
            if (!open || BRACKET_PAIRS[open.char] !== ch) {
                return `Unexpected '${ch}' at line ${line}.`;
            }
        }
    }
    const unclosed = stack.pop();
    return unclosed ? `Unclosed '${unclosed.char}' opened at line ${unclosed.line}.` : null;
}

/**
 * Runs read-only verification checks over generated files.
 */
export class ReadOnlyCheckRunner {
    /**
     * Creates the runner.
     *
     * @param executor Command executor (tests inject a fake).
     * @param pythonCommand Python interpreter for `py_compile`.
     */
    constructor(
        private readonly executor: CommandExecutor = defaultCommandExecutor,
        private readonly pythonCommand: string = READ_ONLY_CHECK_LIMITS.pythonCommand
    ) {}

    /**
     * Runs every check in a stable order.
     *
     * @param request Files, language, base name, and runner files.
     * @returns One result per check.
     */
    public async run(request: CheckRequest): Promise<CheckResult[]> {
        return [
            this.checkRequiredFiles(request),
            this.checkLeftoverTokens(request.files),
            request.language === 'python'
                ? await this.checkPythonSyntax(request.files)
                : { check: CHECK_IDS.pythonSyntax, status: 'skipped', output: 'Not a Python project.' },
            request.language === 'java'
                ? this.checkJavaBraces(request.files)
                : { check: CHECK_IDS.javaBraces, status: 'skipped', output: 'Not a Java project.' },
            request.language === 'python'
                ? this.checkTestImports(request)
                : { check: CHECK_IDS.testImports, status: 'skipped', output: 'Not a Python project.' },
        ];
    }

    /**
     * Checks that the build file, the main and test sources, and the runner files exist.
     *
     * @param request Check request.
     * @returns The result.
     */
    private checkRequiredFiles(request: CheckRequest): CheckResult {
        const present = new Set(request.files.map((file) => file.filename));
        const required = [
            LANGUAGE_BUILD_FILES[request.language],
            ...expectedSourceFiles(request.language, request.baseName),
            ...request.runnerFiles,
        ];
        const missing = required.filter((file) => !present.has(file));
        return missing.length === 0
            ? { check: CHECK_IDS.requiredFiles, status: 'passed', output: '' }
            : { check: CHECK_IDS.requiredFiles, status: 'failed', output: `Missing file(s): ${missing.join(', ')}` };
    }

    /**
     * Checks that no placeholder or redaction sentinel remains in any file.
     *
     * @param files Generated files.
     * @returns The result.
     */
    private checkLeftoverTokens(files: GeneratedFile[]): CheckResult {
        const problems: string[] = [];
        for (const file of files) {
            const tokens = new Set(file.content.match(PLACEHOLDER_PATTERN) ?? []);
            if (file.content.includes(REDACTION_PLACEHOLDER)) {
                tokens.add(REDACTION_PLACEHOLDER);
            }
            if (tokens.size > 0) {
                problems.push(`${file.filename}: ${[...tokens].join(', ')}`);
            }
        }
        return problems.length === 0
            ? { check: CHECK_IDS.leftoverTokens, status: 'passed', output: '' }
            : { check: CHECK_IDS.leftoverTokens, status: 'failed', output: truncateCheckOutput(problems.join('\n')) };
    }

    /**
     * Byte-compiles every Python file in a temp folder.
     *
     * @param files Generated files.
     * @returns The result.
     */
    private async checkPythonSyntax(files: GeneratedFile[]): Promise<CheckResult> {
        const pythonFiles = files.filter((file) => file.filename.endsWith('.py'));
        if (pythonFiles.length === 0) {
            return { check: CHECK_IDS.pythonSyntax, status: 'skipped', output: 'No Python files generated.' };
        }
        const workDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'beam-codegen-checks-'));
        try {
            // 1. Materialize the Python files. Paths are model-controlled, so keep them inside workDir.
            const written: string[] = [];
            for (const file of pythonFiles) {
                const target = path.resolve(workDir, file.filename);
                if (!target.startsWith(workDir + path.sep)) {
                    continue;
                }
                await fs.promises.mkdir(path.dirname(target), { recursive: true });
                await fs.promises.writeFile(target, file.content, 'utf-8');
                written.push(path.relative(workDir, target));
            }
            // 2. Compile. A missing interpreter is a skip, not a failure.
            const outcome = await this.executor(this.pythonCommand, ['-m', 'py_compile', ...written], {
                cwd: workDir,
                timeoutMs: READ_ONLY_CHECK_LIMITS.timeoutMs,
            });
            if (outcome.notFound) {
                return { check: CHECK_IDS.pythonSyntax, status: 'skipped', output: `Python interpreter '${this.pythonCommand}' not found.` };
            }
            const output = truncateCheckOutput([outcome.stdout, outcome.stderr].filter(Boolean).join('\n').trim());
            return { check: CHECK_IDS.pythonSyntax, status: outcome.exitCode === 0 ? 'passed' : 'failed', output };
        } finally {
            // 3. Never leave generated code behind.
            await fs.promises.rm(workDir, { recursive: true, force: true });
        }
    }

    /**
     * Checks bracket balance in every Java file.
     *
     * @param files Generated files.
     * @returns The result.
     */
    private checkJavaBraces(files: GeneratedFile[]): CheckResult {
        const javaFiles = files.filter((file) => file.filename.endsWith('.java'));
        if (javaFiles.length === 0) {
            return { check: CHECK_IDS.javaBraces, status: 'skipped', output: 'No Java files generated.' };
        }
        const problems = javaFiles
            .map((file) => ({ file: file.filename, problem: findJavaBracketProblem(file.content) }))
            .filter((entry) => entry.problem !== null)
            .map((entry) => `${entry.file}: ${entry.problem}`);
        return problems.length === 0
            ? { check: CHECK_IDS.javaBraces, status: 'passed', output: '' }
            : { check: CHECK_IDS.javaBraces, status: 'failed', output: truncateCheckOutput(problems.join('\n')) };
    }

    /**
     * Checks that the Python test module imports the main module by its real name.
     *
     * @param request Check request.
     * @returns The result.
     */
    private checkTestImports(request: CheckRequest): CheckResult {
        const [, testPath] = expectedSourceFiles(request.language, request.baseName);
        const test = request.files.find((file) => file.filename === testPath);
        if (!test) {
            return { check: CHECK_IDS.testImports, status: 'skipped', output: `No ${testPath} generated.` };
        }
        // Accept `from src.<name> import`, `import src.<name>`, `from <name> import`, `import <name>`.
        const name = request.baseName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const pattern = new RegExp(`^\\s*(from\\s+(src\\.)?${name}\\s+import|import\\s+(src\\.)?${name}\\b)`, 'm');
        return pattern.test(test.content)
            ? { check: CHECK_IDS.testImports, status: 'passed', output: '' }
            : {
                check: CHECK_IDS.testImports,
                status: 'failed',
                output: `${testPath} does not import the main module 'src.${request.baseName}'.`,
            };
    }
}
