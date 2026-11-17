/**
 * Read-Only Check Runner
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Fast checks on generated PySpark files with NO model call. The optional reviewer
 * (`CodeReviewer.ts`) sends the results to the review model as evidence:
 * 1. `required_files`: the build file (`requirements.txt`), the main and test sources
 *    (`src/pipeline.py` or `src/<baseName>.py`, `tests/test_pipeline.py` or `src/<baseName>_test.py`),
 *    and every runner template file of the selected runner skills exist.
 * 2. `leftover_tokens`: no `{{...}}` placeholder and no redaction sentinel remain.
 * 3. `python_syntax`: `python3 -m py_compile` on every generated `.py` file. Skipped when no
 *    interpreter is available.
 * 4. `pipeline_contract`: verifies the primary pipeline file (`src/pipeline.py` or `src/<baseName>.py`)
 *    defines `def read_sources(`, `def transform(`, and `def write_sinks(`.
 * 5. `test_imports`: the test module imports `src.pipeline` or `src.<baseName>`.
 * 6. `pytest_collect`: runs `python3 -m pytest --collect-only -q` over the materialized test
 *    files when `pytest` is installed. Gracefully skipped when `python3` or `pytest` is absent.
 *
 * Architectural decisions / notes for future sessions:
 * - Checks NEVER modify generated code (project rule: no procedural post-processing). The
 *   review model decides what to fix and the fix call rewrites the file.
 * - A missing tool (`python3` or `pytest`) marks a check `skipped`, not `failed`.
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
    pipelineContract: 'pipeline_contract',
    testImports: 'test_imports',
    pytestCollect: 'pytest_collect',
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
export type CommandExecutor = (
    command: string,
    args: string[],
    options: { cwd: string; timeoutMs: number }
) => Promise<CommandOutcome>;

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

/** Spark redaction placeholder checked alongside `REDACTION_PLACEHOLDER`. */
const SPARK_REDACTION_PLACEHOLDER = '__REDACTED_BY_SPARK_DESIGNER__';

/** Required 3-phase functions in `src/pipeline.py` (G3 / G11). */
export const REQUIRED_PIPELINE_FUNCTIONS = ['read_sources', 'transform', 'write_sinks'] as const;

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
 * Returns the canonical PySpark main and test source paths (`src/pipeline.py`, `tests/test_pipeline.py`).
 *
 * @param _language Pipeline language.
 * @param _baseName Pipeline base name.
 * @returns `[main, test]` relative paths.
 */
export function expectedSourceFiles(_language: SkillLanguage, _baseName: string): [string, string] {
    return ['src/pipeline.py', 'tests/test_pipeline.py'];
}

/**
 * Resolves the generated main pipeline Python file (`src/pipeline.py` or `src/<baseName>.py`).
 *
 * @param files Generated files.
 * @param baseName Pipeline base name.
 * @returns Matching main pipeline file, if present.
 */
function findMainPipelineFile(files: GeneratedFile[], baseName: string): GeneratedFile | undefined {
    return (
        files.find((f) => f.filename === 'src/pipeline.py') ??
        files.find((f) => f.filename === `src/${baseName}.py`) ??
        files.find((f) => f.filename === 'pipeline.py')
    );
}

/**
 * Resolves the generated unit test Python file (`tests/test_pipeline.py` or `src/<baseName>_test.py`).
 *
 * @param files Generated files.
 * @param baseName Pipeline base name.
 * @returns Matching test file, if present.
 */
function findTestPipelineFile(files: GeneratedFile[], baseName: string): GeneratedFile | undefined {
    return (
        files.find((f) => f.filename === 'tests/test_pipeline.py') ??
        files.find((f) => f.filename === `tests/test_${baseName}.py`) ??
        files.find((f) => f.filename === `src/${baseName}_test.py`) ??
        files.find((f) => f.filename === 'src/pipeline_test.py')
    );
}

/**
 * Materializes generated `.py` files into a temporary workspace directory safely.
 *
 * @param workDir Temporary workspace root.
 * @param pythonFiles Generated `.py` files.
 * @returns Relative paths of files written inside `workDir`.
 */
async function materializePythonFiles(workDir: string, pythonFiles: GeneratedFile[]): Promise<string[]> {
    const written: string[] = [];
    for (const file of pythonFiles) {
        const target = path.resolve(workDir, file.filename);
        if (!target.startsWith(workDir + path.sep)) {
            continue;
        }
        await fs.promises.mkdir(path.dirname(target), { recursive: true });
        await fs.promises.writeFile(target, file.content, 'utf-8');
        written.push(path.relative(workDir, target).split(path.sep).join('/'));
    }
    return written;
}

/**
 * Runs read-only verification checks over generated PySpark files.
 */
export class ReadOnlyCheckRunner {
    /**
     * Creates the runner.
     *
     * @param executor Command executor (tests inject a fake).
     * @param pythonCommand Python interpreter for `py_compile` and `pytest`.
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
        const syntaxAndPytest = await this.checkPythonSyntaxAndPytest(request.files);
        return [
            this.checkRequiredFiles(request),
            this.checkLeftoverTokens(request.files),
            syntaxAndPytest.syntaxResult,
            this.checkPipelineContract(request),
            this.checkTestImports(request),
            syntaxAndPytest.pytestResult,
        ];
    }

    /**
     * Checks that the build file (`requirements.txt`), the main and test sources, and the
     * runner files exist.
     *
     * @param request Check request.
     * @returns The result.
     */
    private checkRequiredFiles(request: CheckRequest): CheckResult {
        const present = new Set(request.files.map((file) => file.filename));
        const missing: string[] = [];

        const buildFile = LANGUAGE_BUILD_FILES[request.language];
        if (buildFile && !present.has(buildFile)) {
            missing.push(buildFile);
        }

        if (!findMainPipelineFile(request.files, request.baseName)) {
            missing.push('src/pipeline.py');
        }
        if (!findTestPipelineFile(request.files, request.baseName)) {
            missing.push('tests/test_pipeline.py');
        }

        for (const runnerFile of request.runnerFiles) {
            if (!present.has(runnerFile)) {
                missing.push(runnerFile);
            }
        }

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
            if (file.content.includes(SPARK_REDACTION_PLACEHOLDER)) {
                tokens.add(SPARK_REDACTION_PLACEHOLDER);
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
     * Verifies that `src/pipeline.py` defines `read_sources`, `transform`, and `write_sinks`.
     *
     * @param request Check request.
     * @returns The result.
     */
    private checkPipelineContract(request: CheckRequest): CheckResult {
        const mainFile = findMainPipelineFile(request.files, request.baseName);
        if (!mainFile) {
            return {
                check: CHECK_IDS.pipelineContract,
                status: 'skipped',
                output: 'No src/pipeline.py generated.',
            };
        }

        const missingFns = REQUIRED_PIPELINE_FUNCTIONS.filter((fnName) => {
            const fnRegex = new RegExp(`^\\s*def\\s+${fnName}\\s*\\(`, 'm');
            return !fnRegex.test(mainFile.content);
        });

        return missingFns.length === 0
            ? { check: CHECK_IDS.pipelineContract, status: 'passed', output: '' }
            : {
                  check: CHECK_IDS.pipelineContract,
                  status: 'failed',
                  output: `${mainFile.filename} is missing required testable entrypoint function(s): ${missingFns.join(', ')}.`,
              };
    }

    /**
     * Checks that the Python test module imports the main module (`src.pipeline` or `src.<baseName>`).
     *
     * @param request Check request.
     * @returns The result.
     */
    private checkTestImports(request: CheckRequest): CheckResult {
        const test = findTestPipelineFile(request.files, request.baseName);
        if (!test) {
            return { check: CHECK_IDS.testImports, status: 'skipped', output: 'No tests/test_pipeline.py generated.' };
        }
        // Accept `from src.pipeline import`, `import src.pipeline`, `from pipeline import`, `import pipeline`,
        // or the pipeline baseName equivalent.
        const escapedBase = request.baseName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const moduleGroup = `(?:pipeline|${escapedBase})`;
        const pattern = new RegExp(
            `^\\s*(from\\s+(src\\.)?${moduleGroup}\\s+import|import\\s+(src\\.)?${moduleGroup}\\b)`,
            'm'
        );
        return pattern.test(test.content)
            ? { check: CHECK_IDS.testImports, status: 'passed', output: '' }
            : {
                  check: CHECK_IDS.testImports,
                  status: 'failed',
                  output: `${test.filename} does not import the main module 'src.pipeline'.`,
              };
    }

    /**
     * Byte-compiles every Python file with `python3 -m py_compile` and runs
     * `python3 -m pytest --collect-only -q` in a single temporary folder.
     *
     * @param files Generated files.
     * @returns Syntax and pytest collection check outcomes.
     */
    private async checkPythonSyntaxAndPytest(
        files: GeneratedFile[]
    ): Promise<{ syntaxResult: CheckResult; pytestResult: CheckResult }> {
        const pythonFiles = files.filter((file) => file.filename.endsWith('.py'));
        if (pythonFiles.length === 0) {
            return {
                syntaxResult: {
                    check: CHECK_IDS.pythonSyntax,
                    status: 'skipped',
                    output: 'No Python files generated.',
                },
                pytestResult: {
                    check: CHECK_IDS.pytestCollect,
                    status: 'skipped',
                    output: 'No Python files generated.',
                },
            };
        }

        const workDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'spark-codegen-checks-'));
        try {
            // 1. Materialize the Python files. Paths are model-controlled, so keep them inside workDir.
            const written = await materializePythonFiles(workDir, pythonFiles);
            if (written.length === 0) {
                return {
                    syntaxResult: {
                        check: CHECK_IDS.pythonSyntax,
                        status: 'skipped',
                        output: 'No safe Python files generated.',
                    },
                    pytestResult: {
                        check: CHECK_IDS.pytestCollect,
                        status: 'skipped',
                        output: 'No safe Python files generated.',
                    },
                };
            }

            // 2. Run python3 -m py_compile. A missing interpreter is a skip, not a failure.
            const compileOutcome = await this.executor(this.pythonCommand, ['-m', 'py_compile', ...written], {
                cwd: workDir,
                timeoutMs: READ_ONLY_CHECK_LIMITS.timeoutMs,
            });
            if (compileOutcome.notFound) {
                const msg = `Python interpreter '${this.pythonCommand}' not found.`;
                return {
                    syntaxResult: { check: CHECK_IDS.pythonSyntax, status: 'skipped', output: msg },
                    pytestResult: { check: CHECK_IDS.pytestCollect, status: 'skipped', output: msg },
                };
            }

            const compileOutput = truncateCheckOutput(
                [compileOutcome.stdout, compileOutcome.stderr].filter(Boolean).join('\n').trim()
            );
            const syntaxResult: CheckResult = {
                check: CHECK_IDS.pythonSyntax,
                status: compileOutcome.exitCode === 0 ? 'passed' : 'failed',
                output: compileOutput,
            };

            // 3. Run python3 -m pytest --collect-only -q when test files exist.
            const hasTestFile = written.some(
                (rel) => rel.startsWith('tests/') || rel.endsWith('_test.py') || path.basename(rel).startsWith('test_')
            );
            if (!hasTestFile) {
                return {
                    syntaxResult,
                    pytestResult: {
                        check: CHECK_IDS.pytestCollect,
                        status: 'skipped',
                        output: 'No pytest test files generated.',
                    },
                };
            }

            const pytestOutcome = await this.executor(
                this.pythonCommand,
                ['-m', 'pytest', '--collect-only', '-q'],
                {
                    cwd: workDir,
                    timeoutMs: READ_ONLY_CHECK_LIMITS.timeoutMs,
                }
            );
            const pytestCombined = [pytestOutcome.stdout, pytestOutcome.stderr].filter(Boolean).join('\n').trim();
            if (
                pytestOutcome.notFound ||
                /No module named ['"]?pytest['"]?/i.test(pytestCombined) ||
                /ModuleNotFoundError:\s*No module named ['"]?pyspark['"]?/i.test(pytestCombined)
            ) {
                return {
                    syntaxResult,
                    pytestResult: {
                        check: CHECK_IDS.pytestCollect,
                        status: 'skipped',
                        output: truncateCheckOutput(pytestCombined || 'pytest or pyspark module not installed in local environment.'),
                    },
                };
            }

            return {
                syntaxResult,
                pytestResult: {
                    check: CHECK_IDS.pytestCollect,
                    status: pytestOutcome.exitCode === 0 ? 'passed' : 'failed',
                    output: truncateCheckOutput(pytestCombined),
                },
            };
        } finally {
            // 4. Never leave generated code behind.
            await fs.promises.rm(workDir, { recursive: true, force: true });
        }
    }
}
