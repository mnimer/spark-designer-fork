/**
 * Offline pytest runner for generated PySpark repositories.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Runs the generated `tests/test_pipeline.py` suite against a local PySpark 4.x
 * test session. The generated repository keeps a pure `transform(dfs, config)`
 * function in `src/pipeline.py`, so the suite needs no cloud access.
 *
 * Architectural decisions:
 * - The runner never fails a test suite because local tooling is missing. If
 *   `python3`, `pyspark>=4`, `pytest`, or Java is absent, it returns a `skipped`
 *   result with a clear reason. If the host JVM cannot start a local
 *   SparkSession, the runner also skips, because the generated code is not at fault.
 * - All child processes start through `execFile` with argument arrays. No shell
 *   parses a command string.
 * - The child environment starts from a small allowlist. Caller variables pass
 *   through the shared `envSanitizer` denylist.
 * - The runner reads pass, fail, skip, and error counts from a JUnit XML report,
 *   not from console text.
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFile } from 'child_process';
import { applyRuntimeVariables, RuntimeVariable } from '../../security/envSanitizer';

/** Default wall-clock limit for one pytest run (5 minutes). */
export const DEFAULT_PYTEST_TIMEOUT_MS = 300_000;

/** Wall-clock limit for the tooling probe and venv creation. */
export const PROBE_TIMEOUT_MS = 60_000;

/** Lowest PySpark major version that the generated repositories target. */
export const MIN_PYSPARK_MAJOR = 4;

/** Maximum bytes of stdout or stderr kept from one child process. */
const MAX_OUTPUT_BYTES = 10 * 1024 * 1024;

/** Relative path of the generated test module. */
export const GENERATED_TEST_FILE = path.join('tests', 'test_pipeline.py');

/** Marker comment that identifies a conftest written by this runner. */
export const CONFTEST_MARKER = '# spark-designer: offline pytest session fixture';

/** Environment map passed to a child process. */
export type ChildEnv = Record<string, string | undefined>;

/** Overall outcome of one pytest run. */
export type PytestRunStatus = 'passed' | 'failed' | 'error' | 'skipped' | 'timeout';

/** Outcome of one JUnit test case. */
export type PytestCaseOutcome = 'passed' | 'failed' | 'error' | 'skipped';

/** One test case read from the JUnit XML report. */
export interface PytestCaseResult {
    /** Test function name, for example `test_transform_filters_nulls`. */
    name: string;
    /** Dotted class or module path, for example `tests.test_pipeline`. */
    classname: string;
    /** Case outcome. */
    outcome: PytestCaseOutcome;
    /** Failure, error, or skip message. Empty for passed cases. */
    message: string;
    /** Duration in seconds as reported by pytest. */
    timeSeconds: number;
}

/** Counts and cases parsed from one JUnit XML report. */
export interface JUnitSummary {
    passed: number;
    failed: number;
    skipped: number;
    errors: number;
    total: number;
    cases: PytestCaseResult[];
}

/** A failed or errored test with its message. */
export interface PytestFailure {
    /** Fully qualified test id, `classname::name`. */
    test: string;
    /** Whether the case failed an assertion or raised an error. */
    kind: 'failed' | 'error';
    /** First lines of the failure message. */
    message: string;
}

/** Typed result of {@link runGeneratedRepoTests}. */
export interface PytestRunResult {
    status: PytestRunStatus;
    /** Reason for a `skipped`, `error`, or `timeout` status. */
    reason?: string;
    passed: number;
    failed: number;
    skipped: number;
    errors: number;
    total: number;
    failures: PytestFailure[];
    cases: PytestCaseResult[];
    /** pytest exit code, or null if the process did not exit normally. */
    exitCode: number | null;
    durationMs: number;
    stdout: string;
    stderr: string;
    /** Python interpreter that ran the suite. */
    python?: string;
    /** Tooling versions found by the probe. */
    tooling?: ToolingProbeResult;
    /** How the session fixture was provided. */
    conftest?: ConftestAction;
}

/** Result of the Python tooling probe. */
export interface ToolingProbeResult {
    ok: boolean;
    /** Reason the tooling is not usable. Set only if `ok` is false. */
    reason?: string;
    pythonVersion?: string;
    pysparkVersion?: string;
    pytestVersion?: string;
    javaFound?: boolean;
    /** Whether a local SparkSession started and stopped. */
    sparkSessionOk?: boolean;
}

/** How {@link ensureConftest} provided the `spark` fixture. */
export type ConftestAction = 'verified' | 'written' | 'appended';

/** Options for {@link runGeneratedRepoTests}. */
export interface PytestRunOptions {
    /** Python interpreter to use. Defaults to `python3`. */
    python?: string;
    /** Virtual environment directory. If it holds an interpreter, the runner uses it. */
    venvDir?: string;
    /** If true and `venvDir` has no interpreter, create it with `--system-site-packages`. */
    createVenv?: boolean;
    /** Wall-clock limit for the pytest run. Defaults to {@link DEFAULT_PYTEST_TIMEOUT_MS}. */
    timeoutMs?: number;
    /** Extra environment variables. Denied keys are dropped. */
    extraEnv?: readonly RuntimeVariable[];
    /** Base environment to copy allowlisted keys from. Defaults to `process.env`. */
    baseEnv?: ChildEnv;
    /** Keep the JUnit report directory after the run. Defaults to false. */
    keepReport?: boolean;
}

/** Raw result of one child process. */
export interface ExecResult {
    exitCode: number | null;
    stdout: string;
    stderr: string;
    timedOut: boolean;
    /** Set if the process could not start, for example ENOENT. */
    spawnError?: string;
}

/** Environment keys copied from the parent process into the pytest child. */
const PASSTHROUGH_ENV_KEYS: readonly string[] = [
    'PATH',
    'HOME',
    'USER',
    'LANG',
    'LC_ALL',
    'TMPDIR',
    'TEMP',
    'TMP',
    'JAVA_HOME',
    'SYSTEMROOT',
];

/**
 * Python source for the tooling probe. It prints one JSON line and never raises.
 */
export const TOOLING_PROBE_SCRIPT = [
    'import json, os, platform, shutil',
    'r = {"python": platform.python_version()}',
    'try:',
    '    import pyspark',
    '    r["pyspark"] = pyspark.__version__',
    'except Exception as e:',
    '    r["pyspark_error"] = str(e)',
    'try:',
    '    import pytest',
    '    r["pytest"] = pytest.__version__',
    'except Exception as e:',
    '    r["pytest_error"] = str(e)',
    'jh = os.environ.get("JAVA_HOME", "")',
    'r["java"] = bool(shutil.which("java")) or bool(jh and os.path.exists(os.path.join(jh, "bin", "java")))',
    'print(json.dumps(r))',
].join('\n');

/**
 * Runs a child process with an argument array and a timeout.
 *
 * @param file Executable to run.
 * @param args Argument array. No shell parses these values.
 * @param options Working directory, environment, and timeout.
 * @param options.cwd Working directory of the child process.
 * @param options.env Environment of the child process.
 * @param options.timeoutMs Wall-clock limit. The child gets SIGKILL after this time.
 * @returns Exit code, captured output, and timeout state. Never rejects.
 */
export function execFileSafe(
    file: string,
    args: readonly string[],
    options: { cwd?: string; env?: ChildEnv; timeoutMs: number }
): Promise<ExecResult> {
    return new Promise((resolve) => {
        execFile(
            file,
            [...args],
            {
                cwd: options.cwd,
                env: options.env,
                timeout: options.timeoutMs,
                killSignal: 'SIGKILL',
                maxBuffer: MAX_OUTPUT_BYTES,
                windowsHide: true,
            },
            (error, stdout, stderr) => {
                const out = String(stdout ?? '');
                const err = String(stderr ?? '');
                if (!error) {
                    resolve({ exitCode: 0, stdout: out, stderr: err, timedOut: false });
                    return;
                }
                const e = error as Error & { killed?: boolean; signal?: string | null; code?: unknown };
                if (typeof e.code === 'string') {
                    resolve({ exitCode: null, stdout: out, stderr: err, timedOut: false, spawnError: `${e.code}: ${e.message}` });
                    return;
                }
                const timedOut = Boolean(e.killed) && e.signal === 'SIGKILL';
                resolve({
                    exitCode: typeof e.code === 'number' ? e.code : null,
                    stdout: out,
                    stderr: err,
                    timedOut,
                });
            }
        );
    });
}

/**
 * Builds the pytest argument array.
 *
 * @param junitXmlPath Absolute path of the JUnit XML report to write.
 * @param testPath Test file or directory relative to the repository root.
 * @returns Arguments for `python -m pytest`.
 */
export function buildPytestArgs(junitXmlPath: string, testPath: string = GENERATED_TEST_FILE): string[] {
    return [
        '-m',
        'pytest',
        '-q',
        '-p',
        'no:cacheprovider',
        '-o',
        'junit_family=xunit2',
        `--junitxml=${junitXmlPath}`,
        testPath,
    ];
}

/**
 * Builds the child environment for pytest.
 *
 * @param python Interpreter path that PySpark workers must use.
 * @param baseEnv Parent environment. Only allowlisted keys are copied.
 * @param extraEnv Caller variables. Keys denied by `envSanitizer` are dropped.
 * @returns The environment and the list of rejected keys.
 */
export function buildChildEnv(
    python: string,
    baseEnv: ChildEnv = process.env,
    extraEnv?: readonly RuntimeVariable[]
): { env: Record<string, string>; rejected: string[] } {
    const env: Record<string, string> = {};
    for (const key of PASSTHROUGH_ENV_KEYS) {
        const value = baseEnv[key];
        if (typeof value === 'string' && value.length > 0) {
            env[key] = value;
        }
    }
    const rejected = applyRuntimeVariables(env, extraEnv);
    // NOTE: These keys are set after caller variables so a caller cannot override them.
    env.PYSPARK_PYTHON = python;
    env.PYSPARK_DRIVER_PYTHON = python;
    env.SPARK_LOCAL_IP = '127.0.0.1';
    env.PYTHONDONTWRITEBYTECODE = '1';
    env.PYTHONUNBUFFERED = '1';
    env.PYTHONHASHSEED = '0';
    return { env, rejected };
}

/**
 * Python expression lines that build the local test SparkSession.
 * The conftest fixture and the session probe share these lines.
 */
const SESSION_BUILDER_LINES: readonly string[] = [
    'SparkSession.builder.master("local[2]")',
    '.appName("spark-designer-offline-tests")',
    '.config("spark.ui.enabled", "false")',
    '.config("spark.sql.shuffle.partitions", "2")',
    '.config("spark.default.parallelism", "2")',
    '.config("spark.driver.host", "127.0.0.1")',
    '.config("spark.driver.bindAddress", "127.0.0.1")',
    '.config("spark.sql.session.timeZone", "UTC")',
    '.getOrCreate()',
];

/** Text that the session probe prints if the session works. */
export const SPARK_SESSION_OK = 'SPARK_SESSION_OK';

/** Python source that starts a local SparkSession, runs one job, and stops it. */
export const SPARK_SESSION_PROBE_SCRIPT = [
    'from pyspark.sql import SparkSession',
    'session = (',
    ...SESSION_BUILDER_LINES.map((l) => `    ${l}`),
    ')',
    'assert session.range(2).count() == 2',
    'session.stop()',
    `print("${SPARK_SESSION_OK}")`,
].join('\n');

/**
 * Returns the conftest source that defines the session `spark` fixture.
 *
 * @returns Python source with a local, network-free SparkSession fixture.
 */
export function buildConftestSource(): string {
    return [
        CONFTEST_MARKER,
        'import os',
        'import sys',
        '',
        'import pytest',
        '',
        '_ROOT = os.path.dirname(os.path.abspath(__file__))',
        'if os.path.basename(_ROOT) == "tests":',
        '    _ROOT = os.path.dirname(_ROOT)',
        'for _p in (_ROOT, os.path.join(_ROOT, "src")):',
        '    if _p not in sys.path:',
        '        sys.path.insert(0, _p)',
        '',
        '',
        '@pytest.fixture(scope="session")',
        'def spark():',
        '    from pyspark.sql import SparkSession',
        '',
        '    session = (',
        ...SESSION_BUILDER_LINES.map((l) => `        ${l}`),
        '    )',
        '    yield session',
        '    session.stop()',
        '',
    ].join('\n');
}

/**
 * Returns true if the Python source defines a pytest fixture named `spark`.
 *
 * @param source Python source of a conftest module.
 * @returns Whether a `spark` fixture is defined.
 */
export function definesSparkFixture(source: string): boolean {
    return /@pytest\.fixture[^\n]*\n(?:\s*@[^\n]*\n)*\s*def\s+spark\s*\(/.test(source);
}

/**
 * Makes sure the repository provides a session `spark` fixture.
 *
 * If `tests/conftest.py` or the root `conftest.py` defines `spark`, the runner
 * keeps it. Otherwise it writes the first missing file. If both exist without
 * the fixture, it appends the fixture to the root `conftest.py`.
 *
 * @param repoDir Root of the generated repository.
 * @returns How the fixture was provided.
 */
export function ensureConftest(repoDir: string): ConftestAction {
    const candidates = [path.join(repoDir, 'tests', 'conftest.py'), path.join(repoDir, 'conftest.py')];
    for (const file of candidates) {
        if (fs.existsSync(file) && definesSparkFixture(fs.readFileSync(file, 'utf8'))) {
            return 'verified';
        }
    }
    const rootConftest = candidates[1];
    if (!fs.existsSync(rootConftest)) {
        fs.writeFileSync(rootConftest, buildConftestSource(), 'utf8');
        return 'written';
    }
    fs.appendFileSync(rootConftest, `\n\n${buildConftestSource()}`, 'utf8');
    return 'appended';
}

/**
 * Decodes the five predefined XML entities and numeric character references.
 *
 * @param value Raw attribute or text value.
 * @returns Decoded text.
 */
export function decodeXmlEntities(value: string): string {
    return value.replace(/&(#x[0-9a-fA-F]+|#[0-9]+|lt|gt|amp|quot|apos);/g, (match, entity: string) => {
        switch (entity) {
            case 'lt':
                return '<';
            case 'gt':
                return '>';
            case 'amp':
                return '&';
            case 'quot':
                return '"';
            case 'apos':
                return "'";
            default: {
                const code = entity.startsWith('#x') ? parseInt(entity.slice(2), 16) : parseInt(entity.slice(1), 10);
                return Number.isFinite(code) && code >= 0 && code <= 0x10ffff ? String.fromCodePoint(code) : match;
            }
        }
    });
}

/**
 * Reads XML attributes from the inside of a start tag.
 *
 * @param tagBody Text between the tag name and the closing `>`.
 * @returns Attribute map with decoded values.
 */
function parseAttributes(tagBody: string): Record<string, string> {
    const attrs: Record<string, string> = {};
    const pattern = /([A-Za-z_:][\w:.-]*)\s*=\s*("([^"]*)"|'([^']*)')/g;
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(tagBody)) !== null) {
        attrs[match[1]] = decodeXmlEntities(match[3] ?? match[4] ?? '');
    }
    return attrs;
}

/** Error thrown if a JUnit XML report cannot be parsed. */
export class JUnitParseError extends Error {
    /**
     * Creates a parse error.
     *
     * @param message Description of the problem.
     */
    constructor(message: string) {
        super(message);
        this.name = 'JUnitParseError';
    }
}

/**
 * Builds a short message from a `<failure>`, `<error>`, or `<skipped>` element.
 *
 * @param attrs Element attributes.
 * @param body Element text content.
 * @returns The `message` attribute, or the first body lines.
 */
function outcomeMessage(attrs: Record<string, string>, body: string): string {
    const fromAttr = (attrs.message ?? '').trim();
    if (fromAttr) {
        return fromAttr;
    }
    const text = decodeXmlEntities(body.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')).trim();
    return text.split('\n').slice(0, 20).join('\n');
}

/**
 * Parses a pytest JUnit XML report.
 *
 * @param xml Report text.
 * @returns Counts and per-case results.
 * @throws {JUnitParseError} If the text is not a JUnit report or a test case is unterminated.
 */
export function parseJUnitXml(xml: string): JUnitSummary {
    if (typeof xml !== 'string' || !/<testsuites?[\s>/]/.test(xml)) {
        throw new JUnitParseError('Report has no <testsuite> or <testsuites> element.');
    }
    const openSuites = (xml.match(/<testsuite[\s>]/g) ?? []).length;
    const closedSuites = (xml.match(/<\/testsuite>|<testsuite\b[^>]*\/>/g) ?? []).length;
    if (openSuites !== closedSuites) {
        throw new JUnitParseError('Report has an unterminated <testsuite> element.');
    }

    const cases: PytestCaseResult[] = [];
    const casePattern = /<testcase\b([^>]*?)(\/>|>([\s\S]*?)<\/testcase>)/g;
    let match: RegExpExecArray | null;
    let consumed = 0;
    while ((match = casePattern.exec(xml)) !== null) {
        consumed++;
        const attrs = parseAttributes(match[1]);
        const body = match[3] ?? '';
        let outcome: PytestCaseOutcome = 'passed';
        let message = '';
        const child = /<(failure|error|skipped)\b([^>]*?)(?:\/>|>([\s\S]*?)<\/\1>)/.exec(body);
        if (child) {
            outcome = child[1] === 'failure' ? 'failed' : (child[1] as PytestCaseOutcome);
            message = outcomeMessage(parseAttributes(child[2]), child[3] ?? '');
        }
        const time = Number.parseFloat(attrs.time ?? '0');
        cases.push({
            name: attrs.name ?? '',
            classname: attrs.classname ?? '',
            outcome,
            message,
            timeSeconds: Number.isFinite(time) ? time : 0,
        });
    }
    const declared = (xml.match(/<testcase\b/g) ?? []).length;
    if (declared !== consumed) {
        throw new JUnitParseError('Report has an unterminated <testcase> element.');
    }

    const count = (outcome: PytestCaseOutcome) => cases.filter((c) => c.outcome === outcome).length;
    return {
        passed: count('passed'),
        failed: count('failed'),
        skipped: count('skipped'),
        errors: count('error'),
        total: cases.length,
        cases,
    };
}

/**
 * Turns failed and errored cases into short failure records.
 *
 * @param cases Parsed test cases.
 * @returns One record per failed or errored case.
 */
export function collectFailures(cases: readonly PytestCaseResult[]): PytestFailure[] {
    return cases
        .filter((c) => c.outcome === 'failed' || c.outcome === 'error')
        .map((c) => ({
            test: c.classname ? `${c.classname}::${c.name}` : c.name,
            kind: c.outcome as 'failed' | 'error',
            message: c.message,
        }));
}

/**
 * Parses the JSON line from the tooling probe.
 *
 * @param stdout Probe output.
 * @returns Probe result with a skip reason if the tooling is not usable.
 */
export function interpretProbeOutput(stdout: string): ToolingProbeResult {
    const line = stdout.trim().split('\n').pop() ?? '';
    let data: Record<string, unknown>;
    try {
        data = JSON.parse(line) as Record<string, unknown>;
    } catch {
        return { ok: false, reason: 'Tooling probe returned no JSON output.' };
    }
    const result: ToolingProbeResult = {
        ok: false,
        pythonVersion: typeof data.python === 'string' ? data.python : undefined,
        pysparkVersion: typeof data.pyspark === 'string' ? data.pyspark : undefined,
        pytestVersion: typeof data.pytest === 'string' ? data.pytest : undefined,
        javaFound: data.java === true,
    };
    if (!result.pysparkVersion) {
        return { ...result, reason: `pyspark is not importable (${String(data.pyspark_error ?? 'unknown error')}).` };
    }
    const major = Number.parseInt(result.pysparkVersion.split('.')[0], 10);
    if (!Number.isFinite(major) || major < MIN_PYSPARK_MAJOR) {
        return { ...result, reason: `pyspark ${result.pysparkVersion} is older than ${MIN_PYSPARK_MAJOR}.0.` };
    }
    if (!result.pytestVersion) {
        return { ...result, reason: `pytest is not importable (${String(data.pytest_error ?? 'unknown error')}).` };
    }
    if (!result.javaFound) {
        return { ...result, reason: 'Java is not on PATH and JAVA_HOME is not set. PySpark needs a JVM.' };
    }
    return { ...result, ok: true };
}

/**
 * Checks that the interpreter can import `pyspark>=4` and `pytest`, and that Java exists.
 *
 * @param python Interpreter to probe.
 * @param env Child environment.
 * @returns Probe result. `ok` is false with a reason if tooling is missing.
 */
export async function probeTooling(python: string, env: ChildEnv): Promise<ToolingProbeResult> {
    const res = await execFileSafe(python, ['-c', TOOLING_PROBE_SCRIPT], { env, timeoutMs: PROBE_TIMEOUT_MS });
    if (res.spawnError) {
        return { ok: false, reason: `Python interpreter "${python}" is not available (${res.spawnError}).` };
    }
    if (res.timedOut) {
        return { ok: false, reason: `Tooling probe timed out after ${PROBE_TIMEOUT_MS} ms.` };
    }
    if (res.exitCode !== 0) {
        return { ok: false, reason: `Tooling probe exited with code ${res.exitCode}: ${res.stderr.trim()}` };
    }
    const tooling = interpretProbeOutput(res.stdout);
    if (!tooling.ok) {
        return tooling;
    }
    return { ...tooling, ...(await probeSparkSession(python, env)) };
}

/**
 * Starts and stops a local SparkSession with the fixture settings.
 *
 * A JVM that Spark does not support (for example Java 24+ with Spark 4.0) fails
 * here. The runner then skips, because the fault is in the host, not the generated code.
 *
 * @param python Interpreter to probe.
 * @param env Child environment.
 * @returns `ok` true if the session started, or `ok` false with a reason.
 */
export async function probeSparkSession(
    python: string,
    env: ChildEnv
): Promise<Pick<ToolingProbeResult, 'ok' | 'reason' | 'sparkSessionOk'>> {
    const res = await execFileSafe(python, ['-c', SPARK_SESSION_PROBE_SCRIPT], { env, timeoutMs: PROBE_TIMEOUT_MS });
    if (res.exitCode === 0 && res.stdout.includes(SPARK_SESSION_OK)) {
        return { ok: true, sparkSessionOk: true };
    }
    const detail = res.timedOut
        ? `timed out after ${PROBE_TIMEOUT_MS} ms`
        : res.spawnError ?? extractJavaError(`${res.stderr}\n${res.stdout}`);
    return { ok: false, sparkSessionOk: false, reason: `Local Spark session cannot start (${detail}).` };
}

/**
 * Picks the most useful error line from Spark or Python output.
 *
 * @param output Combined stderr and stdout.
 * @returns The first Java exception line, else the last non-empty line, at most 300 characters.
 */
export function extractJavaError(output: string): string {
    const lines = output.split('\n').map((l) => l.trim()).filter((l) => l.length > 0);
    const javaLine = lines.find((l) => /^:?\s*java\.[\w.$]+(Exception|Error)\b/.test(l));
    const picked = (javaLine ?? lines[lines.length - 1] ?? 'no output').replace(/^:\s*/, '');
    return picked.length > 300 ? `${picked.slice(0, 297)}...` : picked;
}

/**
 * Returns the interpreter path inside a virtual environment.
 *
 * @param venvDir Virtual environment directory.
 * @returns Path of the venv Python executable.
 */
export function venvPython(venvDir: string): string {
    return process.platform === 'win32'
        ? path.join(venvDir, 'Scripts', 'python.exe')
        : path.join(venvDir, 'bin', 'python');
}

/**
 * Resolves the interpreter, and reuses or creates a venv if requested.
 *
 * @param options Runner options.
 * @param env Child environment.
 * @returns Interpreter path, or a reason if the venv cannot be created.
 */
export async function resolvePython(
    options: PytestRunOptions,
    env: ChildEnv
): Promise<{ python: string; reason?: string }> {
    const base = options.python ?? 'python3';
    if (!options.venvDir) {
        return { python: base };
    }
    const venvPy = venvPython(options.venvDir);
    if (fs.existsSync(venvPy)) {
        return { python: venvPy };
    }
    if (!options.createVenv) {
        return { python: base };
    }
    // NOTE: The venv inherits system site packages so it works offline without pip.
    const res = await execFileSafe(base, ['-m', 'venv', '--system-site-packages', options.venvDir], {
        env,
        timeoutMs: PROBE_TIMEOUT_MS,
    });
    if (res.spawnError || res.exitCode !== 0 || !fs.existsSync(venvPy)) {
        const detail = res.spawnError ?? (res.stderr.trim() || `exit code ${res.exitCode}`);
        return { python: base, reason: `Could not create venv at ${options.venvDir} (${detail}).` };
    }
    return { python: venvPy };
}

/**
 * Builds an empty result with the given status.
 *
 * @param status Run status.
 * @param reason Explanation for the status.
 * @param startedAt Start time in epoch milliseconds.
 * @returns A result with zero counts.
 */
function emptyResult(status: PytestRunStatus, reason: string, startedAt: number): PytestRunResult {
    return {
        status,
        reason,
        passed: 0,
        failed: 0,
        skipped: 0,
        errors: 0,
        total: 0,
        failures: [],
        cases: [],
        exitCode: null,
        durationMs: Date.now() - startedAt,
        stdout: '',
        stderr: '',
    };
}

/**
 * Runs the generated `tests/test_pipeline.py` offline with a local PySpark session.
 *
 * @param repoDir Root of the generated repository.
 * @param options Interpreter, venv, timeout, and environment options.
 * @returns Typed run result. Missing tooling gives `skipped`. Never rejects for tooling problems.
 */
export async function runGeneratedRepoTests(repoDir: string, options: PytestRunOptions = {}): Promise<PytestRunResult> {
    const startedAt = Date.now();
    const repo = path.resolve(repoDir);
    if (!fs.existsSync(path.join(repo, GENERATED_TEST_FILE))) {
        return emptyResult('skipped', `Generated repository has no ${GENERATED_TEST_FILE}.`, startedAt);
    }

    const baseEnv = options.baseEnv ?? process.env;
    const probeEnv = buildChildEnv(options.python ?? 'python3', baseEnv, options.extraEnv).env;
    const resolved = await resolvePython(options, probeEnv);
    if (resolved.reason) {
        return { ...emptyResult('skipped', resolved.reason, startedAt), python: resolved.python };
    }
    const python = resolved.python;
    const { env } = buildChildEnv(python, baseEnv, options.extraEnv);

    const tooling = await probeTooling(python, env);
    if (!tooling.ok) {
        return { ...emptyResult('skipped', tooling.reason ?? 'Tooling is not available.', startedAt), python, tooling };
    }

    let conftest: ConftestAction;
    try {
        conftest = ensureConftest(repo);
    } catch (err) {
        return {
            ...emptyResult('error', `Could not provide the spark fixture: ${(err as Error).message}`, startedAt),
            python,
            tooling,
        };
    }
    const reportDir = fs.mkdtempSync(path.join(os.tmpdir(), 'spark-pytest-'));
    const junitXmlPath = path.join(reportDir, 'junit.xml');
    const timeoutMs = options.timeoutMs ?? DEFAULT_PYTEST_TIMEOUT_MS;

    try {
        const res = await execFileSafe(python, buildPytestArgs(junitXmlPath), { cwd: repo, env, timeoutMs });
        const base = {
            ...emptyResult('error', '', startedAt),
            exitCode: res.exitCode,
            stdout: res.stdout,
            stderr: res.stderr,
            python,
            tooling,
            conftest,
        };
        if (res.timedOut) {
            return { ...base, status: 'timeout', reason: `pytest exceeded the ${timeoutMs} ms timeout and was killed.` };
        }
        if (res.spawnError) {
            return { ...base, status: 'error', reason: `pytest could not start (${res.spawnError}).` };
        }
        if (!fs.existsSync(junitXmlPath)) {
            return { ...base, status: 'error', reason: `pytest exited with code ${res.exitCode} and wrote no JUnit report.` };
        }
        let summary: JUnitSummary;
        try {
            summary = parseJUnitXml(fs.readFileSync(junitXmlPath, 'utf8'));
        } catch (err) {
            return { ...base, status: 'error', reason: `JUnit report is malformed: ${(err as Error).message}` };
        }
        const failures = collectFailures(summary.cases);
        let status: PytestRunStatus;
        let reason: string | undefined;
        if (summary.failed > 0 || summary.errors > 0) {
            status = 'failed';
        } else if (res.exitCode === 0) {
            status = 'passed';
        } else if (res.exitCode === 5 || summary.total === 0) {
            status = 'error';
            reason = 'pytest collected no tests.';
        } else {
            status = 'error';
            reason = `pytest exited with code ${res.exitCode}.`;
        }
        return {
            ...base,
            status,
            reason,
            passed: summary.passed,
            failed: summary.failed,
            skipped: summary.skipped,
            errors: summary.errors,
            total: summary.total,
            failures,
            cases: summary.cases,
            durationMs: Date.now() - startedAt,
        };
    } finally {
        if (!options.keepReport) {
            fs.rmSync(reportDir, { recursive: true, force: true });
        }
    }
}

/**
 * Formats a run result as a short Markdown block for test reports.
 *
 * @param result Run result.
 * @returns Markdown text.
 */
export function formatPytestReport(result: PytestRunResult): string {
    const lines = [
        `### Offline pytest: ${result.status.toUpperCase()}`,
        '',
        `- Passed: ${result.passed}, Failed: ${result.failed}, Errors: ${result.errors}, Skipped: ${result.skipped}`,
    ];
    if (result.reason) {
        lines.push(`- Reason: ${result.reason}`);
    }
    for (const failure of result.failures) {
        const firstLine = failure.message.split('\n')[0] ?? '';
        lines.push(`- ${failure.kind.toUpperCase()} \`${failure.test}\`: ${firstLine}`);
    }
    return lines.join('\n');
}
