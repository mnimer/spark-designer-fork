/**
 * Unit tests for the offline pytest runner.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies JUnit XML parsing, the tooling-missing skip path, the timeout path,
 * argument-array construction, conftest handling, and child environment rules.
 * All child processes and file system calls are mocked.
 */

import { beforeEach, describe, expect, it, vi } from 'vitest';

const { execFileMock, fsMock } = vi.hoisted(() => ({
    execFileMock: vi.fn(),
    fsMock: {
        existsSync: vi.fn(),
        readFileSync: vi.fn(),
        writeFileSync: vi.fn(),
        appendFileSync: vi.fn(),
        mkdtempSync: vi.fn(),
        rmSync: vi.fn(),
    },
}));

vi.mock('child_process', () => ({ execFile: execFileMock }));
vi.mock('fs', () => fsMock);

import * as path from 'path';
import {
    buildChildEnv,
    buildConftestSource,
    buildPytestArgs,
    collectFailures,
    CONFTEST_MARKER,
    decodeXmlEntities,
    definesSparkFixture,
    ensureConftest,
    execFileSafe,
    extractJavaError,
    formatPytestReport,
    GENERATED_TEST_FILE,
    interpretProbeOutput,
    JUnitParseError,
    parseJUnitXml,
    probeSparkSession,
    probeTooling,
    resolvePython,
    runGeneratedRepoTests,
    SPARK_SESSION_OK,
    SPARK_SESSION_PROBE_SCRIPT,
    venvPython,
} from '../integration/pytest-runner';

type ExecCallback = (error: unknown, stdout: string, stderr: string) => void;

/** Scripted reply for one execFile call. */
interface ExecReply {
    error?: unknown;
    stdout?: string;
    stderr?: string;
}

/** Queues execFile replies in call order. */
function queueExec(...replies: ExecReply[]): void {
    for (const reply of replies) {
        execFileMock.mockImplementationOnce((_file: string, _args: string[], _opts: unknown, cb: ExecCallback) => {
            cb(reply.error ?? null, reply.stdout ?? '', reply.stderr ?? '');
        });
    }
}

const PROBE_OK = JSON.stringify({ python: '3.12.1', pyspark: '4.0.1', pytest: '8.3.2', java: true });
const SESSION_OK: ExecReply = { stdout: `${SPARK_SESSION_OK}\n` };

const XML_PASS = `<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest" errors="0" failures="0" skipped="0" tests="2">
<testcase classname="tests.test_pipeline" name="test_a" time="0.51" />
<testcase classname="tests.test_pipeline" name="test_b" time="0.20"></testcase>
</testsuite></testsuites>`;

const XML_MIXED = `<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest" tests="4">
<testcase classname="tests.test_pipeline" name="test_ok" time="0.1"/>
<testcase classname="tests.test_pipeline" name="test_fail" time="0.2"><failure message="assert 3 == 2&#10;where 3 = count()">Traceback &lt;long&gt;</failure></testcase>
<testcase classname="tests.test_pipeline" name="test_err" time="0.3"><error message="failed on setup with &quot;Py4JError&quot;">details</error></testcase>
<testcase classname="tests.test_pipeline" name="test_skip" time="0"><skipped type="pytest.skip" message="needs delta" /></testcase>
</testsuite></testsuites>`;

const REPO = '/tmp/gen-repo';
const REPORT_DIR = '/tmp/spark-pytest-abc';
const JUNIT = path.join(REPORT_DIR, 'junit.xml');

beforeEach(() => {
    execFileMock.mockReset();
    for (const fn of Object.values(fsMock)) {
        fn.mockReset();
    }
    fsMock.mkdtempSync.mockReturnValue(REPORT_DIR);
});

describe('parseJUnitXml', () => {
    it('counts passing cases, including self-closing test cases', () => {
        const summary = parseJUnitXml(XML_PASS);
        expect(summary).toMatchObject({ passed: 2, failed: 0, skipped: 0, errors: 0, total: 2 });
        expect(summary.cases[0]).toEqual({
            name: 'test_a',
            classname: 'tests.test_pipeline',
            outcome: 'passed',
            message: '',
            timeSeconds: 0.51,
        });
    });

    it('classifies failure, error, and skip children and decodes messages', () => {
        const summary = parseJUnitXml(XML_MIXED);
        expect(summary).toMatchObject({ passed: 1, failed: 1, errors: 1, skipped: 1, total: 4 });
        const byName = Object.fromEntries(summary.cases.map((c) => [c.name, c]));
        expect(byName.test_fail.message).toBe('assert 3 == 2\nwhere 3 = count()');
        expect(byName.test_err.message).toBe('failed on setup with "Py4JError"');
        expect(byName.test_skip.outcome).toBe('skipped');
        expect(byName.test_skip.message).toBe('needs delta');
    });

    it('uses the element body if the message attribute is absent', () => {
        const xml = '<testsuite><testcase classname="c" name="n"><failure><![CDATA[boom <x>]]></failure></testcase></testsuite>';
        expect(parseJUnitXml(xml).cases[0]).toMatchObject({ outcome: 'failed', message: 'boom <x>' });
    });

    it('accepts an empty suite', () => {
        expect(parseJUnitXml('<testsuite name="pytest" tests="0"/>')).toMatchObject({ total: 0 });
    });

    it('gives zero time for a non-numeric time attribute', () => {
        const xml = '<testsuite><testcase name="t" time="abc"/></testsuite>';
        expect(parseJUnitXml(xml).cases[0].timeSeconds).toBe(0);
    });

    it.each([
        ['empty text', ''],
        ['non-JUnit XML', '<html><body>nope</body></html>'],
        ['unterminated test case', '<testsuite><testcase name="a"><failure>x</failure></testsuite>'],
        ['unterminated suite', '<testsuites><testsuite><testcase name="a"/>'],
    ])('throws JUnitParseError for %s', (_label, xml) => {
        expect(() => parseJUnitXml(xml)).toThrow(JUnitParseError);
    });

    it('throws JUnitParseError for a non-string value', () => {
        expect(() => parseJUnitXml(undefined as unknown as string)).toThrow(JUnitParseError);
    });
});

describe('decodeXmlEntities', () => {
    it('decodes named and numeric entities', () => {
        expect(decodeXmlEntities('&lt;a&gt; &amp; &quot;b&quot; &apos;c&apos; &#65;&#x42;')).toBe('<a> & "b" \'c\' AB');
    });

    it('keeps an out-of-range code point unchanged', () => {
        expect(decodeXmlEntities('&#x110000;')).toBe('&#x110000;');
    });
});

describe('collectFailures', () => {
    it('returns only failed and errored cases with qualified ids', () => {
        const failures = collectFailures(parseJUnitXml(XML_MIXED).cases);
        expect(failures.map((f) => [f.test, f.kind])).toEqual([
            ['tests.test_pipeline::test_fail', 'failed'],
            ['tests.test_pipeline::test_err', 'error'],
        ]);
    });

    it('omits the class prefix if classname is empty', () => {
        expect(collectFailures([{ name: 't', classname: '', outcome: 'error', message: 'm', timeSeconds: 0 }])[0].test).toBe('t');
    });
});

describe('buildPytestArgs', () => {
    it('builds an argument array with the JUnit path as one element', () => {
        const junit = '/tmp/dir with space/$(rm -rf)/junit.xml';
        const args = buildPytestArgs(junit);
        expect(args).toEqual([
            '-m',
            'pytest',
            '-q',
            '-p',
            'no:cacheprovider',
            '-o',
            'junit_family=xunit2',
            `--junitxml=${junit}`,
            GENERATED_TEST_FILE,
        ]);
    });

    it('accepts a custom test path', () => {
        expect(buildPytestArgs('/r.xml', 'tests').at(-1)).toBe('tests');
    });
});

describe('buildChildEnv', () => {
    it('copies only allowlisted keys and pins PySpark settings', () => {
        const { env, rejected } = buildChildEnv(
            '/venv/bin/python',
            { PATH: '/usr/bin', HOME: '/home/u', SECRET_TOKEN: 'x', GOOGLE_APPLICATION_CREDENTIALS: '/k.json' },
            [
                { key: 'MY_FLAG', value: '1' },
                { key: 'LD_PRELOAD', value: '/evil.so' },
                { key: 'PYSPARK_PYTHON', value: '/evil' },
            ]
        );
        expect(env.PATH).toBe('/usr/bin');
        expect(env.HOME).toBe('/home/u');
        expect(env.MY_FLAG).toBe('1');
        expect(env.SECRET_TOKEN).toBeUndefined();
        expect(env.GOOGLE_APPLICATION_CREDENTIALS).toBeUndefined();
        expect(env.LD_PRELOAD).toBeUndefined();
        expect(env.PYSPARK_PYTHON).toBe('/venv/bin/python');
        expect(env.PYSPARK_DRIVER_PYTHON).toBe('/venv/bin/python');
        expect(env.SPARK_LOCAL_IP).toBe('127.0.0.1');
        expect(rejected).toEqual(['LD_PRELOAD', 'PYSPARK_PYTHON']);
    });

    it('skips empty parent values', () => {
        expect(buildChildEnv('python3', { PATH: '' }).env.PATH).toBeUndefined();
    });
});

describe('conftest handling', () => {
    it('builds a local, UI-free session fixture with two shuffle partitions', () => {
        const src = buildConftestSource();
        expect(src.startsWith(CONFTEST_MARKER)).toBe(true);
        expect(src).toContain('SparkSession.builder.master("local[2]")');
        expect(src).toContain('.config("spark.ui.enabled", "false")');
        expect(src).toContain('.config("spark.sql.shuffle.partitions", "2")');
        expect(src).toContain('@pytest.fixture(scope="session")');
        expect(definesSparkFixture(src)).toBe(true);
    });

    it('detects a spark fixture with stacked decorators', () => {
        expect(definesSparkFixture('@pytest.fixture(scope="session")\n@other\ndef spark(request):\n    pass')).toBe(true);
        expect(definesSparkFixture('def spark():\n    pass')).toBe(false);
        expect(definesSparkFixture('@pytest.fixture\ndef sparkle():\n    pass')).toBe(false);
    });

    it('verifies an existing tests conftest that defines spark', () => {
        fsMock.existsSync.mockImplementation((p: string) => p === path.join(REPO, 'tests', 'conftest.py'));
        fsMock.readFileSync.mockReturnValue('import pytest\n@pytest.fixture(scope="session")\ndef spark():\n    yield 1\n');
        expect(ensureConftest(REPO)).toBe('verified');
        expect(fsMock.writeFileSync).not.toHaveBeenCalled();
        expect(fsMock.appendFileSync).not.toHaveBeenCalled();
    });

    it('writes a root conftest if none exists', () => {
        fsMock.existsSync.mockReturnValue(false);
        expect(ensureConftest(REPO)).toBe('written');
        expect(fsMock.writeFileSync).toHaveBeenCalledWith(path.join(REPO, 'conftest.py'), buildConftestSource(), 'utf8');
    });

    it('appends the fixture if the root conftest lacks it', () => {
        fsMock.existsSync.mockImplementation((p: string) => p === path.join(REPO, 'conftest.py'));
        fsMock.readFileSync.mockReturnValue('import pytest\n');
        expect(ensureConftest(REPO)).toBe('appended');
        expect(fsMock.appendFileSync).toHaveBeenCalledWith(
            path.join(REPO, 'conftest.py'),
            expect.stringContaining(CONFTEST_MARKER),
            'utf8'
        );
    });
});

describe('interpretProbeOutput', () => {
    it('accepts pyspark 4, pytest, and Java', () => {
        expect(interpretProbeOutput(`noise\n${PROBE_OK}\n`)).toMatchObject({ ok: true, pysparkVersion: '4.0.1' });
    });

    it.each([
        ['missing pyspark', { python: '3.12', pytest: '8', java: true, pyspark_error: "No module named 'pyspark'" }, /pyspark is not importable/],
        ['old pyspark', { python: '3.12', pyspark: '3.5.1', pytest: '8', java: true }, /older than 4\.0/],
        ['missing pytest', { python: '3.12', pyspark: '4.0.0', java: true, pytest_error: 'nope' }, /pytest is not importable/],
        ['missing Java', { python: '3.12', pyspark: '4.0.0', pytest: '8', java: false }, /Java is not on PATH/],
    ])('rejects %s', (_label, data, reason) => {
        const res = interpretProbeOutput(JSON.stringify(data));
        expect(res.ok).toBe(false);
        expect(res.reason).toMatch(reason);
    });

    it('rejects non-JSON output', () => {
        expect(interpretProbeOutput('garbage')).toMatchObject({ ok: false, reason: expect.stringMatching(/no JSON/) });
    });
});

describe('execFileSafe', () => {
    it('passes the argument array, timeout, and SIGKILL to execFile', async () => {
        queueExec({ stdout: 'ok' });
        const res = await execFileSafe('python3', ['-c', 'print(1); $(x)'], { cwd: '/w', env: { A: '1' }, timeoutMs: 1234 });
        expect(res).toEqual({ exitCode: 0, stdout: 'ok', stderr: '', timedOut: false });
        const [file, args, opts] = execFileMock.mock.calls[0];
        expect(file).toBe('python3');
        expect(args).toEqual(['-c', 'print(1); $(x)']);
        expect(opts).toMatchObject({ cwd: '/w', env: { A: '1' }, timeout: 1234, killSignal: 'SIGKILL' });
        expect(opts.shell).toBeUndefined();
    });

    it('reports a numeric exit code', async () => {
        queueExec({ error: Object.assign(new Error('exit'), { code: 1 }), stdout: 'F' });
        expect(await execFileSafe('x', [], { timeoutMs: 1 })).toMatchObject({ exitCode: 1, timedOut: false });
    });

    it('reports a timeout if the child is killed with SIGKILL', async () => {
        queueExec({ error: Object.assign(new Error('killed'), { killed: true, signal: 'SIGKILL', code: null }) });
        expect(await execFileSafe('x', [], { timeoutMs: 1 })).toMatchObject({ exitCode: null, timedOut: true });
    });

    it('reports a spawn error for ENOENT', async () => {
        queueExec({ error: Object.assign(new Error('spawn python3 ENOENT'), { code: 'ENOENT' }) });
        const res = await execFileSafe('python3', [], { timeoutMs: 1 });
        expect(res.spawnError).toMatch(/^ENOENT/);
    });
});

describe('probeTooling', () => {
    it('skips if python3 is missing', async () => {
        queueExec({ error: Object.assign(new Error('ENOENT'), { code: 'ENOENT' }) });
        const res = await probeTooling('python3', {});
        expect(res.ok).toBe(false);
        expect(res.reason).toMatch(/Python interpreter "python3" is not available/);
    });

    it('reports a probe timeout', async () => {
        queueExec({ error: Object.assign(new Error('k'), { killed: true, signal: 'SIGKILL' }) });
        expect((await probeTooling('python3', {})).reason).toMatch(/timed out/);
    });

    it('reports a non-zero probe exit', async () => {
        queueExec({ error: Object.assign(new Error('x'), { code: 2 }), stderr: 'bad' });
        expect((await probeTooling('python3', {})).reason).toMatch(/exited with code 2: bad/);
    });

    it('runs the session probe after the import probe passes', async () => {
        queueExec({ stdout: PROBE_OK }, SESSION_OK);
        const res = await probeTooling('python3', {});
        expect(res).toMatchObject({ ok: true, sparkSessionOk: true, pysparkVersion: '4.0.1' });
        expect(execFileMock.mock.calls[1][1]).toEqual(['-c', SPARK_SESSION_PROBE_SCRIPT]);
    });

    it('does not run the session probe if imports fail', async () => {
        queueExec({ stdout: JSON.stringify({ python: '3', pytest: '8', java: true }) });
        expect((await probeTooling('python3', {})).ok).toBe(false);
        expect(execFileMock).toHaveBeenCalledTimes(1);
    });
});

describe('probeSparkSession', () => {
    it('uses the same builder settings as the conftest fixture', () => {
        expect(SPARK_SESSION_PROBE_SCRIPT).toContain('SparkSession.builder.master("local[2]")');
        expect(SPARK_SESSION_PROBE_SCRIPT).toContain('.config("spark.ui.enabled", "false")');
        expect(SPARK_SESSION_PROBE_SCRIPT).toContain('.config("spark.sql.shuffle.partitions", "2")');
    });

    it('skips with the Java error if the JVM is unsupported', async () => {
        queueExec({
            error: Object.assign(new Error('x'), { code: 1 }),
            stderr: 'Traceback\npy4j.protocol.Py4JJavaError: An error occurred\n: java.lang.UnsupportedOperationException: getSubject is not supported\n\tat x',
        });
        const res = await probeSparkSession('python3', {});
        expect(res).toEqual({
            ok: false,
            sparkSessionOk: false,
            reason: 'Local Spark session cannot start (java.lang.UnsupportedOperationException: getSubject is not supported).',
        });
    });

    it('reports a session probe timeout', async () => {
        queueExec({ error: Object.assign(new Error('k'), { killed: true, signal: 'SIGKILL' }) });
        expect((await probeSparkSession('python3', {})).reason).toMatch(/timed out/);
    });

    it('fails if exit is zero but the OK marker is absent', async () => {
        queueExec({ stdout: 'something else' });
        expect((await probeSparkSession('python3', {})).reason).toMatch(/something else/);
    });
});

describe('extractJavaError', () => {
    it('falls back to the last non-empty line', () => {
        expect(extractJavaError('a\nModuleNotFoundError: x\n\n')).toBe('ModuleNotFoundError: x');
    });

    it('reports no output for empty text', () => {
        expect(extractJavaError('')).toBe('no output');
    });

    it('truncates long lines to 300 characters', () => {
        expect(extractJavaError('x'.repeat(400))).toHaveLength(300);
    });
});

describe('resolvePython', () => {
    it('uses the base interpreter without a venv', async () => {
        expect(await resolvePython({ python: '/usr/bin/python3' }, {})).toEqual({ python: '/usr/bin/python3' });
    });

    it('reuses an existing venv interpreter', async () => {
        fsMock.existsSync.mockReturnValue(true);
        expect(await resolvePython({ venvDir: '/v' }, {})).toEqual({ python: venvPython('/v') });
        expect(execFileMock).not.toHaveBeenCalled();
    });

    it('falls back to the base interpreter if the venv is absent and creation is off', async () => {
        fsMock.existsSync.mockReturnValue(false);
        expect(await resolvePython({ venvDir: '/v' }, {})).toEqual({ python: 'python3' });
    });

    it('creates a venv with system site packages', async () => {
        fsMock.existsSync.mockReturnValueOnce(false).mockReturnValueOnce(true);
        queueExec({});
        expect(await resolvePython({ venvDir: '/v', createVenv: true }, {})).toEqual({ python: venvPython('/v') });
        expect(execFileMock.mock.calls[0][1]).toEqual(['-m', 'venv', '--system-site-packages', '/v']);
    });

    it('returns a reason if venv creation fails', async () => {
        fsMock.existsSync.mockReturnValue(false);
        queueExec({ error: Object.assign(new Error('x'), { code: 1 }), stderr: 'ensurepip missing' });
        const res = await resolvePython({ venvDir: '/v', createVenv: true }, {});
        expect(res.reason).toMatch(/Could not create venv at \/v \(ensurepip missing\)/);
    });
});

describe('runGeneratedRepoTests', () => {
    /** Makes the test file and an existing spark conftest visible, and maps the JUnit path to `xml`. */
    function stubRepo(xml: string | null): void {
        fsMock.existsSync.mockImplementation((p: string) => {
            if (p === JUNIT) {
                return xml !== null;
            }
            return p === path.join(REPO, GENERATED_TEST_FILE) || p === path.join(REPO, 'tests', 'conftest.py');
        });
        fsMock.readFileSync.mockImplementation((p: string) =>
            p === JUNIT ? xml : '@pytest.fixture(scope="session")\ndef spark():\n    pass\n'
        );
    }

    it('skips if the generated test file is absent', async () => {
        fsMock.existsSync.mockReturnValue(false);
        const res = await runGeneratedRepoTests(REPO);
        expect(res.status).toBe('skipped');
        expect(res.reason).toMatch(/has no tests/);
        expect(execFileMock).not.toHaveBeenCalled();
    });

    it('skips with a clear reason if pyspark is missing and never runs pytest', async () => {
        stubRepo(XML_PASS);
        queueExec({ stdout: JSON.stringify({ python: '3.12', pytest: '8', java: true, pyspark_error: 'No module' }) });
        const res = await runGeneratedRepoTests(REPO);
        expect(res.status).toBe('skipped');
        expect(res.reason).toMatch(/pyspark is not importable/);
        expect(execFileMock).toHaveBeenCalledTimes(1);
        expect(fsMock.mkdtempSync).not.toHaveBeenCalled();
    });

    it('skips if the local Spark session cannot start and never writes a conftest', async () => {
        stubRepo(XML_PASS);
        queueExec({ stdout: PROBE_OK }, { error: Object.assign(new Error('x'), { code: 1 }), stderr: ': java.lang.Error: bad jvm' });
        const res = await runGeneratedRepoTests(REPO);
        expect(res).toMatchObject({ status: 'skipped', reason: 'Local Spark session cannot start (java.lang.Error: bad jvm).' });
        expect(execFileMock).toHaveBeenCalledTimes(2);
        expect(fsMock.writeFileSync).not.toHaveBeenCalled();
    });

    it('skips if venv creation fails', async () => {
        stubRepo(XML_PASS);
        queueExec({ error: Object.assign(new Error('x'), { code: 1 }), stderr: 'no venv' });
        const res = await runGeneratedRepoTests(REPO, { venvDir: '/v', createVenv: true });
        expect(res.status).toBe('skipped');
        expect(res.reason).toMatch(/Could not create venv/);
    });

    it('returns passed with counts and runs pytest with an argument array in the repo', async () => {
        stubRepo(XML_PASS);
        queueExec({ stdout: PROBE_OK }, SESSION_OK, { stdout: '2 passed' });
        const res = await runGeneratedRepoTests(REPO, { python: '/py', timeoutMs: 5000, baseEnv: { PATH: '/bin' } });
        expect(res).toMatchObject({ status: 'passed', passed: 2, total: 2, exitCode: 0, conftest: 'verified', python: '/py' });
        const [file, args, opts] = execFileMock.mock.calls[2];
        expect(file).toBe('/py');
        expect(args).toEqual(buildPytestArgs(JUNIT));
        expect(opts).toMatchObject({ cwd: path.resolve(REPO), timeout: 5000, killSignal: 'SIGKILL' });
        expect(opts.env).toMatchObject({ PATH: '/bin', PYSPARK_PYTHON: '/py' });
        expect(fsMock.rmSync).toHaveBeenCalledWith(REPORT_DIR, { recursive: true, force: true });
    });

    it('returns failed with failing test names and messages', async () => {
        stubRepo(XML_MIXED);
        queueExec({ stdout: PROBE_OK }, SESSION_OK, { error: Object.assign(new Error('exit 1'), { code: 1 }) });
        const res = await runGeneratedRepoTests(REPO);
        expect(res).toMatchObject({ status: 'failed', passed: 1, failed: 1, errors: 1, skipped: 1, exitCode: 1 });
        expect(res.failures.map((f) => f.test)).toEqual(['tests.test_pipeline::test_fail', 'tests.test_pipeline::test_err']);
        expect(formatPytestReport(res)).toContain('FAILED `tests.test_pipeline::test_fail`: assert 3 == 2');
    });

    it('returns timeout if pytest exceeds the limit', async () => {
        stubRepo(null);
        queueExec({ stdout: PROBE_OK }, SESSION_OK, { error: Object.assign(new Error('killed'), { killed: true, signal: 'SIGKILL' }) });
        const res = await runGeneratedRepoTests(REPO, { timeoutMs: 10 });
        expect(res.status).toBe('timeout');
        expect(res.reason).toMatch(/10 ms timeout/);
        expect(fsMock.rmSync).toHaveBeenCalled();
    });

    it('returns error if pytest writes no report', async () => {
        stubRepo(null);
        queueExec({ stdout: PROBE_OK }, SESSION_OK, { error: Object.assign(new Error('x'), { code: 4 }) });
        const res = await runGeneratedRepoTests(REPO);
        expect(res).toMatchObject({ status: 'error', exitCode: 4 });
        expect(res.reason).toMatch(/wrote no JUnit report/);
    });

    it('returns error for a malformed report', async () => {
        stubRepo('<not-junit/>');
        queueExec({ stdout: PROBE_OK }, SESSION_OK, {});
        const res = await runGeneratedRepoTests(REPO);
        expect(res.status).toBe('error');
        expect(res.reason).toMatch(/malformed/);
    });

    it('returns error if pytest collects no tests', async () => {
        stubRepo('<testsuite tests="0"/>');
        queueExec({ stdout: PROBE_OK }, SESSION_OK, { error: Object.assign(new Error('x'), { code: 5 }) });
        const res = await runGeneratedRepoTests(REPO);
        expect(res).toMatchObject({ status: 'error', reason: 'pytest collected no tests.' });
    });

    it('returns error for a non-zero exit with only passing cases', async () => {
        stubRepo(XML_PASS);
        queueExec({ stdout: PROBE_OK }, SESSION_OK, { error: Object.assign(new Error('x'), { code: 3 }) });
        expect(await runGeneratedRepoTests(REPO)).toMatchObject({ status: 'error', reason: 'pytest exited with code 3.' });
    });

    it('keeps the report directory if keepReport is set', async () => {
        stubRepo(XML_PASS);
        queueExec({ stdout: PROBE_OK }, SESSION_OK, {});
        await runGeneratedRepoTests(REPO, { keepReport: true });
        expect(fsMock.rmSync).not.toHaveBeenCalled();
    });

    it('returns error and never runs pytest if the conftest write fails', async () => {
        fsMock.existsSync.mockImplementation((p: string) => p === path.join(REPO, GENERATED_TEST_FILE));
        fsMock.writeFileSync.mockImplementation(() => {
            throw new Error('EACCES: permission denied');
        });
        queueExec({ stdout: PROBE_OK }, SESSION_OK);
        const res = await runGeneratedRepoTests(REPO);
        expect(res.status).toBe('error');
        expect(res.reason).toMatch(/Could not provide the spark fixture: EACCES/);
        expect(execFileMock).toHaveBeenCalledTimes(2);
        expect(fsMock.mkdtempSync).not.toHaveBeenCalled();
    });
});

describe('formatPytestReport', () => {
    it('includes the reason for a skipped run', () => {
        const text = formatPytestReport({
            status: 'skipped',
            reason: 'pyspark missing',
            passed: 0,
            failed: 0,
            skipped: 0,
            errors: 0,
            total: 0,
            failures: [],
            cases: [],
            exitCode: null,
            durationMs: 0,
            stdout: '',
            stderr: '',
        });
        expect(text).toContain('Offline pytest: SKIPPED');
        expect(text).toContain('- Reason: pyspark missing');
    });
});
