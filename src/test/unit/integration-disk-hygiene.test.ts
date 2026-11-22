/**
 * Unit tests for integration test cache reuse and per-run disk hygiene (CLN11).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies shared cache root resolution, Temurin JDK 21 auto-detection when host Java
 * exceeds JDK 21, shared virtualenv and Ivy cache wiring in `pytest-runner.ts` and
 * `global-setup.ts`, and unconditional `finally` deletion of per-run `.venv`,
 * `.spark-ivy`, `.pytest_cache`, JUnit report folders, and XDG scratch directories
 * without touching `<cacheRoot>`.
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { afterEach, describe, expect, it } from 'vitest';
import {
    PER_RUN_REPO_ARTIFACTS,
    SHARED_CACHE_ENV_VAR,
    SPARK_IVY_CACHE_ENV_VAR,
    cleanupPerRunRepoArtifacts,
    detectHostJavaMajorFromPath,
    ensureSharedTestCacheDirs,
    findCachedJdk21Home,
    getSharedTestCachePaths,
    parseJavaMajorVersion,
    readJdkMajorVersion,
    resolveSharedCacheRoot,
    resolveSparkJavaHome,
} from '../integration/sharedTestCache';
import {
    SPARK_SESSION_OK,
    buildChildEnv,
    buildConftestSource,
    resolvePython,
    runGeneratedRepoTests,
    venvPython,
} from '../integration/pytest-runner';
import { bootstrapIntegrationTestCache } from '../integration/global-setup';

const tempRoots: string[] = [];

/**
 * Allocates a fresh temporary directory in `os.tmpdir()` registered for `afterEach` removal.
 *
 * @param prefix Prefix for the temporary directory name.
 * @returns Absolute path of the created temporary directory.
 */
function makeTempDir(prefix: string): string {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
    tempRoots.push(dir);
    return dir;
}

/**
 * Creates a fake JDK home directory with an executable `bin/java` and an optional `release` file.
 *
 * @param homeDir Target `JAVA_HOME` directory.
 * @param javaVersion Optional `JAVA_VERSION` string written into `<homeDir>/release`.
 * @returns Absolute path of `homeDir`.
 */
function createFakeJdk(homeDir: string, javaVersion?: string): string {
    const binDir = path.join(homeDir, 'bin');
    fs.mkdirSync(binDir, { recursive: true });
    const javaBin = path.join(binDir, process.platform === 'win32' ? 'java.exe' : 'java');
    fs.writeFileSync(javaBin, '#!/usr/bin/env sh\nexit 0\n', { mode: 0o755 });
    if (javaVersion) {
        fs.writeFileSync(path.join(homeDir, 'release'), `JAVA_VERSION="${javaVersion}"\n`, 'utf8');
    }
    return homeDir;
}

/**
 * Creates a generated repository fixture with `src/pipeline.py` and `tests/test_pipeline.py`.
 *
 * @param root Parent temp directory.
 * @returns Absolute path of the repository directory.
 */
function createGeneratedRepoFixture(root: string): string {
    const repoDir = path.join(root, 'generated-repo');
    fs.mkdirSync(path.join(repoDir, 'src'), { recursive: true });
    fs.mkdirSync(path.join(repoDir, 'tests'), { recursive: true });
    fs.writeFileSync(path.join(repoDir, 'src', 'pipeline.py'), 'def transform(dfs, config):\n    return dfs\n', 'utf8');
    fs.writeFileSync(path.join(repoDir, 'tests', 'test_pipeline.py'), 'def test_ok(spark):\n    assert True\n', 'utf8');
    return repoDir;
}

/**
 * Writes an executable fake Python script that simulates venv creation, tooling probes,
 * Spark session probes, and pytest runs that emit per-run scratch files.
 *
 * @param binDir Directory where the script is placed.
 * @param mode Outcome simulated during the pytest step (`pass`, `fail`, or `hang`).
 * @param envLogFile File path where observed pytest environment variables are recorded as JSON.
 * @returns Absolute path to the executable fake Python binary.
 */
function createFakePythonExecutable(
    binDir: string,
    mode: 'pass' | 'fail' | 'hang',
    envLogFile: string
): string {
    fs.mkdirSync(binDir, { recursive: true });
    const driverJs = path.join(binDir, 'fake-python-driver.js');
    const wrapperPath = path.join(binDir, process.platform === 'win32' ? 'fake-python.cmd' : 'fake-python');

    const driverSource = [
        'const fs = require("fs");',
        'const path = require("path");',
        'const args = process.argv.slice(2);',
        `const mode = ${JSON.stringify(mode)};`,
        `const envLogFile = ${JSON.stringify(envLogFile)};`,
        'if (args[0] === "-m" && args[1] === "venv") {',
        '  const targetVenv = args[args.length - 1];',
        '  const subDir = process.platform === "win32" ? "Scripts" : "bin";',
        '  const exeName = process.platform === "win32" ? "python.exe" : "python";',
        '  const destDir = path.join(targetVenv, subDir);',
        '  fs.mkdirSync(destDir, { recursive: true });',
        '  const destBin = path.join(destDir, exeName);',
        `  fs.copyFileSync(${JSON.stringify(wrapperPath)}, destBin);`,
        '  fs.chmodSync(destBin, 0o755);',
        '  fs.writeFileSync(path.join(targetVenv, "venv-created.marker"), "1", "utf8");',
        '  process.exit(0);',
        '}',
        'if (args[0] === "-c") {',
        '  const code = args[1] || "";',
        `  if (code.includes(${JSON.stringify(SPARK_SESSION_OK)})) {`,
        `    process.stdout.write(${JSON.stringify(`${SPARK_SESSION_OK}\n`)});`,
        '    process.exit(0);',
        '  }',
        '  process.stdout.write(JSON.stringify({ python: "3.12.2", pyspark: "4.0.1", pytest: "8.3.3", java: true }) + "\\n");',
        '  process.exit(0);',
        '}',
        'if (args[0] === "-m" && args[1] === "pytest") {',
        '  const cwd = process.cwd();',
        '  fs.mkdirSync(path.join(cwd, ".venv"), { recursive: true });',
        '  fs.writeFileSync(path.join(cwd, ".venv", "leak.txt"), "per-run venv", "utf8");',
        '  fs.mkdirSync(path.join(cwd, ".spark-ivy", "jars"), { recursive: true });',
        '  fs.writeFileSync(path.join(cwd, ".spark-ivy", "jars", "dup.jar"), "jar", "utf8");',
        '  fs.mkdirSync(path.join(cwd, ".pytest_cache"), { recursive: true });',
        '  if (process.env.XDG_CACHE_HOME) {',
        '    fs.mkdirSync(path.join(process.env.XDG_CACHE_HOME, "wheel"), { recursive: true });',
        '    fs.writeFileSync(path.join(process.env.XDG_CACHE_HOME, "wheel", "tmp.whl"), "w", "utf8");',
        '  }',
        '  fs.writeFileSync(envLogFile, JSON.stringify({',
        '    PIP_CACHE_DIR: process.env.PIP_CACHE_DIR,',
        '    SPARK_IVY_CACHE_DIR: process.env.SPARK_IVY_CACHE_DIR,',
        '    XDG_CACHE_HOME: process.env.XDG_CACHE_HOME,',
        '    JAVA_HOME: process.env.JAVA_HOME,',
        '    PYSPARK_PYTHON: process.env.PYSPARK_PYTHON,',
        '    PYSPARK_SUBMIT_ARGS: process.env.PYSPARK_SUBMIT_ARGS,',
        '  }), "utf8");',
        '  if (mode === "hang") {',
        '    setTimeout(() => process.exit(0), 30000);',
        '    return;',
        '  }',
        '  const junitArg = args.find((a) => a.startsWith("--junitxml="));',
        '  if (junitArg) {',
        '    const junitPath = junitArg.slice("--junitxml=".length);',
        '    if (mode === "pass") {',
        '      fs.writeFileSync(junitPath, "<testsuite tests=\\"1\\"><testcase classname=\\"tests.test_pipeline\\" name=\\"test_ok\\" time=\\"0.01\\"/></testsuite>", "utf8");',
        '      process.exit(0);',
        '    }',
        '    fs.writeFileSync(junitPath, "<testsuite tests=\\"1\\"><testcase classname=\\"tests.test_pipeline\\" name=\\"test_ok\\" time=\\"0.01\\"><failure message=\\"boom\\"/></testcase></testsuite>", "utf8");',
        '    process.exit(1);',
        '  }',
        '}',
        'process.exit(0);',
    ].join('\n');

    fs.writeFileSync(driverJs, driverSource, 'utf8');
    const wrapperContent =
        process.platform === 'win32'
            ? `@echo off\r\n"${process.execPath}" "${driverJs}" %*\r\n`
            : `#!/usr/bin/env sh\nexec "${process.execPath}" "${driverJs}" "$@"\n`;
    fs.writeFileSync(wrapperPath, wrapperContent, { mode: 0o755 });
    return wrapperPath;
}

afterEach(() => {
    while (tempRoots.length > 0) {
        const dir = tempRoots.pop();
        if (dir) {
            fs.rmSync(dir, { recursive: true, force: true });
        }
    }
});

describe('sharedTestCache path resolution and directory preservation', () => {
    it('resolves default ~/.cache/spark-pipeline-builder layout and honors env and explicit overrides', () => {
        const defaultPaths = getSharedTestCachePaths(undefined, {});
        if (!process.env[SHARED_CACHE_ENV_VAR]) {
            expect(defaultPaths.rootDir).toBe(path.join(os.homedir(), '.cache', 'spark-pipeline-builder'));
        }

        const envRoot = makeTempDir('spark-cache-env-');
        expect(resolveSharedCacheRoot(undefined, { [SHARED_CACHE_ENV_VAR]: envRoot })).toBe(path.resolve(envRoot));

        const overrideRoot = makeTempDir('spark-cache-override-');
        const paths = getSharedTestCachePaths(overrideRoot, { [SHARED_CACHE_ENV_VAR]: envRoot });
        expect(paths).toEqual({
            rootDir: path.resolve(overrideRoot),
            venvDir: path.join(overrideRoot, 'pytest-venv'),
            pipCacheDir: path.join(overrideRoot, 'pip'),
            ivyCacheDir: path.join(overrideRoot, 'ivy2'),
            jdkCacheDir: path.join(overrideRoot, 'jdk-21'),
        });
    });

    it('creates shared pip and Ivy folders while preserving existing jdk-21 and pytest-venv contents', () => {
        const cacheRoot = makeTempDir('spark-cache-preserve-');
        const jdkMarker = path.join(cacheRoot, 'jdk-21', 'jdk-21.0.13.1+2', 'release');
        const venvMarker = path.join(cacheRoot, 'pytest-venv', 'pyvenv.cfg');
        fs.mkdirSync(path.dirname(jdkMarker), { recursive: true });
        fs.mkdirSync(path.dirname(venvMarker), { recursive: true });
        fs.writeFileSync(jdkMarker, 'JAVA_VERSION="21.0.13.1"\n', 'utf8');
        fs.writeFileSync(venvMarker, 'include-system-site-packages = true\n', 'utf8');

        const paths = ensureSharedTestCacheDirs(cacheRoot, {});
        expect(fs.existsSync(paths.pipCacheDir)).toBe(true);
        expect(fs.existsSync(paths.ivyCacheDir)).toBe(true);
        expect(fs.readFileSync(jdkMarker, 'utf8')).toContain('21.0.13.1');
        expect(fs.readFileSync(venvMarker, 'utf8')).toContain('include-system-site-packages');
    });
});

describe('Temurin JDK 21 auto-detection', () => {
    it('parses JDK version strings across modern and legacy formats', () => {
        expect(parseJavaMajorVersion('21.0.13.1')).toBe(21);
        expect(parseJavaMajorVersion('27.0.1')).toBe(27);
        expect(parseJavaMajorVersion('17.0.12+7')).toBe(17);
        expect(parseJavaMajorVersion('1.8.0_392')).toBe(8);
        expect(parseJavaMajorVersion('')).toBeUndefined();
    });

    it('auto-resolves <cacheRoot>/jdk-21 when JAVA_HOME is unset and host PATH Java exceeds 21', () => {
        const root = makeTempDir('spark-jdk-autodetect-');
        const cacheRoot = path.join(root, 'cache');
        const hostJdk27 = createFakeJdk(path.join(root, 'host-jdk-27'), '27.0.1');
        const cachedTemurin21 = createFakeJdk(
            path.join(cacheRoot, 'jdk-21', 'jdk-21.0.13.1+2'),
            '21.0.13.1'
        );

        expect(readJdkMajorVersion(hostJdk27)).toBe(27);
        expect(detectHostJavaMajorFromPath(path.join(hostJdk27, 'bin'))).toBe(27);
        expect(findCachedJdk21Home(cacheRoot, {})).toBe(cachedTemurin21);

        const resolved = resolveSparkJavaHome({
            cacheRoot,
            env: { PATH: path.join(hostJdk27, 'bin') },
        });
        expect(resolved).toEqual({ javaHome: cachedTemurin21, fromSharedCache: true });
    });

    it('preserves explicit supported JAVA_HOME (JDK 17 or 21) and overrides unsupported JAVA_HOME > 21', () => {
        const root = makeTempDir('spark-jdk-boundary-');
        const cacheRoot = path.join(root, 'cache');
        const explicitJdk17 = createFakeJdk(path.join(root, 'jdk-17'), '17.0.12');
        const explicitJdk25 = createFakeJdk(path.join(root, 'jdk-25'), '25.0.1');
        const macCachedJdk21 = createFakeJdk(
            path.join(cacheRoot, 'jdk-21', 'jdk-21.0.9+10', 'Contents', 'Home'),
            '21.0.9'
        );

        expect(
            resolveSparkJavaHome({
                cacheRoot,
                env: { JAVA_HOME: explicitJdk17 },
            })
        ).toEqual({ javaHome: explicitJdk17, fromSharedCache: false });

        expect(
            resolveSparkJavaHome({
                cacheRoot,
                env: { JAVA_HOME: explicitJdk25 },
            })
        ).toEqual({ javaHome: macCachedJdk21, fromSharedCache: true });
    });
});

describe('pytest-runner and global-setup shared cache reuse and per-run cleanup', () => {
    it('reuses shared pytest-venv, PIP_CACHE_DIR, Ivy cache, and cached JDK 21 across consecutive runs', async () => {
        if (process.platform === 'win32') {
            return;
        }
        const root = makeTempDir('spark-runner-reuse-');
        const cacheRoot = path.join(root, 'shared-cache');
        const cachedJdk21 = createFakeJdk(path.join(cacheRoot, 'jdk-21', 'jdk-21.0.13.1+2'), '21.0.13.1');
        const hostJdk27 = createFakeJdk(path.join(root, 'host-jdk-27'), '27.0.1');
        const envLog = path.join(root, 'observed-env.json');
        const fakePy = createFakePythonExecutable(path.join(root, 'stub-bin'), 'pass', envLog);

        // Pre-populate shared pip and Ivy artifacts to ensure they survive every run.
        const sharedWheel = path.join(cacheRoot, 'pip', 'pyspark-4.0.1.whl');
        const sharedJar = path.join(cacheRoot, 'ivy2', 'jars', 'iceberg-runtime.jar');
        fs.mkdirSync(path.dirname(sharedWheel), { recursive: true });
        fs.mkdirSync(path.dirname(sharedJar), { recursive: true });
        fs.writeFileSync(sharedWheel, 'cached-wheel', 'utf8');
        fs.writeFileSync(sharedJar, 'cached-jar', 'utf8');

        const baseEnv = {
            PATH: `${path.join(hostJdk27, 'bin')}${path.delimiter}${process.env.PATH ?? ''}`,
            HOME: root,
        };

        // Run 1 bootstraps <cacheRoot>/pytest-venv with fakePy and runs pytest.
        const repo1 = createGeneratedRepoFixture(path.join(root, 'run1'));
        const res1 = await runGeneratedRepoTests(repo1, {
            python: fakePy,
            createVenv: true,
            cacheRoot,
            baseEnv,
        });
        expect(res1.status).toBe('passed');
        const expectedSharedPy = venvPython(path.join(cacheRoot, 'pytest-venv'));
        expect(res1.python).toBe(expectedSharedPy);

        const observed = JSON.parse(fs.readFileSync(envLog, 'utf8')) as Record<string, string>;
        expect(observed.PIP_CACHE_DIR).toBe(path.join(cacheRoot, 'pip'));
        expect(observed.SPARK_IVY_CACHE_DIR).toBe(path.join(cacheRoot, 'ivy2'));
        expect(observed.JAVA_HOME).toBe(cachedJdk21);
        expect(observed.XDG_CACHE_HOME).toBe(path.join(repo1, '.xdg-cache'));
        expect(observed.PYSPARK_SUBMIT_ARGS).toContain(`spark.jars.ivy=${path.join(cacheRoot, 'ivy2')}`);

        // Written conftest configures spark.jars.ivy to reuse <cacheRoot>/ivy2.
        const conftestText = fs.readFileSync(path.join(repo1, 'conftest.py'), 'utf8');
        expect(conftestText).toContain('.config("spark.jars.ivy", _IVY_DIR)');
        expect(conftestText).toContain(path.join(cacheRoot, 'ivy2'));

        // Per-run artifacts inside repo1 were deleted in finally.
        for (const artifact of PER_RUN_REPO_ARTIFACTS) {
            expect(fs.existsSync(path.join(repo1, artifact))).toBe(false);
        }

        // Remove the creation marker and run a second repo with default python ('python3') and createVenv=false
        // to prove Run 2 reuses the existing <cacheRoot>/pytest-venv without recreating it.
        const marker = path.join(cacheRoot, 'pytest-venv', 'venv-created.marker');
        expect(fs.existsSync(marker)).toBe(true);
        fs.rmSync(marker);

        const repo2 = createGeneratedRepoFixture(path.join(root, 'run2'));
        const res2 = await runGeneratedRepoTests(repo2, {
            cacheRoot,
            baseEnv,
        });
        expect(res2.status).toBe('passed');
        expect(res2.python).toBe(expectedSharedPy);
        expect(fs.existsSync(marker)).toBe(false);

        // Shared cache artifacts remain intact across both runs.
        expect(fs.readFileSync(sharedWheel, 'utf8')).toBe('cached-wheel');
        expect(fs.readFileSync(sharedJar, 'utf8')).toBe('cached-jar');
        expect(fs.existsSync(cachedJdk21)).toBe(true);
    });

    it('honors explicit venvDir boundary overrides without touching <cacheRoot>/pytest-venv', async () => {
        if (process.platform === 'win32') {
            return;
        }
        const root = makeTempDir('spark-venv-override-');
        const cacheRoot = path.join(root, 'shared-cache');
        const customVenv = path.join(root, 'custom-venv');
        const envLog = path.join(root, 'env.json');
        const fakePy = createFakePythonExecutable(path.join(root, 'bin'), 'pass', envLog);

        const resolved = await resolvePython(
            { python: fakePy, venvDir: customVenv, createVenv: true, cacheRoot },
            buildChildEnv(fakePy, { PATH: process.env.PATH }, undefined, { cacheRoot }).env
        );
        expect(resolved.python).toBe(venvPython(customVenv));
        expect(fs.existsSync(path.join(cacheRoot, 'pytest-venv'))).toBe(false);
    });

    it('deletes per-run .venv, .spark-ivy, .pytest_cache, and XDG scratch on failure and timeout while protecting <cacheRoot>', async () => {
        if (process.platform === 'win32') {
            return;
        }
        const root = makeTempDir('spark-cleanup-failure-');
        const cacheRoot = path.join(root, 'shared-cache');
        const cachedJdk21 = createFakeJdk(path.join(cacheRoot, 'jdk-21', 'jdk-21.0.13.1+2'), '21.0.13.1');
        const sharedIvyJar = path.join(cacheRoot, 'ivy2', 'jars', 'keep.jar');
        fs.mkdirSync(path.dirname(sharedIvyJar), { recursive: true });
        fs.writeFileSync(sharedIvyJar, 'keep', 'utf8');

        // 1. Failing pytest run that leaks .venv, .spark-ivy, .pytest_cache, and custom XDG_CACHE_HOME inside repo.
        const failEnvLog = path.join(root, 'fail-env.json');
        const failPy = createFakePythonExecutable(path.join(root, 'fail-bin'), 'fail', failEnvLog);
        const failRepo = createGeneratedRepoFixture(path.join(root, 'fail-case'));
        const customRepoXdg = path.join(failRepo, 'custom-xdg-scratch');

        const failResult = await runGeneratedRepoTests(failRepo, {
            python: failPy,
            createVenv: true,
            cacheRoot,
            extraEnv: [{ key: 'XDG_CACHE_HOME', value: customRepoXdg }],
            baseEnv: { PATH: process.env.PATH, JAVA_HOME: cachedJdk21 },
        });
        expect(failResult.status).toBe('failed');
        for (const artifact of PER_RUN_REPO_ARTIFACTS) {
            expect(fs.existsSync(path.join(failRepo, artifact))).toBe(false);
        }
        expect(fs.existsSync(customRepoXdg)).toBe(false);

        // 2. Hanging pytest run killed by timeoutMs also cleans up all per-run artifacts in finally.
        const hangEnvLog = path.join(root, 'hang-env.json');
        const hangPy = createFakePythonExecutable(path.join(root, 'hang-bin'), 'hang', hangEnvLog);
        const hangRepo = createGeneratedRepoFixture(path.join(root, 'hang-case'));
        // Pre-seed per-run folders before timeout to guarantee finally removes them even if killed early.
        fs.mkdirSync(path.join(hangRepo, '.venv'), { recursive: true });
        fs.mkdirSync(path.join(hangRepo, '.spark-ivy'), { recursive: true });
        fs.mkdirSync(path.join(hangRepo, '.pytest_cache'), { recursive: true });
        fs.mkdirSync(path.join(hangRepo, '.xdg-cache'), { recursive: true });

        const timeoutResult = await runGeneratedRepoTests(hangRepo, {
            python: hangPy,
            venvDir: path.join(root, 'hang-venv-unused'),
            cacheRoot,
            timeoutMs: 250,
            baseEnv: { PATH: process.env.PATH, JAVA_HOME: cachedJdk21 },
        });
        expect(timeoutResult.status).toBe('timeout');
        for (const artifact of PER_RUN_REPO_ARTIFACTS) {
            expect(fs.existsSync(path.join(hangRepo, artifact))).toBe(false);
        }

        // 3. Safety guard: cleanupPerRunRepoArtifacts never deletes <cacheRoot> even on overlapping paths.
        cleanupPerRunRepoArtifacts(cacheRoot, {
            cacheRoot,
            extraScratchDirs: [path.join(cacheRoot, 'ivy2'), path.join(cacheRoot, 'jdk-21')],
        });
        expect(fs.existsSync(cachedJdk21)).toBe(true);
        expect(fs.readFileSync(sharedIvyJar, 'utf8')).toBe('keep');
        expect(fs.existsSync(path.join(cacheRoot, 'pytest-venv'))).toBe(true);
    });

    it('bootstraps shared cache env and JDK 21 in global-setup without deleting existing cache contents', async () => {
        const root = makeTempDir('spark-global-setup-');
        const cacheRoot = path.join(root, 'cache');
        const hostJdk27 = createFakeJdk(path.join(root, 'jdk-27'), '27.0.1');
        const cachedJdk21 = createFakeJdk(path.join(cacheRoot, 'jdk-21', 'jdk-21.0.13.1+2'), '21.0.13.1');

        const env: Record<string, string | undefined> = {
            PATH: path.join(hostJdk27, 'bin'),
        };
        const paths = await bootstrapIntegrationTestCache({
            cacheRoot,
            env,
            createVenv: false,
        });

        expect(env[SHARED_CACHE_ENV_VAR]).toBe(paths.rootDir);
        expect(env.PIP_CACHE_DIR).toBe(paths.pipCacheDir);
        expect(env[SPARK_IVY_CACHE_ENV_VAR]).toBe(paths.ivyCacheDir);
        expect(env.JAVA_HOME).toBe(cachedJdk21);
        expect(env.PATH?.startsWith(`${path.join(cachedJdk21, 'bin')}${path.delimiter}`)).toBe(true);
        expect(buildConftestSource(paths.ivyCacheDir)).toContain(paths.ivyCacheDir);
    });
});
