/**
 * Invariant and behavior tests for the bundled `run_local.sh` runner template.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * - Keeps every runner template byte-identical between `resources/templates/pyspark/`
 *   and `resources/codegen-skills/runner-<name>/templates/python/`.
 * - Checks that `run_local.sh` bootstraps a venv, selects a JDK 17 or 21, and passes a
 *   Maven Central Ivy settings file with `--packages`.
 * - Runs the template against stub Python, Java, and `spark-submit` binaries so the
 *   bootstrap logic runs offline without a real JDK or PySpark.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';

const RESOURCES = path.resolve(__dirname, '../../../resources');
const PYSPARK_TEMPLATES = path.join(RESOURCES, 'templates', 'pyspark');
const SKILLS_ROOT = path.join(RESOURCES, 'codegen-skills');
const TEMPLATE = path.join(PYSPARK_TEMPLATES, 'run_local.sh');
const HAS_BASH = process.platform !== 'win32' && spawnSync('bash', ['--version']).status === 0;

/**
 * Lists every runner skill template file under `runner-*\/templates/python/`.
 *
 * @returns Absolute paths of the skill template files.
 */
function skillRunnerTemplates(): string[] {
    return fs.readdirSync(SKILLS_ROOT)
        .filter(name => name.startsWith('runner-'))
        .flatMap(name => {
            const dir = path.join(SKILLS_ROOT, name, 'templates', 'python');
            return fs.existsSync(dir) ? fs.readdirSync(dir).map(file => path.join(dir, file)) : [];
        });
}

/**
 * Writes an executable shell stub.
 *
 * @param file - Absolute path of the stub.
 * @param body - Script body without the shebang line.
 */
function writeStub(file: string, body: string): void {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, `#!/usr/bin/env bash\n${body}\n`, { mode: 0o755 });
}

/**
 * Creates a fake JDK home with a `release` file and a stub `bin/java`.
 *
 * @param home - Absolute JDK home path.
 * @param version - Value for `JAVA_VERSION` in the `release` file.
 * @returns The JDK home path.
 */
function fakeJdk(home: string, version: string): string {
    writeStub(path.join(home, 'bin', 'java'), `echo 'openjdk version "${version}"' >&2`);
    fs.writeFileSync(path.join(home, 'release'), `IMPLEMENTOR="Test"\nJAVA_VERSION="${version}"\n`);
    return home;
}

describe('runner template parity', () => {
    it('keeps every runner skill template byte-identical with the pyspark template (normal condition)', () => {
        const templates = skillRunnerTemplates();
        expect(templates.length).toBeGreaterThanOrEqual(3);
        for (const file of templates) {
            const twin = path.join(PYSPARK_TEMPLATES, path.basename(file));
            expect(fs.existsSync(twin), `${twin} is missing`).toBe(true);
            expect(fs.readFileSync(file).equals(fs.readFileSync(twin)), `${path.basename(file)} differs`).toBe(true);
        }
    });

    it.skipIf(!HAS_BASH)('passes bash -n syntax checks (boundary condition)', () => {
        for (const file of [TEMPLATE, ...skillRunnerTemplates().filter(f => f.endsWith('.sh'))]) {
            const result = spawnSync('bash', ['-n', file], { encoding: 'utf-8' });
            expect(result.status, `${file}: ${result.stderr}`).toBe(0);
        }
    });
});

describe('run_local.sh bootstrap content', () => {
    const script = fs.readFileSync(TEMPLATE, 'utf-8');

    it('bootstraps a venv with a requirements hash stamp (normal condition)', () => {
        expect(script).toContain('set -euo pipefail');
        expect(script).toContain('-m venv "${VENV_DIR}"');
        expect(script).toContain('.requirements.sha256');
        expect(script).toContain('-m pip install --disable-pip-version-check -r requirements.txt');
        expect(script).toContain('SKIP_VENV_BOOTSTRAP');
        expect(script).toContain('PYTHON_BIN="${PYTHON_BIN:-python3}"');
    });

    it('selects a JDK 17 or 21 and downloads Temurin 21 only when allowed (boundary condition)', () => {
        expect(script).toContain('[[ "${major}" == "21" || "${major}" == "17" ]]');
        for (const location of ['/usr/lib/jvm/*/', '.sdkman/candidates/java/*/', '.jdks/*/', '/Library/Java/JavaVirtualMachines/*/Contents/Home/']) {
            expect(script).toContain(location);
        }
        expect(script).toContain('https://api.adoptium.net/v3/assets/latest/21/hotspot');
        expect(script).toContain('spark-pipeline-builder/jdk-21');
        expect(script).toContain('AUTO_INSTALL_JDK="${AUTO_INSTALL_JDK:-true}"');
        expect(script).toContain('JDK checksum mismatch');
    });

    it('passes a Maven Central-only Ivy settings file with --packages (failure condition guard)', () => {
        expect(script).toContain('<ibiblio name="central" m2compatible="true" root="https://repo1.maven.org/maven2/"/>');
        expect(script).toContain('--conf "spark.jars.ivySettings=${SPARK_IVY_SETTINGS}"');
        expect(script).toContain('--conf "spark.jars.ivy=${SPARK_IVY_DIR}"');
        expect(script).toContain('PYSPARK_SUBMIT_ARGS');
        expect(script).not.toContain('local-m2-cache');
    });
});

describe.skipIf(!HAS_BASH)('run_local.sh behavior with stub binaries', () => {
    let root: string;
    let project: string;
    let stubBin: string;
    let jdkCache: string;

    /**
     * Runs the template copy in the temporary project with a clean environment.
     *
     * @param args - CLI arguments for run_local.sh.
     * @param env - Extra environment variables.
     * @returns The spawn result.
     */
    function run(args: string[], env: Record<string, string>) {
        return spawnSync('bash', [path.join(project, 'run_local.sh'), ...args], {
            cwd: project,
            encoding: 'utf-8',
            env: {
                HOME: root,
                PATH: `${stubBin}:/usr/bin:/bin`,
                JDK_CACHE_DIR: jdkCache,
                ...env,
            },
        });
    }

    beforeEach(() => {
        root = fs.mkdtempSync(path.join(os.tmpdir(), 'run-local-'));
        project = path.join(root, 'project');
        stubBin = path.join(root, 'bin');
        jdkCache = path.join(root, 'jdk-cache');
        fs.mkdirSync(path.join(project, 'tests'), { recursive: true });
        fs.mkdirSync(jdkCache, { recursive: true });
        fs.copyFileSync(TEMPLATE, path.join(project, 'run_local.sh'));
        fs.writeFileSync(path.join(project, 'requirements.txt'), '# no packages\n');
        writeStub(path.join(stubBin, 'fake-python'), 'echo "PYTHON_ARGS=$* JAVA_HOME=${JAVA_HOME:-}"');
        writeStub(path.join(stubBin, 'spark-submit'), 'printf "SUBMIT_ARG=%s\\n" "$@"; echo "JAVA_HOME=${JAVA_HOME:-}"');
    });

    afterEach(() => {
        fs.rmSync(root, { recursive: true, force: true });
    });

    it('uses a JDK 21 JAVA_HOME as is and runs pytest in --test mode (normal condition)', () => {
        const jdk21 = fakeJdk(path.join(root, 'jdk21'), '21.0.5');
        const result = run(['--test', '-k', 'smoke'], {
            SKIP_VENV_BOOTSTRAP: 'true',
            PYTHON_BIN: 'fake-python',
            JAVA_HOME: jdk21,
        });
        expect(result.status, result.stderr).toBe(0);
        expect(result.stdout).toContain(`[run_local] Using JAVA_HOME=${jdk21} (JDK 21).`);
        expect(result.stdout).toContain(`PYTHON_ARGS=-m pytest tests/ -v -k smoke JAVA_HOME=${jdk21}`);
    });

    it('ignores a JDK 25 JAVA_HOME and selects a JDK 21 from the cache or the host (boundary condition)', () => {
        const jdk25 = fakeJdk(path.join(root, 'jdk25'), '25');
        fakeJdk(path.join(jdkCache, 'jdk-21.0.9+10'), '21.0.9');
        const result = run(['--test'], {
            SKIP_VENV_BOOTSTRAP: 'true',
            PYTHON_BIN: 'fake-python',
            JAVA_HOME: jdk25,
            AUTO_INSTALL_JDK: 'false',
        });
        expect(result.status, result.stderr).toBe(0);
        expect(result.stdout).toContain(`Ignoring JAVA_HOME=${jdk25} (JDK 25)`);
        expect(result.stdout).toMatch(/Using JAVA_HOME=\S+ \(JDK 21\)\./);
        expect(result.stdout).not.toContain(`PYTHON_ARGS=-m pytest tests/ -v JAVA_HOME=${jdk25}`);
    });

    it('fails fast when the Python interpreter does not exist (failure condition)', () => {
        const result = run(['--test'], { PYTHON_BIN: 'no-such-python-binary', SKIP_VENV_BOOTSTRAP: 'true' });
        expect(result.status).toBe(1);
        expect(result.stderr).toContain("Python interpreter 'no-such-python-binary' not found");
    });

    it('passes Ivy settings and the project Ivy cache to spark-submit with --packages (normal condition)', () => {
        const jdk17 = fakeJdk(path.join(root, 'jdk17'), '17.0.12');
        const result = run(['--source-table', 'a.b.c'], {
            SKIP_VENV_BOOTSTRAP: 'true',
            PYTHON_BIN: 'fake-python',
            JAVA_HOME: jdk17,
            SPARK_PACKAGES: 'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1',
        });
        expect(result.status, result.stderr).toBe(0);
        const settings = path.join(project, '.spark-ivy', 'ivysettings.xml');
        expect(result.stdout).toContain('SUBMIT_ARG=--packages');
        expect(result.stdout).toContain(`SUBMIT_ARG=spark.jars.ivySettings=${settings}`);
        expect(result.stdout).toContain(`SUBMIT_ARG=spark.jars.ivy=${path.join(project, '.spark-ivy')}`);
        expect(result.stdout).toContain('SUBMIT_ARG=src/pipeline.py');
        expect(result.stdout).toContain('SUBMIT_ARG=a.b.c');
        expect(fs.readFileSync(settings, 'utf-8')).toContain('defaultResolver="central"');
    });

    it('keeps user Ivy properties and omits Ivy flags without --packages (boundary condition)', () => {
        const jdk21 = fakeJdk(path.join(root, 'jdk21'), '21.0.5');
        const custom = run([], {
            SKIP_VENV_BOOTSTRAP: 'true',
            PYTHON_BIN: 'fake-python',
            JAVA_HOME: jdk21,
            SPARK_PACKAGES: 'g:a:1',
            SPARK_PROPERTIES: 'spark.jars.ivySettings=/custom.xml,spark.jars.ivy=/custom-ivy',
        });
        expect(custom.status, custom.stderr).toBe(0);
        expect(custom.stdout).toContain('SUBMIT_ARG=spark.jars.ivySettings=/custom.xml');
        expect(custom.stdout).not.toContain('.spark-ivy');

        const plain = run([], { SKIP_VENV_BOOTSTRAP: 'true', PYTHON_BIN: 'fake-python', JAVA_HOME: jdk21 });
        expect(plain.status, plain.stderr).toBe(0);
        expect(plain.stdout).not.toContain('spark.jars.ivy');
    });

    it('builds PYSPARK_SUBMIT_ARGS for the direct Python path (normal condition)', () => {
        const jdk21 = fakeJdk(path.join(root, 'jdk21'), '21.0.5');
        writeStub(path.join(stubBin, 'fake-python'), 'echo "PYTHON_ARGS=$*"; echo "SUBMIT_ENV=${PYSPARK_SUBMIT_ARGS:-}"');
        const result = run([], {
            SKIP_VENV_BOOTSTRAP: 'true',
            PYTHON_BIN: 'fake-python',
            JAVA_HOME: jdk21,
            USE_PYTHON_DIRECT: 'true',
            SPARK_PACKAGES: 'g:a:1',
        });
        expect(result.status, result.stderr).toBe(0);
        expect(result.stdout).toContain('PYTHON_ARGS=-m src.pipeline');
        expect(result.stdout).toMatch(/SUBMIT_ENV=--master local\\\[\\\*\\\] --packages g:a:1 .*spark\.jars\.ivySettings=.* pyspark-shell/);
    });

    it('skips the JDK bootstrap in Spark Connect mode (boundary condition)', () => {
        const result = run([], {
            SKIP_VENV_BOOTSTRAP: 'true',
            PYTHON_BIN: 'fake-python',
            JAVA_HOME: path.join(root, 'missing-jdk'),
            AUTO_INSTALL_JDK: 'false',
            USE_SPARK_CONNECT: 'true',
        });
        expect(result.status, result.stderr).toBe(0);
        expect(result.stdout).not.toContain('[run_local] Using JAVA_HOME');
        expect(result.stdout).not.toContain('Ignoring JAVA_HOME');
        expect(result.stdout).toContain('PYTHON_ARGS=-m src.pipeline');
    });

    it('fails fast with AUTO_INSTALL_JDK=false when no supported JDK exists (failure condition)', () => {
        const jdk25 = fakeJdk(path.join(root, 'jdk25'), '25.0.1');
        const result = run(['--test'], {
            SKIP_VENV_BOOTSTRAP: 'true',
            PYTHON_BIN: 'fake-python',
            JAVA_HOME: jdk25,
            AUTO_INSTALL_JDK: 'false',
        });
        // NOTE: A host JDK 17 or 21 under /usr/lib/jvm satisfies the scan. Then the run succeeds.
        if (result.status === 0) {
            expect(result.stdout).toMatch(/Using JAVA_HOME=\S+ \(JDK (17|21)\)\./);
        } else {
            expect(result.status).toBe(1);
            expect(result.stderr).toContain('No JDK 17 or 21 found and AUTO_INSTALL_JDK=false');
        }
    });

    it.skipIf(spawnSync('python3', ['-c', 'import venv, ensurepip']).status !== 0)(
        'creates .venv once and reinstalls only when requirements.txt changes (normal condition)',
        () => {
            const jdk21 = fakeJdk(path.join(root, 'jdk21'), '21.0.5');
            writeStub(path.join(stubBin, 'spark-submit'), 'echo "SUBMIT"');
            const env = { JAVA_HOME: jdk21, PIP_NO_INDEX: '1' };
            const first = run([], env);
            expect(first.status, first.stderr).toBe(0);
            expect(first.stdout).toContain('Creating Python virtual environment');
            expect(first.stdout).toContain('Installing requirements.txt');
            expect(fs.existsSync(path.join(project, '.venv', '.requirements.sha256'))).toBe(true);

            const second = run([], env);
            expect(second.status, second.stderr).toBe(0);
            expect(second.stdout).toContain('requirements.txt is unchanged');
            expect(second.stdout).not.toContain('Creating Python virtual environment');

            fs.appendFileSync(path.join(project, 'requirements.txt'), '# changed\n');
            const third = run([], env);
            expect(third.status, third.stderr).toBe(0);
            expect(third.stdout).toContain('Installing requirements.txt');
        },
        60_000
    );
});

describe('local runner skill guidance', () => {
    const common = fs.readFileSync(path.join(SKILLS_ROOT, 'codegen-common', 'SKILL.md'), 'utf-8');
    const runnerLocal = fs.readFileSync(path.join(SKILLS_ROOT, 'runner-local', 'SKILL.md'), 'utf-8');
    const iceberg = fs.readFileSync(path.join(SKILLS_ROOT, 'io-iceberg', 'SKILL.md'), 'utf-8');

    it('pins pyspark to the Spark minor line in requirements guidance (normal condition)', () => {
        expect(common).toContain('`pyspark=={{spark_version}}.*`');
        expect(runnerLocal).toContain('`pyspark=={{spark_version}}.*`');
    });

    it('documents the bootstrap opt-outs in the runner skill and README guidance (boundary condition)', () => {
        for (const token of ['PYTHON_BIN', 'SKIP_VENV_BOOTSTRAP=true', 'AUTO_INSTALL_JDK=false', 'SPARK_IVY_SETTINGS']) {
            expect(runnerLocal).toContain(token);
            expect(common).toContain(token);
        }
    });

    it('never points local Iceberg runs at the missing 1.9.0 Spark 4.0 runtime (failure condition)', () => {
        expect(iceberg).toContain('iceberg-spark-runtime-4.0_2.13:1.10.1');
        expect(iceberg).not.toContain('iceberg-spark-runtime-4.0_2.13:1.9.0');
    });
});
