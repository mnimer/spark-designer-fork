/**
 * Unit tests for SDP runner scripts, Managed Data Pipelines stage template, scriptGuard
 * allowlist, and PipelineRunner SDP environment exports (`SDP8`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import YAML from 'yaml';

vi.mock('../../../cli/gcloudClient', () => ({ getGcloudProject: vi.fn() }));
vi.mock('../../../cli/cliResolver', () => ({ findGcloudPath: vi.fn(() => '/usr/bin/gcloud') }));

import { ALLOWED_EXECUTABLE_SCRIPTS, isAllowedExecutableScript } from '../../../security/scriptGuard';
import { clearCodegenSkillCatalogCache } from '../../../codegen/CodegenSkillCatalog';
import { buildPromptFromPipelineYaml } from '../../../codegen/CodegenPromptBuilder';
import {
    buildRunnerEnvironment,
    describeRunner,
    findRunnerScript,
    runnerScriptCandidates,
} from '../../../execution/PipelineRunner';

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const SKILL_TEMPLATES_DIR = path.join(REPO_ROOT, 'resources', 'codegen-skills', 'runner-sdp', 'templates', 'python');
const PYSPARK_TEMPLATES_DIR = path.join(REPO_ROOT, 'resources', 'templates', 'pyspark');
const HAS_BASH = process.platform !== 'win32' && spawnSync('bash', ['--version']).status === 0;

/**
 * Writes an executable stub script for offline shell runner verification.
 *
 * @param filePath Target stub file path.
 * @param scriptBody Bash body lines after shebang.
 */
function writeExecutableStub(filePath: string, scriptBody: string): void {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, `#!/usr/bin/env bash\n${scriptBody}\n`, { mode: 0o755 });
}

/**
 * Creates a fake JDK installation folder with release metadata.
 *
 * @param jdkHome Directory path for the fake JDK.
 * @param javaVersion Reported JDK version string.
 * @returns The created JDK home path.
 */
function createFakeJdk(jdkHome: string, javaVersion: string): string {
    writeExecutableStub(path.join(jdkHome, 'bin', 'java'), `echo 'openjdk version "${javaVersion}"' >&2`);
    fs.writeFileSync(path.join(jdkHome, 'release'), `JAVA_VERSION="${javaVersion}"\n`);
    return jdkHome;
}

describe('SDP8 — runner-sdp templates and byte-identical mirrors', () => {
    it('keeps run_sdp_local.sh, run_sdp_dataproc.sh, and sdp_stage.pipeline.yaml byte-identical across skill and pyspark templates', () => {
        for (const name of ['run_sdp_local.sh', 'run_sdp_dataproc.sh', 'sdp_stage.pipeline.yaml']) {
            const skillPath = path.join(SKILL_TEMPLATES_DIR, name);
            const mirrorPath = path.join(PYSPARK_TEMPLATES_DIR, name);
            expect(fs.existsSync(skillPath), `${skillPath} missing`).toBe(true);
            expect(fs.existsSync(mirrorPath), `${mirrorPath} missing`).toBe(true);
            expect(fs.readFileSync(skillPath).equals(fs.readFileSync(mirrorPath)), `${name} mirror mismatch`).toBe(
                true
            );
        }
    });

    it('validates sdp_stage.pipeline.yaml structure and routes static Spark properties to execution_config', () => {
        const raw = fs.readFileSync(path.join(PYSPARK_TEMPLATES_DIR, 'sdp_stage.pipeline.yaml'), 'utf-8');
        // Replace model-facing {{pipeline_name}} placeholder before YAML parsing.
        const concrete = raw.replace(/\{\{pipeline_name\}\}/g, 'retail_medallion');
        const parsed = YAML.parse(concrete);
        expect(parsed.name).toBe('retail_medallion');
        expect(parsed.runtime_version).toBe('4.0');
        expect(Array.isArray(parsed.stages)).toBe(true);
        expect(parsed.stages[0].type).toBe('data_model');
        expect(parsed.stages[0].sdp.spec_path).toBe('spark-pipeline.yml');
        expect(parsed.stages[0].sdp.execution_config.properties['spark.sql.extensions']).toBe(
            'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
        );
        expect(Array.isArray(parsed.stages[0].sdp.execution_config.jar_uris)).toBe(true);
    });

    it('registers SDP runner scripts in ALLOWED_EXECUTABLE_SCRIPTS', () => {
        expect(ALLOWED_EXECUTABLE_SCRIPTS.has('run_sdp_local.sh')).toBe(true);
        expect(ALLOWED_EXECUTABLE_SCRIPTS.has('run_sdp_dataproc.sh')).toBe(true);
        expect(ALLOWED_EXECUTABLE_SCRIPTS.has('run_sdp_serverless.sh')).toBe(true);
        expect(isAllowedExecutableScript('/tmp/out/run_sdp_local.sh')).toBe(true);
        expect(isAllowedExecutableScript('/tmp/out/run_sdp_dataproc.sh')).toBe(true);
        expect(isAllowedExecutableScript('/tmp/out/sdp_stage.pipeline.yaml')).toBe(false);
    });

    it('selects runner-sdp and inlines all three SDP runner templates when building an SDP prompt', () => {
        clearCodegenSkillCatalogCache();
        const skillsRoot = path.join(REPO_ROOT, 'resources', 'codegen-skills');
        const built = buildPromptFromPipelineYaml({
            skillsRoot,
            yamlText: [
                'paradigm: sdp',
                'nodes:',
                '  - id: s1',
                '    data: { type: SOURCE, label: Orders CSV, spark_transform: CsvSource, properties: { readMode: streaming } }',
                '  - id: k1',
                '    data: { type: SINK, label: Bronze Orders, spark_transform: IcebergSink }',
                'edges:',
                '  - { id: e1, source: s1, target: k1 }',
                'jobConfig:',
                '  runner: ServerlessSpark',
                '  runtimeVersion: "4.0"',
                '',
            ].join('\n'),
            language: 'PySpark',
        });
        expect(built.selectedSkills).toContain('runner-sdp');
        expect(built.templateFiles).toEqual([
            'run_sdp_dataproc.sh',
            'run_sdp_local.sh',
            'sdp_stage.pipeline.yaml',
        ]);
        expect(built.prompt).toContain('#### Template: run_sdp_local.sh');
        expect(built.prompt).toContain('#### Template: run_sdp_dataproc.sh');
        expect(built.prompt).toContain('#### Template: sdp_stage.pipeline.yaml');
    });
});

describe('SDP8 — PipelineRunner SDP script candidates and environment exports', () => {
    it('prioritizes SDP scripts when paradigm is sdp and preserves imperative defaults otherwise', () => {
        expect(runnerScriptCandidates('Local', 'sdp')).toEqual(['run_sdp_local.sh', 'run_local.sh']);
        expect(runnerScriptCandidates('DataprocCluster', 'sdp')).toEqual([
            'run_sdp_dataproc.sh',
            'run_dataproc_cluster.sh',
        ]);
        expect(runnerScriptCandidates('ServerlessSpark', 'sdp')).toEqual([
            'run_sdp_serverless.sh',
            'run_sdp_local.sh',
            'run_serverless_spark.sh',
        ]);
        expect(runnerScriptCandidates('Unknown', 'sdp')).toEqual([]);

        // Imperative default remains unchanged.
        expect(runnerScriptCandidates('Local')).toEqual(['run_local.sh', 'run_directrunner.sh']);
        expect(runnerScriptCandidates('DataprocCluster', 'imperative')).toEqual(['run_dataproc_cluster.sh']);

        const sdpFiles = new Set([path.join('/out', 'run_sdp_local.sh'), path.join('/out', 'run_local.sh')]);
        expect(findRunnerScript('/out', runnerScriptCandidates('Local', 'sdp'), (p) => sdpFiles.has(p))).toBe(
            'run_sdp_local.sh'
        );
        expect(describeRunner('Local', 'sdp')).toContain('Spark Declarative Pipeline');
        expect(describeRunner('DataprocCluster', 'sdp')).toContain('Dataproc 4.0');
        expect(describeRunner('ServerlessSpark', 'sdp')).toContain('Serverless Spark 4.0');
    });

    it('exports sanitized SDP environment variables for incremental, refresh, full_refresh, full_refresh_all, and dry-run modes', () => {
        const refreshEnv = buildRunnerEnvironment({
            baseEnv: {},
            exists: () => false,
            jobConfig: {
                paradigm: 'sdp',
                sdpCommand: 'run',
                sdpStorage: 'gs://my-sdp-bucket/checkpoints',
                sdpCatalog: 'lake',
                sdpDatabase: 'retail_silver',
                sdpCreateNamespace: true,
                sdpRefreshMode: 'refresh',
                sdpRefreshDatasets: ['bronze_orders', 'bad-name!', '__spark_autocdc_aux_state_x', 'silver_orders', 'bronze_orders'],
                sdpStaticSparkConf: {
                    'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                },
            },
        }).env;

        expect(refreshEnv.SDP_COMMAND).toBe('run');
        expect(refreshEnv.SDP_STORAGE).toBe('gs://my-sdp-bucket/checkpoints');
        expect(refreshEnv.SDP_CATALOG).toBe('lake');
        expect(refreshEnv.SDP_DATABASE).toBe('retail_silver');
        expect(refreshEnv.SDP_CREATE_NAMESPACE).toBe('true');
        expect(refreshEnv.SDP_REFRESH).toBe('bronze_orders,silver_orders');
        expect(refreshEnv.SDP_FULL_REFRESH).toBeUndefined();
        expect(refreshEnv.SDP_FULL_REFRESH_ALL).toBeUndefined();
        expect(refreshEnv.SPARK_PROPERTIES).toBe(
            'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
        );

        const fullRefreshAllEnv = buildRunnerEnvironment({
            baseEnv: {},
            exists: () => false,
            jobConfig: {
                paradigm: 'sdp',
                sdpCommand: 'run',
                sdpRefreshMode: 'full_refresh_all',
                sdpRefreshDatasets: ['ignored_on_full_all'],
                sdpFullRefreshDatasets: ['ignored_on_full_all'],
            },
        }).env;
        expect(fullRefreshAllEnv.SDP_FULL_REFRESH_ALL).toBe('true');
        expect(fullRefreshAllEnv.SDP_REFRESH).toBeUndefined();
        expect(fullRefreshAllEnv.SDP_FULL_REFRESH).toBeUndefined();

        const dryRunEnv = buildRunnerEnvironment({
            baseEnv: {},
            exists: () => false,
            jobConfig: {
                paradigm: 'sdp',
                sdpCommand: 'dry-run',
                sdpCatalog: 'invalid-hyphen-catalog',
                sdpDatabase: 'bad;db',
                sdpRefreshMode: 'full_refresh',
                sdpFullRefreshDatasets: ['silver_orders'],
            },
        }).env;
        expect(dryRunEnv.SDP_COMMAND).toBe('dry-run');
        expect(dryRunEnv.SDP_CATALOG).toBeUndefined();
        expect(dryRunEnv.SDP_DATABASE).toBeUndefined();
        expect(dryRunEnv.SDP_FULL_REFRESH).toBeUndefined();
    });
});

describe.skipIf(!HAS_BASH)('SDP8 — run_sdp_local.sh and run_sdp_dataproc.sh behavior with stub binaries', () => {
    let tmpDir: string;
    let projectDir: string;
    let stubBin: string;

    beforeEach(() => {
        tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sdp-runner-'));
        projectDir = path.join(tmpDir, 'project');
        stubBin = path.join(tmpDir, 'bin');
        fs.mkdirSync(path.join(projectDir, 'transformations'), { recursive: true });
        fs.mkdirSync(path.join(projectDir, 'src'), { recursive: true });
        fs.mkdirSync(path.join(projectDir, 'tests'), { recursive: true });
        fs.copyFileSync(path.join(PYSPARK_TEMPLATES_DIR, 'run_sdp_local.sh'), path.join(projectDir, 'run_sdp_local.sh'));
        fs.copyFileSync(
            path.join(PYSPARK_TEMPLATES_DIR, 'run_sdp_dataproc.sh'),
            path.join(projectDir, 'run_sdp_dataproc.sh')
        );
        fs.writeFileSync(path.join(projectDir, 'requirements.txt'), 'pyspark==4.2.*\npytest>=8.0.0\nzstandard>=0.25.0\n');
        fs.writeFileSync(
            path.join(projectDir, 'spark-pipeline.yml'),
            'name: demo_sdp\nstorage: gs://bucket/state\ncatalog: lake\ndatabase: sales\nlibraries:\n  - glob:\n      include: transformations/**\n'
        );
        writeExecutableStub(
            path.join(stubBin, 'fake-python'),
            'if [[ "$1" == "-" ]]; then stdin_body="$(cat)"; echo "NAMESPACE_SQL_SETUP:$2:$3:$4:JARS=${6:-}:PKGS=${7:-}:IVY=${8:-}"; if [[ "${stdin_body}" == *"spark.jars.ivySettings"* && "${stdin_body}" == *"spark.jars.packages"* ]]; then echo "NAMESPACE_HAS_IVY_AND_JARS=true"; fi; exit 0; fi; echo "FAKE_PYTHON:$*"'
        );
        writeExecutableStub(path.join(stubBin, 'spark-pipelines'), 'printf "SDP_ARG=%s\\n" "$@"; echo "MASTER=${MASTER:-UNSET}"');
        writeExecutableStub(path.join(stubBin, 'gcloud'), 'printf "GCLOUD_ARG=%s\\n" "$@"');
    });

    afterEach(() => {
        fs.rmSync(tmpDir, { recursive: true, force: true });
    });

    it('runs spark-pipelines dry-run over local[*] Spark Connect with static --conf before subcommand when SPARK_JARS supplies Iceberg', () => {
        const jdk21 = createFakeJdk(path.join(tmpDir, 'jdk21'), '21.0.5');
        const res = spawnSync('bash', [path.join(projectDir, 'run_sdp_local.sh')], {
            cwd: projectDir,
            encoding: 'utf-8',
            env: {
                HOME: tmpDir,
                PATH: `${stubBin}:/usr/bin:/bin`,
                SKIP_VENV_BOOTSTRAP: 'true',
                PYTHON_BIN: 'fake-python',
                JAVA_HOME: jdk21,
                MASTER: 'yarn',
                SDP_COMMAND: 'dry-run',
                SPARK_JARS: '/opt/jars/iceberg-spark-runtime-4.2_2.13.jar',
                SPARK_PROPERTIES: 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            },
        });
        expect(res.status, res.stderr).toBe(0);
        expect(res.stdout).toContain('SDP_ARG=--remote\nSDP_ARG=local[*]');
        expect(res.stdout).toContain('SDP_ARG=--jars\nSDP_ARG=/opt/jars/iceberg-spark-runtime-4.2_2.13.jar');
        expect(res.stdout).toContain(
            'SDP_ARG=--conf\nSDP_ARG=spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
        );
        expect(res.stdout).toContain('SDP_ARG=dry-run\nSDP_ARG=--spec\nSDP_ARG=spark-pipeline.yml');
        expect(res.stdout).toContain('MASTER=UNSET');
        expect(res.stdout).not.toContain('NAMESPACE_SQL_SETUP');
    });

    it('fails fast with MISSING_LOCAL_ICEBERG_RUNTIME before starting any JVM when Iceberg is configured without SPARK_JARS, while allowing --test', () => {
        const jdk21 = createFakeJdk(path.join(tmpDir, 'jdk21'), '21.0.5');

        // 1. Iceberg in SPARK_PROPERTIES on dry-run without SPARK_JARS -> fails fast
        const propFail = spawnSync('bash', [path.join(projectDir, 'run_sdp_local.sh'), '--dry-run'], {
            cwd: projectDir,
            encoding: 'utf-8',
            env: {
                HOME: tmpDir,
                PATH: `${stubBin}:/usr/bin:/bin`,
                SKIP_VENV_BOOTSTRAP: 'true',
                PYTHON_BIN: 'fake-python',
                JAVA_HOME: jdk21,
                SPARK_PROPERTIES: 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            },
        });
        expect(propFail.status).toBe(1);
        expect(propFail.stderr).toContain('MISSING_LOCAL_ICEBERG_RUNTIME');
        expect(propFail.stderr).toContain('iceberg-spark-runtime-4.2_2.13');
        expect(propFail.stderr).toContain('./run_sdp_local.sh --test');
        expect(propFail.stdout).not.toContain('SDP_ARG=');
        expect(propFail.stdout).not.toContain('NAMESPACE_SQL_SETUP');

        // 2. Iceberg catalog in spark-pipeline.yml and format="iceberg" in transformations/*.py on run without SPARK_JARS -> fails fast
        fs.writeFileSync(
            path.join(projectDir, 'spark-pipeline.yml'),
            [
                'name: iceberg_sdp',
                'storage: gs://bucket/state',
                'catalog: lake',
                'database: sales',
                'configuration:',
                '  spark.sql.catalog.lake: "org.apache.iceberg.spark.SparkCatalog"',
                'libraries:',
                '  - glob:',
                '      include: transformations/**',
                '',
            ].join('\n')
        );
        fs.writeFileSync(
            path.join(projectDir, 'transformations', 'pipeline_datasets.py'),
            'from pyspark import pipelines as dp\n@dp.table(name="orders", format="iceberg")\ndef orders():\n    pass\n'
        );

        const specFail = spawnSync('bash', [path.join(projectDir, 'run_sdp_local.sh')], {
            cwd: projectDir,
            encoding: 'utf-8',
            env: {
                HOME: tmpDir,
                PATH: `${stubBin}:/usr/bin:/bin`,
                SKIP_VENV_BOOTSTRAP: 'true',
                PYTHON_BIN: 'fake-python',
                JAVA_HOME: jdk21,
                SDP_COMMAND: 'run',
            },
        });
        expect(specFail.status).toBe(1);
        expect(specFail.stderr).toContain('MISSING_LOCAL_ICEBERG_RUNTIME');
        expect(specFail.stdout).not.toContain('SDP_ARG=');

        // 3. Offline --test mode succeeds even when Iceberg is present in spec, transformations, and SPARK_PROPERTIES
        const testRes = spawnSync('bash', [path.join(projectDir, 'run_sdp_local.sh'), '--test'], {
            cwd: projectDir,
            encoding: 'utf-8',
            env: {
                HOME: tmpDir,
                PATH: `${stubBin}:/usr/bin:/bin`,
                SKIP_VENV_BOOTSTRAP: 'true',
                PYTHON_BIN: 'fake-python',
                JAVA_HOME: jdk21,
                SPARK_PROPERTIES: 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            },
        });
        expect(testRes.status, testRes.stderr).toBe(0);
        expect(testRes.stdout).toContain('FAKE_PYTHON:-m pytest tests/ -v');
    });

    it('writes Maven Central-only .spark-ivy/ivysettings.xml for SPARK_PACKAGES and forwards jars/packages/ivySettings into PYNAMESPACE', () => {
        const jdk21 = createFakeJdk(path.join(tmpDir, 'jdk21'), '21.0.5');
        const res = spawnSync('bash', [path.join(projectDir, 'run_sdp_local.sh')], {
            cwd: projectDir,
            encoding: 'utf-8',
            env: {
                HOME: tmpDir,
                PATH: `${stubBin}:/usr/bin:/bin`,
                SKIP_VENV_BOOTSTRAP: 'true',
                PYTHON_BIN: 'fake-python',
                JAVA_HOME: jdk21,
                SDP_COMMAND: 'run',
                SDP_CATALOG: 'spark_catalog',
                SDP_DATABASE: 'sales',
                SDP_CREATE_NAMESPACE: 'true',
                SPARK_JARS: '/opt/jars/custom-helper.jar',
                SPARK_PACKAGES: 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0',
            },
        });
        expect(res.status, res.stderr).toBe(0);

        const ivyPath = path.join(projectDir, '.spark-ivy', 'ivysettings.xml');
        expect(fs.existsSync(ivyPath)).toBe(true);
        const ivyXml = fs.readFileSync(ivyPath, 'utf-8');
        expect(ivyXml).toContain('https://repo1.maven.org/maven2/');
        expect(ivyXml).not.toContain('.m2');

        expect(res.stdout).toContain(
            `NAMESPACE_SQL_SETUP:spark-pipeline.yml:spark_catalog:sales:JARS=/opt/jars/custom-helper.jar:PKGS=org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0:IVY=${ivyPath}`
        );
        expect(res.stdout).toContain('NAMESPACE_HAS_IVY_AND_JARS=true');
        expect(res.stdout).toContain(`SDP_ARG=--conf\nSDP_ARG=spark.jars.ivySettings=${ivyPath}`);
        expect(res.stdout).toContain(`SDP_ARG=--conf\nSDP_ARG=spark.jars.ivy=${path.join(projectDir, '.spark-ivy')}`);
    });

    it('creates target namespace before run and forwards --refresh / --full-refresh flags', () => {
        const jdk21 = createFakeJdk(path.join(tmpDir, 'jdk21'), '21.0.5');
        const res = spawnSync('bash', [path.join(projectDir, 'run_sdp_local.sh')], {
            cwd: projectDir,
            encoding: 'utf-8',
            env: {
                HOME: tmpDir,
                PATH: `${stubBin}:/usr/bin:/bin`,
                SKIP_VENV_BOOTSTRAP: 'true',
                PYTHON_BIN: 'fake-python',
                JAVA_HOME: jdk21,
                SDP_COMMAND: 'run',
                SDP_CATALOG: 'lake',
                SDP_DATABASE: 'sales',
                SDP_CREATE_NAMESPACE: 'true',
                SDP_REFRESH: 'bronze_orders',
                SDP_FULL_REFRESH: 'silver_orders',
            },
        });
        expect(res.status, res.stderr).toBe(0);
        expect(res.stdout).toContain('NAMESPACE_SQL_SETUP:spark-pipeline.yml:lake:sales');
        expect(res.stdout).toContain('SDP_ARG=run\nSDP_ARG=--spec\nSDP_ARG=spark-pipeline.yml');
        expect(res.stdout).toContain('SDP_ARG=--refresh\nSDP_ARG=bronze_orders');
        expect(res.stdout).toContain('SDP_ARG=--full-refresh\nSDP_ARG=silver_orders');
    });

    it('rejects conflicting refresh options in run_sdp_local.sh (CONFLICTING_PIPELINE_REFRESH_OPTIONS)', () => {
        const jdk21 = createFakeJdk(path.join(tmpDir, 'jdk21'), '21.0.5');
        const overlap = spawnSync('bash', [path.join(projectDir, 'run_sdp_local.sh')], {
            cwd: projectDir,
            encoding: 'utf-8',
            env: {
                HOME: tmpDir,
                PATH: `${stubBin}:/usr/bin:/bin`,
                SKIP_VENV_BOOTSTRAP: 'true',
                PYTHON_BIN: 'fake-python',
                JAVA_HOME: jdk21,
                SDP_REFRESH: 'bronze_orders,silver_orders',
                SDP_FULL_REFRESH: 'silver_orders',
            },
        });
        expect(overlap.status).toBe(1);
        expect(overlap.stderr).toContain('CONFLICTING_PIPELINE_REFRESH_OPTIONS');

        const fullAllConflict = spawnSync('bash', [path.join(projectDir, 'run_sdp_local.sh')], {
            cwd: projectDir,
            encoding: 'utf-8',
            env: {
                HOME: tmpDir,
                PATH: `${stubBin}:/usr/bin:/bin`,
                SKIP_VENV_BOOTSTRAP: 'true',
                PYTHON_BIN: 'fake-python',
                JAVA_HOME: jdk21,
                SDP_FULL_REFRESH_ALL: 'true',
                SDP_REFRESH: 'bronze_orders',
            },
        });
        expect(fullAllConflict.status).toBe(1);
        expect(fullAllConflict.stderr).toContain('CONFLICTING_PIPELINE_REFRESH_OPTIONS');
    });

    it('stages SDP project to GCS and submits standalone Dataproc 4.0 wrapper via run_sdp_dataproc.sh', () => {
        const res = spawnSync('bash', [path.join(projectDir, 'run_sdp_dataproc.sh')], {
            cwd: projectDir,
            encoding: 'utf-8',
            env: {
                HOME: tmpDir,
                PATH: `${stubBin}:/usr/bin:/bin`,
                PROJECT_ID: 'sdp-dev-proj',
                REGION: 'us-central1',
                CLUSTER_NAME: 'sdp-cluster-40',
                DEPS_BUCKET: 'gs://sdp-staging-bucket',
                SDP_COMMAND: 'run',
                SDP_CATALOG: 'lake',
                SDP_DATABASE: 'sales',
                SDP_CREATE_NAMESPACE: 'true',
                SDP_FULL_REFRESH_ALL: 'true',
            },
        });
        expect(res.status, res.stderr).toBe(0);
        expect(res.stdout).toContain('GCLOUD_ARG=storage\nGCLOUD_ARG=cp\nGCLOUD_ARG=-r');
        expect(res.stdout).toContain('GCLOUD_ARG=dataproc\nGCLOUD_ARG=jobs\nGCLOUD_ARG=submit\nGCLOUD_ARG=pyspark');
        expect(res.stdout).toContain('GCLOUD_ARG=--cluster=sdp-cluster-40');
        expect(res.stdout).toContain('GCLOUD_ARG=--create_namespace=lake.sales');
        expect(res.stdout).toContain('GCLOUD_ARG=--full-refresh-all');
    });
});
