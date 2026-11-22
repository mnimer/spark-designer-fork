/**
 * Unit tests for the read-only check runner (`src/codegen/ReadOnlyCheckRunner.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Uses a fake command executor so no process spawns except in the tests that exercise
 * `defaultCommandExecutor`. Covers both Imperative PySpark (`required_files`, `leftover_tokens`,
 * `python_syntax`, `pipeline_contract`, `test_imports`, `pytest_collect`) and Spark Declarative
 * Pipelines (`sdp_spec_contract`, `pipeline_contract` purity and Databricks API bans,
 * `test_imports` for `src.transforms`, and `sdp_dry_run`).
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { afterEach, describe, it, expect, vi } from 'vitest';
import {
    CHECK_IDS,
    CommandExecutor,
    GeneratedFile,
    ReadOnlyCheckRunner,
    buildCheckCommandEnv,
    cleanupCheckWorkspaceArtifacts,
    defaultCommandExecutor,
    expectedSourceFiles,
    isSdpDryRunEnvironmentSkip,
    isSdpGeneratedProject,
    isStaticSdpSparkConfigKey,
    resolveCheckCacheRoot,
    scanSdpPythonViolations,
    truncateCheckOutput,
    validateSdpSpecYaml,
} from '../../../codegen/ReadOnlyCheckRunner';
import { READ_ONLY_CHECK_LIMITS } from '../../../codegen/codegenConfig';

/** A complete, valid imperative PySpark 4.x project. */
const PYSPARK_FILES: GeneratedFile[] = [
    { filename: 'requirements.txt', content: 'pyspark>=4.0.0\npytest>=8.0.0\n' },
    {
        filename: 'src/pipeline.py',
        content: [
            'def read_sources(spark, config):',
            '    return {}',
            '',
            'def transform(dfs, config):',
            '    return dfs',
            '',
            'def write_sinks(dfs, config):',
            '    pass',
            '',
        ].join('\n'),
    },
    {
        filename: 'tests/test_pipeline.py',
        content: 'from src.pipeline import transform\n\ndef test_transform():\n    assert transform({}, {}) == {}\n',
    },
    { filename: 'run_local.sh', content: '#!/usr/bin/env bash\n' },
];

/** A complete, valid Spark Declarative Pipeline (SDP) project. */
const SDP_FILES: GeneratedFile[] = [
    { filename: 'requirements.txt', content: 'pyspark>=4.2.0,<4.3\npytest>=8.0.0\n' },
    {
        filename: 'spark-pipeline.yml',
        content: [
            'name: medallion_orders',
            'storage: gs://my-sdp-checkpoints/medallion',
            'catalog: lake',
            'database: retail_medallion',
            'configuration:',
            '  spark.sql.shuffle.partitions: "200"',
            '  spark.sql.catalog.lake: "org.apache.iceberg.spark.SparkCatalog"',
            '  spark.sql.catalog.lake.type: "rest"',
            'libraries:',
            '  - glob:',
            '      include: transformations/**',
            '',
        ].join('\n'),
    },
    {
        filename: 'transformations/medallion_datasets.py',
        content: [
            'from pyspark import pipelines as dp',
            'from pyspark.sql import DataFrame, SparkSession',
            'from pyspark.sql.types import DoubleType, StringType, StructField, StructType',
            'from src.transforms import clean_orders, summarize_daily_orders',
            '',
            'spark = SparkSession.active()',
            '',
            'ORDERS_SCHEMA = StructType([',
            '    StructField("order_id", StringType()),',
            '    StructField("store_id", StringType()),',
            '    StructField("amount", DoubleType()),',
            '])',
            '',
            '@dp.table(name="bronze_orders", format="iceberg")',
            'def bronze_orders() -> DataFrame:',
            '    return (',
            '        spark.readStream.format("csv")',
            '        .option("header", "true")',
            '        .schema(ORDERS_SCHEMA)',
            '        .load("gs://raw-orders/incoming/")',
            '    )',
            '',
            '@dp.table(name="silver_orders", format="iceberg")',
            'def silver_orders() -> DataFrame:',
            '    return clean_orders(spark.readStream.table("bronze_orders"))',
            '',
            '@dp.materialized_view(name="gold_store_revenue", format="iceberg")',
            'def gold_store_revenue() -> DataFrame:',
            '    return summarize_daily_orders(spark.read.table("silver_orders"))',
            '',
        ].join('\n'),
    },
    {
        filename: 'src/transforms.py',
        content: [
            'from pyspark.sql import DataFrame',
            'from pyspark.sql import functions as F',
            '',
            'def clean_orders(df: DataFrame) -> DataFrame:',
            '    return df.filter(F.col("amount") > 0)',
            '',
            'def summarize_daily_orders(df: DataFrame) -> DataFrame:',
            '    return df.groupBy("store_id").agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue"))',
            '',
        ].join('\n'),
    },
    {
        filename: 'tests/test_transforms.py',
        content: 'from src.transforms import clean_orders\n\ndef test_clean_orders():\n    assert callable(clean_orders)\n',
    },
    { filename: 'run_sdp_local.sh', content: '#!/usr/bin/env bash\n' },
];

/**
 * Creates a fake executor that returns one outcome or a sequence of outcomes.
 *
 * @param outcomes Outcome(s) to return.
 * @returns Executor mock.
 */
function fakeExecutor(
    ...outcomes: Array<{ exitCode: number; stdout?: string; stderr?: string; notFound?: boolean }>
) {
    const queue = [...outcomes];
    const fallback = outcomes[outcomes.length - 1] ?? { exitCode: 0 };
    return vi.fn<CommandExecutor>(async () => {
        const next = queue.shift() ?? fallback;
        return { stdout: '', stderr: '', ...next };
    });
}

describe('ReadOnlyCheckRunner.run (PySpark Imperative)', () => {
    it('passes every check for a complete PySpark project (simple)', async () => {
        const executor = fakeExecutor({ exitCode: 0 }, { exitCode: 0, stdout: '1 test collected' });
        const results = await new ReadOnlyCheckRunner(executor).run({
            files: PYSPARK_FILES,
            language: 'python',
            baseName: 'orders',
            runnerFiles: ['run_local.sh'],
        });
        expect(results.map((r) => [r.check, r.status])).toEqual([
            [CHECK_IDS.requiredFiles, 'passed'],
            [CHECK_IDS.leftoverTokens, 'passed'],
            [CHECK_IDS.pythonSyntax, 'passed'],
            [CHECK_IDS.pipelineContract, 'passed'],
            [CHECK_IDS.testImports, 'passed'],
            [CHECK_IDS.pytestCollect, 'passed'],
        ]);
        // First call is py_compile, second call is pytest --collect-only -q.
        expect(executor).toHaveBeenCalledTimes(2);
        expect(executor.mock.calls[0][1]).toEqual(['-m', 'py_compile', 'src/pipeline.py', 'tests/test_pipeline.py']);
        expect(executor.mock.calls[1][1]).toEqual(['-m', 'pytest', '--collect-only', '-q']);
    });

    it('reports missing files, leftover tokens, syntax errors, missing pipeline functions, wrong test imports, and pytest collection failure (failure)', async () => {
        const files: GeneratedFile[] = [
            {
                filename: 'src/pipeline.py',
                content: 'def read_sources(spark, config):\n    x = "{{pipeline_name}}" + "__REDACTED_BY_SPARK_DESIGNER__"\n',
            },
            { filename: 'tests/test_pipeline.py', content: 'import unittest\n' },
            { filename: '../escape.py', content: 'evil' },
        ];
        const executor = fakeExecutor(
            { exitCode: 1, stderr: 'SyntaxError: bad' },
            { exitCode: 2, stderr: 'ERROR collecting tests/test_pipeline.py' }
        );
        const results = await new ReadOnlyCheckRunner(executor).run({
            files,
            language: 'python',
            baseName: 'orders',
            runnerFiles: ['run_local.sh'],
        });
        const byId = Object.fromEntries(results.map((r) => [r.check, r]));
        expect(byId.required_files.output).toBe('Missing file(s): requirements.txt, run_local.sh');
        expect(byId.leftover_tokens.output).toBe(
            'src/pipeline.py: {{pipeline_name}}, __REDACTED_BY_SPARK_DESIGNER__'
        );
        expect(byId.python_syntax).toEqual({
            check: 'python_syntax',
            status: 'failed',
            output: 'SyntaxError: bad',
        });
        expect(byId.pipeline_contract).toEqual({
            check: 'pipeline_contract',
            status: 'failed',
            output: 'src/pipeline.py is missing required testable entrypoint function(s): transform, write_sinks.',
        });
        expect(byId.test_imports.status).toBe('failed');
        expect(byId.pytest_collect).toEqual({
            check: 'pytest_collect',
            status: 'failed',
            output: 'ERROR collecting tests/test_pipeline.py',
        });
        // The out-of-bounds file is never written or compiled.
        expect(executor.mock.calls[0][1]).toEqual(['-m', 'py_compile', 'src/pipeline.py', 'tests/test_pipeline.py']);
    });

    it('gracefully skips py_compile and pytest when python3 or pytest is absent (extreme)', async () => {
        const missingPython = await new ReadOnlyCheckRunner(
            fakeExecutor({ exitCode: -1, notFound: true }),
            'nopython'
        ).run({
            files: PYSPARK_FILES,
            language: 'python',
            baseName: 'orders',
            runnerFiles: [],
        });
        const byIdMissingPy = Object.fromEntries(missingPython.map((r) => [r.check, r]));
        expect(byIdMissingPy.python_syntax).toEqual({
            check: 'python_syntax',
            status: 'skipped',
            output: "Python interpreter 'nopython' not found.",
        });
        expect(byIdMissingPy.pytest_collect).toEqual({
            check: 'pytest_collect',
            status: 'skipped',
            output: "Python interpreter 'nopython' not found.",
        });

        // Python exists, but pytest module is not installed.
        const missingPytest = await new ReadOnlyCheckRunner(
            fakeExecutor(
                { exitCode: 0 },
                { exitCode: 1, stderr: '/usr/bin/python3: No module named pytest' }
            )
        ).run({
            files: PYSPARK_FILES,
            language: 'python',
            baseName: 'orders',
            runnerFiles: [],
        });
        const byIdMissingPytest = Object.fromEntries(missingPytest.map((r) => [r.check, r]));
        expect(byIdMissingPytest.python_syntax.status).toBe('passed');
        expect(byIdMissingPytest.pytest_collect.status).toBe('skipped');

        // Empty file list skips syntax, contract, imports, and pytest.
        const none = await new ReadOnlyCheckRunner(fakeExecutor({ exitCode: 0 })).run({
            files: [],
            language: 'python',
            baseName: 'orders',
            runnerFiles: [],
        });
        const byIdNone = Object.fromEntries(none.map((r) => [r.check, r]));
        expect(byIdNone.python_syntax.status).toBe('skipped');
        expect(byIdNone.pipeline_contract.status).toBe('skipped');
        expect(byIdNone.test_imports.status).toBe('skipped');
        expect(byIdNone.pytest_collect.status).toBe('skipped');
    });

    it('accepts every supported test import form for src.pipeline and src.<baseName> (extreme)', async () => {
        for (const line of [
            'from src.pipeline import transform',
            'import src.pipeline',
            'from pipeline import transform',
            'import pipeline',
            'from src.orders import transform',
            '  from src.pipeline import (read_sources, transform, write_sinks)',
        ]) {
            const files = [{ filename: 'tests/test_pipeline.py', content: `# test\n${line}\n` }];
            const results = await new ReadOnlyCheckRunner(fakeExecutor({ exitCode: 0 })).run({
                files,
                language: 'python',
                baseName: 'orders',
                runnerFiles: [],
            });
            const byId = Object.fromEntries(results.map((r) => [r.check, r]));
            expect(byId.test_imports.status, line).toBe('passed');
        }
    });
});

describe('ReadOnlyCheckRunner.run (Spark Declarative Pipelines)', () => {
    it('auto-detects SDP mode from spark-pipeline.yml and passes every check when dry-run succeeds', async () => {
        const executor = fakeExecutor(
            { exitCode: 0 },
            { exitCode: 0, stdout: '1 test collected' },
            { exitCode: 0, stdout: 'Run is COMPLETED.' }
        );
        const results = await new ReadOnlyCheckRunner(executor).run({
            files: SDP_FILES,
            language: 'python',
            baseName: 'medallion_orders',
            runnerFiles: ['run_sdp_local.sh'],
        });
        expect(results.map((r) => [r.check, r.status])).toEqual([
            [CHECK_IDS.requiredFiles, 'passed'],
            [CHECK_IDS.leftoverTokens, 'passed'],
            [CHECK_IDS.pythonSyntax, 'passed'],
            [CHECK_IDS.sdpSpecContract, 'passed'],
            [CHECK_IDS.pipelineContract, 'passed'],
            [CHECK_IDS.testImports, 'passed'],
            [CHECK_IDS.pytestCollect, 'passed'],
            [CHECK_IDS.sdpDryRun, 'passed'],
        ]);
        expect(executor).toHaveBeenCalledTimes(3);
        expect(executor.mock.calls[2][0]).toBe('spark-pipelines');
        expect(executor.mock.calls[2][1]).toEqual(['dry-run', '--spec', 'spark-pipeline.yml']);
    });

    it('gracefully skips sdp_dry_run when spark-pipelines CLI or Spark 4.2 environment is missing', async () => {
        const missingCliExecutor = fakeExecutor(
            { exitCode: 0 },
            { exitCode: 0, stdout: '1 test collected' },
            { exitCode: -1, notFound: true }
        );
        const missingCliResults = await new ReadOnlyCheckRunner(missingCliExecutor).run({
            files: SDP_FILES,
            language: 'python',
            baseName: 'medallion_orders',
            runnerFiles: [],
        });
        const byIdMissing = Object.fromEntries(missingCliResults.map((r) => [r.check, r]));
        expect(byIdMissing.sdp_dry_run.status).toBe('skipped');
        expect(byIdMissing.sdp_dry_run.output).toContain("CLI 'spark-pipelines' not installed");

        const missingSpark42Executor = fakeExecutor(
            { exitCode: 0 },
            { exitCode: 0, stdout: '1 test collected' },
            { exitCode: 1, stderr: "ModuleNotFoundError: No module named 'pyspark.pipelines'" }
        );
        const missingSpark42Results = await new ReadOnlyCheckRunner(missingSpark42Executor).run({
            files: SDP_FILES,
            language: 'python',
            baseName: 'medallion_orders',
            runnerFiles: [],
        });
        const byIdSpark42 = Object.fromEntries(missingSpark42Results.map((r) => [r.check, r]));
        expect(byIdSpark42.sdp_dry_run.status).toBe('skipped');
    });

    it('fails sdp_dry_run when spark-pipelines reports a real graph resolution error', async () => {
        const executor = fakeExecutor(
            { exitCode: 0 },
            { exitCode: 0, stdout: '1 test collected' },
            { exitCode: 1, stderr: 'INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW' }
        );
        const results = await new ReadOnlyCheckRunner(executor).run({
            files: SDP_FILES,
            language: 'python',
            baseName: 'medallion_orders',
            runnerFiles: [],
        });
        const byId = Object.fromEntries(results.map((r) => [r.check, r]));
        expect(byId.sdp_dry_run.status).toBe('failed');
        expect(byId.sdp_dry_run.output).toContain('INVALID_FLOW_QUERY_TYPE');
    });

    it('rejects having both spark-pipeline.yml and spark-pipeline.yaml (MULTIPLE_PIPELINE_SPEC_FILES_FOUND)', async () => {
        const files: GeneratedFile[] = [
            ...SDP_FILES,
            { filename: 'spark-pipeline.yaml', content: SDP_FILES[1].content },
        ];
        const results = await new ReadOnlyCheckRunner(fakeExecutor({ exitCode: 0 })).run({
            files,
            language: 'python',
            baseName: 'medallion_orders',
            runnerFiles: [],
            paradigm: 'sdp',
        });
        const byId = Object.fromEntries(results.map((r) => [r.check, r]));
        expect(byId.required_files.status).toBe('failed');
        expect(byId.required_files.output).toContain('MULTIPLE_PIPELINE_SPEC_FILES_FOUND');
        expect(byId.sdp_spec_contract.status).toBe('failed');
        expect(byId.sdp_spec_contract.output).toContain('MULTIPLE_PIPELINE_SPEC_FILES_FOUND');
    });

    it('enforces spark-pipeline.yml field allowlist, URI storage, plain catalog, string config, static-key ban, and glob rules', () => {
        const badSpec = [
            'name: bad_sdp',
            'continuous: true',
            'trigger: "1 minute"',
            'storage: /tmp/local_dir_without_scheme',
            'catalog: my-hyphenated-catalog',
            'configuration:',
            '  spark.sql.shuffle.partitions: 200',
            '  spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"',
            '  spark.jars.packages: "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.9.0"',
            '  spark.driver.memory: "4g"',
            '  spark.executor.cores: "4"',
            '  spark.dynamicAllocation.enabled: "true"',
            'libraries:',
            '  - glob:',
            '      include: transformations/*.py',
        ].join('\n');

        const violations = validateSdpSpecYaml('spark-pipeline.yml', badSpec);
        const codes = violations.map((v) => v.code);
        expect(codes).toContain('PIPELINE_SPEC_UNEXPECTED_FIELD');
        expect(codes).toContain('PIPELINE_STORAGE_ROOT_INVALID');
        expect(codes).toContain('INVALID_IDENTIFIER');
        expect(codes).toContain('PIPELINE_SPEC_DICT_VALUE_NOT_STRING');
        expect(codes).toContain('CANNOT_MODIFY_STATIC_CONFIG');
        expect(codes).toContain('PIPELINE_SPEC_INVALID_GLOB_PATTERN');
    });

    it('accepts Jinja template variables in Managed Data Pipeline spark-pipeline.yml without false positives', async () => {
        const jinjaSpec = [
            'name: "{{ pipeline_id }}"',
            'storage: "{{ storage_uri }}"',
            'catalog: "{{ catalog_alias }}"',
            'database: retail',
            'libraries:',
            '  - glob:',
            '      include: transformations/**',
        ].join('\n');
        expect(validateSdpSpecYaml('spark-pipeline.yml', jinjaSpec)).toEqual([]);

        const files = SDP_FILES.map((f) =>
            f.filename === 'spark-pipeline.yml' ? { ...f, content: jinjaSpec } : f
        );
        const results = await new ReadOnlyCheckRunner(fakeExecutor({ exitCode: 0 })).run({
            files,
            language: 'python',
            baseName: 'medallion_orders',
            runnerFiles: [],
            paradigm: 'sdp',
        });
        const byId = Object.fromEntries(results.map((r) => [r.check, r]));
        expect(byId.leftover_tokens.status).toBe('passed');
        expect(byId.sdp_spec_contract.status).toBe('passed');
    });

    it('detects every banned session mutation, eager action, writer, and Databricks API in transformations/*.py while ignoring comments and docstrings', () => {
        const dirtyTransformations: GeneratedFile[] = [
            {
                filename: 'transformations/bad_flows.py',
                content: [
                    '"""Docstring mentioning df.collect(), df.schema, and SparkSession.builder."""',
                    '# Comment mentioning spark.conf.set and import dlt',
                    'import dlt',
                    'from pyspark.sql import SparkSession',
                    'spark = SparkSession.builder.getOrCreate()',
                    'spark.conf.set("spark.sql.shuffle.partitions", "10")',
                    'spark.catalog.setCurrentCatalog("lake")',
                    'spark.udf.register("my_udf", lambda x: x)',
                    '',
                    '@dlt.view',
                    '@dlt.expect_or_drop("valid_id", "id IS NOT NULL")',
                    'def raw_view():',
                    '    df = spark.readStream.format("cloudFiles").load("gs://b/in")',
                    '    _ = df.schema',
                    '    _ = df.columns',
                    '    _ = df.count()',
                    '    _ = df.collect()',
                    '    df.createOrReplaceTempView("tmp")',
                    '    df.write.saveAsTable("bad_out")',
                    '    return df',
                    '',
                    '@dp.append_flow(target="t", once=True)',
                    'def backfill_once():',
                    '    return spark.readStream.table("src")',
                    '',
                    'dlt.apply_changes(target="t", source="s", keys=["id"], sequence_by="ts", stored_as_scd_type=2)',
                ].join('\n'),
            },
        ];

        const violations = scanSdpPythonViolations(dirtyTransformations);
        const codes = new Set(violations.map((v) => v.code));
        expect(codes.has('SDP_BANNED_SESSION_BUILDER')).toBe(true);
        expect(codes.has('SESSION_MUTATION_IN_DECLARATIVE_PIPELINE')).toBe(true);
        expect(codes.has('ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION')).toBe(true);
        expect(codes.has('SDP_BANNED_DATABRICKS_API')).toBe(true);
        expect(codes.has('AUTOCDC_SCD2_NOT_SUPPORTED')).toBe(true);

        // Clean SDP files (which call .schema(ORDERS_SCHEMA) and F.count("*")) have zero violations!
        expect(scanSdpPythonViolations(SDP_FILES)).toEqual([]);
    });

    it('accepts supported test_transforms.py import styles and rejects missing src.transforms import', async () => {
        for (const importLine of [
            'from src.transforms import clean_orders',
            'import src.transforms',
            'from src import transforms',
            'from transforms import clean_orders',
            'import transforms',
        ]) {
            const files = SDP_FILES.map((f) =>
                f.filename === 'tests/test_transforms.py'
                    ? { ...f, content: `${importLine}\n\ndef test_ok():\n    assert True\n` }
                    : f
            );
            const results = await new ReadOnlyCheckRunner(fakeExecutor({ exitCode: 0 })).run({
                files,
                language: 'python',
                baseName: 'medallion',
                runnerFiles: [],
                paradigm: 'sdp',
            });
            const byId = Object.fromEntries(results.map((r) => [r.check, r]));
            expect(byId.test_imports.status, importLine).toBe('passed');
        }

        const badTestFiles = SDP_FILES.map((f) =>
            f.filename === 'tests/test_transforms.py'
                ? { ...f, content: 'import pytest\n\ndef test_noop():\n    assert True\n' }
                : f
        );
        const badResults = await new ReadOnlyCheckRunner(fakeExecutor({ exitCode: 0 })).run({
            files: badTestFiles,
            language: 'python',
            baseName: 'medallion',
            runnerFiles: [],
            paradigm: 'sdp',
        });
        const badById = Object.fromEntries(badResults.map((r) => [r.check, r]));
        expect(badById.test_imports.status).toBe('failed');
    });
});

describe('helpers', () => {
    it('identifies SDP vs imperative project file lists and static Spark keys accurately', () => {
        expect(isSdpGeneratedProject(PYSPARK_FILES)).toBe(false);
        expect(isSdpGeneratedProject(SDP_FILES)).toBe(true);
        expect(isSdpGeneratedProject(PYSPARK_FILES, 'sdp')).toBe(true);
        expect(isSdpGeneratedProject(SDP_FILES, 'imperative')).toBe(false);

        expect(isStaticSdpSparkConfigKey('spark.sql.extensions')).toBe(true);
        expect(isStaticSdpSparkConfigKey('spark.jars.packages')).toBe(true);
        expect(isStaticSdpSparkConfigKey('spark.driver.memory')).toBe(true);
        expect(isStaticSdpSparkConfigKey('spark.executor.instances')).toBe(true);
        expect(isStaticSdpSparkConfigKey('spark.dynamicAllocation.enabled')).toBe(true);
        expect(isStaticSdpSparkConfigKey('spark.sql.shuffle.partitions')).toBe(false);
        expect(isStaticSdpSparkConfigKey('spark.sql.catalog.lake')).toBe(false);

        expect(isSdpDryRunEnvironmentSkip('JAVA_HOME is not set')).toBe(true);
        expect(isSdpDryRunEnvironmentSkip('PIPELINE_SPEC_UNEXPECTED_FIELD')).toBe(false);
    });

    it('truncates long output with a marker (extreme)', () => {
        const long = 'x'.repeat(READ_ONLY_CHECK_LIMITS.maxOutputChars + 5);
        expect(truncateCheckOutput(long).endsWith('...[truncated]')).toBe(true);
        expect(truncateCheckOutput('short')).toBe('short');
    });

    it('returns the canonical PySpark source paths (simple)', () => {
        expect(expectedSourceFiles('python', 'orders')).toEqual(['src/pipeline.py', 'tests/test_pipeline.py']);
    });

    it('reports notFound from the default executor for a missing binary (failure)', async () => {
        const outcome = await defaultCommandExecutor('definitely-not-a-real-binary-xyz', [], {
            cwd: process.cwd(),
            timeoutMs: 5000,
        });
        expect(outcome.notFound).toBe(true);
        expect(outcome.exitCode).toBe(-1);
    });

    it('maps a killed process (timeout, no numeric exit code) to exit code 1 (failure)', async () => {
        const killed = await defaultCommandExecutor(process.execPath, ['-e', 'setTimeout(() => {}, 5000)'], {
            cwd: process.cwd(),
            timeoutMs: 100,
        });
        expect(killed.exitCode).toBe(1);
        expect(killed.notFound).toBeUndefined();
    });

    it('returns the exit code and output from the default executor (simple)', async () => {
        const ok = await defaultCommandExecutor(process.execPath, ['-e', 'process.stdout.write("hi")'], {
            cwd: process.cwd(),
            timeoutMs: 5000,
        });
        expect(ok).toEqual({ exitCode: 0, stdout: 'hi', stderr: '' });
        const bad = await defaultCommandExecutor(
            process.execPath,
            ['-e', 'process.stderr.write("no");process.exit(3)'],
            { cwd: process.cwd(), timeoutMs: 5000 }
        );
        expect(bad.exitCode).toBe(3);
        expect(bad.stderr).toBe('no');
    });
});

describe('disk hygiene and cache isolation (CLN11)', () => {
    const tempDirs: string[] = [];

    /**
     * Creates a temporary directory in `os.tmpdir()` tracked for `afterEach` cleanup.
     *
     * @param prefix Directory name prefix.
     * @returns Absolute path of the created directory.
     */
    function createTempRoot(prefix: string): string {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
        tempDirs.push(dir);
        return dir;
    }

    afterEach(() => {
        while (tempDirs.length > 0) {
            const dir = tempDirs.pop();
            if (dir) {
                fs.rmSync(dir, { recursive: true, force: true });
            }
        }
    });

    it('routes PIP_CACHE_DIR and Ivy to shared cacheRoot and isolates XDG_CACHE_HOME across syntax, pytest, and sdp_dry_run', async () => {
        const cacheRoot = createTempRoot('check-shared-cache-');
        const observedCwds: string[] = [];
        const observedEnvs: Array<Record<string, string | undefined> | undefined> = [];

        const executor = vi.fn<CommandExecutor>(async (_cmd, _args, opts) => {
            observedCwds.push(opts.cwd);
            observedEnvs.push(opts.env);
            expect(fs.existsSync(path.join(opts.cwd, '.xdg-cache'))).toBe(true);
            return { exitCode: 0, stdout: 'ok', stderr: '' };
        });

        const runner = new ReadOnlyCheckRunner(executor, 'python3', 'spark-pipelines', cacheRoot, {
            PATH: process.env.PATH,
        });
        const results = await runner.run({
            files: SDP_FILES,
            language: 'python',
            baseName: 'medallion_orders',
            runnerFiles: ['run_sdp_local.sh'],
        });

        expect(results.every((r) => r.status === 'passed')).toBe(true);
        expect(observedEnvs).toHaveLength(3);
        for (let i = 0; i < observedEnvs.length; i++) {
            const env = observedEnvs[i];
            const cwd = observedCwds[i];
            expect(env?.PIP_CACHE_DIR).toBe(path.join(cacheRoot, 'pip'));
            expect(env?.SPARK_IVY_CACHE_DIR).toBe(path.join(cacheRoot, 'ivy2'));
            expect(env?.XDG_CACHE_HOME).toBe(path.join(cwd, '.xdg-cache'));
            expect(env?.PYTHONDONTWRITEBYTECODE).toBe('1');
            expect(env?.PYTEST_ADDOPTS).toContain('-p no:cacheprovider');
            expect(fs.existsSync(cwd)).toBe(false);
        }
    });

    it('cleans per-run .venv, .spark-ivy, .pytest_cache, __pycache__, and XDG scratch in finally on failure or thrown timeout while preserving cacheRoot', async () => {
        const cacheRoot = createTempRoot('check-cache-protect-');
        const sharedPipWheel = path.join(cacheRoot, 'pip', 'cached.whl');
        const sharedJdkRelease = path.join(cacheRoot, 'jdk-21', 'jdk-21.0.13', 'release');
        fs.mkdirSync(path.dirname(sharedPipWheel), { recursive: true });
        fs.mkdirSync(path.dirname(sharedJdkRelease), { recursive: true });
        fs.writeFileSync(sharedPipWheel, 'wheel', 'utf8');
        fs.writeFileSync(sharedJdkRelease, 'JAVA_VERSION="21.0.13"\n', 'utf8');

        let leakedWorkDir = '';
        const throwingExecutor = vi.fn<CommandExecutor>(async (_cmd, args, opts) => {
            leakedWorkDir = opts.cwd;
            await fs.promises.mkdir(path.join(opts.cwd, '.venv'), { recursive: true });
            await fs.promises.mkdir(path.join(opts.cwd, '.spark-ivy', 'jars'), { recursive: true });
            await fs.promises.mkdir(path.join(opts.cwd, '.pytest_cache'), { recursive: true });
            await fs.promises.mkdir(path.join(opts.cwd, 'src', '__pycache__'), { recursive: true });
            await fs.promises.writeFile(path.join(opts.cwd, 'src', '__pycache__', 'pipeline.cpython-312.pyc'), 'pyc');
            if (opts.env?.XDG_CACHE_HOME) {
                await fs.promises.writeFile(path.join(opts.env.XDG_CACHE_HOME, 'scratch.tmp'), 'tmp');
            }
            if (args.includes('pytest')) {
                throw new Error('Command timed out');
            }
            return { exitCode: 0, stdout: '', stderr: '' };
        });

        await expect(
            new ReadOnlyCheckRunner(throwingExecutor).run({
                files: PYSPARK_FILES,
                language: 'python',
                baseName: 'orders',
                runnerFiles: ['run_local.sh'],
                cacheRoot,
            })
        ).rejects.toThrow('Command timed out');

        expect(leakedWorkDir).not.toBe('');
        expect(fs.existsSync(leakedWorkDir)).toBe(false);
        expect(fs.readFileSync(sharedPipWheel, 'utf8')).toBe('wheel');
        expect(fs.readFileSync(sharedJdkRelease, 'utf8')).toContain('21.0.13');
    });

    it('cleans per-run artifacts inside an existing workspace when removeProjectDir is false and honors boundary overrides', async () => {
        const root = createTempRoot('check-inplace-clean-');
        const cacheRoot = path.join(root, 'shared-cache');
        const projectDir = path.join(root, 'project');

        fs.mkdirSync(path.join(cacheRoot, 'ivy2'), { recursive: true });
        fs.writeFileSync(path.join(cacheRoot, 'ivy2', 'keep.jar'), 'jar', 'utf8');

        fs.mkdirSync(path.join(projectDir, 'src', '__pycache__'), { recursive: true });
        fs.mkdirSync(path.join(projectDir, 'tests', '__pycache__'), { recursive: true });
        fs.mkdirSync(path.join(projectDir, '.venv'), { recursive: true });
        fs.mkdirSync(path.join(projectDir, '.spark-ivy'), { recursive: true });
        fs.mkdirSync(path.join(projectDir, '.pytest_cache'), { recursive: true });
        fs.mkdirSync(path.join(projectDir, '.xdg-cache'), { recursive: true });
        fs.writeFileSync(path.join(projectDir, 'src', 'pipeline.py'), 'x = 1\n', 'utf8');

        expect(resolveCheckCacheRoot(cacheRoot, {})).toBe(path.resolve(cacheRoot));
        const builtEnv = buildCheckCommandEnv(projectDir, { cacheRoot, baseEnv: { PYTEST_ADDOPTS: '-k smoke' } });
        expect(builtEnv.env.PYTEST_ADDOPTS).toBe('-k smoke -p no:cacheprovider');

        const removed = await cleanupCheckWorkspaceArtifacts(projectDir, {
            cacheRoot,
            xdgScratchDir: builtEnv.xdgScratchDir,
            removeProjectDir: false,
        });
        expect(removed.length).toBeGreaterThanOrEqual(6);
        expect(fs.existsSync(path.join(projectDir, 'src', 'pipeline.py'))).toBe(true);
        expect(fs.existsSync(path.join(projectDir, 'src', '__pycache__'))).toBe(false);
        expect(fs.existsSync(path.join(projectDir, 'tests', '__pycache__'))).toBe(false);
        expect(fs.existsSync(path.join(projectDir, '.venv'))).toBe(false);
        expect(fs.existsSync(path.join(projectDir, '.spark-ivy'))).toBe(false);
        expect(fs.existsSync(path.join(projectDir, '.pytest_cache'))).toBe(false);
        expect(fs.existsSync(path.join(projectDir, '.xdg-cache'))).toBe(false);

        // Boundary guard: never deletes shared cacheRoot even if passed directly as projectDir.
        await cleanupCheckWorkspaceArtifacts(cacheRoot, { cacheRoot, removeProjectDir: true });
        expect(fs.readFileSync(path.join(cacheRoot, 'ivy2', 'keep.jar'), 'utf8')).toBe('jar');
    });
});
