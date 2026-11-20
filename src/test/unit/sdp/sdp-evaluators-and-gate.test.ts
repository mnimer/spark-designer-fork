/**
 * Unit Tests for SDP Deterministic Gate, Specialist Evaluators, and Planner Edge Cases (SDP10).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies Phase 12 SDP static validation and multi-agent evaluator extensions across
 * `deterministic-gate.ts`, `spark-architecture-evaluator.ts`, `pipeline-spec-evaluator.ts`,
 * `packaging-evaluator.ts`, and edge cases in `sdpPlanner.ts`. All tests run offline.
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { describe, it, expect, vi } from 'vitest';
import type { GoogleGenAI } from '@google/genai';
import {
    checkDeliverableCompleteness,
    checkSdpStructureAndSpec,
    detectRepositoryParadigm,
    GeneratedFileCandidate,
    runDeterministicGate,
    runOptionalSdpGateDryRun,
    scanForBannedSparkPatterns,
} from '../../integration/evaluators/deterministic-gate';
import {
    buildSparkArchitecturePrompt,
    collectSparkArchitectureSignals,
    evaluateSparkArchitecture,
    resolveSparkArchitectureParadigm,
    runSdpArchitectureStaticChecks,
    runSparkArchitectureStaticChecks,
} from '../../integration/evaluators/spark-architecture-evaluator';
import {
    auditSdpPlanCompliance,
    buildSpecSdpExecutionPlan,
    evaluatePipelineSpecification,
    generateRequirementMatrix,
    parsePipelineSpec,
} from '../../integration/evaluators/pipeline-spec-evaluator';
import {
    auditRequirementsTxt,
    auditSdpRunnerScriptsAndStageYaml,
    evaluatePackagingAndDeployability,
    pysparkSupportsSdp42,
    runDeterministicPackagingAudit,
} from '../../integration/evaluators/packaging-evaluator';
import {
    buildSdpPlan,
    checkNodeSdpCompatibility,
    evaluateSdpCompatibility,
    sanitizeSdpIdentifier,
} from '../../../shared/sdpPlanner';

/** Builds a complete, valid SDP repository candidate payload. */
function buildValidSdpRepo(overrides: Record<string, string | null> = {}): GeneratedFileCandidate[] {
    const defaults: Record<string, string> = {
        'spark-pipeline.yml': [
            'name: medallion_pipeline',
            'storage: gs://my-sdp-checkpoints/medallion',
            'catalog: lake',
            'database: retail_medallion',
            'configuration:',
            '  spark.sql.shuffle.partitions: "200"',
            '  spark.sql.catalog.lake: "org.apache.iceberg.spark.SparkCatalog"',
            '  spark.sql.catalog.lake.type: "rest"',
            '  spark.sql.catalog.lake.uri: "https://biglake.googleapis.com/iceberg/v1/restcatalog"',
            '  spark.sql.catalog.lake.warehouse: "bl://projects/my-proj/catalogs/main_cat"',
            'libraries:',
            '  - glob:',
            '      include: transformations/**',
            '',
        ].join('\n'),
        'transformations/medallion_flows.py': [
            'from pyspark import pipelines as dp',
            'from pyspark.sql import DataFrame, SparkSession',
            'from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType',
            'from src.transforms import clean_orders, aggregate_revenue',
            '',
            'spark = SparkSession.active()',
            '',
            'ORDER_SCHEMA = StructType([',
            '    StructField("order_id", StringType()),',
            '    StructField("region", StringType()),',
            '    StructField("amount", DoubleType()),',
            '    StructField("event_ts", TimestampType()),',
            '])',
            '',
            '@dp.table(name="bronze_orders", format="iceberg")',
            'def bronze_orders() -> DataFrame:',
            '    return (',
            '        spark.readStream.format("csv")',
            '        .option("header", "true")',
            '        .schema(ORDER_SCHEMA)',
            '        .load("gs://retail-raw/orders/")',
            '    )',
            '',
            '@dp.table(name="silver_orders", format="iceberg")',
            'def silver_orders() -> DataFrame:',
            '    return clean_orders(spark.readStream.table("bronze_orders")).withWatermark("event_ts", "10 minutes")',
            '',
            '@dp.temporary_view(name="gold_store_agg")',
            'def gold_store_agg() -> DataFrame:',
            '    return aggregate_revenue(spark.read.table("silver_orders"))',
            '',
            '@dp.materialized_view(name="gold_revenue", format="iceberg")',
            'def gold_revenue() -> DataFrame:',
            '    return spark.read.table("gold_store_agg")',
            '',
        ].join('\n'),
        'src/transforms.py': [
            'from pyspark.sql import DataFrame',
            'from pyspark.sql import functions as F',
            '',
            'def clean_orders(df: DataFrame) -> DataFrame:',
            '    return df.filter(F.col("amount") > 0)',
            '',
            'def aggregate_revenue(df: DataFrame) -> DataFrame:',
            '    return df.groupBy("region").agg(F.count("*").alias("order_count"), F.sum("amount").alias("total_revenue"))',
            '',
        ].join('\n'),
        'tests/test_transforms.py': [
            'from src.transforms import clean_orders, aggregate_revenue',
            '',
            'def test_helpers_callable():',
            '    assert callable(clean_orders)',
            '    assert callable(aggregate_revenue)',
            '',
        ].join('\n'),
        'requirements.txt': 'pyspark==4.2.*\npytest>=8.0.0\nzstandard>=0.25.0\n',
        'run_sdp_local.sh': [
            '#!/usr/bin/env bash',
            'set -euo pipefail',
            'CMD="${SDP_COMMAND:-run}"',
            'spark-pipelines --remote "local[*]" "${CMD}" --spec spark-pipeline.yml',
            '',
        ].join('\n'),
        'run_sdp_dataproc.sh': [
            '#!/usr/bin/env bash',
            'set -euo pipefail',
            '# CREATE NAMESPACE IF NOT EXISTS lake.retail_medallion',
            'gcloud dataproc jobs submit pyspark run_sdp_on_dataproc.py --cluster="${CLUSTER}" --region="${REGION}" -- --create_namespace=lake.retail_medallion',
            '',
        ].join('\n'),
        'sdp_stage.pipeline.yaml': [
            'stages:',
            '  - id: medallion_sdp',
            '    sdp:',
            '      spec_path: spark-pipeline.yml',
            '      execution_config:',
            '        properties:',
            '          spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            '',
        ].join('\n'),
        'README.md': '# Medallion SDP Pipeline\n',
    };

    const merged: Record<string, string | null> = { ...defaults, ...overrides };
    return Object.entries(merged)
        .filter((entry): entry is [string, string] => entry[1] !== null)
        .map(([filename, content]) => ({
            filename,
            content,
            executable: filename.endsWith('.sh'),
        }));
}

const VALID_SDP_MEDALLION_YAML = [
    'paradigm: sdp',
    'pipelineConfig:',
    '  language: PySpark',
    'jobConfig:',
    '  runner: ServerlessSpark',
    '  runtimeVersion: "4.0"',
    '  sdpStorage: gs://my-sdp-checkpoints/medallion',
    '  sdpCatalog: lake',
    '  sdpDatabase: retail_medallion',
    'nodes:',
    '  - id: n_src',
    '    data:',
    '      label: Bronze Orders',
    '      type: SOURCE',
    '      spark_transform: CsvSource',
    '      properties:',
    '        readMode: streaming',
    '        path: gs://retail-raw/orders/',
    '  - id: n_wm',
    '    data:',
    '      label: Silver Orders',
    '      type: STEP',
    '      spark_transform: Watermark',
    '      properties:',
    '        eventTimeColumn: event_ts',
    '        delayThreshold: 10 minutes',
    '  - id: n_agg',
    '    data:',
    '      label: Gold Store Agg',
    '      type: STEP',
    '      spark_transform: GroupByAgg',
    '      properties:',
    '        groupByColumns: [region]',
    '        windowType: None',
    '        sdpDatasetType: temporary_view',
    '  - id: n_sink',
    '    data:',
    '      label: Gold Revenue',
    '      type: SINK',
    '      spark_transform: IcebergSink',
    '      properties:',
    '        sdpTableName: gold_revenue',
    '        sdpFormat: iceberg',
    'edges:',
    '  - { source: n_src, target: n_wm }',
    '  - { source: n_wm, target: n_agg }',
    '  - { source: n_agg, target: n_sink }',
    '',
].join('\n');

/** Creates a mocked GoogleGenAI client returning a serialized JSON payload. */
function mockGenAiClient(responseJson: Record<string, unknown>): GoogleGenAI {
    return {
        models: {
            generateContent: vi.fn(async () => ({ text: JSON.stringify(responseJson) })),
        },
    } as unknown as GoogleGenAI;
}

describe('SDP Deterministic Gate (deterministic-gate.ts)', () => {
    it('passes every check for a complete, valid SDP repository', async () => {
        const repo = buildValidSdpRepo();
        expect(detectRepositoryParadigm(repo)).toBe('sdp');
        expect(checkDeliverableCompleteness(repo)).toEqual([]);

        const res = await runDeterministicGate(repo, 'Python', {
            paradigm: 'sdp',
            sparkPipelinesCommand: 'nonexistent-spark-pipelines-cli-xyz',
        });
        expect(res.syntaxValid).toBe(true);
        expect(res.missingDeliverables).toEqual([]);
        expect(res.structureFindings).toEqual([]);
        expect(res.bannedPatternFindings).toEqual([]);
        expect(res.secretScanPassed).toBe(true);
        expect(res.sdpDryRunStatus).toBe('skipped');
    });

    it('reports missing SDP deliverables and spec violations cleanly', () => {
        const incomplete = buildValidSdpRepo({
            'src/transforms.py': null,
            'sdp_stage.pipeline.yaml': null,
            'spark-pipeline.yml': [
                'name: broken',
                'continuous: true',
                'storage: relative/no/scheme',
                'catalog: bad-hyphen',
                'configuration:',
                '  spark.driver.memory: "8g"',
                '  spark.sql.shuffle.partitions: 100',
                'libraries:',
                '  - glob:',
                '      include: transformations/*.py',
            ].join('\n'),
        });

        const missing = checkDeliverableCompleteness(incomplete, 'sdp');
        expect(missing).toContain('Missing mandatory deliverable: src/transforms.py');
        expect(missing).toContain('Missing mandatory deliverable: sdp_stage.pipeline.yaml');

        const structFindings = checkSdpStructureAndSpec(incomplete);
        const descriptions = structFindings.map((f) => f.description).join('\n');
        expect(descriptions).toContain('PIPELINE_SPEC_UNEXPECTED_FIELD');
        expect(descriptions).toContain('PIPELINE_STORAGE_ROOT_INVALID');
        expect(descriptions).toContain('INVALID_IDENTIFIER');
        expect(descriptions).toContain('CANNOT_MODIFY_STATIC_CONFIG');
        expect(descriptions).toContain('PIPELINE_SPEC_DICT_VALUE_NOT_STRING');
        expect(descriptions).toContain('PIPELINE_SPEC_INVALID_GLOB_PATTERN');
    });

    it('flags banned session mutations, writers, actions, and Databricks APIs in SDP mode', () => {
        const dirty = buildValidSdpRepo({
            'transformations/medallion_flows.py': [
                'import dlt',
                'from pyspark.sql import SparkSession',
                'spark = SparkSession.builder.getOrCreate()',
                'spark.conf.set("spark.sql.shuffle.partitions", "4")',
                '@dlt.expect("non_null", "id IS NOT NULL")',
                'def bad_table():',
                '    df = spark.readStream.format("cloudFiles").load("gs://b/p")',
                '    _ = df.schema',
                '    df.write.saveAsTable("bad")',
                '    return df',
            ].join('\n'),
        });

        const banned = scanForBannedSparkPatterns(dirty, 'sdp');
        expect(banned.length).toBeGreaterThanOrEqual(5);
        expect(banned.every((f) => f.severity === 'CRITICAL')).toBe(true);
    });

    it('executes spark-pipelines dry-run when a local CLI binary is provided', async () => {
        const tmpBinDir = fs.mkdtempSync(path.join(os.tmpdir(), 'fake-sdp-cli-'));
        const passScript = path.join(tmpBinDir, 'fake-spark-pipelines-pass.sh');
        const failScript = path.join(tmpBinDir, 'fake-spark-pipelines-fail.sh');
        try {
            fs.writeFileSync(passScript, '#!/usr/bin/env bash\necho "Run is COMPLETED."\nexit 0\n', {
                mode: 0o755,
            });
            fs.writeFileSync(
                failScript,
                '#!/usr/bin/env bash\necho "Failed to resolve flow: bronze_orders" >&2\nexit 1\n',
                { mode: 0o755 }
            );

            const repo = buildValidSdpRepo();
            const passOutcome = await runOptionalSdpGateDryRun(repo, passScript);
            expect(passOutcome.status).toBe('passed');

            const failGate = await runDeterministicGate(repo, 'Python', {
                paradigm: 'sdp',
                sparkPipelinesCommand: failScript,
            });
            expect(failGate.sdpDryRunStatus).toBe('failed');
            expect(failGate.structureFindings.some((f) => f.id === 'SDP-DRY-RUN-FAILED')).toBe(true);
        } finally {
            fs.rmSync(tmpBinDir, { recursive: true, force: true });
        }
    });
});

describe('SparkArchitectureAgent SDP Rules (spark-architecture-evaluator.ts)', () => {
    it('passes static checks on a clean SDP project and extracts SDP signals', () => {
        const repo = buildValidSdpRepo();
        expect(resolveSparkArchitectureParadigm(repo, VALID_SDP_MEDALLION_YAML)).toBe('sdp');

        const signals = collectSparkArchitectureSignals(repo, 'sdp');
        expect(signals.paradigm).toBe('sdp');
        expect(signals.sdpSpecFile).toBe('spark-pipeline.yml');
        expect(signals.sdpTransformationFiles).toBe(1);
        expect(signals.hasPureTransformsModule).toBe(true);

        const findings = runSparkArchitectureStaticChecks(repo, {
            paradigm: 'sdp',
            yamlContent: VALID_SDP_MEDALLION_YAML,
        });
        expect(findings).toEqual([]);
    });

    it('flags materialized views reading streams, streaming tables reading batch, and impure src/transforms.py', () => {
        const broken = buildValidSdpRepo({
            'transformations/medallion_flows.py': [
                'from pyspark import pipelines as dp',
                '',
                '@dp.materialized_view(name="bad_mv", format="iceberg")',
                'def bad_mv():',
                '    return spark.readStream.table("bronze_orders")',
                '',
                '@dp.table(name="bad_stream_tbl", format="iceberg")',
                'def bad_stream_tbl():',
                '    return spark.read.table("dim_stores")',
                '',
            ].join('\n'),
            'src/transforms.py': [
                'def impure_helper(spark):',
                '    return spark.read.table("external_table")',
                '',
            ].join('\n'),
        });

        const findings = runSdpArchitectureStaticChecks(broken);
        const descriptions = findings.map((f) => f.description).join('\n');
        expect(descriptions).toContain('STREAMING_RELATION_FOR_MATERIALIZED_VIEW');
        expect(descriptions).toContain('BATCH_RELATION_FOR_STREAMING_TABLE');
        expect(descriptions).toContain('src/transforms.py performs Spark table or storage I/O');
    });

    it('builds SDP prompt and evaluates SDP repositories via evaluateSparkArchitecture', async () => {
        const repo = buildValidSdpRepo();
        const signals = collectSparkArchitectureSignals(repo, 'sdp');
        const prompt = buildSparkArchitecturePrompt(
            'Python',
            VALID_SDP_MEDALLION_YAML,
            repo,
            [],
            signals,
            '4.2',
            'sdp'
        );
        expect(prompt).toContain('Spark Declarative Pipelines');
        expect(prompt).toContain('DETERMINISTIC SDP EXECUTION PLAN');

        const ai = mockGenAiClient({
            passed: true,
            summary: 'Clean SDP architecture.',
            findings: [],
        });
        const result = await evaluateSparkArchitecture(ai, 'Python', VALID_SDP_MEDALLION_YAML, repo);
        expect(result.passed).toBe(true);
        expect(result.findings).toEqual([]);
    });
});

describe('PipelineSpecificationAgent & PackagingAgent SDP Rules', () => {
    it('audits SdpExecutionPlan datasets, Auto CDC flows, quarantine tables, and RTM entries', async () => {
        const spec = parsePipelineSpec(VALID_SDP_MEDALLION_YAML);
        expect(spec.paradigm).toBe('sdp');

        const sdpPlan = buildSpecSdpExecutionPlan(VALID_SDP_MEDALLION_YAML);
        expect(sdpPlan).not.toBeNull();
        if (!sdpPlan) {
            throw new Error('Expected non-null sdpPlan');
        }
        expect(sdpPlan.isCompatible).toBe(true);

        const repo = buildValidSdpRepo();
        expect(auditSdpPlanCompliance(sdpPlan, repo)).toEqual([]);

        const rtm = generateRequirementMatrix(spec, null, sdpPlan);
        expect(rtm.some((item) => item.reqId.startsWith('REQ-SDP-DATASET-'))).toBe(true);

        const missingDatasetRepo = buildValidSdpRepo({
            'transformations/medallion_flows.py': [
                'from pyspark import pipelines as dp',
                '@dp.table(name="bronze_orders")',
                'def bronze_orders():',
                '    return spark.readStream.format("rate").load()',
            ].join('\n'),
        });
        const missingFindings = auditSdpPlanCompliance(sdpPlan, missingDatasetRepo);
        expect(missingFindings.some((f) => f.id.startsWith('SDP-PLAN-DATASET-'))).toBe(true);

        const ai = mockGenAiClient({
            passed: true,
            summary: 'All SDP requirements verified.',
            requirementVerifications: [],
            findings: [],
        });
        const evalRes = await evaluatePipelineSpecification(ai, 'Python', VALID_SDP_MEDALLION_YAML, repo);
        expect(evalRes.passed).toBe(true);
    });

    it('audits SDP Spark 4.2+ requirements.txt, SDP runner scripts, and Managed Data Pipeline stage YAML', async () => {
        expect(pysparkSupportsSdp42('==4.2.*')).toBe(true);
        expect(pysparkSupportsSdp42('>=4.2.0,<4.3')).toBe(true);
        expect(pysparkSupportsSdp42('~=4.2.0')).toBe(true);
        expect(pysparkSupportsSdp42('==4.0.*')).toBe(false);
        expect(pysparkSupportsSdp42('>=4.0.0,<4.2')).toBe(false);

        const repo = buildValidSdpRepo();
        expect(runDeterministicPackagingAudit(repo, null, { paradigm: 'sdp' })).toEqual([]);

        const wrongPyspark = buildValidSdpRepo({
            'requirements.txt': 'pyspark==4.0.*\npytest>=8.0.0\n',
        });
        const reqFindings = auditRequirementsTxt(wrongPyspark, [], { paradigm: 'sdp' });
        expect(reqFindings.some((f) => f.id === 'PKG-REQ-SDP-PYSPARK-42')).toBe(true);

        const missingStage = buildValidSdpRepo({
            'sdp_stage.pipeline.yaml': null,
        });
        expect(
            auditSdpRunnerScriptsAndStageYaml(missingStage).some(
                (f) => f.id === 'PKG-SDP-STAGE-YAML-MISSING'
            )
        ).toBe(true);

        const ai = mockGenAiClient({
            passed: true,
            summary: 'SDP packaging deployable.',
            findings: [],
        });
        const pkgRes = await evaluatePackagingAndDeployability(
            ai,
            'Python',
            repo,
            VALID_SDP_MEDALLION_YAML
        );
        expect(pkgRes.passed).toBe(true);
    });
});

describe('sdpPlanner.ts additional edge cases (SDP10)', () => {
    it('strips reserved __spark_autocdc_ prefixes and sanitizes identifiers deterministically', () => {
        expect(sanitizeSdpIdentifier('__spark_autocdc_customers')).toBe('customers');
        expect(sanitizeSdpIdentifier('__spark_autocdc___spark_autocdc_orders')).toBe('orders');
        expect(sanitizeSdpIdentifier('99-Gold Revenue!')).toBe('_99_gold_revenue');
        expect(sanitizeSdpIdentifier('___', 'fallback_tbl')).toBe('fallback_tbl');
    });

    it('allows PivotUnpivot unpivot without pivotValues while blocking pivot without pivotValues', () => {
        expect(
            checkNodeSdpCompatibility('PivotUnpivot', 'STEP', { operation: 'unpivot' }).supported
        ).toBe(true);
        const blocked = checkNodeSdpCompatibility('PivotUnpivot', 'STEP', {
            operation: 'pivot',
            pivotValues: [],
        });
        expect(blocked.supported).toBe(false);
        expect(blocked.code).toBe('SDP_PIVOT_VALUES_REQUIRED');
    });

    it('detects INVALID_RESETTABLE_DEPENDENCY across intermediate temporary views', () => {
        const plan = buildSdpPlan(
            [
                {
                    id: 'src',
                    data: {
                        label: 'Raw Stream',
                        type: 'SOURCE',
                        spark_transform: 'KafkaSource',
                        properties: { readMode: 'streaming', sdpResetAllowed: true },
                    },
                },
                {
                    id: 'view_step',
                    data: {
                        label: 'Temp Projection',
                        type: 'STEP',
                        spark_transform: 'Select',
                        properties: { sdpDatasetType: 'temporary_view' },
                    },
                },
                {
                    id: 'protected_sink',
                    data: {
                        label: 'Protected Iceberg',
                        type: 'SINK',
                        spark_transform: 'IcebergSink',
                        properties: { sdpResetAllowed: false },
                    },
                },
            ],
            [
                { source: 'src', target: 'view_step' },
                { source: 'view_step', target: 'protected_sink' },
            ],
            { runner: 'ServerlessSpark', runtimeVersion: '4.0' }
        );

        expect(plan.isCompatible).toBe(false);
        expect(plan.blockingIssues.map((i) => i.code)).toContain('INVALID_RESETTABLE_DEPENDENCY');
    });

    it('disambiguates colliding DataQualityAssert quarantine dataset names and warns on local_4.2_connect Iceberg', () => {
        const plan = buildSdpPlan(
            [
                {
                    id: 'src',
                    data: {
                        label: 'Orders',
                        type: 'SOURCE',
                        spark_transform: 'CsvSource',
                        properties: { readMode: 'streaming' },
                    },
                },
                {
                    id: 'dq',
                    data: {
                        label: 'Orders Check',
                        type: 'STEP',
                        spark_transform: 'DataQualityAssert',
                        properties: {},
                    },
                },
                {
                    id: 'sink',
                    data: {
                        label: 'Orders Check Quarantine',
                        type: 'SINK',
                        spark_transform: 'IcebergSink',
                        properties: {},
                    },
                },
            ],
            [
                { source: 'src', target: 'dq' },
                { source: 'dq', target: 'sink', sourceHandle: 'valid' },
            ],
            { runner: 'Local', runtimeVersion: '4.2-connect' }
        );

        expect(plan.isCompatible).toBe(true);
        expect(plan.warnings.some((w) => w.includes('MISSING_LOCAL_ICEBERG_RUNTIME'))).toBe(true);
        const dqDs = plan.datasetsByNodeId.dq;
        const sinkDs = plan.datasetsByNodeId.sink;
        expect(dqDs.quarantineDatasetName).toBeDefined();
        expect(dqDs.quarantineDatasetName).not.toBe(sinkDs.datasetName);

        const compat = evaluateSdpCompatibility(
            [
                {
                    id: 'src',
                    data: { label: 'S', type: 'SOURCE', spark_transform: 'RateSource' },
                },
                {
                    id: 'ml',
                    data: { label: 'ML', type: 'STEP', spark_transform: 'SparkMlAndVertexAi' },
                },
            ],
            [{ source: 'src', target: 'ml' }]
        );
        expect(compat.isCompatible).toBe(false);
        expect(compat.blockingIssues[0].code).toBe('SDP_SPARK_ML_UNSUPPORTED');
    });
});
