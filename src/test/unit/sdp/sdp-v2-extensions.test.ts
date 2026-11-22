/**
 * @fileoverview Unit tests for Spark Declarative Pipelines V2 extensions:
 * - V2.6: Hybrid SDP + imperative companion output for foreachBatch-only sinks.
 * - V2.7: SDP SQL output mode (`transformations/*.sql`) and SQL static validation.
 * - V2.8: Optional `sdp_extensions.expectations` (`@dq.expect*`) support and decorator validation.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import * as path from 'path';
import { describe, expect, it } from 'vitest';
import type { DagEdge, DagNode } from '../../../shared/dagValidator';
import {
    buildSdpPlan,
    checkDatasetSqlEligibility,
    checkNodeSdpCompatibility,
    resolveExpectationAction,
} from '../../../shared/sdpPlanner';
import { PIPELINE_FEATURES } from '../../../codegen/codegenConfig';
import { detectPipelineFeatures } from '../../../codegen/PipelineFeatureDetector';
import { CodegenSkillCatalog } from '../../../codegen/CodegenSkillCatalog';
import { CodegenPromptBuilder } from '../../../codegen/CodegenPromptBuilder';
import {
    CHECK_IDS,
    ReadOnlyCheckRunner,
    scanSdpPythonViolations,
    scanSdpSqlViolations,
    type CommandExecutor,
    type GeneratedFile,
} from '../../../codegen/ReadOnlyCheckRunner';
import { ALLOWED_EXECUTABLE_SCRIPTS, isAllowedExecutableScript } from '../../../security/scriptGuard';
import {
    checkDeliverableCompleteness,
    checkSdpStructureAndSpec,
} from '../../integration/evaluators/deterministic-gate';

const SKILLS_ROOT = path.join(__dirname, '..', '..', '..', '..', 'resources', 'codegen-skills');
const CATALOG = CodegenSkillCatalog.load(SKILLS_ROOT);

/**
 * Builds a minimal DAG node fixture for SDP unit tests.
 *
 * @param id Node identifier.
 * @param type Node category.
 * @param sparkTransform Canonical Spark transform name.
 * @param label Human-readable node label.
 * @param properties Optional node property map.
 * @returns Constructed DAG node.
 */
function makeNode(
    id: string,
    type: 'SOURCE' | 'STEP' | 'SINK',
    sparkTransform: string,
    label: string,
    properties: Record<string, unknown> = {}
): DagNode {
    return {
        id,
        data: {
            type,
            spark_transform: sparkTransform,
            sparkTransform,
            label,
            properties,
        },
    };
}

/**
 * Builds a minimal DAG edge fixture for SDP unit tests.
 *
 * @param source Source node identifier.
 * @param target Target node identifier.
 * @param sourceHandle Optional source handle name.
 * @param targetHandle Optional target handle name.
 * @returns Constructed DAG edge.
 */
function makeEdge(
    source: string,
    target: string,
    sourceHandle?: string,
    targetHandle?: string
): DagEdge {
    return {
        id: `${source}->${target}`,
        source,
        target,
        ...(sourceHandle ? { sourceHandle } : {}),
        ...(targetHandle ? { targetHandle } : {}),
    };
}

const VALID_SPEC_YML = `name: test_sdp_pipeline
storage: gs://my-sdp-bucket/pipelines/test_sdp_pipeline
catalog: lake
database: analytics
libraries:
  - glob:
      include: transformations/**
`;

const fakeSuccessExecutor: CommandExecutor = async () => ({
    exitCode: 0,
    stdout: 'ok',
    stderr: '',
});

describe('V2.6: Hybrid SDP + imperative companion output for foreachBatch-only sinks', () => {
    it('blocks foreachBatch-only sinks by default and unlocks them when allowHybridForeachBatch is true', () => {
        const blocked = checkNodeSdpCompatibility('CloudSqlSink', 'SINK');
        expect(blocked.supported).toBe(false);
        expect(blocked.code).toBe('SDP_FOREACH_BATCH_SINK_UNSUPPORTED');

        const allowed = checkNodeSdpCompatibility('CloudSqlSink', 'SINK', undefined, {
            allowHybridForeachBatch: true,
        });
        expect(allowed.supported).toBe(true);
        expect(allowed.hybridCompanion).toBe(true);
    });

    it('maps foreachBatch-only sinks to hybrid_companion and promotes upstream temporary_view to persisted dataset', () => {
        const nodes: DagNode[] = [
            makeNode('src', 'SOURCE', 'ParquetSource', 'Raw Orders', { readMode: 'batch' }),
            makeNode('step', 'STEP', 'Filter', 'Valid Orders'),
            makeNode('snk', 'SINK', 'CloudSqlSink', 'Orders Postgres'),
        ];
        const edges: DagEdge[] = [makeEdge('src', 'step'), makeEdge('step', 'snk')];

        const plan = buildSdpPlan(nodes, edges, {
            runner: 'ServerlessSpark',
            runtimeVersion: '4.0',
            sdpHybridForeachBatch: true,
        });

        expect(plan.isCompatible).toBe(true);
        expect(plan.hasHybridCompanionSinks).toBe(true);
        expect(plan.hybridCompanionSinkNames).toEqual(['orders_postgres']);
        expect(plan.datasetsByNodeId.snk.kind).toBe('hybrid_companion');
        expect(plan.datasetsByNodeId.snk.badge).toBe('HYBRID');
        expect(plan.datasetsByNodeId.snk.isHybridCompanion).toBe(true);
        // Upstream Filter step promoted from temporary_view to materialized_view so companion job can read it.
        expect(plan.datasetsByNodeId.step.kind).toBe('materialized_view');
        expect(plan.warnings.some((w) => w.includes('companion/sink_job.py'))).toBe(true);
    });

    it('detects sdp_hybrid feature and loads sdp-hybrid-companion skill in CodegenPromptBuilder', () => {
        const parsedPipeline = {
            paradigm: 'sdp',
            jobConfig: {
                runner: 'ServerlessSpark',
                runtimeVersion: '4.0',
                sdpStorage: 'gs://bucket/pipelines/hybrid_demo',
                sdpCatalog: 'lake',
                sdpDatabase: 'analytics',
                sdpHybridForeachBatch: true,
            },
            nodes: [
                makeNode('src', 'SOURCE', 'KafkaSource', 'Orders Stream'),
                makeNode('snk', 'SINK', 'VectorStoreSink', 'Orders Vectors'),
            ],
            edges: [makeEdge('src', 'snk')],
        };

        const detection = detectPipelineFeatures(parsedPipeline, 'PySpark', (raw) =>
            CATALOG.resolveTransform(raw)
        );
        expect(detection.features.has(PIPELINE_FEATURES.sdpHybrid)).toBe(true);

        const built = new CodegenPromptBuilder(CATALOG).build({
            parsedPipeline,
            redactedYaml: 'paradigm: sdp\nnodes: []',
            redactedPaths: [],
            language: 'PySpark',
        });
        expect(built.selectedSkills).toContain('sdp-hybrid-companion');
        expect(built.prompt).toContain('companion/sink_job.py');
        expect(built.prompt).toContain('run_companion_sinks.sh');
    });

    it('allowlists run_companion_sinks.sh and allows imperative writers inside companion/sink_job.py', () => {
        expect(ALLOWED_EXECUTABLE_SCRIPTS.has('run_companion_sinks.sh')).toBe(true);
        expect(isAllowedExecutableScript('run_companion_sinks.sh')).toBe(true);

        const files: GeneratedFile[] = [
            {
                filename: 'transformations/pipeline_datasets.py',
                content:
                    'from pyspark import pipelines as dp\n@dp.table(name="bronze_orders")\ndef bronze_orders():\n    return spark.readStream.format("parquet").schema("id INT").load("gs://b/in")\n',
            },
            {
                filename: 'companion/sink_job.py',
                content:
                    'from pyspark.sql import SparkSession\ndef write_companion_sinks(dfs, config):\n    dfs["bronze_orders"].writeStream.foreachBatch(lambda b, i: None).start()\n',
            },
        ];
        expect(scanSdpPythonViolations(files)).toEqual([]);
    });
});

describe('V2.7: SDP SQL output mode (transformations/*.sql)', () => {
    it('evaluates SQL eligibility deterministically across dataset kinds and transforms', () => {
        expect(checkDatasetSqlEligibility('streaming_table', 'CsvSource')).toEqual({ eligible: true });
        expect(checkDatasetSqlEligibility('materialized_view', 'GroupByAgg')).toEqual({ eligible: true });
        expect(checkDatasetSqlEligibility('external_sink', 'KafkaSink').eligible).toBe(false);
        expect(checkDatasetSqlEligibility('auto_cdc', 'MergeCdcScd').eligible).toBe(false);
        expect(checkDatasetSqlEligibility('streaming_table', 'Filter', ['order_date']).eligible).toBe(false);
        expect(checkDatasetSqlEligibility('streaming_table', 'PandasUdfTransform').eligible).toBe(false);
        expect(checkDatasetSqlEligibility('streaming_table', 'PivotUnpivot').eligible).toBe(false);
        expect(checkDatasetSqlEligibility('streaming_table', 'DataQualityAssert', undefined, true).eligible).toBe(
            false
        );
    });

    it('assigns outputLanguage sql vs python in buildSdpPlan and loads sdp-sql skill', () => {
        const parsedPipeline = {
            paradigm: 'sdp',
            jobConfig: {
                runner: 'ServerlessSpark',
                runtimeVersion: '4.0',
                sdpStorage: 'gs://bucket/pipelines/sql_demo',
                sdpCatalog: 'lake',
                sdpDatabase: 'analytics',
                sdpOutputFormat: 'sql',
            },
            nodes: [
                makeNode('src', 'SOURCE', 'CsvSource', 'Raw Orders', { readMode: 'streaming' }),
                makeNode('agg', 'STEP', 'GroupByAgg', 'Daily Totals'),
                makeNode('udf', 'STEP', 'PandasUdfTransform', 'Score Orders'),
            ],
            edges: [makeEdge('src', 'agg'), makeEdge('agg', 'udf')],
        };

        const plan = buildSdpPlan(parsedPipeline.nodes, parsedPipeline.edges, parsedPipeline.jobConfig);
        expect(plan.outputFormat).toBe('sql');
        expect(plan.sqlDatasetNames).toContain('raw_orders');
        expect(plan.sqlDatasetNames).toContain('daily_totals');
        expect(plan.pythonDatasetNames).toContain('score_orders');
        expect(plan.datasetsByNodeId.udf.sqlFallbackReason).toMatch(/Python UDFs/);
        expect(plan.warnings.some((w) => w.includes('SDP SQL fallback'))).toBe(true);

        const built = new CodegenPromptBuilder(CATALOG).build({
            parsedPipeline,
            redactedYaml: 'paradigm: sdp\nnodes: []',
            redactedPaths: [],
            language: 'PySpark',
        });
        expect(built.selectedSkills).toContain('sdp-sql');
        expect(built.prompt).toContain('CREATE STREAMING TABLE');
        expect(built.prompt).toContain('transformations/*.sql');
    });

    it('detects Spark 4.2 SDP SQL violations and accepts valid pure-SQL SDP repositories', async () => {
        const badSqlFiles: GeneratedFile[] = [
            {
                filename: 'transformations/bad.sql',
                content: [
                    'CREATE SCHEMA IF NOT EXISTS lake.analytics;',
                    'CREATE STREAMING TABLE orders USING iceberg CLUSTER BY (id)',
                    '  CONSTRAINT valid_id EXPECT (id IS NOT NULL)',
                    'AS SELECT * FROM STREAM(raw_orders) PIVOT (SUM(amount) FOR region IN ("US"));',
                    'CREATE FLOW f1 AS INSERT INTO orders SELECT * FROM STREAM(raw_orders_2);',
                    'CREATE FLOW f2 AS INSERT OVERWRITE orders BY NAME SELECT * FROM STREAM(raw_orders_3);',
                ].join('\n'),
            },
        ];
        const violations = scanSdpSqlViolations(badSqlFiles);
        const codes = new Set(violations.map((v) => v.code));
        expect(codes.has('PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR')).toBe(true);
        expect(codes.has('SDP_SQL_EXPECT_UNSUPPORTED')).toBe(true);
        expect(codes.has('SDP_SQL_CLUSTER_BY_UNSUPPORTED')).toBe(true);
        expect(codes.has('SDP_SQL_PIVOT_UNSUPPORTED')).toBe(true);
        expect(codes.has('SDP_SQL_FLOW_REQUIRES_BY_NAME')).toBe(true);
        expect(codes.has('SDP_SQL_FLOW_OVERWRITE_UNSUPPORTED')).toBe(true);

        const validPureSqlRepo: GeneratedFile[] = [
            { filename: 'spark-pipeline.yml', content: VALID_SPEC_YML },
            {
                filename: 'transformations/pipeline_datasets.sql',
                content: [
                    'CREATE STREAMING TABLE bronze_orders USING iceberg AS SELECT * FROM STREAM(raw_orders);',
                    'CREATE MATERIALIZED VIEW gold_orders USING iceberg AS SELECT store_id, COUNT(*) AS cnt FROM bronze_orders GROUP BY store_id;',
                    'CREATE FLOW extra_orders AS INSERT INTO bronze_orders BY NAME SELECT * FROM STREAM(raw_orders_archive);',
                ].join('\n'),
            },
            { filename: 'requirements.txt', content: 'pyspark[connect]==4.2.*\npyyaml>=6.0\n' },
            { filename: 'sdp_stage.pipeline.yaml', content: 'kind: Pipeline\n' },
            { filename: 'run_sdp_local.sh', content: '#!/usr/bin/env bash\n' },
            { filename: 'run_sdp_dataproc.sh', content: '#!/usr/bin/env bash\n' },
            { filename: 'README.md', content: '# Pure SQL SDP\n' },
        ];

        expect(scanSdpSqlViolations(validPureSqlRepo)).toEqual([]);
        expect(checkDeliverableCompleteness(validPureSqlRepo, 'sdp')).toEqual([]);
        expect(checkSdpStructureAndSpec(validPureSqlRepo)).toEqual([]);

        const runner = new ReadOnlyCheckRunner(fakeSuccessExecutor);
        const results = await runner.run({
            files: validPureSqlRepo,
            language: 'python',
            baseName: 'pure_sql',
            runnerFiles: ['sdp_stage.pipeline.yaml', 'run_sdp_local.sh', 'run_sdp_dataproc.sh'],
            paradigm: 'sdp',
        });
        expect(results.find((r) => r.check === CHECK_IDS.requiredFiles)?.status).toBe('passed');
        expect(results.find((r) => r.check === CHECK_IDS.pipelineContract)?.status).toBe('passed');
    });
});

describe('V2.8: Optional sdp_extensions.expectations support in SDP mode', () => {
    it('maps DataQualityAssert failurePolicy values to sdp_extensions expectation actions', () => {
        expect(resolveExpectationAction('fail_job')).toBe('fail');
        expect(resolveExpectationAction('fail')).toBe('fail');
        expect(resolveExpectationAction('warn')).toBe('warn');
        expect(resolveExpectationAction('route_to_dlq')).toBe('warn');
        expect(resolveExpectationAction('drop_rows')).toBe('drop');
        expect(resolveExpectationAction(undefined)).toBe('drop');
    });

    it('plans expectations mode, adds sdp-extensions to pipPackages, and loads sdp-expectations skill', () => {
        const parsedPipeline = {
            paradigm: 'sdp',
            jobConfig: {
                runner: 'ServerlessSpark',
                runtimeVersion: '4.0',
                sdpStorage: 'gs://bucket/pipelines/dq_demo',
                sdpCatalog: 'lake',
                sdpDatabase: 'analytics',
                sdpQualityMode: 'expectations',
            },
            nodes: [
                makeNode('src', 'SOURCE', 'KafkaSource', 'Orders Stream'),
                makeNode('dq', 'STEP', 'DataQualityAssert', 'Clean Orders', {
                    failurePolicy: 'drop_rows',
                }),
                makeNode('snk', 'SINK', 'IcebergSink', 'Silver Orders'),
            ],
            edges: [makeEdge('src', 'dq'), makeEdge('dq', 'snk')],
        };

        const plan = buildSdpPlan(parsedPipeline.nodes, parsedPipeline.edges, parsedPipeline.jobConfig);
        expect(plan.qualityMode).toBe('expectations');
        expect(plan.usesSdpExpectations).toBe(true);
        expect(plan.datasetsByNodeId.dq.usesSdpExpectations).toBe(true);
        expect(plan.datasetsByNodeId.dq.expectationAction).toBe('drop');
        // Direct drop_rows without error edge omits separate quarantine table.
        expect(plan.datasetsByNodeId.dq.quarantineDatasetName).toBeUndefined();

        const built = new CodegenPromptBuilder(CATALOG).build({
            parsedPipeline,
            redactedYaml: 'paradigm: sdp\nnodes: []',
            redactedPaths: [],
            language: 'PySpark',
        });
        expect(built.selectedSkills).toContain('sdp-expectations');
        expect(built.prompt).toContain('sdp-extensions');
        expect(built.prompt).toContain('@dq.expect*');
    });

    it('validates @dq.expect* placement below @dp.table and bans @dq.expect* above @dp.* or on @dp.append_flow', () => {
        const validFile: GeneratedFile = {
            filename: 'transformations/clean_orders.py',
            content: [
                'from pyspark import pipelines as dp',
                'from sdp_extensions import expectations as dq',
                '@dp.table(name="clean_orders", format="iceberg")',
                '@dq.expect_or_drop("positive_amount", "amount > 0")',
                'def clean_orders():',
                '    return spark.readStream.table("bronze_orders")',
            ].join('\n'),
        };
        expect(scanSdpPythonViolations([validFile])).toEqual([]);

        const wrongOrderFile: GeneratedFile = {
            filename: 'transformations/wrong_order.py',
            content: [
                'from pyspark import pipelines as dp',
                'from sdp_extensions import expectations as dq',
                '@dq.expect_or_drop("positive_amount", "amount > 0")',
                '@dp.table(name="clean_orders", format="iceberg")',
                'def clean_orders():',
                '    return spark.readStream.table("bronze_orders")',
            ].join('\n'),
        };
        const orderViolations = scanSdpPythonViolations([wrongOrderFile]);
        expect(orderViolations.some((v) => v.code === 'SDP_EXPECTATIONS_DECORATOR_ORDER')).toBe(true);

        const appendFlowFile: GeneratedFile = {
            filename: 'transformations/bad_append.py',
            content: [
                'from pyspark import pipelines as dp',
                'from sdp_extensions import expectations as dq',
                '@dp.append_flow(target="clean_orders", name="flow_1")',
                '@dq.expect_or_drop("positive_amount", "amount > 0")',
                'def flow_1():',
                '    return spark.readStream.table("bronze_orders")',
            ].join('\n'),
        };
        const appendViolations = scanSdpPythonViolations([appendFlowFile]);
        expect(appendViolations.some((v) => v.code === 'SDP_EXPECTATIONS_ON_APPEND_FLOW')).toBe(true);
    });
});
