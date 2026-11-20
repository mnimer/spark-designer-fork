/**
 * @fileoverview Unit tests for the Spark Declarative Pipelines planner and compatibility gate.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies identifier sanitization, palette and node compatibility checks, runtime Spark 4.2+
 * enforcement, continuous trigger blocking, streaming watermark requirements, Auto CDC SCD1 and
 * MERGE target rules, partition vs cluster mutual exclusion, resettable dependency validation,
 * and deterministic dataset mapping across VIEW, MV, TABLE, CDC, and SINK constructs.
 */

import { describe, it, expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import type { DagEdge, DagNode } from '../../../shared/dagValidator';
import {
    DEFAULT_PIPELINE_PARADIGM,
    buildSdpPlan,
    checkNodeSdpCompatibility,
    evaluateSdpCompatibility,
    normalizeSdpJobConfig,
    sanitizeSdpIdentifier,
} from '../../../shared/sdpPlanner';

/**
 * Builds a minimal DAG node fixture for SDP planner unit tests.
 *
 * @param id Unique node identifier.
 * @param type High-level node category.
 * @param sparkTransform Canonical Spark transform name.
 * @param label Optional display label.
 * @param properties Optional persisted node properties.
 * @returns Configured DagNode fixture.
 */
function makeNode(
    id: string,
    type: 'SOURCE' | 'STEP' | 'SINK',
    sparkTransform: string,
    label?: string,
    properties?: Record<string, unknown>
): DagNode {
    return {
        id,
        type,
        data: {
            label: label ?? id,
            type,
            spark_transform: sparkTransform,
            properties: properties ?? {},
        },
    };
}

/**
 * Builds a directed DAG edge fixture for SDP planner unit tests.
 *
 * @param source Source node identifier.
 * @param target Target node identifier.
 * @param options Optional source and target handle identifiers.
 * @param options.sourceHandle Optional source output handle name.
 * @param options.targetHandle Optional target input handle name.
 * @returns Configured DagEdge fixture.
 */
function makeEdge(
    source: string,
    target: string,
    options?: { sourceHandle?: string; targetHandle?: string }
): DagEdge {
    return {
        id: `${source}->${target}${options?.targetHandle ? `:${options.targetHandle}` : ''}`,
        source,
        target,
        ...(options?.sourceHandle ? { sourceHandle: options.sourceHandle } : {}),
        ...(options?.targetHandle ? { targetHandle: options.targetHandle } : {}),
    };
}

describe('shared SDP types and webview re-exports (SDP5)', () => {
    it('exports DEFAULT_PIPELINE_PARADIGM as imperative and re-exports planner helpers in webview', () => {
        expect(DEFAULT_PIPELINE_PARADIGM).toBe('imperative');
        const plannerShimPath = path.resolve(
            __dirname,
            '../../../../webview/src/utils/sdpPlanner.ts'
        );
        const sharedTypesShimPath = path.resolve(
            __dirname,
            '../../../../webview/src/utils/sharedTypes.ts'
        );
        expect(fs.readFileSync(plannerShimPath, 'utf-8')).toContain(
            "export * from '../../../src/shared/sdpPlanner';"
        );
        expect(fs.readFileSync(sharedTypesShimPath, 'utf-8')).toContain(
            "export * from '../../../src/shared/types';"
        );
    });
});

describe('sanitizeSdpIdentifier', () => {
    it('converts camelCase, spaces, hyphens, and punctuation into snake_case identifiers', () => {
        expect(sanitizeSdpIdentifier('BronzeCustomerOrders')).toBe('bronze_customer_orders');
        expect(sanitizeSdpIdentifier('Silver Orders - US!')).toBe('silver_orders_us');
        expect(sanitizeSdpIdentifier('my-hyphenated-table')).toBe('my_hyphenated_table');
        expect(sanitizeSdpIdentifier('123_events')).toBe('_123_events');
    });

    it('strips the reserved __spark_autocdc_ prefix and falls back cleanly when empty', () => {
        expect(sanitizeSdpIdentifier('__spark_autocdc_customers')).toBe('customers');
        expect(sanitizeSdpIdentifier('__SPARK_AUTOCDC_aux_state_orders')).toBe(
            'aux_state_orders'
        );
        expect(sanitizeSdpIdentifier('__spark_autocdc_')).toBe('dataset');
        expect(sanitizeSdpIdentifier('   ', 'FallbackName')).toBe('fallback_name');
        expect(sanitizeSdpIdentifier('', '')).toBe('dataset');
    });
});

describe('checkNodeSdpCompatibility', () => {
    const FOREACH_BATCH_SINKS = [
        'SpannerSink',
        'BigtableSink',
        'CloudSqlSink',
        'AlloyDbSink',
        'JdbcSink',
        'SnowflakeSink',
        'RedshiftSink',
        'ClickHouseSink',
        'SingleStoreSink',
        'PubSubSink',
        'KinesisSink',
        'MqttSink',
        'FirestoreSink',
        'RedisSink',
        'VectorStoreSink',
        'HiveSink',
        'XmlSink',
        'TFRecordSink',
        'ForeachBatchSink',
    ];

    it('blocks all foreachBatch-only sinks and legacy foreachBatch sink aliases', () => {
        for (const sinkTransform of FOREACH_BATCH_SINKS) {
            const compat = checkNodeSdpCompatibility(sinkTransform, 'SINK');
            expect(compat.supported).toBe(false);
            expect(compat.code).toBe('SDP_FOREACH_BATCH_SINK_UNSUPPORTED');
            expect(compat.reason).toContain('foreachBatch');
        }

        expect(checkNodeSdpCompatibility('SINK:PubSubIO', 'SINK').supported).toBe(false);
        expect(checkNodeSdpCompatibility('SOURCE:PubSubIO', 'SOURCE').supported).toBe(true);
        expect(checkNodeSdpCompatibility('PubSubSource', 'SOURCE').supported).toBe(true);
        expect(checkNodeSdpCompatibility('SpannerSource', 'SOURCE').supported).toBe(true);
    });

    it('allows native SDP-capable catalog and external streaming sinks', () => {
        for (const nativeSink of [
            'IcebergSink',
            'DeltaSink',
            'HudiSink',
            'KafkaSink',
            'BigQuerySink',
            'ParquetSink',
            'ConsoleSink',
            'MemorySink',
        ]) {
            expect(checkNodeSdpCompatibility(nativeSink, 'SINK')).toEqual({ supported: true });
        }
    });

    it('blocks Spark ML steps and CachePersist with explanatory reasons', () => {
        for (const mlStep of [
            'FeaturePrep',
            'SparkMlAndVertexAi',
            'SparkMlInference',
            'VertexAiInference',
        ]) {
            const res = checkNodeSdpCompatibility(mlStep, 'STEP');
            expect(res.supported).toBe(false);
            expect(res.code).toBe('SDP_SPARK_ML_UNSUPPORTED');
            expect(res.reason).toBeTruthy();
        }

        const cacheRes = checkNodeSdpCompatibility('CachePersist', 'STEP');
        expect(cacheRes.supported).toBe(false);
        expect(cacheRes.code).toBe('SDP_CACHE_PERSIST_UNSUPPORTED');
        expect(cacheRes.reason).toContain('persistence');
    });

    it('allows MergeCdcScd in the palette and for SCD1 while blocking SCD2', () => {
        expect(checkNodeSdpCompatibility('MergeCdcScd', 'STEP')).toEqual({ supported: true });
        expect(
            checkNodeSdpCompatibility('MergeCdcScd', 'STEP', { scdType: 'scd1' })
        ).toEqual({ supported: true });

        const scd2 = checkNodeSdpCompatibility('MergeCdcScd', 'STEP', { scdType: 'scd2' });
        expect(scd2.supported).toBe(false);
        expect(scd2.code).toBe('AUTOCDC_SCD2_NOT_SUPPORTED');
    });

    it('allows PivotUnpivot in the palette or unpivot mode, and requires pivotValues for pivot', () => {
        expect(checkNodeSdpCompatibility('PivotUnpivot', 'STEP')).toEqual({ supported: true });
        expect(
            checkNodeSdpCompatibility('PivotUnpivot', 'STEP', { operation: 'unpivot' })
        ).toEqual({ supported: true });
        expect(
            checkNodeSdpCompatibility('PivotUnpivot', 'STEP', {
                operation: 'pivot',
                pivotValues: ['US', 'EU'],
            })
        ).toEqual({ supported: true });

        const missingValues = checkNodeSdpCompatibility('PivotUnpivot', 'STEP', {
            operation: 'pivot',
            pivotValues: [],
        });
        expect(missingValues.supported).toBe(false);
        expect(missingValues.code).toBe('SDP_PIVOT_VALUES_REQUIRED');
    });
});

describe('evaluateSdpCompatibility and runtime gating', () => {
    const simpleNodes = [
        makeNode('src', 'SOURCE', 'KafkaSource', 'Orders Stream'),
        makeNode('snk', 'SINK', 'IcebergSink', 'Bronze Orders'),
    ];
    const simpleEdges = [makeEdge('src', 'snk')];

    it('skips runtime version enforcement by default in evaluateSdpCompatibility for canvas switching', () => {
        const switchCheck = evaluateSdpCompatibility(simpleNodes, simpleEdges, {
            runner: 'Local',
        });
        expect(switchCheck.isCompatible).toBe(true);
        expect(switchCheck.blockingIssues).toEqual([]);

        const strictCheck = evaluateSdpCompatibility(
            simpleNodes,
            simpleEdges,
            { runner: 'Local' },
            { enforceRuntimeVersion: true }
        );
        expect(strictCheck.isCompatible).toBe(false);
        expect(strictCheck.blockingIssues.map((i) => i.code)).toContain(
            'SDP_SPARK_42_REQUIRED'
        );
    });

    it('enforces Spark 4.2+ runtimes in buildSdpPlan when jobConfig.runner is configured', () => {
        for (const badConfig of [
            { runner: 'ServerlessSpark', runtimeVersion: '3.0' },
            { runner: 'DataprocCluster', runtimeVersion: '3.0' },
            { runner: 'Local' },
        ]) {
            const plan = buildSdpPlan(simpleNodes, simpleEdges, badConfig);
            expect(plan.isCompatible).toBe(false);
            expect(plan.blockingIssues.map((i) => i.code)).toContain('SDP_SPARK_42_REQUIRED');
        }

        for (const goodConfig of [
            { runner: 'ServerlessSpark', runtimeVersion: '4.0' },
            { runner: 'DataprocCluster', runtimeVersion: '4.0' },
            { runner: 'Local', runtimeVersion: '4.2-connect' },
        ]) {
            const plan = buildSdpPlan(simpleNodes, simpleEdges, goodConfig);
            expect(plan.isCompatible).toBe(true);
        }
    });

    it('emits MISSING_LOCAL_ICEBERG_RUNTIME warning when local_4.2_connect uses Iceberg', () => {
        const localIcebergPlan = buildSdpPlan(simpleNodes, simpleEdges, {
            runner: 'Local',
            runtimeVersion: '4.2-connect',
        });
        expect(localIcebergPlan.isCompatible).toBe(true);
        expect(
            localIcebergPlan.warnings.some((w) => w.includes('MISSING_LOCAL_ICEBERG_RUNTIME'))
        ).toBe(true);
    });

    it('normalizes default SDP runtime profiles across Local, DataprocCluster, and ServerlessSpark while preserving explicit downgrades', () => {
        expect(normalizeSdpJobConfig(undefined)).toEqual({
            runner: 'Local',
            runtimeVersion: '4.2-connect',
            useSparkConnect: true,
        });
        expect(normalizeSdpJobConfig({})).toEqual({
            runner: 'Local',
            runtimeVersion: '4.2-connect',
            useSparkConnect: true,
        });
        expect(normalizeSdpJobConfig({ runner: 'Local' })).toEqual({
            runner: 'Local',
            runtimeVersion: '4.2-connect',
            useSparkConnect: true,
        });
        expect(
            normalizeSdpJobConfig({
                runner: 'Local',
                runtimeVersion: '4.0',
                useSparkConnect: false,
            })
        ).toEqual({
            runner: 'Local',
            runtimeVersion: '4.2-connect',
            useSparkConnect: true,
        });
        expect(
            normalizeSdpJobConfig({
                runner: 'DirectRunner',
                runtimeVersion: '',
            })
        ).toEqual({
            runner: 'Local',
            runtimeVersion: '4.2-connect',
            useSparkConnect: true,
        });
        expect(
            normalizeSdpJobConfig({
                runner: 'DirectRunner',
                runtimeVersion: '4.0',
            })
        ).toEqual({
            runner: 'Local',
            runtimeVersion: '4.2-connect',
            useSparkConnect: true,
        });
        expect(
            normalizeSdpJobConfig({
                runner: 'DirectRunner',
                runtimeVersion: '3.5',
            })
        ).toEqual({
            runner: 'Local',
            runtimeVersion: '3.5',
        });
        expect(
            normalizeSdpJobConfig({
                runner: 'Local',
                runtimeVersion: '4.2-connect',
            })
        ).toEqual({
            runner: 'Local',
            runtimeVersion: '4.2-connect',
            useSparkConnect: true,
        });

        // Explicit incompatible Local versions remain untouched so SDP_SPARK_42_REQUIRED fires.
        for (const explicitLocalVersion of ['4.0.1', '3.0', '3.5', '4.1', '2.2', '2.3']) {
            const raw = { runner: 'Local', runtimeVersion: explicitLocalVersion };
            expect(normalizeSdpJobConfig(raw)).toEqual(raw);
            const plan = buildSdpPlan(simpleNodes, simpleEdges, normalizeSdpJobConfig(raw));
            expect(plan.isCompatible).toBe(false);
            expect(plan.blockingIssues.map((i) => i.code)).toContain('SDP_SPARK_42_REQUIRED');
        }

        // DataprocCluster unset or 4.2-connect upgrades to 4.0, while explicit 3.0 stays untouched.
        expect(normalizeSdpJobConfig({ runner: 'DataprocCluster' })).toEqual({
            runner: 'DataprocCluster',
            runtimeVersion: '4.0',
        });
        expect(
            normalizeSdpJobConfig({ runner: 'DataprocCluster', runtimeVersion: '4.2-connect' })
        ).toEqual({
            runner: 'DataprocCluster',
            runtimeVersion: '4.0',
        });
        expect(normalizeSdpJobConfig({ runner: 'DataprocCluster', runtimeVersion: '4.0' })).toEqual({
            runner: 'DataprocCluster',
            runtimeVersion: '4.0',
        });
        expect(normalizeSdpJobConfig({ runner: 'DataprocCluster', runtimeVersion: '3.0' })).toEqual({
            runner: 'DataprocCluster',
            runtimeVersion: '3.0',
        });
        expect(normalizeSdpJobConfig({ runner: 'DataprocCluster', imageVersion: '3.0' })).toEqual({
            runner: 'DataprocCluster',
            imageVersion: '3.0',
        });

        // ServerlessSpark unset or 4.2-connect upgrades to 4.0, while explicit 3.0 stays untouched.
        expect(normalizeSdpJobConfig({ runner: 'ServerlessSpark' })).toEqual({
            runner: 'ServerlessSpark',
            runtimeVersion: '4.0',
        });
        expect(normalizeSdpJobConfig({ runner: 'Spark' })).toEqual({
            runner: 'ServerlessSpark',
            runtimeVersion: '4.0',
        });
        expect(
            buildSdpPlan(simpleNodes, simpleEdges, normalizeSdpJobConfig({ runner: 'Spark' })).runtimeArtifacts.profile.id
        ).toBe('serverless_4.0');
        expect(normalizeSdpJobConfig({ runner: 'Spark', runtimeVersion: '3.0' })).toEqual({
            runner: 'ServerlessSpark',
            runtimeVersion: '3.0',
        });
        expect(
            buildSdpPlan(
                simpleNodes,
                simpleEdges,
                normalizeSdpJobConfig({ runner: 'Spark', runtimeVersion: '3.0' })
            ).runtimeArtifacts.profile.id
        ).toBe('serverless_3.0');
        expect(
            normalizeSdpJobConfig({ runner: 'ServerlessSpark', runtimeVersion: '4.2-connect' })
        ).toEqual({
            runner: 'ServerlessSpark',
            runtimeVersion: '4.0',
        });
        expect(normalizeSdpJobConfig({ runner: 'ServerlessSpark', runtimeVersion: '3.0' })).toEqual({
            runner: 'ServerlessSpark',
            runtimeVersion: '3.0',
        });
        expect(normalizeSdpJobConfig({ runner: 'serverless_3.0' })).toEqual({
            runner: 'serverless_3.0',
        });
        expect(normalizeSdpJobConfig({ runner: 'cluster_3.0' })).toEqual({
            runner: 'cluster_3.0',
        });
    });
});

describe('buildSdpPlan blocking rules', () => {
    it('blocks pipeline-level and sink-level continuous streaming triggers', () => {
        const nodes = [
            makeNode('src', 'SOURCE', 'KafkaSource', 'Events'),
            makeNode('snk', 'SINK', 'KafkaSink', 'Out Events', { triggerType: 'continuous' }),
        ];
        const edges = [makeEdge('src', 'snk')];

        const plan = buildSdpPlan(nodes, edges, {
            runner: 'ServerlessSpark',
            runtimeVersion: '4.0',
            defaultTriggerType: 'continuous',
        });
        expect(plan.isCompatible).toBe(false);
        const triggerIssues = plan.blockingIssues.filter(
            (i) => i.code === 'SDP_CONTINUOUS_TRIGGER_UNSUPPORTED'
        );
        expect(triggerIssues).toHaveLength(2);
    });

    it('blocks streaming GroupByAgg, RollupCube, Deduplicate, and stream-stream Join without Watermark', () => {
        const aggNodes = [
            makeNode('s1', 'SOURCE', 'KafkaSource', 'Clicks'),
            makeNode('agg', 'STEP', 'GroupByAgg', 'Click Counts'),
            makeNode('snk', 'SINK', 'IcebergSink', 'Gold Clicks'),
        ];
        const aggPlan = buildSdpPlan(aggNodes, [makeEdge('s1', 'agg'), makeEdge('agg', 'snk')]);
        expect(aggPlan.isCompatible).toBe(false);
        expect(aggPlan.blockingIssues.map((i) => i.code)).toContain(
            'SDP_STREAMING_WATERMARK_REQUIRED'
        );

        const dedupNodes = [
            makeNode('s1', 'SOURCE', 'KafkaSource', 'Clicks'),
            makeNode('dedup', 'STEP', 'Deduplicate', 'Unique Clicks'),
            makeNode('snk', 'SINK', 'IcebergSink', 'Silver Clicks'),
        ];
        const dedupPlan = buildSdpPlan(dedupNodes, [
            makeEdge('s1', 'dedup'),
            makeEdge('dedup', 'snk'),
        ]);
        expect(dedupPlan.blockingIssues.map((i) => i.code)).toContain(
            'SDP_STREAMING_WATERMARK_REQUIRED'
        );

        const joinNodes = [
            makeNode('leftSrc', 'SOURCE', 'KafkaSource', 'Impressions'),
            makeNode('rightSrc', 'SOURCE', 'KafkaSource', 'Clicks'),
            makeNode('wmLeft', 'STEP', 'Watermark', 'Left Watermark'),
            makeNode('join', 'STEP', 'Join', 'Ad Join'),
            makeNode('snk', 'SINK', 'IcebergSink', 'Matched Ads'),
        ];
        const missingRightWmPlan = buildSdpPlan(joinNodes, [
            makeEdge('leftSrc', 'wmLeft'),
            makeEdge('wmLeft', 'join', { targetHandle: 'left' }),
            makeEdge('rightSrc', 'join', { targetHandle: 'right' }),
            makeEdge('join', 'snk'),
        ]);
        expect(missingRightWmPlan.blockingIssues.map((i) => i.code)).toContain(
            'SDP_STREAMING_WATERMARK_REQUIRED'
        );

        const watermarkedJoinNodes = [
            ...joinNodes,
            makeNode('wmRight', 'STEP', 'Watermark', 'Right Watermark'),
        ];
        const validJoinPlan = buildSdpPlan(watermarkedJoinNodes, [
            makeEdge('leftSrc', 'wmLeft'),
            makeEdge('wmLeft', 'join', { targetHandle: 'left' }),
            makeEdge('rightSrc', 'wmRight'),
            makeEdge('wmRight', 'join', { targetHandle: 'right' }),
            makeEdge('join', 'snk'),
        ]);
        expect(validJoinPlan.isCompatible).toBe(true);
        expect(validJoinPlan.datasetsByNodeId.wmLeft.kind).toBe('temporary_view');
        expect(validJoinPlan.datasetsByNodeId.wmLeft.badge).toBe('VIEW');
    });

    it('enforces Auto CDC MERGE target support, single-flow constraint, and column identifier rules', () => {
        const parquetTargetNodes = [
            makeNode('cdcSrc', 'SOURCE', 'JsonSource', 'CDC Events', { readMode: 'streaming' }),
            makeNode('cdc', 'STEP', 'MergeCdcScd', 'Merge Customers', {
                scdType: 'scd1',
                keyColumns: ['customer_id'],
                sequenceColumn: 'updated_at',
            }),
            makeNode('parquetSnk', 'SINK', 'ParquetSink', 'Parquet Out'),
        ];
        const parquetPlan = buildSdpPlan(parquetTargetNodes, [
            makeEdge('cdcSrc', 'cdc', { targetHandle: 'left' }),
            makeEdge('cdc', 'parquetSnk'),
        ]);
        expect(parquetPlan.isCompatible).toBe(false);
        expect(parquetPlan.blockingIssues.map((i) => i.code)).toContain(
            'AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE'
        );

        // Multiple flows into one Auto CDC target sink must fail with AUTOCDC_MULTIPLE_FLOWS_TO_TARGET.
        const multiFlowNodes = [
            makeNode('s1', 'SOURCE', 'KafkaSource', 'CDC East'),
            makeNode('s2', 'SOURCE', 'KafkaSource', 'CDC West'),
            makeNode('cdc1', 'STEP', 'MergeCdcScd', 'Merge East', { scdType: 'scd1' }),
            makeNode('cdc2', 'STEP', 'MergeCdcScd', 'Merge West', { scdType: 'scd1' }),
            makeNode('iceSnk', 'SINK', 'IcebergSink', 'Silver Customers', {
                sdpTableName: 'silver_customers',
            }),
        ];
        const multiFlowPlan = buildSdpPlan(multiFlowNodes, [
            makeEdge('s1', 'cdc1', { targetHandle: 'left' }),
            makeEdge('s2', 'cdc2', { targetHandle: 'left' }),
            makeEdge('cdc1', 'iceSnk'),
            makeEdge('cdc2', 'iceSnk'),
        ]);
        expect(multiFlowPlan.blockingIssues.map((i) => i.code)).toContain(
            'AUTOCDC_MULTIPLE_FLOWS_TO_TARGET'
        );

        // Qualified or __spark_autocdc_ column names must fail validation.
        const badColNodes = [
            makeNode('cdcSrc', 'SOURCE', 'KafkaSource', 'CDC Events'),
            makeNode('cdc', 'STEP', 'MergeCdcScd', 'Merge Customers', {
                scdType: 'scd1',
                keyColumns: ['payload.customer_id'],
                sequenceColumn: '__spark_autocdc_seq',
            }),
            makeNode('iceSnk', 'SINK', 'IcebergSink', 'Silver Customers'),
        ];
        const badColPlan = buildSdpPlan(badColNodes, [
            makeEdge('cdcSrc', 'cdc', { targetHandle: 'left' }),
            makeEdge('cdc', 'iceSnk'),
        ]);
        expect(badColPlan.blockingIssues.map((i) => i.code)).toContain(
            'AUTOCDC_MULTIPART_COLUMN_IDENTIFIER'
        );
        expect(badColPlan.blockingIssues.map((i) => i.code)).toContain(
            'AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT'
        );
    });

    it('blocks mutually exclusive partitionCols and clusterBy on the same node', () => {
        const nodes = [
            makeNode('src', 'SOURCE', 'KafkaSource', 'Events'),
            makeNode('snk', 'SINK', 'IcebergSink', 'Partitioned And Clustered', {
                sdpPartitionCols: ['event_date'],
                sdpClusterBy: ['customer_id'],
            }),
        ];
        const plan = buildSdpPlan(nodes, [makeEdge('src', 'snk')]);
        expect(plan.isCompatible).toBe(false);
        expect(plan.blockingIssues.map((i) => i.code)).toContain(
            'SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED'
        );
    });

    it('blocks INVALID_RESETTABLE_DEPENDENCY when a protected streaming table reads from a resettable upstream streaming table', () => {
        const blockedNodes = [
            makeNode('bronze', 'SOURCE', 'KafkaSource', 'Bronze Stream'),
            makeNode('viewStep', 'STEP', 'Select', 'Projected View', {
                sdpDatasetType: 'temporary_view',
            }),
            makeNode('silver', 'SINK', 'IcebergSink', 'Silver Protected', {
                sdpResetAllowed: false,
            }),
        ];
        const blockedPlan = buildSdpPlan(blockedNodes, [
            makeEdge('bronze', 'viewStep'),
            makeEdge('viewStep', 'silver'),
        ]);
        expect(blockedPlan.isCompatible).toBe(false);
        expect(blockedPlan.blockingIssues.map((i) => i.code)).toContain(
            'INVALID_RESETTABLE_DEPENDENCY'
        );

        const validNodes = [
            makeNode('bronze', 'SOURCE', 'KafkaSource', 'Bronze Stream', {
                tableProperties: "'pipelines.reset.allowed' = 'false'",
            }),
            makeNode('silver', 'SINK', 'IcebergSink', 'Silver Protected', {
                sdpResetAllowed: false,
            }),
        ];
        const validPlan = buildSdpPlan(validNodes, [makeEdge('bronze', 'silver')]);
        expect(validPlan.isCompatible).toBe(true);
    });

    it('detects cyclic dependencies safely without throwing', () => {
        const nodes = [
            makeNode('a', 'STEP', 'Filter', 'Step A'),
            makeNode('b', 'STEP', 'Select', 'Step B'),
        ];
        const edges = [makeEdge('a', 'b'), makeEdge('b', 'a')];
        const plan = buildSdpPlan(nodes, edges);
        expect(plan.isCompatible).toBe(false);
        expect(plan.sortedNodeIds).toEqual([]);
        expect(plan.blockingIssues.map((i) => i.code)).toContain('SDP_CYCLE_DETECTED');
    });
});

describe('buildSdpPlan deterministic dataset mappings', () => {
    it('maps a BigLake Iceberg medallion pipeline to streaming tables and gold materialized views', () => {
        const nodes = [
            makeNode('csvSrc', 'SOURCE', 'CsvSource', 'Bronze Orders', {
                readMode: 'streaming',
            }),
            makeNode('clean', 'STEP', 'WithColumns', 'Clean Orders'),
            makeNode('wm', 'STEP', 'Watermark', 'Order Watermark'),
            makeNode('dq', 'STEP', 'DataQualityAssert', 'Valid Orders'),
            makeNode('goldAgg', 'STEP', 'GroupByAgg', 'Daily Revenue'),
            makeNode('goldSink', 'SINK', 'IcebergSink', 'Gold Store Day', {
                sdpTableName: 'gold_store_day',
                sdpFormat: 'iceberg',
                sdpPartitionCols: ['day'],
            }),
        ];
        const edges = [
            makeEdge('csvSrc', 'clean'),
            makeEdge('clean', 'wm'),
            makeEdge('wm', 'dq'),
            makeEdge('dq', 'goldAgg'),
            makeEdge('goldAgg', 'goldSink'),
        ];

        const plan = buildSdpPlan(nodes, edges, {
            runner: 'ServerlessSpark',
            runtimeVersion: '4.0',
        });

        expect(plan.isCompatible).toBe(true);
        expect(plan.blockingIssues).toEqual([]);
        expect(plan.warnings).toEqual([]);

        expect(plan.datasetsByNodeId.csvSrc).toMatchObject({
            datasetName: 'bronze_orders',
            kind: 'streaming_table',
            badge: 'TABLE',
            isStreaming: true,
        });
        expect(plan.datasetsByNodeId.dq).toMatchObject({
            datasetName: 'valid_orders',
            kind: 'streaming_table',
            badge: 'TABLE',
            quarantineDatasetName: 'valid_orders_quarantine',
        });
        expect(plan.datasetsByNodeId.goldAgg).toMatchObject({
            datasetName: 'daily_revenue',
            kind: 'materialized_view',
            badge: 'MV',
            isStreaming: false,
        });
        expect(plan.datasetsByNodeId.goldSink).toMatchObject({
            datasetName: 'gold_store_day',
            kind: 'materialized_view',
            badge: 'MV',
            isStreaming: false,
            format: 'iceberg',
            partitionCols: ['day'],
        });
        expect(plan.materializedViewNames).toEqual(['daily_revenue', 'gold_store_day']);
        expect(plan.hasExternalSinks).toBe(false);
    });

    it('maps SCD Type 1 MergeCdcScd with single left input to auto_cdc without requiring right input', () => {
        const nodes = [
            makeNode('cdcSrc', 'SOURCE', 'JsonSource', 'Bronze Customer CDC', {
                readMode: 'streaming',
            }),
            makeNode('cdcMerge', 'STEP', 'MergeCdcScd', 'Apply Customer CDC', {
                scdType: 'scd1',
                keyColumns: ['customer_id'],
                sequenceColumn: 'updated_at',
                cdcFormat: 'custom_op',
                operationColumn: 'op',
                deleteValues: 'DELETE',
            }),
            makeNode('iceSink', 'SINK', 'IcebergSink', 'Silver Customers', {
                sdpTableName: 'silver_customers',
                sdpFormat: 'iceberg',
            }),
        ];
        const edges = [
            makeEdge('cdcSrc', 'cdcMerge', { targetHandle: 'left' }),
            makeEdge('cdcMerge', 'iceSink'),
        ];

        const plan = buildSdpPlan(nodes, edges, {
            runner: 'ServerlessSpark',
            runtimeVersion: '4.0',
        });

        expect(plan.isCompatible).toBe(true);
        expect(plan.blockingIssues).toEqual([]);
        expect(plan.warnings).toEqual([]);
        expect(plan.datasetsByNodeId.cdcMerge).toMatchObject({
            datasetName: 'apply_customer_cdc',
            kind: 'auto_cdc',
            badge: 'CDC',
            targetDataset: 'silver_customers',
            upstreamDatasets: ['bronze_customer_cdc'],
            format: 'iceberg',
        });
        expect(plan.datasetsByNodeId.iceSink).toMatchObject({
            datasetName: 'silver_customers',
            kind: 'auto_cdc',
            badge: 'CDC',
            format: 'iceberg',
        });
        expect(plan.streamingTableNames).toEqual(['bronze_customer_cdc', 'silver_customers']);
    });

    it('maps UnionByName, external sinks, batch views, and disambiguates colliding dataset names', () => {
        const nodes = [
            makeNode('west', 'SOURCE', 'KafkaSource', 'Orders'),
            makeNode('east', 'SOURCE', 'KafkaSource', 'Orders'),
            makeNode('union', 'STEP', 'UnionByName', 'Merged Orders'),
            makeNode('kafkaOut', 'SINK', 'KafkaSink', 'Orders Out', {
                sdpSinkName: 'orders_kafka_sink',
            }),
            makeNode('bqBatch', 'SOURCE', 'BigQuerySource', 'Dim Stores'),
            makeNode('batchProj', 'STEP', 'Select', 'Trim Stores'),
        ];
        const edges = [
            makeEdge('west', 'union'),
            makeEdge('east', 'union'),
            makeEdge('union', 'kafkaOut'),
            makeEdge('bqBatch', 'batchProj'),
        ];

        const plan = buildSdpPlan(nodes, edges);
        expect(plan.isCompatible).toBe(true);
        expect(plan.datasetsByNodeId.west.datasetName).toBe('orders');
        expect(plan.datasetsByNodeId.east.datasetName).toBe('orders_2');
        expect(plan.datasetsByNodeId.union).toMatchObject({
            kind: 'append_flow',
            badge: 'TABLE',
            isStreaming: true,
        });
        expect(plan.datasetsByNodeId.kafkaOut).toMatchObject({
            datasetName: 'orders_kafka_sink',
            kind: 'external_sink',
            badge: 'SINK',
            format: 'kafka',
        });
        expect(plan.datasetsByNodeId.bqBatch).toMatchObject({
            kind: 'temporary_view',
            badge: 'VIEW',
            isStreaming: false,
        });
        expect(plan.datasetsByNodeId.batchProj).toMatchObject({
            kind: 'temporary_view',
            badge: 'VIEW',
            isStreaming: false,
        });
        expect(plan.hasExternalSinks).toBe(true);
        expect(plan.externalSinkNames).toEqual(['orders_kafka_sink']);
    });
});
