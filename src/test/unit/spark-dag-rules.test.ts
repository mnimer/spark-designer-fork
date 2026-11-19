/**
 * @fileoverview Unit tests for Spark node capabilities and Spark DAG semantic rules.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies 100% of capability flag lookups across all Spark sources, sinks, and steps,
 * read mode resolution, Structured Streaming propagation, Stream-Static Join classification,
 * DLQ error handle gating, input arity resolution, and ConditionalSplit handle derivation.
 */

import { describe, expect, it } from 'vitest';
import type { DagEdge, DagNode } from '../../dagValidator';
import {
    computeStreamingNodeSet,
    getConditionalSplitHandles,
    getEffectiveSourceReadMode,
    getNodeCapabilities,
    getNodeCategory,
    getNodeInputArity,
    getNodeSparkTransform,
    isConditionalSplitNode,
    isDualInputNode,
    isErrorCapableStep,
    isJoinNode,
    isMergeCdcScdNode,
    SPARK_NODE_CAPABILITIES,
} from '../../shared/sparkDagRules';

describe('SPARK_NODE_CAPABILITIES and getNodeCapabilities (A1)', () => {
    const STREAMING_ONLY_SOURCES = [
        'KinesisSource',
        'MqttSource',
        'RateSource',
        'SocketSource',
        'GenerateSequence',
        'PeriodicImpulse',
    ];

    const DUAL_STREAM_DEFAULT_SOURCES = [
        'KafkaSource',
        'PubSubSource',
        'PulsarSource',
        'EventHubsSource',
    ];

    const DUAL_BATCH_DEFAULT_SOURCES = [
        'IcebergSource',
        'DeltaSource',
        'HudiSource',
        'ParquetSource',
        'OrcSource',
        'AvroSource',
        'CsvSource',
        'JsonSource',
        'TextSource',
        'XmlSource',
        'BinaryFileSource',
        'MongoDbSource',
        'RedisSource',
        'Neo4jSource',
    ];

    const BATCH_ONLY_SOURCES = [
        'HiveSource',
        'TFRecordSource',
        'BigQuerySource',
        'SpannerSource',
        'BigtableSource',
        'CloudSqlSource',
        'AlloyDbSource',
        'JdbcSource',
        'SnowflakeSource',
        'RedshiftSource',
        'ClickHouseSource',
        'SingleStoreSource',
        'FirestoreSource',
        'CassandraSource',
        'ElasticsearchSource',
        'CreateDataFrameSource',
    ];

    const NATIVE_STREAMING_SINKS = [
        'KafkaSink',
        'PulsarSink',
        'EventHubsSink',
        'IcebergSink',
        'DeltaSink',
        'HudiSink',
        'ParquetSink',
        'OrcSink',
        'AvroSink',
        'CsvSink',
        'JsonSink',
        'TextSink',
        'BigQuerySink',
        'MongoDbSink',
        'CassandraSink',
        'ElasticsearchSink',
        'Neo4jSink',
        'ConsoleSink',
        'MemorySink',
    ];

    const FOREACH_BATCH_SINKS = [
        'PubSubSink',
        'KinesisSink',
        'MqttSink',
        'HiveSink',
        'XmlSink',
        'TFRecordSink',
        'SpannerSink',
        'BigtableSink',
        'CloudSqlSink',
        'AlloyDbSink',
        'JdbcSink',
        'SnowflakeSink',
        'RedshiftSink',
        'ClickHouseSink',
        'SingleStoreSink',
        'FirestoreSink',
        'RedisSink',
        'ForeachBatchSink',
    ];

    const ERROR_CAPABLE_STEPS = [
        'ParsePayload',
        'Filter',
        'DataQualityAssert',
        'PandasUdfTransform',
        'CustomTransform',
        'SparkSqlQuery',
        'SparkMlInference',
        'VertexAiInference',
        'SparkMlAndVertexAi',
        'StatefulTransform',
    ];

    const STRUCTURAL_STEPS = [
        'Select',
        'WithColumns',
        'RenameDropColumns',
        'Explode',
        'HandleNulls',
        'OrderBy',
        'LimitSample',
        'ConditionalSplit',
        'Join',
        'UnionByName',
        'SetOperation',
        'GroupByAgg',
        'TopN',
        'Deduplicate',
        'PivotUnpivot',
        'RollupCube',
        'WindowOver',
        'Watermark',
        'RepartitionCoalesce',
        'CachePersist',
        'FeaturePrep',
        'ParDo',
        'ParseMessage',
        'Flatten',
    ];

    it('registers all 38 Spark sources with exact supportedReadModes and defaultReadMode', () => {
        expect(
            STREAMING_ONLY_SOURCES.filter((s) => s.endsWith('Source')).length +
                DUAL_STREAM_DEFAULT_SOURCES.length +
                DUAL_BATCH_DEFAULT_SOURCES.length +
                BATCH_ONLY_SOURCES.length
        ).toBe(38);

        for (const name of STREAMING_ONLY_SOURCES) {
            expect(getNodeCapabilities(name, 'SOURCE')).toEqual({
                supportedReadModes: ['streaming'],
                defaultReadMode: 'streaming',
            });
        }
        for (const name of DUAL_STREAM_DEFAULT_SOURCES) {
            expect(getNodeCapabilities(name, 'SOURCE')).toEqual({
                supportedReadModes: ['batch', 'streaming'],
                defaultReadMode: 'streaming',
            });
        }
        for (const name of DUAL_BATCH_DEFAULT_SOURCES) {
            expect(getNodeCapabilities(name, 'SOURCE')).toEqual({
                supportedReadModes: ['batch', 'streaming'],
                defaultReadMode: 'batch',
            });
        }
        for (const name of BATCH_ONLY_SOURCES) {
            expect(getNodeCapabilities(name, 'SOURCE')).toEqual({
                supportedReadModes: ['batch'],
                defaultReadMode: 'batch',
            });
        }
    });

    it('registers all 37 Spark sinks with exact streamingSinkMode and inputArity 1', () => {
        expect(NATIVE_STREAMING_SINKS.length + FOREACH_BATCH_SINKS.length).toBe(37);

        for (const name of NATIVE_STREAMING_SINKS) {
            expect(getNodeCapabilities(name, 'SINK')).toEqual({
                streamingSinkMode: 'native',
                inputArity: 1,
            });
        }
        for (const name of FOREACH_BATCH_SINKS) {
            expect(getNodeCapabilities(name, 'SINK')).toEqual({
                streamingSinkMode: 'foreachBatch',
                inputArity: 1,
            });
        }
    });

    it('registers all 29 Spark steps plus ML aliases and legacy transforms accurately', () => {
        for (const name of ERROR_CAPABLE_STEPS) {
            const caps = getNodeCapabilities(name, 'STEP');
            expect(caps.errorOutput, `${name} should have errorOutput: true`).toBe(true);
            expect(caps.inputArity).toBe(name === 'SparkSqlQuery' ? 'n' : 1);
        }

        for (const name of STRUCTURAL_STEPS) {
            const caps = getNodeCapabilities(name, 'STEP');
            expect(caps.errorOutput, `${name} should have errorOutput: false`).toBe(false);
        }

        expect(SPARK_NODE_CAPABILITIES.Join.inputArity).toBe(2);
        expect(SPARK_NODE_CAPABILITIES.UnionByName.inputArity).toBe('n');
        expect(SPARK_NODE_CAPABILITIES.SetOperation.inputArity).toBe('n');
        expect(SPARK_NODE_CAPABILITIES.SparkSqlQuery.inputArity).toBe('n');
        expect(SPARK_NODE_CAPABILITIES.Flatten.inputArity).toBe('n');
    });

    it('supports SOURCE:, SINK:, STEP: prefixes, legacy dual-direction transforms, and defensive copies', () => {
        expect(getNodeCapabilities('SOURCE:PubSubIO')).toEqual({
            supportedReadModes: ['batch', 'streaming'],
            defaultReadMode: 'streaming',
        });
        expect(getNodeCapabilities('SINK:PubSubIO')).toEqual({
            streamingSinkMode: 'foreachBatch',
            inputArity: 1,
        });
        expect(getNodeCapabilities('BigQueryIO', 'SOURCE')).toEqual({
            supportedReadModes: ['batch'],
            defaultReadMode: 'batch',
        });
        expect(getNodeCapabilities('BigQueryIO', 'SINK')).toEqual({
            streamingSinkMode: 'native',
            inputArity: 1,
        });
        expect(getNodeCapabilities('STEP:Join')).toEqual({
            inputArity: 2,
            errorOutput: false,
        });

        // Unknown / empty fallbacks
        expect(getNodeCapabilities(undefined, 'SOURCE')).toEqual({
            supportedReadModes: ['batch'],
            defaultReadMode: 'batch',
        });
        expect(getNodeCapabilities('UnknownCustomSink', 'SINK')).toEqual({
            streamingSinkMode: 'foreachBatch',
            inputArity: 1,
        });
        expect(getNodeCapabilities('UnknownCustomStep', 'STEP')).toEqual({
            errorOutput: false,
            inputArity: 1,
        });
        expect(getNodeCapabilities('')).toEqual({});

        // Mutating returned supportedReadModes must not mutate SPARK_NODE_CAPABILITIES
        const copy = getNodeCapabilities('KafkaSource');
        copy.supportedReadModes?.push('batch');
        expect(SPARK_NODE_CAPABILITIES.KafkaSource.supportedReadModes).toEqual([
            'batch',
            'streaming',
        ]);
    });
});

describe('sparkDagRules helpers (A3)', () => {
    it('extracts node category and spark transform safely across React Flow and YAML shapes', () => {
        expect(getNodeCategory({ id: 'n1', type: 'pipeline', data: { type: 'source' } })).toBe('SOURCE');
        expect(getNodeCategory({ id: 'n2', type: 'SINK' })).toBe('SINK');
        expect(getNodeCategory({ id: 'n3', type: 'pipeline' })).toBeUndefined();

        expect(getNodeSparkTransform({ id: 'n1', data: { spark_transform: ' Join ' } })).toBe('Join');
        expect(getNodeSparkTransform({ id: 'n2', data: { sparkTransform: 'Filter' } })).toBe('Filter');
        expect(getNodeSparkTransform({ id: 'n3', data: { spark_transform: '   ' } })).toBeUndefined();
    });

    it('getEffectiveSourceReadMode honors explicit batch/streaming overrides and capability defaults', () => {
        // Streaming-default dual-mode source defaults to streaming, can be overridden to batch
        expect(
            getEffectiveSourceReadMode({
                id: 'k1',
                data: { type: 'SOURCE', spark_transform: 'KafkaSource' },
            })
        ).toBe('streaming');
        expect(
            getEffectiveSourceReadMode({
                id: 'k2',
                data: {
                    type: 'SOURCE',
                    spark_transform: 'KafkaSource',
                    properties: { readMode: 'batch' },
                },
            })
        ).toBe('batch');

        // Batch-default dual-mode file/lakehouse source defaults to batch, can be overridden to streaming
        expect(
            getEffectiveSourceReadMode({
                id: 'p1',
                data: { type: 'SOURCE', spark_transform: 'ParquetSource' },
            })
        ).toBe('batch');
        expect(
            getEffectiveSourceReadMode({
                id: 'p2',
                data: {
                    type: 'SOURCE',
                    spark_transform: 'ParquetSource',
                    properties: { readMode: 'streaming' },
                },
            })
        ).toBe('streaming');

        // Legacy PubSubIO with readMode: 'subscription' falls back to capability default ('streaming')
        expect(
            getEffectiveSourceReadMode({
                id: 'ps1',
                data: {
                    type: 'SOURCE',
                    spark_transform: 'PubSubIO',
                    properties: { readMode: 'subscription' },
                },
            })
        ).toBe('streaming');

        // Explicit data.defaultReadMode fallback when transform is custom
        expect(
            getEffectiveSourceReadMode({
                id: 'c1',
                data: { type: 'SOURCE', defaultReadMode: 'streaming' },
            })
        ).toBe('streaming');
    });

    it('computeStreamingNodeSet keeps purely batch graphs non-streaming', () => {
        const nodes: DagNode[] = [
            { id: 'src1', data: { type: 'SOURCE', spark_transform: 'BigQuerySource' } },
            { id: 'src2', data: { type: 'SOURCE', spark_transform: 'CloudSqlSource' } },
            { id: 'join', data: { type: 'STEP', spark_transform: 'Join' } },
            { id: 'sink', data: { type: 'SINK', spark_transform: 'BigQuerySink' } },
        ];
        const edges: DagEdge[] = [
            { id: 'e1', source: 'src1', target: 'join', targetHandle: 'left' },
            { id: 'e2', source: 'src2', target: 'join', targetHandle: 'right' },
            { id: 'e3', source: 'join', target: 'sink' },
        ];

        const res = computeStreamingNodeSet(nodes, edges);
        expect(res.hasStreamingSource).toBe(false);
        expect([...res.streamingNodeIds]).toEqual([]);
        expect([...res.streamingEdgeIds]).toEqual([]);
        expect(res.joinModes.get('join')).toBe('batch_batch');
    });

    it('computeStreamingNodeSet propagates streaming mode through Stream-Static Join while keeping static lookup branch batch', () => {
        const nodes: DagNode[] = [
            { id: 'kafka', data: { type: 'SOURCE', spark_transform: 'KafkaSource' } },
            { id: 'parse', data: { type: 'STEP', spark_transform: 'ParsePayload' } },
            { id: 'dlq', data: { type: 'SINK', spark_transform: 'ConsoleSink' } },
            { id: 'bq_dim', data: { type: 'SOURCE', spark_transform: 'BigQuerySource' } },
            { id: 'dim_filter', data: { type: 'STEP', spark_transform: 'Filter' } },
            { id: 'join', data: { type: 'STEP', spark_transform: 'Join' } },
            { id: 'iceberg', data: { type: 'SINK', spark_transform: 'IcebergSink' } },
        ];
        const edges: DagEdge[] = [
            { id: 'e_k_p', source: 'kafka', target: 'parse' },
            { id: 'e_p_dlq', source: 'parse', target: 'dlq', sourceHandle: 'error' },
            { id: 'e_p_join', source: 'parse', target: 'join', sourceHandle: 'valid', targetHandle: 'left' },
            { id: 'e_bq_f', source: 'bq_dim', target: 'dim_filter' },
            { id: 'e_f_join', source: 'dim_filter', target: 'join', targetHandle: 'right' },
            { id: 'e_join_sink', source: 'join', target: 'iceberg' },
        ];

        const res = computeStreamingNodeSet(nodes, edges);
        expect(res.hasStreamingSource).toBe(true);
        expect(res.joinModes.get('join')).toBe('stream_static');

        // Streaming branch and everything downstream of Join is streaming
        expect(res.streamingNodeIds.has('kafka')).toBe(true);
        expect(res.streamingNodeIds.has('parse')).toBe(true);
        expect(res.streamingNodeIds.has('dlq')).toBe(true);
        expect(res.streamingNodeIds.has('join')).toBe(true);
        expect(res.streamingNodeIds.has('iceberg')).toBe(true);

        // Static batch lookup branch stays batch!
        expect(res.streamingNodeIds.has('bq_dim')).toBe(false);
        expect(res.streamingNodeIds.has('dim_filter')).toBe(false);

        // Edge propagation
        expect([...res.streamingEdgeIds].sort()).toEqual(
            ['e_join_sink', 'e_k_p', 'e_p_dlq', 'e_p_join'].sort()
        );
        expect(res.streamingEdgeIds.has('e_bq_f')).toBe(false);
        expect(res.streamingEdgeIds.has('e_f_join')).toBe(false);
    });

    it('computeStreamingNodeSet classifies Stream-Stream Joins and terminates safely on cyclic or malformed graphs', () => {
        const nodes: DagNode[] = [
            { id: 's1', data: { type: 'SOURCE', spark_transform: 'RateSource' } },
            {
                id: 's2',
                data: {
                    type: 'SOURCE',
                    spark_transform: 'IcebergSource',
                    properties: { readMode: 'streaming' },
                },
            },
            { id: 'join', data: { type: 'STEP', spark_transform: 'Join' } },
            { id: 'step_cycle', data: { type: 'STEP', spark_transform: 'WithColumns' } },
        ];
        const edges: DagEdge[] = [
            { id: 'e1', source: 's1', target: 'join', targetHandle: 'left' },
            { id: 'e2', source: 's2', target: 'join', targetHandle: 'right' },
            { id: 'e3', source: 'join', target: 'step_cycle' },
            { id: 'e4', source: 'step_cycle', target: 'join' }, // cycle
            { id: 'e_dangling', source: 'missing', target: 'join' },
        ];

        const res = computeStreamingNodeSet(nodes, edges);
        expect(res.hasStreamingSource).toBe(true);
        expect(res.joinModes.get('join')).toBe('stream_stream');
        expect([...res.streamingEdgeIds].sort()).toEqual(['e1', 'e2', 'e3', 'e4']);
    });

    it('evaluates isErrorCapableStep, getNodeInputArity, isJoinNode, and isConditionalSplitNode', () => {
        expect(isErrorCapableStep({ id: '1', data: { type: 'STEP', spark_transform: 'ParsePayload' } })).toBe(true);
        expect(isErrorCapableStep({ id: '2', data: { type: 'STEP', spark_transform: 'Select' } })).toBe(false);
        expect(isErrorCapableStep({ id: '3', data: { type: 'STEP', spark_transform: 'Select', errorOutput: true } })).toBe(true);
        expect(isErrorCapableStep({ id: '4', data: { type: 'SOURCE', spark_transform: 'KafkaSource', errorOutput: true } })).toBe(false);
        expect(isErrorCapableStep({ id: '5', data: { type: 'SINK', spark_transform: 'ConsoleSink' } })).toBe(false);

        expect(getNodeInputArity({ id: 'j', data: { type: 'STEP', spark_transform: 'Join' } })).toBe(2);
        expect(getNodeInputArity({ id: 'u', data: { type: 'STEP', spark_transform: 'UnionByName' } })).toBe('n');
        expect(getNodeInputArity({ id: 's', data: { type: 'STEP', spark_transform: 'SetOperation' } })).toBe('n');
        expect(getNodeInputArity({ id: 'q', data: { type: 'STEP', spark_transform: 'SparkSqlQuery' } })).toBe('n');
        expect(getNodeInputArity({ id: 'f', data: { type: 'STEP', label: 'Flatten' } })).toBe('n');
        expect(getNodeInputArity({ id: 'm', data: { type: 'STEP', spark_transform: 'WithColumns' } })).toBe(1);
        expect(getNodeInputArity({ id: 'x', data: { type: 'STEP', inputArity: 2 } })).toBe(2);

        expect(isJoinNode({ id: 'j', data: { type: 'STEP', spark_transform: 'Join' } })).toBe(true);
        expect(isJoinNode({ id: 'j', data: { type: 'SOURCE', spark_transform: 'Join' } })).toBe(false);

        expect(isMergeCdcScdNode({ id: 'm', data: { type: 'STEP', spark_transform: 'MergeCdcScd' } })).toBe(true);
        expect(isMergeCdcScdNode({ id: 'm', data: { type: 'SOURCE', spark_transform: 'MergeCdcScd' } })).toBe(false);
        expect(isMergeCdcScdNode({ id: 'm', data: { type: 'SINK', spark_transform: 'MergeCdcScd' } })).toBe(false);
        expect(isMergeCdcScdNode({ id: 'm', data: { type: 'STEP', spark_transform: 'Join' } })).toBe(false);

        expect(isDualInputNode({ id: 'j', data: { type: 'STEP', spark_transform: 'Join' } })).toBe(true);
        expect(isDualInputNode({ id: 'm', data: { type: 'STEP', spark_transform: 'MergeCdcScd' } })).toBe(true);
        expect(isDualInputNode({ id: 's', data: { type: 'STEP', spark_transform: 'Select' } })).toBe(false);
        expect(getNodeInputArity({ id: 'm', data: { type: 'STEP', spark_transform: 'MergeCdcScd' } })).toBe(2);
        expect(SPARK_NODE_CAPABILITIES.MergeCdcScd).toEqual({ inputArity: 2, errorOutput: false });

        expect(isConditionalSplitNode({ id: 'c', data: { type: 'STEP', spark_transform: 'ConditionalSplit' } })).toBe(true);
        expect(isConditionalSplitNode({ id: 'c', data: { type: 'SINK', spark_transform: 'ConditionalSplit' } })).toBe(false);
    });

    it('derives ConditionalSplit handles with default rule_1, named rules, and collision protection', () => {
        // Empty / missing rules -> rule_1 + else
        expect(
            getConditionalSplitHandles({
                id: 'cs1',
                data: { type: 'STEP', spark_transform: 'ConditionalSplit' },
            })
        ).toEqual([
            { id: 'rule_1', label: 'rule_1', condition: '' },
            { id: 'else', label: 'else' },
        ]);

        expect(
            getConditionalSplitHandles({
                id: 'cs2',
                data: {
                    type: 'STEP',
                    spark_transform: 'ConditionalSplit',
                    properties: { rules: [] },
                },
            })
        ).toEqual([
            { id: 'rule_1', label: 'rule_1', condition: '' },
            { id: 'else', label: 'else' },
        ]);

        // Named rules + blank name fallback + duplicate/else disambiguation
        expect(
            getConditionalSplitHandles({
                id: 'cs3',
                data: {
                    type: 'STEP',
                    spark_transform: 'ConditionalSplit',
                    properties: {
                        rules: [
                            { name: 'vip', condition: 'tier == "VIP"' },
                            { name: '', condition: 'amount > 50' },
                            { name: 'vip', condition: 'score > 90' },
                            { name: 'else', condition: 'flag == true' },
                        ],
                    },
                },
            })
        ).toEqual([
            { id: 'vip', label: 'vip', condition: 'tier == "VIP"' },
            { id: 'rule_2', label: 'rule_2', condition: 'amount > 50' },
            { id: 'vip_3', label: 'vip', condition: 'score > 90' },
            { id: 'else_4', label: 'else', condition: 'flag == true' },
            { id: 'else', label: 'else' },
        ]);
    });
});

describe('sparkDagRules streaming propagation, Join detection, and arity boundaries (T1)', () => {
    it('returns empty results for empty, null, and malformed node and edge inputs', () => {
        const empty = computeStreamingNodeSet([], []);
        expect(empty.hasStreamingSource).toBe(false);
        expect(empty.streamingNodeIds.size).toBe(0);
        expect(empty.streamingEdgeIds.size).toBe(0);
        expect(empty.joinModes.size).toBe(0);

        const nullish = computeStreamingNodeSet(
            null as unknown as DagNode[],
            undefined as unknown as DagEdge[]
        );
        expect(nullish.hasStreamingSource).toBe(false);
        expect(nullish.joinModes.size).toBe(0);

        const malformed = computeStreamingNodeSet(
            [
                null as unknown as DagNode,
                { id: undefined } as unknown as DagNode,
                { id: 'rate', data: { type: 'SOURCE', spark_transform: 'RateSource' } },
                { id: 'sink', data: { type: 'SINK', spark_transform: 'ConsoleSink' } },
            ],
            [
                null as unknown as DagEdge,
                { id: 'no_target', source: 'rate' } as unknown as DagEdge,
                { id: 'e1', source: 'rate', target: 'sink' },
            ]
        );
        expect(malformed.hasStreamingSource).toBe(true);
        expect([...malformed.streamingNodeIds].sort()).toEqual(['rate', 'sink']);
        expect([...malformed.streamingEdgeIds]).toEqual(['e1']);
    });

    it('propagates streaming mode through diamonds and long chains without duplicate visits', () => {
        const nodes: DagNode[] = [
            { id: 'kafka', data: { type: 'SOURCE', spark_transform: 'KafkaSource' } },
            { id: 'a', data: { type: 'STEP', spark_transform: 'Filter' } },
            { id: 'b', data: { type: 'STEP', spark_transform: 'Select' } },
            { id: 'union', data: { type: 'STEP', spark_transform: 'UnionByName' } },
            { id: 'agg', data: { type: 'STEP', spark_transform: 'GroupByAgg' } },
            { id: 'sink', data: { type: 'SINK', spark_transform: 'DeltaSink' } },
        ];
        const edges: DagEdge[] = [
            { id: 'e1', source: 'kafka', target: 'a' },
            { id: 'e2', source: 'kafka', target: 'b' },
            { id: 'e3', source: 'a', target: 'union' },
            { id: 'e4', source: 'b', target: 'union' },
            { id: 'e5', source: 'union', target: 'agg' },
            { id: 'e6', source: 'agg', target: 'sink' },
        ];
        const res = computeStreamingNodeSet(nodes, edges);
        expect([...res.streamingNodeIds].sort()).toEqual(['a', 'agg', 'b', 'kafka', 'sink', 'union']);
        expect([...res.streamingEdgeIds].sort()).toEqual(['e1', 'e2', 'e3', 'e4', 'e5', 'e6']);
        expect(res.joinModes.size).toBe(0);
    });

    it('never propagates streaming mode into SOURCE nodes or across edges without an id', () => {
        const nodes: DagNode[] = [
            { id: 'rate', data: { type: 'SOURCE', spark_transform: 'RateSource' } },
            { id: 'bq', data: { type: 'SOURCE', spark_transform: 'BigQuerySource' } },
            { id: 'step', data: { type: 'STEP', spark_transform: 'Filter' } },
        ];
        const edges: DagEdge[] = [
            { id: 'bad', source: 'rate', target: 'bq' },
            { source: 'rate', target: 'step' } as DagEdge,
            { id: '', source: 'step', target: 'step' },
        ];
        const res = computeStreamingNodeSet(nodes, edges);
        expect(res.streamingNodeIds.has('bq')).toBe(false);
        expect(res.streamingEdgeIds.has('bad')).toBe(false);
        expect(res.streamingNodeIds.has('step')).toBe(true);
        // Edges without an id and edges with an empty id do not appear in the edge set.
        expect(res.streamingEdgeIds.size).toBe(0);
    });

    it('treats a streaming-default source overridden to batch as a static branch', () => {
        const nodes: DagNode[] = [
            {
                id: 'kafka_batch',
                data: { type: 'SOURCE', spark_transform: 'KafkaSource', properties: { readMode: 'batch' } },
            },
            { id: 'pubsub', data: { type: 'SOURCE', spark_transform: 'PubSubSource' } },
            { id: 'join', data: { type: 'STEP', spark_transform: 'Join' } },
        ];
        const edges: DagEdge[] = [
            { id: 'e1', source: 'pubsub', target: 'join', targetHandle: 'left' },
            { id: 'e2', source: 'kafka_batch', target: 'join', targetHandle: 'right' },
        ];
        const res = computeStreamingNodeSet(nodes, edges);
        expect(res.streamingNodeIds.has('kafka_batch')).toBe(false);
        expect(res.joinModes.get('join')).toBe('stream_static');
        expect(res.streamingEdgeIds.has('e2')).toBe(false);
    });

    it('classifies chained Joins independently as stream-static and stream-stream', () => {
        const nodes: DagNode[] = [
            { id: 'clicks', data: { type: 'SOURCE', spark_transform: 'KafkaSource' } },
            { id: 'dim', data: { type: 'SOURCE', spark_transform: 'ParquetSource' } },
            { id: 'impressions', data: { type: 'SOURCE', spark_transform: 'KinesisSource' } },
            { id: 'enrich', data: { type: 'STEP', spark_transform: 'Join' } },
            { id: 'attribute', data: { type: 'STEP', spark_transform: 'Join' } },
            { id: 'static_join', data: { type: 'STEP', spark_transform: 'Join' } },
        ];
        const edges: DagEdge[] = [
            { id: 'e1', source: 'clicks', target: 'enrich', targetHandle: 'left' },
            { id: 'e2', source: 'dim', target: 'enrich', targetHandle: 'right' },
            { id: 'e3', source: 'enrich', target: 'attribute', targetHandle: 'left' },
            { id: 'e4', source: 'impressions', target: 'attribute', targetHandle: 'right' },
            { id: 'e5', source: 'dim', target: 'static_join', targetHandle: 'left' },
            { id: 'e6', source: 'dim', target: 'static_join', targetHandle: 'right' },
        ];
        const res = computeStreamingNodeSet(nodes, edges);
        expect(res.joinModes.get('enrich')).toBe('stream_static');
        expect(res.joinModes.get('attribute')).toBe('stream_stream');
        expect(res.joinModes.get('static_join')).toBe('batch_batch');
        expect(res.streamingNodeIds.has('static_join')).toBe(false);
    });

    it('classifies a streaming self-join as stream-stream and an unconnected Join as batch-batch', () => {
        const nodes: DagNode[] = [
            { id: 'rate', data: { type: 'SOURCE', spark_transform: 'RateSource' } },
            { id: 'self_join', data: { type: 'STEP', spark_transform: 'Join' } },
            { id: 'lonely_join', data: { type: 'STEP', spark_transform: 'Join' } },
        ];
        const edges: DagEdge[] = [
            { id: 'l', source: 'rate', target: 'self_join', targetHandle: 'left' },
            { id: 'r', source: 'rate', target: 'self_join', targetHandle: 'right' },
        ];
        const res = computeStreamingNodeSet(nodes, edges);
        expect(res.joinModes.get('self_join')).toBe('stream_stream');
        expect(res.joinModes.get('lonely_join')).toBe('batch_batch');
    });

    it('detects Joins from top-level node fields and ignores non-Join lookalikes', () => {
        expect(isJoinNode({ id: 'j1', type: 'STEP', spark_transform: 'Join' } as DagNode)).toBe(true);
        expect(isJoinNode({ id: 'j2', data: { sparkTransform: 'Join' } })).toBe(true);
        expect(isJoinNode({ id: 'j3', data: { type: 'STEP', spark_transform: 'join' } })).toBe(false);
        expect(isJoinNode({ id: 'j4', data: { type: 'STEP', spark_transform: 'BroadcastJoin' } })).toBe(false);
        expect(isJoinNode({ id: 'j5', data: { type: 'SINK', spark_transform: 'Join' } })).toBe(false);
        expect(isJoinNode({ id: 'j6', data: { type: 'STEP' } })).toBe(false);
    });

    it('resolves Join handle arity to two and rejects invalid explicit arity overrides', () => {
        expect(getNodeInputArity({ id: 'j', type: 'STEP', spark_transform: 'Join' } as DagNode)).toBe(2);
        expect(getNodeInputArity({ id: 'j', data: { spark_transform: 'Join' } })).toBe(2);
        expect(getNodeInputArity({ id: 'j', data: { type: 'STEP', spark_transform: 'Join', inputArity: 'n' } })).toBe('n');
        expect(getNodeInputArity({ id: 'j', data: { type: 'STEP', spark_transform: 'Join', inputArity: 3 } })).toBe(2);
        expect(getNodeInputArity({ id: 'j', data: { type: 'STEP', spark_transform: 'Join', inputArity: '2' } })).toBe(2);
        expect(getNodeInputArity({ id: 'j', data: { type: 'STEP', spark_transform: 'Join', inputArity: 0 } })).toBe(2);
        expect(getNodeInputArity({ id: 'u', data: { type: 'STEP', spark_transform: 'UnknownStep' } })).toBe(1);
        expect(getNodeInputArity({ id: 'k', data: { type: 'SINK', spark_transform: 'KafkaSink' } })).toBe(1);
        expect(getNodeInputArity({ id: 'f', data: { type: 'STEP', spark_transform: 'Filter', label: 'Flatten' } })).toBe(1);
    });

    it('prefers data.type over node.type and ignores non-string category or transform values', () => {
        expect(getNodeCategory({ id: 'n', type: 'SINK', data: { type: ' step ' } })).toBe('STEP');
        expect(getNodeCategory({ id: 'n', type: 'source', data: { type: 'custom' } })).toBe('SOURCE');
        expect(getNodeCategory({ id: 'n', data: { type: 7 as unknown as string } })).toBeUndefined();
        expect(getNodeCategory(null)).toBeUndefined();
        expect(getNodeCategory(undefined)).toBeUndefined();

        expect(getNodeSparkTransform({ id: 'n', spark_transform: 'Join' } as DagNode)).toBe('Join');
        expect(getNodeSparkTransform({ id: 'n', sparkTransform: 'Filter' } as DagNode)).toBe('Filter');
        expect(getNodeSparkTransform({ id: 'n', data: { spark_transform: 'Join', sparkTransform: 'Filter' } })).toBe('Join');
        expect(getNodeSparkTransform({ id: 'n', data: { spark_transform: 5 as unknown as string } })).toBeUndefined();
        expect(getNodeSparkTransform(null)).toBeUndefined();
    });

    it('falls back to batch read mode for unknown sources and invalid defaults', () => {
        expect(getEffectiveSourceReadMode({ id: 's', data: { type: 'SOURCE', spark_transform: 'MySource' } })).toBe('batch');
        expect(
            getEffectiveSourceReadMode({ id: 's', data: { type: 'SOURCE', defaultReadMode: 'micro_batch' } })
        ).toBe('batch');
        expect(
            getEffectiveSourceReadMode({
                id: 's',
                data: { type: 'SOURCE', spark_transform: 'ParquetSource', defaultReadMode: 'streaming' },
            })
        ).toBe('streaming');
        expect(
            getEffectiveSourceReadMode({
                id: 's',
                data: {
                    type: 'SOURCE',
                    spark_transform: 'RateSource',
                    defaultReadMode: 'streaming',
                    properties: { readMode: 'batch' },
                },
            })
        ).toBe('batch');
    });

    it('lets an explicit errorOutput false disable the DLQ handle on error-capable steps', () => {
        expect(isErrorCapableStep({ id: 'p', data: { type: 'STEP', spark_transform: 'ParsePayload', errorOutput: false } })).toBe(false);
        expect(isErrorCapableStep({ id: 'p', data: { spark_transform: 'ParsePayload' } })).toBe(true);
        expect(isErrorCapableStep({ id: 'u', data: { type: 'STEP', spark_transform: 'Unknown' } })).toBe(false);
    });

    it('tolerates non-object rules and non-string conditions in ConditionalSplit handles', () => {
        expect(
            getConditionalSplitHandles({
                id: 'cs',
                data: {
                    type: 'STEP',
                    spark_transform: 'ConditionalSplit',
                    properties: { rules: [null, 'raw', { name: '  hot  ', condition: 5 }] },
                },
            })
        ).toEqual([
            { id: 'rule_1', label: 'rule_1' },
            { id: 'rule_2', label: 'rule_2' },
            { id: 'hot', label: 'hot' },
            { id: 'else', label: 'else' },
        ]);

        expect(
            getConditionalSplitHandles({
                id: 'cs',
                data: { type: 'STEP', spark_transform: 'ConditionalSplit', properties: { rules: 'vip' } },
            })
        ).toEqual([
            { id: 'rule_1', label: 'rule_1', condition: '' },
            { id: 'else', label: 'else' },
        ]);
    });
});
