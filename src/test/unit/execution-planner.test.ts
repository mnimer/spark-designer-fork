/**
 * @fileoverview Unit tests for the deterministic Apache Spark Execution Planner.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Verifies topological execution order, batch versus streaming propagation, Join mode
 * classification, DLQ split mechanisms, ConditionalSplit branch routing, streaming sink
 * trigger and checkpoint resolution, and runtime classpath artifact resolution.
 */

import * as fs from 'fs';
import * as path from 'path';
import { describe, expect, it } from 'vitest';
import type { DagEdge, DagNode } from '../../shared/dagValidator';
import {
    buildExecutionPlan,
    resolveStreamingSinkSettings,
} from '../../shared/executionPlanner';

/**
 * Creates a test DagNode fixture with canonical React Flow data fields.
 *
 * @param id Unique node identifier.
 * @param nodeType High-level node category ('SOURCE' | 'STEP' | 'SINK').
 * @param sparkTransform Canonical Spark transform identifier.
 * @param properties Optional node property map.
 * @returns Test DagNode object.
 */
function makeNode(
    id: string,
    nodeType: 'SOURCE' | 'STEP' | 'SINK',
    sparkTransform: string,
    properties: Record<string, unknown> = {}
): DagNode {
    return {
        id,
        type: 'pipelineNode',
        data: {
            label: `${sparkTransform}_${id}`,
            type: nodeType,
            spark_transform: sparkTransform,
            properties,
        },
    };
}

/**
 * Creates a test DagEdge fixture connecting two nodes.
 *
 * @param source Source node identifier.
 * @param target Target node identifier.
 * @param sourceHandle Optional source handle identifier.
 * @param targetHandle Optional target handle identifier.
 * @returns Test DagEdge object.
 */
function makeEdge(
    source: string,
    target: string,
    sourceHandle?: string,
    targetHandle?: string
): DagEdge {
    return {
        id: `${source}_${sourceHandle ?? 'out'}->${target}_${targetHandle ?? 'in'}`,
        source,
        target,
        ...(sourceHandle !== undefined ? { sourceHandle } : {}),
        ...(targetHandle !== undefined ? { targetHandle } : {}),
    };
}

describe('executionPlanner core (A10)', () => {
    describe('webview shim parity', () => {
        it('re-exports the canonical shared executionPlanner module for webview consumers', () => {
            const shimPath = path.resolve(
                __dirname,
                '../../../webview/src/utils/executionPlanner.ts'
            );
            expect(fs.existsSync(shimPath)).toBe(true);
            const content = fs.readFileSync(shimPath, 'utf-8');
            expect(content).toContain("export * from '../../../src/shared/executionPlanner';");
        });
    });

    describe('empty, degenerate, and cyclic graphs', () => {
        it('returns a clean batch plan for an empty graph', () => {
            const plan = buildExecutionPlan([], []);
            expect(plan.sortedNodeIds).toEqual([]);
            expect(plan.pipelineMode).toBe('batch');
            expect(plan.nodes).toEqual({});
            expect(plan.streamingNodeIds).toEqual([]);
            expect(plan.dlqSplitPoints).toEqual([]);
            expect(plan.conditionalSplits).toEqual([]);
            expect(plan.runtimeArtifacts.profile.id).toBe('local');
            expect(plan.runtimeArtifacts.tier).toBe('standard');
        });

        it('ignores dangling edges and duplicate node identifiers safely', () => {
            const nodes = [
                makeNode('src', 'SOURCE', 'ParquetSource'),
                makeNode('src', 'SOURCE', 'ParquetSource'),
                makeNode('sink', 'SINK', 'BigQuerySink'),
            ];
            const edges = [
                makeEdge('src', 'sink'),
                makeEdge('missing_src', 'sink'),
                makeEdge('src', 'missing_tgt'),
            ];

            const plan = buildExecutionPlan(nodes, edges);
            expect(plan.sortedNodeIds).toEqual(['src', 'sink']);
            expect(Object.keys(plan.nodes)).toEqual(['src', 'sink']);
        });

        it('populates per-node metadata without throwing when the graph contains a cycle', () => {
            const nodes = [
                makeNode('k1', 'SOURCE', 'KafkaSource'),
                makeNode('s1', 'STEP', 'Select'),
                makeNode('s2', 'STEP', 'WithColumns'),
            ];
            const edges = [
                makeEdge('k1', 's1'),
                makeEdge('s1', 's2'),
                makeEdge('s2', 's1'),
            ];

            const plan = buildExecutionPlan(nodes, edges);
            expect(plan.sortedNodeIds).toEqual([]);
            expect(plan.pipelineMode).toBe('streaming');
            expect(plan.nodes.k1.executionMode).toBe('streaming');
            expect(plan.nodes.s1.executionMode).toBe('streaming');
            expect(plan.nodes.s2.executionMode).toBe('streaming');
        });
    });

    describe('topological order and stage alignment', () => {
        it('orders sources and steps topologically and places terminal sinks last', () => {
            const nodes = [
                makeNode('src', 'SOURCE', 'BigQuerySource'),
                makeNode('step_a', 'STEP', 'Filter'),
                makeNode('sink_fast', 'SINK', 'ParquetSink'),
                makeNode('step_b', 'STEP', 'WithColumns'),
                makeNode('sink_deep', 'SINK', 'IcebergSink'),
            ];
            const edges = [
                makeEdge('src', 'step_a'),
                makeEdge('step_a', 'sink_fast'),
                makeEdge('step_a', 'step_b'),
                makeEdge('step_b', 'sink_deep'),
            ];

            const plan = buildExecutionPlan(nodes, edges);
            expect(plan.sortedNodeIds).toEqual([
                'src',
                'step_a',
                'step_b',
                'sink_fast',
                'sink_deep',
            ]);
            expect(plan.pipelineMode).toBe('batch');
        });

        it('supports direct zero-transform SOURCE to SINK pipelines', () => {
            const nodes = [
                makeNode('csv', 'SOURCE', 'CsvSource'),
                makeNode('parquet', 'SINK', 'ParquetSink'),
            ];
            const edges = [makeEdge('csv', 'parquet')];

            const plan = buildExecutionPlan(nodes, edges);
            expect(plan.sortedNodeIds).toEqual(['csv', 'parquet']);
            expect(plan.nodes.csv.executionMode).toBe('batch');
            expect(plan.nodes.parquet.executionMode).toBe('batch');
        });
    });

    describe('batch versus streaming propagation and Join modes', () => {
        it('marks downstream steps and sinks streaming while keeping static Join inputs batch', () => {
            const nodes = [
                makeNode('pubsub', 'SOURCE', 'PubSubSource'),
                makeNode('bq_dim', 'SOURCE', 'BigQuerySource'),
                makeNode('dim_prep', 'STEP', 'Select'),
                makeNode('join', 'STEP', 'Join', { broadcastRight: true }),
                makeNode('bq_out', 'SINK', 'BigQuerySink'),
            ];
            const edges = [
                makeEdge('pubsub', 'join', 'valid', 'left'),
                makeEdge('bq_dim', 'dim_prep'),
                makeEdge('dim_prep', 'join', 'valid', 'right'),
                makeEdge('join', 'bq_out'),
            ];

            const plan = buildExecutionPlan(nodes, edges);
            expect(plan.pipelineMode).toBe('streaming');
            expect(plan.streamingNodeIds).toEqual(['pubsub', 'join', 'bq_out']);
            expect(plan.nodes.pubsub.executionMode).toBe('streaming');
            expect(plan.nodes.bq_dim.executionMode).toBe('batch');
            expect(plan.nodes.dim_prep.executionMode).toBe('batch');
            expect(plan.nodes.join.executionMode).toBe('streaming');
            expect(plan.nodes.join.joinMode).toBe('stream_static');
            expect(plan.nodes.bq_out.executionMode).toBe('streaming');
        });

        it('honors readMode overrides on dual-mode sources and classifies batch_batch and stream_stream Joins', () => {
            const batchJoinNodes = [
                makeNode('k_batch', 'SOURCE', 'KafkaSource', { readMode: 'batch' }),
                makeNode('ice_batch', 'SOURCE', 'IcebergSource'),
                makeNode('j_batch', 'STEP', 'Join'),
                makeNode('out', 'SINK', 'ParquetSink'),
            ];
            const batchJoinEdges = [
                makeEdge('k_batch', 'j_batch', undefined, 'left'),
                makeEdge('ice_batch', 'j_batch', undefined, 'right'),
                makeEdge('j_batch', 'out'),
            ];

            const batchPlan = buildExecutionPlan(batchJoinNodes, batchJoinEdges);
            expect(batchPlan.pipelineMode).toBe('batch');
            expect(batchPlan.nodes.j_batch.joinMode).toBe('batch_batch');

            const streamJoinNodes = [
                makeNode('k_stream', 'SOURCE', 'KafkaSource'),
                makeNode('ice_stream', 'SOURCE', 'IcebergSource', { readMode: 'streaming' }),
                makeNode('j_stream', 'STEP', 'Join'),
                makeNode('out_s', 'SINK', 'DeltaSink'),
            ];
            const streamJoinEdges = [
                makeEdge('k_stream', 'j_stream', undefined, 'left'),
                makeEdge('ice_stream', 'j_stream', undefined, 'right'),
                makeEdge('j_stream', 'out_s'),
            ];

            const streamPlan = buildExecutionPlan(streamJoinNodes, streamJoinEdges);
            expect(streamPlan.pipelineMode).toBe('streaming');
            expect(streamPlan.nodes.j_stream.joinMode).toBe('stream_stream');
        });
    });

    describe('DLQ split point detection', () => {
        it('maps ParsePayload, Filter, DataQualityAssert, and UDF steps to exact DLQ mechanisms', () => {
            const nodes = [
                makeNode('src', 'SOURCE', 'ParquetSource'),
                makeNode('parse', 'STEP', 'ParsePayload'),
                makeNode('flt', 'STEP', 'Filter'),
                makeNode('dq', 'STEP', 'DataQualityAssert'),
                makeNode('udf', 'STEP', 'PandasUdfTransform'),
                makeNode('no_dlq_flt', 'STEP', 'Filter'),
                makeNode('good_sink', 'SINK', 'IcebergSink'),
                makeNode('dlq_sink', 'SINK', 'JsonSink'),
            ];
            const edges = [
                makeEdge('src', 'parse'),
                makeEdge('parse', 'flt', 'valid'),
                makeEdge('parse', 'dlq_sink', 'error'),
                makeEdge('flt', 'dq', 'valid'),
                makeEdge('flt', 'dlq_sink', 'error'),
                makeEdge('dq', 'udf', 'valid'),
                makeEdge('dq', 'dlq_sink', 'error'),
                makeEdge('udf', 'no_dlq_flt', 'valid'),
                makeEdge('udf', 'dlq_sink', 'error'),
                makeEdge('no_dlq_flt', 'good_sink', 'valid'),
            ];

            const plan = buildExecutionPlan(nodes, edges);
            expect(plan.dlqSplitPoints).toEqual([
                {
                    nodeId: 'parse',
                    sparkTransform: 'ParsePayload',
                    mechanism: '_corrupt_record',
                    validTargetIds: ['flt'],
                    errorTargetIds: ['dlq_sink'],
                },
                {
                    nodeId: 'flt',
                    sparkTransform: 'Filter',
                    mechanism: 'negated_predicate',
                    validTargetIds: ['dq'],
                    errorTargetIds: ['dlq_sink'],
                },
                {
                    nodeId: 'dq',
                    sparkTransform: 'DataQualityAssert',
                    mechanism: 'expectation_violation',
                    validTargetIds: ['udf'],
                    errorTargetIds: ['dlq_sink'],
                },
                {
                    nodeId: 'udf',
                    sparkTransform: 'PandasUdfTransform',
                    mechanism: 'try_except_error_col',
                    validTargetIds: ['no_dlq_flt'],
                    errorTargetIds: ['dlq_sink'],
                },
            ]);
        });
    });

    describe('ConditionalSplit rule branch resolution', () => {
        it('resolves named rule handles and the else fallback handle in rule order', () => {
            const nodes = [
                makeNode('src', 'SOURCE', 'BigQuerySource'),
                makeNode('split', 'STEP', 'ConditionalSplit', {
                    rules: [
                        { name: 'vip', condition: 'amount >= 1000' },
                        { name: 'standard', condition: 'amount >= 100' },
                    ],
                }),
                makeNode('vip_sink', 'SINK', 'BigQuerySink'),
                makeNode('std_sink', 'SINK', 'ParquetSink'),
                makeNode('else_sink', 'SINK', 'JsonSink'),
            ];
            const edges = [
                makeEdge('src', 'split'),
                makeEdge('split', 'vip_sink', 'vip'),
                makeEdge('split', 'std_sink', 'standard'),
                makeEdge('split', 'else_sink', 'else'),
            ];

            const plan = buildExecutionPlan(nodes, edges);
            expect(plan.conditionalSplits).toHaveLength(1);
            expect(plan.conditionalSplits[0]).toEqual({
                nodeId: 'split',
                branches: [
                    {
                        handleId: 'vip',
                        label: 'vip',
                        condition: 'amount >= 1000',
                        isFallback: false,
                        targetNodeIds: ['vip_sink'],
                    },
                    {
                        handleId: 'standard',
                        label: 'standard',
                        condition: 'amount >= 100',
                        isFallback: false,
                        targetNodeIds: ['std_sink'],
                    },
                    {
                        handleId: 'else',
                        label: 'else',
                        isFallback: true,
                        targetNodeIds: ['else_sink'],
                    },
                ],
            });
            expect(plan.nodes.split.conditionalBranches).toEqual(
                plan.conditionalSplits[0].branches
            );
        });
    });

    describe('effective streaming sink settings resolution', () => {
        it('inherits jobConfig defaults and applies per-sink trigger, outputMode, and checkpoint overrides', () => {
            const nodes = [
                makeNode('rate', 'SOURCE', 'RateSource'),
                makeNode('s_inherit', 'SINK', 'IcebergSink', {
                    triggerType: 'inherit',
                }),
                makeNode('s_override', 'SINK', 'DeltaSink', {
                    outputMode: 'complete',
                    triggerType: 'availableNow',
                    checkpointLocation: 'gs://custom-bucket/ckpt/delta_override',
                }),
                makeNode('s_continuous', 'SINK', 'KafkaSink', {
                    outputMode: 'update',
                    triggerType: 'continuous',
                    triggerInterval: '1 second',
                }),
            ];
            const edges = [
                makeEdge('rate', 's_inherit'),
                makeEdge('rate', 's_override'),
                makeEdge('rate', 's_continuous'),
            ];

            const plan = buildExecutionPlan(nodes, edges, {
                checkpointRoot: 'gs://pipeline-bucket/checkpoints/',
                defaultTriggerType: 'processingTime',
                defaultTriggerInterval: '30 seconds',
            });

            expect(plan.nodes.s_inherit.streamingSinkSettings).toEqual({
                outputMode: 'append',
                triggerType: 'processingTime',
                triggerInterval: '30 seconds',
                checkpointLocation: 'gs://pipeline-bucket/checkpoints/s_inherit',
            });

            expect(plan.nodes.s_override.streamingSinkSettings).toEqual({
                outputMode: 'complete',
                triggerType: 'availableNow',
                checkpointLocation: 'gs://custom-bucket/ckpt/delta_override',
            });

            expect(plan.nodes.s_continuous.streamingSinkSettings).toEqual({
                outputMode: 'update',
                triggerType: 'continuous',
                triggerInterval: '1 second',
                checkpointLocation: 'gs://pipeline-bucket/checkpoints/s_continuous',
            });
        });

        it('leaves checkpointLocation empty when neither checkpointRoot nor sink override is set', () => {
            const sink = makeNode('sink_1', 'SINK', 'ConsoleSink');
            const settings = resolveStreamingSinkSettings(sink);
            expect(settings).toEqual({
                outputMode: 'append',
                triggerType: 'processingTime',
                triggerInterval: '10 seconds',
                checkpointLocation: '',
            });
        });
    });

    describe('runtime classpath and artifact resolution via versionMatrix', () => {
        it('resolves Serverless 4.0 standard tier, GCS JARs, Python DataSources, and Managed Spark exclusions', () => {
            const nodes = [
                makeNode('ps', 'SOURCE', 'PubSubSource'),
                makeNode('alloy', 'SOURCE', 'AlloyDbSource', {
                    driverJarUri: 'gs://my-jars/postgresql.jar',
                }),
                makeNode('join', 'STEP', 'Join'),
                makeNode('bq', 'SINK', 'BigQuerySink'),
                makeNode('spanner', 'SINK', 'SpannerSink'),
            ];
            const edges = [
                makeEdge('ps', 'join', undefined, 'left'),
                makeEdge('alloy', 'join', undefined, 'right'),
                makeEdge('join', 'bq'),
                makeEdge('join', 'spanner'),
            ];

            const plan = buildExecutionPlan(nodes, edges, {
                runner: 'ServerlessSpark',
                runtimeVersion: '4.0',
            });

            expect(plan.runtimeArtifacts.profile.id).toBe('serverless_4.0');
            expect(plan.runtimeArtifacts.profile.sparkVersion).toBe('4.2.0');
            expect(plan.runtimeArtifacts.tier).toBe('standard');
            expect(plan.runtimeArtifacts.gcsJars).toContain(
                'gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar'
            );
            expect(plan.runtimeArtifacts.gcsJars).toContain('gs://my-jars/postgresql.jar');
            expect(plan.runtimeArtifacts.pipPackages).toContain('google-cloud-pubsub');
            expect(plan.runtimeArtifacts.pythonDataSources.length).toBeGreaterThan(0);
            expect(plan.runtimeArtifacts.sparkProperties['dataproc.artifacts.remove']).toBe(
                'conscrypt'
            );
            // BigQuery is preinstalled on Serverless Spark and must not emit Maven coordinates.
            expect(
                plan.runtimeArtifacts.mavenPackages.some((coord) => coord.includes('bigquery'))
            ).toBe(false);
        });
    });
});

describe('batch fan-out auto-cache and unpersist planning (A11)', () => {
    it('auto-caches batch nodes with out-degree > 1 and unpersists after the final downstream sink', () => {
        const nodes = [
            makeNode('bq_src', 'SOURCE', 'BigQuerySource'),
            makeNode('enrich', 'STEP', 'WithColumns'),
            makeNode('sink_iceberg', 'SINK', 'IcebergSink'),
            makeNode('post_flt', 'STEP', 'Filter'),
            makeNode('sink_spanner', 'SINK', 'SpannerSink'),
        ];
        const edges = [
            makeEdge('bq_src', 'enrich'),
            makeEdge('enrich', 'sink_iceberg'),
            makeEdge('enrich', 'post_flt'),
            makeEdge('post_flt', 'sink_spanner'),
        ];

        const plan = buildExecutionPlan(nodes, edges);
        expect(plan.sortedNodeIds).toEqual([
            'bq_src',
            'enrich',
            'post_flt',
            'sink_iceberg',
            'sink_spanner',
        ]);
        expect(plan.cachedNodeIds).toEqual(['enrich']);
        expect(plan.nodes.enrich.isCached).toBe(true);
        expect(plan.nodes.bq_src.isCached).toBe(false);
        expect(plan.nodes.post_flt.isCached).toBe(false);
        expect(plan.cacheDirectives).toEqual([
            {
                nodeId: 'enrich',
                reason: 'batch_fanout',
                outDegree: 2,
                unpersistAfterNodeIds: ['sink_spanner'],
            },
        ]);
        expect(plan.unpersistPoints).toEqual({
            sink_spanner: ['enrich'],
        });
    });

    it('marks batch DLQ splits with dlq_split reason when both valid and error handles branch', () => {
        const nodes = [
            makeNode('csv', 'SOURCE', 'CsvSource'),
            makeNode('parse', 'STEP', 'ParsePayload'),
            makeNode('main_sink', 'SINK', 'ParquetSink'),
            makeNode('dlq_sink', 'SINK', 'JsonSink'),
        ];
        const edges = [
            makeEdge('csv', 'parse'),
            makeEdge('parse', 'main_sink', 'valid'),
            makeEdge('parse', 'dlq_sink', 'error'),
        ];

        const plan = buildExecutionPlan(nodes, edges);
        expect(plan.cachedNodeIds).toEqual(['parse']);
        expect(plan.nodes.parse.isCached).toBe(true);
        expect(plan.cacheDirectives).toEqual([
            {
                nodeId: 'parse',
                reason: 'dlq_split',
                outDegree: 2,
                unpersistAfterNodeIds: ['dlq_sink'],
            },
        ]);
        expect(plan.unpersistPoints).toEqual({
            dlq_sink: ['parse'],
        });
    });

    it('auto-caches ConditionalSplit only when two or more branches are connected', () => {
        const multiNodes = [
            makeNode('src', 'SOURCE', 'ParquetSource'),
            makeNode('router', 'STEP', 'ConditionalSplit', {
                rules: [{ name: 'high', condition: 'score > 80' }],
            }),
            makeNode('s_high', 'SINK', 'BigQuerySink'),
            makeNode('s_else', 'SINK', 'ParquetSink'),
        ];
        const multiEdges = [
            makeEdge('src', 'router'),
            makeEdge('router', 's_high', 'high'),
            makeEdge('router', 's_else', 'else'),
        ];

        const multiPlan = buildExecutionPlan(multiNodes, multiEdges);
        expect(multiPlan.cachedNodeIds).toEqual(['router']);
        expect(multiPlan.cacheDirectives[0].reason).toBe('batch_fanout');
        expect(multiPlan.unpersistPoints).toEqual({ s_else: ['router'] });

        const singleEdges = [
            makeEdge('src', 'router'),
            makeEdge('router', 's_high', 'high'),
        ];
        const singlePlan = buildExecutionPlan(multiNodes.slice(0, 3), singleEdges);
        expect(singlePlan.cachedNodeIds).toEqual([]);
        expect(singlePlan.nodes.router.isCached).toBe(false);
    });

    it('delays unpersist in a diamond Join graph until the terminal downstream sink completes', () => {
        const nodes = [
            makeNode('src', 'SOURCE', 'IcebergSource'),
            makeNode('prep', 'STEP', 'WithColumns'),
            makeNode('left_agg', 'STEP', 'GroupByAgg'),
            makeNode('right_flt', 'STEP', 'Filter'),
            makeNode('rejoin', 'STEP', 'Join'),
            makeNode('final_sink', 'SINK', 'BigQuerySink'),
        ];
        const edges = [
            makeEdge('src', 'prep'),
            makeEdge('prep', 'left_agg'),
            makeEdge('prep', 'right_flt'),
            makeEdge('left_agg', 'rejoin', undefined, 'left'),
            makeEdge('right_flt', 'rejoin', undefined, 'right'),
            makeEdge('rejoin', 'final_sink'),
        ];

        const plan = buildExecutionPlan(nodes, edges);
        expect(plan.cachedNodeIds).toEqual(['prep']);
        expect(plan.cacheDirectives[0].unpersistAfterNodeIds).toEqual(['final_sink']);
        expect(plan.unpersistPoints).toEqual({
            final_sink: ['prep'],
        });
    });

    it('honors manual cacheOutput overrides (true, always, false, never) and CachePersist steps', () => {
        const nodes = [
            makeNode('src_forced', 'SOURCE', 'BigQuerySource', { cacheOutput: 'always' }),
            makeNode('step_suppressed', 'STEP', 'WithColumns', { cacheOutput: false }),
            makeNode('step_explicit_cache', 'STEP', 'CachePersist'),
            makeNode('step_bool_forced', 'STEP', 'Select', { cacheOutput: true }),
            makeNode('step_str_forced', 'STEP', 'Select', { cacheOutput: 'true' }),
            makeNode('step_never_str', 'STEP', 'Filter', { cacheOutput: 'never' }),
            makeNode('sink_a', 'SINK', 'ParquetSink'),
            makeNode('sink_b', 'SINK', 'IcebergSink'),
            makeNode('sink_c', 'SINK', 'JsonSink'),
        ];
        const edges = [
            makeEdge('src_forced', 'step_suppressed'),
            makeEdge('step_suppressed', 'step_explicit_cache'),
            makeEdge('step_suppressed', 'step_str_forced'),
            makeEdge('step_explicit_cache', 'step_bool_forced'),
            makeEdge('step_bool_forced', 'sink_a'),
            makeEdge('step_str_forced', 'step_never_str'),
            makeEdge('step_never_str', 'sink_b'),
            makeEdge('step_never_str', 'sink_c'),
        ];

        const plan = buildExecutionPlan(nodes, edges);
        expect(plan.sortedNodeIds).toEqual([
            'src_forced',
            'step_suppressed',
            'step_explicit_cache',
            'step_str_forced',
            'step_bool_forced',
            'step_never_str',
            'sink_a',
            'sink_b',
            'sink_c',
        ]);
        expect(plan.cachedNodeIds).toEqual([
            'src_forced',
            'step_explicit_cache',
            'step_str_forced',
            'step_bool_forced',
        ]);
        expect(plan.nodes.step_suppressed.isCached).toBe(false);
        expect(plan.nodes.step_never_str.isCached).toBe(false);

        const reasonsById = Object.fromEntries(
            plan.cacheDirectives.map((d) => [d.nodeId, d.reason])
        );
        expect(reasonsById).toEqual({
            src_forced: 'manual_override',
            step_explicit_cache: 'cache_persist_step',
            step_str_forced: 'manual_override',
            step_bool_forced: 'manual_override',
        });

        expect(plan.unpersistPoints).toEqual({
            sink_c: ['src_forced', 'step_str_forced'],
            sink_a: ['step_explicit_cache', 'step_bool_forced'],
        });
    });

    it('does not emit batch cache directives for streaming branches even on fan-out', () => {
        const nodes = [
            makeNode('kafka', 'SOURCE', 'KafkaSource'),
            makeNode('parse', 'STEP', 'ParsePayload'),
            makeNode('s1', 'SINK', 'DeltaSink'),
            makeNode('s2', 'SINK', 'ConsoleSink'),
        ];
        const edges = [
            makeEdge('kafka', 'parse'),
            makeEdge('parse', 's1', 'valid'),
            makeEdge('parse', 's2', 'error'),
        ];

        const plan = buildExecutionPlan(nodes, edges);
        expect(plan.cachedNodeIds).toEqual([]);
        expect(plan.cacheDirectives).toEqual([]);
        expect(plan.nodes.parse.isCached).toBe(false);
    });
});

describe('streaming foreachBatch multi-sink and batch-sink grouping (A12)', () => {
    it('emits a native StreamingSinkGroup when a streaming subquery feeds one native sink', () => {
        const nodes = [
            makeNode('kafka', 'SOURCE', 'KafkaSource'),
            makeNode('parse', 'STEP', 'ParsePayload'),
            makeNode('iceberg', 'SINK', 'IcebergSink'),
        ];
        const edges = [makeEdge('kafka', 'parse'), makeEdge('parse', 'iceberg')];

        const plan = buildExecutionPlan(nodes, edges, {
            jobName: 'orders-stream',
            checkpointRoot: 'gs://ckpt-bucket/streams',
        });

        expect(plan.foreachBatchSinkIds).toEqual([]);
        expect(plan.nodes.iceberg.isForeachBatchSink).toBe(false);
        expect(plan.streamingSinkGroups).toEqual([
            {
                id: 'native_stream_1',
                sourceSubqueryNodeId: 'parse',
                sinkIds: ['iceberg'],
                mode: 'native',
                requiresBatchCache: false,
                idempotentTxnOptions: false,
                txnAppId: '',
                settings: {
                    outputMode: 'append',
                    triggerType: 'processingTime',
                    triggerInterval: '10 seconds',
                    checkpointLocation: 'gs://ckpt-bucket/streams/iceberg',
                },
                sinkSettingsById: {
                    iceberg: {
                        outputMode: 'append',
                        triggerType: 'processingTime',
                        triggerInterval: '10 seconds',
                        checkpointLocation: 'gs://ckpt-bucket/streams/iceberg',
                    },
                },
            },
        ]);
    });

    it('wraps single batch-only or SDK streaming sinks (Spanner, JDBC, Snowflake, PubSub, ForeachBatch) in foreachBatch without batch_df cache', () => {
        const connectors = [
            'SpannerSink',
            'CloudSqlSink',
            'AlloyDbSink',
            'JdbcSink',
            'SnowflakeSink',
            'PubSubSink',
            'ForeachBatchSink',
        ];

        for (const sinkTransform of connectors) {
            const nodes = [
                makeNode('rate', 'SOURCE', 'RateSource'),
                makeNode('sink', 'SINK', sinkTransform),
            ];
            const edges = [makeEdge('rate', 'sink')];

            const plan = buildExecutionPlan(nodes, edges, {
                jobName: 'billing-ingest',
                checkpointRoot: 'gs://ckpt-bucket/root',
            });

            expect(plan.foreachBatchSinkIds).toEqual(['sink']);
            expect(plan.nodes.sink.isForeachBatchSink).toBe(true);
            expect(plan.streamingSinkGroups).toHaveLength(1);
            expect(plan.streamingSinkGroups[0]).toMatchObject({
                id: 'foreach_batch_1',
                sourceSubqueryNodeId: 'rate',
                sinkIds: ['sink'],
                mode: 'foreachBatch',
                requiresBatchCache: false,
                idempotentTxnOptions: true,
                txnAppId: 'billing-ingest_rate',
            });
        }
    });

    it('groups multi-sink streaming fan-outs into foreachBatch with requiresBatchCache and idempotent txn options', () => {
        const nodes = [
            makeNode('kafka', 'SOURCE', 'KafkaSource'),
            makeNode('parse', 'STEP', 'ParsePayload'),
            makeNode('delta_sink', 'SINK', 'DeltaSink'),
            makeNode('cloudsql_sink', 'SINK', 'CloudSqlSink', {
                triggerType: 'processingTime',
                triggerInterval: '5 seconds',
            }),
        ];
        const edges = [
            makeEdge('kafka', 'parse'),
            makeEdge('parse', 'delta_sink'),
            makeEdge('parse', 'cloudsql_sink'),
        ];

        const plan = buildExecutionPlan(nodes, edges, {
            jobName: 'multi-sink-orders',
            checkpointRoot: 'gs://ckpt-bucket/orders/',
        });

        expect(plan.foreachBatchSinkIds).toEqual(['delta_sink', 'cloudsql_sink']);
        expect(plan.nodes.delta_sink.isForeachBatchSink).toBe(true);
        expect(plan.nodes.cloudsql_sink.isForeachBatchSink).toBe(true);
        expect(plan.streamingSinkGroups).toEqual([
            {
                id: 'foreach_batch_1',
                sourceSubqueryNodeId: 'parse',
                sinkIds: ['delta_sink', 'cloudsql_sink'],
                mode: 'foreachBatch',
                requiresBatchCache: true,
                idempotentTxnOptions: true,
                txnAppId: 'multi-sink-orders_parse',
                settings: {
                    outputMode: 'append',
                    triggerType: 'processingTime',
                    triggerInterval: '5 seconds',
                    checkpointLocation: 'gs://ckpt-bucket/orders/foreach_batch_1',
                },
                sinkSettingsById: {
                    delta_sink: {
                        outputMode: 'append',
                        triggerType: 'processingTime',
                        triggerInterval: '10 seconds',
                        checkpointLocation: 'gs://ckpt-bucket/orders/delta_sink',
                    },
                    cloudsql_sink: {
                        outputMode: 'append',
                        triggerType: 'processingTime',
                        triggerInterval: '5 seconds',
                        checkpointLocation: 'gs://ckpt-bucket/orders/cloudsql_sink',
                    },
                },
            },
        ]);
    });

    it('identifies the lowest common streaming ancestor when stateless branches diverge before two native sinks', () => {
        const nodes = [
            makeNode('kafka', 'SOURCE', 'KafkaSource'),
            makeNode('parse', 'STEP', 'ParsePayload'),
            makeNode('flt_us', 'STEP', 'Filter'),
            makeNode('flt_eu', 'STEP', 'Filter'),
            makeNode('bq_us', 'SINK', 'BigQuerySink'),
            makeNode('bq_eu', 'SINK', 'BigQuerySink'),
        ];
        const edges = [
            makeEdge('kafka', 'parse'),
            makeEdge('parse', 'flt_us'),
            makeEdge('parse', 'flt_eu'),
            makeEdge('flt_us', 'bq_us'),
            makeEdge('flt_eu', 'bq_eu'),
        ];

        const plan = buildExecutionPlan(nodes, edges, {
            checkpointRoot: 'gs://ckpt/regional',
        });

        expect(plan.streamingSinkGroups).toHaveLength(1);
        expect(plan.streamingSinkGroups[0].mode).toBe('foreachBatch');
        expect(plan.streamingSinkGroups[0].sourceSubqueryNodeId).toBe('parse');
        expect(plan.streamingSinkGroups[0].requiresBatchCache).toBe(true);
        expect(plan.foreachBatchSinkIds).toEqual(['bq_us', 'bq_eu']);
    });

    it('keeps raw DLQ streaming sinks separate from downstream stateful windowed aggregations', () => {
        const nodes = [
            makeNode('kafka', 'SOURCE', 'KafkaSource'),
            makeNode('parse', 'STEP', 'ParsePayload'),
            makeNode('dlq_console', 'SINK', 'ConsoleSink'),
            makeNode('wm', 'STEP', 'Watermark', {
                eventTimeColumn: 'event_ts',
                delayThreshold: '10 minutes',
            }),
            makeNode('agg', 'STEP', 'GroupByAgg', { windowType: 'Tumbling' }),
            makeNode('iceberg', 'SINK', 'IcebergSink'),
        ];
        const edges = [
            makeEdge('kafka', 'parse'),
            makeEdge('parse', 'dlq_console', 'error'),
            makeEdge('parse', 'wm', 'valid'),
            makeEdge('wm', 'agg'),
            makeEdge('agg', 'iceberg'),
        ];

        const plan = buildExecutionPlan(nodes, edges, {
            checkpointRoot: 'gs://ckpt/iot',
        });

        expect(plan.foreachBatchSinkIds).toEqual([]);
        expect(plan.streamingSinkGroups).toHaveLength(2);
        expect(plan.streamingSinkGroups[0]).toMatchObject({
            id: 'native_stream_1',
            sourceSubqueryNodeId: 'parse',
            sinkIds: ['dlq_console'],
            mode: 'native',
        });
        expect(plan.streamingSinkGroups[1]).toMatchObject({
            id: 'native_stream_2',
            sourceSubqueryNodeId: 'agg',
            sinkIds: ['iceberg'],
            mode: 'native',
        });
    });

    it('maintains separate StreamingSinkGroups for independent streaming sources', () => {
        const nodes = [
            makeNode('k1', 'SOURCE', 'KafkaSource'),
            makeNode('s_native', 'SINK', 'DeltaSink'),
            makeNode('p2', 'SOURCE', 'PubSubSource'),
            makeNode('s_spanner', 'SINK', 'SpannerSink'),
        ];
        const edges = [makeEdge('k1', 's_native'), makeEdge('p2', 's_spanner')];

        const plan = buildExecutionPlan(nodes, edges);
        expect(plan.streamingSinkGroups).toHaveLength(2);
        expect(plan.streamingSinkGroups[0]).toMatchObject({
            id: 'native_stream_1',
            sourceSubqueryNodeId: 'k1',
            sinkIds: ['s_native'],
            mode: 'native',
            requiresBatchCache: false,
            idempotentTxnOptions: false,
        });
        expect(plan.streamingSinkGroups[1]).toMatchObject({
            id: 'foreach_batch_2',
            sourceSubqueryNodeId: 'p2',
            sinkIds: ['s_spanner'],
            mode: 'foreachBatch',
            requiresBatchCache: false,
            idempotentTxnOptions: true,
        });
        expect(plan.foreachBatchSinkIds).toEqual(['s_spanner']);
    });
});
