/**
 * @fileoverview Unit tests for Extension Host Apache Spark DAG Validator (A13).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies topological cycle and orphan detection, direct Source-to-Sink connections,
 * Spark Join handle arity, stream-static join mode rules, stream-stream outer join
 * watermark requirements, multi-input UnionByName/SetOperation/SparkSqlQuery arity,
 * streaming aggregation watermark warnings, and checkpoint path validation.
 */

import { describe, expect, it } from 'vitest';
import { validateDagTopology, type DagEdge, type DagNode } from '../../shared/dagValidator';

describe('Extension Host validateDagTopology — Core Topology & Direct Source->Sink', () => {
    it('approves a valid linear pipeline graph and direct SOURCE -> SINK pipelines', () => {
        const linearNodes: DagNode[] = [
            { id: 'src', data: { label: 'Parquet Source', type: 'SOURCE', spark_transform: 'ParquetSource' } },
            { id: 'step', data: { label: 'Filter', type: 'STEP', spark_transform: 'Filter' } },
            { id: 'sink', data: { label: 'BigQuery Sink', type: 'SINK', spark_transform: 'BigQuerySink' } },
        ];
        const linearEdges: DagEdge[] = [
            { id: 'e1', source: 'src', target: 'step' },
            { id: 'e2', source: 'step', target: 'sink' },
        ];

        const res = validateDagTopology(linearNodes, linearEdges);
        expect(res.isValid).toBe(true);
        expect(res.hasCycles).toBe(false);
        expect(res.sortedNodeIds).toEqual(['src', 'step', 'sink']);
        expect(res.errors).toHaveLength(0);
        expect(res.warnings).toHaveLength(0);

        // Direct SOURCE -> SINK zero-transform replication
        const directRes = validateDagTopology(
            [linearNodes[0], linearNodes[2]],
            [{ id: 'e_direct', source: 'src', target: 'sink' }]
        );
        expect(directRes.isValid).toBe(true);
        expect(directRes.sortedNodeIds).toEqual(['src', 'sink']);
        expect(directRes.errors).toEqual([]);
        expect(directRes.warnings).toEqual([]);
    });

    it('rejects a cyclic pipeline graph and returns cycle node IDs', () => {
        const nodes: DagNode[] = [
            { id: 'n1', data: { label: 'Step 1', type: 'STEP', spark_transform: 'Select' } },
            { id: 'n2', data: { label: 'Step 2', type: 'STEP', spark_transform: 'WithColumns' } },
        ];
        const edges: DagEdge[] = [
            { id: 'e1', source: 'n1', target: 'n2' },
            { id: 'e2', source: 'n2', target: 'n1' },
        ];

        const res = validateDagTopology(nodes, edges);
        expect(res.isValid).toBe(false);
        expect(res.hasCycles).toBe(true);
        expect(res.cycleNodeIds.sort()).toEqual(['n1', 'n2']);
        expect(res.errors[0]).toContain('Cyclic dependency detected');
    });

    it('identifies disconnected orphan components as fatal topology errors', () => {
        const nodes: DagNode[] = [
            { id: 's1', data: { label: 'Source', type: 'SOURCE', spark_transform: 'CsvSource' } },
            { id: 's2', data: { label: 'Sink', type: 'SINK', spark_transform: 'ParquetSink' } },
            { id: 'orphan', data: { label: 'Unconnected', type: 'STEP', spark_transform: 'Filter' } },
        ];
        const edges: DagEdge[] = [{ id: 'e1', source: 's1', target: 's2' }];

        const res = validateDagTopology(nodes, edges);
        expect(res.isValid).toBe(false);
        expect(res.orphanNodeIds).toEqual(['orphan']);
        expect(res.errors).toHaveLength(1);
        expect(res.errors[0]).toContain('Orphan node(s) detected');
        expect(res.nodeErrors?.orphan?.[0]).toContain('Orphan node(s) detected');
    });

    it('flags SOURCE nodes that receive incoming edges', () => {
        const nodes: DagNode[] = [
            { id: 's1', data: { label: 'Source 1', type: 'SOURCE', spark_transform: 'CsvSource' } },
            { id: 's2', data: { label: 'Source 2', type: 'SOURCE', spark_transform: 'ParquetSource' } },
            { id: 'sink', data: { label: 'Sink', type: 'SINK', spark_transform: 'IcebergSink' } },
        ];
        const edges: DagEdge[] = [
            { id: 'e1', source: 's1', target: 's2' },
            { id: 'e2', source: 's2', target: 'sink' },
        ];

        const res = validateDagTopology(nodes, edges);
        expect(res.isValid).toBe(false);
        expect(res.invalidUpstreamNodeIds).toEqual(['s2']);
        expect(res.errors[0]).toContain('Source node "Source 2" (s2) cannot have incoming connections');
    });
});

describe('Extension Host validateDagTopology — Join Handles, Stream-Static & Stream-Stream Rules (A13)', () => {
    const batchLeft: DagNode = { id: 'left_src', data: { label: 'Orders', type: 'SOURCE', spark_transform: 'BigQuerySource' } };
    const batchRight: DagNode = { id: 'right_src', data: { label: 'Customers', type: 'SOURCE', spark_transform: 'CloudSqlSource' } };
    const streamLeft: DagNode = { id: 'stream_l', data: { label: 'Kafka Orders', type: 'SOURCE', spark_transform: 'KafkaSource' } };
    const streamRight: DagNode = { id: 'stream_r', data: { label: 'Kafka Clicks', type: 'SOURCE', spark_transform: 'KafkaSource' } };
    const batchSink: DagNode = { id: 'sink', data: { label: 'Iceberg', type: 'SINK', spark_transform: 'IcebergSink' } };
    const streamSinkWithCkpt: DagNode = {
        id: 'sink',
        data: {
            label: 'Iceberg',
            type: 'SINK',
            spark_transform: 'IcebergSink',
            properties: { checkpointLocation: 'gs://spark-ckpt/iceberg' },
        },
    };

    const makeJoin = (joinType = 'inner'): DagNode => ({
        id: 'join_1',
        data: {
            label: 'Join Orders',
            type: 'STEP',
            spark_transform: 'Join',
            properties: { joinType },
        },
    });

    it('enforces 1 left and 1 right targetHandle on Join nodes', () => {
        // Valid batch join
        const valid = validateDagTopology(
            [batchLeft, batchRight, makeJoin('left'), batchSink],
            [
                { id: 'e1', source: 'left_src', target: 'join_1', targetHandle: 'left' },
                { id: 'e2', source: 'right_src', target: 'join_1', targetHandle: 'right' },
                { id: 'e3', source: 'join_1', target: 'sink' },
            ]
        );
        expect(valid.isValid).toBe(true);

        // Missing right handle
        const missingRight = validateDagTopology(
            [batchLeft, makeJoin('inner'), batchSink],
            [
                { id: 'e1', source: 'left_src', target: 'join_1', targetHandle: 'left' },
                { id: 'e3', source: 'join_1', target: 'sink' },
            ]
        );
        expect(missingRight.isValid).toBe(false);
        expect(missingRight.missingUpstreamNodeIds).toEqual(['join_1']);
        expect(missingRight.errors[0]).toContain('missing "right"');

        // Untyped handle (missing targetHandle)
        const untyped = validateDagTopology(
            [batchLeft, batchRight, makeJoin('inner'), batchSink],
            [
                { id: 'e1', source: 'left_src', target: 'join_1' },
                { id: 'e2', source: 'right_src', target: 'join_1', targetHandle: 'right' },
                { id: 'e3', source: 'join_1', target: 'sink' },
            ]
        );
        expect(untyped.isValid).toBe(false);
        expect(untyped.invalidUpstreamNodeIds).toContain('join_1');
        expect(untyped.missingUpstreamNodeIds).toContain('join_1');

        // Duplicate edges on left handle
        const thirdSrc: DagNode = { id: 'extra_src', data: { label: 'Extra', type: 'SOURCE', spark_transform: 'ParquetSource' } };
        const duplicateLeft = validateDagTopology(
            [batchLeft, thirdSrc, batchRight, makeJoin('inner'), batchSink],
            [
                { id: 'e1', source: 'left_src', target: 'join_1', targetHandle: 'left' },
                { id: 'e1b', source: 'extra_src', target: 'join_1', targetHandle: 'left' },
                { id: 'e2', source: 'right_src', target: 'join_1', targetHandle: 'right' },
                { id: 'e3', source: 'join_1', target: 'sink' },
            ]
        );
        expect(duplicateLeft.isValid).toBe(false);
        expect(duplicateLeft.invalidUpstreamNodeIds).toEqual(['join_1']);
        expect(duplicateLeft.errors[0]).toContain('at most 1 incoming edge per handle');

        // Broadcast lookup Join (broadcastRight: true) with explicit left and right handles passes cleanly
        const broadcastLookupJoin: DagNode = {
            id: 'join_lookup',
            data: {
                label: 'Broadcast Lookup',
                type: 'STEP',
                spark_transform: 'Join',
                properties: { joinType: 'left_outer', broadcastRight: true },
            },
        };
        const lookupValid = validateDagTopology(
            [batchLeft, batchRight, broadcastLookupJoin, batchSink],
            [
                { id: 'e1', source: 'left_src', target: 'join_lookup', targetHandle: 'left' },
                { id: 'e2', source: 'right_src', target: 'join_lookup', targetHandle: 'right' },
                { id: 'e3', source: 'join_lookup', target: 'sink' },
            ]
        );
        expect(lookupValid.isValid).toBe(true);
        expect(Object.keys(lookupValid).sort()).toEqual([
            'cycleEdgeIds',
            'cycleNodeIds',
            'cyclePathString',
            'cyclePaths',
            'errors',
            'hasCycles',
            'invalidUpstreamNodeIds',
            'isValid',
            'missingUpstreamNodeIds',
            'nodeErrors',
            'nodeWarnings',
            'orphanNodeIds',
            'sortedNodeIds',
            'warnings',
        ]);
    });

    it('enforces Spark Structured Streaming join type matrix on stream-static joins', () => {
        // Streaming left + static right allows inner, left_outer, left_semi, and left_anti while rejecting right_outer and full_outer
        for (const allowedType of ['inner', 'left', 'left_outer', 'left_semi', 'left_anti']) {
            const res = validateDagTopology(
                [streamLeft, batchRight, makeJoin(allowedType), streamSinkWithCkpt],
                [
                    { id: 'e1', source: 'stream_l', target: 'join_1', targetHandle: 'left' },
                    { id: 'e2', source: 'right_src', target: 'join_1', targetHandle: 'right' },
                    { id: 'e3', source: 'join_1', target: 'sink' },
                ]
            );
            expect(res.isValid, `Expected ${allowedType} to pass when left is streaming`).toBe(true);
        }

        for (const rejectedType of ['right', 'right_outer', 'full', 'full_outer']) {
            const res = validateDagTopology(
                [streamLeft, batchRight, makeJoin(rejectedType), streamSinkWithCkpt],
                [
                    { id: 'e1', source: 'stream_l', target: 'join_1', targetHandle: 'left' },
                    { id: 'e2', source: 'right_src', target: 'join_1', targetHandle: 'right' },
                    { id: 'e3', source: 'join_1', target: 'sink' },
                ]
            );
            expect(res.isValid, `Expected ${rejectedType} to fail when right is static`).toBe(false);
            expect(res.errors[0]).toContain('Stream-static Join');
        }

        // Static left + streaming right allows inner and right_outer while rejecting left, left_outer, left_semi, left_anti, and full_outer
        for (const allowedType of ['inner', 'right', 'right_outer']) {
            const res = validateDagTopology(
                [batchLeft, streamRight, makeJoin(allowedType), streamSinkWithCkpt],
                [
                    { id: 'e1', source: 'left_src', target: 'join_1', targetHandle: 'left' },
                    { id: 'e2', source: 'stream_r', target: 'join_1', targetHandle: 'right' },
                    { id: 'e3', source: 'join_1', target: 'sink' },
                ]
            );
            expect(res.isValid, `Expected ${allowedType} to pass when right is streaming`).toBe(true);
        }

        for (const rejectedType of ['left', 'left_outer', 'left_semi', 'left_anti', 'full_outer']) {
            const res = validateDagTopology(
                [batchLeft, streamRight, makeJoin(rejectedType), streamSinkWithCkpt],
                [
                    { id: 'e1', source: 'left_src', target: 'join_1', targetHandle: 'left' },
                    { id: 'e2', source: 'stream_r', target: 'join_1', targetHandle: 'right' },
                    { id: 'e3', source: 'join_1', target: 'sink' },
                ]
            );
            expect(res.isValid, `Expected ${rejectedType} to fail when left is static`).toBe(false);
            expect(res.errors[0]).toContain('Stream-static Join');
        }
    });

    it('requires upstream Watermark steps for stream-stream outer joins and permits inner without watermark', () => {
        // Stream-stream inner join without watermark is valid
        const innerRes = validateDagTopology(
            [streamLeft, streamRight, makeJoin('inner'), streamSinkWithCkpt],
            [
                { id: 'e1', source: 'stream_l', target: 'join_1', targetHandle: 'left' },
                { id: 'e2', source: 'stream_r', target: 'join_1', targetHandle: 'right' },
                { id: 'e3', source: 'join_1', target: 'sink' },
            ]
        );
        expect(innerRes.isValid).toBe(true);

        // Stream-stream outer joins without watermark fail
        for (const outerType of ['left', 'left_outer', 'right', 'right_outer', 'full', 'full_outer']) {
            const noWmRes = validateDagTopology(
                [streamLeft, streamRight, makeJoin(outerType), streamSinkWithCkpt],
                [
                    { id: 'e1', source: 'stream_l', target: 'join_1', targetHandle: 'left' },
                    { id: 'e2', source: 'stream_r', target: 'join_1', targetHandle: 'right' },
                    { id: 'e3', source: 'join_1', target: 'sink' },
                ]
            );
            expect(noWmRes.isValid, `Expected ${outerType} without watermark to fail`).toBe(false);
            expect(noWmRes.errors[0]).toContain('requires an upstream Watermark step');
        }

        // Stream-stream outer join with Watermark on both branches (including multi-hop) passes
        const wmLeft: DagNode = { id: 'wm_l', data: { label: 'WM Left', type: 'STEP', spark_transform: 'Watermark' } };
        const filterLeft: DagNode = { id: 'flt_l', data: { label: 'Filter Left', type: 'STEP', spark_transform: 'Filter' } };
        const wmRight: DagNode = { id: 'wm_r', data: { label: 'WM Right', type: 'STEP', spark_transform: 'Watermark' } };

        const validOuter = validateDagTopology(
            [streamLeft, wmLeft, filterLeft, streamRight, wmRight, makeJoin('full_outer'), streamSinkWithCkpt],
            [
                { id: 'e1', source: 'stream_l', target: 'wm_l' },
                { id: 'e2', source: 'wm_l', target: 'flt_l' },
                { id: 'e3', source: 'flt_l', target: 'join_1', targetHandle: 'left' },
                { id: 'e4', source: 'stream_r', target: 'wm_r' },
                { id: 'e5', source: 'wm_r', target: 'join_1', targetHandle: 'right' },
                { id: 'e6', source: 'join_1', target: 'sink' },
            ]
        );
        expect(validOuter.isValid).toBe(true);
        expect(validOuter.errors).toEqual([]);
    });
});

describe('Extension Host validateDagTopology — Multi-Input, Watermark Warnings & Checkpoints (A13)', () => {
    it('enforces >= 2 inputs for UnionByName and SetOperation, >= 1 for SparkSqlQuery, and 1 for single-input steps/sinks', () => {
        const s1: DagNode = { id: 's1', data: { label: 'S1', type: 'SOURCE', spark_transform: 'ParquetSource' } };
        const s2: DagNode = { id: 's2', data: { label: 'S2', type: 'SOURCE', spark_transform: 'CsvSource' } };
        const s3: DagNode = { id: 's3', data: { label: 'S3', type: 'SOURCE', spark_transform: 'JsonSource' } };
        const sink: DagNode = { id: 'sink', data: { label: 'Sink', type: 'SINK', spark_transform: 'IcebergSink' } };

        // UnionByName fails with 1 input and passes with 2 inputs
        const unionNode: DagNode = { id: 'u1', data: { label: 'Union', type: 'STEP', spark_transform: 'UnionByName' } };
        const unionOne = validateDagTopology(
            [s1, unionNode, sink],
            [
                { id: 'e1', source: 's1', target: 'u1' },
                { id: 'e2', source: 'u1', target: 'sink' },
            ]
        );
        expect(unionOne.isValid).toBe(false);
        expect(unionOne.missingUpstreamNodeIds).toEqual(['u1']);
        expect(unionOne.errors[0]).toContain('UnionByName node "Union" (u1) requires at least 2 upstream inputs');

        const unionTwo = validateDagTopology(
            [s1, s2, unionNode, sink],
            [
                { id: 'e1', source: 's1', target: 'u1' },
                { id: 'e2', source: 's2', target: 'u1' },
                { id: 'e3', source: 'u1', target: 'sink' },
            ]
        );
        expect(unionTwo.isValid).toBe(true);

        // SetOperation fails with 1 input and passes with 2 inputs
        const setOpNode: DagNode = { id: 'set1', data: { label: 'Intersect', type: 'STEP', spark_transform: 'SetOperation' } };
        expect(
            validateDagTopology(
                [s1, setOpNode, sink],
                [
                    { id: 'e1', source: 's1', target: 'set1' },
                    { id: 'e2', source: 'set1', target: 'sink' },
                ]
            ).isValid
        ).toBe(false);

        // SparkSqlQuery accepts 1 or 3 inputs, rejects 0 inputs
        const sqlNode: DagNode = { id: 'sql1', data: { label: 'SQL', type: 'STEP', spark_transform: 'SparkSqlQuery' } };
        expect(
            validateDagTopology([sqlNode, sink], [{ id: 'e1', source: 'sql1', target: 'sink' }]).missingUpstreamNodeIds
        ).toEqual(['sql1']);
        expect(
            validateDagTopology(
                [s1, s2, s3, sqlNode, sink],
                [
                    { id: 'e1', source: 's1', target: 'sql1' },
                    { id: 'e2', source: 's2', target: 'sql1' },
                    { id: 'e3', source: 's3', target: 'sql1' },
                    { id: 'e4', source: 'sql1', target: 'sink' },
                ]
            ).isValid
        ).toBe(true);

        // Single-input step or sink rejects > 1 incoming edges and rejects 0 incoming when connected downstream
        const filterNode: DagNode = { id: 'flt', data: { label: 'Filter', type: 'STEP', spark_transform: 'Filter' } };
        const multiIntoSingle = validateDagTopology(
            [s1, s2, filterNode, sink],
            [
                { id: 'e1', source: 's1', target: 'flt' },
                { id: 'e2', source: 's2', target: 'flt' },
                { id: 'e3', source: 'flt', target: 'sink' },
            ]
        );
        expect(multiIntoSingle.isValid).toBe(false);
        expect(multiIntoSingle.invalidUpstreamNodeIds).toEqual(['flt']);

        const noUpstreamStep = validateDagTopology([filterNode, sink], [{ id: 'e1', source: 'flt', target: 'sink' }]);
        expect(noUpstreamStep.isValid).toBe(false);
        expect(noUpstreamStep.missingUpstreamNodeIds).toEqual(['flt']);
    });

    it('emits warnings when streaming GroupByAgg or Deduplicate lacks an upstream Watermark step', () => {
        const rateSrc: DagNode = { id: 'rate', data: { label: 'Rate', type: 'SOURCE', spark_transform: 'RateSource' } };
        const agg: DagNode = { id: 'agg', data: { label: 'Window Agg', type: 'STEP', spark_transform: 'GroupByAgg' } };
        const dedup: DagNode = { id: 'dedup', data: { label: 'Dedup', type: 'STEP', spark_transform: 'Deduplicate' } };
        const wm: DagNode = { id: 'wm', data: { label: 'Watermark', type: 'STEP', spark_transform: 'Watermark' } };
        const sink: DagNode = {
            id: 'sink',
            data: {
                label: 'Delta Sink',
                type: 'SINK',
                spark_transform: 'DeltaSink',
                properties: { checkpointLocation: 'gs://ckpt-bucket/delta' },
            },
        };

        const unwatermarked = validateDagTopology(
            [rateSrc, dedup, agg, sink],
            [
                { id: 'e1', source: 'rate', target: 'dedup' },
                { id: 'e2', source: 'dedup', target: 'agg' },
                { id: 'e3', source: 'agg', target: 'sink' },
            ]
        );
        expect(unwatermarked.isValid).toBe(true);
        expect(unwatermarked.warnings).toHaveLength(2);
        expect(unwatermarked.nodeWarnings?.dedup?.[0]).toContain('Streaming Deduplicate');
        expect(unwatermarked.nodeWarnings?.agg?.[0]).toContain('Streaming GroupByAgg');

        const watermarked = validateDagTopology(
            [rateSrc, wm, dedup, agg, sink],
            [
                { id: 'e0', source: 'rate', target: 'wm' },
                { id: 'e1', source: 'wm', target: 'dedup' },
                { id: 'e2', source: 'dedup', target: 'agg' },
                { id: 'e3', source: 'agg', target: 'sink' },
            ]
        );
        expect(watermarked.isValid).toBe(true);
        expect(watermarked.warnings).toEqual([]);
    });

    it('validates streaming Sink checkpoint paths across canvas warning mode, jobConfig enforcement, and URI format checks', () => {
        const kafkaSrc: DagNode = { id: 'k', data: { label: 'Kafka', type: 'SOURCE', spark_transform: 'KafkaSource' } };
        const bareSink: DagNode = { id: 'sink', data: { label: 'Iceberg', type: 'SINK', spark_transform: 'IcebergSink' } };
        const edges: DagEdge[] = [{ id: 'e1', source: 'k', target: 'sink' }];

        // 2-arg canvas mode: warns without blocking canvas connection
        const canvasRes = validateDagTopology([kafkaSrc, bareSink], edges);
        expect(canvasRes.isValid).toBe(true);
        expect(canvasRes.warnings.some((w) => w.includes('missing a checkpoint path'))).toBe(true);

        // 3-arg jobConfig mode without checkpointRoot or sink checkpointLocation: fatal error
        const missingCkptErr = validateDagTopology([kafkaSrc, bareSink], edges, {
            jobConfig: { runner: 'ServerlessSpark', checkpointRoot: '' },
        });
        expect(missingCkptErr.isValid).toBe(false);
        expect(missingCkptErr.errors[0]).toContain('missing a checkpoint path');
        expect(missingCkptErr.nodeErrors?.sink).toHaveLength(1);

        // Inherits valid pipeline checkpointRoot when sink checkpointLocation is blank
        const inheritedOk = validateDagTopology(
            [
                kafkaSrc,
                {
                    id: 'sink',
                    data: {
                        label: 'Iceberg',
                        type: 'SINK',
                        spark_transform: 'IcebergSink',
                        properties: { checkpointLocation: '' },
                    },
                },
            ],
            edges,
            { jobConfig: { checkpointRoot: 'gs://my-spark-bucket/checkpoints' } }
        );
        expect(inheritedOk.isValid).toBe(true);
        expect(inheritedOk.errors).toEqual([]);
        expect(inheritedOk.warnings).toEqual([]);

        // Rejects malformed checkpoint paths (whitespace, traversal, http scheme, empty bucket)
        for (const badPath of ['   ', 'gs://bucket/../escape', 'http://example.com/ckpt', 'gs://']) {
            const badRes = validateDagTopology(
                [
                    kafkaSrc,
                    {
                        id: 'sink',
                        data: {
                            label: 'Iceberg',
                            type: 'SINK',
                            spark_transform: 'IcebergSink',
                            properties: { checkpointLocation: badPath },
                        },
                    },
                ],
                edges
            );
            expect(badRes.isValid, `Expected checkpointLocation "${badPath}" to fail`).toBe(false);
            expect(badRes.errors[0]).toContain('invalid checkpointLocation');
        }
    });
});

describe('validateDagTopology — cycle detection boundaries (T3)', () => {
    it('returns a clean valid result for empty, null, and id-less node inputs', () => {
        for (const res of [
            validateDagTopology([], []),
            validateDagTopology(null as unknown as DagNode[], null as unknown as DagEdge[]),
            validateDagTopology([{ id: undefined } as unknown as DagNode], []),
        ]) {
            expect(res.isValid).toBe(true);
            expect(res.hasCycles).toBe(false);
            expect(res.sortedNodeIds).toEqual([]);
            expect(res.errors).toEqual([]);
            expect(res.warnings).toEqual([]);
        }
    });

    it('detects a self-loop with an exact path, edge id, and labeled path string', () => {
        const res = validateDagTopology(
            [{ id: 'a', data: { label: 'Loop', type: 'STEP', spark_transform: 'Select' } }],
            [{ id: 'self', source: 'a', target: 'a' }]
        );
        expect(res.hasCycles).toBe(true);
        expect(res.cyclePaths).toEqual([['a', 'a']]);
        expect(res.cycleEdgeIds).toEqual(['self']);
        expect(res.cyclePathString).toBe('Loop (a) -> Loop (a)');
        expect(res.sortedNodeIds).toEqual([]);
    });

    it('marks only the loop members of a cycle and not the upstream source or downstream tail', () => {
        const nodes: DagNode[] = [
            { id: 'src', data: { type: 'SOURCE', spark_transform: 'ParquetSource' } },
            { id: 'b', data: { type: 'STEP', spark_transform: 'Filter' } },
            { id: 'c', data: { type: 'STEP', spark_transform: 'Select' } },
            { id: 'd', data: { type: 'STEP', spark_transform: 'WithColumns' } },
            { id: 'tail', data: { type: 'SINK', spark_transform: 'ParquetSink' } },
        ];
        const edges: DagEdge[] = [
            { id: 'e_src_b', source: 'src', target: 'b' },
            { id: 'e_b_c', source: 'b', target: 'c' },
            { id: 'e_c_d', source: 'c', target: 'd' },
            { id: 'e_d_b', source: 'd', target: 'b' },
            { id: 'e_d_tail', source: 'd', target: 'tail' },
        ];
        const res = validateDagTopology(nodes, edges);
        expect(res.isValid).toBe(false);
        expect([...res.cycleNodeIds].sort()).toEqual(['b', 'c', 'd']);
        expect([...res.cycleEdgeIds].sort()).toEqual(['e_b_c', 'e_c_d', 'e_d_b']);
        expect(Object.keys(res.nodeErrors ?? {}).sort()).toEqual(['b', 'c', 'd']);
        expect(res.errors).toHaveLength(1);
        expect(res.errors[0]).toMatch(/^Cyclic dependency detected in pipeline graph: /);
    });

    it('reports every disjoint cycle and every parallel edge id while ignoring id-less edges', () => {
        const nodes: DagNode[] = ['a', 'b', 'x', 'y'].map((id) => ({
            id,
            data: { type: 'STEP', spark_transform: 'Select' },
        }));
        const edges: DagEdge[] = [
            { id: 'ab1', source: 'a', target: 'b' },
            { id: 'ab2', source: 'a', target: 'b' },
            { id: 'ba', source: 'b', target: 'a' },
            { source: 'x', target: 'y' },
            { id: 'yx', source: 'y', target: 'x' },
        ];
        const res = validateDagTopology(nodes, edges);
        expect(res.cyclePaths).toHaveLength(2);
        expect([...res.cycleNodeIds].sort()).toEqual(['a', 'b', 'x', 'y']);
        expect([...res.cycleEdgeIds].sort()).toEqual(['ab1', 'ab2', 'ba', 'yx']);
        // The path string describes only the first detected loop.
        expect(res.cyclePathString.split(' -> ')).toHaveLength(3);
    });

    it('suppresses source-inbound and arity errors for nodes inside a cycle', () => {
        const nodes: DagNode[] = [
            { id: 'src', data: { type: 'SOURCE', spark_transform: 'ParquetSource' } },
            { id: 'join', data: { type: 'STEP', spark_transform: 'Join' } },
        ];
        const res = validateDagTopology(nodes, [
            { id: 'e1', source: 'src', target: 'join' },
            { id: 'e2', source: 'join', target: 'src' },
        ]);
        expect(res.hasCycles).toBe(true);
        expect(res.errors).toHaveLength(1);
        expect(res.invalidUpstreamNodeIds).toEqual([]);
        expect(res.missingUpstreamNodeIds).toEqual([]);
    });
});

describe('validateDagTopology — orphan node boundaries (T3)', () => {
    it('never flags a single-node graph as orphaned but warns about missing SOURCE or SINK', () => {
        const res = validateDagTopology([{ id: 'only', data: { type: 'STEP', spark_transform: 'Filter' } }], []);
        expect(res.orphanNodeIds).toEqual([]);
        expect(res.isValid).toBe(true);
        expect(res.warnings).toEqual([
            'Pipeline has no SOURCE node. At least one SOURCE is required for execution.',
            'Pipeline has no SINK node. At least one SINK is recommended for output delivery.',
        ]);
    });

    it('lists multiple orphans in one error using labels when present and ids otherwise', () => {
        const nodes: DagNode[] = [
            { id: 'src', data: { type: 'SOURCE', spark_transform: 'ParquetSource' } },
            { id: 'sink', data: { type: 'SINK', spark_transform: 'ParquetSink' } },
            { id: 'o1', data: { label: 'Lonely Filter', type: 'STEP', spark_transform: 'Filter' } },
            { id: 'o2', data: { type: 'STEP', spark_transform: 'Select' } },
        ];
        const res = validateDagTopology(nodes, [{ id: 'e', source: 'src', target: 'sink' }]);
        expect(res.orphanNodeIds).toEqual(['o1', 'o2']);
        expect(res.errors).toEqual([
            'Orphan node(s) detected: "Lonely Filter" (o1), o2. All steps must be connected into the pipeline.',
        ]);
        expect(res.nodeErrors?.o1).toEqual(res.errors);
        expect(res.nodeErrors?.o2).toEqual(res.errors);
    });

    it('treats nodes linked only by dangling edges as orphans', () => {
        const nodes: DagNode[] = [
            { id: 'src', data: { type: 'SOURCE', spark_transform: 'ParquetSource' } },
            { id: 'sink', data: { type: 'SINK', spark_transform: 'ParquetSink' } },
        ];
        const res = validateDagTopology(nodes, [
            { id: 'd1', source: 'src', target: 'ghost' },
            { id: 'd2', source: 'ghost', target: 'sink' },
            { id: 'bad' } as unknown as DagEdge,
        ]);
        expect(res.orphanNodeIds).toEqual(['src', 'sink']);
        expect(res.isValid).toBe(false);
    });

    it('reports only the orphan error for orphaned Join and UnionByName steps', () => {
        const nodes: DagNode[] = [
            { id: 'src', data: { type: 'SOURCE', spark_transform: 'ParquetSource' } },
            { id: 'sink', data: { type: 'SINK', spark_transform: 'ParquetSink' } },
            { id: 'join', data: { type: 'STEP', spark_transform: 'Join' } },
            { id: 'union', data: { type: 'STEP', spark_transform: 'UnionByName' } },
        ];
        const res = validateDagTopology(nodes, [{ id: 'e', source: 'src', target: 'sink' }]);
        expect(res.orphanNodeIds).toEqual(['join', 'union']);
        expect(res.errors).toHaveLength(1);
        expect(res.missingUpstreamNodeIds).toEqual([]);
        expect(res.invalidUpstreamNodeIds).toEqual([]);
    });
});

describe('validateDagTopology — stream-stream outer join watermark boundaries (T3)', () => {
    const streamL: DagNode = { id: 'sl', data: { type: 'SOURCE', spark_transform: 'KafkaSource' } };
    const streamR: DagNode = { id: 'sr', data: { type: 'SOURCE', spark_transform: 'RateSource' } };
    const sink: DagNode = {
        id: 'sink',
        data: { type: 'SINK', spark_transform: 'DeltaSink', properties: { checkpointLocation: 'gs://ckpt/out' } },
    };
    const join = (properties: Record<string, unknown>): DagNode => ({
        id: 'join',
        data: { label: 'J', type: 'STEP', spark_transform: 'Join', properties },
    });
    const directEdges: DagEdge[] = [
        { id: 'l', source: 'sl', target: 'join', targetHandle: 'left' },
        { id: 'r', source: 'sr', target: 'join', targetHandle: 'right' },
        { id: 'o', source: 'join', target: 'sink' },
    ];

    it('normalizes camelCase, spaced, upper-case, and alias join types before the watermark check', () => {
        const cases: Array<[Record<string, unknown>, string]> = [
            [{ joinType: 'leftOuter' }, 'left_outer'],
            [{ joinType: 'FULL OUTER' }, 'full_outer'],
            [{ joinType: 'outer' }, 'full_outer'],
            [{ joinType: 'right-outer' }, 'right_outer'],
            [{ how: 'fullouter' }, 'full_outer'],
        ];
        for (const [props, normalized] of cases) {
            const res = validateDagTopology([streamL, streamR, join(props), sink], directEdges);
            expect(res.isValid, JSON.stringify(props)).toBe(false);
            expect(res.errors).toEqual([
                `Stream-stream "${normalized}" Join node "J" (join) requires an upstream Watermark step on streaming inputs (missing on "left" and "right").`,
            ]);
        }
    });

    it('names only the unwatermarked side when one branch has a Watermark step', () => {
        const wm: DagNode = { id: 'wm', data: { type: 'STEP', spark_transform: 'Watermark' } };
        const res = validateDagTopology(
            [streamL, wm, streamR, join({ joinType: 'left_outer' }), sink],
            [
                { id: 'a', source: 'sl', target: 'wm' },
                { id: 'b', source: 'wm', target: 'join', targetHandle: 'left' },
                { id: 'r', source: 'sr', target: 'join', targetHandle: 'right' },
                { id: 'o', source: 'join', target: 'sink' },
            ]
        );
        expect(res.errors).toHaveLength(1);
        expect(res.errors[0]).toContain('(missing on "right")');
        expect(res.nodeErrors?.join).toEqual(res.errors);
    });

    it('rejects a branch where an unwatermarked stream merges after the Watermark step', () => {
        const extra: DagNode = { id: 'extra', data: { type: 'SOURCE', spark_transform: 'PubSubSource' } };
        const wmL: DagNode = { id: 'wm_l', data: { type: 'STEP', spark_transform: 'Watermark' } };
        const wmR: DagNode = { id: 'wm_r', data: { type: 'STEP', spark_transform: 'Watermark' } };
        const union: DagNode = { id: 'union', data: { type: 'STEP', spark_transform: 'UnionByName' } };
        const res = validateDagTopology(
            [streamL, extra, wmL, union, streamR, wmR, join({ joinType: 'full_outer' }), sink],
            [
                { id: 'a', source: 'sl', target: 'wm_l' },
                { id: 'b', source: 'wm_l', target: 'union' },
                { id: 'c', source: 'extra', target: 'union' },
                { id: 'd', source: 'union', target: 'join', targetHandle: 'left' },
                { id: 'e', source: 'sr', target: 'wm_r' },
                { id: 'f', source: 'wm_r', target: 'join', targetHandle: 'right' },
                { id: 'o', source: 'join', target: 'sink' },
            ]
        );
        expect(res.isValid).toBe(false);
        expect(res.errors[0]).toContain('(missing on "left")');
    });

    it('accepts Watermark steps that feed the Join handles directly', () => {
        const wmL: DagNode = { id: 'wm_l', data: { type: 'STEP', spark_transform: 'Watermark' } };
        const wmR: DagNode = { id: 'wm_r', data: { type: 'STEP', spark_transform: 'Watermark' } };
        const res = validateDagTopology(
            [streamL, wmL, streamR, wmR, join({ joinType: 'right' }), sink],
            [
                { id: 'a', source: 'sl', target: 'wm_l' },
                { id: 'b', source: 'wm_l', target: 'join', targetHandle: 'left' },
                { id: 'c', source: 'sr', target: 'wm_r' },
                { id: 'd', source: 'wm_r', target: 'join', targetHandle: 'right' },
                { id: 'o', source: 'join', target: 'sink' },
            ]
        );
        expect(res.isValid).toBe(true);
        expect(res.errors).toEqual([]);
    });

    it('does not require watermarks for inner, cross, semi, or missing join types', () => {
        for (const props of [{}, { joinType: '' }, { joinType: 'cross' }, { joinType: 'leftsemi' }, { joinType: 'anti' }]) {
            const res = validateDagTopology([streamL, streamR, join(props), sink], directEdges);
            expect(res.isValid, JSON.stringify(props)).toBe(true);
        }
    });

    it('does not apply the watermark rule to batch-batch outer joins', () => {
        const bl: DagNode = { id: 'sl', data: { type: 'SOURCE', spark_transform: 'ParquetSource' } };
        const br: DagNode = { id: 'sr', data: { type: 'SOURCE', spark_transform: 'BigQuerySource' } };
        const batchSink: DagNode = { id: 'sink', data: { type: 'SINK', spark_transform: 'ParquetSink' } };
        const res = validateDagTopology([bl, br, join({ joinType: 'full_outer' }), batchSink], directEdges);
        expect(res.isValid).toBe(true);
    });

    it('reads the join type from top-level YAML-shaped node properties', () => {
        const yamlJoin = {
            id: 'join',
            type: 'STEP',
            spark_transform: 'Join',
            properties: { joinType: 'left_outer' },
        } as DagNode;
        const res = validateDagTopology([streamL, streamR, yamlJoin, sink], directEdges);
        expect(res.isValid).toBe(false);
        expect(res.errors[0]).toContain('Stream-stream "left_outer" Join node join requires an upstream Watermark step');
    });

    it('treats a null targetHandle as an untyped Join input', () => {
        const res = validateDagTopology(
            [streamL, streamR, join({ joinType: 'inner' }), sink],
            [
                { id: 'l', source: 'sl', target: 'join', targetHandle: null },
                { id: 'r', source: 'sr', target: 'join', targetHandle: 'right' },
                { id: 'o', source: 'join', target: 'sink' },
            ]
        );
        expect(res.isValid).toBe(false);
        expect(res.invalidUpstreamNodeIds).toEqual(['join']);
        expect(res.missingUpstreamNodeIds).toEqual(['join']);
    });
});

describe('validateDagTopology — missing and malformed checkpoint boundaries (T3)', () => {
    const kafka: DagNode = { id: 'k', data: { type: 'SOURCE', spark_transform: 'KafkaSource' } };
    const sinkWith = (properties?: Record<string, unknown>, transform = 'DeltaSink'): DagNode => ({
        id: 'sink',
        data: { label: 'Out', type: 'SINK', spark_transform: transform, ...(properties ? { properties } : {}) },
    });
    const edges: DagEdge[] = [{ id: 'e', source: 'k', target: 'sink' }];
    const missingMsg =
        'Streaming Sink node "Out" (sink) is missing a checkpoint path. Configure "checkpointLocation" on the sink or "checkpointRoot" in jobConfig.';

    it('raises a fatal error when requireCheckpoints is true even without jobConfig', () => {
        const res = validateDagTopology([kafka, sinkWith()], edges, { requireCheckpoints: true });
        expect(res.isValid).toBe(false);
        expect(res.errors).toEqual([missingMsg]);
        expect(res.warnings).toEqual([]);
    });

    it('keeps the missing checkpoint as a warning when requireCheckpoints is false and jobConfig is absent', () => {
        const res = validateDagTopology([kafka, sinkWith()], edges, { requireCheckpoints: false });
        expect(res.isValid).toBe(true);
        expect(res.warnings).toEqual([missingMsg]);
        expect(res.nodeWarnings?.sink).toEqual([missingMsg]);
    });

    it('exempts MemorySink and batch sinks from the checkpoint requirement', () => {
        const memory = validateDagTopology([kafka, sinkWith(undefined, 'MemorySink')], edges, { jobConfig: {} });
        expect(memory.isValid).toBe(true);
        expect(memory.warnings).toEqual([]);

        const batchSrc: DagNode = { id: 'k', data: { type: 'SOURCE', spark_transform: 'ParquetSource' } };
        const batch = validateDagTopology([batchSrc, sinkWith()], edges, { requireCheckpoints: true });
        expect(batch.isValid).toBe(true);
        expect(batch.errors).toEqual([]);
    });

    it('accepts every supported cloud, local, and drive-letter checkpoint form', () => {
        for (const ckpt of [
            'gs://bucket/ckpt',
            's3a://bucket/ckpt',
            's3://bucket',
            'abfss://container@acct.dfs.core.windows.net/ckpt',
            'wasbs://container@acct.blob.core.windows.net/ckpt',
            'hdfs://namenode:8020/ckpt',
            'file:///tmp/ckpt',
            'file:/tmp/ckpt',
            'dbfs:/ckpt',
            '/tmp/ckpt',
            'relative/ckpt',
            'C:/ckpt',
        ]) {
            const res = validateDagTopology([kafka, sinkWith({ checkpointLocation: ckpt })], edges, { jobConfig: {} });
            expect(res.isValid, ckpt).toBe(true);
            expect(res.errors, ckpt).toEqual([]);
        }
    });

    it('rejects control characters, missing authority, unsupported schemes, and bad local URIs', () => {
        const cases: Array<[string, string]> = [
            ['gs://bucket/ck\tpt', 'control characters'],
            ['gs:///no-bucket', 'must include a bucket or authority'],
            ['gs://bucket/with space', 'cannot contain whitespace'],
            ['ftp://host/ckpt', 'Unsupported checkpoint URI scheme "ftp:"'],
            ['file:tmp/ckpt', 'must start with "file:/"'],
            ['..\\escape', 'parent directory traversal'],
        ];
        for (const [ckpt, fragment] of cases) {
            const res = validateDagTopology([kafka, sinkWith({ checkpointLocation: ckpt })], edges);
            expect(res.isValid, ckpt).toBe(false);
            expect(res.errors, ckpt).toHaveLength(1);
            expect(res.errors[0], ckpt).toContain('invalid checkpointLocation');
            expect(res.errors[0], ckpt).toContain(fragment);
        }
    });

    it('reports an invalid checkpointRoot and lets a valid sink checkpoint override it', () => {
        const badRoot = validateDagTopology([kafka, sinkWith()], edges, { jobConfig: { checkpointRoot: 'http://x/y' } });
        expect(badRoot.isValid).toBe(false);
        expect(badRoot.errors).toHaveLength(1);
        expect(badRoot.errors[0]).toContain('invalid pipeline checkpointRoot');

        const override = validateDagTopology([kafka, sinkWith({ checkpointLocation: 'gs://ok/ckpt' })], edges, {
            jobConfig: { checkpointRoot: 'http://x/y' },
        });
        expect(override.isValid).toBe(true);
    });

    it('falls back to checkpointRoot when the sink checkpoint is not a string', () => {
        const res = validateDagTopology([kafka, sinkWith({ checkpointLocation: 42 })], edges, {
            jobConfig: { checkpointRoot: 'gs://root/ckpt' },
        });
        expect(res.isValid).toBe(true);
    });

    it('reads checkpointLocation from top-level YAML-shaped sink properties', () => {
        const yamlSink = {
            id: 'sink',
            type: 'SINK',
            spark_transform: 'DeltaSink',
            properties: { checkpointLocation: 'gs://yaml/ckpt' },
        } as DagNode;
        const res = validateDagTopology([kafka, yamlSink], edges, { jobConfig: {} });
        expect(res.isValid).toBe(true);
        expect(res.errors).toEqual([]);
    });

    it('records a separate missing checkpoint error for every streaming sink', () => {
        const s1: DagNode = { id: 's1', data: { type: 'SINK', spark_transform: 'DeltaSink' } };
        const s2: DagNode = { id: 's2', data: { type: 'SINK', spark_transform: 'IcebergSink' } };
        const res = validateDagTopology(
            [kafka, s1, s2],
            [
                { id: 'a', source: 'k', target: 's1' },
                { id: 'b', source: 'k', target: 's2' },
            ],
            { jobConfig: { runner: 'ServerlessSpark' } }
        );
        expect(res.errors).toHaveLength(2);
        expect(Object.keys(res.nodeErrors ?? {}).sort()).toEqual(['s1', 's2']);
    });
});

describe('validateDagTopology — MergeCdcScd two-input handle and static target rules (EXT4)', () => {
    const cdcBatch: DagNode = { id: 'cdc', data: { type: 'SOURCE', spark_transform: 'ParquetSource' } };
    const cdcStream: DagNode = { id: 'cdc_stream', data: { type: 'SOURCE', spark_transform: 'KafkaSource' } };
    const targetSnap: DagNode = { id: 'target_snap', data: { type: 'SOURCE', spark_transform: 'IcebergSource' } };
    const targetStream: DagNode = {
        id: 'target_stream',
        data: { type: 'SOURCE', spark_transform: 'IcebergSource', properties: { readMode: 'streaming' } },
    };
    const mergeNode: DagNode = {
        id: 'merge',
        data: { label: 'Merge CDC', type: 'STEP', spark_transform: 'MergeCdcScd' },
    };
    const sink: DagNode = {
        id: 'sink',
        data: {
            type: 'SINK',
            spark_transform: 'IcebergSink',
            properties: { checkpointLocation: 'gs://ckpt/merge' },
        },
    };

    it('accepts valid batch-batch and stream-static MergeCdcScd graphs with explicit left and right handles', () => {
        const batchRes = validateDagTopology(
            [cdcBatch, targetSnap, mergeNode, sink],
            [
                { id: 'e1', source: 'cdc', target: 'merge', targetHandle: 'left' },
                { id: 'e2', source: 'target_snap', target: 'merge', targetHandle: 'right' },
                { id: 'e3', source: 'merge', target: 'sink' },
            ]
        );
        expect(batchRes.isValid).toBe(true);
        expect(batchRes.errors).toEqual([]);

        const streamStaticRes = validateDagTopology(
            [cdcStream, targetSnap, mergeNode, sink],
            [
                { id: 'e1', source: 'cdc_stream', target: 'merge', targetHandle: 'left' },
                { id: 'e2', source: 'target_snap', target: 'merge', targetHandle: 'right' },
                { id: 'e3', source: 'merge', target: 'sink' },
            ]
        );
        expect(streamStaticRes.isValid).toBe(true);
        expect(streamStaticRes.errors).toEqual([]);
    });

    it('rejects untyped, missing, or duplicate handles on MergeCdcScd', () => {
        const missingRight = validateDagTopology(
            [cdcBatch, mergeNode, sink],
            [
                { id: 'e1', source: 'cdc', target: 'merge', targetHandle: 'left' },
                { id: 'e3', source: 'merge', target: 'sink' },
            ]
        );
        expect(missingRight.isValid).toBe(false);
        expect(missingRight.missingUpstreamNodeIds).toEqual(['merge']);
        expect(missingRight.errors[0]).toContain('missing "right"');

        const untyped = validateDagTopology(
            [cdcBatch, targetSnap, mergeNode, sink],
            [
                { id: 'e1', source: 'cdc', target: 'merge' },
                { id: 'e2', source: 'target_snap', target: 'merge', targetHandle: 'right' },
                { id: 'e3', source: 'merge', target: 'sink' },
            ]
        );
        expect(untyped.isValid).toBe(false);
        expect(untyped.invalidUpstreamNodeIds).toEqual(['merge']);
        expect(untyped.errors[0]).toContain('explicit "left" and "right" handles');

        const extraSrc: DagNode = { id: 'extra', data: { type: 'SOURCE', spark_transform: 'CsvSource' } };
        const duplicateLeft = validateDagTopology(
            [cdcBatch, extraSrc, targetSnap, mergeNode, sink],
            [
                { id: 'e1', source: 'cdc', target: 'merge', targetHandle: 'left' },
                { id: 'e1b', source: 'extra', target: 'merge', targetHandle: 'left' },
                { id: 'e2', source: 'target_snap', target: 'merge', targetHandle: 'right' },
                { id: 'e3', source: 'merge', target: 'sink' },
            ]
        );
        expect(duplicateLeft.isValid).toBe(false);
        expect(duplicateLeft.invalidUpstreamNodeIds).toEqual(['merge']);
        expect(duplicateLeft.errors[0]).toContain('accepts at most 1 incoming edge per handle');
    });

    it('rejects MergeCdcScd when the right target table snapshot input is streaming', () => {
        const res = validateDagTopology(
            [cdcStream, targetStream, mergeNode, sink],
            [
                { id: 'e1', source: 'cdc_stream', target: 'merge', targetHandle: 'left' },
                { id: 'e2', source: 'target_stream', target: 'merge', targetHandle: 'right' },
                { id: 'e3', source: 'merge', target: 'sink' },
            ]
        );
        expect(res.isValid).toBe(false);
        expect(res.invalidUpstreamNodeIds).toEqual(['merge']);
        expect(res.errors[0]).toContain('static batch target table snapshot on the "right" handle');
    });
});
