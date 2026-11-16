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
import { validateDagTopology, type DagEdge, type DagNode } from '../../dagValidator';

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
    });

    it('enforces Spark Structured Streaming join type matrix on stream-static joins', () => {
        // Streaming left + static right: inner, left_outer, left_semi, left_anti are valid; right_outer & full_outer are rejected
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

        // Static left + streaming right: inner & right_outer pass; left, left_outer, left_semi, left_anti, full_outer fail
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

        // UnionByName with 1 input fails; with 2 inputs passes
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

        // SetOperation with 1 input fails; with 2 inputs passes
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
