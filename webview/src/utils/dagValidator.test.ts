/**
 * @fileoverview Unit tests for Webview Apache Spark DAG Topology & Semantics Validator (A13).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Kahn's algorithm topological sorting, cycle path extraction, orphan detection,
 * direct Source-to-Sink pipelines, Spark Join handle rules, stream-static and stream-stream
 * Join validation, multi-input arity, streaming watermark warnings, and checkpoint validation.
 */

import { describe, expect, it } from 'vitest';
import { validateDagTopology, type DagEdge, type DagNode } from './dagValidator';

describe('validateDagTopology (Kahn\'s Algorithm & Core Topology)', () => {
  it('returns valid for an empty graph', () => {
    const res = validateDagTopology([], []);
    expect(res.isValid).toBe(true);
    expect(res.hasCycles).toBe(false);
    expect(res.sortedNodeIds).toEqual([]);
    expect(res.cycleNodeIds).toEqual([]);
    expect(res.cycleEdgeIds).toEqual([]);
    expect(res.orphanNodeIds).toEqual([]);
    expect(res.errors).toEqual([]);
    expect(res.warnings).toEqual([]);
  });

  it('handles a single node pipeline without cycles', () => {
    const nodes: DagNode[] = [
      { id: 'src_1', type: 'pipeline', data: { label: 'Kafka Source', type: 'SOURCE' } },
    ];
    const res = validateDagTopology(nodes, []);
    expect(res.isValid).toBe(true);
    expect(res.hasCycles).toBe(false);
    expect(res.sortedNodeIds).toEqual(['src_1']);
    expect(res.orphanNodeIds).toEqual([]);
    expect(res.warnings).toContain(
      'Pipeline has no SINK node. At least one SINK is recommended for output delivery.'
    );
  });

  it('approves direct SOURCE -> SINK connections for zero-transform pipelines', () => {
    const nodes: DagNode[] = [
      { id: 'csv_src', type: 'pipeline', data: { label: 'CSV Source', type: 'SOURCE', spark_transform: 'CsvSource' } },
      { id: 'pq_sink', type: 'pipeline', data: { label: 'Parquet Sink', type: 'SINK', spark_transform: 'ParquetSink' } },
    ];
    const edges: DagEdge[] = [{ id: 'e1', source: 'csv_src', target: 'pq_sink' }];

    const res = validateDagTopology(nodes, edges);
    expect(res.isValid).toBe(true);
    expect(res.sortedNodeIds).toEqual(['csv_src', 'pq_sink']);
    expect(res.errors).toEqual([]);
    expect(res.warnings).toEqual([]);
  });

  it('correctly sorts a standard linear DAG (Source -> Step -> Sink)', () => {
    const nodes: DagNode[] = [
      { id: 'src_1', type: 'pipeline', data: { label: 'BigQuery Source', type: 'SOURCE' } },
      { id: 'step_1', type: 'pipeline', data: { label: 'Filter Step', type: 'STEP' } },
      { id: 'sink_1', type: 'pipeline', data: { label: 'GCS Sink', type: 'SINK' } },
    ];
    const edges: DagEdge[] = [
      { id: 'e1', source: 'src_1', target: 'step_1' },
      { id: 'e2', source: 'step_1', target: 'sink_1' },
    ];

    const res = validateDagTopology(nodes, edges);
    expect(res.isValid).toBe(true);
    expect(res.hasCycles).toBe(false);
    expect(res.sortedNodeIds).toEqual(['src_1', 'step_1', 'sink_1']);
    expect(res.cycleNodeIds).toEqual([]);
    expect(res.cycleEdgeIds).toEqual([]);
    expect(res.orphanNodeIds).toEqual([]);
    expect(res.errors).toEqual([]);
    expect(res.warnings).toEqual([]);
  });

  it('correctly sorts fan-out and fan-in branching DAGs with UnionByName', () => {
    const nodes: DagNode[] = [
      { id: 'src', data: { label: 'Source', type: 'SOURCE', spark_transform: 'ParquetSource' } },
      { id: 'bA', data: { label: 'Branch A', type: 'STEP', spark_transform: 'Filter' } },
      { id: 'bB', data: { label: 'Branch B', type: 'STEP', spark_transform: 'WithColumns' } },
      { id: 'merge', data: { label: 'Union', type: 'STEP', spark_transform: 'UnionByName' } },
      { id: 'sink', data: { label: 'Sink', type: 'SINK', spark_transform: 'DeltaSink' } },
    ];
    const edges: DagEdge[] = [
      { id: 'e1', source: 'src', target: 'bA' },
      { id: 'e2', source: 'src', target: 'bB' },
      { id: 'e3', source: 'bA', target: 'merge' },
      { id: 'e4', source: 'bB', target: 'merge' },
      { id: 'e5', source: 'merge', target: 'sink' },
    ];

    const res = validateDagTopology(nodes, edges);
    expect(res.isValid).toBe(true);
    expect(res.hasCycles).toBe(false);
    expect(res.sortedNodeIds[0]).toBe('src');
    expect(res.sortedNodeIds[res.sortedNodeIds.length - 1]).toBe('sink');
    expect(res.sortedNodeIds.indexOf('bA')).toBeLessThan(res.sortedNodeIds.indexOf('merge'));
    expect(res.sortedNodeIds.indexOf('bB')).toBeLessThan(res.sortedNodeIds.indexOf('merge'));
    expect(res.errors).toHaveLength(0);
  });

  it('detects a 2-node cycle (A -> B -> A) and isolates cycle nodes and edges', () => {
    const nodes: DagNode[] = [
      { id: 'node_1', data: { label: 'Step 1', type: 'STEP' } },
      { id: 'node_2', data: { label: 'Step 2', type: 'STEP' } },
    ];
    const edges: DagEdge[] = [
      { id: 'edge_1_2', source: 'node_1', target: 'node_2' },
      { id: 'edge_2_1', source: 'node_2', target: 'node_1' },
    ];

    const res = validateDagTopology(nodes, edges);
    expect(res.isValid).toBe(false);
    expect(res.hasCycles).toBe(true);
    expect(res.sortedNodeIds).toEqual([]);
    expect(res.cycleNodeIds.sort()).toEqual(['node_1', 'node_2']);
    expect(res.cycleEdgeIds.sort()).toEqual(['edge_1_2', 'edge_2_1']);
    expect(res.errors[0]).toContain('Cyclic dependency detected in pipeline graph');
    expect(res.cyclePathString).toContain('Step 1 (node_1)');
    expect(res.cyclePathString).toContain('Step 2 (node_2)');
  });

  it('detects a 3-node cycle (A -> B -> C -> A) and self-loop (A -> A)', () => {
    const res3 = validateDagTopology(
      [
        { id: 'n1', data: { label: 'Node 1', type: 'STEP' } },
        { id: 'n2', data: { label: 'Node 2', type: 'STEP' } },
        { id: 'n3', data: { label: 'Node 3', type: 'STEP' } },
      ],
      [
        { id: 'e1', source: 'n1', target: 'n2' },
        { id: 'e2', source: 'n2', target: 'n3' },
        { id: 'e3', source: 'n3', target: 'n1' },
      ]
    );
    expect(res3.isValid).toBe(false);
    expect(res3.hasCycles).toBe(true);
    expect(res3.cycleNodeIds.sort()).toEqual(['n1', 'n2', 'n3']);
    expect(res3.cycleEdgeIds.sort()).toEqual(['e1', 'e2', 'e3']);

    const selfRes = validateDagTopology(
      [{ id: 'loop_node', data: { label: 'Self Loop Step', type: 'STEP' } }],
      [{ id: 'e_self', source: 'loop_node', target: 'loop_node' }]
    );
    expect(selfRes.isValid).toBe(false);
    expect(selfRes.hasCycles).toBe(true);
    expect(selfRes.cycleNodeIds).toEqual(['loop_node']);
    expect(selfRes.cycleEdgeIds).toEqual(['e_self']);
  });

  it('detects cycles embedded within a larger acyclic graph', () => {
    const nodes: DagNode[] = [
      { id: 'src', data: { label: 'Source', type: 'SOURCE' } },
      { id: 'c1', data: { label: 'Cycle 1', type: 'STEP' } },
      { id: 'c2', data: { label: 'Cycle 2', type: 'STEP' } },
      { id: 'sink', data: { label: 'Sink', type: 'SINK' } },
    ];
    const edges: DagEdge[] = [
      { id: 'e_in', source: 'src', target: 'c1' },
      { id: 'e_c1', source: 'c1', target: 'c2' },
      { id: 'e_c2', source: 'c2', target: 'c1' },
      { id: 'e_out', source: 'c2', target: 'sink' },
    ];

    const res = validateDagTopology(nodes, edges);
    expect(res.isValid).toBe(false);
    expect(res.hasCycles).toBe(true);
    expect(res.cycleNodeIds).toContain('c1');
    expect(res.cycleNodeIds).toContain('c2');
    expect(res.cycleEdgeIds).toContain('e_c1');
    expect(res.cycleEdgeIds).toContain('e_c2');
    expect(res.errors[0]).toContain('Cyclic dependency detected');
  });

  it('identifies orphan disconnected nodes and ignores dangling edges', () => {
    const res = validateDagTopology(
      [
        { id: 'src', data: { label: 'Source', type: 'SOURCE' } },
        { id: 'sink', data: { label: 'Sink', type: 'SINK' } },
        { id: 'orphan_1', data: { label: 'Floating Transform', type: 'STEP' } },
        { id: 'orphan_2', data: { label: 'Forgotten Step', type: 'STEP' } },
      ],
      [
        { id: 'e1', source: 'src', target: 'sink' },
        { id: 'e_dangling', source: 'ghost_source', target: 'ghost_target' },
      ]
    );
    expect(res.isValid).toBe(false);
    expect(res.orphanNodeIds).toEqual(['orphan_1', 'orphan_2']);
    expect(res.errors[0]).toContain('"Floating Transform" (orphan_1)');
    expect(res.errors[0]).toContain('"Forgotten Step" (orphan_2)');
  });

  it('warns when missing source or sink on single-step canvas', () => {
    const res = validateDagTopology(
      [{ id: 'step_only', data: { label: 'Step Only', type: 'STEP' } }],
      []
    );
    expect(res.isValid).toBe(true);
    expect(res.warnings).toContain(
      'Pipeline has no SOURCE node. At least one SOURCE is required for execution.'
    );
    expect(res.warnings).toContain(
      'Pipeline has no SINK node. At least one SINK is recommended for output delivery.'
    );
  });
});

describe('validateDagTopology — Spark Join, Multi-Input, Watermark & Checkpoint Rules (A13)', () => {
  it('validates Join left/right handles and rejects missing or duplicate handle connections', () => {
    const leftSrc: DagNode = { id: 'l', data: { label: 'Left', type: 'SOURCE', spark_transform: 'BigQuerySource' } };
    const rightSrc: DagNode = { id: 'r', data: { label: 'Right', type: 'SOURCE', spark_transform: 'SpannerSource' } };
    const joinNode: DagNode = { id: 'j', data: { label: 'Join', type: 'STEP', spark_transform: 'Join' } };
    const sink: DagNode = { id: 's', data: { label: 'Sink', type: 'SINK', spark_transform: 'BigQuerySink' } };

    const missingRight = validateDagTopology(
      [leftSrc, joinNode, sink],
      [
        { id: 'e1', source: 'l', target: 'j', targetHandle: 'left' },
        { id: 'e2', source: 'j', target: 's' },
      ]
    );
    expect(missingRight.isValid).toBe(false);
    expect(missingRight.missingUpstreamNodeIds).toEqual(['j']);

    const validJoin = validateDagTopology(
      [leftSrc, rightSrc, joinNode, sink],
      [
        { id: 'e1', source: 'l', target: 'j', targetHandle: 'left' },
        { id: 'e2', source: 'r', target: 'j', targetHandle: 'right' },
        { id: 'e3', source: 'j', target: 's' },
      ]
    );
    expect(validJoin.isValid).toBe(true);
  });

  it('rejects unsupported stream-static outer joins and unwatermarked stream-stream outer joins', () => {
    const streamL: DagNode = { id: 'sl', data: { label: 'PubSub', type: 'SOURCE', spark_transform: 'PubSubSource' } };
    const staticR: DagNode = { id: 'sr', data: { label: 'BQ', type: 'SOURCE', spark_transform: 'BigQuerySource' } };
    const streamR: DagNode = { id: 'str_r', data: { label: 'Kafka', type: 'SOURCE', spark_transform: 'KafkaSource' } };
    const wmL: DagNode = { id: 'wml', data: { label: 'WM L', type: 'STEP', spark_transform: 'Watermark' } };
    const wmR: DagNode = { id: 'wmr', data: { label: 'WM R', type: 'STEP', spark_transform: 'Watermark' } };
    const sink: DagNode = {
      id: 'sink',
      data: {
        label: 'BigQuery',
        type: 'SINK',
        spark_transform: 'BigQuerySink',
        properties: { checkpointLocation: 'gs://ckpt/bq' },
      },
    };

    // Stream-static right_outer (right is static) -> rejected
    const badStreamStatic = validateDagTopology(
      [
        streamL,
        staticR,
        { id: 'j', data: { label: 'Join', type: 'STEP', spark_transform: 'Join', properties: { how: 'right_outer' } } },
        sink,
      ],
      [
        { id: 'e1', source: 'sl', target: 'j', targetHandle: 'left' },
        { id: 'e2', source: 'sr', target: 'j', targetHandle: 'right' },
        { id: 'e3', source: 'j', target: 'sink' },
      ]
    );
    expect(badStreamStatic.isValid).toBe(false);
    expect(badStreamStatic.errors[0]).toContain('Stream-static Join');

    // Stream-stream left_outer missing watermark -> rejected; with watermarks -> valid
    const joinLeftOuter: DagNode = {
      id: 'j2',
      data: { label: 'Stream Join', type: 'STEP', spark_transform: 'Join', properties: { joinType: 'left_outer' } },
    };
    const missingWm = validateDagTopology(
      [streamL, streamR, joinLeftOuter, sink],
      [
        { id: 'e1', source: 'sl', target: 'j2', targetHandle: 'left' },
        { id: 'e2', source: 'str_r', target: 'j2', targetHandle: 'right' },
        { id: 'e3', source: 'j2', target: 'sink' },
      ]
    );
    expect(missingWm.isValid).toBe(false);
    expect(missingWm.errors[0]).toContain('Watermark');

    const withWm = validateDagTopology(
      [streamL, wmL, streamR, wmR, joinLeftOuter, sink],
      [
        { id: 'e1', source: 'sl', target: 'wml' },
        { id: 'e2', source: 'wml', target: 'j2', targetHandle: 'left' },
        { id: 'e3', source: 'str_r', target: 'wmr' },
        { id: 'e4', source: 'wmr', target: 'j2', targetHandle: 'right' },
        { id: 'e5', source: 'j2', target: 'sink' },
      ]
    );
    expect(withWm.isValid).toBe(true);
  });

  it('validates UnionByName/SetOperation >= 2 inputs, streaming watermark warnings, and checkpoint options', () => {
    const kafka: DagNode = { id: 'k', data: { label: 'Kafka', type: 'SOURCE', spark_transform: 'KafkaSource' } };
    const agg: DagNode = { id: 'agg', data: { label: 'Agg', type: 'STEP', spark_transform: 'GroupByAgg' } };
    const sink: DagNode = { id: 'sink', data: { label: 'Parquet', type: 'SINK', spark_transform: 'ParquetSink' } };
    const edges: DagEdge[] = [
      { id: 'e1', source: 'k', target: 'agg' },
      { id: 'e2', source: 'agg', target: 'sink' },
    ];

    // Canvas 2-arg mode warns on missing Watermark and missing checkpoint without blocking canvas edits
    const canvasRes = validateDagTopology([kafka, agg, sink], edges);
    expect(canvasRes.isValid).toBe(true);
    expect(canvasRes.nodeWarnings?.agg).toHaveLength(1);
    expect(canvasRes.nodeWarnings?.sink).toHaveLength(1);

    // 3-arg jobConfig mode enforces non-empty checkpoint path
    const enforcedFail = validateDagTopology([kafka, agg, sink], edges, { jobConfig: {} });
    expect(enforcedFail.isValid).toBe(false);
    expect(enforcedFail.nodeErrors?.sink?.[0]).toContain('missing a checkpoint path');

    const enforcedPass = validateDagTopology([kafka, agg, sink], edges, {
      jobConfig: { checkpointRoot: 'gs://spark-checkpoints/prod' },
    });
    expect(enforcedPass.isValid).toBe(true);
  });
});
