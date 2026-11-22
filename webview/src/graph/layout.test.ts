/**
 * Unit tests for layout.ts pure graph positioning algorithm
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect } from 'vitest';
import type { Edge } from 'reactflow';
import type { PipelineNode } from '../hooks/usePipelineGraph';
import {
  computeLayoutedNodes,
  START_X,
  START_Y,
  COL_WIDTH,
  ROW_HEIGHT,
} from './layout';

describe('computeLayoutedNodes', () => {
  it('linear chain A->B->C gets increasing x by column', () => {
    const nodes: PipelineNode[] = [
      { id: 'nodeA', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Node A', type: 'SOURCE', iconName: 'Radio' } },
      { id: 'nodeB', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Node B', type: 'STEP', iconName: 'Clock' } },
      { id: 'nodeC', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Node C', type: 'SINK', iconName: 'Database' } },
    ];

    const edges: Edge[] = [
      { id: 'e-AB', source: 'nodeA', target: 'nodeB' },
      { id: 'e-BC', source: 'nodeB', target: 'nodeC' },
    ];

    const result = computeLayoutedNodes(nodes, edges);

    const nodeA = result.find((n) => n.id === 'nodeA')!;
    const nodeB = result.find((n) => n.id === 'nodeB')!;
    const nodeC = result.find((n) => n.id === 'nodeC')!;

    expect(nodeA.position.x).toBe(START_X);
    expect(nodeB.position.x).toBe(START_X + COL_WIDTH);
    expect(nodeC.position.x).toBe(START_X + 2 * COL_WIDTH);

    expect(nodeA.position.x).toBeLessThan(nodeB.position.x);
    expect(nodeB.position.x).toBeLessThan(nodeC.position.x);

    expect(nodeA.position.y).toBe(START_Y);
    expect(nodeB.position.y).toBe(START_Y);
    expect(nodeC.position.y).toBe(START_Y);
  });

  it('two sources feeding one sink => both sources rank 0 and are vertically centred around the same y', () => {
    const nodes: PipelineNode[] = [
      { id: 'src1', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Source 1', type: 'SOURCE', iconName: 'Radio' } },
      { id: 'src2', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Source 2', type: 'SOURCE', iconName: 'Radio' } },
      { id: 'sink', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Sink', type: 'SINK', iconName: 'Database' } },
    ];

    const edges: Edge[] = [
      { id: 'e-s1-sink', source: 'src1', target: 'sink' },
      { id: 'e-s2-sink', source: 'src2', target: 'sink' },
    ];

    const result = computeLayoutedNodes(nodes, edges);

    const src1 = result.find((n) => n.id === 'src1')!;
    const src2 = result.find((n) => n.id === 'src2')!;
    const sink = result.find((n) => n.id === 'sink')!;

    // Both sources get rank 0
    expect(src1.position.x).toBe(START_X);
    expect(src2.position.x).toBe(START_X);
    expect(sink.position.x).toBe(START_X + COL_WIDTH);

    // Two sources vertically centered around START_Y
    expect(src1.position.y).toBe(START_Y - ROW_HEIGHT / 2);
    expect(src2.position.y).toBe(START_Y + ROW_HEIGHT / 2);
    expect((src1.position.y + src2.position.y) / 2).toBe(START_Y);

    // Sink is in its own column, centered at START_Y
    expect(sink.position.y).toBe(START_Y);
  });

  it('a cycle falls back to SOURCE/STEP/SINK ranks', () => {
    const nodes: PipelineNode[] = [
      { id: 'src', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Source', type: 'SOURCE', iconName: 'Radio' } },
      { id: 'step', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Step', type: 'STEP', iconName: 'Clock' } },
      { id: 'sink', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Sink', type: 'SINK', iconName: 'Database' } },
    ];

    // Cycle: src -> step -> sink -> src
    const edges: Edge[] = [
      { id: 'e-1', source: 'src', target: 'step' },
      { id: 'e-2', source: 'step', target: 'sink' },
      { id: 'e-3', source: 'sink', target: 'src' },
    ];

    const result = computeLayoutedNodes(nodes, edges);

    const src = result.find((n) => n.id === 'src')!;
    const step = result.find((n) => n.id === 'step')!;
    const sink = result.find((n) => n.id === 'sink')!;

    // Fallback: SOURCE=0, STEP=1, SINK=2
    expect(src.position.x).toBe(START_X);
    expect(step.position.x).toBe(START_X + COL_WIDTH);
    expect(sink.position.x).toBe(START_X + 2 * COL_WIDTH);

    expect(src.position.y).toBe(START_Y);
    expect(step.position.y).toBe(START_Y);
    expect(sink.position.y).toBe(START_Y);
  });

  it('disconnected single node', () => {
    const nodes: PipelineNode[] = [
      { id: 'single', type: 'pipeline', position: { x: 999, y: 999 }, data: { label: 'Lone Source', type: 'SOURCE', iconName: 'Radio' } },
    ];

    const result = computeLayoutedNodes(nodes, []);

    expect(result).toHaveLength(1);
    expect(result[0].position).toEqual({
      x: START_X,
      y: START_Y,
    });
  });

  it('orders Join left-handle inputs above right-handle lookup inputs regardless of array order', () => {
    const nodes: PipelineNode[] = [
      {
        id: 'source-lookup-right',
        type: 'pipeline',
        position: { x: 0, y: 0 },
        data: { label: 'Currency Lookup', type: 'SOURCE', iconName: 'Database', spark_transform: 'CloudSqlSource' },
      },
      {
        id: 'source-orders-left',
        type: 'pipeline',
        position: { x: 0, y: 0 },
        data: { label: 'Orders Stream', type: 'SOURCE', iconName: 'Radio', spark_transform: 'KafkaSource' },
      },
      {
        id: 'step-broadcast-join',
        type: 'pipeline',
        position: { x: 0, y: 0 },
        data: {
          label: 'Join Lookup',
          type: 'STEP',
          iconName: 'GitMerge',
          spark_transform: 'Join',
          properties: { joinType: 'left_outer', broadcastRight: true },
        },
      },
      {
        id: 'sink-iceberg',
        type: 'pipeline',
        position: { x: 0, y: 0 },
        data: { label: 'Iceberg Sink', type: 'SINK', iconName: 'Database', spark_transform: 'IcebergSink' },
      },
    ];

    const edges: Edge[] = [
      { id: 'e-right', source: 'source-lookup-right', target: 'step-broadcast-join', targetHandle: 'right' },
      { id: 'e-left', source: 'source-orders-left', target: 'step-broadcast-join', targetHandle: 'left' },
      { id: 'e-out', source: 'step-broadcast-join', sourceHandle: 'valid', target: 'sink-iceberg' },
    ];

    const result = computeLayoutedNodes(nodes, edges);
    const byId = Object.fromEntries(result.map((n) => [n.id, n]));

    expect(byId['source-orders-left'].position.x).toBe(START_X);
    expect(byId['source-lookup-right'].position.x).toBe(START_X);
    expect(byId['source-orders-left'].position.y).toBe(START_Y - ROW_HEIGHT / 2);
    expect(byId['source-lookup-right'].position.y).toBe(START_Y + ROW_HEIGHT / 2);
    expect(byId['step-broadcast-join'].position).toEqual({
      x: START_X + COL_WIDTH,
      y: START_Y,
    });
    expect(byId['sink-iceberg'].position).toEqual({
      x: START_X + 2 * COL_WIDTH,
      y: START_Y,
    });
  });

  it('lays out multi-hop broadcast lookup Join branches and widens columns on the 8pt grid for long labels', () => {
    const nodes: PipelineNode[] = [
      {
        id: 'source-fx-rates',
        type: 'pipeline',
        position: { x: 0, y: 0 },
        data: { label: 'FX Rates Parquet', type: 'SOURCE', iconName: 'FileText', spark_transform: 'ParquetSource' },
      },
      {
        id: 'source-orders',
        type: 'pipeline',
        position: { x: 0, y: 0 },
        data: { label: 'Orders BigQuery', type: 'SOURCE', iconName: 'Database', spark_transform: 'BigQuerySource' },
      },
      {
        id: 'step-filter-rates',
        type: 'pipeline',
        position: { x: 0, y: 0 },
        data: { label: 'Active Currency Rates', type: 'STEP', iconName: 'Filter', spark_transform: 'Filter' },
      },
      {
        id: 'step-broadcast-join',
        type: 'pipeline',
        position: { x: 0, y: 0 },
        data: {
          label: 'Broadcast Join Orders With Active Currency Exchange Rates',
          type: 'STEP',
          iconName: 'GitMerge',
          spark_transform: 'Join',
          properties: { joinType: 'left_outer', broadcastRight: true },
        },
      },
      {
        id: 'sink-curated',
        type: 'pipeline',
        position: { x: 0, y: 0 },
        data: { label: 'Curated Orders', type: 'SINK', iconName: 'Database', spark_transform: 'BigQuerySink' },
      },
      {
        id: 'sink-audit',
        type: 'pipeline',
        position: { x: 0, y: 0 },
        data: { label: 'Audit Parquet', type: 'SINK', iconName: 'FileText', spark_transform: 'ParquetSink' },
      },
    ];

    const edges: Edge[] = [
      { id: 'e1', source: 'source-orders', target: 'step-broadcast-join', targetHandle: 'left' },
      { id: 'e2', source: 'source-fx-rates', target: 'step-filter-rates' },
      { id: 'e3', source: 'step-filter-rates', sourceHandle: 'valid', target: 'step-broadcast-join', targetHandle: 'right' },
      { id: 'e4', source: 'step-broadcast-join', sourceHandle: 'valid', target: 'sink-curated' },
      { id: 'e5', source: 'step-broadcast-join', sourceHandle: 'valid', target: 'sink-audit' },
    ];

    const result = computeLayoutedNodes(nodes, edges);
    const byId = Object.fromEntries(result.map((n) => [n.id, n]));

    // 1. Rank 0 places left-branch Orders above right-branch FX Rates even when FX Rates came first
    expect(byId['source-orders'].position.x).toBe(START_X);
    expect(byId['source-fx-rates'].position.x).toBe(START_X);
    expect(byId['source-orders'].position.y).toBe(START_Y - ROW_HEIGHT / 2);
    expect(byId['source-fx-rates'].position.y).toBe(START_Y + ROW_HEIGHT / 2);

    // 2. Intermediate filter step sits in rank 1 and Broadcast Join advances to rank 2
    expect(byId['step-filter-rates'].position.x).toBeGreaterThan(byId['source-fx-rates'].position.x);
    expect(byId['step-filter-rates'].position.y).toBe(START_Y);
    expect(byId['step-broadcast-join'].position.x).toBeGreaterThan(byId['step-filter-rates'].position.x);
    expect(byId['step-broadcast-join'].position.y).toBe(START_Y);

    // 3. Long Broadcast Join label widens rank 2 on the 8pt grid so downstream sinks never overlap
    const joinToSinkStep = byId['sink-curated'].position.x - byId['step-broadcast-join'].position.x;
    expect(joinToSinkStep).toBeGreaterThanOrEqual(400);
    expect(joinToSinkStep % 8).toBe(0);
    expect(byId['sink-audit'].position.x).toBe(byId['sink-curated'].position.x);
    expect((byId['sink-curated'].position.y + byId['sink-audit'].position.y) / 2).toBe(START_Y);
  });
});
