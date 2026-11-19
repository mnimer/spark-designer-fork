/**
 * Unit tests for usePipelineGraph hook.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { usePipelineGraph, type PipelineNode } from './usePipelineGraph';

describe('usePipelineGraph', () => {
  it('initializes with empty nodes and edges arrays', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    expect(result.current.nodes).toEqual([]);
    expect(result.current.edges).toEqual([]);
    expect(result.current.hasPubSubSource).toBe(false);
  });

  it('allows direct SOURCE -> SINK connections for zero-transform Spark pipelines (A2)', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    const sparkNodes: PipelineNode[] = [
      {
        id: 'csv_src',
        type: 'pipeline',
        position: { x: 0, y: 0 },
        data: { label: 'CSV Source', type: 'SOURCE', iconName: 'FileText', spark_transform: 'CsvSource' },
      },
      {
        id: 'parquet_snk',
        type: 'pipeline',
        position: { x: 300, y: 0 },
        data: { label: 'Parquet Sink', type: 'SINK', iconName: 'Database', spark_transform: 'ParquetSink' },
      },
      {
        id: 'jdbc_src',
        type: 'pipeline',
        position: { x: 0, y: 160 },
        data: { label: 'JDBC Source', type: 'SOURCE', iconName: 'Database', spark_transform: 'JdbcSource' },
      },
      {
        id: 'bq_snk',
        type: 'pipeline',
        position: { x: 300, y: 160 },
        data: { label: 'BigQuery Sink', type: 'SINK', iconName: 'Database', spark_transform: 'BigQuerySink' },
      },
      {
        id: 'filter_step',
        type: 'pipeline',
        position: { x: 150, y: 80 },
        data: { label: 'Filter', type: 'STEP', iconName: 'Filter', spark_transform: 'Filter' },
      },
    ];

    act(() => {
      result.current.setNodes(sparkNodes);
    });

    // Direct SOURCE -> SINK connections (CSV -> Parquet, JDBC -> BigQuery) are valid
    expect(
      result.current.isValidConnection({
        source: 'csv_src',
        target: 'parquet_snk',
        sourceHandle: null,
        targetHandle: null,
      })
    ).toBe(true);
    expect(
      result.current.isValidConnection({
        source: 'jdbc_src',
        target: 'bq_snk',
        sourceHandle: null,
        targetHandle: null,
      })
    ).toBe(true);

    // SOURCE -> STEP and STEP -> SINK remain valid
    expect(
      result.current.isValidConnection({
        source: 'csv_src',
        target: 'filter_step',
        sourceHandle: null,
        targetHandle: null,
      })
    ).toBe(true);
    expect(
      result.current.isValidConnection({
        source: 'filter_step',
        target: 'parquet_snk',
        sourceHandle: 'valid',
        targetHandle: null,
      })
    ).toBe(true);

    // Connect CSV -> Parquet directly via onConnect and verify DAG validation
    act(() => {
      result.current.setNodes([sparkNodes[0], sparkNodes[1]]);
    });
    act(() => {
      result.current.onConnect({
        source: 'csv_src',
        target: 'parquet_snk',
        sourceHandle: null,
        targetHandle: null,
      });
    });

    expect(result.current.edges).toHaveLength(1);
    expect(result.current.dagValidation.isValid).toBe(true);
    expect(result.current.displayNodes.every((n) => !n.data.hasValidationError)).toBe(true);
    expect(result.current.displayNodes.every((n) => !n.data.validationError)).toBe(true);
  });

  it('rejects self-connections, connections targeting SOURCE, and connections originating from SINK', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    act(() => {
      result.current.setNodes([
        { id: 'src', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Source', type: 'SOURCE', iconName: 'Radio', spark_transform: 'KafkaSource' } },
        { id: 'step', type: 'pipeline', position: { x: 150, y: 0 }, data: { label: 'Step', type: 'STEP', iconName: 'Code', spark_transform: 'Select' } },
        { id: 'snk', type: 'pipeline', position: { x: 300, y: 0 }, data: { label: 'Sink', type: 'SINK', iconName: 'Database', spark_transform: 'IcebergSink' } },
      ]);
    });

    // Self-connections rejected
    expect(result.current.isValidConnection({ source: 'step', target: 'step', sourceHandle: 'valid', targetHandle: null })).toBe(false);
    expect(result.current.isValidConnection({ source: 'src', target: 'src', sourceHandle: null, targetHandle: null })).toBe(false);

    // Targeting a SOURCE rejected
    expect(result.current.isValidConnection({ source: 'step', target: 'src', sourceHandle: 'valid', targetHandle: null })).toBe(false);
    expect(result.current.isValidConnection({ source: 'src', target: 'src', sourceHandle: null, targetHandle: null })).toBe(false);
    expect(result.current.isValidConnection({ source: 'snk', target: 'src', sourceHandle: null, targetHandle: null })).toBe(false);

    // Originating from a SINK rejected
    expect(result.current.isValidConnection({ source: 'snk', target: 'step', sourceHandle: null, targetHandle: null })).toBe(false);
    expect(result.current.isValidConnection({ source: 'snk', target: 'snk', sourceHandle: null, targetHandle: null })).toBe(false);

    // Missing endpoints or unknown node IDs rejected
    expect(result.current.isValidConnection({ source: null, target: 'snk', sourceHandle: null, targetHandle: null })).toBe(false);
    expect(result.current.isValidConnection({ source: 'unknown', target: 'snk', sourceHandle: null, targetHandle: null })).toBe(false);

    // Programmatic onConnect also ignores invalid connections
    act(() => {
      result.current.onConnect({ source: 'snk', target: 'step', sourceHandle: null, targetHandle: null });
      result.current.onConnect({ source: 'step', target: 'src', sourceHandle: 'valid', targetHandle: null });
    });
    expect(result.current.edges).toHaveLength(0);
  });

  it('handleNodeLabelChange updates node label and flags pipeline for rebuild', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    act(() => {
      result.current.setNodes([
        { id: 'n1', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Old Label', type: 'SOURCE', iconName: 'Radio' } },
      ]);
    });

    act(() => {
      result.current.handleNodeLabelChange('n1', 'New Label');
    });

    expect(takeSnapshot).toHaveBeenCalled();
    expect(result.current.nodes[0].data.label).toBe('New Label');
    expect(setPipelineConfig).toHaveBeenCalled();
  });

  it('handleApplyProperties updates properties and special instructions', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    act(() => {
      result.current.setNodes([
        { id: 'n1', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Node', type: 'STEP', iconName: 'Clock' } },
      ]);
    });

    act(() => {
      result.current.handleApplyProperties('n1', {
        foo: 'bar',
        system_instructions: 'custom instruction',
      });
    });

    expect(result.current.nodes[0].data.properties).toEqual({ foo: 'bar' });
    expect(result.current.nodes[0].data.system_instructions).toBe('custom instruction');
  });

  it('deleteSelected removes selected node and any associated edges', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    act(() => {
      result.current.setNodes([
        { id: 'n1', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'N1', type: 'SOURCE', iconName: 'Radio' } },
        { id: 'n2', type: 'pipeline', position: { x: 100, y: 0 }, data: { label: 'N2', type: 'SINK', iconName: 'Radio' } },
      ]);
      result.current.setEdges([
        { id: 'e1', source: 'n1', target: 'n2' },
      ]);
      result.current.setSelectedNodeId('n1');
    });

    act(() => {
      result.current.deleteSelected();
    });

    expect(result.current.nodes.length).toBe(1);
    expect(result.current.nodes[0].id).toBe('n2');
    expect(result.current.edges.length).toBe(0);
    expect(result.current.selectedNodeId).toBeNull();
  });

  it('deleteSelected removes selected edge', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    act(() => {
      result.current.setEdges([
        { id: 'e1', source: 'n1', target: 'n2' },
      ]);
      result.current.setSelectedEdgeId('e1');
    });

    act(() => {
      result.current.deleteSelected();
    });

    expect(result.current.edges.length).toBe(0);
    expect(result.current.selectedEdgeId).toBeNull();
  });

  it('handleAddNode triggers auto layout and fitView when adding nodes with the + button', () => {
    vi.useFakeTimers();
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();
    const fitView = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig, fitView })
    );

    act(() => {
      result.current.handleAddNode('PubSub', 'SOURCE', 'Radio', 'PubSubSource');
    });

    expect(result.current.nodes).toHaveLength(1);
    expect(result.current.nodes[0].position).toEqual({ x: 80, y: 150 });

    act(() => {
      vi.advanceTimersByTime(150);
    });
    expect(fitView).toHaveBeenCalledWith({ duration: 300, padding: 1 });

    fitView.mockClear();
    act(() => {
      result.current.handleAddNode('Filter', 'STEP', 'Filter', 'Filter');
    });

    expect(result.current.nodes).toHaveLength(2);
    expect(result.current.edges).toHaveLength(1);
    expect(result.current.nodes[0].position).toEqual({ x: 80, y: 150 });
    expect(result.current.nodes[1].position).toEqual({ x: 380, y: 150 });

    act(() => {
      vi.advanceTimersByTime(150);
    });
    expect(fitView).toHaveBeenCalledWith({ duration: 300, padding: 1 });
    vi.useRealTimers();
  });

  it('keeps both nodes when adding two nodes back-to-back in the same act()', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    act(() => {
      result.current.handleAddNode('Source 1', 'SOURCE', 'Radio', 'CsvSource');
      result.current.handleAddNode('Source 2', 'SOURCE', 'Radio', 'ParquetSource');
    });

    expect(result.current.nodes).toHaveLength(2);
    expect(result.current.nodes.map((n) => n.data.label)).toEqual(['Source 1', 'Source 2']);
    expect(result.current.nodes[0].id).not.toBe(result.current.nodes[1].id);
  });

  it('decorates displayNodes with executionMode, supportedReadModes, isCached, isForeachBatchSink, and exposes pipelineMode (A4)', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    // 1. Batch fan-out pipeline: CsvSource -> WithColumns -> (ParquetSink + BigQuerySink)
    act(() => {
      result.current.setNodes([
        { id: 'csv', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'CSV', type: 'SOURCE', iconName: 'FileText', spark_transform: 'CsvSource' } },
        { id: 'enrich', type: 'pipeline', position: { x: 200, y: 0 }, data: { label: 'WithColumns', type: 'STEP', iconName: 'Code', spark_transform: 'WithColumns' } },
        { id: 'snk1', type: 'pipeline', position: { x: 400, y: -50 }, data: { label: 'Parquet', type: 'SINK', iconName: 'Database', spark_transform: 'ParquetSink' } },
        { id: 'snk2', type: 'pipeline', position: { x: 400, y: 50 }, data: { label: 'BigQuery', type: 'SINK', iconName: 'Database', spark_transform: 'BigQuerySink' } },
      ]);
      result.current.setEdges([
        { id: 'e1', source: 'csv', target: 'enrich' },
        { id: 'e2', source: 'enrich', sourceHandle: 'valid', target: 'snk1' },
        { id: 'e3', source: 'enrich', sourceHandle: 'valid', target: 'snk2' },
      ]);
    });

    expect(result.current.pipelineMode).toBe('batch');
    const csvDisplay = result.current.displayNodes.find((n) => n.id === 'csv')!;
    const enrichDisplay = result.current.displayNodes.find((n) => n.id === 'enrich')!;
    const snk1Display = result.current.displayNodes.find((n) => n.id === 'snk1')!;
    expect(csvDisplay.data.executionMode).toBe('batch');
    expect(csvDisplay.data.supportedReadModes).toEqual(['batch', 'streaming']);
    expect(enrichDisplay.data.executionMode).toBe('batch');
    expect(enrichDisplay.data.isCached).toBe(true);
    expect(snk1Display.data.executionMode).toBe('batch');
    expect(snk1Display.data.isForeachBatchSink).toBe(false);

    // 2. Switch CsvSource readMode to streaming and write to SpannerSink (foreachBatch)
    act(() => {
      result.current.setNodes([
        { id: 'kafka', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Kafka', type: 'SOURCE', iconName: 'Radio', spark_transform: 'KafkaSource' } },
        { id: 'spanner', type: 'pipeline', position: { x: 300, y: 0 }, data: { label: 'Spanner', type: 'SINK', iconName: 'Database', spark_transform: 'SpannerSink' } },
      ]);
      result.current.setEdges([
        { id: 'e-ks', source: 'kafka', target: 'spanner' },
      ]);
    });

    expect(result.current.pipelineMode).toBe('streaming');
    const kafkaDisplay = result.current.displayNodes.find((n) => n.id === 'kafka')!;
    const spannerDisplay = result.current.displayNodes.find((n) => n.id === 'spanner')!;
    expect(kafkaDisplay.data.executionMode).toBe('streaming');
    expect(spannerDisplay.data.executionMode).toBe('streaming');
    expect(spannerDisplay.data.isForeachBatchSink).toBe(true);
  });

  it('restricts outgoing error handle connections to error-capable steps only (A5)', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    act(() => {
      result.current.setNodes([
        { id: 'select', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Select', type: 'STEP', iconName: 'Code', spark_transform: 'Select' } },
        { id: 'parse', type: 'pipeline', position: { x: 0, y: 120 }, data: { label: 'ParsePayload', type: 'STEP', iconName: 'FileCode', spark_transform: 'ParsePayload' } },
        { id: 'dlq', type: 'pipeline', position: { x: 300, y: 120 }, data: { label: 'DLQ Console', type: 'SINK', iconName: 'Terminal', spark_transform: 'ConsoleSink' } },
      ]);
    });

    expect(result.current.isValidConnection({ source: 'select', sourceHandle: 'error', target: 'dlq', targetHandle: null })).toBe(false);
    expect(result.current.isValidConnection({ source: 'select', sourceHandle: 'valid', target: 'dlq', targetHandle: null })).toBe(true);
    expect(result.current.isValidConnection({ source: 'parse', sourceHandle: 'error', target: 'dlq', targetHandle: null })).toBe(true);
  });

  it('enforces Join left/right single-edge handles, N-input multi-edges, and single-input step/sink arity (A6)', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    act(() => {
      result.current.setNodes([
        { id: 's1', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Orders', type: 'SOURCE', iconName: 'Database', spark_transform: 'BigQuerySource' } },
        { id: 's2', type: 'pipeline', position: { x: 0, y: 100 }, data: { label: 'Customers', type: 'SOURCE', iconName: 'Database', spark_transform: 'CloudSqlSource' } },
        { id: 's3', type: 'pipeline', position: { x: 0, y: 200 }, data: { label: 'Archive', type: 'SOURCE', iconName: 'FileText', spark_transform: 'ParquetSource' } },
        { id: 'join', type: 'pipeline', position: { x: 250, y: 50 }, data: { label: 'Join', type: 'STEP', iconName: 'GitMerge', spark_transform: 'Join' } },
        { id: 'union', type: 'pipeline', position: { x: 250, y: 180 }, data: { label: 'Union', type: 'STEP', iconName: 'Layers', spark_transform: 'UnionByName' } },
        { id: 'sql', type: 'pipeline', position: { x: 250, y: 280 }, data: { label: 'SQL', type: 'STEP', iconName: 'Code', spark_transform: 'SparkSqlQuery' } },
        { id: 'select', type: 'pipeline', position: { x: 500, y: 50 }, data: { label: 'Select', type: 'STEP', iconName: 'Code', spark_transform: 'Select' } },
        { id: 'sink', type: 'pipeline', position: { x: 750, y: 50 }, data: { label: 'Iceberg', type: 'SINK', iconName: 'Database', spark_transform: 'IcebergSink' } },
      ]);
    });

    // Join requires targetHandle 'left' or 'right' and allows 1 edge per handle
    expect(result.current.isValidConnection({ source: 's1', target: 'join', sourceHandle: null, targetHandle: null })).toBe(false);
    expect(result.current.isValidConnection({ source: 's1', target: 'join', sourceHandle: null, targetHandle: 'left' })).toBe(true);
    expect(result.current.isValidConnection({ source: 's2', target: 'join', sourceHandle: null, targetHandle: 'right' })).toBe(true);

    act(() => {
      result.current.onConnect({ source: 's1', target: 'join', sourceHandle: null, targetHandle: 'left' });
      result.current.onConnect({ source: 's2', target: 'join', sourceHandle: null, targetHandle: 'right' });
    });
    expect(result.current.edges.filter((e) => e.target === 'join')).toHaveLength(2);
    // Rejects a second connection to an occupied Join handle
    expect(result.current.isValidConnection({ source: 's3', target: 'join', sourceHandle: null, targetHandle: 'left' })).toBe(false);
    expect(result.current.isValidConnection({ source: 's3', target: 'join', sourceHandle: null, targetHandle: 'right' })).toBe(false);

    // UnionByName and SparkSqlQuery accept N ordered inputs (3 upstream branches)
    act(() => {
      result.current.onConnect({ source: 's1', target: 'union', sourceHandle: null, targetHandle: null });
      result.current.onConnect({ source: 's2', target: 'union', sourceHandle: null, targetHandle: null });
      result.current.onConnect({ source: 's3', target: 'union', sourceHandle: null, targetHandle: null });
      result.current.onConnect({ source: 's1', target: 'sql', sourceHandle: null, targetHandle: null });
      result.current.onConnect({ source: 's2', target: 'sql', sourceHandle: null, targetHandle: null });
      result.current.onConnect({ source: 's3', target: 'sql', sourceHandle: null, targetHandle: null });
    });
    expect(result.current.edges.filter((e) => e.target === 'union')).toHaveLength(3);
    expect(result.current.edges.filter((e) => e.target === 'sql')).toHaveLength(3);

    // Single-input step (Select) and sink (IcebergSink) accept 1 input and reject a 2nd input
    act(() => {
      result.current.onConnect({ source: 'join', sourceHandle: 'valid', target: 'select', targetHandle: null });
      result.current.onConnect({ source: 'union', sourceHandle: 'valid', target: 'select', targetHandle: null });
      result.current.onConnect({ source: 'select', sourceHandle: 'valid', target: 'sink', targetHandle: null });
      result.current.onConnect({ source: 'sql', sourceHandle: 'valid', target: 'sink', targetHandle: null });
    });
    expect(result.current.edges.filter((e) => e.target === 'select')).toHaveLength(1);
    expect(result.current.edges.filter((e) => e.target === 'sink')).toHaveLength(1);
    expect(result.current.isValidConnection({ source: 'union', sourceHandle: 'valid', target: 'select', targetHandle: null })).toBe(false);
    expect(result.current.isValidConnection({ source: 'sql', sourceHandle: 'valid', target: 'sink', targetHandle: null })).toBe(false);
  });

  it('validates dynamic rule and else sourceHandles on ConditionalSplit nodes and marks batch fan-out cached (A7)', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    act(() => {
      result.current.setNodes([
        { id: 'src', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'Orders', type: 'SOURCE', iconName: 'Database', spark_transform: 'BigQuerySource' } },
        {
          id: 'split',
          type: 'pipeline',
          position: { x: 250, y: 0 },
          data: {
            label: 'Router',
            type: 'STEP',
            iconName: 'GitBranch',
            spark_transform: 'ConditionalSplit',
            properties: {
              rules: [
                { name: 'vip', condition: 'total >= 1000' },
                { name: 'standard', condition: 'total < 1000' },
              ],
            },
          },
        },
        { id: 'snk_vip', type: 'pipeline', position: { x: 500, y: -60 }, data: { label: 'VIP Iceberg', type: 'SINK', iconName: 'Database', spark_transform: 'IcebergSink' } },
        { id: 'snk_else', type: 'pipeline', position: { x: 500, y: 60 }, data: { label: 'Fallback Parquet', type: 'SINK', iconName: 'Database', spark_transform: 'ParquetSink' } },
      ]);
    });

    expect(result.current.isValidConnection({ source: 'split', sourceHandle: 'vip', target: 'snk_vip', targetHandle: null })).toBe(true);
    expect(result.current.isValidConnection({ source: 'split', sourceHandle: 'else', target: 'snk_else', targetHandle: null })).toBe(true);
    expect(result.current.isValidConnection({ source: 'split', sourceHandle: 'unknown_rule', target: 'snk_vip', targetHandle: null })).toBe(false);

    act(() => {
      result.current.onConnect({ source: 'src', target: 'split', sourceHandle: null, targetHandle: null });
      result.current.onConnect({ source: 'split', sourceHandle: 'vip', target: 'snk_vip', targetHandle: null });
      result.current.onConnect({ source: 'split', sourceHandle: 'else', target: 'snk_else', targetHandle: null });
    });

    const splitDisplay = result.current.displayNodes.find((n) => n.id === 'split')!;
    expect(splitDisplay.data.isCached).toBe(true);
  });

  it('enforces explicit left and right single-edge target handles on MergeCdcScd nodes (EXT4)', () => {
    const takeSnapshot = vi.fn();
    const setPipelineConfig = vi.fn();

    const { result } = renderHook(() =>
      usePipelineGraph({ takeSnapshot, setPipelineConfig })
    );

    act(() => {
      result.current.setNodes([
        { id: 'cdc', type: 'pipeline', position: { x: 0, y: 0 }, data: { label: 'CDC Feed', type: 'SOURCE', iconName: 'Cable', spark_transform: 'KafkaSource', properties: { readMode: 'batch' } } },
        { id: 'snap', type: 'pipeline', position: { x: 0, y: 120 }, data: { label: 'Iceberg Target', type: 'SOURCE', iconName: 'Database', spark_transform: 'IcebergSource' } },
        { id: 'extra', type: 'pipeline', position: { x: 0, y: 240 }, data: { label: 'Extra', type: 'SOURCE', iconName: 'FileText', spark_transform: 'ParquetSource' } },
        { id: 'merge', type: 'pipeline', position: { x: 260, y: 60 }, data: { label: 'Merge CDC / SCD', type: 'STEP', iconName: 'GitMerge', spark_transform: 'MergeCdcScd' } },
        { id: 'sink', type: 'pipeline', position: { x: 520, y: 60 }, data: { label: 'Iceberg Sink', type: 'SINK', iconName: 'Database', spark_transform: 'IcebergSink' } },
      ]);
    });

    expect(result.current.isValidConnection({ source: 'cdc', target: 'merge', sourceHandle: null, targetHandle: null })).toBe(false);
    expect(result.current.isValidConnection({ source: 'cdc', target: 'merge', sourceHandle: null, targetHandle: 'left' })).toBe(true);
    expect(result.current.isValidConnection({ source: 'snap', target: 'merge', sourceHandle: null, targetHandle: 'right' })).toBe(true);

    act(() => {
      result.current.onConnect({ source: 'cdc', target: 'merge', sourceHandle: null, targetHandle: 'left' });
      result.current.onConnect({ source: 'snap', target: 'merge', sourceHandle: null, targetHandle: 'right' });
      result.current.onConnect({ source: 'merge', sourceHandle: 'valid', target: 'sink', targetHandle: null });
    });

    expect(result.current.edges.filter((e) => e.target === 'merge')).toHaveLength(2);
    expect(result.current.isValidConnection({ source: 'extra', target: 'merge', sourceHandle: null, targetHandle: 'left' })).toBe(false);
    expect(result.current.isValidConnection({ source: 'extra', target: 'merge', sourceHandle: null, targetHandle: 'right' })).toBe(false);
  });
});
