/**
 * Unit tests for PipelineNode component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { PipelineNode } from './PipelineNode';

// Mock React Flow handles
vi.mock('reactflow', () => ({
  Handle: ({
    type,
    position,
    id,
    title,
    style,
  }: {
    type: string;
    position: string;
    id?: string;
    title?: string;
    style?: React.CSSProperties;
  }) => (
    <div data-testid={`handle-${type}-${position}${id ? `-${id}` : ''}`} title={title} style={style} />
  ),
  Position: {
    Left: 'left',
    Right: 'right',
    Top: 'top'
  },
  useReactFlow: () => ({
    setNodes: vi.fn()
  })
}));

describe('PipelineNode', () => {
  const mockData = {
    label: 'Test Node',
    type: 'SOURCE',
    iconName: 'Radio'
  };

  it('renders label and type with correct color styling for SOURCE', () => {
    const { container } = render(<PipelineNode id="node-1" data={mockData} />);

    expect(screen.getByText('Test Node')).toBeInTheDocument();
    const typeElement = screen.getByText('SOURCE');
    expect(typeElement).toBeInTheDocument();
    expect(typeElement).toHaveClass('text-emerald-400');
    expect(container.firstChild).toHaveClass('border-emerald-500');
  });

  it('renders correct border and text color for STEP', () => {
    const stepData = { ...mockData, type: 'STEP' };
    const { container } = render(<PipelineNode id="node-1" data={stepData} />);

    const typeElement = screen.getByText('STEP');
    expect(typeElement).toHaveClass('text-blue-400');
    expect(container.firstChild).toHaveClass('border-blue-500');
  });

  it('renders correct border and text color for SINK', () => {
    const sinkData = { ...mockData, type: 'SINK' };
    const { container } = render(<PipelineNode id="node-1" data={sinkData} />);

    const typeElement = screen.getByText('SINK');
    expect(typeElement).toHaveClass('text-red-400');
    expect(container.firstChild).toHaveClass('border-red-500');
  });

  it('renders selected styling for SOURCE, STEP, and SINK nodes', () => {
    const { container: sourceContainer } = render(<PipelineNode id="node-1" data={mockData} selected={true} />);
    expect(sourceContainer.firstChild).toHaveClass('border-emerald-400');
    expect(sourceContainer.firstChild).toHaveClass('ring-2');

    const { container: stepContainer } = render(<PipelineNode id="node-2" data={{ ...mockData, type: 'STEP' }} selected={true} />);
    expect(stepContainer.firstChild).toHaveClass('border-blue-400');
    expect(stepContainer.firstChild).toHaveClass('ring-2');

    const { container: sinkContainer } = render(<PipelineNode id="node-3" data={{ ...mockData, type: 'SINK' }} selected={true} />);
    expect(sinkContainer.firstChild).toHaveClass('border-red-400');
    expect(sinkContainer.firstChild).toHaveClass('ring-2');
  });

  it('renders target handle if not source', () => {
    const stepData = { ...mockData, type: 'STEP' };
    render(<PipelineNode id="node-1" data={stepData} />);

    expect(screen.getByTestId('handle-target-left')).toBeInTheDocument();
  });

  it('does not render target handle if source', () => {
    render(<PipelineNode id="node-1" data={mockData} />);

    expect(screen.queryByTestId('handle-target-left')).not.toBeInTheDocument();
  });

  it('renders source handle for source node', () => {
    render(<PipelineNode id="node-1" data={mockData} />); // mockData is SOURCE

    expect(screen.getByTestId('handle-source-right')).toBeInTheDocument();
  });

  it('renders single centered valid output handle without error handle on structural steps (A5)', () => {
    const structuralTransforms = [
      undefined,
      'Select',
      'WithColumns',
      'RenameDropColumns',
      'Explode',
      'HandleNulls',
      'OrderBy',
      'LimitSample',
      'GroupByAgg',
      'Watermark',
      'RepartitionCoalesce',
      'CachePersist',
      'FeaturePrep',
    ];

    for (const transform of structuralTransforms) {
      const { unmount } = render(
        <PipelineNode
          id="node-struct"
          data={{
            ...mockData,
            type: 'STEP',
            label: transform || 'Structural Step',
            ...(transform ? { spark_transform: transform } : {}),
          }}
        />
      );

      const validHandle = screen.getByTestId('handle-source-right-valid');
      expect(validHandle).toBeInTheDocument();
      expect(validHandle).toHaveStyle({ top: '50%' });
      expect(screen.queryByTestId('handle-source-right-error')).not.toBeInTheDocument();
      unmount();
    }
  });

  it('renders both valid (30%) and error DLQ (70%) handles ONLY on error-capable steps (A5)', () => {
    const errorCapableTransforms = [
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

    for (const transform of errorCapableTransforms) {
      const { unmount } = render(
        <PipelineNode
          id={`node-${transform}`}
          data={{
            ...mockData,
            type: 'STEP',
            label: transform,
            spark_transform: transform,
          }}
        />
      );

      const validHandle = screen.getByTestId('handle-source-right-valid');
      const errorHandle = screen.getByTestId('handle-source-right-error');
      expect(validHandle).toBeInTheDocument();
      expect(validHandle).toHaveStyle({ top: '30%' });
      expect(errorHandle).toBeInTheDocument();
      expect(errorHandle).toHaveStyle({ top: '70%' });
      unmount();
    }

    // Explicit data.errorOutput === true override also exposes DLQ handle
    render(
      <PipelineNode
        id="node-explicit-err"
        data={{
          ...mockData,
          type: 'STEP',
          label: 'Custom Override',
          spark_transform: 'Select',
          errorOutput: true,
        }}
      />
    );
    expect(screen.getByTestId('handle-source-right-error')).toBeInTheDocument();
  });

  it('renders target handle and no source handle for Log Message sink node', () => {
    const logSinkData = { ...mockData, type: 'SINK', label: 'Log Message' };
    render(<PipelineNode id="node-1" data={logSinkData} />);

    expect(screen.getByTestId('handle-target-left')).toBeInTheDocument();
    expect(screen.queryByTestId('handle-source-right')).not.toBeInTheDocument();
    expect(screen.queryByTestId('handle-source-right-valid')).not.toBeInTheDocument();
    expect(screen.queryByTestId('handle-source-right-error')).not.toBeInTheDocument();
  });

  it('does not render source handle if sink', () => {
    const sinkData = { ...mockData, type: 'SINK' };
    render(<PipelineNode id="node-1" data={sinkData} />);

    expect(screen.queryByTestId('handle-source-right')).not.toBeInTheDocument();
    expect(screen.queryByTestId('handle-source-right-valid')).not.toBeInTheDocument();
    expect(screen.queryByTestId('handle-source-right-error')).not.toBeInTheDocument();
  });

  it('does not render a side_input handle or label for steps, sources, or sinks', () => {
    const { unmount: unmountStep } = render(
      <PipelineNode id="node-1" data={{ ...mockData, type: 'STEP', spark_transform: 'Select' }} />
    );
    expect(screen.queryByTestId('handle-target-top-side_input')).not.toBeInTheDocument();
    expect(screen.queryByTestId('side-input-handle-label')).not.toBeInTheDocument();
    unmountStep();

    const { unmount: unmountSource } = render(
      <PipelineNode id="node-2" data={{ ...mockData, type: 'SOURCE', spark_transform: 'CsvSource' }} />
    );
    expect(screen.queryByTestId('handle-target-top-side_input')).not.toBeInTheDocument();
    expect(screen.queryByTestId('side-input-handle-label')).not.toBeInTheDocument();
    unmountSource();

    render(<PipelineNode id="node-3" data={{ ...mockData, type: 'SINK', spark_transform: 'ParquetSink' }} />);
    expect(screen.queryByTestId('handle-target-top-side_input')).not.toBeInTheDocument();
    expect(screen.queryByTestId('side-input-handle-label')).not.toBeInTheDocument();
  });

  it('opens text input on label double-click and commits on Enter', () => {
    const handleLabelChange = vi.fn();
    const dataWithHandler = {
      ...mockData,
      onLabelChange: handleLabelChange
    };

    render(<PipelineNode id="node-1" data={dataWithHandler} />);

    const labelSpan = screen.getByText('Test Node');
    fireEvent.doubleClick(labelSpan);

    const input = screen.getByRole('textbox', { name: 'Edit node label' }) as HTMLInputElement;
    expect(input).toBeInTheDocument();
    expect(input.value).toBe('Test Node');

    fireEvent.change(input, { target: { value: 'Renamed Node' } });
    fireEvent.keyDown(input, { key: 'Enter' });

    expect(handleLabelChange).toHaveBeenCalledWith('node-1', 'Renamed Node');
    expect(screen.queryByRole('textbox', { name: 'Edit node label' })).not.toBeInTheDocument();
  });

  it('cancels edit on Escape key and retains original label', () => {
    const handleLabelChange = vi.fn();
    const dataWithHandler = {
      ...mockData,
      onLabelChange: handleLabelChange
    };

    render(<PipelineNode id="node-1" data={dataWithHandler} />);

    const labelSpan = screen.getByText('Test Node');
    fireEvent.doubleClick(labelSpan);

    const input = screen.getByRole('textbox', { name: 'Edit node label' }) as HTMLInputElement;
    fireEvent.change(input, { target: { value: 'Renamed Node' } });
    fireEvent.keyDown(input, { key: 'Escape' });

    expect(handleLabelChange).not.toHaveBeenCalled();
    expect(screen.getByText('Test Node')).toBeInTheDocument();
  });

  it('commits edit on blur', () => {
    const handleLabelChange = vi.fn();
    const dataWithHandler = {
      ...mockData,
      onLabelChange: handleLabelChange
    };

    render(<PipelineNode id="node-1" data={dataWithHandler} />);

    const labelSpan = screen.getByText('Test Node');
    fireEvent.doubleClick(labelSpan);

    const input = screen.getByRole('textbox', { name: 'Edit node label' }) as HTMLInputElement;
    fireEvent.change(input, { target: { value: 'Blurred Node' } });
    fireEvent.blur(input);

    expect(handleLabelChange).toHaveBeenCalledWith('node-1', 'Blurred Node');
  });

  it('opens text input when rename button is clicked', () => {
    const handleLabelChange = vi.fn();
    const dataWithHandler = {
      ...mockData,
      onLabelChange: handleLabelChange
    };

    render(<PipelineNode id="node-1" data={dataWithHandler} />);

    const renameButton = screen.getByRole('button', { name: 'Rename node' });
    fireEvent.click(renameButton);

    const input = screen.getByRole('textbox', { name: 'Edit node label' }) as HTMLInputElement;
    expect(input).toBeInTheDocument();
    expect(input.value).toBe('Test Node');
  });

  it('renders pulsing crimson border and AlertCircle badge when hasCycle is true', () => {
    const cycleData = {
      ...mockData,
      type: 'STEP',
      hasCycle: true,
      validationError: 'Cyclic dependency detected: Step 1 -> Step 2 -> Step 1',
    };
    render(<PipelineNode id="node-cycle" data={cycleData} />);
    const node = screen.getByTestId('pipeline-node-node-cycle');
    expect(node).toHaveClass('border-rose-500');
    expect(node).toHaveClass('animate-pulse');

    const badge = screen.getByTestId('cycle-error-badge');
    expect(badge).toBeInTheDocument();
    expect(badge).toHaveAttribute('title', 'Cyclic dependency detected: Step 1 -> Step 2 -> Step 1');
  });

  it('renders dashed amber border and AlertTriangle badge when isOrphan is true', () => {
    const orphanData = {
      ...mockData,
      type: 'STEP',
      isOrphan: true,
      validationWarning: 'Orphan step: node is not connected to the pipeline',
    };
    render(<PipelineNode id="node-orphan" data={orphanData} />);
    const node = screen.getByTestId('pipeline-node-node-orphan');
    expect(node).toHaveClass('border-amber-500');
    expect(node).toHaveClass('border-dashed');

    const badge = screen.getByTestId('orphan-warning-badge');
    expect(badge).toBeInTheDocument();
    expect(badge).toHaveAttribute('title', 'Orphan step: node is not connected to the pipeline');
  });

  it('renders red dashed outline and error badge when hasValidationError is true on a Spark node', () => {
    const errorData = {
      ...mockData,
      label: 'Join Step',
      type: 'STEP',
      spark_transform: 'Join',
      hasValidationError: true,
      validationError: 'Join requires both left and right upstream inputs.',
    };
    render(<PipelineNode id="node-join-err" data={errorData} selected={true} />);
    const node = screen.getByTestId('pipeline-node-node-join-err');
    expect(node).toHaveClass('border-red-500');
    expect(node).toHaveClass('border-dashed');
    expect(screen.getByTestId('cycle-error-badge')).toHaveAttribute(
      'title',
      'Join requires both left and right upstream inputs.'
    );
  });

  it('renders BATCH and STREAM mode badges on SOURCE and SINK nodes, plus CACHE and foreachBatch badges (A4)', () => {
    const { rerender } = render(
      <PipelineNode
        id="src-batch"
        data={{
          label: 'CSV Source',
          type: 'SOURCE',
          iconName: 'FileText',
          spark_transform: 'CsvSource',
          executionMode: 'batch',
        }}
      />
    );
    expect(screen.getByTestId('node-mode-badge')).toHaveTextContent('BATCH');
    expect(screen.queryByTestId('node-cache-badge')).not.toBeInTheDocument();
    expect(screen.queryByTestId('node-foreachbatch-badge')).not.toBeInTheDocument();

    rerender(
      <PipelineNode
        id="src-stream"
        data={{
          label: 'Kafka Source',
          type: 'SOURCE',
          iconName: 'Radio',
          spark_transform: 'KafkaSource',
        }}
      />
    );
    expect(screen.getByTestId('node-mode-badge')).toHaveTextContent('STREAM');

    rerender(
      <PipelineNode
        id="step-cached"
        data={{
          label: 'WithColumns',
          type: 'STEP',
          iconName: 'Code',
          spark_transform: 'WithColumns',
          isCached: true,
        }}
      />
    );
    expect(screen.queryByTestId('node-mode-badge')).not.toBeInTheDocument();
    expect(screen.getByTestId('node-cache-badge')).toHaveTextContent('CACHE');

    rerender(
      <PipelineNode
        id="snk-foreach"
        data={{
          label: 'Spanner Sink',
          type: 'SINK',
          iconName: 'Database',
          spark_transform: 'SpannerSink',
          executionMode: 'streaming',
          isForeachBatchSink: true,
        }}
      />
    );
    expect(screen.getByTestId('node-mode-badge')).toHaveTextContent('STREAM');
    expect(screen.getByTestId('node-foreachbatch-badge')).toHaveTextContent('foreachBatch');
  });

  it('renders labeled left and right target handles and BROADCAST badge on Join nodes (A6)', () => {
    const { rerender } = render(
      <PipelineNode
        id="join-1"
        data={{
          label: 'Join',
          type: 'STEP',
          iconName: 'GitMerge',
          spark_transform: 'Join',
          properties: { joinType: 'inner', broadcastRight: false },
        }}
      />
    );

    const leftHandle = screen.getByTestId('handle-target-left-left');
    const rightHandle = screen.getByTestId('handle-target-left-right');
    expect(leftHandle).toBeInTheDocument();
    expect(leftHandle).toHaveStyle({ top: '32%' });
    expect(rightHandle).toBeInTheDocument();
    expect(rightHandle).toHaveStyle({ top: '68%' });
    expect(screen.getByTestId('join-left-handle-label')).toHaveTextContent('L');
    expect(screen.getByTestId('join-right-handle-label')).toHaveTextContent('R');
    expect(screen.queryByTestId('handle-target-left')).not.toBeInTheDocument();
    expect(screen.queryByTestId('node-broadcast-badge')).not.toBeInTheDocument();

    rerender(
      <PipelineNode
        id="join-1"
        data={{
          label: 'Join',
          type: 'STEP',
          iconName: 'GitMerge',
          spark_transform: 'Join',
          properties: { joinType: 'left_outer', broadcastRight: true },
        }}
      />
    );
    expect(screen.getByTestId('node-broadcast-badge')).toHaveTextContent('BROADCAST');
  });

  it('renders multi-input target handle and N-IN badge on UnionByName, SetOperation, and SparkSqlQuery (A6)', () => {
    for (const transform of ['UnionByName', 'SetOperation', 'SparkSqlQuery']) {
      const { unmount } = render(
        <PipelineNode
          id={`multi-${transform}`}
          data={{
            label: transform,
            type: 'STEP',
            iconName: 'Layers',
            spark_transform: transform,
          }}
        />
      );

      expect(screen.getByTestId('handle-target-left')).toBeInTheDocument();
      expect(screen.getByTestId('node-multi-input-badge')).toHaveTextContent('N-IN');
      unmount();
    }
  });

  it('renders dynamic named rule handles plus else handle with 8pt-grid minHeight scaling on ConditionalSplit (A7)', () => {
    // 1. Default rules (missing properties.rules -> rule_1 + else, minHeight = 64px)
    const { rerender } = render(
      <PipelineNode
        id="split-1"
        data={{
          label: 'Conditional Split',
          type: 'STEP',
          iconName: 'GitBranch',
          spark_transform: 'ConditionalSplit',
        }}
      />
    );

    const card = screen.getByTestId('pipeline-node-split-1');
    expect(card).toHaveStyle({ minHeight: '64px' });
    expect(screen.getByTestId('handle-source-right-rule_1')).toHaveAttribute('title', 'rule_1');
    expect(screen.getByTestId('handle-source-right-else')).toHaveAttribute('title', 'else');
    expect(screen.queryByTestId('handle-source-right-valid')).not.toBeInTheDocument();
    expect(screen.queryByTestId('handle-source-right-error')).not.toBeInTheDocument();

    // 2. Three custom rules + else (4 handles total -> 20%, 40%, 60%, 80%, minHeight = 112px = 14 * 8px)
    rerender(
      <PipelineNode
        id="split-1"
        data={{
          label: 'Conditional Split',
          type: 'STEP',
          iconName: 'GitBranch',
          spark_transform: 'ConditionalSplit',
          properties: {
            rules: [
              { name: 'vip', condition: 'amount >= 1000' },
              { name: 'standard', condition: 'amount >= 100' },
              { name: 'micro', condition: 'amount > 0' },
            ],
          },
        }}
      />
    );

    expect(card).toHaveStyle({ minHeight: '112px' });
    const vipHandle = screen.getByTestId('handle-source-right-vip');
    const stdHandle = screen.getByTestId('handle-source-right-standard');
    const microHandle = screen.getByTestId('handle-source-right-micro');
    const elseHandle = screen.getByTestId('handle-source-right-else');

    expect(vipHandle).toHaveAttribute('title', 'vip');
    expect(vipHandle).toHaveStyle({ top: '20%' });
    expect(stdHandle).toHaveAttribute('title', 'standard');
    expect(stdHandle).toHaveStyle({ top: '40%' });
    expect(microHandle).toHaveAttribute('title', 'micro');
    expect(microHandle).toHaveStyle({ top: '60%' });
    expect(elseHandle).toHaveAttribute('title', 'else');
    expect(elseHandle).toHaveStyle({ top: '80%' });

    expect(screen.getByTestId('split-handle-label-vip')).toHaveTextContent('vip');
    expect(screen.getByTestId('split-handle-label-else')).toHaveTextContent('else');
  });

  it('renders dual left and right target handles without BROADCAST badge on MergeCdcScd nodes (EXT4)', () => {
    render(
      <PipelineNode
        id="merge-cdc-1"
        data={{
          label: 'Merge CDC / SCD',
          type: 'STEP',
          iconName: 'GitMerge',
          spark_transform: 'MergeCdcScd',
          properties: { scdType: 'scd2', broadcastRight: true },
        }}
      />
    );

    const card = screen.getByTestId('pipeline-node-merge-cdc-1');
    expect(card).toHaveClass('min-h-[64px]');
    expect(screen.getByTestId('handle-target-left-left')).toHaveStyle({ top: '32%' });
    expect(screen.getByTestId('handle-target-left-right')).toHaveStyle({ top: '68%' });
    expect(screen.getByTestId('join-left-handle-label')).toHaveTextContent('L');
    expect(screen.getByTestId('join-right-handle-label')).toHaveTextContent('R');
    expect(screen.queryByTestId('handle-target-left')).not.toBeInTheDocument();
    expect(screen.queryByTestId('node-broadcast-badge')).not.toBeInTheDocument();
    expect(screen.getByTestId('handle-source-right-valid')).toHaveStyle({ top: '50%' });
    expect(screen.queryByTestId('handle-source-right-error')).not.toBeInTheDocument();
  });

  it('renders SDP dataset badges (VIEW, MV, TABLE, CDC, SINK) instead of CACHE/foreachBatch and adjusts SDP handles (SDP2)', () => {
    const badges = ['VIEW', 'MV', 'TABLE', 'CDC', 'SINK'] as const;
    for (const badge of badges) {
      const { unmount } = render(
        <PipelineNode
          id={`sdp-${badge}`}
          data={{
            label: `Dataset ${badge}`,
            type: badge === 'SINK' ? 'SINK' : 'STEP',
            iconName: 'Database',
            spark_transform: badge === 'SINK' ? 'KafkaSink' : 'WithColumns',
            paradigm: 'sdp',
            sdpBadge: badge,
            sdpDatasetName: `ds_${badge.toLowerCase()}`,
            isCached: true,
            isForeachBatchSink: true,
          }}
        />
      );

      expect(screen.getByTestId('node-sdp-badge')).toHaveTextContent(badge);
      expect(screen.queryByTestId('node-cache-badge')).not.toBeInTheDocument();
      expect(screen.queryByTestId('node-foreachbatch-badge')).not.toBeInTheDocument();
      unmount();
    }

    // ParsePayload hides red error DLQ handle in SDP mode, while DataQualityAssert retains it
    const { rerender } = render(
      <PipelineNode
        id="sdp-parse"
        data={{
          label: 'Parse JSON',
          type: 'STEP',
          iconName: 'FileCode',
          spark_transform: 'ParsePayload',
          paradigm: 'sdp',
          sdpBadge: 'TABLE',
        }}
      />
    );
    expect(screen.queryByTestId('handle-source-right-error')).not.toBeInTheDocument();

    rerender(
      <PipelineNode
        id="sdp-dq"
        data={{
          label: 'Assert Quality',
          type: 'STEP',
          iconName: 'ShieldCheck',
          spark_transform: 'DataQualityAssert',
          paradigm: 'sdp',
          sdpBadge: 'TABLE',
        }}
      />
    );
    expect(screen.getByTestId('handle-source-right-error')).toBeInTheDocument();

    // MergeCdcScd in SDP mode exposes single left handle without right snapshot handle
    rerender(
      <PipelineNode
        id="sdp-merge"
        data={{
          label: 'Auto CDC',
          type: 'STEP',
          iconName: 'GitMerge',
          spark_transform: 'MergeCdcScd',
          paradigm: 'sdp',
          sdpBadge: 'CDC',
        }}
      />
    );
    expect(screen.getByTestId('handle-target-left-left')).toBeInTheDocument();
    expect(screen.queryByTestId('handle-target-left-right')).not.toBeInTheDocument();
  });
});


