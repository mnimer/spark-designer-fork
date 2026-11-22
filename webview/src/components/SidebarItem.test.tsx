
/**
 * Unit tests for SidebarItem component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SidebarItem } from './SidebarItem';

describe('SidebarItem', () => {
  it('renders label, icon, badge, and description', () => {
    render(
      <SidebarItem
        label="PubSub"
        type="SOURCE"
        iconName="Radio"
        badge="Streaming"
        description="Read messages from Cloud Pub/Sub"
        onDragStart={vi.fn()}
      />
    );

    expect(screen.getByText('PubSub')).toBeInTheDocument();
    expect(screen.getByText('Streaming')).toBeInTheDocument();
    expect(screen.getByText('Read messages from Cloud Pub/Sub')).toBeInTheDocument();
  });

  it('calls onDragStart when dragged', () => {
    const mockOnDragStart = vi.fn();
    render(
      <SidebarItem
        label="PubSub"
        type="SOURCE"
        iconName="Radio"
        sparkTransform="PubSubIO"
        onDragStart={mockOnDragStart}
      />
    );

    const item = screen.getByTestId('sidebar-item-pubsub');
    fireEvent.dragStart(item);

    expect(mockOnDragStart).toHaveBeenCalledWith(
      expect.any(Object),
      'pipeline',
      'PubSub',
      'SOURCE',
      'Radio',
      'PubSubIO'
    );
  });

  it('calls onAddNode when plus button is clicked', () => {
    const mockOnAddNode = vi.fn();
    render(
      <SidebarItem
        label="BigQuery"
        type="SOURCE"
        iconName="Database"
        sparkTransform="BigQueryIO"
        onDragStart={vi.fn()}
        onAddNode={mockOnAddNode}
      />
    );

    const addButton = screen.getByRole('button', { name: /Add BigQuery/i });
    fireEvent.click(addButton);

    expect(mockOnAddNode).toHaveBeenCalledWith(
      'BigQuery',
      'SOURCE',
      'Database',
      'BigQueryIO'
    );
  });

  it('renders correct theme classes for SOURCE, STEP, and SINK', () => {
    const { rerender } = render(
      <SidebarItem label="Source Node" type="SOURCE" iconName="Radio" badge="Src" onDragStart={vi.fn()} />
    );
    expect(screen.getByText('Src')).toHaveClass('text-emerald-300');

    rerender(
      <SidebarItem label="Step Node" type="STEP" iconName="Activity" badge="Step" onDragStart={vi.fn()} />
    );
    expect(screen.getByText('Step')).toHaveClass('text-blue-300');

    rerender(
      <SidebarItem label="Sink Node" type="SINK" iconName="Database" badge="Sink" onDragStart={vi.fn()} />
    );
    expect(screen.getByText('Sink')).toHaveClass('text-rose-300');
  });

  it('greys out unsupported SDP nodes, disables drag and add, and renders explanatory tooltip (SDP2)', () => {
    const onDragStart = vi.fn();
    const onAddNode = vi.fn();

    render(
      <SidebarItem
        label="Cloud Spanner"
        type="SINK"
        iconName="Database"
        sparkTransform="SpannerSink"
        paradigm="sdp"
        onDragStart={onDragStart}
        onAddNode={onAddNode}
      />
    );

    const card = screen.getByTestId('sidebar-item-cloud-spanner');
    expect(card).toHaveAttribute('aria-disabled', 'true');
    expect(card).toHaveClass('opacity-45');
    expect(card).toHaveClass('cursor-not-allowed');
    expect(card).toHaveAttribute('draggable', 'false');

    fireEvent.dragStart(card);
    expect(onDragStart).not.toHaveBeenCalled();

    const addBtn = screen.getByRole('button', { name: /Add Cloud Spanner/i });
    expect(addBtn).toBeDisabled();
    fireEvent.click(addBtn);
    expect(onAddNode).not.toHaveBeenCalled();

    const tooltip = screen.getByRole('tooltip');
    expect(tooltip).toBeInTheDocument();
    expect(tooltip.getAttribute('aria-label')).toMatch(/foreachBatch|SDP/i);
  });

  it('unlocks foreachBatch-only sinks in SDP mode when allowHybridForeachBatch is enabled (V2.6)', () => {
    const onAddNode = vi.fn();

    render(
      <SidebarItem
        label="Cloud Spanner"
        type="SINK"
        iconName="Database"
        sparkTransform="SpannerSink"
        paradigm="sdp"
        allowHybridForeachBatch={true}
        onDragStart={vi.fn()}
        onAddNode={onAddNode}
      />
    );

    const card = screen.getByTestId('sidebar-item-cloud-spanner');
    expect(card).not.toHaveAttribute('aria-disabled', 'true');
    expect(card).toHaveAttribute('draggable', 'true');

    const addBtn = screen.getByRole('button', { name: /Add Cloud Spanner/i });
    expect(addBtn).not.toBeDisabled();
    fireEvent.click(addBtn);
    expect(onAddNode).toHaveBeenCalledTimes(1);
  });
});

