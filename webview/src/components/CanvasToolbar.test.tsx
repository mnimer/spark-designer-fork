/**
 * Unit tests for CanvasToolbar component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { CanvasToolbar } from './CanvasToolbar';

const mockZoomIn = vi.fn();
const mockZoomOut = vi.fn();
const mockFitView = vi.fn();

vi.mock('reactflow', () => ({
  MiniMap: () => <div data-testid="mini-map" />,
  useReactFlow: () => ({
    zoomIn: mockZoomIn,
    zoomOut: mockZoomOut,
    fitView: mockFitView,
  }),
}));

describe('CanvasToolbar', () => {
  beforeEach(() => {
    mockZoomIn.mockClear();
    mockZoomOut.mockClear();
    mockFitView.mockClear();
  });

  it('renders BATCH pipeline mode indicator by default when pipelineMode is omitted', () => {
    render(
      <CanvasToolbar
        canUndo={false}
        canRedo={false}
        onUndo={vi.fn()}
        onRedo={vi.fn()}
        onAutoLayout={vi.fn()}
      />
    );

    const modeBadge = screen.getByTestId('toolbar-pipeline-mode');
    expect(modeBadge).toBeInTheDocument();
    expect(modeBadge).toHaveTextContent('BATCH');
  });

  it('renders STREAMING pipeline mode indicator when pipelineMode is streaming', () => {
    render(
      <CanvasToolbar
        canUndo={true}
        canRedo={true}
        onUndo={vi.fn()}
        onRedo={vi.fn()}
        onAutoLayout={vi.fn()}
        pipelineMode="streaming"
      />
    );

    const modeBadge = screen.getByTestId('toolbar-pipeline-mode');
    expect(modeBadge).toBeInTheDocument();
    expect(modeBadge).toHaveTextContent('STREAMING');
  });

  it('invokes undo, redo, auto-layout, zoom, and fitView callbacks and hides MiniMap when disabled', () => {
    const onUndo = vi.fn();
    const onRedo = vi.fn();
    const onAutoLayout = vi.fn();

    render(
      <CanvasToolbar
        canUndo={true}
        canRedo={true}
        onUndo={onUndo}
        onRedo={onRedo}
        onAutoLayout={onAutoLayout}
        pipelineMode="batch"
        showMiniMap={false}
      />
    );

    fireEvent.click(screen.getByTestId('canvas-undo-button'));
    expect(onUndo).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByTestId('canvas-redo-button'));
    expect(onRedo).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByTestId('canvas-autolayout-button'));
    expect(onAutoLayout).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByTestId('canvas-zoomin-button'));
    expect(mockZoomIn).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByTestId('canvas-zoomout-button'));
    expect(mockZoomOut).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByTestId('canvas-fitview-button'));
    expect(mockFitView).toHaveBeenCalledWith({ padding: 0.2 });

    expect(screen.queryByTestId('mini-map')).not.toBeInTheDocument();
  });

  it('renders 3-option generated output dropdown (Imperative, SDP, Notebook) and fires onParadigmChange (SDP1, V2.1)', () => {
    const onParadigmChange = vi.fn();
    const { rerender } = render(
      <CanvasToolbar
        canUndo={false}
        canRedo={false}
        onUndo={vi.fn()}
        onRedo={vi.fn()}
        onAutoLayout={vi.fn()}
        onParadigmChange={onParadigmChange}
      />
    );

    const indicator = screen.getByTestId('toolbar-paradigm-indicator');
    expect(indicator).toHaveTextContent('IMPERATIVE');
    expect(indicator).toHaveAttribute('aria-expanded', 'false');

    // Open dropdown and verify all 3 options are listed
    fireEvent.click(indicator);
    expect(indicator).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByTestId('toolbar-paradigm-menu')).toBeInTheDocument();
    expect(screen.getByTestId('paradigm-option-imperative')).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByTestId('paradigm-option-sdp')).toBeInTheDocument();
    expect(screen.getByTestId('paradigm-option-notebook')).toBeInTheDocument();

    // Pick SDP
    fireEvent.click(screen.getByTestId('paradigm-option-sdp'));
    expect(onParadigmChange).toHaveBeenCalledWith('sdp');
    expect(screen.queryByTestId('toolbar-paradigm-menu')).not.toBeInTheDocument();

    // Rerender as SDP and pick Notebook (.ipynb)
    onParadigmChange.mockClear();
    rerender(
      <CanvasToolbar
        canUndo={false}
        canRedo={false}
        onUndo={vi.fn()}
        onRedo={vi.fn()}
        onAutoLayout={vi.fn()}
        paradigm="sdp"
        onParadigmChange={onParadigmChange}
      />
    );

    expect(screen.getByTestId('toolbar-paradigm-indicator')).toHaveTextContent('SDP (4.2+)');
    fireEvent.click(screen.getByTestId('toolbar-paradigm-indicator'));
    fireEvent.click(screen.getByTestId('paradigm-option-notebook'));
    expect(onParadigmChange).toHaveBeenCalledWith('notebook');

    // Rerender as Notebook (.ipynb) and switch back to Imperative
    onParadigmChange.mockClear();
    rerender(
      <CanvasToolbar
        canUndo={false}
        canRedo={false}
        onUndo={vi.fn()}
        onRedo={vi.fn()}
        onAutoLayout={vi.fn()}
        paradigm="notebook"
        onParadigmChange={onParadigmChange}
      />
    );

    expect(screen.getByTestId('toolbar-paradigm-indicator')).toHaveTextContent('NOTEBOOK (.IPYNB)');
    fireEvent.click(screen.getByTestId('toolbar-paradigm-indicator'));
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(screen.queryByTestId('toolbar-paradigm-menu')).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId('toolbar-paradigm-indicator'));
    fireEvent.click(screen.getByTestId('paradigm-option-imperative'));
    expect(onParadigmChange).toHaveBeenCalledWith('imperative');
  });
});
