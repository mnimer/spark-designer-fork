/**
 * @fileoverview Floating top-right React Flow canvas toolbar with pipeline mode indicator, Undo/Redo, Auto Layout, Zoom controls, and MiniMap.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useCallback } from 'react';
import { MiniMap, useReactFlow, type Node } from 'reactflow';
import {
  Layers,
  LayoutGrid,
  Maximize2,
  Radio,
  Redo2,
  Undo2,
  ZoomIn,
  ZoomOut,
} from 'lucide-react';
import type { PipelineNodeData } from '../hooks/usePipelineGraph';

/**
 * Props for the CanvasToolbar component.
 */
export interface CanvasToolbarProps {
  /** Whether an undo snapshot is available. */
  readonly canUndo: boolean;
  /** Whether a redo snapshot is available. */
  readonly canRedo: boolean;
  /** Callback invoked to undo the last diagram mutation. */
  readonly onUndo: () => void;
  /** Callback invoked to redo the last undone diagram mutation. */
  readonly onRedo: () => void;
  /** Callback invoked to auto-layout the diagram nodes. */
  readonly onAutoLayout: () => void;
  /** Effective execution mode across the pipeline graph (defaults to 'batch'). */
  readonly pipelineMode?: 'batch' | 'streaming';
  /** Whether to display the MiniMap directly below the toolbar. */
  readonly showMiniMap?: boolean;
}

/**
 * Resolves the MiniMap node color using the tri-color DAG taxonomy.
 */
function resolveMiniMapNodeColor(node: Node<PipelineNodeData>): string {
  if (node.data?.type === 'SOURCE') return '#10b981';
  if (node.data?.type === 'SINK') return '#ef4444';
  return '#3b82f6';
}

/**
 * Renders the top-right floating canvas control bar, pipeline execution mode pill, and MiniMap inside the React Flow canvas.
 */
export const CanvasToolbar: React.FC<CanvasToolbarProps> = ({
  canUndo,
  canRedo,
  onUndo,
  onRedo,
  onAutoLayout,
  pipelineMode = 'batch',
  showMiniMap = true,
}) => {
  const { zoomIn, zoomOut, fitView } = useReactFlow();
  const isStreaming = pipelineMode === 'streaming';

  const handleZoomIn = useCallback(() => {
    zoomIn?.();
  }, [zoomIn]);

  const handleZoomOut = useCallback(() => {
    zoomOut?.();
  }, [zoomOut]);

  const handleFitView = useCallback(() => {
    fitView?.({ padding: 0.2 });
  }, [fitView]);

  return (
    <div
      data-testid="canvas-toolbar"
      className="absolute top-3 right-3 z-20 flex flex-col items-end gap-2 pointer-events-auto select-none"
    >
      {/* Control Button Toolbar Strip */}
      <div className="flex items-center gap-1.5 px-3 py-1.5 bg-[var(--vscode-workbench-bg,#161b22)]/95 backdrop-blur-md border border-[var(--vscode-border,#30363d)] rounded-xl shadow-xl text-xs text-[var(--vscode-text-secondary,#c9d1d9)]">
        {/* Pipeline Execution Mode Indicator Pill */}
        <div
          data-testid="toolbar-pipeline-mode"
          title={
            isStreaming
              ? 'Structured Streaming mode: at least one upstream source reads unbounded streams'
              : 'Batch mode: all sources execute bounded Spark DataFrame reads'
          }
          className={`flex items-center gap-1.5 px-2 py-1 rounded-md border text-[10px] font-bold tracking-wider uppercase ${
            isStreaming
              ? 'bg-cyan-500/15 border-cyan-500/40 text-cyan-300'
              : 'bg-slate-500/15 border-[var(--vscode-border,#30363d)] text-[var(--vscode-text-secondary,#c9d1d9)]'
          }`}
        >
          {isStreaming ? (
            <Radio className="w-3 h-3 text-cyan-400" />
          ) : (
            <Layers className="w-3 h-3 text-emerald-400" />
          )}
          <span>{isStreaming ? 'STREAMING' : 'BATCH'}</span>
        </div>

        <div className="w-px h-5 bg-[var(--vscode-border,#30363d)] mx-1" />

        {/* Undo Action */}
        <button
          type="button"
          onClick={onUndo}
          disabled={!canUndo}
          title="Undo (Cmd+Z / Ctrl+Z)"
          data-testid="canvas-undo-button"
          className="p-1.5 rounded-lg hover:bg-[var(--vscode-hover,#21262d)] disabled:opacity-30 disabled:cursor-not-allowed text-[var(--vscode-text-secondary,#c9d1d9)] hover:text-[var(--vscode-text-primary,#f0f6fc)] transition-colors cursor-pointer"
        >
          <Undo2 className="w-4 h-4" />
        </button>

        {/* Redo Action */}
        <button
          type="button"
          onClick={onRedo}
          disabled={!canRedo}
          title="Redo (Cmd+Shift+Z / Ctrl+Y)"
          data-testid="canvas-redo-button"
          className="p-1.5 rounded-lg hover:bg-[var(--vscode-hover,#21262d)] disabled:opacity-30 disabled:cursor-not-allowed text-[var(--vscode-text-secondary,#c9d1d9)] hover:text-[var(--vscode-text-primary,#f0f6fc)] transition-colors cursor-pointer"
        >
          <Redo2 className="w-4 h-4" />
        </button>

        <div className="w-px h-5 bg-[var(--vscode-border,#30363d)] mx-1" />

        {/* Auto Layout Action */}
        <button
          type="button"
          onClick={onAutoLayout}
          title="Auto Layout Diagram"
          data-testid="canvas-autolayout-button"
          className="flex items-center gap-2 px-2.5 py-1.5 rounded-lg hover:bg-[var(--vscode-hover,#21262d)] text-[var(--vscode-text-primary,#f0f6fc)] font-semibold text-xs transition-colors cursor-pointer"
        >
          <LayoutGrid className="w-4 h-4 text-sky-400" />
          <span>Auto Layout</span>
        </button>

        <div className="w-px h-5 bg-[var(--vscode-border,#30363d)] mx-1" />

        {/* Zoom In Action */}
        <button
          type="button"
          onClick={handleZoomIn}
          title="Zoom In"
          data-testid="canvas-zoomin-button"
          className="p-1.5 rounded-lg hover:bg-[var(--vscode-hover,#21262d)] text-[var(--vscode-text-secondary,#c9d1d9)] hover:text-[var(--vscode-text-primary,#f0f6fc)] transition-colors cursor-pointer"
        >
          <ZoomIn className="w-4 h-4" />
        </button>

        {/* Zoom Out Action */}
        <button
          type="button"
          onClick={handleZoomOut}
          title="Zoom Out"
          data-testid="canvas-zoomout-button"
          className="p-1.5 rounded-lg hover:bg-[var(--vscode-hover,#21262d)] text-[var(--vscode-text-secondary,#c9d1d9)] hover:text-[var(--vscode-text-primary,#f0f6fc)] transition-colors cursor-pointer"
        >
          <ZoomOut className="w-4 h-4" />
        </button>

        {/* Zoom to Fit Action */}
        <button
          type="button"
          onClick={handleFitView}
          title="Zoom to Fit"
          data-testid="canvas-fitview-button"
          className="flex items-center gap-2 px-2.5 py-1.5 rounded-lg hover:bg-[var(--vscode-hover,#21262d)] text-[var(--vscode-text-primary,#f0f6fc)] font-semibold text-xs transition-colors cursor-pointer"
        >
          <Maximize2 className="w-4 h-4 text-emerald-500" />
          <span>Zoom to Fit</span>
        </button>
      </div>

      {/* MiniMap Positioned Directly Under Controls */}
      {showMiniMap && (
        <MiniMap
          nodeColor={resolveMiniMapNodeColor}
          nodeStrokeWidth={2}
          className="!static !m-0 !h-[120px] !w-[180px] border border-[var(--vscode-border,#30363d)] rounded-xl shadow-xl overflow-hidden !bg-[var(--vscode-workbench-bg,#161b22)]"
          maskColor="rgba(13, 17, 23, 0.75)"
        />
      )}
    </div>
  );
};
