/**
 * PipelineNode Component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Custom React Flow node representing a source, step, or sink in the Apache Spark DAG.
 * Renders the transform icon, uppercase category badge, execution mode and planner
 * micro-badges (BATCH, STREAM, CACHE, foreachBatch), and inline double-click label editing.
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Handle, Position, useReactFlow, type Node } from 'reactflow';
import { Box, Pencil, AlertCircle, AlertTriangle } from 'lucide-react';
import { getNodeIcon } from './common/nodeIcons';
import {
  getConditionalSplitHandles,
  getEffectiveSourceReadMode,
  getNodeInputArity,
  isConditionalSplitNode,
  isErrorCapableStep,
  isJoinNode,
  type ReadMode,
} from '../utils/sparkDagRules';

/**
 * React Flow custom node properties representing an Apache Spark pipeline transform.
 */
export interface PipelineNodeProps {
  id?: string;
  data: {
    label: string;
    type: string;
    iconName: string;
    spark_transform?: string;
    system_instructions?: string;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    properties?: Record<string, any>;
    hasCycle?: boolean;
    hasValidationError?: boolean;
    isOrphan?: boolean;
    validationError?: string;
    validationWarning?: string;
    executionMode?: ReadMode;
    isCached?: boolean;
    isForeachBatchSink?: boolean;
    supportedReadModes?: ReadMode[];
    defaultReadMode?: ReadMode;
    errorOutput?: boolean;
    inputArity?: 1 | 2 | 'n';
    onLabelChange?: (id: string, newLabel: string) => void;
  };
  selected?: boolean;
}

/**
 * Renders a pipeline node card on the React Flow canvas.
 */
export const PipelineNode: React.FC<PipelineNodeProps> = ({ id, data, selected }) => {
  const isSource = data.type === 'SOURCE';
  const isSink = data.type === 'SINK';
  const isStep = data.type === 'STEP';
  const dagNode = { id: id || '', data };
  const showTargetHandle = !isSource;
  const isJoin = isStep && isJoinNode(dagNode);
  const isConditionalSplit = isStep && isConditionalSplitNode(dagNode);
  const splitHandles = isConditionalSplit ? getConditionalSplitHandles(dagNode) : [];
  const splitMinHeight = isConditionalSplit
    ? Math.max(64, splitHandles.length * 24 + 16)
    : undefined;
  const inputArity = !isSource ? getNodeInputArity(dagNode) : 1;
  const isMultiInput = isStep && inputArity === 'n';
  const isBroadcastJoin = isJoin && data.properties?.broadcastRight === true;
  const hasErrorHandle = isStep && !isConditionalSplit && isErrorCapableStep(dagNode);

  const effectiveExecutionMode: ReadMode = isSource
    ? (data.executionMode ?? getEffectiveSourceReadMode(dagNode))
    : (data.executionMode ?? 'batch');

  const IconComponent = getNodeIcon(data.iconName, Box);

  const [isEditing, setIsEditing] = useState(false);
  const [editValue, setEditValue] = useState(data.label);
  const inputRef = useRef<HTMLInputElement>(null);

  let reactFlow: ReturnType<typeof useReactFlow<PipelineNodeProps['data']>> | null = null;
  try {
    // eslint-disable-next-line react-hooks/rules-of-hooks
    reactFlow = useReactFlow<PipelineNodeProps['data']>();
  } catch {
    reactFlow = null;
  }

  useEffect(() => {
    if (isEditing && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [isEditing]);

  const startEditing = useCallback(() => {
    setEditValue(data.label);
    setIsEditing(true);
  }, [data.label]);

  const handleCommit = useCallback(() => {
    const trimmed = editValue.trim();
    if (trimmed && trimmed !== data.label) {
      if (data.onLabelChange && id) {
        data.onLabelChange(id, trimmed);
      } else if (reactFlow?.setNodes && id) {
        reactFlow.setNodes((nds: Node<PipelineNodeProps['data']>[]) =>
          nds.map((n) =>
            n.id === id ? { ...n, data: { ...n.data, label: trimmed } } : n
          )
        );
      }
    } else {
      setEditValue(data.label);
    }
    setIsEditing(false);
  }, [editValue, data, id, reactFlow]);

  const handleCancel = useCallback(() => {
    setEditValue(data.label);
    setIsEditing(false);
  }, [data.label]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === 'Enter') {
        e.stopPropagation();
        handleCommit();
      } else if (e.key === 'Escape') {
        e.stopPropagation();
        handleCancel();
      }
    },
    [handleCommit, handleCancel]
  );

  const handleCardDoubleClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      if (!isEditing) {
        e.stopPropagation();
        startEditing();
      }
    },
    [isEditing, startEditing]
  );

  const handleLabelDoubleClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      e.stopPropagation();
      startEditing();
    },
    [startEditing]
  );

  const handleRenameClick = useCallback(
    (e: React.MouseEvent<HTMLButtonElement>) => {
      e.stopPropagation();
      startEditing();
    },
    [startEditing]
  );

  const handleEditChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setEditValue(e.target.value);
  }, []);

  const stopEventPropagation = useCallback((e: React.SyntheticEvent) => {
    e.stopPropagation();
  }, []);

  const getNodeBorderClasses = () => {
    if (data.hasCycle) {
      return 'border-rose-500 ring-2 ring-rose-500/70 shadow-lg shadow-rose-500/25 animate-pulse';
    }
    if (data.hasValidationError) {
      return selected
        ? 'border-red-500 ring-2 ring-red-500/50 shadow-lg shadow-red-500/20 border-dashed'
        : 'border-red-500 border-dashed hover:border-red-400';
    }
    if (data.isOrphan) {
      return selected
        ? 'border-amber-400 ring-2 ring-amber-500/50 shadow-lg shadow-amber-500/10 border-dashed'
        : 'border-amber-500 border-dashed hover:border-amber-400';
    }
    if (data.type === 'SOURCE') {
      return selected
        ? 'border-emerald-400 ring-2 ring-emerald-500/50 shadow-lg shadow-emerald-500/10'
        : 'border-emerald-500 hover:border-emerald-400';
    }
    if (data.type === 'SINK') {
      return selected
        ? 'border-red-400 ring-2 ring-red-500/50 shadow-lg shadow-red-500/10'
        : 'border-red-500 hover:border-red-400';
    }
    return selected
      ? 'border-blue-400 ring-2 ring-blue-500/50 shadow-lg shadow-blue-500/10'
      : 'border-blue-500 hover:border-blue-400';
  };

  const getIconContainerClasses = () => {
    if (data.type === 'SOURCE') {
      return 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400';
    }
    if (data.type === 'SINK') {
      return 'bg-red-500/10 border-red-500/40 text-red-400';
    }
    return 'bg-blue-500/10 border-blue-500/40 text-blue-400';
  };

  return (
    <div
      style={splitMinHeight ? { minHeight: `${splitMinHeight}px` } : undefined}
      className={`relative bg-[var(--vscode-node-bg,#1E202A)] border ${getNodeBorderClasses()} rounded-md shadow-xl p-3 ${
        isConditionalSplit ? 'pr-16 min-w-[224px]' : 'min-w-[180px]'
      } flex items-center gap-3 ${
        isJoin ? 'min-h-[64px]' : ''
      } text-left group/node transition-colors`}
      onDoubleClick={handleCardDoubleClick}
      data-testid={`pipeline-node-${id || 'node'}`}
    >
      {showTargetHandle && isJoin && (
        <>
          <Handle
            type="target"
            position={Position.Left}
            id="left"
            style={{
              top: '32%',
              left: -7,
              width: 14,
              height: 14,
              backgroundColor: data.hasValidationError ? '#ef4444' : '#3b82f6',
            }}
            className={`w-3.5 h-3.5 border-none ${
              data.hasValidationError ? 'bg-red-500' : 'bg-blue-500'
            }`}
            title="Left Input (left)"
          />
          <span
            data-testid="join-left-handle-label"
            style={{ top: '32%' }}
            className="pointer-events-none select-none absolute -left-6 -translate-y-1/2 text-[10px] font-bold leading-none uppercase text-blue-300 bg-[var(--vscode-canvas-bg,#0d1117)]/90 px-1 py-0.5 rounded border border-blue-500/30"
          >
            L
          </span>
          <Handle
            type="target"
            position={Position.Left}
            id="right"
            style={{
              top: '68%',
              left: -7,
              width: 14,
              height: 14,
              backgroundColor: data.hasValidationError ? '#ef4444' : '#6366f1',
            }}
            className={`w-3.5 h-3.5 border-none ${
              data.hasValidationError ? 'bg-red-500' : 'bg-indigo-500'
            }`}
            title="Right Input (right)"
          />
          <span
            data-testid="join-right-handle-label"
            style={{ top: '68%' }}
            className="pointer-events-none select-none absolute -left-6 -translate-y-1/2 text-[10px] font-bold leading-none uppercase text-indigo-300 bg-[var(--vscode-canvas-bg,#0d1117)]/90 px-1 py-0.5 rounded border border-indigo-500/30"
          >
            R
          </span>
        </>
      )}
      {showTargetHandle && !isJoin && (
        <Handle
          type="target"
          position={Position.Left}
          style={{
            width: 14,
            height: 14,
            left: -7,
            backgroundColor:
              isSink || data.hasValidationError
                ? '#ef4444'
                : isSource
                ? '#10b981'
                : '#3b82f6',
          }}
          className={`w-3.5 h-3.5 border-none ${
            isSink || data.hasValidationError
              ? 'bg-red-500'
              : isSource
              ? 'bg-emerald-500'
              : 'bg-blue-500'
          }`}
          title={isMultiInput ? 'Multi-Input Target (accepts N ordered inputs)' : undefined}
        />
      )}

      <div className="flex items-center gap-3 w-full">
        <div className={`p-2 rounded shrink-0 border ${getIconContainerClasses()}`}>
          {IconComponent && (
            /* eslint-disable-next-line react-hooks/static-components */
            <IconComponent
              className={`w-5 h-5 ${
                data.type === 'SOURCE'
                  ? 'text-emerald-400'
                  : data.type === 'SINK'
                  ? 'text-red-400'
                  : 'text-blue-400'
              }`}
            />
          )}
        </div>
        <div className="flex flex-col flex-1 min-w-0 gap-1">
          <div className="flex items-center flex-wrap gap-1">
            <span
              className={`text-[10px] font-bold tracking-wider uppercase ${
                data.type === 'SOURCE'
                  ? 'text-emerald-400'
                  : data.type === 'SINK'
                  ? 'text-red-400'
                  : 'text-blue-400'
              }`}
            >
              {data.type}
            </span>
            {(isSource || isSink) && (
              <span
                data-testid="node-mode-badge"
                title={
                  effectiveExecutionMode === 'streaming'
                    ? 'Structured Streaming mode'
                    : 'Batch execution mode'
                }
                className={`px-1 py-0.5 rounded text-[10px] font-bold leading-none tracking-wider uppercase border ${
                  effectiveExecutionMode === 'streaming'
                    ? 'bg-cyan-500/15 border-cyan-500/40 text-cyan-300'
                    : 'bg-slate-500/15 border-[var(--vscode-border,#30363d)] text-[var(--vscode-text-secondary,#c9d1d9)]'
                }`}
              >
                {effectiveExecutionMode === 'streaming' ? 'STREAM' : 'BATCH'}
              </span>
            )}
            {isBroadcastJoin && (
              <span
                data-testid="node-broadcast-badge"
                title="Broadcasts right lookup DataFrame via F.broadcast(right_df)"
                className="px-1 py-0.5 rounded text-[10px] font-bold leading-none tracking-wider uppercase border bg-indigo-500/15 border-indigo-500/40 text-indigo-300"
              >
                BROADCAST
              </span>
            )}
            {isMultiInput && (
              <span
                data-testid="node-multi-input-badge"
                title="Accepts N ordered upstream DataFrames"
                className="px-1 py-0.5 rounded text-[10px] font-bold leading-none tracking-wider uppercase border bg-blue-500/15 border-blue-500/40 text-blue-300"
              >
                N-IN
              </span>
            )}
            {data.isCached && (
              <span
                data-testid="node-cache-badge"
                title="Output DataFrame is cached before downstream fan-out and unpersisted after terminal writes"
                className="px-1 py-0.5 rounded text-[10px] font-bold leading-none tracking-wider uppercase border bg-amber-500/15 border-amber-500/40 text-amber-300"
              >
                CACHE
              </span>
            )}
            {isSink && data.isForeachBatchSink && (
              <span
                data-testid="node-foreachbatch-badge"
                title="Written via Structured Streaming foreachBatch micro-batch callback"
                className="px-1 py-0.5 rounded text-[10px] font-bold leading-none tracking-wide border bg-violet-500/15 border-violet-500/40 text-violet-300"
              >
                foreachBatch
              </span>
            )}
            {(data.hasCycle || data.hasValidationError) && (
              <span
                className="flex items-center text-rose-400 cursor-help"
                title={
                  data.validationError ||
                  'Cycle detected: this node participates in a cyclic dependency loop'
                }
                data-testid="cycle-error-badge"
              >
                <AlertCircle className="w-3.5 h-3.5 text-rose-400 fill-rose-500/20" />
              </span>
            )}
            {data.isOrphan && !data.hasCycle && !data.hasValidationError && (
              <span
                className="flex items-center text-amber-400 cursor-help"
                title={data.validationWarning || 'Orphan step: node is not connected to the pipeline'}
                data-testid="orphan-warning-badge"
              >
                <AlertTriangle className="w-3.5 h-3.5 text-amber-400 fill-amber-500/20" />
              </span>
            )}
            {!data.isOrphan &&
              Boolean(data.validationWarning) &&
              !data.hasCycle &&
              !data.hasValidationError && (
                <span
                  className="flex items-center text-amber-400 cursor-help"
                  title={data.validationWarning}
                  data-testid="node-warning-badge"
                >
                  <AlertTriangle className="w-3.5 h-3.5 text-amber-400 fill-amber-500/20" />
                </span>
              )}
          </div>
          {isEditing ? (
            <input
              ref={inputRef}
              type="text"
              value={editValue}
              onChange={handleEditChange}
              onBlur={handleCommit}
              onKeyDown={handleKeyDown}
              onClick={stopEventPropagation}
              onMouseDown={stopEventPropagation}
              onPointerDown={stopEventPropagation}
              className="nodrag bg-[var(--vscode-input-bg)] text-sm font-medium text-[var(--vscode-text-primary)] border border-[var(--vscode-highlight)] rounded px-1.5 py-0.5 outline-none w-full max-w-[140px]"
              aria-label="Edit node label"
            />
          ) : (
            <div
              className="nodrag flex items-center gap-1 cursor-pointer min-w-0"
              onDoubleClick={handleLabelDoubleClick}
              title="Double-click to rename"
            >
              <span className="text-sm font-medium text-[var(--vscode-text-primary)] hover:text-[var(--vscode-highlight)] transition-colors truncate select-none">
                {data.label}
              </span>
              <button
                type="button"
                onClick={handleRenameClick}
                className="opacity-0 group-hover/node:opacity-100 text-[var(--vscode-text-muted)] hover:text-[var(--vscode-highlight)] p-0.5 transition-opacity shrink-0"
                title="Rename node"
                aria-label="Rename node"
              >
                <Pencil className="w-3 h-3" />
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Source handle for outgoing connections (not for sinks) */}
      {!isSink && data.type !== 'STEP' && (
        <Handle
          type="source"
          position={Position.Right}
          style={{ width: 14, height: 14, right: -7, backgroundColor: '#10b981' }}
          className="w-3.5 h-3.5 bg-emerald-500 border-none"
        />
      )}

      {/* ConditionalSplit dynamic rule handles + fallback else handle */}
      {isStep &&
        isConditionalSplit &&
        splitHandles.map((handleItem, idx) => {
          const topPercent = `${(((idx + 1) / (splitHandles.length + 1)) * 100)
            .toFixed(2)
            .replace(/\.00$/, '')}%`;
          const isFallback = handleItem.id === 'else';
          return (
            <React.Fragment key={handleItem.id}>
              <Handle
                type="source"
                position={Position.Right}
                id={handleItem.id}
                style={{
                  top: topPercent,
                  right: -7,
                  width: 14,
                  height: 14,
                  backgroundColor: isFallback ? '#f59e0b' : '#10b981',
                }}
                className={`w-3.5 h-3.5 border-none ${
                  isFallback ? 'bg-amber-500' : 'bg-emerald-500'
                }`}
                title={handleItem.label}
              />
              <span
                data-testid={`split-handle-label-${handleItem.id}`}
                style={{ top: topPercent }}
                className={`pointer-events-none select-none absolute right-3 -translate-y-1/2 text-[10px] font-mono leading-none px-1 py-0.5 rounded border max-w-[56px] truncate ${
                  isFallback
                    ? 'bg-amber-500/15 border-amber-500/30 text-amber-300'
                    : 'bg-emerald-500/15 border-emerald-500/30 text-emerald-300'
                }`}
              >
                {handleItem.label}
              </span>
            </React.Fragment>
          );
        })}

      {/* Step output handles: dual valid + error (DLQ) for error-capable steps, single centered valid handle for structural steps */}
      {isStep && !isConditionalSplit && hasErrorHandle && (
        <>
          <Handle
            type="source"
            position={Position.Right}
            id="valid"
            style={{ top: '30%', right: -7, width: 14, height: 14, backgroundColor: '#10b981' }}
            className="w-3.5 h-3.5 border-none"
            title="Valid Path"
          />
          <Handle
            type="source"
            position={Position.Right}
            id="error"
            style={{ top: '70%', right: -7, width: 14, height: 14, backgroundColor: '#ef4444' }}
            className="w-3.5 h-3.5 border-none"
            title="Error Path"
          />
        </>
      )}
      {isStep && !isConditionalSplit && !hasErrorHandle && (
        <Handle
          type="source"
          position={Position.Right}
          id="valid"
          style={{ top: '50%', right: -7, width: 14, height: 14, backgroundColor: '#10b981' }}
          className="w-3.5 h-3.5 border-none"
          title="Valid Path"
        />
      )}
    </div>
  );
};
