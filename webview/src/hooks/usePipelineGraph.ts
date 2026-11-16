/**
 * Hook for Apache Spark pipeline graph state and mutation helpers.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Manages React Flow node and edge state, drag-and-drop ingestion,
 * connection validation, streaming mode and execution plan decoration,
 * topological auto-layout, property application, and node or edge deletion.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */
import { useCallback, useMemo, useState } from 'react';
import {
  useNodesState,
  useEdgesState,
  addEdge,
  type Node,
  type Edge,
  type Connection,
} from 'reactflow';
import { validateDagTopology, type DagValidationResult } from '../utils/dagValidator';
import {
  computeStreamingNodeSet,
  getConditionalSplitHandles,
  getNodeCapabilities,
  getNodeInputArity,
  getNodeSparkTransform,
  isConditionalSplitNode,
  isErrorCapableStep,
  isJoinNode,
  type ReadMode,
} from '../utils/sparkDagRules';
import { buildExecutionPlan, type ExecutionPlan } from '../utils/executionPlanner';
import { computeLayoutedNodes } from '../graph/layout';
import { createNodesForPaletteItem } from '../graph/nodeFactory';
import { isPubSubSourceNode } from '../graph/nodeKinds';
import { pipelineConfigReducer } from '../state/pipelineConfigReducer';

/**
 * Data payload stored within React Flow nodes for Apache Spark transforms.
 */
export interface PipelineNodeData {
  label: string;
  type: string;
  iconName: string;
  spark_transform?: string;
  system_instructions?: string;
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
}

export type PipelineNode = Node<PipelineNodeData>;

// Initial nodes and edges start empty so opening a .spark file never flashes placeholder nodes.
export const initialNodes: PipelineNode[] = [];
export const initialEdges: Edge[] = [];

/**
 * Options for configuring the {@link usePipelineGraph} hook.
 */
export interface UsePipelineGraphOptions {
  takeSnapshot: () => void;
  setPipelineConfig: React.Dispatch<React.SetStateAction<any>>;
  project?: (position: { x: number; y: number }) => { x: number; y: number };
  fitView?: (options?: any) => void;
  jobConfig?: Record<string, any>;
}

/**
 * Compares two optional ReadMode arrays for shallow element equality.
 */
function sameReadModes(a?: ReadMode[], b?: ReadMode[]): boolean {
  if (a === b) return true;
  if (!a || !b || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

/**
 * Evaluates whether a proposed connection obeys Spark node direction, handle, and arity rules.
 */
function canConnectEdge(
  connection: Connection,
  currentNodes: PipelineNode[],
  currentEdges: Edge[]
): boolean {
  if (!connection.source || !connection.target) {
    return false;
  }
  if (connection.source === connection.target) {
    return false;
  }

  const sourceNode = currentNodes.find((n) => n.id === connection.source);
  const targetNode = currentNodes.find((n) => n.id === connection.target);

  if (!sourceNode || !targetNode) {
    return false;
  }

  if (targetNode.data?.type === 'SOURCE') {
    return false;
  }

  if (sourceNode.data?.type === 'SINK') {
    return false;
  }

  if (isConditionalSplitNode(sourceNode as any)) {
    if (connection.sourceHandle) {
      const validHandles = getConditionalSplitHandles(sourceNode as any);
      if (!validHandles.some((h) => h.id === connection.sourceHandle)) {
        return false;
      }
    }
  } else if (
    sourceNode.data?.type === 'STEP' &&
    connection.sourceHandle === 'error' &&
    !isErrorCapableStep(sourceNode as any)
  ) {
    return false;
  }

  if (isJoinNode(targetNode as any)) {
    const targetHandle = connection.targetHandle;
    if (targetHandle !== 'left' && targetHandle !== 'right') {
      return false;
    }
    const handleOccupied = currentEdges.some(
      (e) => e.target === connection.target && e.targetHandle === targetHandle
    );
    return !handleOccupied;
  }

  const inputArity = getNodeInputArity(targetNode as any);
  if (inputArity === 'n') {
    const duplicateEdge = currentEdges.some(
      (e) =>
        e.source === connection.source &&
        e.target === connection.target &&
        (e.sourceHandle ?? null) === (connection.sourceHandle ?? null)
    );
    return !duplicateEdge;
  }

  const alreadyHasInput = currentEdges.some((e) => e.target === connection.target);
  return !alreadyHasInput;
}

/**
 * Manages pipeline graph nodes, edges, selection, validation, execution planning, layout, and mutations.
 */
export function usePipelineGraph({
  takeSnapshot,
  setPipelineConfig,
  project,
  fitView,
  jobConfig,
}: UsePipelineGraphOptions) {
  const [nodes, setNodes, onNodesChange] = useNodesState<PipelineNodeData>(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);

  const hasPubSubSource = nodes.some(isPubSubSourceNode);

  const handleNodesChange = useCallback(
    (changes: any) => {
      onNodesChange(changes);
      const requiresRebuild = changes.some((c: any) => c.type === 'add' || c.type === 'remove');
      if (requiresRebuild) {
        setPipelineConfig((prev: any) => pipelineConfigReducer(prev, { type: 'markDirty' }));
      }
    },
    [onNodesChange, setPipelineConfig]
  );

  const handleEdgesChange = useCallback(
    (changes: any) => {
      onEdgesChange(changes);
      const requiresRebuild = changes.some((c: any) => c.type === 'remove');
      if (requiresRebuild) {
        setPipelineConfig((prev: any) => pipelineConfigReducer(prev, { type: 'markDirty' }));
      }
    },
    [onEdgesChange, setPipelineConfig]
  );

  const isValidConnection = useCallback(
    (connection: Connection) => canConnectEdge(connection, nodes, edges),
    [nodes, edges]
  );

  const onConnect = useCallback(
    (params: Connection | Edge) => {
      if (!params.source || !params.target) {
        return;
      }
      const normalizedConnection: Connection = {
        source: params.source,
        target: params.target,
        sourceHandle: params.sourceHandle ?? null,
        targetHandle: params.targetHandle ?? null,
      };
      if (!canConnectEdge(normalizedConnection, nodes, edges)) {
        return;
      }
      takeSnapshot();
      setEdges((eds) =>
        canConnectEdge(normalizedConnection, nodes, eds) ? addEdge(params, eds) : eds
      );
      setPipelineConfig((prev: any) => pipelineConfigReducer(prev, { type: 'markDirty' }));
    },
    [nodes, edges, setEdges, takeSnapshot, setPipelineConfig]
  );

  const handleApplyProperties = useCallback(
    (nodeId: string, properties: any) => {
      takeSnapshot();
      setNodes((nds) =>
        nds.map((node) => {
          if (node.id === nodeId) {
            const { system_instructions, special_instructions, ...restProperties } = properties;
            const updatedData: any = {
              ...node.data,
              properties: restProperties,
            };
            const instructionsValue =
              system_instructions !== undefined ? system_instructions : special_instructions;
            if (instructionsValue !== undefined) {
              updatedData.system_instructions = instructionsValue;
            }
            return {
              ...node,
              data: updatedData,
            };
          }
          return node;
        })
      );
      setPipelineConfig((prev: any) => pipelineConfigReducer(prev, { type: 'markDirty' }));
    },
    [setNodes, takeSnapshot, setPipelineConfig]
  );

  const handleNodeLabelChange = useCallback(
    (nodeId: string, newLabel: string) => {
      takeSnapshot();
      setNodes((nds) =>
        nds.map((node) => {
          if (node.id === nodeId) {
            return {
              ...node,
              data: {
                ...node.data,
                label: newLabel,
              },
            };
          }
          return node;
        })
      );
      setPipelineConfig((prev: any) => pipelineConfigReducer(prev, { type: 'markDirty' }));
    },
    [setNodes, takeSnapshot, setPipelineConfig]
  );

  const triggerFitView = useCallback(() => {
    if (fitView) {
      setTimeout(() => {
        fitView({ duration: 300, padding: 1 });
      }, 100);
    }
  }, [fitView]);

  const onLayout = useCallback(() => {
    const newNodes = computeLayoutedNodes(nodes, edges);
    takeSnapshot();
    setNodes(newNodes);
    triggerFitView();
  }, [nodes, edges, setNodes, takeSnapshot, triggerFitView]);

  const onDragStart = useCallback(
    (
      event: React.DragEvent,
      nodeType: string,
      label: string,
      type: string,
      iconName: string,
      sparkTransform?: string
    ) => {
      event.dataTransfer.setData(
        'application/reactflow',
        JSON.stringify({ nodeType, label, type, iconName, sparkTransform })
      );
      event.dataTransfer.effectAllowed = 'move';
    },
    []
  );

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();

      const reactFlowBounds = document.querySelector('.react-flow')?.getBoundingClientRect();
      const dataStr = event.dataTransfer.getData('application/reactflow');

      if (!dataStr || !reactFlowBounds) {
        return;
      }

      const data = JSON.parse(dataStr);

      const rawPos = {
        x: event.clientX - reactFlowBounds.left,
        y: event.clientY - reactFlowBounds.top,
      };
      const position = project ? project(rawPos) : rawPos;

      const { nodes: newNodes, edges: newEdges } = createNodesForPaletteItem(
        data,
        position,
        nodes,
        edges
      );

      takeSnapshot();
      setNodes((prev) => [...prev, ...newNodes]);
      if (newEdges.length > 0) {
        setEdges((prev) => [...prev, ...newEdges]);
      }
      setPipelineConfig((prev: any) => pipelineConfigReducer(prev, { type: 'markDirty' }));
    },
    [project, setNodes, setEdges, nodes, edges, takeSnapshot, setPipelineConfig]
  );

  const handleAddNode = useCallback(
    (label: string, type: string, iconName: string, sparkTransform?: string) => {
      const nodeCount = nodes.length;
      const position = {
        x: 100 + (nodeCount % 6) * 60,
        y: 100 + (nodeCount % 6) * 60,
      };

      const { nodes: newNodes, edges: newEdges } = createNodesForPaletteItem(
        { label, type, iconName, sparkTransform, nodeType: 'pipeline' },
        position,
        nodes,
        edges
      );

      const nextEdges = newEdges.length > 0 ? [...edges, ...newEdges] : edges;

      takeSnapshot();
      setNodes((prev) => computeLayoutedNodes([...prev, ...newNodes], nextEdges));
      if (newEdges.length > 0) {
        setEdges((prev) => [...prev, ...newEdges]);
      }
      setPipelineConfig((prev: any) => pipelineConfigReducer(prev, { type: 'markDirty' }));
      triggerFitView();
    },
    [nodes, edges, setNodes, setEdges, takeSnapshot, setPipelineConfig, triggerFitView]
  );

  const deleteSelected = useCallback(() => {
    if (selectedNodeId) {
      takeSnapshot();
      setNodes((nds) => nds.filter((node) => node.id !== selectedNodeId));
      setEdges((eds) =>
        eds.filter((edge) => edge.source !== selectedNodeId && edge.target !== selectedNodeId)
      );
      setSelectedNodeId(null);
      setPipelineConfig((prev: any) => pipelineConfigReducer(prev, { type: 'markDirty' }));
    } else if (selectedEdgeId) {
      takeSnapshot();
      setEdges((eds) => eds.filter((edge) => edge.id !== selectedEdgeId));
      setSelectedEdgeId(null);
      setPipelineConfig((prev: any) => pipelineConfigReducer(prev, { type: 'markDirty' }));
    }
  }, [selectedNodeId, selectedEdgeId, setNodes, setEdges, takeSnapshot, setPipelineConfig]);

  const dagValidation = useMemo<DagValidationResult>(
    () =>
      validateDagTopology(
        nodes as any,
        edges as any,
        jobConfig?.checkpointRoot !== undefined ? { jobConfig } : undefined
      ),
    [nodes, edges, jobConfig]
  );

  const streamingPropagation = useMemo(
    () => computeStreamingNodeSet(nodes as any, edges as any),
    [nodes, edges]
  );

  const executionPlan = useMemo<ExecutionPlan>(
    () => buildExecutionPlan(nodes as any, edges as any, jobConfig),
    [nodes, edges, jobConfig]
  );

  const pipelineMode: ReadMode = executionPlan.pipelineMode;

  const displayNodes = useMemo(() => {
    const cycleSet = new Set(dagValidation.cycleNodeIds);
    const orphanSet = new Set(dagValidation.orphanNodeIds);
    const missingUpstreamSet = new Set(dagValidation.missingUpstreamNodeIds || []);
    const invalidUpstreamSet = new Set(dagValidation.invalidUpstreamNodeIds || []);
    const nodeErrorsMap = dagValidation.nodeErrors;
    const nodeWarningsMap = dagValidation.nodeWarnings;

    return nodes.map((node) => {
      const nodeErrors = nodeErrorsMap?.[node.id];
      const nodeWarnings = nodeWarningsMap?.[node.id];
      const isMissingUpstream = missingUpstreamSet.has(node.id);
      const isInvalidUpstream = invalidUpstreamSet.has(node.id);
      const hasCycle = cycleSet.has(node.id);
      const hasValidationError =
        Boolean(nodeErrors && nodeErrors.length > 0) || isMissingUpstream || isInvalidUpstream;
      const isOrphan = orphanSet.has(node.id);

      const matchedError = dagValidation.errors.find(
        (err) => err.includes(node.id) || (node.data.label && err.includes(node.data.label))
      );

      const validationError =
        nodeErrors && nodeErrors.length > 0
          ? nodeErrors[0]
          : hasCycle
          ? dagValidation.errors.find((err) => err.includes('Cyclic')) || dagValidation.errors[0]
          : isMissingUpstream
          ? matchedError || 'Node is missing a required upstream input connection.'
          : isInvalidUpstream
          ? matchedError || 'Node has an invalid upstream input connection.'
          : undefined;

      const validationWarning =
        nodeWarnings && nodeWarnings.length > 0
          ? nodeWarnings[0]
          : isOrphan
          ? 'Orphan node: disconnected from pipeline'
          : undefined;

      const nodePlan = executionPlan.nodes[node.id];
      const executionMode: ReadMode =
        nodePlan?.executionMode ??
        (streamingPropagation.streamingNodeIds.has(node.id) ? 'streaming' : 'batch');
      const isCached = Boolean(nodePlan?.isCached);
      const isForeachBatchSink = Boolean(nodePlan?.isForeachBatchSink);

      let supportedReadModes = node.data.supportedReadModes;
      if (node.data.type === 'SOURCE' && !supportedReadModes) {
        supportedReadModes = getNodeCapabilities(
          getNodeSparkTransform(node as any),
          'SOURCE'
        ).supportedReadModes;
      }

      if (
        node.data.hasCycle === hasCycle &&
        node.data.hasValidationError === hasValidationError &&
        node.data.isOrphan === isOrphan &&
        node.data.validationError === validationError &&
        node.data.validationWarning === validationWarning &&
        node.data.executionMode === executionMode &&
        node.data.isCached === isCached &&
        node.data.isForeachBatchSink === isForeachBatchSink &&
        sameReadModes(node.data.supportedReadModes, supportedReadModes)
      ) {
        return node;
      }

      return {
        ...node,
        data: {
          ...node.data,
          hasCycle,
          hasValidationError,
          isOrphan,
          validationError,
          validationWarning,
          executionMode,
          isCached,
          isForeachBatchSink,
          ...(supportedReadModes ? { supportedReadModes } : {}),
        },
      };
    });
  }, [nodes, dagValidation, executionPlan, streamingPropagation]);

  const displayEdges = useMemo(() => {
    const cycleEdgeSet = new Set(dagValidation.cycleEdgeIds);
    const cycleNodeSet = new Set(dagValidation.cycleNodeIds);
    const invalidUpstreamSet = new Set(dagValidation.invalidUpstreamNodeIds || []);

    return edges.map((edge) => {
      const isCycleEdge =
        cycleEdgeSet.has(edge.id) ||
        (cycleNodeSet.has(edge.source) && cycleNodeSet.has(edge.target));
      const isInvalidUpstreamEdge = invalidUpstreamSet.has(edge.target);

      if (isCycleEdge) {
        return {
          ...edge,
          animated: true,
          style: {
            ...edge.style,
            stroke: '#f43f5e',
            strokeWidth: 2.5,
          },
        };
      }
      if (isInvalidUpstreamEdge) {
        return {
          ...edge,
          style: {
            ...edge.style,
            stroke: '#ef4444',
            strokeDasharray: '5,5',
            strokeWidth: 2,
          },
        };
      }
      return edge;
    });
  }, [edges, dagValidation]);

  return {
    nodes,
    setNodes,
    edges,
    setEdges,
    displayNodes,
    displayEdges,
    dagValidation,
    executionPlan,
    pipelineMode,
    onNodesChange,
    onEdgesChange,
    selectedNodeId,
    setSelectedNodeId,
    selectedEdgeId,
    setSelectedEdgeId,
    hasPubSubSource,
    handleNodesChange,
    handleEdgesChange,
    isValidConnection,
    onConnect,
    handleApplyProperties,
    handleNodeLabelChange,
    onLayout,
    onDrop,
    handleAddNode,
    onDragStart,
    onDragOver,
    deleteSelected,
  };
}
