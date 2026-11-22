/**
 * App Component
 * 
 * Author: Mike Nimer <mikenimer@google.com>
 * 
 * Purpose:
 * Main application component for the visual Apache Spark Pipeline Designer canvas and property inspector.
 * Decomposed (A6) into custom hooks for persistence (usePipelinePersistence), graph mutations
 * (usePipelineGraph), and extension host IPC (useVsCodeBridge).
 * 
 * Architectural Decisions:
 * - Uses React Flow for interactive drag-and-drop pipeline graph editing.
 * - Initial state for nodes and edges is empty array to prevent phantom pipeline flash when opening .spark files.
 * - Nodes are passed untouched to ReactFlow, and node label edits are injected via stable LabelChangeContext.
 * - Autosave debounced at 2s with dirty-check byte comparison (P6).
 * - Adheres to 8pt grid system and modern React state/effects.
 */

import React, { useCallback, useState, useEffect, useRef, useMemo, useContext, createContext } from 'react';

import ReactFlow, {
  Background,
  ReactFlowProvider,
  useReactFlow,
  type Edge,
  type Node,
} from 'reactflow';
import 'reactflow/dist/style.css';
import './App.css';
import { PipelineNode, type PipelineNodeProps } from './components/PipelineNode';
import { PropertyPane } from './components/PropertyPane';
import { ErrorBoundary } from './components/common/ErrorBoundary';
import { NodesCard } from './components/NodesCard';
import { CanvasToolbar } from './components/CanvasToolbar';
import { BottomPanel, BOTTOM_PANEL_HEADER_HEIGHT } from './components/BottomPanel/BottomPanel';
import {
  BottomPanelProvider,
  type BottomPanelContextValue,
} from './components/BottomPanel/BottomPanelContext';
import { DEFAULT_CODEGEN_MODEL } from './components/BottomPanel/SourceCodeTab';

import { usePipelinePersistence } from './hooks/usePipelinePersistence';
import { usePipelineGraph, type PipelineNodeData } from './hooks/usePipelineGraph';
import { useVsCodeBridge, PLACEHOLDER_PROJECT_ID, type PipelineConfig } from './hooks/useVsCodeBridge';
import { pipelineConfigReducer } from './state/pipelineConfigReducer';
import { useRuntimeVariables, DEFAULT_RUNTIME_VARIABLES } from './hooks/useRuntimeVariables';
import { usePipelineExecution } from './hooks/usePipelineExecution';
import { usePanelResize } from './hooks/usePanelResize';
import { useUndoRedoHistory, isEditableTarget } from './hooks/useUndoRedoHistory';
import { getVsCodeApi, postToHost } from './bridge/vscodeApi';
import {
  evaluateSdpCompatibility,
  normalizeSdpJobConfig,
  type SdpBlockingIssue,
} from './utils/sdpPlanner';
import { isRuntimeSdpCapable } from './utils/versionMatrix';
import {
  normalizeSparkRunner,
  type PipelineParadigm,
  type SdpJobConfigFields,
} from './utils/sharedTypes';
import { AlertTriangle, X } from 'lucide-react';

export type { PipelineNodeData };

const vscode = getVsCodeApi() ?? null;

/**
 * React Context providing the stable onLabelChange callback to canvas nodes.
 */
const LabelChangeContext = createContext<((id: string, newLabel: string) => void) | null>(null);

/**
 * CanvasPipelineNode
 *
 * Stable wrapper around PipelineNode that injects the memoized onLabelChange callback
 * from React context. This avoids constructing new node/data objects on every App render,
 * preserving React Flow referential memoization and preventing canvas drag jank.
 */
export const CanvasPipelineNode: React.FC<PipelineNodeProps> = React.memo((props) => {
  const onLabelChange = useContext(LabelChangeContext);
  const data = useMemo(() => {
    if (!onLabelChange) return props.data;
    return {
      ...props.data,
      onLabelChange,
    };
  }, [props.data, onLabelChange]);

  return <PipelineNode {...props} data={data} />;
});

const nodeTypes = {
  pipeline: CanvasPipelineNode,
};

type AppJobConfig = BottomPanelContextValue['jobConfig']['value'] &
  SdpJobConfigFields &
  Record<string, unknown>;

/**
 * Persisted VS Code webview state shape for active palette tab tracking.
 */
interface PersistedWebviewState {
  activeNodePaletteTab?: string | null;
  [key: string]: unknown;
}

/**
 * State snapshot recorded for canvas undo and redo operations.
 */
interface PipelineSnapshot {
  nodes: Node<PipelineNodeData>[];
  edges: Edge[];
  runtimeVariables: { key: string; value: string }[];
  jobConfig: AppJobConfig;
  pipelineConfig: PipelineConfig;
}

/**
 * Root designer workspace component hosting the React Flow canvas, node palette, property panel, and bottom panel.
 *
 * @returns The rendered Apache Spark pipeline designer application.
 */
function App() {
  const { project, fitView } = useReactFlow();

  // Navigation and active tab state
  const [activeTab, setActiveTab] = useState<string | null>(() => {
    try {
      const vsState = (getVsCodeApi()?.getState?.() as PersistedWebviewState | undefined)
        ?.activeNodePaletteTab;
      if (vsState !== undefined) {
        return vsState;
      }
      if (typeof localStorage !== 'undefined') {
        const local = localStorage.getItem('spark_node_palette_active_tab');
        if (local !== null) {
          return local === '__NULL__' ? null : local;
        }
      }
    } catch {
      // Ignore persistence read error
    }
    return 'SOURCES';
  });

  const handleSetActiveTab = (tab: string | null) => {
    setActiveTab(tab);
    try {
      const vs = getVsCodeApi();
      if (vs?.setState) {
        const current = (vs.getState?.() as PersistedWebviewState | undefined) ?? {};
        vs.setState({ ...current, activeNodePaletteTab: tab });
      }
      if (typeof localStorage !== 'undefined') {
        localStorage.setItem('spark_node_palette_active_tab', tab === null ? '__NULL__' : tab);
      }
    } catch {
      // Ignore persistence write error
    }
  };

  const [bottomTab, setBottomTab] = useState('SOURCE_CODE');

  // Job and pipeline configuration state
  const [jobConfig, setJobConfig] = useState<AppJobConfig>({
    runner: 'Local',
    useSparkConnect: false,
    sessionTemplate: '',
    clusterName: '',
    customJarUris: '',
    sparkJars: '',
    runtimeVersion: '4.0',
    tier: 'standard',
    serverlessTier: 'standard',
    depsBucket: '',
    serviceAccount: '',
    batchTtlMinutes: '',
    checkpointRoot: '',
    machineType: 'n1-standard-1',
    minInstances: 1,
    maxInstances: 10,
    gcsStagingPath: '',
    network: '',
    subnetwork: '',
    jobId: '',
    region: 'us-central1',
    projectId: '',
  });
  const [projectId, setProjectId] = useState(PLACEHOLDER_PROJECT_ID);
  const [filename, setFilename] = useState('default_job');
  const [pipelineConfig, setPipelineConfig] = useState<PipelineConfig>({
    language: 'Python',
    outputFolder: '',
    needsRebuild: false,
    paradigm: 'imperative',
  });
  const [sdpIssueState, setSdpIssueState] = useState<{
    revision: number;
    issues: SdpBlockingIssue[];
  } | null>(null);
  const activeParadigm: PipelineParadigm =
    pipelineConfig.paradigm === 'sdp' ? 'sdp' : 'imperative';

  // Snapshot ref for useUndoRedoHistory (finding F10)
  const snapshotRef = useRef<PipelineSnapshot>({
    nodes: [],
    edges: [],
    runtimeVariables: DEFAULT_RUNTIME_VARIABLES.map((v) => ({ ...v })),
    jobConfig,
    pipelineConfig,
  });

  const restoreRef = useRef<(snapshot: PipelineSnapshot) => void>(() => {});

  const { takeSnapshot, undo, redo, canUndo, canRedo } = useUndoRedoHistory<PipelineSnapshot>({
    capture: useCallback(() => snapshotRef.current, []),
    restore: useCallback((snapshot: PipelineSnapshot) => {
      restoreRef.current(snapshot);
    }, []),
  });

  const {
    runtimeVariables,
    setRuntimeVariables,
    addVariable,
    updateVariable,
    deleteVariable,
  } = useRuntimeVariables({
    takeSnapshot,
    setPipelineConfig,
  });

  // Hook 1: Graph state and mutation helpers (A6)
  const {
    nodes,
    setNodes,
    edges,
    setEdges,
    displayNodes,
    displayEdges,
    dagValidation,
    pipelineMode,
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
  } = usePipelineGraph({
    takeSnapshot,
    setPipelineConfig,
    project,
    fitView,
    jobConfig,
    paradigm: activeParadigm,
  });

  // Keep snapshotRef and restoreRef synchronised with latest graph and config state
  useEffect(() => {
    snapshotRef.current = { nodes, edges, runtimeVariables, jobConfig, pipelineConfig };
  }, [nodes, edges, runtimeVariables, jobConfig, pipelineConfig]);

  useEffect(() => {
    restoreRef.current = (snapshot: PipelineSnapshot) => {
      setNodes(snapshot.nodes);
      setEdges(snapshot.edges);
      setRuntimeVariables(snapshot.runtimeVariables);
      setJobConfig(snapshot.jobConfig);
      setPipelineConfig((prev) => pipelineConfigReducer(prev, { type: 'reset', config: snapshot.pipelineConfig }));
    };
  }, [setNodes, setEdges, setRuntimeVariables, setJobConfig, setPipelineConfig]);

  const handleBridgeSetJobConfig: BottomPanelContextValue['jobConfig']['set'] = useCallback(
    (update) => {
      setJobConfig((prev: AppJobConfig) => {
        const next = typeof update === 'function' ? update(prev) : update;
        return {
          ...next,
          runner: normalizeSparkRunner(next?.runner),
        } as AppJobConfig;
      });
    },
    []
  );

  // Hook 2: VS Code IPC bridge and message handler (A6)
  const {
    isLoaded,
    loadRevision,
    samples,
    generatedCode,
    subnetworks,
    clusters,
    sessionTemplates,
    folderFiles,
    selectedFileName,
    selectedFileContent,
    isGenerating,
    generationProgress,
    tokenUsage,
    refreshFolderFiles,
    handleOpenSample,
    handlePickFolder,
    handleSelectFile,
    handleRegenerate: ipcRegenerate,
  } = useVsCodeBridge({
    setNodes,
    setEdges,
    setRuntimeVariables,
    setJobConfig: handleBridgeSetJobConfig,
    pipelineConfig,
    setPipelineConfig,
    filename,
    setFilename,
    projectId,
    setProjectId,
    fitView,
    vscode,
  });

  const sdpCompatibilityIssues =
    sdpIssueState && sdpIssueState.revision === loadRevision ? sdpIssueState.issues : null;

  // Hook 3: Pipeline Persistence with 2-second debounce and dirty-check (P6, A6)
  const { lastSavedYamlRef, serializePipeline } = usePipelinePersistence({
    nodes,
    edges,
    runtimeVariables,
    jobConfig,
    pipelineConfig,
    isLoaded,
    loadRevision,
    vscode,
    debounceMs: 2000,
  });

  console.log('App rendering, isLoaded=', isLoaded, 'nodes=', nodes.length);


  // Read files whenever bottom tab switches to SOURCE_CODE
  useEffect(() => {
    if (bottomTab === 'SOURCE_CODE' && isLoaded && !pipelineConfig.needsRebuild) {
      refreshFolderFiles();
    }
  }, [bottomTab, isLoaded, pipelineConfig.needsRebuild, refreshFolderFiles]);

  // Hook 4: Pipeline execution state, log streaming, and IPC handlers
  const {
    pipelineLogs,
    pipelineStatus,
    runnerCommand,
    handleRunNow,
    handleStopPipeline,
    handleClearLogs,
  } = usePipelineExecution({
    jobConfig,
    runtimeVariables,
    pipelineConfig,
    setJobConfig,
    projectId,
    vscode,
  });

  // Keyboard navigation and shortcuts (Undo, Redo, Delete)
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (isEditableTarget(event.target) || isEditableTarget(document.activeElement)) {
        return;
      }

      if (event.key === 'Escape' && sdpCompatibilityIssues && sdpCompatibilityIssues.length > 0) {
        event.preventDefault();
        setSdpIssueState(null);
        return;
      }

      const modifier = event.ctrlKey || event.metaKey;

      if (modifier && event.key === 'z') {
        event.preventDefault();
        undo();
        return;
      }
      if (modifier && (event.key === 'y' || (event.shiftKey && event.key === 'Z'))) {
        event.preventDefault();
        redo();
        return;
      }

      if (event.key === 'Delete' || event.key === 'Backspace') {
        deleteSelected();
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [undo, redo, deleteSelected, sdpCompatibilityIssues]);

  const handleParadigmChange = useCallback(
    (nextParadigm: PipelineParadigm) => {
      if (nextParadigm === activeParadigm) {
        return;
      }
      if (nextParadigm === 'imperative') {
        setSdpIssueState(null);
        takeSnapshot();
        setJobConfig((prev: AppJobConfig) => {
          if (prev.runtimeVersion === '4.2-connect') {
            const next = { ...prev, useSparkConnect: false };
            delete (next as Record<string, unknown>).runtimeVersion;
            return next;
          }
          return prev;
        });
        setPipelineConfig((prev) =>
          pipelineConfigReducer(prev, { type: 'setParadigm', paradigm: 'imperative' })
        );
        return;
      }

      const compatibility = evaluateSdpCompatibility(nodes, edges, jobConfig, {
        enforceRuntimeVersion: false,
      });
      if (!compatibility.isCompatible || compatibility.blockingIssues.length > 0) {
        setSdpIssueState({ revision: loadRevision, issues: compatibility.blockingIssues });
        return;
      }

      setSdpIssueState(null);
      takeSnapshot();
      setJobConfig((prev: AppJobConfig) => {
        const normalized = normalizeSdpJobConfig(prev);
        const normalizedRunner = normalizeSparkRunner(normalized.runner);
        const normalizedVersion =
          typeof normalized.runtimeVersion === 'string' ? normalized.runtimeVersion : undefined;
        if (!isRuntimeSdpCapable(normalizedRunner, normalizedVersion)) {
          return normalizedRunner === 'Local'
            ? {
                ...normalized,
                runner: 'Local',
                runtimeVersion: '4.2-connect',
                useSparkConnect: true,
              }
            : { ...normalized, runner: normalizedRunner, runtimeVersion: '4.0' };
        }
        return { ...normalized, runner: normalizedRunner };
      });
      setPipelineConfig((prev) =>
        pipelineConfigReducer(prev, { type: 'setParadigm', paradigm: 'sdp' })
      );
    },
    [activeParadigm, nodes, edges, jobConfig, loadRevision, takeSnapshot]
  );

  /**
   * Job config update wrapper that records an undo snapshot and flags needsRebuild on a
   * meaningful change.
   *
   * Accepts either a complete config object or an updater function. Children MUST pass an
   * updater: building `{...jobConfig, field}` from a props snapshot and returning it
   * wholesale means any other field changed since that render is silently reverted, because
   * the object replaces state entirely rather than merging into `prev`. That is how
   * `runner` reverted from `ServerlessSpark` to `Local` when a stale snapshot replaced `prev`.
   */
  const handleSetJobConfig: BottomPanelContextValue['jobConfig']['set'] = useCallback(
    (update) => {
      takeSnapshot();
      setJobConfig((prev: AppJobConfig) => {
        // Resolve the updater against the freshest state so a stale snapshot captured in a
        // child render cannot clobber sibling fields.
        const rawConfig =
          typeof update === 'function'
            ? (update as (p: AppJobConfig) => AppJobConfig)(prev)
            : (update as AppJobConfig);
        const config: AppJobConfig = {
          ...rawConfig,
          runner: normalizeSparkRunner(rawConfig?.runner),
        };
        const configChanged =
          prev.runner !== config.runner ||
          prev.useSparkConnect !== config.useSparkConnect ||
          prev.sessionTemplate !== config.sessionTemplate ||
          prev.clusterName !== config.clusterName ||
          prev.region !== config.region ||
          prev.customJarUris !== config.customJarUris ||
          prev.sparkJars !== config.sparkJars ||
          prev.runtimeVersion !== config.runtimeVersion ||
          prev.tier !== config.tier ||
          prev.serverlessTier !== config.serverlessTier ||
          prev.depsBucket !== config.depsBucket ||
          prev.gcsStagingPath !== config.gcsStagingPath ||
          prev.checkpointRoot !== config.checkpointRoot ||
          prev.defaultTriggerType !== config.defaultTriggerType ||
          prev.defaultTriggerInterval !== config.defaultTriggerInterval ||
          prev.serviceAccount !== config.serviceAccount ||
          prev.batchTtlMinutes !== config.batchTtlMinutes ||
          prev.machineType !== config.machineType ||
          prev.minInstances !== config.minInstances ||
          prev.maxInstances !== config.maxInstances ||
          prev.network !== config.network ||
          prev.subnetwork !== config.subnetwork ||
          prev.sdpStorage !== config.sdpStorage ||
          prev.sdpCatalog !== config.sdpCatalog ||
          prev.sdpDatabase !== config.sdpDatabase ||
          prev.sdpBigLakeMode !== config.sdpBigLakeMode ||
          prev.sdpBigLakeProject !== config.sdpBigLakeProject ||
          prev.sdpBigLakeCatalogId !== config.sdpBigLakeCatalogId ||
          prev.sdpBigLakeWarehouseBucket !== config.sdpBigLakeWarehouseBucket ||
          prev.sdpCreateNamespace !== config.sdpCreateNamespace ||
          JSON.stringify(prev.sdpStaticSparkConf ?? '') !==
            JSON.stringify(config.sdpStaticSparkConf ?? '');

        if (configChanged) {
          setPipelineConfig((p) => pipelineConfigReducer(p, { type: 'markDirty' }));
        }
        return config;
      });
    },
    [takeSnapshot]
  );

  const isJobIdValid = (value: string) => {
    const regex = /^[a-z]([-a-z0-9]*[a-z0-9])?$/;
    return regex.test(value);
  };

  const handleOpenConsole = useCallback((url: string) => {
    const vs = getVsCodeApi();
    if (vs) {
      vs.postMessage({
        command: 'openExternal',
        url
      });
    } else {
      window.open(url, '_blank');
    }
  }, []);

  const handleStopGeneration = useCallback(() => {
    postToHost({
      command: 'stopCodeGeneration'
    });
  }, []);

  const [useSdkForUnsupportedNodes, setUseSdkForUnsupportedNodes] = useState(false);
  const [enableReviewer, setEnableReviewer] = useState(false);
  const [selectedModel, setSelectedModel] = useState<string>(DEFAULT_CODEGEN_MODEL);

  const isCodegenValid = dagValidation.isValid;
  const combinedValidationErrors = useMemo(
    () => Array.from(new Set(dagValidation.errors)),
    [dagValidation.errors]
  );

  const handleRegenerate = useCallback((options?: { enableReviewer?: boolean; model?: string }) => {
    if (!isCodegenValid) {
      console.warn('Blocked regenerateCode: validation failed', combinedValidationErrors);
      return;
    }
    const effectiveJobConfig =
      activeParadigm === 'sdp' ? normalizeSdpJobConfig(jobConfig) : jobConfig;
    if (
      activeParadigm === 'sdp' &&
      (effectiveJobConfig.runner !== jobConfig.runner ||
        effectiveJobConfig.runtimeVersion !== jobConfig.runtimeVersion ||
        effectiveJobConfig.useSparkConnect !== jobConfig.useSparkConnect)
    ) {
      setJobConfig(effectiveJobConfig);
    }
    lastSavedYamlRef.current = serializePipeline({
      nodes,
      edges,
      runtimeVariables,
      jobConfig: effectiveJobConfig,
      pipelineConfig,
    });
    const resolvedEnableReviewer =
      typeof options?.enableReviewer === 'boolean' ? options.enableReviewer : enableReviewer;
    const resolvedModel = options?.model?.trim() || selectedModel || DEFAULT_CODEGEN_MODEL;
    ipcRegenerate({
      nodes,
      edges,
      runtimeVariables,
      jobConfig: effectiveJobConfig,
      enableReviewer: resolvedEnableReviewer,
      model: resolvedModel,
    });
  }, [ipcRegenerate, nodes, edges, runtimeVariables, jobConfig, pipelineConfig, activeParadigm, isCodegenValid, combinedValidationErrors, lastSavedYamlRef, serializePipeline, enableReviewer, selectedModel]);

  // Resize state for Bottom Panel (minimized to 40px tab header by default and expands to 80% on tab click or toggle)
  const [isBottomPanelExpanded, setIsBottomPanelExpanded] = useState(false);
  const [hasCustomBottomPanelHeight, setHasCustomBottomPanelHeight] = useState(false);
  const mainContentRef = useRef<HTMLDivElement>(null);

  const {
    size: bottomPanelHeight,
    setSize: setBottomPanelHeight,
    startResize: handleMouseDown,
  } = usePanelResize({
    axis: 'y',
    initialSize: BOTTOM_PANEL_HEADER_HEIGHT,
    cursor: 'row-resize',
    computeSize: (e, startSize) => {
      const container = mainContentRef.current;
      if (!container) return startSize;
      const containerRect = container.getBoundingClientRect();
      let newHeight = containerRect.bottom - e.clientY;

      const minHeight = BOTTOM_PANEL_HEADER_HEIGHT;
      const maxHeight = Math.max(minHeight, containerRect.height - 4);
      if (newHeight < minHeight) newHeight = minHeight;
      if (newHeight > maxHeight) newHeight = maxHeight;

      return newHeight;
    },
  });

  const isEffectiveBottomPanelExpanded = hasCustomBottomPanelHeight
    ? bottomPanelHeight > BOTTOM_PANEL_HEADER_HEIGHT
    : isBottomPanelExpanded;

  const handleExpandBottomPanel = useCallback(() => {
    setHasCustomBottomPanelHeight(false);
    setIsBottomPanelExpanded(true);
  }, []);

  const handleToggleBottomPanelExpand = useCallback(() => {
    const currentlyExpanded = hasCustomBottomPanelHeight
      ? bottomPanelHeight > BOTTOM_PANEL_HEADER_HEIGHT
      : isBottomPanelExpanded;
    setHasCustomBottomPanelHeight(false);
    setIsBottomPanelExpanded(!currentlyExpanded);
  }, [hasCustomBottomPanelHeight, bottomPanelHeight, isBottomPanelExpanded]);

  const handleBottomDividerMouseDown = useCallback(
    (e: React.MouseEvent) => {
      if (!hasCustomBottomPanelHeight && mainContentRef.current) {
        const rect = mainContentRef.current.getBoundingClientRect();
        const currentPixelHeight = isBottomPanelExpanded
          ? Math.round(rect.height * 0.8)
          : BOTTOM_PANEL_HEADER_HEIGHT;
        setBottomPanelHeight(currentPixelHeight);
      }
      setHasCustomBottomPanelHeight(true);
      handleMouseDown(e);
    },
    [hasCustomBottomPanelHeight, isBottomPanelExpanded, setBottomPanelHeight, handleMouseDown]
  );

  // Property panel resizing state (default 320px = w-80, minimum 320px)
  // Note: Width is now persisted to localStorage on mouseup via onResizeEnd (previously every mousemove) — intentional.
  const {
    size: propertyPanelWidth,
    isResizing: isPropertyPanelResizing,
    startResize: handlePropertyPanelResizeStart,
  } = usePanelResize({
    axis: 'x',
    initialSize: () => {
      if (typeof window !== 'undefined' && window.localStorage) {
        try {
          const saved = Number(window.localStorage.getItem('spark_property_panel_width'));
          if (!isNaN(saved) && saved >= 320) {
            return saved;
          }
        } catch {
          // Ignore localStorage access restrictions in iframe
        }
      }
      return 320;
    },
    cursor: 'col-resize',
    computeSize: (e) => {
      const container = mainContentRef.current;
      const containerRect = container?.getBoundingClientRect();
      const rightEdge = (containerRect && containerRect.right > 0) ? containerRect.right : (window.innerWidth || 1024);
      let newWidth = rightEdge - e.clientX;

      const minWidth = 320;
      const containerWidth = (containerRect && containerRect.width > 0) ? containerRect.width : (window.innerWidth || 1024);
      const maxWidth = Math.max(minWidth, containerWidth - 150);

      if (newWidth < minWidth) newWidth = minWidth;
      if (newWidth > maxWidth) newWidth = maxWidth;

      return newWidth;
    },
    onResizeEnd: (size) => {
      if (typeof window !== 'undefined' && window.localStorage) {
        try {
          window.localStorage.setItem('spark_property_panel_width', String(size));
        } catch {
          // Ignore localStorage error
        }
      }
    },
  });

  /**
   * Closes any open property pane when the user interacts with any field or button in the bottom panel tabs.
   */
  const handleBottomPanelInteract = useCallback(() => {
    if (selectedNodeId) {
      setSelectedNodeId(null);
    }
    if (selectedEdgeId) {
      setSelectedEdgeId(null);
    }
  }, [selectedNodeId, selectedEdgeId, setSelectedNodeId, setSelectedEdgeId]);

  // Context value bundle for BottomPanel and its child tabs (Phase 5)
  const bottomPanelContextValue = useMemo<BottomPanelContextValue>(
    () => ({
      execution: {
        logs: pipelineLogs,
        status: pipelineStatus,
        runnerCommand,
        run: handleRunNow,
        stop: handleStopPipeline,
        clear: handleClearLogs,
        openConsole: handleOpenConsole,
      },
      jobConfig: {
        value: jobConfig,
        set: handleSetJobConfig,
        isJobIdValid,
        subnetworks,
        clusters,
        sessionTemplates,
        projectId,
      },
      runtimeVariables: {
        variables: runtimeVariables,
        addVariable,
        updateVariable,
        deleteVariable,
      },
      pipelineConfig: {
        config: pipelineConfig,
        setLanguage: (lang) =>
          setPipelineConfig((prev) =>
            pipelineConfigReducer(prev, { type: 'setLanguage', language: lang })
          ),
        setOutputFolder: (folder) =>
          setPipelineConfig((prev) =>
            pipelineConfigReducer(prev, { type: 'setOutputFolder', folder })
          ),
        setPipelineConfig,
      },
      codegen: {
        generatedCode,
        regenerate: handleRegenerate,
        stopGeneration: handleStopGeneration,
        isGenerating,
        generationProgress,
        files: folderFiles,
        selectedFile: selectedFileName,
        selectedFileContent,
        pickFolder: handlePickFolder,
        selectFile: handleSelectFile,
        isGraphValid: isCodegenValid,
        validationErrors: combinedValidationErrors,
        nodes: displayNodes,
        tokenUsage,
        useSdkForUnsupportedNodes,
        setUseSdkForUnsupportedNodes,
        enableReviewer,
        setEnableReviewer,
        selectedModel,
        setSelectedModel,
      },
    }),
    [
      pipelineLogs,
      pipelineStatus,
      runnerCommand,
      handleRunNow,
      handleStopPipeline,
      handleClearLogs,
      handleOpenConsole,
      jobConfig,
      handleSetJobConfig,
      subnetworks,
      clusters,
      sessionTemplates,
      projectId,
      runtimeVariables,
      addVariable,
      updateVariable,
      deleteVariable,
      pipelineConfig,
      setPipelineConfig,
      generatedCode,
      handleRegenerate,
      handleStopGeneration,
      isGenerating,
      generationProgress,
      folderFiles,
      selectedFileName,
      selectedFileContent,
      handlePickFolder,
      handleSelectFile,
      isCodegenValid,
      combinedValidationErrors,
      displayNodes,
      tokenUsage,
      useSdkForUnsupportedNodes,
      enableReviewer,
      selectedModel,
    ]
  );

  return (
    <div className="flex h-screen w-screen text-[var(--vscode-text-primary)] bg-[var(--vscode-canvas-bg)] overflow-hidden">
      {/* Main Content Area (Canvas + Bottom Panel) */}
      <div ref={mainContentRef} className="flex-1 flex flex-col min-h-0 overflow-hidden relative">
        {/* Diagram Canvas Area */}
        <div className="flex-1 relative min-h-0 overflow-hidden flex">
          {/* Canvas */}
          <div
            className="flex-1 relative h-full min-w-0"
            style={{
              marginRight: selectedNodeId ? `${propertyPanelWidth}px` : 0,
              transition: isPropertyPanelResizing ? 'none' : 'margin-right 150ms ease-out',
            }}
          >
            {/* Floating Card on Left */}
            <NodesCard
              activeTab={activeTab}
              setActiveTab={handleSetActiveTab}
              nodesCount={nodes.length}
              hasPubSubSource={hasPubSubSource}
              onDragStart={onDragStart}
              onAddNode={handleAddNode}
              samples={samples}
              onOpenSample={handleOpenSample}
              paradigm={activeParadigm}
            />

            <LabelChangeContext.Provider value={handleNodeLabelChange}>
              <ReactFlow
                nodes={displayNodes}
                edges={displayEdges}
                onNodesChange={handleNodesChange}
                onEdgesChange={handleEdgesChange}
                onConnect={onConnect}
                nodeTypes={nodeTypes}
                isValidConnection={isValidConnection}
                connectionRadius={32}
                fitView
                onNodeClick={(_, node) => {
                  setSelectedNodeId(node.id);
                  setSelectedEdgeId(null);
                }}
                onEdgeClick={(_, edge) => {
                  setSelectedEdgeId(edge.id);
                  setSelectedNodeId(null);
                }}
                onPaneClick={() => {
                  setSelectedNodeId(null);
                  setSelectedEdgeId(null);
                }}
                onDrop={onDrop}
                onDragOver={onDragOver}
                onNodeDragStop={() => takeSnapshot()}
              >
                <CanvasToolbar
                  canUndo={canUndo}
                  canRedo={canRedo}
                  onUndo={undo}
                  onRedo={redo}
                  onAutoLayout={onLayout}
                  pipelineMode={pipelineMode}
                  paradigm={activeParadigm}
                  onParadigmChange={handleParadigmChange}
                />
                <Background gap={12} size={1} />
              </ReactFlow>
            </LabelChangeContext.Provider>
          </div>
        </div>

        {/* SDP Compatibility Gate Modal */}
        {sdpCompatibilityIssues && sdpCompatibilityIssues.length > 0 && (
          <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4"
            onClick={() => setSdpIssueState(null)}
          >
            <div
              role="dialog"
              aria-modal="true"
              aria-labelledby="sdp-compatibility-modal-title"
              data-testid="sdp-compatibility-modal"
              onClick={(e) => e.stopPropagation()}
              className="w-full max-w-xl bg-[var(--vscode-workbench-bg,#151722)] border border-[var(--vscode-border,#334155)] rounded-xl shadow-2xl overflow-hidden flex flex-col max-h-[80vh]"
            >
              <div className="flex items-center justify-between px-4 py-3 border-b border-[var(--vscode-border,#334155)] bg-[var(--vscode-subcard-bg,#1e202a)]">
                <div className="flex items-center gap-2">
                  <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
                  <h2
                    id="sdp-compatibility-modal-title"
                    className="text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-primary,#ffffff)]"
                  >
                    Cannot Switch to Spark Declarative Pipelines (SDP)
                  </h2>
                </div>
                <button
                  type="button"
                  onClick={() => setSdpIssueState(null)}
                  data-testid="sdp-compatibility-modal-close"
                  aria-label="Close compatibility dialog"
                  className="p-1 rounded text-[var(--vscode-text-muted,#94a3b8)] hover:text-[var(--vscode-text-primary,#ffffff)] hover:bg-[var(--vscode-hover,rgba(255,255,255,0.06))] transition-colors cursor-pointer"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>
              <div className="p-4 flex flex-col gap-3 overflow-y-auto">
                <p className="text-xs text-[var(--vscode-text-secondary,#cbd5e1)]">
                  Spark Declarative Pipelines (Apache Spark 4.2+) cannot express {sdpCompatibilityIssues.length}{' '}
                  {sdpCompatibilityIssues.length === 1 ? 'item' : 'items'} in the current pipeline graph.
                  Update or remove the blocking nodes below before switching paradigms.
                </p>
                <ul className="flex flex-col gap-2">
                  {sdpCompatibilityIssues.map((issue, idx) => (
                    <li
                      key={`${issue.code}-${issue.nodeId ?? idx}`}
                      data-testid="sdp-blocking-issue-item"
                      className="p-3 rounded-lg bg-[var(--vscode-card-bg,#1e293b)] border border-rose-500/30 flex flex-col gap-1"
                    >
                      <div className="flex items-center justify-between gap-2 flex-wrap">
                        <span className="text-xs font-bold text-[var(--vscode-text-primary,#ffffff)]">
                          {issue.nodeLabel || issue.nodeId || 'Pipeline Configuration'}
                        </span>
                        <div className="flex items-center gap-1.5">
                          {issue.sparkTransform && (
                            <span className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-blue-500/15 border border-blue-500/30 text-blue-300">
                              {issue.sparkTransform}
                            </span>
                          )}
                          <span className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-rose-500/15 border border-rose-500/30 text-rose-300">
                            {issue.code}
                          </span>
                        </div>
                      </div>
                      <p className="text-xs text-[var(--vscode-text-muted,#94a3b8)]">
                        {issue.message}
                      </p>
                    </li>
                  ))}
                </ul>
              </div>
              <div className="px-4 py-3 border-t border-[var(--vscode-border,#334155)] bg-[var(--vscode-subcard-bg,#1e202a)] flex justify-end">
                <button
                  type="button"
                  onClick={() => setSdpIssueState(null)}
                  className="px-3 py-1.5 rounded-md text-xs font-semibold bg-[var(--vscode-button-secondary,#334155)] text-[var(--vscode-text-primary,#ffffff)] hover:bg-[var(--vscode-hover,#475569)] transition-colors cursor-pointer"
                >
                  Keep Imperative Mode
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Resizable Divider */}
        <div
          data-testid="bottom-panel-divider"
          className="h-1 bg-[var(--vscode-border)] hover:bg-[var(--vscode-highlight)] cursor-row-resize transition-colors z-30 relative shrink-0"
          onMouseDown={(e) => {
            handleBottomDividerMouseDown(e);
            handleBottomPanelInteract();
          }}
          onDoubleClick={() => {
            handleToggleBottomPanelExpand();
            handleBottomPanelInteract();
          }}
        />

        {/* Bottom Panel */}
        <BottomPanelProvider value={bottomPanelContextValue}>
          <BottomPanel
            bottomTab={bottomTab}
            setBottomTab={setBottomTab}
            height={hasCustomBottomPanelHeight ? bottomPanelHeight : undefined}
            isExpanded={isEffectiveBottomPanelExpanded}
            onToggleExpand={handleToggleBottomPanelExpand}
            onExpand={handleExpandBottomPanel}
            onInteract={handleBottomPanelInteract}
          />
        </BottomPanelProvider>

        {/* Property Pane taking full height of window, over tabs on bottom */}
        {selectedNodeId && displayNodes.find((n) => n.id === selectedNodeId) && (
          <div
            className="absolute right-0 top-0 bottom-0 z-40 shadow-2xl flex"
            style={{ width: `${propertyPanelWidth}px` }}
          >
            <ErrorBoundary fallbackTitle="Property Panel Error">
              <PropertyPane
                node={displayNodes.find((n) => n.id === selectedNodeId)!}
                onChange={(props) => handleApplyProperties(selectedNodeId, props)}
                onLabelChange={(newLabel) => handleNodeLabelChange(selectedNodeId, newLabel)}
                runtimeVariables={runtimeVariables}
                width={propertyPanelWidth}
                onResizeStart={handlePropertyPanelResizeStart}
                onClose={() => setSelectedNodeId(null)}
                paradigm={activeParadigm}
              />
            </ErrorBoundary>
          </div>
        )}
      </div>
    </div>
  );
}

/**
 * Application root wrapped with the ReactFlowProvider context required for canvas coordinate projection.
 *
 * @returns The root application component enclosed in ReactFlowProvider.
 */
export default function AppWithProvider() {
  return (
    <ReactFlowProvider>
      <App />
    </ReactFlowProvider>
  );
}
