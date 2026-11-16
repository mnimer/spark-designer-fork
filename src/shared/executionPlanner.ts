/**
 * @fileoverview Deterministic Pre-Gemini Execution Planner for Apache Spark 4.x Pipelines.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Computes a deterministic ExecutionPlan from a pipeline graph before code generation
 * or canvas badge rendering. Resolves topological execution order, batch versus streaming
 * node modes, DLQ split mechanisms, ConditionalSplit branch rules, streaming sink triggers
 * and checkpoints, batch fan-out caching, unpersist points, streaming foreachBatch sink
 * groups, and runtime classpath artifacts.
 *
 * Architectural Decisions:
 * - Pure TypeScript module with zero host, browser, or Node runtime imports so both
 *   the VS Code extension host and the React Flow webview execute identical logic.
 * - Defers terminal SINK nodes to the end of the topological order to align with
 *   modular read_sources, transform, and write_sinks code generation stages.
 * - Groups streaming sinks by stateful subquery boundaries so multi-sink fan-outs and
 *   batch-only connectors share an idempotent foreachBatch micro-batch writer.
 * - Evaluates cyclic or partially wired graphs safely without throwing exceptions so
 *   live canvas badges stay responsive during interactive editing.
 */

import { validateDagTopology, type DagEdge, type DagNode } from './dagValidator';
import {
    computeStreamingNodeSet,
    getConditionalSplitHandles,
    getNodeCategory,
    getNodeSparkTransform,
    isConditionalSplitNode,
    isErrorCapableStep,
    type JoinExecutionMode,
} from './sparkDagRules';
import {
    getNodeCapabilities,
    type ReadMode,
    type StreamingSinkMode,
} from './sparkNodeCapabilities';
import {
    resolvePipelineRuntimeArtifacts,
    type PipelineRuntimeArtifacts,
} from '../spark/versionMatrix';

export type { PipelineRuntimeArtifacts };

/**
 * Reason why a batch node is persisted in Spark memory or storage.
 */
export type CacheReason =
    | 'batch_fanout'
    | 'dlq_split'
    | 'manual_override'
    | 'cache_persist_step';

/**
 * Directive instructing code generation to call .cache() and .unpersist() for a node.
 */
export interface CacheDirective {
    /** Node identifier whose output DataFrame is cached. */
    nodeId: string;
    /** Classification of why caching applies to this node. */
    reason: CacheReason;
    /** Count of valid outgoing edges leaving this node. */
    outDegree: number;
    /** Terminal consumer node identifiers after which .unpersist() executes. */
    unpersistAfterNodeIds: string[];
}

/**
 * Dead-letter queue extraction mechanism used by an error-capable Spark step.
 */
export type DlqMechanism =
    | '_corrupt_record'
    | 'negated_predicate'
    | 'expectation_violation'
    | 'try_except_error_col';

/**
 * Dead-letter queue split specification for a node with connected error routing.
 */
export interface DlqSplitPoint {
    /** Identifier of the node emitting valid and error branches. */
    nodeId: string;
    /** Canonical Spark transform name on the splitting node. */
    sparkTransform: string;
    /** Underlying PySpark error capture strategy. */
    mechanism: DlqMechanism;
    /** Downstream target node identifiers connected to the primary valid handle. */
    validTargetIds: string[];
    /** Downstream target node identifiers connected to the error DLQ handle. */
    errorTargetIds: string[];
}

/**
 * Resolved output handle and target routing for one ConditionalSplit branch.
 */
export interface ConditionalBranchPlan {
    /** Handle identifier matching sourceHandle on outgoing edges. */
    handleId: string;
    /** Human-readable branch rule name shown on the canvas. */
    label: string;
    /** Optional SQL or DataFrame boolean filter expression for the rule. */
    condition?: string;
    /** True when this branch represents the terminal else fallback handle. */
    isFallback: boolean;
    /** Downstream node identifiers connected to this branch handle. */
    targetNodeIds: string[];
}

/**
 * Complete branch routing plan for a ConditionalSplit step node.
 */
export interface ConditionalSplitPlan {
    /** Identifier of the ConditionalSplit node. */
    nodeId: string;
    /** Ordered rule branches followed by the else fallback branch. */
    branches: ConditionalBranchPlan[];
}

/**
 * Supported Structured Streaming trigger types.
 */
export type StreamingTriggerType =
    | 'processingTime'
    | 'availableNow'
    | 'once'
    | 'continuous';

/**
 * Supported Structured Streaming output modes.
 */
export type StreamingOutputMode = 'append' | 'complete' | 'update';

/**
 * Effective trigger, output mode, and checkpoint configuration for a streaming sink.
 */
export interface ResolvedStreamingSinkSettings {
    /** Effective Structured Streaming output mode. */
    outputMode: StreamingOutputMode;
    /** Effective Structured Streaming trigger type. */
    triggerType: StreamingTriggerType;
    /** Trigger interval string when triggerType uses an interval. */
    triggerInterval?: string;
    /** Resolved checkpoint directory URI or empty string when unconfigured. */
    checkpointLocation: string;
}

/**
 * Execution group of one or more streaming sinks sharing a Structured Streaming query.
 */
export interface StreamingSinkGroup {
    /** Deterministic identifier for this streaming query group. */
    id: string;
    /** Upstream streaming node identifier whose DataFrame starts writeStream. */
    sourceSubqueryNodeId: string;
    /** Ordered sink node identifiers belonging to this streaming query. */
    sinkIds: string[];
    /** Delivery mode using native DataStreamWriter or foreachBatch callback. */
    mode: StreamingSinkMode;
    /** True when batch_df.cache() and batch_df.unpersist() are required inside foreachBatch. */
    requiresBatchCache: boolean;
    /** True when idempotent txnVersion and txnAppId options apply inside foreachBatch. */
    idempotentTxnOptions: boolean;
    /** Deterministic application identifier for idempotent micro-batch sink writes. */
    txnAppId: string;
    /** Effective query-level trigger, output mode, and checkpoint settings. */
    settings: ResolvedStreamingSinkSettings;
    /** Per-sink resolved streaming configuration keyed by sink node identifier. */
    sinkSettingsById: Record<string, ResolvedStreamingSinkSettings>;
}

/**
 * Per-node execution metadata exposed to code generation and canvas UI components.
 */
export interface NodeExecutionPlan {
    /** Unique node identifier in the pipeline graph. */
    nodeId: string;
    /** Canonical Spark transform name for this node. */
    sparkTransform: string;
    /** High-level node category ('SOURCE' | 'STEP' | 'SINK'). */
    nodeType: string;
    /** Effective execution mode ('batch' | 'streaming') for this node. */
    executionMode: ReadMode;
    /** Join execution classification when this node is a Join step. */
    joinMode?: JoinExecutionMode;
    /** True when this node is persisted via .cache() or .persist(). */
    isCached: boolean;
    /** True when this sink executes inside a .writeStream.foreachBatch(...) group. */
    isForeachBatchSink: boolean;
    /** Resolved branch targets when this node is a ConditionalSplit step. */
    conditionalBranches?: ConditionalBranchPlan[];
    /** Resolved streaming sink settings when this node is a streaming SINK. */
    streamingSinkSettings?: ResolvedStreamingSinkSettings;
}

/**
 * Deterministic pre-Gemini execution plan for an Apache Spark pipeline graph.
 */
export interface ExecutionPlan {
    /** Topologically ordered node identifiers with terminal sinks ordered last. */
    sortedNodeIds: string[];
    /** Overall pipeline execution mode ('batch' | 'streaming'). */
    pipelineMode: ReadMode;
    /** Per-node execution plans keyed by node identifier. */
    nodes: Record<string, NodeExecutionPlan>;
    /** Ordered identifiers of all nodes executing in streaming mode. */
    streamingNodeIds: string[];
    /** Batch caching directives in topological order. */
    cacheDirectives: CacheDirective[];
    /** Identifiers of all batch nodes marked for caching. */
    cachedNodeIds: string[];
    /** Map from consumer node identifier to cached node identifiers to unpersist after it. */
    unpersistPoints: Record<string, string[]>;
    /** Dead-letter queue split specifications in topological order. */
    dlqSplitPoints: DlqSplitPoint[];
    /** ConditionalSplit multi-branch routing specifications in topological order. */
    conditionalSplits: ConditionalSplitPlan[];
    /** Grouped Structured Streaming sink queries. */
    streamingSinkGroups: StreamingSinkGroup[];
    /** Identifiers of streaming sinks wrapped in foreachBatch execution. */
    foreachBatchSinkIds: string[];
    /** Resolved Spark runtime profile, JARs, Maven packages, pip dependencies, and properties. */
    runtimeArtifacts: PipelineRuntimeArtifacts;
}

/** Default trigger interval applied when processingTime or continuous omits an interval. */
const DEFAULT_TRIGGER_INTERVAL = '10 seconds';

/** Transforms using Spark _corrupt_record column capture for malformed payloads. */
const CORRUPT_RECORD_TRANSFORMS = new Set<string>([
    'ParsePayload',
    'ParseMessage',
    'CsvSource',
    'JsonSource',
    'XmlSource',
    'CsvIO',
    'JsonIO',
    'XmlIO',
]);

/** Stateful streaming step transforms that establish a Structured Streaming subquery boundary. */
const STATEFUL_STREAMING_TRANSFORMS = new Set<string>([
    'GroupByAgg',
    'RollupCube',
    'PivotUnpivot',
    'Deduplicate',
    'StatefulTransform',
]);

/** Valid Structured Streaming trigger types. */
const VALID_TRIGGER_TYPES = new Set<StreamingTriggerType>([
    'processingTime',
    'availableNow',
    'once',
    'continuous',
]);

/** Valid Structured Streaming output modes. */
const VALID_OUTPUT_MODES = new Set<StreamingOutputMode>([
    'append',
    'complete',
    'update',
]);

/**
 * Narrows an unknown value to a plain key-value object record.
 *
 * @param value Candidate value to inspect.
 * @returns Plain object record or undefined when the value is not a plain object.
 */
function asRecord(value: unknown): Record<string, unknown> | undefined {
    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
        return value as Record<string, unknown>;
    }
    return undefined;
}

/**
 * Reads a trimmed non-empty string from an unknown value.
 *
 * @param value Candidate string value.
 * @returns Trimmed string when non-empty, or undefined otherwise.
 */
function readNonEmptyString(value: unknown): string | undefined {
    if (typeof value === 'string') {
        const trimmed = value.trim();
        if (trimmed.length > 0) {
            return trimmed;
        }
    }
    return undefined;
}

/**
 * Sanitizes an identifier into an alphanumeric slug for idempotent transaction IDs.
 *
 * @param raw Raw identifier string.
 * @returns Sanitized alphanumeric identifier with underscores and hyphens.
 */
function sanitizeIdentifier(raw: string): string {
    const cleaned = raw.replace(/[^a-zA-Z0-9_-]+/g, '_').replace(/^_+|_+$/g, '');
    return cleaned.length > 0 ? cleaned : 'spark_app';
}

/**
 * Extracts the high-level node role ('SOURCE' | 'STEP' | 'SINK') from a DAG node.
 *
 * @param node Pipeline DAG node.
 * @returns Normalized node role string, defaulting to 'STEP'.
 */
function resolveNodeRole(node: DagNode): string {
    return getNodeCategory(node) ?? 'STEP';
}

/**
 * Extracts the configuration properties record from a DAG node.
 *
 * @param node Pipeline DAG node.
 * @returns Node properties record or empty object when unset.
 */
function getNodeProperties(node: DagNode): Record<string, unknown> {
    const dataRecord = asRecord(node.data);
    const nodeRecord = asRecord(node);
    return asRecord(dataRecord?.properties) ?? asRecord(nodeRecord?.properties) ?? {};
}

/**
 * Maps an error-capable Spark transform to its PySpark DLQ capture mechanism.
 *
 * @param sparkTransform Canonical Spark transform identifier.
 * @returns Deterministic DLQ mechanism classification.
 */
function resolveDlqMechanism(sparkTransform: string): DlqMechanism {
    if (CORRUPT_RECORD_TRANSFORMS.has(sparkTransform)) {
        return '_corrupt_record';
    }
    if (sparkTransform === 'Filter') {
        return 'negated_predicate';
    }
    if (sparkTransform === 'DataQualityAssert') {
        return 'expectation_violation';
    }
    return 'try_except_error_col';
}

/**
 * Orders topologically sorted node IDs so zero-out-degree SINK nodes execute last.
 *
 * Moving terminal sinks after sources and steps preserves every directed dependency
 * edge while matching the modular read_sources, transform, and write_sinks stages.
 *
 * @param rawSortedIds Topologically sorted node IDs from Kahn validation.
 * @param nodeMap Lookup map of node IDs to DAG nodes.
 * @param outgoingEdges Map of node IDs to valid outgoing edges.
 * @returns Stage-aligned topological node ID array.
 */
function alignStageTopologicalOrder(
    rawSortedIds: string[],
    nodeMap: Map<string, DagNode>,
    outgoingEdges: Map<string, DagEdge[]>
): string[] {
    const nonTerminalSinks: string[] = [];
    const terminalSinks: string[] = [];

    for (const id of rawSortedIds) {
        const node = nodeMap.get(id);
        const outCount = outgoingEdges.get(id)?.length ?? 0;
        if (node && resolveNodeRole(node) === 'SINK' && outCount === 0) {
            terminalSinks.push(id);
        } else {
            nonTerminalSinks.push(id);
        }
    }

    return [...nonTerminalSinks, ...terminalSinks];
}

/**
 * Resolves effective Structured Streaming trigger, output mode, and checkpoint settings.
 *
 * If a sink omits overrides or selects 'inherit', pipeline defaults from jobConfig apply.
 * If checkpointLocation is blank and checkpointRoot is set, appends the sink identifier.
 *
 * @param sinkNode Target streaming SINK node.
 * @param jobConfig Optional pipeline-level deployment and streaming configuration.
 * @returns Effective streaming sink configuration.
 */
export function resolveStreamingSinkSettings(
    sinkNode: DagNode,
    jobConfig?: Record<string, unknown>
): ResolvedStreamingSinkSettings {
    const props = getNodeProperties(sinkNode);
    const config = jobConfig ?? {};
    const sinkId = String(sinkNode.id);

    // 1. Resolve outputMode (default 'append').
    const rawOutputMode = readNonEmptyString(props.outputMode);
    const outputMode: StreamingOutputMode =
        rawOutputMode && VALID_OUTPUT_MODES.has(rawOutputMode as StreamingOutputMode)
            ? (rawOutputMode as StreamingOutputMode)
            : 'append';

    // 2. Resolve pipeline default trigger type (default 'processingTime').
    const rawDefaultTrigger = readNonEmptyString(config.defaultTriggerType);
    const defaultTriggerType: StreamingTriggerType =
        rawDefaultTrigger && VALID_TRIGGER_TYPES.has(rawDefaultTrigger as StreamingTriggerType)
            ? (rawDefaultTrigger as StreamingTriggerType)
            : 'processingTime';

    // 3. Resolve per-sink triggerType override ('inherit' falls back to pipeline default).
    const rawSinkTrigger = readNonEmptyString(props.triggerType);
    const triggerType: StreamingTriggerType =
        rawSinkTrigger &&
        rawSinkTrigger !== 'inherit' &&
        VALID_TRIGGER_TYPES.has(rawSinkTrigger as StreamingTriggerType)
            ? (rawSinkTrigger as StreamingTriggerType)
            : defaultTriggerType;

    // 4. Resolve triggerInterval only for interval-based triggers.
    let triggerInterval: string | undefined;
    if (triggerType === 'processingTime' || triggerType === 'continuous') {
        const sinkInterval = readNonEmptyString(props.triggerInterval);
        const defaultInterval = readNonEmptyString(config.defaultTriggerInterval);
        triggerInterval = sinkInterval ?? defaultInterval ?? DEFAULT_TRIGGER_INTERVAL;
    }

    // 5. Resolve checkpointLocation override or derive from checkpointRoot + sinkId.
    const explicitCheckpoint = readNonEmptyString(props.checkpointLocation);
    const checkpointRoot = readNonEmptyString(config.checkpointRoot);
    let checkpointLocation = '';
    if (explicitCheckpoint) {
        checkpointLocation = explicitCheckpoint;
    } else if (checkpointRoot) {
        const cleanRoot = checkpointRoot.replace(/\/+$/, '');
        checkpointLocation = `${cleanRoot}/${sinkId}`;
    }

    return {
        outputMode,
        triggerType,
        ...(triggerInterval !== undefined ? { triggerInterval } : {}),
        checkpointLocation,
    };
}

/**
 * Builds DLQ split specifications for nodes with connected error output handles.
 *
 * @param orderedIds Deterministic node evaluation order.
 * @param nodeMap Lookup map of node IDs to DAG nodes.
 * @param outgoingEdges Map of node IDs to valid outgoing edges.
 * @returns Ordered list of DLQ split point definitions.
 */
function buildDlqSplitPoints(
    orderedIds: string[],
    nodeMap: Map<string, DagNode>,
    outgoingEdges: Map<string, DagEdge[]>
): DlqSplitPoint[] {
    const splits: DlqSplitPoint[] = [];

    for (const nodeId of orderedIds) {
        const node = nodeMap.get(nodeId);
        if (!node) {
            continue;
        }
        const edgesFromNode = outgoingEdges.get(nodeId) ?? [];
        if (edgesFromNode.length === 0) {
            continue;
        }

        const errorTargetIds: string[] = [];
        const validTargetIds: string[] = [];

        for (const edge of edgesFromNode) {
            const targetId = String(edge.target);
            if (edge.sourceHandle === 'error') {
                if (!errorTargetIds.includes(targetId)) {
                    errorTargetIds.push(targetId);
                }
            } else if (!validTargetIds.includes(targetId)) {
                validTargetIds.push(targetId);
            }
        }

        // If an error edge is wired, classify and emit a DLQ split point.
        if (errorTargetIds.length > 0 && (isErrorCapableStep(node) || errorTargetIds.length > 0)) {
            const sparkTransform = getNodeSparkTransform(node) ?? '';
            splits.push({
                nodeId,
                sparkTransform,
                mechanism: resolveDlqMechanism(sparkTransform),
                validTargetIds,
                errorTargetIds,
            });
        }
    }

    return splits;
}

/**
 * Builds ConditionalSplit branch routing specifications for ConditionalSplit nodes.
 *
 * @param orderedIds Deterministic node evaluation order.
 * @param nodeMap Lookup map of node IDs to DAG nodes.
 * @param outgoingEdges Map of node IDs to valid outgoing edges.
 * @returns Ordered list of ConditionalSplit branch routing plans.
 */
function buildConditionalSplits(
    orderedIds: string[],
    nodeMap: Map<string, DagNode>,
    outgoingEdges: Map<string, DagEdge[]>
): ConditionalSplitPlan[] {
    const plans: ConditionalSplitPlan[] = [];

    for (const nodeId of orderedIds) {
        const node = nodeMap.get(nodeId);
        if (!node || !isConditionalSplitNode(node)) {
            continue;
        }

        const handles = getConditionalSplitHandles(node);
        const edgesFromNode = outgoingEdges.get(nodeId) ?? [];
        const branches: ConditionalBranchPlan[] = handles.map((handle) => {
            const targetNodeIds: string[] = [];
            for (const edge of edgesFromNode) {
                const edgeHandle = readNonEmptyString(edge.sourceHandle) ?? handles[0]?.id ?? 'else';
                if (edgeHandle === handle.id) {
                    const targetId = String(edge.target);
                    if (!targetNodeIds.includes(targetId)) {
                        targetNodeIds.push(targetId);
                    }
                }
            }
            return {
                handleId: handle.id,
                label: handle.label,
                ...(handle.condition !== undefined ? { condition: handle.condition } : {}),
                isFallback: handle.id === 'else',
                targetNodeIds,
            };
        });

        plans.push({ nodeId, branches });
    }

    return plans;
}

/**
 * Normalizes a manual cacheOutput property setting into a tri-state directive.
 *
 * @param rawValue Raw cacheOutput value from node properties.
 * @returns 'force' when enabled, 'suppress' when disabled, or 'auto' by default.
 */
function resolveManualCacheOverride(rawValue: unknown): 'force' | 'suppress' | 'auto' {
    if (rawValue === true || rawValue === 'always' || rawValue === 'true') {
        return 'force';
    }
    if (rawValue === false || rawValue === 'never' || rawValue === 'false') {
        return 'suppress';
    }
    return 'auto';
}

/**
 * Collects all transitive downstream node IDs reachable from a starting node.
 *
 * @param startNodeId Root node identifier to traverse from.
 * @param outgoingEdges Map of node IDs to valid outgoing edges.
 * @returns Set of reachable downstream node identifiers excluding startNodeId.
 */
function collectTransitiveDescendants(
    startNodeId: string,
    outgoingEdges: Map<string, DagEdge[]>
): Set<string> {
    const descendants = new Set<string>();
    const queue: string[] = [startNodeId];
    let head = 0;

    while (head < queue.length) {
        const curr = queue[head];
        head += 1;
        const edgesFromCurr = outgoingEdges.get(curr) ?? [];
        for (const edge of edgesFromCurr) {
            const nextId = String(edge.target);
            if (nextId !== startNodeId && !descendants.has(nextId)) {
                descendants.add(nextId);
                queue.push(nextId);
            }
        }
    }

    return descendants;
}

/**
 * Computes batch fan-out auto-cache directives, manual overrides, and unpersist points.
 *
 * If a batch non-sink node has out-degree greater than one, or manual caching is forced,
 * or the step is CachePersist, marks the node for .cache() and schedules .unpersist()
 * immediately after the final transitive downstream consumer in topological order.
 *
 * @param evaluationOrderIds Deterministic node evaluation order.
 * @param sortedNodeIds Acyclic topological order (empty when cyclic).
 * @param nodeMap Lookup map of node IDs to DAG nodes.
 * @param outgoingEdges Map of node IDs to valid outgoing edges.
 * @param streamingNodeSet Set of node IDs executing in streaming mode.
 * @returns Batch cache directives, cached node IDs, and unpersist schedule map.
 */
function buildBatchCachePlan(
    evaluationOrderIds: string[],
    sortedNodeIds: string[],
    nodeMap: Map<string, DagNode>,
    outgoingEdges: Map<string, DagEdge[]>,
    streamingNodeSet: Set<string>
): {
    cacheDirectives: CacheDirective[];
    cachedNodeIds: string[];
    unpersistPoints: Record<string, string[]>;
} {
    const cacheDirectives: CacheDirective[] = [];
    const cachedNodeIds: string[] = [];
    const unpersistPoints: Record<string, string[]> = {};

    const topoIndex = new Map<string, number>();
    sortedNodeIds.forEach((id, idx) => {
        topoIndex.set(id, idx);
    });

    for (const nodeId of evaluationOrderIds) {
        const node = nodeMap.get(nodeId);
        if (!node || streamingNodeSet.has(nodeId)) {
            continue;
        }

        const nodeType = resolveNodeRole(node);
        if (nodeType === 'SINK') {
            continue;
        }

        const edgesFromNode = outgoingEdges.get(nodeId) ?? [];
        const outDegree = edgesFromNode.length;
        const sparkTransform = getNodeSparkTransform(node) ?? '';
        const props = getNodeProperties(node);
        const overrideMode = resolveManualCacheOverride(props.cacheOutput);

        let reason: CacheReason | undefined;
        if (sparkTransform === 'CachePersist') {
            reason = 'cache_persist_step';
        } else if (overrideMode === 'suppress') {
            reason = undefined;
        } else if (overrideMode === 'force') {
            reason = 'manual_override';
        } else if (outDegree > 1) {
            const hasErrorBranch = edgesFromNode.some((edge) => edge.sourceHandle === 'error');
            reason = hasErrorBranch ? 'dlq_split' : 'batch_fanout';
        }

        if (!reason) {
            continue;
        }

        const unpersistAfterNodeIds: string[] = [];
        if (sortedNodeIds.length > 0) {
            const descendants = collectTransitiveDescendants(nodeId, outgoingEdges);
            let lastConsumerId: string | undefined;
            let maxIdx = -1;

            for (const descId of descendants) {
                const idx = topoIndex.get(descId) ?? -1;
                if (idx > maxIdx) {
                    maxIdx = idx;
                    lastConsumerId = descId;
                }
            }

            if (lastConsumerId !== undefined) {
                unpersistAfterNodeIds.push(lastConsumerId);
                const existingBucket = unpersistPoints[lastConsumerId];
                if (existingBucket) {
                    existingBucket.push(nodeId);
                } else {
                    unpersistPoints[lastConsumerId] = [nodeId];
                }
            }
        }

        cachedNodeIds.push(nodeId);
        cacheDirectives.push({
            nodeId,
            reason,
            outDegree,
            unpersistAfterNodeIds,
        });
    }

    return {
        cacheDirectives,
        cachedNodeIds,
        unpersistPoints,
    };
}

/**
 * Checks whether a streaming node defines a stateful subquery boundary.
 *
 * @param nodeId Streaming node identifier.
 * @param nodeMap Lookup map of node IDs to DAG nodes.
 * @param joinModes Map of Join node IDs to Join execution classifications.
 * @returns True when the node starts or resets a Structured Streaming state boundary.
 */
function isStatefulStreamingBoundary(
    nodeId: string,
    nodeMap: Map<string, DagNode>,
    joinModes: Map<string, JoinExecutionMode>
): boolean {
    const node = nodeMap.get(nodeId);
    if (!node) {
        return false;
    }
    if (resolveNodeRole(node) === 'SOURCE') {
        return true;
    }
    const transform = getNodeSparkTransform(node) ?? '';
    if (STATEFUL_STREAMING_TRANSFORMS.has(transform)) {
        return true;
    }
    if (transform === 'Join' && joinModes.get(nodeId) === 'stream_stream') {
        return true;
    }
    return false;
}

/**
 * Checks whether a sink node requires foreachBatch execution in streaming mode.
 *
 * @param sinkNode Target SINK node.
 * @returns True when the sink connector uses foreachBatch instead of native writeStream.
 */
function requiresForeachBatchSinkMode(sinkNode: DagNode): boolean {
    const dataRecord = asRecord(sinkNode.data);
    if (dataRecord?.streamingSinkMode === 'foreachBatch') {
        return true;
    }
    if (dataRecord?.streamingSinkMode === 'native') {
        return false;
    }
    const sparkTransform = getNodeSparkTransform(sinkNode) ?? '';
    const capabilities = getNodeCapabilities(sparkTransform, 'SINK');
    return capabilities.streamingSinkMode === 'foreachBatch';
}

/**
 * Checks whether two string sets share at least one common element.
 *
 * @param left First set of identifiers.
 * @param right Second set of identifiers.
 * @returns True when at least one identifier appears in both sets.
 */
function setsIntersect(left: Set<string>, right: Set<string>): boolean {
    for (const item of left) {
        if (right.has(item)) {
            return true;
        }
    }
    return false;
}

/**
 * Resolves unified query-level settings for a multi-sink foreachBatch streaming group.
 *
 * @param groupId Deterministic group identifier.
 * @param sinkIds Ordered sink IDs in the group.
 * @param nodeMap Lookup map of node IDs to DAG nodes.
 * @param sinkSettingsById Per-sink resolved streaming settings.
 * @param jobConfig Optional pipeline-level deployment and streaming settings.
 * @returns Effective query-level trigger, output mode, and checkpoint configuration.
 */
function resolveGroupStreamingSettings(
    groupId: string,
    sinkIds: string[],
    nodeMap: Map<string, DagNode>,
    sinkSettingsById: Record<string, ResolvedStreamingSinkSettings>,
    jobConfig?: Record<string, unknown>
): ResolvedStreamingSinkSettings {
    const firstSinkId = sinkIds[0];
    const firstSettings = sinkSettingsById[firstSinkId] ?? {
        outputMode: 'append',
        triggerType: 'processingTime',
        triggerInterval: DEFAULT_TRIGGER_INTERVAL,
        checkpointLocation: '',
    };

    if (sinkIds.length <= 1) {
        return firstSettings;
    }

    let outputMode: StreamingOutputMode = 'append';
    let triggerType: StreamingTriggerType = firstSettings.triggerType;
    let triggerInterval: string | undefined = firstSettings.triggerInterval;
    let explicitCheckpoint: string | undefined;
    let hasTriggerOverride = false;

    for (const sinkId of sinkIds) {
        const sinkNode = nodeMap.get(sinkId);
        const resolved = sinkSettingsById[sinkId];
        if (!sinkNode || !resolved) {
            continue;
        }
        const props = getNodeProperties(sinkNode);

        const rawOutput = readNonEmptyString(props.outputMode);
        if (
            rawOutput &&
            VALID_OUTPUT_MODES.has(rawOutput as StreamingOutputMode) &&
            outputMode === 'append'
        ) {
            outputMode = rawOutput as StreamingOutputMode;
        }

        const rawTrigger = readNonEmptyString(props.triggerType);
        if (
            !hasTriggerOverride &&
            rawTrigger &&
            rawTrigger !== 'inherit' &&
            VALID_TRIGGER_TYPES.has(rawTrigger as StreamingTriggerType)
        ) {
            triggerType = resolved.triggerType;
            triggerInterval = resolved.triggerInterval;
            hasTriggerOverride = true;
        }

        const rawCheckpoint = readNonEmptyString(props.checkpointLocation);
        if (rawCheckpoint && explicitCheckpoint === undefined) {
            explicitCheckpoint = rawCheckpoint;
        }
    }

    const checkpointRoot = readNonEmptyString(jobConfig?.checkpointRoot);
    let checkpointLocation = '';
    if (explicitCheckpoint) {
        checkpointLocation = explicitCheckpoint;
    } else if (checkpointRoot) {
        const cleanRoot = checkpointRoot.replace(/\/+$/, '');
        checkpointLocation = `${cleanRoot}/${groupId}`;
    }

    return {
        outputMode,
        triggerType,
        ...(triggerInterval !== undefined ? { triggerInterval } : {}),
        checkpointLocation,
    };
}

/**
 * Groups streaming sinks by subquery root and plans native versus foreachBatch execution.
 *
 * If a streaming subquery feeds a single native sink, emits a 'native' group.
 * If any sink uses foreachBatch or a subquery fans out to two or more sinks, wraps
 * the group in 'foreachBatch' with batch caching and idempotent transaction metadata.
 *
 * @param evaluationOrderIds Deterministic node evaluation order.
 * @param nodeMap Lookup map of node IDs to DAG nodes.
 * @param incomingEdges Map of node IDs to valid incoming edges.
 * @param streamingNodeSet Set of node IDs executing in streaming mode.
 * @param joinModes Map of Join node IDs to Join execution classifications.
 * @param nodePlans Mutable per-node execution plan map.
 * @param jobConfig Optional pipeline-level deployment and streaming settings.
 * @returns Ordered streaming sink groups and foreachBatch sink identifier list.
 */
function buildStreamingSinkGroups(
    evaluationOrderIds: string[],
    nodeMap: Map<string, DagNode>,
    incomingEdges: Map<string, DagEdge[]>,
    streamingNodeSet: Set<string>,
    joinModes: Map<string, JoinExecutionMode>,
    nodePlans: Record<string, NodeExecutionPlan>,
    jobConfig?: Record<string, unknown>
): {
    streamingSinkGroups: StreamingSinkGroup[];
    foreachBatchSinkIds: string[];
} {
    const streamingSinkIds = evaluationOrderIds.filter((id) => {
        const node = nodeMap.get(id);
        return Boolean(node && streamingNodeSet.has(id) && resolveNodeRole(node) === 'SINK');
    });

    if (streamingSinkIds.length === 0) {
        return { streamingSinkGroups: [], foreachBatchSinkIds: [] };
    }

    const orderIndex = new Map<string, number>();
    evaluationOrderIds.forEach((id, idx) => {
        orderIndex.set(id, idx);
    });

    // 1. Collect immediate streaming parents and bounded subquery ancestors per streaming sink.
    const immediateParents = new Map<string, string[]>();
    const subqueryAncestors = new Map<string, Set<string>>();

    for (const sinkId of streamingSinkIds) {
        const parents = (incomingEdges.get(sinkId) ?? [])
            .map((edge) => String(edge.source))
            .filter((srcId) => streamingNodeSet.has(srcId));
        immediateParents.set(sinkId, parents);

        const ancestors = new Set<string>();
        const queue = parents.length > 0 ? [...parents] : [sinkId];
        let head = 0;

        while (head < queue.length) {
            const curr = queue[head];
            head += 1;
            if (ancestors.has(curr)) {
                continue;
            }
            ancestors.add(curr);
            if (!isStatefulStreamingBoundary(curr, nodeMap, joinModes)) {
                const upEdges = incomingEdges.get(curr) ?? [];
                for (const upEdge of upEdges) {
                    const upSrc = String(upEdge.source);
                    if (streamingNodeSet.has(upSrc) && !ancestors.has(upSrc)) {
                        queue.push(upSrc);
                    }
                }
            }
        }
        subqueryAncestors.set(sinkId, ancestors);
    }

    // 2. Partition streaming sinks into connected subquery components using union-find.
    const parentIdx = streamingSinkIds.map((_, idx) => idx);
    const findRoot = (idx: number): number => {
        let curr = idx;
        while (parentIdx[curr] !== curr) {
            parentIdx[curr] = parentIdx[parentIdx[curr]];
            curr = parentIdx[curr];
        }
        return curr;
    };

    for (let i = 0; i < streamingSinkIds.length; i++) {
        const ancI = subqueryAncestors.get(streamingSinkIds[i]) ?? new Set<string>();
        for (let j = i + 1; j < streamingSinkIds.length; j++) {
            const ancJ = subqueryAncestors.get(streamingSinkIds[j]) ?? new Set<string>();
            if (setsIntersect(ancI, ancJ)) {
                const rootI = findRoot(i);
                const rootJ = findRoot(j);
                if (rootI !== rootJ) {
                    parentIdx[rootJ] = rootI;
                }
            }
        }
    }

    const groupedSinkLists: string[][] = [];
    const rootToGroup = new Map<number, string[]>();
    for (let i = 0; i < streamingSinkIds.length; i++) {
        const root = findRoot(i);
        let bucket = rootToGroup.get(root);
        if (!bucket) {
            bucket = [];
            rootToGroup.set(root, bucket);
            groupedSinkLists.push(bucket);
        }
        bucket.push(streamingSinkIds[i]);
    }

    // 3. Build StreamingSinkGroup records and mark foreachBatch sinks.
    const appBaseName = sanitizeIdentifier(
        readNonEmptyString(jobConfig?.jobName) ??
            readNonEmptyString(jobConfig?.pipelineName) ??
            readNonEmptyString(jobConfig?.appName) ??
            'spark_pipeline'
    );

    const streamingSinkGroups: StreamingSinkGroup[] = [];
    const foreachBatchSinkIds: string[] = [];

    groupedSinkLists.forEach((groupSinkIds, idx) => {
        const firstSinkId = groupSinkIds[0];

        // Resolve lowest common streaming ancestor (branching root) for the subquery.
        let sourceSubqueryNodeId: string;
        if (groupSinkIds.length === 1) {
            const singleParents = immediateParents.get(firstSinkId) ?? [];
            sourceSubqueryNodeId = singleParents[0] ?? firstSinkId;
        } else {
            const firstAncestors = subqueryAncestors.get(firstSinkId) ?? new Set<string>();
            const common: string[] = [];
            for (const candidate of firstAncestors) {
                const inAll = groupSinkIds.every((sId) =>
                    Boolean(subqueryAncestors.get(sId)?.has(candidate))
                );
                if (inAll) {
                    common.push(candidate);
                }
            }

            if (common.length > 0) {
                sourceSubqueryNodeId = common.reduce((best, curr) =>
                    (orderIndex.get(curr) ?? -1) > (orderIndex.get(best) ?? -1) ? curr : best
                );
            } else {
                const fallbackCandidates = Array.from(firstAncestors);
                sourceSubqueryNodeId =
                    fallbackCandidates.sort(
                        (a, b) => (orderIndex.get(a) ?? 0) - (orderIndex.get(b) ?? 0)
                    )[0] ?? firstSinkId;
            }
        }

        const anyForeachBatchConnector = groupSinkIds.some((sinkId) => {
            const sinkNode = nodeMap.get(sinkId);
            return Boolean(sinkNode && requiresForeachBatchSinkMode(sinkNode));
        });
        const mode: StreamingSinkMode =
            groupSinkIds.length > 1 || anyForeachBatchConnector ? 'foreachBatch' : 'native';

        const groupNumber = idx + 1;
        const id =
            mode === 'foreachBatch'
                ? `foreach_batch_${groupNumber}`
                : `native_stream_${groupNumber}`;
        const requiresBatchCache = mode === 'foreachBatch' && groupSinkIds.length > 1;
        const idempotentTxnOptions = mode === 'foreachBatch';
        const txnAppId =
            mode === 'foreachBatch'
                ? `${appBaseName}_${sanitizeIdentifier(sourceSubqueryNodeId)}`
                : '';

        const sinkSettingsById: Record<string, ResolvedStreamingSinkSettings> = {};
        for (const sinkId of groupSinkIds) {
            const sinkNode = nodeMap.get(sinkId);
            if (!sinkNode) {
                continue;
            }
            sinkSettingsById[sinkId] =
                nodePlans[sinkId]?.streamingSinkSettings ??
                resolveStreamingSinkSettings(sinkNode, jobConfig);
        }

        const settings = resolveGroupStreamingSettings(
            id,
            groupSinkIds,
            nodeMap,
            sinkSettingsById,
            jobConfig
        );

        if (mode === 'foreachBatch') {
            for (const sinkId of groupSinkIds) {
                foreachBatchSinkIds.push(sinkId);
                const planEntry = nodePlans[sinkId];
                if (planEntry) {
                    planEntry.isForeachBatchSink = true;
                }
            }
        }

        streamingSinkGroups.push({
            id,
            sourceSubqueryNodeId,
            sinkIds: groupSinkIds,
            mode,
            requiresBatchCache,
            idempotentTxnOptions,
            txnAppId,
            settings,
            sinkSettingsById,
        });
    });

    return { streamingSinkGroups, foreachBatchSinkIds };
}

/**
 * Builds a deterministic pre-Gemini ExecutionPlan for an Apache Spark pipeline DAG.
 *
 * @param nodes Pipeline graph nodes.
 * @param edges Pipeline graph edges.
 * @param jobConfig Optional pipeline-level deployment and streaming settings.
 * @returns Complete deterministic ExecutionPlan for code generation and UI badges.
 */
export function buildExecutionPlan(
    nodes: DagNode[],
    edges: DagEdge[],
    jobConfig?: Record<string, unknown>
): ExecutionPlan {
    const rawNodes = (nodes ?? []).filter(
        (n): n is DagNode => Boolean(n && n.id !== undefined && n.id !== null)
    );
    const dedupedNodes: DagNode[] = [];
    const nodeMap = new Map<string, DagNode>();
    const inputOrderIds: string[] = [];

    for (const node of rawNodes) {
        const id = String(node.id);
        if (!nodeMap.has(id)) {
            nodeMap.set(id, node);
            dedupedNodes.push(node);
            inputOrderIds.push(id);
        }
    }

    // Filter edges to registered endpoints so dangling canvas edges never corrupt degree counts.
    const validEdges: DagEdge[] = [];
    const outgoingEdges = new Map<string, DagEdge[]>();
    const incomingEdges = new Map<string, DagEdge[]>();
    for (const id of inputOrderIds) {
        outgoingEdges.set(id, []);
        incomingEdges.set(id, []);
    }

    for (const edge of edges ?? []) {
        if (!edge || edge.source === undefined || edge.target === undefined) {
            continue;
        }
        const src = String(edge.source);
        const tgt = String(edge.target);
        const srcBucket = outgoingEdges.get(src);
        const tgtBucket = incomingEdges.get(tgt);
        if (srcBucket && tgtBucket) {
            validEdges.push(edge);
            srcBucket.push(edge);
            tgtBucket.push(edge);
        }
    }

    // 1. Compute topological execution order and align terminal SINK nodes last.
    const topology = validateDagTopology(dedupedNodes, validEdges);
    const sortedNodeIds = topology.hasCycles
        ? []
        : alignStageTopologicalOrder(topology.sortedNodeIds, nodeMap, outgoingEdges);

    // Use topological order when acyclic, or input order fallback when cyclic so UI badges stay populated.
    const evaluationOrderIds = sortedNodeIds.length > 0 ? sortedNodeIds : inputOrderIds;

    // 2. Propagate streaming execution modes and Join classifications across the DAG.
    const streamingPropagation = computeStreamingNodeSet(dedupedNodes, validEdges);
    const pipelineMode: ReadMode =
        streamingPropagation.hasStreamingSource || streamingPropagation.streamingNodeIds.size > 0
            ? 'streaming'
            : 'batch';

    const streamingNodeIds = evaluationOrderIds.filter((id) =>
        streamingPropagation.streamingNodeIds.has(id)
    );

    // 3. Resolve DLQ split points and ConditionalSplit branch routing.
    const dlqSplitPoints = buildDlqSplitPoints(evaluationOrderIds, nodeMap, outgoingEdges);
    const conditionalSplits = buildConditionalSplits(evaluationOrderIds, nodeMap, outgoingEdges);
    const conditionalMap = new Map<string, ConditionalBranchPlan[]>();
    for (const split of conditionalSplits) {
        conditionalMap.set(split.nodeId, split.branches);
    }

    // 4. Plan batch fan-out auto-caching, manual overrides, and unpersist points.
    const { cacheDirectives, cachedNodeIds, unpersistPoints } = buildBatchCachePlan(
        evaluationOrderIds,
        sortedNodeIds,
        nodeMap,
        outgoingEdges,
        streamingPropagation.streamingNodeIds
    );
    const cachedSet = new Set<string>(cachedNodeIds);

    // 5. Assemble per-node execution plans and resolve streaming sink settings.
    const nodePlans: Record<string, NodeExecutionPlan> = {};
    const runtimeInputNodes: Array<{ sparkTransform: string; properties: Record<string, unknown> }> =
        [];

    for (const nodeId of evaluationOrderIds) {
        const node = nodeMap.get(nodeId);
        if (!node) {
            continue;
        }
        const sparkTransform = getNodeSparkTransform(node) ?? '';
        const nodeType = resolveNodeRole(node);
        const executionMode: ReadMode = streamingPropagation.streamingNodeIds.has(nodeId)
            ? 'streaming'
            : 'batch';
        const joinMode = streamingPropagation.joinModes.get(nodeId);
        const conditionalBranches = conditionalMap.get(nodeId);
        const isStreamingSink = nodeType === 'SINK' && executionMode === 'streaming';
        const streamingSinkSettings = isStreamingSink
            ? resolveStreamingSinkSettings(node, jobConfig)
            : undefined;

        getNodeCapabilities(sparkTransform, nodeType);

        nodePlans[nodeId] = {
            nodeId,
            sparkTransform,
            nodeType,
            executionMode,
            ...(joinMode !== undefined ? { joinMode } : {}),
            isCached: cachedSet.has(nodeId),
            isForeachBatchSink: false,
            ...(conditionalBranches !== undefined ? { conditionalBranches } : {}),
            ...(streamingSinkSettings !== undefined ? { streamingSinkSettings } : {}),
        };

        runtimeInputNodes.push({
            sparkTransform,
            properties: getNodeProperties(node),
        });
    }

    // 6. Group streaming sinks into native or foreachBatch Structured Streaming queries.
    const { streamingSinkGroups, foreachBatchSinkIds } = buildStreamingSinkGroups(
        evaluationOrderIds,
        nodeMap,
        incomingEdges,
        streamingPropagation.streamingNodeIds,
        streamingPropagation.joinModes,
        nodePlans,
        jobConfig
    );

    // 7. Resolve runtime profile, connector JARs, Maven packages, and Python DataSources.
    const runtimeArtifacts = resolvePipelineRuntimeArtifacts(runtimeInputNodes, jobConfig);

    return {
        sortedNodeIds,
        pipelineMode,
        nodes: nodePlans,
        streamingNodeIds,
        cacheDirectives,
        cachedNodeIds,
        unpersistPoints,
        dlqSplitPoints,
        conditionalSplits,
        streamingSinkGroups,
        foreachBatchSinkIds,
        runtimeArtifacts,
    };
}
