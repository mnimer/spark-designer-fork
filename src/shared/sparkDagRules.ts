/**
 * @fileoverview Pure TypeScript Apache Spark DAG semantic rules and streaming propagation.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Computes Structured Streaming mode propagation across pipeline graphs, classifies
 * Join execution modes (batch-batch, stream-static, stream-stream), resolves node
 * input arity and DLQ error handle capabilities, and derives dynamic output handles
 * for ConditionalSplit routing steps.
 */

import type { DagEdge, DagNode } from './dagValidator';
import {
    getNodeCapabilities,
    SPARK_NODE_CAPABILITIES,
    type InputArity,
    type NodeCapabilityFlags,
    type ReadMode,
    type StreamingSinkMode,
} from './sparkNodeCapabilities';

export { getNodeCapabilities, SPARK_NODE_CAPABILITIES };
export type { InputArity, NodeCapabilityFlags, ReadMode, StreamingSinkMode };

/**
 * Execution semantics of a Spark Join step based on its upstream branch modes.
 */
export type JoinExecutionMode = 'batch_batch' | 'stream_static' | 'stream_stream';

/**
 * Result of propagating Structured Streaming mode from streaming source nodes.
 */
export interface StreamingPropagationResult {
    streamingNodeIds: Set<string>;
    streamingEdgeIds: Set<string>;
    joinModes: Map<string, JoinExecutionMode>;
    hasStreamingSource: boolean;
}

/**
 * Output handle descriptor for a rule or fallback branch on a ConditionalSplit step.
 */
export interface ConditionalSplitHandle {
    id: string;
    label: string;
    condition?: string;
}

/**
 * Extracts the normalized category (`SOURCE`, `STEP`, or `SINK`) from a DAG node.
 *
 * @param node Pipeline DAG node.
 * @returns Normalized uppercase category, or undefined when absent.
 */
export function getNodeCategory(node?: DagNode | null): 'SOURCE' | 'STEP' | 'SINK' | undefined {
    const rawDataCategory = typeof node?.data?.type === 'string' ? node.data.type.trim().toUpperCase() : '';
    if (rawDataCategory === 'SOURCE' || rawDataCategory === 'STEP' || rawDataCategory === 'SINK') {
        return rawDataCategory;
    }

    const rawNodeType = typeof node?.type === 'string' ? node.type.trim().toUpperCase() : '';
    if (rawNodeType === 'SOURCE' || rawNodeType === 'STEP' || rawNodeType === 'SINK') {
        return rawNodeType;
    }

    return undefined;
}

/**
 * Extracts the Spark transform identifier from a DAG node regardless of casing convention.
 *
 * @param node Pipeline DAG node.
 * @returns Trimmed transform identifier, or undefined when absent.
 */
export function getNodeSparkTransform(node?: DagNode | null): string | undefined {
    const candidate =
        node?.data?.spark_transform ??
        node?.data?.sparkTransform ??
        (node as Record<string, unknown> | null | undefined)?.spark_transform ??
        (node as Record<string, unknown> | null | undefined)?.sparkTransform;

    if (typeof candidate === 'string') {
        const trimmed = candidate.trim();
        return trimmed.length > 0 ? trimmed : undefined;
    }
    return undefined;
}

/**
 * Resolves the effective read mode (`batch` or `streaming`) of a source node.
 * If `node.data.properties.readMode` is explicitly `batch` or `streaming`, returns that mode.
 * Otherwise returns the node or capability default read mode, falling back to `batch`.
 *
 * @param node Source DAG node to inspect.
 * @returns Effective `ReadMode` for the source node.
 */
export function getEffectiveSourceReadMode(node: DagNode): ReadMode {
    const configuredMode = node?.data?.properties?.readMode;
    if (configuredMode === 'streaming') {
        return 'streaming';
    }
    if (configuredMode === 'batch') {
        return 'batch';
    }

    const nodeDefault = node?.data?.defaultReadMode;
    if (nodeDefault === 'streaming' || nodeDefault === 'batch') {
        return nodeDefault;
    }

    const capabilities = getNodeCapabilities(getNodeSparkTransform(node), 'SOURCE');
    return capabilities.defaultReadMode ?? 'batch';
}

/**
 * Returns true if the node is a Spark `Join` transformation step.
 *
 * @param node Pipeline DAG node.
 * @returns True when the node represents a `Join` step.
 */
export function isJoinNode(node: DagNode): boolean {
    const category = getNodeCategory(node);
    if (category === 'SOURCE' || category === 'SINK') {
        return false;
    }
    return getNodeSparkTransform(node) === 'Join';
}

/**
 * Returns true if the node is a Spark `ConditionalSplit` multi-output step.
 *
 * @param node Pipeline DAG node.
 * @returns True when the node represents a `ConditionalSplit` step.
 */
export function isConditionalSplitNode(node: DagNode): boolean {
    const category = getNodeCategory(node);
    if (category === 'SOURCE' || category === 'SINK') {
        return false;
    }
    return getNodeSparkTransform(node) === 'ConditionalSplit';
}

/**
 * Returns true if a transformation step exposes the dead-letter `error` output handle.
 *
 * @param node Pipeline DAG node.
 * @returns True when the node is a step with `errorOutput: true`.
 */
export function isErrorCapableStep(node: DagNode): boolean {
    const category = getNodeCategory(node);
    if (category === 'SOURCE' || category === 'SINK') {
        return false;
    }

    if (typeof node?.data?.errorOutput === 'boolean') {
        return node.data.errorOutput;
    }

    const capabilities = getNodeCapabilities(getNodeSparkTransform(node), 'STEP');
    return capabilities.errorOutput === true;
}

/**
 * Resolves the upstream input arity (`1`, `2`, or `'n'`) accepted by a step or sink node.
 *
 * @param node Pipeline DAG node.
 * @returns Expected input arity for the node.
 */
export function getNodeInputArity(node: DagNode): InputArity {
    const explicitArity = node?.data?.inputArity;
    if (explicitArity === 1 || explicitArity === 2 || explicitArity === 'n') {
        return explicitArity;
    }

    const category = getNodeCategory(node) ?? 'STEP';
    const transform = getNodeSparkTransform(node);
    if (!transform && category === 'STEP' && node?.data?.label === 'Flatten') {
        return 'n';
    }

    const capabilities = getNodeCapabilities(transform, category);
    return capabilities.inputArity ?? 1;
}

/**
 * Computes ordered output handles for a `ConditionalSplit` node from its `rules` property.
 * Defaults to a single `rule_1` handle plus the terminal `else` handle when no rules exist.
 *
 * @param node ConditionalSplit DAG node.
 * @returns Ordered list of rule handles followed by the `else` handle.
 */
export function getConditionalSplitHandles(node: DagNode): ConditionalSplitHandle[] {
    const rawRules = node?.data?.properties?.rules;
    const rulesArray =
        Array.isArray(rawRules) && rawRules.length > 0
            ? rawRules
            : [{ name: 'rule_1', condition: '' }];

    const handles: ConditionalSplitHandle[] = [];
    const usedIds = new Set<string>();

    rulesArray.forEach((rawRule: unknown, index: number) => {
        const ruleObj =
            rawRule !== null && typeof rawRule === 'object'
                ? (rawRule as Record<string, unknown>)
                : {};
        const fallbackName = `rule_${index + 1}`;
        const rawName = typeof ruleObj.name === 'string' ? ruleObj.name.trim() : '';
        const label = rawName.length > 0 ? rawName : fallbackName;

        let handleId = label;
        if (handleId === 'else' || usedIds.has(handleId)) {
            handleId = `${label}_${index + 1}`;
        }
        usedIds.add(handleId);

        const handle: ConditionalSplitHandle = {
            id: handleId,
            label,
        };
        if (typeof ruleObj.condition === 'string') {
            handle.condition = ruleObj.condition;
        }
        handles.push(handle);
    });

    handles.push({ id: 'else', label: 'else' });
    return handles;
}

/**
 * Propagates Structured Streaming execution mode downstream from all streaming `SOURCE` nodes.
 * Preserves non-streaming status on static batch lookup branches feeding Stream-Static Joins.
 *
 * @param nodes Pipeline DAG nodes.
 * @param edges Pipeline DAG edges.
 * @returns Streaming node IDs, streaming edge IDs, Join execution modes, and source flag.
 */
export function computeStreamingNodeSet(
    nodes: DagNode[],
    edges: DagEdge[]
): StreamingPropagationResult {
    const streamingNodeIds = new Set<string>();
    const streamingEdgeIds = new Set<string>();
    const joinModes = new Map<string, JoinExecutionMode>();

    const validNodes = (nodes || []).filter(
        (n): n is DagNode => Boolean(n && n.id !== undefined && n.id !== null)
    );
    const nodeMap = new Map<string, DagNode>();
    for (const node of validNodes) {
        nodeMap.set(String(node.id), node);
    }

    interface NormalizedEdge {
        id?: string;
        source: string;
        target: string;
    }

    const outgoingEdges = new Map<string, NormalizedEdge[]>();
    const incomingEdges = new Map<string, NormalizedEdge[]>();
    for (const id of nodeMap.keys()) {
        outgoingEdges.set(id, []);
        incomingEdges.set(id, []);
    }

    for (const edge of edges || []) {
        if (!edge || edge.source === undefined || edge.target === undefined) {
            continue;
        }
        const src = String(edge.source);
        const tgt = String(edge.target);
        if (!nodeMap.has(src) || !nodeMap.has(tgt)) {
            continue;
        }
        const normalized: NormalizedEdge = {
            id:
                edge.id !== undefined && edge.id !== null && String(edge.id).length > 0
                    ? String(edge.id)
                    : undefined,
            source: src,
            target: tgt,
        };
        outgoingEdges.get(src)?.push(normalized);
        incomingEdges.get(tgt)?.push(normalized);
    }

    const queue: string[] = [];
    for (const [id, node] of nodeMap.entries()) {
        if (getNodeCategory(node) === 'SOURCE' && getEffectiveSourceReadMode(node) === 'streaming') {
            streamingNodeIds.add(id);
            queue.push(id);
        }
    }

    const hasStreamingSource = queue.length > 0;

    let head = 0;
    while (head < queue.length) {
        const currId = queue[head];
        head += 1;
        const neighbors = outgoingEdges.get(currId) || [];
        for (const edge of neighbors) {
            const targetNode = nodeMap.get(edge.target);
            if (!targetNode || getNodeCategory(targetNode) === 'SOURCE') {
                continue;
            }
            if (edge.id) {
                streamingEdgeIds.add(edge.id);
            }
            if (!streamingNodeIds.has(edge.target)) {
                streamingNodeIds.add(edge.target);
                queue.push(edge.target);
            }
        }
    }

    for (const [id, node] of nodeMap.entries()) {
        if (!isJoinNode(node)) {
            continue;
        }
        const inbound = incomingEdges.get(id) || [];
        let streamingCount = 0;
        let staticCount = 0;
        for (const edge of inbound) {
            if (streamingNodeIds.has(edge.source)) {
                streamingCount += 1;
            } else {
                staticCount += 1;
            }
        }

        if (streamingCount > 0 && staticCount > 0) {
            joinModes.set(id, 'stream_static');
        } else if (streamingCount > 0) {
            joinModes.set(id, 'stream_stream');
        } else {
            joinModes.set(id, 'batch_batch');
        }
    }

    return {
        streamingNodeIds,
        streamingEdgeIds,
        joinModes,
        hasStreamingSource,
    };
}
