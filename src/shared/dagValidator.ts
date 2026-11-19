/**
 * @fileoverview Pure TypeScript Apache Spark DAG Topology and Semantics Validator.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * NOTE: Files in src/shared/ must stay strictly free of 'vscode', 'fs', 'path',
 * 'child_process', 'os', 'react', or any 'node:' module imports. This pure-TypeScript
 * module compiles into both the VS Code extension host (CommonJS) and the webview (Vite).
 *
 * Purpose:
 * Provides deterministic topological and semantic validation of Apache Spark pipeline graphs.
 * Detects cyclic dependencies via Kahn's algorithm with exact DFS cycle extraction, identifies
 * disconnected orphan nodes, validates direct Source-to-Sink pipelines, enforces Spark Join
 * left/right handle arity and stream-static or stream-stream watermark rules, checks multi-input
 * step arity, emits streaming aggregation watermark warnings, and validates checkpoint paths.
 */

import {
    computeStreamingNodeSet,
    getNodeCategory,
    getNodeInputArity,
    getNodeSparkTransform,
    isJoinNode,
    isMergeCdcScdNode,
} from './sparkDagRules';

/**
 * Node representation accepted by the DAG topology validator.
 */
export interface DagNode {
    id: string;
    type?: string;
    data?: {
        label?: string;
        type?: string;
        spark_transform?: string;
        [key: string]: any;
    };
    [key: string]: any;
}

/**
 * Directed edge representation accepted by the DAG topology validator.
 */
export interface DagEdge {
    id?: string;
    source: string;
    target: string;
    sourceHandle?: string | null;
    targetHandle?: string | null;
    [key: string]: any;
}

/**
 * Optional pipeline configuration context for Spark semantic validation.
 */
export interface DagValidationOptions {
    jobConfig?: Record<string, any>;
    requireCheckpoints?: boolean;
}

/**
 * Comprehensive topological and semantic validation report for a Spark pipeline DAG.
 */
export interface DagValidationResult {
    /** True if the pipeline DAG has no fatal errors */
    isValid: boolean;
    /** True if one or more cyclic dependencies were detected */
    hasCycles: boolean;
    /** Array of node IDs participating in cycles */
    cycleNodeIds: string[];
    /** Array of edge IDs forming the cycle loop(s) */
    cycleEdgeIds: string[];
    /** Array of cycle path loops as ordered node IDs */
    cyclePaths: string[][];
    /** Formatted human-readable string representation of the cycle path(s) */
    cyclePathString: string;
    /** Topologically sorted node IDs from root to terminal (empty if cyclic) */
    sortedNodeIds: string[];
    /** Disconnected nodes with zero incoming and zero outgoing edges (when graph size > 1) */
    orphanNodeIds: string[];
    /** Nodes missing required upstream connections */
    missingUpstreamNodeIds: string[];
    /** Nodes with disallowed or excess upstream connections */
    invalidUpstreamNodeIds: string[];
    /** Legacy compatibility array for side-input mismatches (always empty in Spark) */
    sideInputMismatchNodeIds: string[];
    /** Fatal error messages that block pipeline code generation */
    errors: string[];
    /** Non-fatal warning messages for user feedback */
    warnings: string[];
    /** Per-node fatal error messages keyed by node ID */
    nodeErrors?: Record<string, string[]>;
    /** Per-node warning messages keyed by node ID */
    nodeWarnings?: Record<string, string[]>;
}

interface NormalizedEdge {
    id?: string;
    source: string;
    target: string;
    sourceHandle?: string | null;
    targetHandle?: string | null;
}

const ALLOWED_CLOUD_CHECKPOINT_SCHEMES = new Set([
    'gs',
    's3a',
    's3',
    'abfss',
    'abfs',
    'wasbs',
    'wasb',
    'hdfs',
]);

const ALLOWED_LOCAL_CHECKPOINT_SCHEMES = new Set(['file', 'dbfs']);

const STREAM_STREAM_OUTER_JOIN_TYPES = new Set([
    'left',
    'left_outer',
    'right',
    'right_outer',
    'full',
    'full_outer',
]);

/**
 * Formats a human-readable node reference including its label when available.
 *
 * @param node DAG node instance.
 * @param id Fallback string node ID.
 * @returns Formatted node label and ID string.
 */
function formatNodeRef(node: DagNode | undefined, id: string): string {
    return node?.data?.label ? `"${node.data.label}" (${id})` : id;
}

/**
 * Appends a message to a per-node diagnostic map without duplicates.
 *
 * @param bucket Map from node ID to diagnostic string array.
 * @param nodeId Target node ID.
 * @param message Diagnostic message to append.
 */
function recordNodeDiagnostic(
    bucket: Record<string, string[]>,
    nodeId: string,
    message: string
): void {
    const existing = bucket[nodeId];
    if (!existing) {
        bucket[nodeId] = [message];
        return;
    }
    if (!existing.includes(message)) {
        existing.push(message);
    }
}

/**
 * Appends a node ID to an array only if it is not already present.
 *
 * @param list Target node ID list.
 * @param nodeId Node ID to insert.
 */
function pushUnique(list: string[], nodeId: string): void {
    if (!list.includes(nodeId)) {
        list.push(nodeId);
    }
}

/**
 * Reads the configuration properties record from a React Flow or YAML-shaped DAG node.
 * If `node.data.properties` is a plain object, returns it.
 * Otherwise returns the top-level `node.properties` object, or an empty record.
 *
 * @param node DAG node instance.
 * @returns Node properties record, or an empty record when unset.
 */
function getNodeProperties(node: DagNode): Record<string, unknown> {
    const isRecord = (value: unknown): value is Record<string, unknown> =>
        value !== null && typeof value === 'object' && !Array.isArray(value);
    const dataProperties: unknown = node.data?.properties;
    if (isRecord(dataProperties)) {
        return dataProperties;
    }
    const topLevelProperties: unknown = node.properties;
    if (isRecord(topLevelProperties)) {
        return topLevelProperties;
    }
    return {};
}

/**
 * Normalizes a Spark join type string (`inner`, `left_outer`, `left_semi`, `full_outer`, etc.).
 *
 * @param raw Raw join type property value.
 * @returns Normalized lowercase snake_case join type.
 */
function normalizeJoinType(raw: unknown): string {
    if (typeof raw !== 'string' || raw.trim().length === 0) {
        return 'inner';
    }
    const cleaned = raw
        .trim()
        .replace(/([a-z])([A-Z])/g, '$1_$2')
        .toLowerCase()
        .replace(/[\s-]+/g, '_');

    if (cleaned === 'outer') {
        return 'full_outer';
    }
    if (cleaned === 'leftouter') {
        return 'left_outer';
    }
    if (cleaned === 'rightouter') {
        return 'right_outer';
    }
    if (cleaned === 'fullouter') {
        return 'full_outer';
    }
    if (cleaned === 'leftsemi' || cleaned === 'semi') {
        return 'left_semi';
    }
    if (cleaned === 'leftanti' || cleaned === 'anti') {
        return 'left_anti';
    }
    return cleaned;
}

/**
 * Validates the syntax of a configured Structured Streaming checkpoint URI or directory path.
 *
 * @param rawPath Raw checkpoint location string.
 * @returns Error reason string when invalid, or null when valid.
 */
function validateCheckpointPathFormat(rawPath: string): string | null {
    if (rawPath.trim().length === 0) {
        return 'Checkpoint path cannot be whitespace-only.';
    }
    const trimmed = rawPath.trim();
    if (/[\r\n\t\0]/.test(trimmed)) {
        return 'Checkpoint path cannot contain control characters.';
    }

    const segments = trimmed.split(/[\\/]+/);
    if (segments.includes('..')) {
        return 'Checkpoint path cannot contain parent directory traversal ("..").';
    }

    const schemeMatch = /^([a-zA-Z][a-zA-Z0-9+.-]*):(.*)$/.exec(trimmed);
    if (schemeMatch) {
        const scheme = schemeMatch[1].toLowerCase();
        const rest = schemeMatch[2];

        // Allow single-letter Windows drive prefixes such as C:/checkpoints
        if (scheme.length === 1) {
            return null;
        }

        if (ALLOWED_CLOUD_CHECKPOINT_SCHEMES.has(scheme)) {
            if (/\s/.test(trimmed)) {
                return `Checkpoint URI "${trimmed}" cannot contain whitespace.`;
            }
            if (!/^\/\/[^/\s]+/.test(rest)) {
                return `Checkpoint URI "${trimmed}" must include a bucket or authority after "${scheme}://".`;
            }
            return null;
        }

        if (ALLOWED_LOCAL_CHECKPOINT_SCHEMES.has(scheme)) {
            if (!rest.startsWith('/')) {
                return `Checkpoint URI "${trimmed}" must start with "${scheme}:/" or "${scheme}:///".`;
            }
            return null;
        }

        return `Unsupported checkpoint URI scheme "${scheme}:". Use gs://, s3a://, abfss://, hdfs://, file://, or a local path.`;
    }

    return null;
}

/**
 * Validates the topology and Apache Spark semantics of a pipeline DAG.
 *
 * @param nodes List of pipeline nodes.
 * @param edges List of pipeline edges.
 * @param options Optional validation context containing `jobConfig` or checkpoint enforcement flags.
 * @returns Comprehensive validation result with cycle details, sorted nodes, errors, and warnings.
 */
export function validateDagTopology(
    nodes: DagNode[],
    edges: DagEdge[],
    options?: DagValidationOptions
): DagValidationResult {
    const validNodes = (nodes || []).filter((n) => n && n.id !== undefined && n.id !== null);
    const nodeIds = validNodes.map((n) => String(n.id));
    const nodeMap = new Map<string, DagNode>();
    for (const node of validNodes) {
        nodeMap.set(String(node.id), node);
    }

    if (nodeIds.length === 0) {
        return {
            isValid: true,
            hasCycles: false,
            cycleNodeIds: [],
            cycleEdgeIds: [],
            cyclePaths: [],
            cyclePathString: '',
            sortedNodeIds: [],
            orphanNodeIds: [],
            missingUpstreamNodeIds: [],
            invalidUpstreamNodeIds: [],
            sideInputMismatchNodeIds: [],
            errors: [],
            warnings: [],
            nodeErrors: {},
            nodeWarnings: {},
        };
    }

    // 1. Build adjacency structures and compute Kahn's in-degrees
    const inDegree = new Map<string, number>();
    const initialInDegree = new Map<string, number>();
    const outgoingNeighbors = new Map<string, string[]>();
    const incomingEdges = new Map<string, NormalizedEdge[]>();
    const outgoingEdges = new Map<string, NormalizedEdge[]>();
    const edgesByPair = new Map<string, string[]>();
    const validEdges: DagEdge[] = [];

    for (const id of nodeIds) {
        inDegree.set(id, 0);
        initialInDegree.set(id, 0);
        outgoingNeighbors.set(id, []);
        incomingEdges.set(id, []);
        outgoingEdges.set(id, []);
    }

    for (const edge of edges || []) {
        if (!edge || edge.source === undefined || edge.target === undefined) {
            continue;
        }
        const src = String(edge.source);
        const tgt = String(edge.target);

        if (inDegree.has(src) && inDegree.has(tgt)) {
            validEdges.push(edge);
            outgoingNeighbors.get(src)?.push(tgt);
            inDegree.set(tgt, (inDegree.get(tgt) ?? 0) + 1);
            initialInDegree.set(tgt, (initialInDegree.get(tgt) ?? 0) + 1);

            const normalized: NormalizedEdge = {
                id: edge.id !== undefined && edge.id !== null ? String(edge.id) : undefined,
                source: src,
                target: tgt,
                sourceHandle: edge.sourceHandle,
                targetHandle: edge.targetHandle,
            };
            outgoingEdges.get(src)?.push(normalized);
            incomingEdges.get(tgt)?.push(normalized);

            const pairKey = `${src}->${tgt}`;
            let pairEdges = edgesByPair.get(pairKey);
            if (!pairEdges) {
                pairEdges = [];
                edgesByPair.set(pairKey, pairEdges);
            }
            if (normalized.id) {
                pairEdges.push(normalized.id);
            }
        }
    }

    // 2. Run Kahn's topological sort
    const queue: string[] = [];
    for (const id of nodeIds) {
        if ((inDegree.get(id) ?? 0) === 0) {
            queue.push(id);
        }
    }

    const sortedNodeIds: string[] = [];
    let queueHead = 0;
    while (queueHead < queue.length) {
        const curr = queue[queueHead];
        queueHead += 1;
        sortedNodeIds.push(curr);

        const neighbors = outgoingNeighbors.get(curr) || [];
        for (const nxt of neighbors) {
            const remainingDegree = (inDegree.get(nxt) ?? 0) - 1;
            inDegree.set(nxt, remainingDegree);
            if (remainingDegree === 0) {
                queue.push(nxt);
            }
        }
    }

    const orphanNodeIds: string[] = [];
    const missingUpstreamNodeIds: string[] = [];
    const invalidUpstreamNodeIds: string[] = [];
    const sideInputMismatchNodeIds: string[] = [];
    const errors: string[] = [];
    const warnings: string[] = [];
    const nodeErrors: Record<string, string[]> = {};
    const nodeWarnings: Record<string, string[]> = {};

    // 3. Detect disconnected orphan nodes when graph contains more than 1 node
    if (nodeIds.length > 1) {
        for (const id of nodeIds) {
            const degIn = initialInDegree.get(id) || 0;
            const degOut = (outgoingNeighbors.get(id) || []).length;
            if (degIn === 0 && degOut === 0) {
                orphanNodeIds.push(id);
            }
        }

        if (orphanNodeIds.length > 0) {
            const orphanLabels = orphanNodeIds.map((id) => formatNodeRef(nodeMap.get(id), id));
            const orphanMsg = `Orphan node(s) detected: ${orphanLabels.join(', ')}. All steps must be connected into the pipeline.`;
            errors.push(orphanMsg);
            for (const id of orphanNodeIds) {
                recordNodeDiagnostic(nodeErrors, id, orphanMsg);
            }
        }
    }

    // 4. Check for missing core Source / Sink components
    const hasSource = validNodes.some((n) => getNodeCategory(n) === 'SOURCE');
    const hasSink = validNodes.some((n) => getNodeCategory(n) === 'SINK');

    if (!hasSource) {
        warnings.push('Pipeline has no SOURCE node. At least one SOURCE is required for execution.');
    }
    if (!hasSink) {
        warnings.push('Pipeline has no SINK node. At least one SINK is recommended for output delivery.');
    }

    // 5. Extract exact cycle paths if Kahn's sort did not resolve every node
    const cycleNodeIds: string[] = [];
    const cycleEdgeIds: string[] = [];
    const cyclePaths: string[][] = [];
    let cyclePathString = '';

    if (sortedNodeIds.length < nodeIds.length) {
        const remainingNodeIds = new Set<string>();
        for (const id of nodeIds) {
            if ((inDegree.get(id) || 0) > 0) {
                remainingNodeIds.add(id);
            }
        }

        const visited = new Set<string>();
        const recStack = new Map<string, number>();
        const currentPath: string[] = [];

        const dfs = (nodeId: string): void => {
            visited.add(nodeId);
            currentPath.push(nodeId);
            recStack.set(nodeId, currentPath.length - 1);

            const neighbors = outgoingNeighbors.get(nodeId) || [];
            for (const nxt of neighbors) {
                if (!remainingNodeIds.has(nxt)) {
                    continue;
                }

                const cycleStartIndex = recStack.get(nxt);
                if (cycleStartIndex !== undefined) {
                    const cycleLoop = currentPath.slice(cycleStartIndex).concat(nxt);
                    cyclePaths.push(cycleLoop);

                    for (let i = 0; i < cycleLoop.length - 1; i++) {
                        const from = cycleLoop[i];
                        const to = cycleLoop[i + 1];
                        const matchingEdges = edgesByPair.get(`${from}->${to}`) || [];
                        for (const eid of matchingEdges) {
                            if (!cycleEdgeIds.includes(eid)) {
                                cycleEdgeIds.push(eid);
                            }
                        }
                    }
                } else if (!visited.has(nxt)) {
                    dfs(nxt);
                }
            }

            currentPath.pop();
            recStack.delete(nodeId);
        };

        for (const id of remainingNodeIds) {
            if (!visited.has(id)) {
                dfs(id);
            }
        }

        if (cyclePaths.length > 0) {
            const uniqueCycleNodes = new Set<string>();
            for (const path of cyclePaths) {
                for (const nid of path) {
                    uniqueCycleNodes.add(nid);
                }
            }
            for (const nid of uniqueCycleNodes) {
                cycleNodeIds.push(nid);
            }

            const firstPath = cyclePaths[0];
            cyclePathString = firstPath
                .map((nid) => {
                    const node = nodeMap.get(nid);
                    return node?.data?.label ? `${node.data.label} (${nid})` : nid;
                })
                .join(' -> ');
        } else {
            for (const nid of remainingNodeIds) {
                cycleNodeIds.push(nid);
            }
            cyclePathString = cycleNodeIds.join(' -> ');
        }

        const cycleError = `Cyclic dependency detected in pipeline graph: ${cyclePathString}`;
        errors.push(cycleError);
        for (const nid of cycleNodeIds) {
            recordNodeDiagnostic(nodeErrors, nid, cycleError);
        }
    }

    // 6. Compute streaming propagation and Spark-specific semantic rules
    const orphanSet = new Set(orphanNodeIds);
    const cycleSet = new Set(cycleNodeIds);
    const streaming = computeStreamingNodeSet(validNodes, validEdges);

    /**
     * Checks whether all streaming paths reaching `branchTipId` pass through a `Watermark` step.
     *
     * @param branchTipId Upstream node ID feeding a streaming join or aggregation.
     * @returns True if every streaming path into `branchTipId` includes a `Watermark` step.
     */
    const branchHasWatermark = (branchTipId: string): boolean => {
        if (!streaming.streamingNodeIds.has(branchTipId)) {
            return false;
        }
        const visited = new Set<string>();
        const walkQueue: string[] = [branchTipId];
        let head = 0;
        let sawWatermark = false;
        let reachedUnwatermarkedSource = false;

        while (head < walkQueue.length) {
            const currId = walkQueue[head];
            head += 1;
            if (visited.has(currId)) {
                continue;
            }
            visited.add(currId);

            const currNode = nodeMap.get(currId);
            if (!currNode) {
                continue;
            }

            if (getNodeSparkTransform(currNode) === 'Watermark') {
                sawWatermark = true;
                continue;
            }

            if (getNodeCategory(currNode) === 'SOURCE') {
                reachedUnwatermarkedSource = true;
                continue;
            }

            const parents = incomingEdges.get(currId) || [];
            for (const parentEdge of parents) {
                if (streaming.streamingNodeIds.has(parentEdge.source) && !visited.has(parentEdge.source)) {
                    walkQueue.push(parentEdge.source);
                }
            }
        }

        return sawWatermark && !reachedUnwatermarkedSource;
    };

    const addFatalNodeError = (nodeId: string, msg: string): void => {
        errors.push(msg);
        recordNodeDiagnostic(nodeErrors, nodeId, msg);
    };

    const addNodeWarning = (nodeId: string, msg: string): void => {
        warnings.push(msg);
        recordNodeDiagnostic(nodeWarnings, nodeId, msg);
    };

    for (const id of nodeIds) {
        const node = nodeMap.get(id);
        if (!node) {
            continue;
        }

        const category = getNodeCategory(node);
        const transform = getNodeSparkTransform(node);
        const inbound = incomingEdges.get(id) || [];
        const outbound = outgoingEdges.get(id) || [];
        const isOrphan = orphanSet.has(id);
        const inCycle = cycleSet.has(id);
        const ref = formatNodeRef(node, id);

        // 6a. SOURCE nodes must never have incoming connections
        if (category === 'SOURCE') {
            if (inbound.length > 0 && !inCycle) {
                pushUnique(invalidUpstreamNodeIds, id);
                addFatalNodeError(
                    id,
                    `Source node ${ref} cannot have incoming connections (found ${inbound.length}).`
                );
            }
            continue;
        }

        // 6b. Join step handle arity, stream-static join compatibility, and stream-stream outer watermarks
        if (isJoinNode(node)) {
            if (!isOrphan && !inCycle) {
                const leftEdges = inbound.filter((e) => e.targetHandle === 'left');
                const rightEdges = inbound.filter((e) => e.targetHandle === 'right');
                const untypedEdges = inbound.filter(
                    (e) => e.targetHandle !== 'left' && e.targetHandle !== 'right'
                );

                if (untypedEdges.length > 0) {
                    pushUnique(invalidUpstreamNodeIds, id);
                    addFatalNodeError(
                        id,
                        `Join node ${ref} requires incoming edges to connect to explicit "left" and "right" handles.`
                    );
                }

                if (leftEdges.length === 0 || rightEdges.length === 0) {
                    const missingHandles: string[] = [];
                    if (leftEdges.length === 0) {
                        missingHandles.push('"left"');
                    }
                    if (rightEdges.length === 0) {
                        missingHandles.push('"right"');
                    }
                    pushUnique(missingUpstreamNodeIds, id);
                    addFatalNodeError(
                        id,
                        `Join node ${ref} requires 1 "left" input and 1 "right" input (missing ${missingHandles.join(' and ')}).`
                    );
                }

                if (leftEdges.length > 1 || rightEdges.length > 1) {
                    pushUnique(invalidUpstreamNodeIds, id);
                    addFatalNodeError(
                        id,
                        `Join node ${ref} accepts at most 1 incoming edge per handle ("left": ${leftEdges.length}, "right": ${rightEdges.length}).`
                    );
                }

                if (leftEdges.length === 1 && rightEdges.length === 1 && untypedEdges.length === 0) {
                    const joinProps = getNodeProperties(node);
                    const joinType = normalizeJoinType(joinProps.joinType ?? joinProps.how);
                    const joinMode = streaming.joinModes.get(id) ?? 'batch_batch';
                    const leftSourceId = leftEdges[0].source;
                    const rightSourceId = rightEdges[0].source;
                    const leftIsStreaming = streaming.streamingNodeIds.has(leftSourceId);
                    const rightIsStreaming = streaming.streamingNodeIds.has(rightSourceId);

                    if (joinMode === 'stream_static') {
                        if (joinType === 'full' || joinType === 'full_outer') {
                            addFatalNodeError(
                                id,
                                `Stream-static Join node ${ref} does not support "${joinType}" joins in Spark Structured Streaming.`
                            );
                        } else if (
                            !leftIsStreaming &&
                            rightIsStreaming &&
                            (joinType === 'left' ||
                                joinType === 'left_outer' ||
                                joinType === 'left_semi' ||
                                joinType === 'left_anti')
                        ) {
                            addFatalNodeError(
                                id,
                                `Stream-static Join node ${ref} cannot use "${joinType}" join when "left" is static batch and "right" is streaming.`
                            );
                        } else if (
                            leftIsStreaming &&
                            !rightIsStreaming &&
                            (joinType === 'right' || joinType === 'right_outer')
                        ) {
                            addFatalNodeError(
                                id,
                                `Stream-static Join node ${ref} cannot use "${joinType}" join when "right" is static batch and "left" is streaming.`
                            );
                        }
                    } else if (
                        joinMode === 'stream_stream' &&
                        STREAM_STREAM_OUTER_JOIN_TYPES.has(joinType)
                    ) {
                        const leftWatermarked = branchHasWatermark(leftSourceId);
                        const rightWatermarked = branchHasWatermark(rightSourceId);
                        if (!leftWatermarked || !rightWatermarked) {
                            const missingSides: string[] = [];
                            if (!leftWatermarked) {
                                missingSides.push('"left"');
                            }
                            if (!rightWatermarked) {
                                missingSides.push('"right"');
                            }
                            addFatalNodeError(
                                id,
                                `Stream-stream "${joinType}" Join node ${ref} requires an upstream Watermark step on streaming inputs (missing on ${missingSides.join(' and ')}).`
                            );
                        }
                    }
                }
            }
            continue;
        }

        // 6b2. MergeCdcScd two-input handle arity and static target snapshot validation
        if (isMergeCdcScdNode(node)) {
            if (!isOrphan && !inCycle) {
                const leftEdges = inbound.filter((e) => e.targetHandle === 'left');
                const rightEdges = inbound.filter((e) => e.targetHandle === 'right');
                const untypedEdges = inbound.filter(
                    (e) => e.targetHandle !== 'left' && e.targetHandle !== 'right'
                );

                if (untypedEdges.length > 0) {
                    pushUnique(invalidUpstreamNodeIds, id);
                    addFatalNodeError(
                        id,
                        `MergeCdcScd node ${ref} requires incoming edges to connect to explicit "left" and "right" handles.`
                    );
                }

                if (leftEdges.length === 0 || rightEdges.length === 0) {
                    const missingHandles: string[] = [];
                    if (leftEdges.length === 0) {
                        missingHandles.push('"left"');
                    }
                    if (rightEdges.length === 0) {
                        missingHandles.push('"right"');
                    }
                    pushUnique(missingUpstreamNodeIds, id);
                    addFatalNodeError(
                        id,
                        `MergeCdcScd node ${ref} requires 1 "left" input and 1 "right" input (missing ${missingHandles.join(' and ')}).`
                    );
                }

                if (leftEdges.length > 1 || rightEdges.length > 1) {
                    pushUnique(invalidUpstreamNodeIds, id);
                    addFatalNodeError(
                        id,
                        `MergeCdcScd node ${ref} accepts at most 1 incoming edge per handle ("left": ${leftEdges.length}, "right": ${rightEdges.length}).`
                    );
                }

                if (leftEdges.length === 1 && rightEdges.length === 1 && untypedEdges.length === 0) {
                    const rightSourceId = rightEdges[0].source;
                    if (streaming.streamingNodeIds.has(rightSourceId)) {
                        pushUnique(invalidUpstreamNodeIds, id);
                        addFatalNodeError(
                            id,
                            `MergeCdcScd node ${ref} requires a static batch target table snapshot on the "right" handle, not a streaming input.`
                        );
                    }
                }
            }
            continue;
        }

        // 6c. Multi-input UnionByName and SetOperation (>= 2 inputs)
        if (transform === 'UnionByName' || transform === 'SetOperation') {
            if (!isOrphan && !inCycle && inbound.length < 2) {
                pushUnique(missingUpstreamNodeIds, id);
                addFatalNodeError(
                    id,
                    `${transform} node ${ref} requires at least 2 upstream inputs (found ${inbound.length}).`
                );
            }
            continue;
        }

        // 6d. SparkSqlQuery and other N-input steps (>= 1 input)
        const inputArity = getNodeInputArity(node);
        if (inputArity === 'n') {
            if (
                !isOrphan &&
                !inCycle &&
                inbound.length < 1 &&
                (transform === 'SparkSqlQuery' || outbound.length > 0)
            ) {
                pushUnique(missingUpstreamNodeIds, id);
                addFatalNodeError(
                    id,
                    `${transform || 'Multi-input step'} node ${ref} requires at least 1 upstream input.`
                );
            }
            continue;
        }

        // 6e. Single-input STEP and SINK arity validation
        if (category === 'STEP' || category === 'SINK') {
            if (!inCycle && inbound.length > 1) {
                pushUnique(invalidUpstreamNodeIds, id);
                addFatalNodeError(
                    id,
                    `${category === 'SINK' ? 'Sink' : 'Step'} node ${ref} accepts only 1 upstream input (found ${inbound.length}).`
                );
            } else if (!isOrphan && !inCycle && inbound.length === 0 && outbound.length > 0) {
                pushUnique(missingUpstreamNodeIds, id);
                addFatalNodeError(id, `Step node ${ref} has no upstream input connection.`);
            }
        }

        // 6f. Streaming GroupByAgg and Deduplicate missing Watermark warnings
        if (
            streaming.streamingNodeIds.has(id) &&
            (transform === 'GroupByAgg' || transform === 'Deduplicate')
        ) {
            const streamingParents = inbound.filter((e) =>
                streaming.streamingNodeIds.has(e.source)
            );
            const hasWatermark =
                streamingParents.length > 0 &&
                streamingParents.every((e) => branchHasWatermark(e.source));

            if (!hasWatermark) {
                if (transform === 'GroupByAgg') {
                    addNodeWarning(
                        id,
                        `Streaming GroupByAgg node ${ref} has no upstream Watermark step. Add a Watermark step before aggregation to bound state.`
                    );
                } else {
                    addNodeWarning(
                        id,
                        `Streaming Deduplicate node ${ref} has no upstream Watermark step. Add a Watermark step for dropDuplicatesWithinWatermark state eviction.`
                    );
                }
            }
        }

        // 6g. Streaming SINK checkpoint path validation
        if (category === 'SINK' && streaming.streamingNodeIds.has(id)) {
            const rawSinkCheckpoint = getNodeProperties(node).checkpointLocation;
            const rawRootCheckpoint = options?.jobConfig?.checkpointRoot;

            let hasConfiguredCheckpoint = false;
            let hasInvalidCheckpointError = false;

            if (typeof rawSinkCheckpoint === 'string' && rawSinkCheckpoint.length > 0) {
                const formatError = validateCheckpointPathFormat(rawSinkCheckpoint);
                if (formatError) {
                    hasInvalidCheckpointError = true;
                    addFatalNodeError(
                        id,
                        `Streaming Sink node ${ref} has an invalid checkpointLocation: ${formatError}`
                    );
                } else {
                    hasConfiguredCheckpoint = true;
                }
            } else if (typeof rawRootCheckpoint === 'string' && rawRootCheckpoint.length > 0) {
                const formatError = validateCheckpointPathFormat(rawRootCheckpoint);
                if (formatError) {
                    hasInvalidCheckpointError = true;
                    addFatalNodeError(
                        id,
                        `Streaming Sink node ${ref} has an invalid pipeline checkpointRoot: ${formatError}`
                    );
                } else {
                    hasConfiguredCheckpoint = true;
                }
            }

            if (
                !hasConfiguredCheckpoint &&
                !hasInvalidCheckpointError &&
                transform !== 'MemorySink'
            ) {
                const enforceCheckpointError =
                    options?.requireCheckpoints === true || options?.jobConfig !== undefined;
                const checkpointMsg = `Streaming Sink node ${ref} is missing a checkpoint path. Configure "checkpointLocation" on the sink or "checkpointRoot" in jobConfig.`;
                if (enforceCheckpointError) {
                    addFatalNodeError(id, checkpointMsg);
                } else {
                    addNodeWarning(id, checkpointMsg);
                }
            }
        }
    }

    const hasCycles = cycleNodeIds.length > 0;
    const isValid = errors.length === 0;

    return {
        isValid,
        hasCycles,
        cycleNodeIds,
        cycleEdgeIds,
        cyclePaths,
        cyclePathString,
        sortedNodeIds: hasCycles ? [] : sortedNodeIds,
        orphanNodeIds,
        missingUpstreamNodeIds,
        invalidUpstreamNodeIds,
        sideInputMismatchNodeIds,
        errors,
        warnings,
        nodeErrors,
        nodeWarnings,
    };
}
