/**
 * @fileoverview Pure layout calculation module for pipeline graph nodes using Kahn's topological sort.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { Edge } from 'reactflow';
import type { PipelineNode } from '../hooks/usePipelineGraph';

/** Starting X coordinate for rank 0 columns, px. */
export const START_X = 80;

/** Starting Y baseline coordinate for centering columns, px. */
export const START_Y = 150;

/** Default horizontal distance between rank columns, px. */
export const COL_WIDTH = 300;

/** Vertical distance between nodes in the same column, px. */
export const ROW_HEIGHT = 120;

/** Minimum horizontal gap between the widest node in a column and the next column, px. */
const MIN_COL_GAP = 64;

/**
 * Estimates the rendered pixel width of a pipeline node so wide labels (such as
 * "Broadcast Join Orders With Currency Rates") push the next column far enough right
 * to prevent overlapping downstream nodes.
 *
 * @param node - Pipeline node to measure
 * @returns Estimated rendered pixel width
 */
function estimateNodeWidth(node: PipelineNode): number {
  const labelLen = node.data?.label?.length ?? 0;
  // 88px accounts for node padding, icon badge, gap, and rename button (~7.5px per label char).
  const fromLabel = Math.ceil(88 + labelLen * 7.5);
  const measured = typeof node.width === 'number' ? node.width : 0;
  return Math.max(180, measured, fromLabel);
}

/**
 * Computes vertical handle affinity for each node (-1 for Join/MergeCdcScd `left` branches,
 * +1 for `right` branches, and 0 otherwise) so `left` inputs sit above `right` inputs.
 *
 * @param targetNodes - Pipeline nodes in the graph
 * @param targetEdges - Directed edges between nodes
 * @param adj - Forward adjacency map keyed by source node ID
 * @param ranks - Topological column rank keyed by node ID
 * @param maxRank - Highest column rank in the graph
 * @returns Map of node ID to vertical ordering affinity (-1, 0, or 1)
 */
function computeHandleAffinity(
  targetNodes: PipelineNode[],
  targetEdges: Edge[],
  adj: Record<string, string[]>,
  ranks: Record<string, number>,
  maxRank: number
): Record<string, number> {
  const affinity: Record<string, number> = {};
  targetNodes.forEach((n) => {
    affinity[n.id] = 0;
  });

  targetEdges.forEach((e) => {
    if (affinity[e.source] === undefined || affinity[e.target] === undefined) {
      return;
    }
    if (e.targetHandle === 'left') {
      affinity[e.source] -= 1;
    } else if (e.targetHandle === 'right') {
      affinity[e.source] += 1;
    }
  });

  const nodesByRank: Record<number, string[]> = {};
  targetNodes.forEach((n) => {
    const r = ranks[n.id] ?? 0;
    if (!nodesByRank[r]) nodesByRank[r] = [];
    nodesByRank[r].push(n.id);
  });

  for (let r = maxRank; r >= 0; r--) {
    for (const id of nodesByRank[r] ?? []) {
      if (affinity[id] !== 0) {
        continue;
      }
      const neighbors = adj[id] ?? [];
      if (neighbors.length === 0) {
        continue;
      }
      const allLeft = neighbors.every((nextId) => (affinity[nextId] ?? 0) < 0);
      const allRight = neighbors.every((nextId) => (affinity[nextId] ?? 0) > 0);
      if (allLeft) {
        affinity[id] = -1;
      } else if (allRight) {
        affinity[id] = 1;
      }
    }
  }

  return affinity;
}

/**
 * Computes topological columns using Kahn's algorithm and positions nodes left-to-right.
 *
 * Rank assignment:
 * - Root nodes (in-degree = 0) start at rank 0.
 * - For each traversed node, downstream neighbors receive rank = max(neighborRank, currentRank + 1).
 * - Leftover nodes due to cycles fall back to predefined ranks:
 *   SOURCE = 0, STEP = 1, SINK = 2.
 *
 * Vertical positioning:
 * - Within each column, branches feeding dual-input `left` handles are ordered above branches
 *   feeding `right` handles so Spark `Join` and `MergeCdcScd` inputs align without crossing.
 * - Nodes within each column are centered vertically around `START_Y` (150).
 *
 * @param targetNodes - List of pipeline nodes to position
 * @param targetEdges - Directed edges representing dependencies between nodes
 * @returns Array of pipeline nodes with updated { x, y } coordinates in position
 */
export function computeLayoutedNodes(targetNodes: PipelineNode[], targetEdges: Edge[]): PipelineNode[] {
  // 1. Build adjacency lists
  const adj: Record<string, string[]> = {};
  const inDegree: Record<string, number> = {};

  targetNodes.forEach((n) => {
    adj[n.id] = [];
    inDegree[n.id] = 0;
  });

  targetEdges.forEach((e) => {
    if (adj[e.source] && adj[e.target] !== undefined) {
      adj[e.source].push(e.target);
      inDegree[e.target]++;
    }
  });

  // 2. Queue for Kahn's topological sort to compute columns (ranks)
  const ranks: Record<string, number> = {};
  const queue: string[] = [];

  // Find all root nodes (in-degree = 0)
  targetNodes.forEach((n) => {
    if (inDegree[n.id] === 0) {
      queue.push(n.id);
      ranks[n.id] = 0;
    }
  });

  while (queue.length > 0) {
    const curr = queue.shift()!;
    const currRank = ranks[curr];

    adj[curr].forEach((neighbor) => {
      ranks[neighbor] = Math.max(ranks[neighbor] || 0, currRank + 1);
      inDegree[neighbor]--;
      if (inDegree[neighbor] === 0) {
        queue.push(neighbor);
      }
    });
  }

  // If there are leftover nodes due to cycles, assign fallback ranks
  targetNodes.forEach((n) => {
    if (ranks[n.id] === undefined) {
      if (n.data?.type === 'SOURCE') ranks[n.id] = 0;
      else if (n.data?.type === 'SINK') ranks[n.id] = 2;
      else ranks[n.id] = 1;
    }
  });

  // 3. Group nodes by their calculated column rank and compute column X coordinates
  const columns: Record<number, string[]> = {};
  const originalOrder: Record<string, number> = {};
  let maxRank = 0;
  targetNodes.forEach((n, index) => {
    originalOrder[n.id] = index;
    const r = ranks[n.id];
    if (!columns[r]) columns[r] = [];
    columns[r].push(n.id);
    if (r > maxRank) maxRank = r;
  });

  const affinity = computeHandleAffinity(targetNodes, targetEdges, adj, ranks, maxRank);
  for (let r = 0; r <= maxRank; r++) {
    columns[r]?.sort((a, b) => {
      const signDiff = Math.sign(affinity[a] ?? 0) - Math.sign(affinity[b] ?? 0);
      return signDiff !== 0 ? signDiff : (originalOrder[a] ?? 0) - (originalOrder[b] ?? 0);
    });
  }

  const nodeById = new Map<string, PipelineNode>(targetNodes.map((n) => [n.id, n]));
  const colX: Record<number, number> = {};
  let currentX = START_X;
  for (let r = 0; r <= maxRank; r++) {
    colX[r] = currentX;
    const colNodeIds = columns[r] ?? [];
    const widest = colNodeIds.reduce((maxW, id) => {
      const node = nodeById.get(id);
      return node ? Math.max(maxW, estimateNodeWidth(node)) : maxW;
    }, 0);
    // Snap any widened column step to the 8pt grid
    const colStep = Math.max(COL_WIDTH, Math.ceil((widest + MIN_COL_GAP) / 8) * 8);
    currentX += colStep;
  }

  // 4. Center each column vertically around START_Y
  const yByNodeId: Record<string, number> = {};
  for (let r = 0; r <= maxRank; r++) {
    const colNodes = columns[r] ?? [];
    const totalHeight = (colNodes.length - 1) * ROW_HEIGHT;
    colNodes.forEach((id, idx) => {
      yByNodeId[id] = START_Y + idx * ROW_HEIGHT - totalHeight / 2;
    });
  }

  // 5. Return nodes with updated positions
  return targetNodes.map((node) => {
    const rank = ranks[node.id];
    return {
      ...node,
      position: {
        x: colX[rank],
        y: yByNodeId[node.id],
      },
    };
  });
}
