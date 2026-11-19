/**
 * @fileoverview Pure factory functions for creating Spark pipeline graph nodes and edges.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { Edge } from 'reactflow';
import type { PipelineNode } from '../hooks/usePipelineGraph';
import { isParseMessageItem, isPubSubSourceNode } from './nodeKinds';

let fallbackCounter = 0;

/**
 * Generates a unique node ID with an optional prefix.
 * If crypto.randomUUID is unavailable, fall back to a counter with random suffix.
 */
export function createNodeId(prefix: string = ''): string {
  let uuid: string;
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    uuid = crypto.randomUUID();
  } else {
    fallbackCounter += 1;
    uuid = `${fallbackCounter}-${Math.random().toString(36).slice(2, 10)}`;
  }
  return prefix ? `${prefix}${uuid}` : uuid;
}

/**
 * Description of an item dragged or added from the sidebar palette.
 */
export interface PaletteItem {
  /** Node display label. */
  label: string;
  /** Node semantic type: 'SOURCE' | 'STEP' | 'SINK'. */
  type: string;
  /** Name of the icon to render. */
  iconName: string;
  /** Optional Apache Spark transform identifier (e.g. 'CsvSource', 'ParquetSink'). */
  sparkTransform?: string;
  /** Optional React Flow node component type (defaults to 'pipeline'). */
  nodeType?: string;
}

/**
 * Result bundle containing constructed pipeline nodes and new edges.
 */
export interface CreatedPipelineElements {
  /** Constructed pipeline nodes. */
  nodes: PipelineNode[];
  /** Constructed auto-wired edges. */
  edges: Edge[];
}

/**
 * Constructs a pipeline node and any automatic upstream edge for a palette item.
 */
export function createNodesForPaletteItem(
  item: PaletteItem,
  position: { x: number; y: number },
  existingNodes: PipelineNode[] = [],
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  _existingEdges: Edge[] = []
): CreatedPipelineElements {
  const nodeType = item.nodeType ?? 'pipeline';
  const nodeId = createNodeId();

  const newNode: PipelineNode = {
    id: nodeId,
    type: nodeType,
    position,
    data: {
      label: item.label,
      type: item.type,
      iconName: item.iconName,
      ...(item.sparkTransform ? { spark_transform: item.sparkTransform } : {}),
    },
  };

  const newEdges: Edge[] = [];
  if (item.type === 'STEP') {
    if (isParseMessageItem(item)) {
      const pubSubSource = existingNodes.find(isPubSubSourceNode);
      if (pubSubSource) {
        newEdges.push({
          id: `e-${pubSubSource.id}-${newNode.id}`,
          source: pubSubSource.id,
          target: newNode.id,
        });
      }
    } else {
      const hasStep = existingNodes.some((n) => n.data?.type === 'STEP');
      if (!hasStep) {
        const sourceNode = existingNodes.find((n) => n.data?.type === 'SOURCE');
        if (sourceNode) {
          newEdges.push({
            id: `e-${sourceNode.id}-${newNode.id}`,
            source: sourceNode.id,
            target: newNode.id,
            ...(item.sparkTransform === 'Join' || item.sparkTransform === 'MergeCdcScd'
              ? { targetHandle: 'left' }
              : {}),
          });
        }
      }
    }
  }

  return {
    nodes: [newNode],
    edges: newEdges,
  };
}
