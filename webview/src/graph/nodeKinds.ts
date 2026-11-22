/**
 * @fileoverview Pure predicates for runtime Spark node and palette item kind detection.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/**
 * Minimal structural shape of node data used for kind detection.
 */
export interface NodeDataLike {
  type?: string;
  spark_transform?: string;
  label?: string;
}

/**
 * Structural container for nodes containing optional node data.
 */
export interface NodeLike {
  data?: NodeDataLike;
}

/**
 * Minimal structural shape of a palette item used for kind matching.
 */
export interface PaletteItemLike {
  type: string;
  label: string;
  sparkTransform?: string;
}

/**
 * Normalizes input to access node data properties from either a full node or a raw data object.
 */
function getData(target?: NodeLike | NodeDataLike | null): NodeDataLike | undefined {
  if (!target || typeof target !== 'object') {
    return undefined;
  }
  if ('data' in target && target.data && typeof target.data === 'object') {
    return target.data as NodeDataLike;
  }
  return target as NodeDataLike;
}

/**
 * Extracts the display label from a node, data object, or raw string.
 */
export function extractLabel(target?: NodeLike | NodeDataLike | string | null): string | undefined {
  if (typeof target === 'string') {
    return target;
  }
  const data = getData(target);
  return typeof data?.label === 'string' ? data.label : undefined;
}

/**
 * Checks whether a palette item represents a payload parsing step.
 */
export function isParseMessageItem(
  item?: { label?: string; sparkTransform?: string } | null
): boolean {
  if (!item || typeof item !== 'object') {
    return false;
  }
  return (
    item.sparkTransform === 'ParsePayload' ||
    item.sparkTransform === 'ParseMessage' ||
    item.label === 'Parse Payload' ||
    item.label === 'Parse Message'
  );
}

/**
 * Checks whether a node represents a Pub/Sub transform.
 */
export function isPubSubNode(target?: NodeLike | NodeDataLike | null): boolean {
  const data = getData(target);
  if (!data) {
    return false;
  }
  return (
    data.spark_transform === 'PubSubSource' ||
    data.spark_transform === 'PubSubSink' ||
    data.spark_transform === 'PubSubIO' ||
    Boolean(data.label && (data.label.includes('PubSub') || data.label.includes('Pub/Sub')))
  );
}

/**
 * Checks whether a node represents a Pub/Sub SOURCE node.
 */
export function isPubSubSourceNode(target?: NodeLike | NodeDataLike | null): boolean {
  const data = getData(target);
  if (!data) {
    return false;
  }
  return data.type === 'SOURCE' && isPubSubNode(data);
}
