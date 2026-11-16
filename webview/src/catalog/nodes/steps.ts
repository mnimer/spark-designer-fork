/** @fileoverview Step node catalog definitions for the pipeline palette.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { NodeDefinition } from './types';

// Step node definitions
/**
 * Step/transform node definitions available in the palette.
 */
export const STEP_NODES: NodeDefinition[] = [
  { label: 'ParDo', type: 'STEP', iconName: 'Code', sparkTransform: 'ParDo', badge: 'Core', description: 'Apply custom per-element DoFn transform', category: 'Core Transforms' },
  { label: 'Parse Message', type: 'STEP', iconName: 'FileCode', sparkTransform: 'ParseMessage', badge: 'Parser', description: 'Parse structured JSON/Avro payload', category: 'Message Parsers' },
  { label: 'Add Timestamp', type: 'STEP', iconName: 'Calendar', sparkTransform: 'WithTimestamps', badge: 'Timestamp', description: 'Assign event timestamps to elements', category: 'Element Transforms' },
  { label: 'Filter', type: 'STEP', iconName: 'Filter', sparkTransform: 'Filter', badge: 'Condition', description: 'Filter elements matching boolean predicate', category: 'Element Transforms' },
  { label: 'FlatMap', type: 'STEP', iconName: 'GitMerge', sparkTransform: 'FlatMap', badge: '1-to-N', description: 'Map one element to zero or more elements', category: 'Element Transforms' },
  { label: 'Keys', type: 'STEP', iconName: 'Key', sparkTransform: 'Keys', badge: 'Key-Value', description: 'Extract key components from KV pairs', category: 'Element Transforms' },
  { label: 'KV Mapper', type: 'STEP', iconName: 'Hash', sparkTransform: 'KVMapper', badge: 'Mapping', description: 'Map elements to key-value pairs', category: 'Element Transforms' },
  { label: 'KV Swap', type: 'STEP', iconName: 'Shuffle', sparkTransform: 'KvSwap', badge: 'Swap', description: 'Swap keys and values in KV pairs', category: 'Element Transforms' },
  { label: 'Map', type: 'STEP', iconName: 'Activity', sparkTransform: 'Map', badge: '1-to-1', description: 'Transform elements with 1-to-1 function', category: 'Element Transforms' },
  { label: 'To String', type: 'STEP', iconName: 'Type', sparkTransform: 'ToString', badge: 'Format', description: 'Convert elements into string representations', category: 'Element Transforms' },
  { label: 'Values', type: 'STEP', iconName: 'Grid', sparkTransform: 'Values', badge: 'Values', description: 'Extract value components from KV pairs', category: 'Element Transforms' },
  { label: 'Approximate Quantiles', type: 'STEP', iconName: 'BarChart', sparkTransform: 'ApproximateQuantiles', badge: 'Quantile', description: 'Compute approximate distribution quantiles', category: 'Aggregations' },
  { label: 'Approximate Unique', type: 'STEP', iconName: 'BarChart', sparkTransform: 'ApproximateUnique', badge: 'Distinct', description: 'Estimate count of distinct elements', category: 'Aggregations' },
  { label: 'Batch Elements', type: 'STEP', iconName: 'BarChart', sparkTransform: 'BatchElements', badge: 'Batch', description: 'Group elements into batches', category: 'Aggregations' },
  { label: 'Co Group By Key', type: 'STEP', iconName: 'BarChart', sparkTransform: 'CoGroupByKey', badge: 'Co-Group', description: 'Group multiple PCollections by common key', category: 'Aggregations' },
  { label: 'Combine', type: 'STEP', iconName: 'BarChart', sparkTransform: 'Combine', badge: 'Combine', description: 'Combine elements with associative function', category: 'Aggregations' },
  { label: 'Combine With Context', type: 'STEP', iconName: 'BarChart', sparkTransform: 'CombineWithContext', badge: 'Context', description: 'Combine elements with pipeline side inputs', category: 'Aggregations' },
  { label: 'Count', type: 'STEP', iconName: 'BarChart', sparkTransform: 'Count', badge: 'Counter', description: 'Count total occurrences of elements', category: 'Aggregations' },
  { label: 'Distinct', type: 'STEP', iconName: 'BarChart', sparkTransform: 'Distinct', badge: 'Unique', description: 'Deduplicate elements in collection', category: 'Aggregations' },
  { label: 'GroupBy & Aggregate', type: 'STEP', iconName: 'BarChart', sparkTransform: 'GroupByAgg', badge: 'Group & Window', description: 'Group rows by columns and optional time window with aggregations', category: 'Aggregations' },
  { label: 'Group By Key', type: 'STEP', iconName: 'BarChart', sparkTransform: 'GroupByKey', badge: 'Group', description: 'Group PCollection of KV pairs by key', category: 'Aggregations' },
  { label: 'Group Into Batches', type: 'STEP', iconName: 'BarChart', sparkTransform: 'GroupIntoBatches', badge: 'Bundle', description: 'Group elements into batched bundles', category: 'Aggregations' },
  { label: 'Latest', type: 'STEP', iconName: 'BarChart', sparkTransform: 'Latest', badge: 'Latest', description: 'Select element with latest timestamp', category: 'Aggregations' },
  { label: 'Max', type: 'STEP', iconName: 'BarChart', sparkTransform: 'Max', badge: 'Max', description: 'Calculate maximum value in collection', category: 'Aggregations' },
  { label: 'Mean', type: 'STEP', iconName: 'BarChart', sparkTransform: 'Mean', badge: 'Average', description: 'Calculate arithmetic mean across values', category: 'Aggregations' },
  { label: 'Min', type: 'STEP', iconName: 'BarChart', sparkTransform: 'Min', badge: 'Min', description: 'Calculate minimum value in collection', category: 'Aggregations' },
  { label: 'Sample', type: 'STEP', iconName: 'BarChart', sparkTransform: 'Sample', badge: 'Sample', description: 'Extract random sample of elements', category: 'Aggregations' },
  { label: 'Sum', type: 'STEP', iconName: 'BarChart', sparkTransform: 'Sum', badge: 'Sum', description: 'Calculate sum across numeric elements', category: 'Aggregations' },
  { label: 'Top', type: 'STEP', iconName: 'BarChart', sparkTransform: 'Top', badge: 'Top-N', description: 'Extract top N elements', category: 'Aggregations' },
  { label: 'Watermark', type: 'STEP', iconName: 'Clock', sparkTransform: 'Watermark', badge: 'Event Time', description: 'Define event-time watermark threshold for late streaming data', category: 'Streaming' },
  { label: 'Window', type: 'STEP', iconName: 'Clock', sparkTransform: 'WindowInto', badge: 'Windowing', description: 'Assign elements into fixed/sliding windows', category: 'Windows' },
  { label: 'Flatten', type: 'STEP', iconName: 'Layers', sparkTransform: 'Flatten', badge: 'Merge', description: 'Merge multiple PCollections into one', category: 'Other Transforms' },
  { label: 'ReShuffle', type: 'STEP', iconName: 'Shuffle', sparkTransform: 'Reshuffle', badge: 'Partition', description: 'Redistribute elements evenly across workers', category: 'Other Transforms' },
];
