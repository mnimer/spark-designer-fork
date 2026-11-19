/** @fileoverview Step node catalog definitions for the Apache Spark pipeline palette.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { NodeDefinition } from './types';

/**
 * Step and transformation node definitions available in the Spark 4.x palette (OP1-OP29).
 */
export const STEP_NODES: NodeDefinition[] = [
  // Column & Row (OP1-OP11)
  { label: 'Select', type: 'STEP', iconName: 'Grid', sparkTransform: 'Select', badge: 'Project', description: 'Project, rename, or compute columns using select or selectExpr expressions', category: 'Column & Row', inputArity: 1, errorOutput: false },
  { label: 'Filter', type: 'STEP', iconName: 'Filter', sparkTransform: 'Filter', badge: 'Predicate', description: 'Filter rows matching a boolean predicate and optionally route rejected rows to error DLQ', category: 'Column & Row', inputArity: 1, errorOutput: true },
  { label: 'With Columns', type: 'STEP', iconName: 'Wrench', sparkTransform: 'WithColumns', badge: 'Derive', description: 'Add or overwrite multiple derived columns in a single Catalyst projection pass', category: 'Column & Row', inputArity: 1, errorOutput: false },
  { label: 'Rename & Drop Columns', type: 'STEP', iconName: 'Type', sparkTransform: 'RenameDropColumns', badge: 'Schema', description: 'Rename columns in bulk with withColumnsRenamed and drop unneeded columns', category: 'Column & Row', inputArity: 1, errorOutput: false },
  { label: 'Parse Payload', type: 'STEP', iconName: 'FileCode', sparkTransform: 'ParsePayload', badge: 'Parser', description: 'Parse JSON, Avro, CSV, or XML payloads into structured columns with corrupt-record DLQ routing', category: 'Column & Row', inputArity: 1, errorOutput: true },
  { label: 'Explode', type: 'STEP', iconName: 'GitMerge', sparkTransform: 'Explode', badge: '1-to-N', description: 'Unnest array, map, or struct-array columns into individual rows using explode or posexplode', category: 'Column & Row', inputArity: 1, errorOutput: false },
  { label: 'Handle Nulls', type: 'STEP', iconName: 'Activity', sparkTransform: 'HandleNulls', badge: 'Cleanse', description: 'Fill, drop, or replace null and NaN values across selected DataFrame columns', category: 'Column & Row', inputArity: 1, errorOutput: false },
  { label: 'Order By', type: 'STEP', iconName: 'ListOrdered', sparkTransform: 'OrderBy', badge: 'Sort', description: 'Sort rows globally with orderBy or within partitions using directional and null-order rules', category: 'Column & Row', inputArity: 1, errorOutput: false },
  { label: 'Limit & Sample', type: 'STEP', iconName: 'Hash', sparkTransform: 'LimitSample', badge: 'Subset', description: 'Cap output row count with limit or draw a deterministic random sample of rows', category: 'Column & Row', inputArity: 1, errorOutput: false },
  { label: 'Conditional Split', type: 'STEP', iconName: 'Share2', sparkTransform: 'ConditionalSplit', badge: 'Router', description: 'Route rows across dynamic boolean rule output handles plus an automatic else fallback branch', category: 'Column & Row', inputArity: 1, errorOutput: false },
  { label: 'Data Quality Assert', type: 'STEP', iconName: 'Activity', sparkTransform: 'DataQualityAssert', badge: 'Quality', description: 'Validate non-null, range, regex, set, or SQL expectations and route violations to error DLQ', category: 'Column & Row', inputArity: 1, errorOutput: true },
  { label: 'Enforce Schema & Cast', type: 'STEP', iconName: 'Key', sparkTransform: 'EnforceSchemaAndCast', badge: 'Contract', description: 'Enforce a target DDL schema contract with drift policies, safe try_cast conversions, and error DLQ routing', category: 'Column & Row', inputArity: 1, errorOutput: true },
  { label: 'Nested Struct & Array Ops', type: 'STEP', iconName: 'Layers', sparkTransform: 'NestedStructArrayOps', badge: 'Nested', description: 'Flatten nested structs recursively, pack struct columns, or apply Catalyst higher-order array functions', category: 'Column & Row', inputArity: 1, errorOutput: false },

  // Joins & Sets (OP12-OP14 + EXT4)
  { label: 'Join', type: 'STEP', iconName: 'GitMerge', sparkTransform: 'Join', badge: '2-Input', description: 'Join left and right DataFrames with inner, outer, semi, anti, cross, or broadcast strategies', category: 'Joins & Sets', inputArity: 2, errorOutput: false },
  { label: 'Merge CDC / SCD', type: 'STEP', iconName: 'GitMerge', sparkTransform: 'MergeCdcScd', badge: 'CDC / SCD', description: 'Stage CDC upserts, deletes, and SCD Type 1 or Type 2 changes from left CDC and right target inputs', category: 'Joins & Sets', inputArity: 2, errorOutput: false },
  { label: 'Union By Name', type: 'STEP', iconName: 'Layers', sparkTransform: 'UnionByName', badge: 'Merge', description: 'Combine two or more upstream DataFrames by column name or position with optional deduplication', category: 'Joins & Sets', inputArity: 'n', errorOutput: false },
  { label: 'Set Operation', type: 'STEP', iconName: 'Layers', sparkTransform: 'SetOperation', badge: 'Set', description: 'Compute intersect, intersectAll, exceptAll, or subtract across upstream DataFrames', category: 'Joins & Sets', inputArity: 'n', errorOutput: false },

  // Aggregations (OP15-OP20)
  { label: 'GroupBy & Aggregate', type: 'STEP', iconName: 'BarChart', sparkTransform: 'GroupByAgg', badge: 'Group & Window', description: 'Group rows by columns and optional event-time window with Catalyst aggregate metrics', category: 'Aggregations', inputArity: 1, errorOutput: false },
  { label: 'Top N', type: 'STEP', iconName: 'ListOrdered', sparkTransform: 'TopN', badge: 'Top-N', description: 'Select global top N rows or per-partition ranked rows using row_number, rank, or dense_rank', category: 'Aggregations', inputArity: 1, errorOutput: false },
  { label: 'Deduplicate', type: 'STEP', iconName: 'Key', sparkTransform: 'Deduplicate', badge: 'Unique', description: 'Remove duplicate rows globally, by key subset, within streaming watermark, or keeping latest row', category: 'Aggregations', inputArity: 1, errorOutput: false },
  { label: 'Pivot / Unpivot', type: 'STEP', iconName: 'Grid', sparkTransform: 'PivotUnpivot', badge: 'Reshape', description: 'Reshape rows into wide pivoted columns with explicit values or unpivot wide columns into rows', category: 'Aggregations', inputArity: 1, errorOutput: false },
  { label: 'Rollup / Cube', type: 'STEP', iconName: 'BarChart', sparkTransform: 'RollupCube', badge: 'OLAP', description: 'Compute hierarchical rollup subtotals or multi-dimensional cube aggregations with grouping IDs', category: 'Aggregations', inputArity: 1, errorOutput: false },
  { label: 'Window Analytic', type: 'STEP', iconName: 'BarChart', sparkTransform: 'WindowOver', badge: 'Analytic', description: 'Compute partitioned analytic rankings, lag/lead offsets, and frame-bounded running metrics', category: 'Aggregations', inputArity: 1, errorOutput: false },

  // Streaming (OP21-OP22)
  { label: 'Watermark', type: 'STEP', iconName: 'Clock', sparkTransform: 'Watermark', badge: 'Event Time', description: 'Define event-time watermark column and late-data delay threshold on streaming DataFrames', category: 'Streaming', inputArity: 1, errorOutput: false },
  { label: 'Stateful Transform', type: 'STEP', iconName: 'Workflow', sparkTransform: 'StatefulTransform', badge: 'State', description: 'Execute arbitrary keyed stateful streaming logic via Spark 4.0 transformWithState or Pandas state', category: 'Streaming', inputArity: 1, errorOutput: true },

  // Custom & SQL (OP23-OP25)
  { label: 'Spark SQL Query', type: 'STEP', iconName: 'Database', sparkTransform: 'SparkSqlQuery', badge: 'SQL', description: 'Register upstream DataFrames as temp views and execute parameterized Catalyst SQL queries', category: 'Custom & SQL', inputArity: 'n', errorOutput: true },
  { label: 'Pandas / Arrow UDF', type: 'STEP', iconName: 'Zap', sparkTransform: 'PandasUdfTransform', badge: 'Arrow', description: 'Apply vectorized Apache Arrow Python transformations via pandas_udf, mapInPandas, or mapInArrow', category: 'Custom & SQL', inputArity: 1, errorOutput: true },
  { label: 'Custom Transform', type: 'STEP', iconName: 'Code', sparkTransform: 'CustomTransform', badge: 'Custom', description: 'Apply modular prompt-driven DataFrame.transform logic using Spark Connect compatible APIs', category: 'Custom & SQL', inputArity: 1, errorOutput: true },

  // Partitioning (OP26-OP27)
  { label: 'Repartition / Coalesce', type: 'STEP', iconName: 'Shuffle', sparkTransform: 'RepartitionCoalesce', badge: 'Partition', description: 'Coalesce partitions without shuffle or repartition DataFrames by hash or range columns', category: 'Partitioning', inputArity: 1, errorOutput: false },
  { label: 'Cache / Persist', type: 'STEP', iconName: 'HardDrive', sparkTransform: 'CachePersist', badge: 'Storage', description: 'Materialize reused batch DataFrames in executor memory or disk with automatic unpersist', category: 'Partitioning', inputArity: 1, errorOutput: false },

  // Spark ML & AI (OP28-OP29 + EXT7)
  { label: 'Feature Prep', type: 'STEP', iconName: 'Wrench', sparkTransform: 'FeaturePrep', badge: 'MLlib', description: 'Assemble imputed, indexed, one-hot encoded, and scaled feature vectors via Spark ML Pipeline', category: 'Spark ML & AI', inputArity: 1, errorOutput: false },
  { label: 'Spark ML & Vertex AI', type: 'STEP', iconName: 'Cpu', sparkTransform: 'SparkMlAndVertexAi', badge: 'ML / AI', description: 'Run Spark MLlib PipelineModel, XGBoost, Vertex AI Endpoint, or batched Gemini inference', category: 'Spark ML & AI', inputArity: 1, errorOutput: true },
  { label: 'Text Chunk & Embed', type: 'STEP', iconName: 'Type', sparkTransform: 'TextChunkAndEmbed', badge: 'RAG / AI', description: 'Split document text into overlapping RAG chunks and generate batched Vertex AI embeddings', category: 'Spark ML & AI', inputArity: 1, errorOutput: true },
];
