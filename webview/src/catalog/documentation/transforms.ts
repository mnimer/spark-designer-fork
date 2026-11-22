/**
 * @fileoverview Documentation metadata registry for pipeline step and transformation nodes.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { NodeDocMetadata } from './types';

/**
 * Documentation metadata for registered Spark 4.x step and legacy transformation nodes.
 */
export const TRANSFORM_DOCS: Record<string, NodeDocMetadata> = {
  // =========================================================================
  // Phase 7 — Canonical Spark 4.x Steps (OP1-OP29 + ML Aliases)
  // =========================================================================
  'STEP:Select': {
    title: 'Select Projection (df.select / df.selectExpr)',
    description: 'Projects, renames, or computes columns using Catalyst column expressions or SQL selectExpr syntax.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html',
  },
  'Select': {
    title: 'Select Projection (df.select / df.selectExpr)',
    description: 'Projects, renames, or computes columns using Catalyst column expressions or SQL selectExpr syntax.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html',
  },
  'STEP:Filter': {
    title: 'Filter Transform (df.filter)',
    description: 'Retains rows satisfying a boolean predicate and optionally routes non-matching rows to an error DLQ handle.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html',
  },
  'Filter': {
    title: 'Filter Transform (df.filter)',
    description: 'Retains rows satisfying a boolean predicate and optionally routes non-matching rows to an error DLQ handle.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html',
  },
  'STEP:WithColumns': {
    title: 'With Columns (df.withColumns)',
    description: 'Adds or overwrites multiple derived and cast columns in a single Catalyst projection pass.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumns.html',
  },
  'WithColumns': {
    title: 'With Columns (df.withColumns)',
    description: 'Adds or overwrites multiple derived and cast columns in a single Catalyst projection pass.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumns.html',
  },
  'STEP:RenameDropColumns': {
    title: 'Rename & Drop Columns (df.withColumnsRenamed / df.drop)',
    description: 'Renames DataFrame columns in bulk and drops unneeded columns without shuffling data.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnsRenamed.html',
  },
  'RenameDropColumns': {
    title: 'Rename & Drop Columns (df.withColumnsRenamed / df.drop)',
    description: 'Renames DataFrame columns in bulk and drops unneeded columns without shuffling data.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnsRenamed.html',
  },
  'STEP:ParsePayload': {
    title: 'Parse Payload (from_json / from_avro / from_csv / from_xml)',
    description: 'Parses raw JSON, Avro, CSV, or XML payloads using an explicit schema and routes malformed records via _corrupt_record.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html',
  },
  'ParsePayload': {
    title: 'Parse Payload (from_json / from_avro / from_csv / from_xml)',
    description: 'Parses raw JSON, Avro, CSV, or XML payloads using an explicit schema and routes malformed records via _corrupt_record.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html',
  },
  'STEP:Explode': {
    title: 'Explode Collection (F.explode / F.posexplode / F.inline)',
    description: 'Unnests array, map, or array-of-struct columns into individual rows with optional null preservation and positional indices.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html',
  },
  'Explode': {
    title: 'Explode Collection (F.explode / F.posexplode / F.inline)',
    description: 'Unnests array, map, or array-of-struct columns into individual rows with optional null preservation and positional indices.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html',
  },
  'STEP:HandleNulls': {
    title: 'Handle Nulls (df.na.fill / drop / replace)',
    description: 'Cleanses missing data by filling default values, dropping incomplete rows, or replacing sentinel values.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html',
  },
  'HandleNulls': {
    title: 'Handle Nulls (df.na.fill / drop / replace)',
    description: 'Cleanses missing data by filling default values, dropping incomplete rows, or replacing sentinel values.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html',
  },
  'STEP:OrderBy': {
    title: 'Order By (df.orderBy / df.sortWithinPartitions)',
    description: 'Sorts rows globally across partitions or locally within each partition with explicit sort direction and null ordering.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html',
  },
  'OrderBy': {
    title: 'Order By (df.orderBy / df.sortWithinPartitions)',
    description: 'Sorts rows globally across partitions or locally within each partition with explicit sort direction and null ordering.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html',
  },
  'STEP:LimitSample': {
    title: 'Limit & Sample (df.limit / df.sample)',
    description: 'Restricts a DataFrame to a fixed row count or draws a random row sample with optional replacement and seed.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sample.html',
  },
  'LimitSample': {
    title: 'Limit & Sample (df.limit / df.sample)',
    description: 'Restricts a DataFrame to a fixed row count or draws a random row sample with optional replacement and seed.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sample.html',
  },
  'STEP:ConditionalSplit': {
    title: 'Conditional Split Router',
    description: 'Routes DataFrame rows across named boolean rule output handles plus a null-safe else fallback branch.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html',
  },
  'ConditionalSplit': {
    title: 'Conditional Split Router',
    description: 'Routes DataFrame rows across named boolean rule output handles plus a null-safe else fallback branch.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html',
  },
  'STEP:DataQualityAssert': {
    title: 'Data Quality Assert',
    description: 'Evaluates non-null, range, regex, set, uniqueness, or SQL expectations and routes violations to error DLQ or fails the job.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.assert_true.html',
  },
  'DataQualityAssert': {
    title: 'Data Quality Assert',
    description: 'Evaluates non-null, range, regex, set, uniqueness, or SQL expectations and routes violations to error DLQ or fails the job.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.assert_true.html',
  },
  'STEP:EnforceSchemaAndCast': {
    title: 'Enforce Schema & Cast (StructType.fromDDL / F.try_cast)',
    description: 'Enforces a target Catalyst DDL contract with schema drift policies, ANSI-safe try_cast conversions, non-nullable checks, and error DLQ routing.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_cast.html',
  },
  'EnforceSchemaAndCast': {
    title: 'Enforce Schema & Cast (StructType.fromDDL / F.try_cast)',
    description: 'Enforces a target Catalyst DDL contract with schema drift policies, ANSI-safe try_cast conversions, non-nullable checks, and error DLQ routing.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_cast.html',
  },
  'STEP:NestedStructArrayOps': {
    title: 'Nested Struct & Array Ops (struct / transform / filter / aggregate)',
    description: 'Flattens nested StructType fields, packs flat columns into structs, and evaluates Catalyst higher-order array functions without shuffling.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.transform.html',
  },
  'NestedStructArrayOps': {
    title: 'Nested Struct & Array Ops (struct / transform / filter / aggregate)',
    description: 'Flattens nested StructType fields, packs flat columns into structs, and evaluates Catalyst higher-order array functions without shuffling.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.transform.html',
  },
  'STEP:Join': {
    title: 'Join DataFrames (df.join / F.broadcast)',
    description: 'Combines left and right DataFrames using inner, outer, semi, anti, or cross joins with optional right-side broadcast.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html',
  },
  'Join': {
    title: 'Join DataFrames (df.join / F.broadcast)',
    description: 'Combines left and right DataFrames using inner, outer, semi, anti, or cross joins with optional right-side broadcast.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html',
  },
  'STEP:MergeCdcScd': {
    title: 'Merge CDC / SCD (Type 1 & Type 2)',
    description: 'Deduplicates incoming left CDC change events against a right target snapshot using null-safe xxhash64 comparisons to stage SCD Type 1 and Type 2 lakehouse merges.',
    docsUrl: 'https://iceberg.apache.org/docs/latest/spark-writes/#merge-into',
  },
  'MergeCdcScd': {
    title: 'Merge CDC / SCD (Type 1 & Type 2)',
    description: 'Deduplicates incoming left CDC change events against a right target snapshot using null-safe xxhash64 comparisons to stage SCD Type 1 and Type 2 lakehouse merges.',
    docsUrl: 'https://iceberg.apache.org/docs/latest/spark-writes/#merge-into',
  },
  'STEP:UnionByName': {
    title: 'Union By Name (df.unionByName / df.union)',
    description: 'Combines two or more upstream DataFrames by column name or position with optional missing-column alignment and deduplication.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unionByName.html',
  },
  'UnionByName': {
    title: 'Union By Name (df.unionByName / df.union)',
    description: 'Combines two or more upstream DataFrames by column name or position with optional missing-column alignment and deduplication.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unionByName.html',
  },
  'STEP:SetOperation': {
    title: 'Set Operation (intersect / exceptAll / subtract)',
    description: 'Computes set intersections or set differences across two or more schema-aligned upstream DataFrames.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.intersect.html',
  },
  'SetOperation': {
    title: 'Set Operation (intersect / exceptAll / subtract)',
    description: 'Computes set intersections or set differences across two or more schema-aligned upstream DataFrames.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.intersect.html',
  },
  'STEP:GroupByAgg': {
    title: 'GroupBy & Aggregate (df.groupBy.agg)',
    description: 'Groups rows by columns and an optional event-time window and computes aggregate metrics.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'GroupByAgg': {
    title: 'GroupBy & Aggregate (df.groupBy.agg)',
    description: 'Groups rows by columns and an optional event-time window and computes aggregate metrics.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'GroupBy & Aggregate': {
    title: 'GroupBy & Aggregate (df.groupBy.agg)',
    description: 'Groups rows by columns and an optional event-time window and computes aggregate metrics.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'STEP:TopN': {
    title: 'Top N Rows (orderBy.limit / Window Rank)',
    description: 'Extracts global top N rows or per-partition top N ranked rows using row_number, rank, or dense_rank.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html',
  },
  'TopN': {
    title: 'Top N Rows (orderBy.limit / Window Rank)',
    description: 'Extracts global top N rows or per-partition top N ranked rows using row_number, rank, or dense_rank.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html',
  },
  'STEP:Deduplicate': {
    title: 'Deduplicate Rows (dropDuplicates / dropDuplicatesWithinWatermark)',
    description: 'Removes duplicate rows across all columns, key subsets, streaming watermark bounds, or deterministic latest-row windows.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html',
  },
  'Deduplicate': {
    title: 'Deduplicate Rows (dropDuplicates / dropDuplicatesWithinWatermark)',
    description: 'Removes duplicate rows across all columns, key subsets, streaming watermark bounds, or deterministic latest-row windows.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html',
  },
  'STEP:PivotUnpivot': {
    title: 'Pivot / Unpivot (GroupedData.pivot / df.unpivot)',
    description: 'Reshapes rows into wide pivoted columns using explicit category values or unpivots wide columns into key-value rows.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unpivot.html',
  },
  'PivotUnpivot': {
    title: 'Pivot / Unpivot (GroupedData.pivot / df.unpivot)',
    description: 'Reshapes rows into wide pivoted columns using explicit category values or unpivots wide columns into key-value rows.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unpivot.html',
  },
  'STEP:RollupCube': {
    title: 'Rollup / Cube Aggregation (df.rollup / df.cube)',
    description: 'Computes hierarchical rollup subtotals or multi-dimensional cube aggregations with grouping_id indicators.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.rollup.html',
  },
  'RollupCube': {
    title: 'Rollup / Cube Aggregation (df.rollup / df.cube)',
    description: 'Computes hierarchical rollup subtotals or multi-dimensional cube aggregations with grouping_id indicators.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.rollup.html',
  },
  'STEP:WindowOver': {
    title: 'Window Analytic Functions (Window.partitionBy.orderBy)',
    description: 'Computes partitioned analytic rankings, lag and lead offsets, and frame-bounded cumulative metrics without collapsing rows.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html',
  },
  'WindowOver': {
    title: 'Window Analytic Functions (Window.partitionBy.orderBy)',
    description: 'Computes partitioned analytic rankings, lag and lead offsets, and frame-bounded cumulative metrics without collapsing rows.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html',
  },
  'STEP:Watermark': {
    title: 'Watermark Step (df.withWatermark)',
    description: 'Defines an event-time watermark column and late-data delay threshold on a streaming DataFrame.',
    docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking',
  },
  'Watermark': {
    title: 'Watermark Step (df.withWatermark)',
    description: 'Defines an event-time watermark column and late-data delay threshold on a streaming DataFrame.',
    docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking',
  },
  'STEP:StatefulTransform': {
    title: 'Stateful Transform (transformWithState / applyInPandasWithState)',
    description: 'Executes keyed stateful streaming logic using Spark 4.0 StatefulProcessor state primitives and timers.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.transformWithState.html',
  },
  'StatefulTransform': {
    title: 'Stateful Transform (transformWithState / applyInPandasWithState)',
    description: 'Executes keyed stateful streaming logic using Spark 4.0 StatefulProcessor state primitives and timers.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.transformWithState.html',
  },
  'STEP:SparkSqlQuery': {
    title: 'Spark SQL Query (spark.sql)',
    description: 'Registers upstream DataFrames as temporary views and executes parameterized Catalyst SQL queries.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html',
  },
  'SparkSqlQuery': {
    title: 'Spark SQL Query (spark.sql)',
    description: 'Registers upstream DataFrames as temporary views and executes parameterized Catalyst SQL queries.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html',
  },
  'STEP:PandasUdfTransform': {
    title: 'Pandas / Arrow UDF (pandas_udf / mapInPandas / mapInArrow)',
    description: 'Executes vectorized Python transformations over Apache Arrow column batches with optional error DLQ routing.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html',
  },
  'PandasUdfTransform': {
    title: 'Pandas / Arrow UDF (pandas_udf / mapInPandas / mapInArrow)',
    description: 'Executes vectorized Python transformations over Apache Arrow column batches with optional error DLQ routing.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html',
  },
  'STEP:CustomTransform': {
    title: 'Custom Transform (df.transform)',
    description: 'Applies modular prompt-driven DataFrame.transform functions using native Catalyst and Spark Connect compatible APIs.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html',
  },
  'CustomTransform': {
    title: 'Custom Transform (df.transform)',
    description: 'Applies modular prompt-driven DataFrame.transform functions using native Catalyst and Spark Connect compatible APIs.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html',
  },
  'STEP:RepartitionCoalesce': {
    title: 'Repartition / Coalesce (df.coalesce / df.repartition)',
    description: 'Tunes DataFrame partition count via shuffle-free coalesce or hash and range repartitioning.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html',
  },
  'RepartitionCoalesce': {
    title: 'Repartition / Coalesce (df.coalesce / df.repartition)',
    description: 'Tunes DataFrame partition count via shuffle-free coalesce or hash and range repartitioning.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html',
  },
  'STEP:CachePersist': {
    title: 'Cache / Persist (df.cache / df.persist)',
    description: 'Materializes reused batch DataFrames in executor memory or disk using StorageLevel constants with automatic unpersist.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html',
  },
  'CachePersist': {
    title: 'Cache / Persist (df.cache / df.persist)',
    description: 'Materializes reused batch DataFrames in executor memory or disk using StorageLevel constants with automatic unpersist.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html',
  },
  'STEP:FeaturePrep': {
    title: 'Feature Prep (Spark ML Feature Pipeline)',
    description: 'Builds imputed, string-indexed, one-hot encoded, assembled, and scaled MLlib feature vectors.',
    docsUrl: 'https://spark.apache.org/docs/latest/ml-features.html',
  },
  'FeaturePrep': {
    title: 'Feature Prep (Spark ML Feature Pipeline)',
    description: 'Builds imputed, string-indexed, one-hot encoded, assembled, and scaled MLlib feature vectors.',
    docsUrl: 'https://spark.apache.org/docs/latest/ml-features.html',
  },
  'STEP:SparkMlAndVertexAi': {
    title: 'Spark ML & Vertex AI Inference',
    description: 'Runs distributed inference via Spark MLlib PipelineModel, Spark XGBoost, Vertex AI Endpoints, or batched Gemini calls.',
    docsUrl: 'https://spark.apache.org/docs/latest/ml-pipeline.html',
  },
  'SparkMlAndVertexAi': {
    title: 'Spark ML & Vertex AI Inference',
    description: 'Runs distributed inference via Spark MLlib PipelineModel, Spark XGBoost, Vertex AI Endpoints, or batched Gemini calls.',
    docsUrl: 'https://spark.apache.org/docs/latest/ml-pipeline.html',
  },
  'STEP:SparkMlInference': {
    title: 'Spark ML Inference (PipelineModel / XGBoost)',
    description: 'Loads a saved Spark MLlib PipelineModel or Spark XGBoost model and transforms DataFrame feature vectors.',
    docsUrl: 'https://spark.apache.org/docs/latest/ml-pipeline.html',
  },
  'SparkMlInference': {
    title: 'Spark ML Inference (PipelineModel / XGBoost)',
    description: 'Loads a saved Spark MLlib PipelineModel or Spark XGBoost model and transforms DataFrame feature vectors.',
    docsUrl: 'https://spark.apache.org/docs/latest/ml-pipeline.html',
  },
  'STEP:VertexAiInference': {
    title: 'Vertex AI Inference (Endpoint / Gemini)',
    description: 'Executes partition-batched predictions against Vertex AI Endpoints or Vertex AI Gemini models via mapInPandas.',
    docsUrl: 'https://docs.cloud.google.com/vertex-ai/docs/predictions/get-predictions',
  },
  'VertexAiInference': {
    title: 'Vertex AI Inference (Endpoint / Gemini)',
    description: 'Executes partition-batched predictions against Vertex AI Endpoints or Vertex AI Gemini models via mapInPandas.',
    docsUrl: 'https://docs.cloud.google.com/vertex-ai/docs/predictions/get-predictions',
  },
  'STEP:TextChunkAndEmbed': {
    title: 'Text Chunk & Embed (RAG Document Preparation)',
    description: 'Splits document text into overlapping chunks and computes batched Vertex AI embeddings via google-genai with optional error DLQ routing.',
    docsUrl: 'https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings',
  },
  'TextChunkAndEmbed': {
    title: 'Text Chunk & Embed (RAG Document Preparation)',
    description: 'Splits document text into overlapping chunks and computes batched Vertex AI embeddings via google-genai with optional error DLQ routing.',
    docsUrl: 'https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings',
  },

  // =========================================================================
  // Legacy Step & Aggregation Aliases (Backwards Compatibility)
  // =========================================================================
  'STEP:ParDo': {
    title: 'ParDo Transform (Custom Transform)',
    description: 'Applies a custom transformation function to rows in the pipeline.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html',
  },
  'ParDo': {
    title: 'ParDo Transform (Custom Transform)',
    description: 'Applies a custom transformation function to rows in the pipeline.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html',
  },
  'STEP:ParseMessage': {
    title: 'Parse Message Transform',
    description: 'Decodes raw incoming byte or string payloads (JSON, Base64, or UTF-8 String) into structured columns.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html',
  },
  'ParseMessage': {
    title: 'Parse Message Transform',
    description: 'Decodes raw incoming byte or string payloads (JSON, Base64, or UTF-8 String) into structured columns.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html',
  },
  'STEP:ApproximateQuantiles': {
    title: 'ApproximateQuantiles',
    description: 'Computes approximate quantiles of numeric columns in a DataFrame.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.approxQuantile.html',
  },
  'ApproximateQuantiles': {
    title: 'ApproximateQuantiles',
    description: 'Computes approximate quantiles of numeric columns in a DataFrame.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.approxQuantile.html',
  },
  'Approximate Quantiles': {
    title: 'Approximate Quantiles',
    description: 'Computes approximate quantiles of numeric columns in a DataFrame.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.approxQuantile.html',
  },
  'Approximate Quantile': {
    title: 'Approximate Quantile',
    description: 'Computes approximate quantiles of numeric columns in a DataFrame.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.approxQuantile.html',
  },
  'STEP:ApproximateUnique': {
    title: 'ApproximateUnique',
    description: 'Estimates the distinct count of column values using HyperLogLog.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.approx_count_distinct.html',
  },
  'ApproximateUnique': {
    title: 'ApproximateUnique',
    description: 'Estimates the distinct count of column values using HyperLogLog.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.approx_count_distinct.html',
  },
  'Approximate Unique': {
    title: 'Approximate Unique',
    description: 'Estimates the distinct count of column values using HyperLogLog.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.approx_count_distinct.html',
  },
  'STEP:BatchElements': {
    title: 'BatchElements',
    description: 'Batches elements into smaller groups for downstream processing.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.mapInPandas.html',
  },
  'BatchElements': {
    title: 'BatchElements',
    description: 'Batches elements into smaller groups for downstream processing.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.mapInPandas.html',
  },
  'Batch Elements': {
    title: 'Batch Elements',
    description: 'Batches elements into smaller groups for downstream processing.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.mapInPandas.html',
  },
  'STEP:CoGroupByKey': {
    title: 'CoGroupByKey',
    description: 'Groups results from multiple collections by their common keys.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'CoGroupByKey': {
    title: 'CoGroupByKey',
    description: 'Groups results from multiple collections by their common keys.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'Co Group By Key': {
    title: 'Co Group By Key',
    description: 'Groups results from multiple collections by their common keys.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'STEP:Combine': {
    title: 'Combine',
    description: 'Combines elements in a collection using a global combine function.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'Combine': {
    title: 'Combine',
    description: 'Combines elements in a collection using a global combine function.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'STEP:CombineWithContext': {
    title: 'CombineWithContext',
    description: 'Combines elements in a collection using contextual state.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'CombineWithContext': {
    title: 'CombineWithContext',
    description: 'Combines elements in a collection using contextual state.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'Combine With Context': {
    title: 'Combine With Context',
    description: 'Combines elements in a collection using contextual state.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'STEP:Count': {
    title: 'Count',
    description: 'Counts the number of elements in a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count.html',
  },
  'Count': {
    title: 'Count',
    description: 'Counts the number of elements in a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count.html',
  },
  'STEP:Distinct': {
    title: 'Distinct',
    description: 'Removes duplicate elements from a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.distinct.html',
  },
  'Distinct': {
    title: 'Distinct',
    description: 'Removes duplicate elements from a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.distinct.html',
  },
  'STEP:GroupByKey': {
    title: 'GroupByKey',
    description: 'Groups key-value pairs by their keys.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'GroupByKey': {
    title: 'GroupByKey',
    description: 'Groups key-value pairs by their keys.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'Group By Key': {
    title: 'Group By Key',
    description: 'Groups key-value pairs by their keys.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'STEP:GroupIntoBatches': {
    title: 'GroupIntoBatches',
    description: 'Groups elements into batches of a specified size or duration.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'GroupIntoBatches': {
    title: 'GroupIntoBatches',
    description: 'Groups elements into batches of a specified size or duration.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'Group Into Batches': {
    title: 'Group Into Batches',
    description: 'Groups elements into batches of a specified size or duration.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html',
  },
  'STEP:Latest': {
    title: 'Latest',
    description: 'Obtains the latest element or value according to timestamps.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.max_by.html',
  },
  'Latest': {
    title: 'Latest',
    description: 'Obtains the latest element or value according to timestamps.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.max_by.html',
  },
  'STEP:Max': {
    title: 'Max',
    description: 'Computes the maximum element or value in a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.max.html',
  },
  'Max': {
    title: 'Max',
    description: 'Computes the maximum element or value in a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.max.html',
  },
  'STEP:Mean': {
    title: 'Mean',
    description: 'Computes the arithmetic mean of elements in a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.avg.html',
  },
  'Mean': {
    title: 'Mean',
    description: 'Computes the arithmetic mean of elements in a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.avg.html',
  },
  'STEP:Min': {
    title: 'Min',
    description: 'Computes the minimum element or value in a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.min.html',
  },
  'Min': {
    title: 'Min',
    description: 'Computes the minimum element or value in a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.min.html',
  },
  'STEP:Sample': {
    title: 'Sample',
    description: 'Takes a random sample of elements from a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sample.html',
  },
  'Sample': {
    title: 'Sample',
    description: 'Takes a random sample of elements from a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sample.html',
  },
  'STEP:Sum': {
    title: 'Sum',
    description: 'Computes the sum of elements in a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum.html',
  },
  'Sum': {
    title: 'Sum',
    description: 'Computes the sum of elements in a collection.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum.html',
  },
  'STEP:Top': {
    title: 'Top',
    description: 'Finds the top elements in a collection according to a comparator.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html',
  },
  'Top': {
    title: 'Top',
    description: 'Finds the top elements in a collection according to a comparator.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html',
  },
};
