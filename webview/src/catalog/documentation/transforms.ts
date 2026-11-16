/**
 * @fileoverview Documentation metadata registry for pipeline step and transformation nodes.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { NodeDocMetadata } from './types';

/**
 * Documentation metadata for registered step and transformation nodes.
 */
export const TRANSFORM_DOCS: Record<string, NodeDocMetadata> = {
  // =========================================================================
  // Core, Parser, Filter, Windowing, and Streaming Transforms
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
  'STEP:Filter': {
    title: 'Filter Transform (df.filter)',
    description: 'Retains rows in a DataFrame that satisfy a boolean predicate expression.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html',
  },
  'Filter': {
    title: 'Filter Transform (df.filter)',
    description: 'Retains rows in a DataFrame that satisfy a boolean predicate expression.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html',
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

  // =========================================================================
  // Aggregation Transforms
  // =========================================================================
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
