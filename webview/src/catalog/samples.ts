/** @fileoverview Sample pipeline definitions and types for the palette.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { SamplePipeline } from '../utils/sharedTypes';

export type { SamplePipeline };

/**
 * Fallback Apache Spark 4.x sample pipeline definitions for standalone webview mode or offline rendering.
 */
export const DEFAULT_SAMPLES: SamplePipeline[] = [
  {
    id: 'gcs-parquet-etl',
    filename: 'gcs-parquet-etl.spark',
    title: 'Gcs Parquet Etl',
    description:
      'Pipeline flow: Raw Orders Parquet ➔ Filter Completed Orders ➔ Derive Revenue Columns ➔ Aggregate Daily Revenue ➔ Coalesce Partitions ➔ Curated Revenue Parquet',
    category: 'Samples',
    badge: 'PySpark',
    language: 'PySpark',
    runner: 'ServerlessSpark',
    nodesCount: 6,
    readmeContent: [
      '# Batch GCS Parquet ETL Pipeline',
      '',
      '## Overview',
      'This Apache Spark 4.x batch pipeline ingests raw order Parquet files from Google Cloud Storage and writes partitioned revenue summaries back to Cloud Storage.',
      '',
      'Catalyst pushes status and amount predicates into the Parquet reader, computes derived columns in one projection pass, aggregates daily metrics, and compacts output files.',
      '',
      '## Pipeline Architecture & Data Flow',
      '`Raw Orders Parquet (ParquetSource)` ➔ `Filter Completed Orders (Filter)` ➔ `Derive Revenue Columns (WithColumns)` ➔ `Aggregate Daily Revenue (GroupByAgg)` ➔ `Coalesce Partitions (RepartitionCoalesce)` ➔ `Curated Revenue Parquet (ParquetSink)`',
      '',
      '## Code Generation & Execution',
      '1. Click **New File** on the sample card to save `gcs-parquet-etl.spark` into your workspace.',
      '2. Open the **Source Code** tab and click **Generate Code** to synthesize the PySpark 4.x project.',
    ].join('\n'),
    content: '',
  },
  {
    id: 'kafka-streaming-to-iceberg',
    filename: 'kafka-streaming-to-iceberg.spark',
    title: 'Kafka Streaming To Iceberg',
    description:
      'Pipeline flow: Kafka Event Stream ➔ Parse Order JSON ➔ [Event-Time Watermark, Corrupt Payload DLQ] ➔ Tumbling Window Metrics ➔ BigLake Iceberg Sink',
    category: 'Samples',
    badge: 'PySpark',
    language: 'PySpark',
    runner: 'ServerlessSpark',
    nodesCount: 6,
    readmeContent: [
      '# Streaming Kafka to BigLake Iceberg Pipeline',
      '',
      '## Overview',
      'This sample pipeline consumes streaming JSON order events from Apache Kafka, isolates malformed payloads, computes tumbling window metrics, and writes to BigLake Iceberg.',
      '',
      'If a Kafka message contains invalid JSON, the parser routes the corrupt payload to a dead-letter console stream. Upstream event-time watermarks bound Structured Streaming state and finalize five-minute regional revenue aggregations for Serverless for Apache Spark.',
      '',
      '## Pipeline Architecture & Data Flow',
      '`Kafka Event Stream ➔ Parse Order JSON ➔ [Event-Time Watermark, Corrupt Payload DLQ] ➔ Tumbling Window Metrics ➔ BigLake Iceberg Sink`',
      '',
      '## Code Generation & Execution',
      '1. Save a copy of `kafka-streaming-to-iceberg.spark` into your workspace from the **Samples** palette.',
      '2. Open the **Source Code** panel and click **Generate Code** to synthesize the PySpark repository.',
    ].join('\n'),
    content: '',
  },
  {
    id: 'bigquery-cloudsql-broadcast-join',
    filename: 'bigquery-cloudsql-broadcast-join.spark',
    title: 'Bigquery Cloudsql Broadcast Join',
    description:
      'Pipeline flow: [BigQuery Transactions Fact, Cloud SQL Merchant Accounts] ➔ Enrich Merchant Tier ➔ Rank Merchant Transactions ➔ BigQuery Enriched Ledger',
    category: 'Samples',
    badge: 'PySpark',
    language: 'PySpark',
    runner: 'ServerlessSpark',
    nodesCount: 5,
    readmeContent: [
      '# BigQuery and Cloud SQL Broadcast Join Pipeline',
      '',
      '## Overview',
      'This Apache Spark 4.x batch pipeline enriches BigQuery payment transactions with Cloud SQL PostgreSQL merchant accounts and writes ranked records to BigQuery.',
      '',
      'Spark reads Cloud SQL in parallel using IAM Socket Factory authentication, broadcasts the lookup DataFrame, and ranks per-merchant transactions.',
      '',
      '## Pipeline Architecture & Data Flow',
      '`[BigQuery Transactions Fact (BigQuerySource), Cloud SQL Merchant Accounts (CloudSqlSource)]` ➔ `Enrich Merchant Tier (Join)` ➔ `Rank Merchant Transactions (WindowOver)` ➔ `BigQuery Enriched Ledger (BigQuerySink)`',
      '',
      '## Code Generation & Execution',
      '1. Click **New File** on the sample card to save `bigquery-cloudsql-broadcast-join.spark` into your workspace.',
      '2. Open the **Source Code** tab and click **Generate Code** to synthesize the PySpark 4.x project.',
    ].join('\n'),
    content: '',
  },
  {
    id: 'rate-stream-to-delta',
    filename: 'rate-stream-to-delta.spark',
    title: 'Rate Stream To Delta',
    description:
      'Pipeline flow: Synthetic Rate Stream ➔ Derive Event Columns ➔ Event-Time Watermark ➔ Watermark Key Deduplicate ➔ Local Delta Lake Sink',
    category: 'Samples',
    badge: 'PySpark',
    language: 'PySpark',
    runner: 'Local',
    nodesCount: 5,
    readmeContent: [
      '# Local Streaming Rate to Delta Lake Pipeline',
      '',
      '## Overview',
      'This sample pipeline generates synthetic streaming events, derives metadata columns, deduplicates keys within an event-time watermark, and writes to Delta Lake.',
      '',
      'The pipeline runs locally with zero cloud dependencies, credentials, or external message brokers using built-in PySpark and local filesystem paths.',
      '',
      '## Pipeline Architecture & Data Flow',
      '`Synthetic Rate Stream ➔ Derive Event Columns ➔ Event-Time Watermark ➔ Watermark Key Deduplicate ➔ Local Delta Lake Sink`',
      '',
      '## Code Generation & Execution',
      '1. Save a copy of `rate-stream-to-delta.spark` into your workspace from the **Samples** palette.',
      '2. Open the **Source Code** panel and click **Generate Code** to synthesize the standalone PySpark project.',
    ].join('\n'),
    content: '',
  },
];
