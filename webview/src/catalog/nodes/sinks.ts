/** @fileoverview Sink node catalog definitions for the Apache Spark pipeline palette.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { NodeDefinition } from './types';

/**
 * Sink node definitions available in the Spark 4.x palette (SNK1-SNK37).
 */
export const SINK_NODES: NodeDefinition[] = [
  // Streaming Sinks (SNK1-SNK6)
  { label: 'Kafka', type: 'SINK', iconName: 'Cable', sparkTransform: 'KafkaSink', badge: 'Streaming', description: 'Publish batch or streaming key-value records to Apache Kafka topics', category: 'Streaming Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Pub/Sub', type: 'SINK', iconName: 'Radio', sparkTransform: 'PubSubSink', badge: 'GCP', description: 'Publish batched messages and attributes to Google Cloud Pub/Sub topics', category: 'Streaming Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'Kinesis', type: 'SINK', iconName: 'Cloud', sparkTransform: 'KinesisSink', badge: 'AWS', description: 'Write partitioned micro-batches to Amazon Kinesis Data Streams', category: 'Streaming Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'Pulsar', type: 'SINK', iconName: 'Radio', sparkTransform: 'PulsarSink', badge: 'Streaming', description: 'Publish batch or streaming messages to Apache Pulsar topics', category: 'Streaming Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Event Hubs', type: 'SINK', iconName: 'Cloud', sparkTransform: 'EventHubsSink', badge: 'Azure', description: 'Publish batch or streaming events to Azure Event Hubs over Kafka SASL_SSL', category: 'Streaming Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'MQTT', type: 'SINK', iconName: 'Radio', sparkTransform: 'MqttSink', badge: 'IoT', description: 'Publish telemetry payloads to an MQTT broker topic per partition or micro-batch', category: 'Streaming Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },

  // Lakehouse Sinks (SNK7-SNK10)
  { label: 'Iceberg', type: 'SINK', iconName: 'Database', sparkTransform: 'IcebergSink', badge: 'Lakehouse', description: 'Append, overwrite partitions, or merge into Apache Iceberg lakehouse tables', category: 'Lakehouse Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Delta Lake', type: 'SINK', iconName: 'Layers', sparkTransform: 'DeltaSink', badge: 'Lakehouse', description: 'Write, overwrite, or merge ACID transactions into Delta Lake tables', category: 'Lakehouse Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Apache Hudi', type: 'SINK', iconName: 'Layers', sparkTransform: 'HudiSink', badge: 'Lakehouse', description: 'Upsert, insert, or bulk-insert records into Copy-on-Write or Merge-on-Read Hudi tables', category: 'Lakehouse Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Hive Table', type: 'SINK', iconName: 'Database', sparkTransform: 'HiveSink', badge: 'Metastore', description: 'Save partitioned or bucketed DataFrames into Dataproc Metastore or Apache Hive tables', category: 'Lakehouse Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },

  // File Format Sinks (SNK11-SNK18)
  { label: 'Parquet', type: 'SINK', iconName: 'FileCode', sparkTransform: 'ParquetSink', badge: 'Parquet', description: 'Write compressed columnar Apache Parquet files to cloud object storage or HDFS', category: 'File Format Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'ORC', type: 'SINK', iconName: 'FileCode', sparkTransform: 'OrcSink', badge: 'ORC', description: 'Write compressed columnar Apache ORC files with optional Bloom filter columns', category: 'File Format Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Avro', type: 'SINK', iconName: 'FileCode', sparkTransform: 'AvroSink', badge: 'Avro', description: 'Write serialized Apache Avro data files with optional writer schema and compression', category: 'File Format Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'CSV', type: 'SINK', iconName: 'FileText', sparkTransform: 'CsvSink', badge: 'CSV', description: 'Write delimited CSV files with header, quote, escape, and compression options', category: 'File Format Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'JSON', type: 'SINK', iconName: 'FileCode', sparkTransform: 'JsonSink', badge: 'JSON', description: 'Write newline-delimited JSON files to cloud object storage, HDFS, or local paths', category: 'File Format Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Text', type: 'SINK', iconName: 'FileText', sparkTransform: 'TextSink', badge: 'Text', description: 'Write projected string values as newline-delimited plain text files', category: 'File Format Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'XML', type: 'SINK', iconName: 'FileCode', sparkTransform: 'XmlSink', badge: 'XML', description: 'Write structured XML documents using the built-in Spark 4.0 XML data source', category: 'File Format Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'TFRecord', type: 'SINK', iconName: 'FileCode', sparkTransform: 'TFRecordSink', badge: 'ML', description: 'Write TensorFlow Example or SequenceExample binary TFRecord datasets', category: 'File Format Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },

  // Warehouse & Database Sinks (SNK19-SNK28)
  { label: 'BigQuery', type: 'SINK', iconName: 'Database', sparkTransform: 'BigQuerySink', badge: 'Analytics', description: 'Write or stream DataFrames into BigQuery tables via direct Storage Write API or GCS staging', category: 'Warehouse & Database Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Cloud Spanner', type: 'SINK', iconName: 'Database', sparkTransform: 'SpannerSink', badge: 'GCP SQL', description: 'Write batched DataFrame mutations into Google Cloud Spanner tables', category: 'Warehouse & Database Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'Cloud Bigtable', type: 'SINK', iconName: 'Database', sparkTransform: 'BigtableSink', badge: 'NoSQL', description: 'Write batched row mutations into Google Cloud Bigtable column families', category: 'Warehouse & Database Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'Cloud SQL', type: 'SINK', iconName: 'Database', sparkTransform: 'CloudSqlSink', badge: 'Cloud SQL', description: 'Write batched rows to Cloud SQL (PostgreSQL, MySQL, SQL Server) via Socket Factory', category: 'Warehouse & Database Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'AlloyDB', type: 'SINK', iconName: 'Database', sparkTransform: 'AlloyDbSink', badge: 'GCP SQL', description: 'Write batched rows to Google Cloud AlloyDB for PostgreSQL via Socket Factory', category: 'Warehouse & Database Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'JDBC Database', type: 'SINK', iconName: 'Database', sparkTransform: 'JdbcSink', badge: 'SQL', description: 'Write batched rows to PostgreSQL, MySQL, Oracle, SQL Server, or DB2 over JDBC', category: 'Warehouse & Database Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'Snowflake', type: 'SINK', iconName: 'Cloud', sparkTransform: 'SnowflakeSink', badge: 'Warehouse', description: 'Load DataFrame rows into Snowflake tables using the Spark Snowflake connector', category: 'Warehouse & Database Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'Amazon Redshift', type: 'SINK', iconName: 'Cloud', sparkTransform: 'RedshiftSink', badge: 'Warehouse', description: 'Load DataFrame rows into Amazon Redshift tables via S3 staging and COPY', category: 'Warehouse & Database Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'ClickHouse', type: 'SINK', iconName: 'BarChart', sparkTransform: 'ClickHouseSink', badge: 'OLAP', description: 'Bulk-insert large row batches into ClickHouse MergeTree analytical tables', category: 'Warehouse & Database Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'SingleStore', type: 'SINK', iconName: 'Database', sparkTransform: 'SingleStoreSink', badge: 'SQL', description: 'Bulk-load or upsert DataFrame rows into distributed SingleStoreDB tables', category: 'Warehouse & Database Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },

  // NoSQL & Graph Sinks (SNK29-SNK34 + EXT3)
  { label: 'MongoDB', type: 'SINK', iconName: 'Database', sparkTransform: 'MongoDbSink', badge: 'NoSQL', description: 'Insert, replace, or upsert BSON documents into a MongoDB collection', category: 'NoSQL & Graph Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Cloud Firestore', type: 'SINK', iconName: 'Database', sparkTransform: 'FirestoreSink', badge: 'GCP NoSQL', description: 'Commit batched document upserts, updates, or deletes to Google Cloud Firestore', category: 'NoSQL & Graph Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'Apache Cassandra', type: 'SINK', iconName: 'Database', sparkTransform: 'CassandraSink', badge: 'NoSQL', description: 'Write wide-column rows into Apache Cassandra keyspaces with optional TTL', category: 'NoSQL & Graph Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Redis', type: 'SINK', iconName: 'Zap', sparkTransform: 'RedisSink', badge: 'Cache', description: 'Execute pipelined writes to Redis hashes, strings, JSON keys, or streams', category: 'NoSQL & Graph Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
  { label: 'Elasticsearch', type: 'SINK', iconName: 'Layers', sparkTransform: 'ElasticsearchSink', badge: 'Search', description: 'Bulk-index or upsert JSON documents into Elasticsearch indices', category: 'NoSQL & Graph Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Neo4j', type: 'SINK', iconName: 'GitMerge', sparkTransform: 'Neo4jSink', badge: 'Graph', description: 'Merge or write graph nodes, relationships, or Cypher queries into Neo4j', category: 'NoSQL & Graph Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Vector Store', type: 'SINK', iconName: 'Cpu', sparkTransform: 'VectorStoreSink', badge: 'Vector / RAG', description: 'Upsert embedding vectors into AlloyDB pgvector or Vertex AI Vector Search indices', category: 'NoSQL & Graph Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },

  // Debug & Custom Sinks (SNK35-SNK37)
  { label: 'Console', type: 'SINK', iconName: 'Terminal', sparkTransform: 'ConsoleSink', badge: 'Debug', description: 'Emit bounded structured logs in batch mode or console output in streaming mode', category: 'Debug & Custom Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'Memory', type: 'SINK', iconName: 'Box', sparkTransform: 'MemorySink', badge: 'In-Memory', description: 'Register an in-memory temporary SQL view or streaming memory table for inspection', category: 'Debug & Custom Sinks', streamingSinkMode: 'native', inputArity: 1 },
  { label: 'ForeachBatch', type: 'SINK', iconName: 'Code', sparkTransform: 'ForeachBatchSink', badge: 'Custom', description: 'Invoke custom Python sink logic or batched HTTP webhooks (REST, Datadog, Splunk HEC)', category: 'Debug & Custom Sinks', streamingSinkMode: 'foreachBatch', inputArity: 1 },
];
