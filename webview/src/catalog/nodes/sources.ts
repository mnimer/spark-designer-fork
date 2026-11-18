/** @fileoverview Source node catalog definitions for the Apache Spark pipeline palette.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { NodeDefinition } from './types';

/**
 * Source node definitions available in the Spark 4.x palette (SRC1-SRC38).
 */
export const SOURCE_NODES: NodeDefinition[] = [
  // Streaming Sources (SRC1-SRC8)
  { label: 'Kafka', type: 'SOURCE', iconName: 'Cable', sparkTransform: 'KafkaSource', badge: 'Streaming', description: 'Consume streaming or batch records from Apache Kafka topics', category: 'Streaming Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'streaming' },
  { label: 'Pub/Sub', type: 'SOURCE', iconName: 'Radio', sparkTransform: 'PubSubSource', badge: 'GCP', description: 'Read streaming or batch messages from Google Cloud Pub/Sub subscriptions', category: 'Streaming Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'streaming' },
  { label: 'Kinesis', type: 'SOURCE', iconName: 'Cloud', sparkTransform: 'KinesisSource', badge: 'AWS', description: 'Consume streaming records from Amazon Kinesis Data Streams', category: 'Streaming Sources', supportedReadModes: ['streaming'], defaultReadMode: 'streaming' },
  { label: 'Pulsar', type: 'SOURCE', iconName: 'Radio', sparkTransform: 'PulsarSource', badge: 'Streaming', description: 'Consume streaming or batch messages from Apache Pulsar topics', category: 'Streaming Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'streaming' },
  { label: 'Event Hubs', type: 'SOURCE', iconName: 'Cloud', sparkTransform: 'EventHubsSource', badge: 'Azure', description: 'Consume streaming or batch events from Azure Event Hubs', category: 'Streaming Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'streaming' },
  { label: 'MQTT', type: 'SOURCE', iconName: 'Radio', sparkTransform: 'MqttSource', badge: 'IoT', description: 'Subscribe to streaming telemetry messages from an MQTT broker topic', category: 'Streaming Sources', supportedReadModes: ['streaming'], defaultReadMode: 'streaming' },
  { label: 'Rate', type: 'SOURCE', iconName: 'Zap', sparkTransform: 'RateSource', badge: 'Synthetic', description: 'Generate streaming benchmark rows at a configurable rate per second', category: 'Streaming Sources', supportedReadModes: ['streaming'], defaultReadMode: 'streaming' },
  { label: 'Socket', type: 'SOURCE', iconName: 'Terminal', sparkTransform: 'SocketSource', badge: 'Debug', description: 'Read UTF-8 text lines from a TCP socket for local streaming debugging', category: 'Streaming Sources', supportedReadModes: ['streaming'], defaultReadMode: 'streaming' },

  // Lakehouse Sources (SRC9-SRC12)
  { label: 'Iceberg', type: 'SOURCE', iconName: 'Database', sparkTransform: 'IcebergSource', badge: 'Lakehouse', description: 'Read Apache Iceberg tables via BigLake REST, REST, Hive, or Hadoop catalogs', category: 'Lakehouse Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'Delta Lake', type: 'SOURCE', iconName: 'Layers', sparkTransform: 'DeltaSource', badge: 'Lakehouse', description: 'Read Delta Lake tables with time travel and Change Data Feed support', category: 'Lakehouse Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'Apache Hudi', type: 'SOURCE', iconName: 'Layers', sparkTransform: 'HudiSource', badge: 'Lakehouse', description: 'Read Apache Hudi tables using snapshot, incremental, or read-optimized queries', category: 'Lakehouse Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'Hive Table', type: 'SOURCE', iconName: 'Database', sparkTransform: 'HiveSource', badge: 'Metastore', description: 'Read managed or external tables from Dataproc Metastore or Apache Hive', category: 'Lakehouse Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },

  // File Format Sources (SRC13-SRC21)
  { label: 'Parquet', type: 'SOURCE', iconName: 'FileCode', sparkTransform: 'ParquetSource', badge: 'Parquet', description: 'Read columnar Apache Parquet files from GCS, S3, ADLS, HDFS, or local storage', category: 'File Format Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'ORC', type: 'SOURCE', iconName: 'FileCode', sparkTransform: 'OrcSource', badge: 'ORC', description: 'Read columnar Apache ORC files from cloud object storage or HDFS', category: 'File Format Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'Avro', type: 'SOURCE', iconName: 'FileCode', sparkTransform: 'AvroSource', badge: 'Avro', description: 'Read serialized Apache Avro data files with optional explicit reader schema', category: 'File Format Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'CSV', type: 'SOURCE', iconName: 'FileText', sparkTransform: 'CsvSource', badge: 'CSV', description: 'Read delimited CSV files with header parsing, schema inference, and corrupt-record routing', category: 'File Format Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'JSON', type: 'SOURCE', iconName: 'FileCode', sparkTransform: 'JsonSource', badge: 'JSON', description: 'Read newline-delimited or multi-line JSON files into structured DataFrames', category: 'File Format Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'Text', type: 'SOURCE', iconName: 'FileText', sparkTransform: 'TextSource', badge: 'Text', description: 'Read plain text files line-by-line or as whole files from storage', category: 'File Format Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'XML', type: 'SOURCE', iconName: 'FileCode', sparkTransform: 'XmlSource', badge: 'XML', description: 'Read repeating XML row elements using the native Spark 4.0 XML data source', category: 'File Format Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'Binary File', type: 'SOURCE', iconName: 'Folder', sparkTransform: 'BinaryFileSource', badge: 'Binary', description: 'Read raw binary files (images, PDFs, audio) as path, metadata, and byte content rows', category: 'File Format Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'TFRecord', type: 'SOURCE', iconName: 'FileCode', sparkTransform: 'TFRecordSource', badge: 'ML', description: 'Read TensorFlow TFRecord Example or SequenceExample binary datasets', category: 'File Format Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },

  // Warehouse & Database Sources (SRC22-SRC31)
  { label: 'BigQuery', type: 'SOURCE', iconName: 'Database', sparkTransform: 'BigQuerySource', badge: 'Analytics', description: 'Read BigQuery tables or SQL queries via the high-throughput Storage Read API', category: 'Warehouse & Database Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'Cloud Spanner', type: 'SOURCE', iconName: 'Database', sparkTransform: 'SpannerSource', badge: 'GCP SQL', description: 'Read Cloud Spanner tables or SQL queries in parallel with optional Data Boost', category: 'Warehouse & Database Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'Cloud Bigtable', type: 'SOURCE', iconName: 'Database', sparkTransform: 'BigtableSource', badge: 'NoSQL', description: 'Scan rows from Google Cloud Bigtable tables with catalog mapping and key pushdown', category: 'Warehouse & Database Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'Cloud SQL', type: 'SOURCE', iconName: 'Database', sparkTransform: 'CloudSqlSource', badge: 'Cloud SQL', description: 'Query Cloud SQL (PostgreSQL, MySQL, SQL Server) via Socket Factory and parallel JDBC bounds', category: 'Warehouse & Database Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'AlloyDB', type: 'SOURCE', iconName: 'Database', sparkTransform: 'AlloyDbSource', badge: 'GCP SQL', description: 'Query Google Cloud AlloyDB for PostgreSQL via AlloyDB JDBC Socket Factory', category: 'Warehouse & Database Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'JDBC Database', type: 'SOURCE', iconName: 'Database', sparkTransform: 'JdbcSource', badge: 'SQL', description: 'Query PostgreSQL, MySQL, Oracle, SQL Server, or DB2 over partitioned JDBC', category: 'Warehouse & Database Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'Snowflake', type: 'SOURCE', iconName: 'Cloud', sparkTransform: 'SnowflakeSource', badge: 'Warehouse', description: 'Read Snowflake tables or SQL queries using the Spark Snowflake connector', category: 'Warehouse & Database Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'Amazon Redshift', type: 'SOURCE', iconName: 'Cloud', sparkTransform: 'RedshiftSource', badge: 'Warehouse', description: 'Read Amazon Redshift tables or queries via S3 UNLOAD with the Spark Redshift connector', category: 'Warehouse & Database Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'ClickHouse', type: 'SOURCE', iconName: 'BarChart', sparkTransform: 'ClickHouseSource', badge: 'OLAP', description: 'Query analytical columnar tables in ClickHouse via the Spark V2 or JDBC connector', category: 'Warehouse & Database Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'SingleStore', type: 'SOURCE', iconName: 'Database', sparkTransform: 'SingleStoreSource', badge: 'SQL', description: 'Read distributed SQL tables from SingleStoreDB with parallel partition reading', category: 'Warehouse & Database Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },

  // NoSQL & Graph Sources (SRC32-SRC37)
  { label: 'MongoDB', type: 'SOURCE', iconName: 'Database', sparkTransform: 'MongoDbSource', badge: 'NoSQL', description: 'Read BSON documents or streaming Change Streams from a MongoDB collection', category: 'NoSQL & Graph Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'Cloud Firestore', type: 'SOURCE', iconName: 'Database', sparkTransform: 'FirestoreSource', badge: 'GCP NoSQL', description: 'Read documents and filtered collections from Google Cloud Firestore', category: 'NoSQL & Graph Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'Apache Cassandra', type: 'SOURCE', iconName: 'Database', sparkTransform: 'CassandraSource', badge: 'NoSQL', description: 'Read wide-column rows from Apache Cassandra keyspaces and tables', category: 'NoSQL & Graph Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'Redis', type: 'SOURCE', iconName: 'Zap', sparkTransform: 'RedisSource', badge: 'Cache', description: 'Read key-value hashes, JSON documents, or streaming Redis Streams', category: 'NoSQL & Graph Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },
  { label: 'Elasticsearch', type: 'SOURCE', iconName: 'Layers', sparkTransform: 'ElasticsearchSource', badge: 'Search', description: 'Read indexed documents or DSL queries from Elasticsearch clusters', category: 'NoSQL & Graph Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
  { label: 'Neo4j', type: 'SOURCE', iconName: 'GitMerge', sparkTransform: 'Neo4jSource', badge: 'Graph', description: 'Read graph nodes, relationships, or Cypher queries from Neo4j', category: 'NoSQL & Graph Sources', supportedReadModes: ['batch', 'streaming'], defaultReadMode: 'batch' },

  // Synthetic & API Sources (SRC38)
  { label: 'Create DataFrame', type: 'SOURCE', iconName: 'Box', sparkTransform: 'CreateDataFrameSource', badge: 'Synthetic', description: 'Create an in-memory DataFrame from static JSON rows, numeric range, or synthetic generator', category: 'Synthetic & API Sources', supportedReadModes: ['batch'], defaultReadMode: 'batch' },
];
