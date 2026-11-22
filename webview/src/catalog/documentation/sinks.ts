/**
 * @fileoverview Documentation metadata registry for Apache Spark 4.x pipeline sink nodes (SNK1-SNK37) and legacy aliases.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { NodeDocMetadata } from './types';

const KAFKA_SINK_DOC: NodeDocMetadata = {
  title: 'Apache Kafka Sink',
  description: 'Publishes batch or streaming key-value records from a DataFrame to an Apache Kafka topic.',
  docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html',
};

const PUBSUB_SINK_DOC: NodeDocMetadata = {
  title: 'Cloud Pub/Sub Sink',
  description: 'Publishes batched messages, ordering keys, and metadata attributes to a Google Cloud Pub/Sub topic.',
  docsUrl: 'https://cloud.google.com/pubsub/docs/overview',
};

const KINESIS_SINK_DOC: NodeDocMetadata = {
  title: 'Amazon Kinesis Sink',
  description: 'Writes partitioned micro-batches to an Amazon Kinesis Data Stream with retry handling.',
  docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html',
};

const PULSAR_SINK_DOC: NodeDocMetadata = {
  title: 'Apache Pulsar Sink',
  description: 'Publishes batch or streaming DataFrame rows to an Apache Pulsar topic.',
  docsUrl: 'https://pulsar.apache.org/docs/next/spark-connector/',
};

const EVENTHUBS_SINK_DOC: NodeDocMetadata = {
  title: 'Azure Event Hubs Sink',
  description: 'Publishes batch or streaming events to Azure Event Hubs over the Kafka SASL_SSL endpoint.',
  docsUrl: 'https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-developer-guide',
};

const MQTT_SINK_DOC: NodeDocMetadata = {
  title: 'MQTT Sink',
  description: 'Publishes telemetry payloads from DataFrame partitions or micro-batches to an MQTT broker topic.',
  docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
};

const ICEBERG_SINK_DOC: NodeDocMetadata = {
  title: 'Apache Iceberg Sink',
  description: 'Appends, overwrites partitions, or merges ACID commits into Apache Iceberg lakehouse tables.',
  docsUrl: 'https://iceberg.apache.org/docs/latest/spark-writes/',
};

const DELTA_SINK_DOC: NodeDocMetadata = {
  title: 'Delta Lake Sink',
  description: 'Writes, overwrites, or merges ACID transactions into Delta Lake tables with idempotent micro-batch commits.',
  docsUrl: 'https://docs.delta.io/latest/delta-batch.html',
};

const HUDI_SINK_DOC: NodeDocMetadata = {
  title: 'Apache Hudi Sink',
  description: 'Upserts, inserts, or bulk-inserts rows into Copy-on-Write or Merge-on-Read Apache Hudi tables.',
  docsUrl: 'https://hudi.apache.org/docs/writing_data',
};

const HIVE_SINK_DOC: NodeDocMetadata = {
  title: 'Apache Hive Table Sink',
  description: 'Saves partitioned or bucketed DataFrames into Dataproc Metastore or Apache Hive tables.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html',
};

const PARQUET_SINK_DOC: NodeDocMetadata = {
  title: 'Apache Parquet Sink',
  description: 'Writes DataFrame rows into compressed columnar Apache Parquet files on cloud storage or HDFS.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-parquet.html',
};

const ORC_SINK_DOC: NodeDocMetadata = {
  title: 'Apache ORC Sink',
  description: 'Writes DataFrame rows into compressed columnar Apache ORC files with optional Bloom filter columns.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-orc.html',
};

const AVRO_SINK_DOC: NodeDocMetadata = {
  title: 'Apache Avro Sink',
  description: 'Writes Avro-serialized DataFrame rows with optional explicit writer schema and compression.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-avro.html',
};

const CSV_SINK_DOC: NodeDocMetadata = {
  title: 'CSV Sink',
  description: 'Writes DataFrame rows to delimited CSV files with configurable header, quote, and escape options.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-csv.html',
};

const JSON_SINK_DOC: NodeDocMetadata = {
  title: 'JSON Sink',
  description: 'Serializes DataFrame rows as newline-delimited JSON files on cloud object storage or HDFS.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-json.html',
};

const TEXT_SINK_DOC: NodeDocMetadata = {
  title: 'Text Sink',
  description: 'Writes a single string column projection as newline-delimited plain text files.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-text.html',
};

const XML_SINK_DOC: NodeDocMetadata = {
  title: 'XML Sink',
  description: 'Writes DataFrame rows into XML files using the built-in Spark 4.0 XML data source.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-xml.html',
};

const TFRECORD_SINK_DOC: NodeDocMetadata = {
  title: 'TFRecord Sink',
  description: 'Writes DataFrame rows as TensorFlow Example or SequenceExample binary TFRecord files.',
  docsUrl: 'https://github.com/linkedin/spark-tfrecord',
};

const BIGQUERY_SINK_DOC: NodeDocMetadata = {
  title: 'BigQuery Sink',
  description: 'Writes or streams DataFrame rows into BigQuery tables using the direct Storage Write API or GCS staging.',
  docsUrl: 'https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example',
};

const SPANNER_SINK_DOC: NodeDocMetadata = {
  title: 'Cloud Spanner Sink',
  description: 'Writes batched DataFrame mutations to a Google Cloud Spanner database table.',
  docsUrl: 'https://cloud.google.com/spanner/docs/spark-connector',
};

const BIGTABLE_SINK_DOC: NodeDocMetadata = {
  title: 'Cloud Bigtable Sink',
  description: 'Writes batched DataFrame row mutations and column families to a Google Cloud Bigtable table.',
  docsUrl: 'https://cloud.google.com/bigtable/docs/spark-connector-overview',
};

const CLOUDSQL_SINK_DOC: NodeDocMetadata = {
  title: 'Cloud SQL Sink',
  description: 'Writes batched rows into Google Cloud SQL over JDBC using the Cloud SQL Socket Factory.',
  docsUrl: 'https://cloud.google.com/sql/docs/postgres/connect-instance-dataproc',
};

const ALLOYDB_SINK_DOC: NodeDocMetadata = {
  title: 'AlloyDB Sink',
  description: 'Writes batched rows into Google Cloud AlloyDB for PostgreSQL using the AlloyDB JDBC Socket Factory.',
  docsUrl: 'https://cloud.google.com/alloydb/docs/connect-language-connectors',
};

const JDBC_SINK_DOC: NodeDocMetadata = {
  title: 'JDBC Database Sink',
  description: 'Writes batched DataFrame rows into PostgreSQL, MySQL, Oracle, SQL Server, or DB2 tables over JDBC.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html',
};

const SNOWFLAKE_SINK_DOC: NodeDocMetadata = {
  title: 'Snowflake Sink',
  description: 'Loads DataFrame rows into a Snowflake table using the Spark Snowflake connector.',
  docsUrl: 'https://docs.snowflake.com/en/user-guide/spark-connector',
};

const REDSHIFT_SINK_DOC: NodeDocMetadata = {
  title: 'Amazon Redshift Sink',
  description: 'Loads DataFrame rows into Amazon Redshift tables through S3 staging with the Spark Redshift connector.',
  docsUrl: 'https://github.com/spark-redshift-community/spark-redshift',
};

const CLICKHOUSE_SINK_DOC: NodeDocMetadata = {
  title: 'ClickHouse Sink',
  description: 'Bulk-inserts large row batches into ClickHouse MergeTree analytical tables.',
  docsUrl: 'https://clickhouse.com/docs/en/integrations/apache-spark',
};

const SINGLESTORE_SINK_DOC: NodeDocMetadata = {
  title: 'SingleStoreDB Sink',
  description: 'Bulk-loads or upserts DataFrame rows into distributed SingleStoreDB tables.',
  docsUrl: 'https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-apache-spark/',
};

const MONGODB_SINK_DOC: NodeDocMetadata = {
  title: 'MongoDB Sink',
  description: 'Inserts, replaces, or upserts BSON documents into a MongoDB collection.',
  docsUrl: 'https://www.mongodb.com/docs/spark-connector/current/',
};

const FIRESTORE_SINK_DOC: NodeDocMetadata = {
  title: 'Cloud Firestore Sink',
  description: 'Commits batched document upserts, updates, or deletes to Google Cloud Firestore.',
  docsUrl: 'https://cloud.google.com/firestore/docs',
};

const CASSANDRA_SINK_DOC: NodeDocMetadata = {
  title: 'Apache Cassandra Sink',
  description: 'Writes DataFrame rows into an Apache Cassandra keyspace table with optional TTL.',
  docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
};

const REDIS_SINK_DOC: NodeDocMetadata = {
  title: 'Redis Sink',
  description: 'Executes pipelined writes to Redis hashes, string or JSON keys, or streams with optional TTL.',
  docsUrl: 'https://redis.io/docs/latest/',
};

const ELASTICSEARCH_SINK_DOC: NodeDocMetadata = {
  title: 'Elasticsearch Sink',
  description: 'Bulk-indexes or upserts JSON documents into an Elasticsearch index.',
  docsUrl: 'https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html',
};

const NEO4J_SINK_DOC: NodeDocMetadata = {
  title: 'Neo4j Graph Sink',
  description: 'Writes or merges graph nodes and relationships into Neo4j using labels or Cypher queries.',
  docsUrl: 'https://neo4j.com/docs/spark/current/',
};

const VECTOR_STORE_SINK_DOC: NodeDocMetadata = {
  title: 'Vector Store Sink (AlloyDB pgvector / Vertex AI Vector Search)',
  description: 'Upserts batched vector embeddings and metadata into AlloyDB pgvector tables or Vertex AI Vector Search indices.',
  docsUrl: 'https://cloud.google.com/alloydb/docs/ai/work-with-embeddings',
};

const CONSOLE_SINK_DOC: NodeDocMetadata = {
  title: 'Console Sink',
  description: 'Emits bounded structured log previews in batch mode or console micro-batches in streaming mode.',
  docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks',
};

const MEMORY_SINK_DOC: NodeDocMetadata = {
  title: 'Memory Sink',
  description: 'Registers a temporary SQL view in batch mode or an in-memory queryable table in streaming mode.',
  docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks',
};

const FOREACH_BATCH_SINK_DOC: NodeDocMetadata = {
  title: 'ForeachBatch Custom Sink',
  description: 'Invokes custom Python micro-batch logic or batched HTTP webhooks (REST, Datadog, Splunk HEC).',
  docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch',
};

/**
 * Documentation metadata for registered sink nodes across streaming, lakehouse, file, warehouse, NoSQL, and debug destinations.
 */
export const SINK_DOCS: Record<string, NodeDocMetadata> = {
  // Phase 6 — Canonical Spark 4.x Sinks (SNK1-SNK37, qualified by SINK: and direct)
  'SINK:KafkaSink': KAFKA_SINK_DOC,
  'KafkaSink': KAFKA_SINK_DOC,
  'SINK:PubSubSink': PUBSUB_SINK_DOC,
  'PubSubSink': PUBSUB_SINK_DOC,
  'SINK:KinesisSink': KINESIS_SINK_DOC,
  'KinesisSink': KINESIS_SINK_DOC,
  'SINK:PulsarSink': PULSAR_SINK_DOC,
  'PulsarSink': PULSAR_SINK_DOC,
  'SINK:EventHubsSink': EVENTHUBS_SINK_DOC,
  'EventHubsSink': EVENTHUBS_SINK_DOC,
  'SINK:MqttSink': MQTT_SINK_DOC,
  'MqttSink': MQTT_SINK_DOC,
  'SINK:IcebergSink': ICEBERG_SINK_DOC,
  'IcebergSink': ICEBERG_SINK_DOC,
  'SINK:DeltaSink': DELTA_SINK_DOC,
  'DeltaSink': DELTA_SINK_DOC,
  'SINK:HudiSink': HUDI_SINK_DOC,
  'HudiSink': HUDI_SINK_DOC,
  'SINK:HiveSink': HIVE_SINK_DOC,
  'HiveSink': HIVE_SINK_DOC,
  'SINK:ParquetSink': PARQUET_SINK_DOC,
  'ParquetSink': PARQUET_SINK_DOC,
  'SINK:OrcSink': ORC_SINK_DOC,
  'OrcSink': ORC_SINK_DOC,
  'SINK:AvroSink': AVRO_SINK_DOC,
  'AvroSink': AVRO_SINK_DOC,
  'SINK:CsvSink': CSV_SINK_DOC,
  'CsvSink': CSV_SINK_DOC,
  'SINK:JsonSink': JSON_SINK_DOC,
  'JsonSink': JSON_SINK_DOC,
  'SINK:TextSink': TEXT_SINK_DOC,
  'TextSink': TEXT_SINK_DOC,
  'SINK:XmlSink': XML_SINK_DOC,
  'XmlSink': XML_SINK_DOC,
  'SINK:TFRecordSink': TFRECORD_SINK_DOC,
  'TFRecordSink': TFRECORD_SINK_DOC,
  'SINK:BigQuerySink': BIGQUERY_SINK_DOC,
  'BigQuerySink': BIGQUERY_SINK_DOC,
  'SINK:SpannerSink': SPANNER_SINK_DOC,
  'SpannerSink': SPANNER_SINK_DOC,
  'SINK:BigtableSink': BIGTABLE_SINK_DOC,
  'BigtableSink': BIGTABLE_SINK_DOC,
  'SINK:CloudSqlSink': CLOUDSQL_SINK_DOC,
  'CloudSqlSink': CLOUDSQL_SINK_DOC,
  'SINK:AlloyDbSink': ALLOYDB_SINK_DOC,
  'AlloyDbSink': ALLOYDB_SINK_DOC,
  'SINK:JdbcSink': JDBC_SINK_DOC,
  'JdbcSink': JDBC_SINK_DOC,
  'SINK:SnowflakeSink': SNOWFLAKE_SINK_DOC,
  'SnowflakeSink': SNOWFLAKE_SINK_DOC,
  'SINK:RedshiftSink': REDSHIFT_SINK_DOC,
  'RedshiftSink': REDSHIFT_SINK_DOC,
  'SINK:ClickHouseSink': CLICKHOUSE_SINK_DOC,
  'ClickHouseSink': CLICKHOUSE_SINK_DOC,
  'SINK:SingleStoreSink': SINGLESTORE_SINK_DOC,
  'SingleStoreSink': SINGLESTORE_SINK_DOC,
  'SINK:MongoDbSink': MONGODB_SINK_DOC,
  'MongoDbSink': MONGODB_SINK_DOC,
  'SINK:FirestoreSink': FIRESTORE_SINK_DOC,
  'FirestoreSink': FIRESTORE_SINK_DOC,
  'SINK:CassandraSink': CASSANDRA_SINK_DOC,
  'CassandraSink': CASSANDRA_SINK_DOC,
  'SINK:RedisSink': REDIS_SINK_DOC,
  'RedisSink': REDIS_SINK_DOC,
  'SINK:ElasticsearchSink': ELASTICSEARCH_SINK_DOC,
  'ElasticsearchSink': ELASTICSEARCH_SINK_DOC,
  'SINK:Neo4jSink': NEO4J_SINK_DOC,
  'Neo4jSink': NEO4J_SINK_DOC,
  'SINK:VectorStoreSink': VECTOR_STORE_SINK_DOC,
  'VectorStoreSink': VECTOR_STORE_SINK_DOC,
  'SINK:ConsoleSink': CONSOLE_SINK_DOC,
  'ConsoleSink': CONSOLE_SINK_DOC,
  'SINK:MemorySink': MEMORY_SINK_DOC,
  'MemorySink': MEMORY_SINK_DOC,
  'SINK:ForeachBatchSink': FOREACH_BATCH_SINK_DOC,
  'ForeachBatchSink': FOREACH_BATCH_SINK_DOC,

  // Legacy Sink aliases (backwards compatibility)
  'SINK:PubSubIO': PUBSUB_SINK_DOC,
  'SINK:PubSub': PUBSUB_SINK_DOC,
  'SINK:BigQueryIO': BIGQUERY_SINK_DOC,
  'SINK:BigQuery': BIGQUERY_SINK_DOC,
  'SINK:TextIO': TEXT_SINK_DOC,
  'SINK:JdbcIO': JDBC_SINK_DOC,
  'SINK:KafkaIO': KAFKA_SINK_DOC,
  'SINK:KinesisIO': KINESIS_SINK_DOC,
  'SINK:ParquetIO': PARQUET_SINK_DOC,
  'SINK:AvroIO': AVRO_SINK_DOC,
  'SINK:TFRecordIO': TFRECORD_SINK_DOC,
  'SINK:CsvIO': CSV_SINK_DOC,
  'SINK:XmlIO': XML_SINK_DOC,
  'SINK:JsonIO': JSON_SINK_DOC,
  'SINK:IcebergIO': ICEBERG_SINK_DOC,
  'SINK:MqttIO': MQTT_SINK_DOC,
  'SINK:CassandraIO': CASSANDRA_SINK_DOC,
  'SINK:MongoDbIO': MONGODB_SINK_DOC,
  'SINK:RedisIO': REDIS_SINK_DOC,
  'SINK:BigTableIO': BIGTABLE_SINK_DOC,
  'SINK:BigtableIO': BIGTABLE_SINK_DOC,
  'SINK:ClickHouseIO': CLICKHOUSE_SINK_DOC,
  'SINK:ElasticsearchIO': ELASTICSEARCH_SINK_DOC,
  'SINK:FirestoreIO': FIRESTORE_SINK_DOC,
  'SINK:Neo4jIO': NEO4J_SINK_DOC,
  'SINK:SingleStoreDBIO': SINGLESTORE_SINK_DOC,
  'SINK:SingleStoreIO': SINGLESTORE_SINK_DOC,
  'SINK:SnowflakeIO': SNOWFLAKE_SINK_DOC,
  'SINK:SpannerIO': SPANNER_SINK_DOC,
  'SINK:HCatalogIO': HIVE_SINK_DOC,
  'SINK:PulsarIO': PULSAR_SINK_DOC,
  'SINK:CloudSQL': CLOUDSQL_SINK_DOC,
  'SINK:LogMessage': CONSOLE_SINK_DOC,
  'LogMessage': CONSOLE_SINK_DOC,
  'SINK:DatadogIO': FOREACH_BATCH_SINK_DOC,
  'SINK:SplunkIO': FOREACH_BATCH_SINK_DOC,
  'SINK:RequestResponseIO': FOREACH_BATCH_SINK_DOC,
  'WriteToAvro': AVRO_SINK_DOC,
  'WriteToBigQuery': BIGQUERY_SINK_DOC,
  'WriteToKafka': KAFKA_SINK_DOC,
  'WriteToKinesis': KINESIS_SINK_DOC,
  'WriteToParquet': PARQUET_SINK_DOC,
  'WriteToTFRecord': TFRECORD_SINK_DOC,
  'WriteToCsv': CSV_SINK_DOC,
  'WriteToJson': JSON_SINK_DOC,
  'WriteToText': TEXT_SINK_DOC,
  'WriteToMqtt': MQTT_SINK_DOC,
};
