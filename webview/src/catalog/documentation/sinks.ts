/**
 * @fileoverview Documentation metadata registry for pipeline sink nodes.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { NodeDocMetadata } from './types';

/**
 * Documentation metadata for registered sink nodes across streaming, lakehouse, database, and file destinations.
 */
export const SINK_DOCS: Record<string, NodeDocMetadata> = {
  // =========================================================================
  // Streaming Sinks
  // =========================================================================
  'SINK:KafkaIO': {
    title: 'Apache Kafka Sink',
    description: 'Publishes key-value records from a DataFrame or stream to an Apache Kafka topic.',
    docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html',
  },
  'WriteToKafka': {
    title: 'Apache Kafka Sink',
    description: 'Publishes key-value records from a DataFrame or stream to an Apache Kafka topic.',
    docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html',
  },
  'SINK:KinesisIO': {
    title: 'Amazon Kinesis Sink',
    description: 'Writes batched or streaming records to an Amazon Kinesis Data Stream using partition keys.',
    docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html',
  },
  'WriteToKinesis': {
    title: 'Amazon Kinesis Sink',
    description: 'Writes batched or streaming records to an Amazon Kinesis Data Stream using partition keys.',
    docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html',
  },
  'SINK:MqttIO': {
    title: 'MQTT Sink',
    description: 'Publishes payloads from a DataFrame or micro-batch stream to an MQTT broker topic.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
  },
  'WriteToMqtt': {
    title: 'MQTT Sink',
    description: 'Publishes payloads from a DataFrame or micro-batch stream to an MQTT broker topic.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
  },
  'SINK:PubSubIO': {
    title: 'Cloud Pub/Sub Sink',
    description: 'Publishes messages and optional metadata attributes to a Google Cloud Pub/Sub topic.',
    docsUrl: 'https://cloud.google.com/pubsub/docs/overview',
  },
  'SINK:PubSub': {
    title: 'Cloud Pub/Sub Sink',
    description: 'Publishes messages and optional metadata attributes to a Google Cloud Pub/Sub topic.',
    docsUrl: 'https://cloud.google.com/pubsub/docs/overview',
  },
  'SINK:PulsarIO': {
    title: 'Apache Pulsar Sink',
    description: 'Publishes batch or streaming rows to an Apache Pulsar topic.',
    docsUrl: 'https://pulsar.apache.org/docs/next/spark-connector/',
  },

  // =========================================================================
  // Database & Warehouse Sinks
  // =========================================================================
  'SINK:BigQueryIO': {
    title: 'BigQuery Sink',
    description: 'Writes or streams DataFrame rows into a Google Cloud BigQuery table using Storage Write API or GCS staging.',
    docsUrl: 'https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example',
  },
  'SINK:BigQuery': {
    title: 'BigQuery Sink',
    description: 'Writes or streams DataFrame rows into a Google Cloud BigQuery table using Storage Write API or GCS staging.',
    docsUrl: 'https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example',
  },
  'SINK:BigTableIO': {
    title: 'Cloud Bigtable Sink',
    description: 'Writes batched DataFrame rows and column families to a Google Cloud Bigtable table.',
    docsUrl: 'https://cloud.google.com/bigtable/docs/spark-connector-overview',
  },
  'SINK:BigtableIO': {
    title: 'Cloud Bigtable Sink',
    description: 'Writes batched DataFrame rows and column families to a Google Cloud Bigtable table.',
    docsUrl: 'https://cloud.google.com/bigtable/docs/spark-connector-overview',
  },
  'SINK:CassandraIO': {
    title: 'Apache Cassandra Sink',
    description: 'Writes DataFrame rows into an Apache Cassandra keyspace table.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
  },
  'SINK:ClickHouseIO': {
    title: 'ClickHouse Sink',
    description: 'Bulk-inserts DataFrame rows into a ClickHouse analytical table.',
    docsUrl: 'https://clickhouse.com/docs/en/integrations/apache-spark',
  },
  'SINK:FirestoreIO': {
    title: 'Cloud Firestore Sink',
    description: 'Applies batched document writes or upserts to Google Cloud Firestore.',
    docsUrl: 'https://cloud.google.com/firestore/docs',
  },
  'SINK:JdbcIO': {
    title: 'JDBC Sink',
    description: 'Writes DataFrame rows in batches into any JDBC-compliant relational database table.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html',
  },
  'SINK:MongoDbIO': {
    title: 'MongoDB Sink',
    description: 'Inserts or upserts BSON documents into a MongoDB collection.',
    docsUrl: 'https://www.mongodb.com/docs/spark-connector/current/',
  },
  'SINK:Neo4jIO': {
    title: 'Neo4j Graph Sink',
    description: 'Writes graph nodes and relationships to a Neo4j database using Cypher or node labels.',
    docsUrl: 'https://neo4j.com/docs/spark/current/',
  },
  'SINK:RedisIO': {
    title: 'Redis Sink',
    description: 'Writes key-value pairs, hashes, or stream entries to Redis with optional TTL.',
    docsUrl: 'https://redis.io/docs/latest/',
  },
  'SINK:SingleStoreDBIO': {
    title: 'SingleStoreDB Sink',
    description: 'Bulk-loads DataFrame rows into a SingleStoreDB table.',
    docsUrl: 'https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-apache-spark/',
  },
  'SINK:SingleStoreIO': {
    title: 'SingleStoreDB Sink',
    description: 'Bulk-loads DataFrame rows into a SingleStoreDB table.',
    docsUrl: 'https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-apache-spark/',
  },
  'SINK:SnowflakeIO': {
    title: 'Snowflake Sink',
    description: 'Loads DataFrame rows into a Snowflake table using the Spark Snowflake connector.',
    docsUrl: 'https://docs.snowflake.com/en/user-guide/spark-connector',
  },
  'SINK:SpannerIO': {
    title: 'Cloud Spanner Sink',
    description: 'Writes batched mutations to a Google Cloud Spanner database table.',
    docsUrl: 'https://cloud.google.com/spanner/docs/spark-connector',
  },

  // =========================================================================
  // Data Services Sinks
  // =========================================================================
  'SINK:ArrowFlightIO': {
    title: 'Apache Arrow Flight Sink',
    description: 'Streams columnar Arrow RecordBatches to an Apache Arrow Flight server via DoPut.',
    docsUrl: 'https://arrow.apache.org/docs/format/Flight.html',
  },
  'SINK:CloudSQL': {
    title: 'Cloud SQL Sink',
    description: 'Writes batched rows into a Google Cloud SQL table over JDBC with the Cloud SQL Socket Factory.',
    docsUrl: 'https://cloud.google.com/sql/docs/postgres/connect-instance-dataproc',
  },
  'SINK:ElasticsearchIO': {
    title: 'Elasticsearch Sink',
    description: 'Bulk-indexes or upserts JSON documents into an Elasticsearch index.',
    docsUrl: 'https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html',
  },
  'SINK:HCatalogIO': {
    title: 'Apache Hive / HCatalog Sink',
    description: 'Writes DataFrame rows into a partitioned table registered in an Apache Hive or Dataproc Metastore.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html',
  },

  // =========================================================================
  // File Sinks
  // =========================================================================
  'SINK:AvroIO': {
    title: 'Apache Avro Sink',
    description: 'Writes Avro-serialized DataFrame rows to cloud object storage or filesystem paths.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-avro.html',
  },
  'WriteToAvro': {
    title: 'Apache Avro Sink',
    description: 'Writes Avro-serialized DataFrame rows to cloud object storage or filesystem paths.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-avro.html',
  },
  'SINK:CsvIO': {
    title: 'CSV Sink',
    description: 'Writes DataFrame rows to CSV files with configurable delimiter, quote, and header options.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-csv.html',
  },
  'WriteToCsv': {
    title: 'CSV Sink',
    description: 'Writes DataFrame rows to CSV files with configurable delimiter, quote, and header options.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-csv.html',
  },
  'SINK:IcebergIO': {
    title: 'Apache Iceberg Sink',
    description: 'Appends, overwrites, or merges DataFrame rows into Apache Iceberg lakehouse tables with ACID commits.',
    docsUrl: 'https://iceberg.apache.org/docs/latest/spark-writes/',
  },
  'SINK:JsonIO': {
    title: 'JSON Sink',
    description: 'Serializes DataFrame rows as newline-delimited JSON files.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-json.html',
  },
  'WriteToJson': {
    title: 'JSON Sink',
    description: 'Serializes DataFrame rows as newline-delimited JSON files.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-json.html',
  },
  'SINK:ParquetIO': {
    title: 'Apache Parquet Sink',
    description: 'Writes DataFrame rows into columnar Apache Parquet files with configurable compression.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-parquet.html',
  },
  'WriteToParquet': {
    title: 'Apache Parquet Sink',
    description: 'Writes DataFrame rows into columnar Apache Parquet files with configurable compression.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-parquet.html',
  },
  'SINK:TextIO': {
    title: 'Text Sink',
    description: 'Writes string column rows as newline-delimited text files.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-text.html',
  },
  'SINK:TFRecordIO': {
    title: 'TFRecord Sink',
    description: 'Writes DataFrame rows as TensorFlow Example or SequenceExample TFRecord files.',
    docsUrl: 'https://github.com/linkedin/spark-tfrecord',
  },
  'WriteToTFRecord': {
    title: 'TFRecord Sink',
    description: 'Writes DataFrame rows as TensorFlow Example or SequenceExample TFRecord files.',
    docsUrl: 'https://github.com/linkedin/spark-tfrecord',
  },
  'SINK:XmlIO': {
    title: 'XML Sink',
    description: 'Writes DataFrame rows into XML files using the built-in Spark 4.0 XML data source.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-xml.html',
  },
};
