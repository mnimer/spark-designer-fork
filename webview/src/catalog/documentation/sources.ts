/**
 * @fileoverview Documentation metadata registry for pipeline source nodes.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { NodeDocMetadata } from './types';

/**
 * Documentation metadata for registered source nodes across streaming, lakehouse, database, and file connectors.
 */
export const SOURCE_DOCS: Record<string, NodeDocMetadata> = {
  // =========================================================================
  // Streaming Sources
  // =========================================================================
  'SOURCE:KafkaIO': {
    title: 'Apache Kafka Source',
    description: 'Reads streaming or bounded key-value records from one or more Apache Kafka topics with configurable starting offsets.',
    docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html',
  },
  'ReadFromKafka': {
    title: 'Apache Kafka Source',
    description: 'Reads streaming or bounded key-value records from one or more Apache Kafka topics with configurable starting offsets.',
    docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html',
  },
  'SOURCE:KinesisIO': {
    title: 'Amazon Kinesis Source',
    description: 'Consumes real-time streaming data records from an Amazon Kinesis Data Stream starting from a shard position or timestamp.',
    docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html',
  },
  'ReadDataFromKinesis': {
    title: 'Amazon Kinesis Source',
    description: 'Consumes real-time streaming data records from an Amazon Kinesis Data Stream starting from a shard position or timestamp.',
    docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html',
  },
  'SOURCE:MqttIO': {
    title: 'MQTT Source',
    description: 'Subscribes to an MQTT broker topic filter and ingests IoT and telemetry messages.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
  },
  'ReadFromMqtt': {
    title: 'MQTT Source',
    description: 'Subscribes to an MQTT broker topic filter and ingests IoT and telemetry messages.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
  },
  'SOURCE:PubSubIO': {
    title: 'Cloud Pub/Sub Source',
    description: 'Ingests messages from a Google Cloud Pub/Sub subscription with optional message attributes.',
    docsUrl: 'https://cloud.google.com/pubsub/docs/overview',
  },
  'SOURCE:PubSub': {
    title: 'Cloud Pub/Sub Source',
    description: 'Ingests messages from a Google Cloud Pub/Sub subscription with optional message attributes.',
    docsUrl: 'https://cloud.google.com/pubsub/docs/overview',
  },
  'SOURCE:PulsarIO': {
    title: 'Apache Pulsar Source',
    description: 'Consumes streaming or batch messages from an Apache Pulsar topic with configurable start and end bounds.',
    docsUrl: 'https://pulsar.apache.org/docs/next/spark-connector/',
  },

  // =========================================================================
  // Database & Warehouse Sources
  // =========================================================================
  'SOURCE:BigQueryIO': {
    title: 'BigQuery Source',
    description: 'Reads rows from a Google Cloud BigQuery table or executes a SQL query using the BigQuery Storage Read API.',
    docsUrl: 'https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example',
  },
  'SOURCE:BigQuery': {
    title: 'BigQuery Source',
    description: 'Reads rows from a Google Cloud BigQuery table or executes a SQL query using the BigQuery Storage Read API.',
    docsUrl: 'https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example',
  },
  'SOURCE:BigTableIO': {
    title: 'Cloud Bigtable Source',
    description: 'Scans rows and cell columns from a Google Cloud Bigtable table using a catalog mapping.',
    docsUrl: 'https://cloud.google.com/bigtable/docs/spark-connector-overview',
  },
  'SOURCE:BigtableIO': {
    title: 'Cloud Bigtable Source',
    description: 'Scans rows and cell columns from a Google Cloud Bigtable table using a catalog mapping.',
    docsUrl: 'https://cloud.google.com/bigtable/docs/spark-connector-overview',
  },
  'SOURCE:CassandraIO': {
    title: 'Apache Cassandra Source',
    description: 'Reads rows from an Apache Cassandra keyspace and table.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
  },
  'SOURCE:ClickHouseIO': {
    title: 'ClickHouse Source',
    description: 'Queries analytical columnar tables in ClickHouse into Spark DataFrames.',
    docsUrl: 'https://clickhouse.com/docs/en/integrations/apache-spark',
  },
  'SOURCE:FirestoreIO': {
    title: 'Cloud Firestore Source',
    description: 'Reads documents or executes structured queries against a Google Cloud Firestore collection.',
    docsUrl: 'https://cloud.google.com/firestore/docs',
  },
  'SOURCE:JdbcIO': {
    title: 'JDBC Source',
    description: 'Executes SQL queries or partitioned table reads against any JDBC-compliant relational database.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html',
  },
  'SOURCE:MongoDbIO': {
    title: 'MongoDB Source',
    description: 'Reads BSON documents or change streams from a MongoDB collection with optional aggregation pipeline stages.',
    docsUrl: 'https://www.mongodb.com/docs/spark-connector/current/',
  },
  'SOURCE:Neo4jIO': {
    title: 'Neo4j Graph Source',
    description: 'Executes Cypher queries or node and relationship reads against a Neo4j graph database.',
    docsUrl: 'https://neo4j.com/docs/spark/current/',
  },
  'SOURCE:RedisIO': {
    title: 'Redis Source',
    description: 'Reads key-value records or Redis Streams matching a key pattern from a Redis cluster.',
    docsUrl: 'https://redis.io/docs/latest/',
  },
  'SOURCE:SingleStoreDBIO': {
    title: 'SingleStoreDB Source',
    description: 'Reads rows from a SingleStoreDB distributed SQL table or query with parallel partition reading.',
    docsUrl: 'https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-apache-spark/',
  },
  'SOURCE:SingleStoreIO': {
    title: 'SingleStoreDB Source',
    description: 'Reads rows from a SingleStoreDB distributed SQL table or query with parallel partition reading.',
    docsUrl: 'https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-apache-spark/',
  },
  'SOURCE:SnowflakeIO': {
    title: 'Snowflake Source',
    description: 'Reads data from a Snowflake table or SQL query into a Spark DataFrame.',
    docsUrl: 'https://docs.snowflake.com/en/user-guide/spark-connector',
  },
  'SOURCE:SpannerIO': {
    title: 'Cloud Spanner Source',
    description: 'Reads rows from a Google Cloud Spanner table or SQL query in parallel across partitions.',
    docsUrl: 'https://cloud.google.com/spanner/docs/spark-connector',
  },

  // =========================================================================
  // Data Services Sources
  // =========================================================================
  'SOURCE:ArrowFlightIO': {
    title: 'Apache Arrow Flight Source',
    description: 'Fetches columnar Arrow RecordBatches from an Apache Arrow Flight gRPC endpoint.',
    docsUrl: 'https://arrow.apache.org/docs/format/Flight.html',
  },
  'SOURCE:CloudSQL': {
    title: 'Cloud SQL Source',
    description: 'Queries a Google Cloud SQL instance over JDBC using the Cloud SQL Socket Factory.',
    docsUrl: 'https://cloud.google.com/sql/docs/postgres/connect-instance-dataproc',
  },
  'SOURCE:CosmosIO': {
    title: 'Azure Cosmos DB Source',
    description: 'Queries JSON documents from an Azure Cosmos DB database and container.',
    docsUrl: 'https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/sdk-java-spark-v4',
  },
  'SOURCE:ElasticsearchIO': {
    title: 'Elasticsearch Source',
    description: 'Reads documents from an Elasticsearch index or query across shards.',
    docsUrl: 'https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html',
  },
  'SOURCE:HCatalogIO': {
    title: 'Apache Hive / HCatalog Source',
    description: 'Reads rows from a table registered in an Apache Hive or Dataproc Metastore.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html',
  },

  // =========================================================================
  // File & Synthetic Sources
  // =========================================================================
  'SOURCE:AvroIO': {
    title: 'Apache Avro Source',
    description: 'Reads Avro-serialized records from object storage or filesystem paths.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-avro.html',
  },
  'ReadFromAvro': {
    title: 'Apache Avro Source',
    description: 'Reads Avro-serialized records from object storage or filesystem paths.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-avro.html',
  },
  'SOURCE:CsvIO': {
    title: 'CSV Source',
    description: 'Reads delimited CSV files with header parsing, custom delimiters, and schema inference.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-csv.html',
  },
  'ReadFromCsv': {
    title: 'CSV Source',
    description: 'Reads delimited CSV files with header parsing, custom delimiters, and schema inference.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-csv.html',
  },
  'SOURCE:IcebergIO': {
    title: 'Apache Iceberg Source',
    description: 'Reads snapshots or incremental appends from Apache Iceberg lakehouse tables.',
    docsUrl: 'https://iceberg.apache.org/docs/latest/spark-queries/',
  },
  'SOURCE:JsonIO': {
    title: 'JSON Source',
    description: 'Reads newline-delimited or multi-line JSON files into structured DataFrame columns.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-json.html',
  },
  'ReadFromJson': {
    title: 'JSON Source',
    description: 'Reads newline-delimited or multi-line JSON files into structured DataFrame columns.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-json.html',
  },
  'SOURCE:ParquetIO': {
    title: 'Apache Parquet Source',
    description: 'Reads columnar Apache Parquet files with column projection and predicate pushdown.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-parquet.html',
  },
  'ReadFromParquet': {
    title: 'Apache Parquet Source',
    description: 'Reads columnar Apache Parquet files with column projection and predicate pushdown.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-parquet.html',
  },
  'SOURCE:TextIO': {
    title: 'Text Source',
    description: 'Reads text files from cloud object storage or local disk into string rows.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-text.html',
  },
  'SOURCE:TFRecordIO': {
    title: 'TFRecord Source',
    description: 'Reads binary TensorFlow TFRecord files containing Example or SequenceExample records.',
    docsUrl: 'https://github.com/linkedin/spark-tfrecord',
  },
  'ReadFromTFRecord': {
    title: 'TFRecord Source',
    description: 'Reads binary TensorFlow TFRecord files containing Example or SequenceExample records.',
    docsUrl: 'https://github.com/linkedin/spark-tfrecord',
  },
  'SOURCE:XmlIO': {
    title: 'XML Source',
    description: 'Reads repeating XML row elements from files using the built-in Spark 4.0 XML data source.',
    docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-xml.html',
  },
  'SOURCE:Create': {
    title: 'Create DataFrame Source',
    description: 'Creates an in-memory DataFrame from static rows or synthetic ranges for prototyping and unit testing.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html',
  },
  'Create': {
    title: 'Create DataFrame Source',
    description: 'Creates an in-memory DataFrame from static rows or synthetic ranges for prototyping and unit testing.',
    docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html',
  },
};
