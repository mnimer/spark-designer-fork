/**
 * @fileoverview Documentation metadata registry for pipeline source nodes.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { NodeDocMetadata } from './types';

const KAFKA_SOURCE_DOC: NodeDocMetadata = {
  title: 'Apache Kafka Source',
  description: 'Reads streaming or bounded key-value records from one or more Apache Kafka topics with configurable starting offsets.',
  docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html',
};

const PUBSUB_SOURCE_DOC: NodeDocMetadata = {
  title: 'Cloud Pub/Sub Source',
  description: 'Ingests messages from a Google Cloud Pub/Sub subscription with optional message attributes using Spark 4.0 Python DataSource.',
  docsUrl: 'https://cloud.google.com/pubsub/docs/overview',
};

const KINESIS_SOURCE_DOC: NodeDocMetadata = {
  title: 'Amazon Kinesis Source',
  description: 'Consumes real-time streaming data records from an Amazon Kinesis Data Stream starting from a shard position or timestamp.',
  docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html',
};

const PULSAR_SOURCE_DOC: NodeDocMetadata = {
  title: 'Apache Pulsar Source',
  description: 'Consumes streaming or batch messages from an Apache Pulsar topic with configurable start and end bounds.',
  docsUrl: 'https://pulsar.apache.org/docs/next/spark-connector/',
};

const EVENTHUBS_SOURCE_DOC: NodeDocMetadata = {
  title: 'Azure Event Hubs Source',
  description: 'Reads streaming or batch events from Azure Event Hubs via its Kafka-compatible endpoint or native Spark connector.',
  docsUrl: 'https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-developer-guide',
};

const MQTT_SOURCE_DOC: NodeDocMetadata = {
  title: 'MQTT Source',
  description: 'Subscribes to an MQTT broker topic filter and ingests IoT and telemetry messages via Spark 4.0 Python DataSourceStreamReader.',
  docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
};

const RATE_SOURCE_DOC: NodeDocMetadata = {
  title: 'Rate Streaming Source',
  description: 'Generates synthetic streaming rows (timestamp and value) at a fixed rate per second for benchmarking and testing.',
  docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources',
};

const SOCKET_SOURCE_DOC: NodeDocMetadata = {
  title: 'TCP Socket Source',
  description: 'Reads UTF-8 text lines from a TCP socket connection for local Structured Streaming debugging.',
  docsUrl: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources',
};

const ICEBERG_SOURCE_DOC: NodeDocMetadata = {
  title: 'Apache Iceberg Source',
  description: 'Reads table snapshots, time-travel queries, or incremental appends across BigLake REST, REST, Hive, Hadoop, Glue, Nessie, and BigQuery Iceberg catalogs.',
  docsUrl: 'https://iceberg.apache.org/docs/latest/spark-queries/',
};

const DELTA_SOURCE_DOC: NodeDocMetadata = {
  title: 'Delta Lake Source',
  description: 'Reads Delta Lake tables in batch or streaming mode with time travel and Change Data Feed support.',
  docsUrl: 'https://docs.delta.io/latest/delta-batch.html',
};

const HUDI_SOURCE_DOC: NodeDocMetadata = {
  title: 'Apache Hudi Source',
  description: 'Reads Apache Hudi lakehouse tables using snapshot, incremental, or read-optimized queries.',
  docsUrl: 'https://hudi.apache.org/docs/quick-start-guide',
};

const HIVE_SOURCE_DOC: NodeDocMetadata = {
  title: 'Apache Hive / Metastore Source',
  description: 'Reads rows from a table registered in Dataproc Metastore or an Apache Hive Metastore.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html',
};

const PARQUET_SOURCE_DOC: NodeDocMetadata = {
  title: 'Apache Parquet Source',
  description: 'Reads columnar Apache Parquet files with column projection, predicate pushdown, and schema evolution.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-parquet.html',
};

const ORC_SOURCE_DOC: NodeDocMetadata = {
  title: 'Apache ORC Source',
  description: 'Reads vectorized columnar Apache ORC files from cloud object storage, HDFS, or local paths.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-orc.html',
};

const AVRO_SOURCE_DOC: NodeDocMetadata = {
  title: 'Apache Avro Source',
  description: 'Reads Avro-serialized records from object storage or filesystem paths with optional reader schema evolution.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-avro.html',
};

const CSV_SOURCE_DOC: NodeDocMetadata = {
  title: 'CSV Source',
  description: 'Reads delimited CSV files with header parsing, custom delimiters, schema inference, and corrupt-record capture.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-csv.html',
};

const JSON_SOURCE_DOC: NodeDocMetadata = {
  title: 'JSON Source',
  description: 'Reads newline-delimited or multi-line JSON files into structured DataFrame columns.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-json.html',
};

const TEXT_SOURCE_DOC: NodeDocMetadata = {
  title: 'Text Source',
  description: 'Reads plain text files from cloud object storage or local disk into string rows.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-text.html',
};

const XML_SOURCE_DOC: NodeDocMetadata = {
  title: 'XML Source',
  description: 'Reads repeating XML row elements from files using the built-in Spark 4.0 XML data source.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-xml.html',
};

const BINARY_FILE_SOURCE_DOC: NodeDocMetadata = {
  title: 'Binary File Source',
  description: 'Reads arbitrary binary files (images, PDFs, audio, archives) into DataFrame rows with path, metadata, and raw bytes.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html',
};

const TFRECORD_SOURCE_DOC: NodeDocMetadata = {
  title: 'TFRecord Source',
  description: 'Reads binary TensorFlow TFRecord files containing Example or SequenceExample records.',
  docsUrl: 'https://github.com/linkedin/spark-tfrecord',
};

const BIGQUERY_SOURCE_DOC: NodeDocMetadata = {
  title: 'BigQuery Source',
  description: 'Reads rows from a Google Cloud BigQuery table or executes a SQL query using the BigQuery Storage Read API.',
  docsUrl: 'https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example',
};

const SPANNER_SOURCE_DOC: NodeDocMetadata = {
  title: 'Cloud Spanner Source',
  description: 'Reads rows from a Google Cloud Spanner table or SQL query in parallel across partitions with optional Data Boost.',
  docsUrl: 'https://cloud.google.com/spanner/docs/spark-connector',
};

const BIGTABLE_SOURCE_DOC: NodeDocMetadata = {
  title: 'Cloud Bigtable Source',
  description: 'Scans rows and cell columns from a Google Cloud Bigtable table using a catalog mapping.',
  docsUrl: 'https://cloud.google.com/bigtable/docs/spark-connector-overview',
};

const CLOUDSQL_SOURCE_DOC: NodeDocMetadata = {
  title: 'Cloud SQL Source',
  description: 'Queries a Google Cloud SQL instance over JDBC using the Cloud SQL Socket Factory and parallel partition bounds.',
  docsUrl: 'https://cloud.google.com/sql/docs/postgres/connect-instance-dataproc',
};

const ALLOYDB_SOURCE_DOC: NodeDocMetadata = {
  title: 'AlloyDB Source',
  description: 'Queries a Google Cloud AlloyDB for PostgreSQL cluster over JDBC using the AlloyDB Socket Factory.',
  docsUrl: 'https://cloud.google.com/alloydb/docs/connect-language-connectors',
};

const JDBC_SOURCE_DOC: NodeDocMetadata = {
  title: 'JDBC Source',
  description: 'Executes SQL queries or partitioned table reads against any JDBC-compliant relational database.',
  docsUrl: 'https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html',
};

const SNOWFLAKE_SOURCE_DOC: NodeDocMetadata = {
  title: 'Snowflake Source',
  description: 'Reads data from a Snowflake table or SQL query into a Spark DataFrame.',
  docsUrl: 'https://docs.snowflake.com/en/user-guide/spark-connector',
};

const REDSHIFT_SOURCE_DOC: NodeDocMetadata = {
  title: 'Amazon Redshift Source',
  description: 'Reads data from an Amazon Redshift table or SQL query using parallel S3 UNLOAD transfers.',
  docsUrl: 'https://github.com/spark-redshift-community/spark-redshift',
};

const CLICKHOUSE_SOURCE_DOC: NodeDocMetadata = {
  title: 'ClickHouse Source',
  description: 'Queries analytical columnar tables in ClickHouse into Spark DataFrames.',
  docsUrl: 'https://clickhouse.com/docs/en/integrations/apache-spark',
};

const SINGLESTORE_SOURCE_DOC: NodeDocMetadata = {
  title: 'SingleStoreDB Source',
  description: 'Reads rows from a SingleStoreDB distributed SQL table or query with parallel partition reading.',
  docsUrl: 'https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-apache-spark/',
};

const MONGODB_SOURCE_DOC: NodeDocMetadata = {
  title: 'MongoDB Source',
  description: 'Reads BSON documents or change streams from a MongoDB collection with optional aggregation pipeline stages.',
  docsUrl: 'https://www.mongodb.com/docs/spark-connector/current/',
};

const FIRESTORE_SOURCE_DOC: NodeDocMetadata = {
  title: 'Cloud Firestore Source',
  description: 'Reads documents or executes structured queries against a Google Cloud Firestore collection.',
  docsUrl: 'https://cloud.google.com/firestore/docs',
};

const CASSANDRA_SOURCE_DOC: NodeDocMetadata = {
  title: 'Apache Cassandra Source',
  description: 'Reads rows from an Apache Cassandra keyspace and table.',
  docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
};

const REDIS_SOURCE_DOC: NodeDocMetadata = {
  title: 'Redis Source',
  description: 'Reads key-value records or Redis Streams matching a key pattern from a Redis instance or cluster.',
  docsUrl: 'https://redis.io/docs/latest/',
};

const ELASTICSEARCH_SOURCE_DOC: NodeDocMetadata = {
  title: 'Elasticsearch Source',
  description: 'Reads documents from an Elasticsearch index or Query DSL filter across shards.',
  docsUrl: 'https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html',
};

const NEO4J_SOURCE_DOC: NodeDocMetadata = {
  title: 'Neo4j Graph Source',
  description: 'Executes Cypher queries or node and relationship reads against a Neo4j graph database.',
  docsUrl: 'https://neo4j.com/docs/spark/current/',
};

const CREATE_DATAFRAME_SOURCE_DOC: NodeDocMetadata = {
  title: 'Create DataFrame Source',
  description: 'Creates an in-memory DataFrame from static rows, numeric ranges, or synthetic generators for prototyping and testing.',
  docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html',
};

const REST_API_SOURCE_DOC: NodeDocMetadata = {
  title: 'REST API Source',
  description: 'Reads paginated JSON records from HTTP and REST API endpoints using Spark 4.0 Python DataSourceReader.',
  docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
};

const SFTP_SOURCE_DOC: NodeDocMetadata = {
  title: 'SFTP Source',
  description: 'Reads CSV, JSON, Parquet, text, or XML files from remote SFTP servers using Paramiko and Spark 4.0 Python DataSourceReader.',
  docsUrl: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html',
};

/**
 * Documentation metadata for registered source nodes across streaming, lakehouse, file, database, NoSQL, and synthetic connectors.
 */
export const SOURCE_DOCS: Record<string, NodeDocMetadata> = {
  // Phase 5 — Canonical Spark 4.x Sources (SRC1-SRC38, qualified by SOURCE: and direct)
  'SOURCE:KafkaSource': KAFKA_SOURCE_DOC,
  'KafkaSource': KAFKA_SOURCE_DOC,
  'SOURCE:PubSubSource': PUBSUB_SOURCE_DOC,
  'PubSubSource': PUBSUB_SOURCE_DOC,
  'SOURCE:KinesisSource': KINESIS_SOURCE_DOC,
  'KinesisSource': KINESIS_SOURCE_DOC,
  'SOURCE:PulsarSource': PULSAR_SOURCE_DOC,
  'PulsarSource': PULSAR_SOURCE_DOC,
  'SOURCE:EventHubsSource': EVENTHUBS_SOURCE_DOC,
  'EventHubsSource': EVENTHUBS_SOURCE_DOC,
  'SOURCE:MqttSource': MQTT_SOURCE_DOC,
  'MqttSource': MQTT_SOURCE_DOC,
  'SOURCE:RateSource': RATE_SOURCE_DOC,
  'RateSource': RATE_SOURCE_DOC,
  'SOURCE:SocketSource': SOCKET_SOURCE_DOC,
  'SocketSource': SOCKET_SOURCE_DOC,
  'SOURCE:IcebergSource': ICEBERG_SOURCE_DOC,
  'IcebergSource': ICEBERG_SOURCE_DOC,
  'SOURCE:DeltaSource': DELTA_SOURCE_DOC,
  'DeltaSource': DELTA_SOURCE_DOC,
  'SOURCE:HudiSource': HUDI_SOURCE_DOC,
  'HudiSource': HUDI_SOURCE_DOC,
  'SOURCE:HiveSource': HIVE_SOURCE_DOC,
  'HiveSource': HIVE_SOURCE_DOC,
  'SOURCE:ParquetSource': PARQUET_SOURCE_DOC,
  'ParquetSource': PARQUET_SOURCE_DOC,
  'SOURCE:OrcSource': ORC_SOURCE_DOC,
  'OrcSource': ORC_SOURCE_DOC,
  'SOURCE:AvroSource': AVRO_SOURCE_DOC,
  'AvroSource': AVRO_SOURCE_DOC,
  'SOURCE:CsvSource': CSV_SOURCE_DOC,
  'CsvSource': CSV_SOURCE_DOC,
  'SOURCE:JsonSource': JSON_SOURCE_DOC,
  'JsonSource': JSON_SOURCE_DOC,
  'SOURCE:TextSource': TEXT_SOURCE_DOC,
  'TextSource': TEXT_SOURCE_DOC,
  'SOURCE:XmlSource': XML_SOURCE_DOC,
  'XmlSource': XML_SOURCE_DOC,
  'SOURCE:BinaryFileSource': BINARY_FILE_SOURCE_DOC,
  'BinaryFileSource': BINARY_FILE_SOURCE_DOC,
  'SOURCE:TFRecordSource': TFRECORD_SOURCE_DOC,
  'TFRecordSource': TFRECORD_SOURCE_DOC,
  'SOURCE:BigQuerySource': BIGQUERY_SOURCE_DOC,
  'BigQuerySource': BIGQUERY_SOURCE_DOC,
  'SOURCE:SpannerSource': SPANNER_SOURCE_DOC,
  'SpannerSource': SPANNER_SOURCE_DOC,
  'SOURCE:BigtableSource': BIGTABLE_SOURCE_DOC,
  'BigtableSource': BIGTABLE_SOURCE_DOC,
  'SOURCE:CloudSqlSource': CLOUDSQL_SOURCE_DOC,
  'CloudSqlSource': CLOUDSQL_SOURCE_DOC,
  'SOURCE:AlloyDbSource': ALLOYDB_SOURCE_DOC,
  'AlloyDbSource': ALLOYDB_SOURCE_DOC,
  'SOURCE:JdbcSource': JDBC_SOURCE_DOC,
  'JdbcSource': JDBC_SOURCE_DOC,
  'SOURCE:SnowflakeSource': SNOWFLAKE_SOURCE_DOC,
  'SnowflakeSource': SNOWFLAKE_SOURCE_DOC,
  'SOURCE:RedshiftSource': REDSHIFT_SOURCE_DOC,
  'RedshiftSource': REDSHIFT_SOURCE_DOC,
  'SOURCE:ClickHouseSource': CLICKHOUSE_SOURCE_DOC,
  'ClickHouseSource': CLICKHOUSE_SOURCE_DOC,
  'SOURCE:SingleStoreSource': SINGLESTORE_SOURCE_DOC,
  'SingleStoreSource': SINGLESTORE_SOURCE_DOC,
  'SOURCE:MongoDbSource': MONGODB_SOURCE_DOC,
  'MongoDbSource': MONGODB_SOURCE_DOC,
  'SOURCE:FirestoreSource': FIRESTORE_SOURCE_DOC,
  'FirestoreSource': FIRESTORE_SOURCE_DOC,
  'SOURCE:CassandraSource': CASSANDRA_SOURCE_DOC,
  'CassandraSource': CASSANDRA_SOURCE_DOC,
  'SOURCE:RedisSource': REDIS_SOURCE_DOC,
  'RedisSource': REDIS_SOURCE_DOC,
  'SOURCE:ElasticsearchSource': ELASTICSEARCH_SOURCE_DOC,
  'ElasticsearchSource': ELASTICSEARCH_SOURCE_DOC,
  'SOURCE:Neo4jSource': NEO4J_SOURCE_DOC,
  'Neo4jSource': NEO4J_SOURCE_DOC,
  'SOURCE:CreateDataFrameSource': CREATE_DATAFRAME_SOURCE_DOC,
  'CreateDataFrameSource': CREATE_DATAFRAME_SOURCE_DOC,
  'SOURCE:RestApiSource': REST_API_SOURCE_DOC,
  'RestApiSource': REST_API_SOURCE_DOC,
  'SOURCE:SftpSource': SFTP_SOURCE_DOC,
  'SftpSource': SFTP_SOURCE_DOC,

  // Legacy Source aliases (backwards compatibility)
  'SOURCE:PubSubIO': PUBSUB_SOURCE_DOC,
  'SOURCE:PubSub': PUBSUB_SOURCE_DOC,
  'SOURCE:BigQueryIO': BIGQUERY_SOURCE_DOC,
  'SOURCE:BigQuery': BIGQUERY_SOURCE_DOC,
  'SOURCE:TextIO': TEXT_SOURCE_DOC,
  'SOURCE:JdbcIO': JDBC_SOURCE_DOC,
  'SOURCE:KafkaIO': KAFKA_SOURCE_DOC,
  'SOURCE:KinesisIO': KINESIS_SOURCE_DOC,
  'SOURCE:ParquetIO': PARQUET_SOURCE_DOC,
  'SOURCE:AvroIO': AVRO_SOURCE_DOC,
  'SOURCE:TFRecordIO': TFRECORD_SOURCE_DOC,
  'SOURCE:CsvIO': CSV_SOURCE_DOC,
  'SOURCE:XmlIO': XML_SOURCE_DOC,
  'SOURCE:JsonIO': JSON_SOURCE_DOC,
  'SOURCE:IcebergIO': ICEBERG_SOURCE_DOC,
  'SOURCE:MqttIO': MQTT_SOURCE_DOC,
  'SOURCE:CassandraIO': CASSANDRA_SOURCE_DOC,
  'SOURCE:MongoDbIO': MONGODB_SOURCE_DOC,
  'SOURCE:RedisIO': REDIS_SOURCE_DOC,
  'SOURCE:BigTableIO': BIGTABLE_SOURCE_DOC,
  'SOURCE:BigtableIO': BIGTABLE_SOURCE_DOC,
  'SOURCE:ClickHouseIO': CLICKHOUSE_SOURCE_DOC,
  'SOURCE:ElasticsearchIO': ELASTICSEARCH_SOURCE_DOC,
  'SOURCE:FirestoreIO': FIRESTORE_SOURCE_DOC,
  'SOURCE:Neo4jIO': NEO4J_SOURCE_DOC,
  'SOURCE:SingleStoreDBIO': SINGLESTORE_SOURCE_DOC,
  'SOURCE:SingleStoreIO': SINGLESTORE_SOURCE_DOC,
  'SOURCE:SnowflakeIO': SNOWFLAKE_SOURCE_DOC,
  'SOURCE:SpannerIO': SPANNER_SOURCE_DOC,
  'SOURCE:HCatalogIO': HIVE_SOURCE_DOC,
  'SOURCE:PulsarIO': PULSAR_SOURCE_DOC,
  'SOURCE:Create': CREATE_DATAFRAME_SOURCE_DOC,
  'SOURCE:CloudSQL': CLOUDSQL_SOURCE_DOC,
  'ReadFromAvro': AVRO_SOURCE_DOC,
  'ReadFromKafka': KAFKA_SOURCE_DOC,
  'ReadDataFromKinesis': KINESIS_SOURCE_DOC,
  'ReadFromParquet': PARQUET_SOURCE_DOC,
  'ReadFromTFRecord': TFRECORD_SOURCE_DOC,
  'ReadFromCsv': CSV_SOURCE_DOC,
  'ReadFromJson': JSON_SOURCE_DOC,
  'ReadFromMqtt': MQTT_SOURCE_DOC,
  'Create': CREATE_DATAFRAME_SOURCE_DOC,
};
