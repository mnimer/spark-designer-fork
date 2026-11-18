/**
 * Legacy Node Migration Utility
 * 
 * Author: Mike Nimer <mikenimer@google.com>
 * 
 * Purpose:
 * Provides deterministic, one-time migration for legacy canvas nodes saved in older .beam
 * files that lack an explicit `spark_transform` metadata property.
 * 
 * Architectural Decisions:
 * - Uses (type, iconName, label) triple to deterministically infer `spark_transform`.
 * - Prioritizes immutable (type, iconName) pairs as the primary key and only uses label
 *   to disambiguate nodes sharing identical icons (e.g. Radio for PubSub vs MQTT).
 * - Strictly idempotent: Never overwrites an existing `spark_transform`.
 * - Returns null for unknown nodes to preserve fail-fast unregistered state.
 * - Logs observable summary when migration occurs at pipeline load time.
 */

/**
 * Shape of legacy node data read from older pipeline YAML representations.
 */
export interface LegacyNodeData {
  label?: string;
  type?: string;
  iconName?: string;
  spark_transform?: string;
  componentType?: string;
  [key: string]: any;
}

/**
 * Canonical mapping from legacy Beam source transform names to Spark 4.x `sparkTransform` identifiers (SRC1-SRC38).
 */
export const LEGACY_SOURCE_TRANSFORM_MAP: Record<string, string> = {
  KafkaIO: 'KafkaSource',
  ReadFromKafka: 'KafkaSource',
  PubSubIO: 'PubSubSource',
  PubSub: 'PubSubSource',
  KinesisIO: 'KinesisSource',
  ReadDataFromKinesis: 'KinesisSource',
  PulsarIO: 'PulsarSource',
  MqttIO: 'MqttSource',
  ReadFromMqtt: 'MqttSource',
  GenerateSequence: 'RateSource',
  PeriodicImpulse: 'RateSource',
  IcebergIO: 'IcebergSource',
  HCatalogIO: 'HiveSource',
  ParquetIO: 'ParquetSource',
  ReadFromParquet: 'ParquetSource',
  AvroIO: 'AvroSource',
  ReadFromAvro: 'AvroSource',
  CsvIO: 'CsvSource',
  ReadFromCsv: 'CsvSource',
  JsonIO: 'JsonSource',
  ReadFromJson: 'JsonSource',
  TextIO: 'TextSource',
  XmlIO: 'XmlSource',
  FileIO: 'BinaryFileSource',
  'FileIO.match': 'BinaryFileSource',
  TFRecordIO: 'TFRecordSource',
  ReadFromTFRecord: 'TFRecordSource',
  BigQueryIO: 'BigQuerySource',
  BigQuery: 'BigQuerySource',
  SpannerIO: 'SpannerSource',
  BigTableIO: 'BigtableSource',
  BigtableIO: 'BigtableSource',
  CloudBigtable: 'BigtableSource',
  CloudSQL: 'CloudSqlSource',
  JdbcIO: 'JdbcSource',
  DatabaseIO: 'JdbcSource',
  MySQL: 'JdbcSource',
  MySQLPostgreSQLOracleSQLServer: 'JdbcSource',
  SnowflakeIO: 'SnowflakeSource',
  ClickHouseIO: 'ClickHouseSource',
  SingleStoreDBIO: 'SingleStoreSource',
  SingleStoreIO: 'SingleStoreSource',
  MongoDbIO: 'MongoDbSource',
  FirestoreIO: 'FirestoreSource',
  DatastoreIO: 'FirestoreSource',
  CassandraIO: 'CassandraSource',
  RedisIO: 'RedisSource',
  ElasticsearchIO: 'ElasticsearchSource',
  Neo4jIO: 'Neo4jSource',
  Create: 'CreateDataFrameSource',
};

/**
 * Resolves a legacy or canonical source transform identifier to its canonical Spark 4.x `sparkTransform`.
 *
 * @param transform Raw transform name from a node or pipeline YAML.
 * @returns Canonical Spark 4.x source transform name, or the original transform when already canonical.
 */
export function resolveCanonicalSourceTransform(transform: string): string {
  const bare = transform.startsWith('SOURCE:') ? transform.slice('SOURCE:'.length) : transform;
  return LEGACY_SOURCE_TRANSFORM_MAP[bare] ?? bare;
}

const KNOWN_AGGREGATIONS = [
  'Approximate Quantiles', 'Approximate Quantile', 'ApproximateQuantiles',
  'Approximate Unique', 'ApproximateUnique',
  'Co Group By Key', 'CoGroupByKey',
  'Combine', 'Combine With Context', 'CombineWithContext',
  'Count', 'Distinct',
  'Group By Key', 'GroupByKey',
  'Group Into Batches', 'GroupIntoBatches',
  'Batch Elements', 'BatchElements',
  'Latest', 'Max', 'Mean', 'Min', 'Sample', 'Sum', 'Top'
];

/**
 * Infers the canonical `spark_transform` for a legacy node based on its (type, iconName, label) triple.
 * 
 * NOTE: Idempotent - if `spark_transform` is already present, returns it directly.
 * Returns null if the node cannot be deterministically identified.
 * 
 * @param data - The legacy node data dictionary
 * @returns Canonical spark_transform string or null if unknown
 */
export function inferBeamTransform(data: LegacyNodeData): string | null {
  if (!data) {
    return null;
  }

  // Idempotent: Never overwrite an existing spark_transform or componentType
  if (data.spark_transform) {
    return data.spark_transform;
  }
  if (data.componentType) {
    return data.componentType;
  }

  const type = (data.type || '').toUpperCase();
  const icon = data.iconName || '';
  const label = (data.label || '').trim();
  const labelLower = label.toLowerCase();

  // =========================================================================
  // Primary Strategy: (type, iconName) with label disambiguation
  // =========================================================================

  if (type === 'SOURCE') {
    switch (icon) {
      case 'ListOrdered':
        return 'GenerateSequence';
      case 'Zap':
        if (labelLower.includes('redis')) return 'RedisIO';
        if (labelLower.includes('rate')) return 'RateSource';
        return 'PeriodicImpulse';
      case 'Terminal':
        if (labelLower.includes('socket')) return 'SocketSource';
        break;
      case 'Box':
        if (labelLower.includes('dataframe')) return 'CreateDataFrameSource';
        return 'Create';
      case 'FileText':
        if (labelLower.includes('tika')) return 'TikaIO';
        if (labelLower.includes('contextual')) return 'ContextualTextIO';
        if (labelLower.includes('csv')) return 'CsvIO';
        return 'TextIO';
      case 'HardDrive':
        return 'HadoopFileSystem';
      case 'Cloud':
        if (labelLower.includes('snowflake')) return 'SnowflakeIO';
        if (labelLower.includes('redshift')) return 'RedshiftSource';
        if (labelLower.includes('event hub') || labelLower.includes('eventhub')) return 'EventHubsSource';
        if (labelLower.includes('gcs') || labelLower.includes('google')) return 'GcsFileSystem';
        if (labelLower.includes('s3') || labelLower.includes('amazon') || labelLower.includes('aws')) return 'S3FileSystem';
        if (labelLower.includes('azure') || labelLower.includes('blob')) return 'AzureBlobStoreFileSystem';
        return 'KinesisIO';
      case 'Share2':
        return 'SnsIO';
      case 'Folder':
        if (labelLower.includes('binary')) return 'BinaryFileSource';
        if (labelLower.includes('local')) return 'LocalFileSystem';
        return 'FileIO.match';
      case 'Radio':
        if (labelLower.includes('pulsar')) return 'PulsarIO';
        return labelLower.includes('mqtt') ? 'MqttIO' : 'PubSubIO';
      case 'Database':
        if (labelLower.includes('iceberg')) return 'IcebergIO';
        if (labelLower.includes('hive') || labelLower.includes('hcatalog')) return 'HCatalogIO';
        if (labelLower.includes('spanner')) return 'SpannerIO';
        if (labelLower.includes('bigtable')) return 'BigTableIO';
        if (labelLower.includes('cloud sql') || labelLower.includes('cloudsql')) return 'CloudSQL';
        if (labelLower.includes('alloydb')) return 'AlloyDbSource';
        if (labelLower.includes('singlestore')) return 'SingleStoreDBIO';
        if (labelLower.includes('mongo')) return 'MongoDbIO';
        if (labelLower.includes('firestore')) return 'FirestoreIO';
        if (labelLower.includes('cassandra')) return 'CassandraIO';
        return labelLower.includes('jdbc') ? 'JdbcIO' : 'BigQueryIO';
      case 'BarChart':
        if (labelLower.includes('clickhouse')) return 'ClickHouseIO';
        break;
      case 'Layers':
        if (labelLower.includes('delta')) return 'DeltaSource';
        if (labelLower.includes('hudi')) return 'HudiSource';
        if (labelLower.includes('elasticsearch') || labelLower.includes('elastic')) return 'ElasticsearchIO';
        if (labelLower.includes('hcatalog') || labelLower.includes('hive')) return 'HCatalogIO';
        break;
      case 'GitMerge':
        if (labelLower.includes('neo4j')) return 'Neo4jIO';
        break;
      case 'Cable':
        if (labelLower.includes('jms')) return 'JmsIO';
        if (labelLower.includes('sqs')) return 'SqsIO';
        return 'KafkaIO';
      case 'FileCode':
        if (labelLower.includes('xml')) return 'XmlIO';
        if (labelLower.includes('orc')) return 'OrcSource';
        if (labelLower.includes('thrift')) return 'ThriftIO';
        if (labelLower.includes('tfrecord')) return 'TFRecordIO';
        if (labelLower.includes('json')) return 'JsonIO';
        return labelLower.includes('parquet') ? 'ParquetIO' : 'AvroIO';
      default:
        break;
    }
  } else if (type === 'SINK') {
    switch (icon) {
      case 'Terminal':
        return 'LogMessage';
      case 'FileText':
        if (labelLower.includes('tika')) return 'TikaIO';
        if (labelLower.includes('contextual')) return 'ContextualTextIO';
        if (labelLower.includes('csv')) return 'CsvIO';
        return 'TextIO';
      case 'HardDrive':
        return 'HadoopFileSystem';
      case 'Cloud':
        if (labelLower.includes('gcs') || labelLower.includes('google')) return 'GcsFileSystem';
        if (labelLower.includes('s3') || labelLower.includes('amazon') || labelLower.includes('aws')) return 'S3FileSystem';
        if (labelLower.includes('azure') || labelLower.includes('blob')) return 'AzureBlobStoreFileSystem';
        return 'KinesisIO';
      case 'Share2':
        return 'SnsIO';
      case 'Folder':
        if (labelLower.includes('local')) return 'LocalFileSystem';
        return 'FileIO';
      case 'Radio':
        return labelLower.includes('mqtt') ? 'MqttIO' : 'PubSubIO';
      case 'Database':
        if (labelLower.includes('iceberg')) return 'IcebergIO';
        return labelLower.includes('jdbc') ? 'JdbcIO' : 'BigQueryIO';
      case 'Cable':
        if (labelLower.includes('jms')) return 'JmsIO';
        if (labelLower.includes('sqs')) return 'SqsIO';
        return 'KafkaIO';
      case 'FileCode':
        if (labelLower.includes('xml')) return 'XmlIO';
        if (labelLower.includes('thrift')) return 'ThriftIO';
        if (labelLower.includes('tfrecord')) return 'TFRecordIO';
        if (labelLower.includes('json')) return 'JsonIO';
        return labelLower.includes('parquet') ? 'ParquetIO' : 'AvroIO';
      default:
        break;
    }
  } else if (type === 'STEP') {
    switch (icon) {
      case 'Code':
        return 'ParDo';
      case 'Terminal':
        return 'LogMessage';
      case 'Calendar':
        return 'WithTimestamps';
      case 'Filter':
        return 'Filter';
      case 'GitMerge':
        return 'FlatMap';
      case 'Hash':
        return 'KVMapper';
      case 'Activity':
        return 'Map';
      case 'Clock':
        return 'Window';
      case 'Layers':
        return 'Flatten';
      case 'Shuffle':
        return 'Reshuffle';
      case 'FileCode':
        return (labelLower.includes('fileio') || labelLower.includes('read matches'))
          ? 'FileIO.readMatches'
          : 'ParseMessage';
      case 'BarChart': {
        const found = KNOWN_AGGREGATIONS.find(agg => agg.toLowerCase() === labelLower);
        return found || 'Combine';
      }
      default:
        break;
    }
  }

  // =========================================================================
  // Secondary Strategy: Disambiguate by known label substring from legacy inventory
  // =========================================================================

  if (labelLower.includes('tika')) return 'TikaIO';
  if (labelLower.includes('contextual')) return 'ContextualTextIO';
  if (labelLower.includes('thrift')) return 'ThriftIO';
  if (labelLower.includes('xml')) return 'XmlIO';
  if (labelLower.includes('iceberg')) return 'IcebergIO';
  if (labelLower.includes('tfrecord')) return 'TFRecordIO';
  if (labelLower.includes('csv')) return 'CsvIO';
  if (labelLower.includes('json') && !labelLower.includes('simplejson')) return 'JsonIO';
  if (labelLower.includes('hadoop') || labelLower.includes('hdfs')) return 'HadoopFileSystem';
  if (labelLower.includes('azure') || labelLower.includes('blob')) return 'AzureBlobStoreFileSystem';
  if (labelLower.includes('s3')) return 'S3FileSystem';
  if (labelLower.includes('gcs')) return 'GcsFileSystem';
  if (labelLower.includes('localfile') || labelLower.includes('local filesystem')) return 'LocalFileSystem';
  if (label.includes('PubSub')) return 'PubSubIO';
  if (label.includes('BigQuery')) return 'BigQueryIO';
  if (label.includes('Kafka') || labelLower.includes('kafka')) return 'KafkaIO';
  if (label.includes('TextIO')) return 'TextIO';
  if (label.includes('FileIO')) {
    if (label.includes('Read Matches')) return 'FileIO.readMatches';
    return type === 'SOURCE' ? 'FileIO.match' : 'FileIO';
  }
  if (label.includes('Jdbc') || label.includes('JDBC')) return 'JdbcIO';
  if (label.includes('Generate Sequence')) return 'GenerateSequence';
  if (label.includes('Periodic Impulse')) return 'PeriodicImpulse';
  if (label === 'Create') return 'Create';
  if (labelLower.includes('avro')) return 'AvroIO';
  if (labelLower.includes('parquet')) return 'ParquetIO';
  if (labelLower.includes('sns')) return 'SnsIO';
  if (labelLower.includes('jms')) return 'JmsIO';
  if (labelLower.includes('sqs')) return 'SqsIO';
  if (labelLower.includes('mqtt')) return 'MqttIO';
  if (label === 'Log Message') return 'LogMessage';
  if (labelLower.includes('window')) return 'Window';
  if (label === 'Parse Message') return 'ParseMessage';
  if (labelLower.includes('timestamp')) return 'WithTimestamps';
  if (label === 'KV Mapper') return 'KVMapper';
  if (label === 'Filter') return 'Filter';
  if (label === 'FlatMap') return 'FlatMap';
  if (label === 'Map') return 'Map';
  if (label === 'ParDo') return 'ParDo';
  if (label === 'Flatten') return 'Flatten';
  if (label.toLowerCase() === 'reshuffle') return 'Reshuffle';

  const aggMatch = KNOWN_AGGREGATIONS.find(agg => agg.toLowerCase() === labelLower);
  if (aggMatch) {
    return aggMatch;
  }

  // No confident match: Fail fast rather than guess
  return null;
}

/**
 * Migrates an array of canvas nodes loaded from a .beam YAML file, enriching
 * legacy nodes that lack `spark_transform` metadata.
 * 
 * NOTE: Logs a single-line summary when legacy nodes are migrated.
 * 
 * @param nodes - Array of React Flow node objects
 * @returns Migrated array of React Flow node objects
 */
export function migrateLegacyNodes(nodes: any[]): any[] {
  if (!Array.isArray(nodes) || nodes.length === 0) {
    return nodes || [];
  }

  let migratedCount = 0;

  const migrated = nodes.map((node) => {
    if (!node || !node.data) {
      return node;
    }

    // Skip nodes that already have spark_transform metadata
    if (node.data.spark_transform) {
      return node;
    }

    const inferred = inferBeamTransform(node.data);
    if (inferred) {
      migratedCount++;
      return {
        ...node,
        data: {
          ...node.data,
          spark_transform: inferred,
        },
      };
    }

    return node;
  });

  if (migratedCount > 0) {
    console.log(`[Migration] Migrated ${migratedCount} legacy node${migratedCount === 1 ? '' : 's'} to include spark_transform metadata`);
  }

  return migrated;
}
