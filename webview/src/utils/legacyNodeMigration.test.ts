/**
 * Legacy Node Migration Tests
 * 
 * Author: Mike Nimer <mikenimer@google.com>
 * 
 * Purpose:
 * Validates deterministic inference of `spark_transform` for legacy nodes, idempotency,
 * unknown node handling, and verifies that real sample files migrate cleanly.
 */
import { describe, it, expect } from 'vitest';
import YAML from 'yaml';
import {
  inferBeamTransform,
  migrateLegacyNodes,
  resolveCanonicalSourceTransform,
  resolveCanonicalSinkTransform,
  resolveCanonicalStepTransform,
  type LegacyNodeData,
} from './legacyNodeMigration';

describe('legacyNodeMigration', () => {
  it('infers PubSubIO for legacy SOURCE node with Radio icon', () => {
    const data = {
      label: 'PubSub Connector',
      type: 'SOURCE',
      iconName: 'Radio'
    };
    expect(inferBeamTransform(data)).toBe('PubSubIO');
  });

  it('infers BigQueryIO for legacy SINK node with Database icon', () => {
    const data = {
      label: 'BigQuery',
      type: 'SINK',
      iconName: 'Database'
    };
    expect(inferBeamTransform(data)).toBe('BigQueryIO');
  });

  it('disambiguates MQTT vs PubSub on Radio icon using label', () => {
    expect(inferBeamTransform({ label: 'MQTT Source', type: 'SOURCE', iconName: 'Radio' })).toBe('MqttIO');
    expect(inferBeamTransform({ label: 'PubSub Topic', type: 'SOURCE', iconName: 'Radio' })).toBe('PubSubIO');
  });

  it('disambiguates JdbcIO vs BigQuery on Database icon using label', () => {
    expect(inferBeamTransform({ label: 'JdbcIO Reader', type: 'SOURCE', iconName: 'Database' })).toBe('JdbcIO');
    expect(inferBeamTransform({ label: 'BigQuery Table', type: 'SOURCE', iconName: 'Database' })).toBe('BigQueryIO');
  });

  it('disambiguates Kafka, JMS, and SQS on Cable icon using label', () => {
    expect(inferBeamTransform({ label: 'JMS Queue', type: 'SOURCE', iconName: 'Cable' })).toBe('JmsIO');
    expect(inferBeamTransform({ label: 'SQS Queue', type: 'SOURCE', iconName: 'Cable' })).toBe('SqsIO');
    expect(inferBeamTransform({ label: 'Kafka Stream', type: 'SOURCE', iconName: 'Cable' })).toBe('KafkaIO');
  });

  it('disambiguates Parquet vs Avro on FileCode icon using label', () => {
    expect(inferBeamTransform({ label: 'Parquet Reader', type: 'SOURCE', iconName: 'FileCode' })).toBe('ParquetIO');
    expect(inferBeamTransform({ label: 'Avro Reader', type: 'SOURCE', iconName: 'FileCode' })).toBe('AvroIO');
  });

  it('is strictly idempotent and never overwrites an existing spark_transform', () => {
    const modernNode = {
      label: 'Renamed Node to PubSub',
      type: 'SOURCE',
      iconName: 'Radio',
      spark_transform: 'CustomExistingTransform'
    };
    expect(inferBeamTransform(modernNode)).toBe('CustomExistingTransform');

    const nodeObject = {
      id: 'n1',
      data: modernNode
    };
    const result = migrateLegacyNodes([nodeObject]);
    expect(result[0].data.spark_transform).toBe('CustomExistingTransform');
  });

  it('returns null for genuinely unknown nodes without guessing', () => {
    expect(inferBeamTransform({ label: 'Unknown Operation', type: 'CUSTOM', iconName: 'HelpCircle' })).toBeNull();
    expect(inferBeamTransform(null as unknown as LegacyNodeData)).toBeNull();
  });

  it('migrates legacy nodes array while logging summary and preserving modern nodes', () => {
    const nodes = [
      { id: '1', data: { label: 'PubSub Connector', type: 'SOURCE', iconName: 'Radio' } },
      { id: '2', data: { label: 'ParDo Step', type: 'STEP', iconName: 'Code', spark_transform: 'ParDo' } },
      { id: '3', data: { label: 'BigQuery', type: 'SINK', iconName: 'Database' } }
    ];

    const migrated = migrateLegacyNodes(nodes);
    expect(migrated[0].data.spark_transform).toBe('PubSubIO');
    expect(migrated[1].data.spark_transform).toBe('ParDo');
    expect(migrated[2].data.spark_transform).toBe('BigQueryIO');
  });

  it('parses legacy pubsub-to-bigquery pipeline YAML and migrates every node deterministically', () => {
    const content = `
nodes:
  - id: n1
    data:
      label: PubSub Source
      type: SOURCE
      iconName: Radio
  - id: n2
    data:
      label: BigQuery Sink
      type: SINK
      iconName: Database
  - id: n3
    data:
      label: Parse Message
      type: STEP
      iconName: FileCode
  - id: n4
    data:
      label: Log Message
      type: SINK
      iconName: Terminal
`;
    const parsed = YAML.parse(content);
    const migrated = migrateLegacyNodes(parsed.nodes);
    expect(migrated.length).toBe(4);

    const transforms = migrated.map((n: { data?: LegacyNodeData }) => n.data?.spark_transform);
    expect(transforms).toEqual(['PubSubIO', 'BigQueryIO', 'ParseMessage', 'LogMessage']);
  });

  it('resolves legacy Beam source transforms to canonical Spark 4.x source transforms (SRC1-SRC38)', () => {
    expect(resolveCanonicalSourceTransform('KafkaIO')).toBe('KafkaSource');
    expect(resolveCanonicalSourceTransform('SOURCE:PubSubIO')).toBe('PubSubSource');
    expect(resolveCanonicalSourceTransform('BigQueryIO')).toBe('BigQuerySource');
    expect(resolveCanonicalSourceTransform('IcebergIO')).toBe('IcebergSource');
    expect(resolveCanonicalSourceTransform('ParquetIO')).toBe('ParquetSource');
    expect(resolveCanonicalSourceTransform('CsvIO')).toBe('CsvSource');
    expect(resolveCanonicalSourceTransform('JsonIO')).toBe('JsonSource');
    expect(resolveCanonicalSourceTransform('AvroIO')).toBe('AvroSource');
    expect(resolveCanonicalSourceTransform('TextIO')).toBe('TextSource');
    expect(resolveCanonicalSourceTransform('XmlIO')).toBe('XmlSource');
    expect(resolveCanonicalSourceTransform('TFRecordIO')).toBe('TFRecordSource');
    expect(resolveCanonicalSourceTransform('SpannerIO')).toBe('SpannerSource');
    expect(resolveCanonicalSourceTransform('BigTableIO')).toBe('BigtableSource');
    expect(resolveCanonicalSourceTransform('CloudSQL')).toBe('CloudSqlSource');
    expect(resolveCanonicalSourceTransform('JdbcIO')).toBe('JdbcSource');
    expect(resolveCanonicalSourceTransform('SnowflakeIO')).toBe('SnowflakeSource');
    expect(resolveCanonicalSourceTransform('ClickHouseIO')).toBe('ClickHouseSource');
    expect(resolveCanonicalSourceTransform('SingleStoreDBIO')).toBe('SingleStoreSource');
    expect(resolveCanonicalSourceTransform('MongoDbIO')).toBe('MongoDbSource');
    expect(resolveCanonicalSourceTransform('FirestoreIO')).toBe('FirestoreSource');
    expect(resolveCanonicalSourceTransform('CassandraIO')).toBe('CassandraSource');
    expect(resolveCanonicalSourceTransform('RedisIO')).toBe('RedisSource');
    expect(resolveCanonicalSourceTransform('ElasticsearchIO')).toBe('ElasticsearchSource');
    expect(resolveCanonicalSourceTransform('Neo4jIO')).toBe('Neo4jSource');
    expect(resolveCanonicalSourceTransform('HCatalogIO')).toBe('HiveSource');
    expect(resolveCanonicalSourceTransform('Create')).toBe('CreateDataFrameSource');
  });

  it('resolves legacy Beam sink transforms to canonical Spark 4.x sink transforms (SNK1-SNK37)', () => {
    expect(resolveCanonicalSinkTransform('KafkaIO')).toBe('KafkaSink');
    expect(resolveCanonicalSinkTransform('WriteToKafka')).toBe('KafkaSink');
    expect(resolveCanonicalSinkTransform('SINK:PubSubIO')).toBe('PubSubSink');
    expect(resolveCanonicalSinkTransform('BigQueryIO')).toBe('BigQuerySink');
    expect(resolveCanonicalSinkTransform('WriteToBigQuery')).toBe('BigQuerySink');
    expect(resolveCanonicalSinkTransform('IcebergIO')).toBe('IcebergSink');
    expect(resolveCanonicalSinkTransform('HCatalogIO')).toBe('HiveSink');
    expect(resolveCanonicalSinkTransform('ParquetIO')).toBe('ParquetSink');
    expect(resolveCanonicalSinkTransform('WriteToParquet')).toBe('ParquetSink');
    expect(resolveCanonicalSinkTransform('CsvIO')).toBe('CsvSink');
    expect(resolveCanonicalSinkTransform('WriteToCsv')).toBe('CsvSink');
    expect(resolveCanonicalSinkTransform('JsonIO')).toBe('JsonSink');
    expect(resolveCanonicalSinkTransform('WriteToJson')).toBe('JsonSink');
    expect(resolveCanonicalSinkTransform('AvroIO')).toBe('AvroSink');
    expect(resolveCanonicalSinkTransform('WriteToAvro')).toBe('AvroSink');
    expect(resolveCanonicalSinkTransform('TextIO')).toBe('TextSink');
    expect(resolveCanonicalSinkTransform('WriteToText')).toBe('TextSink');
    expect(resolveCanonicalSinkTransform('XmlIO')).toBe('XmlSink');
    expect(resolveCanonicalSinkTransform('TFRecordIO')).toBe('TFRecordSink');
    expect(resolveCanonicalSinkTransform('SpannerIO')).toBe('SpannerSink');
    expect(resolveCanonicalSinkTransform('BigTableIO')).toBe('BigtableSink');
    expect(resolveCanonicalSinkTransform('CloudSQL')).toBe('CloudSqlSink');
    expect(resolveCanonicalSinkTransform('JdbcIO')).toBe('JdbcSink');
    expect(resolveCanonicalSinkTransform('SnowflakeIO')).toBe('SnowflakeSink');
    expect(resolveCanonicalSinkTransform('ClickHouseIO')).toBe('ClickHouseSink');
    expect(resolveCanonicalSinkTransform('SingleStoreDBIO')).toBe('SingleStoreSink');
    expect(resolveCanonicalSinkTransform('MongoDbIO')).toBe('MongoDbSink');
    expect(resolveCanonicalSinkTransform('FirestoreIO')).toBe('FirestoreSink');
    expect(resolveCanonicalSinkTransform('CassandraIO')).toBe('CassandraSink');
    expect(resolveCanonicalSinkTransform('RedisIO')).toBe('RedisSink');
    expect(resolveCanonicalSinkTransform('ElasticsearchIO')).toBe('ElasticsearchSink');
    expect(resolveCanonicalSinkTransform('Neo4jIO')).toBe('Neo4jSink');
    expect(resolveCanonicalSinkTransform('LogMessage')).toBe('ConsoleSink');
    expect(resolveCanonicalSinkTransform('SINK:LogMessage')).toBe('ConsoleSink');
    expect(resolveCanonicalSinkTransform('DatadogIO')).toBe('ForeachBatchSink');
    expect(resolveCanonicalSinkTransform('SplunkIO')).toBe('ForeachBatchSink');
    expect(resolveCanonicalSinkTransform('RequestResponseIO')).toBe('ForeachBatchSink');
    expect(resolveCanonicalSinkTransform('DeltaSink')).toBe('DeltaSink');
  });

  it('resolves legacy Beam step transforms to canonical Spark 4.x step transforms (OP1-OP29)', () => {
    expect(resolveCanonicalStepTransform('ParDo')).toBe('CustomTransform');
    expect(resolveCanonicalStepTransform('STEP:ParDo')).toBe('CustomTransform');
    expect(resolveCanonicalStepTransform('ParseMessage')).toBe('ParsePayload');
    expect(resolveCanonicalStepTransform('STEP:ParseMessage')).toBe('ParsePayload');
    expect(resolveCanonicalStepTransform('Flatten')).toBe('UnionByName');
    expect(resolveCanonicalStepTransform('CoGroupByKey')).toBe('Join');
    expect(resolveCanonicalStepTransform('Co Group By Key')).toBe('Join');
    expect(resolveCanonicalStepTransform('Reshuffle')).toBe('RepartitionCoalesce');
    expect(resolveCanonicalStepTransform('ReShuffle')).toBe('RepartitionCoalesce');
    expect(resolveCanonicalStepTransform('Distinct')).toBe('Deduplicate');
    expect(resolveCanonicalStepTransform('Sample')).toBe('LimitSample');
    expect(resolveCanonicalStepTransform('Top')).toBe('TopN');
    expect(resolveCanonicalStepTransform('WindowInto')).toBe('GroupByAgg');
    expect(resolveCanonicalStepTransform('Window')).toBe('GroupByAgg');
    expect(resolveCanonicalStepTransform('WithTimestamps')).toBe('Watermark');
    expect(resolveCanonicalStepTransform('FlatMap')).toBe('Explode');
    expect(resolveCanonicalStepTransform('Map')).toBe('WithColumns');
    expect(resolveCanonicalStepTransform('Keys')).toBe('Select');
    expect(resolveCanonicalStepTransform('Values')).toBe('Select');
    expect(resolveCanonicalStepTransform('BatchElements')).toBe('PandasUdfTransform');
    expect(resolveCanonicalStepTransform('Count')).toBe('GroupByAgg');
    expect(resolveCanonicalStepTransform('Sum')).toBe('GroupByAgg');
    expect(resolveCanonicalStepTransform('Filter')).toBe('Filter');
    expect(resolveCanonicalStepTransform('GroupByAgg')).toBe('GroupByAgg');
    expect(resolveCanonicalStepTransform('Watermark')).toBe('Watermark');
    expect(resolveCanonicalStepTransform('SparkMlInference')).toBe('SparkMlAndVertexAi');
    expect(resolveCanonicalStepTransform('VertexAiInference')).toBe('SparkMlAndVertexAi');
    expect(resolveCanonicalStepTransform('StatefulTransform')).toBe('StatefulTransform');
  });
});
