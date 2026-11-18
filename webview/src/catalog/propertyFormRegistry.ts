/** @fileoverview Registry mapping node keys to dedicated property form components.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Bundle-size design (P8):
 * The dedicated property panels are registered as `React.lazy` components backed by
 * dynamic `import()`, so Vite emits one chunk per panel module under `assets/chunks/`
 * and the entry bundle stays small. The chunk is fetched the first time a node of that
 * kind is selected and cached by the module system afterwards.
 *
 * Consumers must render registry entries inside a `<Suspense>` boundary (see
 * components/PropertyForm.tsx). Code that needs the concrete component (tests,
 * preloading) can resolve it with {@link loadPropertyForm}.
 */
/* eslint-disable @typescript-eslint/no-explicit-any */
import React from 'react';

/**
 * Shared props accepted by dynamic property form components registered in the registry.
 */
export interface PropertyFormProps {
  node: any;
  onChange: (properties: any) => void;
  onAddKVMapper?: (windowNodeId: string) => void;
  onAddPubSubSource?: (targetNodeId: string) => void;
}

/** Any React component usable as a property form (eager or lazy). */
export type PropertyFormComponent = React.ComponentType<PropertyFormProps>;

/** Async factory resolving a property form module to its concrete component. */
type PropertyFormLoader = () => Promise<PropertyFormComponent>;

/**
 * Lazy registry entry -> memoised loader of the concrete component behind it.
 * Backs {@link loadPropertyForm} via a WeakMap so entries never leak.
 */
const FORM_LOADERS = new WeakMap<object, PropertyFormLoader>();

/**
 * Wraps a property form module import as a code-split `React.lazy` component.
 *
 * The loader promise is memoised so that an explicit {@link loadPropertyForm} preload and
 * the subsequent Suspense render share a single in-flight import. A rejected import is
 * dropped from the memo so `loadPropertyForm` callers can retry.
 *
 * @param load Dynamic import resolving to the concrete form component.
 * @returns Lazy component suitable as a registry value.
 */
function lazyForm(load: PropertyFormLoader): React.LazyExoticComponent<PropertyFormComponent> {
  let pending: Promise<PropertyFormComponent> | undefined;
  const loadOnce: PropertyFormLoader = () => {
    if (!pending) {
      pending = load().catch((error: unknown) => {
        pending = undefined;
        throw error;
      });
    }
    return pending;
  };
  const Lazy = React.lazy(() => loadOnce().then((Component) => ({ default: Component })));
  FORM_LOADERS.set(Lazy, loadOnce);
  return Lazy;
}

/**
 * Resolves the concrete component behind a registry entry, triggering its chunk download
 * if necessary. Eager (non-lazy) components resolve to themselves.
 *
 * Use this to warm a panel's chunk ahead of time, or in tests that need to assert on the
 * real component rather than its lazy wrapper.
 *
 * @param entry A value from {@link PROPERTY_FORM_REGISTRY}.
 * @returns Promise of the concrete property form component.
 */
export function loadPropertyForm(entry: PropertyFormComponent): Promise<PropertyFormComponent> {
  const loader = FORM_LOADERS.get(entry);
  return loader ? loader() : Promise.resolve(entry);
}

// ---------------------------------------------------------------------------
// Lazily-loaded panel components (one dynamic import per source module).
// Keep each declaration on a single line so the module path stays greppable.
// ---------------------------------------------------------------------------

// Phase 5 — Source Property Forms (SRC1-SRC38)
const KafkaIOSourceProperties = lazyForm(() => import('../properties/source/KafkaIO').then((m) => m.KafkaIOSourceProperties));
const PubSubProperties = lazyForm(() => import('../properties/source/PubSubIO').then((m) => m.PubSubProperties));
const KinesisIOSourceProperties = lazyForm(() => import('../properties/source/KinesisIO').then((m) => m.KinesisIOSourceProperties));
const PulsarIOSourceProperties = lazyForm(() => import('../properties/source/PulsarIO').then((m) => m.PulsarIOSourceProperties));
const EventHubsSourceProperties = lazyForm(() => import('../properties/source/EventHubsSource').then((m) => m.EventHubsSourceProperties));
const MqttIOSourceProperties = lazyForm(() => import('../properties/source/MqttIO').then((m) => m.MqttIOSourceProperties));
const RateSourceProperties = lazyForm(() => import('../properties/source/RateSource').then((m) => m.RateSourceProperties));
const SocketSourceProperties = lazyForm(() => import('../properties/source/SocketSource').then((m) => m.SocketSourceProperties));
const IcebergIOSourceProperties = lazyForm(() => import('../properties/source/IcebergIO').then((m) => m.IcebergIOSourceProperties));
const DeltaSourceProperties = lazyForm(() => import('../properties/source/DeltaSource').then((m) => m.DeltaSourceProperties));
const HudiSourceProperties = lazyForm(() => import('../properties/source/HudiSource').then((m) => m.HudiSourceProperties));
const HCatalogIOSourceProperties = lazyForm(() => import('../properties/source/HCatalogIO').then((m) => m.HCatalogIOSourceProperties));
const ParquetIOSourceProperties = lazyForm(() => import('../properties/source/ParquetIO').then((m) => m.ParquetIOSourceProperties));
const OrcSourceProperties = lazyForm(() => import('../properties/source/OrcSource').then((m) => m.OrcSourceProperties));
const AvroIOSourceProperties = lazyForm(() => import('../properties/source/AvroIO').then((m) => m.AvroIOSourceProperties));
const CsvIOSourceProperties = lazyForm(() => import('../properties/source/CsvIO').then((m) => m.CsvIOSourceProperties));
const JsonIOSourceProperties = lazyForm(() => import('../properties/source/JsonIO').then((m) => m.JsonIOSourceProperties));
const TextIOSourceProperties = lazyForm(() => import('../properties/source/TextIO').then((m) => m.TextIOSourceProperties));
const XmlIOSourceProperties = lazyForm(() => import('../properties/source/XmlIO').then((m) => m.XmlIOSourceProperties));
const BinaryFileSourceProperties = lazyForm(() => import('../properties/source/BinaryFileSource').then((m) => m.BinaryFileSourceProperties));
const TFRecordIOSourceProperties = lazyForm(() => import('../properties/source/TFRecordIO').then((m) => m.TFRecordIOSourceProperties));
const BigQuerySourceProperties = lazyForm(() => import('../properties/source/BigQueryIO').then((m) => m.BigQuerySourceProperties));
const SpannerIOSourceProperties = lazyForm(() => import('../properties/source/SpannerIO').then((m) => m.SpannerIOSourceProperties));
const BigTableIOSourceProperties = lazyForm(() => import('../properties/source/BigTableIO').then((m) => m.BigTableIOSourceProperties));
const CloudSQLSourceProperties = lazyForm(() => import('../properties/source/CloudSQL').then((m) => m.CloudSQLSourceProperties));
const AlloyDbSourceProperties = lazyForm(() => import('../properties/source/AlloyDbSource').then((m) => m.AlloyDbSourceProperties));
const JdbcIOSourceProperties = lazyForm(() => import('../properties/source/JdbcIO').then((m) => m.JdbcIOSourceProperties));
const SnowflakeIOSourceProperties = lazyForm(() => import('../properties/source/SnowflakeIO').then((m) => m.SnowflakeIOSourceProperties));
const RedshiftSourceProperties = lazyForm(() => import('../properties/source/RedshiftSource').then((m) => m.RedshiftSourceProperties));
const ClickHouseIOSourceProperties = lazyForm(() => import('../properties/source/ClickHouseIO').then((m) => m.ClickHouseIOSourceProperties));
const SingleStoreDBIOSourceProperties = lazyForm(() => import('../properties/source/SingleStoreDBIO').then((m) => m.SingleStoreDBIOSourceProperties));
const MongoDbIOSourceProperties = lazyForm(() => import('../properties/source/MongoDbIO').then((m) => m.MongoDbIOSourceProperties));
const FirestoreIOSourceProperties = lazyForm(() => import('../properties/source/FirestoreIO').then((m) => m.FirestoreIOSourceProperties));
const CassandraIOSourceProperties = lazyForm(() => import('../properties/source/CassandraIO').then((m) => m.CassandraIOSourceProperties));
const RedisIOSourceProperties = lazyForm(() => import('../properties/source/RedisIO').then((m) => m.RedisIOSourceProperties));
const ElasticsearchIOSourceProperties = lazyForm(() => import('../properties/source/ElasticsearchIO').then((m) => m.ElasticsearchIOSourceProperties));
const Neo4jIOSourceProperties = lazyForm(() => import('../properties/source/Neo4jIO').then((m) => m.Neo4jIOSourceProperties));
const CreateProperties = lazyForm(() => import('../properties/source/CreateIO').then((m) => m.CreateProperties));

// Phase 6 — Sink Property Forms (SNK1-SNK37)
const KafkaIOSinkProperties = lazyForm(() => import('../properties/sink/KafkaIO').then((m) => m.KafkaIOSinkProperties));
const PubSubSinkProperties = lazyForm(() => import('../properties/sink/PubSubIO').then((m) => m.PubSubSinkProperties));
const KinesisIOSinkProperties = lazyForm(() => import('../properties/sink/KinesisIO').then((m) => m.KinesisIOSinkProperties));
const PulsarIOSinkProperties = lazyForm(() => import('../properties/sink/PulsarIO').then((m) => m.PulsarIOSinkProperties));
const EventHubsSinkProperties = lazyForm(() => import('../properties/sink/EventHubsSink').then((m) => m.EventHubsSinkProperties));
const MqttIOSinkProperties = lazyForm(() => import('../properties/sink/MqttIO').then((m) => m.MqttIOSinkProperties));
const IcebergIOSinkProperties = lazyForm(() => import('../properties/sink/IcebergIO').then((m) => m.IcebergIOSinkProperties));
const DeltaSinkProperties = lazyForm(() => import('../properties/sink/DeltaSink').then((m) => m.DeltaSinkProperties));
const HudiSinkProperties = lazyForm(() => import('../properties/sink/HudiSink').then((m) => m.HudiSinkProperties));
const HCatalogIOSinkProperties = lazyForm(() => import('../properties/sink/HCatalogIO').then((m) => m.HCatalogIOSinkProperties));
const ParquetIOSinkProperties = lazyForm(() => import('../properties/sink/ParquetIO').then((m) => m.ParquetIOSinkProperties));
const OrcSinkProperties = lazyForm(() => import('../properties/sink/OrcSink').then((m) => m.OrcSinkProperties));
const AvroIOSinkProperties = lazyForm(() => import('../properties/sink/AvroIO').then((m) => m.AvroIOSinkProperties));
const CsvIOSinkProperties = lazyForm(() => import('../properties/sink/CsvIO').then((m) => m.CsvIOSinkProperties));
const JsonIOSinkProperties = lazyForm(() => import('../properties/sink/JsonIO').then((m) => m.JsonIOSinkProperties));
const TextIOSinkProperties = lazyForm(() => import('../properties/sink/TextIO').then((m) => m.TextIOSinkProperties));
const XmlIOSinkProperties = lazyForm(() => import('../properties/sink/XmlIO').then((m) => m.XmlIOSinkProperties));
const TFRecordIOSinkProperties = lazyForm(() => import('../properties/sink/TFRecordIO').then((m) => m.TFRecordIOSinkProperties));
const BigQuerySinkProperties = lazyForm(() => import('../properties/sink/BigQueryIO').then((m) => m.BigQuerySinkProperties));
const SpannerIOSinkProperties = lazyForm(() => import('../properties/sink/SpannerIO').then((m) => m.SpannerIOSinkProperties));
const BigTableIOSinkProperties = lazyForm(() => import('../properties/sink/BigTableIO').then((m) => m.BigTableIOSinkProperties));
const CloudSQLSinkProperties = lazyForm(() => import('../properties/sink/CloudSQL').then((m) => m.CloudSQLSinkProperties));
const AlloyDbSinkProperties = lazyForm(() => import('../properties/sink/AlloyDbSink').then((m) => m.AlloyDbSinkProperties));
const JdbcIOSinkProperties = lazyForm(() => import('../properties/sink/JdbcIO').then((m) => m.JdbcIOSinkProperties));
const SnowflakeIOSinkProperties = lazyForm(() => import('../properties/sink/SnowflakeIO').then((m) => m.SnowflakeIOSinkProperties));
const RedshiftSinkProperties = lazyForm(() => import('../properties/sink/RedshiftSink').then((m) => m.RedshiftSinkProperties));
const ClickHouseIOSinkProperties = lazyForm(() => import('../properties/sink/ClickHouseIO').then((m) => m.ClickHouseIOSinkProperties));
const SingleStoreDBIOSinkProperties = lazyForm(() => import('../properties/sink/SingleStoreDBIO').then((m) => m.SingleStoreDBIOSinkProperties));
const MongoDbIOSinkProperties = lazyForm(() => import('../properties/sink/MongoDbIO').then((m) => m.MongoDbIOSinkProperties));
const FirestoreIOSinkProperties = lazyForm(() => import('../properties/sink/FirestoreIO').then((m) => m.FirestoreIOSinkProperties));
const CassandraIOSinkProperties = lazyForm(() => import('../properties/sink/CassandraIO').then((m) => m.CassandraIOSinkProperties));
const RedisIOSinkProperties = lazyForm(() => import('../properties/sink/RedisIO').then((m) => m.RedisIOSinkProperties));
const ElasticsearchIOSinkProperties = lazyForm(() => import('../properties/sink/ElasticsearchIO').then((m) => m.ElasticsearchIOSinkProperties));
const Neo4jIOSinkProperties = lazyForm(() => import('../properties/sink/Neo4jIO').then((m) => m.Neo4jIOSinkProperties));
const ConsoleSinkProperties = lazyForm(() => import('../properties/sink/ConsoleSink').then((m) => m.ConsoleSinkProperties));
const MemorySinkProperties = lazyForm(() => import('../properties/sink/MemorySink').then((m) => m.MemorySinkProperties));
const ForeachBatchSinkProperties = lazyForm(() => import('../properties/sink/ForeachBatchSink').then((m) => m.ForeachBatchSinkProperties));

// Step Property Forms
const ParseMessageProperties = lazyForm(() => import('../properties/step/ParseMessage').then((m) => m.ParseMessageProperties));
const FilterProperties = lazyForm(() => import('../properties/step/Filter').then((m) => m.FilterProperties));
const AggregationProperties = lazyForm(() => import('../properties/step/Aggregations').then((m) => m.AggregationProperties));
const WatermarkProperties = lazyForm(() => import('../properties/step/Watermark').then((m) => m.WatermarkProperties));
const GroupByAggProperties = lazyForm(() => import('../properties/step/GroupByAgg').then((m) => m.GroupByAggProperties));

/**
 * Map of immutable metadata keys (`${type}:${sparkTransform}` or direct transform name)
 * to their respective property form components.
 *
 * Each entry maps a node key to its dedicated panel component.
 *
 * NOTE: Keys are matched against immutable metadata (spark_transform / componentType),
 * NEVER against user-editable node labels.
 */
export const PROPERTY_FORM_REGISTRY: Record<string, PropertyFormComponent> = {
  // Phase 5 — Canonical Spark 4.x Sources (SRC1-SRC38, qualified by SOURCE: and direct)
  'SOURCE:KafkaSource': KafkaIOSourceProperties,
  'KafkaSource': KafkaIOSourceProperties,
  'SOURCE:PubSubSource': PubSubProperties,
  'PubSubSource': PubSubProperties,
  'SOURCE:KinesisSource': KinesisIOSourceProperties,
  'KinesisSource': KinesisIOSourceProperties,
  'SOURCE:PulsarSource': PulsarIOSourceProperties,
  'PulsarSource': PulsarIOSourceProperties,
  'SOURCE:EventHubsSource': EventHubsSourceProperties,
  'EventHubsSource': EventHubsSourceProperties,
  'SOURCE:MqttSource': MqttIOSourceProperties,
  'MqttSource': MqttIOSourceProperties,
  'SOURCE:RateSource': RateSourceProperties,
  'RateSource': RateSourceProperties,
  'SOURCE:SocketSource': SocketSourceProperties,
  'SocketSource': SocketSourceProperties,
  'SOURCE:IcebergSource': IcebergIOSourceProperties,
  'IcebergSource': IcebergIOSourceProperties,
  'SOURCE:DeltaSource': DeltaSourceProperties,
  'DeltaSource': DeltaSourceProperties,
  'SOURCE:HudiSource': HudiSourceProperties,
  'HudiSource': HudiSourceProperties,
  'SOURCE:HiveSource': HCatalogIOSourceProperties,
  'HiveSource': HCatalogIOSourceProperties,
  'SOURCE:ParquetSource': ParquetIOSourceProperties,
  'ParquetSource': ParquetIOSourceProperties,
  'SOURCE:OrcSource': OrcSourceProperties,
  'OrcSource': OrcSourceProperties,
  'SOURCE:AvroSource': AvroIOSourceProperties,
  'AvroSource': AvroIOSourceProperties,
  'SOURCE:CsvSource': CsvIOSourceProperties,
  'CsvSource': CsvIOSourceProperties,
  'SOURCE:JsonSource': JsonIOSourceProperties,
  'JsonSource': JsonIOSourceProperties,
  'SOURCE:TextSource': TextIOSourceProperties,
  'TextSource': TextIOSourceProperties,
  'SOURCE:XmlSource': XmlIOSourceProperties,
  'XmlSource': XmlIOSourceProperties,
  'SOURCE:BinaryFileSource': BinaryFileSourceProperties,
  'BinaryFileSource': BinaryFileSourceProperties,
  'SOURCE:TFRecordSource': TFRecordIOSourceProperties,
  'TFRecordSource': TFRecordIOSourceProperties,
  'SOURCE:BigQuerySource': BigQuerySourceProperties,
  'BigQuerySource': BigQuerySourceProperties,
  'SOURCE:SpannerSource': SpannerIOSourceProperties,
  'SpannerSource': SpannerIOSourceProperties,
  'SOURCE:BigtableSource': BigTableIOSourceProperties,
  'BigtableSource': BigTableIOSourceProperties,
  'SOURCE:CloudSqlSource': CloudSQLSourceProperties,
  'CloudSqlSource': CloudSQLSourceProperties,
  'SOURCE:AlloyDbSource': AlloyDbSourceProperties,
  'AlloyDbSource': AlloyDbSourceProperties,
  'SOURCE:JdbcSource': JdbcIOSourceProperties,
  'JdbcSource': JdbcIOSourceProperties,
  'SOURCE:SnowflakeSource': SnowflakeIOSourceProperties,
  'SnowflakeSource': SnowflakeIOSourceProperties,
  'SOURCE:RedshiftSource': RedshiftSourceProperties,
  'RedshiftSource': RedshiftSourceProperties,
  'SOURCE:ClickHouseSource': ClickHouseIOSourceProperties,
  'ClickHouseSource': ClickHouseIOSourceProperties,
  'SOURCE:SingleStoreSource': SingleStoreDBIOSourceProperties,
  'SingleStoreSource': SingleStoreDBIOSourceProperties,
  'SOURCE:MongoDbSource': MongoDbIOSourceProperties,
  'MongoDbSource': MongoDbIOSourceProperties,
  'SOURCE:FirestoreSource': FirestoreIOSourceProperties,
  'FirestoreSource': FirestoreIOSourceProperties,
  'SOURCE:CassandraSource': CassandraIOSourceProperties,
  'CassandraSource': CassandraIOSourceProperties,
  'SOURCE:RedisSource': RedisIOSourceProperties,
  'RedisSource': RedisIOSourceProperties,
  'SOURCE:ElasticsearchSource': ElasticsearchIOSourceProperties,
  'ElasticsearchSource': ElasticsearchIOSourceProperties,
  'SOURCE:Neo4jSource': Neo4jIOSourceProperties,
  'Neo4jSource': Neo4jIOSourceProperties,
  'SOURCE:CreateDataFrameSource': CreateProperties,
  'CreateDataFrameSource': CreateProperties,

  // Legacy Source aliases (backwards compatibility)
  'SOURCE:PubSubIO': PubSubProperties,
  'SOURCE:PubSub': PubSubProperties,
  'SOURCE:BigQueryIO': BigQuerySourceProperties,
  'SOURCE:BigQuery': BigQuerySourceProperties,
  'SOURCE:TextIO': TextIOSourceProperties,
  'SOURCE:JdbcIO': JdbcIOSourceProperties,
  'SOURCE:KafkaIO': KafkaIOSourceProperties,
  'SOURCE:KinesisIO': KinesisIOSourceProperties,
  'SOURCE:ParquetIO': ParquetIOSourceProperties,
  'SOURCE:AvroIO': AvroIOSourceProperties,
  'SOURCE:TFRecordIO': TFRecordIOSourceProperties,
  'SOURCE:CsvIO': CsvIOSourceProperties,
  'SOURCE:XmlIO': XmlIOSourceProperties,
  'SOURCE:JsonIO': JsonIOSourceProperties,
  'SOURCE:IcebergIO': IcebergIOSourceProperties,
  'SOURCE:MqttIO': MqttIOSourceProperties,
  'SOURCE:CassandraIO': CassandraIOSourceProperties,
  'SOURCE:MongoDbIO': MongoDbIOSourceProperties,
  'SOURCE:RedisIO': RedisIOSourceProperties,
  'SOURCE:BigTableIO': BigTableIOSourceProperties,
  'SOURCE:BigtableIO': BigTableIOSourceProperties,
  'SOURCE:ClickHouseIO': ClickHouseIOSourceProperties,
  'SOURCE:ElasticsearchIO': ElasticsearchIOSourceProperties,
  'SOURCE:FirestoreIO': FirestoreIOSourceProperties,
  'SOURCE:Neo4jIO': Neo4jIOSourceProperties,
  'SOURCE:SingleStoreDBIO': SingleStoreDBIOSourceProperties,
  'SOURCE:SingleStoreIO': SingleStoreDBIOSourceProperties,
  'SOURCE:SnowflakeIO': SnowflakeIOSourceProperties,
  'SOURCE:SpannerIO': SpannerIOSourceProperties,
  'SOURCE:HCatalogIO': HCatalogIOSourceProperties,
  'SOURCE:PulsarIO': PulsarIOSourceProperties,
  'SOURCE:Create': CreateProperties,
  'SOURCE:CloudSQL': CloudSQLSourceProperties,
  'ReadFromAvro': AvroIOSourceProperties,
  'ReadFromKafka': KafkaIOSourceProperties,
  'ReadDataFromKinesis': KinesisIOSourceProperties,
  'ReadFromParquet': ParquetIOSourceProperties,
  'ReadFromTFRecord': TFRecordIOSourceProperties,
  'ReadFromCsv': CsvIOSourceProperties,
  'ReadFromJson': JsonIOSourceProperties,
  'ReadFromMqtt': MqttIOSourceProperties,
  'Create': CreateProperties,

  // Phase 6 — Canonical Spark 4.x Sinks (SNK1-SNK37, qualified by SINK: and direct)
  'SINK:KafkaSink': KafkaIOSinkProperties,
  'KafkaSink': KafkaIOSinkProperties,
  'SINK:PubSubSink': PubSubSinkProperties,
  'PubSubSink': PubSubSinkProperties,
  'SINK:KinesisSink': KinesisIOSinkProperties,
  'KinesisSink': KinesisIOSinkProperties,
  'SINK:PulsarSink': PulsarIOSinkProperties,
  'PulsarSink': PulsarIOSinkProperties,
  'SINK:EventHubsSink': EventHubsSinkProperties,
  'EventHubsSink': EventHubsSinkProperties,
  'SINK:MqttSink': MqttIOSinkProperties,
  'MqttSink': MqttIOSinkProperties,
  'SINK:IcebergSink': IcebergIOSinkProperties,
  'IcebergSink': IcebergIOSinkProperties,
  'SINK:DeltaSink': DeltaSinkProperties,
  'DeltaSink': DeltaSinkProperties,
  'SINK:HudiSink': HudiSinkProperties,
  'HudiSink': HudiSinkProperties,
  'SINK:HiveSink': HCatalogIOSinkProperties,
  'HiveSink': HCatalogIOSinkProperties,
  'SINK:ParquetSink': ParquetIOSinkProperties,
  'ParquetSink': ParquetIOSinkProperties,
  'SINK:OrcSink': OrcSinkProperties,
  'OrcSink': OrcSinkProperties,
  'SINK:AvroSink': AvroIOSinkProperties,
  'AvroSink': AvroIOSinkProperties,
  'SINK:CsvSink': CsvIOSinkProperties,
  'CsvSink': CsvIOSinkProperties,
  'SINK:JsonSink': JsonIOSinkProperties,
  'JsonSink': JsonIOSinkProperties,
  'SINK:TextSink': TextIOSinkProperties,
  'TextSink': TextIOSinkProperties,
  'SINK:XmlSink': XmlIOSinkProperties,
  'XmlSink': XmlIOSinkProperties,
  'SINK:TFRecordSink': TFRecordIOSinkProperties,
  'TFRecordSink': TFRecordIOSinkProperties,
  'SINK:BigQuerySink': BigQuerySinkProperties,
  'BigQuerySink': BigQuerySinkProperties,
  'SINK:SpannerSink': SpannerIOSinkProperties,
  'SpannerSink': SpannerIOSinkProperties,
  'SINK:BigtableSink': BigTableIOSinkProperties,
  'BigtableSink': BigTableIOSinkProperties,
  'SINK:CloudSqlSink': CloudSQLSinkProperties,
  'CloudSqlSink': CloudSQLSinkProperties,
  'SINK:AlloyDbSink': AlloyDbSinkProperties,
  'AlloyDbSink': AlloyDbSinkProperties,
  'SINK:JdbcSink': JdbcIOSinkProperties,
  'JdbcSink': JdbcIOSinkProperties,
  'SINK:SnowflakeSink': SnowflakeIOSinkProperties,
  'SnowflakeSink': SnowflakeIOSinkProperties,
  'SINK:RedshiftSink': RedshiftSinkProperties,
  'RedshiftSink': RedshiftSinkProperties,
  'SINK:ClickHouseSink': ClickHouseIOSinkProperties,
  'ClickHouseSink': ClickHouseIOSinkProperties,
  'SINK:SingleStoreSink': SingleStoreDBIOSinkProperties,
  'SingleStoreSink': SingleStoreDBIOSinkProperties,
  'SINK:MongoDbSink': MongoDbIOSinkProperties,
  'MongoDbSink': MongoDbIOSinkProperties,
  'SINK:FirestoreSink': FirestoreIOSinkProperties,
  'FirestoreSink': FirestoreIOSinkProperties,
  'SINK:CassandraSink': CassandraIOSinkProperties,
  'CassandraSink': CassandraIOSinkProperties,
  'SINK:RedisSink': RedisIOSinkProperties,
  'RedisSink': RedisIOSinkProperties,
  'SINK:ElasticsearchSink': ElasticsearchIOSinkProperties,
  'ElasticsearchSink': ElasticsearchIOSinkProperties,
  'SINK:Neo4jSink': Neo4jIOSinkProperties,
  'Neo4jSink': Neo4jIOSinkProperties,
  'SINK:ConsoleSink': ConsoleSinkProperties,
  'ConsoleSink': ConsoleSinkProperties,
  'SINK:MemorySink': MemorySinkProperties,
  'MemorySink': MemorySinkProperties,
  'SINK:ForeachBatchSink': ForeachBatchSinkProperties,
  'ForeachBatchSink': ForeachBatchSinkProperties,

  // Legacy Sink aliases (backwards compatibility)
  'SINK:PubSubIO': PubSubSinkProperties,
  'SINK:PubSub': PubSubSinkProperties,
  'SINK:BigQueryIO': BigQuerySinkProperties,
  'SINK:BigQuery': BigQuerySinkProperties,
  'SINK:TextIO': TextIOSinkProperties,
  'SINK:JdbcIO': JdbcIOSinkProperties,
  'SINK:KafkaIO': KafkaIOSinkProperties,
  'SINK:KinesisIO': KinesisIOSinkProperties,
  'SINK:ParquetIO': ParquetIOSinkProperties,
  'SINK:AvroIO': AvroIOSinkProperties,
  'SINK:TFRecordIO': TFRecordIOSinkProperties,
  'SINK:CsvIO': CsvIOSinkProperties,
  'SINK:XmlIO': XmlIOSinkProperties,
  'SINK:JsonIO': JsonIOSinkProperties,
  'SINK:IcebergIO': IcebergIOSinkProperties,
  'SINK:MqttIO': MqttIOSinkProperties,
  'SINK:CassandraIO': CassandraIOSinkProperties,
  'SINK:MongoDbIO': MongoDbIOSinkProperties,
  'SINK:RedisIO': RedisIOSinkProperties,
  'SINK:BigTableIO': BigTableIOSinkProperties,
  'SINK:BigtableIO': BigTableIOSinkProperties,
  'SINK:ClickHouseIO': ClickHouseIOSinkProperties,
  'SINK:ElasticsearchIO': ElasticsearchIOSinkProperties,
  'SINK:FirestoreIO': FirestoreIOSinkProperties,
  'SINK:Neo4jIO': Neo4jIOSinkProperties,
  'SINK:SingleStoreDBIO': SingleStoreDBIOSinkProperties,
  'SINK:SingleStoreIO': SingleStoreDBIOSinkProperties,
  'SINK:SnowflakeIO': SnowflakeIOSinkProperties,
  'SINK:SpannerIO': SpannerIOSinkProperties,
  'SINK:HCatalogIO': HCatalogIOSinkProperties,
  'SINK:PulsarIO': PulsarIOSinkProperties,
  'SINK:CloudSQL': CloudSQLSinkProperties,
  'SINK:LogMessage': ConsoleSinkProperties,
  'LogMessage': ConsoleSinkProperties,
  'SINK:DatadogIO': ForeachBatchSinkProperties,
  'SINK:SplunkIO': ForeachBatchSinkProperties,
  'SINK:RequestResponseIO': ForeachBatchSinkProperties,
  'WriteToAvro': AvroIOSinkProperties,
  'WriteToBigQuery': BigQuerySinkProperties,
  'WriteToKafka': KafkaIOSinkProperties,
  'WriteToKinesis': KinesisIOSinkProperties,
  'WriteToParquet': ParquetIOSinkProperties,
  'WriteToTFRecord': TFRecordIOSinkProperties,
  'WriteToCsv': CsvIOSinkProperties,
  'WriteToJson': JsonIOSinkProperties,
  'WriteToText': TextIOSinkProperties,
  'WriteToMqtt': MqttIOSinkProperties,

  // Steps & Core Transforms (with and without STEP: prefix)
  'STEP:ParDo': AggregationProperties,
  'ParDo': AggregationProperties,
  'STEP:ParseMessage': ParseMessageProperties,
  'ParseMessage': ParseMessageProperties,
  'STEP:Filter': FilterProperties,
  'Filter': FilterProperties,
  'STEP:Watermark': WatermarkProperties,
  'Watermark': WatermarkProperties,
  'STEP:GroupByAgg': GroupByAggProperties,
  'GroupByAgg': GroupByAggProperties,
  'GroupBy & Aggregate': GroupByAggProperties,

  // Aggregation transforms
  'STEP:ApproximateQuantiles': AggregationProperties,
  'ApproximateQuantiles': AggregationProperties,
  'Approximate Quantiles': AggregationProperties,
  'Approximate Quantile': AggregationProperties,
  'STEP:ApproximateUnique': AggregationProperties,
  'ApproximateUnique': AggregationProperties,
  'Approximate Unique': AggregationProperties,
  'STEP:CoGroupByKey': AggregationProperties,
  'CoGroupByKey': AggregationProperties,
  'Co Group By Key': AggregationProperties,
  'STEP:Combine': AggregationProperties,
  'Combine': AggregationProperties,
  'STEP:CombineWithContext': AggregationProperties,
  'CombineWithContext': AggregationProperties,
  'Combine With Context': AggregationProperties,
  'STEP:Count': AggregationProperties,
  'Count': AggregationProperties,
  'STEP:Distinct': AggregationProperties,
  'Distinct': AggregationProperties,
  'STEP:GroupByKey': AggregationProperties,
  'GroupByKey': AggregationProperties,
  'Group By Key': AggregationProperties,
  'STEP:GroupIntoBatches': AggregationProperties,
  'GroupIntoBatches': AggregationProperties,
  'Group Into Batches': AggregationProperties,
  'STEP:BatchElements': AggregationProperties,
  'BatchElements': AggregationProperties,
  'Batch Elements': AggregationProperties,
  'STEP:Latest': AggregationProperties,
  'Latest': AggregationProperties,
  'STEP:Max': AggregationProperties,
  'Max': AggregationProperties,
  'STEP:Mean': AggregationProperties,
  'Mean': AggregationProperties,
  'STEP:Min': AggregationProperties,
  'Min': AggregationProperties,
  'STEP:Sample': AggregationProperties,
  'Sample': AggregationProperties,
  'STEP:Sum': AggregationProperties,
  'Sum': AggregationProperties,
  'STEP:Top': AggregationProperties,
  'Top': AggregationProperties,
};
