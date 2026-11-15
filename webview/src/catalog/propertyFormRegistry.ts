/** @fileoverview Registry mapping node keys to dedicated property form components.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Bundle-size design (P8):
 * The ~150 dedicated property panels are the bulk of the webview's JavaScript, yet a
 * user only ever opens a handful of them per session. Every panel is therefore
 * registered as a `React.lazy` component backed by a dynamic `import()`, so Vite emits
 * one chunk per panel module under `assets/chunks/` and the entry bundle stays small.
 * The chunk is fetched the first time a node of that kind is selected and cached by the
 * module system afterwards.
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
 * Backs {@link loadPropertyForm}; a WeakMap so entries can never leak.
 */
const FORM_LOADERS = new WeakMap<object, PropertyFormLoader>();

/**
 * Wraps a property form module import as a code-split `React.lazy` component.
 *
 * The loader promise is memoised so that an explicit {@link loadPropertyForm} preload and
 * the subsequent Suspense render share a single in-flight import. A rejected import is
 * dropped from the memo so `loadPropertyForm` callers can retry; note that `React.lazy`
 * itself caches the first outcome for the lifetime of the wrapper.
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
const PubSubProperties = lazyForm(() => import('../properties/source/PubSubIO').then((m) => m.PubSubProperties));
const BigQuerySourceProperties = lazyForm(() => import('../properties/source/BigQueryIO').then((m) => m.BigQuerySourceProperties));
const CreateProperties = lazyForm(() => import('../properties/source/CreateIO').then((m) => m.CreateProperties));
const BigQuerySinkProperties = lazyForm(() => import('../properties/sink/BigQueryIO').then((m) => m.BigQuerySinkProperties));
const PubSubSinkProperties = lazyForm(() => import('../properties/sink/PubSubIO').then((m) => m.PubSubSinkProperties));
const ParseMessageProperties = lazyForm(() => import('../properties/step/ParseMessage').then((m) => m.ParseMessageProperties));
const FilterProperties = lazyForm(() => import('../properties/step/Filter').then((m) => m.FilterProperties));
const AggregationProperties = lazyForm(() => import('../properties/step/Aggregations').then((m) => m.AggregationProperties));
const TextIOSourceProperties = lazyForm(() => import('../properties/source/TextIO').then((m) => m.TextIOSourceProperties));
const TextIOSinkProperties = lazyForm(() => import('../properties/sink/TextIO').then((m) => m.TextIOSinkProperties));
const JdbcIOSourceProperties = lazyForm(() => import('../properties/source/JdbcIO').then((m) => m.JdbcIOSourceProperties));
const JdbcIOSinkProperties = lazyForm(() => import('../properties/sink/JdbcIO').then((m) => m.JdbcIOSinkProperties));
const KafkaIOSourceProperties = lazyForm(() => import('../properties/source/KafkaIO').then((m) => m.KafkaIOSourceProperties));
const KafkaIOSinkProperties = lazyForm(() => import('../properties/sink/KafkaIO').then((m) => m.KafkaIOSinkProperties));
const KinesisIOSourceProperties = lazyForm(() => import('../properties/source/KinesisIO').then((m) => m.KinesisIOSourceProperties));
const KinesisIOSinkProperties = lazyForm(() => import('../properties/sink/KinesisIO').then((m) => m.KinesisIOSinkProperties));
const ParquetIOSourceProperties = lazyForm(() => import('../properties/source/ParquetIO').then((m) => m.ParquetIOSourceProperties));
const ParquetIOSinkProperties = lazyForm(() => import('../properties/sink/ParquetIO').then((m) => m.ParquetIOSinkProperties));
const AvroIOSourceProperties = lazyForm(() => import('../properties/source/AvroIO').then((m) => m.AvroIOSourceProperties));
const AvroIOSinkProperties = lazyForm(() => import('../properties/sink/AvroIO').then((m) => m.AvroIOSinkProperties));
const MqttIOSourceProperties = lazyForm(() => import('../properties/source/MqttIO').then((m) => m.MqttIOSourceProperties));
const MqttIOSinkProperties = lazyForm(() => import('../properties/sink/MqttIO').then((m) => m.MqttIOSinkProperties));
const CassandraIOSourceProperties = lazyForm(() => import('../properties/source/CassandraIO').then((m) => m.CassandraIOSourceProperties));
const CassandraIOSinkProperties = lazyForm(() => import('../properties/sink/CassandraIO').then((m) => m.CassandraIOSinkProperties));
const MongoDbIOSourceProperties = lazyForm(() => import('../properties/source/MongoDbIO').then((m) => m.MongoDbIOSourceProperties));
const MongoDbIOSinkProperties = lazyForm(() => import('../properties/sink/MongoDbIO').then((m) => m.MongoDbIOSinkProperties));
const RedisIOSourceProperties = lazyForm(() => import('../properties/source/RedisIO').then((m) => m.RedisIOSourceProperties));
const RedisIOSinkProperties = lazyForm(() => import('../properties/sink/RedisIO').then((m) => m.RedisIOSinkProperties));
const BigTableIOSourceProperties = lazyForm(() => import('../properties/source/BigTableIO').then((m) => m.BigTableIOSourceProperties));
const BigTableIOSinkProperties = lazyForm(() => import('../properties/sink/BigTableIO').then((m) => m.BigTableIOSinkProperties));
const ClickHouseIOSourceProperties = lazyForm(() => import('../properties/source/ClickHouseIO').then((m) => m.ClickHouseIOSourceProperties));
const ClickHouseIOSinkProperties = lazyForm(() => import('../properties/sink/ClickHouseIO').then((m) => m.ClickHouseIOSinkProperties));
const ElasticsearchIOSourceProperties = lazyForm(() => import('../properties/source/ElasticsearchIO').then((m) => m.ElasticsearchIOSourceProperties));
const ElasticsearchIOSinkProperties = lazyForm(() => import('../properties/sink/ElasticsearchIO').then((m) => m.ElasticsearchIOSinkProperties));
const FirestoreIOSourceProperties = lazyForm(() => import('../properties/source/FirestoreIO').then((m) => m.FirestoreIOSourceProperties));
const FirestoreIOSinkProperties = lazyForm(() => import('../properties/sink/FirestoreIO').then((m) => m.FirestoreIOSinkProperties));
const Neo4jIOSourceProperties = lazyForm(() => import('../properties/source/Neo4jIO').then((m) => m.Neo4jIOSourceProperties));
const Neo4jIOSinkProperties = lazyForm(() => import('../properties/sink/Neo4jIO').then((m) => m.Neo4jIOSinkProperties));
const SingleStoreDBIOSourceProperties = lazyForm(() => import('../properties/source/SingleStoreDBIO').then((m) => m.SingleStoreDBIOSourceProperties));
const SingleStoreDBIOSinkProperties = lazyForm(() => import('../properties/sink/SingleStoreDBIO').then((m) => m.SingleStoreDBIOSinkProperties));
const SnowflakeIOSourceProperties = lazyForm(() => import('../properties/source/SnowflakeIO').then((m) => m.SnowflakeIOSourceProperties));
const SnowflakeIOSinkProperties = lazyForm(() => import('../properties/sink/SnowflakeIO').then((m) => m.SnowflakeIOSinkProperties));
const SpannerIOSourceProperties = lazyForm(() => import('../properties/source/SpannerIO').then((m) => m.SpannerIOSourceProperties));
const SpannerIOSinkProperties = lazyForm(() => import('../properties/sink/SpannerIO').then((m) => m.SpannerIOSinkProperties));
const CosmosIOSourceProperties = lazyForm(() => import('../properties/source/CosmosIO').then((m) => m.CosmosIOSourceProperties));
const HCatalogIOSourceProperties = lazyForm(() => import('../properties/source/HCatalogIO').then((m) => m.HCatalogIOSourceProperties));
const HCatalogIOSinkProperties = lazyForm(() => import('../properties/sink/HCatalogIO').then((m) => m.HCatalogIOSinkProperties));
const ArrowFlightIOSourceProperties = lazyForm(() => import('../properties/source/ArrowFlightIO').then((m) => m.ArrowFlightIOSourceProperties));
const ArrowFlightIOSinkProperties = lazyForm(() => import('../properties/sink/ArrowFlightIO').then((m) => m.ArrowFlightIOSinkProperties));
const PulsarIOSourceProperties = lazyForm(() => import('../properties/source/PulsarIO').then((m) => m.PulsarIOSourceProperties));
const PulsarIOSinkProperties = lazyForm(() => import('../properties/sink/PulsarIO').then((m) => m.PulsarIOSinkProperties));
const TFRecordIOSourceProperties = lazyForm(() => import('../properties/source/TFRecordIO').then((m) => m.TFRecordIOSourceProperties));
const TFRecordIOSinkProperties = lazyForm(() => import('../properties/sink/TFRecordIO').then((m) => m.TFRecordIOSinkProperties));
const CsvIOSourceProperties = lazyForm(() => import('../properties/source/CsvIO').then((m) => m.CsvIOSourceProperties));
const CsvIOSinkProperties = lazyForm(() => import('../properties/sink/CsvIO').then((m) => m.CsvIOSinkProperties));
const XmlIOSourceProperties = lazyForm(() => import('../properties/source/XmlIO').then((m) => m.XmlIOSourceProperties));
const XmlIOSinkProperties = lazyForm(() => import('../properties/sink/XmlIO').then((m) => m.XmlIOSinkProperties));
const JsonIOSourceProperties = lazyForm(() => import('../properties/source/JsonIO').then((m) => m.JsonIOSourceProperties));
const JsonIOSinkProperties = lazyForm(() => import('../properties/sink/JsonIO').then((m) => m.JsonIOSinkProperties));
const IcebergIOSourceProperties = lazyForm(() => import('../properties/source/IcebergIO').then((m) => m.IcebergIOSourceProperties));
const IcebergIOSinkProperties = lazyForm(() => import('../properties/sink/IcebergIO').then((m) => m.IcebergIOSinkProperties));
const CloudSQLSourceProperties = lazyForm(() => import('../properties/source/CloudSQL').then((m) => m.CloudSQLSourceProperties));
const CloudSQLSinkProperties = lazyForm(() => import('../properties/sink/CloudSQL').then((m) => m.CloudSQLSinkProperties));


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
  // Source IOs (qualified by SOURCE:)
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
  'SOURCE:CosmosIO': CosmosIOSourceProperties,
  'SOURCE:HCatalogIO': HCatalogIOSourceProperties,
  'SOURCE:ArrowFlightIO': ArrowFlightIOSourceProperties,
  'SOURCE:PulsarIO': PulsarIOSourceProperties,
  'SOURCE:Create': CreateProperties,
  'SOURCE:CloudSQL': CloudSQLSourceProperties,

  // Direct Source transforms (Python Beam transforms / unique names)
  'ReadFromAvro': AvroIOSourceProperties,
  'ReadFromKafka': KafkaIOSourceProperties,
  'ReadDataFromKinesis': KinesisIOSourceProperties,
  'ReadFromParquet': ParquetIOSourceProperties,
  'ReadFromTFRecord': TFRecordIOSourceProperties,
  'ReadFromCsv': CsvIOSourceProperties,
  'ReadFromJson': JsonIOSourceProperties,
  'ReadFromMqtt': MqttIOSourceProperties,
  'Create': CreateProperties,

  // Sink IOs (qualified by SINK:)
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
  'SINK:ArrowFlightIO': ArrowFlightIOSinkProperties,
  'SINK:PulsarIO': PulsarIOSinkProperties,
  'SINK:CloudSQL': CloudSQLSinkProperties,

  // Direct Sink transforms (Python Beam transforms / unique names)
  'WriteToAvro': AvroIOSinkProperties,
  'WriteToKafka': KafkaIOSinkProperties,
  'WriteToKinesis': KinesisIOSinkProperties,
  'WriteToParquet': ParquetIOSinkProperties,
  'WriteToTFRecord': TFRecordIOSinkProperties,
  'WriteToCsv': CsvIOSinkProperties,
  'WriteToJson': JsonIOSinkProperties,
  'WriteToMqtt': MqttIOSinkProperties,

  // Steps & Core Transforms (with and without STEP: prefix)
  'STEP:ParseMessage': ParseMessageProperties,
  'ParseMessage': ParseMessageProperties,
  'STEP:Filter': FilterProperties,
  'Filter': FilterProperties,

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

  // Other Transforms
};
