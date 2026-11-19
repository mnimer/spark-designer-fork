/**
 * @fileoverview Canonical Apache Spark node capability registry and lookup helpers.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Defines pure TypeScript capability metadata for all Apache Spark sources, sinks,
 * and transformation steps, plus legacy transform fallbacks. Both the extension host
 * and the React webview import this registry to resolve batch or streaming modes,
 * sink execution strategies, dead-letter error handles, and input arity.
 */

/**
 * Execution read mode supported by a pipeline source node.
 */
export type ReadMode = 'batch' | 'streaming';

/**
 * Write execution mode used when a sink receives a streaming DataFrame.
 */
export type StreamingSinkMode = 'native' | 'foreachBatch';

/**
 * Number of upstream DataFrame inputs accepted by a step or sink node.
 */
export type InputArity = 1 | 2 | 'n';

/**
 * Capability flags describing read modes, sink streaming support, DLQ handles, and input arity.
 */
export interface NodeCapabilityFlags {
    supportedReadModes?: ReadMode[];
    defaultReadMode?: ReadMode;
    streamingSinkMode?: StreamingSinkMode;
    errorOutput?: boolean;
    inputArity?: InputArity;
}

const STREAMING_ONLY_SOURCE: NodeCapabilityFlags = {
    supportedReadModes: ['streaming'],
    defaultReadMode: 'streaming',
};

const DUAL_MODE_STREAMING_DEFAULT_SOURCE: NodeCapabilityFlags = {
    supportedReadModes: ['batch', 'streaming'],
    defaultReadMode: 'streaming',
};

const DUAL_MODE_BATCH_DEFAULT_SOURCE: NodeCapabilityFlags = {
    supportedReadModes: ['batch', 'streaming'],
    defaultReadMode: 'batch',
};

const BATCH_ONLY_SOURCE: NodeCapabilityFlags = {
    supportedReadModes: ['batch'],
    defaultReadMode: 'batch',
};

const NATIVE_STREAMING_SINK: NodeCapabilityFlags = {
    streamingSinkMode: 'native',
    inputArity: 1,
};

const FOREACH_BATCH_SINK: NodeCapabilityFlags = {
    streamingSinkMode: 'foreachBatch',
    inputArity: 1,
};

const SINGLE_INPUT_STEP: NodeCapabilityFlags = {
    inputArity: 1,
    errorOutput: false,
};

const ERROR_CAPABLE_SINGLE_INPUT_STEP: NodeCapabilityFlags = {
    inputArity: 1,
    errorOutput: true,
};

/**
 * Canonical capability lookup map covering all Spark sources, sinks, steps, and legacy aliases.
 */
export const SPARK_NODE_CAPABILITIES: Record<string, NodeCapabilityFlags> = {
    // -------------------------------------------------------------------------
    // Phase 5 — Spark Sources (SRC1–SRC38)
    // -------------------------------------------------------------------------
    // Streaming Category (SRC1–SRC8)
    KafkaSource: DUAL_MODE_STREAMING_DEFAULT_SOURCE,
    PubSubSource: DUAL_MODE_STREAMING_DEFAULT_SOURCE,
    KinesisSource: STREAMING_ONLY_SOURCE,
    PulsarSource: DUAL_MODE_STREAMING_DEFAULT_SOURCE,
    EventHubsSource: DUAL_MODE_STREAMING_DEFAULT_SOURCE,
    MqttSource: STREAMING_ONLY_SOURCE,
    RateSource: STREAMING_ONLY_SOURCE,
    SocketSource: STREAMING_ONLY_SOURCE,

    // Lakehouse Category (SRC9–SRC12)
    IcebergSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    DeltaSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    HudiSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    HiveSource: BATCH_ONLY_SOURCE,

    // File Format Category (SRC13–SRC21)
    ParquetSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    OrcSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    AvroSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    CsvSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    JsonSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    TextSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    XmlSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    BinaryFileSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    TFRecordSource: BATCH_ONLY_SOURCE,

    // Warehouse & Database Category (SRC22–SRC31)
    BigQuerySource: BATCH_ONLY_SOURCE,
    SpannerSource: BATCH_ONLY_SOURCE,
    BigtableSource: BATCH_ONLY_SOURCE,
    CloudSqlSource: BATCH_ONLY_SOURCE,
    AlloyDbSource: BATCH_ONLY_SOURCE,
    JdbcSource: BATCH_ONLY_SOURCE,
    SnowflakeSource: BATCH_ONLY_SOURCE,
    RedshiftSource: BATCH_ONLY_SOURCE,
    ClickHouseSource: BATCH_ONLY_SOURCE,
    SingleStoreSource: BATCH_ONLY_SOURCE,

    // NoSQL & Graph Category (SRC32–SRC37)
    MongoDbSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    FirestoreSource: BATCH_ONLY_SOURCE,
    CassandraSource: BATCH_ONLY_SOURCE,
    RedisSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,
    ElasticsearchSource: BATCH_ONLY_SOURCE,
    Neo4jSource: DUAL_MODE_BATCH_DEFAULT_SOURCE,

    // Synthetic / API Category (SRC38 + Phase 11 EXT1–EXT2)
    CreateDataFrameSource: BATCH_ONLY_SOURCE,
    RestApiSource: BATCH_ONLY_SOURCE,
    SftpSource: BATCH_ONLY_SOURCE,

    // -------------------------------------------------------------------------
    // Phase 6 — Spark Sinks (SNK1–SNK37)
    // -------------------------------------------------------------------------
    // Streaming Category (SNK1–SNK6)
    KafkaSink: NATIVE_STREAMING_SINK,
    PubSubSink: FOREACH_BATCH_SINK,
    KinesisSink: FOREACH_BATCH_SINK,
    PulsarSink: NATIVE_STREAMING_SINK,
    EventHubsSink: NATIVE_STREAMING_SINK,
    MqttSink: FOREACH_BATCH_SINK,

    // Lakehouse Category (SNK7–SNK10)
    IcebergSink: NATIVE_STREAMING_SINK,
    DeltaSink: NATIVE_STREAMING_SINK,
    HudiSink: NATIVE_STREAMING_SINK,
    HiveSink: FOREACH_BATCH_SINK,

    // File Format Category (SNK11–SNK18)
    ParquetSink: NATIVE_STREAMING_SINK,
    OrcSink: NATIVE_STREAMING_SINK,
    AvroSink: NATIVE_STREAMING_SINK,
    CsvSink: NATIVE_STREAMING_SINK,
    JsonSink: NATIVE_STREAMING_SINK,
    TextSink: NATIVE_STREAMING_SINK,
    XmlSink: FOREACH_BATCH_SINK,
    TFRecordSink: FOREACH_BATCH_SINK,

    // Warehouse & Database Category (SNK19–SNK28)
    BigQuerySink: NATIVE_STREAMING_SINK,
    SpannerSink: FOREACH_BATCH_SINK,
    BigtableSink: FOREACH_BATCH_SINK,
    CloudSqlSink: FOREACH_BATCH_SINK,
    AlloyDbSink: FOREACH_BATCH_SINK,
    JdbcSink: FOREACH_BATCH_SINK,
    SnowflakeSink: FOREACH_BATCH_SINK,
    RedshiftSink: FOREACH_BATCH_SINK,
    ClickHouseSink: FOREACH_BATCH_SINK,
    SingleStoreSink: FOREACH_BATCH_SINK,

    // NoSQL & Graph Category (SNK29–SNK34 + Phase 11 EXT3)
    MongoDbSink: NATIVE_STREAMING_SINK,
    FirestoreSink: FOREACH_BATCH_SINK,
    CassandraSink: NATIVE_STREAMING_SINK,
    RedisSink: FOREACH_BATCH_SINK,
    ElasticsearchSink: NATIVE_STREAMING_SINK,
    Neo4jSink: NATIVE_STREAMING_SINK,
    VectorStoreSink: FOREACH_BATCH_SINK,

    // Debug & Custom Category (SNK35–SNK37)
    ConsoleSink: NATIVE_STREAMING_SINK,
    MemorySink: NATIVE_STREAMING_SINK,
    ForeachBatchSink: FOREACH_BATCH_SINK,

    // -------------------------------------------------------------------------
    // Phase 7 — Spark Steps / Transformations (OP1–OP29 + ML aliases)
    // -------------------------------------------------------------------------
    Select: SINGLE_INPUT_STEP,
    Filter: ERROR_CAPABLE_SINGLE_INPUT_STEP,
    WithColumns: SINGLE_INPUT_STEP,
    RenameDropColumns: SINGLE_INPUT_STEP,
    ParsePayload: ERROR_CAPABLE_SINGLE_INPUT_STEP,
    Explode: SINGLE_INPUT_STEP,
    HandleNulls: SINGLE_INPUT_STEP,
    OrderBy: SINGLE_INPUT_STEP,
    LimitSample: SINGLE_INPUT_STEP,
    ConditionalSplit: SINGLE_INPUT_STEP,
    DataQualityAssert: ERROR_CAPABLE_SINGLE_INPUT_STEP,
    EnforceSchemaAndCast: ERROR_CAPABLE_SINGLE_INPUT_STEP,
    NestedStructArrayOps: SINGLE_INPUT_STEP,
    Join: { inputArity: 2, errorOutput: false },
    MergeCdcScd: { inputArity: 2, errorOutput: false },
    UnionByName: { inputArity: 'n', errorOutput: false },
    SetOperation: { inputArity: 'n', errorOutput: false },
    GroupByAgg: SINGLE_INPUT_STEP,
    TopN: SINGLE_INPUT_STEP,
    Deduplicate: SINGLE_INPUT_STEP,
    PivotUnpivot: SINGLE_INPUT_STEP,
    RollupCube: SINGLE_INPUT_STEP,
    WindowOver: SINGLE_INPUT_STEP,
    Watermark: SINGLE_INPUT_STEP,
    StatefulTransform: ERROR_CAPABLE_SINGLE_INPUT_STEP,
    SparkSqlQuery: { inputArity: 'n', errorOutput: true },
    PandasUdfTransform: ERROR_CAPABLE_SINGLE_INPUT_STEP,
    CustomTransform: ERROR_CAPABLE_SINGLE_INPUT_STEP,
    RepartitionCoalesce: SINGLE_INPUT_STEP,
    CachePersist: SINGLE_INPUT_STEP,
    FeaturePrep: SINGLE_INPUT_STEP,
    SparkMlAndVertexAi: ERROR_CAPABLE_SINGLE_INPUT_STEP,
    SparkMlInference: ERROR_CAPABLE_SINGLE_INPUT_STEP,
    VertexAiInference: ERROR_CAPABLE_SINGLE_INPUT_STEP,
    TextChunkAndEmbed: ERROR_CAPABLE_SINGLE_INPUT_STEP,

    // -------------------------------------------------------------------------
    // Legacy & Transitional Transform Fallbacks (Raw + SOURCE: / SINK: Scoped)
    // -------------------------------------------------------------------------
    GenerateSequence: STREAMING_ONLY_SOURCE,
    PeriodicImpulse: STREAMING_ONLY_SOURCE,
    FlinkStreamingImpulseSource: STREAMING_ONLY_SOURCE,
    SparkReceiverIO: STREAMING_ONLY_SOURCE,
    DebeziumIO: STREAMING_ONLY_SOURCE,
    Create: BATCH_ONLY_SOURCE,

    KafkaIO: { ...DUAL_MODE_STREAMING_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:KafkaIO': DUAL_MODE_STREAMING_DEFAULT_SOURCE,
    'SINK:KafkaIO': NATIVE_STREAMING_SINK,

    PubSubIO: { ...DUAL_MODE_STREAMING_DEFAULT_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:PubSubIO': DUAL_MODE_STREAMING_DEFAULT_SOURCE,
    'SINK:PubSubIO': FOREACH_BATCH_SINK,

    PulsarIO: { ...DUAL_MODE_STREAMING_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:PulsarIO': DUAL_MODE_STREAMING_DEFAULT_SOURCE,
    'SINK:PulsarIO': NATIVE_STREAMING_SINK,

    KinesisIO: { ...STREAMING_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:KinesisIO': STREAMING_ONLY_SOURCE,
    'SINK:KinesisIO': FOREACH_BATCH_SINK,

    MqttIO: { ...STREAMING_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:MqttIO': STREAMING_ONLY_SOURCE,
    'SINK:MqttIO': FOREACH_BATCH_SINK,

    AmqpIO: { ...STREAMING_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:AmqpIO': STREAMING_ONLY_SOURCE,
    'SINK:AmqpIO': FOREACH_BATCH_SINK,

    JmsIO: { ...STREAMING_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:JmsIO': STREAMING_ONLY_SOURCE,
    'SINK:JmsIO': FOREACH_BATCH_SINK,

    RabbitMqIO: { ...STREAMING_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:RabbitMqIO': STREAMING_ONLY_SOURCE,
    'SINK:RabbitMqIO': FOREACH_BATCH_SINK,

    SolaceIO: { ...STREAMING_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:SolaceIO': STREAMING_ONLY_SOURCE,
    'SINK:SolaceIO': FOREACH_BATCH_SINK,

    SnsIO: { ...STREAMING_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:SnsIO': STREAMING_ONLY_SOURCE,
    'SINK:SnsIO': FOREACH_BATCH_SINK,

    SqsIO: { ...STREAMING_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:SqsIO': STREAMING_ONLY_SOURCE,
    'SINK:SqsIO': FOREACH_BATCH_SINK,

    TrepWsIO: { ...STREAMING_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:TrepWsIO': STREAMING_ONLY_SOURCE,
    'SINK:TrepWsIO': FOREACH_BATCH_SINK,

    BigQueryIO: { ...BATCH_ONLY_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:BigQueryIO': BATCH_ONLY_SOURCE,
    'SINK:BigQueryIO': NATIVE_STREAMING_SINK,

    IcebergIO: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:IcebergIO': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:IcebergIO': NATIVE_STREAMING_SINK,

    SpannerIO: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:SpannerIO': BATCH_ONLY_SOURCE,
    'SINK:SpannerIO': FOREACH_BATCH_SINK,

    BigTableIO: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:BigTableIO': BATCH_ONLY_SOURCE,
    'SINK:BigTableIO': FOREACH_BATCH_SINK,

    CloudBigtable: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:CloudBigtable': BATCH_ONLY_SOURCE,
    'SINK:CloudBigtable': FOREACH_BATCH_SINK,

    JdbcIO: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:JdbcIO': BATCH_ONLY_SOURCE,
    'SINK:JdbcIO': FOREACH_BATCH_SINK,

    CloudSQL: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:CloudSQL': BATCH_ONLY_SOURCE,
    'SINK:CloudSQL': FOREACH_BATCH_SINK,

    DatabaseIO: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:DatabaseIO': BATCH_ONLY_SOURCE,
    'SINK:DatabaseIO': FOREACH_BATCH_SINK,

    MySQL: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:MySQL': BATCH_ONLY_SOURCE,
    'SINK:MySQL': FOREACH_BATCH_SINK,

    MySQLPostgreSQLOracleSQLServer: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:MySQLPostgreSQLOracleSQLServer': BATCH_ONLY_SOURCE,
    'SINK:MySQLPostgreSQLOracleSQLServer': FOREACH_BATCH_SINK,

    SnowflakeIO: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:SnowflakeIO': BATCH_ONLY_SOURCE,
    'SINK:SnowflakeIO': FOREACH_BATCH_SINK,

    ClickHouseIO: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:ClickHouseIO': BATCH_ONLY_SOURCE,
    'SINK:ClickHouseIO': FOREACH_BATCH_SINK,

    SingleStoreDBIO: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:SingleStoreDBIO': BATCH_ONLY_SOURCE,
    'SINK:SingleStoreDBIO': FOREACH_BATCH_SINK,

    MongoDbIO: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:MongoDbIO': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:MongoDbIO': NATIVE_STREAMING_SINK,

    FirestoreIO: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:FirestoreIO': BATCH_ONLY_SOURCE,
    'SINK:FirestoreIO': FOREACH_BATCH_SINK,

    CassandraIO: { ...BATCH_ONLY_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:CassandraIO': BATCH_ONLY_SOURCE,
    'SINK:CassandraIO': NATIVE_STREAMING_SINK,

    RedisIO: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:RedisIO': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:RedisIO': FOREACH_BATCH_SINK,

    ElasticsearchIO: { ...BATCH_ONLY_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:ElasticsearchIO': BATCH_ONLY_SOURCE,
    'SINK:ElasticsearchIO': NATIVE_STREAMING_SINK,

    Neo4jIO: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:Neo4jIO': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:Neo4jIO': NATIVE_STREAMING_SINK,

    ParquetIO: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:ParquetIO': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:ParquetIO': NATIVE_STREAMING_SINK,

    CsvIO: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:CsvIO': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:CsvIO': NATIVE_STREAMING_SINK,

    JsonIO: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:JsonIO': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:JsonIO': NATIVE_STREAMING_SINK,

    AvroIO: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:AvroIO': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:AvroIO': NATIVE_STREAMING_SINK,

    TextIO: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:TextIO': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:TextIO': NATIVE_STREAMING_SINK,

    XmlIO: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:XmlIO': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:XmlIO': FOREACH_BATCH_SINK,

    TFRecordIO: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:TFRecordIO': BATCH_ONLY_SOURCE,
    'SINK:TFRecordIO': FOREACH_BATCH_SINK,

    FileIO: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:FileIO': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:FileIO': NATIVE_STREAMING_SINK,

    GcsFileSystem: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:GcsFileSystem': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:GcsFileSystem': NATIVE_STREAMING_SINK,

    S3FileSystem: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:S3FileSystem': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:S3FileSystem': NATIVE_STREAMING_SINK,

    AzureBlobStoreFileSystem: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:AzureBlobStoreFileSystem': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:AzureBlobStoreFileSystem': NATIVE_STREAMING_SINK,

    HadoopFileSystem: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:HadoopFileSystem': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:HadoopFileSystem': NATIVE_STREAMING_SINK,

    LocalFileSystem: { ...DUAL_MODE_BATCH_DEFAULT_SOURCE, ...NATIVE_STREAMING_SINK },
    'SOURCE:LocalFileSystem': DUAL_MODE_BATCH_DEFAULT_SOURCE,
    'SINK:LocalFileSystem': NATIVE_STREAMING_SINK,

    HCatalogIO: { ...BATCH_ONLY_SOURCE, ...FOREACH_BATCH_SINK },
    'SOURCE:HCatalogIO': BATCH_ONLY_SOURCE,
    'SINK:HCatalogIO': FOREACH_BATCH_SINK,

    LogMessage: NATIVE_STREAMING_SINK,
    DatadogIO: FOREACH_BATCH_SINK,
    SplunkIO: FOREACH_BATCH_SINK,

    // Legacy Step Transforms (structural steps keep errorOutput: false)
    ParseMessage: SINGLE_INPUT_STEP,
    ParDo: SINGLE_INPUT_STEP,
    Flatten: { inputArity: 'n', errorOutput: false },
    CoGroupByKey: { inputArity: 'n', errorOutput: false },
};

/**
 * Clones a capability flag object so callers cannot mutate registry constants.
 *
 * @param flags Capability flags to copy.
 * @returns Defensive copy of the capability flags.
 */
function cloneFlags(flags: NodeCapabilityFlags): NodeCapabilityFlags {
    return {
        ...(flags.supportedReadModes ? { supportedReadModes: [...flags.supportedReadModes] } : {}),
        ...(flags.defaultReadMode !== undefined ? { defaultReadMode: flags.defaultReadMode } : {}),
        ...(flags.streamingSinkMode !== undefined ? { streamingSinkMode: flags.streamingSinkMode } : {}),
        ...(flags.errorOutput !== undefined ? { errorOutput: flags.errorOutput } : {}),
        ...(flags.inputArity !== undefined ? { inputArity: flags.inputArity } : {}),
    };
}

/**
 * Resolves capability flags for a Spark transform identifier and optional node category.
 * Supports `SOURCE:<name>`, `SINK:<name>`, and bare transform names, applying node-type defaults.
 *
 * @param sparkTransform Transform identifier on the node or catalog item.
 * @param nodeType Optional node type (`SOURCE`, `STEP`, or `SINK`).
 * @returns Resolved capability flags for the node.
 */
export function getNodeCapabilities(
    sparkTransform?: string,
    nodeType?: string
): NodeCapabilityFlags {
    const rawTransform = (sparkTransform ?? '').trim();
    let prefixType: string | undefined;
    let bareTransform = rawTransform;

    const colonIndex = rawTransform.indexOf(':');
    if (colonIndex > 0) {
        const candidatePrefix = rawTransform.slice(0, colonIndex).trim().toUpperCase();
        if (candidatePrefix === 'SOURCE' || candidatePrefix === 'SINK' || candidatePrefix === 'STEP') {
            prefixType = candidatePrefix;
            bareTransform = rawTransform.slice(colonIndex + 1).trim();
        }
    }

    const effectiveType = nodeType?.trim().toUpperCase() || prefixType;

    let matched: NodeCapabilityFlags | undefined;
    if (bareTransform) {
        if (effectiveType) {
            matched = SPARK_NODE_CAPABILITIES[`${effectiveType}:${bareTransform}`];
        }
        if (!matched && rawTransform !== bareTransform) {
            matched = SPARK_NODE_CAPABILITIES[rawTransform];
        }
        if (!matched) {
            matched = SPARK_NODE_CAPABILITIES[bareTransform];
        }
    }

    if (effectiveType === 'SOURCE') {
        const base = matched ?? {};
        return cloneFlags({
            supportedReadModes: base.supportedReadModes ?? ['batch'],
            defaultReadMode: base.defaultReadMode ?? base.supportedReadModes?.[0] ?? 'batch',
        });
    }

    if (effectiveType === 'SINK') {
        const base = matched ?? {};
        return cloneFlags({
            streamingSinkMode: base.streamingSinkMode ?? 'foreachBatch',
            inputArity: base.inputArity ?? 1,
        });
    }

    if (effectiveType === 'STEP') {
        const base = matched ?? {};
        return cloneFlags({
            errorOutput: base.errorOutput ?? false,
            inputArity: base.inputArity ?? 1,
        });
    }

    if (matched) {
        return cloneFlags(matched);
    }

    return {};
}
