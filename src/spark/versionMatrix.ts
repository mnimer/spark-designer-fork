/**
 * @fileoverview Apache Spark 4.x Runtime Version and Connector Classpath Matrix.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * NOTE: Keep this module free of host or browser imports such as vscode, fs,
 * path, child_process, os, react, or node built-ins. Both the VS Code extension
 * host and the Vite webview bundle compile this file directly.
 *
 * Purpose:
 * Maps target Spark execution runtimes to Spark, Python, Java, and Scala versions.
 * Resolves connector delivery mechanisms across pre-installed Managed Spark artifacts,
 * Cloud Storage JAR URIs, Scala 2.13 Maven coordinates, and Spark 4.0 Python
 * DataSource or foreachBatch SDK packages.
 *
 * Architectural Decisions:
 * - Default Serverless Spark tier to standard. Never default silently to premium.
 * - Exclude pre-installed connectors from Maven package lists on Managed Spark
 *   targets to prevent classpath conflicts.
 * - Emit Conscrypt removal properties for AlloyDB and SQL Server JDBC workloads
 *   so TLS handshakes succeed on Managed Spark.
 * - Route connectors without Spark 4.x Scala 2.13 JARs to Spark 4.0 Python
 *   DataSource classes or foreachBatch SDK client libraries.
 */

/**
 * Supported Spark 4.x execution runtime targets.
 */
export type SparkRuntimeTarget =
  | 'serverless_4.0'
  | 'serverless_3.0'
  | 'cluster_4.0'
  | 'cluster_3.0'
  | 'local_4.2_connect'
  | 'local';

/**
 * Supported runner identifiers for pipeline execution.
 */
export type SparkRunnerType = 'ServerlessSpark' | 'DataprocCluster' | 'Local';

/**
 * Compute billing tier for Google Cloud Serverless for Apache Spark.
 */
export type ServerlessTier = 'standard' | 'premium';

/**
 * Default Serverless Spark billing tier.
 */
export const DEFAULT_SERVERLESS_TIER: ServerlessTier = 'standard';

/**
 * Runtime version and pre-installed classpath metadata for a target platform.
 */
export interface RuntimeProfile {
  /** Canonical target identifier. */
  id: SparkRuntimeTarget;
  /** Normalized runner family name. */
  runner: SparkRunnerType;
  /** Platform runtime or image version string. */
  runtimeVersion: string;
  /** Bundled Apache Spark engine version. */
  sparkVersion: string;
  /** Bundled Python interpreter minor version. */
  pythonVersion: string;
  /** Bundled Java runtime major version. */
  javaVersion: string;
  /** Bundled Scala binary compatibility version. */
  scalaVersion: string;
  /** Default Serverless tier when running on Serverless Spark. */
  defaultTier?: ServerlessTier;
  /** Connector artifacts already present on the runtime classpath. */
  preinstalledConnectors: string[];
  /** True when the runtime supports Spark Declarative Pipelines on Spark 4.2 or newer. */
  sdpSupported: boolean;
  /** Operational guidance describing SDP support or runtime constraints. */
  sdpNotes?: string;
  /** Dataproc cluster optional components required for SDP execution. */
  optionalComponents?: string[];
  /** Python packages required on the Spark driver for SDP execution. */
  driverPipRequirements?: string[];
  /** True when Maven Central lacks an Iceberg runtime JAR for this local Spark version. */
  missingLocalIcebergRuntime?: boolean;
}

/**
 * Mechanism used to supply a connector at runtime.
 */
export type ConnectorDeliveryMechanism =
  | 'builtin'
  | 'preinstalled'
  | 'gcs_jar'
  | 'maven_package'
  | 'python_datasource'
  | 'foreach_batch_sdk';

/**
 * Resolved dependency and configuration payload for a single pipeline node.
 */
export interface ConnectorResolution {
  /** Transform identifier that was resolved. */
  sparkTransform: string;
  /** Primary delivery mechanism for this connector on the target runtime. */
  mechanism: ConnectorDeliveryMechanism;
  /** Cloud Storage JAR URIs to attach through spark.jars or --jars. */
  gcsJarUris: string[];
  /** Maven coordinates to attach through spark.jars.packages or --packages. */
  mavenCoordinates: string[];
  /** Python PyPI package names required for requirements files or containers. */
  pipPackages: string[];
  /** Spark or Dataproc properties required by the connector. */
  sparkProperties: Record<string, string>;
  /** Pre-installed Managed Spark artifact identifier when provided by the platform. */
  preinstalledArtifact?: string;
  /** Spark 4.0 Python DataSource class name when backed by Python DataSource API. */
  pythonDataSourceClass?: string;
  /** True when the target local Spark version lacks a published Iceberg runtime JAR. */
  missingLocalIcebergRuntime?: boolean;
  /** Operational guidance for code generation and execution planning. */
  notes?: string;
}

/**
 * Minimal node descriptor accepted when resolving pipeline-wide runtime artifacts.
 */
export interface PipelineRuntimeNodeInput {
  /** Direct camelCase Spark transform name. */
  sparkTransform?: string;
  /** Direct snake_case Spark transform name from serialized YAML nodes. */
  spark_transform?: string;
  /** Node configuration properties. */
  properties?: Record<string, unknown>;
  /** Optional React Flow or DAG validator node data wrapper. */
  data?: {
    sparkTransform?: string;
    spark_transform?: string;
    properties?: Record<string, unknown>;
  };
}

/**
 * Aggregated and deduplicated runtime dependencies for a complete Spark pipeline.
 */
export interface PipelineRuntimeArtifacts {
  /** Resolved target runtime profile. */
  profile: RuntimeProfile;
  /** Normalized Serverless Spark tier. */
  tier: ServerlessTier;
  /** Deduplicated Cloud Storage JAR URIs in deterministic order. */
  gcsJars: string[];
  /** Deduplicated Maven coordinates in deterministic order. */
  mavenPackages: string[];
  /** Deduplicated PyPI packages in deterministic order. */
  pipPackages: string[];
  /** Merged Spark and Dataproc configuration properties. */
  sparkProperties: Record<string, string>;
  /** Deduplicated Spark 4.0 Python DataSource class names. */
  pythonDataSources: string[];
  /** Deduplicated Managed Spark pre-installed connector names used by the pipeline. */
  preinstalledConnectors: string[];
  /** True when the pipeline uses Iceberg on a local Spark 4.2 profile without a published runtime JAR. */
  missingLocalIcebergRuntime?: boolean;
}

const MANAGED_SPARK_PREINSTALLED_CONNECTORS: readonly string[] = [
  'spark-bigquery',
  'iceberg-spark-runtime',
  'gcs-connector',
  'hadoop-aws',
  'hadoop-azure',
];

const DEFAULT_SPANNER_GCS_JAR = 'gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar';
const DEFAULT_BIGTABLE_GCS_JAR = 'gs://spark-lib/bigtable/spark-bigtable_2.13-0.1.0.jar';

const LOCAL_BIGQUERY_MAVEN =
  'com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.42.1';
const LOCAL_ICEBERG_MAVEN =
  'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1';
const LOCAL_GCS_CONNECTOR_MAVEN =
  'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25';
const LOCAL_HADOOP_AWS_MAVEN = 'org.apache.hadoop:hadoop-aws:3.4.1';
const LOCAL_HADOOP_AZURE_MAVEN = 'org.apache.hadoop:hadoop-azure:3.4.1';

/**
 * Canonical runtime profile definitions indexed by target identifier.
 */
export const RUNTIME_PROFILES: Readonly<Record<SparkRuntimeTarget, RuntimeProfile>> = {
  'serverless_4.0': {
    id: 'serverless_4.0',
    runner: 'ServerlessSpark',
    runtimeVersion: '4.0',
    sparkVersion: '4.2.0',
    pythonVersion: '3.13',
    javaVersion: '25',
    scalaVersion: '2.13',
    defaultTier: DEFAULT_SERVERLESS_TIER,
    preinstalledConnectors: [...MANAGED_SPARK_PREINSTALLED_CONNECTORS],
    sdpSupported: true,
    sdpNotes:
      'Supports Spark Declarative Pipelines via Managed Data Pipelines sdp stages or Serverless Spark 4.0.',
  },
  'serverless_3.0': {
    id: 'serverless_3.0',
    runner: 'ServerlessSpark',
    runtimeVersion: '3.0',
    sparkVersion: '4.0.1',
    pythonVersion: '3.12',
    javaVersion: '21',
    scalaVersion: '2.13',
    defaultTier: DEFAULT_SERVERLESS_TIER,
    preinstalledConnectors: [...MANAGED_SPARK_PREINSTALLED_CONNECTORS],
    sdpSupported: false,
    sdpNotes: 'Spark Declarative Pipelines requires Apache Spark 4.2+ (Serverless Runtime 4.0+).',
  },
  'cluster_4.0': {
    id: 'cluster_4.0',
    runner: 'DataprocCluster',
    runtimeVersion: '4.0',
    sparkVersion: '4.2.0',
    pythonVersion: '3.13',
    javaVersion: '25',
    scalaVersion: '2.13',
    preinstalledConnectors: [...MANAGED_SPARK_PREINSTALLED_CONNECTORS],
    sdpSupported: true,
    optionalComponents: ['ICEBERG'],
    driverPipRequirements: ['zstandard>=0.25.0'],
    sdpNotes:
      'Dataproc Compute Engine image 4.0 (Spark 4.2) requires --optional-components=ICEBERG and driver package zstandard>=0.25.0.',
  },
  'cluster_3.0': {
    id: 'cluster_3.0',
    runner: 'DataprocCluster',
    runtimeVersion: '3.0',
    sparkVersion: '4.1.2',
    pythonVersion: '3.12',
    javaVersion: '21',
    scalaVersion: '2.13',
    preinstalledConnectors: [...MANAGED_SPARK_PREINSTALLED_CONNECTORS],
    sdpSupported: false,
  },
  'local_4.2_connect': {
    id: 'local_4.2_connect',
    runner: 'Local',
    runtimeVersion: '4.2-connect',
    sparkVersion: '4.2.0',
    pythonVersion: '3.13',
    javaVersion: '21',
    scalaVersion: '2.13',
    preinstalledConnectors: [],
    sdpSupported: true,
    missingLocalIcebergRuntime: true,
    sdpNotes:
      'Local Spark 4.2 Spark Connect profile supports spark-pipelines dry-run and run, but Maven Central lacks iceberg-spark-runtime-4.2_2.13.',
  },
  local: {
    id: 'local',
    runner: 'Local',
    runtimeVersion: 'local',
    // NOTE: Keep this at Spark 4.0 to match the local Maven coordinates and the
    // generated pyspark pin. No Iceberg runtime exists for Spark 4.2 yet.
    sparkVersion: '4.0.1',
    pythonVersion: '3.13',
    javaVersion: '21',
    scalaVersion: '2.13',
    preinstalledConnectors: [],
    sdpSupported: false,
  },
};

/**
 * Formats the PEP 440 minor-line PySpark requirement pin for a Spark engine version.
 * Falls back to the local runtime profile minor line when the input version is blank or malformed.
 *
 * @param sparkVersion Full Spark engine version string such as 4.0.1.
 * @returns Minor-line PySpark requirement pin such as pyspark==4.0.*.
 */
export function getDefaultPysparkPin(
  sparkVersion = RUNTIME_PROFILES.local.sparkVersion
): string {
  const match = /^(\d+)\.(\d+)/.exec(sparkVersion.trim());
  if (match) {
    return `pyspark==${match[1]}.${match[2]}.*`;
  }
  const fallbackMatch = /^(\d+)\.(\d+)/.exec(RUNTIME_PROFILES.local.sparkVersion);
  const major = fallbackMatch?.[1] ?? '4';
  const minor = fallbackMatch?.[2] ?? '0';
  return `pyspark==${major}.${minor}.*`;
}

const FIXED_MAVEN_BY_TRANSFORM: Readonly<Record<string, string>> = {
  DeltaSource: 'io.delta:delta-spark_2.13:4.0.0',
  DeltaSink: 'io.delta:delta-spark_2.13:4.0.0',
  HudiSource: 'org.apache.hudi:hudi-spark4.0-bundle_2.13:1.0.1',
  HudiSink: 'org.apache.hudi:hudi-spark4.0-bundle_2.13:1.0.1',
  PulsarSource: 'io.streamnative.connectors:pulsar-spark-connector_2.13:4.0.0.1',
  PulsarSink: 'io.streamnative.connectors:pulsar-spark-connector_2.13:4.0.0.1',
  PulsarIO: 'io.streamnative.connectors:pulsar-spark-connector_2.13:4.0.0.1',
  SnowflakeSource: 'net.snowflake:spark-snowflake_2.13:3.1.0',
  SnowflakeSink: 'net.snowflake:spark-snowflake_2.13:3.1.0',
  SnowflakeIO: 'net.snowflake:spark-snowflake_2.13:3.1.0',
  RedshiftSource: 'io.github.spark-redshift-community:spark-redshift_2.13:6.3.0-spark_4.0',
  RedshiftSink: 'io.github.spark-redshift-community:spark-redshift_2.13:6.3.0-spark_4.0',
  MongoDbSource: 'org.mongodb.spark:mongo-spark-connector_2.13:10.4.1',
  MongoDbSink: 'org.mongodb.spark:mongo-spark-connector_2.13:10.4.1',
  MongoDbIO: 'org.mongodb.spark:mongo-spark-connector_2.13:10.4.1',
  ElasticsearchSource: 'org.elasticsearch:elasticsearch-spark-40_2.13:8.17.0',
  ElasticsearchSink: 'org.elasticsearch:elasticsearch-spark-40_2.13:8.17.0',
  ElasticsearchIO: 'org.elasticsearch:elasticsearch-spark-40_2.13:8.17.0',
  Neo4jSource: 'org.neo4j:neo4j-connector-apache-spark_2.13:5.3.4_for_spark_4',
  Neo4jSink: 'org.neo4j:neo4j-connector-apache-spark_2.13:5.3.4_for_spark_4',
  Neo4jIO: 'org.neo4j:neo4j-connector-apache-spark_2.13:5.3.4_for_spark_4',
  ClickHouseSource: 'com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.8.1',
  ClickHouseSink: 'com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.8.1',
  ClickHouseIO: 'com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.8.1',
  SingleStoreSource: 'com.singlestore:singlestore-spark-connector_2.13:4.1.9-spark-4.0.0',
  SingleStoreSink: 'com.singlestore:singlestore-spark-connector_2.13:4.1.9-spark-4.0.0',
  SingleStoreDBIO: 'com.singlestore:singlestore-spark-connector_2.13:4.1.9-spark-4.0.0',
  TFRecordSource: 'com.linkedin.sparktfrecord:spark-tfrecord_2.13:0.7.0',
  TFRecordSink: 'com.linkedin.sparktfrecord:spark-tfrecord_2.13:0.7.0',
  TFRecordIO: 'com.linkedin.sparktfrecord:spark-tfrecord_2.13:0.7.0',
};

/**
 * Internal specification for Python SDK or Spark 4.0 DataSource connectors.
 */
interface PythonConnectorSpec {
  mechanism: 'python_datasource' | 'foreach_batch_sdk';
  pipPackage: string;
  pythonDataSourceClass?: string;
  notes: string;
}

const PYTHON_CONNECTOR_BY_TRANSFORM: Readonly<Record<string, PythonConnectorSpec>> = {
  PubSubSource: {
    mechanism: 'python_datasource',
    pipPackage: 'google-cloud-pubsub',
    pythonDataSourceClass: 'PubSubDataSource',
    notes: 'Uses Spark 4.0 Python DataSourceStreamReader and DataSourceReader with google-cloud-pubsub.',
  },
  PubSubSink: {
    mechanism: 'foreach_batch_sdk',
    pipPackage: 'google-cloud-pubsub',
    notes: 'Publishes batches with google-cloud-pubsub through foreachBatch or Spark 4.0 DataSourceWriter.',
  },
  KinesisSource: {
    mechanism: 'python_datasource',
    pipPackage: 'boto3',
    pythonDataSourceClass: 'KinesisDataSource',
    notes: 'Reads AWS Kinesis shards with boto3 through Spark 4.0 Python DataSourceStreamReader.',
  },
  KinesisSink: {
    mechanism: 'foreach_batch_sdk',
    pipPackage: 'boto3',
    notes: 'Writes micro-batches to AWS Kinesis with boto3 put_records inside foreachBatch.',
  },
  MqttSource: {
    mechanism: 'python_datasource',
    pipPackage: 'paho-mqtt',
    pythonDataSourceClass: 'MqttDataSource',
    notes: 'Subscribes to MQTT topics with paho-mqtt through Spark 4.0 Python DataSourceStreamReader.',
  },
  MqttSink: {
    mechanism: 'foreach_batch_sdk',
    pipPackage: 'paho-mqtt',
    notes: 'Publishes records to MQTT topics with paho-mqtt inside foreachBatch.',
  },
  FirestoreSource: {
    mechanism: 'python_datasource',
    pipPackage: 'google-cloud-firestore',
    pythonDataSourceClass: 'FirestoreDataSource',
    notes: 'Reads Firestore collections with google-cloud-firestore through Spark 4.0 Python DataSourceReader.',
  },
  FirestoreSink: {
    mechanism: 'foreach_batch_sdk',
    pipPackage: 'google-cloud-firestore',
    notes: 'Commits Firestore WriteBatch mutations with google-cloud-firestore.',
  },
  CassandraSource: {
    mechanism: 'python_datasource',
    pipPackage: 'cassandra-driver',
    pythonDataSourceClass: 'CassandraDataSource',
    notes: 'Uses Spark 4.0 Python DataSource with cassandra-driver because spark-cassandra-connector targets Spark 3.5.',
  },
  CassandraSink: {
    mechanism: 'foreach_batch_sdk',
    pipPackage: 'cassandra-driver',
    notes: 'Writes partition batches with cassandra-driver because spark-cassandra-connector targets Spark 3.5.',
  },
  RedisSource: {
    mechanism: 'python_datasource',
    pipPackage: 'redis',
    pythonDataSourceClass: 'RedisDataSource',
    notes: 'Reads keys and Redis Streams with redis-py through Spark 4.0 Python DataSource.',
  },
  RedisSink: {
    mechanism: 'foreach_batch_sdk',
    pipPackage: 'redis',
    notes: 'Executes pipelined writes with redis-py.',
  },
  ForeachBatchSink: {
    mechanism: 'foreach_batch_sdk',
    pipPackage: 'requests',
    notes: 'Invokes custom Python foreachBatch logic or batched HTTP webhooks with requests.',
  },
  RestApiSource: {
    mechanism: 'python_datasource',
    pipPackage: 'requests',
    pythonDataSourceClass: 'RestApiDataSource',
    notes: 'Reads HTTP and REST API endpoints with requests through Spark 4.0 Python DataSourceReader.',
  },
  SftpSource: {
    mechanism: 'python_datasource',
    pipPackage: 'paramiko',
    pythonDataSourceClass: 'SftpDataSource',
    notes: 'Reads remote files over SFTP with paramiko through Spark 4.0 Python DataSourceReader.',
  },
};

const BUILTIN_FILE_FORMAT_TRANSFORMS = new Set<string>([
  'ParquetSource',
  'ParquetSink',
  'ParquetIO',
  'OrcSource',
  'OrcSink',
  'CsvSource',
  'CsvSink',
  'CsvIO',
  'JsonSource',
  'JsonSink',
  'JsonIO',
  'TextSource',
  'TextSink',
  'TextIO',
  'XmlSource',
  'XmlSink',
  'XmlIO',
  'BinaryFileSource',
  'FileIO',
]);

const STORAGE_AWARE_MAVEN_TRANSFORMS = new Set<string>([
  'AvroSource',
  'AvroSink',
  'AvroIO',
  'TFRecordSource',
  'TFRecordSink',
  'TFRecordIO',
  'DeltaSource',
  'DeltaSink',
  'HudiSource',
  'HudiSink',
]);

const JDBC_FAMILY_TRANSFORMS = new Set<string>([
  'CloudSqlSource',
  'CloudSqlSink',
  'CloudSQL',
  'AlloyDbSource',
  'AlloyDbSink',
  'JdbcSource',
  'JdbcSink',
  'JdbcIO',
  'DatabaseIO',
  'MySQL',
  'MySQLPostgreSQLOracleSQLServer',
]);

type CloudStorageBackend = 'gcs' | 's3a' | 'azure';

/**
 * Clones a canonical runtime profile so callers cannot mutate shared tables.
 *
 * @param profile Canonical runtime profile entry.
 * @returns Defensive copy of the runtime profile.
 */
function cloneRuntimeProfile(profile: RuntimeProfile): RuntimeProfile {
  return {
    ...profile,
    preinstalledConnectors: [...profile.preinstalledConnectors],
    ...(profile.optionalComponents
      ? { optionalComponents: [...profile.optionalComponents] }
      : {}),
    ...(profile.driverPipRequirements
      ? { driverPipRequirements: [...profile.driverPipRequirements] }
      : {}),
  };
}

/**
 * Checks whether a target runs on Google Cloud Managed Spark infrastructure.
 *
 * @param target Target runtime identifier.
 * @returns True if the target is Serverless Spark or Dataproc Cluster.
 */
export function isManagedSparkRuntime(target: SparkRuntimeTarget): boolean {
  return (
    target === 'serverless_4.0' ||
    target === 'serverless_3.0' ||
    target === 'cluster_4.0' ||
    target === 'cluster_3.0'
  );
}

/**
 * Checks whether a resolved runtime profile or runner selection supports Spark Declarative Pipelines.
 *
 * @param profileOrRunner Optional resolved runtime profile or runner identifier.
 * @param runtimeVersion Optional runtime or image version string when passing a runner name.
 * @returns True when the resolved runtime ships Apache Spark 4.2 or newer with SDP support.
 */
export function isRuntimeSdpCapable(
  profileOrRunner?: RuntimeProfile | string,
  runtimeVersion?: string
): boolean {
  if (profileOrRunner && typeof profileOrRunner === 'object') {
    return profileOrRunner.sdpSupported === true;
  }
  const profile = resolveRuntimeProfile(profileOrRunner, runtimeVersion);
  return profile.sdpSupported === true;
}

/**
 * Normalizes a Serverless Spark tier value.
 * If tier equals premium after trimming and lowercasing, return premium.
 * Otherwise return standard so pipelines never default silently to premium.
 *
 * @param tier Optional raw tier string from pipeline configuration.
 * @returns Normalized Serverless Spark tier.
 */
export function resolveServerlessTier(tier?: string): ServerlessTier {
  if (typeof tier === 'string' && tier.trim().toLowerCase() === 'premium') {
    return 'premium';
  }
  return DEFAULT_SERVERLESS_TIER;
}

/**
 * Resolves the canonical Spark 4.x runtime profile for a runner and version.
 *
 * @param runner Optional runner identifier or runtime target shorthand.
 * @param runtimeVersion Optional runtime or image version string.
 * @returns Matching runtime profile copy.
 */
export function resolveRuntimeProfile(
  runner?: string,
  runtimeVersion?: string
): RuntimeProfile {
  const normalizedRunner = typeof runner === 'string' ? runner.trim() : '';
  const lowerRunner = normalizedRunner.toLowerCase();
  const normalizedVersion =
    typeof runtimeVersion === 'string' ? runtimeVersion.trim().toLowerCase() : '';

  // Handle direct SparkRuntimeTarget identifiers first.
  if (lowerRunner === 'serverless_4.0') {
    return cloneRuntimeProfile(RUNTIME_PROFILES['serverless_4.0']);
  }
  if (lowerRunner === 'serverless_3.0') {
    return cloneRuntimeProfile(RUNTIME_PROFILES['serverless_3.0']);
  }
  if (lowerRunner === 'cluster_4.0') {
    return cloneRuntimeProfile(RUNTIME_PROFILES['cluster_4.0']);
  }
  if (lowerRunner === 'cluster_3.0') {
    return cloneRuntimeProfile(RUNTIME_PROFILES['cluster_3.0']);
  }
  if (lowerRunner === 'local_4.2_connect') {
    return cloneRuntimeProfile(RUNTIME_PROFILES['local_4.2_connect']);
  }
  if (lowerRunner === 'local' || lowerRunner === '') {
    if (
      normalizedVersion === '4.2-connect' ||
      normalizedVersion === '4.2_connect' ||
      normalizedVersion === '4.2' ||
      normalizedVersion.startsWith('4.2 ') ||
      normalizedVersion.startsWith('4.2(') ||
      normalizedVersion.startsWith('4.2.') ||
      normalizedVersion === 'local_4.2_connect'
    ) {
      return cloneRuntimeProfile(RUNTIME_PROFILES['local_4.2_connect']);
    }
    return cloneRuntimeProfile(RUNTIME_PROFILES.local);
  }

  // Resolve Google Cloud Serverless for Apache Spark profiles.
  if (
    lowerRunner === 'serverlessspark' ||
    lowerRunner === 'serverless_spark' ||
    lowerRunner === 'serverless'
  ) {
    if (
      normalizedVersion === '3.0' ||
      normalizedVersion.startsWith('3.0 ') ||
      normalizedVersion.startsWith('3.0(') ||
      normalizedVersion === 'serverless_3.0'
    ) {
      return cloneRuntimeProfile(RUNTIME_PROFILES['serverless_3.0']);
    }
    return cloneRuntimeProfile(RUNTIME_PROFILES['serverless_4.0']);
  }

  // Resolve Google Cloud Dataproc Cluster profiles.
  if (
    lowerRunner === 'dataproccluster' ||
    lowerRunner === 'dataproc_cluster' ||
    lowerRunner === 'dataproc' ||
    lowerRunner === 'cluster'
  ) {
    if (
      normalizedVersion.startsWith('4.0') ||
      normalizedVersion === 'cluster_4.0'
    ) {
      return cloneRuntimeProfile(RUNTIME_PROFILES['cluster_4.0']);
    }
    return cloneRuntimeProfile(RUNTIME_PROFILES['cluster_3.0']);
  }

  return cloneRuntimeProfile(RUNTIME_PROFILES.local);
}

/**
 * Strips optional node-kind prefixes such as SOURCE: or SINK: from a transform key.
 *
 * @param rawTransform Raw transform string.
 * @returns Trimmed transform name and optional prefix kind.
 */
function parseTransformKey(rawTransform: string): {
  cleanTransform: string;
  prefixKind?: 'SOURCE' | 'SINK' | 'STEP';
} {
  const trimmed = rawTransform.trim();
  if (trimmed.startsWith('SOURCE:')) {
    return { cleanTransform: trimmed.slice('SOURCE:'.length), prefixKind: 'SOURCE' };
  }
  if (trimmed.startsWith('SINK:')) {
    return { cleanTransform: trimmed.slice('SINK:'.length), prefixKind: 'SINK' };
  }
  if (trimmed.startsWith('STEP:')) {
    return { cleanTransform: trimmed.slice('STEP:'.length), prefixKind: 'STEP' };
  }
  return { cleanTransform: trimmed };
}

/**
 * Reads a trimmed non-empty string property if present.
 *
 * @param properties Optional property bag.
 * @param key Property key to inspect.
 * @returns Trimmed non-empty string or undefined.
 */
function getNonEmptyString(
  properties: Record<string, unknown> | undefined,
  key: string
): string | undefined {
  const raw = properties?.[key];
  if (typeof raw === 'string') {
    const trimmed = raw.trim();
    if (trimmed.length > 0) {
      return trimmed;
    }
  }
  return undefined;
}

/**
 * Appends unique non-empty strings into a target list while preserving first-seen order.
 *
 * @param target Mutable accumulator array.
 * @param seen Set tracking previously added items.
 * @param items Candidate strings to append.
 */
function pushUnique(target: string[], seen: Set<string>, items: readonly string[]): void {
  for (const item of items) {
    const trimmed = item.trim();
    if (trimmed.length > 0 && !seen.has(trimmed)) {
      seen.add(trimmed);
      target.push(trimmed);
    }
  }
}

/**
 * Detects a cloud object-storage backend from explicit backend fields or URI schemes.
 *
 * @param properties Optional node property bag.
 * @returns Cloud storage backend identifier or undefined for local and HDFS paths.
 */
function detectCloudStorageBackend(
  properties?: Record<string, unknown>
): CloudStorageBackend | undefined {
  if (!properties) {
    return undefined;
  }

  const explicitBackend = getNonEmptyString(properties, 'storageBackend')?.toLowerCase();
  if (explicitBackend === 'gcs' || explicitBackend === 'gs') {
    return 'gcs';
  }
  if (explicitBackend === 's3' || explicitBackend === 's3a') {
    return 's3a';
  }
  if (
    explicitBackend === 'azure' ||
    explicitBackend === 'abfss' ||
    explicitBackend === 'abfs' ||
    explicitBackend === 'wasbs' ||
    explicitBackend === 'wasb'
  ) {
    return 'azure';
  }

  const candidateUriKeys = [
    'path',
    'uri',
    'inputPath',
    'outputPath',
    'location',
    'warehouse',
    'warehouseLocation',
  ];
  for (const key of candidateUriKeys) {
    const uri = getNonEmptyString(properties, key)?.toLowerCase();
    if (!uri) {
      continue;
    }
    if (uri.startsWith('gs://')) {
      return 'gcs';
    }
    if (uri.startsWith('s3a://') || uri.startsWith('s3://')) {
      return 's3a';
    }
    if (
      uri.startsWith('abfss://') ||
      uri.startsWith('abfs://') ||
      uri.startsWith('wasbs://') ||
      uri.startsWith('wasb://')
    ) {
      return 'azure';
    }
  }

  return undefined;
}

/**
 * Maps a cloud storage backend to its Managed Spark pre-installed artifact and local Maven coordinate.
 *
 * @param backend Cloud storage backend kind.
 * @returns Pre-installed artifact name and local Maven coordinate.
 */
function getStorageBackendArtifacts(backend: CloudStorageBackend): {
  preinstalledArtifact: string;
  localMavenCoordinate: string;
} {
  switch (backend) {
    case 'gcs':
      return {
        preinstalledArtifact: 'gcs-connector',
        localMavenCoordinate: LOCAL_GCS_CONNECTOR_MAVEN,
      };
    case 's3a':
      return {
        preinstalledArtifact: 'hadoop-aws',
        localMavenCoordinate: LOCAL_HADOOP_AWS_MAVEN,
      };
    case 'azure':
      return {
        preinstalledArtifact: 'hadoop-azure',
        localMavenCoordinate: LOCAL_HADOOP_AZURE_MAVEN,
      };
  }
}

/**
 * Resolves JDBC, Cloud SQL, and AlloyDB JAR URIs and Conscrypt properties.
 *
 * @param cleanTransform Normalized transform name.
 * @param properties Optional node property bag.
 * @returns Resolved connector descriptor for JDBC-family nodes.
 */
function resolveJdbcFamilyConnector(
  cleanTransform: string,
  properties?: Record<string, unknown>
): ConnectorResolution {
  const gcsJarUris: string[] = [];
  const seenJars = new Set<string>();

  const jarKeys = ['driverJarUri', 'socketFactoryJarUri', 'jarUri'];
  for (const key of jarKeys) {
    const uri = getNonEmptyString(properties, key);
    if (uri) {
      pushUnique(gcsJarUris, seenJars, [uri]);
    }
  }

  const engine = (
    getNonEmptyString(properties, 'engine') ??
    getNonEmptyString(properties, 'dbEngine') ??
    getNonEmptyString(properties, 'dialect') ??
    ''
  ).toLowerCase();
  const jdbcUrl = (
    getNonEmptyString(properties, 'url') ??
    getNonEmptyString(properties, 'jdbcUrl') ??
    ''
  ).toLowerCase();

  const isAlloyDb = cleanTransform === 'AlloyDbSource' || cleanTransform === 'AlloyDbSink';
  const isSqlServer =
    engine === 'sqlserver' ||
    engine === 'mssql' ||
    engine === 'sql_server' ||
    jdbcUrl.startsWith('jdbc:sqlserver:');

  const sparkProperties: Record<string, string> = {};
  if (isAlloyDb || isSqlServer) {
    sparkProperties['dataproc.artifacts.remove'] = 'conscrypt';
  }

  return {
    sparkTransform: cleanTransform,
    mechanism: 'gcs_jar',
    gcsJarUris,
    mavenCoordinates: [],
    pipPackages: [],
    sparkProperties,
    notes: isAlloyDb || isSqlServer
      ? 'Attaches JDBC driver and socket factory JARs and removes Conscrypt for TLS compatibility.'
      : 'Attaches JDBC driver and optional socket factory JARs through spark.jars.',
  };
}

/**
 * Resolves AlloyDB pgvector and Vertex AI Vector Search sink dependencies.
 *
 * @param cleanTransform Normalized transform name.
 * @param properties Optional node property bag.
 * @returns Resolved connector descriptor for VectorStoreSink nodes.
 */
function resolveVectorStoreConnector(
  cleanTransform: string,
  properties?: Record<string, unknown>
): ConnectorResolution {
  const backend = getNonEmptyString(properties, 'backend')?.toLowerCase();
  if (backend === 'vertex_vector_search') {
    return {
      sparkTransform: cleanTransform,
      mechanism: 'foreach_batch_sdk',
      gcsJarUris: [],
      mavenCoordinates: [],
      pipPackages: ['google-cloud-aiplatform'],
      sparkProperties: {},
      notes: 'Upserts embedding datapoints or exports JSON Lines to Vertex AI Vector Search with google-cloud-aiplatform.',
    };
  }

  const gcsJarUris: string[] = [];
  const seenJars = new Set<string>();
  for (const key of ['driverJarUri', 'socketFactoryJarUri']) {
    const uri = getNonEmptyString(properties, key);
    if (uri) {
      pushUnique(gcsJarUris, seenJars, [uri]);
    }
  }

  return {
    sparkTransform: cleanTransform,
    mechanism: 'foreach_batch_sdk',
    gcsJarUris,
    mavenCoordinates: [],
    pipPackages: ['psycopg[binary]', 'google-cloud-alloydb-connector'],
    sparkProperties: {
      'dataproc.artifacts.remove': 'conscrypt',
    },
    notes: 'Upserts pgvector embeddings into AlloyDB with psycopg and google-cloud-alloydb-connector and removes Conscrypt.',
  };
}

/**
 * Resolves Python ML and Vertex AI packages for SparkMlAndVertexAi step nodes.
 *
 * @param cleanTransform Normalized transform name.
 * @param properties Optional node property bag.
 * @returns Resolved connector descriptor for ML and Vertex AI inference steps.
 */
function resolveMlAndVertexConnector(
  cleanTransform: string,
  properties?: Record<string, unknown>
): ConnectorResolution {
  if (cleanTransform === 'VertexAiInference') {
    return {
      sparkTransform: cleanTransform,
      mechanism: 'foreach_batch_sdk',
      gcsJarUris: [],
      mavenCoordinates: [],
      pipPackages: ['google-cloud-aiplatform'],
      sparkProperties: {},
      notes: 'Executes batched Vertex AI inference with google-cloud-aiplatform.',
    };
  }

  const configTokens = [
    getNonEmptyString(properties, 'mode'),
    getNonEmptyString(properties, 'modelType'),
    getNonEmptyString(properties, 'inferenceType'),
    getNonEmptyString(properties, 'provider'),
    getNonEmptyString(properties, 'framework'),
    getNonEmptyString(properties, 'algorithm'),
    getNonEmptyString(properties, 'engine'),
  ]
    .filter((val): val is string => Boolean(val))
    .map((val) => val.toLowerCase());

  const pipPackages: string[] = [];
  const seenPip = new Set<string>();

  const wantsXgboost =
    properties?.useXgboost === true ||
    configTokens.some((token) => token.includes('xgb'));
  const wantsVertexAi =
    properties?.useVertexAi === true ||
    Boolean(getNonEmptyString(properties, 'endpointId')) ||
    Boolean(getNonEmptyString(properties, 'endpoint')) ||
    Boolean(getNonEmptyString(properties, 'geminiModel')) ||
    configTokens.some(
      (token) =>
        token.includes('vertex') ||
        token.includes('gemini') ||
        token.includes('aiplatform')
    );

  if (wantsXgboost) {
    pushUnique(pipPackages, seenPip, ['xgboost']);
  }
  if (wantsVertexAi && cleanTransform !== 'SparkMlInference') {
    pushUnique(pipPackages, seenPip, ['google-cloud-aiplatform']);
  }

  if (pipPackages.length > 0) {
    return {
      sparkTransform: cleanTransform,
      mechanism: 'foreach_batch_sdk',
      gcsJarUris: [],
      mavenCoordinates: [],
      pipPackages,
      sparkProperties: {},
      notes: 'Installs configured Python ML or Vertex AI SDK packages.',
    };
  }

  return {
    sparkTransform: cleanTransform,
    mechanism: 'builtin',
    gcsJarUris: [],
    mavenCoordinates: [],
    pipPackages: [],
    sparkProperties: {},
    notes: 'Uses built-in pyspark.ml PipelineModel APIs.',
  };
}

/**
 * Resolves legacy Beam IO transform names for Python SDK connectors when present.
 *
 * @param cleanTransform Normalized transform name.
 * @param prefixKind Optional node-kind prefix.
 * @returns Canonical Spark transform key for lookup.
 */
function mapLegacyPythonTransform(
  cleanTransform: string,
  prefixKind?: 'SOURCE' | 'SINK' | 'STEP'
): string {
  const isSink = prefixKind === 'SINK';
  switch (cleanTransform) {
    case 'PubSubIO':
      return isSink ? 'PubSubSink' : 'PubSubSource';
    case 'KinesisIO':
      return isSink ? 'KinesisSink' : 'KinesisSource';
    case 'MqttIO':
      return isSink ? 'MqttSink' : 'MqttSource';
    case 'FirestoreIO':
      return isSink ? 'FirestoreSink' : 'FirestoreSource';
    case 'CassandraIO':
      return isSink ? 'CassandraSink' : 'CassandraSource';
    case 'RedisIO':
      return isSink ? 'RedisSink' : 'RedisSource';
    case 'DatadogIO':
    case 'SplunkIO':
    case 'RequestResponseIO':
      return 'ForeachBatchSink';
    default:
      return cleanTransform;
  }
}

/**
 * Resolves the runtime delivery mechanism, JARs, Maven packages, PyPI dependencies,
 * and Spark properties for a single transform on a specific Spark 4.x target.
 *
 * @param sparkTransform Spark source, sink, or step transform identifier.
 * @param target Target Spark 4.x runtime identifier.
 * @param properties Optional node property bag for dynamic overrides.
 * @returns Complete connector resolution record.
 */
export function resolveConnectorForRuntime(
  sparkTransform: string,
  target: SparkRuntimeTarget,
  properties?: Record<string, unknown>
): ConnectorResolution {
  const { cleanTransform, prefixKind } = parseTransformKey(sparkTransform);
  const profile = RUNTIME_PROFILES[target] ?? RUNTIME_PROFILES.local;
  const managed = isManagedSparkRuntime(profile.id);
  const serverless = profile.id === 'serverless_4.0' || profile.id === 'serverless_3.0';

  // 1. Pre-installed Managed Spark connectors: BigQuery and Iceberg.
  if (
    cleanTransform === 'BigQuerySource' ||
    cleanTransform === 'BigQuerySink' ||
    cleanTransform === 'BigQueryIO'
  ) {
    if (managed) {
      return {
        sparkTransform: cleanTransform,
        mechanism: 'preinstalled',
        gcsJarUris: [],
        mavenCoordinates: [],
        pipPackages: [],
        sparkProperties: {},
        preinstalledArtifact: 'spark-bigquery',
        notes: 'Pre-installed on Managed Spark. Never pass spark-bigquery via Maven packages.',
      };
    }
    return {
      sparkTransform: cleanTransform,
      mechanism: 'maven_package',
      gcsJarUris: [],
      mavenCoordinates: [LOCAL_BIGQUERY_MAVEN],
      pipPackages: [],
      sparkProperties: {},
      notes: 'Uses spark-bigquery Scala 2.13 package for local Spark execution.',
    };
  }

  if (
    cleanTransform === 'IcebergSource' ||
    cleanTransform === 'IcebergSink' ||
    cleanTransform === 'IcebergIO'
  ) {
    const backend = detectCloudStorageBackend(properties);
    if (managed) {
      return {
        sparkTransform: cleanTransform,
        mechanism: 'preinstalled',
        gcsJarUris: [],
        mavenCoordinates: [],
        pipPackages: [],
        sparkProperties: {},
        preinstalledArtifact: 'iceberg-spark-runtime',
        notes: serverless
          ? 'Pre-installed iceberg-spark-runtime with GCSFileIO on Serverless Spark.'
          : 'Pre-installed iceberg-spark-runtime on Dataproc Cluster.',
      };
    }
    if (profile.missingLocalIcebergRuntime) {
      const localMaven: string[] = [];
      if (backend) {
        localMaven.push(getStorageBackendArtifacts(backend).localMavenCoordinate);
      }
      return {
        sparkTransform: cleanTransform,
        mechanism: 'maven_package',
        gcsJarUris: [],
        mavenCoordinates: localMaven,
        pipPackages: [],
        sparkProperties: {},
        missingLocalIcebergRuntime: true,
        notes:
          'Missing local Iceberg runtime for Spark 4.2 because Maven Central lacks iceberg-spark-runtime-4.2_2.13.',
      };
    }
    const localMaven = [LOCAL_ICEBERG_MAVEN];
    if (backend) {
      localMaven.push(getStorageBackendArtifacts(backend).localMavenCoordinate);
    }
    return {
      sparkTransform: cleanTransform,
      mechanism: 'maven_package',
      gcsJarUris: [],
      mavenCoordinates: localMaven,
      pipPackages: [],
      sparkProperties: {},
      notes: 'Uses Apache Iceberg Spark 4.0 Scala 2.13 runtime on local Spark.',
    };
  }

  // 2. Cloud Storage JAR connectors: Spanner, Bigtable, Cloud SQL, AlloyDB, and JDBC.
  if (
    cleanTransform === 'SpannerSource' ||
    cleanTransform === 'SpannerSink' ||
    cleanTransform === 'SpannerIO'
  ) {
    const jarUri = getNonEmptyString(properties, 'jarUri') ?? DEFAULT_SPANNER_GCS_JAR;
    return {
      sparkTransform: cleanTransform,
      mechanism: 'gcs_jar',
      gcsJarUris: [jarUri],
      mavenCoordinates: [],
      pipPackages: [],
      sparkProperties: {},
      notes: 'Attaches the Cloud Spanner Spark 4.0 connector JAR from Cloud Storage.',
    };
  }

  if (
    cleanTransform === 'BigtableSource' ||
    cleanTransform === 'BigtableSink' ||
    cleanTransform === 'BigTableIO' ||
    cleanTransform === 'CloudBigtable'
  ) {
    const jarUri = getNonEmptyString(properties, 'jarUri') ?? DEFAULT_BIGTABLE_GCS_JAR;
    return {
      sparkTransform: cleanTransform,
      mechanism: 'gcs_jar',
      gcsJarUris: [jarUri],
      mavenCoordinates: [],
      pipPackages: [],
      sparkProperties: {},
      notes: 'Attaches the Cloud Bigtable Scala 2.13 connector JAR from Cloud Storage.',
    };
  }

  if (JDBC_FAMILY_TRANSFORMS.has(cleanTransform)) {
    return resolveJdbcFamilyConnector(cleanTransform, properties);
  }

  // 3. Spark-versioned Scala 2.13 Maven connectors: Kafka, Event Hubs, and Avro.
  if (
    cleanTransform === 'KafkaSource' ||
    cleanTransform === 'KafkaSink' ||
    cleanTransform === 'KafkaIO' ||
    cleanTransform === 'EventHubsSource' ||
    cleanTransform === 'EventHubsSink'
  ) {
    return {
      sparkTransform: cleanTransform,
      mechanism: 'maven_package',
      gcsJarUris: [],
      mavenCoordinates: [
        `org.apache.spark:spark-sql-kafka-0-10_2.13:${profile.sparkVersion}`,
      ],
      pipPackages: [],
      sparkProperties: {},
      notes: 'Attaches spark-sql-kafka-0-10 matching the runtime Spark and Scala 2.13 version.',
    };
  }

  if (
    cleanTransform === 'AvroSource' ||
    cleanTransform === 'AvroSink' ||
    cleanTransform === 'AvroIO' ||
    (cleanTransform === 'ParsePayload' &&
      getNonEmptyString(properties, 'format')?.toLowerCase() === 'avro')
  ) {
    const mavenCoordinates = [
      `org.apache.spark:spark-avro_2.13:${profile.sparkVersion}`,
    ];
    const backend = detectCloudStorageBackend(properties);
    let preinstalledArtifact: string | undefined;
    if (backend) {
      const storageArtifacts = getStorageBackendArtifacts(backend);
      if (managed) {
        preinstalledArtifact = storageArtifacts.preinstalledArtifact;
      } else {
        mavenCoordinates.push(storageArtifacts.localMavenCoordinate);
      }
    }
    return {
      sparkTransform: cleanTransform,
      mechanism: 'maven_package',
      gcsJarUris: [],
      mavenCoordinates,
      pipPackages: [],
      sparkProperties: {},
      ...(preinstalledArtifact ? { preinstalledArtifact } : {}),
      notes: 'Attaches spark-avro matching the runtime Spark and Scala 2.13 version.',
    };
  }

  // 4. Fixed-version Scala 2.13 Maven connectors.
  const fixedMaven = FIXED_MAVEN_BY_TRANSFORM[cleanTransform];
  if (fixedMaven) {
    const mavenCoordinates = [fixedMaven];
    let preinstalledArtifact: string | undefined;
    if (STORAGE_AWARE_MAVEN_TRANSFORMS.has(cleanTransform)) {
      const backend = detectCloudStorageBackend(properties);
      if (backend) {
        const storageArtifacts = getStorageBackendArtifacts(backend);
        if (managed) {
          preinstalledArtifact = storageArtifacts.preinstalledArtifact;
        } else {
          mavenCoordinates.push(storageArtifacts.localMavenCoordinate);
        }
      }
    }
    return {
      sparkTransform: cleanTransform,
      mechanism: 'maven_package',
      gcsJarUris: [],
      mavenCoordinates,
      pipPackages: [],
      sparkProperties: {},
      ...(preinstalledArtifact ? { preinstalledArtifact } : {}),
      notes: 'Attaches Scala 2.13 Spark 4.x connector package from Maven Central.',
    };
  }

  // 5. Python 4.0 DataSource API and foreachBatch SDK connectors.
  if (cleanTransform === 'VectorStoreSink') {
    return resolveVectorStoreConnector(cleanTransform, properties);
  }

  const mappedPythonTransform = mapLegacyPythonTransform(cleanTransform, prefixKind);
  const pythonSpec = PYTHON_CONNECTOR_BY_TRANSFORM[mappedPythonTransform];
  if (pythonSpec) {
    return {
      sparkTransform: cleanTransform,
      mechanism: pythonSpec.mechanism,
      gcsJarUris: [],
      mavenCoordinates: [],
      pipPackages: [pythonSpec.pipPackage],
      sparkProperties: {},
      ...(pythonSpec.pythonDataSourceClass
        ? { pythonDataSourceClass: pythonSpec.pythonDataSourceClass }
        : {}),
      notes: pythonSpec.notes,
    };
  }

  // 6. Spark ML, Vertex AI, and RAG text embedding step connectors.
  if (cleanTransform === 'TextChunkAndEmbed') {
    const mode = getNonEmptyString(properties, 'mode')?.toLowerCase();
    if (mode === 'chunk_only') {
      return {
        sparkTransform: cleanTransform,
        mechanism: 'builtin',
        gcsJarUris: [],
        mavenCoordinates: [],
        pipPackages: [],
        sparkProperties: {},
        notes: 'Splits text chunks with pure Python inside mapInPandas without external embedding SDK calls.',
      };
    }
    return {
      sparkTransform: cleanTransform,
      mechanism: 'foreach_batch_sdk',
      gcsJarUris: [],
      mavenCoordinates: [],
      pipPackages: ['google-genai'],
      sparkProperties: {},
      notes: 'Generates batched Vertex AI text embeddings with google-genai inside mapInPandas.',
    };
  }

  if (
    cleanTransform === 'SparkMlAndVertexAi' ||
    cleanTransform === 'SparkMlInference' ||
    cleanTransform === 'VertexAiInference'
  ) {
    return resolveMlAndVertexConnector(cleanTransform, properties);
  }

  // 7. Built-in file format nodes with optional cloud storage backends.
  if (BUILTIN_FILE_FORMAT_TRANSFORMS.has(cleanTransform)) {
    const backend = detectCloudStorageBackend(properties);
    if (backend) {
      const storageArtifacts = getStorageBackendArtifacts(backend);
      if (managed) {
        return {
          sparkTransform: cleanTransform,
          mechanism: 'preinstalled',
          gcsJarUris: [],
          mavenCoordinates: [],
          pipPackages: [],
          sparkProperties: {},
          preinstalledArtifact: storageArtifacts.preinstalledArtifact,
          notes: `Uses built-in Spark 4.x file format with pre-installed ${storageArtifacts.preinstalledArtifact}.`,
        };
      }
      return {
        sparkTransform: cleanTransform,
        mechanism: 'maven_package',
        gcsJarUris: [],
        mavenCoordinates: [storageArtifacts.localMavenCoordinate],
        pipPackages: [],
        sparkProperties: {},
        notes: 'Uses built-in Spark 4.x file format with cloud storage connector for local execution.',
      };
    }
  }

  // 8. Built-in Spark 4.x sources, sinks, and DataFrame transformations.
  return {
    sparkTransform: cleanTransform,
    mechanism: 'builtin',
    gcsJarUris: [],
    mavenCoordinates: [],
    pipPackages: [],
    sparkProperties: {},
    notes: 'Provided natively by Apache Spark 4.x.',
  };
}

/**
 * Extracts custom JAR URIs from pipeline-level job configuration when provided.
 *
 * @param jobConfig Optional pipeline job configuration map.
 * @returns Trimmed non-empty JAR URIs.
 */
function extractJobConfigJars(jobConfig?: Record<string, unknown>): string[] {
  if (!jobConfig) {
    return [];
  }
  const results: string[] = [];
  for (const key of ['customJarUris', 'jars']) {
    const raw = jobConfig[key];
    if (Array.isArray(raw)) {
      for (const item of raw) {
        if (typeof item === 'string' && item.trim().length > 0) {
          results.push(item.trim());
        }
      }
    } else if (typeof raw === 'string' && raw.trim().length > 0) {
      for (const part of raw.split(',')) {
        if (part.trim().length > 0) {
          results.push(part.trim());
        }
      }
    }
  }
  return results;
}

/**
 * Resolves and deduplicates all runtime JARs, Maven packages, PyPI dependencies,
 * Spark properties, Python DataSource classes, and pre-installed connectors for a pipeline.
 *
 * @param nodes Pipeline nodes containing Spark transform identifiers and property bags.
 * @param jobConfig Optional pipeline deployment and runtime configuration.
 * @returns Aggregated runtime artifact plan.
 */
export function resolvePipelineRuntimeArtifacts(
  nodes: Array<{ sparkTransform?: string; properties?: Record<string, unknown> }>,
  jobConfig?: Record<string, unknown>
): PipelineRuntimeArtifacts {
  const rawRunner =
    typeof jobConfig?.runner === 'string' ? jobConfig.runner : undefined;
  const rawRuntimeVersion =
    typeof jobConfig?.runtimeVersion === 'string'
      ? jobConfig.runtimeVersion
      : typeof jobConfig?.version === 'string'
        ? jobConfig.version
        : typeof jobConfig?.imageVersion === 'string'
          ? jobConfig.imageVersion
          : undefined;
  const rawTier =
    typeof jobConfig?.tier === 'string'
      ? jobConfig.tier
      : typeof jobConfig?.serverlessTier === 'string'
        ? jobConfig.serverlessTier
        : undefined;

  const profile = resolveRuntimeProfile(rawRunner, rawRuntimeVersion);
  const tier = resolveServerlessTier(rawTier);

  const gcsJars: string[] = [];
  const seenGcsJars = new Set<string>();

  const mavenPackages: string[] = [];
  const seenMaven = new Set<string>();

  const pipPackages: string[] = [];
  const seenPip = new Set<string>();

  const pythonDataSources: string[] = [];
  const seenPythonDataSources = new Set<string>();

  const preinstalledConnectors: string[] = [];
  const seenPreinstalled = new Set<string>();

  const sparkProperties: Record<string, string> = {};
  let missingLocalIcebergRuntime = false;

  for (const rawNode of nodes as PipelineRuntimeNodeInput[]) {
    const transform =
      rawNode.sparkTransform ??
      rawNode.spark_transform ??
      rawNode.data?.sparkTransform ??
      rawNode.data?.spark_transform ??
      '';
    if (!transform || transform.trim().length === 0) {
      continue;
    }
    const nodeProperties = rawNode.properties ?? rawNode.data?.properties;
    const resolution = resolveConnectorForRuntime(transform, profile.id, nodeProperties);

    pushUnique(gcsJars, seenGcsJars, resolution.gcsJarUris);
    pushUnique(mavenPackages, seenMaven, resolution.mavenCoordinates);
    pushUnique(pipPackages, seenPip, resolution.pipPackages);

    if (resolution.missingLocalIcebergRuntime) {
      missingLocalIcebergRuntime = true;
    }

    if (resolution.pythonDataSourceClass) {
      pushUnique(pythonDataSources, seenPythonDataSources, [
        resolution.pythonDataSourceClass,
      ]);
    }
    if (resolution.preinstalledArtifact) {
      pushUnique(preinstalledConnectors, seenPreinstalled, [
        resolution.preinstalledArtifact,
      ]);
    }

    for (const [propKey, propVal] of Object.entries(resolution.sparkProperties)) {
      sparkProperties[propKey] = propVal;
    }
  }

  // Include pipeline-level custom GCS JARs if configured.
  pushUnique(gcsJars, seenGcsJars, extractJobConfigJars(jobConfig));

  // Include driver pip packages required by SDP runtimes when running in SDP mode.
  if (jobConfig?.paradigm === 'sdp' && profile.driverPipRequirements) {
    pushUnique(pipPackages, seenPip, profile.driverPipRequirements);
  }

  // Include google-cloud-spark-connect when Local mode enables Spark Connect.
  if (
    (profile.id === 'local' || profile.id === 'local_4.2_connect') &&
    jobConfig?.useSparkConnect === true
  ) {
    pushUnique(pipPackages, seenPip, ['google-cloud-spark-connect']);
  }

  return {
    profile,
    tier,
    gcsJars,
    mavenPackages,
    pipPackages,
    sparkProperties,
    pythonDataSources,
    preinstalledConnectors,
    ...(missingLocalIcebergRuntime ? { missingLocalIcebergRuntime: true } : {}),
  };
}
