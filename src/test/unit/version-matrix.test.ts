/**
 * @fileoverview Unit tests for the Spark 4.x runtime version and connector matrix.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies runtime profile mapping, Serverless tier defaulting, Managed Spark
 * pre-installed classpath exclusions, Cloud Storage JAR resolution, Conscrypt
 * removal properties, Scala 2.13 Maven coordinates, Python 4.0 DataSource
 * fallbacks, and pipeline-level dependency deduplication.
 */

import { describe, it, expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import {
  DEFAULT_SERVERLESS_TIER,
  RUNTIME_PROFILES,
  isManagedSparkRuntime,
  resolveServerlessTier,
  resolveRuntimeProfile,
  resolveConnectorForRuntime,
  resolvePipelineRuntimeArtifacts,
} from '../../spark/versionMatrix';
import type { SparkRuntimeTarget } from '../../spark/versionMatrix';

const ALL_PHASE_5_SOURCES = [
  'KafkaSource',
  'PubSubSource',
  'KinesisSource',
  'PulsarSource',
  'EventHubsSource',
  'MqttSource',
  'RateSource',
  'SocketSource',
  'IcebergSource',
  'DeltaSource',
  'HudiSource',
  'HiveSource',
  'ParquetSource',
  'OrcSource',
  'AvroSource',
  'CsvSource',
  'JsonSource',
  'TextSource',
  'XmlSource',
  'BinaryFileSource',
  'TFRecordSource',
  'BigQuerySource',
  'SpannerSource',
  'BigtableSource',
  'CloudSqlSource',
  'AlloyDbSource',
  'JdbcSource',
  'SnowflakeSource',
  'RedshiftSource',
  'ClickHouseSource',
  'SingleStoreSource',
  'MongoDbSource',
  'FirestoreSource',
  'CassandraSource',
  'RedisSource',
  'ElasticsearchSource',
  'Neo4jSource',
  'CreateDataFrameSource',
  'RestApiSource',
  'SftpSource',
] as const;

const ALL_PHASE_6_SINKS = [
  'KafkaSink',
  'PubSubSink',
  'KinesisSink',
  'PulsarSink',
  'EventHubsSink',
  'MqttSink',
  'IcebergSink',
  'DeltaSink',
  'HudiSink',
  'HiveSink',
  'ParquetSink',
  'OrcSink',
  'AvroSink',
  'CsvSink',
  'JsonSink',
  'TextSink',
  'XmlSink',
  'TFRecordSink',
  'BigQuerySink',
  'SpannerSink',
  'BigtableSink',
  'CloudSqlSink',
  'AlloyDbSink',
  'JdbcSink',
  'SnowflakeSink',
  'RedshiftSink',
  'ClickHouseSink',
  'SingleStoreSink',
  'MongoDbSink',
  'FirestoreSink',
  'CassandraSink',
  'RedisSink',
  'ElasticsearchSink',
  'Neo4jSink',
  'ConsoleSink',
  'MemorySink',
  'ForeachBatchSink',
  'VectorStoreSink',
] as const;

describe('versionMatrix module purity and webview shim', () => {
  it('keeps src/spark/versionMatrix.ts free of host and browser imports', () => {
    const matrixPath = path.resolve(__dirname, '../../spark/versionMatrix.ts');
    const source = fs.readFileSync(matrixPath, 'utf-8');
    const forbidden = ['vscode', 'fs', 'path', 'child_process', 'os', 'react'];
    const importRegex = /(?:from\s+|import\s*\(|require\s*\()\s*['"]([^'"]+)['"]/g;

    const violations: string[] = [];
    let match: RegExpExecArray | null;
    while ((match = importRegex.exec(source)) !== null) {
      const mod = match[1];
      if (forbidden.includes(mod) || mod.startsWith('node:')) {
        violations.push(mod);
      }
    }
    expect(violations).toEqual([]);
  });

  it('re-exports canonical module from webview/src/utils/versionMatrix.ts', () => {
    const shimPath = path.resolve(
      __dirname,
      '../../../webview/src/utils/versionMatrix.ts'
    );
    const shimSource = fs.readFileSync(shimPath, 'utf-8');
    expect(shimSource).toContain("export * from '../../../src/spark/versionMatrix';");
  });
});

describe('resolveServerlessTier and tier defaults', () => {
  it('defaults DEFAULT_SERVERLESS_TIER to standard and never defaults silently to premium', () => {
    expect(DEFAULT_SERVERLESS_TIER).toBe('standard');
    expect(resolveServerlessTier()).toBe('standard');
    expect(resolveServerlessTier(undefined)).toBe('standard');
    expect(resolveServerlessTier('')).toBe('standard');
    expect(resolveServerlessTier('   ')).toBe('standard');
    expect(resolveServerlessTier('standard')).toBe('standard');
    expect(resolveServerlessTier('STANDARD')).toBe('standard');
    expect(resolveServerlessTier('auto')).toBe('standard');
    expect(resolveServerlessTier('lightning')).toBe('standard');
    expect(resolveServerlessTier('enterprise')).toBe('standard');
  });

  it('returns premium only when tier is explicitly premium', () => {
    expect(resolveServerlessTier('premium')).toBe('premium');
    expect(resolveServerlessTier('PREMIUM')).toBe('premium');
    expect(resolveServerlessTier('  premium  ')).toBe('premium');
  });
});

describe('resolveRuntimeProfile', () => {
  it('resolves ServerlessSpark 4.0 by default and when version is 4.0', () => {
    for (const version of [undefined, '', '4.0', '4.0 (Spark 4.2)']) {
      const profile = resolveRuntimeProfile('ServerlessSpark', version);
      expect(profile).toEqual({
        id: 'serverless_4.0',
        runner: 'ServerlessSpark',
        runtimeVersion: '4.0',
        sparkVersion: '4.2.0',
        pythonVersion: '3.13',
        javaVersion: '25',
        scalaVersion: '2.13',
        defaultTier: 'standard',
        preinstalledConnectors: [
          'spark-bigquery',
          'iceberg-spark-runtime',
          'gcs-connector',
          'hadoop-aws',
          'hadoop-azure',
        ],
      });
    }
  });

  it('resolves ServerlessSpark 3.0 when runtimeVersion is 3.0', () => {
    for (const version of ['3.0', '3.0 (Spark 4.0)', 'serverless_3.0']) {
      const profile = resolveRuntimeProfile('ServerlessSpark', version);
      expect(profile).toEqual({
        id: 'serverless_3.0',
        runner: 'ServerlessSpark',
        runtimeVersion: '3.0',
        sparkVersion: '4.0.1',
        pythonVersion: '3.12',
        javaVersion: '21',
        scalaVersion: '2.13',
        defaultTier: 'standard',
        preinstalledConnectors: [
          'spark-bigquery',
          'iceberg-spark-runtime',
          'gcs-connector',
          'hadoop-aws',
          'hadoop-azure',
        ],
      });
    }
  });

  it('resolves DataprocCluster 3.0 with Spark 4.1.2, Python 3.12, Java 21, and Scala 2.13', () => {
    for (const version of [undefined, '3.0']) {
      const profile = resolveRuntimeProfile('DataprocCluster', version);
      expect(profile).toEqual({
        id: 'cluster_3.0',
        runner: 'DataprocCluster',
        runtimeVersion: '3.0',
        sparkVersion: '4.1.2',
        pythonVersion: '3.12',
        javaVersion: '21',
        scalaVersion: '2.13',
        preinstalledConnectors: [
          'spark-bigquery',
          'iceberg-spark-runtime',
          'gcs-connector',
          'hadoop-aws',
          'hadoop-azure',
        ],
      });
      expect(profile.defaultTier).toBeUndefined();
    }
  });

  it('resolves Local profile for Local, DirectRunner, omitted, or unknown runners', () => {
    for (const runner of [undefined, '', 'Local', 'DirectRunner', 'UnknownRunner']) {
      const profile = resolveRuntimeProfile(runner);
      expect(profile).toEqual({
        id: 'local',
        runner: 'Local',
        runtimeVersion: 'local',
        sparkVersion: '4.0.1',
        pythonVersion: '3.13',
        javaVersion: '21',
        scalaVersion: '2.13',
        preinstalledConnectors: [],
      });
      expect(profile.defaultTier).toBeUndefined();
    }
  });

  it('supports direct target shorthands and returns defensive copies', () => {
    expect(resolveRuntimeProfile('serverless_4.0').id).toBe('serverless_4.0');
    expect(resolveRuntimeProfile('serverless_3.0').id).toBe('serverless_3.0');
    expect(resolveRuntimeProfile('cluster_3.0').id).toBe('cluster_3.0');
    expect(resolveRuntimeProfile('local').id).toBe('local');
    expect(resolveRuntimeProfile('serverless').id).toBe('serverless_4.0');
    expect(resolveRuntimeProfile('dataproc').id).toBe('cluster_3.0');

    const copy = resolveRuntimeProfile('ServerlessSpark', '4.0');
    copy.preinstalledConnectors.push('mutated');
    expect(RUNTIME_PROFILES['serverless_4.0'].preinstalledConnectors).not.toContain(
      'mutated'
    );

    expect(isManagedSparkRuntime('serverless_4.0')).toBe(true);
    expect(isManagedSparkRuntime('serverless_3.0')).toBe(true);
    expect(isManagedSparkRuntime('cluster_3.0')).toBe(true);
    expect(isManagedSparkRuntime('local')).toBe(false);
  });
});

describe('Managed Spark pre-installed connectors vs local Maven inclusion', () => {
  const managedTargets: SparkRuntimeTarget[] = [
    'serverless_4.0',
    'serverless_3.0',
    'cluster_3.0',
  ];

  it('marks BigQuerySource and BigQuerySink preinstalled on Managed Spark with zero Maven coords', () => {
    for (const target of managedTargets) {
      for (const transform of ['BigQuerySource', 'BigQuerySink', 'BigQueryIO']) {
        const res = resolveConnectorForRuntime(transform, target);
        expect(res.mechanism).toBe('preinstalled');
        expect(res.preinstalledArtifact).toBe('spark-bigquery');
        expect(res.mavenCoordinates).toEqual([]);
        expect(res.gcsJarUris).toEqual([]);
      }
    }

    const localRes = resolveConnectorForRuntime('BigQuerySource', 'local');
    expect(localRes.mechanism).toBe('maven_package');
    expect(localRes.mavenCoordinates).toHaveLength(1);
    expect(localRes.mavenCoordinates[0]).toContain('spark-bigquery');
    expect(localRes.mavenCoordinates[0]).toContain('_2.13');
  });

  it('marks IcebergSource and IcebergSink preinstalled on Managed Spark and emits Iceberg 4.0_2.13 on local', () => {
    for (const target of managedTargets) {
      for (const transform of ['IcebergSource', 'IcebergSink', 'IcebergIO']) {
        const res = resolveConnectorForRuntime(transform, target);
        expect(res.mechanism).toBe('preinstalled');
        expect(res.preinstalledArtifact).toBe('iceberg-spark-runtime');
        expect(res.mavenCoordinates).toEqual([]);
        if (target !== 'cluster_3.0') {
          expect(res.notes).toContain('GCSFileIO');
        }
      }
    }

    const localMinor = RUNTIME_PROFILES.local.sparkVersion.split('.').slice(0, 2).join('.');
    expect(localMinor).toBe('4.0');
    expect(
      resolveConnectorForRuntime('IcebergSink', 'local').mavenCoordinates[0]
    ).toBe(`org.apache.iceberg:iceberg-spark-runtime-${localMinor}_2.13:1.10.1`);

    const localIceberg = resolveConnectorForRuntime('IcebergSource', 'local', {
      warehouse: 'gs://my-lakehouse/warehouse',
    });
    expect(localIceberg.mechanism).toBe('maven_package');
    expect(localIceberg.mavenCoordinates[0]).toContain(
      'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13'
    );
    expect(localIceberg.mavenCoordinates).toContain(
      'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25'
    );
  });

  it('resolves cloud storage backends on built-in file formats as preinstalled on Managed Spark and Maven on local', () => {
    const cases: Array<{
      props: Record<string, unknown>;
      expectedArtifact: string;
      expectedLocalCoordFragment: string;
    }> = [
      {
        props: { path: 'gs://analytics-bucket/events/' },
        expectedArtifact: 'gcs-connector',
        expectedLocalCoordFragment: 'gcs-connector',
      },
      {
        props: { storageBackend: 's3a', uri: 's3a://raw-bucket/logs/' },
        expectedArtifact: 'hadoop-aws',
        expectedLocalCoordFragment: 'hadoop-aws',
      },
      {
        props: { path: 'abfss://container@acct.dfs.core.windows.net/data' },
        expectedArtifact: 'hadoop-azure',
        expectedLocalCoordFragment: 'hadoop-azure',
      },
      {
        props: { storageBackend: 'azure' },
        expectedArtifact: 'hadoop-azure',
        expectedLocalCoordFragment: 'hadoop-azure',
      },
      {
        props: { storageBackend: 'gcs' },
        expectedArtifact: 'gcs-connector',
        expectedLocalCoordFragment: 'gcs-connector',
      },
      {
        props: { uri: 'wasbs://container@acct.blob.core.windows.net/data' },
        expectedArtifact: 'hadoop-azure',
        expectedLocalCoordFragment: 'hadoop-azure',
      },
    ];

    for (const testCase of cases) {
      for (const target of managedTargets) {
        const managedRes = resolveConnectorForRuntime(
          'ParquetSource',
          target,
          testCase.props
        );
        expect(managedRes.mechanism).toBe('preinstalled');
        expect(managedRes.preinstalledArtifact).toBe(testCase.expectedArtifact);
        expect(managedRes.mavenCoordinates).toEqual([]);
      }

      const localRes = resolveConnectorForRuntime(
        'ParquetSource',
        'local',
        testCase.props
      );
      expect(localRes.mechanism).toBe('maven_package');
      expect(localRes.mavenCoordinates[0]).toContain(
        testCase.expectedLocalCoordFragment
      );
    }

    // Local or HDFS paths on built-in formats remain pure builtin across all runtimes.
    for (const props of [undefined, {}, { path: '/tmp/data' }, { path: 'hdfs://nn/data' }]) {
      expect(resolveConnectorForRuntime('XmlSource', 'serverless_4.0', props).mechanism).toBe(
        'builtin'
      );
      expect(resolveConnectorForRuntime('OrcSink', 'local', props).mechanism).toBe(
        'builtin'
      );
    }
  });
});

describe('GCS JAR connectors and Conscrypt TLS properties', () => {
  it('resolves Spanner and Bigtable default GCS JARs and honors custom jarUri overrides', () => {
    const spanner = resolveConnectorForRuntime('SpannerSource', 'serverless_4.0');
    expect(spanner.mechanism).toBe('gcs_jar');
    expect(spanner.gcsJarUris).toEqual([
      'gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar',
    ]);

    const spannerSinkCustom = resolveConnectorForRuntime('SpannerSink', 'cluster_3.0', {
      jarUri: 'gs://custom-bucket/jars/spanner-custom.jar',
    });
    expect(spannerSinkCustom.gcsJarUris).toEqual([
      'gs://custom-bucket/jars/spanner-custom.jar',
    ]);

    const bigtable = resolveConnectorForRuntime('BigtableSource', 'serverless_4.0');
    expect(bigtable.mechanism).toBe('gcs_jar');
    expect(bigtable.gcsJarUris).toEqual([
      'gs://spark-lib/bigtable/spark-bigtable_2.13-0.1.0.jar',
    ]);

    const bigtableCustom = resolveConnectorForRuntime('BigtableSink', 'serverless_3.0', {
      jarUri: 'gs://custom-bucket/jars/bigtable-custom.jar',
    });
    expect(bigtableCustom.gcsJarUris).toEqual([
      'gs://custom-bucket/jars/bigtable-custom.jar',
    ]);
  });

  it('attaches driver and socket factory JARs and sets dataproc.artifacts.remove=conscrypt for AlloyDB and SQL Server', () => {
    const alloySource = resolveConnectorForRuntime('AlloyDbSource', 'serverless_4.0', {
      driverJarUri: 'gs://my-jars/postgresql-42.7.4.jar',
      socketFactoryJarUri: 'gs://my-jars/alloydb-jdbc-connector-1.2.0-jar-with-dependencies.jar',
    });
    expect(alloySource.mechanism).toBe('gcs_jar');
    expect(alloySource.gcsJarUris).toEqual([
      'gs://my-jars/postgresql-42.7.4.jar',
      'gs://my-jars/alloydb-jdbc-connector-1.2.0-jar-with-dependencies.jar',
    ]);
    expect(alloySource.sparkProperties).toEqual({
      'dataproc.artifacts.remove': 'conscrypt',
    });

    const alloySink = resolveConnectorForRuntime('AlloyDbSink', 'cluster_3.0');
    expect(alloySink.sparkProperties).toEqual({
      'dataproc.artifacts.remove': 'conscrypt',
    });

    const sqlServerJdbc = resolveConnectorForRuntime('JdbcSource', 'serverless_4.0', {
      engine: 'sqlserver',
      driverJarUri: 'gs://my-jars/mssql-jdbc-12.8.1.jre11.jar',
    });
    expect(sqlServerJdbc.gcsJarUris).toEqual([
      'gs://my-jars/mssql-jdbc-12.8.1.jre11.jar',
    ]);
    expect(sqlServerJdbc.sparkProperties).toEqual({
      'dataproc.artifacts.remove': 'conscrypt',
    });

    const sqlServerByUrl = resolveConnectorForRuntime('JdbcSink', 'cluster_3.0', {
      url: 'jdbc:sqlserver://10.0.0.5:1433;databaseName=sales',
    });
    expect(sqlServerByUrl.sparkProperties).toEqual({
      'dataproc.artifacts.remove': 'conscrypt',
    });

    const postgresCloudSql = resolveConnectorForRuntime(
      'CloudSqlSource',
      'serverless_4.0',
      {
        engine: 'postgres',
        driverJarUri: 'gs://my-jars/postgresql-42.7.4.jar',
        socketFactoryJarUri: 'gs://my-jars/postgres-socket-factory-1.21.0.jar',
      }
    );
    expect(postgresCloudSql.gcsJarUris).toHaveLength(2);
    expect(postgresCloudSql.sparkProperties).toEqual({});
  });
});

describe('Scala 2.13 Maven coordinates', () => {
  it('aligns spark-sql-kafka and spark-avro versions with each runtime profile Spark version', () => {
    expect(resolveConnectorForRuntime('KafkaSource', 'serverless_4.0').mavenCoordinates).toEqual([
      'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0',
    ]);
    expect(
      resolveConnectorForRuntime('EventHubsSink', 'serverless_3.0').mavenCoordinates
    ).toEqual(['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1']);
    expect(resolveConnectorForRuntime('KafkaSink', 'cluster_3.0').mavenCoordinates).toEqual([
      'org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2',
    ]);

    expect(resolveConnectorForRuntime('AvroSource', 'serverless_4.0', {
      path: 'gs://my-bucket/events.avro',
    })).toMatchObject({
      mechanism: 'maven_package',
      mavenCoordinates: ['org.apache.spark:spark-avro_2.13:4.2.0'],
      preinstalledArtifact: 'gcs-connector',
    });

    expect(
      resolveConnectorForRuntime('AvroSink', 'local', {
        path: 's3a://my-bucket/events.avro',
      }).mavenCoordinates
    ).toEqual([
      'org.apache.spark:spark-avro_2.13:4.0.1',
      'org.apache.hadoop:hadoop-aws:3.4.1',
    ]);

    expect(
      resolveConnectorForRuntime('ParsePayload', 'cluster_3.0', { format: 'avro' })
        .mavenCoordinates
    ).toEqual(['org.apache.spark:spark-avro_2.13:4.1.2']);
    expect(
      resolveConnectorForRuntime('ParsePayload', 'cluster_3.0', { format: 'json' })
        .mechanism
    ).toBe('builtin');
  });

  it('maps all fixed Spark 4.x Scala 2.13 Maven connectors accurately', () => {
    const expectedMaven: Array<[string, string]> = [
      ['DeltaSource', 'io.delta:delta-spark_2.13:4.0.0'],
      ['DeltaSink', 'io.delta:delta-spark_2.13:4.0.0'],
      ['HudiSource', 'org.apache.hudi:hudi-spark4.0-bundle_2.13:1.0.1'],
      ['HudiSink', 'org.apache.hudi:hudi-spark4.0-bundle_2.13:1.0.1'],
      ['PulsarSource', 'io.streamnative.connectors:pulsar-spark-connector_2.13:4.0.0.1'],
      ['PulsarSink', 'io.streamnative.connectors:pulsar-spark-connector_2.13:4.0.0.1'],
      ['SnowflakeSource', 'net.snowflake:spark-snowflake_2.13:3.1.0'],
      ['SnowflakeSink', 'net.snowflake:spark-snowflake_2.13:3.1.0'],
      [
        'RedshiftSource',
        'io.github.spark-redshift-community:spark-redshift_2.13:6.3.0-spark_4.0',
      ],
      [
        'RedshiftSink',
        'io.github.spark-redshift-community:spark-redshift_2.13:6.3.0-spark_4.0',
      ],
      ['MongoDbSource', 'org.mongodb.spark:mongo-spark-connector_2.13:10.4.1'],
      ['MongoDbSink', 'org.mongodb.spark:mongo-spark-connector_2.13:10.4.1'],
      ['ElasticsearchSource', 'org.elasticsearch:elasticsearch-spark-40_2.13:8.17.0'],
      ['ElasticsearchSink', 'org.elasticsearch:elasticsearch-spark-40_2.13:8.17.0'],
      [
        'Neo4jSource',
        'org.neo4j:neo4j-connector-apache-spark_2.13:5.3.4_for_spark_4',
      ],
      [
        'Neo4jSink',
        'org.neo4j:neo4j-connector-apache-spark_2.13:5.3.4_for_spark_4',
      ],
      [
        'ClickHouseSource',
        'com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.8.1',
      ],
      [
        'ClickHouseSink',
        'com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.8.1',
      ],
      [
        'SingleStoreSource',
        'com.singlestore:singlestore-spark-connector_2.13:4.1.9-spark-4.0.0',
      ],
      [
        'SingleStoreSink',
        'com.singlestore:singlestore-spark-connector_2.13:4.1.9-spark-4.0.0',
      ],
      ['TFRecordSource', 'com.linkedin.sparktfrecord:spark-tfrecord_2.13:0.7.0'],
      ['TFRecordSink', 'com.linkedin.sparktfrecord:spark-tfrecord_2.13:0.7.0'],
    ];

    for (const [transform, coord] of expectedMaven) {
      const res = resolveConnectorForRuntime(transform, 'serverless_4.0');
      expect(res.mechanism).toBe('maven_package');
      expect(res.mavenCoordinates).toEqual([coord]);
    }

    // Verify storage backend augmentation on storage-aware Maven connectors.
    const deltaGcsManaged = resolveConnectorForRuntime('DeltaSink', 'serverless_4.0', {
      path: 'gs://lakehouse/delta-events',
    });
    expect(deltaGcsManaged.mavenCoordinates).toEqual([
      'io.delta:delta-spark_2.13:4.0.0',
    ]);
    expect(deltaGcsManaged.preinstalledArtifact).toBe('gcs-connector');

    const deltaGcsLocal = resolveConnectorForRuntime('DeltaSink', 'local', {
      path: 'gs://lakehouse/delta-events',
    });
    expect(deltaGcsLocal.mavenCoordinates).toEqual([
      'io.delta:delta-spark_2.13:4.0.0',
      'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25',
    ]);
  });
});

describe('Python 4.0 DataSource API and foreachBatch SDK fallbacks', () => {
  it('maps Python DataSource readers and foreachBatch SDK writers to PyPI packages', () => {
    const sourceExpectations: Array<[string, string, string]> = [
      ['PubSubSource', 'google-cloud-pubsub', 'PubSubDataSource'],
      ['KinesisSource', 'boto3', 'KinesisDataSource'],
      ['MqttSource', 'paho-mqtt', 'MqttDataSource'],
      ['FirestoreSource', 'google-cloud-firestore', 'FirestoreDataSource'],
      ['CassandraSource', 'cassandra-driver', 'CassandraDataSource'],
      ['RedisSource', 'redis', 'RedisDataSource'],
      ['RestApiSource', 'requests', 'RestApiDataSource'],
      ['SftpSource', 'paramiko', 'SftpDataSource'],
    ];

    for (const [transform, pkg, cls] of sourceExpectations) {
      const res = resolveConnectorForRuntime(transform, 'serverless_4.0');
      expect(res.mechanism).toBe('python_datasource');
      expect(res.pipPackages).toEqual([pkg]);
      expect(res.pythonDataSourceClass).toBe(cls);
      expect(res.mavenCoordinates).toEqual([]);
      expect(res.gcsJarUris).toEqual([]);
    }

    const sinkExpectations: Array<[string, string]> = [
      ['PubSubSink', 'google-cloud-pubsub'],
      ['KinesisSink', 'boto3'],
      ['MqttSink', 'paho-mqtt'],
      ['FirestoreSink', 'google-cloud-firestore'],
      ['CassandraSink', 'cassandra-driver'],
      ['RedisSink', 'redis'],
      ['ForeachBatchSink', 'requests'],
    ];

    for (const [transform, pkg] of sinkExpectations) {
      const res = resolveConnectorForRuntime(transform, 'serverless_4.0');
      expect(res.mechanism).toBe('foreach_batch_sdk');
      expect(res.pipPackages).toEqual([pkg]);
      expect(res.mavenCoordinates).toEqual([]);
      expect(res.gcsJarUris).toEqual([]);
    }
  });

  it('resolves VectorStoreSink for both AlloyDB pgvector and Vertex AI Vector Search backends', () => {
    const defaultAlloyDb = resolveConnectorForRuntime('VectorStoreSink', 'serverless_4.0');
    expect(defaultAlloyDb).toMatchObject({
      sparkTransform: 'VectorStoreSink',
      mechanism: 'foreach_batch_sdk',
      gcsJarUris: [],
      mavenCoordinates: [],
      pipPackages: ['psycopg[binary]', 'google-cloud-alloydb-connector'],
      sparkProperties: { 'dataproc.artifacts.remove': 'conscrypt' },
    });

    const alloyDbWithJars = resolveConnectorForRuntime('SINK:VectorStoreSink', 'cluster_3.0', {
      backend: 'alloydb_pgvector',
      driverJarUri: 'gs://jars/postgresql-42.7.4.jar',
      socketFactoryJarUri: 'gs://jars/alloydb-jdbc-connector-1.2.0.jar',
    });
    expect(alloyDbWithJars.mechanism).toBe('foreach_batch_sdk');
    expect(alloyDbWithJars.pipPackages).toEqual(['psycopg[binary]', 'google-cloud-alloydb-connector']);
    expect(alloyDbWithJars.gcsJarUris).toEqual([
      'gs://jars/postgresql-42.7.4.jar',
      'gs://jars/alloydb-jdbc-connector-1.2.0.jar',
    ]);
    expect(alloyDbWithJars.sparkProperties).toEqual({
      'dataproc.artifacts.remove': 'conscrypt',
    });

    const vertexVector = resolveConnectorForRuntime('VectorStoreSink', 'serverless_4.0', {
      backend: 'vertex_vector_search',
      driverJarUri: 'gs://jars/ignored.jar',
    });
    expect(vertexVector).toMatchObject({
      sparkTransform: 'VectorStoreSink',
      mechanism: 'foreach_batch_sdk',
      gcsJarUris: [],
      mavenCoordinates: [],
      pipPackages: ['google-cloud-aiplatform'],
      sparkProperties: {},
    });
  });

  it('resolves SparkMlAndVertexAi packages based on configuration', () => {
    expect(
      resolveConnectorForRuntime('SparkMlAndVertexAi', 'serverless_4.0', {
        modelType: 'xgboost',
      })
    ).toMatchObject({
      mechanism: 'foreach_batch_sdk',
      pipPackages: ['xgboost'],
    });

    expect(
      resolveConnectorForRuntime('SparkMlAndVertexAi', 'serverless_4.0', {
        provider: 'vertex_ai',
      })
    ).toMatchObject({
      mechanism: 'foreach_batch_sdk',
      pipPackages: ['google-cloud-aiplatform'],
    });

    expect(
      resolveConnectorForRuntime('SparkMlAndVertexAi', 'serverless_4.0', {
        useXgboost: true,
        endpointId: 'projects/p/locations/us-central1/endpoints/123',
      }).pipPackages
    ).toEqual(['xgboost', 'google-cloud-aiplatform']);

    expect(
      resolveConnectorForRuntime('VertexAiInference', 'serverless_4.0').pipPackages
    ).toEqual(['google-cloud-aiplatform']);

    expect(
      resolveConnectorForRuntime('SparkMlInference', 'serverless_4.0', {
        mode: 'pipeline_model',
      })
    ).toMatchObject({
      mechanism: 'builtin',
      pipPackages: [],
    });

    expect(
      resolveConnectorForRuntime('TextChunkAndEmbed', 'serverless_4.0')
    ).toMatchObject({
      sparkTransform: 'TextChunkAndEmbed',
      mechanism: 'foreach_batch_sdk',
      pipPackages: ['google-genai'],
      gcsJarUris: [],
      mavenCoordinates: [],
    });

    expect(
      resolveConnectorForRuntime('STEP:TextChunkAndEmbed', 'cluster_3.0', {
        mode: 'embed_only',
      })
    ).toMatchObject({
      sparkTransform: 'TextChunkAndEmbed',
      mechanism: 'foreach_batch_sdk',
      pipPackages: ['google-genai'],
    });

    expect(
      resolveConnectorForRuntime('TextChunkAndEmbed', 'local', {
        mode: 'chunk_only',
      })
    ).toMatchObject({
      sparkTransform: 'TextChunkAndEmbed',
      mechanism: 'builtin',
      pipPackages: [],
    });
  });

  it('supports prefixed and legacy Beam transform identifiers', () => {
    expect(resolveConnectorForRuntime('SOURCE:PubSubIO', 'serverless_4.0')).toMatchObject({
      sparkTransform: 'PubSubIO',
      mechanism: 'python_datasource',
      pipPackages: ['google-cloud-pubsub'],
      pythonDataSourceClass: 'PubSubDataSource',
    });
    expect(resolveConnectorForRuntime('SINK:PubSubIO', 'serverless_4.0')).toMatchObject({
      sparkTransform: 'PubSubIO',
      mechanism: 'foreach_batch_sdk',
      pipPackages: ['google-cloud-pubsub'],
    });
    expect(resolveConnectorForRuntime('STEP:Filter', 'serverless_4.0').mechanism).toBe(
      'builtin'
    );
    expect(resolveConnectorForRuntime('SOURCE:KinesisIO', 'local').pipPackages).toEqual([
      'boto3',
    ]);
    expect(resolveConnectorForRuntime('SINK:KinesisIO', 'local').mechanism).toBe(
      'foreach_batch_sdk'
    );
    expect(resolveConnectorForRuntime('SOURCE:MqttIO', 'local').pipPackages).toEqual([
      'paho-mqtt',
    ]);
    expect(resolveConnectorForRuntime('SINK:MqttIO', 'local').mechanism).toBe(
      'foreach_batch_sdk'
    );
    expect(resolveConnectorForRuntime('SOURCE:FirestoreIO', 'local').pipPackages).toEqual([
      'google-cloud-firestore',
    ]);
    expect(resolveConnectorForRuntime('SINK:FirestoreIO', 'local').mechanism).toBe(
      'foreach_batch_sdk'
    );
    expect(resolveConnectorForRuntime('SOURCE:CassandraIO', 'local').pipPackages).toEqual([
      'cassandra-driver',
    ]);
    expect(resolveConnectorForRuntime('SINK:CassandraIO', 'local').mechanism).toBe(
      'foreach_batch_sdk'
    );
    expect(resolveConnectorForRuntime('SOURCE:RedisIO', 'local').pipPackages).toEqual([
      'redis',
    ]);
    expect(resolveConnectorForRuntime('SINK:RedisIO', 'local').mechanism).toBe(
      'foreach_batch_sdk'
    );
    expect(resolveConnectorForRuntime('SINK:SplunkIO', 'local').pipPackages).toEqual([
      'requests',
    ]);
  });
});

describe('resolvePipelineRuntimeArtifacts and catalog sweep', () => {
  it('deduplicates JARs, Maven coords, pip packages, DataSources, and preinstalled connectors on Managed Spark', () => {
    const nodes = [
      { sparkTransform: 'KafkaSource' },
      { sparkTransform: 'EventHubsSource' },
      { sparkTransform: 'BigQuerySource' },
      { sparkTransform: 'BigQuerySink' },
      { sparkTransform: 'IcebergSink' },
      { sparkTransform: 'SpannerSource' },
      { sparkTransform: 'SpannerSink' },
      {
        sparkTransform: 'AlloyDbSource',
        properties: {
          driverJarUri: 'gs://jars/postgresql.jar',
          socketFactoryJarUri: 'gs://jars/alloydb-socket.jar',
        },
      },
      { sparkTransform: 'PubSubSource' },
      { sparkTransform: 'PubSubSink' },
      { data: { spark_transform: 'ParquetSource', properties: { path: 'gs://b/p' } } },
      { sparkTransform: '' },
    ];

    const plan = resolvePipelineRuntimeArtifacts(nodes, {
      runner: 'ServerlessSpark',
      runtimeVersion: '4.0',
      customJarUris: ['gs://jars/postgresql.jar', 'gs://jars/custom-udf.jar'],
    });

    expect(plan.profile.id).toBe('serverless_4.0');
    expect(plan.tier).toBe('standard');
    expect(plan.mavenPackages).toEqual([
      'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0',
    ]);
    expect(plan.preinstalledConnectors).toEqual([
      'spark-bigquery',
      'iceberg-spark-runtime',
      'gcs-connector',
    ]);
    expect(plan.gcsJars).toEqual([
      'gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar',
      'gs://jars/postgresql.jar',
      'gs://jars/alloydb-socket.jar',
      'gs://jars/custom-udf.jar',
    ]);
    expect(plan.pipPackages).toEqual(['google-cloud-pubsub']);
    expect(plan.pythonDataSources).toEqual(['PubSubDataSource']);
    expect(plan.sparkProperties).toEqual({
      'dataproc.artifacts.remove': 'conscrypt',
    });
  });

  it('includes local Maven coordinates and Spark Connect pip package when running locally', () => {
    const plan = resolvePipelineRuntimeArtifacts(
      [
        { sparkTransform: 'BigQuerySource' },
        { sparkTransform: 'IcebergSink' },
      ],
      {
        runner: 'Local',
        useSparkConnect: true,
        jars: 'gs://jars/one.jar, gs://jars/two.jar',
      }
    );

    expect(plan.profile.id).toBe('local');
    expect(plan.tier).toBe('standard');
    expect(plan.preinstalledConnectors).toEqual([]);
    expect(plan.mavenPackages).toEqual([
      'com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.42.1',
      'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1',
    ]);
    expect(plan.pipPackages).toEqual(['google-cloud-spark-connect']);
    expect(plan.gcsJars).toEqual(['gs://jars/one.jar', 'gs://jars/two.jar']);
  });

  it('honors explicit premium tier and alternative version keys in jobConfig', () => {
    const plan = resolvePipelineRuntimeArtifacts(
      [{ sparkTransform: 'KafkaSource' }],
      {
        runner: 'ServerlessSpark',
        version: '3.0',
        serverlessTier: 'premium',
      }
    );
    expect(plan.profile.id).toBe('serverless_3.0');
    expect(plan.tier).toBe('premium');
    expect(plan.mavenPackages).toEqual([
      'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1',
    ]);

    const clusterPlan = resolvePipelineRuntimeArtifacts(
      [{ sparkTransform: 'KafkaSource' }],
      {
        runner: 'DataprocCluster',
        imageVersion: '3.0',
      }
    );
    expect(clusterPlan.profile.id).toBe('cluster_3.0');
    expect(clusterPlan.mavenPackages).toEqual([
      'org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2',
    ]);
  });

  it('resolves all 40 Spark 4.x sources and 38 Spark 4.x sinks without Scala 2.12 artifacts', () => {
    expect(ALL_PHASE_5_SOURCES).toHaveLength(40);
    expect(ALL_PHASE_6_SINKS).toHaveLength(38);

    const targets: SparkRuntimeTarget[] = [
      'serverless_4.0',
      'serverless_3.0',
      'cluster_3.0',
      'local',
    ];

    for (const target of targets) {
      for (const transform of [...ALL_PHASE_5_SOURCES, ...ALL_PHASE_6_SINKS]) {
        const res = resolveConnectorForRuntime(transform, target);
        expect(res.sparkTransform).toBe(transform);
        for (const coord of res.mavenCoordinates) {
          expect(coord).not.toContain('_2.12');
          expect(coord.split(':').length).toBe(3);
        }
      }
    }
  });
});

describe('versionMatrix boundary and failure conditions (T1)', () => {
  it('normalizes padded, mixed-case, and non-premium tier values to the documented tiers', () => {
    expect(resolveServerlessTier('  Premium  ')).toBe('premium');
    expect(resolveServerlessTier('standard')).toBe('standard');
    expect(resolveServerlessTier('')).toBe('standard');
    expect(resolveServerlessTier('   ')).toBe('standard');
    expect(resolveServerlessTier('premium-plus')).toBe('standard');
    expect(resolveServerlessTier(undefined)).toBe('standard');
    expect(resolveServerlessTier(42 as unknown as string)).toBe('standard');
  });

  it('resolves runner aliases case-insensitively with surrounding whitespace', () => {
    expect(resolveRuntimeProfile('  SERVERLESS_4.0 ').id).toBe('serverless_4.0');
    expect(resolveRuntimeProfile('Cluster_3.0').id).toBe('cluster_3.0');
    expect(resolveRuntimeProfile('serverless_spark').id).toBe('serverless_4.0');
    expect(resolveRuntimeProfile('Serverless').id).toBe('serverless_4.0');
    expect(resolveRuntimeProfile('dataproc_cluster').id).toBe('cluster_3.0');
    expect(resolveRuntimeProfile('cluster').id).toBe('cluster_3.0');
    expect(resolveRuntimeProfile('   ').id).toBe('local');
    expect(resolveRuntimeProfile('DirectRunner').id).toBe('local');
    expect(resolveRuntimeProfile('FlinkRunner').id).toBe('local');
    expect(resolveRuntimeProfile(7 as unknown as string).id).toBe('local');
  });

  it('matches Serverless 3.0 version labels only on the exact 3.0 prefix forms', () => {
    expect(resolveRuntimeProfile('ServerlessSpark', ' 3.0 ').id).toBe('serverless_3.0');
    expect(resolveRuntimeProfile('ServerlessSpark', '3.0 (Spark 4.1)').id).toBe('serverless_3.0');
    expect(resolveRuntimeProfile('ServerlessSpark', '3.0(spark 4.1)').id).toBe('serverless_3.0');
    expect(resolveRuntimeProfile('ServerlessSpark', 'SERVERLESS_3.0').id).toBe('serverless_3.0');
    expect(resolveRuntimeProfile('ServerlessSpark', '3.01').id).toBe('serverless_4.0');
    expect(resolveRuntimeProfile('ServerlessSpark', '2.2').id).toBe('serverless_4.0');
    expect(resolveRuntimeProfile('ServerlessSpark', '').id).toBe('serverless_4.0');
    // Cluster and local profiles ignore the runtime version argument.
    expect(resolveRuntimeProfile('DataprocCluster', '3.0').id).toBe('cluster_3.0');
    expect(resolveRuntimeProfile('Local', '3.0').id).toBe('local');
  });

  it('isolates each resolved profile so caller mutation never leaks into RUNTIME_PROFILES', () => {
    const first = resolveRuntimeProfile('ServerlessSpark');
    first.preinstalledConnectors.push('mutated-connector');
    first.sparkVersion = '0.0.0';
    const second = resolveRuntimeProfile('ServerlessSpark');
    expect(second.preinstalledConnectors).not.toContain('mutated-connector');
    expect(second.sparkVersion).toBe(RUNTIME_PROFILES['serverless_4.0'].sparkVersion);
  });

  it('classifies only Serverless and Dataproc targets as Managed Spark', () => {
    const managed = (Object.keys(RUNTIME_PROFILES) as SparkRuntimeTarget[]).filter((target) =>
      isManagedSparkRuntime(target)
    );
    expect(managed.sort()).toEqual(['cluster_3.0', 'serverless_3.0', 'serverless_4.0']);
    expect(isManagedSparkRuntime('local')).toBe(false);
  });

  it('falls back to the local profile for an unknown target and to builtin for an unknown transform', () => {
    const unknownTarget = resolveConnectorForRuntime(
      'BigQuerySource',
      'serverless_9.9' as SparkRuntimeTarget
    );
    expect(unknownTarget.mechanism).toBe('maven_package');
    expect(unknownTarget.preinstalledArtifact).toBeUndefined();

    const unknownTransform = resolveConnectorForRuntime('  STEP:MyCustomStep  ', 'serverless_4.0');
    expect(unknownTransform).toMatchObject({
      sparkTransform: 'MyCustomStep',
      mechanism: 'builtin',
      gcsJarUris: [],
      mavenCoordinates: [],
      pipPackages: [],
      sparkProperties: {},
    });
  });

  it('attaches spark-avro to ParsePayload only when the payload format is Avro', () => {
    const avro = resolveConnectorForRuntime('ParsePayload', 'local', { format: ' AVRO ' });
    expect(avro.mechanism).toBe('maven_package');
    expect(avro.mavenCoordinates).toEqual([
      `org.apache.spark:spark-avro_2.13:${RUNTIME_PROFILES.local.sparkVersion}`,
    ]);

    const json = resolveConnectorForRuntime('ParsePayload', 'local', { format: 'json' });
    expect(json.mechanism).toBe('builtin');
    expect(json.mavenCoordinates).toEqual([]);

    const missingFormat = resolveConnectorForRuntime('ParsePayload', 'local');
    expect(missingFormat.mechanism).toBe('builtin');
  });

  it('returns an empty artifact plan for empty pipelines and nodes without a transform', () => {
    const empty = resolvePipelineRuntimeArtifacts([]);
    expect(empty.profile.id).toBe('local');
    expect(empty.tier).toBe('standard');
    expect(empty.gcsJars).toEqual([]);
    expect(empty.mavenPackages).toEqual([]);
    expect(empty.pipPackages).toEqual([]);
    expect(empty.sparkProperties).toEqual({});
    expect(empty.pythonDataSources).toEqual([]);
    expect(empty.preinstalledConnectors).toEqual([]);

    const blank = resolvePipelineRuntimeArtifacts(
      [{ sparkTransform: '   ' }, { properties: { path: 'gs://bucket/x' } }],
      { runner: 'ServerlessSpark' }
    );
    expect(blank.profile.id).toBe('serverless_4.0');
    expect(blank.preinstalledConnectors).toEqual([]);
    expect(blank.mavenPackages).toEqual([]);
  });

  it('reads transforms and properties from snake_case and React Flow data wrappers', () => {
    const plan = resolvePipelineRuntimeArtifacts(
      [
        { spark_transform: 'BigQuerySource' },
        { data: { spark_transform: 'IcebergSink' } },
        { data: { sparkTransform: 'ParquetSource', properties: { path: 's3a://lake/raw' } } },
      ] as unknown as Array<{ sparkTransform?: string; properties?: Record<string, unknown> }>,
      { runner: 'DataprocCluster' }
    );
    expect(plan.profile.id).toBe('cluster_3.0');
    expect(plan.preinstalledConnectors).toEqual([
      'spark-bigquery',
      'iceberg-spark-runtime',
      'hadoop-aws',
    ]);
    expect(plan.mavenPackages).toEqual([]);
  });

  it('ignores malformed jobConfig JAR entries and Spark Connect on Managed Spark', () => {
    const plan = resolvePipelineRuntimeArtifacts([], {
      runner: 'ServerlessSpark',
      useSparkConnect: true,
      customJarUris: ['  gs://jars/a.jar  ', '', 42, null, 'gs://jars/a.jar'],
      jars: ' , gs://jars/b.jar ,, gs://jars/a.jar ',
    });
    expect(plan.gcsJars).toEqual(['gs://jars/a.jar', 'gs://jars/b.jar']);
    expect(plan.pipPackages).not.toContain('google-cloud-spark-connect');

    const truthyButNotTrue = resolvePipelineRuntimeArtifacts([], {
      runner: 'Local',
      useSparkConnect: 'true',
    });
    expect(truthyButNotTrue.pipPackages).toEqual([]);
  });

  it('prefers runtimeVersion over version and imageVersion, and tier over serverlessTier', () => {
    const plan = resolvePipelineRuntimeArtifacts([], {
      runner: 'ServerlessSpark',
      runtimeVersion: '4.0',
      version: '3.0',
      imageVersion: '3.0',
      tier: 'standard',
      serverlessTier: 'premium',
    });
    expect(plan.profile.id).toBe('serverless_4.0');
    expect(plan.tier).toBe('standard');

    const nonStringRunner = resolvePipelineRuntimeArtifacts([], { runner: 5, tier: true });
    expect(nonStringRunner.profile.id).toBe('local');
    expect(nonStringRunner.tier).toBe('standard');
  });
});
