/**
 * @fileoverview Unit tests for Spark Declarative Pipelines configuration validation and static setting routing (SDP3).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect } from 'vitest';
import {
  validateSdpStorageUri,
  validateSdpCatalogAlias,
  validateSdpDatabaseName,
  validateBigLakeWarehouse,
  partitionSdpSparkSettings,
  buildBigLakeRestCatalogConfig,
  isStaticSdpSparkConfigKey,
  extractGraphSourcePaths,
  hasIcebergGraphNodes,
  parseSparkConfEntries,
  formatSparkConfEntries,
} from './sdpConfigValidation';

describe('sdpConfigValidation (SDP3)', () => {
  describe('validateSdpStorageUri', () => {
    it('accepts absolute URIs with schemes located outside source folders', () => {
      expect(
        validateSdpStorageUri('gs://my-sdp-bucket/pipelines/orders', [
          'gs://my-raw-bucket/orders/incoming/',
        ])
      ).toEqual({ valid: true });

      // Sibling prefix in same bucket is valid
      expect(
        validateSdpStorageUri('gs://shared-bucket/orders_checkpoints', [
          'gs://shared-bucket/orders',
        ])
      ).toEqual({ valid: true });

      expect(validateSdpStorageUri('file:///tmp/sdp_checkpoints')).toEqual({
        valid: true,
      });
    });

    it('rejects empty strings and bare paths without URI schemes with PIPELINE_STORAGE_ROOT_INVALID', () => {
      const emptyRes = validateSdpStorageUri('   ');
      expect(emptyRes.valid).toBe(false);
      expect(emptyRes.error).toMatch(/PIPELINE_STORAGE_ROOT_INVALID/);

      const relativeRes = validateSdpStorageUri('my-bucket/checkpoints');
      expect(relativeRes.valid).toBe(false);
      expect(relativeRes.error).toMatch(/PIPELINE_STORAGE_ROOT_INVALID/);

      const localPosixRes = validateSdpStorageUri('/tmp/sdp/checkpoints');
      expect(localPosixRes.valid).toBe(false);
      expect(localPosixRes.error).toMatch(/PIPELINE_STORAGE_ROOT_INVALID/);

      const emptyBucketRes = validateSdpStorageUri('gs://');
      expect(emptyBucketRes.valid).toBe(false);
      expect(emptyBucketRes.error).toMatch(/PIPELINE_STORAGE_ROOT_INVALID/);
    });

    it('rejects storage URIs equal to or nested inside any source node folder URI', () => {
      const equalRes = validateSdpStorageUri('gs://raw-landing/orders/', [
        'gs://raw-landing/orders',
      ]);
      expect(equalRes.valid).toBe(false);
      expect(equalRes.error).toMatch(/cannot equal or be nested inside source path/i);

      const nestedRes = validateSdpStorageUri('gs://raw-landing/orders/_checkpoints', [
        'gs://raw-landing/orders/*.csv',
      ]);
      expect(nestedRes.valid).toBe(false);
      expect(nestedRes.error).toMatch(/cannot equal or be nested inside source path/i);
    });
  });

  describe('validateSdpCatalogAlias', () => {
    it('accepts plain identifiers and Managed Data Pipeline Jinja templates', () => {
      expect(validateSdpCatalogAlias('lake')).toEqual({ valid: true });
      expect(validateSdpCatalogAlias('bronze_catalog_01')).toEqual({ valid: true });
      expect(validateSdpCatalogAlias('_internal_lake')).toEqual({ valid: true });
      expect(validateSdpCatalogAlias('{{ catalog_alias }}')).toEqual({ valid: true });
      expect(validateSdpCatalogAlias('{{ env.sdp_catalog }}')).toEqual({ valid: true });
    });

    it('rejects empty, hyphenated, dotted, or digit-leading names with INVALID_IDENTIFIER', () => {
      const hyphenated = validateSdpCatalogAlias('my-bucket');
      expect(hyphenated.valid).toBe(false);
      expect(hyphenated.code).toBe('INVALID_IDENTIFIER');
      expect(hyphenated.error).toMatch(/INVALID_IDENTIFIER/);

      expect(validateSdpCatalogAlias('').valid).toBe(false);
      expect(validateSdpCatalogAlias('123lake').valid).toBe(false);
      expect(validateSdpCatalogAlias('proj.lake').valid).toBe(false);
      expect(validateSdpCatalogAlias('my catalog').valid).toBe(false);
    });
  });

  describe('validateSdpDatabaseName', () => {
    it('allows empty optional database and validates non-empty identifiers', () => {
      expect(validateSdpDatabaseName('')).toEqual({ valid: true });
      expect(validateSdpDatabaseName('retail_silver')).toEqual({ valid: true });
      expect(validateSdpDatabaseName('{{ target_db }}')).toEqual({ valid: true });
      expect(validateSdpDatabaseName('retail-silver').valid).toBe(false);
    });
  });

  describe('validateBigLakeWarehouse', () => {
    it('validates lakehouse bl://projects/<project>/catalogs/<catalog_id> URIs', () => {
      expect(
        validateBigLakeWarehouse('lakehouse', 'bl://projects/my-gcp-proj/catalogs/lakehouse_prod')
      ).toEqual({ valid: true });
      expect(
        validateBigLakeWarehouse('lakehouse', 'bl://projects/{{ project }}/catalogs/{{ cat }}')
      ).toEqual({ valid: true });

      const invalidScheme = validateBigLakeWarehouse('lakehouse', 'gs://my-bucket');
      expect(invalidScheme.valid).toBe(false);
      expect(invalidScheme.error).toMatch(/bl:\/\/projects\/<project>\/catalogs\/<catalog_id>/);

      const missingCatalog = validateBigLakeWarehouse('lakehouse', 'bl://projects/my-gcp-proj');
      expect(missingCatalog.valid).toBe(false);
    });

    it('validates gcs_bucket root gs://<bucket> URIs and rejects /warehouse suffixes', () => {
      expect(validateBigLakeWarehouse('gcs_bucket', 'gs://my-iceberg-bucket')).toEqual({
        valid: true,
      });

      const withSuffix = validateBigLakeWarehouse(
        'gcs_bucket',
        'gs://my-iceberg-bucket/warehouse'
      );
      expect(withSuffix.valid).toBe(false);
      expect(withSuffix.error).toMatch(/BadRequestException: Bucket name is invalid/);

      const trailingSlash = validateBigLakeWarehouse('gcs_bucket', 'gs://my-iceberg-bucket/');
      expect(trailingSlash.valid).toBe(false);
      expect(trailingSlash.error).toMatch(/BadRequestException: Bucket name is invalid/);

      const missingScheme = validateBigLakeWarehouse('gcs_bucket', 'my-iceberg-bucket');
      expect(missingScheme.valid).toBe(false);
    });

    it('handles custom and none modes cleanly', () => {
      expect(validateBigLakeWarehouse('none', '')).toEqual({ valid: true });
      expect(validateBigLakeWarehouse('custom', 'gs://my-bucket/warehouse')).toEqual({
        valid: true,
      });
      expect(validateBigLakeWarehouse('custom', '').valid).toBe(false);
      expect(validateBigLakeWarehouse('custom', 'relative/warehouse').valid).toBe(false);
    });
  });

  describe('partitionSdpSparkSettings & buildBigLakeRestCatalogConfig', () => {
    it('routes all static Spark and cluster keys to staticRunnerConfig and dynamic keys to dynamicSpecConfig', () => {
      const rawSettings: Record<string, string> = {
        'spark.sql.shuffle.partitions': '200',
        'spark.sql.ansi.enabled': 'true',
        'spark.jars': 'gs://spark-lib/custom.jar',
        'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0',
        'spark.sql.extensions':
          'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalogImplementation': 'in-memory',
        'spark.driver.memory': '8g',
        'spark.executor.instances': '4',
        'spark.executor.memory': '16g',
        'spark.dynamicAllocation.enabled': 'true',
        'spark.files': 'gs://my-bucket/config.json',
        'spark.archives': 'gs://my-bucket/deps.tar.gz',
      };

      const partitioned = partitionSdpSparkSettings(rawSettings, {
        catalogAlias: 'lake',
        mode: 'lakehouse',
        project: 'analytics-prod',
        catalogId: 'retail_iceberg',
      });

      expect(partitioned.staticRunnerConfig).toEqual({
        'spark.jars': 'gs://spark-lib/custom.jar',
        'spark.jars.packages': 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0',
        'spark.sql.extensions':
          'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalogImplementation': 'in-memory',
        'spark.driver.memory': '8g',
        'spark.executor.instances': '4',
        'spark.executor.memory': '16g',
        'spark.dynamicAllocation.enabled': 'true',
        'spark.files': 'gs://my-bucket/config.json',
        'spark.archives': 'gs://my-bucket/deps.tar.gz',
      });

      expect(partitioned.dynamicSpecConfig).toEqual({
        'spark.sql.shuffle.partitions': '200',
        'spark.sql.ansi.enabled': 'true',
        'spark.sql.catalog.lake': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.lake.type': 'rest',
        'spark.sql.catalog.lake.uri': 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
        'spark.sql.catalog.lake.warehouse':
          'bl://projects/analytics-prod/catalogs/retail_iceberg',
        'spark.sql.catalog.lake.header.x-goog-user-project': 'analytics-prod',
        'spark.sql.catalog.lake.rest.auth.type':
          'org.apache.iceberg.gcp.auth.GoogleAuthManager',
        'spark.sql.catalog.lake.io-impl': 'org.apache.iceberg.gcp.gcs.GCSFileIO',
      });

      // Verify zero static keys leaked into dynamicSpecConfig
      for (const key of Object.keys(partitioned.dynamicSpecConfig)) {
        expect(isStaticSdpSparkConfigKey(key)).toBe(false);
      }
    });

    it('builds gcs_bucket BigLake REST catalog config with bucket-root warehouse', () => {
      const cfg = buildBigLakeRestCatalogConfig({
        catalogAlias: 'lake',
        mode: 'gcs_bucket',
        project: 'my-proj',
        warehouseBucket: 'gs://my-lake-bucket',
      });
      expect(cfg['spark.sql.catalog.lake.warehouse']).toBe('gs://my-lake-bucket');
      expect(cfg['spark.sql.catalog.lake.type']).toBe('rest');
      expect(cfg['spark.sql.catalog.lake.uri']).toBe(
        'https://biglake.googleapis.com/iceberg/v1/restcatalog'
      );
    });
  });

  describe('graph helpers and Spark conf parsing', () => {
    it('extracts source paths and detects Iceberg nodes from canvas graph including React Flow type="pipeline" nodes', () => {
      const nodes = [
        {
          id: 'src-1',
          type: 'pipeline',
          data: {
            type: 'SOURCE',
            spark_transform: 'CsvSource',
            properties: {
              path: 'gs://raw-bucket/orders/',
              filePattern: 'gs://raw-bucket/events/*.csv',
            },
          },
        },
        {
          id: 'snk-1',
          type: 'pipeline',
          data: {
            type: 'SINK',
            spark_transform: 'SINK:IcebergIO',
            properties: { path: 'gs://ignored-sink-path' },
          },
        },
      ];
      expect(extractGraphSourcePaths(nodes)).toEqual([
        'gs://raw-bucket/orders/',
        'gs://raw-bucket/events/*.csv',
      ]);
      expect(hasIcebergGraphNodes(nodes)).toBe(true);
      expect(
        hasIcebergGraphNodes([
          {
            id: 'src-2',
            type: 'pipeline',
            data: { type: 'SOURCE', spark_transform: 'SOURCE:IcebergSource', properties: {} },
          },
        ])
      ).toBe(true);
      expect(hasIcebergGraphNodes([nodes[0]])).toBe(false);
    });

    it('parses and formats multiline key=value Spark configuration entries', () => {
      const text = '# comment\nspark.driver.memory = 4g\nspark.sql.shuffle.partitions=100\n';
      expect(parseSparkConfEntries(text)).toEqual({
        'spark.driver.memory': '4g',
        'spark.sql.shuffle.partitions': '100',
      });
      expect(formatSparkConfEntries({ 'spark.driver.memory': '4g' })).toBe(
        'spark.driver.memory=4g'
      );
    });
  });
});
