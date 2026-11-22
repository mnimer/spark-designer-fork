/**
 * @fileoverview Validation, BigLake Iceberg REST catalog construction, and static Spark property routing for Spark Declarative Pipelines.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import type { SdpBigLakeMode } from './sharedTypes';

/**
 * Supported BigLake Iceberg catalog configuration modes for Spark Declarative Pipelines.
 */
export type SdpBigLakeConfigMode = SdpBigLakeMode;

/**
 * Validation result for a Spark Declarative Pipeline setting.
 */
export interface SdpValidationResult {
  /** True when the input value satisfies Spark Declarative Pipeline rules. */
  valid: boolean;
  /** Optional Spark 4.2 error class identifier when validation fails. */
  code?: string;
  /** Human-readable remediation message when validation fails. */
  error?: string;
}

/**
 * Partitioned Spark settings separating dynamic spec entries from static runner properties.
 */
export interface PartitionedSdpSparkSettings {
  /** Dynamic runtime and catalog settings safe for spark-pipeline.yml configuration. */
  dynamicSpecConfig: Record<string, string>;
  /** Static cluster and driver settings routed to execution_config.properties or CLI --conf flags. */
  staticRunnerConfig: Record<string, string>;
}

/**
 * Input options for building declarative BigLake Iceberg REST catalog entries.
 */
export interface BigLakeCatalogConfigOptions {
  /** Plain Spark catalog alias used inside spark-pipeline.yml (defaults to lake). */
  catalogAlias?: string;
  /** Selected BigLake warehouse configuration mode. */
  mode: SdpBigLakeConfigMode;
  /** Billing and catalog host GCP project identifier. */
  project?: string;
  /** BigLake Lakehouse catalog identifier for bl:// warehouse URIs. */
  catalogId?: string;
  /** Cloud Storage root bucket or custom warehouse URI. */
  warehouseBucket?: string;
}

const URI_SCHEME_REGEX = /^([a-zA-Z][a-zA-Z0-9+.-]*):\/\/(.+)$/;
const PLAIN_IDENTIFIER_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;
const JINJA_TEMPLATE_IDENTIFIER_REGEX = /^\{\{\s*[A-Za-z_][A-Za-z0-9_.]*\s*\}\}$/;
const BIGLAKE_LAKEHOUSE_URI_REGEX =
  /^bl:\/\/projects\/([A-Za-z0-9.:_-]+|\{\{\s*[A-Za-z_][A-Za-z0-9_.]*\s*\}\})\/catalogs\/([A-Za-z0-9_-]+|\{\{\s*[A-Za-z_][A-Za-z0-9_.]*\s*\}\})$/;
const GCS_ROOT_BUCKET_URI_REGEX =
  /^gs:\/\/([a-zA-Z0-9._-]+|\{\{\s*[A-Za-z_][A-Za-z0-9_.]*\s*\}\})$/;

/**
 * Static Spark configuration prefixes and exact keys rejected inside spark-pipeline.yml configuration.
 */
export const STATIC_SDP_SPARK_EXACT_KEYS: ReadonlySet<string> = new Set([
  'spark.jars',
  'spark.sql.extensions',
  'spark.sql.catalogImplementation',
  'spark.files',
  'spark.archives',
  'spark.master',
  'spark.submit.deployMode',
]);

/**
 * Static Spark configuration key prefixes that fail with CANNOT_MODIFY_STATIC_CONFIG in spark-pipeline.yml.
 */
export const STATIC_SDP_SPARK_KEY_PREFIXES: readonly string[] = [
  'spark.jars.',
  'spark.driver.',
  'spark.executor.',
  'spark.dynamicAllocation.',
  'spark.files.',
  'spark.archives.',
];

/**
 * Normalizes a storage or source URI for prefix comparison by stripping glob suffixes and trailing slashes.
 *
 * @param rawUri - Raw storage or source URI string.
 * @returns Normalized URI string with lowercase scheme and no trailing slash, or empty string when blank.
 */
export function normalizeComparableUri(rawUri: string): string {
  const trimmed = rawUri.trim();
  if (!trimmed) {
    return '';
  }

  const schemeMatch = URI_SCHEME_REGEX.exec(trimmed);
  if (!schemeMatch) {
    return trimmed.replace(/\/+$/, '');
  }

  const scheme = schemeMatch[1].toLowerCase();
  let remainder = schemeMatch[2].replace(/\/+$/, '');

  // If the last path segment contains wildcard glob characters, strip that filename segment to get the folder.
  const slashIdx = remainder.lastIndexOf('/');
  if (slashIdx > 0) {
    const tail = remainder.slice(slashIdx + 1);
    if (/[*?[\]]/.test(tail)) {
      remainder = remainder.slice(0, slashIdx).replace(/\/+$/, '');
    }
  }

  return `${scheme}://${remainder}`;
}

/**
 * Validates the Spark Declarative Pipeline storage root URI.
 *
 * If the URI lacks a scheme, returns PIPELINE_STORAGE_ROOT_INVALID.
 * If the URI equals or sits inside any source node folder URI, rejects it to protect source streams.
 *
 * @param storage - Candidate pipeline storage root URI.
 * @param sourcePaths - Optional array of source node folder or file URIs from the pipeline graph.
 * @returns Validation outcome with optional error message.
 */
export function validateSdpStorageUri(
  storage: string,
  sourcePaths?: readonly string[]
): SdpValidationResult {
  const trimmed = storage.trim();
  if (!trimmed) {
    return {
      valid: false,
      code: 'PIPELINE_STORAGE_ROOT_INVALID',
      error:
        'PIPELINE_STORAGE_ROOT_INVALID: Pipeline storage root must be an absolute URI with a scheme (for example gs://my-bucket/pipelines/orders).',
    };
  }

  const schemeMatch = URI_SCHEME_REGEX.exec(trimmed);
  if (!schemeMatch) {
    return {
      valid: false,
      code: 'PIPELINE_STORAGE_ROOT_INVALID',
      error:
        'PIPELINE_STORAGE_ROOT_INVALID: Bare paths without a URI scheme are rejected. Provide an absolute URI such as gs://<bucket>/pipelines/<name>.',
    };
  }

  const scheme = schemeMatch[1].toLowerCase();
  const remainder = schemeMatch[2].replace(/\/+$/, '');
  if (!remainder) {
    return {
      valid: false,
      code: 'PIPELINE_STORAGE_ROOT_INVALID',
      error: `PIPELINE_STORAGE_ROOT_INVALID: Storage URI "${trimmed}" is missing a bucket or path target.`,
    };
  }

  if ((scheme === 'gs' || scheme === 's3' || scheme === 's3a') && remainder.startsWith('/')) {
    return {
      valid: false,
      code: 'PIPELINE_STORAGE_ROOT_INVALID',
      error: `PIPELINE_STORAGE_ROOT_INVALID: Cloud storage URI "${trimmed}" must include a bucket authority.`,
    };
  }

  const normalizedStorage = normalizeComparableUri(trimmed);
  if (sourcePaths && sourcePaths.length > 0) {
    for (const rawSource of sourcePaths) {
      if (typeof rawSource !== 'string' || !rawSource.trim()) {
        continue;
      }
      const normalizedSource = normalizeComparableUri(rawSource);
      if (!normalizedSource) {
        continue;
      }
      if (
        normalizedStorage === normalizedSource ||
        normalizedStorage.startsWith(`${normalizedSource}/`)
      ) {
        return {
          valid: false,
          code: 'STORAGE_NESTED_IN_SOURCE',
          error: `Pipeline storage URI "${trimmed}" cannot equal or be nested inside source path "${rawSource.trim()}". Use a sibling prefix or dedicated checkpoint bucket.`,
        };
      }
    }
  }

  return { valid: true };
}

/**
 * Extracts source folder or file URIs from canvas nodes for storage root overlap validation.
 *
 * @param nodes - Graph nodes from the editor or codegen context.
 * @returns Non-empty URI strings configured on SOURCE nodes.
 */
export function extractGraphSourcePaths(nodes?: readonly unknown[]): string[] {
  if (!Array.isArray(nodes)) {
    return [];
  }
  const paths: string[] = [];
  const candidateKeys = [
    'path',
    'uri',
    'inputPath',
    'location',
    'folderUri',
    'sourcePath',
    'filePattern',
  ];

  for (const rawNode of nodes) {
    if (!rawNode || typeof rawNode !== 'object') {
      continue;
    }
    const record = rawNode as Record<string, unknown>;
    const data =
      record.data && typeof record.data === 'object'
        ? (record.data as Record<string, unknown>)
        : undefined;
    const nodeTypeRaw = String(
      data?.type ?? data?.nodeType ?? record.type ?? ''
    ).toUpperCase();
    const transformRaw = String(
      data?.sparkTransform ??
        data?.spark_transform ??
        record.sparkTransform ??
        record.spark_transform ??
        ''
    );
    const isSource =
      nodeTypeRaw.includes('SOURCE') ||
      transformRaw.startsWith('SOURCE:') ||
      ((nodeTypeRaw === '' || nodeTypeRaw === 'PIPELINE') &&
        transformRaw.endsWith('Source'));
    if (!isSource) {
      continue;
    }
    const props =
      (data?.properties && typeof data.properties === 'object'
        ? (data.properties as Record<string, unknown>)
        : record.properties && typeof record.properties === 'object'
          ? (record.properties as Record<string, unknown>)
          : undefined) ?? {};

    for (const key of candidateKeys) {
      const val = props[key];
      if (typeof val === 'string' && val.trim()) {
        paths.push(val.trim());
      }
    }
  }

  return Array.from(new Set(paths));
}

/**
 * Checks whether any canvas node in the pipeline uses Apache Iceberg.
 *
 * @param nodes - Graph nodes from the editor or codegen context.
 * @returns True when an IcebergSource, IcebergSink, IcebergIO, or iceberg SDP format is present.
 */
export function hasIcebergGraphNodes(nodes?: readonly unknown[]): boolean {
  if (!Array.isArray(nodes)) {
    return false;
  }
  for (const rawNode of nodes) {
    if (!rawNode || typeof rawNode !== 'object') {
      continue;
    }
    const record = rawNode as Record<string, unknown>;
    const data =
      record.data && typeof record.data === 'object'
        ? (record.data as Record<string, unknown>)
        : undefined;
    const transformRaw = String(
      data?.sparkTransform ??
        data?.spark_transform ??
        record.sparkTransform ??
        record.spark_transform ??
        ''
    );
    const bareTransform = transformRaw.replace(/^(SOURCE|SINK|STEP):/i, '');
    if (
      bareTransform === 'IcebergSource' ||
      bareTransform === 'IcebergSink' ||
      bareTransform === 'IcebergIO' ||
      /^Iceberg/i.test(bareTransform)
    ) {
      return true;
    }
    const props =
      (data?.properties && typeof data.properties === 'object'
        ? (data.properties as Record<string, unknown>)
        : record.properties && typeof record.properties === 'object'
          ? (record.properties as Record<string, unknown>)
          : undefined) ?? {};
    if (String(props.sdpFormat ?? '').toLowerCase() === 'iceberg') {
      return true;
    }
  }
  return false;
}

/**
 * Validates the Spark catalog alias in spark-pipeline.yml.
 *
 * SDP does not quote catalog identifiers, so hyphenated names such as my-bucket fail with INVALID_IDENTIFIER.
 * Plain identifiers and Managed Data Pipeline Jinja variable templates are valid.
 *
 * @param catalog - Candidate Spark catalog alias.
 * @returns Validation outcome with INVALID_IDENTIFIER error when invalid.
 */
export function validateSdpCatalogAlias(catalog: string): SdpValidationResult {
  const trimmed = catalog.trim();
  if (!trimmed) {
    return {
      valid: false,
      code: 'INVALID_IDENTIFIER',
      error:
        'INVALID_IDENTIFIER: Spark catalog alias is required. Use a plain identifier such as "lake".',
    };
  }

  if (PLAIN_IDENTIFIER_REGEX.test(trimmed) || JINJA_TEMPLATE_IDENTIFIER_REGEX.test(trimmed)) {
    return { valid: true };
  }

  return {
    valid: false,
    code: 'INVALID_IDENTIFIER',
    error: `INVALID_IDENTIFIER: Catalog alias "${trimmed}" must use only letters, digits, and underscores (for example "lake"), or a Managed Data Pipeline {{ var }} template. Hyphenated names fail streaming table registration.`,
  };
}

/**
 * Validates an optional target database or namespace identifier for spark-pipeline.yml.
 *
 * @param database - Candidate target database name.
 * @returns Validation outcome.
 */
export function validateSdpDatabaseName(database: string): SdpValidationResult {
  const trimmed = database.trim();
  if (!trimmed) {
    return { valid: true };
  }
  if (PLAIN_IDENTIFIER_REGEX.test(trimmed) || JINJA_TEMPLATE_IDENTIFIER_REGEX.test(trimmed)) {
    return { valid: true };
  }
  return {
    valid: false,
    code: 'INVALID_IDENTIFIER',
    error: `INVALID_IDENTIFIER: Database "${trimmed}" must be a plain Spark identifier (letters, digits, and underscores) or a {{ var }} template.`,
  };
}

/**
 * Validates the BigLake Iceberg REST catalog warehouse value for the chosen BigLake mode.
 *
 * - In lakehouse mode, expects bl://projects/<project>/catalogs/<catalog_id>.
 * - In gcs_bucket mode, expects bucket-root gs://<bucket> without /warehouse or path suffixes.
 *
 * @param mode - BigLake catalog configuration mode.
 * @param value - Candidate warehouse URI string.
 * @returns Validation outcome with remediation message on failure.
 */
export function validateBigLakeWarehouse(
  mode: SdpBigLakeMode,
  value: string
): SdpValidationResult {
  if (mode === 'none') {
    return { valid: true };
  }

  const trimmed = value.trim();
  if (mode === 'lakehouse') {
    if (!trimmed) {
      return {
        valid: false,
        code: 'BIGLAKE_WAREHOUSE_INVALID',
        error:
          'BigLake Lakehouse warehouse requires bl://projects/<project>/catalogs/<catalog_id>.',
      };
    }
    if (!BIGLAKE_LAKEHOUSE_URI_REGEX.test(trimmed)) {
      return {
        valid: false,
        code: 'BIGLAKE_WAREHOUSE_INVALID',
        error: `BigLake Lakehouse warehouse "${trimmed}" must match bl://projects/<project>/catalogs/<catalog_id>.`,
      };
    }
    return { valid: true };
  }

  if (mode === 'gcs_bucket') {
    if (!trimmed) {
      return {
        valid: false,
        code: 'BIGLAKE_BUCKET_INVALID',
        error:
          'BigLake GCS REST catalog warehouse requires a bucket-root URI (gs://<bucket>).',
      };
    }
    if (!trimmed.startsWith('gs://')) {
      return {
        valid: false,
        code: 'BIGLAKE_BUCKET_INVALID',
        error: `BigLake GCS REST catalog warehouse "${trimmed}" must start with gs://<bucket>.`,
      };
    }
    const afterScheme = trimmed.slice('gs://'.length);
    if (afterScheme.includes('/')) {
      return {
        valid: false,
        code: 'BIGLAKE_BUCKET_INVALID',
        error: `BadRequestException: Bucket name is invalid. BigLake GCS REST catalog warehouse "${trimmed}" must be bucket-root gs://<bucket> only without /warehouse or path suffixes.`,
      };
    }
    if (!GCS_ROOT_BUCKET_URI_REGEX.test(trimmed)) {
      return {
        valid: false,
        code: 'BIGLAKE_BUCKET_INVALID',
        error: `BigLake GCS REST catalog warehouse "${trimmed}" contains invalid bucket characters.`,
      };
    }
    return { valid: true };
  }

  // mode === 'custom'
  if (!trimmed) {
    return {
      valid: false,
      code: 'CUSTOM_WAREHOUSE_REQUIRED',
      error: 'Custom catalog mode requires a warehouse URI (such as gs://<bucket>/warehouse).',
    };
  }
  if (!URI_SCHEME_REGEX.test(trimmed)) {
    return {
      valid: false,
      code: 'CUSTOM_WAREHOUSE_INVALID',
      error: `Custom catalog warehouse "${trimmed}" must include a URI scheme.`,
    };
  }
  return { valid: true };
}

/**
 * Returns true when a Spark configuration key is static and cannot appear in spark-pipeline.yml configuration.
 *
 * @param key - Spark property key name.
 * @returns True if the key must be routed to runner execution_config.properties or --conf.
 */
export function isStaticSdpSparkConfigKey(key: string): boolean {
  const trimmed = key.trim();
  if (!trimmed) {
    return false;
  }
  if (STATIC_SDP_SPARK_EXACT_KEYS.has(trimmed)) {
    return true;
  }
  return STATIC_SDP_SPARK_KEY_PREFIXES.some((prefix) => trimmed.startsWith(prefix));
}

/**
 * Builds declarative Spark SQL catalog entries for the BigLake Iceberg REST catalog.
 *
 * @param options - Catalog alias, mode, project, catalog ID, or warehouse bucket inputs.
 * @returns String-valued Spark configuration map for spark-pipeline.yml configuration.
 */
export function buildBigLakeRestCatalogConfig(
  options: BigLakeCatalogConfigOptions
): Record<string, string> {
  if (options.mode === 'none') {
    return {};
  }

  const alias = (options.catalogAlias ?? 'lake').trim() || 'lake';
  const prefix = `spark.sql.catalog.${alias}`;
  const project = (options.project ?? '').trim() || '<project>';

  if (options.mode === 'custom') {
    const customWarehouse = (options.warehouseBucket ?? '').trim();
    if (!customWarehouse) {
      return {};
    }
    return {
      [prefix]: 'org.apache.iceberg.spark.SparkCatalog',
      [`${prefix}.type`]: 'hadoop',
      [`${prefix}.warehouse`]: customWarehouse,
    };
  }

  let warehouse: string;
  if (options.mode === 'lakehouse') {
    const rawBucket = (options.warehouseBucket ?? '').trim();
    if (rawBucket.startsWith('bl://')) {
      warehouse = rawBucket;
    } else {
      const catalogId = (options.catalogId ?? '').trim() || '<catalog_id>';
      warehouse = `bl://projects/${project}/catalogs/${catalogId}`;
    }
  } else {
    const rawBucket = (options.warehouseBucket ?? '').trim();
    if (!rawBucket) {
      warehouse = 'gs://<bucket>';
    } else if (rawBucket.startsWith('gs://')) {
      warehouse = rawBucket;
    } else {
      warehouse = `gs://${rawBucket}`;
    }
  }

  return {
    [prefix]: 'org.apache.iceberg.spark.SparkCatalog',
    [`${prefix}.type`]: 'rest',
    [`${prefix}.uri`]: 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
    [`${prefix}.warehouse`]: warehouse,
    [`${prefix}.header.x-goog-user-project`]: project,
    [`${prefix}.rest.auth.type`]: 'org.apache.iceberg.gcp.auth.GoogleAuthManager',
    [`${prefix}.io-impl`]: 'org.apache.iceberg.gcp.gcs.GCSFileIO',
  };
}

/**
 * Splits Spark settings into dynamic spark-pipeline.yml entries and static runner properties.
 *
 * Static settings (spark.jars*, spark.sql.extensions, spark.sql.catalogImplementation,
 * spark.driver.*, spark.executor.*, spark.dynamicAllocation.*, spark.files, spark.archives)
 * always route to staticRunnerConfig so spark-pipeline.yml never fails with CANNOT_MODIFY_STATIC_CONFIG.
 *
 * @param settings - Raw Spark key-value configuration map.
 * @param bigLakeOptions - Optional BigLake catalog options merged into dynamicSpecConfig.
 * @returns Partitioned dynamic spec configuration and static runner configuration maps.
 */
export function partitionSdpSparkSettings(
  settings: Record<string, string>,
  bigLakeOptions?: BigLakeCatalogConfigOptions
): PartitionedSdpSparkSettings {
  const dynamicSpecConfig: Record<string, string> = bigLakeOptions
    ? { ...buildBigLakeRestCatalogConfig(bigLakeOptions) }
    : {};
  const staticRunnerConfig: Record<string, string> = {};

  for (const [rawKey, rawVal] of Object.entries(settings ?? {})) {
    const key = rawKey.trim();
    if (!key) {
      continue;
    }
    const strVal = String(rawVal ?? '');
    if (isStaticSdpSparkConfigKey(key)) {
      staticRunnerConfig[key] = strVal;
    } else {
      dynamicSpecConfig[key] = strVal;
    }
  }

  return { dynamicSpecConfig, staticRunnerConfig };
}

/**
 * Parses a key=value multiline string or object map into a normalized string dictionary.
 *
 * @param raw - Multiline key=value text or key-value record stored on jobConfig.sdpStaticSparkConf.
 * @returns Parsed Spark configuration map.
 */
export function parseSparkConfEntries(
  raw?: Record<string, string> | string | null
): Record<string, string> {
  if (!raw) {
    return {};
  }
  if (typeof raw === 'object') {
    const normalized: Record<string, string> = {};
    for (const [k, v] of Object.entries(raw)) {
      const trimmedKey = k.trim();
      if (trimmedKey && v !== undefined && v !== null) {
        normalized[trimmedKey] = String(v);
      }
    }
    return normalized;
  }

  const result: Record<string, string> = {};
  const lines = raw.split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }
    const eqIndex = trimmed.indexOf('=');
    if (eqIndex <= 0) {
      continue;
    }
    const key = trimmed.slice(0, eqIndex).trim();
    const value = trimmed.slice(eqIndex + 1).trim();
    if (key) {
      result[key] = value;
    }
  }
  return result;
}

/**
 * Formats a Spark configuration dictionary or raw string into multiline key=value text for editing.
 *
 * @param raw - Raw dictionary or multiline string from jobConfig.sdpStaticSparkConf.
 * @returns Editable multiline text.
 */
export function formatSparkConfEntries(
  raw?: Record<string, string> | string | null
): string {
  if (typeof raw === 'string') {
    return raw;
  }
  if (!raw || typeof raw !== 'object') {
    return '';
  }
  return Object.entries(raw)
    .map(([k, v]) => `${k}=${String(v ?? '')}`)
    .join('\n');
}
