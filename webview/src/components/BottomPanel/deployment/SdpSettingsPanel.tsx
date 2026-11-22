/**
 * @fileoverview Pipeline-level Spark Declarative Pipelines settings panel (SDP3).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React from 'react';
import { InfoTooltip } from '../../InfoTooltip';
import { useAssetLoader } from '../../../hooks/useAssetLoader';
import {
  validateSdpStorageUri,
  validateSdpCatalogAlias,
  validateSdpDatabaseName,
  validateBigLakeWarehouse,
  partitionSdpSparkSettings,
  extractGraphSourcePaths,
  parseSparkConfEntries,
  formatSparkConfEntries,
  type SdpBigLakeConfigMode,
} from '../../../utils/sdpConfigValidation';

/**
 * Props for the {@link SdpSettingsPanel} component.
 */
export interface SdpSettingsPanelProps<
  T extends Record<string, unknown> = Record<string, unknown>,
> {
  /** Active pipeline job configuration state. */
  jobConfig: T;
  /** Functional state updater for modifying jobConfig without clobbering sibling keys. */
  setJobConfig: React.Dispatch<React.SetStateAction<T>>;
  /** Optional graph nodes used to detect source folder URI overlaps. */
  nodes?: readonly unknown[];
  /** Optional resolved GCP project identifier fallback. */
  projectId?: string;
}

/**
 * Renders pipeline-level Spark Declarative Pipeline storage, catalog, BigLake REST, and Spark property routing controls.
 *
 * @param props - Job configuration, functional updater, graph nodes, and project ID.
 * @returns Rendered SDP pipeline settings card.
 */
export function SdpSettingsPanel<
  T extends Record<string, unknown> = Record<string, unknown>,
>({
  jobConfig,
  setJobConfig,
  nodes = [],
  projectId,
}: SdpSettingsPanelProps<T>): React.ReactElement {
  const sdpStorage = typeof jobConfig.sdpStorage === 'string' ? jobConfig.sdpStorage : '';
  const sdpCatalog =
    typeof jobConfig.sdpCatalog === 'string' ? jobConfig.sdpCatalog : 'lake';
  const sdpDatabase = typeof jobConfig.sdpDatabase === 'string' ? jobConfig.sdpDatabase : '';
  const sdpBigLakeMode: SdpBigLakeConfigMode =
    jobConfig.sdpBigLakeMode === 'gcs_bucket' ||
    jobConfig.sdpBigLakeMode === 'custom' ||
    jobConfig.sdpBigLakeMode === 'none'
      ? jobConfig.sdpBigLakeMode
      : 'lakehouse';
  const sdpBigLakeProject =
    typeof jobConfig.sdpBigLakeProject === 'string'
      ? jobConfig.sdpBigLakeProject
      : typeof jobConfig.projectId === 'string' && jobConfig.projectId !== 'YOUR_PROJECT_ID'
        ? jobConfig.projectId
        : projectId && projectId !== 'YOUR_PROJECT_ID'
          ? projectId
          : '';
  const sdpBigLakeCatalogId =
    typeof jobConfig.sdpBigLakeCatalogId === 'string' ? jobConfig.sdpBigLakeCatalogId : '';
  const sdpBigLakeWarehouseBucket =
    typeof jobConfig.sdpBigLakeWarehouseBucket === 'string'
      ? jobConfig.sdpBigLakeWarehouseBucket
      : '';
  const sdpCreateNamespace =
    typeof jobConfig.sdpCreateNamespace === 'boolean' ? jobConfig.sdpCreateNamespace : true;
  const rawStaticSparkConf: string | Record<string, string> | null | undefined =
    typeof jobConfig.sdpStaticSparkConf === 'string' ||
    (typeof jobConfig.sdpStaticSparkConf === 'object' &&
      jobConfig.sdpStaticSparkConf !== null &&
      !Array.isArray(jobConfig.sdpStaticSparkConf))
      ? (jobConfig.sdpStaticSparkConf as string | Record<string, string>)
      : undefined;
  const sdpStaticSparkConfText = formatSparkConfEntries(rawStaticSparkConf);

  // Discover BigLake catalogs and Cloud Storage buckets when applicable
  const { items: rawBigLakeCatalogs } = useAssetLoader<
    string | { id?: string; name?: string; value?: string }
  >('bigLakeCatalogs', {
    enabled: sdpBigLakeMode === 'lakehouse',
  });
  const { items: rawBuckets } = useAssetLoader<string>('storageBuckets', {
    enabled: sdpBigLakeMode === 'gcs_bucket',
  });

  const catalogSuggestions = React.useMemo(() => {
    const seen = new Set<string>();
    const list: string[] = [];
    for (const item of rawBigLakeCatalogs) {
      const raw =
        typeof item === 'string'
          ? item.trim()
          : (item?.id || item?.value || item?.name || '').trim();
      if (!raw) continue;
      const parts = raw.replace(/\/+$/, '').split('/');
      const shortId = parts[parts.length - 1] || '';
      if (shortId && !seen.has(shortId)) {
        seen.add(shortId);
        list.push(shortId);
      }
    }
    return list;
  }, [rawBigLakeCatalogs]);

  const bucketSuggestions = React.useMemo(() => {
    return rawBuckets
      .map((b) => {
        const trimmed = typeof b === 'string' ? b.trim().replace(/\/+$/, '') : '';
        if (!trimmed) return '';
        return trimmed.startsWith('gs://') ? trimmed : `gs://${trimmed}`;
      })
      .filter(Boolean);
  }, [rawBuckets]);

  const sourcePaths = React.useMemo(() => extractGraphSourcePaths(nodes), [nodes]);
  const storageValidation =
    sdpStorage.trim().length > 0
      ? validateSdpStorageUri(sdpStorage, sourcePaths)
      : { valid: true };
  const catalogValidation = validateSdpCatalogAlias(sdpCatalog);
  const databaseValidation = validateSdpDatabaseName(sdpDatabase);

  const computedLakehouseUri =
    sdpBigLakeProject.trim() && sdpBigLakeCatalogId.trim()
      ? `bl://projects/${sdpBigLakeProject.trim()}/catalogs/${sdpBigLakeCatalogId.trim()}`
      : '';

  const warehouseToValidate =
    sdpBigLakeMode === 'lakehouse' ? computedLakehouseUri : sdpBigLakeWarehouseBucket;
  const shouldValidateWarehouse =
    sdpBigLakeMode === 'lakehouse'
      ? Boolean(sdpBigLakeProject.trim() && sdpBigLakeCatalogId.trim())
      : sdpBigLakeMode !== 'none' && sdpBigLakeWarehouseBucket.trim().length > 0;
  const warehouseValidation = shouldValidateWarehouse
    ? validateBigLakeWarehouse(sdpBigLakeMode, warehouseToValidate)
    : { valid: true };

  const parsedSparkConf = React.useMemo(
    () => parseSparkConfEntries(rawStaticSparkConf),
    [rawStaticSparkConf]
  );

  const partitionedConf = React.useMemo(
    () =>
      partitionSdpSparkSettings(parsedSparkConf, {
        catalogAlias: sdpCatalog || 'lake',
        mode: sdpBigLakeMode,
        project: sdpBigLakeProject || undefined,
        catalogId: sdpBigLakeCatalogId || undefined,
        warehouseBucket: sdpBigLakeWarehouseBucket || undefined,
      }),
    [
      parsedSparkConf,
      sdpCatalog,
      sdpBigLakeMode,
      sdpBigLakeProject,
      sdpBigLakeCatalogId,
      sdpBigLakeWarehouseBucket,
    ]
  );

  const staticKeys = Object.keys(partitionedConf.staticRunnerConfig);
  const dynamicKeys = Object.keys(partitionedConf.dynamicSpecConfig);

  return (
    <div
      data-testid="sdp-settings-panel"
      className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-lg border border-[var(--vscode-border)]"
    >
      <div className="text-xs font-bold uppercase tracking-wider text-purple-400">
        Declarative Pipeline Spec (SDP 4.2+)
      </div>

      {/* Pipeline Storage Root URI */}
      <div className="flex flex-col gap-1">
        <label
          htmlFor="sdpStorageUri"
          className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
        >
          <span>Pipeline Storage URI</span>
          <InfoTooltip text="Required root location storing streaming flow checkpoints and state outside all input data folders." />
        </label>
        <input
          id="sdpStorageUri"
          name="sdpStorage"
          type="text"
          value={sdpStorage}
          onChange={(e) => {
            const nextValue = e.target.value;
            setJobConfig((prev) => ({ ...prev, sdpStorage: nextValue }));
          }}
          placeholder="gs://<bucket>/pipelines/<pipeline_name>"
          aria-invalid={!storageValidation.valid}
          className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-xs font-mono focus:outline-none ${
            !storageValidation.valid
              ? 'border-amber-500 focus:border-amber-400'
              : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
          }`}
        />
        {!storageValidation.valid && storageValidation.error && (
          <span
            role="alert"
            data-testid="sdp-storage-error"
            className="text-[11px] text-amber-400 leading-snug"
          >
            {storageValidation.error}
          </span>
        )}
      </div>

      {/* Spark Catalog Alias & Target Database */}
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-1">
          <label
            htmlFor="sdpCatalogAlias"
            className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
          >
            <span>Catalog Alias</span>
            <InfoTooltip text="Plain Spark identifier (such as lake) written to the spec. Avoid hyphens because unquoted catalog names fail." />
          </label>
          <input
            id="sdpCatalogAlias"
            name="sdpCatalog"
            type="text"
            value={sdpCatalog}
            onChange={(e) => {
              const nextValue = e.target.value;
              setJobConfig((prev) => ({ ...prev, sdpCatalog: nextValue }));
            }}
            placeholder="lake"
            aria-invalid={!catalogValidation.valid}
            className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-xs font-mono focus:outline-none ${
              !catalogValidation.valid
                ? 'border-amber-500 focus:border-amber-400'
                : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
            }`}
          />
        </div>

        <div className="flex flex-col gap-1">
          <label
            htmlFor="sdpDatabaseName"
            className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
          >
            <span>Target Database</span>
            <InfoTooltip text="Default schema or namespace where declared streaming tables and materialized views register." />
          </label>
          <input
            id="sdpDatabaseName"
            name="sdpDatabase"
            type="text"
            value={sdpDatabase}
            onChange={(e) => {
              const nextValue = e.target.value;
              setJobConfig((prev) => ({ ...prev, sdpDatabase: nextValue }));
            }}
            placeholder="retail_medallion"
            aria-invalid={!databaseValidation.valid}
            className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-xs font-mono focus:outline-none ${
              !databaseValidation.valid
                ? 'border-amber-500 focus:border-amber-400'
                : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
            }`}
          />
        </div>
      </div>

      {!catalogValidation.valid && catalogValidation.error && (
        <span
          role="alert"
          data-testid="sdp-catalog-error"
          className="text-[11px] text-amber-400 leading-snug"
        >
          {catalogValidation.error}
        </span>
      )}

      {!databaseValidation.valid && databaseValidation.error && (
        <span
          role="alert"
          data-testid="sdp-database-error"
          className="text-[11px] text-amber-400 leading-snug"
        >
          {databaseValidation.error}
        </span>
      )}

      {/* BigLake Iceberg REST Catalog Mode */}
      <div className="flex flex-col gap-1">
        <label
          htmlFor="sdpBigLakeMode"
          className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
        >
          <span>BigLake Catalog Mode</span>
          <InfoTooltip text="Configures declarative Iceberg REST catalog entries so ephemeral Serverless batches retain table metadata across runs." />
        </label>
        <select
          id="sdpBigLakeMode"
          name="sdpBigLakeMode"
          value={sdpBigLakeMode}
          onChange={(e) => {
            const nextMode = e.target.value as SdpBigLakeConfigMode;
            setJobConfig((prev) => ({ ...prev, sdpBigLakeMode: nextMode }));
          }}
          className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs focus:outline-none focus:border-[var(--vscode-highlight)] cursor-pointer"
        >
          <option value="lakehouse">BigLake Lakehouse (bl://projects/.../catalogs/...)</option>
          <option value="gcs_bucket">BigLake GCS Bucket REST (gs://&lt;bucket&gt;)</option>
          <option value="custom">Custom Iceberg Warehouse URI</option>
          <option value="none">None (Manual Configuration)</option>
        </select>
      </div>

      {/* BigLake Lakehouse Fields */}
      {sdpBigLakeMode === 'lakehouse' && (
        <div className="flex flex-col gap-2">
          <div className="grid grid-cols-2 gap-2">
            <div className="flex flex-col gap-1">
              <label
                htmlFor="sdpBigLakeProject"
                className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
              >
                <span>BigLake Project</span>
                <InfoTooltip text="Google Cloud project hosting the BigLake catalog and x-goog-user-project header." />
              </label>
              <input
                id="sdpBigLakeProject"
                name="sdpBigLakeProject"
                type="text"
                value={sdpBigLakeProject}
                onChange={(e) => {
                  const nextValue = e.target.value;
                  setJobConfig((prev) => ({ ...prev, sdpBigLakeProject: nextValue }));
                }}
                placeholder="my-gcp-project"
                className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
              />
            </div>

            <div className="flex flex-col gap-1">
              <label
                htmlFor="sdpBigLakeCatalogId"
                className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
              >
                <span>BigLake Catalog ID</span>
                <InfoTooltip text="Lakehouse catalog identifier mapped to the plain Spark alias." />
              </label>
              <input
                id="sdpBigLakeCatalogId"
                name="sdpBigLakeCatalogId"
                type="text"
                list="sdpBigLakeCatalogsList"
                value={sdpBigLakeCatalogId}
                onChange={(e) => {
                  const nextValue = e.target.value;
                  setJobConfig((prev) => ({ ...prev, sdpBigLakeCatalogId: nextValue }));
                }}
                placeholder="retail_iceberg"
                className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
              />
              <datalist id="sdpBigLakeCatalogsList">
                {catalogSuggestions.map((catId) => (
                  <option key={catId} value={catId}>
                    {catId}
                  </option>
                ))}
              </datalist>
            </div>
          </div>

          <div
            data-testid="sdp-four-part-contract-note"
            className="text-[11px] text-[var(--vscode-text-muted)] leading-snug"
          >
            Downstream BigQuery and dbt queries reference{' '}
            <code className="font-mono text-slate-300">
              {sdpBigLakeProject.trim() || '<project>'}.
              {sdpBigLakeCatalogId.trim() || '<catalog_id>'}.
              {sdpDatabase.trim() || '<database>'}.&lt;table&gt;
            </code>
            .
          </div>
        </div>
      )}

      {/* BigLake GCS Bucket REST Fields */}
      {sdpBigLakeMode === 'gcs_bucket' && (
        <div className="flex flex-col gap-2">
          <div className="flex flex-col gap-1">
            <label
              htmlFor="sdpBigLakeProject"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>BigLake Project</span>
              <InfoTooltip text="Google Cloud billing project sent via x-goog-user-project." />
            </label>
            <input
              id="sdpBigLakeProject"
              name="sdpBigLakeProject"
              type="text"
              value={sdpBigLakeProject}
              onChange={(e) => {
                const nextValue = e.target.value;
                setJobConfig((prev) => ({ ...prev, sdpBigLakeProject: nextValue }));
              }}
              placeholder="my-gcp-project"
              className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
            />
          </div>

          <div className="flex flex-col gap-1">
            <label
              htmlFor="sdpBigLakeWarehouseBucket"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Warehouse Bucket Root</span>
              <InfoTooltip text="Root Cloud Storage bucket URI only. Appending /warehouse fails on BigLake REST GCS catalogs." />
            </label>
            <input
              id="sdpBigLakeWarehouseBucket"
              name="sdpBigLakeWarehouseBucket"
              type="text"
              list="sdpBigLakeBucketsList"
              value={sdpBigLakeWarehouseBucket}
              onChange={(e) => {
                const nextValue = e.target.value;
                setJobConfig((prev) => ({
                  ...prev,
                  sdpBigLakeWarehouseBucket: nextValue,
                }));
              }}
              placeholder="gs://<bucket>"
              aria-invalid={!warehouseValidation.valid}
              className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-xs font-mono focus:outline-none ${
                !warehouseValidation.valid
                  ? 'border-amber-500 focus:border-amber-400'
                  : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
              }`}
            />
            <datalist id="sdpBigLakeBucketsList">
              {bucketSuggestions.map((b) => (
                <option key={b} value={b}>
                  {b}
                </option>
              ))}
            </datalist>
          </div>
        </div>
      )}

      {/* Custom Warehouse Field */}
      {sdpBigLakeMode === 'custom' && (
        <div className="flex flex-col gap-1">
          <label
            htmlFor="sdpBigLakeWarehouseBucket"
            className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
          >
            <span>Custom Warehouse URI</span>
            <InfoTooltip text="Explicit Iceberg catalog warehouse path with scheme." />
          </label>
          <input
            id="sdpBigLakeWarehouseBucket"
            name="sdpBigLakeWarehouseBucket"
            type="text"
            value={sdpBigLakeWarehouseBucket}
            onChange={(e) => {
              const nextValue = e.target.value;
              setJobConfig((prev) => ({
                ...prev,
                sdpBigLakeWarehouseBucket: nextValue,
              }));
            }}
            placeholder="gs://<bucket>/warehouse"
            aria-invalid={!warehouseValidation.valid}
            className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-xs font-mono focus:outline-none ${
              !warehouseValidation.valid
                ? 'border-amber-500 focus:border-amber-400'
                : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
            }`}
          />
        </div>
      )}

      {!warehouseValidation.valid && warehouseValidation.error && (
        <span
          role="alert"
          data-testid="sdp-biglake-warehouse-error"
          className="text-[11px] text-amber-400 leading-snug"
        >
          {warehouseValidation.error}
        </span>
      )}

      {/* Create Target Namespace Toggle */}
      <label
        htmlFor="sdpCreateNamespace"
        className="flex items-center gap-2 text-xs text-[var(--vscode-text-primary)] cursor-pointer select-none"
      >
        <input
          id="sdpCreateNamespace"
          name="sdpCreateNamespace"
          type="checkbox"
          checked={sdpCreateNamespace}
          onChange={(e) => {
            const checked = e.target.checked;
            setJobConfig((prev) => ({ ...prev, sdpCreateNamespace: checked }));
          }}
          className="rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] text-purple-500 focus:ring-0 cursor-pointer"
        />
        <span className="font-semibold">Create Target Namespace Before Run</span>
        <InfoTooltip text="Executes CREATE NAMESPACE IF NOT EXISTS before standalone runs because SDP does not create missing databases." />
      </label>

      {/* Spark & Runner Properties with Automatic Static Config Routing */}
      <div className="flex flex-col gap-1">
        <label
          htmlFor="sdpStaticSparkConf"
          className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
        >
          <span>Spark &amp; Execution Properties</span>
          <InfoTooltip text="Enter key=value lines. Static cluster keys route automatically to runner execution properties instead of the spec." />
        </label>
        <textarea
          id="sdpStaticSparkConf"
          name="sdpStaticSparkConf"
          rows={3}
          value={sdpStaticSparkConfText}
          onChange={(e) => {
            const nextValue = e.target.value;
            setJobConfig((prev) => ({ ...prev, sdpStaticSparkConf: nextValue }));
          }}
          placeholder={
            'spark.sql.shuffle.partitions=200\nspark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
          }
          className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)] resize-y"
        />
        <div
          data-testid="sdp-config-routing-summary"
          className="text-[11px] text-[var(--vscode-text-muted)] leading-snug"
        >
          Spec configuration entries: {dynamicKeys.length}. Runner static entries:{' '}
          {staticKeys.length}.
        </div>
        {staticKeys.length > 0 && (
          <div
            data-testid="sdp-static-config-routing-notice"
            className="p-2 rounded bg-blue-950/40 border border-blue-700/50 text-[11px] text-blue-200 leading-snug"
          >
            Static settings ({staticKeys.join(', ')}) route automatically to runner execution
            properties and CLI --conf flags to prevent CANNOT_MODIFY_STATIC_CONFIG errors.
          </div>
        )}
      </div>
    </div>
  );
}
