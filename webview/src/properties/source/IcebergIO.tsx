/**
 * IcebergIO Source Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark IcebergSource (`IcebergIO`) nodes.
 * Configures batch (`spark.read.format("iceberg")`) and streaming (`spark.readStream.format("iceberg")`)
 * reads from Apache Iceberg tables across Cloud Storage (`gs://`), Amazon S3 (`s3a://`), HDFS (`hdfs://`),
 * or local filesystems using BigLake REST, REST, Hive, Hadoop, AWS Glue, Project Nessie, or BigQuery
 * Metastore catalogs.
 *
 * Architectural Decisions:
 * - Supports `readMode` (`'batch' | 'streaming'`, default `'batch'`).
 * - Preserves Phase 4 D3 BigLake catalog discovery (`useAssetLoader` `bigLakeCatalogs`) with
 *   `normalizeBigLakeCatalogOptions` and inline error alerting.
 * - Supports `catalogName`, `catalogType` (`'biglake' | 'rest' | 'hive' | 'hadoop' | 'glue' | 'nessie' | 'bigquery'`),
 *   `namespace`, `table`, and `warehouse` (`warehouseLocation`) with Cloud Storage bucket autocomplete via `StoragePathField`.
 * - Enforces mutual exclusivity between historical `snapshotId` and `asOfTimestamp` in batch mode.
 * - Exposes streaming incremental options (`streamingFromTimestamp`, `streamingSkipDeleteSnapshots`,
 *   and `streamingSkipOverwriteSnapshots`) when `readMode === 'streaming'`.
 * - Integrates `SecretManagerAuthFields`, `CollapsibleSection`, and `SystemInstructionsField` on the 8pt grid.
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import { Sliders } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import type { PropertyFieldOption } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { StoragePathField } from '../../components/common/StoragePathField';
import { SecretManagerAuthFields } from '../../components/common/SecretManagerAuthFields';
import { useAssetLoader } from '../../hooks/useAssetLoader';

/** Discovered BigLake Iceberg catalog descriptor from the extension host. */
export interface BigLakeCatalogOption {
  name?: string;
  id?: string;
  value?: string;
  warehouse?: string;
  catalogType?: string;
  [key: string]: unknown;
}

/**
 * Normalizes raw BigLake catalog items (strings or objects) into PropertyField datalist options.
 *
 * @param items Raw catalog items from `useAssetLoader('bigLakeCatalogs')`.
 * @returns Normalized datalist options.
 */
export function normalizeBigLakeCatalogOptions(
  items: Array<string | BigLakeCatalogOption>
): PropertyFieldOption[] {
  const seen = new Set<string>();
  const options: PropertyFieldOption[] = [];
  for (const item of items || []) {
    const raw =
      typeof item === 'string'
        ? item.trim()
        : (item?.value || item?.id || item?.name || '').trim();
    if (!raw) continue;
    const stripped = raw.replace(/^gs:\/\//, '').replace(/\/+$/, '');
    const parts = stripped.split('/').filter(Boolean);
    const shortName = parts[parts.length - 1] || '';
    if (!shortName || seen.has(shortName)) continue;
    seen.add(shortName);
    const warehouse =
      typeof item === 'object' && item !== null && typeof item.warehouse === 'string'
        ? item.warehouse.trim()
        : '';
    options.push({
      value: shortName,
      label: warehouse ? `${shortName} (${warehouse})` : shortName,
    });
  }
  return options;
}

/** Read mode options for IcebergSource. */
export const ICEBERG_READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'Batch (spark.read.format("iceberg"))' },
  { value: 'streaming', label: 'Streaming (spark.readStream.format("iceberg"))' },
];

/** Supported Iceberg catalog integration types for Spark Iceberg table resolution. */
export const ICEBERG_CATALOG_TYPES: SelectFieldOption[] = [
  { value: 'biglake', label: 'BigLake Metastore / Iceberg REST Catalog' },
  { value: 'rest', label: 'REST Catalog' },
  { value: 'hive', label: 'Hive Metastore (HMS)' },
  { value: 'hadoop', label: 'Hadoop (Filesystem catalog)' },
  { value: 'glue', label: 'AWS Glue Data Catalog' },
  { value: 'nessie', label: 'Project Nessie Catalog' },
  { value: 'bigquery', label: 'BigQuery Metastore' },
];

/** React Flow node shape for the IcebergIO source panel. */
export interface IcebergIOSourceNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      readMode?: 'batch' | 'streaming';
      table?: string;
      namespace?: string;
      catalogName?: string;
      catalogType?: string;
      warehouse?: string;
      warehouseLocation?: string;
      catalogUri?: string;
      snapshotId?: string | number;
      asOfTimestamp?: string;
      streamingFromTimestamp?: string | number;
      streamingSkipDeleteSnapshots?: boolean;
      streamingSkipOverwriteSnapshots?: boolean;
      filterExpression?: string;
      columnProjection?: string;
      authMode?: string;
      secretName?: string;
      configProperties?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link IcebergIOSourceProperties} component. */
export interface IcebergIOSourcePropertiesProps {
  node: IcebergIOSourceNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * IcebergIOSourceProperties component renders configuration fields for Apache Spark `.format("iceberg")` reads.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React component.
 */
export const IcebergIOSourceProperties: React.FC<IcebergIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};
  const readMode = properties.readMode || 'batch';
  const table = properties.table || '';
  const namespace = properties.namespace || '';
  const catalogName = properties.catalogName || '';
  const catalogType = properties.catalogType || 'hadoop';
  const warehouseLocation = properties.warehouseLocation ?? properties.warehouse ?? '';
  const catalogUri = properties.catalogUri || '';
  const snapshotId = properties.snapshotId ?? '';
  const asOfTimestamp = properties.asOfTimestamp || '';
  const streamingFromTimestamp = properties.streamingFromTimestamp ?? '';
  const streamingSkipDeleteSnapshots = properties.streamingSkipDeleteSnapshots ?? false;
  const streamingSkipOverwriteSnapshots = properties.streamingSkipOverwriteSnapshots ?? false;
  const filterExpression = properties.filterExpression || '';
  const columnProjection = properties.columnProjection || '';
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';
  const configProperties = properties.configProperties || '';

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

  // D3: Request BigLake Iceberg catalogs via useAssetLoader for datalist autocompletion
  const {
    items: rawCatalogs,
    loading: catalogsLoading,
    error: catalogError,
  } = useAssetLoader<string | BigLakeCatalogOption>('bigLakeCatalogs');
  const catalogOptions = normalizeBigLakeCatalogOptions(rawCatalogs);

  const isTableInvalid = !table.trim();
  const isWarehouseLocationInvalid = !warehouseLocation.trim();

  const hasSnapshotId = typeof snapshotId === 'string' ? snapshotId.trim().length > 0 : Boolean(snapshotId);
  const hasAsOfTimestamp = typeof asOfTimestamp === 'string' ? asOfTimestamp.trim().length > 0 : Boolean(asOfTimestamp);
  const hasTimeTravelConflict = hasSnapshotId && hasAsOfTimestamp;
  const timeTravelErrorMessage = hasTimeTravelConflict
    ? 'Cannot specify both Snapshot ID and As Of Timestamp. Time travel parameters are mutually exclusive.'
    : null;

  const showCatalogUri =
    catalogType === 'rest' ||
    catalogType === 'hive' ||
    catalogType === 'biglake' ||
    catalogType === 'nessie';

  const systemInstructions = resolveSystemInstructions(node);

  /**
   * Handles generic input, select, and checkbox changes for form fields.
   *
   * @param e Form change event from input, select, or textarea.
   */
  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value, type } = e.target;
    const isCheckbox = type === 'checkbox';
    const checked = isCheckbox ? (e.target as HTMLInputElement).checked : undefined;
    const nextValue = isCheckbox ? checked : value;

    if (name === 'warehouseLocation' || name === 'warehouse') {
      onChange({
        ...properties,
        warehouse: nextValue,
        warehouseLocation: nextValue,
      });
      return;
    }

    onChange({
      ...properties,
      [name]: nextValue,
    });
  };

  /**
   * Toggles the advanced tuning accordion state.
   */
  const handleToggleAdvanced = () => {
    setIsAdvancedOpen((prev) => !prev);
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Read Mode Select */}
      <SelectField
        id="icebergSourceReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select batch mode (spark.read.format('iceberg')) for snapshot scans or streaming mode (spark.readStream.format('iceberg')) for incremental append scans."
        value={readMode}
        options={ICEBERG_READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Namespace & Table Identifier */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="icebergSourceNamespace"
          name="namespace"
          label="Namespace / Database"
          tooltip="Optional Iceberg namespace or database name (e.g. analytics). You can also provide a fully-qualified identifier in Table Identifier."
          value={namespace}
          onChange={handleChange}
          placeholder="analytics"
          mono={true}
        />

        <PropertyField
          id="icebergSourceTable"
          name="table"
          label="Table Identifier"
          required="Table is required."
          tooltip="Iceberg table identifier (e.g. db.table or catalog.db.table). Required."
          aria-required="true"
          aria-invalid={isTableInvalid}
          value={table}
          onChange={handleChange}
          placeholder="db.table"
          invalid={isTableInvalid}
          error={isTableInvalid ? 'Table is required.' : null}
          mono={true}
          helperText={
            !isTableInvalid ? (
              <span className="text-xs text-[var(--vscode-text-muted)]">
                Fully-qualified database and table name in the Iceberg catalog.
              </span>
            ) : null
          }
        />
      </div>

      {/* Catalog Name Input */}
      <PropertyField
        id="icebergSourceCatalogName"
        name="catalogName"
        label="Catalog Name"
        tooltip="Name of the Spark Iceberg catalog instance configured in spark.sql.catalog.<catalogName> (e.g. lakehouse or biglake)."
        value={catalogName}
        onChange={handleChange}
        placeholder="lakehouse"
        list="iceberg-source-catalogs-list"
        datalist={catalogOptions}
        loading={catalogsLoading}
        mono={true}
        helperText={
          <span className="text-[11px] text-[var(--vscode-text-muted)]">
            Logical catalog name configured under <code>spark.sql.catalog.&lt;catalogName&gt;</code>.
          </span>
        }
      />
      {catalogError && (
        <span className="text-xs text-red-400 mt-1" role="alert">
          Failed to load BigLake catalogs: {catalogError}
        </span>
      )}

      {/* Catalog Type Select */}
      <SelectField
        id="icebergSourceCatalogType"
        name="catalogType"
        label="Catalog Type"
        tooltip="Iceberg catalog implementation type (biglake, rest, hive, hadoop, glue, nessie, or bigquery)."
        value={catalogType}
        options={ICEBERG_CATALOG_TYPES}
        onChange={handleChange}
        helperText={
          catalogType === 'glue' ? (
            <span>
              Uses AWS Glue Data Catalog (<code>GlueCatalog</code>) with <code>S3FileIO</code> for S3 warehouses.
            </span>
          ) : catalogType === 'bigquery' ? (
            <span>
              Uses BigQuery Metastore (<code>BigQueryMetastoreCatalog</code>) with <code>GCSFileIO</code> for Cloud Storage warehouses.
            </span>
          ) : catalogType === 'nessie' ? (
            <span>
              Uses Project Nessie Catalog (<code>NessieCatalog</code>) with branch and tag version references.
            </span>
          ) : (
            <span>
              Catalog implementation type configuring table metadata lookup. Defaults to <code>hadoop</code>.
            </span>
          )
        }
      />

      {/* Warehouse Location Input with Cloud Storage bucket autocomplete */}
      <StoragePathField
        id="icebergSourceWarehouseLocation"
        name="warehouseLocation"
        label="Warehouse Location"
        tooltip="Base storage path for Iceberg metadata and data files (e.g. gs://lakehouse/iceberg/warehouse). Required."
        value={warehouseLocation}
        onChange={handleChange}
        placeholder="gs://lakehouse/iceberg/warehouse"
        errorMessage="Warehouse Location is required."
        helperText={
          !isWarehouseLocationInvalid ? (
            <span className="text-xs text-[var(--vscode-text-muted)]">
              Base storage path where Iceberg table metadata and data files reside.
            </span>
          ) : null
        }
      />

      {/* Conditional Catalog URI (Shown for BigLake, REST, Hive, and Nessie catalogs) */}
      {showCatalogUri && (
        <PropertyField
          id="icebergSourceCatalogUri"
          name="catalogUri"
          label="Catalog URI"
          tooltip="Endpoint URI for BigLake REST, Iceberg REST, Hive Metastore, or Project Nessie (for example https://biglake.googleapis.com/iceberg/v1beta/restcatalog, thrift://metastore:9083, or http://nessie:19120/api/v1)."
          value={catalogUri}
          onChange={handleChange}
          placeholder={
            catalogType === 'hive'
              ? 'thrift://metastore:9083'
              : catalogType === 'nessie'
                ? 'http://nessie:19120/api/v1'
                : catalogType === 'biglake'
                  ? 'https://biglake.googleapis.com/iceberg/v1beta/restcatalog'
                  : 'http://rest-catalog:8181'
          }
          mono={true}
          helperText={
            <span className="text-[11px] text-[var(--vscode-text-muted)]">
              {catalogType === 'hive'
                ? 'Thrift URI for Hive Metastore (HMS).'
                : catalogType === 'nessie'
                  ? 'HTTP or HTTPS endpoint for Project Nessie API server (for example http://nessie:19120/api/v1).'
                  : catalogType === 'biglake'
                    ? 'HTTPS endpoint for BigLake Iceberg REST Catalog.'
                    : 'HTTP/HTTPS endpoint for Iceberg REST Catalog server.'}
            </span>
          }
        />
      )}

      {/* Batch Time-Travel Historical Query Controls */}
      {readMode === 'batch' && (
        <div className="flex flex-col gap-2 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
          <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)] flex items-center gap-1.5">
            <span>Time Travel / Historical Snapshot</span>
            <InfoTooltip text="Query historical snapshots of the Iceberg table either by snapshot ID (snapshot-id) or timestamp (as-of-timestamp). These modes are mutually exclusive." />
          </span>
          <div className="grid grid-cols-2 gap-3">
            {/* Snapshot ID */}
            <PropertyField
              id="icebergSourceSnapshotId"
              name="snapshotId"
              label="Snapshot ID"
              tooltip="Iceberg snapshot ID (int64) to query for historical time travel (option snapshot-id). Mutually exclusive with As Of Timestamp."
              value={snapshotId}
              onChange={handleChange}
              placeholder="e.g. 1234567890123456789"
              mono={true}
              invalid={hasTimeTravelConflict}
              helperText="Iceberg snapshot ID integer."
            />

            {/* As Of Timestamp */}
            <PropertyField
              id="icebergSourceAsOfTimestamp"
              name="asOfTimestamp"
              label="As Of Timestamp"
              tooltip="Iceberg commit timestamp in epoch milliseconds or ISO-8601 string for historical time travel (option as-of-timestamp). Mutually exclusive with Snapshot ID."
              value={asOfTimestamp}
              onChange={handleChange}
              placeholder="e.g. 2024-01-01T00:00:00Z"
              mono={true}
              invalid={hasTimeTravelConflict}
              helperText="ISO-8601 timestamp string or epoch ms."
            />
          </div>
          {hasTimeTravelConflict && (
            <span className="text-xs text-red-400 mt-1" role="alert">
              {timeTravelErrorMessage}
            </span>
          )}
        </div>
      )}

      {/* Streaming Incremental Options */}
      {readMode === 'streaming' && (
        <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
          <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)] flex items-center gap-1.5">
            <span>Streaming Incremental Scan Options</span>
            <InfoTooltip text="Configure Iceberg structured streaming start timestamp and snapshot handling options." />
          </span>

          <PropertyField
            id="icebergStreamingFromTimestamp"
            name="streamingFromTimestamp"
            label="Streaming From Timestamp (ms)"
            tooltip="Start streaming from the first snapshot after this epoch millisecond timestamp (option stream-from-timestamp)."
            value={streamingFromTimestamp}
            onChange={handleChange}
            placeholder="e.g. 1704067200000"
            mono={true}
          />

          <label htmlFor="icebergStreamingSkipDeleteSnapshots" className="flex items-center gap-2 cursor-pointer">
            <input
              id="icebergStreamingSkipDeleteSnapshots"
              type="checkbox"
              name="streamingSkipDeleteSnapshots"
              checked={streamingSkipDeleteSnapshots}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
            />
            <div className="flex items-center">
              <span className="text-sm font-medium text-slate-300">Skip Delete Snapshots</span>
              <InfoTooltip text="Ignore snapshots that contain row deletions during streaming scans (option streaming-skip-delete-snapshots)." />
            </div>
          </label>

          <label htmlFor="icebergStreamingSkipOverwriteSnapshots" className="flex items-center gap-2 cursor-pointer">
            <input
              id="icebergStreamingSkipOverwriteSnapshots"
              type="checkbox"
              name="streamingSkipOverwriteSnapshots"
              checked={streamingSkipOverwriteSnapshots}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
            />
            <div className="flex items-center">
              <span className="text-sm font-medium text-slate-300">Skip Overwrite Snapshots</span>
              <InfoTooltip text="Ignore snapshots created by overwrite operations during streaming scans (option streaming-skip-overwrite-snapshots)." />
            </div>
          </label>
        </div>
      )}

      {/* Filter Expression (Row filter pushdown) */}
      <PropertyField
        id="icebergSourceFilterExpression"
        name="filterExpression"
        label="Filter Expression (Pushdown)"
        tooltip="SQL row filter expression pushed down to Iceberg during scan planning (e.g. event_date >= '2024-01-01' AND status = 'ACTIVE')."
        value={filterExpression}
        onChange={handleChange}
        placeholder="status = 'ACTIVE' AND amount > 100"
        mono={true}
        helperText={
          <span className="text-[11px] text-[var(--vscode-text-muted)]">
            Pushdown predicate expression evaluated at the storage scan layer.
          </span>
        }
      />

      {/* Column Projection */}
      <PropertyField
        id="icebergSourceColumnProjection"
        name="columnProjection"
        label="Column Projection"
        tooltip="Comma-separated list of columns to read. Iceberg prunes unselected columns from the disk scan."
        value={columnProjection}
        onChange={handleChange}
        placeholder="event_id, timestamp, user_id, action, amount"
        mono={true}
        helperText={
          <span className="text-[11px] text-[var(--vscode-text-muted)]">
            Comma-separated column names for Iceberg column projection pushdown (e.g. <code>id, timestamp, payload</code>).
          </span>
        }
      />

      {/* Authentication and Secret Manager Credentials */}
      <SecretManagerAuthFields
        idPrefix="icebergSource"
        datalistId="iceberg-source-secrets-list"
        authMode={authMode}
        secretName={secretName}
        serviceName="Apache Iceberg"
        onChange={handleChange}
      />

      {/* Advanced Tuning Accordion */}
      <CollapsibleSection
        title="Advanced & Catalog Configuration"
        icon={<Sliders className="w-4 h-4 text-emerald-400" />}
        isOpen={isAdvancedOpen}
        onToggle={handleToggleAdvanced}
      >
        <PropertyField
          id="icebergSourceConfigProperties"
          type="textarea"
          name="configProperties"
          label="Configuration Properties"
          tooltip="Key=value pairs (one per line) passed to Spark Iceberg reader and catalog configuration (e.g. io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO)."
          value={configProperties}
          onChange={handleChange}
          placeholder={'io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO\ns3.endpoint=https://...'}
          rows={3}
          mono={true}
          helperText={
            <span className="text-[11px] text-[var(--vscode-text-muted)]">
              Catalog and Iceberg engine properties passed to Spark options (key=value per line).
            </span>
          }
        />
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="icebergSourceInstructions"
        value={systemInstructions}
        tooltip="Custom instructions provided directly to Gemini when generating PySpark code for this Iceberg reader step."
        placeholder="e.g. Read Iceberg table records, apply column pruning, and return the DataFrame."
        helperText="Additional guidance appended to prompt when synthesizing PySpark code."
        textareaClassName="h-24"
        wrapperClassName="pt-2 border-t border-[var(--vscode-border-light)]"
        onChange={(value) =>
          onChange({
            ...properties,
            system_instructions: value,
          })
        }
      />
    </div>
  );
};

/** Alias matching the canonical Spark transform name `IcebergSource`. */
export const IcebergSourceProperties = IcebergIOSourceProperties;
