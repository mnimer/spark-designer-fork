/**
 * @fileoverview IcebergSink (`IcebergIO`) Properties Component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark `IcebergSink` (`IcebergIO`) nodes.
 * Configures DataFrameWriterV2 (`df.writeTo(...).using("iceberg")`), SQL `MERGE INTO`, and
 * Structured Streaming (`.writeStream.format("iceberg")`) writes across BigLake REST, REST,
 * Hive, and Hadoop catalogs.
 *
 * Architectural Decisions:
 * - Supports live BigLake Iceberg catalog discovery through `useAssetLoader('bigLakeCatalogs')`.
 * - Exposes Spark 4.x Iceberg write operations (`append`, `createOrReplace`, `overwritePartitions`, `merge`).
 * - Validates merge keys or explicit SQL `ON` conditions when `writeOperation` equals `'merge'`.
 * - Exposes partition transform expressions, fanout writer toggle (`fanout-enabled`), and table properties.
 * - Integrates `StoragePathField`, `SecretManagerAuthFields`, `CollapsibleSection`, and `SystemInstructionsField`.
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import { Sliders } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { StoragePathField } from '../../components/common/StoragePathField';
import { SecretManagerAuthFields } from '../../components/common/SecretManagerAuthFields';
import { useAssetLoader } from '../../hooks/useAssetLoader';
import {
  normalizeBigLakeCatalogOptions,
  type BigLakeCatalogOption,
} from '../source/IcebergIO';

/** Supported Iceberg catalog integration types for Spark Iceberg table writes. */
export const ICEBERG_CATALOG_TYPES: SelectFieldOption[] = [
  { value: 'biglake', label: 'BigLake Metastore / Iceberg REST Catalog' },
  { value: 'rest', label: 'REST Catalog' },
  { value: 'hive', label: 'Hive Metastore (HMS)' },
  { value: 'hadoop', label: 'Hadoop (Filesystem catalog)' },
  { value: 'glue', label: 'AWS Glue Data Catalog' },
  { value: 'nessie', label: 'Project Nessie Catalog' },
  { value: 'bigquery', label: 'BigQuery Metastore' },
];

/** Write operation options for Spark 4.x IcebergSink. */
export const ICEBERG_WRITE_OPERATION_OPTIONS: SelectFieldOption[] = [
  { value: 'append', label: 'append (df.writeTo(...).append())' },
  { value: 'createOrReplace', label: 'createOrReplace (df.writeTo(...).createOrReplace())' },
  { value: 'overwritePartitions', label: 'overwritePartitions (Dynamic partition overwrite)' },
  { value: 'merge', label: 'merge (SQL MERGE INTO upsert)' },
];

/** Write operation union type for IcebergSink. */
export type IcebergWriteOperation =
  | 'append'
  | 'createOrReplace'
  | 'overwritePartitions'
  | 'merge';

/** React Flow node shape for the IcebergSink panel. */
export interface IcebergIOSinkNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      namespace?: string;
      table?: string;
      catalogName?: string;
      catalogType?: string;
      warehouse?: string;
      warehouseLocation?: string;
      catalogUri?: string;
      writeOperation?: IcebergWriteOperation;
      mergeKeys?: string;
      mergeCondition?: string;
      partitionFields?: string;
      partitionSpec?: string;
      fanoutEnabled?: boolean;
      tableProperties?: string;
      authMode?: string;
      secretName?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link IcebergIOSinkProperties} component. */
export interface IcebergIOSinkPropertiesProps {
  node: IcebergIOSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders configuration fields for Apache Spark 4.x `IcebergSink` writes.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React element for the Iceberg sink panel.
 */
export const IcebergIOSinkProperties: React.FC<IcebergIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data?.properties || {};
  const namespace = typeof properties.namespace === 'string' ? properties.namespace : '';
  const table = typeof properties.table === 'string' ? properties.table : '';
  const catalogName = typeof properties.catalogName === 'string' ? properties.catalogName : '';
  const catalogType = typeof properties.catalogType === 'string' ? properties.catalogType : 'biglake';
  const warehouseLocation =
    typeof properties.warehouseLocation === 'string'
      ? properties.warehouseLocation
      : typeof properties.warehouse === 'string'
        ? properties.warehouse
        : '';
  const catalogUri = typeof properties.catalogUri === 'string' ? properties.catalogUri : '';
  const writeOperation: IcebergWriteOperation = properties.writeOperation || 'append';
  const mergeKeys = typeof properties.mergeKeys === 'string' ? properties.mergeKeys : '';
  const mergeCondition = typeof properties.mergeCondition === 'string' ? properties.mergeCondition : '';
  const partitionFields =
    typeof properties.partitionFields === 'string'
      ? properties.partitionFields
      : typeof properties.partitionSpec === 'string'
        ? properties.partitionSpec
        : '';
  const fanoutEnabled = Boolean(properties.fanoutEnabled ?? false);
  const tableProperties = typeof properties.tableProperties === 'string' ? properties.tableProperties : '';
  const authMode = typeof properties.authMode === 'string' ? properties.authMode : 'default';
  const secretName = typeof properties.secretName === 'string' ? properties.secretName : '';

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

  // Request BigLake Iceberg catalogs from the extension host for datalist autocomplete.
  const {
    items: rawCatalogs,
    loading: catalogsLoading,
    error: catalogError,
  } = useAssetLoader<string | BigLakeCatalogOption>('bigLakeCatalogs');
  const catalogOptions = normalizeBigLakeCatalogOptions(rawCatalogs);

  const isTableInvalid = !table.trim();
  const isWarehouseLocationInvalid = !warehouseLocation.trim();
  const isMergeMissingCriteria =
    writeOperation === 'merge' && !mergeKeys.trim() && !mergeCondition.trim();

  const showCatalogUri =
    catalogType === 'biglake' || catalogType === 'rest' || catalogType === 'hive';

  const systemInstructions = resolveSystemInstructions(node);

  /**
   * Updates node properties in response to input, select, checkbox, or textarea events.
   *
   * @param e Form change event from a controlled control.
   */
  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const isCheckbox = type === 'checkbox';
    const nextValue = isCheckbox ? (e.target as HTMLInputElement).checked : value;

    if (name === 'warehouseLocation' || name === 'warehouse') {
      onChange({
        ...properties,
        warehouse: nextValue,
        warehouseLocation: nextValue,
      });
      return;
    }

    if (name === 'partitionFields') {
      onChange({
        ...properties,
        partitionFields: nextValue,
        partitionSpec: nextValue,
      });
      return;
    }

    onChange({
      ...properties,
      [name]: nextValue,
    });
  };

  /**
   * Toggles the advanced Iceberg table and writer accordion.
   */
  const handleToggleAdvanced = () => {
    setIsAdvancedOpen((prev) => !prev);
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Write Operation Selector */}
      <SelectField
        id="icebergSinkWriteOperation"
        name="writeOperation"
        label="Write Operation"
        tooltip="Select the Spark DataFrameWriterV2 operation (append, createOrReplace, overwritePartitions) or SQL MERGE INTO upsert."
        value={writeOperation}
        options={ICEBERG_WRITE_OPERATION_OPTIONS}
        onChange={handleChange}
      />

      {/* Merge Keys and SQL Condition (Visible when writeOperation is merge) */}
      {writeOperation === 'merge' && (
        <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
          <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)] flex items-center gap-1.5">
            <span>MERGE INTO Upsert Configuration</span>
            <InfoTooltip text="Configure primary key columns or an explicit SQL ON predicate for MERGE INTO target USING source." />
          </span>

          <PropertyField
            id="icebergSinkMergeKeys"
            name="mergeKeys"
            label="Merge Key Columns"
            tooltip="Comma-separated key columns matched between target and source tables (for example order_id, tenant_id)."
            value={mergeKeys}
            onChange={handleChange}
            placeholder="order_id, tenant_id"
            invalid={isMergeMissingCriteria}
            mono={true}
          />

          <PropertyField
            id="icebergSinkMergeCondition"
            name="mergeCondition"
            label="Custom Merge ON Condition (Optional)"
            tooltip="Optional SQL ON predicate overriding Merge Key Columns (for example target.order_id = source.order_id AND target.dt = source.dt)."
            value={mergeCondition}
            onChange={handleChange}
            placeholder="target.order_id = source.order_id"
            invalid={isMergeMissingCriteria}
            error={
              isMergeMissingCriteria
                ? 'Provide Merge Key Columns or a Custom Merge ON Condition when Write Operation is merge.'
                : null
            }
            mono={true}
          />
        </div>
      )}

      {/* Namespace and Table Identifier */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="icebergSinkNamespace"
          name="namespace"
          label="Namespace / Database"
          tooltip="Optional Iceberg namespace or database name (for example analytics). You can also enter a qualified name in Table Identifier."
          value={namespace}
          onChange={handleChange}
          placeholder="analytics"
          mono={true}
        />

        <PropertyField
          id="icebergSinkTable"
          name="table"
          label="Table Identifier"
          required="Table is required."
          tooltip="Target Iceberg table identifier (for example db.table or catalog.db.table). Required."
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
                Target database and table name in the Iceberg catalog.
              </span>
            ) : null
          }
        />
      </div>

      {/* Catalog Name & Type */}
      <PropertyField
        id="icebergSinkCatalogName"
        name="catalogName"
        label="Catalog Name"
        tooltip="Spark Iceberg catalog name configured under spark.sql.catalog.<catalogName> (for example biglake or lakehouse)."
        value={catalogName}
        onChange={handleChange}
        placeholder="biglake"
        list="iceberg-sink-catalogs-list"
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

      <SelectField
        id="icebergSinkCatalogType"
        name="catalogType"
        label="Catalog Type"
        tooltip="Iceberg catalog implementation type (biglake, rest, hive, hadoop, glue, nessie, or bigquery)."
        value={catalogType}
        options={ICEBERG_CATALOG_TYPES}
        onChange={handleChange}
        helperText={
          <span>
            Catalog implementation configuring table metadata lookup. Defaults to <code>biglake</code>.
          </span>
        }
      />

      {/* Warehouse Location Input with Cloud Storage bucket autocomplete */}
      <StoragePathField
        id="icebergSinkWarehouseLocation"
        name="warehouseLocation"
        label="Warehouse Location"
        tooltip="Base storage path for Iceberg metadata and data files (for example gs://lakehouse/iceberg/warehouse). Required."
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

      {/* Conditional Catalog URI */}
      {showCatalogUri && (
        <PropertyField
          id="icebergSinkCatalogUri"
          name="catalogUri"
          label="Catalog URI"
          tooltip="Endpoint URI for the BigLake REST catalog, Iceberg REST catalog, or Hive Metastore."
          value={catalogUri}
          onChange={handleChange}
          placeholder={
            catalogType === 'hive'
              ? 'thrift://metastore:9083'
              : catalogType === 'biglake'
                ? 'https://biglake.googleapis.com/iceberg/v1beta/restcatalog'
                : 'http://rest-catalog:8181'
          }
          mono={true}
          helperText={
            <span className="text-[11px] text-[var(--vscode-text-muted)]">
              {catalogType === 'hive'
                ? 'Thrift URI for Hive Metastore (HMS).'
                : 'HTTP or HTTPS endpoint for the Iceberg REST Catalog server.'}
            </span>
          }
        />
      )}

      {/* Partition Specs & Fanout Writer */}
      <PropertyField
        id="icebergSinkPartitionFields"
        name="partitionFields"
        label="Partition Specs"
        tooltip="Comma-separated partition columns or Iceberg hidden partition transforms (for example days(event_time), region, bucket(16, user_id))."
        value={partitionFields}
        onChange={handleChange}
        placeholder="days(event_time), region"
        mono={true}
        helperText={
          <span className="text-[11px] text-[var(--vscode-text-muted)]">
            Comma-separated partition fields or transform expressions (for example <code>days(event_time), region</code>).
          </span>
        }
      />

      <label htmlFor="icebergSinkFanoutEnabled" className="flex items-center gap-2 cursor-pointer">
        <input
          id="icebergSinkFanoutEnabled"
          type="checkbox"
          name="fanoutEnabled"
          checked={fanoutEnabled}
          onChange={handleChange}
          className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
        />
        <div className="flex items-center">
          <span className="text-sm font-medium text-slate-300">Enable Fanout Writer (fanout-enabled)</span>
          <InfoTooltip text="Enable option fanout-enabled=true so streaming and unsorted batch partitions write open file handles without requiring a pre-write sort." />
        </div>
      </label>

      {/* Table & Write Options Accordion */}
      <CollapsibleSection
        title="Table & Write Options"
        icon={<Sliders className="w-4 h-4 text-emerald-400" />}
        isOpen={isAdvancedOpen}
        onToggle={handleToggleAdvanced}
      >
        <PropertyField
          id="icebergSinkTableProperties"
          type="textarea"
          name="tableProperties"
          label="Table Properties"
          tooltip="Key=value pairs (one per line) specifying Iceberg table write and format properties (for example write.format.default=parquet)."
          value={tableProperties}
          onChange={handleChange}
          placeholder={'write.format.default=parquet\nwrite.parquet.compression-codec=zstd'}
          rows={3}
          mono={true}
          helperText={
            <span className="text-[11px] text-[var(--vscode-text-muted)]">
              Iceberg table and writer configuration properties (one key=value pair per line).
            </span>
          }
        />
      </CollapsibleSection>

      {/* Authentication and Secret Manager Credentials */}
      <SecretManagerAuthFields
        idPrefix="icebergSink"
        datalistId="iceberg-sink-secrets-list"
        authMode={authMode}
        secretName={secretName}
        serviceName="Apache Iceberg"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="icebergSinkInstructions"
        value={systemInstructions}
        tooltip="Custom instructions provided directly to Gemini when generating PySpark code for this Iceberg sink node."
        placeholder="e.g. Deduplicate incoming batch rows on order_id before running MERGE INTO."
        helperText="Additional guidance appended to the prompt when synthesizing PySpark code."
        textareaClassName="h-24"
        wrapperClassName="pt-2 border-t border-[var(--vscode-border-light)]"
        onChange={(value) => onChange({ ...properties, system_instructions: value })}
      />
    </div>
  );
};

/** Alias matching the canonical Spark transform name `IcebergSink`. */
export const IcebergSinkProperties = IcebergIOSinkProperties;
