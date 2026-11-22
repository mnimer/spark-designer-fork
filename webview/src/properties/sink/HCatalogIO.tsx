/**
 * @fileoverview HCatalogIOSinkProperties (`HiveSinkProperties`) Component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.x `HiveSink` (`HCatalogIO`) nodes.
 * Configures Hive Metastore and Google Cloud Dataproc Metastore table writes via
 * `df.write.mode(...).format(...).partitionBy(...).bucketBy(...).saveAsTable("<db>.<table>")`,
 * `insertInto("<db>.<table>")`, and Structured Streaming `foreachBatch` micro-batch appends.
 *
 * Architectural Decisions:
 * - Configures `database` (default `'default'`), required `table`, `writeMethod` (`'saveAsTable' | 'insertInto'`),
 *   `saveMode` (`'append' | 'overwrite' | 'errorifexists' | 'ignore'`), and storage `fileFormat`.
 * - Exposes `partitionCols` and `dynamicPartitionOverwrite` (`spark.sql.sources.partitionOverwriteMode=dynamic`).
 * - Treats `metastoreUri` as optional for Dataproc Metastore and warns when a non-empty URI lacks `thrift://`.
 * - Encapsulates bucketing (`numBuckets`, `bucketCols`, `sortCols`), external `externalPath`, and extra Hive
 *   properties inside a collapsible advanced section.
 */

import React, { useState } from 'react';
import { Database } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { StoragePathField } from '../../components/common/StoragePathField';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Write method options for Spark 4.x HiveSink. */
const HIVE_WRITE_METHOD_OPTIONS: SelectFieldOption[] = [
  { value: 'saveAsTable', label: 'saveAsTable (Create or write catalog table by name)' },
  { value: 'insertInto', label: 'insertInto (Positional insert into existing table)' },
];

/** Save mode options for Spark 4.x HiveSink. */
const HIVE_SAVE_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'append', label: 'append (Append rows to table)' },
  { value: 'overwrite', label: 'overwrite (Overwrite table or dynamic partitions)' },
  { value: 'errorifexists', label: 'errorifexists (Fail if table already exists)' },
  { value: 'ignore', label: 'ignore (No-op if table already exists)' },
];

/** Underlying storage format options for `saveAsTable`. */
const HIVE_FILE_FORMAT_OPTIONS: SelectFieldOption[] = [
  { value: 'parquet', label: 'parquet (Columnar Apache Parquet)' },
  { value: 'orc', label: 'orc (Optimized Row Columnar)' },
  { value: 'hive', label: 'hive (Native Hive SerDe)' },
  { value: 'avro', label: 'avro (Apache Avro)' },
];

/** Hive write method union type. */
export type HiveWriteMethod = 'saveAsTable' | 'insertInto';

/** Hive save mode union type. */
export type HiveSaveMode = 'append' | 'overwrite' | 'errorifexists' | 'ignore';

/** Props for the {@link HCatalogIOSinkProperties} component. */
export interface HCatalogIOSinkPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        database?: string;
        table?: string;
        writeMethod?: HiveWriteMethod;
        saveMode?: HiveSaveMode;
        fileFormat?: 'parquet' | 'orc' | 'hive' | 'avro';
        partitionCols?: string;
        partition?: string;
        dynamicPartitionOverwrite?: boolean;
        metastoreUri?: string;
        externalPath?: string;
        numBuckets?: number | string;
        bucketCols?: string;
        sortCols?: string;
        configProperties?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the configuration panel for Spark 4.x `HiveSink` (`HCatalogIO`) catalog writes.
 *
 * @param props Component properties containing the active node and onChange callback.
 * @returns React element for the Hive sink panel.
 */
export const HCatalogIOSinkProperties: React.FC<HCatalogIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data?.properties || {};

  const database = typeof properties.database === 'string' ? properties.database : 'default';
  const table = typeof properties.table === 'string' ? properties.table : '';
  const writeMethod: HiveWriteMethod = properties.writeMethod || 'saveAsTable';
  const saveMode: HiveSaveMode = properties.saveMode || 'append';
  const fileFormat = properties.fileFormat || 'parquet';
  const partitionCols =
    typeof properties.partitionCols === 'string'
      ? properties.partitionCols
      : typeof properties.partition === 'string'
        ? properties.partition
        : '';
  const dynamicPartitionOverwrite = Boolean(properties.dynamicPartitionOverwrite ?? false);
  const metastoreUri = typeof properties.metastoreUri === 'string' ? properties.metastoreUri : '';
  const externalPath = typeof properties.externalPath === 'string' ? properties.externalPath : '';
  const numBuckets = properties.numBuckets ?? '';
  const bucketCols = typeof properties.bucketCols === 'string' ? properties.bucketCols : '';
  const sortCols = typeof properties.sortCols === 'string' ? properties.sortCols : '';
  const configProperties =
    typeof properties.configProperties === 'string' ? properties.configProperties : '';

  const [showAdvanced, setShowAdvanced] = useState(false);

  const isTableEmpty = !table.trim();
  const isMetastoreUriInvalid =
    metastoreUri.trim().length > 0 && !metastoreUri.trim().toLowerCase().startsWith('thrift://');

  const systemInstructions = resolveSystemInstructions(node);

  /**
   * Updates node properties when text, select, checkbox, or textarea controls change.
   *
   * @param e React change event from a controlled form element.
   */
  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const isCheckbox = type === 'checkbox';
    const nextValue = isCheckbox ? (e.target as HTMLInputElement).checked : value;

    onChange({
      ...properties,
      [name]: nextValue,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Execution Mode Banner */}
      <div className="flex items-center justify-between px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-subcard-bg)] text-xs">
        <span className="flex items-center gap-1.5 font-bold uppercase text-[var(--vscode-text-muted)]">
          <Database className="w-3.5 h-3.5 text-blue-400" />
          <span>Streaming Strategy</span>
        </span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          BATCH / FOREACHBATCH
        </span>
      </div>

      {/* Hive Database & Table Card */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <span className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Hive Metastore &amp; Destination Table</span>
          <InfoTooltip text="Configure the Hive or Dataproc Metastore database, table name, and optional Thrift URI." />
        </span>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="hcatSinkDatabase"
            name="database"
            label="Database"
            tooltip="Hive Metastore database name. Defaults to default."
            value={database}
            onChange={handleChange}
            placeholder="default"
            mono={true}
          />
          <PropertyField
            id="hcatSinkTable"
            name="table"
            label="Table"
            required="Table is required"
            tooltip="Destination Hive table name written via saveAsTable or insertInto. Required."
            value={table}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Table is required' : null}
            placeholder="daily_aggregates"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="hcatSinkMetastoreUri"
          name="metastoreUri"
          label="Hive Metastore URI (Optional)"
          tooltip="Optional Thrift URI for an external Hive Metastore (spark.hadoop.hive.metastore.uris). Leave blank when attached to Dataproc Metastore."
          value={metastoreUri}
          helperText={
            isMetastoreUriInvalid ? (
              <span className="text-amber-400" role="alert">
                Warning: Hive Metastore URIs typically start with thrift://
              </span>
            ) : null
          }
          placeholder="thrift://localhost:9083"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Write Method, Save Mode & File Format */}
      <div className="grid grid-cols-2 gap-3">
        <SelectField
          id="hcatSinkWriteMethod"
          name="writeMethod"
          label="Write Method"
          tooltip="Choose saveAsTable (creates or writes by column name) or insertInto (inserts into an existing table by column position)."
          value={writeMethod}
          options={HIVE_WRITE_METHOD_OPTIONS}
          onChange={handleChange}
        />

        <SelectField
          id="hcatSinkSaveMode"
          name="saveMode"
          label="Save Mode"
          tooltip="Spark DataFrameWriter save mode (append, overwrite, errorifexists, or ignore)."
          value={saveMode}
          options={HIVE_SAVE_MODE_OPTIONS}
          onChange={handleChange}
        />
      </div>

      {writeMethod === 'saveAsTable' && (
        <SelectField
          id="hcatSinkFileFormat"
          name="fileFormat"
          label="Table Storage Format"
          tooltip="Underlying table storage format passed to .format(...) during saveAsTable."
          value={fileFormat}
          options={HIVE_FILE_FORMAT_OPTIONS}
          onChange={handleChange}
        />
      )}

      {/* Partitioning & Dynamic Overwrite */}
      <PropertyField
        id="hcatSinkPartitionCols"
        name="partitionCols"
        label="Partition Columns (Optional)"
        tooltip="Comma-separated partition column names passed to .partitionBy(...) during saveAsTable."
        value={partitionCols}
        onChange={handleChange}
        placeholder="ds, region"
        mono={true}
      />

      {saveMode === 'overwrite' && (
        <label htmlFor="hcatSinkDynamicPartitionOverwrite" className="flex items-center gap-2 cursor-pointer">
          <input
            id="hcatSinkDynamicPartitionOverwrite"
            type="checkbox"
            name="dynamicPartitionOverwrite"
            checked={dynamicPartitionOverwrite}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">
              Dynamic Partition Overwrite (partitionOverwriteMode=dynamic)
            </span>
            <InfoTooltip text="Overwrite only partitions present in the DataFrame instead of dropping all partitions in the Hive table." />
          </div>
        </label>
      )}

      {/* Collapsible Bucketing, External Path & Extra Settings */}
      <CollapsibleSection
        title="Bucketing, External Path & Advanced Settings"
        isOpen={showAdvanced}
        onToggle={() => setShowAdvanced((prev) => !prev)}
      >
        <StoragePathField
          id="hcatSinkExternalPath"
          name="externalPath"
          label="External Table Warehouse Path (Optional)"
          tooltip="Optional storage path (.option('path', ...)) creating an external Hive table rather than a managed warehouse table."
          value={externalPath}
          onChange={handleChange}
          placeholder="gs://my-warehouse/hive/daily_aggregates"
          required={false}
        />

        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="hcatSinkNumBuckets"
            name="numBuckets"
            label="Number of Buckets"
            type="number"
            min="1"
            tooltip="Bucket count passed to .bucketBy(numBuckets, *bucketCols) during saveAsTable."
            value={numBuckets}
            onChange={handleChange}
            placeholder="32"
            mono={true}
          />

          <PropertyField
            id="hcatSinkBucketCols"
            name="bucketCols"
            label="Bucket Columns"
            tooltip="Comma-separated columns hashed across buckets in .bucketBy(...)."
            value={bucketCols}
            onChange={handleChange}
            placeholder="customer_id"
            mono={true}
          />
        </div>

        <PropertyField
          id="hcatSinkSortCols"
          name="sortCols"
          label="Bucket Sort Columns (Optional)"
          tooltip="Optional comma-separated sort columns passed to .sortBy(...) when bucketing is enabled."
          value={sortCols}
          onChange={handleChange}
          placeholder="event_timestamp"
          mono={true}
        />

        <PropertyField
          id="hcatSinkConfigProperties"
          name="configProperties"
          label="Additional Hive Configuration"
          tooltip="Custom Spark or Hive configuration key=value pairs (one per line)."
          type="textarea"
          rows={3}
          value={configProperties}
          onChange={handleChange}
          placeholder={'hive.exec.dynamic.partition.mode=nonstrict\nspark.sql.orc.compression.codec=zstd'}
          mono={true}
        />
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="hcatSinkInstructions"
        value={systemInstructions}
        tooltip="Optional custom prompt instructions passed to the AI code generator for this Hive sink node."
        placeholder="e.g. Repartition by ds before calling saveAsTable to produce one Parquet file per partition."
        onChange={(value) => onChange({ ...properties, system_instructions: value })}
      />
    </div>
  );
};

/** Alias matching the canonical Spark transform name `HiveSink`. */
export const HiveSinkProperties = HCatalogIOSinkProperties;
