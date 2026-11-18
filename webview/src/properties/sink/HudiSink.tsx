/**
 * @fileoverview HudiSink Properties Component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Hudi Sink nodes (`HudiSink`)
 * in Apache Spark 4.x pipelines (`org.apache.hudi:hudi-spark4.0-bundle_2.13`, `.format("hudi")`).
 *
 * Architectural Decisions:
 * - Configures required `path` (`StoragePathField`) and `tableName` (`hoodie.table.name`).
 * - Exposes `tableType` (`'COPY_ON_WRITE' | 'MERGE_ON_READ'`, default `'COPY_ON_WRITE'`) and
 *   `operation` (`'upsert' | 'insert' | 'bulk_insert' | 'delete'`, default `'upsert'`).
 * - Validates `recordKeyField` (`hoodie.datasource.write.recordkey.field`) and conditional
 *   `precombineField` (`hoodie.datasource.write.precombine.field`) when `operation` equals `'upsert'`.
 * - Configures `partitionPathField`, `hiveStylePartitioning`, `saveMode`, shuffle parallelism,
 *   and extra `hoodie.*` properties.
 */

import React, { useState } from 'react';
import { Sliders } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { StoragePathField } from '../../components/common/StoragePathField';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Hudi storage table type options (`hoodie.datasource.write.table.type`). */
const HUDI_TABLE_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'COPY_ON_WRITE', label: 'COPY_ON_WRITE (Columnar Parquet rewrite on update)' },
  { value: 'MERGE_ON_READ', label: 'MERGE_ON_READ (Base Parquet + Avro delta logs)' },
];

/** Hudi write operation options (`hoodie.datasource.write.operation`). */
const HUDI_WRITE_OPERATION_OPTIONS: SelectFieldOption[] = [
  { value: 'upsert', label: 'upsert (Deduplicate and update existing keys)' },
  { value: 'insert', label: 'insert (Insert with small-file bin-packing)' },
  { value: 'bulk_insert', label: 'bulk_insert (High-throughput initial load)' },
  { value: 'delete', label: 'delete (Hard delete matching record keys)' },
];

/** Spark save mode options for batch Hudi writes. */
const HUDI_SAVE_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'append', label: 'append (Incremental Hudi commit)' },
  { value: 'overwrite', label: 'overwrite (Recreate target Hudi table)' },
];

/** Hudi table type union. */
export type HudiTableType = 'COPY_ON_WRITE' | 'MERGE_ON_READ';

/** Hudi write operation union. */
export type HudiWriteOperation = 'upsert' | 'insert' | 'bulk_insert' | 'delete';

/** React Flow node shape for the HudiSink panel. */
export interface HudiSinkNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      path?: string;
      tableName?: string;
      tableType?: HudiTableType;
      operation?: HudiWriteOperation;
      saveMode?: 'append' | 'overwrite';
      recordKeyField?: string;
      precombineField?: string;
      partitionPathField?: string;
      hiveStylePartitioning?: boolean;
      writeParallelism?: number | string;
      hoodieProperties?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link HudiSinkProperties} component. */
export interface HudiSinkPropertiesProps {
  node: HudiSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the configuration panel for Apache Hudi batch and streaming writes.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React element for the HudiSink property sheet.
 */
export const HudiSinkProperties: React.FC<HudiSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data?.properties || {};

  const path = typeof properties.path === 'string' ? properties.path : '';
  const tableName = typeof properties.tableName === 'string' ? properties.tableName : '';
  const tableType: HudiTableType = properties.tableType || 'COPY_ON_WRITE';
  const operation: HudiWriteOperation = properties.operation || 'upsert';
  const saveMode = properties.saveMode || 'append';
  const recordKeyField = typeof properties.recordKeyField === 'string' ? properties.recordKeyField : '';
  const precombineField = typeof properties.precombineField === 'string' ? properties.precombineField : '';
  const partitionPathField =
    typeof properties.partitionPathField === 'string' ? properties.partitionPathField : '';
  const hiveStylePartitioning = Boolean(properties.hiveStylePartitioning ?? false);
  const writeParallelism = properties.writeParallelism ?? '';
  const hoodieProperties = typeof properties.hoodieProperties === 'string' ? properties.hoodieProperties : '';

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

  const systemInstructions = resolveSystemInstructions(node);

  const isTableNameInvalid = !tableName.trim();
  const isRecordKeyInvalid = !recordKeyField.trim();
  const isPrecombineMissing = operation === 'upsert' && !precombineField.trim();

  /**
   * Updates node properties when form inputs, selects, or checkboxes change.
   *
   * @param e Form change event from an input, select, or textarea control.
   */
  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const isCheckbox = type === 'checkbox';
    const checked = isCheckbox ? (e.target as HTMLInputElement).checked : undefined;

    onChange({
      ...properties,
      [name]: isCheckbox ? checked : value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Table Type & Operation */}
      <div className="grid grid-cols-2 gap-3">
        <SelectField
          id="hudiSinkTableType"
          name="tableType"
          label="Table Type"
          tooltip="Hudi storage architecture (hoodie.datasource.write.table.type): COPY_ON_WRITE or MERGE_ON_READ."
          value={tableType}
          options={HUDI_TABLE_TYPE_OPTIONS}
          onChange={handleChange}
        />

        <SelectField
          id="hudiSinkOperation"
          name="operation"
          label="Write Operation"
          tooltip="Hudi write operation (hoodie.datasource.write.operation): upsert, insert, bulk_insert, or delete."
          value={operation}
          options={HUDI_WRITE_OPERATION_OPTIONS}
          onChange={handleChange}
        />
      </div>

      {/* Hudi Base Path & Table Name */}
      <StoragePathField
        id="hudiSinkPath"
        name="path"
        label="Hudi Table Base Path"
        tooltip="Cloud Storage, S3, or HDFS base URI of the destination Apache Hudi table (for example gs://lakehouse/hudi/orders). Required."
        value={path}
        onChange={handleChange}
        placeholder="gs://lakehouse/hudi/orders"
        errorMessage="Hudi Table Base Path is required."
      />

      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="hudiSinkTableName"
          name="tableName"
          label="Hudi Table Name"
          required="Hudi Table Name is required."
          tooltip="Logical Hudi table name passed to hoodie.table.name. Required."
          value={tableName}
          onChange={handleChange}
          placeholder="orders_hudi"
          invalid={isTableNameInvalid}
          error={isTableNameInvalid ? 'Hudi Table Name is required.' : null}
          mono={true}
        />

        <SelectField
          id="hudiSinkSaveMode"
          name="saveMode"
          label="Batch Save Mode"
          tooltip="Spark DataFrameWriter mode (.mode('append') or .mode('overwrite')) used in batch mode."
          value={saveMode}
          options={HUDI_SAVE_MODE_OPTIONS}
          onChange={handleChange}
        />
      </div>

      {/* Record Key & Precombine Fields */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="hudiSinkRecordKeyField"
          name="recordKeyField"
          label="Record Key Field"
          required="Record Key Field is required."
          tooltip="Primary key column or comma-separated composite key columns (hoodie.datasource.write.recordkey.field). Required."
          value={recordKeyField}
          onChange={handleChange}
          placeholder="order_id"
          invalid={isRecordKeyInvalid}
          error={isRecordKeyInvalid ? 'Record Key Field is required.' : null}
          mono={true}
        />

        <PropertyField
          id="hudiSinkPrecombineField"
          name="precombineField"
          label="Precombine Field"
          required={operation === 'upsert' ? 'Precombine Field is required for upsert.' : false}
          tooltip="Timestamp or version column resolving duplicate keys before commit (hoodie.datasource.write.precombine.field)."
          value={precombineField}
          onChange={handleChange}
          placeholder="updated_at"
          invalid={isPrecombineMissing}
          error={isPrecombineMissing ? 'Precombine Field is required for upsert.' : null}
          mono={true}
        />
      </div>

      {/* Partition Path & Hive-Style Partitioning */}
      <PropertyField
        id="hudiSinkPartitionPathField"
        name="partitionPathField"
        label="Partition Path Field (Optional)"
        tooltip="Comma-separated partition column names (hoodie.datasource.write.partitionpath.field). Leave blank for non-partitioned tables."
        value={partitionPathField}
        onChange={handleChange}
        placeholder="event_date, region"
        mono={true}
      />

      <label htmlFor="hudiSinkHiveStylePartitioning" className="flex items-center gap-2 cursor-pointer">
        <input
          id="hudiSinkHiveStylePartitioning"
          type="checkbox"
          name="hiveStylePartitioning"
          checked={hiveStylePartitioning}
          onChange={handleChange}
          className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
        />
        <div className="flex items-center">
          <span className="text-sm font-medium text-slate-300">Hive-Style Partitioning (col=value)</span>
          <InfoTooltip text="Format partition directory paths as key=value pairs (option hoodie.datasource.write.hive_style_partitioning=true)." />
        </div>
      </label>

      {/* Advanced Hudi Write Options */}
      <CollapsibleSection
        title="Advanced Hudi Write Options"
        icon={<Sliders className="w-4 h-4 text-emerald-400" />}
        isOpen={isAdvancedOpen}
        onToggle={() => setIsAdvancedOpen((prev) => !prev)}
      >
        <PropertyField
          id="hudiSinkWriteParallelism"
          name="writeParallelism"
          label="Write Shuffle Parallelism"
          type="number"
          min="1"
          tooltip="Shuffle parallelism applied to hoodie.upsert.shuffle.parallelism and hoodie.insert.shuffle.parallelism."
          value={writeParallelism}
          onChange={handleChange}
          placeholder="200"
          mono={true}
        />

        <PropertyField
          id="hudiSinkHoodieProperties"
          name="hoodieProperties"
          label="Additional Hudi Options"
          type="textarea"
          rows={3}
          tooltip="Extra hoodie.* key=value pairs (one per line) passed to the Spark Hudi writer."
          value={hoodieProperties}
          onChange={handleChange}
          placeholder={'hoodie.cleaner.commits.retained=10\nhoodie.parquet.compression.codec=zstd'}
          mono={true}
        />
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="hudiSinkSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Apache Hudi sink node."
        placeholder="Enter custom instructions for Apache Hudi sink code generation..."
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
