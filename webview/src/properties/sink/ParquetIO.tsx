/**
 * @fileoverview Parquet Sink (SNK11 ParquetSink / ParquetIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Parquet sink nodes
 * using df.write.parquet(uri) or df.writeStream.format("parquet").start(uri).
 * Embeds the shared StorageBackendFields component for multi-cloud URI and Secret Manager
 * configuration alongside Spark 4.x saveMode, compression codec, partitionBy columns,
 * maxRecordsPerFile, datetime/INT96 write rebase modes, and SystemInstructionsField.
 */

/* eslint-disable @typescript-eslint/no-explicit-any, react-refresh/only-export-components */
import React, { useState } from 'react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { StorageBackendFields } from '../../components/common/StorageBackendFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported Spark 4.x batch DataFrameWriter save modes.
 */
export const SPARK_SAVE_MODES: SelectFieldOption[] = [
  { value: 'append', label: 'append (Append rows to existing path)' },
  { value: 'overwrite', label: 'overwrite (Replace existing data)' },
  { value: 'errorifexists', label: 'errorifexists (Fail if destination exists)' },
  { value: 'ignore', label: 'ignore (Skip write if destination exists)' },
];

/**
 * Supported Spark 4.x Parquet compression codecs.
 */
export const PARQUET_COMPRESSION_CODECS: SelectFieldOption[] = [
  { value: 'snappy', label: 'snappy (Default, balanced throughput)' },
  { value: 'zstd', label: 'zstd (High compression ratio and speed)' },
  { value: 'gzip', label: 'gzip (Higher compression, higher CPU)' },
  { value: 'none', label: 'none (Uncompressed)' },
];

/**
 * Supported Spark 4.x Parquet calendar rebase modes when writing dates and timestamps.
 */
export const PARQUET_WRITE_REBASE_MODES: SelectFieldOption[] = [
  { value: 'EXCEPTION', label: 'EXCEPTION (Fail on ambiguous ancient dates)' },
  { value: 'CORRECTED', label: 'CORRECTED (Write Proleptic Gregorian verbatim)' },
  { value: 'LEGACY', label: 'LEGACY (Rebase Gregorian to Julian calendar)' },
];

/**
 * React Flow node shape for the Parquet sink panel.
 */
export interface ParquetIOSinkNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      storageBackend?: string;
      path?: string;
      to?: string;
      uri?: string;
      authMode?: string;
      secretName?: string;
      saveMode?: 'append' | 'overwrite' | 'errorifexists' | 'ignore';
      mode?: string;
      compression?: 'snappy' | 'zstd' | 'gzip' | 'none' | string;
      codec?: string;
      partitionBy?: string | string[];
      maxRecordsPerFile?: string | number;
      datetimeRebaseMode?: 'EXCEPTION' | 'CORRECTED' | 'LEGACY';
      int96RebaseMode?: 'EXCEPTION' | 'CORRECTED' | 'LEGACY';
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: any;
    };
  };
}

/**
 * Props for the {@link ParquetIOSinkProperties} component.
 */
export interface ParquetIOSinkPropertiesProps {
  node: ParquetIOSinkNode;
  onChange: (properties: any) => void;
}

/**
 * Normalizes a raw Parquet compression codec value to a canonical Spark codec name.
 */
function normalizeParquetCompression(raw?: string): string {
  const lower = (raw ?? '').trim().toLowerCase();
  if (lower === 'uncompressed') {
    return 'none';
  }
  if (PARQUET_COMPRESSION_CODECS.some((opt) => opt.value === lower)) {
    return lower;
  }
  return 'snappy';
}

/**
 * Formats a partitionBy property value as a comma-separated column list string.
 */
function resolvePartitionByValue(raw: unknown): string {
  if (Array.isArray(raw)) {
    return raw.join(', ');
  }
  return typeof raw === 'string' ? raw : '';
}

/**
 * Dedicated property panel for Apache Spark ParquetSink / ParquetIO sink nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered Parquet sink property panel.
 */
export const ParquetIOSinkProperties: React.FC<ParquetIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data.properties || {};
  const storageProperties =
    properties.path === undefined && typeof properties.to === 'string' && properties.to.length > 0
      ? { ...properties, path: properties.to }
      : properties;

  const rawSaveMode = (properties.saveMode || properties.mode || 'append').toLowerCase();
  const saveMode = SPARK_SAVE_MODES.some((opt) => opt.value === rawSaveMode)
    ? rawSaveMode
    : 'append';
  const compression = normalizeParquetCompression(properties.compression || properties.codec);
  const partitionBy = resolvePartitionByValue(properties.partitionBy);
  const maxRecordsPerFile = properties.maxRecordsPerFile ?? '';
  const datetimeRebaseMode = properties.datetimeRebaseMode || 'EXCEPTION';
  const int96RebaseMode = properties.int96RebaseMode || 'EXCEPTION';
  const systemInstructions = resolveSystemInstructions(node);

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

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
      {/* Shared Multi-Cloud Storage Backend Configuration */}
      <StorageBackendFields
        properties={storageProperties}
        onChange={onChange}
        idPrefix="parquetSink"
        pathLabel="Output Parquet URI / Path"
        pathPlaceholder="gs://my-bucket/output/parquet/"
        showGlobAndRecursive={false}
      />

      {/* Parquet Writer Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          Parquet Writer Options
        </span>

        <SelectField
          id="parquetSinkSaveMode"
          name="saveMode"
          label="Save Mode (mode)"
          tooltip="Controls how Spark handles existing data at the destination URI in batch writes. Structured Streaming writes use append mode."
          value={saveMode}
          options={SPARK_SAVE_MODES}
          onChange={handleChange}
        />

        <SelectField
          id="parquetSinkCompression"
          name="compression"
          label="Compression Codec (compression)"
          tooltip="Block-level compression codec applied to output Parquet files."
          value={compression}
          options={PARQUET_COMPRESSION_CODECS}
          onChange={handleChange}
        />

        <PropertyField
          id="parquetSinkPartitionBy"
          name="partitionBy"
          label="Partition Columns (partitionBy)"
          tooltip="Optional comma-separated column names used to partition output directories in Hive layout (for example, event_date, region)."
          value={partitionBy}
          onChange={handleChange}
          placeholder="e.g. event_date, region"
          mono={true}
        />

        <PropertyField
          id="parquetSinkMaxRecordsPerFile"
          type="number"
          name="maxRecordsPerFile"
          label="Max Records Per File (maxRecordsPerFile)"
          tooltip="Optional maximum number of rows written to a single Parquet part-file before rolling to a new file."
          value={maxRecordsPerFile}
          onChange={handleChange}
          placeholder="e.g. 1000000"
          min="1"
          mono={true}
        />
      </div>

      {/* Advanced Parquet Calendar Rebase Settings */}
      <CollapsibleSection
        title="Advanced Parquet Write Settings"
        isOpen={isAdvancedOpen}
        onToggle={() => setIsAdvancedOpen(!isAdvancedOpen)}
      >
        <div className="flex flex-col gap-3 mt-2">
          <SelectField
            id="parquetSinkDatetimeRebaseMode"
            name="datetimeRebaseMode"
            label="Datetime Rebase Mode (datetimeRebaseModeInWrite)"
            tooltip="Controls calendar rebasing when writing DATE and TIMESTAMP values older than 1582-10-15."
            value={datetimeRebaseMode}
            options={PARQUET_WRITE_REBASE_MODES}
            onChange={handleChange}
          />

          <SelectField
            id="parquetSinkInt96RebaseMode"
            name="int96RebaseMode"
            label="INT96 Rebase Mode (int96RebaseModeInWrite)"
            tooltip="Controls calendar rebasing when writing legacy INT96 timestamp values."
            value={int96RebaseMode}
            options={PARQUET_WRITE_REBASE_MODES}
            onChange={handleChange}
          />
        </div>
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="parquetSinkInstructions"
        value={systemInstructions}
        placeholder="e.g. Repartition by event_date before writing to avoid small files."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark Parquet writer code."
        helperText="Additional guidance appended to the prompt when generating PySpark code."
        wrapperClassName="pt-2 border-t border-[var(--vscode-border-light)]"
        textareaClassName="h-24"
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

export const ParquetSinkProperties = ParquetIOSinkProperties;
