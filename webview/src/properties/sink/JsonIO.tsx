/**
 * @fileoverview JSON Sink (SNK15 JsonSink / JsonIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark JSON sink nodes
 * using df.write.json(uri) or df.writeStream.format("json").start(uri).
 * Embeds the shared StorageBackendFields component alongside Spark 4.x saveMode,
 * compression codecs, partitionBy columns, maxRecordsPerFile, ignoreNullFields,
 * timestampFormat, dateFormat, lineSep, and SystemInstructionsField.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { StorageBackendFields } from '../../components/common/StorageBackendFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported Spark 4.x batch DataFrameWriter save modes.
 */
const JSON_SAVE_MODES: SelectFieldOption[] = [
  { value: 'append', label: 'append (Append rows to existing path)' },
  { value: 'overwrite', label: 'overwrite (Replace existing data)' },
  { value: 'errorifexists', label: 'errorifexists (Fail if destination exists)' },
  { value: 'ignore', label: 'ignore (Skip write if destination exists)' },
];

/**
 * Supported Spark 4.x JSON output compression codecs.
 */
const JSON_COMPRESSION_CODECS: SelectFieldOption[] = [
  { value: 'none', label: 'none (Uncompressed JSON Lines)' },
  { value: 'gzip', label: 'gzip (.json.gz)' },
  { value: 'zstd', label: 'zstd (.json.zst)' },
  { value: 'bzip2', label: 'bzip2 (.json.bz2)' },
  { value: 'lz4', label: 'lz4 (.json.lz4)' },
  { value: 'snappy', label: 'snappy (.json.snappy)' },
  { value: 'deflate', label: 'deflate (.json.deflate)' },
];

/**
 * React Flow node shape for the JSON sink panel.
 */
export interface JsonIOSinkNode {
  data: {
    label?: string;
    type?: string;
    spark_transform?: string;
    executionMode?: string;
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
      compression?: 'none' | 'gzip' | 'zstd' | 'bzip2' | 'lz4' | 'snappy' | 'deflate' | string;
      partitionBy?: string | string[];
      maxRecordsPerFile?: string | number;
      ignoreNullFields?: boolean;
      timestampFormat?: string;
      dateFormat?: string;
      lineSep?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/**
 * Props for the {@link JsonIOSinkProperties} component.
 */
export interface JsonIOSinkPropertiesProps {
  node: JsonIOSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Normalizes a raw JSON compression codec value to a canonical Spark codec name.
 */
function normalizeJsonCompression(raw?: string): string {
  const lower = (raw ?? '').trim().toLowerCase();
  if (!lower || lower === 'uncompressed') {
    return 'none';
  }
  if (JSON_COMPRESSION_CODECS.some((opt) => opt.value === lower)) {
    return lower;
  }
  return 'none';
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
 * Dedicated property panel for Apache Spark JsonSink / JsonIO sink nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered JSON sink property panel.
 */
export const JsonIOSinkProperties: React.FC<JsonIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};
  const storageProperties =
    properties.path === undefined && typeof properties.to === 'string' && properties.to.length > 0
      ? { ...properties, path: properties.to }
      : properties;

  const rawSaveMode = (properties.saveMode || properties.mode || 'append').toLowerCase();
  const saveMode = JSON_SAVE_MODES.some((opt) => opt.value === rawSaveMode)
    ? rawSaveMode
    : 'append';
  const compression = normalizeJsonCompression(properties.compression);
  const partitionBy = resolvePartitionByValue(properties.partitionBy);
  const maxRecordsPerFile = properties.maxRecordsPerFile ?? '';
  const ignoreNullFields = properties.ignoreNullFields ?? true;
  const timestampFormat = properties.timestampFormat ?? '';
  const dateFormat = properties.dateFormat ?? '';
  const lineSep = properties.lineSep ?? '';
  const systemInstructions = resolveSystemInstructions(node);

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
        idPrefix="jsonSink"
        pathLabel="Output JSON URI / Path"
        pathPlaceholder="gs://my-bucket/output/json/"
        showGlobAndRecursive={false}
      />

      {/* JSON Writer Save Mode, Compression & Partitioning */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          JSON Writer Options
        </span>

        <SelectField
          id="jsonSinkSaveMode"
          name="saveMode"
          label="Save Mode (mode)"
          tooltip="Controls how Spark handles existing data at the destination URI in batch writes. Structured Streaming writes use append mode."
          value={saveMode}
          options={JSON_SAVE_MODES}
          onChange={handleChange}
        />

        <SelectField
          id="jsonSinkCompression"
          name="compression"
          label="Compression Codec (compression)"
          tooltip="Compression codec applied to output JSON Lines part-files."
          value={compression}
          options={JSON_COMPRESSION_CODECS}
          onChange={handleChange}
        />

        <PropertyField
          id="jsonSinkPartitionBy"
          name="partitionBy"
          label="Partition Columns (partitionBy)"
          tooltip="Optional comma-separated column names used to partition output directories in Hive layout (for example, event_date, region)."
          value={partitionBy}
          onChange={handleChange}
          placeholder="e.g. event_date, region"
          mono={true}
        />

        <PropertyField
          id="jsonSinkMaxRecordsPerFile"
          type="number"
          name="maxRecordsPerFile"
          label="Max Records Per File (maxRecordsPerFile)"
          tooltip="Optional maximum number of JSON Lines records written per output part-file."
          value={maxRecordsPerFile}
          onChange={handleChange}
          placeholder="e.g. 500000"
          min="1"
          mono={true}
        />
      </div>

      {/* JSON Serialization & Formatting Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          JSON Serialization Formatting
        </span>

        <label
          htmlFor="jsonSinkIgnoreNullFields"
          className="flex items-center gap-2 cursor-pointer"
        >
          <input
            id="jsonSinkIgnoreNullFields"
            type="checkbox"
            name="ignoreNullFields"
            checked={ignoreNullFields}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Omit Null Fields (ignoreNullFields=true)
          </span>
          <InfoTooltip text="Drops fields whose value is null during JSON serialization. Uncheck to emit explicit null values in each JSON object." />
        </label>

        <PropertyField
          id="jsonSinkTimestampFormat"
          name="timestampFormat"
          label="Timestamp Format (timestampFormat)"
          tooltip="Optional DateTimeFormatter pattern for serializing TIMESTAMP columns (defaults to ISO-8601)."
          value={timestampFormat}
          onChange={handleChange}
          placeholder="yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"
          mono={true}
        />

        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="jsonSinkDateFormat"
            name="dateFormat"
            label="Date Format (dateFormat)"
            tooltip="Optional DateTimeFormatter pattern for serializing DATE columns (default yyyy-MM-dd)."
            value={dateFormat}
            onChange={handleChange}
            placeholder="yyyy-MM-dd"
            mono={true}
          />

          <PropertyField
            id="jsonSinkLineSep"
            name="lineSep"
            label="Line Separator (lineSep)"
            tooltip="Character or sequence separating JSON records (default '\\n')."
            value={lineSep}
            onChange={handleChange}
            placeholder="\n"
            mono={true}
          />
        </div>
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="jsonSinkInstructions"
        value={systemInstructions}
        placeholder="e.g. Preserve explicit null keys for downstream schema-strict consumers."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark JSON writer code."
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

export const JsonSinkProperties = JsonIOSinkProperties;
