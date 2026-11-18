/**
 * @fileoverview Text Sink (SNK16 TextSink / TextIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Text sink nodes
 * using df.write.text(uri) or df.writeStream.format("text").start(uri).
 * Embeds the shared StorageBackendFields component alongside Spark 4.x saveMode,
 * compression codecs, partitionBy columns, maxRecordsPerFile, single-string-column
 * projection strategy (valueColumn, F.concat_ws, or F.to_json), lineSep, and
 * SystemInstructionsField.
 */

/* eslint-disable @typescript-eslint/no-explicit-any, react-refresh/only-export-components */
import React from 'react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { StorageBackendFields } from '../../components/common/StorageBackendFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported Spark 4.x batch DataFrameWriter save modes.
 */
export const TEXT_SAVE_MODES: SelectFieldOption[] = [
  { value: 'append', label: 'append (Append rows to existing path)' },
  { value: 'overwrite', label: 'overwrite (Replace existing data)' },
  { value: 'errorifexists', label: 'errorifexists (Fail if destination exists)' },
  { value: 'ignore', label: 'ignore (Skip write if destination exists)' },
];

/**
 * Supported Spark 4.x Text output compression codecs.
 */
export const TEXT_COMPRESSION_CODECS: SelectFieldOption[] = [
  { value: 'none', label: 'none (Uncompressed plain text)' },
  { value: 'gzip', label: 'gzip (.txt.gz)' },
  { value: 'zstd', label: 'zstd (.txt.zst)' },
  { value: 'bzip2', label: 'bzip2 (.txt.bz2)' },
  { value: 'lz4', label: 'lz4 (.txt.lz4)' },
  { value: 'snappy', label: 'snappy (.txt.snappy)' },
  { value: 'deflate', label: 'deflate (.txt.deflate)' },
];

/**
 * Supported strategies for satisfying Spark Text writer's single StringType column requirement.
 */
export const TEXT_SERIALIZATION_STRATEGIES: SelectFieldOption[] = [
  { value: 'column', label: 'Project Single Column as String (F.col(name).cast("string"))' },
  { value: 'concat_ws', label: 'Concatenate Columns (F.concat_ws)' },
  { value: 'to_json', label: 'Serialize Row as JSON Line (F.to_json(F.struct(*cols)))' },
];

/**
 * React Flow node shape for the Text sink panel.
 */
export interface TextIOSinkNode {
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
      compression?: 'none' | 'gzip' | 'zstd' | 'bzip2' | 'lz4' | 'snappy' | 'deflate' | string;
      partitionBy?: string | string[];
      maxRecordsPerFile?: string | number;
      serializationStrategy?: 'column' | 'concat_ws' | 'to_json';
      valueColumn?: string;
      concatDelimiter?: string;
      lineSep?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: any;
    };
  };
}

/**
 * Props for the {@link TextIOSinkProperties} component.
 */
export interface TextIOSinkPropertiesProps {
  node: TextIOSinkNode;
  onChange: (properties: any) => void;
}

/**
 * Normalizes a raw Text compression codec value to a canonical Spark codec name.
 */
function normalizeTextCompression(raw?: string): string {
  const lower = (raw ?? '').trim().toLowerCase();
  if (!lower || lower === 'uncompressed' || lower === 'auto' || lower === 'zip') {
    return 'none';
  }
  if (TEXT_COMPRESSION_CODECS.some((opt) => opt.value === lower)) {
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
 * Dedicated property panel for Apache Spark TextSink / TextIO sink nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered Text sink property panel.
 */
export const TextIOSinkProperties: React.FC<TextIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};
  const storageProperties =
    properties.path === undefined && typeof properties.to === 'string' && properties.to.length > 0
      ? { ...properties, path: properties.to }
      : properties;

  const rawSaveMode = (properties.saveMode || properties.mode || 'append').toLowerCase();
  const saveMode = TEXT_SAVE_MODES.some((opt) => opt.value === rawSaveMode)
    ? rawSaveMode
    : 'append';
  const compression = normalizeTextCompression(properties.compression);
  const partitionBy = resolvePartitionByValue(properties.partitionBy);
  const maxRecordsPerFile = properties.maxRecordsPerFile ?? '';
  const serializationStrategy =
    properties.serializationStrategy === 'concat_ws' ||
    properties.serializationStrategy === 'to_json'
      ? properties.serializationStrategy
      : 'column';
  const valueColumn = properties.valueColumn ?? 'value';
  const concatDelimiter = properties.concatDelimiter ?? ',';
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
        idPrefix="textSink"
        pathLabel="Output Text URI / Path"
        pathPlaceholder="gs://my-bucket/output/text/"
        showGlobAndRecursive={false}
      />

      {/* Text Writer Save Mode, Compression & Partitioning */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          Text Writer Options
        </span>

        <SelectField
          id="textSinkSaveMode"
          name="saveMode"
          label="Save Mode (mode)"
          tooltip="Controls how Spark handles existing data at the destination URI in batch writes. Structured Streaming writes use append mode."
          value={saveMode}
          options={TEXT_SAVE_MODES}
          onChange={handleChange}
        />

        <SelectField
          id="textSinkCompression"
          name="compression"
          label="Compression Codec (compression)"
          tooltip="Compression codec applied to output text files."
          value={compression}
          options={TEXT_COMPRESSION_CODECS}
          onChange={handleChange}
        />

        <PropertyField
          id="textSinkPartitionBy"
          name="partitionBy"
          label="Partition Columns (partitionBy)"
          tooltip="Optional comma-separated column names used to partition output directories in Hive layout (excluded from single-column check)."
          value={partitionBy}
          onChange={handleChange}
          placeholder="e.g. log_date, service"
          mono={true}
        />

        <PropertyField
          id="textSinkMaxRecordsPerFile"
          type="number"
          name="maxRecordsPerFile"
          label="Max Records Per File (maxRecordsPerFile)"
          tooltip="Optional maximum number of lines written per output text part-file."
          value={maxRecordsPerFile}
          onChange={handleChange}
          placeholder="e.g. 500000"
          min="1"
          mono={true}
        />
      </div>

      {/* Single String Column Projection & Line Separator */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          Single String Column Projection (Required by Spark Text Writer)
        </span>

        <SelectField
          id="textSinkSerializationStrategy"
          name="serializationStrategy"
          label="Row-to-Text Projection"
          tooltip="Spark .write.text() requires a single StringType non-partition column. Select how upstream columns map to that string column."
          value={serializationStrategy}
          options={TEXT_SERIALIZATION_STRATEGIES}
          onChange={handleChange}
        />

        {serializationStrategy === 'column' && (
          <PropertyField
            id="textSinkValueColumn"
            name="valueColumn"
            label="Source Text Column (valueColumn)"
            tooltip="Column projected and cast to StringType as the text output line."
            value={valueColumn}
            onChange={handleChange}
            placeholder="value"
            mono={true}
          />
        )}

        {serializationStrategy === 'concat_ws' && (
          <PropertyField
            id="textSinkConcatDelimiter"
            name="concatDelimiter"
            label="Concatenation Separator (F.concat_ws)"
            tooltip="Separator string inserted between non-partition columns in F.concat_ws."
            value={concatDelimiter}
            onChange={handleChange}
            placeholder=","
            mono={true}
          />
        )}

        <PropertyField
          id="textSinkLineSep"
          name="lineSep"
          label="Line Separator (lineSep)"
          tooltip="Line separator written after each text record (default '\\n')."
          value={lineSep}
          onChange={handleChange}
          placeholder="\n"
          mono={true}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="textSinkInstructions"
        value={systemInstructions}
        placeholder="e.g. Format log entries as '[LEVEL] timestamp message' into value before writing."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark Text writer code."
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

export const TextSinkProperties = TextIOSinkProperties;
