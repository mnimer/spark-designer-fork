/**
 * @fileoverview CSV Sink (SNK14 CsvSink / CsvIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark CSV sink nodes
 * using df.write.csv(uri) or df.writeStream.format("csv").start(uri).
 * Embeds the shared StorageBackendFields component alongside Spark 4.x saveMode,
 * CSV dialect options (header, sep, quote, escape, nullValue, emptyValue, quoteAll),
 * compression codecs, partitionBy columns, maxRecordsPerFile, and SystemInstructionsField.
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
const CSV_SAVE_MODES: SelectFieldOption[] = [
  { value: 'append', label: 'append (Append rows to existing path)' },
  { value: 'overwrite', label: 'overwrite (Replace existing data)' },
  { value: 'errorifexists', label: 'errorifexists (Fail if destination exists)' },
  { value: 'ignore', label: 'ignore (Skip write if destination exists)' },
];

/**
 * Supported Spark 4.x CSV output compression codecs.
 */
const CSV_COMPRESSION_CODECS: SelectFieldOption[] = [
  { value: 'none', label: 'none (Uncompressed CSV)' },
  { value: 'gzip', label: 'gzip (.csv.gz)' },
  { value: 'zstd', label: 'zstd (.csv.zst)' },
  { value: 'bzip2', label: 'bzip2 (.csv.bz2)' },
  { value: 'lz4', label: 'lz4 (.csv.lz4)' },
  { value: 'snappy', label: 'snappy (.csv.snappy)' },
  { value: 'deflate', label: 'deflate (.csv.deflate)' },
];

/**
 * React Flow node shape for the CSV sink panel.
 */
export interface CsvIOSinkNode {
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
      header?: boolean | string;
      writeHeader?: boolean;
      sep?: string;
      delimiter?: string;
      quote?: string;
      quoteChar?: string;
      escape?: string;
      nullValue?: string;
      nullString?: string;
      emptyValue?: string;
      quoteAll?: boolean;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/**
 * Props for the {@link CsvIOSinkProperties} component.
 */
export interface CsvIOSinkPropertiesProps {
  node: CsvIOSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Normalizes a raw CSV compression codec value to a canonical Spark codec name.
 */
function normalizeCsvCompression(raw?: string): string {
  const lower = (raw ?? '').trim().toLowerCase();
  if (!lower || lower === 'uncompressed') {
    return 'none';
  }
  if (CSV_COMPRESSION_CODECS.some((opt) => opt.value === lower)) {
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
 * Resolves the boolean CSV header toggle from current or legacy node properties.
 */
function resolveHeaderFlag(properties: Record<string, unknown>): boolean {
  if (typeof properties.header === 'boolean') {
    return properties.header;
  }
  if (typeof properties.header === 'string') {
    const trimmed = properties.header.trim().toLowerCase();
    if (trimmed === 'false') {
      return false;
    }
    if (trimmed === 'true') {
      return true;
    }
  }
  if (typeof properties.writeHeader === 'boolean') {
    return properties.writeHeader;
  }
  return true;
}

/**
 * Dedicated property panel for Apache Spark CsvSink / CsvIO sink nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered CSV sink property panel.
 */
export const CsvIOSinkProperties: React.FC<CsvIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};
  const storageProperties =
    properties.path === undefined && typeof properties.to === 'string' && properties.to.length > 0
      ? { ...properties, path: properties.to }
      : properties;

  const rawSaveMode = (properties.saveMode || properties.mode || 'append').toLowerCase();
  const saveMode = CSV_SAVE_MODES.some((opt) => opt.value === rawSaveMode) ? rawSaveMode : 'append';
  const compression = normalizeCsvCompression(properties.compression);
  const partitionBy = resolvePartitionByValue(properties.partitionBy);
  const maxRecordsPerFile = properties.maxRecordsPerFile ?? '';
  const header = resolveHeaderFlag(properties);
  const quoteAll = Boolean(properties.quoteAll);
  const sep = properties.sep ?? properties.delimiter ?? ',';
  const quote = properties.quote ?? properties.quoteChar ?? '"';
  const escape = properties.escape ?? '\\';
  const nullValue = properties.nullValue ?? properties.nullString ?? '';
  const emptyValue = properties.emptyValue ?? '';
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
        idPrefix="csvSink"
        pathLabel="Output CSV URI / Path"
        pathPlaceholder="gs://my-bucket/output/csv/"
        showGlobAndRecursive={false}
      />

      {/* CSV Save Mode, Compression & Partitioning */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          CSV Writer Options
        </span>

        <SelectField
          id="csvSinkSaveMode"
          name="saveMode"
          label="Save Mode (mode)"
          tooltip="Controls how Spark handles existing data at the destination URI in batch writes. Structured Streaming writes use append mode."
          value={saveMode}
          options={CSV_SAVE_MODES}
          onChange={handleChange}
        />

        <SelectField
          id="csvSinkCompression"
          name="compression"
          label="Compression Codec (compression)"
          tooltip="Compression codec applied to output CSV part-files."
          value={compression}
          options={CSV_COMPRESSION_CODECS}
          onChange={handleChange}
        />

        <PropertyField
          id="csvSinkPartitionBy"
          name="partitionBy"
          label="Partition Columns (partitionBy)"
          tooltip="Optional comma-separated column names used to partition output directories in Hive layout (for example, event_date, region)."
          value={partitionBy}
          onChange={handleChange}
          placeholder="e.g. event_date, region"
          mono={true}
        />

        <PropertyField
          id="csvSinkMaxRecordsPerFile"
          type="number"
          name="maxRecordsPerFile"
          label="Max Records Per File (maxRecordsPerFile)"
          tooltip="Optional maximum number of rows written per output CSV part-file."
          value={maxRecordsPerFile}
          onChange={handleChange}
          placeholder="e.g. 500000"
          min="1"
          mono={true}
        />
      </div>

      {/* CSV Dialect & Quoting Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          CSV Dialect &amp; Formatting
        </span>

        <label htmlFor="csvSinkHeader" className="flex items-center gap-2 cursor-pointer">
          <input
            id="csvSinkHeader"
            type="checkbox"
            name="header"
            checked={header}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Write Column Header Row (header=true)
          </span>
          <InfoTooltip text="Writes column names as the first line in each generated CSV file." />
        </label>

        <label htmlFor="csvSinkQuoteAll" className="flex items-center gap-2 cursor-pointer">
          <input
            id="csvSinkQuoteAll"
            type="checkbox"
            name="quoteAll"
            checked={quoteAll}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Quote All Fields (quoteAll)
          </span>
          <InfoTooltip text="Encloses all non-null values in quote characters regardless of whether they contain delimiters." />
        </label>

        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="csvSinkSep"
            name="sep"
            label="Field Delimiter (sep)"
            tooltip="Character or string separating columns in each CSV row (default ',')."
            value={sep}
            onChange={handleChange}
            placeholder=","
            mono={true}
          />

          <PropertyField
            id="csvSinkQuote"
            name="quote"
            label="Quote Character (quote)"
            tooltip={'Character used to quote fields containing delimiters (default \'"\').'}
            value={quote}
            onChange={handleChange}
            placeholder='"'
            mono={true}
          />
        </div>

        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="csvSinkEscape"
            name="escape"
            label="Escape Character (escape)"
            tooltip="Character used to escape quote characters inside quoted values (default '\\')."
            value={escape}
            onChange={handleChange}
            placeholder="\"
            mono={true}
          />

          <PropertyField
            id="csvSinkNullValue"
            name="nullValue"
            label="Null Value (nullValue)"
            tooltip="String representation written for null values (default empty string)."
            value={nullValue}
            onChange={handleChange}
            placeholder="e.g. NULL or \N"
            mono={true}
          />
        </div>

        <PropertyField
          id="csvSinkEmptyValue"
          name="emptyValue"
          label="Empty String Value (emptyValue)"
          tooltip="String representation written when a string column contains an empty string."
          value={emptyValue}
          onChange={handleChange}
          placeholder='e.g. ""'
          mono={true}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="csvSinkInstructions"
        value={systemInstructions}
        placeholder="e.g. Serialize nested struct columns with F.to_json before writing CSV."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark CSV writer code."
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

export const CsvSinkProperties = CsvIOSinkProperties;
