/**
 * @fileoverview Avro Sink (SNK13 AvroSink / AvroIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Avro sink nodes
 * using df.write.format("avro").save(uri) or df.writeStream.format("avro").start(uri).
 * Embeds the shared StorageBackendFields component alongside Spark 4.x saveMode,
 * Avro compression codecs, partitionBy columns, maxRecordsPerFile, recordName,
 * recordNamespace, explicit avroSchema writer schema JSON, and SystemInstructionsField.
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { StorageBackendFields } from '../../components/common/StorageBackendFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Sample Avro writer schema JSON string for placeholder guidance.
 */
export const SAMPLE_AVRO_WRITER_SCHEMA = JSON.stringify(
  {
    type: 'record',
    name: 'OrderEvent',
    namespace: 'com.example.events',
    fields: [
      { name: 'record_id', type: 'string' },
      { name: 'account_id', type: ['null', 'string'], default: null },
      { name: 'created_epoch_ms', type: 'long' },
    ],
  },
  null,
  2
);

/**
 * Supported Spark 4.x batch DataFrameWriter save modes.
 */
const AVRO_SAVE_MODES: SelectFieldOption[] = [
  { value: 'append', label: 'append (Append rows to existing path)' },
  { value: 'overwrite', label: 'overwrite (Replace existing data)' },
  { value: 'errorifexists', label: 'errorifexists (Fail if destination exists)' },
  { value: 'ignore', label: 'ignore (Skip write if destination exists)' },
];

/**
 * Supported Spark 4.x spark-avro compression codecs.
 */
const AVRO_COMPRESSION_CODECS: SelectFieldOption[] = [
  { value: 'snappy', label: 'snappy (Default, fast low-CPU compression)' },
  { value: 'deflate', label: 'deflate (Standard balanced Avro compression)' },
  { value: 'zstandard', label: 'zstandard (Modern high-ratio compression)' },
  { value: 'bzip2', label: 'bzip2 (High compression ratio)' },
  { value: 'xz', label: 'xz (Maximum archival compression)' },
  { value: 'uncompressed', label: 'uncompressed (No block compression)' },
];

/**
 * React Flow node shape for the Avro sink panel.
 */
export interface AvroIOSinkNode {
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
      compression?: 'snappy' | 'deflate' | 'bzip2' | 'xz' | 'zstandard' | 'uncompressed' | string;
      codec?: string;
      partitionBy?: string | string[];
      maxRecordsPerFile?: string | number;
      recordName?: string;
      recordNamespace?: string;
      avroSchema?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/**
 * Props for the {@link AvroIOSinkProperties} component.
 */
export interface AvroIOSinkPropertiesProps {
  node: AvroIOSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Normalizes a raw Avro compression codec value to a canonical spark-avro codec name.
 */
function normalizeAvroCompression(raw?: string): string {
  const lower = (raw ?? '').trim().toLowerCase();
  if (lower === 'null' || lower === 'none') {
    return 'uncompressed';
  }
  if (lower === 'zstd') {
    return 'zstandard';
  }
  if (AVRO_COMPRESSION_CODECS.some((opt) => opt.value === lower)) {
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
 * Dedicated property panel for Apache Spark AvroSink / AvroIO sink nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered Avro sink property panel.
 */
export const AvroIOSinkProperties: React.FC<AvroIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};
  const storageProperties =
    properties.path === undefined && typeof properties.to === 'string' && properties.to.length > 0
      ? { ...properties, path: properties.to }
      : properties;

  const rawSaveMode = (properties.saveMode || properties.mode || 'append').toLowerCase();
  const saveMode = AVRO_SAVE_MODES.some((opt) => opt.value === rawSaveMode)
    ? rawSaveMode
    : 'append';
  const compression = normalizeAvroCompression(properties.compression || properties.codec);
  const partitionBy = resolvePartitionByValue(properties.partitionBy);
  const maxRecordsPerFile = properties.maxRecordsPerFile ?? '';
  const recordName = properties.recordName ?? '';
  const recordNamespace = properties.recordNamespace ?? '';
  const avroSchema = properties.avroSchema ?? '';
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
        idPrefix="avroSink"
        pathLabel="Output Avro URI / Path"
        pathPlaceholder="gs://my-bucket/output/avro/"
        showGlobAndRecursive={false}
      />

      {/* Avro Writer Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          Avro Writer Options (.format(&quot;avro&quot;))
        </span>

        <SelectField
          id="avroSinkSaveMode"
          name="saveMode"
          label="Save Mode (mode)"
          tooltip="Controls how Spark handles existing data at the destination URI in batch writes. Structured Streaming writes use append mode."
          value={saveMode}
          options={AVRO_SAVE_MODES}
          onChange={handleChange}
        />

        <SelectField
          id="avroSinkCompression"
          name="compression"
          label="Compression Codec (compression)"
          tooltip="Avro container block compression algorithm applied to output files."
          value={compression}
          options={AVRO_COMPRESSION_CODECS}
          onChange={handleChange}
        />

        <PropertyField
          id="avroSinkPartitionBy"
          name="partitionBy"
          label="Partition Columns (partitionBy)"
          tooltip="Optional comma-separated column names used to partition output directories in Hive layout (for example, event_date, region)."
          value={partitionBy}
          onChange={handleChange}
          placeholder="e.g. event_date, region"
          mono={true}
        />

        <PropertyField
          id="avroSinkMaxRecordsPerFile"
          type="number"
          name="maxRecordsPerFile"
          label="Max Records Per File (maxRecordsPerFile)"
          tooltip="Optional maximum number of rows written per output Avro file."
          value={maxRecordsPerFile}
          onChange={handleChange}
          placeholder="e.g. 500000"
          min="1"
          mono={true}
        />

        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="avroSinkRecordName"
            name="recordName"
            label="Record Name (recordName)"
            tooltip="Optional top-level Avro record name written into the output Avro container schema (defaults to topLevelRecord)."
            value={recordName}
            onChange={handleChange}
            placeholder="topLevelRecord"
            mono={true}
          />

          <PropertyField
            id="avroSinkRecordNamespace"
            name="recordNamespace"
            label="Record Namespace (recordNamespace)"
            tooltip="Optional Avro namespace string qualifying the output record schema (for example, com.example.events)."
            value={recordNamespace}
            onChange={handleChange}
            placeholder="com.example.events"
            mono={true}
          />
        </div>

        <PropertyField
          id="avroSinkAvroSchema"
          type="textarea"
          name="avroSchema"
          label="Avro Writer Schema JSON (avroSchema)"
          tooltip="Optional explicit Avro JSON schema passed to .option('avroSchema', ...). Leave blank to derive the Avro schema from the DataFrame StructType."
          value={avroSchema}
          onChange={handleChange}
          placeholder={SAMPLE_AVRO_WRITER_SCHEMA}
          rows={5}
          mono={true}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="avroSinkInstructions"
        value={systemInstructions}
        placeholder="e.g. Enforce the registered Schema Registry Avro schema and zstandard compression."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark Avro writer code."
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

export const AvroSinkProperties = AvroIOSinkProperties;
