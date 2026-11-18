/**
 * @fileoverview TFRecord Sink (SNK18 TFRecordSink / TFRecordIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark TFRecord sink nodes
 * using df.write.format("tfrecords").save(uri) and foreachBatch for Structured Streaming.
 * Embeds the shared StorageBackendFields component alongside Spark 4.x saveMode,
 * compression codecs, partitionBy columns, maxRecordsPerFile, recordType
 * (Example / SequenceExample), writeLocality (distributed / local), and
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
export const TFRECORD_SAVE_MODES: SelectFieldOption[] = [
  { value: 'append', label: 'append (Append rows to existing path)' },
  { value: 'overwrite', label: 'overwrite (Replace existing data)' },
  { value: 'errorifexists', label: 'errorifexists (Fail if destination exists)' },
  { value: 'ignore', label: 'ignore (Skip write if destination exists)' },
];

/**
 * Supported Hadoop compression codecs for spark-tfrecord output files.
 */
export const TFRECORD_COMPRESSION_CODECS: SelectFieldOption[] = [
  { value: 'none', label: 'none (Uncompressed TFRecord)' },
  { value: 'gzip', label: 'gzip (Hadoop GzipCodec .tfrecord.gz)' },
  { value: 'deflate', label: 'deflate (Hadoop DeflateCodec)' },
];

/**
 * Supported TensorFlow protobuf message types for spark-tfrecord writes.
 */
export const TFRECORD_WRITE_RECORD_TYPES: SelectFieldOption[] = [
  { value: 'Example', label: 'Example (tf.train.Example flat feature map)' },
  {
    value: 'SequenceExample',
    label: 'SequenceExample (tf.train.SequenceExample sequential feature lists)',
  },
];

/**
 * Supported writeLocality modes in spark-tfrecord.
 */
export const TFRECORD_WRITE_LOCALITY_MODES: SelectFieldOption[] = [
  {
    value: 'distributed',
    label: 'distributed (Write through Hadoop FileSystem to GCS, S3A, ADLS, or HDFS)',
  },
  {
    value: 'local',
    label: 'local (Write directly to executor local filesystem)',
  },
];

/**
 * React Flow node shape for the TFRecord sink panel.
 */
export interface TFRecordIOSinkNode {
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
      compression?: 'none' | 'gzip' | 'deflate' | string;
      compressionType?: string;
      partitionBy?: string | string[];
      maxRecordsPerFile?: string | number;
      recordType?: 'Example' | 'SequenceExample';
      writeLocality?: 'distributed' | 'local';
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: any;
    };
  };
}

/**
 * Props for the {@link TFRecordIOSinkProperties} component.
 */
export interface TFRecordIOSinkPropertiesProps {
  node: TFRecordIOSinkNode;
  onChange: (properties: any) => void;
}

/**
 * Normalizes a raw TFRecord compression setting to a canonical codec key.
 */
function normalizeTFRecordCompression(raw?: string): string {
  const lower = (raw ?? '').trim().toLowerCase();
  if (!lower || lower === 'uncompressed' || lower === 'auto') {
    return 'none';
  }
  if (lower.includes('gzip')) {
    return 'gzip';
  }
  if (lower.includes('deflate')) {
    return 'deflate';
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
 * Dedicated property panel for Apache Spark TFRecordSink / TFRecordIO sink nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered TFRecord sink property panel.
 */
export const TFRecordIOSinkProperties: React.FC<TFRecordIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data.properties || {};
  const storageProperties =
    properties.path === undefined && typeof properties.to === 'string' && properties.to.length > 0
      ? { ...properties, path: properties.to }
      : properties;

  const rawSaveMode = (properties.saveMode || properties.mode || 'append').toLowerCase();
  const saveMode = TFRECORD_SAVE_MODES.some((opt) => opt.value === rawSaveMode)
    ? rawSaveMode
    : 'append';
  const compression = normalizeTFRecordCompression(
    properties.compression || properties.compressionType
  );
  const partitionBy = resolvePartitionByValue(properties.partitionBy);
  const maxRecordsPerFile = properties.maxRecordsPerFile ?? '';
  const recordType =
    properties.recordType === 'SequenceExample' ? 'SequenceExample' : 'Example';
  const writeLocality = properties.writeLocality === 'local' ? 'local' : 'distributed';
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
        idPrefix="tfrecordSink"
        pathLabel="Output TFRecord URI / Path"
        pathPlaceholder="gs://my-bucket/ml/tfrecords_out/"
        showGlobAndRecursive={false}
      />

      {/* TFRecord Writer Save Mode, Compression & Partitioning */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          TFRecord Writer Options (.format(&quot;tfrecords&quot;))
        </span>

        <SelectField
          id="tfrecordSinkSaveMode"
          name="saveMode"
          label="Save Mode (mode)"
          tooltip="Controls how Spark handles existing data at the destination URI in batch writes. Streaming branches execute idempotent micro-batch writes via foreachBatch."
          value={saveMode}
          options={TFRECORD_SAVE_MODES}
          onChange={handleChange}
        />

        <SelectField
          id="tfrecordSinkCompression"
          name="compression"
          label="Compression Codec (codec)"
          tooltip="Hadoop compression codec applied to output TFRecord shards."
          value={compression}
          options={TFRECORD_COMPRESSION_CODECS}
          onChange={handleChange}
        />

        <PropertyField
          id="tfrecordSinkPartitionBy"
          name="partitionBy"
          label="Partition Columns (partitionBy)"
          tooltip="Optional comma-separated column names used to partition output directories (for example, split, dt)."
          value={partitionBy}
          onChange={handleChange}
          placeholder="e.g. split, dt"
          mono={true}
        />

        <PropertyField
          id="tfrecordSinkMaxRecordsPerFile"
          type="number"
          name="maxRecordsPerFile"
          label="Max Records Per File (maxRecordsPerFile)"
          tooltip="Optional maximum number of TensorFlow examples written per TFRecord shard file."
          value={maxRecordsPerFile}
          onChange={handleChange}
          placeholder="e.g. 100000"
          min="1"
          mono={true}
        />
      </div>

      {/* TensorFlow Protobuf & Locality Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          TensorFlow Protobuf &amp; Locality Settings
        </span>

        <SelectField
          id="tfrecordSinkRecordType"
          name="recordType"
          label="TFRecord Protobuf Type (recordType)"
          tooltip="Select Example for flat feature columns or SequenceExample when writing 2-D sequential feature arrays."
          value={recordType}
          options={TFRECORD_WRITE_RECORD_TYPES}
          onChange={handleChange}
        />

        <SelectField
          id="tfrecordSinkWriteLocality"
          name="writeLocality"
          label="Write Locality (writeLocality)"
          tooltip="Select distributed for GCS, S3A, ADLS, and HDFS storage, or local when writing directly onto executor local disks."
          value={writeLocality}
          options={TFRECORD_WRITE_LOCALITY_MODES}
          onChange={handleChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="tfrecordSinkInstructions"
        value={systemInstructions}
        placeholder="e.g. Convert Spark ML VectorUDT features using vector_to_array before writing TFRecord Example shards."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark TFRecord writer code."
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

export const TFRecordSinkProperties = TFRecordIOSinkProperties;
