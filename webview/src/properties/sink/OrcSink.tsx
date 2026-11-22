/**
 * @fileoverview ORC Sink (SNK12 OrcSink) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark ORC sink nodes
 * using df.write.orc(uri) or df.writeStream.format("orc").start(uri).
 * Embeds the shared StorageBackendFields component alongside Spark 4.x saveMode,
 * ORC compression codecs, partitionBy columns, maxRecordsPerFile, ORC bloom filter
 * column indexing options, and SystemInstructionsField.
 */

import React from 'react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { StorageBackendFields } from '../../components/common/StorageBackendFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported Spark 4.x batch DataFrameWriter save modes.
 */
const ORC_SAVE_MODES: SelectFieldOption[] = [
  { value: 'append', label: 'append (Append rows to existing path)' },
  { value: 'overwrite', label: 'overwrite (Replace existing data)' },
  { value: 'errorifexists', label: 'errorifexists (Fail if destination exists)' },
  { value: 'ignore', label: 'ignore (Skip write if destination exists)' },
];

/**
 * Supported Spark 4.x ORC compression codecs.
 */
const ORC_COMPRESSION_CODECS: SelectFieldOption[] = [
  { value: 'snappy', label: 'snappy (Default, fast columnar compression)' },
  { value: 'zlib', label: 'zlib (High compression ratio)' },
  { value: 'zstd', label: 'zstd (Balanced high ratio and throughput)' },
  { value: 'lzo', label: 'lzo (Low CPU overhead)' },
  { value: 'none', label: 'none (Uncompressed)' },
];

/**
 * React Flow node shape for the ORC sink panel.
 */
export interface OrcSinkNode {
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
      compression?: 'snappy' | 'zlib' | 'zstd' | 'lzo' | 'none' | string;
      partitionBy?: string | string[];
      maxRecordsPerFile?: string | number;
      bloomFilterColumns?: string | string[];
      bloomFilterFpp?: string | number;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/**
 * Props for the {@link OrcSinkProperties} component.
 */
export interface OrcSinkPropertiesProps {
  node: OrcSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Normalizes a raw ORC compression codec value to a canonical Spark codec name.
 */
function normalizeOrcCompression(raw?: string): string {
  const lower = (raw ?? '').trim().toLowerCase();
  if (lower === 'uncompressed') {
    return 'none';
  }
  if (ORC_COMPRESSION_CODECS.some((opt) => opt.value === lower)) {
    return lower;
  }
  return 'snappy';
}

/**
 * Formats a string or array column list as a comma-separated string.
 */
function resolveCommaList(raw: unknown): string {
  if (Array.isArray(raw)) {
    return raw.join(', ');
  }
  return typeof raw === 'string' ? raw : '';
}

/**
 * Dedicated property panel for Apache Spark OrcSink nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered ORC sink property panel.
 */
export const OrcSinkProperties: React.FC<OrcSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};
  const storageProperties =
    properties.path === undefined && typeof properties.to === 'string' && properties.to.length > 0
      ? { ...properties, path: properties.to }
      : properties;

  const rawSaveMode = (properties.saveMode || properties.mode || 'append').toLowerCase();
  const saveMode = ORC_SAVE_MODES.some((opt) => opt.value === rawSaveMode) ? rawSaveMode : 'append';
  const compression = normalizeOrcCompression(properties.compression);
  const partitionBy = resolveCommaList(properties.partitionBy);
  const maxRecordsPerFile = properties.maxRecordsPerFile ?? '';
  const bloomFilterColumns = resolveCommaList(properties.bloomFilterColumns);
  const bloomFilterFpp = properties.bloomFilterFpp ?? '';
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
        idPrefix="orcSink"
        pathLabel="Output ORC URI / Path"
        pathPlaceholder="gs://my-bucket/output/orc/"
        showGlobAndRecursive={false}
      />

      {/* ORC Writer Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          ORC Writer Options
        </span>

        <SelectField
          id="orcSinkSaveMode"
          name="saveMode"
          label="Save Mode (mode)"
          tooltip="Controls how Spark handles existing data at the destination URI in batch writes. Structured Streaming writes use append mode."
          value={saveMode}
          options={ORC_SAVE_MODES}
          onChange={handleChange}
        />

        <SelectField
          id="orcSinkCompression"
          name="compression"
          label="Compression Codec (compression)"
          tooltip="Columnar compression algorithm applied to output ORC stripes."
          value={compression}
          options={ORC_COMPRESSION_CODECS}
          onChange={handleChange}
        />

        <PropertyField
          id="orcSinkPartitionBy"
          name="partitionBy"
          label="Partition Columns (partitionBy)"
          tooltip="Optional comma-separated column names used to partition output directories in Hive layout (for example, event_date, region)."
          value={partitionBy}
          onChange={handleChange}
          placeholder="e.g. event_date, region"
          mono={true}
        />

        <PropertyField
          id="orcSinkMaxRecordsPerFile"
          type="number"
          name="maxRecordsPerFile"
          label="Max Records Per File (maxRecordsPerFile)"
          tooltip="Optional maximum number of rows written per ORC part-file before rolling to a new file."
          value={maxRecordsPerFile}
          onChange={handleChange}
          placeholder="e.g. 1000000"
          min="1"
          mono={true}
        />

        <PropertyField
          id="orcSinkBloomFilterColumns"
          name="bloomFilterColumns"
          label="Bloom Filter Columns (orc.bloom.filter.columns)"
          tooltip="Optional comma-separated column names on which Spark builds ORC bloom filter indexes for fast predicate pushdown."
          value={bloomFilterColumns}
          onChange={handleChange}
          placeholder="e.g. user_id, event_id"
          mono={true}
        />

        {bloomFilterColumns.trim().length > 0 && (
          <PropertyField
            id="orcSinkBloomFilterFpp"
            type="number"
            name="bloomFilterFpp"
            label="Bloom Filter FPP (orc.bloom.filter.fpp)"
            tooltip="False positive probability for ORC bloom filters (between 0 and 1, default 0.05)."
            value={bloomFilterFpp}
            onChange={handleChange}
            placeholder="0.05"
            mono={true}
          />
        )}
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="orcSinkInstructions"
        value={systemInstructions}
        placeholder="e.g. Sort by user_id within partitions to maximize ORC stripe min/max and bloom filter skipping."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark ORC writer code."
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

export const OrcIOSinkProperties = OrcSinkProperties;
