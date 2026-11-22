/**
 * HudiSource Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Hudi Source nodes (`HudiSource`)
 * in Apache Spark 4.x pipelines (`org.apache.hudi:hudi-spark4.0-bundle_2.13`, `.format("hudi")`).
 *
 * Architectural Decisions:
 * - Supports `readMode` (`'batch' | 'streaming'`, default `'batch'`).
 * - Configures `path` (via `StoragePathField` with Cloud Storage bucket autocomplete), `tableName`,
 *   `queryType` (`'snapshot' | 'incremental' | 'read_optimized'`, default `'snapshot'`),
 *   `beginInstantTime`, `endInstantTime`, and `readPaths`.
 * - Uses shared primitives (`PropertyField`, `SelectField`, `StoragePathField`,
 *   `SystemInstructionsField`) on the 8pt grid system.
 */

import React from 'react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { StoragePathField } from '../../components/common/StoragePathField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Read mode options for HudiSource. */
const HUDI_READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'Batch (spark.read.format("hudi"))' },
  { value: 'streaming', label: 'Streaming (spark.readStream.format("hudi"))' },
];

/** Hudi query type options (`hoodie.datasource.query.type`). */
const HUDI_QUERY_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'snapshot', label: 'snapshot (Latest table state across base + log files)' },
  { value: 'incremental', label: 'incremental (Changes since beginInstantTime)' },
  { value: 'read_optimized', label: 'read_optimized (Columnar base files only for MOR tables)' },
];

/** React Flow node shape for the HudiSource panel. */
export interface HudiSourceNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      readMode?: 'batch' | 'streaming';
      path?: string;
      tableName?: string;
      queryType?: 'snapshot' | 'incremental' | 'read_optimized';
      beginInstantTime?: string;
      endInstantTime?: string;
      readPaths?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link HudiSourceProperties} component. */
export interface HudiSourcePropertiesProps {
  node: HudiSourceNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * HudiSourceProperties renders the configuration panel for Apache Hudi batch and streaming reads.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React component.
 */
export const HudiSourceProperties: React.FC<HudiSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const readMode = properties.readMode || 'batch';
  const path = properties.path || '';
  const tableName = properties.tableName || '';
  const queryType = properties.queryType || 'snapshot';
  const beginInstantTime = properties.beginInstantTime || '';
  const endInstantTime = properties.endInstantTime || '';
  const readPaths = properties.readPaths || '';

  const systemInstructions = resolveSystemInstructions(node);

  const showIncrementalInstants = queryType === 'incremental' || readMode === 'streaming';
  const isBeginInstantMissing = queryType === 'incremental' && readMode === 'batch' && !beginInstantTime.trim();

  /**
   * Handles generic text and select input changes.
   *
   * @param e Form change event from input, select, or textarea.
   */
  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;

    onChange({
      ...properties,
      [name]: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Read Mode & Query Type */}
      <div className="grid grid-cols-2 gap-3">
        <SelectField
          id="hudiReadMode"
          name="readMode"
          label="Read Mode"
          tooltip="Select batch mode (spark.read.format('hudi')) or streaming mode (spark.readStream.format('hudi'))."
          value={readMode}
          options={HUDI_READ_MODE_OPTIONS}
          onChange={handleChange}
        />

        <SelectField
          id="hudiQueryType"
          name="queryType"
          label="Query Type"
          tooltip="Hudi table query mode (option hoodie.datasource.query.type): snapshot, incremental, or read_optimized."
          value={queryType}
          options={HUDI_QUERY_TYPE_OPTIONS}
          onChange={handleChange}
        />
      </div>

      {/* Hudi Base Path (Required) with GCS bucket autocomplete */}
      <StoragePathField
        id="hudiPath"
        name="path"
        label="Hudi Table Base Path"
        tooltip="Cloud Storage, S3, or HDFS base path of the Apache Hudi table (e.g. gs://lakehouse/hudi/orders). Required."
        value={path}
        onChange={handleChange}
        placeholder="gs://lakehouse/hudi/orders"
        errorMessage="Hudi Table Base Path is required."
      />

      {/* Hudi Table Name */}
      <PropertyField
        id="hudiTableName"
        name="tableName"
        label="Hudi Table Name"
        tooltip="Optional logical table identifier (hoodie.table.name) or metastore catalog entry."
        value={tableName}
        onChange={handleChange}
        placeholder="orders_hudi"
        mono={true}
      />

      {/* Incremental Commit Instant Range */}
      {showIncrementalInstants && (
        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="hudiBeginInstantTime"
            name="beginInstantTime"
            label="Begin Instant Time"
            required={queryType === 'incremental' && readMode === 'batch' ? 'Begin Instant Time is required for incremental queries.' : false}
            tooltip="Start commit instant timestamp (YYYYMMDDHHmmssSSS or 000) for incremental reads (option hoodie.datasource.read.begin.instanttime)."
            value={beginInstantTime}
            onChange={handleChange}
            placeholder="000 or 20260101000000000"
            invalid={isBeginInstantMissing}
            error={isBeginInstantMissing ? 'Begin Instant Time is required for incremental queries.' : null}
            mono={true}
          />

          <PropertyField
            id="hudiEndInstantTime"
            name="endInstantTime"
            label="End Instant Time"
            tooltip="Optional end commit instant timestamp for bounded incremental queries (option hoodie.datasource.read.end.instanttime)."
            value={endInstantTime}
            onChange={handleChange}
            placeholder="e.g. 20260102000000000"
            mono={true}
          />
        </div>
      )}

      {/* Specific Partition Read Paths */}
      <PropertyField
        id="hudiReadPaths"
        name="readPaths"
        label="Specific Read Paths (Optional)"
        tooltip="Comma-separated list of specific partition paths or globs to read (option hoodie.datasource.read.paths)."
        value={readPaths}
        onChange={handleChange}
        placeholder="gs://lakehouse/hudi/orders/2026/01/*"
        mono={true}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="hudiSourceSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Hudi source node."
        placeholder="Enter custom instructions for Apache Hudi source code generation..."
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
