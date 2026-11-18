/**
 * DeltaSource Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Delta Lake Source nodes (`DeltaSource`)
 * in Apache Spark 4.x pipelines (`io.delta:delta-spark_2.13:4.0.0`, `.format("delta")`).
 *
 * Architectural Decisions:
 * - Supports `readMode` (`'batch' | 'streaming'`, default `'batch'`).
 * - Supports `sourceMode` (`'path' | 'table'`, default `'path'`) with `StoragePathField` bucket
 *   autocomplete when reading by storage path (`gs://`, `s3a://`, `abfss://`).
 * - Configures batch time-travel (`versionAsOf`, `timestampAsOf`) with mutual exclusivity validation.
 * - Configures Delta Change Data Feed (`readChangeFeed` toggle, `startingVersion`, `startingTimestamp`)
 *   and streaming micro-batch controls (`maxFilesPerTrigger`, `maxBytesPerTrigger`, `ignoreChanges`,
 *   `ignoreDeletes`).
 * - Uses shared primitives on the 8pt grid system.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { StoragePathField } from '../../components/common/StoragePathField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Read mode options for DeltaSource. */
export const DELTA_READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'Batch (spark.read.format("delta"))' },
  { value: 'streaming', label: 'Streaming (spark.readStream.format("delta"))' },
];

/** Source resolution mode options for Delta Lake tables. */
export const DELTA_SOURCE_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'path', label: 'Storage Path (.load("gs://..."))' },
  { value: 'table', label: 'Catalog Table (.table("db.table"))' },
];

/** React Flow node shape for the DeltaSource panel. */
export interface DeltaSourceNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      readMode?: 'batch' | 'streaming';
      sourceMode?: 'path' | 'table';
      path?: string;
      tableName?: string;
      versionAsOf?: number | string;
      timestampAsOf?: string;
      readChangeFeed?: boolean;
      startingVersion?: string | number;
      startingTimestamp?: string;
      maxFilesPerTrigger?: number | string;
      maxBytesPerTrigger?: string;
      ignoreChanges?: boolean;
      ignoreDeletes?: boolean;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: any;
    };
  };
}

/** Props for the {@link DeltaSourceProperties} component. */
export interface DeltaSourcePropertiesProps {
  node: DeltaSourceNode;
  onChange: (properties: any) => void;
}

/**
 * DeltaSourceProperties renders the configuration panel for Delta Lake batch and streaming reads.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React component.
 */
export const DeltaSourceProperties: React.FC<DeltaSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const readMode = properties.readMode || 'batch';
  const sourceMode = properties.sourceMode || 'path';
  const path = properties.path || '';
  const tableName = properties.tableName || '';
  const versionAsOf = properties.versionAsOf ?? '';
  const timestampAsOf = properties.timestampAsOf || '';
  const readChangeFeed = properties.readChangeFeed ?? false;
  const startingVersion = properties.startingVersion ?? '';
  const startingTimestamp = properties.startingTimestamp || '';
  const maxFilesPerTrigger = properties.maxFilesPerTrigger ?? '';
  const maxBytesPerTrigger = properties.maxBytesPerTrigger || '';
  const ignoreChanges = properties.ignoreChanges ?? false;
  const ignoreDeletes = properties.ignoreDeletes ?? false;

  const systemInstructions = resolveSystemInstructions(node);

  const isTableNameInvalid = sourceMode === 'table' && !tableName.trim();
  const hasVersionAsOf = String(versionAsOf).trim().length > 0;
  const hasTimestampAsOf = timestampAsOf.trim().length > 0;
  const hasTimeTravelConflict = hasVersionAsOf && hasTimestampAsOf;

  /**
   * Handles generic text, select, and checkbox input changes.
   *
   * @param e Form change event from input, select, or textarea.
   */
  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
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
      {/* Read Mode & Source Mode */}
      <div className="grid grid-cols-2 gap-3">
        <SelectField
          id="deltaReadMode"
          name="readMode"
          label="Read Mode"
          tooltip="Select batch mode (spark.read.format('delta')) or streaming mode (spark.readStream.format('delta'))."
          value={readMode}
          options={DELTA_READ_MODE_OPTIONS}
          onChange={handleChange}
        />

        <SelectField
          id="deltaSourceMode"
          name="sourceMode"
          label="Source Location Mode"
          tooltip="Locate the Delta table by cloud storage path (.load(path)) or catalog table identifier (.table(tableName))."
          value={sourceMode}
          options={DELTA_SOURCE_MODE_OPTIONS}
          onChange={handleChange}
        />
      </div>

      {/* Conditional Path vs Table Name */}
      {sourceMode === 'path' ? (
        <StoragePathField
          id="deltaPath"
          name="path"
          label="Delta Table Path"
          tooltip="Cloud Storage, S3, ADLS, or local directory URI of the Delta Lake table (e.g. gs://lakehouse/delta/events). Required."
          value={path}
          onChange={handleChange}
          placeholder="gs://lakehouse/delta/events"
          errorMessage="Delta Table Path is required."
        />
      ) : (
        <PropertyField
          id="deltaTableName"
          name="tableName"
          label="Delta Table Name"
          required="Delta Table Name is required."
          tooltip="Catalog-registered Delta table identifier (e.g. default.events or spark_catalog.analytics.events). Required."
          value={tableName}
          onChange={handleChange}
          placeholder="analytics.events"
          invalid={isTableNameInvalid}
          error={isTableNameInvalid ? 'Delta Table Name is required.' : null}
          mono={true}
        />
      )}

      {/* Batch Time Travel Controls */}
      {readMode === 'batch' && (
        <div className="flex flex-col gap-2 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
          <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)] flex items-center gap-1.5">
            <span>Time Travel (Batch)</span>
            <InfoTooltip text="Query a historical version of the Delta table by commit version number (versionAsOf) or timestamp (timestampAsOf)." />
          </span>
          <div className="grid grid-cols-2 gap-3">
            <PropertyField
              id="deltaVersionAsOf"
              name="versionAsOf"
              label="Version As Of"
              type="number"
              min="0"
              tooltip="Delta commit version integer to query (option versionAsOf). Mutually exclusive with the commit timestamp option."
              value={versionAsOf}
              onChange={handleChange}
              placeholder="e.g. 42"
              invalid={hasTimeTravelConflict}
              mono={true}
            />

            <PropertyField
              id="deltaTimestampAsOf"
              name="timestampAsOf"
              label="Timestamp As Of"
              tooltip="Commit timestamp string to query (option timestampAsOf, e.g. 2026-01-01T00:00:00Z). Mutually exclusive with the commit version option."
              value={timestampAsOf}
              onChange={handleChange}
              placeholder="2026-01-01T00:00:00Z"
              invalid={hasTimeTravelConflict}
              mono={true}
            />
          </div>
          {hasTimeTravelConflict && (
            <span className="text-xs text-red-400 mt-1" role="alert">
              Cannot specify both Version As Of and Timestamp As Of.
            </span>
          )}
        </div>
      )}

      {/* Change Data Feed (CDF) & Starting Position */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <label htmlFor="deltaReadChangeFeed" className="flex items-center gap-2 cursor-pointer">
          <input
            id="deltaReadChangeFeed"
            type="checkbox"
            name="readChangeFeed"
            checked={readChangeFeed}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Read Change Data Feed (CDF)</span>
            <InfoTooltip text="Enable Delta Change Data Feed (option readChangeFeed=true) to emit row-level insert, update_preimage, update_postimage, and delete events." />
          </div>
        </label>

        {(readChangeFeed || readMode === 'streaming') && (
          <div className="grid grid-cols-2 gap-3 pt-1">
            <PropertyField
              id="deltaStartingVersion"
              name="startingVersion"
              label="Starting Version"
              tooltip="Starting Delta table version for streaming or Change Data Feed reads (option startingVersion, e.g. 0 or latest)."
              value={startingVersion}
              onChange={handleChange}
              placeholder="latest or 0"
              datalist={['latest', '0']}
              mono={true}
            />

            <PropertyField
              id="deltaStartingTimestamp"
              name="startingTimestamp"
              label="Starting Timestamp"
              tooltip="Starting timestamp for streaming or Change Data Feed reads (option startingTimestamp, e.g. 2026-01-01T00:00:00Z)."
              value={startingTimestamp}
              onChange={handleChange}
              placeholder="2026-01-01T00:00:00Z"
              mono={true}
            />
          </div>
        )}
      </div>

      {/* Streaming Rate Limits & Mutation Handling */}
      {readMode === 'streaming' && (
        <div className="flex flex-col gap-3 p-3 bg-slate-800/40 rounded border border-slate-700/50">
          <div className="grid grid-cols-2 gap-3">
            <PropertyField
              id="deltaMaxFilesPerTrigger"
              name="maxFilesPerTrigger"
              label="Max Files Per Trigger"
              type="number"
              min="1"
              tooltip="Maximum number of new Parquet files processed in each streaming micro-batch (option maxFilesPerTrigger, default 1000)."
              value={maxFilesPerTrigger}
              onChange={handleChange}
              placeholder="1000"
              mono={true}
            />

            <PropertyField
              id="deltaMaxBytesPerTrigger"
              name="maxBytesPerTrigger"
              label="Max Bytes Per Trigger"
              tooltip="Soft maximum volume of data processed per micro-batch trigger (option maxBytesPerTrigger, e.g. 1g or 512m)."
              value={maxBytesPerTrigger}
              onChange={handleChange}
              placeholder="e.g. 1g"
              mono={true}
            />
          </div>

          <label htmlFor="deltaIgnoreChanges" className="flex items-center gap-2 cursor-pointer">
            <input
              id="deltaIgnoreChanges"
              type="checkbox"
              name="ignoreChanges"
              checked={ignoreChanges}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
            />
            <div className="flex items-center">
              <span className="text-sm font-medium text-slate-300">Ignore Changes</span>
              <InfoTooltip text="Continue streaming when data files are rewritten by UPDATE, MERGE, or DELETE operations (option ignoreChanges)." />
            </div>
          </label>

          <label htmlFor="deltaIgnoreDeletes" className="flex items-center gap-2 cursor-pointer">
            <input
              id="deltaIgnoreDeletes"
              type="checkbox"
              name="ignoreDeletes"
              checked={ignoreDeletes}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
            />
            <div className="flex items-center">
              <span className="text-sm font-medium text-slate-300">Ignore Deletes</span>
              <InfoTooltip text="Ignore transactions that delete data at partition boundaries (option ignoreDeletes)." />
            </div>
          </label>
        </div>
      )}

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="deltaSourceSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Delta Lake source node."
        placeholder="Enter custom instructions for Delta Lake source code generation..."
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
