/**
 * @fileoverview DeltaSink Properties Component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Delta Lake Sink nodes (`DeltaSink`)
 * in Apache Spark 4.x pipelines (`io.delta:delta-spark_2.13:4.0.0`, `.format("delta")`).
 *
 * Architectural Decisions:
 * - Supports `targetMode` (`'path' | 'table'`, default `'path'`) with `StoragePathField` bucket
 *   autocomplete when writing to cloud object storage.
 * - Supports `writeMode` (`'append' | 'overwrite' | 'merge'`, default `'append'`).
 * - Configures selective `replaceWhere` and `overwriteSchema` when `writeMode` equals `'overwrite'`.
 * - Configures `DeltaTable.merge` upsert keys and custom SQL `ON` conditions when `writeMode` equals `'merge'`.
 * - Exposes `mergeSchema`, `partitionBy`, `optimizeWrite`, and idempotent `txnAppId` options.
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

/** Target destination mode options for Delta Lake writes. */
const DELTA_TARGET_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'path', label: 'Storage Path (.save("gs://..."))' },
  { value: 'table', label: 'Catalog Table (.saveAsTable("db.table"))' },
];

/** Write mode options for DeltaSink. */
const DELTA_WRITE_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'append', label: 'append (Append rows to Delta table)' },
  { value: 'overwrite', label: 'overwrite (Overwrite table or replaceWhere)' },
  { value: 'merge', label: 'merge (DeltaTable.merge upsert)' },
];

/** Write mode union type for DeltaSink. */
export type DeltaWriteMode = 'append' | 'overwrite' | 'merge';

/** Target resolution mode union type for DeltaSink. */
export type DeltaTargetMode = 'path' | 'table';

/** React Flow node shape for the DeltaSink panel. */
export interface DeltaSinkNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      targetMode?: DeltaTargetMode;
      path?: string;
      tableName?: string;
      writeMode?: DeltaWriteMode;
      replaceWhere?: string;
      mergeKeys?: string;
      mergeCondition?: string;
      mergeSchema?: boolean;
      overwriteSchema?: boolean;
      partitionBy?: string;
      optimizeWrite?: boolean;
      txnAppId?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link DeltaSinkProperties} component. */
export interface DeltaSinkPropertiesProps {
  node: DeltaSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the configuration panel for Delta Lake batch, upsert, and streaming writes.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React element for the DeltaSink property sheet.
 */
export const DeltaSinkProperties: React.FC<DeltaSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data?.properties || {};

  const targetMode: DeltaTargetMode = properties.targetMode || 'path';
  const path = typeof properties.path === 'string' ? properties.path : '';
  const tableName = typeof properties.tableName === 'string' ? properties.tableName : '';
  const writeMode: DeltaWriteMode = properties.writeMode || 'append';
  const replaceWhere = typeof properties.replaceWhere === 'string' ? properties.replaceWhere : '';
  const mergeKeys = typeof properties.mergeKeys === 'string' ? properties.mergeKeys : '';
  const mergeCondition = typeof properties.mergeCondition === 'string' ? properties.mergeCondition : '';
  const mergeSchema = Boolean(properties.mergeSchema ?? false);
  const overwriteSchema = Boolean(properties.overwriteSchema ?? false);
  const partitionBy = typeof properties.partitionBy === 'string' ? properties.partitionBy : '';
  const optimizeWrite = Boolean(properties.optimizeWrite ?? false);
  const txnAppId = typeof properties.txnAppId === 'string' ? properties.txnAppId : '';

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

  const systemInstructions = resolveSystemInstructions(node);

  const isTableNameInvalid = targetMode === 'table' && !tableName.trim();
  const isMergeMissingCriteria =
    writeMode === 'merge' && !mergeKeys.trim() && !mergeCondition.trim();

  /**
   * Updates node properties when form inputs, selects, or checkboxes change.
   *
   * @param e Change event from an input, select, or textarea control.
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
      {/* Write Mode & Target Mode */}
      <div className="grid grid-cols-2 gap-3">
        <SelectField
          id="deltaSinkWriteMode"
          name="writeMode"
          label="Write Mode"
          tooltip="Select append, overwrite (full or replaceWhere), or DeltaTable.merge upsert."
          value={writeMode}
          options={DELTA_WRITE_MODE_OPTIONS}
          onChange={handleChange}
        />

        <SelectField
          id="deltaSinkTargetMode"
          name="targetMode"
          label="Target Location Mode"
          tooltip="Write to a cloud storage URI (.save(path)) or a catalog table identifier (.saveAsTable(tableName))."
          value={targetMode}
          options={DELTA_TARGET_MODE_OPTIONS}
          onChange={handleChange}
        />
      </div>

      {/* Conditional Path vs Table Name */}
      {targetMode === 'path' ? (
        <StoragePathField
          id="deltaSinkPath"
          name="path"
          label="Delta Table Path"
          tooltip="Cloud Storage, S3, ADLS, or local directory URI of the destination Delta table (for example gs://lakehouse/delta/events). Required."
          value={path}
          onChange={handleChange}
          placeholder="gs://lakehouse/delta/events"
          errorMessage="Delta Table Path is required."
        />
      ) : (
        <PropertyField
          id="deltaSinkTableName"
          name="tableName"
          label="Delta Table Name"
          required="Delta Table Name is required."
          tooltip="Catalog-registered Delta table identifier (for example analytics.events). Required."
          value={tableName}
          onChange={handleChange}
          placeholder="analytics.events"
          invalid={isTableNameInvalid}
          error={isTableNameInvalid ? 'Delta Table Name is required.' : null}
          mono={true}
        />
      )}

      {/* Overwrite Options */}
      {writeMode === 'overwrite' && (
        <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
          <PropertyField
            id="deltaSinkReplaceWhere"
            name="replaceWhere"
            label="Replace Where Predicate (Optional)"
            tooltip="SQL predicate restricting which partitions or rows Delta overwrites atomically (option replaceWhere, for example event_date >= '2026-01-01')."
            value={replaceWhere}
            onChange={handleChange}
            placeholder="event_date >= '2026-01-01'"
            mono={true}
          />

          <label htmlFor="deltaSinkOverwriteSchema" className="flex items-center gap-2 cursor-pointer">
            <input
              id="deltaSinkOverwriteSchema"
              type="checkbox"
              name="overwriteSchema"
              checked={overwriteSchema}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
            />
            <div className="flex items-center">
              <span className="text-sm font-medium text-slate-300">Overwrite Schema (overwriteSchema)</span>
              <InfoTooltip text="Allow full table overwrite operations to replace the existing Delta table schema (option overwriteSchema=true)." />
            </div>
          </label>
        </div>
      )}

      {/* Merge / Upsert Controls */}
      {writeMode === 'merge' && (
        <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
          <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)] flex items-center gap-1.5">
            <span>DeltaTable.merge Upsert Configuration</span>
            <InfoTooltip text="Configure primary key columns or an explicit SQL ON condition for DeltaTable.alias('target').merge(source, condition)." />
          </span>

          <PropertyField
            id="deltaSinkMergeKeys"
            name="mergeKeys"
            label="Merge Key Columns"
            tooltip="Comma-separated key columns matched between target and source tables (for example account_id, region)."
            value={mergeKeys}
            onChange={handleChange}
            placeholder="account_id"
            invalid={isMergeMissingCriteria}
            mono={true}
          />

          <PropertyField
            id="deltaSinkMergeCondition"
            name="mergeCondition"
            label="Custom Merge Condition (Optional)"
            tooltip="Optional SQL ON condition overriding Merge Key Columns (for example target.account_id = source.account_id)."
            value={mergeCondition}
            onChange={handleChange}
            placeholder="target.account_id = source.account_id"
            invalid={isMergeMissingCriteria}
            error={
              isMergeMissingCriteria
                ? 'Provide Merge Key Columns or a Custom Merge Condition when Write Mode is merge.'
                : null
            }
            mono={true}
          />
        </div>
      )}

      {/* Partitioning & Schema Evolution */}
      <PropertyField
        id="deltaSinkPartitionBy"
        name="partitionBy"
        label="Partition Columns (Optional)"
        tooltip="Comma-separated column names passed to .partitionBy(...) when creating or writing the Delta table."
        value={partitionBy}
        onChange={handleChange}
        placeholder="event_date, region"
        mono={true}
      />

      <label htmlFor="deltaSinkMergeSchema" className="flex items-center gap-2 cursor-pointer">
        <input
          id="deltaSinkMergeSchema"
          type="checkbox"
          name="mergeSchema"
          checked={mergeSchema}
          onChange={handleChange}
          className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
        />
        <div className="flex items-center">
          <span className="text-sm font-medium text-slate-300">Merge Schema Evolution (mergeSchema)</span>
          <InfoTooltip text="Automatically add new columns from the incoming DataFrame to the target Delta table schema (option mergeSchema=true)." />
        </div>
      </label>

      {/* Advanced & Idempotency Settings */}
      <CollapsibleSection
        title="Advanced & Idempotency Options"
        icon={<Sliders className="w-4 h-4 text-emerald-400" />}
        isOpen={isAdvancedOpen}
        onToggle={() => setIsAdvancedOpen((prev) => !prev)}
      >
        <PropertyField
          id="deltaSinkTxnAppId"
          name="txnAppId"
          label="Transaction App ID (txnAppId)"
          tooltip="Optional application identifier paired with txnVersion=batch_id for idempotent foreachBatch streaming writes."
          value={txnAppId}
          onChange={handleChange}
          placeholder="spark_delta_writer"
          mono={true}
        />

        <label htmlFor="deltaSinkOptimizeWrite" className="flex items-center gap-2 cursor-pointer mt-2">
          <input
            id="deltaSinkOptimizeWrite"
            type="checkbox"
            name="optimizeWrite"
            checked={optimizeWrite}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Optimize Write Bin-Packing (optimizeWrite)</span>
            <InfoTooltip text="Coalesce small partition files dynamically during Delta writes (option optimizeWrite=true)." />
          </div>
        </label>
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="deltaSinkSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Delta Lake sink node."
        placeholder="Enter custom instructions for Delta Lake sink code generation..."
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
