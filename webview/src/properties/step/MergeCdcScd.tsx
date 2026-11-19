/**
 * @fileoverview Property configuration panel for the Apache Spark MergeCdcScd step (MergeCdcScd).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import { GitMerge, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported Slowly Changing Dimension strategies for `MergeCdcScd`.
 */
export type ScdType = 'scd1' | 'scd2';

/**
 * Supported Change Data Capture operation envelope formats.
 */
export type CdcFormat = 'debezium' | 'delta_cdf' | 'custom_op' | 'upsert_only';

/**
 * Sentinel value assigned to `effectiveToColumn` on active SCD Type 2 rows.
 */
export type Scd2OpenEndValue = 'null' | '9999-12-31';

/**
 * Persisted properties for the Spark `MergeCdcScd` step node.
 */
export interface MergeCdcScdStepProperties {
  scdType?: ScdType;
  keyColumns?: string[];
  sequenceColumn?: string;
  cdcFormat?: CdcFormat;
  operationColumn?: string;
  deleteValues?: string | string[];
  trackedColumns?: string[];
  effectiveFromColumn?: string;
  effectiveToColumn?: string;
  isCurrentColumn?: string;
  openEndValue?: Scd2OpenEndValue;
  actionColumn?: string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link MergeCdcScdProperties}.
 */
export interface MergeCdcScdNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: MergeCdcScdStepProperties;
  };
}

/**
 * Props for the {@link MergeCdcScdProperties} component.
 */
export interface MergeCdcScdPropertiesProps {
  node: MergeCdcScdNode;
  onChange: (properties: MergeCdcScdStepProperties) => void;
}

const SCD_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'scd1', label: 'SCD Type 1 (Overwrite latest state + deletes)' },
  { value: 'scd2', label: 'SCD Type 2 (Historical versions with effective dates)' },
];

const CDC_FORMAT_OPTIONS: SelectFieldOption[] = [
  { value: 'debezium', label: 'Debezium Envelope (op == "d" for deletes)' },
  { value: 'delta_cdf', label: 'Delta Change Data Feed (_change_type)' },
  { value: 'custom_op', label: 'Custom Operation Column & Delete Markers' },
  { value: 'upsert_only', label: 'Upsert Only (No delete tombstones)' },
];

const OPEN_END_VALUE_OPTIONS: SelectFieldOption[] = [
  { value: 'null', label: 'NULL (Open-ended active version)' },
  { value: '9999-12-31', label: '9999-12-31 (High-date sentinel)' },
];

const VALID_CDC_FORMATS = new Set<string>(CDC_FORMAT_OPTIONS.map((opt) => opt.value));

/**
 * Normalizes a raw `scdType` property value into a canonical {@link ScdType}.
 *
 * @param raw Raw `scdType` property value.
 * @returns Canonical SCD strategy defaulting to `'scd1'`.
 */
export function normalizeScdType(raw: unknown): ScdType {
  return raw === 'scd2' ? 'scd2' : 'scd1';
}

/**
 * Normalizes a raw `cdcFormat` property value into a canonical {@link CdcFormat}.
 *
 * @param raw Raw `cdcFormat` property value.
 * @returns Canonical CDC format defaulting to `'debezium'`.
 */
export function normalizeCdcFormat(raw: unknown): CdcFormat {
  if (typeof raw === 'string' && VALID_CDC_FORMATS.has(raw)) {
    return raw as CdcFormat;
  }
  return 'debezium';
}

/**
 * Normalizes a raw column list from YAML array or comma-separated string into `string[]`.
 *
 * @param raw Raw `keyColumns` or `trackedColumns` value.
 * @returns Trimmed list of non-empty column names.
 */
export function normalizeColumnList(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw
      .map((item) => String(item ?? '').trim())
      .filter((item) => item.length > 0);
  }
  if (typeof raw === 'string' && raw.trim().length > 0) {
    return raw
      .split(',')
      .map((part) => part.trim())
      .filter((part) => part.length > 0);
  }
  return [];
}

/**
 * Normalizes `deleteValues` from string or string array into a display string.
 *
 * @param raw Raw `deleteValues` value.
 * @returns Comma-separated delete marker string.
 */
export function normalizeDeleteValuesText(raw: unknown): string {
  if (Array.isArray(raw)) {
    return raw
      .map((item) => String(item ?? '').trim())
      .filter((item) => item.length > 0)
      .join(', ');
  }
  if (typeof raw === 'string') {
    return raw;
  }
  return 'D, DELETE';
}

/**
 * Renders the property configuration panel for the Apache Spark MergeCdcScd step.
 *
 * @param props Selected MergeCdcScd node and property update callback.
 * @returns The rendered MergeCdcScd configuration form.
 */
export const MergeCdcScdProperties: React.FC<MergeCdcScdPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: MergeCdcScdStepProperties = node?.data?.properties || {};
  const scdType = normalizeScdType(properties.scdType);
  const keyColumns = normalizeColumnList(properties.keyColumns);
  const sequenceColumn =
    typeof properties.sequenceColumn === 'string' ? properties.sequenceColumn : '';
  const cdcFormat = normalizeCdcFormat(properties.cdcFormat);
  const operationColumn =
    typeof properties.operationColumn === 'string' ? properties.operationColumn : 'op';
  const deleteValues = normalizeDeleteValuesText(properties.deleteValues);
  const trackedColumns = normalizeColumnList(properties.trackedColumns);
  const effectiveFromColumn =
    typeof properties.effectiveFromColumn === 'string'
      ? properties.effectiveFromColumn
      : 'effective_from';
  const effectiveToColumn =
    typeof properties.effectiveToColumn === 'string'
      ? properties.effectiveToColumn
      : 'effective_to';
  const isCurrentColumn =
    typeof properties.isCurrentColumn === 'string' ? properties.isCurrentColumn : 'is_current';
  const openEndValue: Scd2OpenEndValue =
    properties.openEndValue === '9999-12-31' ? '9999-12-31' : 'null';
  const actionColumn =
    typeof properties.actionColumn === 'string' ? properties.actionColumn : '_scd_action';
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingKeyCol, setPendingKeyCol] = useState<string>('');
  const [pendingTrackedCol, setPendingTrackedCol] = useState<string>('');

  const isKeyColumnsInvalid = keyColumns.length === 0;
  const isSequenceColumnInvalid = sequenceColumn.trim().length === 0;
  const isCustomOp = cdcFormat === 'custom_op';
  const isOperationColumnInvalid = isCustomOp && operationColumn.trim().length === 0;
  const isDeleteValuesInvalid = isCustomOp && deleteValues.trim().length === 0;
  const isScd2 = scdType === 'scd2';
  const isEffectiveFromInvalid = isScd2 && effectiveFromColumn.trim().length === 0;
  const isEffectiveToInvalid = isScd2 && effectiveToColumn.trim().length === 0;
  const isIsCurrentInvalid = isScd2 && isCurrentColumn.trim().length === 0;
  const isActionColumnInvalid = actionColumn.trim().length === 0;

  const emitPatch = (patch: Partial<MergeCdcScdStepProperties>) => {
    onChange({
      ...properties,
      scdType,
      keyColumns,
      sequenceColumn,
      cdcFormat,
      operationColumn,
      deleteValues,
      trackedColumns,
      effectiveFromColumn,
      effectiveToColumn,
      isCurrentColumn,
      openEndValue,
      actionColumn,
      ...patch,
    });
  };

  const handleScdTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitPatch({ scdType: normalizeScdType(e.target.value) });
  };

  const handleCdcFormatChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitPatch({ cdcFormat: normalizeCdcFormat(e.target.value) });
  };

  const handleOpenEndValueChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitPatch({
      openEndValue: e.target.value === '9999-12-31' ? '9999-12-31' : 'null',
    });
  };

  const handleTextFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    emitPatch({ [name]: value });
  };

  const appendKeyColumns = () => {
    const parsed = normalizeColumnList(pendingKeyCol);
    if (parsed.length === 0) {
      return;
    }
    const seen = new Set(keyColumns);
    const merged = [...keyColumns];
    for (const col of parsed) {
      if (!seen.has(col)) {
        seen.add(col);
        merged.push(col);
      }
    }
    setPendingKeyCol('');
    emitPatch({ keyColumns: merged });
  };

  const handlePendingKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendKeyColumns();
    }
  };

  const handleKeyColumnEdit = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...keyColumns];
    updated[idx] = e.target.value;
    emitPatch({ keyColumns: normalizeColumnList(updated) });
  };

  const handleRemoveKeyColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    emitPatch({ keyColumns: keyColumns.filter((_, i) => i !== idx) });
  };

  const appendTrackedColumns = () => {
    const parsed = normalizeColumnList(pendingTrackedCol);
    if (parsed.length === 0) {
      return;
    }
    const seen = new Set(trackedColumns);
    const merged = [...trackedColumns];
    for (const col of parsed) {
      if (!seen.has(col)) {
        seen.add(col);
        merged.push(col);
      }
    }
    setPendingTrackedCol('');
    emitPatch({ trackedColumns: merged });
  };

  const handlePendingTrackedKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendTrackedColumns();
    }
  };

  const handleTrackedColumnEdit = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...trackedColumns];
    updated[idx] = e.target.value;
    emitPatch({ trackedColumns: normalizeColumnList(updated) });
  };

  const handleRemoveTrackedColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    emitPatch({ trackedColumns: trackedColumns.filter((_, i) => i !== idx) });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Dual-handle CDC + Target Snapshot wiring banner */}
      <div
        data-testid="merge-cdc-scd-handle-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <GitMerge className="w-4 h-4 text-blue-400 shrink-0" />
          <span>CDC Stream & Target Snapshot Inputs</span>
          <InfoTooltip text="Connect the incoming CDC stream or batch source to the top L (left) handle and the current target table snapshot source to the bottom R (right) handle." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          Connect the incoming CDC feed to <code className="font-mono text-blue-300">L</code>{' '}
          (left) and the current target dimension or table snapshot to{' '}
          <code className="font-mono text-indigo-300">R</code> (right). This step deduplicates
          keys, skips no-op updates via <code className="font-mono">F.xxhash64</code>, and stages
          pure SCD merge rows without external catalog I/O inside{' '}
          <code className="font-mono">transform()</code>.
        </p>
      </div>

      {/* 1. SCD Strategy, Primary Keys & Sequence Column */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <SelectField
          id="mergeCdcScdType"
          name="scdType"
          label="SCD Strategy"
          tooltip="Choose SCD Type 1 to overwrite matching keys and apply deletes in place, or SCD Type 2 to expire changed active rows and insert versioned history rows."
          value={scdType}
          options={SCD_TYPE_OPTIONS}
          onChange={handleScdTypeChange}
        />

        {/* Primary Key Columns List Editor */}
        <div className="flex flex-col gap-2" data-testid="merge-cdc-keys-section">
          <label
            htmlFor="mergeCdcKeyAddInput"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Primary Key Columns</span>
            <span className="text-red-400 font-bold">*</span>
            <InfoTooltip text="One or more primary business key columns shared by the incoming CDC input and target table." />
          </label>

          {keyColumns.length > 0 && (
            <div className="flex flex-col gap-2" data-testid="merge-cdc-keys-list">
              {keyColumns.map((col, index) => (
                <div key={`${index}-${col}`} className="flex items-center gap-2">
                  <input
                    type="text"
                    data-index={index}
                    aria-label={`Key column ${index + 1}`}
                    value={col}
                    onChange={handleKeyColumnEdit}
                    className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                  />
                  <button
                    type="button"
                    data-index={index}
                    aria-label={`Remove key column ${col}`}
                    onClick={handleRemoveKeyColumn}
                    className="p-2 rounded border border-[var(--vscode-border)] text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              ))}
            </div>
          )}

          <div className="flex items-center gap-2">
            <input
              id="mergeCdcKeyAddInput"
              type="text"
              value={pendingKeyCol}
              onChange={(e) => setPendingKeyCol(e.target.value)}
              onKeyDown={handlePendingKeyDown}
              placeholder="e.g. customer_id or tenant_id, account_id"
              className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
            />
            <button
              type="button"
              onClick={appendKeyColumns}
              className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
            >
              <Plus className="w-3.5 h-3.5" />
              <span>Add Key</span>
            </button>
          </div>

          {isKeyColumnsInvalid && (
            <span className="text-xs text-red-400" role="alert">
              At least one primary key column is required.
            </span>
          )}
        </div>

        <PropertyField
          id="mergeCdcSequenceColumn"
          name="sequenceColumn"
          label="Sequence / Event Timestamp Column"
          required="Required"
          tooltip="Monotonic timestamp, version, or LSN column passed to Window.partitionBy(*keys).orderBy(F.col(seq).desc_nulls_last()) to keep the newest event per key and populate SCD Type 2 effective dates."
          value={sequenceColumn}
          onChange={handleTextFieldChange}
          placeholder="updated_at"
          mono={true}
          invalid={isSequenceColumnInvalid}
          error={isSequenceColumnInvalid ? 'Sequence column is required.' : null}
        />
      </div>

      {/* 2. CDC Operation Format */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <SelectField
          id="mergeCdcFormat"
          name="cdcFormat"
          label="CDC Operation Format"
          tooltip="Select how delete tombstones and change envelopes are represented on the incoming left CDC DataFrame."
          value={cdcFormat}
          options={CDC_FORMAT_OPTIONS}
          onChange={handleCdcFormatChange}
          helperText={
            cdcFormat === 'debezium' ? (
              <span>
                Treats <code>{'op = \'d\''}</code> as delete and strips envelope metadata.
              </span>
            ) : cdcFormat === 'delta_cdf' ? (
              <span>
                Filters out <code>update_preimage</code> rows and treats{' '}
                <code>{'_change_type = \'delete\''}</code> as delete.
              </span>
            ) : cdcFormat === 'upsert_only' ? (
              <span>Treats all deduplicated incoming rows as inserts or updates.</span>
            ) : null
          }
        />

        {isCustomOp && (
          <div
            data-testid="merge-cdc-custom-op-section"
            className="grid grid-cols-2 gap-3"
          >
            <PropertyField
              id="mergeCdcOperationColumn"
              name="operationColumn"
              label="Operation Column"
              required="Required"
              tooltip="Column on the incoming CDC DataFrame containing insert, update, or delete operation codes."
              value={operationColumn}
              onChange={handleTextFieldChange}
              placeholder="op"
              mono={true}
              invalid={isOperationColumnInvalid}
              error={isOperationColumnInvalid ? 'Operation column is required.' : null}
            />

            <PropertyField
              id="mergeCdcDeleteValues"
              name="deleteValues"
              label="Delete Marker Values"
              required="Required"
              tooltip="Comma-separated operation codes that identify delete tombstones (for example D, DELETE)."
              value={deleteValues}
              onChange={handleTextFieldChange}
              placeholder="D, DELETE"
              mono={true}
              invalid={isDeleteValuesInvalid}
              error={isDeleteValuesInvalid ? 'Delete marker values are required.' : null}
            />
          </div>
        )}
      </div>

      {/* 3. Tracked Columns & Action Column */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <div className="flex flex-col gap-2" data-testid="merge-cdc-tracked-section">
          <label
            htmlFor="mergeCdcTrackedAddInput"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Tracked Value Columns (Optional)</span>
            <InfoTooltip text="Columns hashed with null-safe F.xxhash64(*tracked_cols) to detect attribute changes and skip no-op updates. Leave empty to hash all shared non-key business columns." />
          </label>

          {trackedColumns.length > 0 && (
            <div className="flex flex-col gap-2" data-testid="merge-cdc-tracked-list">
              {trackedColumns.map((col, index) => (
                <div key={`${index}-${col}`} className="flex items-center gap-2">
                  <input
                    type="text"
                    data-index={index}
                    aria-label={`Tracked column ${index + 1}`}
                    value={col}
                    onChange={handleTrackedColumnEdit}
                    className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                  />
                  <button
                    type="button"
                    data-index={index}
                    aria-label={`Remove tracked column ${col}`}
                    onClick={handleRemoveTrackedColumn}
                    className="p-2 rounded border border-[var(--vscode-border)] text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              ))}
            </div>
          )}

          <div className="flex items-center gap-2">
            <input
              id="mergeCdcTrackedAddInput"
              type="text"
              value={pendingTrackedCol}
              onChange={(e) => setPendingTrackedCol(e.target.value)}
              onKeyDown={handlePendingTrackedKeyDown}
              placeholder="Leave empty for all non-key columns, or enter tier, email"
              className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
            />
            <button
              type="button"
              onClick={appendTrackedColumns}
              className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
            >
              <Plus className="w-3.5 h-3.5" />
              <span>Add Column</span>
            </button>
          </div>
        </div>

        <PropertyField
          id="mergeCdcActionColumn"
          name="actionColumn"
          label="Staging Action Column"
          required="Required"
          tooltip="Staging column emitted on output rows ('INSERT', 'UPDATE', 'DELETE', or 'EXPIRE') and excluded from final sink table inserts."
          value={actionColumn}
          onChange={handleTextFieldChange}
          placeholder="_scd_action"
          mono={true}
          invalid={isActionColumnInvalid}
          error={isActionColumnInvalid ? 'Staging action column is required.' : null}
        />
      </div>

      {/* 4. Conditional SCD Type 2 Historical Versioning Fields */}
      {isScd2 && (
        <div
          data-testid="merge-cdc-scd2-section"
          className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)] flex items-center gap-1.5">
            <span>SCD Type 2 Versioning Columns</span>
            <InfoTooltip text="Configure target dimension columns tracking row validity intervals and active version flags." />
          </span>

          <div className="grid grid-cols-2 gap-3">
            <PropertyField
              id="mergeCdcEffectiveFrom"
              name="effectiveFromColumn"
              label="Effective From Column"
              required="Required"
              tooltip="Target column storing the version start timestamp."
              value={effectiveFromColumn}
              onChange={handleTextFieldChange}
              placeholder="effective_from"
              mono={true}
              invalid={isEffectiveFromInvalid}
              error={isEffectiveFromInvalid ? 'Effective From column is required.' : null}
            />

            <PropertyField
              id="mergeCdcEffectiveTo"
              name="effectiveToColumn"
              label="Effective To Column"
              required="Required"
              tooltip="Target column storing the version expiration timestamp or open-end sentinel."
              value={effectiveToColumn}
              onChange={handleTextFieldChange}
              placeholder="effective_to"
              mono={true}
              invalid={isEffectiveToInvalid}
              error={isEffectiveToInvalid ? 'Effective To column is required.' : null}
            />
          </div>

          <div className="grid grid-cols-2 gap-3">
            <PropertyField
              id="mergeCdcIsCurrent"
              name="isCurrentColumn"
              label="Is Current Column"
              required="Required"
              tooltip="Boolean target column marking the currently active row version (true for active, false for historical)."
              value={isCurrentColumn}
              onChange={handleTextFieldChange}
              placeholder="is_current"
              mono={true}
              invalid={isIsCurrentInvalid}
              error={isIsCurrentInvalid ? 'Is Current column is required.' : null}
            />

            <SelectField
              id="mergeCdcOpenEndValue"
              name="openEndValue"
              label="Open-End Active Value"
              tooltip="Value assigned to Effective To on active rows ('null' for SQL NULL or '9999-12-31' for high-date sentinel)."
              value={openEndValue}
              options={OPEN_END_VALUE_OPTIONS}
              onChange={handleOpenEndValueChange}
            />
          </div>
        </div>
      )}

      <SystemInstructionsField
        id="mergeCdcSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this MergeCdcScd step."
        placeholder="Enter custom instructions for MergeCdcScd code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
