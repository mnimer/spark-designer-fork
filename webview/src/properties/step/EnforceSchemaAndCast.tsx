/**
 * @fileoverview Property configuration panel for the Apache Spark EnforceSchemaAndCast step.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useState } from 'react';
import { AlertTriangle, FileCheck2, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Schema drift policy applied when comparing upstream columns against the target DDL contract.
 */
export type SchemaDriftPolicy = 'drop_extra' | 'add_nulls' | 'fail';

/**
 * Type conversion strategy applied under Spark 4.0 ANSI compliance.
 */
export type SchemaCastMode = 'try_cast_dlq' | 'try_cast_null' | 'ansi_strict';

/**
 * Persisted properties for the Spark `EnforceSchemaAndCast` step node.
 */
export interface EnforceSchemaAndCastStepProperties {
  schemaDDL?: string;
  driftPolicy?: SchemaDriftPolicy;
  allowMissingNullable?: boolean;
  castMode?: SchemaCastMode;
  nonNullableColumns?: string[];
  dateFormat?: string;
  timestampFormat?: string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link EnforceSchemaAndCastProperties}.
 */
export interface EnforceSchemaAndCastNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: EnforceSchemaAndCastStepProperties;
  };
}

/**
 * Props for the {@link EnforceSchemaAndCastProperties} component.
 */
export interface EnforceSchemaAndCastPropertiesProps {
  node: EnforceSchemaAndCastNode;
  onChange: (properties: EnforceSchemaAndCastStepProperties) => void;
}

const DRIFT_POLICY_OPTIONS: SelectFieldOption[] = [
  { value: 'drop_extra', label: 'Drop Extra Columns (Project Contract Only)' },
  { value: 'add_nulls', label: 'Add Missing as NULL & Keep Extra Columns' },
  { value: 'fail', label: 'Fail on Schema Drift (Strict Contract)' },
];

const VALID_DRIFT_POLICIES = new Set<string>(DRIFT_POLICY_OPTIONS.map((opt) => opt.value));

const CAST_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'try_cast_dlq', label: 'ANSI-Safe F.try_cast + Route Failures to DLQ' },
  { value: 'try_cast_null', label: 'ANSI-Safe F.try_cast + Coerce Invalid to NULL' },
  { value: 'ansi_strict', label: 'Spark 4.0 ANSI Strict Cast (Fail Job on Invalid)' },
];

const VALID_CAST_MODES = new Set<string>(CAST_MODE_OPTIONS.map((opt) => opt.value));

/**
 * Normalizes raw `driftPolicy` input into a valid {@link SchemaDriftPolicy}.
 *
 * @param raw Raw `driftPolicy` property value from YAML or UI state.
 * @returns Validated drift policy defaulting to `'drop_extra'`.
 */
function normalizeDriftPolicy(raw: unknown): SchemaDriftPolicy {
  if (typeof raw === 'string' && VALID_DRIFT_POLICIES.has(raw.trim())) {
    return raw.trim() as SchemaDriftPolicy;
  }
  return 'drop_extra';
}

/**
 * Normalizes raw `castMode` input into a valid {@link SchemaCastMode}.
 *
 * @param raw Raw `castMode` property value from YAML or UI state.
 * @returns Validated cast mode defaulting to `'try_cast_dlq'`.
 */
function normalizeCastMode(raw: unknown): SchemaCastMode {
  if (typeof raw === 'string' && VALID_CAST_MODES.has(raw.trim())) {
    return raw.trim() as SchemaCastMode;
  }
  return 'try_cast_dlq';
}

/**
 * Normalizes raw column lists or comma-separated strings into trimmed unique names.
 *
 * @param raw Raw array or comma-separated string of column names.
 * @returns Trimmed non-empty array of column names.
 */
function normalizeColumnList(raw: unknown): string[] {
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
 * Renders the property configuration panel for the Apache Spark EnforceSchemaAndCast step.
 *
 * @param props Selected EnforceSchemaAndCast node and property update callback.
 * @returns The rendered EnforceSchemaAndCast configuration form.
 */
export const EnforceSchemaAndCastProperties: React.FC<EnforceSchemaAndCastPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: EnforceSchemaAndCastStepProperties = node?.data?.properties || {};
  const schemaDDL = typeof properties.schemaDDL === 'string' ? properties.schemaDDL : '';
  const driftPolicy = normalizeDriftPolicy(properties.driftPolicy);
  const allowMissingNullable =
    typeof properties.allowMissingNullable === 'boolean' ? properties.allowMissingNullable : false;
  const castMode = normalizeCastMode(properties.castMode);
  const nonNullableColumns = normalizeColumnList(properties.nonNullableColumns);
  const dateFormat = typeof properties.dateFormat === 'string' ? properties.dateFormat : '';
  const timestampFormat =
    typeof properties.timestampFormat === 'string' ? properties.timestampFormat : '';
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingNonNullInput, setPendingNonNullInput] = useState<string>('');

  const isSchemaEmpty = schemaDDL.trim().length === 0;

  const commitPatch = (patch: Partial<EnforceSchemaAndCastStepProperties>) => {
    onChange({
      ...properties,
      schemaDDL,
      driftPolicy,
      allowMissingNullable,
      castMode,
      nonNullableColumns,
      dateFormat,
      timestampFormat,
      ...patch,
    });
  };

  const handleTextChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    commitPatch({ [name]: value });
  };

  const handleDriftPolicyChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    commitPatch({ driftPolicy: normalizeDriftPolicy(e.target.value) });
  };

  const handleCastModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    commitPatch({ castMode: normalizeCastMode(e.target.value) });
  };

  const handleAllowMissingToggle = (e: React.ChangeEvent<HTMLInputElement>) => {
    commitPatch({ allowMissingNullable: e.target.checked });
  };

  const appendNonNullableColumns = () => {
    const parsed = normalizeColumnList(pendingNonNullInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(nonNullableColumns);
    const merged = [...nonNullableColumns];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    setPendingNonNullInput('');
    commitPatch({ nonNullableColumns: merged });
  };

  const handlePendingNonNullKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendNonNullableColumns();
    }
  };

  const handleExistingNonNullChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...nonNullableColumns];
    updated[idx] = e.target.value;
    commitPatch({ nonNullableColumns: normalizeColumnList(updated) });
  };

  const handleRemoveNonNullColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    commitPatch({
      nonNullableColumns: nonNullableColumns.filter((_, i) => i !== idx),
    });
  };

  const handleSystemInstructionsChange = (value: string) => {
    commitPatch({ system_instructions: value });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Data Contract Guidance Banner */}
      <div
        data-testid="enforce-contract-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <FileCheck2 className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Data Contract &amp; Schema Enforcement</span>
          <InfoTooltip text="Parses target Spark SQL DDL via StructType.fromDDL(), enforces column drift policies, and converts types safely under Spark 4.0 ANSI mode." />
        </div>
        <p className="text-xs text-[var(--vscode-text-muted)] leading-relaxed">
          Enforce an explicit <code className="font-mono text-[var(--vscode-text-primary)]">StructType.fromDDL()</code>{' '}
          contract in a single vectorized Catalyst pass. Because Spark 4.0 enables ANSI mode by
          default, use <code className="font-mono text-[var(--vscode-text-primary)]">F.try_cast()</code> to prevent
          malformed strings from aborting tasks with <code className="font-mono text-[var(--vscode-text-primary)]">CAST_INVALID_INPUT</code>.
        </p>
      </div>

      {/* 2. Target Schema DDL & Drift Policy */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <PropertyField
          id="enforceSchemaDdl"
          name="schemaDDL"
          label="Target Schema DDL"
          type="textarea"
          rows={4}
          mono={true}
          required="Required"
          tooltip="Target Spark SQL DDL string parsed via StructType.fromDDL(), including optional NOT NULL constraints."
          invalid={isSchemaEmpty}
          error={isSchemaEmpty ? 'Target Spark SQL DDL schema is required.' : null}
          value={schemaDDL}
          onChange={handleTextChange}
          placeholder="order_id BIGINT NOT NULL, customer_id STRING NOT NULL, amount DECIMAL(18, 2), event_ts TIMESTAMP"
        />

        <SelectField
          id="enforceDriftPolicy"
          name="driftPolicy"
          label="Schema Drift Policy"
          tooltip="Controls how upstream extra or missing columns are handled relative to the target DDL contract."
          value={driftPolicy}
          options={DRIFT_POLICY_OPTIONS}
          onChange={handleDriftPolicyChange}
        />

        {driftPolicy === 'fail' && (
          <label
            htmlFor="enforceAllowMissingNullable"
            className="flex items-center justify-between gap-2 pt-1 text-xs font-bold uppercase text-[var(--vscode-text-primary)] cursor-pointer"
          >
            <span className="flex items-center gap-1.5">
              <span>Allow Missing Nullable Columns</span>
              <InfoTooltip text="If enabled under Fail drift policy, fill missing nullable contract columns with typed NULLs while still failing on extra columns or missing NOT NULL columns." />
            </span>
            <input
              id="enforceAllowMissingNullable"
              name="allowMissingNullable"
              type="checkbox"
              checked={allowMissingNullable}
              onChange={handleAllowMissingToggle}
              className="accent-blue-500 w-4 h-4 cursor-pointer"
            />
          </label>
        )}
      </div>

      {/* 3. Cast Mode & Date/Timestamp Format Overrides */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <SelectField
          id="enforceCastMode"
          name="castMode"
          label="Cast Safety Mode"
          tooltip="Select F.try_cast DLQ routing, silent NULL coercion, or strict Spark 4.0 ANSI cast failure."
          value={castMode}
          options={CAST_MODE_OPTIONS}
          onChange={handleCastModeChange}
        />

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="enforceDateFormat"
            name="dateFormat"
            label="Date Format"
            tooltip="Optional Java DateTimeFormatter pattern applied when parsing string inputs to DATE columns."
            value={dateFormat}
            onChange={handleTextChange}
            placeholder="yyyy-MM-dd"
          />

          <PropertyField
            id="enforceTimestampFormat"
            name="timestampFormat"
            label="Timestamp Format"
            tooltip="Optional Java DateTimeFormatter pattern applied via F.try_to_timestamp() when parsing string inputs to TIMESTAMP columns."
            value={timestampFormat}
            onChange={handleTextChange}
            placeholder="yyyy-MM-dd HH:mm:ss"
          />
        </div>
      </div>

      {/* 4. Non-Nullable Columns List Editor */}
      <div
        data-testid="non-nullable-columns-section"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <label
          htmlFor="nonNullableColumnAddInput"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <span>Non-Nullable Columns</span>
          <InfoTooltip text="Additional contract columns that must not evaluate to NULL after casting, merged with NOT NULL columns declared in Target Schema DDL." />
        </label>

        {nonNullableColumns.length > 0 && (
          <div className="flex flex-col gap-2" data-testid="non-nullable-columns-list">
            {nonNullableColumns.map((col, index) => (
              <div key={`${index}-${col}`} className="flex items-center gap-2">
                <input
                  type="text"
                  data-index={index}
                  aria-label={`Non-nullable column ${index + 1}`}
                  value={col}
                  onChange={handleExistingNonNullChange}
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <button
                  type="button"
                  data-index={index}
                  aria-label={`Remove non-nullable column ${col}`}
                  onClick={handleRemoveNonNullColumn}
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
            id="nonNullableColumnAddInput"
            type="text"
            value={pendingNonNullInput}
            onChange={(e) => setPendingNonNullInput(e.target.value)}
            onKeyDown={handlePendingNonNullKeyDown}
            placeholder="e.g. order_id, customer_id"
            className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
          />
          <button
            type="button"
            onClick={appendNonNullableColumns}
            className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add Column</span>
          </button>
        </div>
      </div>

      {/* 5. Dead-Letter Queue Error Handle Banner */}
      <div
        data-testid="enforce-dlq-banner"
        className="flex flex-col gap-1.5 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
          <span>Dead-Letter Error Handle Routing</span>
        </div>
        <p className="text-xs text-[var(--vscode-text-muted)] leading-relaxed">
          {castMode === 'try_cast_dlq' &&
            'Connect the red error handle on the canvas to route rows that fail F.try_cast() or NOT NULL checks to a dead-letter sink with original values and a diagnostic _error column.'}
          {castMode === 'try_cast_null' &&
            'Malformed values coerce to NULL via F.try_cast(). Rows violating NOT NULL constraints route to the red error handle with _error when connected.'}
          {castMode === 'ansi_strict' &&
            'Strict ANSI mode aborts the Spark task immediately on invalid casts or NOT NULL violations instead of emitting rows to the error handle.'}
        </p>
      </div>

      <SystemInstructionsField
        id="enforceSchemaAndCastSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this EnforceSchemaAndCast step."
        placeholder="Enter custom instructions for EnforceSchemaAndCast code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
