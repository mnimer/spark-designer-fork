/**
 * @fileoverview Property configuration panel for the Apache Spark HandleNulls step (DataFrameNaFunctions).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import { Eraser, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported PySpark `df.na` null-handling strategies.
 */
export type HandleNullsStrategy = 'fill' | 'drop' | 'replace';

/**
 * Drop threshold mode passed to `df.na.drop(how=...)`.
 */
export type HandleNullsDropHow = 'any' | 'all';

/**
 * Per-column fill value entry stored in `properties.fillMappings`.
 */
export interface HandleNullsFillMapping {
  column: string;
  value: string;
}

/**
 * Sentinel replacement rule stored in `properties.replaceMappings`.
 */
export interface HandleNullsReplaceMapping {
  fromValue: string;
  toValue: string;
}

/**
 * Persisted properties for the Spark `HandleNulls` step node.
 */
export interface HandleNullsStepProperties {
  strategy?: HandleNullsStrategy;
  subsetColumns?: string[];
  dropHow?: HandleNullsDropHow;
  dropThresh?: number | string;
  defaultFillValue?: string;
  fillMappings?: HandleNullsFillMapping[];
  replaceMappings?: HandleNullsReplaceMapping[];
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link HandleNullsProperties}.
 */
export interface HandleNullsNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: HandleNullsStepProperties;
  };
}

/**
 * Props for the {@link HandleNullsProperties} component.
 */
export interface HandleNullsPropertiesProps {
  node: HandleNullsNode;
  onChange: (properties: HandleNullsStepProperties) => void;
}

const STRATEGY_OPTIONS: SelectFieldOption[] = [
  { value: 'fill', label: 'Fill Nulls (df.na.fill)' },
  { value: 'drop', label: 'Drop Rows with Nulls (df.na.drop)' },
  { value: 'replace', label: 'Replace Sentinel Values (df.na.replace)' },
];

const DROP_HOW_OPTIONS: SelectFieldOption[] = [
  { value: 'any', label: 'any (drop row if any column is null)' },
  { value: 'all', label: 'all (drop row only if all columns are null)' },
];

/**
 * Normalizes raw subset column configuration into a trimmed `string[]`.
 *
 * @param raw Raw `subsetColumns` property value from YAML or UI state.
 * @returns Trimmed array of non-empty column names.
 */
export function normalizeSubsetColumns(raw: unknown): string[] {
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
 * Normalizes raw per-column fill mappings into typed {@link HandleNullsFillMapping} entries.
 *
 * @param raw Raw `fillMappings` property value from YAML or UI state.
 * @returns Validated array of per-column fill mappings.
 */
export function normalizeFillMappings(raw: unknown): HandleNullsFillMapping[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw.map((entry) => {
    const obj = typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    return {
      column: typeof obj.column === 'string' ? obj.column : '',
      value: obj.value !== undefined && obj.value !== null ? String(obj.value) : '',
    };
  });
}

/**
 * Normalizes raw sentinel replacement rules into typed {@link HandleNullsReplaceMapping} entries.
 *
 * @param raw Raw `replaceMappings` property value from YAML or UI state.
 * @returns Validated array of sentinel value replacement pairs.
 */
export function normalizeReplaceMappings(raw: unknown): HandleNullsReplaceMapping[] {
  if (!Array.isArray(raw)) {
    return [{ fromValue: '', toValue: '' }];
  }
  return raw.map((entry) => {
    const obj = typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    return {
      fromValue: obj.fromValue !== undefined && obj.fromValue !== null ? String(obj.fromValue) : '',
      toValue: obj.toValue !== undefined && obj.toValue !== null ? String(obj.toValue) : '',
    };
  });
}

/**
 * Validates the optional `dropThresh` value for `df.na.drop(thresh=...)`.
 *
 * @param raw Raw `dropThresh` property value.
 * @returns Validation error message when invalid, or null when valid or empty.
 */
export function validateDropThresh(raw: unknown): string | null {
  if (raw === undefined || raw === null || String(raw).trim() === '') {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 1) {
    return 'Minimum non-null threshold must be a positive integer.';
  }
  return null;
}

/**
 * Renders the property configuration panel for the Apache Spark HandleNulls step.
 *
 * @param props Selected HandleNulls node and property update callback.
 * @returns The rendered HandleNulls configuration form.
 */
export const HandleNullsProperties: React.FC<HandleNullsPropertiesProps> = ({ node, onChange }) => {
  const properties: HandleNullsStepProperties = node?.data?.properties || {};
  const strategy: HandleNullsStrategy =
    properties.strategy === 'drop' || properties.strategy === 'replace'
      ? properties.strategy
      : 'fill';
  const subsetColumns = normalizeSubsetColumns(properties.subsetColumns);
  const dropHow: HandleNullsDropHow = properties.dropHow === 'all' ? 'all' : 'any';
  const dropThreshRaw =
    properties.dropThresh !== undefined && properties.dropThresh !== null
      ? String(properties.dropThresh)
      : '';
  const defaultFillValue =
    typeof properties.defaultFillValue === 'string' ? properties.defaultFillValue : '';
  const fillMappings = normalizeFillMappings(properties.fillMappings);
  const replaceMappings = normalizeReplaceMappings(properties.replaceMappings);
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingSubsetInput, setPendingSubsetInput] = useState<string>('');

  const dropThreshError = strategy === 'drop' ? validateDropThresh(dropThreshRaw) : null;

  const handleStrategyChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextStrategy = e.target.value as HandleNullsStrategy;
    const nextPatch: HandleNullsStepProperties = {
      ...properties,
      strategy: nextStrategy,
      subsetColumns,
    };
    if (nextStrategy === 'drop') {
      nextPatch.dropHow = dropHow;
    } else if (nextStrategy === 'replace' && !Array.isArray(properties.replaceMappings)) {
      nextPatch.replaceMappings = [{ fromValue: '', toValue: '' }];
    }
    onChange(nextPatch);
  };

  const commitSubsetColumns = (nextColumns: string[]) => {
    onChange({
      ...properties,
      strategy,
      subsetColumns: nextColumns,
    });
  };

  const appendPendingSubsetColumns = () => {
    const parsed = normalizeSubsetColumns(pendingSubsetInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(subsetColumns);
    const merged = [...subsetColumns];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    setPendingSubsetInput('');
    commitSubsetColumns(merged);
  };

  const handlePendingSubsetKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendPendingSubsetColumns();
    }
  };

  const handleExistingSubsetChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...subsetColumns];
    updated[idx] = e.target.value;
    commitSubsetColumns(normalizeSubsetColumns(updated));
  };

  const handleRemoveSubsetColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    commitSubsetColumns(subsetColumns.filter((_, i) => i !== idx));
  };

  const handleDropHowChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    onChange({
      ...properties,
      strategy,
      subsetColumns,
      dropHow: e.target.value as HandleNullsDropHow,
    });
  };

  const handleDropThreshChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const raw = e.target.value;
    const trimmed = raw.trim();
    const numeric = Number(trimmed);
    const nextThresh =
      trimmed === ''
        ? undefined
        : Number.isInteger(numeric) && numeric >= 1
        ? numeric
        : raw;
    onChange({
      ...properties,
      strategy,
      subsetColumns,
      dropHow,
      dropThresh: nextThresh,
    });
  };

  const handleDefaultFillChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    onChange({
      ...properties,
      strategy,
      subsetColumns,
      defaultFillValue: e.target.value,
      fillMappings,
    });
  };

  const handleAddFillMapping = () => {
    onChange({
      ...properties,
      strategy,
      subsetColumns,
      defaultFillValue,
      fillMappings: [...fillMappings, { column: '', value: '' }],
    });
  };

  const handleRemoveFillMapping = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    onChange({
      ...properties,
      strategy,
      subsetColumns,
      defaultFillValue,
      fillMappings: fillMappings.filter((_, i) => i !== idx),
    });
  };

  const handleFillMappingChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof HandleNullsFillMapping | undefined;
    if (Number.isNaN(idx) || !field || !fillMappings[idx]) {
      return;
    }
    const nextMappings = fillMappings.map((item, i) =>
      i === idx ? { ...item, [field]: e.target.value } : item
    );
    onChange({
      ...properties,
      strategy,
      subsetColumns,
      defaultFillValue,
      fillMappings: nextMappings,
    });
  };

  const handleAddReplaceMapping = () => {
    onChange({
      ...properties,
      strategy,
      subsetColumns,
      replaceMappings: [...replaceMappings, { fromValue: '', toValue: '' }],
    });
  };

  const handleRemoveReplaceMapping = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    onChange({
      ...properties,
      strategy,
      subsetColumns,
      replaceMappings: replaceMappings.filter((_, i) => i !== idx),
    });
  };

  const handleReplaceMappingChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof HandleNullsReplaceMapping | undefined;
    if (Number.isNaN(idx) || !field || !replaceMappings[idx]) {
      return;
    }
    const nextMappings = replaceMappings.map((item, i) =>
      i === idx ? { ...item, [field]: e.target.value } : item
    );
    onChange({
      ...properties,
      strategy,
      subsetColumns,
      replaceMappings: nextMappings,
    });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Strategy Selection */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <Eraser className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Null Handling Strategy</span>
          <InfoTooltip text="Select how Spark DataFrameNaFunctions handles SQL NULL, NaN, or sentinel values." />
        </div>

        <SelectField
          id="handleNullsStrategy"
          name="strategy"
          label="Strategy"
          tooltip="Choose df.na.fill() to substitute defaults, df.na.drop() to filter null rows, or df.na.replace() to map sentinel values."
          value={strategy}
          options={STRATEGY_OPTIONS}
          onChange={handleStrategyChange}
        />
      </div>

      {/* 2. Target Subset Columns */}
      <div className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <label
          htmlFor="handleNullsSubsetAddInput"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <span>Subset Columns</span>
          <InfoTooltip text="Optional list of columns passed as subset=[...] to df.na.fill(), df.na.drop(), or df.na.replace(). Leave empty to apply across all eligible columns." />
        </label>

        {subsetColumns.length > 0 && (
          <div className="flex flex-col gap-2" data-testid="subset-columns-list">
            {subsetColumns.map((col, index) => (
              <div key={`${index}-${col}`} className="flex items-center gap-2">
                <input
                  type="text"
                  data-index={index}
                  aria-label={`Subset column ${index + 1}`}
                  value={col}
                  onChange={handleExistingSubsetChange}
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <button
                  type="button"
                  data-index={index}
                  aria-label={`Remove subset column ${col}`}
                  onClick={handleRemoveSubsetColumn}
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
            id="handleNullsSubsetAddInput"
            type="text"
            value={pendingSubsetInput}
            onChange={(e) => setPendingSubsetInput(e.target.value)}
            onKeyDown={handlePendingSubsetKeyDown}
            placeholder="e.g. amount, country (leave empty for all columns)"
            className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
          />
          <button
            type="button"
            onClick={appendPendingSubsetColumns}
            className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add</span>
          </button>
        </div>
      </div>

      {/* 3A. Fill Strategy Configuration */}
      {strategy === 'fill' && (
        <div
          data-testid="handle-nulls-fill-section"
          className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <PropertyField
            id="handleNullsDefaultFillValue"
            name="defaultFillValue"
            label="Default Fill Value"
            tooltip="Global scalar fallback value applied to null columns in subsetColumns when no per-column mapping overrides that column."
            value={defaultFillValue}
            onChange={handleDefaultFillChange}
            placeholder="e.g. UNKNOWN or 0"
            helperText="Spark coerces scalar fill values to match target column types."
          />

          <div className="flex items-center justify-between gap-2 pt-2 border-t border-[var(--vscode-border)]">
            <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
              <span>Per-Column Fill Mappings</span>
              <InfoTooltip text="Dictionary entries passed to df.na.fill({column: value}) for column-specific defaults." />
            </div>
            <button
              type="button"
              onClick={handleAddFillMapping}
              className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
            >
              <Plus className="w-3.5 h-3.5" />
              <span>Add Mapping</span>
            </button>
          </div>

          {fillMappings.length === 0 ? (
            <p className="text-xs text-[var(--vscode-text-muted)]">
              No per-column overrides configured. Click Add Mapping to set column-specific fill values.
            </p>
          ) : (
            <div className="flex flex-col gap-2">
              {fillMappings.map((mapping, index) => {
                const isColMissing = mapping.column.trim().length === 0 && mapping.value.trim().length > 0;
                return (
                  <div
                    key={index}
                    data-testid={`fill-mapping-row-${index}`}
                    className="flex flex-col gap-2 p-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)]"
                  >
                    <div className="grid grid-cols-[1fr_1fr_auto] items-end gap-2">
                      <div className="flex flex-col gap-1">
                        <label
                          htmlFor={`fillCol-${index}`}
                          className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                        >
                          <span>Column</span>
                          <InfoTooltip text="Target DataFrame column name to fill when null or NaN." />
                        </label>
                        <input
                          id={`fillCol-${index}`}
                          type="text"
                          data-index={index}
                          data-field="column"
                          aria-label={`Fill mapping ${index + 1} column`}
                          aria-invalid={isColMissing}
                          value={mapping.column}
                          onChange={handleFillMappingChange}
                          placeholder="amount"
                          className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                            isColMissing
                              ? 'border-red-500/80 focus:border-red-500'
                              : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                          }`}
                        />
                      </div>

                      <div className="flex flex-col gap-1">
                        <label
                          htmlFor={`fillVal-${index}`}
                          className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                        >
                          <span>Fill Value</span>
                          <InfoTooltip text="Replacement value substituted when the column is null or NaN." />
                        </label>
                        <input
                          id={`fillVal-${index}`}
                          type="text"
                          data-index={index}
                          data-field="value"
                          aria-label={`Fill mapping ${index + 1} value`}
                          value={mapping.value}
                          onChange={handleFillMappingChange}
                          placeholder="0.0"
                          className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                        />
                      </div>

                      <button
                        type="button"
                        data-index={index}
                        aria-label={`Remove fill mapping ${index + 1}`}
                        onClick={handleRemoveFillMapping}
                        className="p-2 rounded border border-[var(--vscode-border)] text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                    {isColMissing && (
                      <span className="text-xs text-red-400" role="alert">
                        Column name is required for this fill mapping.
                      </span>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* 3B. Drop Strategy Configuration */}
      {strategy === 'drop' && (
        <div
          data-testid="handle-nulls-drop-section"
          className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <SelectField
            id="handleNullsDropHow"
            name="dropHow"
            label="Drop Condition (how)"
            tooltip="Passed to df.na.drop(how=...) when Minimum Non-Null Count is blank."
            value={dropHow}
            options={DROP_HOW_OPTIONS}
            onChange={handleDropHowChange}
          />

          <PropertyField
            id="handleNullsDropThresh"
            name="dropThresh"
            type="number"
            min={1}
            step={1}
            label="Minimum Non-Null Count (thresh)"
            tooltip="Optional positive integer passed to df.na.drop(thresh=...). If set, Spark keeps only rows with at least this many non-null values and ignores how."
            value={dropThreshRaw}
            onChange={handleDropThreshChange}
            placeholder="Optional (overrides how when set)"
            invalid={Boolean(dropThreshError)}
            error={dropThreshError}
          />
        </div>
      )}

      {/* 3C. Replace Strategy Configuration */}
      {strategy === 'replace' && (
        <div
          data-testid="handle-nulls-replace-section"
          className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
              <span>Sentinel Replacement Rules</span>
              <InfoTooltip text="Value pairs passed to df.na.replace(to_replace, value, subset=...). Use NULL in To Value to convert sentinel tokens to SQL NULL." />
            </div>
            <button
              type="button"
              onClick={handleAddReplaceMapping}
              className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
            >
              <Plus className="w-3.5 h-3.5" />
              <span>Add Replacement</span>
            </button>
          </div>

          <div className="flex flex-col gap-2">
            {replaceMappings.map((mapping, index) => {
              const isFromMissing =
                mapping.fromValue.trim().length === 0 && mapping.toValue.trim().length > 0;
              return (
                <div
                  key={index}
                  data-testid={`replace-mapping-row-${index}`}
                  className="flex flex-col gap-2 p-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)]"
                >
                  <div className="grid grid-cols-[1fr_1fr_auto] items-end gap-2">
                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`replaceFrom-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>From Value</span>
                        <InfoTooltip text="Existing sentinel value matched in DataFrame cells, such as N/A, -1, or UNKNOWN." />
                      </label>
                      <input
                        id={`replaceFrom-${index}`}
                        type="text"
                        data-index={index}
                        data-field="fromValue"
                        aria-label={`Replace mapping ${index + 1} from value`}
                        aria-invalid={isFromMissing}
                        value={mapping.fromValue}
                        onChange={handleReplaceMappingChange}
                        placeholder="N/A"
                        className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                          isFromMissing
                            ? 'border-red-500/80 focus:border-red-500'
                            : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                        }`}
                      />
                    </div>

                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`replaceTo-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>To Value</span>
                        <InfoTooltip text="Replacement value written when From Value matches. Enter NULL to replace the sentinel with SQL NULL." />
                      </label>
                      <input
                        id={`replaceTo-${index}`}
                        type="text"
                        data-index={index}
                        data-field="toValue"
                        aria-label={`Replace mapping ${index + 1} to value`}
                        value={mapping.toValue}
                        onChange={handleReplaceMappingChange}
                        placeholder="NULL or clean_value"
                        className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                      />
                    </div>

                    {replaceMappings.length > 1 && (
                      <button
                        type="button"
                        data-index={index}
                        aria-label={`Remove replace mapping ${index + 1}`}
                        onClick={handleRemoveReplaceMapping}
                        className="p-2 rounded border border-[var(--vscode-border)] text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    )}
                  </div>
                  {isFromMissing && (
                    <span className="text-xs text-red-400" role="alert">
                      From value is required for this replacement rule.
                    </span>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      <SystemInstructionsField
        id="handleNullsSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this HandleNulls step."
        placeholder="Enter custom instructions for HandleNulls code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
