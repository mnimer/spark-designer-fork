/**
 * @fileoverview Property configuration panel for the Apache Spark Deduplicate step (Deduplicate).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import { CopyCheck, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported Spark DataFrame deduplication strategies.
 */
export type DeduplicateStrategy =
  | 'distinct_all'
  | 'drop_duplicates'
  | 'within_watermark'
  | 'keep_latest';

/**
 * Sort direction for deterministic `keep_latest` window deduplication.
 */
export type DeduplicateOrderDirection = 'desc' | 'asc';

/**
 * Persisted properties for the Spark `Deduplicate` step node.
 */
export interface DeduplicateStepProperties {
  strategy?: DeduplicateStrategy;
  subsetColumns?: string[];
  orderByColumn?: string;
  orderDirection?: DeduplicateOrderDirection;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link DeduplicateProperties}.
 */
export interface DeduplicateNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: DeduplicateStepProperties;
  };
}

/**
 * Props for the {@link DeduplicateProperties} component.
 */
export interface DeduplicatePropertiesProps {
  node: DeduplicateNode;
  onChange: (properties: DeduplicateStepProperties) => void;
}

const DEDUPLICATE_STRATEGY_OPTIONS: SelectFieldOption[] = [
  { value: 'distinct_all', label: 'Distinct All Columns (df.distinct())' },
  { value: 'drop_duplicates', label: 'Drop Duplicates by Key Subset (df.dropDuplicates)' },
  {
    value: 'within_watermark',
    label: 'Streaming Bounded Watermark (df.dropDuplicatesWithinWatermark)',
  },
  {
    value: 'keep_latest',
    label: 'Deterministic Latest Row (Batch Window row_number == 1)',
  },
];

const ORDER_DIRECTION_OPTIONS: SelectFieldOption[] = [
  { value: 'desc', label: 'DESC (Keep newest / largest value)' },
  { value: 'asc', label: 'ASC (Keep oldest / smallest value)' },
];

const VALID_STRATEGIES = new Set<string>(
  DEDUPLICATE_STRATEGY_OPTIONS.map((opt) => opt.value)
);

/**
 * Normalizes a raw `strategy` value into a valid {@link DeduplicateStrategy}.
 *
 * @param raw Raw `strategy` property value.
 * @returns Validated deduplication strategy defaulting to `'drop_duplicates'`.
 */
export function normalizeDeduplicateStrategy(raw: unknown): DeduplicateStrategy {
  if (typeof raw === 'string' && VALID_STRATEGIES.has(raw)) {
    return raw as DeduplicateStrategy;
  }
  return 'drop_duplicates';
}

/**
 * Normalizes raw `subsetColumns` configuration into a clean `string[]`.
 *
 * @param raw Raw `subsetColumns` property value from YAML or UI state.
 * @returns Trimmed array of key column names.
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
 * Renders the property configuration panel for the Apache Spark Deduplicate step.
 *
 * @param props Selected Deduplicate node and property update callback.
 * @returns The rendered Deduplicate configuration form.
 */
export const DeduplicateProperties: React.FC<DeduplicatePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: DeduplicateStepProperties = node?.data?.properties || {};
  const strategy = normalizeDeduplicateStrategy(properties.strategy);
  const subsetColumns = normalizeSubsetColumns(properties.subsetColumns);
  const orderByColumn =
    typeof properties.orderByColumn === 'string' ? properties.orderByColumn : '';
  const orderDirection: DeduplicateOrderDirection =
    properties.orderDirection === 'asc' ? 'asc' : 'desc';
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingSubsetCol, setPendingSubsetCol] = useState<string>('');

  const usesSubset = strategy !== 'distinct_all';
  const isKeepLatest = strategy === 'keep_latest';
  const isOrderByInvalid = isKeepLatest && orderByColumn.trim().length === 0;
  const isSubsetMissingForKeepLatest = isKeepLatest && subsetColumns.length === 0;

  const emitPatch = (patch: Partial<DeduplicateStepProperties>) => {
    onChange({
      ...properties,
      strategy,
      subsetColumns,
      orderByColumn,
      orderDirection,
      ...patch,
    });
  };

  const handleStrategyChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitPatch({ strategy: normalizeDeduplicateStrategy(e.target.value) });
  };

  const appendSubsetColumns = () => {
    const parsed = normalizeSubsetColumns(pendingSubsetCol);
    if (parsed.length === 0) {
      return;
    }
    const seen = new Set(subsetColumns);
    const merged = [...subsetColumns];
    for (const col of parsed) {
      if (!seen.has(col)) {
        seen.add(col);
        merged.push(col);
      }
    }
    setPendingSubsetCol('');
    emitPatch({ subsetColumns: merged });
  };

  const handlePendingSubsetKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendSubsetColumns();
    }
  };

  const handleSubsetColumnEdit = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...subsetColumns];
    updated[idx] = e.target.value;
    emitPatch({ subsetColumns: normalizeSubsetColumns(updated) });
  };

  const handleRemoveSubsetColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    emitPatch({
      subsetColumns: subsetColumns.filter((_, i) => i !== idx),
    });
  };

  const handleOrderByColumnChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    emitPatch({ orderByColumn: e.target.value });
  };

  const handleOrderDirectionChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitPatch({ orderDirection: e.target.value === 'asc' ? 'asc' : 'desc' });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Streaming watermark guidance banner */}
      <div
        data-testid="deduplicate-watermark-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <CopyCheck className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Streaming State & Watermarks</span>
          <InfoTooltip text="On Structured Streaming pipelines, place an upstream Watermark step and select within_watermark so Spark evicts expired deduplication keys from the state store." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          If deduplicating a streaming DataFrame, place an upstream{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">Watermark</code> step and
          use{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">
            dropDuplicatesWithinWatermark
          </code>{' '}
          to bound executor state memory.
        </p>
      </div>

      {/* 1. Deduplication Strategy */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <SelectField
          id="deduplicateStrategySelect"
          name="strategy"
          label="Deduplication Strategy"
          tooltip="Choose df.distinct() for full-row dedup, df.dropDuplicates(subset) for key dedup, dropDuplicatesWithinWatermark for bounded streaming dedup, or keep_latest for deterministic batch CDC dedup."
          value={strategy}
          options={DEDUPLICATE_STRATEGY_OPTIONS}
          onChange={handleStrategyChange}
        />
      </div>

      {/* 2. Key Subset Columns */}
      {usesSubset && (
        <div
          data-testid="deduplicate-subset-section"
          className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <label
            htmlFor="deduplicateSubsetAddInput"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Key Subset Columns</span>
            {isKeepLatest && <span className="text-red-400 font-bold">*</span>}
            <InfoTooltip text="Columns identifying duplicate records. Leave empty in drop_duplicates or within_watermark to inspect all columns." />
          </label>

          {subsetColumns.length > 0 && (
            <div className="flex flex-col gap-2" data-testid="deduplicate-subset-list">
              {subsetColumns.map((col, index) => (
                <div key={`${index}-${col}`} className="flex items-center gap-2">
                  <input
                    type="text"
                    data-index={index}
                    aria-label={`Subset column ${index + 1}`}
                    value={col}
                    onChange={handleSubsetColumnEdit}
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
              id="deduplicateSubsetAddInput"
              type="text"
              value={pendingSubsetCol}
              onChange={(e) => setPendingSubsetCol(e.target.value)}
              onKeyDown={handlePendingSubsetKeyDown}
              placeholder="e.g. event_id or tenant_id, order_id"
              className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
            />
            <button
              type="button"
              onClick={appendSubsetColumns}
              className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
            >
              <Plus className="w-3.5 h-3.5" />
              <span>Add</span>
            </button>
          </div>

          {isSubsetMissingForKeepLatest && (
            <span className="text-xs text-red-400" role="alert">
              At least one key subset column is required for keep_latest deduplication.
            </span>
          )}
        </div>
      )}

      {/* 3. Deterministic Ordering for keep_latest */}
      {isKeepLatest && (
        <div
          data-testid="deduplicate-keep-latest-section"
          className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <PropertyField
            id="deduplicateOrderByColumn"
            name="orderByColumn"
            label="Order By Column"
            required="Required"
            tooltip="Timestamp or sequence column passed to Window.partitionBy(*subset).orderBy(...) to deterministically pick row_number() == 1."
            value={orderByColumn}
            onChange={handleOrderByColumnChange}
            placeholder="updated_at"
            invalid={isOrderByInvalid}
            error={isOrderByInvalid ? 'Order by column is required for keep_latest.' : null}
          />

          <SelectField
            id="deduplicateOrderDirection"
            name="orderDirection"
            label="Order Direction"
            tooltip="Select DESC to retain the newest or highest sequence record, or ASC to retain the earliest record."
            value={orderDirection}
            options={ORDER_DIRECTION_OPTIONS}
            onChange={handleOrderDirectionChange}
          />
        </div>
      )}

      <SystemInstructionsField
        id="deduplicateSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this Deduplicate step."
        placeholder="Enter custom instructions for Deduplicate code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
