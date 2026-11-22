/**
 * @fileoverview Property configuration panel for the Apache Spark TopN step (TopN).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import { Award, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported Catalyst Window ranking functions for `TopN`.
 */
export type TopNRankingFunction = 'row_number' | 'rank' | 'dense_rank';

/**
 * Sort direction for an `orderByColumns` entry in `TopN`.
 */
export type TopNSortDirection = 'desc' | 'asc';

/**
 * Single sort expression entry stored in `properties.orderByColumns`.
 */
export interface TopNOrderBySpec {
  column: string;
  direction: TopNSortDirection;
}

/**
 * Persisted properties for the Spark `TopN` step node.
 */
export interface TopNStepProperties {
  n?: number;
  rankingFunction?: TopNRankingFunction;
  partitionByColumns?: string[];
  orderByColumns?: TopNOrderBySpec[];
  keepRankColumn?: boolean;
  rankColumnAlias?: string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link TopNProperties}.
 */
export interface TopNNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: TopNStepProperties;
  };
}

/**
 * Props for the {@link TopNProperties} component.
 */
export interface TopNPropertiesProps {
  node: TopNNode;
  onChange: (properties: TopNStepProperties) => void;
}

const RANKING_FUNCTION_OPTIONS: SelectFieldOption[] = [
  { value: 'row_number', label: 'row_number (Exact N rows, deterministic tie-break)' },
  { value: 'rank', label: 'rank (Include ties at N with rank gaps)' },
  { value: 'dense_rank', label: 'dense_rank (Top N distinct values without gaps)' },
];

const DIRECTION_OPTIONS: SelectFieldOption[] = [
  { value: 'desc', label: 'DESC (Largest / Newest first)' },
  { value: 'asc', label: 'ASC (Smallest / Oldest first)' },
];

const VALID_RANKING_FUNCTIONS = new Set<string>(
  RANKING_FUNCTION_OPTIONS.map((opt) => opt.value)
);

const DEFAULT_ORDER_BY: TopNOrderBySpec[] = [{ column: '', direction: 'desc' }];

/**
 * Normalizes raw partition columns into a trimmed `string[]`.
 *
 * @param raw Raw `partitionByColumns` value from YAML or UI state.
 * @returns Clean array of partition column names.
 */
export function normalizePartitionColumns(raw: unknown): string[] {
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
 * Normalizes raw `orderByColumns` entries into typed {@link TopNOrderBySpec} items.
 *
 * @param raw Raw `orderByColumns` value from YAML or UI state.
 * @returns Non-empty array of sort column descriptors.
 */
export function normalizeOrderByColumns(raw: unknown): TopNOrderBySpec[] {
  if (!Array.isArray(raw) || raw.length === 0) {
    return DEFAULT_ORDER_BY.map((item) => ({ ...item }));
  }
  return raw.map((entry) => {
    const obj =
      typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    const direction: TopNSortDirection = obj.direction === 'asc' ? 'asc' : 'desc';
    return {
      column: typeof obj.column === 'string' ? obj.column : '',
      direction,
    };
  });
}

/**
 * Renders the property configuration panel for the Apache Spark TopN step.
 *
 * @param props Selected TopN node and property update callback.
 * @returns The rendered TopN configuration form.
 */
export const TopNProperties: React.FC<TopNPropertiesProps> = ({ node, onChange }) => {
  const properties: TopNStepProperties = node?.data?.properties || {};
  const rawN: unknown = properties.n;
  const nValue =
    typeof rawN === 'number' && Number.isFinite(rawN)
      ? rawN
      : typeof rawN === 'string' && rawN.trim() !== '' && !Number.isNaN(Number(rawN))
        ? Number(rawN)
        : 10;
  const rankingFunction: TopNRankingFunction =
    typeof properties.rankingFunction === 'string' &&
    VALID_RANKING_FUNCTIONS.has(properties.rankingFunction)
      ? (properties.rankingFunction as TopNRankingFunction)
      : 'row_number';
  const partitionByColumns = normalizePartitionColumns(properties.partitionByColumns);
  const orderByColumns = normalizeOrderByColumns(properties.orderByColumns);
  const keepRankColumn = properties.keepRankColumn === true;
  const rankColumnAlias =
    typeof properties.rankColumnAlias === 'string' ? properties.rankColumnAlias : 'rank_num';
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingPartitionCol, setPendingPartitionCol] = useState<string>('');

  const isNInvalid = !Number.isInteger(nValue) || nValue < 1;
  const isRankAliasInvalid = keepRankColumn && rankColumnAlias.trim().length === 0;

  const emitPatch = (patch: Partial<TopNStepProperties>) => {
    onChange({
      ...properties,
      n: nValue,
      rankingFunction,
      partitionByColumns,
      orderByColumns,
      keepRankColumn,
      rankColumnAlias,
      ...patch,
    });
  };

  const handleNChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const parsed = Number(e.target.value);
    emitPatch({ n: Number.isNaN(parsed) ? 0 : parsed });
  };

  const handleRankingFunctionChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitPatch({ rankingFunction: e.target.value as TopNRankingFunction });
  };

  const appendPartitionColumns = () => {
    const parsed = normalizePartitionColumns(pendingPartitionCol);
    if (parsed.length === 0) {
      return;
    }
    const seen = new Set(partitionByColumns);
    const merged = [...partitionByColumns];
    for (const col of parsed) {
      if (!seen.has(col)) {
        seen.add(col);
        merged.push(col);
      }
    }
    setPendingPartitionCol('');
    emitPatch({ partitionByColumns: merged });
  };

  const handlePendingPartitionKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendPartitionColumns();
    }
  };

  const handlePartitionColumnEdit = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...partitionByColumns];
    updated[idx] = e.target.value;
    emitPatch({ partitionByColumns: normalizePartitionColumns(updated) });
  };

  const handleRemovePartitionColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    emitPatch({
      partitionByColumns: partitionByColumns.filter((_, i) => i !== idx),
    });
  };

  const handleAddOrderBy = () => {
    emitPatch({
      orderByColumns: [...orderByColumns, { column: '', direction: 'desc' }],
    });
  };

  const handleRemoveOrderBy = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx) || orderByColumns.length <= 1) {
      return;
    }
    emitPatch({
      orderByColumns: orderByColumns.filter((_, i) => i !== idx),
    });
  };

  const handleOrderByFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>
  ) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof TopNOrderBySpec | undefined;
    if (Number.isNaN(idx) || !field || !orderByColumns[idx]) {
      return;
    }
    const nextOrder = orderByColumns.map((item, i) =>
      i === idx ? { ...item, [field]: e.target.value } : item
    );
    emitPatch({ orderByColumns: nextOrder });
  };

  const handleKeepRankChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    emitPatch({ keepRankColumn: e.target.checked });
  };

  const handleRankAliasChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    emitPatch({ rankColumnAlias: e.target.value });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Limit N & Ranking Function */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <Award className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Top-N Limit & Tie Handling</span>
          <InfoTooltip text="When Partition By Columns is empty and row_number is selected without keeping the rank column, Spark executes a global df.orderBy(...).limit(n) heap without a full window shuffle." />
        </div>

        <PropertyField
          id="topNLimitInput"
          name="n"
          type="number"
          min={1}
          step={1}
          label="Top N Rows"
          required="Required"
          tooltip="Positive integer number of top rows to keep globally or per partition group."
          value={nValue}
          onChange={handleNChange}
          invalid={isNInvalid}
          error={isNInvalid ? 'Top N must be an integer greater than or equal to 1.' : null}
        />

        <SelectField
          id="topNRankingFunction"
          name="rankingFunction"
          label="Ranking Function"
          tooltip="Catalyst Window ranking function applied when partitioning by group or retaining ties."
          value={rankingFunction}
          options={RANKING_FUNCTION_OPTIONS}
          onChange={handleRankingFunctionChange}
        />
      </div>

      {/* 2. Partition By Columns (Optional) */}
      <div className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <label
          htmlFor="topNPartitionAddInput"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <span>Partition By Columns (Optional)</span>
          <InfoTooltip text="Leave empty for global Top-N across the entire DataFrame, or add columns for per-group Window.partitionBy(...) Top-N." />
        </label>

        {partitionByColumns.length > 0 && (
          <div className="flex flex-col gap-2" data-testid="topn-partition-columns-list">
            {partitionByColumns.map((col, index) => (
              <div key={`${index}-${col}`} className="flex items-center gap-2">
                <input
                  type="text"
                  data-index={index}
                  aria-label={`Partition column ${index + 1}`}
                  value={col}
                  onChange={handlePartitionColumnEdit}
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <button
                  type="button"
                  data-index={index}
                  aria-label={`Remove partition column ${col}`}
                  onClick={handleRemovePartitionColumn}
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
            id="topNPartitionAddInput"
            type="text"
            value={pendingPartitionCol}
            onChange={(e) => setPendingPartitionCol(e.target.value)}
            onKeyDown={handlePendingPartitionKeyDown}
            placeholder="e.g. region, category (empty for global Top-N)"
            className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
          />
          <button
            type="button"
            onClick={appendPartitionColumns}
            className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add</span>
          </button>
        </div>
      </div>

      {/* 3. Order By Columns */}
      <div
        data-testid="topn-orderby-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <span>Order By Columns</span>
            <InfoTooltip text="Ordered sort columns determining which rows rank in the top N." />
          </div>
          <button
            type="button"
            onClick={handleAddOrderBy}
            className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add Sort</span>
          </button>
        </div>

        <div className="flex flex-col gap-2">
          {orderByColumns.map((spec, index) => (
            <div
              key={index}
              data-testid={`topn-orderby-row-${index}`}
              className="grid grid-cols-[1fr_auto_auto] items-end gap-2 p-2 rounded border border-[var(--vscode-border-light)] bg-[var(--vscode-card-bg)]"
            >
              <div className="flex flex-col gap-1">
                <label
                  htmlFor={`topNOrderCol-${index}`}
                  className="text-[11px] font-bold uppercase text-[var(--vscode-text-muted)]"
                >
                  Column
                </label>
                <input
                  id={`topNOrderCol-${index}`}
                  type="text"
                  data-index={index}
                  data-field="column"
                  aria-label={`Order by ${index + 1} column`}
                  value={spec.column}
                  onChange={handleOrderByFieldChange}
                  placeholder="revenue"
                  className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
              </div>

              <div className="flex flex-col gap-1">
                <label
                  htmlFor={`topNOrderDir-${index}`}
                  className="text-[11px] font-bold uppercase text-[var(--vscode-text-muted)]"
                >
                  Direction
                </label>
                <select
                  id={`topNOrderDir-${index}`}
                  data-index={index}
                  data-field="direction"
                  aria-label={`Order by ${index + 1} direction`}
                  value={spec.direction}
                  onChange={handleOrderByFieldChange}
                  className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                >
                  {DIRECTION_OPTIONS.map((opt) => (
                    <option key={opt.value} value={opt.value}>
                      {opt.value.toUpperCase()}
                    </option>
                  ))}
                </select>
              </div>

              <button
                type="button"
                data-index={index}
                disabled={orderByColumns.length <= 1}
                aria-label={`Remove order by ${index + 1}`}
                onClick={handleRemoveOrderBy}
                className="p-2 rounded border border-[var(--vscode-border)] text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] disabled:opacity-40 disabled:cursor-not-allowed transition-colors cursor-pointer"
              >
                <Trash2 className="w-4 h-4" />
              </button>
            </div>
          ))}
        </div>
      </div>

      {/* 4. Rank Output Column Options */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <label
          htmlFor="topNKeepRankColumn"
          className="flex items-start gap-2 text-xs text-[var(--vscode-text-primary)] cursor-pointer select-none"
        >
          <input
            id="topNKeepRankColumn"
            name="keepRankColumn"
            type="checkbox"
            checked={keepRankColumn}
            onChange={handleKeepRankChange}
            className="mt-0.5 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] cursor-pointer"
          />
          <span className="flex items-center flex-wrap gap-1">
            <span className="font-bold">Keep Rank Column in Output Schema</span>
            <InfoTooltip text="Drops the helper rank column after filtering by default. Enable this option to retain the computed rank number column downstream." />
          </span>
        </label>

        {keepRankColumn && (
          <PropertyField
            id="topNRankColumnAlias"
            name="rankColumnAlias"
            label="Rank Column Alias"
            required="Required"
            tooltip="Output column name assigned to the computed Window rank value."
            value={rankColumnAlias}
            onChange={handleRankAliasChange}
            placeholder="rank_num"
            invalid={isRankAliasInvalid}
            error={isRankAliasInvalid ? 'Rank column alias is required.' : null}
          />
        )}
      </div>

      <SystemInstructionsField
        id="topNSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this TopN step."
        placeholder="Enter custom instructions for TopN code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
