/**
 * @fileoverview Property configuration panel for the Apache Spark OrderBy step (df.orderBy / df.sortWithinPartitions).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { ArrowDown, ArrowUp, ArrowUpDown, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Sort direction for a single `OrderBy` column rule.
 */
export type SortDirection = 'asc' | 'desc';

/**
 * Explicit null placement option for a single `OrderBy` column rule.
 */
export type SortNullsOrder = 'default' | 'nulls_first' | 'nulls_last';

/**
 * Single sort specification stored in `properties.sortColumns`.
 */
export interface OrderBySortSpec {
  column: string;
  direction: SortDirection;
  nullsOrder: SortNullsOrder;
}

/**
 * Persisted properties for the Spark `OrderBy` step node.
 */
export interface OrderByStepProperties {
  sortColumns?: OrderBySortSpec[];
  withinPartitionsOnly?: boolean;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link OrderByProperties}.
 */
export interface OrderByNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    executionMode?: 'batch' | 'streaming';
    system_instructions?: string;
    special_instructions?: string;
    properties?: OrderByStepProperties;
  };
}

/**
 * Props for the {@link OrderByProperties} component.
 */
export interface OrderByPropertiesProps {
  node: OrderByNode;
  onChange: (properties: OrderByStepProperties) => void;
}

const DIRECTION_OPTIONS: Array<{ value: SortDirection; label: string }> = [
  { value: 'asc', label: 'Ascending (asc)' },
  { value: 'desc', label: 'Descending (desc)' },
];

const NULLS_ORDER_OPTIONS: Array<{ value: SortNullsOrder; label: string }> = [
  { value: 'default', label: 'Default' },
  { value: 'nulls_first', label: 'Nulls First' },
  { value: 'nulls_last', label: 'Nulls Last' },
];

const DEFAULT_SORT_COLUMNS: OrderBySortSpec[] = [
  { column: '', direction: 'asc', nullsOrder: 'default' },
];

/**
 * Normalizes raw `sortColumns` configuration into typed {@link OrderBySortSpec} entries.
 *
 * @param raw Raw `sortColumns` value from YAML or UI state.
 * @returns Non-empty array of normalized sort specifications.
 */
export function normalizeSortColumns(raw: unknown): OrderBySortSpec[] {
  if (!Array.isArray(raw) || raw.length === 0) {
    return DEFAULT_SORT_COLUMNS.map((item) => ({ ...item }));
  }
  return raw.map((entry) => {
    const obj = typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    const direction: SortDirection = obj.direction === 'desc' ? 'desc' : 'asc';
    const nullsOrder: SortNullsOrder =
      obj.nullsOrder === 'nulls_first' || obj.nullsOrder === 'nulls_last'
        ? obj.nullsOrder
        : 'default';
    return {
      column: typeof obj.column === 'string' ? obj.column : '',
      direction,
      nullsOrder,
    };
  });
}

/**
 * Renders the property configuration panel for the Apache Spark OrderBy step.
 *
 * @param props Selected OrderBy node and property update callback.
 * @returns The rendered OrderBy configuration form.
 */
export const OrderByProperties: React.FC<OrderByPropertiesProps> = ({ node, onChange }) => {
  const properties: OrderByStepProperties = node?.data?.properties || {};
  const sortColumns = normalizeSortColumns(properties.sortColumns);
  const withinPartitionsOnly = Boolean(properties.withinPartitionsOnly);
  const systemInstructions = resolveSystemInstructions(node);

  const commitSortColumns = (nextSortColumns: OrderBySortSpec[]) => {
    onChange({
      ...properties,
      sortColumns: nextSortColumns,
      withinPartitionsOnly,
    });
  };

  const handleAddSortColumn = () => {
    commitSortColumns([
      ...sortColumns,
      { column: '', direction: 'asc', nullsOrder: 'default' },
    ]);
  };

  const handleRemoveSortColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx) || sortColumns.length <= 1) {
      return;
    }
    commitSortColumns(sortColumns.filter((_, i) => i !== idx));
  };

  const handleMoveSortColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const delta = Number(e.currentTarget.dataset.delta);
    const targetIdx = idx + delta;
    if (
      Number.isNaN(idx) ||
      Number.isNaN(delta) ||
      targetIdx < 0 ||
      targetIdx >= sortColumns.length
    ) {
      return;
    }
    const reordered = [...sortColumns];
    const [moved] = reordered.splice(idx, 1);
    reordered.splice(targetIdx, 0, moved);
    commitSortColumns(reordered);
  };

  const handleSortFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>
  ) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof OrderBySortSpec | undefined;
    if (Number.isNaN(idx) || !field || !sortColumns[idx]) {
      return;
    }
    const nextSortColumns = sortColumns.map((item, i) =>
      i === idx ? { ...item, [field]: e.target.value } : item
    );
    commitSortColumns(nextSortColumns);
  };

  const handleWithinPartitionsChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange({
      ...properties,
      sortColumns,
      withinPartitionsOnly: e.target.checked,
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
      {/* 1. Sort Scope & Streaming Guidance */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <ArrowUpDown className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Ordering Scope</span>
          <InfoTooltip text="Choose global df.orderBy() across partitions or local df.sortWithinPartitions() inside each existing partition." />
        </div>

        <label
          htmlFor="orderByWithinPartitionsOnly"
          className="flex items-start gap-2 text-xs text-[var(--vscode-text-primary)] cursor-pointer select-none"
        >
          <input
            id="orderByWithinPartitionsOnly"
            name="withinPartitionsOnly"
            type="checkbox"
            checked={withinPartitionsOnly}
            onChange={handleWithinPartitionsChange}
            className="mt-0.5 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] cursor-pointer"
          />
          <span className="flex flex-col gap-1">
            <span className="flex items-center gap-1.5 font-semibold">
              <span>Sort Within Partitions Only</span>
              <InfoTooltip text="When enabled, calls df.sortWithinPartitions(*exprs) to sort inside each partition without triggering a cluster-wide range shuffle." />
            </span>
            <span className="text-[var(--vscode-text-muted)] leading-relaxed">
              {withinPartitionsOnly
                ? 'Uses df.sortWithinPartitions() for partition-local ordering without a global shuffle.'
                : 'Uses df.orderBy() for a total global order across all partitions.'}
            </span>
          </span>
        </label>

        <p className="text-xs text-[var(--vscode-text-muted)] leading-relaxed pt-2 border-t border-[var(--vscode-border)]">
          In Structured Streaming, global <code className="font-mono text-[var(--vscode-text-primary)]">df.orderBy()</code> is
          supported only in <code className="font-mono text-[var(--vscode-text-primary)]">complete</code> output mode after an
          aggregation step.
        </p>
      </div>

      {/* 2. Ordered Sort Columns */}
      <div
        data-testid="orderby-columns-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <span>Sort Columns</span>
            <InfoTooltip text="Ordered sort expressions evaluated left-to-right for primary and tie-breaker ordering." />
          </div>
          <button
            type="button"
            onClick={handleAddSortColumn}
            className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add Sort Key</span>
          </button>
        </div>

        <div className="flex flex-col gap-3">
          {sortColumns.map((spec, index) => {
            const isColEmpty = spec.column.trim().length === 0;
            return (
              <div
                key={index}
                data-testid={`sort-column-row-${index}`}
                className="flex flex-col gap-2 p-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)]"
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
                    Sort Key #{index + 1}
                  </span>
                  <div className="flex items-center gap-1">
                    <button
                      type="button"
                      data-index={index}
                      data-delta={-1}
                      disabled={index === 0}
                      aria-label={`Move sort key ${index + 1} up`}
                      onClick={handleMoveSortColumn}
                      className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] disabled:opacity-40 disabled:cursor-not-allowed transition-colors cursor-pointer"
                    >
                      <ArrowUp className="w-3.5 h-3.5" />
                    </button>
                    <button
                      type="button"
                      data-index={index}
                      data-delta={1}
                      disabled={index === sortColumns.length - 1}
                      aria-label={`Move sort key ${index + 1} down`}
                      onClick={handleMoveSortColumn}
                      className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] disabled:opacity-40 disabled:cursor-not-allowed transition-colors cursor-pointer"
                    >
                      <ArrowDown className="w-3.5 h-3.5" />
                    </button>
                    {sortColumns.length > 1 && (
                      <button
                        type="button"
                        data-index={index}
                        aria-label={`Remove sort key ${index + 1}`}
                        onClick={handleRemoveSortColumn}
                        className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    )}
                  </div>
                </div>

                <div className="flex flex-col gap-1">
                  <label
                    htmlFor={`sortColName-${index}`}
                    className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                  >
                    <span>Column</span>
                    <span className="text-red-400 font-bold" title="Required">*</span>
                    <InfoTooltip text="Column name passed to F.col() in the sort expression." />
                  </label>
                  <input
                    id={`sortColName-${index}`}
                    type="text"
                    data-index={index}
                    data-field="column"
                    aria-label={`Sort key ${index + 1} column`}
                    aria-invalid={isColEmpty}
                    value={spec.column}
                    onChange={handleSortFieldChange}
                    placeholder="event_timestamp"
                    className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                      isColEmpty
                        ? 'border-red-500/80 focus:border-red-500'
                        : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                    }`}
                  />
                  {isColEmpty && (
                    <span className="text-xs text-red-400" role="alert">
                      Sort column name is required.
                    </span>
                  )}
                </div>

                <div className="grid grid-cols-2 gap-2">
                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`sortColDir-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Direction</span>
                      <InfoTooltip text="Ascending (.asc) or Descending (.desc) sort direction." />
                    </label>
                    <select
                      id={`sortColDir-${index}`}
                      data-index={index}
                      data-field="direction"
                      aria-label={`Sort key ${index + 1} direction`}
                      value={spec.direction}
                      onChange={handleSortFieldChange}
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    >
                      {DIRECTION_OPTIONS.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`sortColNulls-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Nulls Placement</span>
                      <InfoTooltip text="Controls asc_nulls_first, asc_nulls_last, desc_nulls_first, or desc_nulls_last." />
                    </label>
                    <select
                      id={`sortColNulls-${index}`}
                      data-index={index}
                      data-field="nullsOrder"
                      aria-label={`Sort key ${index + 1} nulls placement`}
                      value={spec.nullsOrder}
                      onChange={handleSortFieldChange}
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    >
                      {NULLS_ORDER_OPTIONS.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      <SystemInstructionsField
        id="orderBySystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this OrderBy step."
        placeholder="Enter custom instructions for OrderBy code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
