/**
 * @fileoverview Property configuration panel for the Apache Spark Window Over step (WindowOver).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useState } from 'react';
import { AlertTriangle, ArrowUpDown, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Sort direction for an `orderBy` clause inside `Window.orderBy(...)`.
 */
export type WindowSortDirection = 'asc' | 'desc';

/**
 * Ordering specification for one column inside `Window.orderBy(...)`.
 */
export interface WindowOrderBySpec {
  column: string;
  direction: WindowSortDirection;
}

/**
 * Frame specification mode for `WindowSpec`.
 */
export type WindowFrameType = 'default' | 'rowsBetween' | 'rangeBetween';

/**
 * Analytic or cumulative window metric descriptor stored in `properties.windowMetrics`.
 */
export interface WindowMetricSpec {
  fn: string;
  sourceColumn?: string;
  offset?: number;
  defaultValue?: string;
  alias: string;
}

/**
 * Persisted properties for the Spark `WindowOver` step node.
 */
export interface WindowOverStepProperties {
  partitionByColumns?: string[];
  orderByColumns?: WindowOrderBySpec[];
  frameType?: WindowFrameType;
  frameStart?: string;
  frameEnd?: string;
  windowMetrics?: WindowMetricSpec[];
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link WindowOverProperties}.
 */
export interface WindowOverNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: WindowOverStepProperties;
  };
}

/**
 * Props for the {@link WindowOverProperties} component.
 */
export interface WindowOverPropertiesProps {
  node: WindowOverNode;
  onChange: (properties: WindowOverStepProperties) => void;
}

const FRAME_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'default', label: 'Default Frame (Catalyst Standard)' },
  { value: 'rowsBetween', label: 'rowsBetween (Physical Row Offsets)' },
  { value: 'rangeBetween', label: 'rangeBetween (Logical Value Range)' },
];

const FRAME_BOUNDARY_PRESETS = [
  'unboundedPreceding',
  'currentRow',
  'unboundedFollowing',
  '-1',
  '1',
] as const;

const WINDOW_FN_OPTIONS: SelectFieldOption[] = [
  { value: 'row_number', label: 'row_number' },
  { value: 'rank', label: 'rank' },
  { value: 'dense_rank', label: 'dense_rank' },
  { value: 'percent_rank', label: 'percent_rank' },
  { value: 'cume_dist', label: 'cume_dist' },
  { value: 'ntile', label: 'ntile' },
  { value: 'lag', label: 'lag' },
  { value: 'lead', label: 'lead' },
  { value: 'first', label: 'first' },
  { value: 'last', label: 'last' },
  { value: 'sum', label: 'sum' },
  { value: 'avg', label: 'avg' },
  { value: 'min', label: 'min' },
  { value: 'max', label: 'max' },
  { value: 'count', label: 'count' },
];

const VALID_WINDOW_FNS = new Set<string>(WINDOW_FN_OPTIONS.map((opt) => opt.value));

const FNS_REQUIRING_SOURCE_COLUMN = new Set<string>([
  'lag',
  'lead',
  'first',
  'last',
  'sum',
  'avg',
  'min',
  'max',
  'count',
]);

const FNS_WITH_OFFSET = new Set<string>(['lag', 'lead', 'ntile']);
const FNS_WITH_DEFAULT_VALUE = new Set<string>(['lag', 'lead']);

const DEFAULT_METRICS: WindowMetricSpec[] = [{ fn: 'row_number', alias: 'row_num' }];

/**
 * Normalizes raw partition columns into a trimmed string array.
 *
 * @param raw Raw `partitionByColumns` value from YAML or UI input.
 * @returns Clean list of partition column names.
 */
function normalizePartitionColumns(raw: unknown): string[] {
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
 * Normalizes raw ordering entries into typed {@link WindowOrderBySpec} items.
 *
 * @param raw Raw `orderByColumns` array from YAML or UI state.
 * @returns Normalized ordering list.
 */
function normalizeOrderByColumns(raw: unknown): WindowOrderBySpec[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw.map((entry) => {
    const obj =
      typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    return {
      column: typeof obj.column === 'string' ? obj.column : '',
      direction: obj.direction === 'desc' ? 'desc' : 'asc',
    };
  });
}

/**
 * Normalizes raw window metric rows into typed {@link WindowMetricSpec} items.
 *
 * @param raw Raw `windowMetrics` array from YAML or UI state.
 * @returns Normalized window metrics list.
 */
function normalizeWindowMetrics(raw: unknown): WindowMetricSpec[] {
  if (!Array.isArray(raw)) {
    return DEFAULT_METRICS.map((item) => ({ ...item }));
  }
  return raw.map((entry) => {
    const obj =
      typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    const fn = typeof obj.fn === 'string' && VALID_WINDOW_FNS.has(obj.fn) ? obj.fn : 'row_number';
    const spec: WindowMetricSpec = {
      fn,
      alias: typeof obj.alias === 'string' ? obj.alias : '',
    };
    if (typeof obj.sourceColumn === 'string') {
      spec.sourceColumn = obj.sourceColumn;
    }
    if (typeof obj.offset === 'number' && !Number.isNaN(obj.offset)) {
      spec.offset = obj.offset;
    }
    if (typeof obj.defaultValue === 'string') {
      spec.defaultValue = obj.defaultValue;
    }
    return spec;
  });
}

/**
 * Renders the property configuration panel for the Apache Spark Window Over step.
 *
 * @param props Selected WindowOver node and property update callback.
 * @returns The rendered WindowOver configuration form.
 */
export const WindowOverProperties: React.FC<WindowOverPropertiesProps> = ({ node, onChange }) => {
  const properties: WindowOverStepProperties = node?.data?.properties || {};
  const partitionByColumns = normalizePartitionColumns(properties.partitionByColumns);
  const orderByColumns = normalizeOrderByColumns(properties.orderByColumns);
  const frameType: WindowFrameType =
    properties.frameType === 'rowsBetween' || properties.frameType === 'rangeBetween'
      ? properties.frameType
      : 'default';
  const frameStart =
    typeof properties.frameStart === 'string' ? properties.frameStart : 'unboundedPreceding';
  const frameEnd = typeof properties.frameEnd === 'string' ? properties.frameEnd : 'currentRow';
  const windowMetrics = normalizeWindowMetrics(properties.windowMetrics);
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingPartitionInput, setPendingPartitionInput] = useState<string>('');

  const hasCustomFrame = frameType !== 'default';
  const isFrameStartInvalid = hasCustomFrame && frameStart.trim().length === 0;
  const isFrameEndInvalid = hasCustomFrame && frameEnd.trim().length === 0;

  const emitUpdate = (patch: Partial<WindowOverStepProperties>) => {
    onChange({
      ...properties,
      partitionByColumns,
      orderByColumns,
      frameType,
      frameStart,
      frameEnd,
      windowMetrics,
      ...patch,
    });
  };

  const appendPendingPartitions = () => {
    const parsed = normalizePartitionColumns(pendingPartitionInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(partitionByColumns);
    const merged = [...partitionByColumns];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    setPendingPartitionInput('');
    emitUpdate({ partitionByColumns: merged });
  };

  const handlePartitionKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendPendingPartitions();
    }
  };

  const handleExistingPartitionChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...partitionByColumns];
    updated[idx] = e.target.value;
    emitUpdate({ partitionByColumns: normalizePartitionColumns(updated) });
  };

  const handleRemovePartition = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    emitUpdate({ partitionByColumns: partitionByColumns.filter((_, i) => i !== idx) });
  };

  const handleAddOrderBy = () => {
    emitUpdate({
      orderByColumns: [...orderByColumns, { column: '', direction: 'asc' }],
    });
  };

  const handleOrderByFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>
  ) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof WindowOrderBySpec | undefined;
    if (Number.isNaN(idx) || !field || !orderByColumns[idx]) {
      return;
    }
    const nextOrder = orderByColumns.map((item, i) =>
      i === idx
        ? {
            ...item,
            [field]:
              field === 'direction'
                ? (e.target.value === 'desc' ? 'desc' : 'asc')
                : e.target.value,
          }
        : item
    );
    emitUpdate({ orderByColumns: nextOrder });
  };

  const handleRemoveOrderBy = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    emitUpdate({ orderByColumns: orderByColumns.filter((_, i) => i !== idx) });
  };

  const handleFrameTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitUpdate({ frameType: e.target.value as WindowFrameType });
  };

  const handleFrameBoundChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    emitUpdate({ [name]: value });
  };

  const handleAddMetric = () => {
    const nextMetrics: WindowMetricSpec[] = [
      ...windowMetrics,
      { fn: 'sum', sourceColumn: '', alias: `win_metric_${windowMetrics.length + 1}` },
    ];
    emitUpdate({ windowMetrics: nextMetrics });
  };

  const handleRemoveMetric = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    emitUpdate({ windowMetrics: windowMetrics.filter((_, i) => i !== idx) });
  };

  const handleMetricFnChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx) || !windowMetrics[idx]) {
      return;
    }
    const nextFn = e.target.value;
    const current = windowMetrics[idx];
    const updated: WindowMetricSpec = {
      fn: nextFn,
      alias: current.alias,
    };
    if (FNS_REQUIRING_SOURCE_COLUMN.has(nextFn)) {
      updated.sourceColumn = current.sourceColumn ?? (nextFn === 'count' ? '*' : '');
    }
    if (FNS_WITH_OFFSET.has(nextFn)) {
      updated.offset = current.offset ?? (nextFn === 'ntile' ? 4 : 1);
    }
    if (FNS_WITH_DEFAULT_VALUE.has(nextFn) && current.defaultValue !== undefined) {
      updated.defaultValue = current.defaultValue;
    }
    const nextMetrics = windowMetrics.map((item, i) => (i === idx ? updated : item));
    emitUpdate({ windowMetrics: nextMetrics });
  };

  const handleMetricFieldChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field;
    if (Number.isNaN(idx) || !field || !windowMetrics[idx]) {
      return;
    }
    const val = e.target.value;
    const nextMetrics = windowMetrics.map((item, i) => {
      if (i !== idx) {
        return item;
      }
      if (field === 'offset') {
        if (val.trim() === '') {
          const copy = { ...item };
          delete copy.offset;
          return copy;
        }
        const parsed = Number(val);
        return Number.isNaN(parsed) ? item : { ...item, offset: parsed };
      }
      return { ...item, [field]: val };
    });
    emitUpdate({ windowMetrics: nextMetrics });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Partition By Section */}
      <div
        data-testid="window-partition-section"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <label
          htmlFor="windowPartitionAddInput"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <span>Partition By Columns</span>
          <InfoTooltip text="Columns passed to Window.partitionBy(*cols) to distribute window evaluation across executors in parallel." />
        </label>

        {partitionByColumns.length === 0 && (
          <div
            data-testid="window-empty-partition-warning"
            className="flex items-start gap-2 p-2 rounded border border-amber-500/40 bg-amber-500/10 text-xs text-amber-200"
          >
            <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0 mt-0.5" />
            <span>
              No partition columns configured. Spark moves all rows into a single partition when
              Window.partitionBy() is empty, which can exhaust executor memory on large datasets.
            </span>
          </div>
        )}

        {partitionByColumns.length > 0 && (
          <div className="flex flex-col gap-2">
            {partitionByColumns.map((col, index) => (
              <div key={`${index}-${col}`} className="flex items-center gap-2">
                <input
                  type="text"
                  data-index={index}
                  aria-label={`Partition column ${index + 1}`}
                  value={col}
                  onChange={handleExistingPartitionChange}
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <button
                  type="button"
                  data-index={index}
                  aria-label={`Remove partition column ${col}`}
                  onClick={handleRemovePartition}
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
            id="windowPartitionAddInput"
            type="text"
            value={pendingPartitionInput}
            onChange={(e) => setPendingPartitionInput(e.target.value)}
            onKeyDown={handlePartitionKeyDown}
            placeholder="e.g. account_id, region"
            className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
          />
          <button
            type="button"
            aria-label="Add Partition Column"
            onClick={appendPendingPartitions}
            className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add</span>
          </button>
        </div>
      </div>

      {/* 2. Order By Section */}
      <div
        data-testid="window-orderby-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <ArrowUpDown className="w-3.5 h-3.5 text-blue-400 shrink-0" />
            <span>Order By Columns</span>
            <InfoTooltip text="Sort columns and directions passed to Window.orderBy(*sortExprs) within each partition." />
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

        {orderByColumns.length === 0 ? (
          <p className="text-xs text-[var(--vscode-text-muted)]">
            Add at least one sort column for ranking, lag/lead offsets, or ordered running frames.
          </p>
        ) : (
          <div className="flex flex-col gap-2">
            {orderByColumns.map((ord, index) => (
              <div key={index} className="flex items-center gap-2">
                <input
                  type="text"
                  data-index={index}
                  data-field="column"
                  aria-label={`Order by column ${index + 1}`}
                  value={ord.column}
                  onChange={handleOrderByFieldChange}
                  placeholder="event_timestamp"
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <select
                  data-index={index}
                  data-field="direction"
                  aria-label={`Order by direction ${index + 1}`}
                  value={ord.direction}
                  onChange={handleOrderByFieldChange}
                  className="w-24 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                >
                  <option value="asc">asc</option>
                  <option value="desc">desc</option>
                </select>
                <button
                  type="button"
                  data-index={index}
                  aria-label={`Remove order by column ${index + 1}`}
                  onClick={handleRemoveOrderBy}
                  className="p-2 rounded border border-[var(--vscode-border)] text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                >
                  <Trash2 className="w-4 h-4" />
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* 3. Window Frame Section */}
      <div
        data-testid="window-frame-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <SelectField
          id="windowFrameType"
          name="frameType"
          label="Window Frame"
          tooltip="Configure rowsBetween or rangeBetween bounds for cumulative or sliding window aggregates. Ranking and lag/lead functions always use the unframed partition order."
          value={frameType}
          options={FRAME_TYPE_OPTIONS}
          onChange={handleFrameTypeChange}
        />

        {hasCustomFrame && (
          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="windowFrameStart"
              name="frameStart"
              label="Frame Start"
              required="Required"
              tooltip="Start bound passed to rowsBetween or rangeBetween, such as unboundedPreceding, currentRow, or a negative integer offset."
              value={frameStart}
              datalist={[...FRAME_BOUNDARY_PRESETS]}
              onChange={handleFrameBoundChange}
              placeholder="unboundedPreceding"
              invalid={isFrameStartInvalid}
              error={isFrameStartInvalid ? 'Frame start is required.' : null}
            />

            <PropertyField
              id="windowFrameEnd"
              name="frameEnd"
              label="Frame End"
              required="Required"
              tooltip="End bound passed to rowsBetween or rangeBetween, such as currentRow, unboundedFollowing, or an integer offset."
              value={frameEnd}
              datalist={[...FRAME_BOUNDARY_PRESETS]}
              onChange={handleFrameBoundChange}
              placeholder="currentRow"
              invalid={isFrameEndInvalid}
              error={isFrameEndInvalid ? 'Frame end is required.' : null}
            />
          </div>
        )}
      </div>

      {/* 4. Window Metrics Section */}
      <div
        data-testid="window-metrics-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <span>Window Metrics</span>
            <InfoTooltip text="Analytic and cumulative columns computed in a single df.withColumns({...}) projection pass." />
          </div>
          <button
            type="button"
            onClick={handleAddMetric}
            className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add Metric</span>
          </button>
        </div>

        <div className="flex flex-col gap-3">
          {windowMetrics.map((metric, index) => {
            const needsSourceCol = FNS_REQUIRING_SOURCE_COLUMN.has(metric.fn);
            const needsOffset = FNS_WITH_OFFSET.has(metric.fn);
            const needsDefault = FNS_WITH_DEFAULT_VALUE.has(metric.fn);
            const isAliasInvalid = metric.alias.trim().length === 0;

            return (
              <div
                key={index}
                data-testid={`window-metric-row-${index}`}
                className="flex flex-col gap-2 p-2 rounded border border-[var(--vscode-border-light)] bg-[var(--vscode-card-bg)]"
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
                    Window Column #{index + 1}
                  </span>
                  {windowMetrics.length > 1 && (
                    <button
                      type="button"
                      data-index={index}
                      aria-label={`Remove window metric ${index + 1}`}
                      onClick={handleRemoveMetric}
                      className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  )}
                </div>

                <div className="grid grid-cols-2 gap-2">
                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`winFn-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Function</span>
                      <InfoTooltip text="Analytic ranking, offset, or aggregate function applied with .over(windowSpec)." />
                    </label>
                    <select
                      id={`winFn-${index}`}
                      data-index={index}
                      aria-label={`Window metric ${index + 1} function`}
                      value={metric.fn}
                      onChange={handleMetricFnChange}
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    >
                      {WINDOW_FN_OPTIONS.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`winAlias-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Output Alias</span>
                      <span className="text-red-400 font-bold" title="Required">
                        *
                      </span>
                      <InfoTooltip text="Column name written into df.withColumns({alias: expr.over(windowSpec)})." />
                    </label>
                    <input
                      id={`winAlias-${index}`}
                      type="text"
                      data-index={index}
                      data-field="alias"
                      aria-label={`Window metric ${index + 1} alias`}
                      aria-invalid={isAliasInvalid}
                      value={metric.alias}
                      onChange={handleMetricFieldChange}
                      placeholder="row_num"
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    />
                  </div>
                </div>

                {needsSourceCol && (
                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`winSourceCol-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Source Column</span>
                      <InfoTooltip text="Input column passed to lag, lead, first, last, or cumulative aggregate functions." />
                    </label>
                    <input
                      id={`winSourceCol-${index}`}
                      type="text"
                      data-index={index}
                      data-field="sourceColumn"
                      aria-label={`Window metric ${index + 1} source column`}
                      value={metric.sourceColumn ?? ''}
                      onChange={handleMetricFieldChange}
                      placeholder={metric.fn === 'count' ? '*' : 'amount'}
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    />
                  </div>
                )}

                {(needsOffset || needsDefault) && (
                  <div className="grid grid-cols-2 gap-2">
                    {needsOffset && (
                      <div className="flex flex-col gap-1">
                        <label
                          htmlFor={`winOffset-${index}`}
                          className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                        >
                          <span>{metric.fn === 'ntile' ? 'Buckets (N)' : 'Row Offset'}</span>
                          <InfoTooltip
                            text={
                              metric.fn === 'ntile'
                                ? 'Number of quantile buckets passed to F.ntile(n).'
                                : 'Row offset distance passed to F.lag or F.lead.'
                            }
                          />
                        </label>
                        <input
                          id={`winOffset-${index}`}
                          type="number"
                          data-index={index}
                          data-field="offset"
                          aria-label={`Window metric ${index + 1} offset`}
                          value={metric.offset ?? ''}
                          onChange={handleMetricFieldChange}
                          placeholder={metric.fn === 'ntile' ? '4' : '1'}
                          className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                        />
                      </div>
                    )}

                    {needsDefault && (
                      <div className="flex flex-col gap-1">
                        <label
                          htmlFor={`winDefault-${index}`}
                          className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                        >
                          <span>Default Value</span>
                          <InfoTooltip text="Optional fallback literal returned when lag or lead looks beyond the partition boundary." />
                        </label>
                        <input
                          id={`winDefault-${index}`}
                          type="text"
                          data-index={index}
                          data-field="defaultValue"
                          aria-label={`Window metric ${index + 1} default value`}
                          value={metric.defaultValue ?? ''}
                          onChange={handleMetricFieldChange}
                          placeholder="0"
                          className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                        />
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>

      <SystemInstructionsField
        id="windowOverSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this Window Over step."
        placeholder="Enter custom instructions for Window Over code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
