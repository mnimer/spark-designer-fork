/**
 * @fileoverview Property configuration panel for the Apache Spark GroupBy & Aggregate step (GroupByAgg).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import { Clock, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported Spark time-window strategies inside `df.groupBy()`.
 */
export type GroupByWindowType = 'None' | 'Tumbling' | 'Sliding' | 'Session';

/**
 * Supported PySpark `pyspark.sql.functions` aggregate functions for `GroupByAgg`.
 */
export type SparkAggFunction =
  | 'count'
  | 'sum'
  | 'avg'
  | 'min'
  | 'max'
  | 'countDistinct'
  | 'approx_count_distinct'
  | 'stddev'
  | 'variance'
  | 'first'
  | 'last'
  | 'collect_list'
  | 'collect_set';

/**
 * Single aggregate expression descriptor stored in `properties.aggregations`.
 */
export interface GroupByAggregationSpec {
  column: string;
  fn: SparkAggFunction;
  alias: string;
}

/**
 * Persisted properties for the Spark `GroupByAgg` step node.
 */
export interface GroupByAggStepProperties {
  groupByColumns?: string[];
  windowType?: GroupByWindowType;
  timeColumn?: string;
  windowDuration?: string;
  slideDuration?: string;
  sessionGapDuration?: string;
  flattenWindowStruct?: boolean;
  aggregations?: GroupByAggregationSpec[];
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link GroupByAggProperties}.
 */
export interface GroupByAggNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: GroupByAggStepProperties;
  };
}

/**
 * Props for the {@link GroupByAggProperties} component.
 */
export interface GroupByAggPropertiesProps {
  node: GroupByAggNode;
  onChange: (properties: GroupByAggStepProperties) => void;
}

const WINDOW_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'None', label: 'None (Standard GroupBy)' },
  { value: 'Tumbling', label: 'Tumbling (Fixed F.window)' },
  { value: 'Sliding', label: 'Sliding (Overlapping F.window)' },
  { value: 'Session', label: 'Session (Dynamic F.session_window)' },
];

const AGG_FUNCTION_OPTIONS: SelectFieldOption[] = [
  { value: 'count', label: 'count' },
  { value: 'sum', label: 'sum' },
  { value: 'avg', label: 'avg' },
  { value: 'min', label: 'min' },
  { value: 'max', label: 'max' },
  { value: 'countDistinct', label: 'countDistinct' },
  { value: 'approx_count_distinct', label: 'approx_count_distinct' },
  { value: 'stddev', label: 'stddev' },
  { value: 'variance', label: 'variance' },
  { value: 'first', label: 'first' },
  { value: 'last', label: 'last' },
  { value: 'collect_list', label: 'collect_list' },
  { value: 'collect_set', label: 'collect_set' },
];

const VALID_AGG_FUNCTIONS = new Set<string>(AGG_FUNCTION_OPTIONS.map((opt) => opt.value));

const DEFAULT_AGGREGATIONS: GroupByAggregationSpec[] = [
  { column: '*', fn: 'count', alias: 'record_count' },
];

/**
 * Normalizes raw grouping column configuration into a clean `string[]`.
 *
 * @param raw Raw `groupByColumns` property value from YAML or UI state.
 * @returns Trimmed array of grouping column names.
 */
export function normalizeGroupByColumns(raw: unknown): string[] {
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
 * Normalizes raw aggregation specifications into typed {@link GroupByAggregationSpec} entries.
 *
 * @param raw Raw `aggregations` property value from YAML or UI state.
 * @returns Validated array of aggregation definitions.
 */
export function normalizeAggregations(raw: unknown): GroupByAggregationSpec[] {
  if (!Array.isArray(raw)) {
    return DEFAULT_AGGREGATIONS.map((item) => ({ ...item }));
  }
  return raw.map((entry) => {
    const obj = typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    const rawFn = typeof obj.fn === 'string' && VALID_AGG_FUNCTIONS.has(obj.fn) ? obj.fn : 'count';
    return {
      column: typeof obj.column === 'string' ? obj.column : '',
      fn: rawFn as SparkAggFunction,
      alias: typeof obj.alias === 'string' ? obj.alias : '',
    };
  });
}

/**
 * Renders the property configuration panel for the Apache Spark GroupBy & Aggregate step.
 *
 * @param props Selected GroupByAgg node and property update callback.
 * @returns The rendered GroupByAgg configuration form.
 */
export const GroupByAggProperties: React.FC<GroupByAggPropertiesProps> = ({ node, onChange }) => {
  const properties: GroupByAggStepProperties = node?.data?.properties || {};
  const groupByColumns = normalizeGroupByColumns(properties.groupByColumns);
  const windowType: GroupByWindowType =
    properties.windowType === 'Tumbling' ||
    properties.windowType === 'Sliding' ||
    properties.windowType === 'Session'
      ? properties.windowType
      : 'None';
  const timeColumn = typeof properties.timeColumn === 'string' ? properties.timeColumn : '';
  const windowDuration =
    typeof properties.windowDuration === 'string' ? properties.windowDuration : '5 minutes';
  const slideDuration =
    typeof properties.slideDuration === 'string' ? properties.slideDuration : '1 minute';
  const sessionGapDuration =
    typeof properties.sessionGapDuration === 'string'
      ? properties.sessionGapDuration
      : '10 minutes';
  const flattenWindowStruct = properties.flattenWindowStruct === true;
  const aggregations = normalizeAggregations(properties.aggregations);
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingColumnInput, setPendingColumnInput] = useState<string>('');

  const hasWindow = windowType !== 'None';
  const usesFixedOrSlidingWindow = windowType === 'Tumbling' || windowType === 'Sliding';
  const isTimeColumnInvalid = hasWindow && timeColumn.trim().length === 0;
  const isWindowDurationInvalid = usesFixedOrSlidingWindow && windowDuration.trim().length === 0;
  const isSlideDurationInvalid = windowType === 'Sliding' && slideDuration.trim().length === 0;
  const isSessionGapInvalid = windowType === 'Session' && sessionGapDuration.trim().length === 0;

  const commitColumns = (nextColumns: string[]) => {
    onChange({
      ...properties,
      groupByColumns: nextColumns,
    });
  };

  const handlePendingColumnChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setPendingColumnInput(e.target.value);
  };

  const appendPendingColumns = () => {
    const parsed = normalizeGroupByColumns(pendingColumnInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(groupByColumns);
    const merged = [...groupByColumns];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    setPendingColumnInput('');
    commitColumns(merged);
  };

  const handlePendingColumnKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendPendingColumns();
    }
  };

  const handleExistingColumnChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...groupByColumns];
    updated[idx] = e.target.value;
    commitColumns(normalizeGroupByColumns(updated));
  };

  const handleRemoveColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = groupByColumns.filter((_, i) => i !== idx);
    commitColumns(updated);
  };

  const handleWindowTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextWindowType = e.target.value as GroupByWindowType;
    const nextPatch: GroupByAggStepProperties = {
      ...properties,
      groupByColumns,
      windowType: nextWindowType,
      aggregations,
    };
    if (nextWindowType === 'Tumbling' || nextWindowType === 'Sliding') {
      nextPatch.windowDuration = windowDuration;
    }
    if (nextWindowType === 'Sliding') {
      nextPatch.slideDuration = slideDuration;
    }
    if (nextWindowType === 'Session') {
      nextPatch.sessionGapDuration = sessionGapDuration;
    }
    onChange(nextPatch);
  };

  const handleWindowFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    onChange({
      ...properties,
      groupByColumns,
      windowType,
      aggregations,
      [name]: value,
    });
  };

  const handleAddAggregation = () => {
    const nextAggs: GroupByAggregationSpec[] = [
      ...aggregations,
      { column: '', fn: 'sum', alias: `agg_${aggregations.length + 1}` },
    ];
    onChange({
      ...properties,
      groupByColumns,
      windowType,
      aggregations: nextAggs,
    });
  };

  const handleRemoveAggregation = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const nextAggs = aggregations.filter((_, i) => i !== idx);
    onChange({
      ...properties,
      groupByColumns,
      windowType,
      aggregations: nextAggs,
    });
  };

  const handleAggregationFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>
  ) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof GroupByAggregationSpec | undefined;
    if (Number.isNaN(idx) || !field || !aggregations[idx]) {
      return;
    }
    const nextAggs = aggregations.map((item, i) =>
      i === idx ? { ...item, [field]: e.target.value } : item
    );
    onChange({
      ...properties,
      groupByColumns,
      windowType,
      aggregations: nextAggs,
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
      {/* 1. Grouping Columns Section */}
      <div className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <label
          htmlFor="groupByColumnAddInput"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <span>Group By Columns</span>
          <InfoTooltip text="Columns passed to df.groupBy(). Enter one column or a comma-separated list, or leave empty for a global aggregation." />
        </label>

        {groupByColumns.length > 0 && (
          <div className="flex flex-col gap-2" data-testid="groupby-columns-list">
            {groupByColumns.map((col, index) => (
              <div key={`${index}-${col}`} className="flex items-center gap-2">
                <input
                  type="text"
                  data-index={index}
                  aria-label={`Group by column ${index + 1}`}
                  value={col}
                  onChange={handleExistingColumnChange}
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <button
                  type="button"
                  data-index={index}
                  aria-label={`Remove group by column ${col}`}
                  onClick={handleRemoveColumn}
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
            id="groupByColumnAddInput"
            type="text"
            value={pendingColumnInput}
            onChange={handlePendingColumnChange}
            onKeyDown={handlePendingColumnKeyDown}
            placeholder="e.g. region or user_id, device_type"
            className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
          />
          <button
            type="button"
            onClick={appendPendingColumns}
            className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add</span>
          </button>
        </div>
      </div>

      {/* 2. Built-in Spark Time Window Section */}
      <div
        data-testid="groupby-time-window-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <Clock className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Time Window</span>
          <InfoTooltip text="Spark configures event-time windows directly inside df.groupBy() via F.window() or F.session_window()." />
        </div>

        <SelectField
          id="groupByWindowType"
          name="windowType"
          label="Window Type"
          tooltip="Select None for unwindowed aggregation, Tumbling or Sliding for F.window(), or Session for F.session_window()."
          value={windowType}
          options={WINDOW_TYPE_OPTIONS}
          onChange={handleWindowTypeChange}
        />

        {hasWindow && (
          <>
            <PropertyField
              id="groupByTimeColumn"
              name="timeColumn"
              label="Time Column"
              required="Required"
              tooltip="TimestampType column passed as the first argument to F.window() or F.session_window()."
              value={timeColumn}
              onChange={handleWindowFieldChange}
              placeholder="event_timestamp"
              invalid={isTimeColumnInvalid}
              error={isTimeColumnInvalid ? 'Time column is required when a window is active.' : null}
            />

            {usesFixedOrSlidingWindow && (
              <PropertyField
                id="groupByWindowDuration"
                name="windowDuration"
                label="Window Duration"
                required="Required"
                tooltip="Window size interval string passed to F.window(), such as 5 minutes or 1 hour."
                value={windowDuration}
                onChange={handleWindowFieldChange}
                placeholder="5 minutes"
                invalid={isWindowDurationInvalid}
                error={isWindowDurationInvalid ? 'Window duration is required.' : null}
              />
            )}

            {windowType === 'Sliding' && (
              <PropertyField
                id="groupBySlideDuration"
                name="slideDuration"
                label="Slide Duration"
                required="Required"
                tooltip="Slide interval string passed as the third argument to F.window(), such as 1 minute."
                value={slideDuration}
                onChange={handleWindowFieldChange}
                placeholder="1 minute"
                invalid={isSlideDurationInvalid}
                error={isSlideDurationInvalid ? 'Slide duration is required for sliding windows.' : null}
              />
            )}

            {windowType === 'Session' && (
              <PropertyField
                id="groupBySessionGapDuration"
                name="sessionGapDuration"
                label="Session Gap Duration"
                required="Required"
                tooltip="Inactivity gap duration passed to F.session_window(), such as 10 minutes."
                value={sessionGapDuration}
                onChange={handleWindowFieldChange}
                placeholder="10 minutes"
                invalid={isSessionGapInvalid}
                error={isSessionGapInvalid ? 'Session gap duration is required for session windows.' : null}
              />
            )}

            <label
              htmlFor="groupByFlattenWindowStruct"
              className="flex items-start gap-2 text-xs text-[var(--vscode-text-primary)] cursor-pointer select-none"
            >
              <input
                id="groupByFlattenWindowStruct"
                name="flattenWindowStruct"
                type="checkbox"
                checked={flattenWindowStruct}
                onChange={(e) =>
                  onChange({
                    ...properties,
                    groupByColumns,
                    windowType,
                    aggregations,
                    flattenWindowStruct: e.target.checked,
                  })
                }
                className="mt-0.5 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] cursor-pointer"
              />
              <span className="flex items-center flex-wrap gap-1">
                <span className="font-bold">
                  Flatten Window Struct (`window_start`, `window_end`)
                </span>
                <InfoTooltip text="Unpacks window.start and window.end into top-level TimestampType columns and drops the nested window struct for flat downstream sinks." />
              </span>
            </label>

            <p className="text-xs text-[var(--vscode-text-muted)] leading-relaxed">
              If this step runs on a streaming DataFrame, place an upstream Watermark step so Spark
              can finalize window state and evict expired keys.
            </p>
          </>
        )}
      </div>

      {/* 3. Aggregations List Section */}
      <div
        data-testid="groupby-aggregations-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <span>Aggregations</span>
            <InfoTooltip text="Aggregate expressions passed to .agg(), mapping each input column and aggregate function to an output alias." />
          </div>
          <button
            type="button"
            onClick={handleAddAggregation}
            className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add Aggregation</span>
          </button>
        </div>

        <div className="flex flex-col gap-3">
          {aggregations.map((agg, index) => (
            <div
              key={index}
              data-testid={`aggregation-row-${index}`}
              className="flex flex-col gap-2 p-2 rounded border border-[var(--vscode-border-light)] bg-[var(--vscode-card-bg)]"
            >
              <div className="flex items-center justify-between gap-2">
                <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
                  Metric #{index + 1}
                </span>
                {aggregations.length > 1 && (
                  <button
                    type="button"
                    data-index={index}
                    aria-label={`Remove aggregation ${index + 1}`}
                    onClick={handleRemoveAggregation}
                    className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                  >
                    <Trash2 className="w-3.5 h-3.5" />
                  </button>
                )}
              </div>

              <div className="grid grid-cols-2 gap-2">
                <div className="flex flex-col gap-1">
                  <label
                    htmlFor={`aggFn-${index}`}
                    className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                  >
                    <span>Function</span>
                    <InfoTooltip text="PySpark aggregate function from pyspark.sql.functions." />
                  </label>
                  <select
                    id={`aggFn-${index}`}
                    data-index={index}
                    data-field="fn"
                    aria-label={`Aggregation ${index + 1} function`}
                    value={agg.fn}
                    onChange={handleAggregationFieldChange}
                    className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                  >
                    {AGG_FUNCTION_OPTIONS.map((opt) => (
                      <option key={opt.value} value={opt.value}>
                        {opt.label}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="flex flex-col gap-1">
                  <label
                    htmlFor={`aggCol-${index}`}
                    className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                  >
                    <span>Column</span>
                    <InfoTooltip text="Source column passed to the aggregate function. Use * with count for row counts." />
                  </label>
                  <input
                    id={`aggCol-${index}`}
                    type="text"
                    data-index={index}
                    data-field="column"
                    aria-label={`Aggregation ${index + 1} column`}
                    value={agg.column}
                    onChange={handleAggregationFieldChange}
                    placeholder={agg.fn === 'count' ? '*' : 'amount'}
                    className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                  />
                </div>
              </div>

              <div className="flex flex-col gap-1">
                <label
                  htmlFor={`aggAlias-${index}`}
                  className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                >
                  <span>Output Alias</span>
                  <InfoTooltip text="Output column name assigned via .alias() on the aggregate expression." />
                </label>
                <input
                  id={`aggAlias-${index}`}
                  type="text"
                  data-index={index}
                  data-field="alias"
                  aria-label={`Aggregation ${index + 1} alias`}
                  value={agg.alias}
                  onChange={handleAggregationFieldChange}
                  placeholder="total_amount"
                  className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
              </div>
            </div>
          ))}
        </div>
      </div>

      <SystemInstructionsField
        id="groupByAggSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this GroupBy & Aggregate step."
        placeholder="Enter custom instructions for GroupBy & Aggregate code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
