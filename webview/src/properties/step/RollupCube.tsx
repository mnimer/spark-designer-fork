/**
 * @fileoverview Property configuration panel for the Apache Spark Rollup / Cube step (RollupCube).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useState } from 'react';
import { AlertTriangle, Layers, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Multidimensional grouping strategy supported by the Spark RollupCube step.
 */
export type RollupCubeMode = 'rollup' | 'cube';

/**
 * Aggregate expression descriptor stored in `properties.aggregations`.
 */
export interface RollupCubeAggregationSpec {
  column: string;
  fn: string;
  alias: string;
}

/**
 * Persisted properties for the Spark `RollupCube` step node.
 */
export interface RollupCubeStepProperties {
  mode?: RollupCubeMode;
  dimensionColumns?: string[];
  aggregations?: RollupCubeAggregationSpec[];
  includeGroupingId?: boolean;
  groupingIdAlias?: string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link RollupCubeProperties}.
 */
export interface RollupCubeNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: RollupCubeStepProperties;
  };
}

/**
 * Props for the {@link RollupCubeProperties} component.
 */
export interface RollupCubePropertiesProps {
  node: RollupCubeNode;
  onChange: (properties: RollupCubeStepProperties) => void;
}

const MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'rollup', label: 'Rollup (Hierarchical N + 1 Subtotals)' },
  { value: 'cube', label: 'Cube (All 2^N Dimension Combinations)' },
];

const AGG_FUNCTION_OPTIONS: SelectFieldOption[] = [
  { value: 'count', label: 'count' },
  { value: 'sum', label: 'sum' },
  { value: 'avg', label: 'avg' },
  { value: 'min', label: 'min' },
  { value: 'max', label: 'max' },
  { value: 'countDistinct', label: 'countDistinct' },
  { value: 'first', label: 'first' },
  { value: 'last', label: 'last' },
];

const VALID_AGG_FUNCTIONS = new Set<string>(AGG_FUNCTION_OPTIONS.map((opt) => opt.value));

const DEFAULT_AGGREGATIONS: RollupCubeAggregationSpec[] = [
  { column: '*', fn: 'count', alias: 'record_count' },
];

/**
 * Normalizes raw dimension column values into a trimmed string array.
 *
 * @param raw Raw `dimensionColumns` value from YAML or UI input.
 * @returns Clean list of dimension column names.
 */
function normalizeDimensionColumns(raw: unknown): string[] {
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
 * Normalizes raw aggregation specifications into typed {@link RollupCubeAggregationSpec} entries.
 *
 * @param raw Raw `aggregations` array from YAML or UI state.
 * @returns Validated aggregation list.
 */
function normalizeAggregations(raw: unknown): RollupCubeAggregationSpec[] {
  if (!Array.isArray(raw)) {
    return DEFAULT_AGGREGATIONS.map((item) => ({ ...item }));
  }
  return raw.map((entry) => {
    const obj =
      typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    const fn = typeof obj.fn === 'string' && VALID_AGG_FUNCTIONS.has(obj.fn) ? obj.fn : 'count';
    return {
      column: typeof obj.column === 'string' ? obj.column : '',
      fn,
      alias: typeof obj.alias === 'string' ? obj.alias : '',
    };
  });
}

/**
 * Renders the property configuration panel for the Apache Spark Rollup / Cube step.
 *
 * @param props Selected RollupCube node and property update callback.
 * @returns The rendered RollupCube configuration form.
 */
export const RollupCubeProperties: React.FC<RollupCubePropertiesProps> = ({ node, onChange }) => {
  const properties: RollupCubeStepProperties = node?.data?.properties || {};
  const mode: RollupCubeMode = properties.mode === 'cube' ? 'cube' : 'rollup';
  const dimensionColumns = normalizeDimensionColumns(properties.dimensionColumns);
  const aggregations = normalizeAggregations(properties.aggregations);
  const includeGroupingId =
    typeof properties.includeGroupingId === 'boolean' ? properties.includeGroupingId : true;
  const groupingIdAlias =
    typeof properties.groupingIdAlias === 'string' ? properties.groupingIdAlias : 'grouping_id';
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingDimInput, setPendingDimInput] = useState<string>('');

  const isDimensionsEmpty = dimensionColumns.length === 0;
  const showCubeCombinatorialWarning = mode === 'cube' && dimensionColumns.length >= 5;
  const isGroupingIdAliasInvalid = includeGroupingId && groupingIdAlias.trim().length === 0;

  const emitUpdate = (patch: Partial<RollupCubeStepProperties>) => {
    onChange({
      ...properties,
      mode,
      dimensionColumns,
      aggregations,
      includeGroupingId,
      groupingIdAlias,
      ...patch,
    });
  };

  const handleModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitUpdate({ mode: e.target.value as RollupCubeMode });
  };

  const appendPendingDimensions = () => {
    const parsed = normalizeDimensionColumns(pendingDimInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(dimensionColumns);
    const merged = [...dimensionColumns];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    setPendingDimInput('');
    emitUpdate({ dimensionColumns: merged });
  };

  const handlePendingDimKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendPendingDimensions();
    }
  };

  const handleExistingDimChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...dimensionColumns];
    updated[idx] = e.target.value;
    emitUpdate({ dimensionColumns: normalizeDimensionColumns(updated) });
  };

  const handleRemoveDim = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    emitUpdate({ dimensionColumns: dimensionColumns.filter((_, i) => i !== idx) });
  };

  const handleAddAggregation = () => {
    const nextAggs: RollupCubeAggregationSpec[] = [
      ...aggregations,
      { column: '', fn: 'sum', alias: `agg_${aggregations.length + 1}` },
    ];
    emitUpdate({ aggregations: nextAggs });
  };

  const handleRemoveAggregation = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    emitUpdate({ aggregations: aggregations.filter((_, i) => i !== idx) });
  };

  const handleAggregationFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>
  ) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof RollupCubeAggregationSpec | undefined;
    if (Number.isNaN(idx) || !field || !aggregations[idx]) {
      return;
    }
    const nextAggs = aggregations.map((item, i) =>
      i === idx ? { ...item, [field]: e.target.value } : item
    );
    emitUpdate({ aggregations: nextAggs });
  };

  const handleIncludeGroupingIdChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    emitUpdate({ includeGroupingId: e.target.checked });
  };

  const handleGroupingIdAliasChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    emitUpdate({ groupingIdAlias: e.target.value });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      <SelectField
        id="rollupCubeMode"
        name="mode"
        label="Grouping Set Mode"
        tooltip="Select Rollup for ordered top-down hierarchical subtotals (df.rollup) or Cube for cross-tabular subtotals across all dimension combinations (df.cube)."
        value={mode}
        options={MODE_OPTIONS}
        onChange={handleModeChange}
      />

      {/* Dimension Columns Section */}
      <div
        data-testid="rollup-cube-dimensions-section"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <label
          htmlFor="rollupCubeDimAddInput"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <Layers className="w-3.5 h-3.5 text-blue-400 shrink-0" />
          <span>Dimension Columns</span>
          <span className="text-red-400 font-bold" title="Required">
            *
          </span>
          <InfoTooltip text="Ordered dimension columns passed to df.rollup(*cols) or df.cube(*cols). Column order defines the drill-down hierarchy in Rollup mode." />
        </label>

        {dimensionColumns.length > 0 && (
          <div className="flex flex-col gap-2" data-testid="rollup-cube-dimensions-list">
            {dimensionColumns.map((col, index) => (
              <div key={`${index}-${col}`} className="flex items-center gap-2">
                <input
                  type="text"
                  data-index={index}
                  aria-label={`Dimension column ${index + 1}`}
                  value={col}
                  onChange={handleExistingDimChange}
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <button
                  type="button"
                  data-index={index}
                  aria-label={`Remove dimension column ${col}`}
                  onClick={handleRemoveDim}
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
            id="rollupCubeDimAddInput"
            type="text"
            value={pendingDimInput}
            onChange={(e) => setPendingDimInput(e.target.value)}
            onKeyDown={handlePendingDimKeyDown}
            placeholder="e.g. region, country, channel"
            aria-invalid={isDimensionsEmpty}
            className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
          />
          <button
            type="button"
            aria-label="Add Dimension Column"
            onClick={appendPendingDimensions}
            className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add</span>
          </button>
        </div>

        {isDimensionsEmpty && (
          <span className="text-xs text-red-400" role="alert">
            At least one dimension column is required for Rollup or Cube.
          </span>
        )}

        {showCubeCombinatorialWarning && (
          <div
            data-testid="cube-combinatorial-warning"
            className="flex items-start gap-2 p-2 rounded border border-amber-500/40 bg-amber-500/10 text-xs text-amber-200"
          >
            <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0 mt-0.5" />
            <span>
              Cube mode with {dimensionColumns.length} dimensions generates{' '}
              {2 ** dimensionColumns.length} grouping sets per input row. Consider Rollup if you
              only need hierarchical subtotals.
            </span>
          </div>
        )}
      </div>

      {/* Aggregations Section */}
      <div
        data-testid="rollup-cube-aggregations-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <span>Aggregations</span>
            <InfoTooltip text="Aggregate expressions evaluated across every grouping set in .agg(*exprs)." />
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
              data-testid={`rollup-agg-row-${index}`}
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
                    htmlFor={`rollupAggFn-${index}`}
                    className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                  >
                    <span>Function</span>
                    <InfoTooltip text="PySpark aggregate function from pyspark.sql.functions." />
                  </label>
                  <select
                    id={`rollupAggFn-${index}`}
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
                    htmlFor={`rollupAggCol-${index}`}
                    className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                  >
                    <span>Column</span>
                    <InfoTooltip text="Source column passed to the aggregate function. Use * with count for row counts." />
                  </label>
                  <input
                    id={`rollupAggCol-${index}`}
                    type="text"
                    data-index={index}
                    data-field="column"
                    aria-label={`Aggregation ${index + 1} column`}
                    value={agg.column}
                    onChange={handleAggregationFieldChange}
                    placeholder={agg.fn === 'count' ? '*' : 'revenue'}
                    className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                  />
                </div>
              </div>

              <div className="flex flex-col gap-1">
                <label
                  htmlFor={`rollupAggAlias-${index}`}
                  className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                >
                  <span>Output Alias</span>
                  <InfoTooltip text="Output column name assigned via .alias() on the aggregate expression." />
                </label>
                <input
                  id={`rollupAggAlias-${index}`}
                  type="text"
                  data-index={index}
                  data-field="alias"
                  aria-label={`Aggregation ${index + 1} alias`}
                  value={agg.alias}
                  onChange={handleAggregationFieldChange}
                  placeholder="total_revenue"
                  className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Grouping ID Disambiguation Section */}
      <div
        data-testid="rollup-cube-grouping-id-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <label
          htmlFor="rollupIncludeGroupingId"
          className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)] cursor-pointer"
        >
          <input
            id="rollupIncludeGroupingId"
            type="checkbox"
            checked={includeGroupingId}
            onChange={handleIncludeGroupingIdChange}
            className="rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)]"
          />
          <span>Include F.grouping_id() Column</span>
          <InfoTooltip text="Appends F.grouping_id(*dimensionColumns) to distinguish subtotal and grand-total NULL rows from genuine NULL values in input dimension keys." />
        </label>

        {includeGroupingId && (
          <PropertyField
            id="rollupGroupingIdAlias"
            name="groupingIdAlias"
            label="Grouping ID Alias"
            required="Required"
            tooltip="Output alias assigned to F.grouping_id(*dimensionColumns), where 0 marks detail rows and (2^N - 1) marks the grand total."
            value={groupingIdAlias}
            onChange={handleGroupingIdAliasChange}
            placeholder="grouping_id"
            invalid={isGroupingIdAliasInvalid}
            error={isGroupingIdAliasInvalid ? 'Grouping ID alias is required when enabled.' : null}
          />
        )}
      </div>

      <SystemInstructionsField
        id="rollupCubeSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this Rollup / Cube step."
        placeholder="Enter custom instructions for Rollup / Cube code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
