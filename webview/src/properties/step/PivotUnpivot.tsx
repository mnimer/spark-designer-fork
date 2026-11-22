/**
 * @fileoverview Property configuration panel for the Apache Spark Pivot / Unpivot step (PivotUnpivot).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useState } from 'react';
import { Plus, Sparkles, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Reshaping direction supported by the Spark PivotUnpivot step.
 */
export type PivotUnpivotOperation = 'pivot' | 'unpivot';

/**
 * Aggregate functions supported inside `df.groupBy(...).pivot(...).agg(...)`.
 */
export type PivotAggFunction = 'sum' | 'avg' | 'count' | 'min' | 'max' | 'first';

/**
 * Persisted properties for the Spark `PivotUnpivot` step node.
 */
export interface PivotUnpivotStepProperties {
  operation?: PivotUnpivotOperation;
  groupByColumns?: string[];
  pivotColumn?: string;
  pivotValues?: string[];
  aggFunction?: PivotAggFunction;
  aggColumn?: string;
  idColumns?: string[];
  valueColumns?: string[];
  variableColumnName?: string;
  valueColumnName?: string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link PivotUnpivotProperties}.
 */
export interface PivotUnpivotNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: PivotUnpivotStepProperties;
  };
}

/**
 * Props for the {@link PivotUnpivotProperties} component.
 */
export interface PivotUnpivotPropertiesProps {
  node: PivotUnpivotNode;
  onChange: (properties: PivotUnpivotStepProperties) => void;
}

const OPERATION_OPTIONS: SelectFieldOption[] = [
  { value: 'pivot', label: 'Pivot (Rows to Wide Columns)' },
  { value: 'unpivot', label: 'Unpivot (Wide Columns to Rows)' },
];

const PIVOT_AGG_OPTIONS: SelectFieldOption[] = [
  { value: 'sum', label: 'sum' },
  { value: 'avg', label: 'avg' },
  { value: 'count', label: 'count' },
  { value: 'min', label: 'min' },
  { value: 'max', label: 'max' },
  { value: 'first', label: 'first' },
];

const VALID_PIVOT_AGG_FUNCTIONS = new Set<string>(PIVOT_AGG_OPTIONS.map((opt) => opt.value));

/**
 * Normalizes a string array or comma-delimited string into trimmed non-empty strings.
 *
 * @param raw Raw property value from persisted YAML or form input.
 * @returns Clean array of non-empty strings.
 */
function normalizeStringList(raw: unknown): string[] {
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
 * Props for an internal editable string list card section.
 */
interface StringListSectionProps {
  id: string;
  title: string;
  tooltip: string;
  placeholder: string;
  itemAriaPrefix: string;
  items: string[];
  testId: string;
  helperBanner?: React.ReactNode;
  onItemsChange: (next: string[]) => void;
}

/**
 * Renders a reusable editable list of column names or literal values.
 *
 * @param props List configuration and update handler.
 * @returns The rendered list editor section.
 */
function StringListSection({
  id,
  title,
  tooltip,
  placeholder,
  itemAriaPrefix,
  items,
  testId,
  helperBanner,
  onItemsChange,
}: StringListSectionProps): React.ReactElement {
  const [pendingInput, setPendingInput] = useState<string>('');

  const appendPendingItems = () => {
    const parsed = normalizeStringList(pendingInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(items);
    const merged = [...items];
    for (const entry of parsed) {
      if (!existing.has(entry)) {
        existing.add(entry);
        merged.push(entry);
      }
    }
    setPendingInput('');
    onItemsChange(merged);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendPendingItems();
    }
  };

  const handleExistingChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...items];
    updated[idx] = e.target.value;
    onItemsChange(normalizeStringList(updated));
  };

  const handleRemove = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    onItemsChange(items.filter((_, i) => i !== idx));
  };

  return (
    <div
      data-testid={testId}
      className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
    >
      <label
        htmlFor={id}
        className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
      >
        <span>{title}</span>
        <InfoTooltip text={tooltip} />
      </label>

      {helperBanner}

      {items.length > 0 && (
        <div className="flex flex-col gap-2">
          {items.map((item, index) => (
            <div key={`${index}-${item}`} className="flex items-center gap-2">
              <input
                type="text"
                data-index={index}
                aria-label={`${itemAriaPrefix} ${index + 1}`}
                value={item}
                onChange={handleExistingChange}
                className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
              />
              <button
                type="button"
                data-index={index}
                aria-label={`Remove ${itemAriaPrefix.toLowerCase()} ${item}`}
                onClick={handleRemove}
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
          id={id}
          type="text"
          value={pendingInput}
          onChange={(e) => setPendingInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
        />
        <button
          type="button"
          aria-label={`Add ${title}`}
          onClick={appendPendingItems}
          className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
        >
          <Plus className="w-3.5 h-3.5" />
          <span>Add</span>
        </button>
      </div>
    </div>
  );
}

/**
 * Renders the property configuration panel for the Apache Spark Pivot / Unpivot step.
 *
 * @param props Selected PivotUnpivot node and property update callback.
 * @returns The rendered PivotUnpivot configuration form.
 */
export const PivotUnpivotProperties: React.FC<PivotUnpivotPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: PivotUnpivotStepProperties = node?.data?.properties || {};
  const operation: PivotUnpivotOperation =
    properties.operation === 'unpivot' ? 'unpivot' : 'pivot';

  const groupByColumns = normalizeStringList(properties.groupByColumns);
  const pivotColumn = typeof properties.pivotColumn === 'string' ? properties.pivotColumn : '';
  const pivotValues = normalizeStringList(properties.pivotValues);
  const aggFunction: PivotAggFunction =
    typeof properties.aggFunction === 'string' && VALID_PIVOT_AGG_FUNCTIONS.has(properties.aggFunction)
      ? (properties.aggFunction as PivotAggFunction)
      : 'sum';
  const aggColumn =
    typeof properties.aggColumn === 'string'
      ? properties.aggColumn
      : aggFunction === 'count'
        ? '*'
        : '';

  const idColumns = normalizeStringList(properties.idColumns);
  const valueColumns = normalizeStringList(properties.valueColumns);
  const variableColumnName =
    typeof properties.variableColumnName === 'string' ? properties.variableColumnName : 'variable';
  const valueColumnName =
    typeof properties.valueColumnName === 'string' ? properties.valueColumnName : 'value';

  const systemInstructions = resolveSystemInstructions(node);

  const isPivotColumnInvalid = operation === 'pivot' && pivotColumn.trim().length === 0;
  const isAggColumnInvalid =
    operation === 'pivot' && aggFunction !== 'count' && aggColumn.trim().length === 0;
  const isVariableColInvalid = operation === 'unpivot' && variableColumnName.trim().length === 0;
  const isValueColInvalid = operation === 'unpivot' && valueColumnName.trim().length === 0;

  const handleOperationChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextOperation = e.target.value as PivotUnpivotOperation;
    if (nextOperation === 'unpivot') {
      onChange({
        ...properties,
        operation: 'unpivot',
        idColumns,
        valueColumns,
        variableColumnName,
        valueColumnName,
      });
      return;
    }
    onChange({
      ...properties,
      operation: 'pivot',
      groupByColumns,
      pivotColumn,
      pivotValues,
      aggFunction,
      aggColumn,
    });
  };

  const handlePivotFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    onChange({
      ...properties,
      operation: 'pivot',
      groupByColumns,
      pivotColumn,
      pivotValues,
      aggFunction,
      aggColumn,
      [name]: value,
    });
  };

  const handleAggFunctionChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextFn = e.target.value as PivotAggFunction;
    const nextAggCol = nextFn === 'count' && aggColumn.trim().length === 0 ? '*' : aggColumn;
    onChange({
      ...properties,
      operation: 'pivot',
      groupByColumns,
      pivotColumn,
      pivotValues,
      aggFunction: nextFn,
      aggColumn: nextAggCol,
    });
  };

  const handleUnpivotFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    onChange({
      ...properties,
      operation: 'unpivot',
      idColumns,
      valueColumns,
      variableColumnName,
      valueColumnName,
      [name]: value,
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
      <SelectField
        id="pivotUnpivotOperation"
        name="operation"
        label="Reshape Operation"
        tooltip="Choose Pivot to rotate row values into wide columns with df.groupBy().pivot().agg(), or Unpivot to stack wide columns into rows with df.unpivot()."
        value={operation}
        options={OPERATION_OPTIONS}
        onChange={handleOperationChange}
      />

      {operation === 'pivot' ? (
        <>
          <StringListSection
            id="pivotGroupByColumnsInput"
            testId="pivot-groupby-columns-section"
            title="Group By Columns"
            tooltip="Row identifier columns passed to df.groupBy(*groupByColumns) before calling .pivot()."
            placeholder="e.g. region or user_id, country"
            itemAriaPrefix="Group by column"
            items={groupByColumns}
            onItemsChange={(next) =>
              onChange({
                ...properties,
                operation: 'pivot',
                groupByColumns: next,
                pivotColumn,
                pivotValues,
                aggFunction,
                aggColumn,
              })
            }
          />

          <PropertyField
            id="pivotColumnInput"
            name="pivotColumn"
            label="Pivot Column"
            required="Required"
            tooltip="Categorical column whose distinct values become new output columns in .pivot(pivotColumn, values)."
            value={pivotColumn}
            onChange={handlePivotFieldChange}
            placeholder="quarter"
            invalid={isPivotColumnInvalid}
            error={isPivotColumnInvalid ? 'Pivot column is required.' : null}
          />

          <StringListSection
            id="pivotValuesInput"
            testId="pivot-values-section"
            title="Explicit Pivot Values"
            tooltip="Performance tip: supply explicit domain values for .pivot(pivotColumn, values) so Catalyst skips an extra distinct-values job scan across the dataset."
            placeholder="e.g. Q1, Q2, Q3, Q4"
            itemAriaPrefix="Pivot value"
            items={pivotValues}
            helperBanner={
              <div
                data-testid="pivot-values-perf-tip"
                className="flex items-start gap-2 p-2 rounded border border-[var(--vscode-border-light)] bg-[var(--vscode-card-bg)] text-xs text-[var(--vscode-text-muted)]"
              >
                <Sparkles className="w-4 h-4 text-amber-400 shrink-0 mt-0.5" />
                <span>
                  Pass explicit values whenever known so Spark Catalyst avoids running an eager
                  distinct-values scan before planning the aggregation.
                </span>
              </div>
            }
            onItemsChange={(next) =>
              onChange({
                ...properties,
                operation: 'pivot',
                groupByColumns,
                pivotColumn,
                pivotValues: next,
                aggFunction,
                aggColumn,
              })
            }
          />

          <div className="grid grid-cols-2 gap-2">
            <SelectField
              id="pivotAggFunction"
              name="aggFunction"
              label="Aggregate Function"
              tooltip="Aggregate function from pyspark.sql.functions applied inside .pivot(...).agg(...)."
              value={aggFunction}
              options={PIVOT_AGG_OPTIONS}
              onChange={handleAggFunctionChange}
            />

            <PropertyField
              id="pivotAggColumn"
              name="aggColumn"
              label="Aggregate Column"
              required={aggFunction !== 'count' ? 'Required' : undefined}
              tooltip="Numeric or measure column aggregated per pivoted value. Use * with count for row counts."
              value={aggColumn}
              onChange={handlePivotFieldChange}
              placeholder={aggFunction === 'count' ? '*' : 'revenue'}
              invalid={isAggColumnInvalid}
              error={isAggColumnInvalid ? 'Aggregate column is required.' : null}
            />
          </div>
        </>
      ) : (
        <>
          <StringListSection
            id="unpivotIdColumnsInput"
            testId="unpivot-id-columns-section"
            title="ID Columns"
            tooltip="Identifier columns passed as ids to df.unpivot(ids, values, ...) that remain intact on every output row."
            placeholder="e.g. region, product_line"
            itemAriaPrefix="ID column"
            items={idColumns}
            onItemsChange={(next) =>
              onChange({
                ...properties,
                operation: 'unpivot',
                idColumns: next,
                valueColumns,
                variableColumnName,
                valueColumnName,
              })
            }
          />

          <StringListSection
            id="unpivotValueColumnsInput"
            testId="unpivot-value-columns-section"
            title="Value Columns"
            tooltip="Columns rotated into tall rows via df.unpivot(ids, values, ...). If left empty, Spark unpivots all columns not listed in ID Columns."
            placeholder="e.g. Q1, Q2, Q3, Q4 (leave empty for all non-ID columns)"
            itemAriaPrefix="Value column"
            items={valueColumns}
            onItemsChange={(next) =>
              onChange({
                ...properties,
                operation: 'unpivot',
                idColumns,
                valueColumns: next,
                variableColumnName,
                valueColumnName,
              })
            }
          />

          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="unpivotVariableColumnName"
              name="variableColumnName"
              label="Variable Column Name"
              required="Required"
              tooltip="Name of the output column receiving the original column names (variableColumnName in df.unpivot)."
              value={variableColumnName}
              onChange={handleUnpivotFieldChange}
              placeholder="variable"
              invalid={isVariableColInvalid}
              error={isVariableColInvalid ? 'Variable column name is required.' : null}
            />

            <PropertyField
              id="unpivotValueColumnName"
              name="valueColumnName"
              label="Value Column Name"
              required="Required"
              tooltip="Name of the output column receiving the unpivoted cell values (valueColumnName in df.unpivot)."
              value={valueColumnName}
              onChange={handleUnpivotFieldChange}
              placeholder="value"
              invalid={isValueColInvalid}
              error={isValueColInvalid ? 'Value column name is required.' : null}
            />
          </div>
        </>
      )}

      <SystemInstructionsField
        id="pivotUnpivotSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this Pivot / Unpivot step."
        placeholder="Enter custom instructions for Pivot / Unpivot code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
