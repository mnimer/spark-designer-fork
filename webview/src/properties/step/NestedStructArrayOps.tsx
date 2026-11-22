/**
 * @fileoverview Property configuration panel for the Apache Spark NestedStructArrayOps step.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useState } from 'react';
import { Layers, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Nested data transformation mode executed without shuffling or exploding rows.
 */
export type NestedOperationMode = 'flatten_structs' | 'pack_struct' | 'array_higher_order';

/**
 * Supported Catalyst higher-order array functions in PySpark 4.x.
 */
export type HigherOrderArrayFunction =
  | 'transform'
  | 'filter'
  | 'exists'
  | 'forall'
  | 'aggregate';

/**
 * Single Catalyst higher-order array expression rule stored in `properties.arrayRules`.
 */
export interface HigherOrderArrayRule {
  sourceArrayColumn: string;
  targetColumn: string;
  functionType: HigherOrderArrayFunction;
  lambdaExpression: string;
  initialValueExpr?: string;
  finishLambdaExpr?: string;
}

/**
 * Persisted properties for the Spark `NestedStructArrayOps` step node.
 */
export interface NestedStructArrayOpsStepProperties {
  operationMode?: NestedOperationMode;
  structColumns?: string[];
  separator?: string;
  maxDepth?: number;
  dropOriginalStructs?: boolean;
  outputStructColumn?: string;
  sourceColumns?: string[];
  dropPackedSourceColumns?: boolean;
  arrayRules?: HigherOrderArrayRule[];
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link NestedStructArrayOpsProperties}.
 */
export interface NestedStructArrayOpsNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: NestedStructArrayOpsStepProperties;
  };
}

/**
 * Props for the {@link NestedStructArrayOpsProperties} component.
 */
export interface NestedStructArrayOpsPropertiesProps {
  node: NestedStructArrayOpsNode;
  onChange: (properties: NestedStructArrayOpsStepProperties) => void;
}

const OPERATION_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'flatten_structs', label: 'Flatten Nested Structs (Recursive df.select)' },
  { value: 'pack_struct', label: 'Pack Columns into Struct (F.struct)' },
  { value: 'array_higher_order', label: 'Higher-Order Array Functions (Zero-Shuffle HOF)' },
];

const VALID_OPERATION_MODES = new Set<string>(OPERATION_MODE_OPTIONS.map((opt) => opt.value));

const HOF_FUNCTION_OPTIONS: SelectFieldOption[] = [
  { value: 'transform', label: 'transform (Map Array Elements)' },
  { value: 'filter', label: 'filter (Keep Matching Elements)' },
  { value: 'exists', label: 'exists (Any Element Matches -> Boolean)' },
  { value: 'forall', label: 'forall (All Elements Match -> Boolean)' },
  { value: 'aggregate', label: 'aggregate (Fold Array into Scalar/Struct)' },
];

const VALID_HOF_FUNCTIONS = new Set<string>(HOF_FUNCTION_OPTIONS.map((opt) => opt.value));

const DEFAULT_ARRAY_RULES: HigherOrderArrayRule[] = [
  {
    sourceArrayColumn: '',
    targetColumn: '',
    functionType: 'transform',
    lambdaExpression: 'x -> x',
  },
];

/**
 * Normalizes raw `operationMode` input into a valid {@link NestedOperationMode}.
 *
 * @param raw Raw `operationMode` property value from YAML or UI state.
 * @returns Validated operation mode defaulting to `'flatten_structs'`.
 */
function normalizeOperationMode(raw: unknown): NestedOperationMode {
  if (typeof raw === 'string' && VALID_OPERATION_MODES.has(raw.trim())) {
    return raw.trim() as NestedOperationMode;
  }
  return 'flatten_structs';
}

/**
 * Normalizes raw column lists or comma-separated strings into trimmed names.
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
 * Normalizes raw `arrayRules` input into typed {@link HigherOrderArrayRule} items.
 *
 * @param raw Raw `arrayRules` property value from YAML or UI state.
 * @returns Non-empty array of normalized higher-order array rules.
 */
function normalizeArrayRules(raw: unknown): HigherOrderArrayRule[] {
  if (!Array.isArray(raw) || raw.length === 0) {
    return DEFAULT_ARRAY_RULES.map((item) => ({ ...item }));
  }
  return raw.map((entry) => {
    const obj =
      typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    const fnType =
      typeof obj.functionType === 'string' && VALID_HOF_FUNCTIONS.has(obj.functionType.trim())
        ? (obj.functionType.trim() as HigherOrderArrayFunction)
        : 'transform';
    const normalized: HigherOrderArrayRule = {
      sourceArrayColumn: typeof obj.sourceArrayColumn === 'string' ? obj.sourceArrayColumn : '',
      targetColumn: typeof obj.targetColumn === 'string' ? obj.targetColumn : '',
      functionType: fnType,
      lambdaExpression: typeof obj.lambdaExpression === 'string' ? obj.lambdaExpression : '',
    };
    if (typeof obj.initialValueExpr === 'string') {
      normalized.initialValueExpr = obj.initialValueExpr;
    }
    if (typeof obj.finishLambdaExpr === 'string') {
      normalized.finishLambdaExpr = obj.finishLambdaExpr;
    }
    return normalized;
  });
}

/**
 * Validates one {@link HigherOrderArrayRule} and returns an error message when incomplete.
 *
 * @param rule Higher-order array rule to validate.
 * @returns Validation message, or null when valid.
 */
function validateArrayRule(rule: HigherOrderArrayRule): string | null {
  if (rule.sourceArrayColumn.trim().length === 0) {
    return 'Source array column is required.';
  }
  if (rule.targetColumn.trim().length === 0) {
    return 'Target output column is required.';
  }
  if (rule.lambdaExpression.trim().length === 0) {
    return 'Lambda expression is required.';
  }
  if (rule.functionType === 'aggregate' && (rule.initialValueExpr ?? '').trim().length === 0) {
    return 'Initial accumulator expression is required for aggregate.';
  }
  return null;
}

/**
 * Renders the property configuration panel for the Apache Spark NestedStructArrayOps step.
 *
 * @param props Selected NestedStructArrayOps node and property update callback.
 * @returns The rendered NestedStructArrayOps configuration form.
 */
export const NestedStructArrayOpsProperties: React.FC<NestedStructArrayOpsPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: NestedStructArrayOpsStepProperties = node?.data?.properties || {};
  const operationMode = normalizeOperationMode(properties.operationMode);

  // Flatten Structs state
  const structColumns = normalizeColumnList(properties.structColumns);
  const separator = typeof properties.separator === 'string' ? properties.separator : '_';
  const rawMaxDepth = properties.maxDepth !== undefined ? Number(properties.maxDepth) : 3;
  const maxDepth = Number.isNaN(rawMaxDepth) ? 0 : rawMaxDepth;
  const dropOriginalStructs =
    typeof properties.dropOriginalStructs === 'boolean' ? properties.dropOriginalStructs : true;

  // Pack Struct state
  const outputStructColumn =
    typeof properties.outputStructColumn === 'string' ? properties.outputStructColumn : '';
  const sourceColumns = normalizeColumnList(properties.sourceColumns);
  const dropPackedSourceColumns =
    typeof properties.dropPackedSourceColumns === 'boolean'
      ? properties.dropPackedSourceColumns
      : false;

  // Higher-Order Array state
  const arrayRules = normalizeArrayRules(properties.arrayRules);
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingStructColInput, setPendingStructColInput] = useState<string>('');
  const [pendingPackColInput, setPendingPackColInput] = useState<string>('');

  const isSeparatorInvalid = operationMode === 'flatten_structs' && separator.length === 0;
  const isMaxDepthInvalid =
    operationMode === 'flatten_structs' && (!Number.isInteger(maxDepth) || maxDepth < 1);
  const isOutputStructInvalid =
    operationMode === 'pack_struct' && outputStructColumn.trim().length === 0;
  const isPackSourcesEmpty = operationMode === 'pack_struct' && sourceColumns.length === 0;

  const commitPatch = (patch: Partial<NestedStructArrayOpsStepProperties>) => {
    onChange({
      ...properties,
      operationMode,
      ...patch,
    });
  };

  const handleModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextMode = normalizeOperationMode(e.target.value);
    commitPatch({ operationMode: nextMode });
  };

  // --- Flatten Structs handlers ---
  const appendStructColumns = () => {
    const parsed = normalizeColumnList(pendingStructColInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(structColumns);
    const merged = [...structColumns];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    setPendingStructColInput('');
    commitPatch({
      structColumns: merged,
      separator,
      maxDepth,
      dropOriginalStructs,
    });
  };

  const handleStructColKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendStructColumns();
    }
  };

  const handleExistingStructColChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...structColumns];
    updated[idx] = e.target.value;
    commitPatch({
      structColumns: normalizeColumnList(updated),
      separator,
      maxDepth,
      dropOriginalStructs,
    });
  };

  const handleRemoveStructCol = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    commitPatch({
      structColumns: structColumns.filter((_, i) => i !== idx),
      separator,
      maxDepth,
      dropOriginalStructs,
    });
  };

  const handleSeparatorChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    commitPatch({
      structColumns,
      separator: e.target.value,
      maxDepth,
      dropOriginalStructs,
    });
  };

  const handleMaxDepthChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const parsed = e.target.value === '' ? 0 : Number(e.target.value);
    commitPatch({
      structColumns,
      separator,
      maxDepth: Number.isNaN(parsed) ? 0 : parsed,
      dropOriginalStructs,
    });
  };

  const handleDropOriginalStructsToggle = (e: React.ChangeEvent<HTMLInputElement>) => {
    commitPatch({
      structColumns,
      separator,
      maxDepth,
      dropOriginalStructs: e.target.checked,
    });
  };

  // --- Pack Struct handlers ---
  const handleOutputStructChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    commitPatch({
      outputStructColumn: e.target.value,
      sourceColumns,
      dropPackedSourceColumns,
    });
  };

  const appendPackColumns = () => {
    const parsed = normalizeColumnList(pendingPackColInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(sourceColumns);
    const merged = [...sourceColumns];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    setPendingPackColInput('');
    commitPatch({
      outputStructColumn,
      sourceColumns: merged,
      dropPackedSourceColumns,
    });
  };

  const handlePackColKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendPackColumns();
    }
  };

  const handleExistingPackColChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...sourceColumns];
    updated[idx] = e.target.value;
    commitPatch({
      outputStructColumn,
      sourceColumns: normalizeColumnList(updated),
      dropPackedSourceColumns,
    });
  };

  const handleRemovePackCol = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    commitPatch({
      outputStructColumn,
      sourceColumns: sourceColumns.filter((_, i) => i !== idx),
      dropPackedSourceColumns,
    });
  };

  const handleDropPackedToggle = (e: React.ChangeEvent<HTMLInputElement>) => {
    commitPatch({
      outputStructColumn,
      sourceColumns,
      dropPackedSourceColumns: e.target.checked,
    });
  };

  // --- Higher-Order Array handlers ---
  const commitArrayRules = (nextRules: HigherOrderArrayRule[]) => {
    commitPatch({ arrayRules: nextRules });
  };

  const handleAddArrayRule = () => {
    commitArrayRules([
      ...arrayRules,
      {
        sourceArrayColumn: '',
        targetColumn: '',
        functionType: 'transform',
        lambdaExpression: 'x -> x',
      },
    ]);
  };

  const handleRemoveArrayRule = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx) || arrayRules.length <= 1) {
      return;
    }
    commitArrayRules(arrayRules.filter((_, i) => i !== idx));
  };

  const handleArrayRuleFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>
  ) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof HigherOrderArrayRule | undefined;
    if (Number.isNaN(idx) || !field || !arrayRules[idx]) {
      return;
    }
    const nextValue = e.target.value;
    const nextRules = arrayRules.map((rule, i) => {
      if (i !== idx) {
        return rule;
      }
      if (field === 'functionType') {
        const nextFn = VALID_HOF_FUNCTIONS.has(nextValue)
          ? (nextValue as HigherOrderArrayFunction)
          : 'transform';
        return {
          ...rule,
          functionType: nextFn,
          ...(nextFn === 'aggregate' && rule.initialValueExpr === undefined
            ? { initialValueExpr: '' }
            : {}),
        };
      }
      return { ...rule, [field]: nextValue };
    });
    commitArrayRules(nextRules);
  };

  const handleSystemInstructionsChange = (value: string) => {
    commitPatch({ system_instructions: value });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Operation Mode Selector & Zero-Shuffle Guidance */}
      <div
        data-testid="nested-ops-banner"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <Layers className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Zero-Shuffle Nested Catalyst Operations</span>
          <InfoTooltip text="Transforms nested StructType and ArrayType columns in-place inside Tungsten without explode + groupBy(collect_list) shuffles." />
        </div>

        <SelectField
          id="nestedOperationMode"
          name="operationMode"
          label="Operation Mode"
          tooltip="Choose recursive StructType flattening, F.struct column packing, or Catalyst higher-order array functions."
          value={operationMode}
          options={OPERATION_MODE_OPTIONS}
          onChange={handleModeChange}
        />
      </div>

      {/* 2A. Flatten Structs Controls */}
      {operationMode === 'flatten_structs' && (
        <div
          data-testid="flatten-structs-section"
          className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="nestedFlattenSeparator"
              name="separator"
              label="Field Separator"
              required="Required"
              tooltip="Delimiter inserted between parent and child struct field names when generating flat column aliases."
              invalid={isSeparatorInvalid}
              error={isSeparatorInvalid ? 'Field separator is required.' : null}
              value={separator}
              onChange={handleSeparatorChange}
              placeholder="_"
            />

            <PropertyField
              id="nestedFlattenMaxDepth"
              name="maxDepth"
              type="number"
              min={1}
              step={1}
              label="Max Recursion Depth"
              required="Required"
              tooltip="Maximum nested StructType levels unpacked in a single df.select() pass. Must be at least 1."
              invalid={isMaxDepthInvalid}
              error={isMaxDepthInvalid ? 'Max depth must be an integer >= 1.' : null}
              value={maxDepth}
              onChange={handleMaxDepthChange}
              placeholder="3"
            />
          </div>

          <div className="flex flex-col gap-2">
            <label
              htmlFor="structColumnAddInput"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Struct Columns Filter (Optional)</span>
              <InfoTooltip text="Leave empty to flatten all top-level StructType columns, or specify only the struct columns to flatten." />
            </label>

            {structColumns.length > 0 && (
              <div className="flex flex-col gap-2" data-testid="struct-columns-list">
                {structColumns.map((col, index) => (
                  <div key={`${index}-${col}`} className="flex items-center gap-2">
                    <input
                      type="text"
                      data-index={index}
                      aria-label={`Struct column ${index + 1}`}
                      value={col}
                      onChange={handleExistingStructColChange}
                      className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    />
                    <button
                      type="button"
                      data-index={index}
                      aria-label={`Remove struct column ${col}`}
                      onClick={handleRemoveStructCol}
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
                id="structColumnAddInput"
                type="text"
                value={pendingStructColInput}
                onChange={(e) => setPendingStructColInput(e.target.value)}
                onKeyDown={handleStructColKeyDown}
                placeholder="e.g. customer, billing_address"
                className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
              />
              <button
                type="button"
                onClick={appendStructColumns}
                className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
              >
                <Plus className="w-3.5 h-3.5" />
                <span>Add Struct</span>
              </button>
            </div>
          </div>

          <label
            htmlFor="nestedDropOriginalStructs"
            className="flex items-center justify-between gap-2 pt-1 text-xs font-bold uppercase text-[var(--vscode-text-primary)] cursor-pointer"
          >
            <span className="flex items-center gap-1.5">
              <span>Drop Original Struct Columns</span>
              <InfoTooltip text="Omit the original top-level StructType columns after projecting their flattened child columns." />
            </span>
            <input
              id="nestedDropOriginalStructs"
              name="dropOriginalStructs"
              type="checkbox"
              checked={dropOriginalStructs}
              onChange={handleDropOriginalStructsToggle}
              className="accent-blue-500 w-4 h-4 cursor-pointer"
            />
          </label>
        </div>
      )}

      {/* 2B. Pack Struct Controls */}
      {operationMode === 'pack_struct' && (
        <div
          data-testid="pack-struct-section"
          className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <PropertyField
            id="nestedOutputStructColumn"
            name="outputStructColumn"
            label="Output Struct Column"
            required="Required"
            tooltip="Name of the new StructType column created via F.struct(...) inside df.withColumns()."
            invalid={isOutputStructInvalid}
            error={isOutputStructInvalid ? 'Output struct column name is required.' : null}
            value={outputStructColumn}
            onChange={handleOutputStructChange}
            placeholder="shipping_address"
          />

          <div className="flex flex-col gap-2">
            <label
              htmlFor="packSourceColumnAddInput"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Source Columns to Pack</span>
              <span className="text-red-400 font-bold" title="Required">*</span>
              <InfoTooltip text="One or more upstream columns packed as fields inside the target StructType column." />
            </label>

            {sourceColumns.length > 0 && (
              <div className="flex flex-col gap-2" data-testid="pack-source-columns-list">
                {sourceColumns.map((col, index) => (
                  <div key={`${index}-${col}`} className="flex items-center gap-2">
                    <input
                      type="text"
                      data-index={index}
                      aria-label={`Pack source column ${index + 1}`}
                      value={col}
                      onChange={handleExistingPackColChange}
                      className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    />
                    <button
                      type="button"
                      data-index={index}
                      aria-label={`Remove pack source column ${col}`}
                      onClick={handleRemovePackCol}
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
                id="packSourceColumnAddInput"
                type="text"
                value={pendingPackColInput}
                onChange={(e) => setPendingPackColInput(e.target.value)}
                onKeyDown={handlePackColKeyDown}
                placeholder="e.g. street, city, postal_code"
                className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
              />
              <button
                type="button"
                onClick={appendPackColumns}
                className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
              >
                <Plus className="w-3.5 h-3.5" />
                <span>Add Source</span>
              </button>
            </div>

            {isPackSourcesEmpty && (
              <span className="text-xs text-red-400" role="alert">
                At least one source column is required to pack a struct.
              </span>
            )}
          </div>

          <label
            htmlFor="nestedDropPackedSources"
            className="flex items-center justify-between gap-2 pt-1 text-xs font-bold uppercase text-[var(--vscode-text-primary)] cursor-pointer"
          >
            <span className="flex items-center gap-1.5">
              <span>Drop Packed Source Columns</span>
              <InfoTooltip text="Remove the packed top-level source columns after creating the nested struct column." />
            </span>
            <input
              id="nestedDropPackedSources"
              name="dropPackedSourceColumns"
              type="checkbox"
              checked={dropPackedSourceColumns}
              onChange={handleDropPackedToggle}
              className="accent-blue-500 w-4 h-4 cursor-pointer"
            />
          </label>
        </div>
      )}

      {/* 2C. Higher-Order Array Rules Builder */}
      {operationMode === 'array_higher_order' && (
        <div
          data-testid="array-hof-section"
          className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
              <span>Higher-Order Array Rules</span>
              <InfoTooltip text="Catalyst higher-order array expressions evaluated together in one df.withColumns() pass." />
            </div>
            <button
              type="button"
              onClick={handleAddArrayRule}
              className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
            >
              <Plus className="w-3.5 h-3.5" />
              <span>Add Array Rule</span>
            </button>
          </div>

          <div className="flex flex-col gap-3">
            {arrayRules.map((rule, index) => {
              const cardError = validateArrayRule(rule);
              return (
                <div
                  key={index}
                  data-testid={`array-hof-card-${index}`}
                  className="flex flex-col gap-2 p-2.5 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)]"
                >
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
                      Array Rule #{index + 1}
                    </span>
                    {arrayRules.length > 1 && (
                      <button
                        type="button"
                        data-index={index}
                        aria-label={`Remove array rule ${index + 1}`}
                        onClick={handleRemoveArrayRule}
                        className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    )}
                  </div>

                  <div className="grid grid-cols-2 gap-2">
                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`hofSourceCol-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>Source Array</span>
                        <span className="text-red-400 font-bold" title="Required">*</span>
                        <InfoTooltip text="Input ArrayType column passed as the first argument to the higher-order function." />
                      </label>
                      <input
                        id={`hofSourceCol-${index}`}
                        type="text"
                        data-index={index}
                        data-field="sourceArrayColumn"
                        aria-label={`Array rule ${index + 1} source column`}
                        value={rule.sourceArrayColumn}
                        onChange={handleArrayRuleFieldChange}
                        placeholder="items"
                        className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                      />
                    </div>

                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`hofTargetCol-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>Target Column</span>
                        <span className="text-red-400 font-bold" title="Required">*</span>
                        <InfoTooltip text="Output column name written inside df.withColumns({...})." />
                      </label>
                      <input
                        id={`hofTargetCol-${index}`}
                        type="text"
                        data-index={index}
                        data-field="targetColumn"
                        aria-label={`Array rule ${index + 1} target column`}
                        value={rule.targetColumn}
                        onChange={handleArrayRuleFieldChange}
                        placeholder="filtered_items"
                        className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                      />
                    </div>
                  </div>

                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`hofFunctionType-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Higher-Order Function</span>
                      <InfoTooltip text="Built-in Catalyst higher-order array function executed without row explosion." />
                    </label>
                    <select
                      id={`hofFunctionType-${index}`}
                      data-index={index}
                      data-field="functionType"
                      aria-label={`Array rule ${index + 1} function type`}
                      value={rule.functionType}
                      onChange={handleArrayRuleFieldChange}
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    >
                      {HOF_FUNCTION_OPTIONS.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`hofLambda-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>
                        {rule.functionType === 'aggregate'
                          ? 'Merge Lambda Expression'
                          : 'Lambda Expression'}
                      </span>
                      <span className="text-red-400 font-bold" title="Required">*</span>
                      <InfoTooltip text="Catalyst lambda expression such as x -> lower(trim(x)) or (acc, x) -> acc + x.amount." />
                    </label>
                    <input
                      id={`hofLambda-${index}`}
                      type="text"
                      data-index={index}
                      data-field="lambdaExpression"
                      aria-label={`Array rule ${index + 1} lambda expression`}
                      value={rule.lambdaExpression}
                      onChange={handleArrayRuleFieldChange}
                      placeholder={
                        rule.functionType === 'aggregate'
                          ? '(acc, x) -> acc + x.amount'
                          : 'x -> x IS NOT NULL'
                      }
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    />
                  </div>

                  {rule.functionType === 'aggregate' && (
                    <div className="grid grid-cols-2 gap-2">
                      <div className="flex flex-col gap-1">
                        <label
                          htmlFor={`hofInitVal-${index}`}
                          className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                        >
                          <span>Initial Value Expr</span>
                          <span className="text-red-400 font-bold" title="Required">*</span>
                          <InfoTooltip text="Zero/seed SQL expression for aggregate(), such as CAST(0.0 AS DOUBLE)." />
                        </label>
                        <input
                          id={`hofInitVal-${index}`}
                          type="text"
                          data-index={index}
                          data-field="initialValueExpr"
                          aria-label={`Array rule ${index + 1} initial value`}
                          value={rule.initialValueExpr ?? ''}
                          onChange={handleArrayRuleFieldChange}
                          placeholder="CAST(0.0 AS DOUBLE)"
                          className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                        />
                      </div>

                      <div className="flex flex-col gap-1">
                        <label
                          htmlFor={`hofFinishLambda-${index}`}
                          className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                        >
                          <span>Finish Lambda (Optional)</span>
                          <InfoTooltip text="Optional finalizer lambda applied to the accumulator, such as acc -> round(acc, 2)." />
                        </label>
                        <input
                          id={`hofFinishLambda-${index}`}
                          type="text"
                          data-index={index}
                          data-field="finishLambdaExpr"
                          aria-label={`Array rule ${index + 1} finish lambda`}
                          value={rule.finishLambdaExpr ?? ''}
                          onChange={handleArrayRuleFieldChange}
                          placeholder="acc -> round(acc, 2)"
                          className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                        />
                      </div>
                    </div>
                  )}

                  {cardError && (
                    <span className="text-xs text-red-400" role="alert">
                      {cardError}
                    </span>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      <SystemInstructionsField
        id="nestedStructArrayOpsSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this NestedStructArrayOps step."
        placeholder="Enter custom instructions for NestedStructArrayOps code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
