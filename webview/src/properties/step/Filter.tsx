/**
 * @fileoverview Property configuration panel for the Apache Spark Filter step (df.filter).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { AlertTriangle, Filter as FilterIcon } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Predicate authoring mode for the Spark `Filter` step.
 */
export type FilterMode = 'builder' | 'sql';

/**
 * Supported Spark column comparison and unary operators in builder mode.
 */
export type FilterOperator =
  | '=='
  | '!='
  | '>'
  | '>='
  | '<'
  | '<='
  | 'contains'
  | 'startswith'
  | 'endswith'
  | 'isin'
  | 'rlike'
  | 'is not null'
  | 'is null'
  | 'is not empty';

/**
 * Legacy comparison target retained for backwards compatibility with older saved graphs.
 */
export type LegacyCompareTarget = 'Column' | 'Message Property' | 'Header Property' | 'Element';

/**
 * Persisted properties for the Spark `Filter` step node.
 */
export interface FilterStepProperties {
  mode?: FilterMode;
  column?: string;
  property?: string;
  compareTarget?: LegacyCompareTarget;
  operator?: FilterOperator;
  value?: string;
  condition?: string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link FilterProperties}.
 */
export interface FilterNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: FilterStepProperties;
  };
}

/**
 * Props for the {@link FilterProperties} component.
 */
export interface FilterPropertiesProps {
  node: FilterNode;
  onChange: (properties: FilterStepProperties) => void;
}

const FILTER_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'builder', label: 'Column Predicate Builder (F.col)' },
  { value: 'sql', label: 'SQL / Catalyst Expression (df.filter)' },
];

const FILTER_OPERATOR_OPTIONS: SelectFieldOption[] = [
  { value: '==', label: '== (Equals)' },
  { value: '!=', label: '!= (Not Equals)' },
  { value: '>', label: '> (Greater Than)' },
  { value: '>=', label: '>= (Greater Than or Equal)' },
  { value: '<', label: '< (Less Than)' },
  { value: '<=', label: '<= (Less Than or Equal)' },
  { value: 'contains', label: 'contains (Substring)' },
  { value: 'startswith', label: 'startswith (Prefix)' },
  { value: 'endswith', label: 'endswith (Suffix)' },
  { value: 'isin', label: 'isin (Comma-Separated Set)' },
  { value: 'rlike', label: 'rlike (Regex Match)' },
  { value: 'is not null', label: 'is not null' },
  { value: 'is null', label: 'is null' },
  { value: 'is not empty', label: 'is not empty' },
];

const UNARY_OPERATORS = new Set<string>(['is not null', 'is null', 'is not empty']);

/**
 * Constructs a plain-English system instruction describing a filter predicate for backwards compatibility.
 *
 * @param compareTarget Target descriptor ('Column', 'Message Property', 'Header Property', or 'Element').
 * @param prop Column or property name to evaluate.
 * @param op Comparison or unary operator string.
 * @param val Comparison threshold or literal value string.
 * @returns Human-readable system instruction sentence.
 */
export const generateFilterSystemInstruction = (
  compareTarget: string,
  prop: string,
  op: string,
  val: string
): string => {
  if (compareTarget === 'Element') {
    if (op === 'is not null') {
      return 'Filter out elements that are null.';
    }
    if (op === 'is null') {
      return 'Keep elements that are null.';
    }
    if (op === 'is not empty') {
      return 'Filter out elements that are null or empty.';
    }
    if (val.trim()) {
      return `Filter elements where the element ${op} '${val.trim()}'.`;
    }
    return `Filter elements where the element ${op} value.`;
  }

  const trimmedProp = prop.trim();
  if (trimmedProp) {
    if (compareTarget === 'Column') {
      if (op === 'is not null') {
        return `Apply df.filter(F.col('${trimmedProp}').isNotNull()).`;
      }
      if (op === 'is null') {
        return `Apply df.filter(F.col('${trimmedProp}').isNull()).`;
      }
      if (op === 'is not empty') {
        return `Apply df.filter(F.col('${trimmedProp}').isNotNull() & (F.trim(F.col('${trimmedProp}').cast('string')) != '')).`;
      }
      if (val.trim()) {
        return `Apply df.filter() where column '${trimmedProp}' ${op} '${val.trim()}'.`;
      }
      return `Apply df.filter() where column '${trimmedProp}' ${op} value.`;
    }

    const target = compareTarget === 'Header Property' ? 'message header' : 'message property';
    if (op === 'is not null') {
      return `Filter elements where ${target} '${trimmedProp}' is not null.`;
    }
    if (op === 'is null') {
      return `Filter elements where ${target} '${trimmedProp}' is null.`;
    }
    if (op === 'is not empty') {
      return `Filter elements where ${target} '${trimmedProp}' is not null or empty.`;
    }
    if (val.trim()) {
      return `Filter elements where ${target} '${trimmedProp}' ${op} '${val.trim()}'.`;
    }
    return `Filter elements where ${target} '${trimmedProp}' ${op} value.`;
  }
  return 'Create a custom function with a todo comment and return true.';
};

/**
 * Builds a Spark-native system instruction when SQL expression mode is active.
 *
 * @param condition SQL boolean expression passed to `df.filter()`.
 * @returns Human-readable PySpark filter instruction string.
 */
export function buildSqlFilterSystemInstruction(condition: string): string {
  const trimmed = condition.trim();
  if (!trimmed) {
    return 'Apply df.filter() with a boolean SQL predicate expression.';
  }
  return `Apply df.filter(F.expr("${trimmed.replace(/"/g, '\\"')}")) and route negated or null rows to the error branch when connected.`;
}

/**
 * Renders the property configuration panel for the Apache Spark Filter step.
 *
 * @param props Selected Filter node and property update callback.
 * @returns The rendered Filter configuration form.
 */
export const FilterProperties: React.FC<FilterPropertiesProps> = ({ node, onChange }) => {
  const properties: FilterStepProperties = node?.data?.properties || {};
  const mode: FilterMode =
    properties.mode === 'sql' ||
    (properties.mode === undefined &&
      typeof properties.condition === 'string' &&
      properties.condition.trim().length > 0)
      ? 'sql'
      : 'builder';

  const rawColumn =
    typeof properties.column === 'string' && properties.column.length > 0
      ? properties.column
      : typeof properties.property === 'string'
        ? properties.property
        : '';
  const compareTarget: LegacyCompareTarget = properties.compareTarget || 'Column';
  const operator: FilterOperator = properties.operator || '==';
  const value = typeof properties.value === 'string' ? properties.value : '';
  const condition = typeof properties.condition === 'string' ? properties.condition : '';

  const isWholeElement = compareTarget === 'Element';
  const isUnaryOperator = UNARY_OPERATORS.has(operator);
  const isColumnInvalid = mode === 'builder' && !isWholeElement && rawColumn.trim().length === 0;
  const isConditionInvalid = mode === 'sql' && condition.trim().length === 0;
  const systemInstructions = resolveSystemInstructions(node);

  const handleModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextMode: FilterMode = e.target.value === 'sql' ? 'sql' : 'builder';
    const nextInstruction =
      nextMode === 'sql'
        ? buildSqlFilterSystemInstruction(condition)
        : generateFilterSystemInstruction(compareTarget, rawColumn, operator, value);
    onChange({
      ...properties,
      mode: nextMode,
      column: rawColumn,
      property: rawColumn,
      operator,
      value,
      condition,
      system_instructions: nextInstruction,
    });
  };

  const handleColumnChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const nextCol = e.target.value;
    const nextInstruction = generateFilterSystemInstruction(
      compareTarget,
      nextCol,
      operator,
      value
    );
    onChange({
      ...properties,
      mode,
      compareTarget,
      column: nextCol,
      property: nextCol,
      operator,
      value,
      system_instructions: nextInstruction,
    });
  };

  const handleOperatorChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextOp = e.target.value as FilterOperator;
    const nextInstruction = generateFilterSystemInstruction(
      compareTarget,
      rawColumn,
      nextOp,
      value
    );
    onChange({
      ...properties,
      mode,
      compareTarget,
      column: rawColumn,
      property: rawColumn,
      operator: nextOp,
      value,
      system_instructions: nextInstruction,
    });
  };

  const handleValueChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const nextVal = e.target.value;
    const nextInstruction = generateFilterSystemInstruction(
      compareTarget,
      rawColumn,
      operator,
      nextVal
    );
    onChange({
      ...properties,
      mode,
      compareTarget,
      column: rawColumn,
      property: rawColumn,
      operator,
      value: nextVal,
      system_instructions: nextInstruction,
    });
  };

  const handleConditionChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const nextCond = e.target.value;
    onChange({
      ...properties,
      mode: 'sql',
      condition: nextCond,
      system_instructions: buildSqlFilterSystemInstruction(nextCond),
    });
  };

  const handleSystemInstructionsChange = (nextValue: string) => {
    onChange({
      ...properties,
      system_instructions: nextValue,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Predicate Authoring Mode */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <FilterIcon className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Row Predicate</span>
          <InfoTooltip text="Configure a structured single-column F.col() comparison or write a full Catalyst SQL boolean expression passed to df.filter()." />
        </div>

        <SelectField
          id="filterModeSelect"
          name="mode"
          label="Filter Mode"
          tooltip="Choose Column Predicate Builder for single-column operators or SQL Expression for compound AND / OR Catalyst predicates."
          value={mode}
          options={FILTER_MODE_OPTIONS}
          onChange={handleModeChange}
        />

        {mode === 'builder' ? (
          <>
            {!isWholeElement && (
              <PropertyField
                id="filterPropertyInput"
                name="column"
                label="Column Name"
                required="Required"
                tooltip="Input DataFrame column or nested struct field path evaluated by F.col(column)."
                invalid={isColumnInvalid}
                error={isColumnInvalid ? 'Column name is required.' : null}
                value={rawColumn}
                onChange={handleColumnChange}
                placeholder="e.g. status or payload.amount"
                className="w-full"
              />
            )}

            <SelectField
              id="filterOperatorSelect"
              name="operator"
              label="Operator"
              tooltip="Catalyst comparison, string matching, set membership, regex, or null-check operator."
              value={operator}
              options={FILTER_OPERATOR_OPTIONS}
              onChange={handleOperatorChange}
            />

            {!isUnaryOperator && (
              <PropertyField
                id="filterValueInput"
                name="value"
                label="Target Value"
                tooltip={
                  operator === 'isin'
                    ? 'Comma-separated literal values passed to F.col(column).isin([...]).'
                    : operator === 'rlike'
                      ? 'Java / Spark regular expression matched via F.col(column).rlike(pattern).'
                      : 'Literal comparison value compared against F.col(column).'
                }
                value={value}
                onChange={handleValueChange}
                placeholder={
                  operator === 'isin'
                    ? 'US, CA, GB'
                    : operator === 'rlike'
                      ? '^ERR_[0-9]+$'
                      : 'ACTIVE'
                }
                className="w-full"
              />
            )}
          </>
        ) : (
          <PropertyField
            id="filterConditionInput"
            name="condition"
            type="textarea"
            rows={3}
            label="SQL / Column Predicate"
            required="Required"
            tooltip="Catalyst SQL boolean expression passed to df.filter(), such as status = 'ACTIVE' AND amount >= 100.0."
            invalid={isConditionInvalid}
            error={isConditionInvalid ? 'SQL predicate expression is required.' : null}
            value={condition}
            onChange={handleConditionChange}
            placeholder="status = 'ACTIVE' AND amount >= 100.0"
            className="w-full"
          />
        )}
      </div>

      {/* 2. Dead-Letter Queue (error handle) Guidance Banner */}
      <div
        data-testid="filter-dlq-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
          <span>Dead-Letter Routing (Error Handle)</span>
          <InfoTooltip text="When the error output handle is connected, Spark routes non-matching and null-predicate rows to the error branch without re-reading upstream sources." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          Matching rows continue from the primary output via{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">df.filter(cond)</code>. If
          you connect the <code className="font-mono text-[var(--vscode-text-primary)]">error</code>{' '}
          handle, rejected and null-predicate rows route to the dead-letter branch via{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">
            ~(cond) | (cond).isNull()
          </code>
          .
        </p>
      </div>

      {/* 3. Code Generation Instructions */}
      <SystemInstructionsField
        id="filterSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this Filter step."
        placeholder="Enter custom instructions for Filter code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
