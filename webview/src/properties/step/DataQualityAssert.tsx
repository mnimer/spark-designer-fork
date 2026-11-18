/**
 * @fileoverview Property configuration panel for the Apache Spark DataQualityAssert step.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { Plus, ShieldCheck, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported vectorized expectation types evaluated by `DataQualityAssert`.
 */
export type DataQualityCheckType =
  | 'not_null'
  | 'range'
  | 'regex'
  | 'in_set'
  | 'unique'
  | 'sql_expr';

/**
 * Policy governing how Spark handles rows that violate one or more expectations.
 */
export type DataQualityFailurePolicy = 'route_to_dlq' | 'drop_invalid' | 'fail_job';

/**
 * Single data quality expectation stored in `properties.expectations`.
 */
export interface DataQualityExpectation {
  name: string;
  column: string;
  checkType: DataQualityCheckType;
  minValue?: string;
  maxValue?: string;
  regexPattern?: string;
  allowedValues?: string;
  sqlExpression?: string;
}

/**
 * Persisted properties for the Spark `DataQualityAssert` step node.
 */
export interface DataQualityAssertStepProperties {
  expectations?: DataQualityExpectation[];
  failurePolicy?: DataQualityFailurePolicy;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link DataQualityAssertProperties}.
 */
export interface DataQualityAssertNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: DataQualityAssertStepProperties;
  };
}

/**
 * Props for the {@link DataQualityAssertProperties} component.
 */
export interface DataQualityAssertPropertiesProps {
  node: DataQualityAssertNode;
  onChange: (properties: DataQualityAssertStepProperties) => void;
}

const CHECK_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'not_null', label: 'Not Null (col IS NOT NULL)' },
  { value: 'range', label: 'Numeric / Comparable Range (min .. max)' },
  { value: 'regex', label: 'Regex Match (rlike)' },
  { value: 'in_set', label: 'Allowed Set (isin)' },
  { value: 'unique', label: 'Unique Key (Window count == 1)' },
  { value: 'sql_expr', label: 'Custom SQL Predicate (F.expr)' },
];

const VALID_CHECK_TYPES = new Set<string>(CHECK_TYPE_OPTIONS.map((opt) => opt.value));

const FAILURE_POLICY_OPTIONS: SelectFieldOption[] = [
  { value: 'route_to_dlq', label: 'Route Violations to Error DLQ Handle' },
  { value: 'drop_invalid', label: 'Drop Invalid Rows Silently' },
  { value: 'fail_job', label: 'Fail Spark Job (F.assert_true)' },
];

const DEFAULT_EXPECTATIONS: DataQualityExpectation[] = [
  { name: 'not_null_check_1', column: '', checkType: 'not_null' },
];

/**
 * Normalizes raw `expectations` configuration into typed {@link DataQualityExpectation} items.
 *
 * @param raw Raw `expectations` value from YAML or UI state.
 * @returns Non-empty array of normalized expectations.
 */
export function normalizeExpectations(raw: unknown): DataQualityExpectation[] {
  if (!Array.isArray(raw) || raw.length === 0) {
    return DEFAULT_EXPECTATIONS.map((item) => ({ ...item }));
  }
  return raw.map((entry, idx) => {
    const obj = typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    const rawType =
      typeof obj.checkType === 'string' && VALID_CHECK_TYPES.has(obj.checkType)
        ? (obj.checkType as DataQualityCheckType)
        : 'not_null';
    const normalized: DataQualityExpectation = {
      name: typeof obj.name === 'string' ? obj.name : `expectation_${idx + 1}`,
      column: typeof obj.column === 'string' ? obj.column : '',
      checkType: rawType,
    };
    if (typeof obj.minValue === 'string') {
      normalized.minValue = obj.minValue;
    }
    if (typeof obj.maxValue === 'string') {
      normalized.maxValue = obj.maxValue;
    }
    if (typeof obj.regexPattern === 'string') {
      normalized.regexPattern = obj.regexPattern;
    }
    if (typeof obj.allowedValues === 'string') {
      normalized.allowedValues = obj.allowedValues;
    }
    if (typeof obj.sqlExpression === 'string') {
      normalized.sqlExpression = obj.sqlExpression;
    }
    return normalized;
  });
}

/**
 * Validates a single {@link DataQualityExpectation} and returns a descriptive error message when incomplete or inconsistent.
 *
 * @param exp Expectation entry to validate.
 * @returns Validation error message, or null when valid.
 */
export function validateExpectation(exp: DataQualityExpectation): string | null {
  if (exp.name.trim().length === 0) {
    return 'Expectation name is required.';
  }
  if (exp.checkType !== 'sql_expr' && exp.column.trim().length === 0) {
    return 'Target column is required for this check type.';
  }
  if (exp.checkType === 'range') {
    const minStr = (exp.minValue ?? '').trim();
    const maxStr = (exp.maxValue ?? '').trim();
    if (minStr === '' && maxStr === '') {
      return 'Specify at least a minimum value or a maximum value for a range check.';
    }
    const minNum = Number(minStr);
    const maxNum = Number(maxStr);
    if (minStr !== '' && maxStr !== '' && !Number.isNaN(minNum) && !Number.isNaN(maxNum) && minNum > maxNum) {
      return 'Minimum value cannot exceed maximum value.';
    }
  }
  if (exp.checkType === 'regex' && (exp.regexPattern ?? '').trim().length === 0) {
    return 'Regex pattern is required for a regex expectation.';
  }
  if (exp.checkType === 'in_set' && (exp.allowedValues ?? '').trim().length === 0) {
    return 'Allowed values list is required for an in_set expectation.';
  }
  if (exp.checkType === 'sql_expr' && (exp.sqlExpression ?? '').trim().length === 0) {
    return 'SQL boolean expression is required for a sql_expr expectation.';
  }
  return null;
}

/**
 * Renders the property configuration panel for the Apache Spark DataQualityAssert step.
 *
 * @param props Selected DataQualityAssert node and property update callback.
 * @returns The rendered DataQualityAssert configuration form.
 */
export const DataQualityAssertProperties: React.FC<DataQualityAssertPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: DataQualityAssertStepProperties = node?.data?.properties || {};
  const expectations = normalizeExpectations(properties.expectations);
  const failurePolicy: DataQualityFailurePolicy =
    properties.failurePolicy === 'drop_invalid' || properties.failurePolicy === 'fail_job'
      ? properties.failurePolicy
      : 'route_to_dlq';
  const systemInstructions = resolveSystemInstructions(node);

  const commitExpectations = (nextExpectations: DataQualityExpectation[]) => {
    onChange({
      ...properties,
      failurePolicy,
      expectations: nextExpectations,
    });
  };

  const handleFailurePolicyChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    onChange({
      ...properties,
      failurePolicy: e.target.value as DataQualityFailurePolicy,
      expectations,
    });
  };

  const handleAddExpectation = () => {
    const nextIdx = expectations.length + 1;
    commitExpectations([
      ...expectations,
      {
        name: `expectation_${nextIdx}`,
        column: '',
        checkType: 'not_null',
      },
    ]);
  };

  const handleRemoveExpectation = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx) || expectations.length <= 1) {
      return;
    }
    commitExpectations(expectations.filter((_, i) => i !== idx));
  };

  const handleExpectationFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>
  ) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof DataQualityExpectation | undefined;
    if (Number.isNaN(idx) || !field || !expectations[idx]) {
      return;
    }
    const nextValue = e.target.value;
    const nextExpectations = expectations.map((item, i) =>
      i === idx ? { ...item, [field]: nextValue } : item
    );
    commitExpectations(nextExpectations);
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Failure Policy & DLQ Banner */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <ShieldCheck className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Violation Handling Policy</span>
          <InfoTooltip text="Configure whether violated expectations route to the dead-letter error handle, drop silently, or abort the Spark job." />
        </div>

        <SelectField
          id="dqFailurePolicy"
          name="failurePolicy"
          label="Failure Policy"
          tooltip="Controls runtime behavior when one or more expectations evaluate to FALSE or NULL."
          value={failurePolicy}
          options={FAILURE_POLICY_OPTIONS}
          onChange={handleFailurePolicyChange}
        />

        <p
          data-testid="dq-dlq-guidance"
          className="text-xs text-[var(--vscode-text-muted)] leading-relaxed pt-2 border-t border-[var(--vscode-border)]"
        >
          {failurePolicy === 'route_to_dlq' &&
            'Connect the red error handle on the canvas to route failed rows with _failed_rules and _error columns to a dead-letter sink.'}
          {failurePolicy === 'drop_invalid' &&
            'Rows failing any expectation are filtered out in a single Catalyst pass.'}
          {failurePolicy === 'fail_job' &&
            'Raises a Spark assertion failure via F.assert_true() when any row violates an expectation.'}
        </p>
      </div>

      {/* 2. Expectations Cards */}
      <div
        data-testid="dq-expectations-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <span>Data Quality Expectations</span>
            <InfoTooltip text="Vectorized expectations evaluated together in a single Catalyst projection pass using F.array_compact()." />
          </div>
          <button
            type="button"
            onClick={handleAddExpectation}
            className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add Expectation</span>
          </button>
        </div>

        <div className="flex flex-col gap-3">
          {expectations.map((exp, index) => {
            const cardError = validateExpectation(exp);
            return (
              <div
                key={index}
                data-testid={`dq-expectation-card-${index}`}
                className="flex flex-col gap-2 p-2.5 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)]"
              >
                <div className="flex items-center justify-between gap-2">
                  <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
                    Expectation #{index + 1}
                  </span>
                  {expectations.length > 1 && (
                    <button
                      type="button"
                      data-index={index}
                      aria-label={`Remove expectation ${index + 1}`}
                      onClick={handleRemoveExpectation}
                      className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  )}
                </div>

                <div className="grid grid-cols-2 gap-2">
                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`dqExpName-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Rule Name</span>
                      <span className="text-red-400 font-bold" title="Required">*</span>
                      <InfoTooltip text="Identifier recorded in _failed_rules and _error when this expectation fails." />
                    </label>
                    <input
                      id={`dqExpName-${index}`}
                      type="text"
                      data-index={index}
                      data-field="name"
                      aria-label={`Expectation ${index + 1} name`}
                      value={exp.name}
                      onChange={handleExpectationFieldChange}
                      placeholder={`expectation_${index + 1}`}
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    />
                  </div>

                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`dqExpType-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Check Type</span>
                      <InfoTooltip text="Type of Catalyst expectation evaluated on each row." />
                    </label>
                    <select
                      id={`dqExpType-${index}`}
                      data-index={index}
                      data-field="checkType"
                      aria-label={`Expectation ${index + 1} check type`}
                      value={exp.checkType}
                      onChange={handleExpectationFieldChange}
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    >
                      {CHECK_TYPE_OPTIONS.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                <div className="flex flex-col gap-1">
                  <label
                    htmlFor={`dqExpCol-${index}`}
                    className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                  >
                    <span>Column</span>
                    {exp.checkType !== 'sql_expr' && (
                      <span className="text-red-400 font-bold" title="Required">*</span>
                    )}
                    <InfoTooltip text="Target DataFrame column inspected by this expectation. Optional when Check Type is Custom SQL Predicate." />
                  </label>
                  <input
                    id={`dqExpCol-${index}`}
                    type="text"
                    data-index={index}
                    data-field="column"
                    aria-label={`Expectation ${index + 1} column`}
                    value={exp.column}
                    onChange={handleExpectationFieldChange}
                    placeholder={exp.checkType === 'sql_expr' ? 'Optional primary column' : 'order_id'}
                    className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                  />
                </div>

                {exp.checkType === 'range' && (
                  <div className="grid grid-cols-2 gap-2">
                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`dqExpMin-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>Min Value</span>
                        <InfoTooltip text="Inclusive lower bound (col >= minValue). Leave empty for no lower bound." />
                      </label>
                      <input
                        id={`dqExpMin-${index}`}
                        type="text"
                        data-index={index}
                        data-field="minValue"
                        aria-label={`Expectation ${index + 1} min value`}
                        value={exp.minValue ?? ''}
                        onChange={handleExpectationFieldChange}
                        placeholder="0"
                        className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                      />
                    </div>

                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`dqExpMax-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>Max Value</span>
                        <InfoTooltip text="Inclusive upper bound (col <= maxValue). Leave empty for no upper bound." />
                      </label>
                      <input
                        id={`dqExpMax-${index}`}
                        type="text"
                        data-index={index}
                        data-field="maxValue"
                        aria-label={`Expectation ${index + 1} max value`}
                        value={exp.maxValue ?? ''}
                        onChange={handleExpectationFieldChange}
                        placeholder="100000"
                        className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                      />
                    </div>
                  </div>
                )}

                {exp.checkType === 'regex' && (
                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`dqExpRegex-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Regex Pattern</span>
                      <span className="text-red-400 font-bold" title="Required">*</span>
                      <InfoTooltip text="Java regular expression passed to col.cast('string').rlike(pattern)." />
                    </label>
                    <input
                      id={`dqExpRegex-${index}`}
                      type="text"
                      data-index={index}
                      data-field="regexPattern"
                      aria-label={`Expectation ${index + 1} regex pattern`}
                      value={exp.regexPattern ?? ''}
                      onChange={handleExpectationFieldChange}
                      placeholder="^[^@]+@[^@]+\.[^@]+$"
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    />
                  </div>
                )}

                {exp.checkType === 'in_set' && (
                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`dqExpAllowed-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Allowed Values</span>
                      <span className="text-red-400 font-bold" title="Required">*</span>
                      <InfoTooltip text="Comma-separated set of permitted values passed to col.isin([...])." />
                    </label>
                    <input
                      id={`dqExpAllowed-${index}`}
                      type="text"
                      data-index={index}
                      data-field="allowedValues"
                      aria-label={`Expectation ${index + 1} allowed values`}
                      value={exp.allowedValues ?? ''}
                      onChange={handleExpectationFieldChange}
                      placeholder="USD, EUR, GBP"
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    />
                  </div>
                )}

                {exp.checkType === 'sql_expr' && (
                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`dqExpSql-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>SQL Expression</span>
                      <span className="text-red-400 font-bold" title="Required">*</span>
                      <InfoTooltip text="Catalyst SQL boolean invariant passed to F.expr(), such as discount_amount <= gross_amount." />
                    </label>
                    <input
                      id={`dqExpSql-${index}`}
                      type="text"
                      data-index={index}
                      data-field="sqlExpression"
                      aria-label={`Expectation ${index + 1} SQL expression`}
                      value={exp.sqlExpression ?? ''}
                      onChange={handleExpectationFieldChange}
                      placeholder="discount_amount <= gross_amount"
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    />
                  </div>
                )}

                {exp.checkType === 'unique' && (
                  <p className="text-xs text-[var(--vscode-text-muted)]">
                    Evaluates batch uniqueness via <code className="font-mono text-[var(--vscode-text-primary)]">Window.partitionBy(col)</code>.
                  </p>
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

      <SystemInstructionsField
        id="dataQualityAssertSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this DataQualityAssert step."
        placeholder="Enter custom instructions for DataQualityAssert code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
