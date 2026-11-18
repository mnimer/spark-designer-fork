/**
 * @fileoverview Property configuration panel for the Apache Spark WithColumns step (df.withColumns).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { Layers, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Single derived or overwritten column definition for `df.withColumns({...})`.
 */
export interface WithColumnSpec {
  name: string;
  expression: string;
  castType?: string;
}

/**
 * Persisted properties for the Spark `WithColumns` step node.
 */
export interface WithColumnsStepProperties {
  columns?: WithColumnSpec[];
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link WithColumnsProperties}.
 */
export interface WithColumnsNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: WithColumnsStepProperties;
  };
}

/**
 * Props for the {@link WithColumnsProperties} component.
 */
export interface WithColumnsPropertiesProps {
  node: WithColumnsNode;
  onChange: (properties: WithColumnsStepProperties) => void;
}

const SPARK_CAST_TYPE_OPTIONS: Array<{ value: string; label: string }> = [
  { value: '', label: 'Inferred (No Explicit Cast)' },
  { value: 'STRING', label: 'STRING' },
  { value: 'BIGINT', label: 'BIGINT (LongType)' },
  { value: 'INT', label: 'INT (IntegerType)' },
  { value: 'DOUBLE', label: 'DOUBLE (DoubleType)' },
  { value: 'DECIMAL(18, 2)', label: 'DECIMAL(18, 2)' },
  { value: 'BOOLEAN', label: 'BOOLEAN' },
  { value: 'DATE', label: 'DATE' },
  { value: 'TIMESTAMP', label: 'TIMESTAMP' },
  { value: 'BINARY', label: 'BINARY' },
];

const DEFAULT_WITH_COLUMNS: WithColumnSpec[] = [{ name: '', expression: '', castType: '' }];

/**
 * Normalizes raw `columns` configuration into typed {@link WithColumnSpec} entries.
 *
 * @param raw Raw `columns` property value from YAML or UI state.
 * @returns Normalized array of derived column specifications.
 */
export function normalizeWithColumns(raw: unknown): WithColumnSpec[] {
  if (!Array.isArray(raw)) {
    return DEFAULT_WITH_COLUMNS.map((item) => ({ ...item }));
  }
  return raw.map((entry) => {
    const obj =
      typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    return {
      name: typeof obj.name === 'string' ? obj.name : '',
      expression: typeof obj.expression === 'string' ? obj.expression : '',
      castType: typeof obj.castType === 'string' ? obj.castType : '',
    };
  });
}

/**
 * Renders the property configuration panel for the Apache Spark WithColumns step.
 *
 * @param props Selected WithColumns node and property update callback.
 * @returns The rendered WithColumns configuration form.
 */
export const WithColumnsProperties: React.FC<WithColumnsPropertiesProps> = ({ node, onChange }) => {
  const properties: WithColumnsStepProperties = node?.data?.properties || {};
  const columns = normalizeWithColumns(properties.columns);
  const systemInstructions = resolveSystemInstructions(node);

  const commitColumns = (nextColumns: WithColumnSpec[]) => {
    onChange({
      ...properties,
      columns: nextColumns,
    });
  };

  const handleAddColumn = () => {
    commitColumns([...columns, { name: '', expression: '', castType: '' }]);
  };

  const handleRemoveColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    commitColumns(columns.filter((_, i) => i !== idx));
  };

  const handleFieldChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof WithColumnSpec | undefined;
    if (Number.isNaN(idx) || !field || !columns[idx]) {
      return;
    }
    const nextColumns = columns.map((item, i) =>
      i === idx ? { ...item, [field]: e.target.value } : item
    );
    commitColumns(nextColumns);
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      columns,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Catalyst Single-Pass Projection Banner */}
      <div
        data-testid="with-columns-catalyst-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <Layers className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Single-Pass Catalyst Projection</span>
          <InfoTooltip text="Spark 4.x compiles all column expressions into one df.withColumns({...}) dictionary call instead of chaining .withColumn() in a loop." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          All derived and cast columns execute together in a single{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">
            df.withColumns(&#123;...&#125;)
          </code>{' '}
          projection to avoid nested Catalyst plan trees.
        </p>
      </div>

      {/* 2. Derived Columns List */}
      <div
        data-testid="with-columns-list-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <span>Derived Columns</span>
            <InfoTooltip text="Map each target column name to a Catalyst SQL or F.expr() expression and optional Spark SQL cast type." />
          </div>
          <button
            type="button"
            onClick={handleAddColumn}
            className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add Column</span>
          </button>
        </div>

        {columns.length === 0 ? (
          <p
            role="alert"
            className="text-xs text-red-400 p-2 rounded border border-red-500/40 bg-red-500/10"
          >
            Add at least one derived column definition.
          </p>
        ) : (
          <div className="flex flex-col gap-3">
            {columns.map((col, index) => {
              const isNameEmpty = col.name.trim().length === 0;
              const isExpressionEmpty = col.expression.trim().length === 0;
              const currentCast = col.castType ?? '';
              const hasCustomCast =
                currentCast.length > 0 &&
                !SPARK_CAST_TYPE_OPTIONS.some((opt) => opt.value === currentCast);
              const castOptions = hasCustomCast
                ? [...SPARK_CAST_TYPE_OPTIONS, { value: currentCast, label: currentCast }]
                : SPARK_CAST_TYPE_OPTIONS;

              return (
                <div
                  key={index}
                  data-testid={`with-column-row-${index}`}
                  className="flex flex-col gap-2 p-2 rounded border border-[var(--vscode-border-light)] bg-[var(--vscode-card-bg)]"
                >
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
                      Column #{index + 1}
                    </span>
                    <button
                      type="button"
                      data-index={index}
                      aria-label={`Remove derived column ${index + 1}`}
                      onClick={handleRemoveColumn}
                      className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>

                  <div className="grid grid-cols-2 gap-2">
                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`withColName-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>Column Name</span>
                        <span className="text-red-400 font-bold">*</span>
                        <InfoTooltip text="New or existing column name written into the output DataFrame." />
                      </label>
                      <input
                        id={`withColName-${index}`}
                        type="text"
                        data-index={index}
                        data-field="name"
                        aria-label={`Derived column ${index + 1} name`}
                        aria-invalid={isNameEmpty}
                        value={col.name}
                        onChange={handleFieldChange}
                        placeholder="net_amount"
                        className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                          isNameEmpty
                            ? 'border-red-500/80 focus:border-red-500'
                            : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                        }`}
                      />
                    </div>

                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`withColCast-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>Cast Type</span>
                        <InfoTooltip text="Optional Spark SQL target data type applied via .cast(type) on the computed expression." />
                      </label>
                      <select
                        id={`withColCast-${index}`}
                        data-index={index}
                        data-field="castType"
                        aria-label={`Derived column ${index + 1} cast type`}
                        value={currentCast}
                        onChange={handleFieldChange}
                        className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                      >
                        {castOptions.map((opt) => (
                          <option key={opt.value} value={opt.value}>
                            {opt.label}
                          </option>
                        ))}
                      </select>
                    </div>
                  </div>

                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`withColExpr-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Catalyst Expression</span>
                      <span className="text-red-400 font-bold">*</span>
                      <InfoTooltip text="Catalyst SQL or Column expression evaluated via F.expr(), such as gross_amount - COALESCE(discount, 0)." />
                    </label>
                    <input
                      id={`withColExpr-${index}`}
                      type="text"
                      data-index={index}
                      data-field="expression"
                      aria-label={`Derived column ${index + 1} expression`}
                      aria-invalid={isExpressionEmpty}
                      value={col.expression}
                      onChange={handleFieldChange}
                      placeholder="gross_amount - COALESCE(discount_amount, 0)"
                      className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                        isExpressionEmpty
                          ? 'border-red-500/80 focus:border-red-500'
                          : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                      }`}
                    />
                  </div>

                  {(isNameEmpty || isExpressionEmpty) && (
                    <span className="text-xs text-red-400" role="alert">
                      {isNameEmpty && isExpressionEmpty
                        ? 'Column name and expression are required.'
                        : isNameEmpty
                          ? 'Column name is required.'
                          : 'Column expression is required.'}
                    </span>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      <SystemInstructionsField
        id="withColumnsSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this WithColumns step."
        placeholder="Enter custom instructions for WithColumns code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
