/**
 * @fileoverview Property configuration panel for the Apache Spark Select step (df.select / df.selectExpr).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { ArrowDown, ArrowUp, Columns, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Projection API mode for the Spark `Select` step.
 */
export type SelectMode = 'select' | 'selectExpr';

/**
 * Single column or SQL expression entry in the ordered `Select` projection list.
 */
export interface SelectColumnSpec {
  expression: string;
  alias?: string;
}

/**
 * Persisted properties for the Spark `Select` step node.
 */
export interface SelectStepProperties {
  mode?: SelectMode;
  columns?: SelectColumnSpec[];
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link SelectProperties}.
 */
export interface SelectNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: SelectStepProperties;
  };
}

/**
 * Props for the {@link SelectProperties} component.
 */
export interface SelectPropertiesProps {
  node: SelectNode;
  onChange: (properties: SelectStepProperties) => void;
}

const SELECT_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'select', label: 'Column Selection (df.select)' },
  { value: 'selectExpr', label: 'SQL Expressions (df.selectExpr)' },
];

const DEFAULT_SELECT_COLUMNS: SelectColumnSpec[] = [{ expression: '*', alias: '' }];

/**
 * Normalizes raw projection items into an ordered array of {@link SelectColumnSpec} entries.
 *
 * @param raw Raw `columns` property value from YAML or UI state.
 * @returns Ordered array of normalized projection items.
 */
export function normalizeSelectColumns(raw: unknown): SelectColumnSpec[] {
  if (!Array.isArray(raw)) {
    return DEFAULT_SELECT_COLUMNS.map((item) => ({ ...item }));
  }
  return raw.map((entry) => {
    if (typeof entry === 'string') {
      return { expression: entry, alias: '' };
    }
    const obj =
      typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    return {
      expression: typeof obj.expression === 'string' ? obj.expression : '',
      alias: typeof obj.alias === 'string' ? obj.alias : '',
    };
  });
}

/**
 * Renders the property configuration panel for the Apache Spark Select step.
 *
 * @param props Selected Select node and property update callback.
 * @returns The rendered Select step configuration form.
 */
export const SelectProperties: React.FC<SelectPropertiesProps> = ({ node, onChange }) => {
  const properties: SelectStepProperties = node?.data?.properties || {};
  const mode: SelectMode = properties.mode === 'selectExpr' ? 'selectExpr' : 'select';
  const columns = normalizeSelectColumns(properties.columns);
  const systemInstructions = resolveSystemInstructions(node);

  const commitChanges = (nextMode: SelectMode, nextColumns: SelectColumnSpec[]) => {
    onChange({
      ...properties,
      mode: nextMode,
      columns: nextColumns,
    });
  };

  const handleModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextMode: SelectMode = e.target.value === 'selectExpr' ? 'selectExpr' : 'select';
    commitChanges(nextMode, columns);
  };

  const handleAddColumn = () => {
    const nextColumns: SelectColumnSpec[] = [...columns, { expression: '', alias: '' }];
    commitChanges(mode, nextColumns);
  };

  const handleRemoveColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const nextColumns = columns.filter((_, i) => i !== idx);
    commitChanges(mode, nextColumns);
  };

  const handleMoveColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const direction = e.currentTarget.dataset.direction;
    if (Number.isNaN(idx)) {
      return;
    }
    const targetIdx = direction === 'up' ? idx - 1 : idx + 1;
    if (targetIdx < 0 || targetIdx >= columns.length) {
      return;
    }
    const nextColumns = [...columns];
    const [moved] = nextColumns.splice(idx, 1);
    nextColumns.splice(targetIdx, 0, moved);
    commitChanges(mode, nextColumns);
  };

  const handleColumnFieldChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof SelectColumnSpec | undefined;
    if (Number.isNaN(idx) || !field || !columns[idx]) {
      return;
    }
    const nextColumns = columns.map((item, i) =>
      i === idx ? { ...item, [field]: e.target.value } : item
    );
    commitChanges(mode, nextColumns);
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      mode,
      columns,
      system_instructions: value,
    });
  };

  const isSelectExpr = mode === 'selectExpr';

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Projection Mode */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <Columns className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Projection Mode</span>
          <InfoTooltip text="Choose df.select() for column names, struct paths, wildcard *, or backtick regexes, or choose df.selectExpr() for Catalyst SQL expressions." />
        </div>

        <SelectField
          id="selectMode"
          name="mode"
          label="Selection API"
          tooltip="Select df.select(*cols) for column references and regex patterns or df.selectExpr(*exprs) for SQL expressions."
          value={mode}
          options={SELECT_MODE_OPTIONS}
          onChange={handleModeChange}
        />
      </div>

      {/* 2. Ordered Projection List */}
      <div
        data-testid="select-columns-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <span>{isSelectExpr ? 'SQL Expressions' : 'Projected Columns'}</span>
            <InfoTooltip
              text={
                isSelectExpr
                  ? 'Ordered SQL projection strings passed to df.selectExpr(), such as UPPER(currency) or CAST(amount AS DOUBLE).'
                  : 'Ordered columns passed to df.select(). Supports column names, nested struct.*, wildcard *, and backtick regexes like `^metric_.*`.'
              }
            />
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
            Add at least one column or projection expression.
          </p>
        ) : (
          <div className="flex flex-col gap-3">
            {columns.map((col, index) => {
              const isExpressionEmpty = col.expression.trim().length === 0;
              const isWildcardOrRegex =
                col.expression.trim().endsWith('*') || col.expression.trim().startsWith('`');
              return (
                <div
                  key={index}
                  data-testid={`select-row-${index}`}
                  className="flex flex-col gap-2 p-2 rounded border border-[var(--vscode-border-light)] bg-[var(--vscode-card-bg)]"
                >
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
                      Column #{index + 1}
                    </span>
                    <div className="flex items-center gap-1">
                      <button
                        type="button"
                        data-index={index}
                        data-direction="up"
                        disabled={index === 0}
                        aria-label={`Move column ${index + 1} up`}
                        onClick={handleMoveColumn}
                        className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] disabled:opacity-40 disabled:cursor-not-allowed transition-colors cursor-pointer"
                      >
                        <ArrowUp className="w-3.5 h-3.5" />
                      </button>
                      <button
                        type="button"
                        data-index={index}
                        data-direction="down"
                        disabled={index === columns.length - 1}
                        aria-label={`Move column ${index + 1} down`}
                        onClick={handleMoveColumn}
                        className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] disabled:opacity-40 disabled:cursor-not-allowed transition-colors cursor-pointer"
                      >
                        <ArrowDown className="w-3.5 h-3.5" />
                      </button>
                      <button
                        type="button"
                        data-index={index}
                        aria-label={`Remove column ${index + 1}`}
                        onClick={handleRemoveColumn}
                        className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-2">
                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`selectExpression-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>{isSelectExpr ? 'SQL Expression' : 'Column / Pattern'}</span>
                        <span className="text-red-400 font-bold">*</span>
                        <InfoTooltip
                          text={
                            isSelectExpr
                              ? 'Catalyst SQL expression such as UPPER(country) or amount * 1.1.'
                              : 'Column name, struct field path (payload.id), wildcard (*), or backtick regex (`^raw_.*`).'
                          }
                        />
                      </label>
                      <input
                        id={`selectExpression-${index}`}
                        type="text"
                        data-index={index}
                        data-field="expression"
                        aria-label={`Column ${index + 1} expression`}
                        aria-invalid={isExpressionEmpty}
                        value={col.expression}
                        onChange={handleColumnFieldChange}
                        placeholder={isSelectExpr ? 'UPPER(currency)' : 'order_id or *'}
                        className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                          isExpressionEmpty
                            ? 'border-red-500/80 focus:border-red-500'
                            : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                        }`}
                      />
                    </div>

                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`selectAlias-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>Alias</span>
                        <InfoTooltip text="Optional output column name applied via .alias(name) in select mode or AS name in selectExpr mode." />
                      </label>
                      <input
                        id={`selectAlias-${index}`}
                        type="text"
                        data-index={index}
                        data-field="alias"
                        aria-label={`Column ${index + 1} alias`}
                        disabled={isWildcardOrRegex}
                        value={col.alias ?? ''}
                        onChange={handleColumnFieldChange}
                        placeholder={isWildcardOrRegex ? 'N/A for wildcard/regex' : 'optional_alias'}
                        className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)] disabled:opacity-50"
                      />
                    </div>
                  </div>

                  {isExpressionEmpty && (
                    <span className="text-xs text-red-400" role="alert">
                      Column or SQL expression is required.
                    </span>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      <SystemInstructionsField
        id="selectSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this Select step."
        placeholder="Enter custom instructions for Select code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
