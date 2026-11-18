/**
 * @fileoverview Property configuration panel for the Apache Spark SQL Query step (SparkSqlQuery).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React from 'react';
import { AlertTriangle, Code2, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Mapping between an upstream DataFrame input index and a temporary SQL view alias.
 */
export interface InputViewAliasMapping {
  /** Zero-based index of the upstream input edge. */
  inputIndex: number;
  /** Temporary view identifier passed to createOrReplaceTempView. */
  viewName: string;
}

/**
 * Named parameter binding passed to Spark 4.x parameterized spark.sql(query, args={...}).
 */
export interface SqlParameterBinding {
  /** Parameter identifier referenced as :key inside the SQL query. */
  key: string;
  /** Default literal or configuration expression bound to the named parameter. */
  value: string;
}

/**
 * Persisted properties for the SparkSqlQuery step node.
 */
export interface SparkSqlQueryStepProperties {
  inputViewAliases?: InputViewAliasMapping[];
  sqlQuery?: string;
  sqlParameters?: SqlParameterBinding[];
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link SparkSqlQueryProperties}.
 */
export interface SparkSqlQueryNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: SparkSqlQueryStepProperties;
  };
}

/**
 * Props for the {@link SparkSqlQueryProperties} component.
 */
export interface SparkSqlQueryPropertiesProps {
  node: SparkSqlQueryNode;
  onChange: (properties: SparkSqlQueryStepProperties) => void;
}

const VALID_SQL_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

const DEFAULT_VIEW_ALIASES: InputViewAliasMapping[] = [
  { inputIndex: 0, viewName: 'input_0' },
];

/**
 * Normalizes raw input view alias definitions from node properties.
 *
 * @param raw Raw inputViewAliases array from node state.
 * @returns Normalized array of input index to view alias mappings.
 */
function normalizeInputViewAliases(raw: unknown): InputViewAliasMapping[] {
  if (!Array.isArray(raw)) {
    return DEFAULT_VIEW_ALIASES.map((item) => ({ ...item }));
  }
  return raw.map((entry, idx) => {
    const obj = typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    const parsedIdx = Number(obj.inputIndex);
    const inputIndex = Number.isInteger(parsedIdx) && parsedIdx >= 0 ? parsedIdx : idx;
    const viewName = typeof obj.viewName === 'string' ? obj.viewName : '';
    return { inputIndex, viewName };
  });
}

/**
 * Normalizes raw Spark 4.x SQL named parameter bindings from node properties.
 *
 * @param raw Raw sqlParameters array from node state.
 * @returns Normalized array of key-value SQL parameter bindings.
 */
function normalizeSqlParameters(raw: unknown): SqlParameterBinding[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw.map((entry) => {
    const obj = typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    return {
      key: typeof obj.key === 'string' ? obj.key : '',
      value: typeof obj.value === 'string' ? obj.value : '',
    };
  });
}

/**
 * Checks whether a SQL view or parameter identifier matches safe Spark SQL identifier syntax.
 *
 * @param identifier Candidate view or parameter name.
 * @returns True when the trimmed identifier is non-empty and alphanumeric/underscore safe.
 */
function isValidIdentifier(identifier: string): boolean {
  const cleaned = identifier.trim().replace(/^:/, '');
  return cleaned.length > 0 && VALID_SQL_IDENTIFIER.test(cleaned);
}

/**
 * Renders the property configuration panel for the Apache Spark SQL Query step.
 *
 * @param props Selected SparkSqlQuery node and property update callback.
 * @returns The rendered SparkSqlQuery configuration form.
 */
export const SparkSqlQueryProperties: React.FC<SparkSqlQueryPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: SparkSqlQueryStepProperties = node?.data?.properties || {};
  const inputViewAliases = normalizeInputViewAliases(properties.inputViewAliases);
  const sqlQuery = typeof properties.sqlQuery === 'string' ? properties.sqlQuery : '';
  const sqlParameters = normalizeSqlParameters(properties.sqlParameters);
  const systemInstructions = resolveSystemInstructions(node);

  const isSqlQueryEmpty = sqlQuery.trim().length === 0;

  const handleAddViewAlias = () => {
    const nextIndex =
      inputViewAliases.length > 0
        ? Math.max(...inputViewAliases.map((item) => item.inputIndex)) + 1
        : 0;
    onChange({
      ...properties,
      inputViewAliases: [
        ...inputViewAliases,
        { inputIndex: nextIndex, viewName: `input_${nextIndex}` },
      ],
      sqlQuery,
      sqlParameters,
    });
  };

  const handleRemoveViewAlias = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx) || inputViewAliases.length <= 1) {
      return;
    }
    onChange({
      ...properties,
      inputViewAliases: inputViewAliases.filter((_, i) => i !== idx),
      sqlQuery,
      sqlParameters,
    });
  };

  const handleViewAliasFieldChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as 'inputIndex' | 'viewName' | undefined;
    if (Number.isNaN(idx) || !field || !inputViewAliases[idx]) {
      return;
    }
    const nextAliases = inputViewAliases.map((item, i) => {
      if (i !== idx) {
        return item;
      }
      if (field === 'inputIndex') {
        const parsed = Number(e.target.value);
        return {
          ...item,
          inputIndex: Number.isInteger(parsed) && parsed >= 0 ? parsed : 0,
        };
      }
      return { ...item, viewName: e.target.value };
    });
    onChange({
      ...properties,
      inputViewAliases: nextAliases,
      sqlQuery,
      sqlParameters,
    });
  };

  const handleSqlQueryChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    onChange({
      ...properties,
      inputViewAliases,
      sqlQuery: e.target.value,
      sqlParameters,
    });
  };

  const handleAddParameter = () => {
    onChange({
      ...properties,
      inputViewAliases,
      sqlQuery,
      sqlParameters: [...sqlParameters, { key: '', value: '' }],
    });
  };

  const handleRemoveParameter = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    onChange({
      ...properties,
      inputViewAliases,
      sqlQuery,
      sqlParameters: sqlParameters.filter((_, i) => i !== idx),
    });
  };

  const handleParameterFieldChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof SqlParameterBinding | undefined;
    if (Number.isNaN(idx) || !field || !sqlParameters[idx]) {
      return;
    }
    const rawValue = e.target.value;
    const normalizedVal = field === 'key' ? rawValue.replace(/^:+/, '') : rawValue;
    const nextParams = sqlParameters.map((item, i) =>
      i === idx ? { ...item, [field]: normalizedVal } : item
    );
    onChange({
      ...properties,
      inputViewAliases,
      sqlQuery,
      sqlParameters: nextParams,
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
      {/* 1. Upstream Temporary View Aliases */}
      <div
        data-testid="sql-view-aliases-section"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <Code2 className="w-4 h-4 text-blue-400 shrink-0" />
            <span>Upstream Temp Views</span>
            <InfoTooltip text="Maps each upstream input DataFrame index to a temporary view name registered via df.createOrReplaceTempView(viewName) before running spark.sql()." />
          </div>
          <button
            type="button"
            onClick={handleAddViewAlias}
            className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add View</span>
          </button>
        </div>

        <div className="flex flex-col gap-2">
          {inputViewAliases.map((mapping, index) => {
            const viewInvalid = !isValidIdentifier(mapping.viewName);
            return (
              <div
                key={index}
                data-testid={`view-alias-row-${index}`}
                className="flex flex-col gap-2 p-2 rounded border border-[var(--vscode-border-light)] bg-[var(--vscode-card-bg)]"
              >
                <div className="grid grid-cols-[96px_1fr_auto] items-end gap-2">
                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`sqlInputIdx-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Input #</span>
                      <InfoTooltip text="Zero-based upstream input edge index." />
                    </label>
                    <input
                      id={`sqlInputIdx-${index}`}
                      type="number"
                      min={0}
                      step={1}
                      data-index={index}
                      data-field="inputIndex"
                      aria-label={`View alias ${index + 1} input index`}
                      value={mapping.inputIndex}
                      onChange={handleViewAliasFieldChange}
                      className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    />
                  </div>

                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor={`sqlViewName-${index}`}
                      className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                    >
                      <span>Temp View Name</span>
                      <InfoTooltip text="Alphanumeric SQL view identifier registered via createOrReplaceTempView()." />
                    </label>
                    <input
                      id={`sqlViewName-${index}`}
                      type="text"
                      data-index={index}
                      data-field="viewName"
                      aria-label={`View alias ${index + 1} name`}
                      aria-invalid={viewInvalid}
                      value={mapping.viewName}
                      onChange={handleViewAliasFieldChange}
                      placeholder={`input_${mapping.inputIndex}`}
                      className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                        viewInvalid
                          ? 'border-red-500/80 focus:border-red-500'
                          : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                      }`}
                    />
                  </div>

                  <button
                    type="button"
                    data-index={index}
                    aria-label={`Remove view alias ${index + 1}`}
                    disabled={inputViewAliases.length <= 1}
                    onClick={handleRemoveViewAlias}
                    className="p-2 rounded border border-[var(--vscode-border)] text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>

                {viewInvalid && (
                  <span className="text-xs text-red-400" role="alert">
                    Temp view name must start with a letter or underscore and contain only letters, digits, or underscores.
                  </span>
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* 2. Catalyst SQL Query Editor */}
      <PropertyField
        id="sparkSqlQueryText"
        name="sqlQuery"
        label="SQL Query"
        type="textarea"
        rows={6}
        mono={true}
        required="Required"
        tooltip="Spark Catalyst SQL query executed via spark.sql(query, args={...}). Reference upstream views by alias and bind runtime values with :param_name markers."
        value={sqlQuery}
        onChange={handleSqlQueryChange}
        placeholder="SELECT * FROM input_0 WHERE status = :active_status"
        invalid={isSqlQueryEmpty}
        error={isSqlQueryEmpty ? 'SQL query is required.' : null}
      />

      {/* 3. Spark 4.x Parameterized SQL Bindings (:param) */}
      <div
        data-testid="sql-parameters-section"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <span>SQL Parameters (:param)</span>
            <InfoTooltip text="Named parameter bindings passed to Spark 4.x spark.sql(query, args={key: value}). Avoids unsafe string interpolation." />
          </div>
          <button
            type="button"
            onClick={handleAddParameter}
            className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add Parameter</span>
          </button>
        </div>

        {sqlParameters.length === 0 ? (
          <p className="text-xs text-[var(--vscode-text-muted)]">
            No named parameters configured. Add a parameter to bind <code className="font-mono">:param_name</code> markers safely via <code className="font-mono">args</code>.
          </p>
        ) : (
          <div className="flex flex-col gap-2">
            {sqlParameters.map((param, index) => {
              const keyInvalid = !isValidIdentifier(param.key);
              return (
                <div
                  key={index}
                  data-testid={`sql-param-row-${index}`}
                  className="flex flex-col gap-2 p-2 rounded border border-[var(--vscode-border-light)] bg-[var(--vscode-card-bg)]"
                >
                  <div className="grid grid-cols-[1fr_1fr_auto] items-end gap-2">
                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`sqlParamKey-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>Name</span>
                        <InfoTooltip text="Parameter name without leading colon, referenced as :name in SQL." />
                      </label>
                      <input
                        id={`sqlParamKey-${index}`}
                        type="text"
                        data-index={index}
                        data-field="key"
                        aria-label={`SQL parameter ${index + 1} name`}
                        aria-invalid={keyInvalid}
                        value={param.key}
                        onChange={handleParameterFieldChange}
                        placeholder="active_status"
                        className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                          keyInvalid
                            ? 'border-red-500/80 focus:border-red-500'
                            : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                        }`}
                      />
                    </div>

                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`sqlParamVal-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>Value</span>
                        <InfoTooltip text="Value or config reference bound to this SQL parameter." />
                      </label>
                      <input
                        id={`sqlParamVal-${index}`}
                        type="text"
                        data-index={index}
                        data-field="value"
                        aria-label={`SQL parameter ${index + 1} value`}
                        value={param.value}
                        onChange={handleParameterFieldChange}
                        placeholder="ACTIVE"
                        className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                      />
                    </div>

                    <button
                      type="button"
                      data-index={index}
                      aria-label={`Remove SQL parameter ${index + 1}`}
                      onClick={handleRemoveParameter}
                      className="p-2 rounded border border-[var(--vscode-border)] text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>

                  {keyInvalid && (
                    <span className="text-xs text-red-400" role="alert">
                      Parameter name must be a valid SQL identifier.
                    </span>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* 4. Dead-Letter Queue Guidance Banner */}
      <div
        data-testid="sql-dlq-guidance-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
          <span>Dead-Letter Queue (error Handle)</span>
          <InfoTooltip text="If the error output handle is wired on the canvas, rows producing a non-null _error column via TRY_CAST or error capture route to the error branch." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          If you connect the <code className="font-mono text-[var(--vscode-text-primary)]">error</code> output
          handle, use <code className="font-mono text-[var(--vscode-text-primary)]">TRY_CAST</code> or project an{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">_error</code> column so malformed rows split
          to dead-letter storage without aborting the job.
        </p>
      </div>

      <SystemInstructionsField
        id="sparkSqlQuerySystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this SparkSqlQuery step."
        placeholder="Enter custom instructions for SparkSqlQuery code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
