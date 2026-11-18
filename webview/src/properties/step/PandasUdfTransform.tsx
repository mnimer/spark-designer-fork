/**
 * @fileoverview Property configuration panel for the Apache Spark Pandas & Arrow UDF step (PandasUdfTransform).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useState } from 'react';
import { AlertTriangle, Cpu, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Vectorized Apache Arrow execution strategy for {@link PandasUdfTransformProperties}.
 */
export type PandasUdfExecutionMode = 'pandas_udf' | 'mapInPandas' | 'mapInArrow';

/**
 * Persisted properties for the PandasUdfTransform step node.
 */
export interface PandasUdfTransformStepProperties {
  udfMode?: PandasUdfExecutionMode;
  functionName?: string;
  inputColumns?: string[];
  outputColumn?: string;
  returnSchemaDDL?: string;
  maxRecordsPerBatch?: number | string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link PandasUdfTransformProperties}.
 */
export interface PandasUdfTransformNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: PandasUdfTransformStepProperties;
  };
}

/**
 * Props for the {@link PandasUdfTransformProperties} component.
 */
export interface PandasUdfTransformPropertiesProps {
  node: PandasUdfTransformNode;
  onChange: (properties: PandasUdfTransformStepProperties) => void;
}

const UDF_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'pandas_udf', label: '@pandas_udf (Series -> Series)' },
  { value: 'mapInPandas', label: 'df.mapInPandas (Iterator[DataFrame])' },
  { value: 'mapInArrow', label: 'df.mapInArrow (Zero-Copy Arrow Batch)' },
];

const VALID_PYTHON_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Normalizes raw input column definitions into a clean string array.
 *
 * @param raw Raw inputColumns value from node properties or user input.
 * @returns Trimmed non-empty column names.
 */
function normalizeColumns(raw: unknown): string[] {
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
 * Renders the property configuration panel for the Apache Spark Pandas & Arrow UDF step.
 *
 * @param props Selected PandasUdfTransform node and property update callback.
 * @returns The rendered PandasUdfTransform configuration form.
 */
export const PandasUdfTransformProperties: React.FC<PandasUdfTransformPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: PandasUdfTransformStepProperties = node?.data?.properties || {};
  const udfMode: PandasUdfExecutionMode =
    properties.udfMode === 'mapInPandas' || properties.udfMode === 'mapInArrow'
      ? properties.udfMode
      : 'pandas_udf';
  const functionName =
    typeof properties.functionName === 'string'
      ? properties.functionName
      : 'vectorized_transform';
  const inputColumns = normalizeColumns(properties.inputColumns);
  const outputColumn = typeof properties.outputColumn === 'string' ? properties.outputColumn : '';
  const returnSchemaDDL =
    typeof properties.returnSchemaDDL === 'string' ? properties.returnSchemaDDL : '';
  const rawMaxBatch = properties.maxRecordsPerBatch;
  const maxRecordsText =
    rawMaxBatch === undefined || rawMaxBatch === null ? '' : String(rawMaxBatch);
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingColumnInput, setPendingColumnInput] = useState<string>('');

  const isPandasUdfMode = udfMode === 'pandas_udf';
  const isFunctionNameInvalid =
    functionName.trim().length === 0 || !VALID_PYTHON_IDENT.test(functionName.trim());
  const isOutputColumnInvalid = isPandasUdfMode && outputColumn.trim().length === 0;
  const isReturnSchemaInvalid = returnSchemaDDL.trim().length === 0;
  const parsedMaxBatch = Number(maxRecordsText.trim());
  const isMaxRecordsInvalid =
    maxRecordsText.trim().length > 0 &&
    (!Number.isInteger(parsedMaxBatch) || parsedMaxBatch <= 0);

  const handleUdfModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    onChange({
      ...properties,
      udfMode: e.target.value as PandasUdfExecutionMode,
      functionName,
      inputColumns,
      returnSchemaDDL,
    });
  };

  const handleFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    if (name === 'maxRecordsPerBatch') {
      const trimmed = value.trim();
      const numeric = Number(trimmed);
      onChange({
        ...properties,
        udfMode,
        functionName,
        inputColumns,
        returnSchemaDDL,
        maxRecordsPerBatch:
          trimmed === ''
            ? undefined
            : Number.isInteger(numeric) && numeric > 0
              ? numeric
              : value,
      });
      return;
    }
    onChange({
      ...properties,
      udfMode,
      functionName,
      inputColumns,
      returnSchemaDDL,
      [name]: value,
    });
  };

  const appendPendingColumns = () => {
    const parsed = normalizeColumns(pendingColumnInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(inputColumns);
    const merged = [...inputColumns];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    setPendingColumnInput('');
    onChange({
      ...properties,
      udfMode,
      functionName,
      inputColumns: merged,
      returnSchemaDDL,
    });
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
    const updated = [...inputColumns];
    updated[idx] = e.target.value;
    onChange({
      ...properties,
      udfMode,
      functionName,
      inputColumns: normalizeColumns(updated),
      returnSchemaDDL,
    });
  };

  const handleRemoveColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    onChange({
      ...properties,
      udfMode,
      functionName,
      inputColumns: inputColumns.filter((_, i) => i !== idx),
      returnSchemaDDL,
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
      {/* 1. Arrow Vectorization Guidance Banner */}
      <div
        data-testid="pandas-udf-arrow-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <Cpu className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Apache Arrow Vectorized Execution</span>
          <InfoTooltip text="Executes column batches via Apache Arrow instead of row-by-row pickled @udf, adding pandas and pyarrow to requirements.txt." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          Transfers columnar batches via Apache Arrow without row-by-row Python pickling overhead
          and includes <code className="font-mono text-[var(--vscode-text-primary)]">pandas</code> and{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">pyarrow</code> in generated
          dependencies.
        </p>
      </div>

      {/* 2. Mode & Function Name */}
      <SelectField
        id="pandasUdfMode"
        name="udfMode"
        label="UDF Execution Mode"
        tooltip="Choose @pandas_udf for Series column projection, mapInPandas for Pandas DataFrame batches, or mapInArrow for zero-copy Arrow RecordBatch transforms."
        value={udfMode}
        options={UDF_MODE_OPTIONS}
        onChange={handleUdfModeChange}
      />

      <PropertyField
        id="pandasUdfFunctionName"
        name="functionName"
        label="Python Function Name"
        required="Required"
        tooltip="Python identifier generated for the vectorized Arrow transformation function."
        value={functionName}
        onChange={handleFieldChange}
        placeholder="vectorized_transform"
        invalid={isFunctionNameInvalid}
        error={
          isFunctionNameInvalid
            ? 'Function name must be a valid Python identifier.'
            : null
        }
      />

      {/* 3. Input Columns List */}
      <div className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <label
          htmlFor="pandasUdfInputColAdd"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <span>Input Columns</span>
          <InfoTooltip text="Upstream DataFrame columns passed as arguments to @pandas_udf or read inside mapInPandas and mapInArrow." />
        </label>

        {inputColumns.length > 0 && (
          <div className="flex flex-col gap-2" data-testid="pandas-udf-input-columns">
            {inputColumns.map((col, index) => (
              <div key={`${index}-${col}`} className="flex items-center gap-2">
                <input
                  type="text"
                  data-index={index}
                  aria-label={`Input column ${index + 1}`}
                  value={col}
                  onChange={handleExistingColumnChange}
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <button
                  type="button"
                  data-index={index}
                  aria-label={`Remove input column ${col}`}
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
            id="pandasUdfInputColAdd"
            type="text"
            value={pendingColumnInput}
            onChange={(e) => setPendingColumnInput(e.target.value)}
            onKeyDown={handlePendingColumnKeyDown}
            placeholder="e.g. amount or price, quantity"
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

      {/* 4. Output Column (pandas_udf only) & Return Schema DDL */}
      {isPandasUdfMode && (
        <PropertyField
          id="pandasUdfOutputColumn"
          name="outputColumn"
          label="Output Column"
          required="Required"
          tooltip="Destination column name assigned via df.withColumn(outputColumn, udf(...)) when udfMode is pandas_udf."
          value={outputColumn}
          onChange={handleFieldChange}
          placeholder="normalized_score"
          invalid={isOutputColumnInvalid}
          error={
            isOutputColumnInvalid
              ? 'Output column is required for @pandas_udf mode.'
              : null
          }
        />
      )}

      <PropertyField
        id="pandasUdfReturnSchema"
        name="returnSchemaDDL"
        label="Return Schema (Spark DDL)"
        required="Required"
        tooltip={
          isPandasUdfMode
            ? 'Spark SQL return type string for @pandas_udf, such as double or struct<score: double, tag: string>.'
            : 'Full output DataFrame DDL schema passed to mapInPandas or mapInArrow, such as id string, score double.'
        }
        value={returnSchemaDDL}
        onChange={handleFieldChange}
        placeholder={isPandasUdfMode ? 'double' : 'id string, normalized_score double'}
        invalid={isReturnSchemaInvalid}
        error={
          isReturnSchemaInvalid
            ? 'Return schema DDL is required.'
            : null
        }
      />

      {/* 5. Optional Arrow Batch Size */}
      <PropertyField
        id="pandasUdfMaxRecordsPerBatch"
        name="maxRecordsPerBatch"
        label="Max Records Per Arrow Batch"
        type="number"
        min={1}
        step={1}
        tooltip="Optional positive integer overriding spark.sql.execution.arrow.maxRecordsPerBatch (Spark default is 10000)."
        value={maxRecordsText}
        onChange={handleFieldChange}
        placeholder="10000"
        invalid={isMaxRecordsInvalid}
        error={
          isMaxRecordsInvalid
            ? 'Max records per batch must be a positive integer.'
            : null
        }
      />

      {/* 6. DLQ Guidance Banner */}
      <div
        data-testid="pandas-udf-dlq-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
          <span>Dead-Letter Queue (error Handle)</span>
          <InfoTooltip text="When the error handle is connected, batch exceptions populate a nullable _error column so malformed rows route to dead-letter storage." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          If you connect the <code className="font-mono text-[var(--vscode-text-primary)]">error</code> handle,
          generated code wraps vectorized execution in <code className="font-mono text-[var(--vscode-text-primary)]">try/except</code>{' '}
          and captures failures into <code className="font-mono text-[var(--vscode-text-primary)]">_error</code>.
        </p>
      </div>

      <SystemInstructionsField
        id="pandasUdfSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this PandasUdfTransform step."
        placeholder="Describe the vectorized Pandas or Arrow transformation logic..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
