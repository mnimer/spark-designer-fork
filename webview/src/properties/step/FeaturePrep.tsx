/**
 * @fileoverview Property configuration panel for the Apache Spark ML Feature Prep step (FeaturePrep).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useState } from 'react';
import { Plus, Sliders, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Numeric missing-value imputation strategy for `pyspark.ml.feature.Imputer`.
 */
export type FeatureImputeStrategy = 'none' | 'mean' | 'median' | 'mode';

/**
 * Vector scaling strategy applied after `VectorAssembler`.
 */
export type FeatureScalerType = 'none' | 'standard' | 'minmax';

/**
 * Invalid or unseen category/null handling policy for Spark ML feature transformers.
 */
export type FeatureHandleInvalid = 'keep' | 'skip' | 'error';

/**
 * Persisted properties for the FeaturePrep step node.
 */
export interface FeaturePrepStepProperties {
  categoricalColumns?: string[];
  numericColumns?: string[];
  enableOneHotEncoding?: boolean;
  imputeStrategy?: FeatureImputeStrategy;
  scalerType?: FeatureScalerType;
  handleInvalid?: FeatureHandleInvalid;
  outputFeaturesColumn?: string;
  pretrainedPipelineUri?: string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link FeaturePrepProperties}.
 */
export interface FeaturePrepNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: FeaturePrepStepProperties;
  };
}

/**
 * Props for the {@link FeaturePrepProperties} component.
 */
export interface FeaturePrepPropertiesProps {
  node: FeaturePrepNode;
  onChange: (properties: FeaturePrepStepProperties) => void;
}

const IMPUTE_STRATEGY_OPTIONS: SelectFieldOption[] = [
  { value: 'none', label: 'None (Skip Imputer)' },
  { value: 'mean', label: 'Mean (Imputer strategy="mean")' },
  { value: 'median', label: 'Median (Imputer strategy="median")' },
  { value: 'mode', label: 'Mode / Most Frequent (strategy="mode")' },
];

const SCALER_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'none', label: 'None (Raw Assembled Vector)' },
  { value: 'standard', label: 'StandardScaler (Unit StdDev)' },
  { value: 'minmax', label: 'MinMaxScaler (0 to 1 Range)' },
];

const HANDLE_INVALID_OPTIONS: SelectFieldOption[] = [
  { value: 'keep', label: 'keep (Assign Unseen / Null Bucket)' },
  { value: 'skip', label: 'skip (Filter Rows With Invalid Values)' },
  { value: 'error', label: 'error (Fail Fast on Unseen / Null)' },
];

/**
 * Normalizes raw column lists into trimmed non-empty column names.
 *
 * @param raw Raw column list or comma-separated string.
 * @returns Clean array of column identifiers.
 */
function normalizeFeatureColumns(raw: unknown): string[] {
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
 * Renders the property configuration panel for the Apache Spark FeaturePrep step.
 *
 * @param props Selected FeaturePrep node and property update callback.
 * @returns The rendered FeaturePrep configuration form.
 */
export const FeaturePrepProperties: React.FC<FeaturePrepPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: FeaturePrepStepProperties = node?.data?.properties || {};
  const categoricalColumns = normalizeFeatureColumns(properties.categoricalColumns);
  const numericColumns = normalizeFeatureColumns(properties.numericColumns);
  const enableOneHotEncoding =
    typeof properties.enableOneHotEncoding === 'boolean'
      ? properties.enableOneHotEncoding
      : true;
  const imputeStrategy: FeatureImputeStrategy =
    properties.imputeStrategy === 'mean' ||
    properties.imputeStrategy === 'median' ||
    properties.imputeStrategy === 'mode'
      ? properties.imputeStrategy
      : 'none';
  const scalerType: FeatureScalerType =
    properties.scalerType === 'standard' || properties.scalerType === 'minmax'
      ? properties.scalerType
      : 'none';
  const handleInvalid: FeatureHandleInvalid =
    properties.handleInvalid === 'skip' || properties.handleInvalid === 'error'
      ? properties.handleInvalid
      : 'keep';
  const outputFeaturesColumn =
    typeof properties.outputFeaturesColumn === 'string'
      ? properties.outputFeaturesColumn
      : 'features';
  const pretrainedPipelineUri =
    typeof properties.pretrainedPipelineUri === 'string'
      ? properties.pretrainedPipelineUri
      : '';
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingCategoricalInput, setPendingCategoricalInput] = useState<string>('');
  const [pendingNumericInput, setPendingNumericInput] = useState<string>('');

  const hasPretrainedUri = pretrainedPipelineUri.trim().length > 0;
  const isMissingFeatureColumns =
    !hasPretrainedUri && categoricalColumns.length === 0 && numericColumns.length === 0;
  const isOutputColInvalid = outputFeaturesColumn.trim().length === 0;

  const baseState = (): FeaturePrepStepProperties => ({
    ...properties,
    categoricalColumns,
    numericColumns,
    enableOneHotEncoding,
    imputeStrategy,
    scalerType,
    handleInvalid,
    outputFeaturesColumn,
    pretrainedPipelineUri,
  });

  const appendColumns = (
    kind: 'categoricalColumns' | 'numericColumns',
    pendingValue: string,
    clearInput: () => void
  ) => {
    const parsed = normalizeFeatureColumns(pendingValue);
    if (parsed.length === 0) {
      return;
    }
    const current = kind === 'categoricalColumns' ? categoricalColumns : numericColumns;
    const existing = new Set(current);
    const merged = [...current];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    clearInput();
    onChange({
      ...baseState(),
      [kind]: merged,
    });
  };

  const handleRemoveColumn = (
    kind: 'categoricalColumns' | 'numericColumns',
    index: number
  ) => {
    const current = kind === 'categoricalColumns' ? categoricalColumns : numericColumns;
    onChange({
      ...baseState(),
      [kind]: current.filter((_, i) => i !== index),
    });
  };

  const handleEditColumn = (
    kind: 'categoricalColumns' | 'numericColumns',
    index: number,
    value: string
  ) => {
    const current = kind === 'categoricalColumns' ? categoricalColumns : numericColumns;
    const updated = [...current];
    updated[index] = value;
    onChange({
      ...baseState(),
      [kind]: normalizeFeatureColumns(updated),
    });
  };

  const handleSelectChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const { name, value } = e.target;
    onChange({
      ...baseState(),
      [name]: value,
    });
  };

  const handleTextFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    onChange({
      ...baseState(),
      [name]: value,
    });
  };

  const handleOneHotToggle = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange({
      ...baseState(),
      enableOneHotEncoding: e.target.checked,
    });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...baseState(),
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. MLlib Feature Pipeline Overview Banner */}
      <div
        data-testid="feature-prep-overview-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <Sliders className="w-4 h-4 text-blue-400 shrink-0" />
          <span>PySpark MLlib Feature Pipeline</span>
          <InfoTooltip text="Builds a pyspark.ml.Pipeline chaining Imputer, StringIndexer, OneHotEncoder, VectorAssembler, and StandardScaler or MinMaxScaler, or loads a saved PipelineModel." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          Assembles categorical and numeric columns into a unified MLlib feature vector, or scores
          records directly through a saved <code className="font-mono text-[var(--vscode-text-primary)]">PipelineModel</code> URI.
        </p>
      </div>

      {/* 2. Categorical Columns Section */}
      <div className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <label
          htmlFor="featurePrepCategoricalAdd"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <span>Categorical Columns</span>
          <InfoTooltip text="String or discrete category columns encoded via StringIndexer and optional OneHotEncoder." />
        </label>

        {categoricalColumns.length > 0 && (
          <div className="flex flex-col gap-2" data-testid="categorical-columns-list">
            {categoricalColumns.map((col, index) => (
              <div key={`${index}-${col}`} className="flex items-center gap-2">
                <input
                  type="text"
                  aria-label={`Categorical column ${index + 1}`}
                  value={col}
                  onChange={(e) => handleEditColumn('categoricalColumns', index, e.target.value)}
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <button
                  type="button"
                  aria-label={`Remove categorical column ${col}`}
                  onClick={() => handleRemoveColumn('categoricalColumns', index)}
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
            id="featurePrepCategoricalAdd"
            type="text"
            value={pendingCategoricalInput}
            onChange={(e) => setPendingCategoricalInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                appendColumns('categoricalColumns', pendingCategoricalInput, () =>
                  setPendingCategoricalInput('')
                );
              }
            }}
            placeholder="e.g. region, device_type"
            className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
          />
          <button
            type="button"
            aria-label="Add categorical column"
            onClick={() =>
              appendColumns('categoricalColumns', pendingCategoricalInput, () =>
                setPendingCategoricalInput('')
              )
            }
            className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add</span>
          </button>
        </div>

        <div className="flex items-center justify-between gap-2 pt-2 border-t border-[var(--vscode-border)]">
          <label
            htmlFor="featurePrepOneHotToggle"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)] cursor-pointer"
          >
            <span>Enable One-Hot Encoding</span>
            <InfoTooltip text="Chains OneHotEncoder after StringIndexer so categorical indices expand into sparse binary vectors." />
          </label>
          <input
            id="featurePrepOneHotToggle"
            name="enableOneHotEncoding"
            type="checkbox"
            checked={enableOneHotEncoding}
            onChange={handleOneHotToggle}
            className="w-4 h-4 accent-blue-500 cursor-pointer"
          />
        </div>
      </div>

      {/* 3. Numeric Columns Section */}
      <div className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <label
          htmlFor="featurePrepNumericAdd"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <span>Numeric Columns</span>
          <InfoTooltip text="Continuous or integer columns optionally imputed with Imputer and combined via VectorAssembler." />
        </label>

        {numericColumns.length > 0 && (
          <div className="flex flex-col gap-2" data-testid="numeric-columns-list">
            {numericColumns.map((col, index) => (
              <div key={`${index}-${col}`} className="flex items-center gap-2">
                <input
                  type="text"
                  aria-label={`Numeric column ${index + 1}`}
                  value={col}
                  onChange={(e) => handleEditColumn('numericColumns', index, e.target.value)}
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <button
                  type="button"
                  aria-label={`Remove numeric column ${col}`}
                  onClick={() => handleRemoveColumn('numericColumns', index)}
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
            id="featurePrepNumericAdd"
            type="text"
            value={pendingNumericInput}
            onChange={(e) => setPendingNumericInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                appendColumns('numericColumns', pendingNumericInput, () =>
                  setPendingNumericInput('')
                );
              }
            }}
            placeholder="e.g. age, total_spend"
            className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
          />
          <button
            type="button"
            aria-label="Add numeric column"
            onClick={() =>
              appendColumns('numericColumns', pendingNumericInput, () =>
                setPendingNumericInput('')
              )
            }
            className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add</span>
          </button>
        </div>

        {isMissingFeatureColumns && (
          <span className="text-xs text-red-400" role="alert">
            Configure at least one categorical or numeric column, or specify a pre-trained PipelineModel URI.
          </span>
        )}
      </div>

      {/* 4. Imputation, Scaling & Invalid Handling */}
      <SelectField
        id="featurePrepImputeStrategy"
        name="imputeStrategy"
        label="Numeric Imputation Strategy"
        tooltip="Strategy passed to pyspark.ml.feature.Imputer for missing or NaN numeric values."
        value={imputeStrategy}
        options={IMPUTE_STRATEGY_OPTIONS}
        onChange={handleSelectChange}
      />

      <SelectField
        id="featurePrepScalerType"
        name="scalerType"
        label="Feature Scaler"
        tooltip="Optional scaling stage appended after VectorAssembler (StandardScaler with withMean=False or MinMaxScaler)."
        value={scalerType}
        options={SCALER_TYPE_OPTIONS}
        onChange={handleSelectChange}
      />

      <SelectField
        id="featurePrepHandleInvalid"
        name="handleInvalid"
        label="Invalid / Unseen Value Policy"
        tooltip="Policy passed to StringIndexer, OneHotEncoder, and VectorAssembler when encountering unseen categories or nulls."
        value={handleInvalid}
        options={HANDLE_INVALID_OPTIONS}
        onChange={handleSelectChange}
      />

      {/* 5. Output Vector Column & Pre-Trained PipelineModel URI */}
      <PropertyField
        id="featurePrepOutputColumn"
        name="outputFeaturesColumn"
        label="Output Features Column"
        required="Required"
        tooltip="Name of the output MLlib Vector column produced by VectorAssembler or the feature scaler."
        value={outputFeaturesColumn}
        onChange={handleTextFieldChange}
        placeholder="features"
        invalid={isOutputColInvalid}
        error={isOutputColInvalid ? 'Output features column is required.' : null}
      />

      <PropertyField
        id="featurePrepPretrainedUri"
        name="pretrainedPipelineUri"
        label="Pre-Trained PipelineModel URI (Optional)"
        tooltip="Optional Cloud Storage or filesystem URI loaded via PipelineModel.load(uri) instead of fitting stages dynamically."
        value={pretrainedPipelineUri}
        onChange={handleTextFieldChange}
        placeholder="gs://my-ml-bucket/pipelines/customer-features-v1"
        helperText="When set, loads the pre-fitted PipelineModel directly for consistent production scoring."
      />

      <SystemInstructionsField
        id="featurePrepSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this FeaturePrep step."
        placeholder="Enter custom instructions for feature engineering stages..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
