/**
 * @fileoverview Property configuration panel for the Apache Spark ML & Vertex AI step (SparkMlAndVertexAi).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useState } from 'react';
import { AlertTriangle, BrainCircuit, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Distributed ML or Vertex AI inference engine for {@link SparkMlAndVertexAiProperties}.
 */
export type MlInferenceEngine =
  | 'spark_ml'
  | 'xgboost'
  | 'vertex_ai_endpoint'
  | 'vertex_ai_gemini';

/**
 * Supervised learning task type for `xgboost.spark` models.
 */
export type XgboostTaskType = 'classification' | 'regression';

/**
 * Persisted properties for the SparkMlAndVertexAi step node.
 */
export interface SparkMlAndVertexAiStepProperties {
  engine?: MlInferenceEngine;
  useXgboost?: boolean;
  useVertexAi?: boolean;
  modelUri?: string;
  xgboostTask?: XgboostTaskType;
  featuresColumn?: string;
  predictionColumn?: string;
  convertVectorsToArray?: boolean;
  projectId?: string;
  location?: string;
  endpointId?: string;
  geminiModel?: string;
  promptTemplate?: string;
  inputColumns?: string[];
  batchSize?: number | string;
  outputColumn?: string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link SparkMlAndVertexAiProperties}.
 */
export interface SparkMlAndVertexAiNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: SparkMlAndVertexAiStepProperties;
  };
}

/**
 * Props for the {@link SparkMlAndVertexAiProperties} component.
 */
export interface SparkMlAndVertexAiPropertiesProps {
  node: SparkMlAndVertexAiNode;
  onChange: (properties: SparkMlAndVertexAiStepProperties) => void;
}

const ENGINE_OPTIONS: SelectFieldOption[] = [
  { value: 'spark_ml', label: 'Spark MLlib (PipelineModel.load)' },
  { value: 'xgboost', label: 'XGBoost on Spark (xgboost.spark)' },
  { value: 'vertex_ai_endpoint', label: 'Vertex AI Online Endpoint (Batched)' },
  { value: 'vertex_ai_gemini', label: 'Vertex AI Gemini (google-genai Global)' },
];

const XGBOOST_TASK_OPTIONS: SelectFieldOption[] = [
  { value: 'classification', label: 'Classification (SparkXGBClassifierModel)' },
  { value: 'regression', label: 'Regression (SparkXGBRegressorModel)' },
];

const FORBIDDEN_LEGACY_GEMINI_PATTERN = /^gemini-(1\.|2\.0|2\.5)/i;
const GEMINI_GLOBAL_ONLY_MODEL_PATTERN = /^gemini-(3\.|4[-.])/i;

/**
 * Normalizes raw input column definitions into a clean string array.
 *
 * @param raw Raw inputColumns value from node state or comma-separated user input.
 * @returns Trimmed non-empty column names.
 */
function normalizeMlInputColumns(raw: unknown): string[] {
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
 * Resolves the effective inference engine from persisted properties and legacy alias flags.
 *
 * @param properties Persisted node property bag.
 * @param sparkTransform Optional node spark_transform alias.
 * @returns Canonical MlInferenceEngine identifier.
 */
function resolveEngine(
  properties: SparkMlAndVertexAiStepProperties,
  sparkTransform?: string
): MlInferenceEngine {
  if (
    properties.engine === 'spark_ml' ||
    properties.engine === 'xgboost' ||
    properties.engine === 'vertex_ai_endpoint' ||
    properties.engine === 'vertex_ai_gemini'
  ) {
    return properties.engine;
  }
  if (properties.useXgboost === true) {
    return 'xgboost';
  }
  if (typeof properties.geminiModel === 'string' && properties.geminiModel.trim().length > 0) {
    return 'vertex_ai_gemini';
  }
  if (properties.useVertexAi === true || sparkTransform === 'VertexAiInference') {
    return 'vertex_ai_endpoint';
  }
  return 'spark_ml';
}

/**
 * Renders the property configuration panel for the Apache Spark ML & Vertex AI inference step.
 *
 * @param props Selected SparkMlAndVertexAi node and property update callback.
 * @returns The rendered SparkMlAndVertexAi configuration form.
 */
export const SparkMlAndVertexAiProperties: React.FC<SparkMlAndVertexAiPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: SparkMlAndVertexAiStepProperties = node?.data?.properties || {};
  const engine = resolveEngine(properties, node?.data?.spark_transform);
  const isLocalMlEngine = engine === 'spark_ml' || engine === 'xgboost';
  const isVertexEngine = engine === 'vertex_ai_endpoint' || engine === 'vertex_ai_gemini';

  const modelUri = typeof properties.modelUri === 'string' ? properties.modelUri : '';
  const xgboostTask: XgboostTaskType =
    properties.xgboostTask === 'regression' ? 'regression' : 'classification';
  const featuresColumn =
    typeof properties.featuresColumn === 'string' ? properties.featuresColumn : 'features';
  const predictionColumn =
    typeof properties.predictionColumn === 'string'
      ? properties.predictionColumn
      : 'prediction';
  const convertVectorsToArray =
    typeof properties.convertVectorsToArray === 'boolean'
      ? properties.convertVectorsToArray
      : true;

  const projectId = typeof properties.projectId === 'string' ? properties.projectId : '';
  const defaultLocation = engine === 'vertex_ai_gemini' ? 'global' : 'us-central1';
  const location =
    typeof properties.location === 'string' && properties.location.trim().length > 0
      ? properties.location
      : defaultLocation;
  const endpointId = typeof properties.endpointId === 'string' ? properties.endpointId : '';
  const geminiModel =
    typeof properties.geminiModel === 'string'
      ? properties.geminiModel
      : 'gemini-4-hydrogen';
  const promptTemplate =
    typeof properties.promptTemplate === 'string' ? properties.promptTemplate : '';
  const inputColumns = normalizeMlInputColumns(properties.inputColumns);
  const rawBatchSize = properties.batchSize;
  const batchSizeText =
    rawBatchSize === undefined || rawBatchSize === null ? '32' : String(rawBatchSize);
  const outputColumn =
    typeof properties.outputColumn === 'string' ? properties.outputColumn : 'ai_response';
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingColumnInput, setPendingColumnInput] = useState<string>('');

  const parsedBatchSize = Number(batchSizeText.trim());
  const isBatchSizeInvalid =
    batchSizeText.trim().length === 0 ||
    !Number.isInteger(parsedBatchSize) ||
    parsedBatchSize <= 0;
  const resolvedBatchSize = !isBatchSizeInvalid ? parsedBatchSize : batchSizeText;

  const isModelUriInvalid = isLocalMlEngine && modelUri.trim().length === 0;
  const isFeaturesColInvalid = isLocalMlEngine && featuresColumn.trim().length === 0;
  const isPredictionColInvalid = isLocalMlEngine && predictionColumn.trim().length === 0;
  const isProjectIdInvalid = isVertexEngine && projectId.trim().length === 0;
  const isEndpointIdInvalid =
    engine === 'vertex_ai_endpoint' && endpointId.trim().length === 0;
  const isGeminiModelEmpty =
    engine === 'vertex_ai_gemini' && geminiModel.trim().length === 0;
  const isGeminiModelForbidden =
    engine === 'vertex_ai_gemini' &&
    FORBIDDEN_LEGACY_GEMINI_PATTERN.test(geminiModel.trim());
  const isGeminiRegionalMismatch =
    engine === 'vertex_ai_gemini' &&
    GEMINI_GLOBAL_ONLY_MODEL_PATTERN.test(geminiModel.trim()) &&
    location.trim() !== 'global';
  const isPromptTemplateInvalid =
    engine === 'vertex_ai_gemini' && promptTemplate.trim().length === 0;
  const isOutputColumnInvalid = isVertexEngine && outputColumn.trim().length === 0;

  const buildPatch = (
    overrides: Partial<SparkMlAndVertexAiStepProperties>,
    targetEngine: MlInferenceEngine = engine
  ): SparkMlAndVertexAiStepProperties => ({
    ...properties,
    engine: targetEngine,
    useXgboost: targetEngine === 'xgboost',
    useVertexAi:
      targetEngine === 'vertex_ai_endpoint' || targetEngine === 'vertex_ai_gemini',
    ...overrides,
  });

  const handleEngineChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextEngine = e.target.value as MlInferenceEngine;
    const nextPatch: Partial<SparkMlAndVertexAiStepProperties> = {};
    if (nextEngine === 'vertex_ai_gemini') {
      nextPatch.location = 'global';
      nextPatch.geminiModel = geminiModel || 'gemini-4-hydrogen';
      nextPatch.batchSize = resolvedBatchSize;
      nextPatch.outputColumn = outputColumn;
    } else if (nextEngine === 'vertex_ai_endpoint') {
      nextPatch.location =
        properties.location && properties.location !== 'global'
          ? properties.location
          : 'us-central1';
      nextPatch.batchSize = resolvedBatchSize;
      nextPatch.outputColumn = outputColumn;
    } else if (nextEngine === 'xgboost') {
      nextPatch.xgboostTask = xgboostTask;
      nextPatch.convertVectorsToArray = convertVectorsToArray;
    } else {
      nextPatch.convertVectorsToArray = convertVectorsToArray;
    }
    onChange(buildPatch(nextPatch, nextEngine));
  };

  const handleSelectFieldChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const { name, value } = e.target;
    onChange(buildPatch({ [name]: value }));
  };

  const handleTextFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    if (name === 'batchSize') {
      const numeric = Number(value.trim());
      onChange(
        buildPatch({
          batchSize:
            value.trim() !== '' && Number.isInteger(numeric) && numeric > 0
              ? numeric
              : value,
        })
      );
      return;
    }
    onChange(buildPatch({ [name]: value }));
  };

  const handleVectorConvertToggle = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange(buildPatch({ convertVectorsToArray: e.target.checked }));
  };

  const appendInputColumns = () => {
    const parsed = normalizeMlInputColumns(pendingColumnInput);
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
    onChange(buildPatch({ inputColumns: merged }));
  };

  const handleEditInputColumn = (index: number, value: string) => {
    const updated = [...inputColumns];
    updated[index] = value;
    onChange(buildPatch({ inputColumns: normalizeMlInputColumns(updated) }));
  };

  const handleRemoveInputColumn = (index: number) => {
    onChange(buildPatch({ inputColumns: inputColumns.filter((_, i) => i !== index) }));
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange(buildPatch({ system_instructions: value }));
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Inference Architecture Guidance Banner */}
      <div
        data-testid="ml-vertex-guidance-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <BrainCircuit className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Distributed ML & Vertex AI Scoring</span>
          <InfoTooltip text="Supports in-JVM Spark MLlib PipelineModel, distributed xgboost.spark, and batched mapInPandas Vertex AI Endpoint or Gemini 4 Hydrogen inference initialized once per partition." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          {isLocalMlEngine
            ? 'Loads saved Spark MLlib or XGBoost models across executors and converts VectorUDT columns via vector_to_array() for BigQuery and Iceberg compatibility.'
            : 'Initializes the Vertex AI client once per Spark partition inside df.mapInPandas() and sends batched requests with retry backoff.'}
        </p>
      </div>

      {/* 2. Engine Selector */}
      <SelectField
        id="sparkMlEngine"
        name="engine"
        label="Inference Engine"
        tooltip="Select Spark MLlib PipelineModel, distributed XGBoost on Spark, Vertex AI Online Endpoint, or Vertex AI Gemini via google-genai."
        value={engine}
        options={ENGINE_OPTIONS}
        onChange={handleEngineChange}
      />

      {/* 3A. Spark MLlib & XGBoost Configuration */}
      {isLocalMlEngine && (
        <>
          <PropertyField
            id="sparkMlModelUri"
            name="modelUri"
            label="Model Artifact URI"
            required="Required"
            tooltip="Cloud Storage or filesystem path passed to PipelineModel.load(uri) or SparkXGBClassifierModel.load(uri)."
            value={modelUri}
            onChange={handleTextFieldChange}
            placeholder="gs://my-ml-bucket/models/fraud-detector-v1"
            invalid={isModelUriInvalid}
            error={isModelUriInvalid ? 'Model artifact URI is required.' : null}
          />

          {engine === 'xgboost' && (
            <SelectField
              id="sparkMlXgboostTask"
              name="xgboostTask"
              label="XGBoost Task"
              tooltip="Select SparkXGBClassifierModel for classification or SparkXGBRegressorModel for continuous regression."
              value={xgboostTask}
              options={XGBOOST_TASK_OPTIONS}
              onChange={handleSelectFieldChange}
            />
          )}

          <PropertyField
            id="sparkMlFeaturesColumn"
            name="featuresColumn"
            label="Features Column"
            required="Required"
            tooltip="Input MLlib Vector column consumed by the Spark ML or XGBoost model."
            value={featuresColumn}
            onChange={handleTextFieldChange}
            placeholder="features"
            invalid={isFeaturesColInvalid}
            error={isFeaturesColInvalid ? 'Features column is required.' : null}
          />

          <PropertyField
            id="sparkMlPredictionColumn"
            name="predictionColumn"
            label="Prediction Column"
            required="Required"
            tooltip="Output column name receiving model predictions."
            value={predictionColumn}
            onChange={handleTextFieldChange}
            placeholder="prediction"
            invalid={isPredictionColInvalid}
            error={isPredictionColInvalid ? 'Prediction column is required.' : null}
          />

          <div className="flex items-center justify-between gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
            <label
              htmlFor="sparkMlConvertVectors"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)] cursor-pointer"
            >
              <span>Convert VectorUDT to Array</span>
              <InfoTooltip text="Applies pyspark.ml.functions.vector_to_array() on probability, rawPrediction, and features columns so BigQuery and Iceberg sinks accept the output schema." />
            </label>
            <input
              id="sparkMlConvertVectors"
              name="convertVectorsToArray"
              type="checkbox"
              checked={convertVectorsToArray}
              onChange={handleVectorConvertToggle}
              className="w-4 h-4 accent-blue-500 cursor-pointer"
            />
          </div>
        </>
      )}

      {/* 3B. Vertex AI Endpoint & Gemini Configuration */}
      {isVertexEngine && (
        <>
          <PropertyField
            id="vertexAiProjectId"
            name="projectId"
            label="GCP Project ID"
            required="Required"
            tooltip="Google Cloud project hosting the Vertex AI Endpoint or Gemini API."
            value={projectId}
            onChange={handleTextFieldChange}
            placeholder="my-gcp-project"
            invalid={isProjectIdInvalid}
            error={isProjectIdInvalid ? 'GCP project ID is required.' : null}
          />

          <PropertyField
            id="vertexAiLocation"
            name="location"
            label="Vertex AI Location"
            required="Required"
            tooltip={
              engine === 'vertex_ai_gemini'
                ? 'Must be global when calling Gemini 3.x/4.x models such as gemini-4-hydrogen via google-genai (regional us-central1 returns 404).'
                : 'GCP region hosting the Vertex AI Online Prediction Endpoint, such as us-central1.'
            }
            value={location}
            onChange={handleTextFieldChange}
            placeholder={engine === 'vertex_ai_gemini' ? 'global' : 'us-central1'}
            invalid={isGeminiRegionalMismatch}
            error={
              isGeminiRegionalMismatch
                ? 'Location must be global for Gemini 3.x/4.x models such as gemini-4-hydrogen (regional endpoints return 404).'
                : null
            }
          />

          {engine === 'vertex_ai_endpoint' && (
            <PropertyField
              id="vertexAiEndpointId"
              name="endpointId"
              label="Vertex AI Endpoint ID"
              required="Required"
              tooltip="Numeric Endpoint ID or full projects/.../locations/.../endpoints/... resource name."
              value={endpointId}
              onChange={handleTextFieldChange}
              placeholder="1234567890123456789"
              invalid={isEndpointIdInvalid}
              error={isEndpointIdInvalid ? 'Vertex AI Endpoint ID is required.' : null}
            />
          )}

          {engine === 'vertex_ai_gemini' && (
            <>
              <PropertyField
                id="vertexAiGeminiModel"
                name="geminiModel"
                label="Gemini Model"
                required="Required"
                tooltip="Enterprise Gemini model ID invoked via google-genai Client(vertexai=True, location='global'). Default is gemini-4-hydrogen."
                value={geminiModel}
                onChange={handleTextFieldChange}
                placeholder="gemini-4-hydrogen"
                invalid={isGeminiModelEmpty || isGeminiModelForbidden}
                error={
                  isGeminiModelEmpty
                    ? 'Gemini model name is required.'
                    : isGeminiModelForbidden
                      ? 'Legacy Gemini 1.x, 2.0, and 2.5 models are deprecated. Use gemini-4-hydrogen.'
                      : null
                }
              />

              <PropertyField
                id="vertexAiPromptTemplate"
                name="promptTemplate"
                label="Prompt Template"
                type="textarea"
                rows={4}
                required="Required"
                tooltip="Python format template referencing inputColumns in single braces, such as Classify ticket: {ticket_body}."
                value={promptTemplate}
                onChange={handleTextFieldChange}
                placeholder="Classify customer feedback sentiment as POSITIVE, NEUTRAL, or NEGATIVE: {feedback_text}"
                invalid={isPromptTemplateInvalid}
                error={
                  isPromptTemplateInvalid
                    ? 'Prompt template is required for Gemini inference.'
                    : null
                }
              />
            </>
          )}

          {/* Input Columns for Vertex AI Payloads */}
          <div className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
            <label
              htmlFor="vertexAiInputColAdd"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Input Columns</span>
              <InfoTooltip text="DataFrame columns serialized into Vertex AI Endpoint instances or interpolated into the Gemini prompt template." />
            </label>

            {inputColumns.length > 0 && (
              <div className="flex flex-col gap-2" data-testid="vertex-input-columns-list">
                {inputColumns.map((col, index) => (
                  <div key={`${index}-${col}`} className="flex items-center gap-2">
                    <input
                      type="text"
                      aria-label={`Vertex input column ${index + 1}`}
                      value={col}
                      onChange={(e) => handleEditInputColumn(index, e.target.value)}
                      className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                    />
                    <button
                      type="button"
                      aria-label={`Remove Vertex input column ${col}`}
                      onClick={() => handleRemoveInputColumn(index)}
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
                id="vertexAiInputColAdd"
                type="text"
                value={pendingColumnInput}
                onChange={(e) => setPendingColumnInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    e.preventDefault();
                    appendInputColumns();
                  }
                }}
                placeholder="e.g. feedback_text or age, tenure_months"
                className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
              />
              <button
                type="button"
                aria-label="Add Vertex input column"
                onClick={appendInputColumns}
                className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
              >
                <Plus className="w-3.5 h-3.5" />
                <span>Add</span>
              </button>
            </div>
          </div>

          <PropertyField
            id="vertexAiBatchSize"
            name="batchSize"
            label="Prediction Batch Size"
            type="number"
            min={1}
            step={1}
            required="Required"
            tooltip="Number of rows grouped per Vertex AI prediction chunk inside mapInPandas."
            value={batchSizeText}
            onChange={handleTextFieldChange}
            placeholder="32"
            invalid={isBatchSizeInvalid}
            error={isBatchSizeInvalid ? 'Batch size must be a positive integer.' : null}
          />

          <PropertyField
            id="vertexAiOutputColumn"
            name="outputColumn"
            label="Output Response Column"
            required="Required"
            tooltip="Column appended to the output DataFrame containing the Vertex AI prediction or Gemini response."
            value={outputColumn}
            onChange={handleTextFieldChange}
            placeholder="ai_response"
            invalid={isOutputColumnInvalid}
            error={isOutputColumnInvalid ? 'Output response column is required.' : null}
          />
        </>
      )}

      {/* 4. Dead-Letter Queue Guidance Banner */}
      <div
        data-testid="ml-vertex-dlq-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
          <span>Dead-Letter Queue (error Handle)</span>
          <InfoTooltip text="When the error handle is wired, failed inference batches or rows capture error diagnostics into _error and route to dead-letter storage." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          If you connect the <code className="font-mono text-[var(--vscode-text-primary)]">error</code> handle,
          failed predictions populate <code className="font-mono text-[var(--vscode-text-primary)]">_error</code> so
          transient endpoint or payload errors route to dead-letter sinks safely.
        </p>
      </div>

      <SystemInstructionsField
        id="sparkMlAndVertexAiSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this SparkMlAndVertexAi step."
        placeholder="Enter custom instructions for model scoring, post-processing, or Vertex AI structured output..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
