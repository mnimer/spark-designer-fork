/**
 * @fileoverview Property configuration panel for the RAG Text Chunk & Embed step (EXT7 TextChunkAndEmbed).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.x TextChunkAndEmbed step nodes
 * supporting chunk_only, embed_only, and chunk_and_embed RAG preparation modes with overlap
 * bounds validation, unified google-genai batched Vertex AI embeddings, and DLQ guidance.
 */

import React from 'react';
import { AlertTriangle, Sparkles } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Execution mode for {@link TextChunkAndEmbedProperties}. */
export type TextChunkEmbedMode = 'chunk_and_embed' | 'chunk_only' | 'embed_only';

/** Text splitting strategy for chunk_only and chunk_and_embed modes. */
export type TextChunkStrategy = 'recursive_character' | 'fixed_chars' | 'sentence';

/** Vertex AI embedding task type passed to EmbedContentConfig. */
export type EmbeddingTaskType =
  | 'RETRIEVAL_DOCUMENT'
  | 'RETRIEVAL_QUERY'
  | 'SEMANTIC_SIMILARITY'
  | 'CLASSIFICATION'
  | 'CLUSTERING';

/** Persisted properties for the TextChunkAndEmbed step node. */
export interface TextChunkAndEmbedStepProperties {
  mode?: TextChunkEmbedMode;
  sourceTextColumn?: string;
  documentIdColumn?: string;
  chunkStrategy?: TextChunkStrategy;
  chunkSize?: number | string;
  chunkOverlap?: number | string;
  chunkTextColumn?: string;
  chunkIdColumn?: string;
  chunkIndexColumn?: string;
  projectId?: string;
  location?: string;
  embeddingModel?: string;
  taskType?: EmbeddingTaskType;
  outputDimensionality?: number | string;
  batchSize?: number | string;
  embeddingColumn?: string;
  system_instructions?: string;
  special_instructions?: string;
  [key: string]: unknown;
}

/** React Flow node shape accepted by {@link TextChunkAndEmbedProperties}. */
export interface TextChunkAndEmbedNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: TextChunkAndEmbedStepProperties;
  };
}

/** Props for the {@link TextChunkAndEmbedProperties} component. */
export interface TextChunkAndEmbedPropertiesProps {
  node: TextChunkAndEmbedNode;
  onChange: (properties: TextChunkAndEmbedStepProperties) => void;
}

const MODE_OPTIONS: SelectFieldOption[] = [
  {
    value: 'chunk_and_embed',
    label: 'Chunk & Embed (Split text + batched Vertex AI embeddings)',
  },
  {
    value: 'chunk_only',
    label: 'Chunk Only (Pure PySpark mapInPandas text splitting)',
  },
  {
    value: 'embed_only',
    label: 'Embed Only (Batched Vertex AI embeddings on existing rows)',
  },
];

const CHUNK_STRATEGY_OPTIONS: SelectFieldOption[] = [
  {
    value: 'recursive_character',
    label: 'Recursive Character (Paragraphs, lines, sentences)',
  },
  {
    value: 'fixed_chars',
    label: 'Fixed Characters (Sliding character window)',
  },
  {
    value: 'sentence',
    label: 'Sentence Packing (Sentence boundary aware)',
  },
];

const TASK_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'RETRIEVAL_DOCUMENT', label: 'RETRIEVAL_DOCUMENT (RAG corpus indexing)' },
  { value: 'RETRIEVAL_QUERY', label: 'RETRIEVAL_QUERY (Search query embedding)' },
  { value: 'SEMANTIC_SIMILARITY', label: 'SEMANTIC_SIMILARITY (Symmetric similarity)' },
  { value: 'CLASSIFICATION', label: 'CLASSIFICATION (Text categorization)' },
  { value: 'CLUSTERING', label: 'CLUSTERING (Topic grouping)' },
];

const EMBEDDING_MODEL_SUGGESTIONS: string[] = [
  'gemini-embedding-001',
  'text-embedding-005',
  'text-multilingual-embedding-002',
];

const LEGACY_EMBEDDING_MODEL_RE =
  /^(textembedding-gecko|gemini-(1\.|2\.0|2\.5))/i;

/**
 * Resolves the canonical TextChunkEmbedMode from persisted properties.
 *
 * @param raw Raw mode property value.
 * @returns Canonical TextChunkEmbedMode value.
 */
function resolveMode(raw: unknown): TextChunkEmbedMode {
  if (raw === 'chunk_only' || raw === 'embed_only' || raw === 'chunk_and_embed') {
    return raw;
  }
  return 'chunk_and_embed';
}

/**
 * Resolves the canonical TextChunkStrategy from persisted properties.
 *
 * @param raw Raw chunkStrategy property value.
 * @returns Canonical TextChunkStrategy value.
 */
function resolveChunkStrategy(raw: unknown): TextChunkStrategy {
  if (raw === 'fixed_chars' || raw === 'sentence' || raw === 'recursive_character') {
    return raw;
  }
  return 'recursive_character';
}

/**
 * Resolves the canonical EmbeddingTaskType from persisted properties.
 *
 * @param raw Raw taskType property value.
 * @returns Canonical EmbeddingTaskType value.
 */
function resolveTaskType(raw: unknown): EmbeddingTaskType {
  if (
    raw === 'RETRIEVAL_DOCUMENT' ||
    raw === 'RETRIEVAL_QUERY' ||
    raw === 'SEMANTIC_SIMILARITY' ||
    raw === 'CLASSIFICATION' ||
    raw === 'CLUSTERING'
  ) {
    return raw;
  }
  return 'RETRIEVAL_DOCUMENT';
}

/**
 * Renders the property configuration panel for the RAG Text Chunk & Embed step.
 *
 * @param props Selected TextChunkAndEmbed node and property update callback.
 * @returns The rendered TextChunkAndEmbed configuration form.
 */
export const TextChunkAndEmbedProperties: React.FC<TextChunkAndEmbedPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: TextChunkAndEmbedStepProperties = node?.data?.properties || {};
  const mode = resolveMode(properties.mode);
  const showChunking = mode === 'chunk_and_embed' || mode === 'chunk_only';
  const showEmbedding = mode === 'chunk_and_embed' || mode === 'embed_only';

  const sourceTextColumn =
    typeof properties.sourceTextColumn === 'string' ? properties.sourceTextColumn : 'text';
  const documentIdColumn =
    typeof properties.documentIdColumn === 'string' ? properties.documentIdColumn : 'doc_id';
  const chunkStrategy = resolveChunkStrategy(properties.chunkStrategy);

  const rawChunkSize = properties.chunkSize;
  const chunkSizeText =
    rawChunkSize === undefined || rawChunkSize === null ? '1000' : String(rawChunkSize);
  const parsedChunkSize = Number(chunkSizeText.trim());
  const isChunkSizeInvalid =
    showChunking &&
    (chunkSizeText.trim().length === 0 ||
      !Number.isInteger(parsedChunkSize) ||
      parsedChunkSize <= 0);

  const rawChunkOverlap = properties.chunkOverlap;
  const chunkOverlapText =
    rawChunkOverlap === undefined || rawChunkOverlap === null ? '200' : String(rawChunkOverlap);
  const parsedChunkOverlap = Number(chunkOverlapText.trim());
  const isChunkOverlapNonIntOrNegative =
    showChunking &&
    (chunkOverlapText.trim().length === 0 ||
      !Number.isInteger(parsedChunkOverlap) ||
      parsedChunkOverlap < 0);
  const isChunkOverlapTooLarge =
    showChunking &&
    !isChunkOverlapNonIntOrNegative &&
    !isChunkSizeInvalid &&
    parsedChunkOverlap >= parsedChunkSize;
  const isChunkOverlapInvalid = isChunkOverlapNonIntOrNegative || isChunkOverlapTooLarge;

  const chunkTextColumn =
    typeof properties.chunkTextColumn === 'string' ? properties.chunkTextColumn : 'chunk_text';
  const chunkIdColumn =
    typeof properties.chunkIdColumn === 'string' ? properties.chunkIdColumn : 'chunk_id';
  const chunkIndexColumn =
    typeof properties.chunkIndexColumn === 'string'
      ? properties.chunkIndexColumn
      : 'chunk_index';

  const projectId = typeof properties.projectId === 'string' ? properties.projectId : '';
  const location =
    typeof properties.location === 'string' && properties.location.trim().length > 0
      ? properties.location
      : 'global';
  const embeddingModel =
    typeof properties.embeddingModel === 'string'
      ? properties.embeddingModel
      : 'gemini-embedding-001';
  const taskType = resolveTaskType(properties.taskType);

  const rawOutputDim = properties.outputDimensionality;
  const outputDimText =
    rawOutputDim === undefined || rawOutputDim === null ? '768' : String(rawOutputDim);
  const parsedOutputDim = Number(outputDimText.trim());
  const isOutputDimInvalid =
    showEmbedding &&
    (outputDimText.trim().length === 0 ||
      !Number.isInteger(parsedOutputDim) ||
      parsedOutputDim <= 0);

  const rawBatchSize = properties.batchSize;
  const batchSizeText =
    rawBatchSize === undefined || rawBatchSize === null ? '64' : String(rawBatchSize);
  const parsedBatchSize = Number(batchSizeText.trim());
  const isBatchSizeInvalid =
    showEmbedding &&
    (batchSizeText.trim().length === 0 ||
      !Number.isInteger(parsedBatchSize) ||
      parsedBatchSize < 1 ||
      parsedBatchSize > 250);

  const embeddingColumn =
    typeof properties.embeddingColumn === 'string' ? properties.embeddingColumn : 'embedding';

  const isSourceTextInvalid = sourceTextColumn.trim().length === 0;
  const isDocumentIdInvalid = showChunking && documentIdColumn.trim().length === 0;
  const isChunkTextColInvalid = showChunking && chunkTextColumn.trim().length === 0;
  const isChunkIdColInvalid = showChunking && chunkIdColumn.trim().length === 0;
  const isChunkIndexColInvalid = showChunking && chunkIndexColumn.trim().length === 0;

  const isProjectIdInvalid = showEmbedding && projectId.trim().length === 0;
  const isLocationInvalid = showEmbedding && location.trim().length === 0;
  const trimmedModel = embeddingModel.trim();
  const isEmbeddingModelEmpty = showEmbedding && trimmedModel.length === 0;
  const isEmbeddingModelLegacy =
    showEmbedding && !isEmbeddingModelEmpty && LEGACY_EMBEDDING_MODEL_RE.test(trimmedModel);
  const isEmbeddingColInvalid = showEmbedding && embeddingColumn.trim().length === 0;

  const systemInstructions = resolveSystemInstructions(node);

  const emitPatch = (overrides: Partial<TextChunkAndEmbedStepProperties>) => {
    onChange({
      ...properties,
      mode,
      ...overrides,
    });
  };

  const handleModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextMode = e.target.value as TextChunkEmbedMode;
    onChange({
      ...properties,
      mode: nextMode,
    });
  };

  const handleSelectChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const { name, value } = e.target;
    emitPatch({ [name]: value });
  };

  const handleInputChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    if (name === 'chunkSize' || name === 'outputDimensionality') {
      const numeric = Number(value.trim());
      const resolved =
        value.trim() !== '' && Number.isInteger(numeric) && numeric > 0 ? numeric : value;
      emitPatch({ [name]: resolved });
      return;
    }
    if (name === 'chunkOverlap') {
      const numeric = Number(value.trim());
      const resolved =
        value.trim() !== '' && Number.isInteger(numeric) && numeric >= 0 ? numeric : value;
      emitPatch({ chunkOverlap: resolved });
      return;
    }
    if (name === 'batchSize') {
      const numeric = Number(value.trim());
      const resolved =
        value.trim() !== '' && Number.isInteger(numeric) && numeric >= 1 && numeric <= 250
          ? numeric
          : value;
      emitPatch({ batchSize: resolved });
      return;
    }
    emitPatch({ [name]: value });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. RAG Architecture Banner */}
      <div
        data-testid="text-chunk-embed-guidance-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <Sparkles className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Distributed RAG Chunking & Vertex AI Embeddings</span>
          <InfoTooltip text="Splits documents with configurable overlap and generates batched embeddings via google-genai Client(vertexai=True) initialized once per Spark partition inside df.mapInPandas()." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          {mode === 'chunk_only'
            ? 'Splits source documents into overlapping text chunks with deterministic chunk IDs using pure PySpark mapInPandas and zero external network calls.'
            : 'Initializes google-genai once per Spark executor partition and sends batched embed_content requests with retry backoff.'}
        </p>
      </div>

      {/* 2. Mode & Input Column */}
      <SelectField
        id="textChunkEmbedMode"
        name="mode"
        label="RAG Preparation Mode"
        tooltip="Choose combined chunking and embedding, standalone text chunking, or standalone batched embedding."
        value={mode}
        options={MODE_OPTIONS}
        onChange={handleModeChange}
      />

      <PropertyField
        id="textChunkEmbedSourceTextColumn"
        name="sourceTextColumn"
        label="Source Text Column"
        required="Source text column is required"
        tooltip="Input DataFrame string column containing raw document or passage text (default text)."
        value={sourceTextColumn}
        invalid={isSourceTextInvalid}
        error={isSourceTextInvalid ? 'Source text column is required.' : null}
        onChange={handleInputChange}
        placeholder="text"
        mono={true}
      />

      {/* 3. Text Chunking Settings */}
      {showChunking && (
        <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
          <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            Text Chunking Configuration
          </span>

          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="textChunkEmbedDocumentIdColumn"
              name="documentIdColumn"
              label="Document ID Column"
              required="Document ID column is required"
              tooltip="Upstream primary key column used to derive deterministic chunk IDs such as doc_id_0 (default doc_id)."
              value={documentIdColumn}
              invalid={isDocumentIdInvalid}
              error={isDocumentIdInvalid ? 'Document ID column is required.' : null}
              onChange={handleInputChange}
              placeholder="doc_id"
              mono={true}
            />

            <SelectField
              id="textChunkEmbedChunkStrategy"
              name="chunkStrategy"
              label="Chunking Strategy"
              tooltip="Algorithm used to split sourceTextColumn into bounded text windows."
              value={chunkStrategy}
              options={CHUNK_STRATEGY_OPTIONS}
              onChange={handleSelectChange}
            />
          </div>

          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="textChunkEmbedChunkSize"
              name="chunkSize"
              label="Chunk Size (Characters)"
              type="number"
              min={1}
              step={1}
              required="Chunk size is required"
              tooltip="Maximum character length per emitted text chunk (default 1000)."
              value={chunkSizeText}
              invalid={isChunkSizeInvalid}
              error={
                isChunkSizeInvalid ? 'Chunk size must be a positive integer greater than 0.' : null
              }
              onChange={handleInputChange}
              placeholder="1000"
              mono={true}
            />

            <PropertyField
              id="textChunkEmbedChunkOverlap"
              name="chunkOverlap"
              label="Chunk Overlap (Characters)"
              type="number"
              min={0}
              step={1}
              required="Chunk overlap is required"
              tooltip="Trailing characters shared across consecutive chunks. Must satisfy 0 <= chunkOverlap < chunkSize (default 200)."
              value={chunkOverlapText}
              invalid={isChunkOverlapInvalid}
              error={
                isChunkOverlapNonIntOrNegative
                  ? 'Chunk overlap must be a non-negative integer (0 or greater).'
                  : isChunkOverlapTooLarge
                    ? 'Chunk overlap must be strictly less than chunk size.'
                    : null
              }
              onChange={handleInputChange}
              placeholder="200"
              mono={true}
            />
          </div>

          <div className="grid grid-cols-3 gap-2">
            <PropertyField
              id="textChunkEmbedChunkIdColumn"
              name="chunkIdColumn"
              label="Chunk ID Column"
              required="Chunk ID column is required"
              tooltip="Output string column holding deterministic chunk identifiers (default chunk_id)."
              value={chunkIdColumn}
              invalid={isChunkIdColInvalid}
              error={isChunkIdColInvalid ? 'Chunk ID column is required.' : null}
              onChange={handleInputChange}
              placeholder="chunk_id"
              mono={true}
            />

            <PropertyField
              id="textChunkEmbedChunkIndexColumn"
              name="chunkIndexColumn"
              label="Chunk Index Column"
              required="Chunk index column is required"
              tooltip="Output integer column holding the 0-based chunk position per document (default chunk_index)."
              value={chunkIndexColumn}
              invalid={isChunkIndexColInvalid}
              error={isChunkIndexColInvalid ? 'Chunk index column is required.' : null}
              onChange={handleInputChange}
              placeholder="chunk_index"
              mono={true}
            />

            <PropertyField
              id="textChunkEmbedChunkTextColumn"
              name="chunkTextColumn"
              label="Chunk Text Column"
              required="Chunk text column is required"
              tooltip="Output string column holding each split text segment (default chunk_text)."
              value={chunkTextColumn}
              invalid={isChunkTextColInvalid}
              error={isChunkTextColInvalid ? 'Chunk text column is required.' : null}
              onChange={handleInputChange}
              placeholder="chunk_text"
              mono={true}
            />
          </div>
        </div>
      )}

      {/* 4. Batched Vertex AI Embedding Settings */}
      {showEmbedding && (
        <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
          <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            Vertex AI Embedding Configuration (google-genai)
          </span>

          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="textChunkEmbedProjectId"
              name="projectId"
              label="GCP Project ID"
              required="GCP project ID is required"
              tooltip="Google Cloud project used when initializing genai.Client(vertexai=True)."
              value={projectId}
              invalid={isProjectIdInvalid}
              error={isProjectIdInvalid ? 'GCP project ID is required when embedding.' : null}
              onChange={handleInputChange}
              placeholder="my-gcp-project"
              mono={true}
            />

            <PropertyField
              id="textChunkEmbedLocation"
              name="location"
              label="Vertex AI Location"
              required="Location is required"
              tooltip="Vertex AI endpoint location passed to genai.Client (default global)."
              value={location}
              invalid={isLocationInvalid}
              error={isLocationInvalid ? 'Location is required.' : null}
              onChange={handleInputChange}
              placeholder="global"
              mono={true}
            />
          </div>

          <PropertyField
            id="textChunkEmbedEmbeddingModel"
            name="embeddingModel"
            label="Embedding Model"
            required="Embedding model is required"
            tooltip="Vertex AI embedding model invoked via client.models.embed_content (default gemini-embedding-001, also supports text-embedding-005)."
            value={embeddingModel}
            datalist={EMBEDDING_MODEL_SUGGESTIONS}
            invalid={isEmbeddingModelEmpty || isEmbeddingModelLegacy}
            error={
              isEmbeddingModelEmpty
                ? 'Embedding model is required.'
                : isEmbeddingModelLegacy
                  ? 'Legacy textembedding-gecko, Gemini 1.x, 2.0, and 2.5 models are deprecated. Use gemini-embedding-001 or text-embedding-005.'
                  : null
            }
            onChange={handleInputChange}
            placeholder="gemini-embedding-001"
            mono={true}
          />

          <SelectField
            id="textChunkEmbedTaskType"
            name="taskType"
            label="Embedding Task Type"
            tooltip="Optimization task type passed to types.EmbedContentConfig(task_type=...)."
            value={taskType}
            options={TASK_TYPE_OPTIONS}
            onChange={handleSelectChange}
          />

          <div className="grid grid-cols-3 gap-2">
            <PropertyField
              id="textChunkEmbedOutputDimensionality"
              name="outputDimensionality"
              label="Output Dimensionality"
              type="number"
              min={1}
              step={1}
              required="Output dimensionality is required"
              tooltip="Length of each output float embedding vector (default 768)."
              value={outputDimText}
              invalid={isOutputDimInvalid}
              error={
                isOutputDimInvalid
                  ? 'Output dimensionality must be a positive integer.'
                  : null
              }
              onChange={handleInputChange}
              placeholder="768"
              mono={true}
            />

            <PropertyField
              id="textChunkEmbedBatchSize"
              name="batchSize"
              label="Embedding Batch Size"
              type="number"
              min={1}
              max={250}
              step={1}
              required="Batch size is required"
              tooltip="Number of texts grouped per embed_content call inside mapInPandas (1 to 250, default 64)."
              value={batchSizeText}
              invalid={isBatchSizeInvalid}
              error={
                isBatchSizeInvalid ? 'Batch size must be an integer between 1 and 250.' : null
              }
              onChange={handleInputChange}
              placeholder="64"
              mono={true}
            />

            <PropertyField
              id="textChunkEmbedEmbeddingColumn"
              name="embeddingColumn"
              label="Embedding Output Column"
              required="Embedding output column is required"
              tooltip="Output ARRAY<FLOAT> column storing generated embedding vectors (default embedding)."
              value={embeddingColumn}
              invalid={isEmbeddingColInvalid}
              error={isEmbeddingColInvalid ? 'Embedding output column is required.' : null}
              onChange={handleInputChange}
              placeholder="embedding"
              mono={true}
            />
          </div>
        </div>
      )}

      {/* 5. Dead-Letter Queue Guidance Banner */}
      <div
        data-testid="text-chunk-embed-dlq-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
          <span>Dead-Letter Queue (error Handle)</span>
          <InfoTooltip text="When the error handle is connected, chunking or Vertex AI embedding exceptions populate _error and route failed rows to dead-letter storage." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          If you connect the <code className="font-mono text-[var(--vscode-text-primary)]">error</code> handle,
          failed chunks or embedding batches record diagnostics in{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">_error</code> and route to dead-letter sinks without aborting the job.
        </p>
      </div>

      {/* 6. Code Generation Instructions */}
      <SystemInstructionsField
        id="textChunkAndEmbedSystemInstructions"
        value={systemInstructions}
        tooltip="Custom guidance appended when generating PySpark code for this TextChunkAndEmbed step."
        placeholder="Enter custom instructions for Markdown header preservation, custom separators, or metadata normalization..."
        onChange={(value) => emitPatch({ system_instructions: value })}
      />
    </div>
  );
};
