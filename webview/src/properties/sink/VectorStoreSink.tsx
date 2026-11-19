/**
 * @fileoverview Vector Store Sink Properties Component (EXT3 VectorStoreSink).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.x VectorStoreSink nodes
 * targeting Google Cloud AlloyDB for PostgreSQL pgvector upserts or Vertex AI Vector Search
 * streaming datapoint upserts and Cloud Storage batch JSONL exports.
 */

import React, { useState } from 'react';
import { Cpu, Database } from 'lucide-react';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { PropertyField } from '../../components/common/PropertyField';
import {
  SecretManagerAuthFields,
  type AuthModeOption,
} from '../../components/common/SecretManagerAuthFields';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { StoragePathField } from '../../components/common/StoragePathField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { InfoTooltip } from '../../components/InfoTooltip';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Supported vector store backends for {@link VectorStoreSinkProperties}. */
export type VectorStoreBackend = 'alloydb_pgvector' | 'vertex_vector_search';

/** Network connectivity path for AlloyDB Connector sessions. */
export type AlloyDbVectorIpType = 'PRIVATE' | 'PUBLIC' | 'PSC';

/** Conflict resolution policy for AlloyDB pgvector ON CONFLICT upserts. */
export type UpsertConflictAction = 'do_update' | 'do_nothing';

/** Write mode for Vertex AI Vector Search index ingestion. */
export type VertexVectorWriteMode = 'stream_upsert' | 'gcs_batch_json';

/** Authentication mode for AlloyDB pgvector connections. */
export type VectorStoreAuthMode = 'iam' | 'secret_manager';

/** Persisted properties for the VectorStoreSink node. */
export interface VectorStoreSinkNodeProperties {
  backend?: VectorStoreBackend;
  alloydbInstanceUri?: string;
  ipType?: AlloyDbVectorIpType;
  database?: string;
  schemaName?: string;
  table?: string;
  dbUser?: string;
  idColumn?: string;
  embeddingColumn?: string;
  contentColumn?: string;
  metadataColumns?: string | string[];
  vectorDimensions?: number | string;
  upsertConflictAction?: UpsertConflictAction;
  batchSize?: number | string;
  authMode?: VectorStoreAuthMode | string;
  secretName?: string;
  socketFactoryJarUri?: string;
  driverJarUri?: string;
  projectId?: string;
  location?: string;
  indexName?: string;
  writeMode?: VertexVectorWriteMode;
  gcsOutputUri?: string;
  restrictsColumn?: string;
  crowdingTagColumn?: string;
  system_instructions?: string;
  special_instructions?: string;
  [key: string]: unknown;
}

/** Props for the {@link VectorStoreSinkProperties} component. */
export interface VectorStoreSinkPropertiesProps {
  node: {
    id?: string;
    data: {
      label?: string;
      spark_transform?: string;
      properties?: VectorStoreSinkNodeProperties;
      system_instructions?: string;
      special_instructions?: string;
    };
  };
  onChange: (properties: VectorStoreSinkNodeProperties) => void;
}

const BACKEND_OPTIONS: SelectFieldOption[] = [
  {
    value: 'alloydb_pgvector',
    label: 'AlloyDB for PostgreSQL pgvector (Connector + ::vector Upsert)',
  },
  {
    value: 'vertex_vector_search',
    label: 'Vertex AI Vector Search (MatchingEngineIndex / GCS JSONL)',
  },
];

const IP_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'PRIVATE', label: 'PRIVATE (VPC Peering)' },
  { value: 'PUBLIC', label: 'PUBLIC' },
  { value: 'PSC', label: 'PSC (Private Service Connect)' },
];

const CONFLICT_ACTION_OPTIONS: SelectFieldOption[] = [
  { value: 'do_update', label: 'DO UPDATE SET (Overwrite existing chunk embedding)' },
  { value: 'do_nothing', label: 'DO NOTHING (Keep existing row on duplicate ID)' },
];

const VERTEX_WRITE_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'stream_upsert', label: 'Stream Upsert (MatchingEngineIndex.upsert_datapoints)' },
  { value: 'gcs_batch_json', label: 'GCS Batch JSONL Export (Batch Index Update)' },
];

const ALLOYDB_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'iam', label: 'AlloyDB IAM Authentication (enable_iam_auth=True)' },
  { value: 'secret_manager', label: 'Google Cloud Secret Manager (Password Secret)' },
];

const ALLOYDB_URI_RE =
  /^projects\/[^/]+\/locations\/[^/]+\/clusters\/[^/]+\/instances\/[^/]+$/i;
const DATABASE_RE = /^[^\s/?#@:]+$/;
const SQL_IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Formats metadataColumns into a comma-separated string for form display.
 *
 * @param raw Persisted string or string array of metadata column names.
 * @returns Comma-separated column names.
 */
function formatMetadataColumnsText(raw: unknown): string {
  if (Array.isArray(raw)) {
    return raw
      .map((col) => String(col ?? '').trim())
      .filter((col) => col.length > 0)
      .join(', ');
  }
  return typeof raw === 'string' ? raw : '';
}

/**
 * Validates comma-separated SQL column identifiers against SQL injection rules.
 *
 * @param text Comma-separated column names entered by the user.
 * @returns Validation error message, or null when valid or blank.
 */
function validateMetadataColumns(text: string): string | null {
  const trimmed = text.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const parts = trimmed
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  for (const col of parts) {
    if (!SQL_IDENT_RE.test(col)) {
      return `Invalid SQL column identifier: ${col}`;
    }
  }
  return null;
}

/**
 * Configures properties for Apache Spark 4.x VectorStoreSink batch and streaming foreachBatch nodes.
 *
 * @param props Selected VectorStoreSink node and property update callback.
 * @returns The rendered VectorStoreSink configuration form.
 */
export const VectorStoreSinkProperties: React.FC<VectorStoreSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: VectorStoreSinkNodeProperties = node?.data?.properties || {};

  const backend: VectorStoreBackend =
    properties.backend === 'vertex_vector_search' ? 'vertex_vector_search' : 'alloydb_pgvector';
  const isAlloyDb = backend === 'alloydb_pgvector';

  const alloydbInstanceUri =
    typeof properties.alloydbInstanceUri === 'string' ? properties.alloydbInstanceUri : '';
  const ipType: AlloyDbVectorIpType =
    properties.ipType === 'PUBLIC' || properties.ipType === 'PSC' ? properties.ipType : 'PRIVATE';
  const database = typeof properties.database === 'string' ? properties.database : '';
  const schemaName =
    typeof properties.schemaName === 'string' ? properties.schemaName : 'public';
  const table = typeof properties.table === 'string' ? properties.table : '';
  const dbUser = typeof properties.dbUser === 'string' ? properties.dbUser : '';
  const idColumn = typeof properties.idColumn === 'string' ? properties.idColumn : 'chunk_id';
  const embeddingColumn =
    typeof properties.embeddingColumn === 'string' ? properties.embeddingColumn : 'embedding';
  const contentColumn =
    typeof properties.contentColumn === 'string' ? properties.contentColumn : 'chunk_text';
  const metadataColumnsText = formatMetadataColumnsText(properties.metadataColumns);

  const rawVectorDims = properties.vectorDimensions;
  const vectorDimensionsText =
    rawVectorDims === undefined || rawVectorDims === null ? '768' : String(rawVectorDims);
  const parsedVectorDims = Number(vectorDimensionsText.trim());
  const isVectorDimensionsInvalid =
    vectorDimensionsText.trim().length === 0 ||
    !Number.isInteger(parsedVectorDims) ||
    parsedVectorDims <= 0;

  const upsertConflictAction: UpsertConflictAction =
    properties.upsertConflictAction === 'do_nothing' ? 'do_nothing' : 'do_update';

  const defaultBatchSize = isAlloyDb ? '500' : '200';
  const rawBatchSize = properties.batchSize;
  const batchSizeText =
    rawBatchSize === undefined || rawBatchSize === null
      ? defaultBatchSize
      : String(rawBatchSize);
  const parsedBatchSize = Number(batchSizeText.trim());
  const isBatchSizeInvalid =
    batchSizeText.trim().length === 0 ||
    !Number.isInteger(parsedBatchSize) ||
    parsedBatchSize <= 0;

  const authMode = typeof properties.authMode === 'string' ? properties.authMode : 'iam';
  const secretName = typeof properties.secretName === 'string' ? properties.secretName : '';
  const socketFactoryJarUri =
    typeof properties.socketFactoryJarUri === 'string' ? properties.socketFactoryJarUri : '';
  const driverJarUri = typeof properties.driverJarUri === 'string' ? properties.driverJarUri : '';

  const projectId = typeof properties.projectId === 'string' ? properties.projectId : '';
  const location =
    typeof properties.location === 'string' && properties.location.trim().length > 0
      ? properties.location
      : 'us-central1';
  const indexName = typeof properties.indexName === 'string' ? properties.indexName : '';
  const writeMode: VertexVectorWriteMode =
    properties.writeMode === 'gcs_batch_json' ? 'gcs_batch_json' : 'stream_upsert';
  const gcsOutputUri =
    typeof properties.gcsOutputUri === 'string' ? properties.gcsOutputUri : '';
  const restrictsColumn =
    typeof properties.restrictsColumn === 'string' ? properties.restrictsColumn : '';
  const crowdingTagColumn =
    typeof properties.crowdingTagColumn === 'string' ? properties.crowdingTagColumn : '';

  const [isJarsOpen, setIsJarsOpen] = useState<boolean>(
    Boolean(socketFactoryJarUri.trim() || driverJarUri.trim())
  );

  // AlloyDB validation checks
  const uriTrimmed = alloydbInstanceUri.trim();
  const isUriEmpty = isAlloyDb && uriTrimmed.length === 0;
  const isUriMalformed = isAlloyDb && !isUriEmpty && !ALLOYDB_URI_RE.test(uriTrimmed);

  const dbTrimmed = database.trim();
  const isDatabaseEmpty = isAlloyDb && dbTrimmed.length === 0;
  const databaseError =
    isAlloyDb && !isDatabaseEmpty && !DATABASE_RE.test(dbTrimmed)
      ? 'Database name cannot contain spaces or / ? # @ :'
      : null;

  const schemaTrimmed = schemaName.trim();
  const isSchemaInvalid =
    isAlloyDb && (schemaTrimmed.length === 0 || !SQL_IDENT_RE.test(schemaTrimmed));

  const tableTrimmed = table.trim();
  const isTableEmpty = isAlloyDb && tableTrimmed.length === 0;
  const isTableMalformed = isAlloyDb && !isTableEmpty && !SQL_IDENT_RE.test(tableTrimmed);

  const isDbUserEmpty = isAlloyDb && dbUser.trim().length === 0;
  const isSecretRequiredError =
    isAlloyDb && authMode === 'secret_manager' && secretName.trim().length === 0
      ? 'Secret Manager secret name is required when password secret authentication is selected.'
      : null;

  // Shared and backend-specific column checks
  const idColTrimmed = idColumn.trim();
  const isIdColEmpty = idColTrimmed.length === 0;
  const isIdColInvalidSql = isAlloyDb && !isIdColEmpty && !SQL_IDENT_RE.test(idColTrimmed);

  const embColTrimmed = embeddingColumn.trim();
  const isEmbColEmpty = embColTrimmed.length === 0;
  const isEmbColInvalidSql = isAlloyDb && !isEmbColEmpty && !SQL_IDENT_RE.test(embColTrimmed);

  const contentTrimmed = contentColumn.trim();
  const isContentInvalidSql =
    isAlloyDb && contentTrimmed.length > 0 && !SQL_IDENT_RE.test(contentTrimmed);

  const metadataError = isAlloyDb ? validateMetadataColumns(metadataColumnsText) : null;

  // Vertex AI Vector Search validation checks
  const isProjectIdInvalid = !isAlloyDb && projectId.trim().length === 0;
  const isLocationInvalid = !isAlloyDb && location.trim().length === 0;
  const isIndexNameInvalid = !isAlloyDb && indexName.trim().length === 0;

  const systemInstructions = resolveSystemInstructions(node);

  const handleBackendChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextBackend = e.target.value as VectorStoreBackend;
    const nextBatchDefault = nextBackend === 'vertex_vector_search' ? 200 : 500;
    const shouldResetBatch =
      properties.batchSize === undefined ||
      (nextBackend === 'vertex_vector_search' && properties.batchSize === 500) ||
      (nextBackend === 'alloydb_pgvector' && properties.batchSize === 200);

    onChange({
      ...properties,
      backend: nextBackend,
      batchSize: shouldResetBatch ? nextBatchDefault : properties.batchSize,
    });
  };

  const handleFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    if (name === 'vectorDimensions' || name === 'batchSize') {
      const numeric = Number(value.trim());
      const resolved =
        value.trim() !== '' && Number.isInteger(numeric) && numeric > 0 ? numeric : value;
      onChange({
        ...properties,
        backend,
        [name]: resolved,
      });
      return;
    }

    onChange({
      ...properties,
      backend,
      [name]: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Architecture Banner */}
      <div
        data-testid="vector-store-guidance-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
            <Cpu className="w-4 h-4 text-blue-400 shrink-0" />
            <span>Vector Index Upsert Sink</span>
            <InfoTooltip text="Writes dense embedding arrays to AlloyDB pgvector or Vertex AI Vector Search using once-per-partition clients and foreachBatch streaming micro-batches." />
          </div>
          <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 font-mono text-[11px]">
            foreachPartition · foreachBatch
          </span>
        </div>
        {isAlloyDb ? (
          <span className="text-[11px] text-amber-300 font-mono">
            Managed Spark property: dataproc.artifacts.remove=conscrypt
          </span>
        ) : (
          <span className="text-[11px] text-[var(--vscode-text-muted)]">
            Initializes Vertex AI MatchingEngineIndex once per executor partition or exports JSONL to Cloud Storage.
          </span>
        )}
      </div>

      {/* Backend Selector */}
      <SelectField
        id="vectorStoreBackend"
        name="backend"
        label="Vector Store Backend"
        tooltip="Choose AlloyDB for PostgreSQL pgvector upserts or Vertex AI Vector Search index ingestion."
        value={backend}
        options={BACKEND_OPTIONS}
        onChange={handleBackendChange}
      />

      {/* AlloyDB pgvector Configuration */}
      {isAlloyDb && (
        <>
          <PropertyField
            id="vectorStoreAlloydbInstanceUri"
            name="alloydbInstanceUri"
            label="AlloyDB Instance URI"
            required="AlloyDB instance URI is required"
            tooltip="Full AlloyDB instance resource URI (projects/<project>/locations/<region>/clusters/<cluster>/instances/<instance>)."
            value={alloydbInstanceUri}
            invalid={isUriEmpty || isUriMalformed}
            error={
              isUriEmpty
                ? 'AlloyDB instance URI is required.'
                : isUriMalformed
                  ? 'Use projects/<project>/locations/<region>/clusters/<cluster>/instances/<instance>.'
                  : null
            }
            onChange={handleFieldChange}
            placeholder="projects/my-project/locations/us-central1/clusters/rag-cluster/instances/primary"
            mono={true}
          />

          <div className="grid grid-cols-2 gap-2">
            <SelectField
              id="vectorStoreIpType"
              name="ipType"
              label="IP Connectivity Type"
              tooltip="Network path used by the AlloyDB Connector (PRIVATE VPC peering, PUBLIC IP, or PSC Private Service Connect)."
              value={ipType}
              options={IP_TYPE_OPTIONS}
              onChange={handleFieldChange}
            />

            <PropertyField
              id="vectorStoreDatabase"
              name="database"
              label="Database"
              required="Database is required"
              tooltip="PostgreSQL database name hosting the pgvector extension."
              value={database}
              invalid={isDatabaseEmpty || Boolean(databaseError)}
              error={isDatabaseEmpty ? 'Database is required.' : databaseError}
              onChange={handleFieldChange}
              placeholder="postgres"
              mono={true}
            />
          </div>

          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="vectorStoreSchemaName"
              name="schemaName"
              label="Schema Name"
              required="Schema name is required"
              tooltip="Target PostgreSQL schema validated against SQL identifier rules (default public)."
              value={schemaName}
              invalid={isSchemaInvalid}
              error={
                isSchemaInvalid
                  ? 'Schema name must be a valid SQL identifier (letters, digits, underscores).'
                  : null
              }
              onChange={handleFieldChange}
              placeholder="public"
              mono={true}
            />

            <PropertyField
              id="vectorStoreTable"
              name="table"
              label="Table Name"
              required="Table name is required"
              tooltip="Destination pgvector table name without schema prefix, validated against SQL identifier rules."
              value={table}
              invalid={isTableEmpty || isTableMalformed}
              error={
                isTableEmpty
                  ? 'Table name is required.'
                  : isTableMalformed
                    ? 'Table name must be a valid SQL identifier (letters, digits, underscores).'
                    : null
              }
              onChange={handleFieldChange}
              placeholder="document_chunks"
              mono={true}
            />
          </div>

          {/* Column Mapping & Vector Schema */}
          <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
            <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
              pgvector Column Mapping & Upsert Policy
            </span>

            <div className="grid grid-cols-2 gap-2">
              <PropertyField
                id="vectorStoreIdColumn"
                name="idColumn"
                label="ID Column (Primary Key)"
                required="ID column is required"
                tooltip="Unique primary key column targeted by ON CONFLICT upserts (default chunk_id)."
                value={idColumn}
                invalid={isIdColEmpty || isIdColInvalidSql}
                error={
                  isIdColEmpty
                    ? 'ID column is required.'
                    : isIdColInvalidSql
                      ? 'ID column must be a valid SQL identifier.'
                      : null
                }
                onChange={handleFieldChange}
                placeholder="chunk_id"
                mono={true}
              />

              <PropertyField
                id="vectorStoreEmbeddingColumn"
                name="embeddingColumn"
                label="Embedding Column"
                required="Embedding column is required"
                tooltip="Input ARRAY<FLOAT> column cast to ::vector(vectorDimensions) during upsert."
                value={embeddingColumn}
                invalid={isEmbColEmpty || isEmbColInvalidSql}
                error={
                  isEmbColEmpty
                    ? 'Embedding column is required.'
                    : isEmbColInvalidSql
                      ? 'Embedding column must be a valid SQL identifier.'
                      : null
                }
                onChange={handleFieldChange}
                placeholder="embedding"
                mono={true}
              />
            </div>

            <div className="grid grid-cols-2 gap-2">
              <PropertyField
                id="vectorStoreContentColumn"
                name="contentColumn"
                label="Content Column"
                tooltip="Optional source chunk text column persisted alongside the embedding vector (default chunk_text)."
                value={contentColumn}
                invalid={isContentInvalidSql}
                error={
                  isContentInvalidSql ? 'Content column must be a valid SQL identifier.' : null
                }
                onChange={handleFieldChange}
                placeholder="chunk_text"
                mono={true}
              />

              <PropertyField
                id="vectorStoreVectorDimensions"
                name="vectorDimensions"
                label="Vector Dimensions"
                type="number"
                min={1}
                step={1}
                required="Vector dimensions is required"
                tooltip="Fixed dimensionality cast in SQL via ::vector(N). Must be a positive integer matching your embedding model (default 768)."
                value={vectorDimensionsText}
                invalid={isVectorDimensionsInvalid}
                error={
                  isVectorDimensionsInvalid
                    ? 'Vector dimensions must be a positive integer greater than 0.'
                    : null
                }
                onChange={handleFieldChange}
                placeholder="768"
                mono={true}
              />
            </div>

            <PropertyField
              id="vectorStoreMetadataColumns"
              name="metadataColumns"
              label="Metadata Columns (Optional)"
              tooltip="Comma-separated upstream metadata columns persisted alongside each chunk row."
              value={metadataColumnsText}
              invalid={Boolean(metadataError)}
              error={metadataError}
              onChange={handleFieldChange}
              placeholder="doc_id, source_uri, updated_at"
              mono={true}
            />

            <div className="grid grid-cols-2 gap-2">
              <SelectField
                id="vectorStoreUpsertConflictAction"
                name="upsertConflictAction"
                label="Upsert Conflict Action"
                tooltip="PostgreSQL ON CONFLICT policy applied when a chunk ID already exists in the target table."
                value={upsertConflictAction}
                options={CONFLICT_ACTION_OPTIONS}
                onChange={handleFieldChange}
              />

              <PropertyField
                id="vectorStoreBatchSize"
                name="batchSize"
                label="Upsert Batch Size"
                type="number"
                min={1}
                step={1}
                required="Batch size is required"
                tooltip="Number of rows grouped per parameterized INSERT ... ON CONFLICT statement inside foreachPartition (default 500)."
                value={batchSizeText}
                invalid={isBatchSizeInvalid}
                error={
                  isBatchSizeInvalid ? 'Batch size must be a positive integer greater than 0.' : null
                }
                onChange={handleFieldChange}
                placeholder="500"
                mono={true}
              />
            </div>
          </div>

          {/* AlloyDB Authentication */}
          <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
            <PropertyField
              id="vectorStoreDbUser"
              name="dbUser"
              label="Database User"
              required="Database user is required"
              tooltip="AlloyDB PostgreSQL database user or IAM service account principal."
              value={dbUser}
              invalid={isDbUserEmpty}
              error={isDbUserEmpty ? 'Database user is required.' : null}
              onChange={handleFieldChange}
              placeholder="postgres"
              mono={true}
            />

            <SecretManagerAuthFields
              idPrefix="vectorStoreSink"
              serviceName="AlloyDB pgvector"
              authMode={authMode}
              secretName={secretName}
              authOptions={ALLOYDB_AUTH_OPTIONS}
              secretManagerValue="secret_manager"
              datalistId="vector-store-sink-secret-options"
              secretPlaceholder="e.g. alloydb-pgvector-password"
              secretError={isSecretRequiredError}
              onChange={handleFieldChange}
            />
          </div>

          {/* Optional JVM Socket Factory JAR Overrides */}
          <CollapsibleSection
            id="vectorStoreJarSection"
            title="Optional AlloyDB JVM Socket Factory JARs"
            icon={<Database className="w-4 h-4 text-slate-400" />}
            isOpen={isJarsOpen}
            onToggle={() => setIsJarsOpen((prev) => !prev)}
          >
            <PropertyField
              id="vectorStoreSocketFactoryJarUri"
              name="socketFactoryJarUri"
              label="AlloyDB Socket Factory JAR URI (gs://)"
              tooltip="Optional Cloud Storage URI for the AlloyDB JDBC Socket Factory Uber JAR attached via --jars."
              value={socketFactoryJarUri}
              onChange={handleFieldChange}
              placeholder="gs://my-bucket/jars/alloydb-jdbc-connector-1.2.0-jar-with-dependencies.jar"
              mono={true}
            />

            <PropertyField
              id="vectorStoreDriverJarUri"
              name="driverJarUri"
              label="PostgreSQL Driver JAR URI (gs://)"
              tooltip="Optional Cloud Storage URI for the PostgreSQL JDBC driver JAR attached via --jars."
              value={driverJarUri}
              onChange={handleFieldChange}
              placeholder="gs://my-bucket/jars/postgresql-42.7.4.jar"
              mono={true}
            />
          </CollapsibleSection>
        </>
      )}

      {/* Vertex AI Vector Search Configuration */}
      {!isAlloyDb && (
        <>
          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="vectorStoreProjectId"
              name="projectId"
              label="GCP Project ID"
              required="GCP project ID is required"
              tooltip="Google Cloud project hosting the Vertex AI Vector Search index."
              value={projectId}
              invalid={isProjectIdInvalid}
              error={isProjectIdInvalid ? 'GCP project ID is required.' : null}
              onChange={handleFieldChange}
              placeholder="my-gcp-project"
              mono={true}
            />

            <PropertyField
              id="vectorStoreLocation"
              name="location"
              label="Vertex AI Location"
              required="Location is required"
              tooltip="Google Cloud region hosting the Vertex AI Vector Search index (default us-central1)."
              value={location}
              invalid={isLocationInvalid}
              error={isLocationInvalid ? 'Location is required.' : null}
              onChange={handleFieldChange}
              placeholder="us-central1"
              mono={true}
            />
          </div>

          <PropertyField
            id="vectorStoreIndexName"
            name="indexName"
            label="Vector Search Index Name / ID"
            required="Index name is required"
            tooltip="Vertex AI MatchingEngineIndex resource name (projects/.../locations/.../indexes/...) or numeric index ID."
            value={indexName}
            invalid={isIndexNameInvalid}
            error={isIndexNameInvalid ? 'Vector Search index name is required.' : null}
            onChange={handleFieldChange}
            placeholder="projects/my-project/locations/us-central1/indexes/1234567890"
            mono={true}
          />

          <SelectField
            id="vectorStoreWriteMode"
            name="writeMode"
            label="Index Write Mode"
            tooltip="Choose direct streaming datapoint upserts via MatchingEngineIndex.upsert_datapoints or Cloud Storage JSONL files for batch index updates."
            value={writeMode}
            options={VERTEX_WRITE_MODE_OPTIONS}
            onChange={handleFieldChange}
          />

          {writeMode === 'gcs_batch_json' && (
            <StoragePathField
              id="vectorStoreGcsOutputUri"
              name="gcsOutputUri"
              label="GCS Batch JSONL Output URI"
              tooltip="Cloud Storage folder URI where Vertex AI Vector Search JSONL datapoint files (id, embedding, restricts) are written."
              value={gcsOutputUri}
              onChange={handleFieldChange}
              placeholder="gs://my-rag-bucket/vector-search/batch-001/"
              required={true}
              errorMessage="GCS output URI is required when write mode is GCS Batch JSONL Export."
            />
          )}

          <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
            <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
              Datapoint Column Mapping & Batching
            </span>

            <div className="grid grid-cols-2 gap-2">
              <PropertyField
                id="vectorStoreVertexIdColumn"
                name="idColumn"
                label="Datapoint ID Column"
                required="ID column is required"
                tooltip="DataFrame column mapped to datapoint_id (or id in GCS JSONL exports)."
                value={idColumn}
                invalid={isIdColEmpty}
                error={isIdColEmpty ? 'ID column is required.' : null}
                onChange={handleFieldChange}
                placeholder="chunk_id"
                mono={true}
              />

              <PropertyField
                id="vectorStoreVertexEmbeddingColumn"
                name="embeddingColumn"
                label="Embedding Vector Column"
                required="Embedding column is required"
                tooltip="DataFrame ARRAY<FLOAT> column mapped to feature_vector (or embedding in GCS JSONL exports)."
                value={embeddingColumn}
                invalid={isEmbColEmpty}
                error={isEmbColEmpty ? 'Embedding column is required.' : null}
                onChange={handleFieldChange}
                placeholder="embedding"
                mono={true}
              />
            </div>

            <div className="grid grid-cols-2 gap-2">
              <PropertyField
                id="vectorStoreRestrictsColumn"
                name="restrictsColumn"
                label="Restricts Column (Optional)"
                tooltip="Optional DataFrame column containing namespace filter restricts or numeric_restricts structures."
                value={restrictsColumn}
                onChange={handleFieldChange}
                placeholder="restricts"
                mono={true}
              />

              <PropertyField
                id="vectorStoreCrowdingTagColumn"
                name="crowdingTagColumn"
                label="Crowding Tag Column (Optional)"
                tooltip="Optional DataFrame column containing per-datapoint crowding_attribute diversity tags."
                value={crowdingTagColumn}
                onChange={handleFieldChange}
                placeholder="doc_id"
                mono={true}
              />
            </div>

            <PropertyField
              id="vectorStoreVertexBatchSize"
              name="batchSize"
              label="Upsert Batch Size"
              type="number"
              min={1}
              step={1}
              required="Batch size is required"
              tooltip="Number of datapoints sent per MatchingEngineIndex.upsert_datapoints RPC call (default 200)."
              value={batchSizeText}
              invalid={isBatchSizeInvalid}
              error={
                isBatchSizeInvalid ? 'Batch size must be a positive integer greater than 0.' : null
              }
              onChange={handleFieldChange}
              placeholder="200"
              mono={true}
            />
          </div>
        </>
      )}

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="vectorStoreSinkSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions appended when generating PySpark VectorStoreSink code."
        placeholder="Enter custom instructions for pgvector HNSW indexing or Vertex AI Vector Search restricts..."
        onChange={(value) =>
          onChange({
            ...properties,
            backend,
            system_instructions: value,
          })
        }
      />
    </div>
  );
};
