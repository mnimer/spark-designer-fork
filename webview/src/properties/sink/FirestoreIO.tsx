/**
 * @fileoverview Firestore Sink Properties Component (SNK30).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud Firestore Sink nodes
 * in Apache Spark 4.x pipelines using the Spark 4.0 Python DataSourceWriter API
 * and Structured Streaming foreachBatch (google-cloud-firestore WriteBatch).
 *
 * Architectural Decisions:
 * - Configures projectId, databaseId (default '(default)'), and collectionPath.
 * - Supports Firestore WriteBatch mutation operations ('set_merge' | 'set' | 'update' | 'delete'),
 *   documentIdColumn, mergeFields, and maxBatchSize (capped at 500 mutations per commit).
 * - Uses SecretManagerAuthFields for Application Default Credentials or Secret Manager JSON keys.
 */

import type React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import {
  SecretManagerAuthFields,
  type AuthModeOption,
  type SecretItem,
} from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };
export type FirestoreWriteOperation = 'set_merge' | 'set' | 'update' | 'delete';

const FIRESTORE_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'adc', label: 'Application Default Credentials (IAM / Managed Spark SA)' },
  { value: 'secretManager', label: 'Google Cloud Secret Manager (Service Account JSON)' },
];

/** Props for the {@link FirestoreIOSinkProperties} component. */
export interface FirestoreIOSinkPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        projectId?: string;
        databaseId?: string;
        collectionPath?: string;
        targetCollection?: string;
        operation?: FirestoreWriteOperation | string;
        documentIdColumn?: string;
        documentIdField?: string;
        mergeFields?: string;
        maxBatchSize?: number | string;
        batchSize?: number | string;
        authMode?: string;
        secretName?: string;
        serviceAccountSecret?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Resolves the normalized Firestore WriteBatch operation from current or legacy properties.
 */
function resolveFirestoreOperation(properties: Record<string, unknown>): FirestoreWriteOperation {
  const raw = properties.operation;
  if (raw === 'set_merge' || raw === 'set' || raw === 'update' || raw === 'delete') {
    return raw;
  }
  return 'set_merge';
}

/**
 * Configures properties for Apache Spark 4.x Firestore sink nodes (Python DataSourceWriter / foreachBatch).
 */
export const FirestoreIOSinkProperties: React.FC<FirestoreIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = (node?.data?.properties ?? {}) as Record<string, unknown>;

  const projectId = typeof properties.projectId === 'string' ? properties.projectId : '';
  const databaseId =
    typeof properties.databaseId === 'string' && properties.databaseId.length > 0
      ? properties.databaseId
      : '(default)';
  const collectionPath =
    typeof properties.collectionPath === 'string'
      ? properties.collectionPath
      : typeof properties.targetCollection === 'string'
        ? properties.targetCollection
        : '';
  const operation = resolveFirestoreOperation(properties);
  const documentIdColumn =
    typeof properties.documentIdColumn === 'string'
      ? properties.documentIdColumn
      : typeof properties.documentIdField === 'string'
        ? properties.documentIdField
        : '';
  const mergeFields = typeof properties.mergeFields === 'string' ? properties.mergeFields : '';
  const rawBatchSize = properties.maxBatchSize ?? properties.batchSize;
  const maxBatchSize =
    typeof rawBatchSize === 'number' || typeof rawBatchSize === 'string' ? rawBatchSize : 500;
  const rawAuthMode = properties.authMode;
  const authMode =
    rawAuthMode === 'secretManager' || rawAuthMode === 'secret_manager' ? 'secretManager' : 'adc';
  const secretName =
    typeof properties.secretName === 'string'
      ? properties.secretName
      : typeof properties.serviceAccountSecret === 'string'
        ? properties.serviceAccountSecret
        : '';

  const isProjectEmpty = !projectId.trim();
  const isCollectionEmpty = !collectionPath.trim();
  const requiresDocId = operation === 'update' || operation === 'delete';
  const isDocIdEmpty = requiresDocId && !documentIdColumn.trim();
  const numericBatchSize = Number(maxBatchSize);
  const exceedsBatchCap =
    String(maxBatchSize).trim().length > 0 &&
    !Number.isNaN(numericBatchSize) &&
    numericBatchSize > 500;

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    if (name === 'collectionPath') {
      delete next.targetCollection;
    }
    if (name === 'documentIdColumn') {
      delete next.documentIdField;
    }
    if (name === 'maxBatchSize') {
      delete next.batchSize;
    }
    if (name === 'secretName') {
      delete next.serviceAccountSecret;
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark 4.0 Python DataSourceWriter (Firestore)</span>
        <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 font-mono text-[11px]">
          WriteBatch · FOREACHBATCH
        </span>
      </div>

      {/* Firestore Target Coordinates */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Firestore Collection Coordinates</span>
          <InfoTooltip text="Configure GCP Project ID, Firestore Database ID, and target collection path." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="firestoreSinkProjectId"
            name="projectId"
            label="GCP Project ID"
            required="Project ID is required"
            tooltip="Google Cloud Project ID hosting the destination Firestore database."
            value={projectId}
            invalid={isProjectEmpty}
            error={isProjectEmpty ? 'Project ID is required' : null}
            placeholder="my-gcp-project"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="firestoreSinkDatabaseId"
            name="databaseId"
            label="Database ID"
            tooltip="Firestore database identifier (defaults to '(default)')."
            value={databaseId}
            placeholder="(default)"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="firestoreSinkCollectionPath"
          name="collectionPath"
          label="Collection Path"
          required="Collection path is required"
          tooltip="Destination Firestore collection or subcollection path (for example processed_events)."
          value={collectionPath}
          invalid={isCollectionEmpty}
          error={isCollectionEmpty ? 'Collection path is required' : null}
          placeholder="processed_events"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* WriteBatch Mutation & Batching */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>WriteBatch Mutation &amp; Document Key</span>
          <InfoTooltip text="Configure Firestore WriteBatch mutation semantics and batch commit size (maximum 500 mutations per commit)." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="firestoreSinkOperation"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Write Operation</span>
              <InfoTooltip text="Choose set(merge=True) for idempotent upserts, set for full overwrite, update, or delete." />
            </label>
            <select
              id="firestoreSinkOperation"
              name="operation"
              value={operation}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="set_merge">set(..., merge=True) (Upsert)</option>
              <option value="set">set(...) (Overwrite document)</option>
              <option value="update">update(...) (Update existing)</option>
              <option value="delete">delete() (Delete by ID)</option>
            </select>
          </div>

          <PropertyField
            id="firestoreSinkMaxBatchSize"
            name="maxBatchSize"
            label="Mutations per Batch (<=500)"
            type="number"
            min={1}
            max={500}
            tooltip="Number of mutations committed per Firestore WriteBatch RPC (hard cap is 500)."
            value={maxBatchSize}
            helperText={
              exceedsBatchCap ? (
                <span className="text-amber-400" role="alert">
                  Firestore caps each WriteBatch commit at 500 mutations
                </span>
              ) : null
            }
            placeholder="500"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="firestoreSinkDocumentIdColumn"
          name="documentIdColumn"
          label="Document ID Column (documentIdColumn)"
          required={requiresDocId ? 'Document ID column is required for update or delete' : undefined}
          tooltip="Input DataFrame column used as the Firestore document ID. Leave blank in set modes only when auto-generated IDs are desired."
          value={documentIdColumn}
          invalid={isDocIdEmpty}
          error={isDocIdEmpty ? 'Document ID column is required for update or delete' : null}
          placeholder="document_id"
          onChange={handleChange}
          mono={true}
        />

        {operation === 'set_merge' && (
          <PropertyField
            id="firestoreSinkMergeFields"
            name="mergeFields"
            label="Merge Fields (Optional)"
            tooltip="Optional comma-separated list of field paths to merge. If blank, merges all present DataFrame columns."
            value={mergeFields}
            placeholder="status, updated_at"
            onChange={handleChange}
            mono={true}
          />
        )}
      </div>

      {/* Authentication */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <SecretManagerAuthFields
          idPrefix="firestoreSink"
          serviceName="Google Cloud Firestore"
          authMode={authMode}
          secretName={secretName}
          authOptions={FIRESTORE_AUTH_OPTIONS}
          secretManagerValue="secretManager"
          datalistId="firestore-sink-secret-options"
          secretPlaceholder="e.g. firestore-sa-key-secret"
          onChange={handleChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="firestoreSinkInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Firestore WriteBatch sink code generation..."
        onChange={(value) =>
          onChange({
            ...properties,
            system_instructions: value,
          })
        }
      />
    </div>
  );
};

export const FirestoreSinkProperties = FirestoreIOSinkProperties;
