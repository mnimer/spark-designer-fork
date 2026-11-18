/**
 * @fileoverview MongoDB Sink Properties Component (SNK29).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.x MongoDB Sink nodes
 * (.write.format("mongodb") and .writeStream.format("mongodb") via mongo-spark-connector v10.4+).
 *
 * Architectural Decisions:
 * - Supports batch save modes ('append' | 'overwrite') and native Structured Streaming writes.
 * - Configures database, collection, operationType ('insert' | 'replace' | 'update'),
 *   idFieldList, upsertDocument, maxBatchSize, ordered, and writeConcern.
 * - Uses SecretManagerAuthFields to resolve connection.uri from Google Cloud Secret Manager.
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
export type MongoOperationType = 'insert' | 'replace' | 'update';
export type MongoSaveMode = 'append' | 'overwrite';

const MONGO_SINK_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'secretManager', label: 'Google Cloud Secret Manager (connection.uri Secret)' },
  { value: 'default', label: 'Custom / Local Connection URI' },
];

/** Props for the {@link MongoDbIOSinkProperties} component. */
export interface MongoDbIOSinkPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        database?: string;
        collection?: string;
        saveMode?: MongoSaveMode | string;
        operationType?: MongoOperationType | string;
        writeMode?: string;
        idFieldList?: string;
        keyFields?: string;
        upsertDocument?: boolean;
        maxBatchSize?: number | string;
        batchSize?: number | string;
        ordered?: boolean;
        writeConcern?: string;
        authMode?: string;
        secretName?: string;
        uri?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Resolves the normalized MongoDB Spark Connector v10+ operationType from current or legacy properties.
 */
function resolveOperationType(properties: Record<string, unknown>): MongoOperationType {
  const rawOp = properties.operationType;
  if (rawOp === 'insert' || rawOp === 'replace' || rawOp === 'update') {
    return rawOp;
  }
  if (properties.writeMode === 'upsert') {
    return 'replace';
  }
  return 'insert';
}

/**
 * Configures properties for Apache Spark 4.x MongoDB batch and streaming sink nodes (.format("mongodb")).
 */
export const MongoDbIOSinkProperties: React.FC<MongoDbIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = (node?.data?.properties ?? {}) as Record<string, unknown>;

  const database = typeof properties.database === 'string' ? properties.database : '';
  const collection = typeof properties.collection === 'string' ? properties.collection : '';
  const saveMode: MongoSaveMode = properties.saveMode === 'overwrite' ? 'overwrite' : 'append';
  const operationType = resolveOperationType(properties);
  const idFieldList =
    typeof properties.idFieldList === 'string'
      ? properties.idFieldList
      : typeof properties.keyFields === 'string'
        ? properties.keyFields
        : '';
  const upsertDocument =
    typeof properties.upsertDocument === 'boolean' ? properties.upsertDocument : true;
  const rawBatchSize = properties.maxBatchSize ?? properties.batchSize;
  const maxBatchSize =
    typeof rawBatchSize === 'number' || typeof rawBatchSize === 'string' ? rawBatchSize : '';
  const ordered = typeof properties.ordered === 'boolean' ? properties.ordered : true;
  const writeConcern =
    typeof properties.writeConcern === 'string' && properties.writeConcern.length > 0
      ? properties.writeConcern
      : 'majority';
  const authMode =
    typeof properties.authMode === 'string' && properties.authMode.length > 0
      ? properties.authMode
      : 'secretManager';
  const secretName = typeof properties.secretName === 'string' ? properties.secretName : '';
  const uri = typeof properties.uri === 'string' ? properties.uri : '';

  const isDatabaseEmpty = !database.trim();
  const isCollectionEmpty = !collection.trim();
  const requiresIdFields = operationType === 'replace' || operationType === 'update';
  const isIdFieldListEmpty = requiresIdFields && !idFieldList.trim();
  const isUriMissingScheme =
    authMode !== 'secretManager' &&
    uri.trim().length > 0 &&
    !uri.trim().startsWith('mongodb://') &&
    !uri.trim().startsWith('mongodb+srv://');

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    if (name === 'operationType') {
      delete next.writeMode;
    }
    if (name === 'idFieldList') {
      delete next.keyFields;
    }
    if (name === 'maxBatchSize') {
      delete next.batchSize;
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark MongoDB Connector v10+</span>
        <span className="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300 font-mono text-[11px]">
          .format(&quot;mongodb&quot;) · NATIVE
        </span>
      </div>

      {/* Database & Collection */}
      <div className="grid grid-cols-2 gap-2">
        <PropertyField
          id="mongoSinkDatabase"
          name="database"
          label="Database"
          required="Database name is required"
          tooltip="Target MongoDB database name (.option('database', ...))."
          value={database}
          invalid={isDatabaseEmpty}
          error={isDatabaseEmpty ? 'Database name is required' : null}
          placeholder="analytics"
          onChange={handleChange}
          mono={true}
        />
        <PropertyField
          id="mongoSinkCollection"
          name="collection"
          label="Collection"
          required="Collection name is required"
          tooltip="Target MongoDB collection name (.option('collection', ...))."
          value={collection}
          invalid={isCollectionEmpty}
          error={isCollectionEmpty ? 'Collection name is required' : null}
          placeholder="aggregated_metrics"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Save Mode & Operation Type */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="mongoSinkSaveMode"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Batch Save Mode</span>
              <InfoTooltip text="Spark batch DataFrameWriter save mode. Streaming writes use the query outputMode." />
            </label>
            <select
              id="mongoSinkSaveMode"
              name="saveMode"
              value={saveMode}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="append">append (Write new or upserted documents)</option>
              <option value="overwrite">overwrite (Drop and recreate collection)</option>
            </select>
          </div>

          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="mongoSinkOperationType"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Operation Type (operationType)</span>
              <InfoTooltip text="MongoDB Spark Connector bulk write operation: insert, replace, or update." />
            </label>
            <select
              id="mongoSinkOperationType"
              name="operationType"
              value={operationType}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="insert">insert (Append documents)</option>
              <option value="replace">replace (Replace document by key)</option>
              <option value="update">update (Update fields by key)</option>
            </select>
          </div>
        </div>

        {requiresIdFields && (
          <div className="flex flex-col gap-2 pt-1 border-t border-slate-700/50">
            <PropertyField
              id="mongoSinkIdFieldList"
              name="idFieldList"
              label="Document Key Fields (idFieldList)"
              required="Key field list is required for replace or update operations"
              tooltip="Comma-separated column names used to match existing documents (.option('idFieldList', ...))."
              value={idFieldList}
              invalid={isIdFieldListEmpty}
              error={
                isIdFieldListEmpty
                  ? 'Key field list is required for replace or update operations'
                  : null
              }
              placeholder="_id"
              onChange={handleChange}
              mono={true}
            />

            <div className="flex items-center gap-2">
              <input
                id="mongoSinkUpsertDocument"
                type="checkbox"
                name="upsertDocument"
                checked={upsertDocument}
                onChange={handleChange}
                className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
              />
              <label
                htmlFor="mongoSinkUpsertDocument"
                className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
              >
                <span>Upsert Unmatched Documents (upsertDocument=true)</span>
                <InfoTooltip text="If enabled, inserts a new document when no existing document matches idFieldList." />
              </label>
            </div>
          </div>
        )}
      </div>

      {/* Bulk Write Tuning */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Bulk Write Tuning</span>
          <InfoTooltip text="Configure maxBatchSize, ordered bulk execution, and replica write concern." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="mongoSinkMaxBatchSize"
            name="maxBatchSize"
            label="Max Batch Size (maxBatchSize)"
            type="number"
            min={1}
            tooltip="Maximum number of operations grouped per MongoDB bulkWrite call (default 512)."
            value={maxBatchSize}
            placeholder="512"
            onChange={handleChange}
            mono={true}
          />

          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="mongoSinkWriteConcern"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Write Concern (writeConcern.w)</span>
              <InfoTooltip text="Replica acknowledgment level for MongoDB write operations." />
            </label>
            <select
              id="mongoSinkWriteConcern"
              name="writeConcern"
              value={writeConcern}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="majority">majority (Quorum)</option>
              <option value="1">1 (Primary acknowledged)</option>
              <option value="0">0 (Unacknowledged)</option>
            </select>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <input
            id="mongoSinkOrdered"
            type="checkbox"
            name="ordered"
            checked={ordered}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="mongoSinkOrdered"
            className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
          >
            <span>Ordered Bulk Writes (ordered=true)</span>
            <InfoTooltip text="If enabled, stops processing remaining operations in a batch after the first failure." />
          </label>
        </div>
      </div>

      {/* Secret Manager Auth for connection.uri */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <SecretManagerAuthFields
          idPrefix="mongoSink"
          serviceName="MongoDB (spark.mongodb.write.connection.uri)"
          authMode={authMode}
          secretName={secretName}
          authOptions={MONGO_SINK_AUTH_OPTIONS}
          secretManagerValue="secretManager"
          datalistId="mongodb-sink-secret-options"
          secretPlaceholder="e.g. mongodb-connection-uri-secret"
          onChange={handleChange}
        />

        {authMode !== 'secretManager' && (
          <PropertyField
            id="mongoSinkUri"
            name="uri"
            label="Connection URI (connection.uri)"
            tooltip="MongoDB connection URI used when Secret Manager is not active."
            value={uri}
            helperText={
              isUriMissingScheme ? (
                <span className="text-amber-400" role="alert">
                  Connection URI must start with mongodb:// or mongodb+srv://
                </span>
              ) : null
            }
            placeholder="mongodb://localhost:27017"
            onChange={handleChange}
            mono={true}
          />
        )}
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="mongoSinkInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark MongoDB sink code generation..."
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

export const MongoDbSinkProperties = MongoDbIOSinkProperties;
