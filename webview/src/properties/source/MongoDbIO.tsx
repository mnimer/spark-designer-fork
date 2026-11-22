/**
 * MongoDB Source Properties Component (SRC32)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark MongoDB Source nodes
 * (.format("mongodb") via mongo-spark-connector v10+ supporting batch and Change Streams).
 *
 * Architectural Decisions:
 * - Supports readMode ('batch' | 'streaming', default 'batch').
 * - Configures database, collection, aggregationPipeline, partitioner (batch),
 *   and changeStreamPublishFullDocumentOnly (when streaming).
 * - Uses SecretManagerAuthFields to resolve connection.uri from Google Cloud Secret Manager.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type AuthModeOption, type SecretItem } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };
export type MongoReadMode = 'batch' | 'streaming';

const MONGO_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'secretManager', label: 'Google Cloud Secret Manager (connection.uri Secret)' },
  { value: 'default', label: 'Custom / Local Connection URI' },
];

const MONGO_PARTITIONERS = [
  'com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner',
  'com.mongodb.spark.sql.connector.read.partitioner.PaginateBySizePartitioner',
  'com.mongodb.spark.sql.connector.read.partitioner.PaginateIntoPartitionsPartitioner',
  'com.mongodb.spark.sql.connector.read.partitioner.AutoBucketPartitioner',
  'com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner',
] as const;

/** Props for the {@link MongoDbIOSourceProperties} component. */
export interface MongoDbIOSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        readMode?: MongoReadMode;
        database?: string;
        collection?: string;
        aggregationPipeline?: string;
        filter?: string;
        partitioner?: string;
        changeStreamPublishFullDocumentOnly?: boolean;
        uri?: string;
        authMode?: string;
        secretName?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Configures properties for Apache Spark MongoDB batch and streaming source nodes (.format("mongodb")).
 */
export const MongoDbIOSourceProperties: React.FC<MongoDbIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const readMode: MongoReadMode = properties.readMode === 'streaming' ? 'streaming' : 'batch';
  const database = properties.database || '';
  const collection = properties.collection || '';
  const aggregationPipeline = properties.aggregationPipeline ?? properties.filter ?? '';
  const partitioner =
    properties.partitioner || 'com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner';
  const changeStreamPublishFullDocumentOnly =
    properties.changeStreamPublishFullDocumentOnly ?? true;
  const uri = properties.uri || '';
  const authMode = properties.authMode || 'secretManager';
  const secretName = properties.secretName || '';

  const isDatabaseEmpty = !database.trim();
  const isCollectionEmpty = !collection.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    onChange({ ...properties, [name]: updatedValue });
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark MongoDB Connector v10+</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          .format(&quot;mongodb&quot;) · {readMode.toUpperCase()}
        </span>
      </div>

      {/* Read Mode Selector */}
      <div className="flex flex-col gap-1.5">
        <label
          htmlFor="mongoSourceReadMode"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <span>Execution Read Mode</span>
          <InfoTooltip text="Choose batch collection scan (spark.read) or MongoDB Change Streams (spark.readStream)." />
        </label>
        <select
          id="mongoSourceReadMode"
          name="readMode"
          value={readMode}
          onChange={handleChange}
          className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
        >
          <option value="batch">Batch (spark.read.format(&quot;mongodb&quot;))</option>
          <option value="streaming">Streaming Change Streams (spark.readStream.format(&quot;mongodb&quot;))</option>
        </select>
      </div>

      {/* Database & Collection */}
      <div className="grid grid-cols-2 gap-2">
        <PropertyField
          id="mongoSourceDatabase"
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
          id="mongoSourceCollection"
          name="collection"
          label="Collection"
          required="Collection name is required"
          tooltip="Target MongoDB collection name (.option('collection', ...))."
          value={collection}
          invalid={isCollectionEmpty}
          error={isCollectionEmpty ? 'Collection name is required' : null}
          placeholder="events"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Aggregation Pipeline */}
      <PropertyField
        id="mongoSourceAggregationPipeline"
        name="aggregationPipeline"
        label="Aggregation Pipeline JSON (aggregation.pipeline)"
        type="textarea"
        rows={3}
        tooltip="Optional JSON array of MongoDB aggregation stages pushed down to MongoDB (.option('aggregation.pipeline', ...))."
        value={aggregationPipeline}
        placeholder='[{"$match": {"status": "active"}}]'
        onChange={handleChange}
        mono={true}
      />

      {/* Mode-specific Partitioner or Change Stream Options */}
      {readMode === 'batch' ? (
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="mongoSourcePartitioner"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Partitioner Strategy</span>
            <InfoTooltip text="MongoDB Spark V2 partitioner class used to split the collection across Spark tasks." />
          </label>
          <select
            id="mongoSourcePartitioner"
            name="partitioner"
            value={partitioner}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-xs font-mono focus:outline-none focus:border-blue-500"
          >
            {MONGO_PARTITIONERS.map((p) => (
              <option key={p} value={p}>
                {p.split('.').pop()} ({p})
              </option>
            ))}
          </select>
        </div>
      ) : (
        <div className="flex items-center gap-2 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
          <input
            id="mongoSourceChangeStreamFullDocumentOnly"
            type="checkbox"
            name="changeStreamPublishFullDocumentOnly"
            checked={changeStreamPublishFullDocumentOnly}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="mongoSourceChangeStreamFullDocumentOnly"
            className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
          >
            <span>Publish Full Document Only (change.stream.publish.full.document.only=true)</span>
            <InfoTooltip text="Emits the full updated document directly instead of the raw Change Stream envelope." />
          </label>
        </div>
      )}

      {/* Secret Manager Auth for connection.uri */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <SecretManagerAuthFields
          idPrefix="mongoSource"
          serviceName="MongoDB (spark.mongodb.read.connection.uri)"
          authMode={authMode}
          secretName={secretName}
          authOptions={MONGO_AUTH_OPTIONS}
          secretManagerValue="secretManager"
          datalistId="mongodb-source-secret-options"
          secretPlaceholder="e.g. mongodb-connection-uri-secret"
          onChange={handleChange}
        />

        {authMode !== 'secretManager' && (
          <PropertyField
            id="mongoSourceUri"
            name="uri"
            label="Connection URI (connection.uri)"
            tooltip="MongoDB connection URI when not using Secret Manager."
            value={uri}
            placeholder="mongodb://localhost:27017"
            onChange={handleChange}
            mono={true}
          />
        )}
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="mongoSourceInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark MongoDB source code generation..."
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

export const MongoDbSourceProperties = MongoDbIOSourceProperties;
