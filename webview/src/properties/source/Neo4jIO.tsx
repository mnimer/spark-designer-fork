/**
 * Neo4j Source Properties Component (SRC37)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Neo4j Source nodes
 * (.format("org.neo4j.spark.DataSource")).
 *
 * Architectural Decisions:
 * - Supports readMode ('batch' | 'streaming', default 'batch').
 * - Configures url ('bolt://...' or 'neo4j+s://...'), database (default 'neo4j'),
 *   queryMode ('labels' | 'relationship' | 'query'), and corresponding labels / relationship / query.
 * - When streaming, exposes streamingFrom ('NOW' | 'ALL') and streamingPropertyName.
 * - Uses SecretManagerAuthFields for Neo4j credentials.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type SecretItem } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };
export type Neo4jReadMode = 'batch' | 'streaming';
export type Neo4jQueryMode = 'labels' | 'relationship' | 'query';
export type Neo4jStreamingFrom = 'NOW' | 'ALL';

/** Props for the {@link Neo4jIOSourceProperties} component. */
export interface Neo4jIOSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        readMode?: Neo4jReadMode;
        url?: string;
        database?: string;
        queryMode?: Neo4jQueryMode;
        labels?: string;
        relationship?: string;
        query?: string;
        cypher?: string;
        streamingFrom?: Neo4jStreamingFrom;
        streamingPropertyName?: string;
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
 * Configures properties for Apache Spark Neo4j batch and streaming source nodes (.format("org.neo4j.spark.DataSource")).
 */
export const Neo4jIOSourceProperties: React.FC<Neo4jIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const readMode: Neo4jReadMode = properties.readMode === 'streaming' ? 'streaming' : 'batch';
  const url = properties.url || '';
  const database = properties.database !== undefined ? properties.database : 'neo4j';
  const query = properties.query ?? properties.cypher ?? '';
  const labels = properties.labels || '';
  const relationship = properties.relationship || '';
  const queryMode: Neo4jQueryMode =
    properties.queryMode || (query.trim() ? 'query' : relationship.trim() ? 'relationship' : 'labels');
  const streamingFrom: Neo4jStreamingFrom = properties.streamingFrom || 'NOW';
  const streamingPropertyName = properties.streamingPropertyName || 'timestamp';
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';

  const isUrlEmpty = !url.trim();
  const isLabelsEmpty = queryMode === 'labels' && !labels.trim();
  const isRelationshipEmpty = queryMode === 'relationship' && !relationship.trim();
  const isQueryEmpty = queryMode === 'query' && !query.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next = { ...properties, [name]: updatedValue };
    if (name === 'query') {
      delete next.cypher;
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark Neo4j Connector</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          org.neo4j.spark.DataSource · {readMode.toUpperCase()}
        </span>
      </div>

      {/* Read Mode & Connection */}
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="neo4jSourceReadMode"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Execution Read Mode</span>
            <InfoTooltip text="Choose batch graph extraction (spark.read) or streaming property polling (spark.readStream)." />
          </label>
          <select
            id="neo4jSourceReadMode"
            name="readMode"
            value={readMode}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="batch">Batch (spark.read)</option>
            <option value="streaming">Streaming (spark.readStream)</option>
          </select>
        </div>

        <PropertyField
          id="neo4jSourceDatabase"
          name="database"
          label="Database"
          tooltip="Target Neo4j database name (default 'neo4j')."
          value={database}
          placeholder="neo4j"
          onChange={handleChange}
          mono={true}
        />
      </div>

      <PropertyField
        id="neo4jSourceUrl"
        name="url"
        label="Bolt / Neo4j URL (url)"
        required="Connection URL is required"
        tooltip="Neo4j connection URI (for example bolt://localhost:7687 or neo4j+s://aura.databases.neo4j.io)."
        value={url}
        invalid={isUrlEmpty}
        error={isUrlEmpty ? 'Connection URL is required' : null}
        placeholder="neo4j+s://xxxx.databases.neo4j.io"
        onChange={handleChange}
        mono={true}
      />

      {/* Query Mode & Target */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="neo4jSourceQueryMode"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Graph Query Mode</span>
            <InfoTooltip text="Read graph entities by node labels, relationship type, or custom Cypher query." />
          </label>
          <select
            id="neo4jSourceQueryMode"
            name="queryMode"
            value={queryMode}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="labels">Node Labels (.option(&quot;labels&quot;, ...))</option>
            <option value="relationship">Relationship (.option(&quot;relationship&quot;, ...))</option>
            <option value="query">Cypher Query (.option(&quot;query&quot;, ...))</option>
          </select>
        </div>

        {queryMode === 'labels' && (
          <PropertyField
            id="neo4jSourceLabels"
            name="labels"
            label="Node Labels (labels)"
            required="Node labels are required in Labels mode"
            tooltip="Colon-prefixed Neo4j node label(s) to read (for example :Person or :Customer:VIP)."
            value={labels}
            invalid={isLabelsEmpty}
            error={isLabelsEmpty ? 'Node labels are required in Labels mode' : null}
            placeholder=":Person"
            onChange={handleChange}
            mono={true}
          />
        )}

        {queryMode === 'relationship' && (
          <PropertyField
            id="neo4jSourceRelationship"
            name="relationship"
            label="Relationship Type (relationship)"
            required="Relationship type is required in Relationship mode"
            tooltip="Neo4j relationship type to extract (for example KNOWS or PURCHASED)."
            value={relationship}
            invalid={isRelationshipEmpty}
            error={isRelationshipEmpty ? 'Relationship type is required in Relationship mode' : null}
            placeholder="PURCHASED"
            onChange={handleChange}
            mono={true}
          />
        )}

        {queryMode === 'query' && (
          <PropertyField
            id="neo4jSourceQuery"
            name="query"
            label="Cypher Query (query)"
            type="textarea"
            rows={3}
            required="Cypher query is required in Query mode"
            tooltip="Custom Cypher query executed by the Neo4j Spark Connector."
            value={query}
            invalid={isQueryEmpty}
            error={isQueryEmpty ? 'Cypher query is required in Query mode' : null}
            placeholder="MATCH (p:Person) RETURN p.id AS id, p.name AS name"
            onChange={handleChange}
            mono={true}
          />
        )}
      </div>

      {/* Streaming Options */}
      {readMode === 'streaming' && (
        <div className="grid grid-cols-2 gap-2 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="neo4jSourceStreamingFrom"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Streaming From (streaming.from)</span>
              <InfoTooltip text="Start streaming changes from NOW or read ALL existing records first." />
            </label>
            <select
              id="neo4jSourceStreamingFrom"
              name="streamingFrom"
              value={streamingFrom}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="NOW">NOW (New changes only)</option>
              <option value="ALL">ALL (Full snapshot + changes)</option>
            </select>
          </div>

          <PropertyField
            id="neo4jSourceStreamingPropertyName"
            name="streamingPropertyName"
            label="Streaming Property (streaming.property.name)"
            tooltip="Monotonically increasing timestamp/integer property tracked by Neo4j streaming."
            value={streamingPropertyName}
            placeholder="timestamp"
            onChange={handleChange}
            mono={true}
          />
        </div>
      )}

      {/* Secret Manager Authentication */}
      <SecretManagerAuthFields
        idPrefix="neo4jSource"
        serviceName="Neo4j"
        authMode={authMode}
        secretName={secretName}
        datalistId="neo4j-source-secret-options"
        secretPlaceholder="e.g. neo4j-password-secret"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="neo4jSourceInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Neo4j source code generation..."
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

export const Neo4jSourceProperties = Neo4jIOSourceProperties;
