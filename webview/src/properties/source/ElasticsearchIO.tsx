/**
 * Elasticsearch Source Properties Component (SRC36)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Elasticsearch Source nodes
 * (.format("org.elasticsearch.spark.sql")).
 *
 * Architectural Decisions:
 * - Batch-only source configuring es.nodes (nodes), es.port (port, default 9200),
 *   es.resource (resource / index), es.query (Query DSL JSON), and es.nodes.wan.only (wanOnly, default true).
 * - Uses SecretManagerAuthFields for Elasticsearch credentials.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type SecretItem } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };

/** Props for the {@link ElasticsearchIOSourceProperties} component. */
export interface ElasticsearchIOSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        nodes?: string;
        addresses?: string;
        port?: number | string;
        resource?: string;
        index?: string;
        query?: string;
        wanOnly?: boolean;
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
 * Configures properties for Apache Spark Elasticsearch batch source nodes (.format("org.elasticsearch.spark.sql")).
 */
export const ElasticsearchIOSourceProperties: React.FC<ElasticsearchIOSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const nodes = properties.nodes ?? properties.addresses ?? '';
  const port = properties.port !== undefined && properties.port !== '' ? properties.port : 9200;
  const resource = properties.resource ?? properties.index ?? '';
  const query = properties.query || '';
  const wanOnly = properties.wanOnly ?? true;
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';

  const isNodesEmpty = !String(nodes).trim();
  const isResourceEmpty = !String(resource).trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next = { ...properties, [name]: updatedValue };
    if (name === 'nodes') {
      delete next.addresses;
    }
    if (name === 'resource') {
      next.index = String(updatedValue);
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark Elasticsearch Connector</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          org.elasticsearch.spark.sql · BATCH
        </span>
      </div>

      {/* Nodes, Port, Resource */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="esSourceNodes"
            name="nodes"
            label="Elasticsearch Nodes (es.nodes)"
            required="Elasticsearch nodes are required"
            tooltip="Comma-separated Elasticsearch hostnames or endpoints."
            value={nodes}
            invalid={isNodesEmpty}
            error={isNodesEmpty ? 'Elasticsearch nodes are required' : null}
            placeholder="es-cluster.internal"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="esSourcePort"
            name="port"
            label="Port (es.port)"
            type="number"
            tooltip="Elasticsearch HTTP/HTTPS port (default 9200)."
            value={port}
            placeholder="9200"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="esSourceResource"
          name="resource"
          label="Index / Resource (es.resource)"
          required="Index resource is required"
          tooltip="Target Elasticsearch index or data stream name."
          value={resource}
          invalid={isResourceEmpty}
          error={isResourceEmpty ? 'Index resource is required' : null}
          placeholder="logs-production"
          onChange={handleChange}
          mono={true}
        />

        <div className="flex items-center gap-2">
          <input
            id="esSourceWanOnly"
            type="checkbox"
            name="wanOnly"
            checked={wanOnly}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="esSourceWanOnly"
            className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
          >
            <span>WAN Only Mode (es.nodes.wan.only=true)</span>
            <InfoTooltip text="Required for Cloud / VPC load-balanced Elasticsearch endpoints to disable direct shard node discovery." />
          </label>
        </div>
      </div>

      {/* Query DSL JSON */}
      <PropertyField
        id="esSourceQuery"
        name="query"
        label="Query DSL JSON (es.query)"
        type="textarea"
        rows={4}
        tooltip="Optional Elasticsearch Query DSL JSON pushed down to search shards."
        value={query}
        placeholder='{"query": {"match_all": {}}}'
        onChange={handleChange}
        mono={true}
      />

      {/* Secret Manager Authentication */}
      <SecretManagerAuthFields
        idPrefix="esSource"
        serviceName="Elasticsearch"
        authMode={authMode}
        secretName={secretName}
        datalistId="elasticsearch-source-secret-options"
        secretPlaceholder="e.g. elasticsearch-password-secret"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="es-source-instructions"
        value={systemInstructions}
        rows={4}
        placeholder="Enter custom instructions for Spark Elasticsearch source code generation..."
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

export const ElasticsearchSourceProperties = ElasticsearchIOSourceProperties;
