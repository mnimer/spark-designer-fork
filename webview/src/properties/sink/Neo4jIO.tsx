/**
 * @fileoverview Neo4j Sink Properties Component (SNK34).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.x Neo4j Sink nodes
 * (.write.format("org.neo4j.spark.DataSource") and .writeStream.format("org.neo4j.spark.DataSource")).
 *
 * Architectural Decisions:
 * - Configures Bolt / Neo4j Aura connection URL, database (default 'neo4j'), and saveMode ('Overwrite' | 'Append').
 * - Supports three Neo4j Spark Connector write targets:
 *   1. Node Labels (labels + node.keys)
 *   2. Relationship (relationship + source/target labels and node.keys)
 *   3. Custom Cypher MERGE query (query + batch.size)
 * - Uses SecretManagerAuthFields for Bolt password resolution.
 */

import type React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import {
  SecretManagerAuthFields,
  type SecretItem,
} from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };
export type Neo4jWriteTarget = 'labels' | 'relationship' | 'query';
export type Neo4jSaveMode = 'Overwrite' | 'Append';

const VALID_NEO4J_SCHEMES = [
  'neo4j://',
  'neo4j+s://',
  'neo4j+ssc://',
  'bolt://',
  'bolt+s://',
  'bolt+ssc://',
] as const;

/** Props for the {@link Neo4jIOSinkProperties} component. */
export interface Neo4jIOSinkPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        url?: string;
        database?: string;
        saveMode?: Neo4jSaveMode | string;
        writeTarget?: Neo4jWriteTarget | string;
        labels?: string;
        nodeKeys?: string;
        relationship?: string;
        sourceLabels?: string;
        sourceNodeKeys?: string;
        targetLabels?: string;
        targetNodeKeys?: string;
        query?: string;
        cypher?: string;
        batchSize?: number | string;
        username?: string;
        authMode?: string;
        secretName?: string;
        passwordSecret?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Resolves the Neo4j write target mode from explicit or inferred properties.
 */
function resolveWriteTarget(
  properties: Record<string, unknown>,
  query: string,
  relationship: string
): Neo4jWriteTarget {
  const raw = properties.writeTarget;
  if (raw === 'labels' || raw === 'relationship' || raw === 'query') {
    return raw;
  }
  if (query.trim().length > 0) {
    return 'query';
  }
  if (relationship.trim().length > 0) {
    return 'relationship';
  }
  return 'labels';
}

/**
 * Configures properties for Apache Spark 4.x Neo4j batch and streaming sink nodes.
 */
export const Neo4jIOSinkProperties: React.FC<Neo4jIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = (node?.data?.properties ?? {}) as Record<string, unknown>;

  const url = typeof properties.url === 'string' ? properties.url : '';
  const database = typeof properties.database === 'string' ? properties.database : 'neo4j';
  const saveMode: Neo4jSaveMode = properties.saveMode === 'Append' ? 'Append' : 'Overwrite';
  const labels = typeof properties.labels === 'string' ? properties.labels : '';
  const nodeKeys = typeof properties.nodeKeys === 'string' ? properties.nodeKeys : '';
  const relationship = typeof properties.relationship === 'string' ? properties.relationship : '';
  const sourceLabels = typeof properties.sourceLabels === 'string' ? properties.sourceLabels : '';
  const sourceNodeKeys =
    typeof properties.sourceNodeKeys === 'string' ? properties.sourceNodeKeys : '';
  const targetLabels = typeof properties.targetLabels === 'string' ? properties.targetLabels : '';
  const targetNodeKeys =
    typeof properties.targetNodeKeys === 'string' ? properties.targetNodeKeys : '';
  const query =
    typeof properties.query === 'string'
      ? properties.query
      : typeof properties.cypher === 'string'
        ? properties.cypher
        : '';
  const writeTarget = resolveWriteTarget(properties, query, relationship);
  const batchSize =
    typeof properties.batchSize === 'number' || typeof properties.batchSize === 'string'
      ? properties.batchSize
      : 5000;
  const username = typeof properties.username === 'string' ? properties.username : 'neo4j';
  const rawAuthMode = properties.authMode;
  const authMode =
    rawAuthMode === 'secretManager' || rawAuthMode === 'secret_manager'
      ? 'secretManager'
      : 'default';
  const secretName =
    typeof properties.secretName === 'string'
      ? properties.secretName
      : typeof properties.passwordSecret === 'string'
        ? properties.passwordSecret
        : '';

  const trimmedUrl = url.trim();
  const isUrlEmpty = !trimmedUrl;
  const hasInvalidScheme =
    !isUrlEmpty &&
    !VALID_NEO4J_SCHEMES.some((scheme) => trimmedUrl.toLowerCase().startsWith(scheme));

  const isLabelsEmpty = writeTarget === 'labels' && !labels.trim();
  const isNodeKeysEmpty =
    writeTarget === 'labels' && saveMode === 'Overwrite' && !nodeKeys.trim();
  const isRelationshipEmpty = writeTarget === 'relationship' && !relationship.trim();
  const isSourceKeysEmpty = writeTarget === 'relationship' && !sourceNodeKeys.trim();
  const isTargetKeysEmpty = writeTarget === 'relationship' && !targetNodeKeys.trim();
  const isQueryEmpty = writeTarget === 'query' && !query.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    if (name === 'query') {
      delete next.cypher;
    }
    if (name === 'secretName') {
      delete next.passwordSecret;
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark Neo4j 4.0 Connector</span>
        <span className="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300 font-mono text-[11px]">
          org.neo4j.spark.DataSource · NATIVE
        </span>
      </div>

      {/* Connection URL, Database & Save Mode */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <PropertyField
          id="neo4jSinkUrl"
          name="url"
          label="Bolt / Neo4j URL (url)"
          required="Connection URL is required"
          tooltip="Neo4j Bolt or Aura connection URI (for example neo4j+s://xxxx.databases.neo4j.io or bolt://localhost:7687)."
          value={url}
          invalid={isUrlEmpty}
          error={isUrlEmpty ? 'Connection URL is required' : null}
          helperText={
            hasInvalidScheme ? (
              <span className="text-amber-400" role="alert">
                Connection URL must start with neo4j://, neo4j+s://, bolt://, or bolt+s://
              </span>
            ) : null
          }
          placeholder="neo4j+s://xxxx.databases.neo4j.io"
          onChange={handleChange}
          mono={true}
        />

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="neo4jSinkDatabase"
            name="database"
            label="Database"
            tooltip="Destination Neo4j database name (default 'neo4j')."
            value={database}
            placeholder="neo4j"
            onChange={handleChange}
            mono={true}
          />

          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="neo4jSinkSaveMode"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Save Mode</span>
              <InfoTooltip text="Overwrite executes idempotent Cypher MERGE by key. Append executes Cypher CREATE." />
            </label>
            <select
              id="neo4jSinkSaveMode"
              name="saveMode"
              value={saveMode}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="Overwrite">Overwrite (Cypher MERGE)</option>
              <option value="Append">Append (Cypher CREATE)</option>
            </select>
          </div>
        </div>
      </div>

      {/* Graph Write Target Configuration */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="neo4jSinkWriteTarget"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Graph Write Target</span>
              <InfoTooltip text="Write DataFrame rows as Neo4j nodes, graph relationships, or a custom Cypher MERGE query." />
            </label>
            <select
              id="neo4jSinkWriteTarget"
              name="writeTarget"
              value={writeTarget}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="labels">Node Labels (labels + node.keys)</option>
              <option value="relationship">Relationship (source / target keys)</option>
              <option value="query">Custom Cypher MERGE (query)</option>
            </select>
          </div>

          <PropertyField
            id="neo4jSinkBatchSize"
            name="batchSize"
            label="Transaction Batch Size (batch.size)"
            type="number"
            min={1}
            tooltip="Number of rows committed per Neo4j transaction batch (default 5000)."
            value={batchSize}
            placeholder="5000"
            onChange={handleChange}
            mono={true}
          />
        </div>

        {writeTarget === 'labels' && (
          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="neo4jSinkLabels"
              name="labels"
              label="Node Labels (labels)"
              required="Node labels are required in Labels mode"
              tooltip="Colon-prefixed Neo4j node label(s) to write (for example :Customer)."
              value={labels}
              invalid={isLabelsEmpty}
              error={isLabelsEmpty ? 'Node labels are required in Labels mode' : null}
              placeholder=":Customer"
              onChange={handleChange}
              mono={true}
            />
            <PropertyField
              id="neo4jSinkNodeKeys"
              name="nodeKeys"
              label="Node Key Columns (node.keys)"
              required={
                saveMode === 'Overwrite'
                  ? 'Node keys are required for Overwrite (MERGE) mode'
                  : undefined
              }
              tooltip="Comma-separated key columns matched by MERGE (for example customer_id)."
              value={nodeKeys}
              invalid={isNodeKeysEmpty}
              error={
                isNodeKeysEmpty ? 'Node keys are required for Overwrite (MERGE) mode' : null
              }
              placeholder="customer_id"
              onChange={handleChange}
              mono={true}
            />
          </div>
        )}

        {writeTarget === 'relationship' && (
          <div className="flex flex-col gap-2">
            <PropertyField
              id="neo4jSinkRelationship"
              name="relationship"
              label="Relationship Type (relationship)"
              required="Relationship type is required in Relationship mode"
              tooltip="Neo4j relationship type to write (for example PURCHASED)."
              value={relationship}
              invalid={isRelationshipEmpty}
              error={
                isRelationshipEmpty ? 'Relationship type is required in Relationship mode' : null
              }
              placeholder="PURCHASED"
              onChange={handleChange}
              mono={true}
            />

            <div className="grid grid-cols-2 gap-2">
              <PropertyField
                id="neo4jSinkSourceLabels"
                name="sourceLabels"
                label="Source Labels"
                tooltip="Source node labels (relationship.source.labels)."
                value={sourceLabels}
                placeholder=":Customer"
                onChange={handleChange}
                mono={true}
              />
              <PropertyField
                id="neo4jSinkSourceNodeKeys"
                name="sourceNodeKeys"
                label="Source Node Keys"
                required="Source node keys are required in Relationship mode"
                tooltip="Column-to-property key mapping for source nodes (for example customer_id:id)."
                value={sourceNodeKeys}
                invalid={isSourceKeysEmpty}
                error={
                  isSourceKeysEmpty
                    ? 'Source node keys are required in Relationship mode'
                    : null
                }
                placeholder="customer_id:id"
                onChange={handleChange}
                mono={true}
              />
            </div>

            <div className="grid grid-cols-2 gap-2">
              <PropertyField
                id="neo4jSinkTargetLabels"
                name="targetLabels"
                label="Target Labels"
                tooltip="Target node labels (relationship.target.labels)."
                value={targetLabels}
                placeholder=":Product"
                onChange={handleChange}
                mono={true}
              />
              <PropertyField
                id="neo4jSinkTargetNodeKeys"
                name="targetNodeKeys"
                label="Target Node Keys"
                required="Target node keys are required in Relationship mode"
                tooltip="Column-to-property key mapping for target nodes (for example product_id:id)."
                value={targetNodeKeys}
                invalid={isTargetKeysEmpty}
                error={
                  isTargetKeysEmpty
                    ? 'Target node keys are required in Relationship mode'
                    : null
                }
                placeholder="product_id:id"
                onChange={handleChange}
                mono={true}
              />
            </div>
          </div>
        )}

        {writeTarget === 'query' && (
          <PropertyField
            id="neo4jSinkQuery"
            name="query"
            label="Custom Cypher MERGE Query (query)"
            type="textarea"
            rows={3}
            required="Cypher query is required in Query mode"
            tooltip="Cypher statement referencing each DataFrame row through event.<column>."
            value={query}
            invalid={isQueryEmpty}
            error={isQueryEmpty ? 'Cypher query is required in Query mode' : null}
            placeholder="MERGE (c:Customer {id: event.customer_id}) SET c.name = event.name"
            onChange={handleChange}
            mono={true}
          />
        )}
      </div>

      {/* Bolt Authentication */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <PropertyField
          id="neo4jSinkUsername"
          name="username"
          label="Bolt Username (authentication.basic.username)"
          tooltip="Neo4j basic authentication username (default 'neo4j')."
          value={username}
          placeholder="neo4j"
          onChange={handleChange}
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="neo4jSink"
          serviceName="Neo4j"
          authMode={authMode}
          secretName={secretName}
          datalistId="neo4j-sink-secret-options"
          secretPlaceholder="e.g. neo4j-password-secret"
          onChange={handleChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="neo4jSinkInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Neo4j sink code generation..."
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

export const Neo4jSinkProperties = Neo4jIOSinkProperties;
