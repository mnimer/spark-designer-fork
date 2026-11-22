/**
 * @fileoverview Elasticsearch Sink Properties Component (SNK33).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.x Elasticsearch Sink nodes
 * (.write.format("org.elasticsearch.spark.sql") and .writeStream.format("org.elasticsearch.spark.sql")).
 *
 * Architectural Decisions:
 * - Configures es.nodes, es.port (default 9200), es.resource, es.nodes.wan.only (default true), and es.net.ssl.
 * - Configures es.write.operation ('index' | 'upsert' | 'create' | 'update'), es.mapping.id,
 *   es.batch.size.entries (default 1000), and es.batch.size.bytes.
 * - Uses SecretManagerAuthFields for Elasticsearch password or API key resolution.
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
export type ElasticsearchWriteOperation = 'index' | 'upsert' | 'create' | 'update';

/** Props for the {@link ElasticsearchIOSinkProperties} component. */
export interface ElasticsearchIOSinkPropertiesProps {
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
        wanOnly?: boolean;
        ssl?: boolean;
        writeOperation?: ElasticsearchWriteOperation | string;
        mappingId?: string;
        idFn?: string;
        batchSizeEntries?: number | string;
        maxBatchSize?: number | string;
        batchSizeBytes?: string | number;
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
 * Resolves the Elasticsearch write operation from node properties.
 */
function resolveWriteOperation(
  properties: Record<string, unknown>
): ElasticsearchWriteOperation {
  const raw = properties.writeOperation;
  if (raw === 'index' || raw === 'upsert' || raw === 'create' || raw === 'update') {
    return raw;
  }
  return 'index';
}

/**
 * Configures properties for Apache Spark 4.x Elasticsearch batch and streaming sink nodes.
 */
export const ElasticsearchIOSinkProperties: React.FC<ElasticsearchIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = (node?.data?.properties ?? {}) as Record<string, unknown>;

  const nodes =
    typeof properties.nodes === 'string'
      ? properties.nodes
      : typeof properties.addresses === 'string'
        ? properties.addresses
        : '';
  const port =
    properties.port !== undefined && properties.port !== ''
      ? (properties.port as number | string)
      : 9200;
  const resource =
    typeof properties.resource === 'string'
      ? properties.resource
      : typeof properties.index === 'string'
        ? properties.index
        : '';
  const wanOnly = typeof properties.wanOnly === 'boolean' ? properties.wanOnly : true;
  const ssl = typeof properties.ssl === 'boolean' ? properties.ssl : false;
  const writeOperation = resolveWriteOperation(properties);
  const mappingId =
    typeof properties.mappingId === 'string'
      ? properties.mappingId
      : typeof properties.idFn === 'string'
        ? properties.idFn
        : '';
  const rawBatchEntries = properties.batchSizeEntries ?? properties.maxBatchSize;
  const batchSizeEntries =
    typeof rawBatchEntries === 'number' || typeof rawBatchEntries === 'string'
      ? rawBatchEntries
      : 1000;
  const batchSizeBytes =
    typeof properties.batchSizeBytes === 'string' || typeof properties.batchSizeBytes === 'number'
      ? String(properties.batchSizeBytes)
      : '';
  const username = typeof properties.username === 'string' ? properties.username : '';
  const rawAuthMode = properties.authMode;
  const authMode =
    rawAuthMode === 'secretManager' ||
    rawAuthMode === 'basic_secret_manager' ||
    rawAuthMode === 'api_key_secret'
      ? 'secretManager'
      : 'default';
  const secretName =
    typeof properties.secretName === 'string'
      ? properties.secretName
      : typeof properties.passwordSecret === 'string'
        ? properties.passwordSecret
        : '';

  const isNodesEmpty = !nodes.trim();
  const isResourceEmpty = !resource.trim();
  const requiresMappingId =
    writeOperation === 'upsert' || writeOperation === 'update' || writeOperation === 'create';
  const isMappingIdEmpty = requiresMappingId && !mappingId.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    if (name === 'nodes') {
      delete next.addresses;
    }
    if (name === 'resource') {
      next.index = String(updatedValue);
    }
    if (name === 'mappingId') {
      delete next.idFn;
    }
    if (name === 'batchSizeEntries') {
      delete next.maxBatchSize;
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
        <span className="font-semibold text-slate-200">Spark Elasticsearch 4.0 Connector</span>
        <span className="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300 font-mono text-[11px]">
          org.elasticsearch.spark.sql · NATIVE
        </span>
      </div>

      {/* Cluster Nodes, Port & Target Resource */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Cluster Endpoint &amp; Index Resource</span>
          <InfoTooltip text="Configure es.nodes, es.port, es.resource, WAN-only routing, and HTTPS TLS." />
        </label>

        <div className="grid grid-cols-3 gap-2">
          <div className="col-span-2">
            <PropertyField
              id="esSinkNodes"
              name="nodes"
              label="Elasticsearch Nodes (es.nodes)"
              required="Elasticsearch nodes are required"
              tooltip="Comma-separated Elasticsearch hostnames or Elastic Cloud endpoint."
              value={nodes}
              invalid={isNodesEmpty}
              error={isNodesEmpty ? 'Elasticsearch nodes are required' : null}
              placeholder="es-cluster.internal"
              onChange={handleChange}
              mono={true}
            />
          </div>
          <PropertyField
            id="esSinkPort"
            name="port"
            label="Port (es.port)"
            type="number"
            tooltip="Elasticsearch HTTP or HTTPS port (default 9200)."
            value={port}
            placeholder="9200"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="esSinkResource"
          name="resource"
          label="Index / Resource (es.resource)"
          required="Index resource is required"
          tooltip="Destination Elasticsearch index or data stream name."
          value={resource}
          invalid={isResourceEmpty}
          error={isResourceEmpty ? 'Index resource is required' : null}
          placeholder="logs-enriched"
          onChange={handleChange}
          mono={true}
        />

        <div className="grid grid-cols-2 gap-2">
          <div className="flex items-center gap-2">
            <input
              id="esSinkWanOnly"
              type="checkbox"
              name="wanOnly"
              checked={wanOnly}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
            />
            <label
              htmlFor="esSinkWanOnly"
              className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
            >
              <span>WAN Only (es.nodes.wan.only=true)</span>
              <InfoTooltip text="Connects exclusively through the configured endpoint without direct shard node discovery." />
            </label>
          </div>

          <div className="flex items-center gap-2">
            <input
              id="esSinkSsl"
              type="checkbox"
              name="ssl"
              checked={ssl}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
            />
            <label
              htmlFor="esSinkSsl"
              className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
            >
              <span>Enable HTTPS (es.net.ssl=true)</span>
              <InfoTooltip text="Enables TLS over HTTP when connecting to port 443 or secured clusters." />
            </label>
          </div>
        </div>
      </div>

      {/* Write Operation, Document ID & Bulk Sizing */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Write Operation &amp; Bulk Sizing</span>
          <InfoTooltip text="Configure es.write.operation, es.mapping.id document key, and bulk request limits." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="esSinkWriteOperation"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Write Operation (es.write.operation)</span>
              <InfoTooltip text="Elasticsearch bulk indexing mode: index, upsert, create, or update." />
            </label>
            <select
              id="esSinkWriteOperation"
              name="writeOperation"
              value={writeOperation}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="index">index (Create or replace)</option>
              <option value="upsert">upsert (Update or insert by ID)</option>
              <option value="create">create (Fail if ID exists)</option>
              <option value="update">update (Partial update by ID)</option>
            </select>
          </div>

          <PropertyField
            id="esSinkMappingId"
            name="mappingId"
            label="Document ID Column (es.mapping.id)"
            required={
              requiresMappingId
                ? 'Document ID column is required for upsert, create, or update'
                : undefined
            }
            tooltip="Input DataFrame column mapped to Elasticsearch _id."
            value={mappingId}
            invalid={isMappingIdEmpty}
            error={
              isMappingIdEmpty
                ? 'Document ID column is required for upsert, create, or update'
                : null
            }
            placeholder="doc_id"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="esSinkBatchSizeEntries"
            name="batchSizeEntries"
            label="Batch Entries (es.batch.size.entries)"
            type="number"
            min={1}
            tooltip="Maximum number of documents per Elasticsearch _bulk request (default 1000)."
            value={batchSizeEntries}
            placeholder="1000"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="esSinkBatchSizeBytes"
            name="batchSizeBytes"
            label="Batch Bytes (es.batch.size.bytes)"
            tooltip="Optional maximum byte size per _bulk request (for example 5mb)."
            value={batchSizeBytes}
            placeholder="5mb"
            onChange={handleChange}
            mono={true}
          />
        </div>
      </div>

      {/* Authentication */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <PropertyField
          id="esSinkUsername"
          name="username"
          label="HTTP Auth User (es.net.http.auth.user)"
          tooltip="Optional Elasticsearch username paired with the Secret Manager password."
          value={username}
          placeholder="elastic"
          onChange={handleChange}
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="esSink"
          serviceName="Elasticsearch"
          authMode={authMode}
          secretName={secretName}
          datalistId="elasticsearch-sink-secret-options"
          secretPlaceholder="e.g. elasticsearch-password-secret"
          onChange={handleChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="esSinkInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Elasticsearch sink code generation..."
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

export const ElasticsearchSinkProperties = ElasticsearchIOSinkProperties;
