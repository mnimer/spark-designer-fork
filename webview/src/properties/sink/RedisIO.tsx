/**
 * @fileoverview Redis Sink Properties Component (SNK32).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.x Redis Sink nodes
 * using the Spark 4.0 Python DataSourceWriter API and Structured Streaming foreachBatch
 * backed by non-transactional redis-py pipelines.
 *
 * Architectural Decisions:
 * - Configures Redis endpoint (host, port, database index, TLS ssl toggle).
 * - Supports hash (HSET), json (SET JSON), string (SET), and stream (XADD) writes.
 * - Configures keyPrefix, keyColumn, valueColumn, streamKey, maxStreamLen, ttlSeconds, and batchSize.
 * - Uses SecretManagerAuthFields for Redis AUTH password resolution.
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
export type RedisSinkDataType = 'hash' | 'json' | 'string' | 'stream';

/** Props for the {@link RedisIOSinkProperties} component. */
export interface RedisIOSinkPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        host?: string;
        port?: number | string;
        database?: number | string;
        ssl?: boolean;
        dataType?: RedisSinkDataType | string;
        keyPrefix?: string;
        keyColumn?: string;
        valueColumn?: string;
        streamKey?: string;
        maxStreamLen?: number | string;
        ttlSeconds?: number | string;
        expireTime?: number | string;
        batchSize?: number | string;
        authMode?: string;
        secretName?: string;
        authPasswordSecret?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Resolves the Redis sink target data structure from node properties.
 */
function resolveRedisDataType(properties: Record<string, unknown>): RedisSinkDataType {
  const raw = properties.dataType;
  if (raw === 'hash' || raw === 'json' || raw === 'string' || raw === 'stream') {
    return raw;
  }
  return 'hash';
}

/**
 * Configures properties for Apache Spark 4.x Redis batch and streaming sink nodes.
 */
export const RedisIOSinkProperties: React.FC<RedisIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = (node?.data?.properties ?? {}) as Record<string, unknown>;

  const host = typeof properties.host === 'string' ? properties.host : '';
  const port =
    properties.port !== undefined && properties.port !== ''
      ? (properties.port as number | string)
      : 6379;
  const database =
    typeof properties.database === 'number' || typeof properties.database === 'string'
      ? properties.database
      : 0;
  const ssl = typeof properties.ssl === 'boolean' ? properties.ssl : false;
  const dataType = resolveRedisDataType(properties);
  const keyPrefix = typeof properties.keyPrefix === 'string' ? properties.keyPrefix : '';
  const keyColumn = typeof properties.keyColumn === 'string' ? properties.keyColumn : '';
  const valueColumn = typeof properties.valueColumn === 'string' ? properties.valueColumn : '';
  const streamKey = typeof properties.streamKey === 'string' ? properties.streamKey : '';
  const maxStreamLen =
    typeof properties.maxStreamLen === 'number' || typeof properties.maxStreamLen === 'string'
      ? properties.maxStreamLen
      : '';
  const rawTtl = properties.ttlSeconds ?? properties.expireTime;
  const ttlSeconds = typeof rawTtl === 'number' || typeof rawTtl === 'string' ? rawTtl : '';
  const batchSize =
    typeof properties.batchSize === 'number' || typeof properties.batchSize === 'string'
      ? properties.batchSize
      : 1000;
  const rawAuthMode = properties.authMode;
  const authMode =
    rawAuthMode === 'secretManager' || rawAuthMode === 'secret_manager'
      ? 'secretManager'
      : 'default';
  const secretName =
    typeof properties.secretName === 'string'
      ? properties.secretName
      : typeof properties.authPasswordSecret === 'string'
        ? properties.authPasswordSecret
        : '';

  const isHostEmpty = !host.trim();
  const isStreamMode = dataType === 'stream';
  const isKeyColumnEmpty = !isStreamMode && !keyColumn.trim();
  const isStreamKeyEmpty = isStreamMode && !streamKey.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    if (name === 'ttlSeconds') {
      delete next.expireTime;
    }
    if (name === 'secretName') {
      delete next.authPasswordSecret;
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark 4.0 Python DataSourceWriter (Redis)</span>
        <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 font-mono text-[11px]">
          redis-py pipeline · FOREACHBATCH
        </span>
      </div>

      {/* Redis Endpoint & TLS */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Redis Endpoint &amp; TLS</span>
          <InfoTooltip text="Configure Redis / Memorystore hostname, port, logical database index, and TLS encryption." />
        </label>

        <div className="grid grid-cols-3 gap-2">
          <div className="col-span-2">
            <PropertyField
              id="redisSinkHost"
              name="host"
              label="Redis Host"
              required="Host is required"
              tooltip="Redis or Memorystore hostname or IP address."
              value={host}
              invalid={isHostEmpty}
              error={isHostEmpty ? 'Host is required' : null}
              placeholder="10.0.0.5"
              onChange={handleChange}
              mono={true}
            />
          </div>
          <PropertyField
            id="redisSinkPort"
            name="port"
            label="Port"
            type="number"
            tooltip="Redis server port (default 6379)."
            value={port}
            placeholder="6379"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <div className="grid grid-cols-2 gap-2 items-end">
          <PropertyField
            id="redisSinkDatabase"
            name="database"
            label="Database Index"
            type="number"
            min={0}
            tooltip="Logical Redis database index (default 0)."
            value={database}
            placeholder="0"
            onChange={handleChange}
            mono={true}
          />

          <div className="flex items-center gap-2 pb-2">
            <input
              id="redisSinkSsl"
              type="checkbox"
              name="ssl"
              checked={ssl}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
            />
            <label
              htmlFor="redisSinkSsl"
              className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
            >
              <span>Enable TLS / SSL</span>
              <InfoTooltip text="Enables TLS encryption on the redis-py client connection." />
            </label>
          </div>
        </div>
      </div>

      {/* Data Structure, Key Mapping & Pipeline Flush */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Data Structure &amp; Pipeline Settings</span>
          <InfoTooltip text="Choose Redis write command (HSET, SET JSON, SET string, or XADD) and pipeline flush size." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="redisSinkDataType"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Redis Target Data Type</span>
              <InfoTooltip text="Redis data structure written per DataFrame row inside client.pipeline(transaction=False)." />
            </label>
            <select
              id="redisSinkDataType"
              name="dataType"
              value={dataType}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="hash">Hash (HSET)</option>
              <option value="json">JSON String (SET JSON)</option>
              <option value="string">String Column (SET)</option>
              <option value="stream">Redis Stream (XADD)</option>
            </select>
          </div>

          <PropertyField
            id="redisSinkBatchSize"
            name="batchSize"
            label="Pipeline Batch Size"
            type="number"
            min={1}
            tooltip="Commands buffered per redis-py pipeline(transaction=False) flush (default 1000)."
            value={batchSize}
            placeholder="1000"
            onChange={handleChange}
            mono={true}
          />
        </div>

        {isStreamMode ? (
          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="redisSinkStreamKey"
              name="streamKey"
              label="Stream Key (streamKey)"
              required="Stream key is required in Stream mode"
              tooltip="Destination Redis Stream key for XADD commands."
              value={streamKey}
              invalid={isStreamKeyEmpty}
              error={isStreamKeyEmpty ? 'Stream key is required in Stream mode' : null}
              placeholder="events:stream"
              onChange={handleChange}
              mono={true}
            />
            <PropertyField
              id="redisSinkMaxStreamLen"
              name="maxStreamLen"
              label="Approximate MAXLEN (Optional)"
              type="number"
              min={1}
              tooltip="Optional approximate stream cap passed to XADD MAXLEN ~."
              value={maxStreamLen}
              placeholder="100000"
              onChange={handleChange}
              mono={true}
            />
          </div>
        ) : (
          <>
            <div className="grid grid-cols-2 gap-2">
              <PropertyField
                id="redisSinkKeyPrefix"
                name="keyPrefix"
                label="Key Prefix (keyPrefix)"
                tooltip="Namespace prefix prepended to each keyColumn value (for example user:)."
                value={keyPrefix}
                placeholder="user:"
                onChange={handleChange}
                mono={true}
              />
              <PropertyField
                id="redisSinkKeyColumn"
                name="keyColumn"
                label="Key Column (keyColumn)"
                required="Key column is required"
                tooltip="Input DataFrame column combined with keyPrefix to form the Redis key."
                value={keyColumn}
                invalid={isKeyColumnEmpty}
                error={isKeyColumnEmpty ? 'Key column is required' : null}
                placeholder="user_id"
                onChange={handleChange}
                mono={true}
              />
            </div>

            <div className="grid grid-cols-2 gap-2">
              {dataType === 'string' && (
                <PropertyField
                  id="redisSinkValueColumn"
                  name="valueColumn"
                  label="Value Column (valueColumn)"
                  tooltip="Column written as the Redis string payload. If blank, serializes remaining columns as JSON."
                  value={valueColumn}
                  placeholder="payload"
                  onChange={handleChange}
                  mono={true}
                />
              )}
              <PropertyField
                id="redisSinkTtlSeconds"
                name="ttlSeconds"
                label="Key TTL Seconds (Optional)"
                type="number"
                min={1}
                tooltip="Optional expiration TTL in seconds applied via EXPIRE or SET EX."
                value={ttlSeconds}
                placeholder="3600"
                onChange={handleChange}
                mono={true}
              />
            </div>
          </>
        )}
      </div>

      {/* Secret Manager Authentication */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <SecretManagerAuthFields
          idPrefix="redisSink"
          serviceName="Redis"
          authMode={authMode}
          secretName={secretName}
          datalistId="redis-sink-secret-options"
          secretPlaceholder="e.g. redis-auth-password-secret"
          onChange={handleChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="redisSinkInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Redis pipeline sink code generation..."
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

export const RedisSinkProperties = RedisIOSinkProperties;
