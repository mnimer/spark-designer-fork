/**
 * Redis Source Properties Component (SRC35)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Redis Source nodes
 * using the Spark 4.0 Python DataSource API (redis-py SCAN for batch and XREADGROUP for Redis Streams).
 *
 * Architectural Decisions:
 * - Supports readMode ('batch' | 'streaming', default 'batch').
 * - Configures host, port (default 6379), dataType ('hash' | 'json' | 'stream' | 'string'),
 *   keyPattern + scanCount (in batch mode), or streamKey + consumerGroup (in streaming mode).
 * - Uses SecretManagerAuthFields for Redis AUTH password secret.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type SecretItem } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };
export type RedisReadMode = 'batch' | 'streaming';
export type RedisDataType = 'hash' | 'json' | 'stream' | 'string';

/** Props for the {@link RedisIOSourceProperties} component. */
export interface RedisIOSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        readMode?: RedisReadMode | string;
        host?: string;
        port?: number | string;
        dataType?: RedisDataType;
        keyPattern?: string;
        streamKey?: string;
        consumerGroup?: string;
        scanCount?: number | string;
        authMode?: string;
        secretName?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: any;
      };
    };
  };
  onChange: (properties: any) => void;
}

/**
 * Configures properties for Apache Spark Redis batch and streaming source nodes.
 */
export const RedisIOSourceProperties: React.FC<RedisIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const readMode: RedisReadMode = properties.readMode === 'streaming' ? 'streaming' : 'batch';
  const host = properties.host || '';
  const port = properties.port !== undefined && properties.port !== '' ? properties.port : 6379;
  const dataType: RedisDataType =
    properties.dataType || (readMode === 'streaming' ? 'stream' : 'hash');
  const keyPattern = properties.keyPattern || '';
  const streamKey = properties.streamKey || '';
  const consumerGroup = properties.consumerGroup || 'spark-consumer-group';
  const scanCount = properties.scanCount ?? 1000;
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || properties.authPasswordSecret || '';

  const isHostEmpty = !host.trim();
  const isKeyPatternEmpty = readMode === 'batch' && !keyPattern.trim();
  const isStreamKeyEmpty = readMode === 'streaming' && !streamKey.trim();

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
        <span className="font-semibold text-slate-200">Spark 4.0 Python DataSource (Redis)</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          redis-py · {readMode.toUpperCase()}
        </span>
      </div>

      {/* Read Mode & Data Type */}
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="redisSourceReadMode"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Execution Read Mode</span>
            <InfoTooltip text="Choose batch key scan (SCAN) or streaming Redis Streams (XREADGROUP)." />
          </label>
          <select
            id="redisSourceReadMode"
            name="readMode"
            value={readMode}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="batch">Batch (SCAN Keys)</option>
            <option value="streaming">Streaming (Redis Streams)</option>
          </select>
        </div>

        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="redisSourceDataType"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Redis Value Data Type</span>
            <InfoTooltip text="Redis data structure type to deserialize into DataFrame rows." />
          </label>
          <select
            id="redisSourceDataType"
            name="dataType"
            value={dataType}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="hash">Hash (HGETALL)</option>
            <option value="json">JSON (RedisJSON / String JSON)</option>
            <option value="stream">Stream (XREADGROUP)</option>
            <option value="string">String (GET)</option>
          </select>
        </div>
      </div>

      {/* Redis Endpoint */}
      <div className="grid grid-cols-2 gap-2">
        <PropertyField
          id="redisSourceHost"
          name="host"
          label="Redis Host"
          required="Host is required"
          tooltip="Redis / Memorystore hostname or IP address."
          value={host}
          invalid={isHostEmpty}
          error={isHostEmpty ? 'Host is required' : null}
          placeholder="10.0.0.5"
          onChange={handleChange}
          mono={true}
        />
        <PropertyField
          id="redisSourcePort"
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

      {/* Mode-Specific Target Configuration */}
      {readMode === 'batch' ? (
        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="redisSourceKeyPattern"
            name="keyPattern"
            label="Key Pattern (keyPattern)"
            required="Key pattern is required in Batch mode"
            tooltip="Glob pattern passed to Redis SCAN (for example user:*)."
            value={keyPattern}
            invalid={isKeyPatternEmpty}
            error={isKeyPatternEmpty ? 'Key pattern is required in Batch mode' : null}
            placeholder="user:*"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="redisSourceScanCount"
            name="scanCount"
            label="SCAN Batch Count (scanCount)"
            type="number"
            min={1}
            tooltip="COUNT hint per Redis SCAN cursor iteration (default 1000)."
            value={scanCount}
            placeholder="1000"
            onChange={handleChange}
            mono={true}
          />
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="redisSourceStreamKey"
            name="streamKey"
            label="Stream Key (streamKey)"
            required="Stream key is required in Streaming mode"
            tooltip="Redis Stream key to consume via XREADGROUP."
            value={streamKey}
            invalid={isStreamKeyEmpty}
            error={isStreamKeyEmpty ? 'Stream key is required in Streaming mode' : null}
            placeholder="events:stream"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="redisSourceConsumerGroup"
            name="consumerGroup"
            label="Consumer Group (consumerGroup)"
            tooltip="Redis Streams consumer group name."
            value={consumerGroup}
            placeholder="spark-consumer-group"
            onChange={handleChange}
            mono={true}
          />
        </div>
      )}

      {/* Secret Manager Authentication */}
      <SecretManagerAuthFields
        idPrefix="redisSource"
        serviceName="Redis"
        authMode={authMode}
        secretName={secretName}
        datalistId="redis-source-secret-options"
        secretPlaceholder="e.g. redis-auth-password-secret"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="redisSourceInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Redis source code generation..."
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

export const RedisSourceProperties = RedisIOSourceProperties;
