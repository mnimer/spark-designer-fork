/**
 * @fileoverview Cassandra Sink Properties Component (SNK31).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.x Cassandra Sink nodes
 * supporting both the Spark 4.0 Python DataSourceWriter / foreachBatch fallback (cassandra-driver)
 * and the native JVM connector (.format("org.apache.spark.sql.cassandra")).
 *
 * Architectural Decisions:
 * - Configures contactPoints, port (default 9042), localDc, and SSL encryption.
 * - Configures destination keyspace, table, connectorBackend ('python_driver' | 'jvm_connector'),
 *   consistencyLevel, ttlColumn, writetimeColumn, and statement concurrency.
 * - Uses SecretManagerAuthFields for Apache Cassandra password resolution.
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
export type CassandraConnectorBackend = 'python_driver' | 'jvm_connector';
export type CassandraConsistencyLevel =
  | 'LOCAL_QUORUM'
  | 'QUORUM'
  | 'LOCAL_ONE'
  | 'ONE'
  | 'EACH_QUORUM'
  | 'ALL';

const CONSISTENCY_LEVELS: readonly CassandraConsistencyLevel[] = [
  'LOCAL_QUORUM',
  'QUORUM',
  'LOCAL_ONE',
  'ONE',
  'EACH_QUORUM',
  'ALL',
];

/** Props for the {@link CassandraIOSinkProperties} component. */
export interface CassandraIOSinkPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        contactPoints?: string;
        hosts?: string;
        port?: number | string;
        localDc?: string;
        ssl?: boolean;
        keyspace?: string;
        table?: string;
        connectorBackend?: CassandraConnectorBackend | string;
        consistencyLevel?: CassandraConsistencyLevel | string;
        ttlColumn?: string;
        writetimeColumn?: string;
        concurrency?: number | string;
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
 * Configures properties for Apache Spark 4.x Cassandra batch and streaming sink nodes.
 */
export const CassandraIOSinkProperties: React.FC<CassandraIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = (node?.data?.properties ?? {}) as Record<string, unknown>;

  const contactPoints =
    typeof properties.contactPoints === 'string'
      ? properties.contactPoints
      : typeof properties.hosts === 'string'
        ? properties.hosts
        : '';
  const port =
    properties.port !== undefined && properties.port !== ''
      ? (properties.port as number | string)
      : 9042;
  const localDc = typeof properties.localDc === 'string' ? properties.localDc : '';
  const ssl = typeof properties.ssl === 'boolean' ? properties.ssl : false;
  const keyspace = typeof properties.keyspace === 'string' ? properties.keyspace : '';
  const table = typeof properties.table === 'string' ? properties.table : '';
  const connectorBackend: CassandraConnectorBackend =
    properties.connectorBackend === 'jvm_connector' ? 'jvm_connector' : 'python_driver';
  const consistencyLevel: CassandraConsistencyLevel =
    typeof properties.consistencyLevel === 'string' &&
    (CONSISTENCY_LEVELS as readonly string[]).includes(properties.consistencyLevel)
      ? (properties.consistencyLevel as CassandraConsistencyLevel)
      : 'LOCAL_QUORUM';
  const ttlColumn = typeof properties.ttlColumn === 'string' ? properties.ttlColumn : '';
  const writetimeColumn =
    typeof properties.writetimeColumn === 'string' ? properties.writetimeColumn : '';
  const concurrency =
    typeof properties.concurrency === 'number' || typeof properties.concurrency === 'string'
      ? properties.concurrency
      : 100;
  const username = typeof properties.username === 'string' ? properties.username : '';
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

  const isContactPointsEmpty = !contactPoints.trim();
  const isKeyspaceEmpty = !keyspace.trim();
  const isTableEmpty = !table.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    if (name === 'contactPoints') {
      delete next.hosts;
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
        <span className="font-semibold text-slate-200">Spark 4.0 Cassandra Sink</span>
        <span className="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300 font-mono text-[11px]">
          {connectorBackend === 'jvm_connector'
            ? 'org.apache.spark.sql.cassandra'
            : 'cassandra-driver'}{' '}
          · NATIVE
        </span>
      </div>

      {/* Cluster Connection */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Cassandra Cluster Connection</span>
          <InfoTooltip text="Configure contactPoints, native CQL port (9042), local datacenter, and TLS encryption." />
        </label>

        <PropertyField
          id="cassandraSinkContactPoints"
          name="contactPoints"
          label="Contact Points (contactPoints)"
          required="Contact points are required"
          tooltip="Comma-separated list of Cassandra seed node hostnames or IP addresses."
          value={contactPoints}
          invalid={isContactPointsEmpty}
          error={isContactPointsEmpty ? 'Contact points are required' : null}
          placeholder="10.0.0.1,10.0.0.2"
          onChange={handleChange}
          mono={true}
        />

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="cassandraSinkPort"
            name="port"
            label="Port"
            type="number"
            tooltip="Native CQL transport port (default 9042)."
            value={port}
            placeholder="9042"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="cassandraSinkLocalDc"
            name="localDc"
            label="Local Datacenter (localDc)"
            tooltip="Local datacenter name for DCAwareRoundRobinPolicy."
            value={localDc}
            placeholder="datacenter1"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <div className="flex items-center gap-2">
          <input
            id="cassandraSinkSsl"
            type="checkbox"
            name="ssl"
            checked={ssl}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="cassandraSinkSsl"
            className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
          >
            <span>Enable TLS / SSL Client Encryption</span>
            <InfoTooltip text="Encrypts CQL client-to-node traffic using standard TLS certificates." />
          </label>
        </div>
      </div>

      {/* Destination Table, Backend & CQL Write Semantics */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Target Table &amp; CQL Write Options</span>
          <InfoTooltip text="Configure destination keyspace, table, Spark 4.x execution backend, consistency level, TTL, and writetime." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="cassandraSinkKeyspace"
            name="keyspace"
            label="Keyspace"
            required="Keyspace is required"
            tooltip="Destination Cassandra keyspace name."
            value={keyspace}
            invalid={isKeyspaceEmpty}
            error={isKeyspaceEmpty ? 'Keyspace is required' : null}
            placeholder="telemetry"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="cassandraSinkTable"
            name="table"
            label="Table"
            required="Table is required"
            tooltip="Destination Cassandra table name."
            value={table}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Table is required' : null}
            placeholder="device_rollups"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="cassandraSinkConnectorBackend"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Spark 4.x Backend</span>
              <InfoTooltip text="Default to Spark 4.0 Python cassandra-driver because OSS JVM spark-cassandra-connector lags Spark 4.x." />
            </label>
            <select
              id="cassandraSinkConnectorBackend"
              name="connectorBackend"
              value={connectorBackend}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-xs focus:outline-none focus:border-blue-500"
            >
              <option value="python_driver">
                Python DataSourceWriter / foreachBatch (cassandra-driver)
              </option>
              <option value="jvm_connector">
                JVM Connector (.format(&quot;org.apache.spark.sql.cassandra&quot;))
              </option>
            </select>
          </div>

          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="cassandraSinkConsistencyLevel"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Consistency Level</span>
              <InfoTooltip text="Cassandra write consistency level (default LOCAL_QUORUM)." />
            </label>
            <select
              id="cassandraSinkConsistencyLevel"
              name="consistencyLevel"
              value={consistencyLevel}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-xs font-mono focus:outline-none focus:border-blue-500"
            >
              {CONSISTENCY_LEVELS.map((level) => (
                <option key={level} value={level}>
                  {level}
                </option>
              ))}
            </select>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="cassandraSinkTtlColumn"
            name="ttlColumn"
            label="TTL Column / Seconds (Optional)"
            tooltip="Column name or static integer seconds applied to CQL USING TTL."
            value={ttlColumn}
            placeholder="ttl_seconds or 86400"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="cassandraSinkWritetimeColumn"
            name="writetimeColumn"
            label="Writetime Column (Optional)"
            tooltip="Microsecond epoch column applied to CQL USING TIMESTAMP."
            value={writetimeColumn}
            placeholder="event_time_micros"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="cassandraSinkConcurrency"
          name="concurrency"
          label="Statement Concurrency"
          type="number"
          min={1}
          tooltip="Number of concurrent in-flight prepared statements per executor task (default 100)."
          value={concurrency}
          placeholder="100"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Authentication */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <PropertyField
          id="cassandraSinkUsername"
          name="username"
          label="CQL Username (Optional)"
          tooltip="Cassandra PlainTextAuthProvider username when cluster authentication is enabled."
          value={username}
          placeholder="cassandra"
          onChange={handleChange}
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="cassandraSink"
          serviceName="Apache Cassandra"
          authMode={authMode}
          secretName={secretName}
          datalistId="cassandra-sink-secret-options"
          secretPlaceholder="e.g. cassandra-password-secret"
          onChange={handleChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="cassandraSinkInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Cassandra sink code generation..."
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

export const CassandraSinkProperties = CassandraIOSinkProperties;
