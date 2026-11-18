/**
 * ClickHouse Source Properties Component (SRC30)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for ClickHouse Source nodes in Apache Spark 4.x
 * pipelines (ClickHouse Spark V2 connector / .format("jdbc")).
 *
 * Architectural Decisions:
 * - Batch-only source supporting host / jdbcUrl, port, database, table or query,
 *   partitionColumn, numPartitions, SecretManagerAuthFields, and SystemInstructionsField.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type SecretItem } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };

/** Props for the {@link ClickHouseIOSourceProperties} component. */
export interface ClickHouseIOSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        host?: string;
        jdbcUrl?: string;
        url?: string;
        port?: number | string;
        database?: string;
        readTargetMode?: 'table' | 'query';
        table?: string;
        query?: string;
        partitionColumn?: string;
        numPartitions?: number | string;
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
 * Configures properties for Apache Spark ClickHouse batch source nodes.
 */
export const ClickHouseIOSourceProperties: React.FC<ClickHouseIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const host = properties.host || '';
  const jdbcUrl = properties.jdbcUrl ?? properties.url ?? '';
  const port = properties.port ?? 8123;
  const database = properties.database || 'default';
  const table = properties.table || '';
  const query = properties.query || '';
  const readTargetMode: 'table' | 'query' =
    properties.readTargetMode || (query.trim() && !table.trim() ? 'query' : 'table');
  const partitionColumn = properties.partitionColumn || '';
  const numPartitions = properties.numPartitions ?? 8;
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';

  const isEndpointEmpty = !host.trim() && !String(jdbcUrl).trim();
  const isTableEmpty = readTargetMode === 'table' && !table.trim();
  const isQueryEmpty = readTargetMode === 'query' && !query.trim();

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
        <span className="font-semibold text-slate-200">Spark ClickHouse V2 / JDBC Connector</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          clickhouse / jdbc · BATCH
        </span>
      </div>

      {/* Host, Port, JDBC URL, Database */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>ClickHouse Endpoint &amp; Database</span>
          <InfoTooltip text="Configure ClickHouse host and HTTP/native port or full JDBC URL." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="clickhouseSourceHost"
            name="host"
            label="Host"
            required="Host or JDBC URL is required"
            tooltip="ClickHouse hostname or IP address."
            value={host}
            invalid={isEndpointEmpty}
            error={isEndpointEmpty ? 'Host or JDBC URL is required' : null}
            placeholder="clickhouse.internal"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="clickhouseSourcePort"
            name="port"
            label="Port"
            type="number"
            tooltip="ClickHouse HTTP (8123 / 8443) or native (9000) port."
            value={port}
            placeholder="8123"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="clickhouseSourceJdbcUrl"
          name="jdbcUrl"
          label="JDBC URL (Optional Override)"
          tooltip="Optional explicit ClickHouse JDBC URL (jdbc:clickhouse://host:8123/default)."
          value={jdbcUrl}
          placeholder="jdbc:clickhouse://clickhouse.internal:8123/default"
          onChange={handleChange}
          mono={true}
        />

        <PropertyField
          id="clickhouseSourceDatabase"
          name="database"
          label="Database"
          tooltip="ClickHouse database name (default 'default')."
          value={database}
          placeholder="default"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Read Target (Table vs Query) */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="clickhouseSourceReadTargetMode"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Read Target</span>
            <InfoTooltip text="Choose whether to read a ClickHouse table or run a custom SQL query." />
          </label>
          <select
            id="clickhouseSourceReadTargetMode"
            name="readTargetMode"
            value={readTargetMode}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="table">Table</option>
            <option value="query">SQL Query</option>
          </select>
        </div>

        {readTargetMode === 'table' ? (
          <PropertyField
            id="clickhouseSourceTable"
            name="table"
            label="Table Name"
            required="Table name is required in Table mode"
            tooltip="ClickHouse table name to read."
            value={table}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Table name is required in Table mode' : null}
            placeholder="events"
            onChange={handleChange}
            mono={true}
          />
        ) : (
          <PropertyField
            id="clickhouseSourceQuery"
            name="query"
            label="SQL Query"
            type="textarea"
            rows={3}
            required="SQL query is required in Query mode"
            tooltip="ClickHouse SQL SELECT statement."
            value={query}
            invalid={isQueryEmpty}
            error={isQueryEmpty ? 'SQL query is required in Query mode' : null}
            placeholder="SELECT event_id, user_id, event_time FROM events"
            onChange={handleChange}
            mono={true}
          />
        )}
      </div>

      {/* Parallel Partitioning */}
      <div className="grid grid-cols-2 gap-2">
        <PropertyField
          id="clickhouseSourcePartitionColumn"
          name="partitionColumn"
          label="Partition Column"
          tooltip="Numeric or date column used to partition parallel JDBC reads."
          value={partitionColumn}
          placeholder="event_id"
          onChange={handleChange}
          mono={true}
        />
        <PropertyField
          id="clickhouseSourceNumPartitions"
          name="numPartitions"
          label="Num Partitions"
          type="number"
          min={1}
          tooltip="Number of parallel Spark partitions."
          value={numPartitions}
          placeholder="8"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Secret Manager Authentication */}
      <SecretManagerAuthFields
        idPrefix="clickhouseSource"
        serviceName="ClickHouse"
        authMode={authMode}
        secretName={secretName}
        datalistId="clickhouse-source-secret-options"
        secretPlaceholder="e.g. clickhouse-password-secret"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="clickhouseSourceInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark ClickHouse source code generation..."
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

export const ClickHouseSourceProperties = ClickHouseIOSourceProperties;
