/**
 * ClickHouse Sink Properties Component (SNK27)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for ClickHouse Sink nodes in Apache Spark 4.x
 * pipelines (ClickHouse Spark V2 connector / .write.format("jdbc") and foreachBatch).
 *
 * Architectural Decisions:
 * - Configures ClickHouse host, port, optional JDBC URL override, database, destination table,
 *   and Spark saveMode with safe truncate=true on overwrite.
 * - Defaults batchsize to 50000 (warns when batchsize < 1000 to prevent MergeTree part
 *   explosion) and defaults isolationLevel to NONE.
 * - Exposes rewriteBatchedStatements and server-side async_insert toggles plus
 *   SecretManagerAuthFields.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import {
  SecretManagerAuthFields,
  type SecretItem,
} from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };

export type ClickHouseSaveMode = 'append' | 'overwrite' | 'errorifexists' | 'ignore';
export type ClickHouseIsolationLevel = 'NONE' | 'READ_COMMITTED';

/** Props for the {@link ClickHouseIOSinkProperties} component. */
export interface ClickHouseIOSinkPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        host?: string;
        port?: number | string;
        jdbcUrl?: string;
        url?: string;
        database?: string;
        table?: string;
        tableName?: string;
        saveMode?: ClickHouseSaveMode;
        truncate?: boolean;
        batchsize?: number | string;
        batchSize?: number | string;
        maxInsertBlockSize?: number | string;
        numPartitions?: number | string;
        isolationLevel?: ClickHouseIsolationLevel;
        rewriteBatchedStatements?: boolean;
        asyncInsert?: boolean;
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
 * Configures properties for Apache Spark ClickHouse sink nodes.
 */
export const ClickHouseIOSinkProperties: React.FC<ClickHouseIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const host = properties.host || '';
  const port = properties.port ?? 8123;
  const jdbcUrl = properties.jdbcUrl ?? properties.url ?? '';
  const database = properties.database || 'default';
  const table = properties.table ?? properties.tableName ?? '';
  const saveMode: ClickHouseSaveMode = properties.saveMode || 'append';
  const truncate = properties.truncate ?? true;
  const batchsize =
    properties.batchsize ?? properties.batchSize ?? properties.maxInsertBlockSize ?? 50000;
  const numPartitions = properties.numPartitions ?? '';
  const isolationLevel: ClickHouseIsolationLevel = properties.isolationLevel || 'NONE';
  const rewriteBatchedStatements = properties.rewriteBatchedStatements ?? true;
  const asyncInsert = properties.asyncInsert ?? false;
  const username = properties.username || 'default';
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName ?? properties.passwordSecret ?? '';

  const isEndpointEmpty = !host.trim() && !String(jdbcUrl).trim();
  const isUrlFormatWarning =
    String(jdbcUrl).trim().length > 0 && !String(jdbcUrl).trim().startsWith('jdbc:clickhouse:');
  const isTableEmpty = !String(table).trim();

  const parsedBatchSize = Number(batchsize);
  const isTinyBatchWarning =
    String(batchsize).trim().length > 0 &&
    !Number.isNaN(parsedBatchSize) &&
    parsedBatchSize > 0 &&
    parsedBatchSize < 1000;

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    delete next.password;

    if (name === 'jdbcUrl') {
      delete next.url;
    }
    if (name === 'table') {
      delete next.tableName;
    }
    if (name === 'batchsize') {
      delete next.batchSize;
      delete next.maxInsertBlockSize;
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
        <span className="font-semibold text-slate-200">Spark ClickHouse V2 / JDBC Sink</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          clickhouse / jdbc · foreachBatch
        </span>
      </div>

      {/* Host, Port, JDBC URL, Database */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>ClickHouse Endpoint &amp; Database</span>
          <InfoTooltip text="Configure ClickHouse host and HTTP/HTTPS port or an explicit jdbc:clickhouse:// URL." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="clickhouseSinkHost"
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
            id="clickhouseSinkPort"
            name="port"
            label="Port"
            type="number"
            tooltip="ClickHouse HTTP (8123) or HTTPS TLS (8443) port."
            value={port}
            placeholder="8123"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="clickhouseSinkJdbcUrl"
          name="jdbcUrl"
          label="JDBC URL (Optional Override)"
          tooltip="Optional explicit ClickHouse JDBC URL using com.clickhouse.jdbc.ClickHouseDriver."
          value={jdbcUrl}
          helperText={
            isUrlFormatWarning ? (
              <span className="text-amber-400" role="alert">
                Warning: ClickHouse JDBC URL should start with &quot;jdbc:clickhouse:&quot;
              </span>
            ) : null
          }
          placeholder="jdbc:clickhouse://clickhouse.internal:8443/default?ssl=true"
          onChange={handleChange}
          mono={true}
        />

        <PropertyField
          id="clickhouseSinkDatabase"
          name="database"
          label="Database"
          tooltip="ClickHouse database name (default 'default')."
          value={database}
          placeholder="default"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Target Table, Save Mode & MergeTree Bulk Tuning */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Target Table &amp; MergeTree Bulk Tuning</span>
          <InfoTooltip text="Configures destination table, saveMode, and large MergeTree bulk insert batch sizing (10000 to 100000 rows)." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="clickhouseSinkTable"
            name="table"
            label="Destination Table"
            required="Target table name is required"
            tooltip="Destination ClickHouse MergeTree or Distributed table name."
            value={table}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Target table name is required' : null}
            placeholder="events_rollup"
            onChange={handleChange}
            mono={true}
          />

          <div className="flex flex-col gap-2">
            <label
              htmlFor="clickhouseSinkSaveMode"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Save Mode</span>
              <InfoTooltip text="Spark save mode applied when writing into ClickHouse." />
            </label>
            <select
              id="clickhouseSinkSaveMode"
              name="saveMode"
              value={saveMode}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="append">append</option>
              <option value="overwrite">overwrite</option>
              <option value="errorifexists">errorifexists</option>
              <option value="ignore">ignore</option>
            </select>
          </div>
        </div>

        {saveMode === 'overwrite' && (
          <div className="flex items-center gap-2">
            <input
              id="clickhouseSinkTruncate"
              type="checkbox"
              name="truncate"
              checked={truncate}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
            />
            <label
              htmlFor="clickhouseSinkTruncate"
              className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
            >
              <span>Truncate Table on Overwrite (truncate=true)</span>
              <InfoTooltip text="Preserves ClickHouse MergeTree engine, ORDER BY, and PARTITION BY clauses on overwrite." />
            </label>
          </div>
        )}

        <div className="grid grid-cols-3 gap-2">
          <PropertyField
            id="clickhouseSinkBatchsize"
            name="batchsize"
            label="Batch Size (batchsize)"
            type="number"
            min={1}
            tooltip="Rows per ClickHouse bulk insert block. Keep between 10000 and 100000 to avoid MergeTree Too many parts errors."
            value={batchsize}
            onChange={handleChange}
            placeholder="50000"
            mono={true}
          />

          <PropertyField
            id="clickhouseSinkNumPartitions"
            name="numPartitions"
            label="Max Partitions"
            type="number"
            min={1}
            tooltip="Optional cap on concurrent Spark write partitions."
            value={numPartitions}
            onChange={handleChange}
            placeholder="8"
            mono={true}
          />

          <div className="flex flex-col gap-2">
            <label
              htmlFor="clickhouseSinkIsolationLevel"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Isolation</span>
              <InfoTooltip text="Set NONE for ClickHouse JDBC writes." />
            </label>
            <select
              id="clickhouseSinkIsolationLevel"
              name="isolationLevel"
              value={isolationLevel}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="NONE">NONE</option>
              <option value="READ_COMMITTED">READ_COMMITTED</option>
            </select>
          </div>
        </div>

        {isTinyBatchWarning && (
          <div
            role="alert"
            className="p-2 rounded bg-amber-500/10 border border-amber-500/40 text-xs text-amber-300"
          >
            Warning: Tiny batch sizes (&lt; 1000) create excessive MergeTree parts in ClickHouse.
            Use 10000 to 100000 rows per batch.
          </div>
        )}

        <div className="flex items-center gap-2">
          <input
            id="clickhouseSinkRewriteBatched"
            type="checkbox"
            name="rewriteBatchedStatements"
            checked={rewriteBatchedStatements}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="clickhouseSinkRewriteBatched"
            className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
          >
            <span>Rewrite Batched Statements (rewriteBatchedStatements=true)</span>
            <InfoTooltip text="Packs JDBC executeBatch rows into single multi-row ClickHouse INSERT blocks." />
          </label>
        </div>

        <div className="flex items-center gap-2">
          <input
            id="clickhouseSinkAsyncInsert"
            type="checkbox"
            name="asyncInsert"
            checked={asyncInsert}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="clickhouseSinkAsyncInsert"
            className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
          >
            <span>Enable Server Async Insert (async_insert=1)</span>
            <InfoTooltip text="Buffers small streaming micro-batches on the ClickHouse server before flushing a MergeTree part." />
          </label>
        </div>
      </div>

      {/* Authentication */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <PropertyField
          id="clickhouseSinkUsername"
          name="username"
          label="ClickHouse Username"
          tooltip="ClickHouse user name (default 'default')."
          value={username}
          placeholder="default"
          onChange={handleChange}
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="clickhouseSink"
          serviceName="ClickHouse"
          authMode={authMode}
          secretName={secretName}
          datalistId="clickhouse-sink-secret-options"
          secretPlaceholder="e.g. clickhouse-password-secret"
          onChange={handleChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="clickhouseSinkInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark ClickHouse sink code generation..."
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

export const ClickHouseSinkProperties = ClickHouseIOSinkProperties;
