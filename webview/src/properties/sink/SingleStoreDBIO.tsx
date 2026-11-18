/**
 * SingleStore Sink Properties Component (SNK28)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for SingleStoreDB Sink nodes in Apache Spark 4.x
 * pipelines (.write.format("singlestore") or .write.format("jdbc") fallback and foreachBatch).
 *
 * Architectural Decisions:
 * - Configures SingleStore ddlEndpoint, database, destination table, and connector format
 *   (native singlestore LOAD DATA vs jdbc fallback).
 * - Exposes loadDataCompression (LZ4, GZip, Skip), overwriteBehavior (truncate,
 *   dropAndCreate, merge), and onDuplicateKeySQL upsert clauses.
 * - Integrates SecretManagerAuthFields and strips legacy Beam mapper properties on edit.
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

export type SingleStoreWriteFormat = 'singlestore' | 'jdbc';
export type SingleStoreSaveMode = 'append' | 'overwrite' | 'errorifexists' | 'ignore';
export type SingleStoreOverwriteBehavior = 'truncate' | 'dropAndCreate' | 'merge';
export type SingleStoreLoadDataCompression = 'LZ4' | 'GZip' | 'Skip';

/** Props for the {@link SingleStoreDBIOSinkProperties} component. */
export interface SingleStoreDBIOSinkPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        ddlEndpoint?: string;
        endpoint?: string;
        database?: string;
        table?: string;
        writeFormat?: SingleStoreWriteFormat;
        saveMode?: SingleStoreSaveMode;
        overwriteBehavior?: SingleStoreOverwriteBehavior;
        loadDataCompression?: SingleStoreLoadDataCompression;
        onDuplicateKeySQL?: string;
        batchsize?: number | string;
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
 * Configures properties for Apache Spark SingleStoreDB sink nodes (.write.format("singlestore")).
 */
export const SingleStoreDBIOSinkProperties: React.FC<SingleStoreDBIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const ddlEndpoint = properties.ddlEndpoint ?? properties.endpoint ?? '';
  const database = properties.database || '';
  const table = properties.table || '';
  const writeFormat: SingleStoreWriteFormat = properties.writeFormat || 'singlestore';
  const saveMode: SingleStoreSaveMode = properties.saveMode || 'append';
  const overwriteBehavior: SingleStoreOverwriteBehavior =
    properties.overwriteBehavior || 'truncate';
  const loadDataCompression: SingleStoreLoadDataCompression =
    properties.loadDataCompression || 'LZ4';
  const onDuplicateKeySQL = properties.onDuplicateKeySQL || '';
  const batchsize = properties.batchsize ?? properties.batchSize ?? 5000;
  const username = properties.username || '';
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName ?? properties.passwordSecret ?? '';

  const isEndpointEmpty = !String(ddlEndpoint).trim();
  const isDatabaseEmpty = !database.trim();
  const isTableEmpty = !table.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    delete next.password;

    if (name === 'ddlEndpoint') {
      delete next.endpoint;
    }
    if (name === 'batchsize') {
      delete next.batchSize;
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
        <span className="font-semibold text-slate-200">Spark SingleStore Sink</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          .write.format(&quot;{writeFormat}&quot;) · foreachBatch
        </span>
      </div>

      {/* DDL Endpoint, Database, Table & Connector Format */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>SingleStore Destination</span>
          <InfoTooltip text="Configures SingleStore master aggregator ddlEndpoint, database, destination table, and connector mode." />
        </label>

        <div className="grid grid-cols-3 gap-2">
          <div className="col-span-2">
            <PropertyField
              id="singleStoreSinkDdlEndpoint"
              name="ddlEndpoint"
              label="DDL Endpoint (ddlEndpoint)"
              required="DDL endpoint host:port is required"
              tooltip="SingleStore master aggregator host:port (.option('ddlEndpoint', ...))."
              value={ddlEndpoint}
              invalid={isEndpointEmpty}
              error={isEndpointEmpty ? 'DDL endpoint host:port is required' : null}
              placeholder="svc-singlestore-dml.internal:3306"
              onChange={handleChange}
              mono={true}
            />
          </div>

          <div className="flex flex-col gap-2">
            <label
              htmlFor="singleStoreSinkWriteFormat"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Format</span>
              <InfoTooltip text="Use native singlestore LOAD DATA connector or fallback to JDBC." />
            </label>
            <select
              id="singleStoreSinkWriteFormat"
              name="writeFormat"
              value={writeFormat}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="singlestore">singlestore</option>
              <option value="jdbc">jdbc (fallback)</option>
            </select>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="singleStoreSinkDatabase"
            name="database"
            label="Database Name"
            required="Database name is required"
            tooltip="Target SingleStore database name."
            value={database}
            invalid={isDatabaseEmpty}
            error={isDatabaseEmpty ? 'Database name is required' : null}
            placeholder="telemetry_db"
            onChange={handleChange}
            mono={true}
          />

          <PropertyField
            id="singleStoreSinkTable"
            name="table"
            label="Destination Table"
            required="Destination table is required"
            tooltip="Target SingleStore table name."
            value={table}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Destination table is required' : null}
            placeholder="telemetry_rollups"
            onChange={handleChange}
            mono={true}
          />
        </div>
      </div>

      {/* Save Mode, Overwrite Behavior, Compression & Upsert */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Load Compression, Overwrite &amp; Upsert</span>
          <InfoTooltip text="Configures Spark saveMode, SingleStore overwriteBehavior, LOAD DATA compression, and onDuplicateKeySQL upserts." />
        </label>

        <div className="grid grid-cols-3 gap-2">
          <div className="flex flex-col gap-2">
            <label
              htmlFor="singleStoreSinkSaveMode"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Save Mode</span>
              <InfoTooltip text="Spark save mode applied when writing to SingleStore." />
            </label>
            <select
              id="singleStoreSinkSaveMode"
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

          <div className="flex flex-col gap-2">
            <label
              htmlFor="singleStoreSinkOverwriteBehavior"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Overwrite Mode</span>
              <InfoTooltip text="Controls SingleStore overwriteBehavior when Save Mode is overwrite." />
            </label>
            <select
              id="singleStoreSinkOverwriteBehavior"
              name="overwriteBehavior"
              value={overwriteBehavior}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="truncate">truncate</option>
              <option value="dropAndCreate">dropAndCreate</option>
              <option value="merge">merge</option>
            </select>
          </div>

          <div className="flex flex-col gap-2">
            <label
              htmlFor="singleStoreSinkLoadDataCompression"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Compression</span>
              <InfoTooltip text="Compression codec for SingleStore parallel LOAD DATA streams (loadDataCompression)." />
            </label>
            <select
              id="singleStoreSinkLoadDataCompression"
              name="loadDataCompression"
              value={loadDataCompression}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="LZ4">LZ4</option>
              <option value="GZip">GZip</option>
              <option value="Skip">Skip</option>
            </select>
          </div>
        </div>

        {writeFormat === 'singlestore' ? (
          <PropertyField
            id="singleStoreSinkOnDuplicateKeySQL"
            name="onDuplicateKeySQL"
            label="Upsert Clause (onDuplicateKeySQL)"
            type="textarea"
            rows={2}
            tooltip="Optional SQL assignment list appended to LOAD DATA ... ON DUPLICATE KEY UPDATE for idempotent upserts."
            value={onDuplicateKeySQL}
            placeholder="metric_val = VALUES(metric_val), updated_at = VALUES(updated_at)"
            onChange={handleChange}
            mono={true}
          />
        ) : (
          <PropertyField
            id="singleStoreSinkBatchsize"
            name="batchsize"
            label="JDBC Fallback Batch Size (batchsize)"
            type="number"
            min={1}
            tooltip="Rows buffered per JDBC executeBatch call when using the jdbc fallback format."
            value={batchsize}
            placeholder="5000"
            onChange={handleChange}
            mono={true}
          />
        )}
      </div>

      {/* Authentication */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <PropertyField
          id="singleStoreSinkUsername"
          name="username"
          label="SingleStore Username"
          tooltip="SingleStore database login user name."
          value={username}
          placeholder="admin"
          onChange={handleChange}
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="singleStoreSink"
          serviceName="SingleStoreDB"
          authMode={authMode}
          secretName={secretName}
          datalistId="singlestore-sink-secret-options"
          secretPlaceholder="e.g. singlestore-password-secret"
          onChange={handleChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="singleStoreSinkInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark SingleStore sink code generation..."
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

export const SingleStoreSinkProperties = SingleStoreDBIOSinkProperties;
