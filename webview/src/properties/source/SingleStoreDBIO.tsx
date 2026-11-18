/**
 * SingleStore Source Properties Component (SRC31)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for SingleStoreDB Source nodes
 * in Apache Spark 4.x pipelines (.format("singlestore") / .format("jdbc")).
 *
 * Architectural Decisions:
 * - Batch-only source configuring ddlEndpoint, database, table or query,
 *   enableParallelRead toggle, SecretManagerAuthFields, and SystemInstructionsField.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type SecretItem } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };

/** Props for the {@link SingleStoreDBIOSourceProperties} component. */
export interface SingleStoreDBIOSourcePropertiesProps {
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
        readTargetMode?: 'table' | 'query';
        table?: string;
        query?: string;
        enableParallelRead?: boolean;
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
 * Configures properties for Apache Spark SingleStoreDB batch source nodes (.format("singlestore")).
 */
export const SingleStoreDBIOSourceProperties: React.FC<SingleStoreDBIOSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const ddlEndpoint = properties.ddlEndpoint ?? properties.endpoint ?? '';
  const database = properties.database || '';
  const table = properties.table || '';
  const query = properties.query || '';
  const readTargetMode: 'table' | 'query' =
    properties.readTargetMode || (query.trim() && !table.trim() ? 'query' : 'table');
  const enableParallelRead = properties.enableParallelRead ?? true;
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';

  const isEndpointEmpty = !String(ddlEndpoint).trim();
  const isDatabaseEmpty = !database.trim();
  const isTableEmpty = readTargetMode === 'table' && !table.trim();
  const isQueryEmpty = readTargetMode === 'query' && !query.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next = { ...properties, [name]: updatedValue };
    if (name === 'ddlEndpoint') {
      delete next.endpoint;
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark SingleStore Connector</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          .format(&quot;singlestore&quot;) · BATCH
        </span>
      </div>

      {/* DDL Endpoint & Database */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <PropertyField
          id="singleStoreSourceDdlEndpoint"
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

        <PropertyField
          id="singleStoreSourceDatabase"
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
      </div>

      {/* Read Target (Table vs Query) */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="singleStoreSourceReadTargetMode"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Read Target</span>
            <InfoTooltip text="Choose whether to read a SingleStore table or run a custom SQL query." />
          </label>
          <select
            id="singleStoreSourceReadTargetMode"
            name="readTargetMode"
            value={readTargetMode}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="table">Table (.option(&quot;dbtable&quot;, ...))</option>
            <option value="query">SQL Query (.option(&quot;query&quot;, ...))</option>
          </select>
        </div>

        {readTargetMode === 'table' ? (
          <PropertyField
            id="singleStoreSourceTable"
            name="table"
            label="Table Name"
            required="Table name is required in Table mode"
            tooltip="SingleStore table name to read."
            value={table}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Table name is required in Table mode' : null}
            placeholder="sensor_readings"
            onChange={handleChange}
            mono={true}
          />
        ) : (
          <PropertyField
            id="singleStoreSourceQuery"
            name="query"
            label="SQL Query"
            type="textarea"
            rows={3}
            required="SQL query is required in Query mode"
            tooltip="SQL SELECT query executed on SingleStore."
            value={query}
            invalid={isQueryEmpty}
            error={isQueryEmpty ? 'SQL query is required in Query mode' : null}
            placeholder="SELECT id, metric_val, recorded_at FROM sensor_readings WHERE active = 1"
            onChange={handleChange}
            mono={true}
          />
        )}

        <div className="flex items-center gap-2">
          <input
            id="singleStoreSourceEnableParallelRead"
            type="checkbox"
            name="enableParallelRead"
            checked={enableParallelRead}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="singleStoreSourceEnableParallelRead"
            className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
          >
            <span>Enable Parallel Leaf Read (enableParallelRead=automatic)</span>
            <InfoTooltip text="Reads partitions directly from SingleStore leaf aggregators in parallel." />
          </label>
        </div>
      </div>

      {/* Secret Manager Authentication */}
      <SecretManagerAuthFields
        idPrefix="singleStoreSource"
        serviceName="SingleStoreDB"
        authMode={authMode}
        secretName={secretName}
        datalistId="singlestore-source-secret-options"
        secretPlaceholder="e.g. singlestore-password-secret"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="singleStoreSourceInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark SingleStore source code generation..."
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

export const SingleStoreSourceProperties = SingleStoreDBIOSourceProperties;
