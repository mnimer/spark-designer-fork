/**
 * JDBC Sink Properties Component (SNK24)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Generic JDBC Sink nodes
 * (.write.format("jdbc") and streaming foreachBatch).
 *
 * Architectural Decisions:
 * - Reuses JDBC_DRIVER_PRESETS and getJdbcUrlExample from source/JdbcIO across PostgreSQL,
 *   MySQL, Oracle, SQL Server, IBM DB2, and custom drivers.
 * - Configures destination table, Spark saveMode, safe truncate=true and cascadeTruncate
 *   on overwrite, batchsize (default 5000), numPartitions connection cap, isolationLevel,
 *   and createTableOptions.
 * - Integrates SecretManagerAuthFields and strips legacy plaintext passwords on edit.
 */

import React from 'react';
import { JDBC_DRIVER_PRESETS, getJdbcUrlExample, type JdbcDbEngine } from '../source/JdbcIO';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type AuthModeOption } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type JdbcSaveMode = 'append' | 'overwrite' | 'errorifexists' | 'ignore';
export type JdbcIsolationLevel =
  | 'READ_COMMITTED'
  | 'READ_UNCOMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE'
  | 'NONE';

const JDBC_SINK_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'secretManager', label: 'Google Cloud Secret Manager' },
  { value: 'default', label: 'No Password / Embedded Auth' },
];

/**
 * Infers the database engine preset when only a legacy driverClassName is stored.
 */
function inferEngineFromDriver(driverClass?: string): JdbcDbEngine {
  if (!driverClass) {
    return 'postgresql';
  }
  const matched = JDBC_DRIVER_PRESETS.find((preset) => preset.driverClass === driverClass);
  return matched ? matched.engine : 'custom';
}

/** Props for the {@link JdbcIOSinkProperties} component. */
export interface JdbcIOSinkPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        dbEngine?: JdbcDbEngine;
        driverClass?: string;
        driverClassName?: string;
        jdbcUrl?: string;
        url?: string;
        table?: string;
        dbtable?: string;
        tableName?: string;
        saveMode?: JdbcSaveMode;
        truncate?: boolean;
        cascadeTruncate?: boolean;
        batchsize?: number | string;
        batchSize?: number | string;
        numPartitions?: number | string;
        isolationLevel?: JdbcIsolationLevel;
        createTableOptions?: string;
        driverJarUri?: string;
        driverJars?: string;
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
 * Configures properties for Apache Spark Generic JDBC sink nodes (.write.format("jdbc")).
 */
export const JdbcIOSinkProperties: React.FC<JdbcIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const dbEngine: JdbcDbEngine =
    properties.dbEngine || inferEngineFromDriver(properties.driverClass ?? properties.driverClassName);
  const preset = JDBC_DRIVER_PRESETS.find((p) => p.engine === dbEngine) || JDBC_DRIVER_PRESETS[0];
  const driverClass =
    properties.driverClass ??
    properties.driverClassName ??
    (dbEngine === 'custom' ? '' : preset.driverClass || 'org.postgresql.Driver');
  const jdbcUrl = properties.jdbcUrl ?? properties.url ?? '';
  const table = properties.table ?? properties.dbtable ?? properties.tableName ?? '';
  const saveMode: JdbcSaveMode = properties.saveMode || 'append';
  const truncate = properties.truncate ?? true;
  const cascadeTruncate = properties.cascadeTruncate ?? false;
  const batchsize = properties.batchsize ?? properties.batchSize ?? 5000;
  const numPartitions = properties.numPartitions ?? '';
  const isolationLevel: JdbcIsolationLevel = properties.isolationLevel || 'READ_COMMITTED';
  const createTableOptions = properties.createTableOptions || '';
  const driverJarUri = properties.driverJarUri ?? properties.driverJars ?? '';
  const username = properties.username || '';
  const authMode = properties.authMode || 'secretManager';
  const secretName = properties.secretName ?? properties.passwordSecret ?? '';

  const isDriverInvalid = !String(driverClass).trim();
  const isUrlInvalid = !String(jdbcUrl).trim();
  const isTableInvalid = !String(table).trim();

  const exampleUrl = getJdbcUrlExample(driverClass || dbEngine);
  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    delete next.password;

    if (name === 'dbEngine') {
      const selectedPreset = JDBC_DRIVER_PRESETS.find((p) => p.engine === value);
      if (selectedPreset && selectedPreset.driverClass) {
        next.driverClass = selectedPreset.driverClass;
        delete next.driverClassName;
      }
    }
    if (name === 'driverClass') {
      delete next.driverClassName;
    }
    if (name === 'jdbcUrl') {
      delete next.url;
    }
    if (name === 'table') {
      delete next.dbtable;
      delete next.tableName;
    }
    if (name === 'batchsize') {
      delete next.batchSize;
    }
    if (name === 'driverJarUri') {
      delete next.driverJars;
    }
    if (name === 'secretName') {
      delete next.passwordSecret;
    }

    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Connector Header Badge */}
      <div className="flex flex-col gap-1 p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <div className="flex items-center justify-between">
          <span className="font-semibold text-slate-200">Spark Generic JDBC Sink</span>
          <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
            .write.format(&quot;jdbc&quot;) · foreachBatch
          </span>
        </div>
        {dbEngine === 'sqlserver' && (
          <span className="text-[11px] text-amber-300 font-mono">
            SQL Server TLS note: sets dataproc.artifacts.remove=conscrypt
          </span>
        )}
      </div>

      {/* Database Engine Preset & Driver Class */}
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-2">
          <label
            htmlFor="jdbcSinkDbEngine"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>Database Engine Preset</span>
            <InfoTooltip text="Select a database engine preset to populate the JDBC driver class and sample URL." />
          </label>
          <select
            id="jdbcSinkDbEngine"
            name="dbEngine"
            value={dbEngine}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            {JDBC_DRIVER_PRESETS.map((p) => (
              <option key={p.engine} value={p.engine}>
                {p.label}
              </option>
            ))}
          </select>
        </div>

        <PropertyField
          id="jdbcSinkDriverClass"
          name="driverClass"
          label="Driver Class"
          required="Driver class is required"
          tooltip="Fully qualified Java JDBC driver class name passed to .option('driver', ...)."
          value={driverClass}
          invalid={isDriverInvalid}
          error={isDriverInvalid ? 'Driver class is required' : null}
          onChange={handleChange}
          placeholder="org.postgresql.Driver"
          mono={true}
        />
      </div>

      {/* JDBC Connection URL */}
      <PropertyField
        id="jdbcSinkJdbcUrl"
        name="jdbcUrl"
        label="JDBC Connection URL"
        required="JDBC Connection URL is required"
        tooltip="JDBC connection URL including host, port, and database name."
        value={jdbcUrl}
        invalid={isUrlInvalid}
        error={isUrlInvalid ? 'JDBC Connection URL is required' : null}
        onChange={handleChange}
        placeholder={exampleUrl}
        mono={true}
      />

      {/* Destination Table & Save Mode */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <label className="flex items-center text-xs font-bold uppercase text-slate-500">
          <span>Target Table &amp; Save Mode</span>
          <InfoTooltip text="Configures destination dbtable, Spark save mode, and safe TRUNCATE behavior on overwrite." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="jdbcSinkTable"
            name="table"
            label="Destination Table (dbtable)"
            required="Destination table is required"
            tooltip="Target relational schema and table name passed to .option('dbtable', ...)."
            value={table}
            invalid={isTableInvalid}
            error={isTableInvalid ? 'Destination table is required' : null}
            onChange={handleChange}
            placeholder="public.orders_summary"
            mono={true}
          />

          <div className="flex flex-col gap-2">
            <label
              htmlFor="jdbcSinkSaveMode"
              className="flex items-center text-xs font-bold uppercase text-slate-500"
            >
              <span>Save Mode</span>
              <InfoTooltip text="Controls how Spark handles existing data in the destination table." />
            </label>
            <select
              id="jdbcSinkSaveMode"
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
          <div className="flex flex-col gap-2 pt-1">
            <div className="flex items-center gap-2">
              <input
                id="jdbcSinkTruncate"
                type="checkbox"
                name="truncate"
                checked={truncate}
                onChange={handleChange}
                className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
              />
              <label
                htmlFor="jdbcSinkTruncate"
                className="flex items-center text-xs text-slate-300 cursor-pointer"
              >
                <span>Truncate Table on Overwrite (truncate=true)</span>
                <InfoTooltip text="Issues TRUNCATE TABLE instead of dropping and recreating the table so indexes, grants, and foreign keys survive." />
              </label>
            </div>

            {truncate && (
              <div className="flex items-center gap-2 pl-6">
                <input
                  id="jdbcSinkCascadeTruncate"
                  type="checkbox"
                  name="cascadeTruncate"
                  checked={cascadeTruncate}
                  onChange={handleChange}
                  className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
                />
                <label
                  htmlFor="jdbcSinkCascadeTruncate"
                  className="flex items-center text-xs text-slate-300 cursor-pointer"
                >
                  <span>Cascade Truncate (cascadeTruncate=true)</span>
                  <InfoTooltip text="Appends CASCADE to TRUNCATE TABLE on databases such as PostgreSQL and Oracle." />
                </label>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Write Batching, Connection Cap & Isolation */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <label className="flex items-center text-xs font-bold uppercase text-slate-500">
          <span>Batching, Connections &amp; Transactions</span>
          <InfoTooltip text="Controls JDBC batchsize, maximum concurrent write partitions, transaction isolation, and DDL options." />
        </label>

        <div className="grid grid-cols-3 gap-2">
          <PropertyField
            id="jdbcSinkBatchsize"
            name="batchsize"
            label="Batch Size (batchsize)"
            type="number"
            min={1}
            tooltip="Rows buffered per JDBC executeBatch call (default 5000)."
            value={batchsize}
            onChange={handleChange}
            placeholder="5000"
            mono={true}
          />
          <PropertyField
            id="jdbcSinkNumPartitions"
            name="numPartitions"
            label="Max Partitions"
            type="number"
            min={1}
            tooltip="Caps concurrent executor JDBC connections during writes (.option('numPartitions', ...))."
            value={numPartitions}
            onChange={handleChange}
            placeholder="8"
            mono={true}
          />
          <div className="flex flex-col gap-2">
            <label
              htmlFor="jdbcSinkIsolationLevel"
              className="flex items-center text-xs font-bold uppercase text-slate-500"
            >
              <span>Isolation Level</span>
              <InfoTooltip text="JDBC transaction isolation level for each partition write." />
            </label>
            <select
              id="jdbcSinkIsolationLevel"
              name="isolationLevel"
              value={isolationLevel}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="READ_COMMITTED">READ_COMMITTED</option>
              <option value="READ_UNCOMMITTED">READ_UNCOMMITTED</option>
              <option value="REPEATABLE_READ">REPEATABLE_READ</option>
              <option value="SERIALIZABLE">SERIALIZABLE</option>
              <option value="NONE">NONE</option>
            </select>
          </div>
        </div>

        <PropertyField
          id="jdbcSinkCreateTableOptions"
          name="createTableOptions"
          label="Create Table Options (createTableOptions)"
          tooltip="Optional database-specific DDL clause appended to CREATE TABLE when creating new tables."
          value={createTableOptions}
          onChange={handleChange}
          placeholder="ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
          mono={true}
        />
      </div>

      {/* Authentication & Driver JAR */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <PropertyField
          id="jdbcSinkUsername"
          name="username"
          label="Database Username"
          tooltip="Database login username."
          value={username}
          onChange={handleChange}
          placeholder="db_writer"
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="jdbcSink"
          serviceName="JDBC Database"
          authMode={authMode}
          secretName={secretName}
          authOptions={JDBC_SINK_AUTH_OPTIONS}
          secretManagerValue="secretManager"
          datalistId="jdbc-sink-secrets-list"
          secretPlaceholder="e.g. jdbc-db-password"
          onChange={handleChange}
        />
      </div>

      <PropertyField
        id="jdbcSinkDriverJarUri"
        name="driverJarUri"
        label="JDBC Driver JAR URI (gs://)"
        tooltip="Cloud Storage URI for the vendor JDBC driver JAR passed to --jars."
        value={driverJarUri}
        onChange={handleChange}
        placeholder="gs://my-bucket/jars/postgresql-42.7.4.jar"
        mono={true}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="jdbcSinkSystemInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark JDBC sink code generation..."
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

export const JdbcSinkProperties = JdbcIOSinkProperties;
