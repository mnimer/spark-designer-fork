/**
 * @fileoverview Cloud SQL Sink Properties Component (SNK22).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud SQL Sink nodes
 * in Apache Spark 4.x pipelines (.write.format("jdbc") and streaming foreachBatch
 * via the Cloud SQL JDBC Socket Factory).
 *
 * Architectural Decisions:
 * - Supports PostgreSQL, MySQL, and SQL Server engines over Cloud SQL Socket Factory,
 *   displaying the Conscrypt removal requirement when SQL Server is selected.
 * - Uses useAssetLoader('sqlInstances') and gated useAssetLoader('sqlDatabases') for live
 *   GCP Cloud SQL discovery.
 * - Exposes Spark saveMode with safe truncate=true default on overwrite so indexes and
 *   foreign keys survive, plus batchsize=5000, isolationLevel, and numPartitions.
 * - Supports IAM database authentication or Google Cloud Secret Manager credentials.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import {
  SecretManagerAuthFields,
  type AuthModeOption,
} from '../../components/common/SecretManagerAuthFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { useAssetLoader } from '../../hooks/useAssetLoader';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type CloudSqlDbEngine = 'postgresql' | 'mysql' | 'sqlserver';
export type CloudSqlAuthMode = 'iam' | 'secret_manager';
export type CloudSqlSaveMode = 'append' | 'overwrite' | 'errorifexists' | 'ignore';
export type CloudSqlIsolationLevel =
  | 'READ_COMMITTED'
  | 'READ_UNCOMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE'
  | 'NONE';

const CONNECTION_NAME_RE = /^(?:[a-z0-9.-]+:)?[a-z0-9-]+:[a-z0-9-]+:[a-z][a-z0-9-]*$/i;
const DATABASE_RE = /^[^\s/?#@:]+$/;
const NO_WHITESPACE_RE = /^\S+$/;

const CLOUD_SQL_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'iam', label: 'Cloud SQL IAM Database Authentication (enableIamAuth=true)' },
  { value: 'secret_manager', label: 'Google Cloud Secret Manager (Password Secret)' },
];

/** Props for the {@link CloudSQLSinkProperties} component. */
export interface CloudSQLSinkPropertiesProps {
  node: {
    data: {
      properties?: {
        instanceConnectionName?: string;
        cloudSqlInstance?: string;
        dbEngine?: CloudSqlDbEngine;
        database?: string;
        table?: string;
        saveMode?: CloudSqlSaveMode;
        truncate?: boolean;
        batchsize?: number | string;
        isolationLevel?: CloudSqlIsolationLevel;
        numPartitions?: number | string;
        authMode?: CloudSqlAuthMode | string;
        username?: string;
        secretName?: string;
        passwordSecret?: string;
        driverJarUri?: string;
        socketFactoryJarUri?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
      system_instructions?: string;
      special_instructions?: string;
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Dedicated property panel for the Apache Spark Cloud SQL sink node (.write.format("jdbc")).
 */
export const CloudSQLSinkProperties: React.FC<CloudSQLSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const instanceConnectionName = String(
    properties.instanceConnectionName ?? properties.cloudSqlInstance ?? ''
  );
  const dbEngine: CloudSqlDbEngine = properties.dbEngine || 'postgresql';
  const database = properties.database || '';
  const table = properties.table || '';
  const saveMode: CloudSqlSaveMode = properties.saveMode || 'append';
  const truncate = properties.truncate ?? true;
  const batchsize = properties.batchsize ?? 5000;
  const isolationLevel: CloudSqlIsolationLevel = properties.isolationLevel || 'READ_COMMITTED';
  const numPartitions = properties.numPartitions ?? '';
  const authMode =
    dbEngine === 'sqlserver' && !properties.authMode
      ? 'secret_manager'
      : properties.authMode || 'iam';
  const username = properties.username || '';
  const secretName = String(properties.secretName ?? properties.passwordSecret ?? '');
  const driverJarUri = properties.driverJarUri || '';
  const socketFactoryJarUri = properties.socketFactoryJarUri || '';

  const {
    items: sqlInstances,
    loading: sqlInstancesLoading,
    error: sqlInstancesError,
  } = useAssetLoader<string>('sqlInstances');

  const instanceTrimmed = instanceConnectionName.trim();
  const instanceWellFormed = CONNECTION_NAME_RE.test(instanceTrimmed);

  const {
    items: sqlDatabases,
    loading: sqlDatabasesLoading,
    error: sqlDatabasesError,
  } = useAssetLoader<string>('sqlDatabases', {
    params: { instance: instanceTrimmed },
    enabled: instanceWellFormed,
  });

  const isInstanceEmpty = instanceTrimmed.length === 0;
  const instanceFormatError =
    !isInstanceEmpty && !instanceWellFormed
      ? 'Use the instance connection name PROJECT:REGION:INSTANCE'
      : null;
  const instanceError = instanceFormatError ?? sqlInstancesError;

  const databaseTrimmed = database.trim();
  const isDatabaseEmpty = databaseTrimmed.length === 0;
  const databaseFormatError =
    !isDatabaseEmpty && !DATABASE_RE.test(databaseTrimmed)
      ? 'Database name cannot contain spaces or / ? # @ :'
      : null;
  const databaseError =
    databaseFormatError ?? (instanceWellFormed ? sqlDatabasesError : null);

  const tableTrimmed = table.trim();
  const isTableEmpty = tableTrimmed.length === 0;
  const tableError =
    !isTableEmpty && !NO_WHITESPACE_RE.test(tableTrimmed)
      ? 'Table name cannot contain whitespace'
      : null;

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    if (name === 'instanceConnectionName') {
      delete next.cloudSqlInstance;
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
          <span className="font-semibold text-slate-200">
            Spark Cloud SQL JDBC + Socket Factory
          </span>
          <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 font-mono text-[11px]">
            {'.format("jdbc") · foreachBatch'}
          </span>
        </div>
        {dbEngine === 'sqlserver' && (
          <span className="text-[11px] text-amber-300 font-mono">
            Requires spark property: dataproc.artifacts.remove=conscrypt
          </span>
        )}
      </div>

      {/* Instance Connection Name, Engine, Database, and Table */}
      <PropertyField
        id="cloudSqlSinkInstance"
        name="instanceConnectionName"
        label="Instance Connection Name"
        required="Instance connection name is required"
        tooltip="Cloud SQL instance connection name (project:region:instance)."
        value={instanceConnectionName}
        invalid={isInstanceEmpty || Boolean(instanceError)}
        error={isInstanceEmpty ? 'Instance connection name is required' : instanceError}
        onChange={handleChange}
        placeholder="my-project:us-central1:my-instance"
        list="cloudsql-sink-instance-options"
        datalist={sqlInstances}
        loading={sqlInstancesLoading}
        mono={true}
      />

      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-2">
          <label
            htmlFor="cloudSqlSinkDbEngine"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>Database Engine</span>
            <InfoTooltip text="Cloud SQL database engine determining the JDBC driver and Socket Factory class." />
          </label>
          <select
            id="cloudSqlSinkDbEngine"
            name="dbEngine"
            value={dbEngine}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="postgresql">PostgreSQL</option>
            <option value="mysql">MySQL</option>
            <option value="sqlserver">SQL Server</option>
          </select>
        </div>

        <PropertyField
          id="cloudSqlSinkDatabase"
          name="database"
          label="Database"
          required="Database is required"
          tooltip="Destination database name on the Cloud SQL instance."
          value={database}
          invalid={isDatabaseEmpty || Boolean(databaseError)}
          error={isDatabaseEmpty ? 'Database is required' : databaseError}
          onChange={handleChange}
          placeholder="postgres"
          list="cloudsql-sink-database-options"
          datalist={instanceWellFormed ? sqlDatabases : []}
          loading={instanceWellFormed ? sqlDatabasesLoading : false}
          mono={true}
        />
      </div>

      <PropertyField
        id="cloudSqlSinkTable"
        name="table"
        label="Destination Table"
        required="Table name is required"
        tooltip="Destination database table name passed to .option('dbtable', ...), such as public.orders."
        value={table}
        invalid={isTableEmpty || Boolean(tableError)}
        error={isTableEmpty ? 'Table name is required' : tableError}
        onChange={handleChange}
        placeholder="public.orders"
        mono={true}
      />

      {/* Save Mode, Safe Truncate, Batch Size, and Isolation Level */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <label className="flex items-center text-xs font-bold uppercase text-slate-500">
          <span>JDBC Write and Transaction Settings</span>
          <InfoTooltip text="Configure Spark JDBC save mode, safe table truncation, multi-row insert batch size, and transaction isolation." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-2">
            <label
              htmlFor="cloudSqlSinkSaveMode"
              className="flex items-center text-xs font-bold uppercase text-slate-500"
            >
              <span>Save Mode</span>
              <InfoTooltip text="Spark DataFrameWriter save mode (.mode(...))." />
            </label>
            <select
              id="cloudSqlSinkSaveMode"
              name="saveMode"
              value={saveMode}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="append">Append (append)</option>
              <option value="overwrite">Overwrite (overwrite)</option>
              <option value="errorifexists">Error If Exists (errorifexists)</option>
              <option value="ignore">Ignore If Exists (ignore)</option>
            </select>
          </div>

          <div className="flex flex-col gap-2">
            <label
              htmlFor="cloudSqlSinkIsolationLevel"
              className="flex items-center text-xs font-bold uppercase text-slate-500"
            >
              <span>Isolation Level</span>
              <InfoTooltip text="JDBC transaction isolation level per executor partition (.option('isolationLevel', ...))." />
            </label>
            <select
              id="cloudSqlSinkIsolationLevel"
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

        {saveMode === 'overwrite' && (
          <div className="flex items-center gap-2">
            <input
              id="cloudSqlSinkTruncate"
              type="checkbox"
              name="truncate"
              checked={truncate}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
            />
            <label
              htmlFor="cloudSqlSinkTruncate"
              className="flex items-center text-xs text-slate-300 cursor-pointer"
            >
              <span>Safe Truncate on Overwrite (truncate=true)</span>
              <InfoTooltip text="Executes TRUNCATE TABLE instead of DROP and CREATE so primary keys, indexes, foreign keys, and grants survive." />
            </label>
          </div>
        )}

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="cloudSqlSinkBatchsize"
            name="batchsize"
            label="Batch Size"
            type="number"
            min={1}
            tooltip="Number of rows grouped per JDBC batch insert round trip (default 5000)."
            value={batchsize}
            onChange={handleChange}
            placeholder="5000"
            mono={true}
          />

          <PropertyField
            id="cloudSqlSinkNumPartitions"
            name="numPartitions"
            label="Max Write Partitions (Optional)"
            type="number"
            min={1}
            tooltip="Optional maximum parallel JDBC writer connections (.option('numPartitions', ...)) to protect Cloud SQL max_connections."
            value={numPartitions}
            onChange={handleChange}
            placeholder="e.g. 8"
            mono={true}
          />
        </div>
      </div>

      {/* Authentication */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <PropertyField
          id="cloudSqlSinkUsername"
          name="username"
          label="Database Username"
          tooltip="Database user account or Cloud SQL IAM principal name."
          value={username}
          onChange={handleChange}
          placeholder="postgres"
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="cloudSqlSink"
          serviceName="Cloud SQL"
          authMode={authMode}
          secretName={secretName}
          authOptions={CLOUD_SQL_AUTH_OPTIONS}
          secretManagerValue="secret_manager"
          datalistId="cloudsql-sink-secret-options"
          secretPlaceholder="e.g. cloudsql-db-password"
          onChange={handleChange}
        />
      </div>

      {/* Runtime JAR URIs */}
      <div className="grid grid-cols-2 gap-2">
        <PropertyField
          id="cloudSqlSinkDriverJarUri"
          name="driverJarUri"
          label="JDBC Driver JAR URI (gs://)"
          tooltip="Cloud Storage URI for the JDBC driver JAR passed to --jars."
          value={driverJarUri}
          onChange={handleChange}
          placeholder="gs://my-bucket/jars/postgresql-42.7.4.jar"
          mono={true}
        />
        <PropertyField
          id="cloudSqlSinkSocketFactoryJarUri"
          name="socketFactoryJarUri"
          label="Socket Factory Uber JAR URI (gs://)"
          tooltip="Cloud Storage URI for the Cloud SQL JDBC Socket Factory Uber JAR."
          value={socketFactoryJarUri}
          onChange={handleChange}
          placeholder="gs://my-bucket/jars/postgres-socket-factory-1.21.0-jar-with-dependencies.jar"
          mono={true}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="cloudSqlSinkSystemInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Cloud SQL sink code generation..."
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

export const CloudSqlSinkProperties = CloudSQLSinkProperties;
