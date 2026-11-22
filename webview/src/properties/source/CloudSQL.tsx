/**
 * Cloud SQL Source Properties Component (SRC25)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud SQL Source nodes
 * in Apache Spark 4.x pipelines (.format("jdbc") + Cloud SQL JDBC Socket Factory).
 *
 * Architectural Decisions:
 * - Supports PostgreSQL, MySQL, and SQL Server engines over Cloud SQL Socket Factory.
 * - Supports IAM database authentication or Google Cloud Secret Manager credentials.
 * - Configures parallel partitioned reads with dynamic MIN/MAX bounds query toggle
 *   or manual lowerBound/upperBound, numPartitions (default 8), and fetchsize (default 5000).
 * - Exposes driverJarUri and socketFactoryJarUri for Dataproc / Serverless Spark --jars.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type AuthModeOption } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';
import { useAssetLoader } from '../../hooks/useAssetLoader';

export type CloudSqlDbEngine = 'postgresql' | 'mysql' | 'sqlserver';
export type CloudSqlAuthMode = 'iam' | 'secret_manager';

const CONNECTION_NAME_RE = /^(?:[a-z0-9.-]+:)?[a-z0-9-]+:[a-z0-9-]+:[a-z][a-z0-9-]*$/i;

const CLOUD_SQL_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'iam', label: 'Cloud SQL IAM Database Authentication (enableIamAuth=true)' },
  { value: 'secret_manager', label: 'Google Cloud Secret Manager (Password Secret)' },
];

/** Props for the {@link CloudSQLSourceProperties} component. */
export interface CloudSQLSourcePropertiesProps {
  node: {
    data: {
      properties?: {
        instanceConnectionName?: string;
        cloudSqlInstance?: string;
        dbEngine?: CloudSqlDbEngine;
        database?: string;
        readTargetMode?: 'table' | 'query';
        table?: string;
        query?: string;
        authMode?: CloudSqlAuthMode | string;
        username?: string;
        secretName?: string;
        passwordSecret?: string;
        partitionColumn?: string;
        numPartitions?: number | string;
        dynamicBounds?: boolean;
        lowerBound?: number | string;
        upperBound?: number | string;
        fetchsize?: number | string;
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
 * Dedicated property panel for the Apache Spark Cloud SQL source node (.format("jdbc")).
 */
export const CloudSQLSourceProperties: React.FC<CloudSQLSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const instanceConnectionName = properties.instanceConnectionName ?? properties.cloudSqlInstance ?? '';
  const dbEngine: CloudSqlDbEngine = properties.dbEngine || 'postgresql';
  const database = properties.database || '';
  const table = properties.table || '';
  const query = properties.query || '';
  const readTargetMode: 'table' | 'query' =
    properties.readTargetMode || (query.trim() && !table.trim() ? 'query' : 'table');
  const authMode = properties.authMode || 'iam';
  const username = properties.username || '';
  const secretName = properties.secretName ?? properties.passwordSecret ?? '';
  const partitionColumn = properties.partitionColumn || '';
  const numPartitions = properties.numPartitions ?? 8;
  const dynamicBounds = properties.dynamicBounds ?? true;
  const lowerBound = properties.lowerBound ?? '';
  const upperBound = properties.upperBound ?? '';
  const fetchsize = properties.fetchsize ?? 5000;
  const driverJarUri = properties.driverJarUri || '';
  const socketFactoryJarUri = properties.socketFactoryJarUri || '';

  const { items: sqlInstances, loading: sqlInstancesLoading, error: sqlInstancesError } =
    useAssetLoader<string>('sqlInstances');

  const instanceTrimmed = String(instanceConnectionName).trim();
  const instanceWellFormed = CONNECTION_NAME_RE.test(instanceTrimmed);

  const { items: sqlDatabases, loading: sqlDatabasesLoading, error: sqlDatabasesError } =
    useAssetLoader<string>('sqlDatabases', {
      params: { instance: instanceTrimmed },
      enabled: instanceWellFormed,
    });

  const isInstanceEmpty = instanceTrimmed.length === 0;
  const instanceFormatError =
    !isInstanceEmpty && !instanceWellFormed
      ? 'Use the instance connection name PROJECT:REGION:INSTANCE'
      : null;
  const instanceError = instanceFormatError ?? sqlInstancesError;
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
    if (name === 'instanceConnectionName') {
      delete next.cloudSqlInstance;
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Connector Header Badge */}
      <div className="flex flex-col gap-1 p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <div className="flex items-center justify-between">
          <span className="font-semibold text-slate-200">Spark Cloud SQL JDBC + Socket Factory</span>
          <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
            .format(&quot;jdbc&quot;) · BATCH
          </span>
        </div>
        {dbEngine === 'sqlserver' && (
          <span className="text-[11px] text-amber-300 font-mono">
            Requires spark property: dataproc.artifacts.remove=conscrypt
          </span>
        )}
      </div>

      {/* Instance Connection Name & Engine */}
      <PropertyField
        id="cloudSqlSourceInstance"
        name="instanceConnectionName"
        label="Instance Connection Name"
        required="Instance connection name is required"
        tooltip="Cloud SQL instance connection name (project:region:instance)."
        value={instanceConnectionName}
        invalid={isInstanceEmpty || Boolean(instanceError)}
        error={isInstanceEmpty ? 'Instance connection name is required' : instanceError}
        onChange={handleChange}
        placeholder="my-project:us-central1:my-instance"
        list="cloudsql-source-instance-options"
        datalist={sqlInstances}
        loading={sqlInstancesLoading}
        mono={true}
      />

      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-2">
          <label
            htmlFor="cloudSqlSourceDbEngine"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>Database Engine</span>
            <InfoTooltip text="Cloud SQL database engine determining the JDBC driver and Socket Factory class." />
          </label>
          <select
            id="cloudSqlSourceDbEngine"
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
          id="cloudSqlSourceDatabase"
          name="database"
          label="Database"
          required="Database is required"
          tooltip="Database name on the Cloud SQL instance."
          value={database}
          invalid={isDatabaseEmpty}
          error={isDatabaseEmpty ? 'Database is required' : instanceWellFormed ? sqlDatabasesError : null}
          onChange={handleChange}
          placeholder="postgres"
          list="cloudsql-source-database-options"
          datalist={instanceWellFormed ? sqlDatabases : []}
          loading={instanceWellFormed ? sqlDatabasesLoading : false}
          mono={true}
        />
      </div>

      {/* Read Target Mode (Table vs Query) */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="cloudSqlSourceReadTargetMode"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>Read Target</span>
            <InfoTooltip text="Choose whether to read a database table or execute a custom SQL query." />
          </label>
          <select
            id="cloudSqlSourceReadTargetMode"
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
            id="cloudSqlSourceTable"
            name="table"
            label="Table Name"
            required="Table name is required in Table mode"
            tooltip="Database table name (for example public.orders)."
            value={table}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Table name is required in Table mode' : null}
            onChange={handleChange}
            placeholder="public.orders"
            mono={true}
          />
        ) : (
          <PropertyField
            id="cloudSqlSourceQuery"
            name="query"
            label="SQL Query"
            type="textarea"
            rows={3}
            required="SQL query is required in Query mode"
            tooltip="SQL SELECT query executed via Spark JDBC."
            value={query}
            invalid={isQueryEmpty}
            error={isQueryEmpty ? 'SQL query is required in Query mode' : null}
            onChange={handleChange}
            placeholder="SELECT order_id, customer_id, total FROM orders WHERE active = true"
            mono={true}
          />
        )}
      </div>

      {/* Parallel Partitioning & Bounds */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <label className="flex items-center text-xs font-bold uppercase text-slate-500">
          <span>Parallel JDBC Partitioning &amp; Fetch Size</span>
          <InfoTooltip text="Configures parallel partitioned JDBC reads across Spark executors using partitionColumn and numPartitions." />
        </label>

        <div className="grid grid-cols-3 gap-2">
          <PropertyField
            id="cloudSqlSourcePartitionColumn"
            name="partitionColumn"
            label="Partition Column"
            tooltip="Numeric, date, or timestamp column used to split parallel JDBC reads."
            value={partitionColumn}
            onChange={handleChange}
            placeholder="order_id"
            mono={true}
          />
          <PropertyField
            id="cloudSqlSourceNumPartitions"
            name="numPartitions"
            label="Num Partitions"
            type="number"
            min={1}
            tooltip="Number of parallel JDBC partitions (default 8)."
            value={numPartitions}
            onChange={handleChange}
            placeholder="8"
            mono={true}
          />
          <PropertyField
            id="cloudSqlSourceFetchsize"
            name="fetchsize"
            label="Fetch Size"
            type="number"
            min={1}
            tooltip="JDBC statement fetch size per round trip (default 5000)."
            value={fetchsize}
            onChange={handleChange}
            placeholder="5000"
            mono={true}
          />
        </div>

        <div className="flex items-center gap-2">
          <input
            id="cloudSqlSourceDynamicBounds"
            type="checkbox"
            name="dynamicBounds"
            checked={dynamicBounds}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="cloudSqlSourceDynamicBounds"
            className="flex items-center text-xs text-slate-300 cursor-pointer"
          >
            <span>Dynamic Bounds Query (SELECT MIN(col), MAX(col))</span>
            <InfoTooltip text="Automatically runs a pre-flight MIN/MAX query to compute lowerBound and upperBound at runtime." />
          </label>
        </div>

        {!dynamicBounds && (
          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="cloudSqlSourceLowerBound"
              name="lowerBound"
              label="Lower Bound"
              type="number"
              tooltip="Manual lower bound for partitionColumn stride calculation."
              value={lowerBound}
              onChange={handleChange}
              placeholder="1"
              mono={true}
            />
            <PropertyField
              id="cloudSqlSourceUpperBound"
              name="upperBound"
              label="Upper Bound"
              type="number"
              tooltip="Manual upper bound for partitionColumn stride calculation."
              value={upperBound}
              onChange={handleChange}
              placeholder="1000000"
              mono={true}
            />
          </div>
        )}
      </div>

      {/* Authentication */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <PropertyField
          id="cloudSqlSourceUsername"
          name="username"
          label="Database Username"
          tooltip="Database user account or IAM principal name."
          value={username}
          onChange={handleChange}
          placeholder="postgres"
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="cloudSqlSource"
          serviceName="Cloud SQL"
          authMode={authMode}
          secretName={secretName}
          authOptions={CLOUD_SQL_AUTH_OPTIONS}
          secretManagerValue="secret_manager"
          datalistId="cloudsql-source-secret-options"
          secretPlaceholder="e.g. cloudsql-db-password"
          onChange={handleChange}
        />
      </div>

      {/* Runtime JAR URIs */}
      <div className="grid grid-cols-2 gap-2">
        <PropertyField
          id="cloudSqlSourceDriverJarUri"
          name="driverJarUri"
          label="JDBC Driver JAR URI (gs://)"
          tooltip="Cloud Storage URI for the JDBC driver JAR passed to --jars."
          value={driverJarUri}
          onChange={handleChange}
          placeholder="gs://my-bucket/jars/postgresql-42.7.4.jar"
          mono={true}
        />
        <PropertyField
          id="cloudSqlSourceSocketFactoryJarUri"
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
        id="cloudSqlSourceSystemInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Cloud SQL source code generation..."
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

export const CloudSqlSourceProperties = CloudSQLSourceProperties;
