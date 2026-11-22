/**
 * JDBC Source Properties Component (SRC27)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Generic JDBC Source nodes
 * (.format("jdbc")).
 *
 * Architectural Decisions:
 * - Provides dbEngine presets ('postgresql' | 'mysql' | 'oracle' | 'sqlserver' | 'db2' | 'custom')
 *   that auto-populate driverClass and JDBC URL examples.
 * - Supports table or SQL query read targets, parallel partitioning with dynamicBounds toggle
 *   or manual lowerBound/upperBound, numPartitions, fetchsize (default 5000), and driverJarUri.
 * - Integrates SecretManagerAuthFields so database passwords are never stored in plaintext.
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type AuthModeOption } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type JdbcDbEngine = 'postgresql' | 'mysql' | 'oracle' | 'sqlserver' | 'db2' | 'custom';
export type JdbcAuthMode = 'secretManager' | 'default';

/**
 * JDBC driver metadata preset for supported relational database engines.
 */
export interface JdbcDriverPreset {
  engine: JdbcDbEngine;
  label: string;
  driverClass: string;
  exampleUrl: string;
}

export const JDBC_DRIVER_PRESETS: JdbcDriverPreset[] = [
  {
    engine: 'postgresql',
    label: 'PostgreSQL',
    driverClass: 'org.postgresql.Driver',
    exampleUrl: 'jdbc:postgresql://localhost:5432/mydb',
  },
  {
    engine: 'mysql',
    label: 'MySQL',
    driverClass: 'com.mysql.cj.jdbc.Driver',
    exampleUrl: 'jdbc:mysql://localhost:3306/mydb',
  },
  {
    engine: 'oracle',
    label: 'Oracle',
    driverClass: 'oracle.jdbc.OracleDriver',
    exampleUrl: 'jdbc:oracle:thin:@//localhost:1521/ORCLPDB1',
  },
  {
    engine: 'sqlserver',
    label: 'SQL Server',
    driverClass: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
    exampleUrl: 'jdbc:sqlserver://localhost:1433;databaseName=mydb',
  },
  {
    engine: 'db2',
    label: 'IBM DB2',
    driverClass: 'com.ibm.db2.jcc.DB2Driver',
    exampleUrl: 'jdbc:db2://localhost:50000/mydb',
  },
  {
    engine: 'custom',
    label: 'Custom JDBC Driver',
    driverClass: '',
    exampleUrl: 'jdbc:<driver>://<host>:<port>/<database>',
  },
];

const JDBC_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'secretManager', label: 'Google Cloud Secret Manager' },
  { value: 'default', label: 'No Password / Embedded Auth' },
];

/**
 * Returns an example JDBC URL for a given driver class or engine preset.
 */
export function getJdbcUrlExample(driverClassOrEngine?: string): string {
  if (!driverClassOrEngine) {
    return 'jdbc:postgresql://localhost:5432/mydb';
  }
  const lower = driverClassOrEngine.toLowerCase();
  if (lower.includes('mysql')) {
    return 'jdbc:mysql://localhost:3306/mydb';
  }
  if (lower.includes('sqlserver') || lower.includes('microsoft')) {
    return 'jdbc:sqlserver://localhost:1433;databaseName=mydb';
  }
  if (lower.includes('oracle')) {
    return 'jdbc:oracle:thin:@//localhost:1521/ORCLPDB1';
  }
  if (lower.includes('db2')) {
    return 'jdbc:db2://localhost:50000/mydb';
  }
  if (lower.includes('postgres')) {
    return 'jdbc:postgresql://localhost:5432/mydb';
  }
  return 'jdbc:<driver>://<host>:<port>/<database>';
}

/** Props for the {@link JdbcIOSourceProperties} component. */
export interface JdbcIOSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        dbEngine?: JdbcDbEngine;
        jdbcUrl?: string;
        url?: string;
        driverClass?: string;
        driverClassName?: string;
        readTargetMode?: 'table' | 'query';
        table?: string;
        tableName?: string;
        query?: string;
        partitionColumn?: string;
        dynamicBounds?: boolean;
        lowerBound?: number | string;
        upperBound?: number | string;
        numPartitions?: number | string;
        fetchsize?: number | string;
        fetchSize?: number | string;
        driverJarUri?: string;
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
 * Configures properties for Apache Spark Generic JDBC batch source nodes (.format("jdbc")).
 */
export const JdbcIOSourceProperties: React.FC<JdbcIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const dbEngine: JdbcDbEngine = properties.dbEngine || 'postgresql';
  const preset = JDBC_DRIVER_PRESETS.find((p) => p.engine === dbEngine) || JDBC_DRIVER_PRESETS[0];
  const driverClass =
    properties.driverClass ?? properties.driverClassName ?? ( preset.driverClass || 'org.postgresql.Driver' );
  const jdbcUrl = properties.jdbcUrl ?? properties.url ?? '';
  const table = properties.table ?? properties.tableName ?? '';
  const query = properties.query || '';
  const readTargetMode: 'table' | 'query' =
    properties.readTargetMode || (query.trim() && !String(table).trim() ? 'query' : 'table');
  const partitionColumn = properties.partitionColumn || '';
  const dynamicBounds = properties.dynamicBounds ?? true;
  const lowerBound = properties.lowerBound ?? '';
  const upperBound = properties.upperBound ?? '';
  const numPartitions = properties.numPartitions ?? 8;
  const fetchsize = properties.fetchsize ?? properties.fetchSize ?? 5000;
  const driverJarUri = properties.driverJarUri || '';
  const username = properties.username || '';
  const authMode = properties.authMode || 'secretManager';
  const secretName = properties.secretName ?? properties.passwordSecret ?? '';

  const isUrlInvalid = !String(jdbcUrl).trim();
  const isDriverInvalid = !String(driverClass).trim();
  const isTableInvalid = readTargetMode === 'table' && !String(table).trim();
  const isQueryInvalid = readTargetMode === 'query' && !query.trim();

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
      }
    }
    if (name === 'jdbcUrl') {
      delete next.url;
    }
    if (name === 'driverClass') {
      delete next.driverClassName;
    }
    if (name === 'table') {
      delete next.tableName;
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Connector Header Badge */}
      <div className="flex flex-col gap-1 p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <div className="flex items-center justify-between">
          <span className="font-semibold text-slate-200">Spark Generic JDBC Source</span>
          <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
            .format(&quot;jdbc&quot;) · BATCH
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
            htmlFor="jdbcSourceDbEngine"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>Database Engine Preset</span>
            <InfoTooltip text="Select a database engine preset to populate the JDBC driver class and sample URL." />
          </label>
          <select
            id="jdbcSourceDbEngine"
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
          id="jdbcDriverClass"
          name="driverClass"
          label="Driver Class"
          required="Driver class is required"
          tooltip="Fully qualified Java JDBC driver class name (.option('driver', ...))."
          value={driverClass}
          invalid={isDriverInvalid}
          error={isDriverInvalid ? 'Driver class is required' : null}
          onChange={handleChange}
          placeholder="org.postgresql.Driver"
          mono={true}
        />
      </div>

      {/* JDBC URL */}
      <PropertyField
        id="jdbcUrl"
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

      {/* Read Target (Table vs Query) */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="jdbcSourceReadTargetMode"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>Read Target</span>
            <InfoTooltip text="Choose whether to read a full table or run a custom SQL query." />
          </label>
          <select
            id="jdbcSourceReadTargetMode"
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
            id="jdbcTable"
            name="table"
            label="Table Name"
            required="Table Name is required"
            tooltip="Database table name to read from."
            value={table}
            invalid={isTableInvalid}
            error={isTableInvalid ? 'Table Name is required' : null}
            onChange={handleChange}
            placeholder="public.orders"
            mono={true}
          />
        ) : (
          <PropertyField
            id="jdbcQuery"
            name="query"
            label="SQL Query"
            type="textarea"
            rows={3}
            required="SQL Query is required"
            tooltip="SQL query executed via Spark JDBC."
            value={query}
            invalid={isQueryInvalid}
            error={isQueryInvalid ? 'SQL Query is required' : null}
            onChange={handleChange}
            placeholder="SELECT id, name, created_at FROM orders WHERE active = 1"
            mono={true}
          />
        )}
      </div>

      {/* Parallel Partitioning & Bounds */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <label className="flex items-center text-xs font-bold uppercase text-slate-500">
          <span>Parallel JDBC Partitioning &amp; Fetch Size</span>
          <InfoTooltip text="Configures parallel partitioned JDBC reads using partitionColumn, lowerBound, upperBound, and numPartitions." />
        </label>

        <div className="grid grid-cols-3 gap-2">
          <PropertyField
            id="jdbcPartitionColumn"
            name="partitionColumn"
            label="Partition Column"
            tooltip="Numeric, date, or timestamp column used to split parallel JDBC reads."
            value={partitionColumn}
            onChange={handleChange}
            placeholder="id"
            mono={true}
          />
          <PropertyField
            id="jdbcNumPartitions"
            name="numPartitions"
            label="Num Partitions"
            type="number"
            min={1}
            tooltip="Number of parallel JDBC partitions."
            value={numPartitions}
            onChange={handleChange}
            placeholder="8"
            mono={true}
          />
          <PropertyField
            id="jdbcFetchsize"
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
            id="jdbcDynamicBounds"
            type="checkbox"
            name="dynamicBounds"
            checked={dynamicBounds}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label htmlFor="jdbcDynamicBounds" className="flex items-center text-xs text-slate-300 cursor-pointer">
            <span>Dynamic Bounds Query (SELECT MIN(col), MAX(col))</span>
            <InfoTooltip text="Automatically runs a pre-flight MIN/MAX query to compute lowerBound and upperBound at runtime." />
          </label>
        </div>

        {!dynamicBounds && (
          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="jdbcLowerBound"
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
              id="jdbcUpperBound"
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

      {/* Authentication & Driver JAR */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <PropertyField
          id="jdbcUsername"
          name="username"
          label="Database Username"
          tooltip="Database login username."
          value={username}
          onChange={handleChange}
          placeholder="db_reader"
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="jdbcSource"
          serviceName="JDBC Database"
          authMode={authMode}
          secretName={secretName}
          authOptions={JDBC_AUTH_OPTIONS}
          secretManagerValue="secretManager"
          datalistId="jdbc-source-secrets-list"
          secretPlaceholder="e.g. jdbc-db-password"
          onChange={handleChange}
        />
      </div>

      <PropertyField
        id="jdbcDriverJarUri"
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
        id="jdbcSourceSystemInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark JDBC source code generation..."
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

export const JdbcSourceProperties = JdbcIOSourceProperties;
