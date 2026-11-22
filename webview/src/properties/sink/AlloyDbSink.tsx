/**
 * @fileoverview AlloyDB Sink Properties Component (SNK23).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud AlloyDB for PostgreSQL
 * Sink nodes in Apache Spark 4.x pipelines (.write.format("jdbc") and streaming
 * foreachBatch via com.google.cloud.alloydb.SocketFactory).
 *
 * Architectural Decisions:
 * - Displays the Managed Spark Conscrypt removal requirement (dataproc.artifacts.remove=conscrypt).
 * - Validates alloydbInstanceUri format (projects/<p>/locations/<r>/clusters/<c>/instances/<i>),
 *   database, and destination table names.
 * - Exposes ipType ('PRIVATE' | 'PUBLIC' | 'PSC'), saveMode with safe truncate=true default on
 *   overwrite, batchsize=5000, isolationLevel, numPartitions, IAM vs Secret Manager auth,
 *   and driverJarUri / socketFactoryJarUri fields.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import {
  SecretManagerAuthFields,
  type AuthModeOption,
} from '../../components/common/SecretManagerAuthFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type AlloyDbIpType = 'PRIVATE' | 'PUBLIC' | 'PSC';
export type AlloyDbAuthMode = 'iam' | 'secret_manager';
export type AlloyDbSaveMode = 'append' | 'overwrite' | 'errorifexists' | 'ignore';
export type AlloyDbIsolationLevel =
  | 'READ_COMMITTED'
  | 'READ_UNCOMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE'
  | 'NONE';

const ALLOYDB_URI_RE =
  /^projects\/[^/]+\/locations\/[^/]+\/clusters\/[^/]+\/instances\/[^/]+$/i;
const DATABASE_RE = /^[^\s/?#@:]+$/;
const NO_WHITESPACE_RE = /^\S+$/;

const ALLOYDB_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'iam', label: 'AlloyDB IAM Authentication (alloydbEnableIAMAuth=true)' },
  { value: 'secret_manager', label: 'Google Cloud Secret Manager (Password Secret)' },
];

/** Props for the {@link AlloyDbSinkProperties} component. */
export interface AlloyDbSinkPropertiesProps {
  node: {
    data: {
      properties?: {
        alloydbInstanceUri?: string;
        alloydbInstanceName?: string;
        ipType?: AlloyDbIpType;
        database?: string;
        table?: string;
        saveMode?: AlloyDbSaveMode;
        truncate?: boolean;
        batchsize?: number | string;
        isolationLevel?: AlloyDbIsolationLevel;
        numPartitions?: number | string;
        authMode?: AlloyDbAuthMode | string;
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
 * Configures properties for Apache Spark AlloyDB for PostgreSQL batch and micro-batch sink nodes.
 */
export const AlloyDbSinkProperties: React.FC<AlloyDbSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const alloydbInstanceUri = String(
    properties.alloydbInstanceUri ?? properties.alloydbInstanceName ?? ''
  );
  const ipType: AlloyDbIpType = properties.ipType || 'PRIVATE';
  const database = properties.database || '';
  const table = properties.table || '';
  const saveMode: AlloyDbSaveMode = properties.saveMode || 'append';
  const truncate = properties.truncate ?? true;
  const batchsize = properties.batchsize ?? 5000;
  const isolationLevel: AlloyDbIsolationLevel = properties.isolationLevel || 'READ_COMMITTED';
  const numPartitions = properties.numPartitions ?? '';
  const authMode = properties.authMode || 'iam';
  const username = properties.username || '';
  const secretName = String(properties.secretName ?? properties.passwordSecret ?? '');
  const driverJarUri = properties.driverJarUri || '';
  const socketFactoryJarUri = properties.socketFactoryJarUri || '';

  const uriTrimmed = alloydbInstanceUri.trim();
  const isUriEmpty = uriTrimmed.length === 0;
  const isUriMalformed = !isUriEmpty && !ALLOYDB_URI_RE.test(uriTrimmed);

  const databaseTrimmed = database.trim();
  const isDatabaseEmpty = databaseTrimmed.length === 0;
  const databaseError =
    !isDatabaseEmpty && !DATABASE_RE.test(databaseTrimmed)
      ? 'Database name cannot contain spaces or / ? # @ :'
      : null;

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
    if (name === 'alloydbInstanceUri') {
      delete next.alloydbInstanceName;
    }
    if (name === 'secretName') {
      delete next.passwordSecret;
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Connector Header and Conscrypt Note */}
      <div className="flex flex-col gap-1 p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <div className="flex items-center justify-between">
          <span className="font-semibold text-slate-200">
            Spark AlloyDB JDBC + Socket Factory
          </span>
          <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 font-mono text-[11px]">
            {'.format("jdbc") · foreachBatch'}
          </span>
        </div>
        <span className="text-[11px] text-amber-300 font-mono">
          Managed Spark property: dataproc.artifacts.remove=conscrypt
        </span>
      </div>

      {/* AlloyDB Instance URI */}
      <PropertyField
        id="alloydbSinkInstanceUri"
        name="alloydbInstanceUri"
        label="AlloyDB Instance URI"
        required="AlloyDB instance URI is required"
        tooltip="Full AlloyDB instance resource URI passed as alloydbInstanceName (projects/<p>/locations/<r>/clusters/<c>/instances/<i>)."
        value={alloydbInstanceUri}
        invalid={isUriEmpty || isUriMalformed}
        error={
          isUriEmpty
            ? 'AlloyDB instance URI is required'
            : isUriMalformed
            ? 'Use projects/<project>/locations/<region>/clusters/<cluster>/instances/<instance>'
            : null
        }
        onChange={handleChange}
        placeholder="projects/my-project/locations/us-central1/clusters/prod-cluster/instances/primary-inst"
        mono={true}
      />

      {/* IP Type, Database, and Destination Table */}
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-2">
          <label
            htmlFor="alloydbSinkIpType"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>IP Connectivity Type</span>
            <InfoTooltip text="Network path used by com.google.cloud.alloydb.SocketFactory (PRIVATE VPC peering, PUBLIC IP, or PSC Private Service Connect)." />
          </label>
          <select
            id="alloydbSinkIpType"
            name="ipType"
            value={ipType}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="PRIVATE">PRIVATE (VPC)</option>
            <option value="PUBLIC">PUBLIC</option>
            <option value="PSC">PSC (Private Service Connect)</option>
          </select>
        </div>

        <PropertyField
          id="alloydbSinkDatabase"
          name="database"
          label="Database"
          required="Database is required"
          tooltip="PostgreSQL database name on the AlloyDB instance."
          value={database}
          invalid={isDatabaseEmpty || Boolean(databaseError)}
          error={isDatabaseEmpty ? 'Database is required' : databaseError}
          onChange={handleChange}
          placeholder="postgres"
          mono={true}
        />
      </div>

      <PropertyField
        id="alloydbSinkTable"
        name="table"
        label="Destination Table"
        required="Table name is required"
        tooltip="Destination AlloyDB PostgreSQL table name passed to .option('dbtable', ...), such as public.transactions."
        value={table}
        invalid={isTableEmpty || Boolean(tableError)}
        error={isTableEmpty ? 'Table name is required' : tableError}
        onChange={handleChange}
        placeholder="public.transactions"
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
              htmlFor="alloydbSinkSaveMode"
              className="flex items-center text-xs font-bold uppercase text-slate-500"
            >
              <span>Save Mode</span>
              <InfoTooltip text="Spark DataFrameWriter save mode (.mode(...))." />
            </label>
            <select
              id="alloydbSinkSaveMode"
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
              htmlFor="alloydbSinkIsolationLevel"
              className="flex items-center text-xs font-bold uppercase text-slate-500"
            >
              <span>Isolation Level</span>
              <InfoTooltip text="JDBC transaction isolation level per executor partition (.option('isolationLevel', ...))." />
            </label>
            <select
              id="alloydbSinkIsolationLevel"
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
              id="alloydbSinkTruncate"
              type="checkbox"
              name="truncate"
              checked={truncate}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
            />
            <label
              htmlFor="alloydbSinkTruncate"
              className="flex items-center text-xs text-slate-300 cursor-pointer"
            >
              <span>Safe Truncate on Overwrite (truncate=true)</span>
              <InfoTooltip text="Executes TRUNCATE TABLE instead of DROP and CREATE so primary keys, indexes, foreign keys, and columnar settings survive." />
            </label>
          </div>
        )}

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="alloydbSinkBatchsize"
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
            id="alloydbSinkNumPartitions"
            name="numPartitions"
            label="Max Write Partitions (Optional)"
            type="number"
            min={1}
            tooltip="Optional maximum parallel JDBC writer connections (.option('numPartitions', ...)) to stay within AlloyDB connection limits."
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
          id="alloydbSinkUsername"
          name="username"
          label="Database Username"
          tooltip="AlloyDB PostgreSQL user or IAM service account principal."
          value={username}
          onChange={handleChange}
          placeholder="postgres"
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="alloydbSink"
          serviceName="AlloyDB"
          authMode={authMode}
          secretName={secretName}
          authOptions={ALLOYDB_AUTH_OPTIONS}
          secretManagerValue="secret_manager"
          datalistId="alloydb-sink-secret-options"
          secretPlaceholder="e.g. alloydb-password-secret"
          onChange={handleChange}
        />
      </div>

      {/* Runtime JAR URIs */}
      <div className="grid grid-cols-2 gap-2">
        <PropertyField
          id="alloydbSinkDriverJarUri"
          name="driverJarUri"
          label="PostgreSQL Driver JAR URI (gs://)"
          tooltip="Cloud Storage URI for the PostgreSQL JDBC driver JAR."
          value={driverJarUri}
          onChange={handleChange}
          placeholder="gs://my-bucket/jars/postgresql-42.7.4.jar"
          mono={true}
        />
        <PropertyField
          id="alloydbSinkSocketFactoryJarUri"
          name="socketFactoryJarUri"
          label="AlloyDB Socket Factory JAR URI (gs://)"
          tooltip="Cloud Storage URI for the AlloyDB JDBC Socket Factory Uber JAR."
          value={socketFactoryJarUri}
          onChange={handleChange}
          placeholder="gs://my-bucket/jars/alloydb-jdbc-connector-1.2.0-jar-with-dependencies.jar"
          mono={true}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="alloydbSinkSystemInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark AlloyDB sink code generation..."
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
