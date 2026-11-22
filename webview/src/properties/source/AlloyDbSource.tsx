/**
 * AlloyDB Source Properties Component (SRC26)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud AlloyDB for PostgreSQL
 * Source nodes in Apache Spark 4.x pipelines (.format("jdbc") + AlloyDB JDBC Socket Factory).
 *
 * Architectural Decisions:
 * - Batch-only JDBC source with AlloyDB Socket Factory and Conscrypt removal helper badge
 *   (dataproc.artifacts.remove=conscrypt).
 * - Configures alloydbInstanceUri, ipType ('PRIVATE' | 'PUBLIC' | 'PSC'), database,
 *   table or SQL query, IAM vs Secret Manager authentication, and dynamic MIN/MAX bounds.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type AuthModeOption } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type AlloyDbIpType = 'PRIVATE' | 'PUBLIC' | 'PSC';
export type AlloyDbAuthMode = 'iam' | 'secret_manager';

const ALLOYDB_URI_RE =
  /^projects\/[^/]+\/locations\/[^/]+\/clusters\/[^/]+\/instances\/[^/]+$/i;

const ALLOYDB_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'iam', label: 'AlloyDB IAM Authentication (enableIAMAuth=true)' },
  { value: 'secret_manager', label: 'Google Cloud Secret Manager (Password Secret)' },
];

/** Props for the {@link AlloyDbSourceProperties} component. */
export interface AlloyDbSourcePropertiesProps {
  node: {
    data: {
      properties?: {
        alloydbInstanceUri?: string;
        ipType?: AlloyDbIpType;
        database?: string;
        readTargetMode?: 'table' | 'query';
        table?: string;
        query?: string;
        authMode?: AlloyDbAuthMode | string;
        username?: string;
        secretName?: string;
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
 * Configures properties for Apache Spark AlloyDB for PostgreSQL batch source nodes (.format("jdbc")).
 */
export const AlloyDbSourceProperties: React.FC<AlloyDbSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const alloydbInstanceUri = properties.alloydbInstanceUri || '';
  const ipType: AlloyDbIpType = properties.ipType || 'PRIVATE';
  const database = properties.database || '';
  const table = properties.table || '';
  const query = properties.query || '';
  const readTargetMode: 'table' | 'query' =
    properties.readTargetMode || (query.trim() && !table.trim() ? 'query' : 'table');
  const authMode = properties.authMode || 'iam';
  const username = properties.username || '';
  const secretName = properties.secretName || '';
  const partitionColumn = properties.partitionColumn || '';
  const numPartitions = properties.numPartitions ?? 8;
  const dynamicBounds = properties.dynamicBounds ?? true;
  const lowerBound = properties.lowerBound ?? '';
  const upperBound = properties.upperBound ?? '';
  const fetchsize = properties.fetchsize ?? 5000;
  const driverJarUri = properties.driverJarUri || '';
  const socketFactoryJarUri = properties.socketFactoryJarUri || '';

  const uriTrimmed = alloydbInstanceUri.trim();
  const isUriEmpty = uriTrimmed.length === 0;
  const isUriMalformed = !isUriEmpty && !ALLOYDB_URI_RE.test(uriTrimmed);
  const isDatabaseEmpty = !database.trim();
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
    <div className="flex-1 flex flex-col gap-4">
      {/* Connector Header & Conscrypt Note */}
      <div className="flex flex-col gap-1 p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <div className="flex items-center justify-between">
          <span className="font-semibold text-slate-200">Spark AlloyDB JDBC + Socket Factory</span>
          <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
            .format(&quot;jdbc&quot;) · BATCH
          </span>
        </div>
        <span className="text-[11px] text-amber-300 font-mono">
          Managed Spark property: dataproc.artifacts.remove=conscrypt
        </span>
      </div>

      {/* AlloyDB Instance URI */}
      <PropertyField
        id="alloydbSourceInstanceUri"
        name="alloydbInstanceUri"
        label="AlloyDB Instance URI"
        required="AlloyDB instance URI is required"
        tooltip="Full AlloyDB instance resource URI (projects/<p>/locations/<r>/clusters/<c>/instances/<i>)."
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

      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-2">
          <label
            htmlFor="alloydbSourceIpType"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>IP Connectivity Type</span>
            <InfoTooltip text="Network path used by the AlloyDB Socket Factory (PRIVATE VPC peering, PUBLIC IP, or PSC Private Service Connect)." />
          </label>
          <select
            id="alloydbSourceIpType"
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
          id="alloydbSourceDatabase"
          name="database"
          label="Database"
          required="Database is required"
          tooltip="PostgreSQL database name on the AlloyDB instance."
          value={database}
          invalid={isDatabaseEmpty}
          error={isDatabaseEmpty ? 'Database is required' : null}
          onChange={handleChange}
          placeholder="postgres"
          mono={true}
        />
      </div>

      {/* Read Target (Table vs Query) */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="alloydbSourceReadTargetMode"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>Read Target</span>
            <InfoTooltip text="Choose whether to read a full table or run a custom SQL query." />
          </label>
          <select
            id="alloydbSourceReadTargetMode"
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
            id="alloydbSourceTable"
            name="table"
            label="Table Name"
            required="Table name is required in Table mode"
            tooltip="AlloyDB PostgreSQL table name to read."
            value={table}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Table name is required in Table mode' : null}
            onChange={handleChange}
            placeholder="public.transactions"
            mono={true}
          />
        ) : (
          <PropertyField
            id="alloydbSourceQuery"
            name="query"
            label="SQL Query"
            type="textarea"
            rows={3}
            required="SQL query is required in Query mode"
            tooltip="PostgreSQL SELECT statement executed via Spark JDBC."
            value={query}
            invalid={isQueryEmpty}
            error={isQueryEmpty ? 'SQL query is required in Query mode' : null}
            onChange={handleChange}
            placeholder="SELECT txn_id, account_id, amount FROM transactions WHERE status = 'SETTLED'"
            mono={true}
          />
        )}
      </div>

      {/* Parallel Partitioning & Bounds */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <label className="flex items-center text-xs font-bold uppercase text-slate-500">
          <span>Parallel JDBC Partitioning &amp; Fetch Size</span>
          <InfoTooltip text="Configures parallel partitioned reads across Spark executors." />
        </label>

        <div className="grid grid-cols-3 gap-2">
          <PropertyField
            id="alloydbSourcePartitionColumn"
            name="partitionColumn"
            label="Partition Column"
            tooltip="Numeric or timestamp column used to split parallel JDBC reads."
            value={partitionColumn}
            onChange={handleChange}
            placeholder="txn_id"
            mono={true}
          />
          <PropertyField
            id="alloydbSourceNumPartitions"
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
            id="alloydbSourceFetchsize"
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
            id="alloydbSourceDynamicBounds"
            type="checkbox"
            name="dynamicBounds"
            checked={dynamicBounds}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="alloydbSourceDynamicBounds"
            className="flex items-center text-xs text-slate-300 cursor-pointer"
          >
            <span>Dynamic Bounds Query (SELECT MIN(col), MAX(col))</span>
            <InfoTooltip text="Automatically runs a pre-flight MIN/MAX query to compute lowerBound and upperBound at runtime." />
          </label>
        </div>

        {!dynamicBounds && (
          <div className="grid grid-cols-2 gap-2">
            <PropertyField
              id="alloydbSourceLowerBound"
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
              id="alloydbSourceUpperBound"
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
          id="alloydbSourceUsername"
          name="username"
          label="Database Username"
          tooltip="AlloyDB PostgreSQL user or IAM service account principal."
          value={username}
          onChange={handleChange}
          placeholder="postgres"
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="alloydbSource"
          serviceName="AlloyDB"
          authMode={authMode}
          secretName={secretName}
          authOptions={ALLOYDB_AUTH_OPTIONS}
          secretManagerValue="secret_manager"
          datalistId="alloydb-source-secret-options"
          secretPlaceholder="e.g. alloydb-password-secret"
          onChange={handleChange}
        />
      </div>

      {/* Runtime JAR URIs */}
      <div className="grid grid-cols-2 gap-2">
        <PropertyField
          id="alloydbSourceDriverJarUri"
          name="driverJarUri"
          label="PostgreSQL Driver JAR URI (gs://)"
          tooltip="Cloud Storage URI for the PostgreSQL JDBC driver JAR."
          value={driverJarUri}
          onChange={handleChange}
          placeholder="gs://my-bucket/jars/postgresql-42.7.4.jar"
          mono={true}
        />
        <PropertyField
          id="alloydbSourceSocketFactoryJarUri"
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
        id="alloydbSourceSystemInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark AlloyDB source code generation..."
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
