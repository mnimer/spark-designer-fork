/**
 * Snowflake Source Properties Component (SRC28)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Snowflake Source nodes
 * (.format("net.snowflake.spark.snowflake")).
 *
 * Architectural Decisions:
 * - Configures standard Spark-Snowflake options: sfURL, sfWarehouse, sfDatabase,
 *   sfSchema, sfRole, and dbtable or query.
 * - Uses SecretManagerAuthFields to reference Snowflake password or private key
 *   from Google Cloud Secret Manager.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type AuthModeOption, type SecretItem } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };

const SNOWFLAKE_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'secretManager', label: 'Google Cloud Secret Manager (sfPassword / pem_private_key)' },
];

/** Props for the {@link SnowflakeIOSourceProperties} component. */
export interface SnowflakeIOSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        sfURL?: string;
        serverName?: string;
        sfWarehouse?: string;
        warehouse?: string;
        sfDatabase?: string;
        database?: string;
        sfSchema?: string;
        schema?: string;
        sfRole?: string;
        role?: string;
        sfUser?: string;
        readTargetMode?: 'dbtable' | 'query';
        dbtable?: string;
        table?: string;
        query?: string;
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
 * Configures properties for Apache Spark Snowflake batch source nodes (.format("net.snowflake.spark.snowflake")).
 */
export const SnowflakeIOSourceProperties: React.FC<SnowflakeIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const sfURL = properties.sfURL ?? properties.serverName ?? '';
  const sfWarehouse = properties.sfWarehouse ?? properties.warehouse ?? '';
  const sfDatabase = properties.sfDatabase ?? properties.database ?? '';
  const sfSchema = properties.sfSchema ?? properties.schema ?? 'PUBLIC';
  const sfRole = properties.sfRole ?? properties.role ?? '';
  const sfUser = properties.sfUser || '';
  const dbtable = properties.dbtable ?? properties.table ?? '';
  const query = properties.query || '';
  const readTargetMode: 'dbtable' | 'query' =
    properties.readTargetMode || (query.trim() && !String(dbtable).trim() ? 'query' : 'dbtable');
  const authMode = properties.authMode || 'secretManager';
  const secretName = properties.secretName ?? properties.passwordSecret ?? '';

  const isUrlEmpty = !String(sfURL).trim();
  const isWarehouseEmpty = !String(sfWarehouse).trim();
  const isDatabaseEmpty = !String(sfDatabase).trim();
  const isTableEmpty = readTargetMode === 'dbtable' && !String(dbtable).trim();
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
        <span className="font-semibold text-slate-200">Spark Snowflake Connector</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          net.snowflake.spark.snowflake · BATCH
        </span>
      </div>

      {/* Snowflake Coordinates */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Snowflake Connection Coordinates</span>
          <InfoTooltip text="Configures sfURL, sfWarehouse, sfDatabase, sfSchema, and sfRole options for the Spark Snowflake connector." />
        </label>

        <PropertyField
          id="snowflakeSourceSfUrl"
          name="sfURL"
          label="Snowflake URL (sfURL)"
          required="sfURL is required"
          tooltip="Snowflake account URL (for example xy12345.us-central1.gcp.snowflakecomputing.com)."
          value={sfURL}
          invalid={isUrlEmpty}
          error={isUrlEmpty ? 'sfURL is required' : null}
          placeholder="xy12345.us-central1.gcp.snowflakecomputing.com"
          onChange={handleChange}
          mono={true}
        />

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="snowflakeSourceSfWarehouse"
            name="sfWarehouse"
            label="Warehouse (sfWarehouse)"
            required="sfWarehouse is required"
            tooltip="Snowflake virtual warehouse name used for query execution."
            value={sfWarehouse}
            invalid={isWarehouseEmpty}
            error={isWarehouseEmpty ? 'sfWarehouse is required' : null}
            placeholder="COMPUTE_WH"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="snowflakeSourceSfDatabase"
            name="sfDatabase"
            label="Database (sfDatabase)"
            required="sfDatabase is required"
            tooltip="Target Snowflake database name."
            value={sfDatabase}
            invalid={isDatabaseEmpty}
            error={isDatabaseEmpty ? 'sfDatabase is required' : null}
            placeholder="ANALYTICS_DB"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="snowflakeSourceSfSchema"
            name="sfSchema"
            label="Schema (sfSchema)"
            tooltip="Target Snowflake schema name (default PUBLIC)."
            value={sfSchema}
            placeholder="PUBLIC"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="snowflakeSourceSfRole"
            name="sfRole"
            label="Role (sfRole)"
            tooltip="Snowflake access control role."
            value={sfRole}
            placeholder="DATA_ENGINEER"
            onChange={handleChange}
            mono={true}
          />
        </div>
      </div>

      {/* Read Target (dbtable vs query) */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="snowflakeSourceReadTargetMode"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Read Target</span>
            <InfoTooltip text="Select whether to read a Snowflake table (dbtable) or execute a custom SQL query." />
          </label>
          <select
            id="snowflakeSourceReadTargetMode"
            name="readTargetMode"
            value={readTargetMode}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="dbtable">Table (.option(&quot;dbtable&quot;, ...))</option>
            <option value="query">SQL Query (.option(&quot;query&quot;, ...))</option>
          </select>
        </div>

        {readTargetMode === 'dbtable' ? (
          <PropertyField
            id="snowflakeSourceDbtable"
            name="dbtable"
            label="Table Name (dbtable)"
            required="dbtable is required in Table mode"
            tooltip="Snowflake table name to read."
            value={dbtable}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'dbtable is required in Table mode' : null}
            placeholder="ORDERS"
            onChange={handleChange}
            mono={true}
          />
        ) : (
          <PropertyField
            id="snowflakeSourceQuery"
            name="query"
            label="SQL Query (query)"
            type="textarea"
            rows={3}
            required="SQL query is required in Query mode"
            tooltip="SQL query pushed down to Snowflake."
            value={query}
            invalid={isQueryEmpty}
            error={isQueryEmpty ? 'SQL query is required in Query mode' : null}
            placeholder="SELECT ORDER_ID, CUSTOMER_ID, AMOUNT FROM ORDERS WHERE AMOUNT > 0"
            onChange={handleChange}
            mono={true}
          />
        )}
      </div>

      {/* Authentication */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <PropertyField
          id="snowflakeSourceSfUser"
          name="sfUser"
          label="Snowflake Username (sfUser)"
          tooltip="Snowflake login user name."
          value={sfUser}
          placeholder="SPARK_SVC_USER"
          onChange={handleChange}
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="snowflakeSource"
          serviceName="Snowflake"
          authMode={authMode}
          secretName={secretName}
          authOptions={SNOWFLAKE_AUTH_OPTIONS}
          secretManagerValue="secretManager"
          datalistId="snowflake-source-secret-options"
          secretPlaceholder="e.g. snowflake-private-key-or-password"
          onChange={handleChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="snowflakeSourceSystemInstructions"
        value={systemInstructions}
        rows={4}
        placeholder="Enter custom instructions for Spark Snowflake source code generation..."
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

export const SnowflakeSourceProperties = SnowflakeIOSourceProperties;
