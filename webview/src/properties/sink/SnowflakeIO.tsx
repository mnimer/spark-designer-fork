/**
 * Snowflake Sink Properties Component (SNK25)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Snowflake Sink nodes
 * (.write.format("net.snowflake.spark.snowflake") and streaming foreachBatch).
 *
 * Architectural Decisions:
 * - Configures standard Spark-Snowflake sink options: sfURL, sfWarehouse, sfDatabase,
 *   sfSchema, sfRole, dbtable, saveMode, usestagingtable, truncateTable, preactions,
 *   and postactions.
 * - Supports password (sfPassword) and PEM key-pair (pem_private_key) authentication
 *   resolved through Google Cloud Secret Manager via SecretManagerAuthFields.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import {
  SecretManagerAuthFields,
  type AuthModeOption,
  type SecretItem,
} from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };

export type SnowflakeSaveMode = 'append' | 'overwrite' | 'errorifexists' | 'ignore';
export type SnowflakeCredentialType = 'password' | 'pem_private_key';

const SNOWFLAKE_SINK_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'secretManager', label: 'Google Cloud Secret Manager (sfPassword / pem_private_key)' },
];

/** Props for the {@link SnowflakeIOSinkProperties} component. */
export interface SnowflakeIOSinkPropertiesProps {
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
        dbtable?: string;
        table?: string;
        saveMode?: SnowflakeSaveMode;
        usestagingtable?: boolean;
        truncateTable?: boolean;
        preactions?: string;
        postactions?: string;
        credentialType?: SnowflakeCredentialType;
        authMode?: string;
        secretName?: string;
        passwordSecret?: string;
        privateKeySecret?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Configures properties for Apache Spark Snowflake sink nodes (.write.format("net.snowflake.spark.snowflake")).
 */
export const SnowflakeIOSinkProperties: React.FC<SnowflakeIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const sfURL = properties.sfURL ?? properties.serverName ?? '';
  const sfWarehouse = properties.sfWarehouse ?? properties.warehouse ?? '';
  const sfDatabase = properties.sfDatabase ?? properties.database ?? '';
  const sfSchema = properties.sfSchema ?? properties.schema ?? 'PUBLIC';
  const sfRole = properties.sfRole ?? properties.role ?? '';
  const sfUser = properties.sfUser || '';
  const dbtable = properties.dbtable ?? properties.table ?? '';
  const saveMode: SnowflakeSaveMode = properties.saveMode || 'append';
  const usestagingtable = properties.usestagingtable ?? true;
  const truncateTable = properties.truncateTable ?? false;
  const preactions = properties.preactions || '';
  const postactions = properties.postactions || '';
  const credentialType: SnowflakeCredentialType =
    properties.credentialType || (properties.privateKeySecret ? 'pem_private_key' : 'password');
  const authMode = 'secretManager';
  const secretName =
    properties.secretName ?? properties.privateKeySecret ?? properties.passwordSecret ?? '';

  const isUrlEmpty = !String(sfURL).trim();
  const isWarehouseEmpty = !String(sfWarehouse).trim();
  const isDatabaseEmpty = !String(sfDatabase).trim();
  const isTableEmpty = !String(dbtable).trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next: Record<string, unknown> = { ...properties, [name]: updatedValue };
    delete next.sfPassword;

    if (name === 'sfURL') {
      delete next.serverName;
    }
    if (name === 'sfWarehouse') {
      delete next.warehouse;
    }
    if (name === 'sfDatabase') {
      delete next.database;
    }
    if (name === 'sfSchema') {
      delete next.schema;
    }
    if (name === 'sfRole') {
      delete next.role;
    }
    if (name === 'dbtable') {
      delete next.table;
    }
    if (name === 'secretName') {
      delete next.passwordSecret;
      delete next.privateKeySecret;
    }

    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark Snowflake Sink</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          net.snowflake.spark.snowflake · foreachBatch
        </span>
      </div>

      {/* Snowflake Connection Coordinates */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Snowflake Connection Coordinates</span>
          <InfoTooltip text="Configures sfURL, sfWarehouse, sfDatabase, sfSchema, and sfRole options for the Spark Snowflake connector." />
        </label>

        <PropertyField
          id="snowflakeSinkSfUrl"
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
            id="snowflakeSinkSfWarehouse"
            name="sfWarehouse"
            label="Warehouse (sfWarehouse)"
            required="sfWarehouse is required"
            tooltip="Snowflake virtual warehouse name used for write execution."
            value={sfWarehouse}
            invalid={isWarehouseEmpty}
            error={isWarehouseEmpty ? 'sfWarehouse is required' : null}
            placeholder="COMPUTE_WH"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="snowflakeSinkSfDatabase"
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
            id="snowflakeSinkSfSchema"
            name="sfSchema"
            label="Schema (sfSchema)"
            tooltip="Target Snowflake schema name (default PUBLIC)."
            value={sfSchema}
            placeholder="PUBLIC"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="snowflakeSinkSfRole"
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

      {/* Destination Table, Save Mode & Staging */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Target Table &amp; Write Behavior</span>
          <InfoTooltip text="Configures destination dbtable, Spark saveMode, ephemeral staging table swaps, and SQL preactions or postactions." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="snowflakeSinkDbtable"
            name="dbtable"
            label="Destination Table (dbtable)"
            required="dbtable is required"
            tooltip="Target Snowflake table name passed to .option('dbtable', ...)."
            value={dbtable}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'dbtable is required' : null}
            placeholder="AGGREGATED_METRICS"
            onChange={handleChange}
            mono={true}
          />

          <div className="flex flex-col gap-2">
            <label
              htmlFor="snowflakeSinkSaveMode"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Save Mode</span>
              <InfoTooltip text="Spark save mode applied when writing to the Snowflake table." />
            </label>
            <select
              id="snowflakeSinkSaveMode"
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

        <div className="flex items-center gap-2">
          <input
            id="snowflakeSinkUseStagingTable"
            type="checkbox"
            name="usestagingtable"
            checked={usestagingtable}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="snowflakeSinkUseStagingTable"
            className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
          >
            <span>Use Ephemeral Staging Table (usestagingtable=ON)</span>
            <InfoTooltip text="Writes into a temporary Snowflake table and swaps or merges atomically on completion." />
          </label>
        </div>

        {saveMode === 'overwrite' && (
          <div className="flex items-center gap-2">
            <input
              id="snowflakeSinkTruncateTable"
              type="checkbox"
              name="truncateTable"
              checked={truncateTable}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
            />
            <label
              htmlFor="snowflakeSinkTruncateTable"
              className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
            >
              <span>Truncate Table on Overwrite (truncate_table=ON)</span>
              <InfoTooltip text="Truncates the existing Snowflake table on overwrite so grants and clustering definitions survive." />
            </label>
          </div>
        )}

        <PropertyField
          id="snowflakeSinkPreactions"
          name="preactions"
          label="Pre-Actions SQL (preactions)"
          type="textarea"
          rows={2}
          tooltip="Optional SQL statements executed in Snowflake before the connector writes data."
          value={preactions}
          placeholder="DELETE FROM AGGREGATED_METRICS WHERE BATCH_DATE = CURRENT_DATE()"
          onChange={handleChange}
          mono={true}
        />

        <PropertyField
          id="snowflakeSinkPostactions"
          name="postactions"
          label="Post-Actions SQL (postactions)"
          type="textarea"
          rows={2}
          tooltip="Optional SQL statements executed in Snowflake after the connector finishes writing."
          value={postactions}
          placeholder="GRANT SELECT ON TABLE AGGREGATED_METRICS TO ROLE ANALYST_ROLE"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Authentication */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="snowflakeSinkSfUser"
            name="sfUser"
            label="Snowflake Username (sfUser)"
            tooltip="Snowflake login user name."
            value={sfUser}
            placeholder="SPARK_SVC_USER"
            onChange={handleChange}
            mono={true}
          />

          <div className="flex flex-col gap-2">
            <label
              htmlFor="snowflakeSinkCredentialType"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Credential Type</span>
              <InfoTooltip text="Select whether the Secret Manager payload holds sfPassword or a PEM private key (pem_private_key)." />
            </label>
            <select
              id="snowflakeSinkCredentialType"
              name="credentialType"
              value={credentialType}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="password">Password (sfPassword)</option>
              <option value="pem_private_key">PEM Private Key (pem_private_key)</option>
            </select>
          </div>
        </div>

        <SecretManagerAuthFields
          idPrefix="snowflakeSink"
          serviceName="Snowflake"
          authMode={authMode}
          secretName={secretName}
          authOptions={SNOWFLAKE_SINK_AUTH_OPTIONS}
          secretManagerValue="secretManager"
          datalistId="snowflake-sink-secret-options"
          secretPlaceholder={
            credentialType === 'pem_private_key'
              ? 'e.g. snowflake-rsa-private-key'
              : 'e.g. snowflake-password'
          }
          onChange={handleChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="snowflakeSinkSystemInstructions"
        value={systemInstructions}
        rows={4}
        placeholder="Enter custom instructions for Spark Snowflake sink code generation..."
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

export const SnowflakeSinkProperties = SnowflakeIOSinkProperties;
