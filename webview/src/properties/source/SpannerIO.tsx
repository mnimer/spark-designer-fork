/**
 * Spanner Source Properties Component (SRC23)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud Spanner Source nodes
 * in Apache Spark 4.x pipelines (.format("cloud-spanner") via
 * gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar).
 *
 * Architectural Decisions:
 * - Batch-only source reading from a Spanner table or SQL query.
 * - Exposes enableDataBoost for serverless Spanner Data Boost compute isolation.
 * - Supports timestampBound staleness/read-timestamp configuration and Secret Manager authentication.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type SecretItem } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };
export type SpannerReadMode = 'table' | 'query';

export const DEFAULT_SPANNER_JAR_URI = 'gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar';

/** Props for the {@link SpannerIOSourceProperties} component. */
export interface SpannerIOSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        projectId?: string;
        instanceId?: string;
        databaseId?: string;
        readTargetMode?: SpannerReadMode;
        table?: string;
        query?: string;
        enableDataBoost?: boolean;
        timestampBound?: string;
        authMode?: string;
        secretName?: string;
        serviceAccountKeySecret?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Configures properties for Apache Spark Cloud Spanner batch source nodes (.format("cloud-spanner")).
 */
export const SpannerIOSourceProperties: React.FC<SpannerIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const projectId = properties.projectId || '';
  const instanceId = properties.instanceId || '';
  const databaseId = properties.databaseId || '';
  const table = properties.table || '';
  const query = properties.query || '';
  const readTargetMode: SpannerReadMode =
    properties.readTargetMode || (query.trim() && !table.trim() ? 'query' : 'table');
  const enableDataBoost = properties.enableDataBoost ?? true;
  const timestampBound = properties.timestampBound || '';
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || properties.serviceAccountKeySecret || '';

  const isProjectEmpty = !projectId.trim();
  const isInstanceEmpty = !instanceId.trim();
  const isDatabaseEmpty = !databaseId.trim();
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
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex flex-col gap-1 p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <div className="flex items-center justify-between">
          <span className="font-semibold text-slate-200">Spark Cloud Spanner Connector</span>
          <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
            .format(&quot;cloud-spanner&quot;) · BATCH
          </span>
        </div>
        <span className="text-[11px] text-slate-400 font-mono truncate">{DEFAULT_SPANNER_JAR_URI}</span>
      </div>

      {/* Spanner Coordinates */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Spanner Database Coordinates</span>
          <InfoTooltip text="Cloud Spanner project, instance, and database identifiers." />
        </label>

        <PropertyField
          id="spannerSourceProjectId"
          name="projectId"
          label="Project ID"
          required="Project ID is required"
          tooltip="Google Cloud Project ID hosting the Cloud Spanner instance."
          value={projectId}
          invalid={isProjectEmpty}
          error={isProjectEmpty ? 'Project ID is required' : null}
          placeholder="my-gcp-project"
          onChange={handleChange}
          mono={true}
        />

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="spannerSourceInstanceId"
            name="instanceId"
            label="Instance ID"
            required="Spanner instance ID is required"
            tooltip="Cloud Spanner instance identifier."
            value={instanceId}
            invalid={isInstanceEmpty}
            error={isInstanceEmpty ? 'Spanner instance ID is required' : null}
            placeholder="prod-spanner"
            onChange={handleChange}
            mono={true}
          />

          <PropertyField
            id="spannerSourceDatabaseId"
            name="databaseId"
            label="Database ID"
            required="Spanner database ID is required"
            tooltip="Cloud Spanner database identifier."
            value={databaseId}
            invalid={isDatabaseEmpty}
            error={isDatabaseEmpty ? 'Spanner database ID is required' : null}
            placeholder="orders_db"
            onChange={handleChange}
            mono={true}
          />
        </div>
      </div>

      {/* Read Target (Table vs Query) */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <div className="flex flex-col gap-1.5">
          <label
            htmlFor="spannerSourceReadTargetMode"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Read Target Mode</span>
            <InfoTooltip text="Select whether to read a full Spanner table or execute a distributed SQL query." />
          </label>
          <select
            id="spannerSourceReadTargetMode"
            name="readTargetMode"
            value={readTargetMode}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="table">Table Scan (.option(&quot;table&quot;, ...))</option>
            <option value="query">SQL Query (.option(&quot;query&quot;, ...))</option>
          </select>
        </div>

        {readTargetMode === 'table' ? (
          <PropertyField
            id="spannerSourceTable"
            name="table"
            label="Table Name"
            required="Table name is required in Table mode"
            tooltip="Cloud Spanner table name to read."
            value={table}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Table name is required in Table mode' : null}
            placeholder="Singers"
            onChange={handleChange}
            mono={true}
          />
        ) : (
          <PropertyField
            id="spannerSourceQuery"
            name="query"
            label="SQL Query"
            type="textarea"
            rows={3}
            required="SQL query is required in Query mode"
            tooltip="Partitionable GoogleSQL query executed across Spanner Batch API partitions."
            value={query}
            invalid={isQueryEmpty}
            error={isQueryEmpty ? 'SQL query is required in Query mode' : null}
            placeholder="SELECT SingerId, FirstName, LastName FROM Singers WHERE Active = TRUE"
            onChange={handleChange}
            mono={true}
          />
        )}
      </div>

      {/* Spanner Data Boost & Timestamp Bound */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <div className="flex items-center gap-2">
          <input
            id="spannerSourceEnableDataBoost"
            type="checkbox"
            name="enableDataBoost"
            checked={enableDataBoost}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="spannerSourceEnableDataBoost"
            className="flex items-center gap-1.5 text-xs text-slate-300 cursor-pointer"
          >
            <span>Enable Spanner Data Boost (enableDataBoost=true)</span>
            <InfoTooltip text="Executes Spark batch reads on isolated Spanner Data Boost serverless compute with zero impact on transactional OLTP workloads." />
          </label>
        </div>

        <PropertyField
          id="spannerSourceTimestampBound"
          name="timestampBound"
          label="Timestamp Bound / Read Timestamp (Optional)"
          tooltip="Optional RFC 3339 read timestamp or staleness bound for snapshot reads (for example 2026-01-01T00:00:00Z)."
          value={timestampBound}
          placeholder="2026-01-01T00:00:00Z"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Secret Manager Authentication */}
      <SecretManagerAuthFields
        idPrefix="spannerSource"
        serviceName="Cloud Spanner"
        authMode={authMode}
        secretName={secretName}
        datalistId="spanner-source-secret-options"
        secretPlaceholder="e.g. spanner-sa-key-secret"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="spannerSourceSystemInstructions"
        value={systemInstructions}
        rows={4}
        placeholder="Enter custom instructions for Spark Cloud Spanner source code generation..."
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

export const SpannerSourceProperties = SpannerIOSourceProperties;
