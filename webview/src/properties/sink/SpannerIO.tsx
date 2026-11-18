/**
 * @fileoverview Spanner Sink Properties Component (SNK20).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud Spanner Sink nodes
 * in Apache Spark 4.x pipelines (.write.format("cloud-spanner") and streaming
 * foreachBatch via gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar).
 *
 * Architectural Decisions:
 * - Enforces required projectId, instanceId, databaseId, and destination table fields.
 * - Exposes Spark saveMode and Spanner mutationMode (insert_or_update, insert, update, replace).
 * - Supports mutationsPerTransaction tuning, custom jarUri override, Secret Manager auth,
 *   and collapsible advanced settings for databaseRole, emulatorHost, and commitTimeoutSeconds.
 */

import React, { useState } from 'react';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import {
  SecretManagerAuthFields,
  type SecretItem,
} from '../../components/common/SecretManagerAuthFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };
export type SpannerSaveMode = 'append' | 'overwrite' | 'errorifexists' | 'ignore';
export type SpannerMutationMode = 'insert_or_update' | 'insert' | 'update' | 'replace';

export const DEFAULT_SPANNER_JAR_URI = 'gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar';

/** Props for the {@link SpannerIOSinkProperties} component. */
export interface SpannerIOSinkPropertiesProps {
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
        table?: string;
        saveMode?: SpannerSaveMode;
        mutationMode?: SpannerMutationMode | string;
        mutationsPerTransaction?: number | string;
        jarUri?: string;
        authMode?: string;
        secretName?: string;
        serviceAccountKeySecret?: string;
        databaseRole?: string;
        emulatorHost?: string;
        host?: string;
        commitTimeoutSeconds?: number | string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Normalizes legacy Spanner mutation mode strings to Spark connector values.
 */
function normalizeMutationMode(raw?: string): SpannerMutationMode {
  if (raw === 'insert' || raw === 'update' || raw === 'replace' || raw === 'insert_or_update') {
    return raw;
  }
  return 'insert_or_update';
}

/**
 * Normalizes legacy Spanner authentication mode values for SecretManagerAuthFields.
 */
function normalizeAuthMode(raw?: string): string {
  if (raw === 'secretManager' || raw === 'secret_manager') {
    return 'secretManager';
  }
  return 'default';
}

/**
 * Configures properties for Apache Spark Cloud Spanner batch and micro-batch sink nodes.
 */
export const SpannerIOSinkProperties: React.FC<SpannerIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};
  const [showAdvanced, setShowAdvanced] = useState(false);

  const projectId = properties.projectId || '';
  const instanceId = properties.instanceId || '';
  const databaseId = properties.databaseId || '';
  const table = properties.table || '';
  const saveMode: SpannerSaveMode = properties.saveMode || 'append';
  const mutationMode = normalizeMutationMode(properties.mutationMode);
  const mutationsPerTransaction = properties.mutationsPerTransaction ?? '';
  const jarUri = properties.jarUri || '';
  const authMode = normalizeAuthMode(properties.authMode);
  const secretName = properties.secretName || properties.serviceAccountKeySecret || '';
  const databaseRole = properties.databaseRole || '';
  const emulatorHost = properties.emulatorHost || properties.host || '';
  const commitTimeoutSeconds = properties.commitTimeoutSeconds ?? '';

  const isProjectEmpty = !projectId.trim();
  const isInstanceEmpty = !instanceId.trim();
  const isDatabaseEmpty = !databaseId.trim();
  const isTableEmpty = !table.trim();

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
          <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 font-mono text-[11px]">
            {'.format("cloud-spanner") · foreachBatch'}
          </span>
        </div>
        <span className="text-[11px] text-slate-400 font-mono truncate">
          {jarUri.trim() || DEFAULT_SPANNER_JAR_URI}
        </span>
      </div>

      {/* Spanner Destination Coordinates */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Spanner Destination Coordinates</span>
          <InfoTooltip text="Target Google Cloud project, Cloud Spanner instance, database, and table identifiers." />
        </label>

        <PropertyField
          id="spannerSinkProjectId"
          name="projectId"
          label="Project ID"
          required="Project ID is required"
          tooltip="Google Cloud Project ID hosting the destination Cloud Spanner instance."
          value={projectId}
          invalid={isProjectEmpty}
          error={isProjectEmpty ? 'Project ID is required' : null}
          placeholder="my-gcp-project"
          onChange={handleChange}
          mono={true}
        />

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="spannerSinkInstanceId"
            name="instanceId"
            label="Instance ID"
            required="Spanner instance ID is required"
            tooltip="Cloud Spanner destination instance identifier."
            value={instanceId}
            invalid={isInstanceEmpty}
            error={isInstanceEmpty ? 'Spanner instance ID is required' : null}
            placeholder="prod-spanner"
            onChange={handleChange}
            mono={true}
          />

          <PropertyField
            id="spannerSinkDatabaseId"
            name="databaseId"
            label="Database ID"
            required="Spanner database ID is required"
            tooltip="Cloud Spanner destination database identifier."
            value={databaseId}
            invalid={isDatabaseEmpty}
            error={isDatabaseEmpty ? 'Spanner database ID is required' : null}
            placeholder="orders_db"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="spannerSinkTable"
          name="table"
          label="Table Name"
          required="Table name is required"
          tooltip="Destination Cloud Spanner table name. Incoming DataFrame columns must include the table primary key."
          value={table}
          invalid={isTableEmpty}
          error={isTableEmpty ? 'Table name is required' : null}
          placeholder="Orders"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Save Mode and Mutation Settings */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Write and Mutation Settings</span>
          <InfoTooltip text="Configure Spark DataFrameWriter save mode, Spanner mutation semantics, and transaction batch size. Streaming pipelines execute writes via foreachBatch." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="spannerSinkSaveMode"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Save Mode</span>
              <InfoTooltip text="Spark DataFrameWriter save mode (.mode(...))." />
            </label>
            <select
              id="spannerSinkSaveMode"
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

          <div className="flex flex-col gap-1.5">
            <label
              htmlFor="spannerSinkMutationMode"
              className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
            >
              <span>Mutation Mode</span>
              <InfoTooltip text="Spanner write mutation type. Use insert_or_update for idempotent streaming foreachBatch upserts." />
            </label>
            <select
              id="spannerSinkMutationMode"
              name="mutationMode"
              value={mutationMode}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="insert_or_update">insert_or_update (Upsert)</option>
              <option value="insert">insert (Insert Only)</option>
              <option value="update">update (Update Only)</option>
              <option value="replace">replace (Replace Row)</option>
            </select>
          </div>
        </div>

        <PropertyField
          id="spannerSinkMutationsPerTransaction"
          name="mutationsPerTransaction"
          label="Mutations Per Transaction (Optional)"
          type="number"
          min={1}
          tooltip="Maximum mutated cells committed per Cloud Spanner transaction batch (.option('mutationsPerTransaction', ...))."
          value={mutationsPerTransaction}
          placeholder="e.g. 5000"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Secret Manager Authentication */}
      <SecretManagerAuthFields
        idPrefix="spannerSink"
        serviceName="Cloud Spanner"
        authMode={authMode}
        secretName={secretName}
        datalistId="spanner-sink-secret-options"
        secretPlaceholder="e.g. spanner-sa-key-secret"
        onChange={handleChange}
      />

      {/* Collapsible Advanced Sink Settings */}
      <CollapsibleSection
        title="Advanced Sink Settings"
        isOpen={showAdvanced}
        onToggle={() => setShowAdvanced(!showAdvanced)}
      >
        <PropertyField
          id="spannerSinkJarUri"
          name="jarUri"
          label="Connector JAR URI Override (Optional)"
          tooltip="Optional Cloud Storage JAR URI overriding gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar."
          value={jarUri}
          placeholder={DEFAULT_SPANNER_JAR_URI}
          onChange={handleChange}
          mono={true}
        />

        <PropertyField
          id="spannerSinkDatabaseRole"
          name="databaseRole"
          label="Database Role (FGAC)"
          tooltip="Optional Cloud Spanner fine-grained access control database role (.option('databaseRole', ...))."
          value={databaseRole}
          placeholder="analytics_writer"
          onChange={handleChange}
          mono={true}
        />

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="spannerSinkEmulatorHost"
            name="emulatorHost"
            label="Emulator Host"
            tooltip="Optional local Spanner emulator host and port for integration testing (.option('emulatorHost', ...))."
            value={emulatorHost}
            placeholder="localhost:9010"
            onChange={handleChange}
            mono={true}
          />

          <PropertyField
            id="spannerSinkCommitTimeoutSeconds"
            name="commitTimeoutSeconds"
            label="Commit Timeout (Seconds)"
            type="number"
            min={1}
            tooltip="Optional commit timeout in seconds for Cloud Spanner write RPCs."
            value={commitTimeoutSeconds}
            placeholder="60"
            onChange={handleChange}
            mono={true}
          />
        </div>
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="spannerSinkSystemInstructions"
        value={systemInstructions}
        rows={4}
        placeholder="Enter custom instructions for Spark Cloud Spanner sink code generation..."
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

export const SpannerSinkProperties = SpannerIOSinkProperties;
