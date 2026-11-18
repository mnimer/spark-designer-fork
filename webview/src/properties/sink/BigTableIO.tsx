/**
 * @fileoverview Bigtable Sink Properties Component (SNK21).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud Bigtable Sink nodes
 * in Apache Spark 4.x pipelines (.write.format("bigtable") and streaming foreachBatch
 * via spark-bigtable_2.13 connector).
 *
 * Architectural Decisions:
 * - Enforces required projectId, instanceId, tableId, and catalog JSON mapping fields.
 * - Uses useAssetLoader for live GCP Bigtable instance and table autocomplete.
 * - Exposes createNewTable, writeRowTimestamp, batchMutateSize, and enableFlowControl
 *   for Spark Bigtable write tuning, plus Secret Manager authentication and advanced timeouts.
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
import { useAssetLoader } from '../../hooks/useAssetLoader';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };

export const DEFAULT_BIGTABLE_JAR_URI = 'gs://spark-lib/bigtable/spark-bigtable_2.13-0.1.0.jar';

const SAMPLE_BIGTABLE_SINK_CATALOG = `{
  "table": {"name": "user_sessions"},
  "rowkey": "row_key",
  "columns": {
    "row_key": {"cf": "rowkey", "col": "rowkey", "type": "string"},
    "event_type": {"cf": "events", "col": "event_type", "type": "string"}
  }
}`;

/** Props for the {@link BigTableIOSinkProperties} component. */
export interface BigTableIOSinkPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        projectId?: string;
        instanceId?: string;
        tableId?: string;
        table?: string;
        appProfileId?: string;
        catalog?: string;
        createNewTable?: boolean;
        writeRowTimestamp?: number | string;
        batchMutateSize?: number | string;
        maxElementsPerBatch?: number | string;
        enableFlowControl?: boolean;
        flowControl?: boolean;
        authMode?: string;
        secretName?: string;
        serviceAccountSecret?: string;
        jarUri?: string;
        emulatorHost?: string;
        attemptTimeoutMs?: number | string;
        operationTimeoutMs?: number | string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Normalizes legacy authentication mode identifiers for SecretManagerAuthFields.
 */
function normalizeAuthMode(raw?: string): string {
  if (raw === 'secretManager' || raw === 'secret_manager') {
    return 'secretManager';
  }
  return 'default';
}

/**
 * Configures properties for Apache Spark Cloud Bigtable batch and micro-batch sink nodes.
 */
export const BigTableIOSinkProperties: React.FC<BigTableIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};
  const [showAdvanced, setShowAdvanced] = useState(false);

  const projectId = properties.projectId || '';
  const instanceId = properties.instanceId || '';
  const tableId = String(properties.tableId ?? properties.table ?? '');
  const appProfileId = properties.appProfileId || '';
  const catalog = properties.catalog || '';
  const createNewTable = properties.createNewTable ?? false;
  const writeRowTimestamp = properties.writeRowTimestamp ?? '';
  const batchMutateSize = properties.batchMutateSize ?? properties.maxElementsPerBatch ?? '';
  const enableFlowControl = properties.enableFlowControl ?? properties.flowControl ?? false;
  const authMode = normalizeAuthMode(properties.authMode);
  const secretName = properties.secretName || properties.serviceAccountSecret || '';
  const jarUri = properties.jarUri || '';
  const emulatorHost = properties.emulatorHost || '';
  const attemptTimeoutMs = properties.attemptTimeoutMs ?? '';
  const operationTimeoutMs = properties.operationTimeoutMs ?? '';

  const instanceTrimmed = instanceId.trim();
  const { items: bigtableInstances, loading: instancesLoading } =
    useAssetLoader<string>('bigtableInstances');
  const { items: bigtableTables, loading: tablesLoading } = useAssetLoader<string>(
    'bigtableTables',
    {
      params: { instance: instanceTrimmed },
      enabled: instanceTrimmed.length > 0,
    }
  );

  const isProjectEmpty = !projectId.trim();
  const isInstanceEmpty = !instanceTrimmed;
  const isTableEmpty = !tableId.trim();
  const isCatalogEmpty = !catalog.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    if (name === 'tableId') {
      onChange({ ...properties, tableId: updatedValue, table: updatedValue });
      return;
    }
    onChange({ ...properties, [name]: updatedValue });
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex flex-col gap-1 p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <div className="flex items-center justify-between">
          <span className="font-semibold text-slate-200">Spark Cloud Bigtable Connector</span>
          <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 font-mono text-[11px]">
            {'.format("bigtable") · foreachBatch'}
          </span>
        </div>
        <span className="text-[11px] text-slate-400 font-mono truncate">
          {jarUri.trim() || DEFAULT_BIGTABLE_JAR_URI}
        </span>
      </div>

      {/* Bigtable Coordinates */}
      <div className="flex flex-col gap-3">
        <PropertyField
          id="bigtableSinkProjectId"
          name="projectId"
          label="Project ID"
          required="Project ID is required"
          tooltip="Google Cloud Project ID hosting the destination Cloud Bigtable instance (spark.bigtable.project.id)."
          value={projectId}
          invalid={isProjectEmpty}
          error={isProjectEmpty ? 'Project ID is required' : null}
          placeholder="e.g. my-gcp-project"
          onChange={handleChange}
          mono={true}
        />

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="bigtableSinkInstanceId"
            name="instanceId"
            label="Instance ID"
            required="Instance ID is required"
            tooltip="Cloud Bigtable Instance ID (spark.bigtable.instance.id)."
            value={instanceId}
            invalid={isInstanceEmpty}
            error={isInstanceEmpty ? 'Instance ID is required' : null}
            placeholder="e.g. prod-bigtable-instance"
            list="bigtable-sink-instance-options"
            datalist={bigtableInstances}
            loading={instancesLoading}
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="bigtableSinkTableId"
            name="tableId"
            label="Table ID"
            required="Table ID is required"
            tooltip="Destination Cloud Bigtable table name referenced in the catalog JSON mapping."
            value={tableId}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Table ID is required' : null}
            placeholder="e.g. user_sessions"
            list="bigtable-sink-table-options"
            datalist={instanceTrimmed ? bigtableTables : []}
            loading={instanceTrimmed ? tablesLoading : false}
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="bigtableSinkAppProfileId"
          name="appProfileId"
          label="App Profile ID (Optional)"
          tooltip="Optional Bigtable App Profile ID for write workload routing (spark.bigtable.app_profile.id)."
          value={appProfileId}
          onChange={handleChange}
          placeholder="e.g. batch-writes-profile"
          mono={true}
        />
      </div>

      {/* Catalog JSON Schema Mapping */}
      <PropertyField
        id="bigtableSinkCatalog"
        name="catalog"
        label="Catalog JSON Schema Mapping"
        type="textarea"
        rows={6}
        required="Catalog JSON schema mapping is required"
        tooltip="Spark Bigtable catalog JSON mapping DataFrame columns to rowkey and column families (.option('catalog', catalogJson))."
        value={catalog}
        invalid={isCatalogEmpty}
        error={isCatalogEmpty ? 'Catalog JSON schema mapping is required' : null}
        placeholder={SAMPLE_BIGTABLE_SINK_CATALOG}
        onChange={handleChange}
        mono={true}
      />

      {/* Table Creation, Timestamp, and Batch Mutation Tuning */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Write Options and Batch Mutation Tuning</span>
          <InfoTooltip text="Configure table creation, cell write timestamps, batch mutation size, and adaptive flow control." />
        </label>

        <div className="flex items-center gap-2">
          <input
            id="bigtableSinkCreateNewTable"
            type="checkbox"
            name="createNewTable"
            checked={createNewTable}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="bigtableSinkCreateNewTable"
            className="flex items-center gap-1.5 text-xs text-[var(--vscode-text-secondary)] cursor-pointer"
          >
            <span>Create New Table If Missing (spark.bigtable.create.new.table)</span>
            <InfoTooltip text="Creates the Bigtable table and catalog column families automatically when the table does not exist." />
          </label>
        </div>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="bigtableSinkWriteRowTimestamp"
            name="writeRowTimestamp"
            label="Write Row Timestamp (ms)"
            type="number"
            min={0}
            tooltip="Optional epoch timestamp in milliseconds applied to all written cells (spark.bigtable.write.timestamp.milliseconds)."
            value={writeRowTimestamp}
            onChange={handleChange}
            placeholder="e.g. 1735689600000"
            mono={true}
          />

          <PropertyField
            id="bigtableSinkBatchMutateSize"
            name="batchMutateSize"
            label="Batch Mutate Size"
            type="number"
            min={1}
            max={100000}
            tooltip="Maximum number of row mutations sent per Bigtable batch RPC (spark.bigtable.batch_mutate.size, default 100, max 100000)."
            value={batchMutateSize}
            onChange={handleChange}
            placeholder="e.g. 1000"
            mono={true}
          />
        </div>

        <div className="flex items-center gap-2">
          <input
            id="bigtableSinkEnableFlowControl"
            type="checkbox"
            name="enableFlowControl"
            checked={enableFlowControl}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="bigtableSinkEnableFlowControl"
            className="flex items-center gap-1.5 text-xs text-[var(--vscode-text-secondary)] cursor-pointer"
          >
            <span>Enable Batch Mutate Flow Control</span>
            <InfoTooltip text="Applies client-side flow control to prevent overloading Bigtable tablet servers (spark.bigtable.enable.batch_mutate.flow_control)." />
          </label>
        </div>
      </div>

      {/* Authentication */}
      <SecretManagerAuthFields
        idPrefix="bigtableSink"
        serviceName="Cloud Bigtable"
        authMode={authMode}
        secretName={secretName}
        datalistId="bigtable-sink-secret-options"
        secretPlaceholder="e.g. gcp-bigtable-sa-key"
        onChange={handleChange}
      />

      {/* Collapsible Advanced Sink Settings */}
      <CollapsibleSection
        title="Advanced Sink Settings"
        isOpen={showAdvanced}
        onToggle={() => setShowAdvanced(!showAdvanced)}
      >
        <PropertyField
          id="bigtableSinkJarUri"
          name="jarUri"
          label="Connector JAR URI Override (Optional)"
          tooltip="Optional Cloud Storage JAR URI overriding gs://spark-lib/bigtable/spark-bigtable_2.13-0.1.0.jar."
          value={jarUri}
          onChange={handleChange}
          placeholder={DEFAULT_BIGTABLE_JAR_URI}
          mono={true}
        />

        <PropertyField
          id="bigtableSinkEmulatorHost"
          name="emulatorHost"
          label="Emulator Host"
          tooltip="Optional Bigtable emulator host and port for local testing (for example localhost:8086)."
          value={emulatorHost}
          onChange={handleChange}
          placeholder="localhost:8086"
          mono={true}
        />

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="bigtableSinkAttemptTimeoutMs"
            name="attemptTimeoutMs"
            label="Attempt Timeout (ms)"
            type="number"
            min={1}
            value={attemptTimeoutMs}
            onChange={handleChange}
            placeholder="e.g. 20000"
            tooltip="Timeout in milliseconds for each individual Bigtable RPC attempt."
            mono={true}
          />
          <PropertyField
            id="bigtableSinkOperationTimeoutMs"
            name="operationTimeoutMs"
            label="Operation Timeout (ms)"
            type="number"
            min={1}
            value={operationTimeoutMs}
            onChange={handleChange}
            placeholder="e.g. 60000"
            tooltip="Total timeout in milliseconds for a Bigtable batch mutation operation including retries."
            mono={true}
          />
        </div>
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="bigtableSinkInstructions"
        value={systemInstructions}
        tooltip="Optional custom instructions for Spark Bigtable sink code generation."
        placeholder="Enter custom instructions for Spark Bigtable sink code generation..."
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

export const BigtableSinkProperties = BigTableIOSinkProperties;
