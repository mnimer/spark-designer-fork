/**
 * Bigtable Source Properties Component (SRC24)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud Bigtable Source nodes
 * in Apache Spark 4.x pipelines (.format("bigtable") via spark-bigtable connector).
 *
 * Architectural Decisions:
 * - Batch-only source reading from Cloud Bigtable with a JSON catalog schema mapping.
 * - Requires projectId, instanceId, tableId/table, and catalog JSON definition.
 * - Exposes pushDownRowKeyFilters toggle for server-side row key filter pushdown.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

const SAMPLE_BIGTABLE_CATALOG = `{
  "table": {"name": "user_events"},
  "rowkey": "key",
  "columns": {
    "key": {"cf": "rowkey", "col": "key", "type": "string"},
    "event_type": {"cf": "cf1", "col": "event_type", "type": "string"}
  }
}`;

/** Props for the {@link BigTableIOSourceProperties} component. */
export interface BigTableIOSourcePropertiesProps {
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
        catalog?: string;
        pushDownRowKeyFilters?: boolean;
        appProfileId?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: any;
      };
    };
  };
  onChange: (properties: any) => void;
}

/**
 * Configures properties for Apache Spark Cloud Bigtable batch source nodes (.format("bigtable")).
 */
export const BigTableIOSourceProperties: React.FC<BigTableIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const projectId = properties.projectId || '';
  const instanceId = properties.instanceId || '';
  const tableId = properties.tableId ?? properties.table ?? '';
  const catalog = properties.catalog || '';
  const pushDownRowKeyFilters = properties.pushDownRowKeyFilters ?? true;
  const appProfileId = properties.appProfileId || '';

  const isProjectEmpty = !projectId.trim();
  const isInstanceEmpty = !instanceId.trim();
  const isTableEmpty = !String(tableId).trim();
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
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark Cloud Bigtable Connector</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          .format(&quot;bigtable&quot;) · BATCH
        </span>
      </div>

      {/* Bigtable Coordinates */}
      <div className="flex flex-col gap-3">
        <PropertyField
          id="bigtableSourceProjectId"
          name="projectId"
          label="Project ID"
          required="Project ID is required"
          tooltip="Google Cloud Project ID hosting the Cloud Bigtable instance (spark.bigtable.project.id)."
          value={projectId}
          invalid={isProjectEmpty}
          error={isProjectEmpty ? 'Project ID is required' : null}
          placeholder="e.g. my-gcp-project"
          onChange={handleChange}
          mono={true}
        />

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="bigtableSourceInstanceId"
            name="instanceId"
            label="Instance ID"
            required="Instance ID is required"
            tooltip="Cloud Bigtable Instance ID (spark.bigtable.instance.id)."
            value={instanceId}
            invalid={isInstanceEmpty}
            error={isInstanceEmpty ? 'Instance ID is required' : null}
            placeholder="e.g. prod-bigtable-instance"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="bigtableSourceTableId"
            name="tableId"
            label="Table ID"
            required="Table ID is required"
            tooltip="Cloud Bigtable Table ID to read from."
            value={tableId}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Table ID is required' : null}
            placeholder="e.g. user_sessions"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="bigtableSourceAppProfileId"
          name="appProfileId"
          label="App Profile ID (Optional)"
          tooltip="Optional Bigtable App Profile ID (spark.bigtable.app_profile.id)."
          value={appProfileId}
          onChange={handleChange}
          placeholder="e.g. batch-analytics-profile"
          mono={true}
        />
      </div>

      {/* Catalog JSON Schema Mapping */}
      <PropertyField
        id="bigtableSourceCatalog"
        name="catalog"
        label="Catalog JSON Schema Mapping"
        type="textarea"
        rows={6}
        required="Catalog JSON schema mapping is required"
        tooltip="Spark Bigtable catalog JSON mapping DataFrame columns to Bigtable rowkey and column families (.option('catalog', catalogJson))."
        value={catalog}
        invalid={isCatalogEmpty}
        error={isCatalogEmpty ? 'Catalog JSON schema mapping is required' : null}
        placeholder={SAMPLE_BIGTABLE_CATALOG}
        onChange={handleChange}
        mono={true}
      />

      {/* Row Key Filter Pushdown */}
      <div className="flex items-center gap-2 p-2.5 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <input
          id="bigtableSourcePushDownRowKeyFilters"
          type="checkbox"
          name="pushDownRowKeyFilters"
          checked={pushDownRowKeyFilters}
          onChange={handleChange}
          className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
        />
        <label
          htmlFor="bigtableSourcePushDownRowKeyFilters"
          className="flex items-center gap-1.5 text-xs text-[var(--vscode-text-secondary)] cursor-pointer"
        >
          <span>Push Down Row Key Filters (spark.bigtable.push.down.row.key.filters)</span>
          <InfoTooltip text="Pushes DataFrame row key predicates down to Cloud Bigtable server-side scans to avoid full table scans." />
        </label>
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="bigtableSourceInstructions"
        value={systemInstructions}
        tooltip="Optional custom instructions for Spark Bigtable source code generation."
        placeholder="Enter custom instructions for Spark Bigtable source code generation..."
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

export const BigtableSourceProperties = BigTableIOSourceProperties;
