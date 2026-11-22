/**
 * HCatalogIO / HiveSource Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark HiveSource (`HCatalogIO`) nodes.
 * Configures batch reads (`spark.table("<database>.<table>")`) against Dataproc Metastore or
 * Apache Hive Metastore with `.enableHiveSupport()`.
 *
 * Architectural Decisions:
 * - Batch-only source (`readMode: 'batch'`).
 * - Configures `database` (default `'default'`), `table` (required), `filterPredicate`
 *   (pushdown `.filter(...)` predicate, also supporting legacy `filter` key), and optional
 *   `metastoreUri` (`spark.hadoop.hive.metastore.uris`).
 * - Displays an inline warning when a non-empty `metastoreUri` does not start with `thrift://`.
 * - Uses shared primitives (`PropertyField`, `CollapsibleSection`, `SystemInstructionsField`) on the 8pt grid.
 */

import React, { useState } from 'react';
import { Database } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Props for the {@link HCatalogIOSourceProperties} component. */
export interface HCatalogIOSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        readMode?: 'batch';
        database?: string;
        table?: string;
        filterPredicate?: string;
        filter?: string;
        metastoreUri?: string;
        partitionCols?: string;
        configProperties?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * HCatalogIOSourceProperties renders the configuration panel for Spark Hive Metastore table reads.
 *
 * @param props Component properties containing the active node and onChange callback.
 * @returns React component.
 */
export const HCatalogIOSourceProperties: React.FC<HCatalogIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data?.properties || {};

  const database = properties.database !== undefined ? properties.database : 'default';
  const table = properties.table || '';
  const filterPredicate = properties.filterPredicate ?? properties.filter ?? '';
  const metastoreUri = properties.metastoreUri || '';
  const partitionCols = properties.partitionCols || '';
  const configProperties = properties.configProperties || '';

  const [showAdvanced, setShowAdvanced] = useState(false);

  const isTableEmpty = !table.trim();
  const isMetastoreUriInvalid =
    metastoreUri.trim().length > 0 && !metastoreUri.trim().toLowerCase().startsWith('thrift://');

  const systemInstructions = resolveSystemInstructions(node);

  /**
   * Handles text and textarea input changes.
   *
   * @param e Standard React change event from input or textarea.
   */
  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;
    if (name === 'filterPredicate') {
      onChange({
        ...properties,
        readMode: 'batch',
        filterPredicate: value,
        filter: value,
      });
      return;
    }
    onChange({
      ...properties,
      readMode: 'batch',
      [name]: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Batch Mode Indicator Banner */}
      <div className="flex items-center justify-between px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-subcard-bg)] text-xs">
        <span className="flex items-center gap-1.5 font-bold uppercase text-[var(--vscode-text-muted)]">
          <Database className="w-3.5 h-3.5 text-blue-400" />
          <span>Execution Mode</span>
        </span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          BATCH ONLY (spark.table)
        </span>
      </div>

      {/* Hive Database & Table Card */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Hive Metastore &amp; Table</span>
          <InfoTooltip text="Configure the Hive / Dataproc Metastore database, table name, and optional Thrift URI read via spark.table('<database>.<table>')." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="hcatSourceDatabase"
            name="database"
            label="Database"
            tooltip="Hive Metastore database name. Defaults to 'default'."
            value={database}
            onChange={handleChange}
            placeholder="default"
            mono={true}
          />
          <PropertyField
            id="hcatSourceTable"
            name="table"
            label="Table"
            required="Table is required"
            tooltip="Hive table name to read via spark.table('<database>.<table>'). Required."
            value={table}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Table is required' : null}
            placeholder="my_table"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="hcatSourceFilterPredicate"
          name="filterPredicate"
          label="Partition / Row Filter Predicate"
          tooltip="Optional SQL filter predicate applied immediately to spark.table('<database>.<table>').filter(filterPredicate) for partition pruning."
          value={filterPredicate}
          onChange={handleChange}
          placeholder="e.g. ds = '2026-09-23' AND region = 'us'"
          mono={true}
        />

        <PropertyField
          id="hcatSourceMetastoreUri"
          name="metastoreUri"
          label="Hive Metastore URI (Optional)"
          tooltip="Optional Thrift URI for an external Hive Metastore service (spark.hadoop.hive.metastore.uris, e.g. thrift://metastore:9083). Leave blank when attached to Dataproc Metastore."
          value={metastoreUri}
          helperText={
            isMetastoreUriInvalid ? (
              <span className="text-amber-400" role="alert">
                Warning: Hive Metastore URIs typically start with thrift://
              </span>
            ) : null
          }
          placeholder="thrift://localhost:9083"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Collapsible Advanced Source Settings */}
      <CollapsibleSection
        title="Advanced Hive Settings"
        isOpen={showAdvanced}
        onToggle={() => setShowAdvanced(!showAdvanced)}
      >
        <PropertyField
          id="hcatSourcePartitionCols"
          name="partitionCols"
          label="Partition Columns"
          tooltip="Optional comma-separated list of partition columns for documentation and pruning hints."
          value={partitionCols}
          onChange={handleChange}
          placeholder="e.g. ds, region"
          mono={true}
        />

        <PropertyField
          id="hcatSourceConfigProperties"
          name="configProperties"
          label="Additional Hive Configuration"
          tooltip="Custom Spark/Hive configuration key=value pairs (one per line)."
          type="textarea"
          rows={3}
          value={configProperties}
          onChange={handleChange}
          placeholder="spark.sql.hive.metastorePartitionPruning=true&#10;hive.metastore.warehouse.dir=gs://my-warehouse/hive"
          mono={true}
        />
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="hcatSourceInstructions"
        value={systemInstructions}
        tooltip="Optional custom prompt instructions passed to the AI code generator for this Hive source node."
        placeholder="e.g. Read partitioned Hive table via spark.table() and prune to active partitions"
        onChange={(value) =>
          onChange({
            ...properties,
            readMode: 'batch',
            system_instructions: value,
          })
        }
      />
    </div>
  );
};

/** Alias matching the canonical Spark transform name `HiveSource`. */
export const HiveSourceProperties = HCatalogIOSourceProperties;
