/**
 * BigQuery Source Properties Component (SRC22)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark BigQuery Source nodes
 * using the Spark BigQuery Storage Read API connector (.format("bigquery")).
 *
 * Architectural Decisions:
 * - Supports batch reads via direct table scan or standard SQL query pushdown.
 * - Uses useAssetLoader for live GCP BigQuery dataset and table discovery.
 * - Configures viewsEnabled and materializationDataset when reading from a SQL query.
 * - Supports Storage Read API filter pushdown and maxParallelism tuning.
 */

import React, { useRef } from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';
import { useAssetLoader } from '../../hooks/useAssetLoader';

export type BigQueryReadModeType = 'table' | 'query';

/** Props for the {@link BigQuerySourceProperties} component. */
export interface BigQuerySourcePropertiesProps {
  node: {
    data: {
      properties?: {
        readModeType?: BigQueryReadModeType;
        projectId?: string;
        dataset?: string;
        table?: string;
        query?: string;
        viewsEnabled?: boolean;
        materializationDataset?: string;
        filter?: string;
        maxParallelism?: number | string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: any;
      };
      system_instructions?: string;
      special_instructions?: string;
    };
  };
  onChange: (properties: any) => void;
}

/**
 * Configures properties for Apache Spark BigQuery batch source nodes (.format("bigquery")).
 */
export const BigQuerySourceProperties: React.FC<BigQuerySourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};
  const propertiesRef = useRef(properties);
  propertiesRef.current = properties;

  const { items: datasets, error: datasetError, loading: datasetsLoading } = useAssetLoader<string>('datasets', {
    onLoaded: (_items, rawMessage) => {
      if (rawMessage?.projectId) {
        onChange({ ...propertiesRef.current, projectId: rawMessage.projectId });
      }
    },
  });

  const { items: tables, error: tableError, loading: tablesLoading } = useAssetLoader<string>('tables', {
    dataset: properties.dataset,
    enabled: Boolean(properties.dataset),
  });

  const table = properties.table || '';
  const query = properties.query || '';
  const readModeType: BigQueryReadModeType =
    properties.readModeType || (query.trim() && !table.trim() ? 'query' : 'table');
  const viewsEnabled = properties.viewsEnabled ?? true;
  const materializationDataset = properties.materializationDataset || '';
  const filter = properties.filter || '';
  const maxParallelism = properties.maxParallelism ?? '';

  const isTableInvalid = readModeType === 'table' && !table.trim();
  const isQueryInvalid = readModeType === 'query' && !query.trim();
  const isMaterializationInvalid =
    readModeType === 'query' && viewsEnabled && !materializationDataset.trim() && !properties.dataset?.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    if (type === 'checkbox') {
      const checked = (e.target as HTMLInputElement).checked;
      onChange({ ...properties, [name]: checked });
      return;
    }
    if (name === 'dataset') {
      onChange({ ...properties, dataset: value, table: '' });
      return;
    }
    onChange({ ...properties, [name]: value });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark BigQuery Storage Read API</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          .format(&quot;bigquery&quot;) · BATCH
        </span>
      </div>

      {/* Read Mode Type Selector */}
      <div className="flex flex-col gap-2">
        <label
          htmlFor="bqSourceReadModeType"
          className="flex items-center text-xs font-bold uppercase text-slate-500"
        >
          <span>Read Target Mode</span>
          <InfoTooltip text="Choose whether to read directly from a BigQuery table using the Storage Read API or run a standard SQL query." />
        </label>
        <select
          id="bqSourceReadModeType"
          name="readModeType"
          value={readModeType}
          onChange={handleChange}
          className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
        >
          <option value="table">Table (Direct Storage Read API)</option>
          <option value="query">SQL Query (viewsEnabled=true)</option>
        </select>
      </div>

      {/* Dataset Input */}
      <PropertyField
        id="bqSourceDataset"
        name="dataset"
        label="Dataset"
        tooltip="BigQuery dataset name containing the source table or used as the default dataset for queries."
        value={properties.dataset || ''}
        onChange={handleChange}
        placeholder="my_dataset"
        list="dataset-options"
        datalist={datasets}
        loading={datasetsLoading}
        error={datasetError}
        mono={true}
      />

      {readModeType === 'table' ? (
        <PropertyField
          id="bqSourceTable"
          name="table"
          label="Table"
          required="Table is required in Table read mode"
          tooltip="BigQuery table specification (for example project.dataset.table or table_name)."
          value={table}
          invalid={isTableInvalid}
          error={isTableInvalid ? 'Table is required in Table read mode' : tableError}
          onChange={handleChange}
          placeholder="my-project.my_dataset.my_table"
          list="table-options"
          datalist={tables}
          loading={tablesLoading}
          mono={true}
        />
      ) : (
        <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
          <PropertyField
            id="bqSourceQuery"
            name="query"
            label="SQL Query"
            type="textarea"
            rows={3}
            required="SQL Query is required in Query read mode"
            tooltip="Standard SQL query executed by the Spark BigQuery connector with viewsEnabled=true."
            value={query}
            invalid={isQueryInvalid}
            error={isQueryInvalid ? 'SQL Query is required in Query read mode' : null}
            onChange={handleChange}
            placeholder="SELECT * FROM `my-project.my_dataset.my_table` WHERE event_date >= '2026-01-01'"
            mono={true}
          />

          <div className="flex items-center gap-2">
            <input
              id="bqSourceViewsEnabled"
              type="checkbox"
              name="viewsEnabled"
              checked={viewsEnabled}
              onChange={handleChange}
              className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
            />
            <label htmlFor="bqSourceViewsEnabled" className="flex items-center text-xs text-slate-300 cursor-pointer">
              <span>Enable Views &amp; SQL Query Materialization (viewsEnabled=true)</span>
              <InfoTooltip text="Required by the Spark BigQuery connector when reading from views or executing custom SQL queries." />
            </label>
          </div>

          <PropertyField
            id="bqSourceMaterializationDataset"
            name="materializationDataset"
            label="Materialization Dataset"
            required={viewsEnabled ? 'Materialization dataset is required for SQL queries' : false}
            tooltip="BigQuery dataset where temporary query result tables are materialized."
            value={materializationDataset}
            invalid={isMaterializationInvalid}
            error={
              isMaterializationInvalid
                ? 'Specify a Materialization Dataset or Dataset for query materialization'
                : null
            }
            onChange={handleChange}
            placeholder="temp_spark_materialization"
            list="materialization-dataset-options"
            datalist={datasets}
            mono={true}
          />
        </div>
      )}

      {/* Storage Read Pushdown & Parallelism */}
      <div className="grid grid-cols-2 gap-2">
        <PropertyField
          id="bqSourceFilter"
          name="filter"
          label="Filter Pushdown"
          tooltip="Optional SQL WHERE predicate pushed down to the BigQuery Storage Read API (for example event_date = '2026-01-01')."
          value={filter}
          onChange={handleChange}
          placeholder="status = 'ACTIVE'"
          mono={true}
        />

        <PropertyField
          id="bqSourceMaxParallelism"
          name="maxParallelism"
          label="Max Parallelism"
          type="number"
          min={1}
          tooltip="Maximum number of BigQuery Storage Read streams (Spark partitions) created for reading."
          value={maxParallelism}
          onChange={handleChange}
          placeholder="e.g. 100"
          mono={true}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="bqSourceSystemInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark BigQuery source code generation..."
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
