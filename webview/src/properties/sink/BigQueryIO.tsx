/**
 * @fileoverview BigQuery Sink Properties Component (SNK19).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark BigQuery Sink nodes
 * using the pre-installed Spark BigQuery connector (.write.format("bigquery") and
 * .writeStream.format("bigquery")).
 *
 * Architectural Decisions:
 * - Preserves live GCP dataset, table, and Cloud Storage bucket discovery via useAssetLoader.
 * - Supports direct Storage Write API writes and indirect GCS staging load writes.
 * - Exposes Spark saveMode, createDisposition with optional BigQuerySchemaEditor,
 *   time-unit partitioning, clustering up to four columns, and schema evolution toggles.
 */

import React, { useMemo } from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';
import {
  BigQuerySchemaEditor,
  type BigQueryField,
  jsonToFields,
  stringToFields,
} from './BigQuerySchemaEditor';
import { useAssetLoader } from '../../hooks/useAssetLoader';

export type BigQueryWriteMethod = 'direct' | 'indirect';
export type BigQuerySaveMode = 'append' | 'overwrite' | 'errorifexists' | 'ignore';
export type BigQueryCreateDisposition = 'CREATE_IF_NEEDED' | 'CREATE_NEVER';
export type BigQueryPartitionType = 'HOUR' | 'DAY' | 'MONTH' | 'YEAR';

/** Props for the {@link BigQuerySinkProperties} component. */
export interface BigQuerySinkPropertiesProps {
  node: {
    data: {
      properties?: {
        projectId?: string;
        dataset?: string;
        table?: string;
        writeMethod?: BigQueryWriteMethod;
        method?: string;
        temporaryGcsBucket?: string;
        custom_gcs_temp_location?: string;
        saveMode?: BigQuerySaveMode;
        writeDisposition?: string;
        createDisposition?: string;
        partitionField?: string;
        partitionType?: BigQueryPartitionType;
        clusteredFields?: string;
        allowFieldAddition?: boolean;
        allowFieldRelaxation?: boolean;
        schemaFields?: BigQueryField[];
        schema?: string;
        schemaString?: string;
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
 * Normalizes legacy createDisposition values to Spark BigQuery connector constants.
 */
function normalizeCreateDisposition(raw?: string): BigQueryCreateDisposition {
  if (raw === 'CREATE_NEVER' || raw === 'beam.io.BigQueryDisposition.CREATE_NEVER') {
    return 'CREATE_NEVER';
  }
  return 'CREATE_IF_NEEDED';
}

/**
 * Normalizes legacy writeMethod or Beam method values to Spark direct or indirect modes.
 */
function normalizeWriteMethod(writeMethod?: string, legacyMethod?: string): BigQueryWriteMethod {
  if (writeMethod === 'indirect' || writeMethod === 'direct') {
    return writeMethod;
  }
  if (legacyMethod === 'FILE_LOADS') {
    return 'indirect';
  }
  return 'direct';
}

/**
 * Normalizes Spark saveMode with fallback for legacy writeDisposition values.
 */
function normalizeSaveMode(saveMode?: string, legacyDisposition?: string): BigQuerySaveMode {
  if (
    saveMode === 'append' ||
    saveMode === 'overwrite' ||
    saveMode === 'errorifexists' ||
    saveMode === 'ignore'
  ) {
    return saveMode;
  }
  if (legacyDisposition?.includes('WRITE_TRUNCATE')) {
    return 'overwrite';
  }
  if (legacyDisposition?.includes('WRITE_EMPTY')) {
    return 'errorifexists';
  }
  return 'append';
}

/**
 * Validates optional comma-separated BigQuery clustering column names (up to four columns).
 */
function validateClusteredFields(raw: string): string | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const fields = trimmed.split(',').map((item) => item.trim());
  if (fields.some((item) => item.length === 0 || /\s/.test(item))) {
    return 'Enter comma-separated column names without spaces, e.g. customer_id,region';
  }
  if (fields.length > 4) {
    return 'BigQuery supports at most 4 clustered columns per table';
  }
  return null;
}

/**
 * Configures properties for Apache Spark BigQuery batch and streaming sink nodes.
 */
export const BigQuerySinkProperties: React.FC<BigQuerySinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const dataset = properties.dataset || '';
  const table = properties.table || '';
  const writeMethod = normalizeWriteMethod(properties.writeMethod, properties.method);
  const temporaryGcsBucket =
    properties.temporaryGcsBucket ?? properties.custom_gcs_temp_location ?? '';
  const saveMode = normalizeSaveMode(properties.saveMode, properties.writeDisposition);
  const createDisposition = normalizeCreateDisposition(properties.createDisposition);
  const partitionField = properties.partitionField || '';
  const partitionType: BigQueryPartitionType = properties.partitionType || 'DAY';
  const clusteredFields = properties.clusteredFields || '';
  const allowFieldAddition = properties.allowFieldAddition ?? false;
  const allowFieldRelaxation = properties.allowFieldRelaxation ?? false;

  const isCreateIfNeeded = createDisposition === 'CREATE_IF_NEEDED';

  const schemaFields: BigQueryField[] = useMemo(() => {
    if (Array.isArray(properties.schemaFields)) {
      return properties.schemaFields;
    }
    if (typeof properties.schema === 'string' && properties.schema.trim()) {
      const parsed = jsonToFields(properties.schema);
      if (parsed) {
        return parsed;
      }
    }
    if (typeof properties.schemaString === 'string' && properties.schemaString.trim()) {
      return stringToFields(properties.schemaString);
    }
    return [];
  }, [properties.schemaFields, properties.schema, properties.schemaString]);

  const { items: datasets, error: datasetError, loading: datasetsLoading } =
    useAssetLoader<string>('datasets', {
      onLoaded: (_items, message) => {
        if (message?.projectId) {
          onChange({ ...properties, projectId: message.projectId });
        }
      },
    });

  const { items: tables, error: tableError, loading: tablesLoading } = useAssetLoader<string>(
    'tables',
    {
      params: { dataset },
    }
  );

  const {
    items: buckets,
    error: bucketError,
    loading: bucketsLoading,
  } = useAssetLoader<string>('storageBuckets', {
    enabled: writeMethod === 'indirect',
  });

  const isTableInvalid = !table.trim();
  const isTempBucketInvalid = writeMethod === 'indirect' && !temporaryGcsBucket.trim();
  const clusteredFieldsError = validateClusteredFields(clusteredFields);

  const systemInstructions = resolveSystemInstructions(node);

  const handleSchemaEditorChange = (data: {
    schemaFields: BigQueryField[];
    schema: string;
    schemaString: string;
  }) => {
    onChange({
      ...properties,
      schemaFields: data.schemaFields,
      schema: data.schema,
      schemaString: data.schemaString,
    });
  };

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
      <div className="flex flex-col gap-1 p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <div className="flex items-center justify-between">
          <span className="font-semibold text-slate-200">Spark BigQuery Storage Write API</span>
          <span className="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300 font-mono text-[11px]">
            {'.format("bigquery") · NATIVE'}
          </span>
        </div>
        <span className="text-[11px] text-slate-400">
          Pre-installed on Managed Spark. Supports batch writes and native Structured Streaming.
        </span>
      </div>

      {/* Destination Dataset and Table */}
      <PropertyField
        id="bigQuerySinkDataset"
        name="dataset"
        label="Dataset"
        tooltip="BigQuery dataset containing or receiving the destination table."
        value={dataset}
        onChange={handleChange}
        placeholder="my_dataset"
        list="bq-sink-dataset-options"
        datalist={datasets}
        loading={datasetsLoading}
        error={datasetError}
        mono={true}
      />

      <PropertyField
        id="bigQuerySinkTable"
        name="table"
        label="Table"
        required="Table is required"
        tooltip="Destination BigQuery table name or full identifier (project.dataset.table)."
        value={table}
        invalid={isTableInvalid}
        error={tableError || (isTableInvalid ? 'Table is required.' : null)}
        onChange={handleChange}
        placeholder="my_table"
        list="bq-sink-table-options"
        datalist={tables}
        loading={tablesLoading}
        mono={true}
      />

      {/* Write Method and Save Mode */}
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-2">
          <label
            htmlFor="bigQuerySinkWriteMethod"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>Write Method</span>
            <InfoTooltip text="Select direct to stream rows via the BigQuery Storage Write API, or indirect to stage files in a temporary Cloud Storage bucket." />
          </label>
          <select
            id="bigQuerySinkWriteMethod"
            name="writeMethod"
            value={writeMethod}
            onChange={handleChange}
            className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
          >
            <option value="direct">Direct (Storage Write API)</option>
            <option value="indirect">Indirect (GCS Temp Bucket)</option>
          </select>
        </div>

        <div className="flex flex-col gap-2">
          <label
            htmlFor="bigQuerySinkSaveMode"
            className="flex items-center text-xs font-bold uppercase text-slate-500"
          >
            <span>Save Mode</span>
            <InfoTooltip text="Spark DataFrameWriter save mode controlling how existing table data is handled." />
          </label>
          <select
            id="bigQuerySinkSaveMode"
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
      </div>

      {writeMethod === 'indirect' && (
        <PropertyField
          id="bigQuerySinkTemporaryGcsBucket"
          name="temporaryGcsBucket"
          label="Temporary GCS Bucket"
          required="Temporary GCS bucket is required when Write Method is indirect"
          tooltip="Cloud Storage bucket used to stage temporary Parquet or Avro files before running a BigQuery Load Job (.option('temporaryGcsBucket', ...))."
          value={temporaryGcsBucket}
          invalid={isTempBucketInvalid || Boolean(bucketError)}
          error={
            isTempBucketInvalid
              ? 'Temporary GCS bucket is required for indirect writes.'
              : bucketError
          }
          onChange={handleChange}
          placeholder="my-spark-staging-bucket"
          list="bq-sink-temp-bucket-options"
          datalist={buckets}
          loading={bucketsLoading}
          mono={true}
        />
      )}

      {/* Create Disposition and Optional Schema Editor */}
      <div className="flex flex-col gap-2">
        <label
          htmlFor="bigQuerySinkCreateDisposition"
          className="flex items-center text-xs font-bold uppercase text-slate-500"
        >
          <span>Create Disposition</span>
          <InfoTooltip text="Choose CREATE_IF_NEEDED to create missing destination tables automatically, or CREATE_NEVER to fail if the table does not exist." />
        </label>
        <select
          id="bigQuerySinkCreateDisposition"
          name="createDisposition"
          value={createDisposition}
          onChange={handleChange}
          className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
        >
          <option value="CREATE_IF_NEEDED">Create If Needed (CREATE_IF_NEEDED)</option>
          <option value="CREATE_NEVER">Create Never (CREATE_NEVER)</option>
        </select>
      </div>

      {isCreateIfNeeded && (
        <div className="p-3 bg-slate-800/40 rounded border border-purple-500/30 flex flex-col gap-2.5">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-1.5">
              <span className="text-xs font-bold uppercase text-slate-300">
                Table Schema (Optional Override)
              </span>
              <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-purple-500/20 text-purple-300 font-mono font-semibold">
                {schemaFields.length} {schemaFields.length === 1 ? 'FIELD' : 'FIELDS'}
              </span>
            </div>
            <InfoTooltip text="Optional explicit BigQuery table schema definition when creating a new table. Leave empty to infer from the Spark DataFrame schema." />
          </div>
          <BigQuerySchemaEditor fields={schemaFields} onChange={handleSchemaEditorChange} />
        </div>
      )}

      {/* Partitioning and Clustering */}
      <div className="flex flex-col gap-3 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <label className="flex items-center text-xs font-bold uppercase text-slate-500">
          <span>Partitioning and Clustering</span>
          <InfoTooltip text="Configure BigQuery time-unit partitioning and up to four clustering columns when creating or overwriting tables." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="bigQuerySinkPartitionField"
            name="partitionField"
            label="Partition Field"
            tooltip="DATE or TIMESTAMP column used to partition the destination BigQuery table (.option('partitionField', ...))."
            value={partitionField}
            onChange={handleChange}
            placeholder="event_timestamp"
            mono={true}
          />

          <div className="flex flex-col gap-2">
            <label
              htmlFor="bigQuerySinkPartitionType"
              className="flex items-center text-xs font-bold uppercase text-slate-500"
            >
              <span>Partition Type</span>
              <InfoTooltip text="Time-unit granularity applied when Partition Field is set (.option('partitionType', ...))." />
            </label>
            <select
              id="bigQuerySinkPartitionType"
              name="partitionType"
              value={partitionType}
              onChange={handleChange}
              className="bg-[#252836] text-white p-2 rounded border border-slate-700 text-sm focus:outline-none focus:border-blue-500"
            >
              <option value="HOUR">HOUR</option>
              <option value="DAY">DAY</option>
              <option value="MONTH">MONTH</option>
              <option value="YEAR">YEAR</option>
            </select>
          </div>
        </div>

        <PropertyField
          id="bigQuerySinkClusteredFields"
          name="clusteredFields"
          label="Clustered Fields (Comma-Separated)"
          tooltip="Up to four comma-separated column names for BigQuery table clustering (.option('clusteredFields', 'col1,col2'))."
          value={clusteredFields}
          invalid={Boolean(clusteredFieldsError)}
          error={clusteredFieldsError}
          onChange={handleChange}
          placeholder="customer_id,region"
          mono={true}
        />
      </div>

      {/* Schema and Field Evolution */}
      <div className="flex flex-col gap-2.5 p-3 rounded bg-slate-800/30 border border-slate-700/60">
        <label className="flex items-center text-xs font-bold uppercase text-slate-500">
          <span>Schema Evolution</span>
          <InfoTooltip text="Allow BigQuery to evolve destination table columns automatically during append or overwrite operations." />
        </label>

        <div className="flex items-center gap-2">
          <input
            id="bigQuerySinkAllowFieldAddition"
            type="checkbox"
            name="allowFieldAddition"
            checked={allowFieldAddition}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="bigQuerySinkAllowFieldAddition"
            className="flex items-center text-xs text-slate-300 cursor-pointer"
          >
            <span>Allow Field Addition (allowFieldAddition=true)</span>
            <InfoTooltip text="Appends new nullable columns from the Spark DataFrame to the destination BigQuery table schema." />
          </label>
        </div>

        <div className="flex items-center gap-2">
          <input
            id="bigQuerySinkAllowFieldRelaxation"
            type="checkbox"
            name="allowFieldRelaxation"
            checked={allowFieldRelaxation}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 bg-[#252836]"
          />
          <label
            htmlFor="bigQuerySinkAllowFieldRelaxation"
            className="flex items-center text-xs text-slate-300 cursor-pointer"
          >
            <span>Allow Field Relaxation (allowFieldRelaxation=true)</span>
            <InfoTooltip text="Relaxes existing REQUIRED columns in BigQuery to NULLABLE when incoming DataFrame columns are nullable." />
          </label>
        </div>
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="bigQuerySinkSystemInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark BigQuery sink code generation..."
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
