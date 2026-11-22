/**
 * @fileoverview Spark Declarative Pipelines target dataset and external sink settings card for Sink panels (SDP2).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useState } from 'react';
import { Sparkles } from 'lucide-react';
import { CollapsibleSection } from './CollapsibleSection';
import { PropertyField } from './PropertyField';
import { SelectField, type SelectFieldOption } from './SelectField';
import { InfoTooltip } from '../InfoTooltip';
import { sanitizeSdpIdentifier } from '../../utils/sdpPlanner';
import type { PipelineParadigm } from '../../utils/sharedTypes';

/**
 * Target mode distinguishing persisted SDP catalog tables from external streaming sinks.
 */
export type SdpSinkTargetKind = 'catalog_table' | 'external_sink';

/**
 * Mutually exclusive physical layout mode for SDP catalog tables.
 */
export type SdpTableLayoutMode = 'none' | 'partition_cols' | 'cluster_by';

/**
 * Node shape accepted by {@link SdpSinkSettingsField}.
 */
export interface SdpSinkInspectorNode {
  id?: string;
  data?: {
    label?: string;
    type?: string;
    spark_transform?: string;
    componentType?: string;
    paradigm?: PipelineParadigm;
    sdpDatasetName?: string;
    properties?: Record<string, unknown>;
  };
}

/**
 * Props for the {@link SdpSinkSettingsField} component.
 */
export interface SdpSinkSettingsFieldProps {
  /** Selected React Flow sink node inspected in PropertyForm. */
  readonly node: SdpSinkInspectorNode | null | undefined;
  /** Callback invoked with the merged sink properties object. */
  readonly onChange: (properties: Record<string, unknown>) => void;
  /** Active pipeline authoring paradigm ('imperative' default or 'sdp'). */
  readonly paradigm?: PipelineParadigm;
}

const LAKEHOUSE_CATALOG_SINKS = new Set([
  'IcebergSink',
  'DeltaSink',
  'HudiSink',
  'IcebergIO',
]);

const TARGET_MODE_OPTIONS: SelectFieldOption[] = [
  {
    value: 'catalog_table',
    label: 'Catalog Table (@dp.table / @dp.materialized_view)',
  },
  {
    value: 'external_sink',
    label: 'External Streaming Sink (dp.create_sink)',
  },
];

const CATALOG_FORMAT_OPTIONS: SelectFieldOption[] = [
  { value: 'iceberg', label: 'iceberg (BigLake / REST / Metastore)' },
  { value: 'delta', label: 'delta (Delta Lake)' },
  { value: 'hudi', label: 'hudi (Apache Hudi)' },
  { value: 'parquet', label: 'parquet (Managed Parquet)' },
];

const LAYOUT_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'none', label: 'None (Default Table Layout)' },
  { value: 'partition_cols', label: 'Partition Columns (partition_cols)' },
  { value: 'cluster_by', label: 'Cluster By Columns (cluster_by)' },
];

/**
 * Resolves the default SDP sink target mode from the connector transform identifier.
 */
function getDefaultSinkTargetKind(transform: string): SdpSinkTargetKind {
  return LAKEHOUSE_CATALOG_SINKS.has(transform) ? 'catalog_table' : 'external_sink';
}

/**
 * Resolves the default Spark format identifier for an SDP catalog table or external sink.
 */
function getDefaultSdpFormat(transform: string, targetKind: SdpSinkTargetKind): string {
  if (targetKind === 'catalog_table') {
    if (transform === 'DeltaSink') return 'delta';
    if (transform === 'HudiSink') return 'hudi';
    return 'iceberg';
  }
  switch (transform) {
    case 'KafkaSink':
    case 'KafkaIO':
    case 'WriteToKafka':
    case 'EventHubsSink':
      return 'kafka';
    case 'BigQuerySink':
    case 'BigQueryIO':
    case 'BigQuery':
    case 'WriteToBigQuery':
      return 'bigquery';
    case 'PulsarSink':
    case 'PulsarIO':
      return 'pulsar';
    case 'IcebergSink':
    case 'IcebergIO':
      return 'iceberg';
    case 'DeltaSink':
      return 'delta';
    case 'HudiSink':
      return 'hudi';
    case 'ParquetSink':
    case 'ParquetIO':
    case 'WriteToParquet':
      return 'parquet';
    case 'OrcSink':
      return 'orc';
    case 'AvroSink':
    case 'AvroIO':
    case 'WriteToAvro':
      return 'avro';
    case 'CsvSink':
    case 'CsvIO':
    case 'WriteToCsv':
      return 'csv';
    case 'JsonSink':
    case 'JsonIO':
    case 'WriteToJson':
      return 'json';
    case 'TextSink':
    case 'TextIO':
    case 'WriteToText':
      return 'text';
    case 'MongoDbSink':
    case 'MongoDbIO':
      return 'mongodb';
    case 'CassandraSink':
    case 'CassandraIO':
      return 'org.apache.spark.sql.cassandra';
    case 'ElasticsearchSink':
    case 'ElasticsearchIO':
      return 'org.elasticsearch.spark.sql';
    case 'Neo4jSink':
    case 'Neo4jIO':
      return 'org.neo4j.spark.DataSource';
    case 'MemorySink':
      return 'memory';
    case 'ConsoleSink':
    case 'LogMessage':
    default:
      return 'console';
  }
}

/**
 * Formats an optional string or string-array column list as a comma-separated string.
 */
function formatColumnList(raw: unknown): string {
  if (Array.isArray(raw)) {
    return raw.map((entry) => String(entry).trim()).filter(Boolean).join(', ');
  }
  return typeof raw === 'string' ? raw : '';
}

/**
 * Resolves the active mutually exclusive table layout mode from persisted sink properties.
 */
function resolveLayoutMode(properties: Record<string, unknown>): SdpTableLayoutMode {
  if (
    properties.sdpLayoutMode === 'partition_cols' ||
    properties.sdpLayoutMode === 'cluster_by' ||
    properties.sdpLayoutMode === 'none'
  ) {
    return properties.sdpLayoutMode;
  }
  const hasCluster = formatColumnList(properties.sdpClusterBy ?? properties.clusterBy).trim().length > 0;
  if (hasCluster) {
    return 'cluster_by';
  }
  const hasPartition =
    formatColumnList(properties.sdpPartitionCols ?? properties.partitionCols).trim().length > 0;
  if (hasPartition) {
    return 'partition_cols';
  }
  return 'none';
}

/**
 * Renders Spark Declarative Pipelines (SDP) target dataset and sink configuration controls
 * inside Sink property panels when `paradigm === 'sdp'`.
 *
 * @param props Sink node, active paradigm, and property change callback.
 * @returns Collapsible SDP sink settings card, or null outside SDP Sink inspection.
 */
export const SdpSinkSettingsField: React.FC<SdpSinkSettingsFieldProps> = ({
  node,
  onChange,
  paradigm,
}) => {
  const [isOpen, setIsOpen] = useState<boolean>(true);

  const nodeType = String(node?.data?.type || '').toUpperCase();
  const effectiveParadigm: PipelineParadigm =
    paradigm ?? (node?.data?.paradigm === 'sdp' ? 'sdp' : 'imperative');

  if (nodeType !== 'SINK' || effectiveParadigm !== 'sdp') {
    return null;
  }

  const properties: Record<string, unknown> = node?.data?.properties ?? {};
  const transform = String(node?.data?.spark_transform || node?.data?.componentType || '');
  const defaultKind = getDefaultSinkTargetKind(transform);
  const targetKind: SdpSinkTargetKind =
    properties.sdpSinkKind === 'catalog_table' || properties.sdpSinkKind === 'external_sink'
      ? properties.sdpSinkKind
      : defaultKind;

  const fallbackIdentifier = sanitizeSdpIdentifier(
    String(
      node?.data?.sdpDatasetName ||
        properties.table ||
        node?.data?.label ||
        node?.id ||
        'sdp_target'
    ),
    'sdp_target'
  );

  const sdpTableName =
    typeof properties.sdpTableName === 'string' ? properties.sdpTableName : '';
  const sdpSinkName =
    typeof properties.sdpSinkName === 'string' ? properties.sdpSinkName : '';
  const defaultFormat = getDefaultSdpFormat(transform, targetKind);
  const sdpFormat =
    typeof properties.sdpFormat === 'string' && properties.sdpFormat.trim().length > 0
      ? properties.sdpFormat
      : defaultFormat;

  const layoutMode = resolveLayoutMode(properties);
  const sdpPartitionCols = formatColumnList(
    properties.sdpPartitionCols ?? properties.partitionCols
  );
  const sdpClusterBy = formatColumnList(properties.sdpClusterBy ?? properties.clusterBy);
  const sdpResetAllowed = properties.sdpResetAllowed !== false;
  const isCatalogTable = targetKind === 'catalog_table';

  const handleToggleOpen = () => {
    setIsOpen((prev) => !prev);
  };

  const handleTargetKindChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextKind: SdpSinkTargetKind =
      e.target.value === 'catalog_table' ? 'catalog_table' : 'external_sink';
    const nextDefaultFormat = getDefaultSdpFormat(transform, nextKind);
    onChange({
      ...properties,
      sdpSinkKind: nextKind,
      sdpFormat: nextDefaultFormat,
    });
  };

  const handleTableNameChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    onChange({
      ...properties,
      sdpTableName: e.target.value,
    });
  };

  const handleSinkNameChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    onChange({
      ...properties,
      sdpSinkName: e.target.value,
    });
  };

  const handleFormatSelectChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    onChange({
      ...properties,
      sdpFormat: e.target.value,
    });
  };

  const handleFormatInputChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    onChange({
      ...properties,
      sdpFormat: e.target.value,
    });
  };

  const handleLayoutModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextMode: SdpTableLayoutMode =
      e.target.value === 'partition_cols'
        ? 'partition_cols'
        : e.target.value === 'cluster_by'
          ? 'cluster_by'
          : 'none';

    const nextProperties: Record<string, unknown> = {
      ...properties,
      sdpLayoutMode: nextMode,
    };

    if (nextMode === 'partition_cols') {
      nextProperties.sdpClusterBy = '';
      if ('clusterBy' in nextProperties) {
        nextProperties.clusterBy = '';
      }
    } else if (nextMode === 'cluster_by') {
      nextProperties.sdpPartitionCols = '';
      if ('partitionCols' in nextProperties) {
        nextProperties.partitionCols = '';
      }
    } else {
      nextProperties.sdpPartitionCols = '';
      nextProperties.sdpClusterBy = '';
      if ('partitionCols' in nextProperties) {
        nextProperties.partitionCols = '';
      }
      if ('clusterBy' in nextProperties) {
        nextProperties.clusterBy = '';
      }
    }

    onChange(nextProperties);
  };

  const handlePartitionColsChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const nextProperties: Record<string, unknown> = {
      ...properties,
      sdpLayoutMode: 'partition_cols',
      sdpPartitionCols: e.target.value,
      sdpClusterBy: '',
    };
    if ('clusterBy' in nextProperties) {
      nextProperties.clusterBy = '';
    }
    onChange(nextProperties);
  };

  const handleClusterByChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const nextProperties: Record<string, unknown> = {
      ...properties,
      sdpLayoutMode: 'cluster_by',
      sdpClusterBy: e.target.value,
      sdpPartitionCols: '',
    };
    if ('partitionCols' in nextProperties) {
      nextProperties.partitionCols = '';
    }
    onChange(nextProperties);
  };

  const handleResetAllowedChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange({
      ...properties,
      sdpResetAllowed: e.target.checked,
    });
  };

  return (
    <CollapsibleSection
      id="sdp-sink-settings"
      title="SDP Target Dataset & Sink Settings"
      icon={<Sparkles className="w-3.5 h-3.5 text-amber-400" />}
      isOpen={isOpen}
      onToggle={handleToggleOpen}
    >
      <SelectField
        id="sdpSinkTargetMode"
        name="sdpSinkKind"
        label="SDP Target Mode"
        tooltip="Choose Catalog Table to declare a managed @dp.table or @dp.materialized_view in the pipeline catalog, or External Streaming Sink to declare dp.create_sink with an @dp.append_flow."
        value={targetKind}
        options={TARGET_MODE_OPTIONS}
        onChange={handleTargetKindChange}
      />

      {isCatalogTable ? (
        <>
          <PropertyField
            id="sdpSinkTableName"
            name="sdpTableName"
            label="Catalog Table Name"
            tooltip="Unqualified snake_case dataset name registered under the pipeline catalog and database in spark-pipeline.yml."
            value={sdpTableName}
            onChange={handleTableNameChange}
            placeholder={fallbackIdentifier}
            helperText={`Defaults to "${fallbackIdentifier}" in <catalog>.<database> when left blank.`}
          />

          <SelectField
            id="sdpSinkFormat"
            name="sdpFormat"
            label="Catalog Table Format"
            tooltip="Storage table format passed to format= on @dp.table, @dp.materialized_view, or dp.create_streaming_table."
            value={sdpFormat}
            options={CATALOG_FORMAT_OPTIONS}
            onChange={handleFormatSelectChange}
          />
        </>
      ) : (
        <>
          <PropertyField
            id="sdpSinkNameInput"
            name="sdpSinkName"
            label="External Sink Name"
            tooltip="Plain snake_case identifier passed to dp.create_sink(name=..., format=..., options=...) and targeted by @dp.append_flow."
            value={sdpSinkName}
            onChange={handleSinkNameChange}
            placeholder={fallbackIdentifier}
            helperText={`Defaults to "${fallbackIdentifier}" when left blank.`}
          />

          <PropertyField
            id="sdpSinkFormatInput"
            name="sdpFormat"
            label="External Sink Format"
            tooltip="Spark Structured Streaming format identifier passed to dp.create_sink(name, format, options), such as kafka or bigquery."
            value={sdpFormat}
            onChange={handleFormatInputChange}
            placeholder={defaultFormat}
          />
        </>
      )}

      <SelectField
        id="sdpSinkLayoutMode"
        name="sdpLayoutMode"
        label="Table Layout"
        tooltip="SDP forbids setting both partition_cols and cluster_by on the same dataset (SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED)."
        value={isCatalogTable ? layoutMode : 'none'}
        options={LAYOUT_MODE_OPTIONS}
        onChange={handleLayoutModeChange}
        disabled={!isCatalogTable}
        helperText={
          isCatalogTable
            ? 'Mutually exclusive: selecting partition_cols clears cluster_by and vice versa.'
            : 'Table layout applies only to Catalog Tables, not dp.create_sink targets.'
        }
      />

      {isCatalogTable && layoutMode === 'partition_cols' && (
        <PropertyField
          id="sdpSinkPartitionCols"
          name="sdpPartitionCols"
          label="Partition Columns (partition_cols)"
          tooltip="Comma-separated identity partition columns passed to partition_cols=[...] on the SDP catalog dataset."
          value={sdpPartitionCols}
          onChange={handlePartitionColsChange}
          placeholder="event_date, region"
        />
      )}

      {isCatalogTable && layoutMode === 'cluster_by' && (
        <PropertyField
          id="sdpSinkClusterBy"
          name="sdpClusterBy"
          label="Cluster By Columns (cluster_by)"
          tooltip="Comma-separated clustering columns passed to cluster_by=[...] on the SDP catalog dataset."
          value={sdpClusterBy}
          onChange={handleClusterByChange}
          placeholder="customer_id, event_date"
        />
      )}

      <div className="flex flex-col gap-1.5 pt-1">
        <div className="flex items-center gap-2">
          <input
            id="sdpSinkResetAllowed"
            name="sdpResetAllowed"
            type="checkbox"
            checked={isCatalogTable ? sdpResetAllowed : true}
            disabled={!isCatalogTable}
            onChange={handleResetAllowedChange}
            className="rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] text-blue-500 focus:ring-blue-500 disabled:opacity-50"
          />
          <label
            htmlFor="sdpSinkResetAllowed"
            className="flex items-center text-xs font-bold uppercase text-[var(--vscode-text-muted)] gap-1"
          >
            <span>Allow Full Refresh Reset (pipelines.reset.allowed)</span>
            <InfoTooltip text="When unchecked on a catalog streaming table, emits table_properties={'pipelines.reset.allowed': 'false'} so full-refresh runs cannot truncate accumulated history." />
          </label>
        </div>

        {isCatalogTable && !sdpResetAllowed && (
          <span
            data-testid="sdp-reset-dependency-note"
            className="text-[11px] text-amber-300/90 leading-relaxed"
          >
            Emits <code>pipelines.reset.allowed = &quot;false&quot;</code>. Note: any upstream
            streaming table read as a stream must also disable resets to avoid{' '}
            <code>INVALID_RESETTABLE_DEPENDENCY</code>.
          </span>
        )}

        {!isCatalogTable && (
          <span
            data-testid="sdp-external-sink-reset-note"
            className="text-[11px] text-amber-300/90 leading-relaxed"
          >
            <code>pipelines.reset.allowed</code> cannot protect external{' '}
            <code>dp.create_sink</code> targets. Running <code>--full-refresh-all</code> resets
            sink flow checkpoints and re-sends available source records as duplicates.
          </span>
        )}
      </div>
    </CollapsibleSection>
  );
};
