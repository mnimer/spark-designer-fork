/**
 * @fileoverview Shared execution mode controls for dual-mode Sources (Read Mode) and batch Steps (Cache Output, A11).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { SOURCE_NODES } from '../../catalog/nodes/sources';
import type { PropertyFormNode } from '../../catalog/propertyFormRegistry';
import { SelectField, type SelectFieldOption } from './SelectField';
import type { PipelineParadigm } from '../../utils/sharedTypes';

/**
 * Props for the {@link StepExecutionSettingsField} component.
 */
export interface StepExecutionSettingsFieldProps {
  /** Selected React Flow node inspected in PropertyForm. */
  node: PropertyFormNode | null | undefined;
  /** Callback invoked with the merged node properties object. */
  onChange: (properties: Record<string, unknown>) => void;
  /** Active pipeline authoring paradigm ('imperative' default or 'sdp'). */
  paradigm?: PipelineParadigm;
}

const READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'BATCH (spark.read)' },
  { value: 'streaming', label: 'STREAM (spark.readStream)' },
];

const CACHE_OUTPUT_OPTIONS: SelectFieldOption[] = [
  { value: 'auto', label: 'Auto (Cache on Fan-Out > 1)' },
  { value: 'true', label: 'Always Cache (.cache())' },
  { value: 'false', label: 'Never Auto-Cache' },
];

const SDP_STEP_DATASET_OPTIONS: SelectFieldOption[] = [
  { value: 'auto', label: 'Auto (Planner Recommended)' },
  { value: 'temporary_view', label: 'Temporary View (@dp.temporary_view)' },
  { value: 'streaming_table', label: 'Streaming Table (@dp.table)' },
  { value: 'materialized_view', label: 'Materialized View (@dp.materialized_view)' },
];

/**
 * Resolves supported read modes declared on the node payload or catalog definition.
 *
 * @param node React Flow node object.
 * @returns Array of supported read modes when available.
 */
function getSupportedReadModes(node?: PropertyFormNode | null): readonly string[] | undefined {
  if (Array.isArray(node?.data?.supportedReadModes)) {
    return node.data.supportedReadModes;
  }
  const transform = node?.data?.spark_transform || node?.data?.componentType;
  if (!transform) {
    return undefined;
  }
  const catalogMatch = SOURCE_NODES.find((entry) => entry.sparkTransform === transform) as
    | { supportedReadModes?: string[] }
    | undefined;
  return Array.isArray(catalogMatch?.supportedReadModes)
    ? catalogMatch.supportedReadModes
    : undefined;
}

/**
 * Determines whether a Source node supports toggling between `batch` and `streaming` read modes.
 *
 * @param node React Flow node object.
 * @returns True when the node is a dual-mode Source or already carries a `readMode` property.
 */
export function shouldShowSourceReadModeToggle(node?: PropertyFormNode | null): boolean {
  const nodeType = String(node?.data?.type || '').toUpperCase();
  if (nodeType !== 'SOURCE') {
    return false;
  }
  if (node?.data?.properties && node.data.properties.readMode !== undefined) {
    return true;
  }
  const modes = getSupportedReadModes(node);
  return Boolean(modes && modes.includes('batch') && modes.includes('streaming'));
}

/**
 * Determines whether a Step node should expose the manual `Cache output` override selector (A11).
 * Always returns false in Spark Declarative Pipelines mode because SDP forbids manual `.cache()`.
 *
 * @param node React Flow node object.
 * @param paradigm Active pipeline authoring paradigm.
 * @returns True when the node is an imperative batch Step node.
 */
export function shouldShowStepCacheToggle(
  node?: PropertyFormNode | null,
  paradigm?: PipelineParadigm
): boolean {
  if (paradigm === 'sdp' || node?.data?.paradigm === 'sdp') {
    return false;
  }
  const nodeType = String(node?.data?.type || '').toUpperCase();
  if (nodeType !== 'STEP') {
    return false;
  }
  return node?.data?.executionMode !== 'streaming' && node?.data?.isStreaming !== true;
}

/**
 * Normalizes a persisted `cacheOutput` property value to a select option value (`'auto' | 'true' | 'false'`).
 *
 * @param raw Persisted `cacheOutput` property value.
 * @returns Normalized string option value for the dropdown.
 */
export function normalizeCacheOutputSelection(raw: unknown): 'auto' | 'true' | 'false' {
  if (raw === true || raw === 'true' || raw === 'always') {
    return 'true';
  }
  if (raw === false || raw === 'false' || raw === 'never') {
    return 'false';
  }
  return 'auto';
}

/**
 * Renders shared execution controls inside PropertyForm:
 * - For dual-mode `SOURCE` nodes: **Read Mode** (`batch` vs `streaming`) selector.
 * - For imperative batch `STEP` nodes: **Cache Output** (`auto` vs `true` vs `false`) override selector (A11).
 * - For SDP `STEP` nodes: **SDP Dataset Type** (`auto` vs `@dp.temporary_view` / `@dp.table` / `@dp.materialized_view`).
 *
 * @param props Selected node and property update callback.
 * @returns Execution settings card, or null when not applicable.
 */
export const StepExecutionSettingsField: React.FC<StepExecutionSettingsFieldProps> = ({
  node,
  onChange,
  paradigm,
}) => {
  const effectiveParadigm: PipelineParadigm =
    paradigm ?? (node?.data?.paradigm === 'sdp' ? 'sdp' : 'imperative');
  const isSdpStep =
    effectiveParadigm === 'sdp' &&
    String(node?.data?.type || '').toUpperCase() === 'STEP' &&
    node?.data?.spark_transform !== 'MergeCdcScd';
  const showSourceReadMode = shouldShowSourceReadModeToggle(node);
  const showStepCache = shouldShowStepCacheToggle(node, effectiveParadigm);

  if (!showSourceReadMode && !showStepCache && !isSdpStep) {
    return null;
  }

  const properties = node?.data?.properties || {};

  const effectiveReadMode =
    properties.readMode === 'streaming'
      ? 'streaming'
      : properties.readMode === 'batch'
        ? 'batch'
        : node?.data?.defaultReadMode === 'streaming' || node?.data?.executionMode === 'streaming'
          ? 'streaming'
          : 'batch';

  const cacheSelection = normalizeCacheOutputSelection(properties.cacheOutput);
  const sdpDatasetSelection =
    properties.sdpDatasetType === 'temporary_view' ||
    properties.sdpDatasetType === 'streaming_table' ||
    properties.sdpDatasetType === 'materialized_view'
      ? properties.sdpDatasetType
      : 'auto';

  const handleReadModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextMode = e.target.value === 'streaming' ? 'streaming' : 'batch';
    onChange({
      ...properties,
      readMode: nextMode,
    });
  };

  const handleCacheOutputChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const selected = e.target.value;
    const mappedValue: 'auto' | boolean =
      selected === 'true' ? true : selected === 'false' ? false : 'auto';
    onChange({
      ...properties,
      cacheOutput: mappedValue,
    });
  };

  const handleSdpDatasetTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    onChange({
      ...properties,
      sdpDatasetType: e.target.value,
    });
  };

  return (
    <div
      data-testid="node-execution-settings"
      className="p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] flex flex-col gap-3"
    >
      {showSourceReadMode && (
        <SelectField
          id="sourceReadModeSelect"
          name="readMode"
          label="Read Mode"
          tooltip="Select BATCH (spark.read) for bounded snapshots or STREAM (spark.readStream) for continuous Structured Streaming ingestion."
          value={effectiveReadMode}
          options={READ_MODE_OPTIONS}
          onChange={handleReadModeChange}
        />
      )}

      {showStepCache && (
        <SelectField
          id="stepCacheOutputSelect"
          name="cacheOutput"
          label="Cache Output"
          tooltip="Controls DataFrame caching before downstream branches. Auto caches when out-degree exceeds 1 and unpersists after downstream writes complete."
          value={cacheSelection}
          options={CACHE_OUTPUT_OPTIONS}
          onChange={handleCacheOutputChange}
        />
      )}

      {isSdpStep && (
        <SelectField
          id="stepSdpDatasetTypeSelect"
          name="sdpDatasetType"
          label="SDP Dataset Persistence"
          tooltip="Controls whether Spark Declarative Pipelines registers this step as a non-persisted @dp.temporary_view, a persisted @dp.table streaming table, or a @dp.materialized_view."
          value={sdpDatasetSelection}
          options={SDP_STEP_DATASET_OPTIONS}
          onChange={handleSdpDatasetTypeChange}
        />
      )}
    </div>
  );
};
