/**
 * @fileoverview Property configuration panel for the Apache Spark Cache & Persist step (CachePersist).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React from 'react';
import { HardDrive } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Persistence API mode for {@link CachePersistProperties}.
 */
export type CachePersistMode = 'cache' | 'persist';

/**
 * Spark 4.x and Spark Connect compatible StorageLevel constants.
 */
export type SparkStorageLevel =
  | 'MEMORY_AND_DISK'
  | 'MEMORY_ONLY'
  | 'DISK_ONLY'
  | 'MEMORY_AND_DISK_DESER'
  | 'OFF_HEAP';

/**
 * Persisted properties for the CachePersist step node.
 */
export interface CachePersistStepProperties {
  mode?: CachePersistMode;
  storageLevel?: SparkStorageLevel;
  unpersistAfterDownstream?: boolean;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link CachePersistProperties}.
 */
export interface CachePersistNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: CachePersistStepProperties;
  };
}

/**
 * Props for the {@link CachePersistProperties} component.
 */
export interface CachePersistPropertiesProps {
  node: CachePersistNode;
  onChange: (properties: CachePersistStepProperties) => void;
}

const MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'cache', label: 'df.cache() (Default MEMORY_AND_DISK)' },
  { value: 'persist', label: 'df.persist(StorageLevel)' },
];

const STORAGE_LEVEL_OPTIONS: SelectFieldOption[] = [
  { value: 'MEMORY_AND_DISK', label: 'MEMORY_AND_DISK (Spill Overflow to Disk)' },
  { value: 'MEMORY_ONLY', label: 'MEMORY_ONLY (Executor RAM Only)' },
  { value: 'DISK_ONLY', label: 'DISK_ONLY (Executor Scratch Disk)' },
  { value: 'MEMORY_AND_DISK_DESER', label: 'MEMORY_AND_DISK_DESER (Deserialized RAM + Disk)' },
  { value: 'OFF_HEAP', label: 'OFF_HEAP (Managed Off-Heap Memory)' },
];

const VALID_STORAGE_LEVELS = new Set<string>(
  STORAGE_LEVEL_OPTIONS.map((opt) => opt.value)
);

/**
 * Renders the property configuration panel for the Apache Spark CachePersist step.
 *
 * @param props Selected CachePersist node and property update callback.
 * @returns The rendered CachePersist configuration form.
 */
export const CachePersistProperties: React.FC<CachePersistPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: CachePersistStepProperties = node?.data?.properties || {};
  const mode: CachePersistMode = properties.mode === 'persist' ? 'persist' : 'cache';
  const storageLevel: SparkStorageLevel =
    typeof properties.storageLevel === 'string' && VALID_STORAGE_LEVELS.has(properties.storageLevel)
      ? (properties.storageLevel as SparkStorageLevel)
      : 'MEMORY_AND_DISK';
  const unpersistAfterDownstream =
    typeof properties.unpersistAfterDownstream === 'boolean'
      ? properties.unpersistAfterDownstream
      : true;
  const systemInstructions = resolveSystemInstructions(node);

  const handleModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextMode = e.target.value as CachePersistMode;
    onChange({
      ...properties,
      mode: nextMode,
      storageLevel,
      unpersistAfterDownstream,
    });
  };

  const handleStorageLevelChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    onChange({
      ...properties,
      mode,
      storageLevel: e.target.value as SparkStorageLevel,
      unpersistAfterDownstream,
    });
  };

  const handleUnpersistToggle = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange({
      ...properties,
      mode,
      storageLevel,
      unpersistAfterDownstream: e.target.checked,
    });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      mode,
      storageLevel,
      unpersistAfterDownstream,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Batch Materialization & Streaming Guard Banner */}
      <div
        data-testid="cache-persist-guidance-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <HardDrive className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Executor Storage Materialization</span>
          <InfoTooltip text="Persists expensive intermediate batch DataFrames reused across multiple downstream branches. Streaming pipelines skip direct stream caching and cache micro-batches inside foreachBatch instead." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          Materializes reusable batch DataFrames across executor memory or disk. In Structured
          Streaming pipelines, unbounded stream caching is bypassed automatically outside{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">foreachBatch</code>.
        </p>
      </div>

      {/* 2. Persistence Mode & StorageLevel */}
      <SelectField
        id="cachePersistMode"
        name="mode"
        label="Persistence Mode"
        tooltip="Select df.cache() for Spark default MEMORY_AND_DISK caching, or df.persist(StorageLevel) to choose a specific storage tier."
        value={mode}
        options={MODE_OPTIONS}
        onChange={handleModeChange}
      />

      {mode === 'persist' && (
        <SelectField
          id="cachePersistStorageLevel"
          name="storageLevel"
          label="Storage Level"
          tooltip="PySpark 4.x and Spark Connect compatible pyspark.StorageLevel constant passed to df.persist()."
          value={storageLevel}
          options={STORAGE_LEVEL_OPTIONS}
          onChange={handleStorageLevelChange}
        />
      )}

      {/* 3. Deterministic Unpersist Toggle */}
      <div className="flex items-center justify-between gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <label
          htmlFor="cachePersistUnpersistAfter"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)] cursor-pointer"
        >
          <span>Unpersist After Downstream Sinks</span>
          <InfoTooltip text="Calls df.unpersist() inside write_sinks() at ExecutionPlan.unpersistPoints after the last downstream consumer finishes." />
        </label>
        <input
          id="cachePersistUnpersistAfter"
          name="unpersistAfterDownstream"
          type="checkbox"
          checked={unpersistAfterDownstream}
          onChange={handleUnpersistToggle}
          className="w-4 h-4 accent-blue-500 cursor-pointer"
        />
      </div>

      <SystemInstructionsField
        id="cachePersistSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this CachePersist step."
        placeholder="Enter custom instructions for caching or unpersist lifecycle..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
