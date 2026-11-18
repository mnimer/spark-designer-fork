/**
 * @fileoverview Property configuration panel for the Apache Spark Repartition & Coalesce step (RepartitionCoalesce).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useState } from 'react';
import { Layers, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Partitioning operation supported by {@link RepartitionCoalesceProperties}.
 */
export type PartitionOperation = 'coalesce' | 'repartition' | 'repartitionByRange';

/**
 * Persisted properties for the RepartitionCoalesce step node.
 */
export interface RepartitionCoalesceStepProperties {
  operation?: PartitionOperation;
  numPartitions?: number | string;
  partitionColumns?: string[];
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link RepartitionCoalesceProperties}.
 */
export interface RepartitionCoalesceNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: RepartitionCoalesceStepProperties;
  };
}

/**
 * Props for the {@link RepartitionCoalesceProperties} component.
 */
export interface RepartitionCoalescePropertiesProps {
  node: RepartitionCoalesceNode;
  onChange: (properties: RepartitionCoalesceStepProperties) => void;
}

const OPERATION_OPTIONS: SelectFieldOption[] = [
  { value: 'coalesce', label: 'coalesce (Reduce Without Shuffle)' },
  { value: 'repartition', label: 'repartition (Hash / Round-Robin Shuffle)' },
  { value: 'repartitionByRange', label: 'repartitionByRange (Sorted Range Shuffle)' },
];

/**
 * Normalizes raw partition column entries into a clean string array.
 *
 * @param raw Raw partitionColumns value from node properties or user input.
 * @returns Trimmed array of non-empty column names.
 */
function normalizePartitionColumns(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw
      .map((item) => String(item ?? '').trim())
      .filter((item) => item.length > 0);
  }
  if (typeof raw === 'string' && raw.trim().length > 0) {
    return raw
      .split(',')
      .map((part) => part.trim())
      .filter((part) => part.length > 0);
  }
  return [];
}

/**
 * Renders the property configuration panel for the Apache Spark RepartitionCoalesce step.
 *
 * @param props Selected RepartitionCoalesce node and property update callback.
 * @returns The rendered RepartitionCoalesce configuration form.
 */
export const RepartitionCoalesceProperties: React.FC<RepartitionCoalescePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: RepartitionCoalesceStepProperties = node?.data?.properties || {};
  const operation: PartitionOperation =
    properties.operation === 'coalesce' || properties.operation === 'repartitionByRange'
      ? properties.operation
      : 'repartition';
  const rawNumPartitions = properties.numPartitions;
  const numPartitionsText =
    rawNumPartitions === undefined || rawNumPartitions === null ? '8' : String(rawNumPartitions);
  const partitionColumns = normalizePartitionColumns(properties.partitionColumns);
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingColumnInput, setPendingColumnInput] = useState<string>('');

  const parsedNum = Number(numPartitionsText.trim());
  const isNumPartitionsInvalid =
    numPartitionsText.trim().length === 0 || !Number.isInteger(parsedNum) || parsedNum <= 0;
  const supportsPartitionColumns =
    operation === 'repartition' || operation === 'repartitionByRange';
  const isRangeMissingColumns =
    operation === 'repartitionByRange' && partitionColumns.length === 0;

  const resolvedNumericPartitions =
    !isNumPartitionsInvalid ? parsedNum : numPartitionsText;

  const handleOperationChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextOperation = e.target.value as PartitionOperation;
    onChange({
      ...properties,
      operation: nextOperation,
      numPartitions: resolvedNumericPartitions,
      partitionColumns: nextOperation === 'coalesce' ? [] : partitionColumns,
    });
  };

  const handleNumPartitionsChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const rawVal = e.target.value;
    const numeric = Number(rawVal.trim());
    onChange({
      ...properties,
      operation,
      numPartitions:
        rawVal.trim() !== '' && Number.isInteger(numeric) && numeric > 0 ? numeric : rawVal,
      partitionColumns,
    });
  };

  const appendPendingColumns = () => {
    const parsed = normalizePartitionColumns(pendingColumnInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(partitionColumns);
    const merged = [...partitionColumns];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    setPendingColumnInput('');
    onChange({
      ...properties,
      operation,
      numPartitions: resolvedNumericPartitions,
      partitionColumns: merged,
    });
  };

  const handlePendingColumnKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendPendingColumns();
    }
  };

  const handleExistingColumnChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...partitionColumns];
    updated[idx] = e.target.value;
    onChange({
      ...properties,
      operation,
      numPartitions: resolvedNumericPartitions,
      partitionColumns: normalizePartitionColumns(updated),
    });
  };

  const handleRemoveColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    onChange({
      ...properties,
      operation,
      numPartitions: resolvedNumericPartitions,
      partitionColumns: partitionColumns.filter((_, i) => i !== idx),
    });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Shuffle Cost Guidance Banner */}
      <div
        data-testid="repartition-shuffle-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <Layers className="w-4 h-4 text-blue-400 shrink-0" />
          <span>
            {operation === 'coalesce'
              ? 'Narrow Partition Merge (Zero Shuffle)'
              : operation === 'repartitionByRange'
                ? 'Full Cluster Range Shuffle'
                : 'Full Cluster Hash / Round-Robin Shuffle'}
          </span>
          <InfoTooltip text="Use coalesce to shrink partition count before file sinks without shuffling data across the network. Use repartition or repartitionByRange when increasing parallelism or clustering by key." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          {operation === 'coalesce'
            ? 'Merges existing executor partitions locally without network shuffle. Place immediately before file sinks rather than upstream of heavy compute steps.'
            : operation === 'repartitionByRange'
              ? 'Samples key boundaries and performs a full cluster shuffle into sorted ranges, improving Parquet and Iceberg min-max file pruning.'
              : 'Executes a full network shuffle to rebalance skewed partitions or co-locate rows by hash key.'}
        </p>
      </div>

      {/* 2. Operation & Target Partition Count */}
      <SelectField
        id="repartitionOperation"
        name="operation"
        label="Partitioning Operation"
        tooltip="Select df.coalesce(n) to reduce partitions without shuffle, df.repartition(n, *cols) for hash or round-robin shuffle, or df.repartitionByRange(n, *cols) for range shuffle."
        value={operation}
        options={OPERATION_OPTIONS}
        onChange={handleOperationChange}
      />

      <PropertyField
        id="repartitionNumPartitions"
        name="numPartitions"
        label="Number of Partitions"
        type="number"
        min={1}
        step={1}
        required="Required"
        tooltip="Target positive integer partition count passed to coalesce, repartition, or repartitionByRange."
        value={numPartitionsText}
        onChange={handleNumPartitionsChange}
        placeholder="8"
        invalid={isNumPartitionsInvalid}
        error={
          isNumPartitionsInvalid
            ? 'Number of partitions must be a positive integer.'
            : null
        }
      />

      {/* 3. Partition Columns (repartition & repartitionByRange only) */}
      {supportsPartitionColumns && (
        <div
          data-testid="repartition-columns-section"
          className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <label
            htmlFor="repartitionColAddInput"
            className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
          >
            <span>Partition Columns</span>
            {operation === 'repartitionByRange' && (
              <span className="text-red-400 font-bold" title="Required">*</span>
            )}
            <InfoTooltip
              text={
                operation === 'repartitionByRange'
                  ? 'Required sort/range columns passed to df.repartitionByRange(n, *cols).'
                  : 'Optional hash key columns passed to df.repartition(n, *cols). Leave empty for round-robin rebalancing.'
              }
            />
          </label>

          {partitionColumns.length > 0 && (
            <div className="flex flex-col gap-2" data-testid="repartition-columns-list">
              {partitionColumns.map((col, index) => (
                <div key={`${index}-${col}`} className="flex items-center gap-2">
                  <input
                    type="text"
                    data-index={index}
                    aria-label={`Partition column ${index + 1}`}
                    value={col}
                    onChange={handleExistingColumnChange}
                    className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                  />
                  <button
                    type="button"
                    data-index={index}
                    aria-label={`Remove partition column ${col}`}
                    onClick={handleRemoveColumn}
                    className="p-2 rounded border border-[var(--vscode-border)] text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              ))}
            </div>
          )}

          <div className="flex items-center gap-2">
            <input
              id="repartitionColAddInput"
              type="text"
              aria-invalid={isRangeMissingColumns}
              value={pendingColumnInput}
              onChange={(e) => setPendingColumnInput(e.target.value)}
              onKeyDown={handlePendingColumnKeyDown}
              placeholder="e.g. event_date or tenant_id, event_date"
              className={`flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                isRangeMissingColumns
                  ? 'border-red-500/80 focus:border-red-500'
                  : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
              }`}
            />
            <button
              type="button"
              onClick={appendPendingColumns}
              className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
            >
              <Plus className="w-3.5 h-3.5" />
              <span>Add</span>
            </button>
          </div>

          {isRangeMissingColumns && (
            <span className="text-xs text-red-400" role="alert">
              At least one partition column is required for repartitionByRange.
            </span>
          )}
        </div>
      )}

      <SystemInstructionsField
        id="repartitionCoalesceSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this RepartitionCoalesce step."
        placeholder="Enter custom instructions for partitioning or file compaction..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
