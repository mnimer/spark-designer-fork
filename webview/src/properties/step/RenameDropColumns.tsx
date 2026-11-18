/**
 * @fileoverview Property configuration panel for the Apache Spark RenameDropColumns step (df.withColumnsRenamed / df.drop).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import { ArrowRight, Plus, Scissors, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Single old-to-new column rename pair passed to `df.withColumnsRenamed({...})`.
 */
export interface RenameColumnMapping {
  from: string;
  to: string;
}

/**
 * Persisted properties for the Spark `RenameDropColumns` step node.
 */
export interface RenameDropColumnsStepProperties {
  renameMappings?: RenameColumnMapping[];
  dropColumns?: string[];
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link RenameDropColumnsProperties}.
 */
export interface RenameDropColumnsNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: RenameDropColumnsStepProperties;
  };
}

/**
 * Props for the {@link RenameDropColumnsProperties} component.
 */
export interface RenameDropColumnsPropertiesProps {
  node: RenameDropColumnsNode;
  onChange: (properties: RenameDropColumnsStepProperties) => void;
}

/**
 * Normalizes raw `renameMappings` configuration into typed {@link RenameColumnMapping} entries.
 *
 * @param raw Raw `renameMappings` value from YAML or UI state.
 * @returns Array of `{ from, to }` rename mappings.
 */
export function normalizeRenameMappings(raw: unknown): RenameColumnMapping[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw.map((entry) => {
    const obj =
      typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    return {
      from: typeof obj.from === 'string' ? obj.from : '',
      to: typeof obj.to === 'string' ? obj.to : '',
    };
  });
}

/**
 * Normalizes raw `dropColumns` configuration into a trimmed `string[]`.
 *
 * @param raw Raw `dropColumns` array or comma-separated string.
 * @returns Trimmed array of column names to drop.
 */
export function normalizeDropColumns(raw: unknown): string[] {
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
 * Renders the property configuration panel for the Apache Spark RenameDropColumns step.
 *
 * @param props Selected RenameDropColumns node and property update callback.
 * @returns The rendered RenameDropColumns configuration form.
 */
export const RenameDropColumnsProperties: React.FC<RenameDropColumnsPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: RenameDropColumnsStepProperties = node?.data?.properties || {};
  const renameMappings = normalizeRenameMappings(properties.renameMappings);
  const dropColumns = normalizeDropColumns(properties.dropColumns);
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingDropInput, setPendingDropInput] = useState<string>('');

  const commitState = (
    nextRenames: RenameColumnMapping[],
    nextDrops: string[]
  ) => {
    onChange({
      ...properties,
      renameMappings: nextRenames,
      dropColumns: nextDrops,
    });
  };

  const handleAddRename = () => {
    commitState([...renameMappings, { from: '', to: '' }], dropColumns);
  };

  const handleRemoveRename = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    commitState(
      renameMappings.filter((_, i) => i !== idx),
      dropColumns
    );
  };

  const handleRenameFieldChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof RenameColumnMapping | undefined;
    if (Number.isNaN(idx) || !field || !renameMappings[idx]) {
      return;
    }
    const nextRenames = renameMappings.map((item, i) =>
      i === idx ? { ...item, [field]: e.target.value } : item
    );
    commitState(nextRenames, dropColumns);
  };

  const appendPendingDropColumns = () => {
    const parsed = normalizeDropColumns(pendingDropInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(dropColumns);
    const merged = [...dropColumns];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    setPendingDropInput('');
    commitState(renameMappings, merged);
  };

  const handlePendingDropKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendPendingDropColumns();
    }
  };

  const handleExistingDropChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...dropColumns];
    updated[idx] = e.target.value;
    commitState(renameMappings, normalizeDropColumns(updated));
  };

  const handleRemoveDropColumn = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    commitState(
      renameMappings,
      dropColumns.filter((_, i) => i !== idx)
    );
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      renameMappings,
      dropColumns,
      system_instructions: value,
    });
  };

  const isBothEmpty = renameMappings.length === 0 && dropColumns.length === 0;

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Rename Mappings Section (df.withColumnsRenamed) */}
      <div
        data-testid="rename-mappings-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <span>Rename Columns</span>
            <InfoTooltip text="Bulk old-to-new column name pairs passed to Spark 4.x df.withColumnsRenamed({'old': 'new'})." />
          </div>
          <button
            type="button"
            onClick={handleAddRename}
            className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add Rename</span>
          </button>
        </div>

        {renameMappings.length > 0 ? (
          <div className="flex flex-col gap-2">
            {renameMappings.map((mapping, index) => {
              const isFromEmpty = mapping.from.trim().length === 0;
              const isToEmpty = mapping.to.trim().length === 0;
              const isRowInvalid = isFromEmpty || isToEmpty;
              return (
                <div
                  key={index}
                  data-testid={`rename-row-${index}`}
                  className="flex flex-col gap-2 p-2 rounded border border-[var(--vscode-border-light)] bg-[var(--vscode-card-bg)]"
                >
                  <div className="flex items-center gap-2">
                    <div className="flex-1 flex flex-col gap-1">
                      <label
                        htmlFor={`renameFrom-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>From</span>
                        <InfoTooltip text="Existing upstream column name to rename." />
                      </label>
                      <input
                        id={`renameFrom-${index}`}
                        type="text"
                        data-index={index}
                        data-field="from"
                        aria-label={`Rename ${index + 1} source column`}
                        aria-invalid={isFromEmpty}
                        value={mapping.from}
                        onChange={handleRenameFieldChange}
                        placeholder="cust_id"
                        className={`w-full bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                          isFromEmpty
                            ? 'border-red-500/80 focus:border-red-500'
                            : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                        }`}
                      />
                    </div>

                    <ArrowRight className="w-4 h-4 text-[var(--vscode-text-muted)] mt-5 shrink-0" />

                    <div className="flex-1 flex flex-col gap-1">
                      <label
                        htmlFor={`renameTo-${index}`}
                        className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        <span>To</span>
                        <InfoTooltip text="New output column name assigned in df.withColumnsRenamed()." />
                      </label>
                      <input
                        id={`renameTo-${index}`}
                        type="text"
                        data-index={index}
                        data-field="to"
                        aria-label={`Rename ${index + 1} target column`}
                        aria-invalid={isToEmpty}
                        value={mapping.to}
                        onChange={handleRenameFieldChange}
                        placeholder="customer_id"
                        className={`w-full bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                          isToEmpty
                            ? 'border-red-500/80 focus:border-red-500'
                            : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                        }`}
                      />
                    </div>

                    <button
                      type="button"
                      data-index={index}
                      aria-label={`Remove rename mapping ${index + 1}`}
                      onClick={handleRemoveRename}
                      className="p-2 mt-5 rounded border border-[var(--vscode-border)] text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>

                  {isRowInvalid && (
                    <span className="text-xs text-red-400" role="alert">
                      Both source and target column names are required.
                    </span>
                  )}
                </div>
              );
            })}
          </div>
        ) : (
          <p className="text-xs text-[var(--vscode-text-muted)]">
            No column renames configured. Click Add Rename to map old column names to new names.
          </p>
        )}
      </div>

      {/* 2. Drop Columns Section (df.drop) */}
      <div
        data-testid="drop-columns-section"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <label
          htmlFor="dropColumnAddInput"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <Scissors className="w-3.5 h-3.5 text-blue-400 shrink-0" />
          <span>Drop Columns</span>
          <InfoTooltip text="Columns removed from the DataFrame via df.drop(*cols). Enter one column or a comma-separated list." />
        </label>

        {dropColumns.length > 0 && (
          <div className="flex flex-col gap-2" data-testid="drop-columns-list">
            {dropColumns.map((col, index) => (
              <div key={`${index}-${col}`} className="flex items-center gap-2">
                <input
                  type="text"
                  data-index={index}
                  aria-label={`Drop column ${index + 1}`}
                  value={col}
                  onChange={handleExistingDropChange}
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <button
                  type="button"
                  data-index={index}
                  aria-label={`Remove drop column ${col}`}
                  onClick={handleRemoveDropColumn}
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
            id="dropColumnAddInput"
            type="text"
            value={pendingDropInput}
            onChange={(e) => setPendingDropInput(e.target.value)}
            onKeyDown={handlePendingDropKeyDown}
            placeholder="e.g. raw_payload or _tmp_col1, _tmp_col2"
            className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
          />
          <button
            type="button"
            onClick={appendPendingDropColumns}
            className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add Drop</span>
          </button>
        </div>
      </div>

      {isBothEmpty && (
        <p className="text-xs text-[var(--vscode-text-muted)] px-1">
          Configure at least one column rename mapping or drop column.
        </p>
      )}

      <SystemInstructionsField
        id="renameDropColumnsSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this RenameDropColumns step."
        placeholder="Enter custom instructions for RenameDropColumns code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
