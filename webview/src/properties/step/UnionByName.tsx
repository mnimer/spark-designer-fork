/**
 * @fileoverview Property configuration panel for the Apache Spark UnionByName step (UnionByName).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { Layers } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Column alignment mode for multi-input Spark DataFrame unions.
 */
export type UnionMode = 'by_name' | 'by_position';

/**
 * Persisted properties for the Spark `UnionByName` step node.
 */
export interface UnionByNameStepProperties {
  unionMode?: UnionMode;
  allowMissingColumns?: boolean;
  deduplicate?: boolean;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link UnionByNameProperties}.
 */
export interface UnionByNameNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: UnionByNameStepProperties;
  };
}

/**
 * Props for the {@link UnionByNameProperties} component.
 */
export interface UnionByNamePropertiesProps {
  node: UnionByNameNode;
  onChange: (properties: UnionByNameStepProperties) => void;
}

const UNION_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'by_name', label: 'By Column Name (df.unionByName)' },
  { value: 'by_position', label: 'By Column Position (df.union)' },
];

/**
 * Normalizes a raw `unionMode` value to a supported {@link UnionMode}.
 *
 * @param raw Raw `unionMode` property value.
 * @returns Validated union mode defaulting to `'by_name'`.
 */
export function normalizeUnionMode(raw: unknown): UnionMode {
  return raw === 'by_position' ? 'by_position' : 'by_name';
}

/**
 * Renders the property configuration panel for the Apache Spark UnionByName step.
 *
 * @param props Selected UnionByName node and property update callback.
 * @returns The rendered UnionByName configuration form.
 */
export const UnionByNameProperties: React.FC<UnionByNamePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: UnionByNameStepProperties = node?.data?.properties || {};
  const unionMode = normalizeUnionMode(properties.unionMode);
  const allowMissingColumns =
    typeof properties.allowMissingColumns === 'boolean'
      ? properties.allowMissingColumns
      : true;
  const deduplicate = properties.deduplicate === true;
  const systemInstructions = resolveSystemInstructions(node);

  const emitPatch = (patch: Partial<UnionByNameStepProperties>) => {
    onChange({
      ...properties,
      unionMode,
      allowMissingColumns,
      deduplicate,
      ...patch,
    });
  };

  const handleUnionModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitPatch({ unionMode: normalizeUnionMode(e.target.value) });
  };

  const handleAllowMissingChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    emitPatch({ allowMissingColumns: e.target.checked });
  };

  const handleDeduplicateChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    emitPatch({ deduplicate: e.target.checked });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Multi-input ordering helper note */}
      <div
        data-testid="union-multi-input-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <Layers className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Multi-Input Ordering (`N-IN`)</span>
          <InfoTooltip text="Connect 2 or more upstream branches to the target handle. Spark combines upstream DataFrames in incoming connection order via functools.reduce." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          Connect at least two upstream branches to this node. Spark merges all incoming DataFrames
          in connection order using a flat{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">functools.reduce</code>{' '}
          pass, adopting initial column order from the first connected branch.
        </p>
      </div>

      {/* Union Mode & Schema Options */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <SelectField
          id="unionModeSelect"
          name="unionMode"
          label="Union Strategy"
          tooltip="Select By Column Name for df.unionByName() schema alignment, or By Column Position for positional df.union()."
          value={unionMode}
          options={UNION_MODE_OPTIONS}
          onChange={handleUnionModeChange}
        />

        {unionMode === 'by_name' && (
          <label
            htmlFor="unionAllowMissingColumns"
            className="flex items-start gap-2 text-xs text-[var(--vscode-text-primary)] cursor-pointer select-none"
          >
            <input
              id="unionAllowMissingColumns"
              name="allowMissingColumns"
              type="checkbox"
              checked={allowMissingColumns}
              onChange={handleAllowMissingChange}
              className="mt-0.5 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] cursor-pointer"
            />
            <span className="flex items-center flex-wrap gap-1">
              <span className="font-bold">
                Allow Missing Columns (`allowMissingColumns=True`)
              </span>
              <InfoTooltip text="When enabled, Spark null-fills columns that exist in some upstream branches but are absent in others instead of failing schema validation." />
            </span>
          </label>
        )}

        <label
          htmlFor="unionDeduplicate"
          className="flex items-start gap-2 text-xs text-[var(--vscode-text-primary)] cursor-pointer select-none"
        >
          <input
            id="unionDeduplicate"
            name="deduplicate"
            type="checkbox"
            checked={deduplicate}
            onChange={handleDeduplicateChange}
            className="mt-0.5 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] cursor-pointer"
          />
          <span className="flex items-center flex-wrap gap-1">
            <span className="font-bold">Deduplicate Rows After Union (`.distinct()`)</span>
            <InfoTooltip text="PySpark unionByName and union retain duplicate rows like SQL UNION ALL. Enable this option to append .distinct() (or .dropDuplicates() on streams) after merging." />
          </span>
        </label>
      </div>

      <SystemInstructionsField
        id="unionByNameSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this UnionByName step."
        placeholder="Enter custom instructions for UnionByName code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
