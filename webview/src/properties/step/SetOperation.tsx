/**
 * @fileoverview Property configuration panel for the Apache Spark SetOperation step (SetOperation).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { GitCompare } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported PySpark relational set operations across two or more DataFrames.
 */
export type SetOperationType = 'intersect' | 'intersectAll' | 'exceptAll' | 'subtract';

/**
 * Persisted properties for the Spark `SetOperation` step node.
 */
export interface SetOperationStepProperties {
  operation?: SetOperationType;
  alignColumnsByName?: boolean;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link SetOperationProperties}.
 */
export interface SetOperationNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: SetOperationStepProperties;
  };
}

/**
 * Props for the {@link SetOperationProperties} component.
 */
export interface SetOperationPropertiesProps {
  node: SetOperationNode;
  onChange: (properties: SetOperationStepProperties) => void;
}

const SET_OPERATION_OPTIONS: SelectFieldOption[] = [
  { value: 'intersect', label: 'intersect (Distinct rows common to all inputs)' },
  { value: 'intersectAll', label: 'intersectAll (Common rows preserving duplicates)' },
  { value: 'exceptAll', label: 'exceptAll (Difference preserving duplicates)' },
  { value: 'subtract', label: 'subtract (Distinct rows in first input only)' },
];

const VALID_SET_OPERATIONS = new Set<string>(SET_OPERATION_OPTIONS.map((opt) => opt.value));

/**
 * Normalizes a raw `operation` value into a valid {@link SetOperationType}.
 *
 * @param raw Raw `operation` property value.
 * @returns Validated Spark set operation name defaulting to `'intersect'`.
 */
export function normalizeSetOperation(raw: unknown): SetOperationType {
  if (typeof raw === 'string' && VALID_SET_OPERATIONS.has(raw)) {
    return raw as SetOperationType;
  }
  return 'intersect';
}

/**
 * Renders the property configuration panel for the Apache Spark SetOperation step.
 *
 * @param props Selected SetOperation node and property update callback.
 * @returns The rendered SetOperation configuration form.
 */
export const SetOperationProperties: React.FC<SetOperationPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: SetOperationStepProperties = node?.data?.properties || {};
  const operation = normalizeSetOperation(properties.operation);
  const alignColumnsByName =
    typeof properties.alignColumnsByName === 'boolean'
      ? properties.alignColumnsByName
      : true;
  const systemInstructions = resolveSystemInstructions(node);

  const emitPatch = (patch: Partial<SetOperationStepProperties>) => {
    onChange({
      ...properties,
      operation,
      alignColumnsByName,
      ...patch,
    });
  };

  const handleOperationChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitPatch({ operation: normalizeSetOperation(e.target.value) });
  };

  const handleAlignColumnsChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    emitPatch({ alignColumnsByName: e.target.checked });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Multi-input ordering banner */}
      <div
        data-testid="set-operation-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <GitCompare className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Ordered Set Inputs (`N-IN`)</span>
          <InfoTooltip text="Connect at least 2 upstream branches. For exceptAll and subtract, the first connected branch is the base set from which later branches are removed." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          Connect two or more upstream DataFrames with identical column sets. For{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">exceptAll</code> and{' '}
          <code className="font-mono text-[var(--vscode-text-primary)]">subtract</code>, Spark
          subtracts subsequent inputs from the first connected branch in edge order.
        </p>
      </div>

      {/* Set Operation & Column Alignment */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <SelectField
          id="setOperationSelect"
          name="operation"
          label="Set Operation"
          tooltip="PySpark relational set method applied across upstream DataFrames via functools.reduce."
          value={operation}
          options={SET_OPERATION_OPTIONS}
          onChange={handleOperationChange}
        />

        <label
          htmlFor="setAlignColumnsByName"
          className="flex items-start gap-2 text-xs text-[var(--vscode-text-primary)] cursor-pointer select-none"
        >
          <input
            id="setAlignColumnsByName"
            name="alignColumnsByName"
            type="checkbox"
            checked={alignColumnsByName}
            onChange={handleAlignColumnsChange}
            className="mt-0.5 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] cursor-pointer"
          />
          <span className="flex items-center flex-wrap gap-1">
            <span className="font-bold">
              Align Columns by Name (`df2.select(*df1.columns)`)
            </span>
            <InfoTooltip text="PySpark intersect, intersectAll, exceptAll, and subtract compare columns by position. Keep this enabled to reorder every secondary DataFrame to match the first DataFrame's column names." />
          </span>
        </label>
      </div>

      <SystemInstructionsField
        id="setOperationSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this SetOperation step."
        placeholder="Enter custom instructions for SetOperation code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
