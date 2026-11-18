/**
 * @fileoverview Property configuration panel for the Apache Spark Explode step (F.explode / F.posexplode / F.inline).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { Expand } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported PySpark collection generator functions for the `Explode` step.
 */
export type ExplodeFunction =
  | 'explode'
  | 'explode_outer'
  | 'posexplode'
  | 'posexplode_outer'
  | 'inline'
  | 'inline_outer';

/**
 * Persisted properties for the Spark `Explode` step node.
 */
export interface ExplodeStepProperties {
  explodeFunction?: ExplodeFunction;
  sourceColumn?: string;
  outputAlias?: string;
  positionAlias?: string;
  dropSourceColumn?: boolean;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link ExplodeProperties}.
 */
export interface ExplodeNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: ExplodeStepProperties;
  };
}

/**
 * Props for the {@link ExplodeProperties} component.
 */
export interface ExplodePropertiesProps {
  node: ExplodeNode;
  onChange: (properties: ExplodeStepProperties) => void;
}

const EXPLODE_FUNCTION_OPTIONS: SelectFieldOption[] = [
  { value: 'explode', label: 'F.explode (Drop null / empty collections)' },
  { value: 'explode_outer', label: 'F.explode_outer (Keep null / empty as NULL)' },
  { value: 'posexplode', label: 'F.posexplode (Include 0-based position)' },
  { value: 'posexplode_outer', label: 'F.posexplode_outer (Outer + 0-based position)' },
  { value: 'inline', label: 'F.inline (Flatten Array of Structs)' },
  { value: 'inline_outer', label: 'F.inline_outer (Outer Flatten Array of Structs)' },
];

const VALID_EXPLODE_FUNCTIONS = new Set<string>(
  EXPLODE_FUNCTION_OPTIONS.map((opt) => opt.value)
);

/**
 * Normalizes a raw generator name into a valid {@link ExplodeFunction}.
 *
 * @param raw Raw `explodeFunction` property value from YAML or UI state.
 * @returns Validated generator function defaulting to `'explode'`.
 */
export function normalizeExplodeFunction(raw: unknown): ExplodeFunction {
  if (typeof raw === 'string' && VALID_EXPLODE_FUNCTIONS.has(raw.trim())) {
    return raw.trim() as ExplodeFunction;
  }
  return 'explode';
}

/**
 * Renders the property configuration panel for the Apache Spark Explode step.
 *
 * @param props Selected Explode node and property update callback.
 * @returns The rendered Explode configuration form.
 */
export const ExplodeProperties: React.FC<ExplodePropertiesProps> = ({ node, onChange }) => {
  const properties: ExplodeStepProperties = node?.data?.properties || {};
  const explodeFunction = normalizeExplodeFunction(properties.explodeFunction);
  const sourceColumn =
    typeof properties.sourceColumn === 'string' ? properties.sourceColumn : '';
  const outputAlias =
    typeof properties.outputAlias === 'string' ? properties.outputAlias : 'item';
  const positionAlias =
    typeof properties.positionAlias === 'string' ? properties.positionAlias : 'pos';
  const dropSourceColumn =
    typeof properties.dropSourceColumn === 'boolean' ? properties.dropSourceColumn : false;
  const systemInstructions = resolveSystemInstructions(node);

  const isPosExplode =
    explodeFunction === 'posexplode' || explodeFunction === 'posexplode_outer';
  const isInline = explodeFunction === 'inline' || explodeFunction === 'inline_outer';

  const isSourceInvalid = sourceColumn.trim().length === 0;
  const isOutputAliasInvalid = !isInline && outputAlias.trim().length === 0;
  const isPositionAliasInvalid = isPosExplode && positionAlias.trim().length === 0;

  const commitPatch = (patch: Partial<ExplodeStepProperties>) => {
    onChange({
      ...properties,
      explodeFunction,
      sourceColumn,
      outputAlias,
      dropSourceColumn,
      ...(isPosExplode ? { positionAlias } : {}),
      ...patch,
    });
  };

  const handleFunctionChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextFn = normalizeExplodeFunction(e.target.value);
    const patch: Partial<ExplodeStepProperties> = { explodeFunction: nextFn };
    if (nextFn === 'posexplode' || nextFn === 'posexplode_outer') {
      patch.positionAlias = positionAlias || 'pos';
    }
    commitPatch(patch);
  };

  const handleTextFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    commitPatch({ [name]: value });
  };

  const handleDropSourceToggle = (e: React.ChangeEvent<HTMLInputElement>) => {
    commitPatch({ dropSourceColumn: e.target.checked });
  };

  const handleSystemInstructionsChange = (value: string) => {
    commitPatch({ system_instructions: value });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Generator Function & Source Collection */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <Expand className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Collection Unnesting</span>
          <InfoTooltip text="Unnest ArrayType, MapType, or ArrayType(StructType) columns into one row per element inside df.select()." />
        </div>

        <SelectField
          id="explodeFunctionSelect"
          name="explodeFunction"
          label="Generator Function"
          tooltip="Choose *_outer variants to preserve parent rows when the array or map is NULL or empty."
          value={explodeFunction}
          options={EXPLODE_FUNCTION_OPTIONS}
          onChange={handleFunctionChange}
        />

        <PropertyField
          id="explodeSourceColumn"
          name="sourceColumn"
          label="Source Collection Column"
          required="Required"
          tooltip="Input ArrayType, MapType, or ArrayType(StructType) column passed to the generator function."
          invalid={isSourceInvalid}
          error={isSourceInvalid ? 'Source collection column is required.' : null}
          value={sourceColumn}
          onChange={handleTextFieldChange}
          placeholder="line_items"
        />

        {isPosExplode && (
          <PropertyField
            id="explodePositionAlias"
            name="positionAlias"
            label="Position Output Alias"
            required="Required"
            tooltip="Column alias assigned to the 0-based index emitted by F.posexplode or F.posexplode_outer."
            invalid={isPositionAliasInvalid}
            error={isPositionAliasInvalid ? 'Position output alias is required.' : null}
            value={positionAlias}
            onChange={handleTextFieldChange}
            placeholder="pos"
          />
        )}

        {!isInline ? (
          <PropertyField
            id="explodeOutputAlias"
            name="outputAlias"
            label="Element Output Alias"
            required="Required"
            tooltip="Column alias assigned to each unnested array element (or map value column)."
            invalid={isOutputAliasInvalid}
            error={isOutputAliasInvalid ? 'Element output alias is required.' : null}
            value={outputAlias}
            onChange={handleTextFieldChange}
            placeholder="item"
          />
        ) : (
          <p
            data-testid="explode-inline-note"
            className="text-xs text-[var(--vscode-text-muted)] p-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] leading-relaxed"
          >
            <code className="font-mono text-[var(--vscode-text-primary)]">{explodeFunction}</code>{' '}
            expands an array of structs directly into top-level columns matching the nested struct
            field names.
          </p>
        )}

        <label
          htmlFor="explodeDropSourceColumn"
          className="flex items-center justify-between gap-2 pt-1 text-xs font-bold uppercase text-[var(--vscode-text-primary)] cursor-pointer"
        >
          <span className="flex items-center gap-1.5">
            <span>Drop Source Column</span>
            <InfoTooltip text="Omit the original array or map column from the output DataFrame after unnesting." />
          </span>
          <input
            id="explodeDropSourceColumn"
            name="dropSourceColumn"
            type="checkbox"
            checked={dropSourceColumn}
            onChange={handleDropSourceToggle}
            className="accent-blue-500 w-4 h-4 cursor-pointer"
          />
        </label>
      </div>

      <SystemInstructionsField
        id="explodeSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this Explode step."
        placeholder="Enter custom instructions for Explode code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
