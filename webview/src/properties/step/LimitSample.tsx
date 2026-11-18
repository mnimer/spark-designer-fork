/**
 * @fileoverview Property configuration panel for the Apache Spark LimitSample step (df.limit / df.sample).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { Filter } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported execution modes for the `LimitSample` step.
 */
export type LimitSampleMode = 'limit' | 'sample';

/**
 * Persisted properties for the Spark `LimitSample` step node.
 */
export interface LimitSampleStepProperties {
  mode?: LimitSampleMode;
  limitCount?: number | string;
  fraction?: number | string;
  withReplacement?: boolean;
  seed?: number | string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link LimitSampleProperties}.
 */
export interface LimitSampleNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: LimitSampleStepProperties;
  };
}

/**
 * Props for the {@link LimitSampleProperties} component.
 */
export interface LimitSamplePropertiesProps {
  node: LimitSampleNode;
  onChange: (properties: LimitSampleStepProperties) => void;
}

const MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'limit', label: 'Row Limit (df.limit)' },
  { value: 'sample', label: 'Fractional Sample (df.sample)' },
];

/**
 * Validates `limitCount` for `df.limit(n)`.
 *
 * @param raw Raw `limitCount` property value.
 * @returns Validation error message when invalid, or null when a positive integer.
 */
export function validateLimitCount(raw: unknown): string | null {
  if (raw === undefined || raw === null || String(raw).trim() === '') {
    return 'Limit count is required.';
  }
  const num = Number(raw);
  if (!Number.isInteger(num) || num < 1) {
    return 'Limit count must be a positive integer greater than or equal to 1.';
  }
  return null;
}

/**
 * Validates `fraction` for `df.sample(withReplacement, fraction, seed)`.
 *
 * @param raw Raw `fraction` property value.
 * @returns Validation error message when outside `[0, 1]`, or null when valid.
 */
export function validateSampleFraction(raw: unknown): string | null {
  if (raw === undefined || raw === null || String(raw).trim() === '') {
    return 'Sample fraction is required.';
  }
  const num = Number(raw);
  if (Number.isNaN(num) || num < 0 || num > 1) {
    return 'Sample fraction must be a number between 0.0 and 1.0.';
  }
  return null;
}

/**
 * Validates the optional random `seed` integer for `df.sample(..., seed=...)`.
 *
 * @param raw Raw `seed` property value.
 * @returns Validation error message when non-integer, or null when valid or omitted.
 */
export function validateSampleSeed(raw: unknown): string | null {
  if (raw === undefined || raw === null || String(raw).trim() === '') {
    return null;
  }
  const num = Number(raw);
  if (!Number.isInteger(num)) {
    return 'Random seed must be an integer when specified.';
  }
  return null;
}

/**
 * Renders the property configuration panel for the Apache Spark LimitSample step.
 *
 * @param props Selected LimitSample node and property update callback.
 * @returns The rendered LimitSample configuration form.
 */
export const LimitSampleProperties: React.FC<LimitSamplePropertiesProps> = ({ node, onChange }) => {
  const properties: LimitSampleStepProperties = node?.data?.properties || {};
  const mode: LimitSampleMode = properties.mode === 'sample' ? 'sample' : 'limit';

  const limitCountValue =
    properties.limitCount !== undefined && properties.limitCount !== null
      ? String(properties.limitCount)
      : '1000';
  const fractionValue =
    properties.fraction !== undefined && properties.fraction !== null
      ? String(properties.fraction)
      : '0.1';
  const withReplacement = Boolean(properties.withReplacement);
  const seedValue =
    properties.seed !== undefined && properties.seed !== null ? String(properties.seed) : '';
  const systemInstructions = resolveSystemInstructions(node);

  const limitError = mode === 'limit' ? validateLimitCount(limitCountValue) : null;
  const fractionError = mode === 'sample' ? validateSampleFraction(fractionValue) : null;
  const seedError = mode === 'sample' ? validateSampleSeed(seedValue) : null;

  const handleModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextMode = e.target.value as LimitSampleMode;
    if (nextMode === 'sample') {
      onChange({
        ...properties,
        mode: 'sample',
        fraction: properties.fraction ?? 0.1,
        withReplacement: Boolean(properties.withReplacement),
      });
    } else {
      onChange({
        ...properties,
        mode: 'limit',
        limitCount: properties.limitCount ?? 1000,
      });
    }
  };

  const handleLimitCountChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const raw = e.target.value;
    const trimmed = raw.trim();
    const numeric = Number(trimmed);
    const nextLimit =
      trimmed !== '' && Number.isInteger(numeric) && numeric >= 1 ? numeric : raw;
    onChange({
      ...properties,
      mode,
      limitCount: nextLimit,
    });
  };

  const handleFractionChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const raw = e.target.value;
    const trimmed = raw.trim();
    const numeric = Number(trimmed);
    const nextFraction =
      trimmed !== '' && !Number.isNaN(numeric) && !trimmed.endsWith('.') ? numeric : raw;
    onChange({
      ...properties,
      mode,
      fraction: nextFraction,
      withReplacement,
    });
  };

  const handleWithReplacementChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange({
      ...properties,
      mode,
      fraction: properties.fraction ?? 0.1,
      withReplacement: e.target.checked,
    });
  };

  const handleSeedChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const raw = e.target.value;
    const trimmed = raw.trim();
    const numeric = Number(trimmed);
    const nextSeed =
      trimmed === '' ? undefined : Number.isInteger(numeric) ? numeric : raw;
    onChange({
      ...properties,
      mode,
      fraction: properties.fraction ?? 0.1,
      withReplacement,
      seed: nextSeed,
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
      {/* 1. Mode Selector */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <Filter className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Limit or Sample Mode</span>
          <InfoTooltip text="Select lazy df.limit(n) to cap output row count or df.sample(...) for statistical row sampling." />
        </div>

        <SelectField
          id="limitSampleMode"
          name="mode"
          label="Execution Mode"
          tooltip="Choose Row Limit for an exact maximum row ceiling or Fractional Sample for probabilistic sampling."
          value={mode}
          options={MODE_OPTIONS}
          onChange={handleModeChange}
        />
      </div>

      {/* 2A. Limit Configuration */}
      {mode === 'limit' && (
        <div
          data-testid="limit-sample-limit-section"
          className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <PropertyField
            id="limitSampleCount"
            name="limitCount"
            type="number"
            min={1}
            step={1}
            label="Row Limit (n)"
            required="Required"
            tooltip="Positive integer passed to df.limit(n). Catalyst pushes LocalLimit down to partitions before GlobalLimit."
            value={limitCountValue}
            onChange={handleLimitCountChange}
            placeholder="1000"
            invalid={Boolean(limitError)}
            error={limitError}
            helperText="Applies lazy df.limit(n) without driver collection."
          />
        </div>
      )}

      {/* 2B. Sample Configuration */}
      {mode === 'sample' && (
        <div
          data-testid="limit-sample-sample-section"
          className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <PropertyField
            id="limitSampleFraction"
            name="fraction"
            type="number"
            min={0}
            max={1}
            step={0.01}
            label="Sample Fraction"
            required="Required"
            tooltip="Target fraction of rows between 0.0 and 1.0 passed to df.sample(withReplacement, fraction, seed)."
            value={fractionValue}
            onChange={handleFractionChange}
            placeholder="0.1"
            invalid={Boolean(fractionError)}
            error={fractionError}
            helperText="Enter a probability between 0.0 (0%) and 1.0 (100%)."
          />

          <label
            htmlFor="limitSampleWithReplacement"
            className="flex items-start gap-2 text-xs text-[var(--vscode-text-primary)] cursor-pointer select-none pt-1"
          >
            <input
              id="limitSampleWithReplacement"
              name="withReplacement"
              type="checkbox"
              checked={withReplacement}
              onChange={handleWithReplacementChange}
              className="mt-0.5 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] cursor-pointer"
            />
            <span className="flex flex-col gap-1">
              <span className="flex items-center gap-1.5 font-semibold">
                <span>Sample With Replacement</span>
                <InfoTooltip text="Passed as withReplacement to df.sample(). When enabled, rows may be selected more than once." />
              </span>
              <span className="text-[var(--vscode-text-muted)] leading-relaxed">
                Allow individual rows to be sampled multiple times.
              </span>
            </span>
          </label>

          <PropertyField
            id="limitSampleSeed"
            name="seed"
            type="number"
            step={1}
            label="Random Seed"
            tooltip="Optional integer seed passed to df.sample(..., seed=...) for reproducible deterministic sampling."
            value={seedValue}
            onChange={handleSeedChange}
            placeholder="Optional (e.g. 42)"
            invalid={Boolean(seedError)}
            error={seedError}
            helperText="Leave empty for a randomized runtime seed, or set an integer for deterministic runs."
          />
        </div>
      )}

      <SystemInstructionsField
        id="limitSampleSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this LimitSample step."
        placeholder="Enter custom instructions for LimitSample code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
