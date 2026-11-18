/**
 * @fileoverview Property configuration panel for the Apache Spark ConditionalSplit multi-output router step.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { ArrowDown, ArrowUp, GitFork, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Rule evaluation strategy across ordered `ConditionalSplit` predicates.
 */
export type ConditionalSplitEvaluationMode = 'first_match' | 'independent';

/**
 * Single routing rule stored in `properties.rules` and consumed by `getConditionalSplitHandles`.
 */
export interface ConditionalSplitRule {
  name: string;
  condition: string;
}

/**
 * Validation findings for a single `ConditionalSplit` rule entry.
 */
export interface ConditionalSplitRuleValidation {
  nameError: string | null;
  conditionError: string | null;
}

/**
 * Persisted properties for the Spark `ConditionalSplit` step node.
 */
export interface ConditionalSplitStepProperties {
  rules?: ConditionalSplitRule[];
  evaluationMode?: ConditionalSplitEvaluationMode;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link ConditionalSplitProperties}.
 */
export interface ConditionalSplitNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: ConditionalSplitStepProperties;
  };
}

/**
 * Props for the {@link ConditionalSplitProperties} component.
 */
export interface ConditionalSplitPropertiesProps {
  node: ConditionalSplitNode;
  onChange: (properties: ConditionalSplitStepProperties) => void;
}

const EVALUATION_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'first_match', label: 'First Match Waterfall (mutually exclusive)' },
  { value: 'independent', label: 'Independent Rules (rows can match multiple branches)' },
];

const DEFAULT_RULES: ConditionalSplitRule[] = [{ name: 'rule_1', condition: '' }];

/**
 * Normalizes raw `rules` configuration into typed {@link ConditionalSplitRule} entries
 * matching `getConditionalSplitHandles` in `sparkDagRules.ts`.
 *
 * @param raw Raw `rules` value from YAML or UI state.
 * @returns Non-empty array of routing rules.
 */
export function normalizeConditionalSplitRules(raw: unknown): ConditionalSplitRule[] {
  if (!Array.isArray(raw) || raw.length === 0) {
    return DEFAULT_RULES.map((item) => ({ ...item }));
  }
  return raw.map((entry, idx) => {
    const obj = typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    return {
      name: typeof obj.name === 'string' ? obj.name : `rule_${idx + 1}`,
      condition: typeof obj.condition === 'string' ? obj.condition : '',
    };
  });
}

/**
 * Generates the next unique rule handle name (`rule_N`) that does not collide with existing rules or `else`.
 *
 * @param existingRules Currently configured routing rules.
 * @returns Unique rule name string.
 */
export function buildNextUniqueRuleName(existingRules: ConditionalSplitRule[]): string {
  const taken = new Set(
    existingRules.map((rule) => rule.name.trim().toLowerCase()).filter((n) => n.length > 0)
  );
  taken.add('else');
  let counter = existingRules.length + 1;
  while (taken.has(`rule_${counter}`)) {
    counter += 1;
  }
  return `rule_${counter}`;
}

/**
 * Validates all routing rules for empty names, reserved `else` handle collisions, duplicate names, and blank predicates.
 *
 * @param rules Ordered routing rules to validate.
 * @returns Per-rule validation error messages aligned by index.
 */
export function validateConditionalSplitRules(
  rules: ConditionalSplitRule[]
): ConditionalSplitRuleValidation[] {
  const countsByNormalizedName = new Map<string, number>();
  for (const rule of rules) {
    const normalized = rule.name.trim();
    if (normalized.length > 0) {
      countsByNormalizedName.set(normalized, (countsByNormalizedName.get(normalized) ?? 0) + 1);
    }
  }

  return rules.map((rule) => {
    const trimmedName = rule.name.trim();
    let nameError: string | null = null;
    if (trimmedName.length === 0) {
      nameError = 'Rule handle name is required.';
    } else if (trimmedName.toLowerCase() === 'else') {
      nameError = 'Rule name "else" is reserved for the automatic fallback output handle.';
    } else if ((countsByNormalizedName.get(trimmedName) ?? 0) > 1) {
      nameError = `Duplicate rule name "${trimmedName}". Each output handle must be unique.`;
    }

    const conditionError =
      rule.condition.trim().length === 0 ? 'Boolean SQL condition is required.' : null;

    return { nameError, conditionError };
  });
}

/**
 * Renders the property configuration panel for the Apache Spark ConditionalSplit step.
 *
 * @param props Selected ConditionalSplit node and property update callback.
 * @returns The rendered ConditionalSplit configuration form.
 */
export const ConditionalSplitProperties: React.FC<ConditionalSplitPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: ConditionalSplitStepProperties = node?.data?.properties || {};
  const rules = normalizeConditionalSplitRules(properties.rules);
  const evaluationMode: ConditionalSplitEvaluationMode =
    properties.evaluationMode === 'independent' ? 'independent' : 'first_match';
  const validations = validateConditionalSplitRules(rules);
  const systemInstructions = resolveSystemInstructions(node);

  const commitRules = (nextRules: ConditionalSplitRule[]) => {
    onChange({
      ...properties,
      evaluationMode,
      rules: nextRules,
    });
  };

  const handleEvaluationModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    onChange({
      ...properties,
      evaluationMode: e.target.value as ConditionalSplitEvaluationMode,
      rules,
    });
  };

  const handleAddRule = () => {
    const nextName = buildNextUniqueRuleName(rules);
    commitRules([...rules, { name: nextName, condition: '' }]);
  };

  const handleRemoveRule = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx) || rules.length <= 1) {
      return;
    }
    commitRules(rules.filter((_, i) => i !== idx));
  };

  const handleMoveRule = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const delta = Number(e.currentTarget.dataset.delta);
    const targetIdx = idx + delta;
    if (
      Number.isNaN(idx) ||
      Number.isNaN(delta) ||
      targetIdx < 0 ||
      targetIdx >= rules.length
    ) {
      return;
    }
    const reordered = [...rules];
    const [moved] = reordered.splice(idx, 1);
    reordered.splice(targetIdx, 0, moved);
    commitRules(reordered);
  };

  const handleRuleFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof ConditionalSplitRule | undefined;
    if (Number.isNaN(idx) || !field || !rules[idx]) {
      return;
    }
    const nextRules = rules.map((item, i) =>
      i === idx ? { ...item, [field]: e.target.value } : item
    );
    commitRules(nextRules);
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Evaluation Mode */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <GitFork className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Branch Evaluation Mode</span>
          <InfoTooltip text="Controls whether rules evaluate as a mutually exclusive CASE WHEN waterfall or as independent filters." />
        </div>

        <SelectField
          id="conditionalSplitEvaluationMode"
          name="evaluationMode"
          label="Evaluation Mode"
          tooltip="First Match routes each row to the first matching rule only. Independent routes each row to every matching rule."
          value={evaluationMode}
          options={EVALUATION_MODE_OPTIONS}
          onChange={handleEvaluationModeChange}
          helperText={
            evaluationMode === 'first_match'
              ? 'Each branch excludes rows matched by earlier rules above it.'
              : 'Each rule evaluates independently on the input DataFrame.'
          }
        />
      </div>

      {/* 2. Dynamic Output Handle Rules */}
      <div
        data-testid="conditional-split-rules-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            <span>Output Branch Rules</span>
            <InfoTooltip text="Each rule creates a named green output handle on the ConditionalSplit canvas node." />
          </div>
          <button
            type="button"
            onClick={handleAddRule}
            className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add Rule</span>
          </button>
        </div>

        <div className="flex flex-col gap-3">
          {rules.map((rule, index) => {
            const { nameError, conditionError } = validations[index] ?? {
              nameError: null,
              conditionError: null,
            };
            return (
              <div
                key={index}
                data-testid={`conditional-rule-card-${index}`}
                className="flex flex-col gap-2 p-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)]"
              >
                <div className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
                      Rule #{index + 1}
                    </span>
                    <span className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-emerald-500/15 border border-emerald-500/30 text-emerald-300">
                      {rule.name.trim() || `rule_${index + 1}`}
                    </span>
                  </div>
                  <div className="flex items-center gap-1">
                    <button
                      type="button"
                      data-index={index}
                      data-delta={-1}
                      disabled={index === 0}
                      aria-label={`Move rule ${index + 1} up`}
                      onClick={handleMoveRule}
                      className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] disabled:opacity-40 disabled:cursor-not-allowed transition-colors cursor-pointer"
                    >
                      <ArrowUp className="w-3.5 h-3.5" />
                    </button>
                    <button
                      type="button"
                      data-index={index}
                      data-delta={1}
                      disabled={index === rules.length - 1}
                      aria-label={`Move rule ${index + 1} down`}
                      onClick={handleMoveRule}
                      className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] disabled:opacity-40 disabled:cursor-not-allowed transition-colors cursor-pointer"
                    >
                      <ArrowDown className="w-3.5 h-3.5" />
                    </button>
                    {rules.length > 1 && (
                      <button
                        type="button"
                        data-index={index}
                        aria-label={`Remove rule ${index + 1}`}
                        onClick={handleRemoveRule}
                        className="p-1 rounded text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    )}
                  </div>
                </div>

                <div className="flex flex-col gap-1">
                  <label
                    htmlFor={`splitRuleName-${index}`}
                    className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                  >
                    <span>Handle Name</span>
                    <span className="text-red-400 font-bold" title="Required">*</span>
                    <InfoTooltip text="Unique output handle identifier shown on the canvas node. Cannot be named else." />
                  </label>
                  <input
                    id={`splitRuleName-${index}`}
                    type="text"
                    data-index={index}
                    data-field="name"
                    aria-label={`Rule ${index + 1} name`}
                    aria-invalid={Boolean(nameError)}
                    value={rule.name}
                    onChange={handleRuleFieldChange}
                    placeholder={`rule_${index + 1}`}
                    className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                      nameError
                        ? 'border-red-500/80 focus:border-red-500'
                        : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                    }`}
                  />
                  {nameError && (
                    <span className="text-xs text-red-400" role="alert">
                      {nameError}
                    </span>
                  )}
                </div>

                <div className="flex flex-col gap-1">
                  <label
                    htmlFor={`splitRuleCond-${index}`}
                    className="flex items-center gap-1 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
                  >
                    <span>SQL Condition</span>
                    <span className="text-red-400 font-bold" title="Required">*</span>
                    <InfoTooltip text="Catalyst SQL boolean expression passed to F.expr(), such as amount >= 1000 AND status = 'PAID'." />
                  </label>
                  <input
                    id={`splitRuleCond-${index}`}
                    type="text"
                    data-index={index}
                    data-field="condition"
                    aria-label={`Rule ${index + 1} condition`}
                    aria-invalid={Boolean(conditionError)}
                    value={rule.condition}
                    onChange={handleRuleFieldChange}
                    placeholder="amount >= 1000"
                    className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-sm font-mono focus:outline-none ${
                      conditionError
                        ? 'border-red-500/80 focus:border-red-500'
                        : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
                    }`}
                  />
                  {conditionError && (
                    <span className="text-xs text-red-400" role="alert">
                      {conditionError}
                    </span>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        {/* 3. Read-Only Else Fallback Handle Preview Card */}
        <div
          data-testid="conditional-split-else-preview"
          className="flex flex-col gap-1.5 p-2.5 rounded border border-amber-500/40 bg-amber-500/10"
        >
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <span className="text-xs font-bold uppercase text-amber-300">
                Fallback Handle (Read-Only)
              </span>
              <span className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-amber-500/20 border border-amber-500/40 text-amber-200">
                else
              </span>
            </div>
            <InfoTooltip text="Automatically appended by getConditionalSplitHandles. Receives all rows where every rule predicate above evaluates to FALSE or NULL." />
          </div>
          <p className="text-xs text-[var(--vscode-text-secondary)] leading-relaxed">
            Spark automatically exposes the amber <code className="font-mono text-amber-300">else</code> output
            handle for unmatched and null-predicate rows using null-safe{' '}
            <code className="font-mono text-[var(--vscode-text-primary)]">F.coalesce(cond, F.lit(False))</code>.
          </p>
        </div>
      </div>

      <SystemInstructionsField
        id="conditionalSplitSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this ConditionalSplit step."
        placeholder="Enter custom instructions for ConditionalSplit code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
