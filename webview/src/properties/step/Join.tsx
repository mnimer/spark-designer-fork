/**
 * @fileoverview Property configuration panel for the Apache Spark Join step (Join).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { GitMerge, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Canonical PySpark DataFrame join types supported by `Join` and `dagValidator`.
 */
export type JoinType =
  | 'inner'
  | 'left_outer'
  | 'right_outer'
  | 'full_outer'
  | 'left_semi'
  | 'left_anti'
  | 'cross';

/**
 * Join predicate authoring mode (`keys` for equi-join column pairs, `expression` for SQL/Column predicates).
 */
export type JoinConditionMode = 'keys' | 'expression';

/**
 * Equi-join column pair mapping a `left` DataFrame column to a `right` DataFrame column.
 */
export interface JoinKeyPair {
  leftColumn: string;
  rightColumn: string;
}

/**
 * Persisted properties for the Spark `Join` step node.
 */
export interface JoinStepProperties {
  joinType?: JoinType;
  how?: string;
  broadcastRight?: boolean;
  conditionMode?: JoinConditionMode;
  joinKeys?: JoinKeyPair[];
  joinExpression?: string;
  streamTimeConstraint?: string;
  dropDuplicateRightKeys?: boolean;
  rightColumnPrefix?: string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link JoinProperties}.
 */
export interface JoinNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: JoinStepProperties;
  };
}

/**
 * Props for the {@link JoinProperties} component.
 */
export interface JoinPropertiesProps {
  node: JoinNode;
  onChange: (properties: JoinStepProperties) => void;
}

const JOIN_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'inner', label: 'inner (Matching rows from both inputs)' },
  { value: 'left_outer', label: 'left_outer (All left rows + matched right)' },
  { value: 'right_outer', label: 'right_outer (All right rows + matched left)' },
  { value: 'full_outer', label: 'full_outer (All rows from both inputs)' },
  { value: 'left_semi', label: 'left_semi (Filter left rows with right match)' },
  { value: 'left_anti', label: 'left_anti (Filter left rows without right match)' },
  { value: 'cross', label: 'cross (Cartesian product)' },
];

const CONDITION_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'keys', label: 'Equi-Join Column Keys (left == right)' },
  { value: 'expression', label: 'Custom SQL / Column Expression' },
];

const VALID_JOIN_TYPES = new Set<string>(JOIN_TYPE_OPTIONS.map((opt) => opt.value));

const DEFAULT_JOIN_KEYS: JoinKeyPair[] = [{ leftColumn: '', rightColumn: '' }];

/**
 * Normalizes a raw `joinType` or legacy `how` string into a canonical {@link JoinType}.
 *
 * @param raw Raw `joinType` or `how` property value.
 * @returns Canonical Spark join type string.
 */
export function normalizeJoinTypeValue(raw: unknown): JoinType {
  if (typeof raw !== 'string' || raw.trim().length === 0) {
    return 'inner';
  }
  const cleaned = raw
    .trim()
    .replace(/([a-z])([A-Z])/g, '$1_$2')
    .toLowerCase()
    .replace(/[\s-]+/g, '_');

  if (cleaned === 'left' || cleaned === 'leftouter') {
    return 'left_outer';
  }
  if (cleaned === 'right' || cleaned === 'rightouter') {
    return 'right_outer';
  }
  if (cleaned === 'full' || cleaned === 'outer' || cleaned === 'fullouter') {
    return 'full_outer';
  }
  if (cleaned === 'semi' || cleaned === 'leftsemi') {
    return 'left_semi';
  }
  if (cleaned === 'anti' || cleaned === 'leftanti') {
    return 'left_anti';
  }
  if (VALID_JOIN_TYPES.has(cleaned)) {
    return cleaned as JoinType;
  }
  return 'inner';
}

/**
 * Normalizes raw `joinKeys` configuration into typed {@link JoinKeyPair} rows.
 *
 * @param raw Raw `joinKeys` value from YAML or UI state.
 * @returns Non-empty array of join key descriptors.
 */
export function normalizeJoinKeys(raw: unknown): JoinKeyPair[] {
  if (!Array.isArray(raw) || raw.length === 0) {
    return DEFAULT_JOIN_KEYS.map((item) => ({ ...item }));
  }
  return raw.map((entry) => {
    const obj =
      typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : {};
    return {
      leftColumn: typeof obj.leftColumn === 'string' ? obj.leftColumn : '',
      rightColumn: typeof obj.rightColumn === 'string' ? obj.rightColumn : '',
    };
  });
}

/**
 * Renders the property configuration panel for the Apache Spark Join step.
 *
 * @param props Selected Join node and property update callback.
 * @returns The rendered Join configuration form.
 */
export const JoinProperties: React.FC<JoinPropertiesProps> = ({ node, onChange }) => {
  const properties: JoinStepProperties = node?.data?.properties || {};
  const joinType = normalizeJoinTypeValue(properties.joinType ?? properties.how);
  const broadcastRight = properties.broadcastRight === true;
  const conditionMode: JoinConditionMode =
    properties.conditionMode === 'expression' ? 'expression' : 'keys';
  const joinKeys = normalizeJoinKeys(properties.joinKeys);
  const joinExpression =
    typeof properties.joinExpression === 'string' ? properties.joinExpression : '';
  const streamTimeConstraint =
    typeof properties.streamTimeConstraint === 'string' ? properties.streamTimeConstraint : '';
  const dropDuplicateRightKeys =
    typeof properties.dropDuplicateRightKeys === 'boolean'
      ? properties.dropDuplicateRightKeys
      : true;
  const rightColumnPrefix =
    typeof properties.rightColumnPrefix === 'string' ? properties.rightColumnPrefix : '';
  const systemInstructions = resolveSystemInstructions(node);

  const isCrossJoin = joinType === 'cross';
  const isSemiOrAnti = joinType === 'left_semi' || joinType === 'left_anti';
  const isExpressionInvalid =
    !isCrossJoin && conditionMode === 'expression' && joinExpression.trim().length === 0;
  const hasEmptyJoinKey =
    !isCrossJoin &&
    conditionMode === 'keys' &&
    joinKeys.some((pair) => pair.leftColumn.trim().length === 0 || pair.rightColumn.trim().length === 0);

  const emitPatch = (patch: Partial<JoinStepProperties>) => {
    onChange({
      ...properties,
      joinType,
      broadcastRight,
      conditionMode,
      joinKeys,
      dropDuplicateRightKeys,
      ...patch,
    });
  };

  const handleJoinTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitPatch({ joinType: e.target.value as JoinType });
  };

  const handleBroadcastChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    emitPatch({ broadcastRight: e.target.checked });
  };

  const handleConditionModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    emitPatch({ conditionMode: e.target.value as JoinConditionMode });
  };

  const handleAddJoinKey = () => {
    emitPatch({
      joinKeys: [...joinKeys, { leftColumn: '', rightColumn: '' }],
    });
  };

  const handleRemoveJoinKey = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx) || joinKeys.length <= 1) {
      return;
    }
    emitPatch({
      joinKeys: joinKeys.filter((_, i) => i !== idx),
    });
  };

  const handleJoinKeyFieldChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    const field = e.currentTarget.dataset.field as keyof JoinKeyPair | undefined;
    if (Number.isNaN(idx) || !field || !joinKeys[idx]) {
      return;
    }
    const nextKeys = joinKeys.map((pair, i) =>
      i === idx ? { ...pair, [field]: e.target.value } : pair
    );
    emitPatch({ joinKeys: nextKeys });
  };

  const handleTextFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    emitPatch({ [name]: value });
  };

  const handleDropDuplicateRightKeysChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    emitPatch({ dropDuplicateRightKeys: e.target.checked });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Dual-handle wiring banner */}
      <div
        data-testid="join-handle-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <GitMerge className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Left & Right Inputs</span>
          <InfoTooltip text="Connect one upstream branch to the top L (left) handle and one upstream branch to the bottom R (right) handle on the canvas." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          Connect upstream DataFrames to the <code className="font-mono text-blue-300">L</code>{' '}
          (left) and <code className="font-mono text-indigo-300">R</code> (right) target handles.
          Spark adapts execution automatically across batch, stream-static, and stream-stream joins.
        </p>
      </div>

      {/* 1. Join Type & Broadcast Hint */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <SelectField
          id="joinTypeSelect"
          name="joinType"
          label="Join Type"
          tooltip="PySpark DataFrame.join(how=...) strategy. Stream-static joins forbid full_outer, and stream-stream outer joins require watermarks on both sides."
          value={joinType}
          options={JOIN_TYPE_OPTIONS}
          onChange={handleJoinTypeChange}
        />

        <label
          htmlFor="joinBroadcastRight"
          className="flex items-start gap-2 text-xs text-[var(--vscode-text-primary)] cursor-pointer select-none"
        >
          <input
            id="joinBroadcastRight"
            name="broadcastRight"
            type="checkbox"
            checked={broadcastRight}
            onChange={handleBroadcastChange}
            className="mt-0.5 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] cursor-pointer"
          />
          <span className="flex items-center flex-wrap gap-1">
            <span className="font-bold">Broadcast Right Input (`F.broadcast(right_df)`)</span>
            <InfoTooltip text="Wraps the right static lookup DataFrame in F.broadcast(right_df) to execute a BroadcastHashJoin without shuffling the left DataFrame. Displays the BROADCAST badge on the canvas node." />
          </span>
        </label>
      </div>

      {/* 2. Join Condition Section */}
      {!isCrossJoin && (
        <div
          data-testid="join-condition-section"
          className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
        >
          <SelectField
            id="joinConditionMode"
            name="conditionMode"
            label="Join Condition Mode"
            tooltip="Choose Equi-Join Column Keys for equality predicates across one or more key pairs, or Custom SQL / Column Expression for range and non-equi predicates."
            value={conditionMode}
            options={CONDITION_MODE_OPTIONS}
            onChange={handleConditionModeChange}
          />

          {conditionMode === 'keys' ? (
            <div className="flex flex-col gap-2">
              <div className="flex items-center justify-between gap-2">
                <div className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
                  <span>Join Key Pairs</span>
                  <InfoTooltip text="Equality key mappings matching l.<leftColumn> == r.<rightColumn>." />
                </div>
                <button
                  type="button"
                  onClick={handleAddJoinKey}
                  className="flex items-center gap-1 px-2 py-1 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer"
                >
                  <Plus className="w-3.5 h-3.5" />
                  <span>Add Key</span>
                </button>
              </div>

              <div className="flex flex-col gap-2" data-testid="join-keys-list">
                {joinKeys.map((pair, index) => (
                  <div
                    key={index}
                    data-testid={`join-key-row-${index}`}
                    className="grid grid-cols-[1fr_1fr_auto] items-end gap-2 p-2 rounded border border-[var(--vscode-border-light)] bg-[var(--vscode-card-bg)]"
                  >
                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`joinLeftCol-${index}`}
                        className="text-[11px] font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        Left Column
                      </label>
                      <input
                        id={`joinLeftCol-${index}`}
                        type="text"
                        data-index={index}
                        data-field="leftColumn"
                        aria-label={`Join key ${index + 1} left column`}
                        value={pair.leftColumn}
                        onChange={handleJoinKeyFieldChange}
                        placeholder="customer_id"
                        className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                      />
                    </div>
                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor={`joinRightCol-${index}`}
                        className="text-[11px] font-bold uppercase text-[var(--vscode-text-muted)]"
                      >
                        Right Column
                      </label>
                      <input
                        id={`joinRightCol-${index}`}
                        type="text"
                        data-index={index}
                        data-field="rightColumn"
                        aria-label={`Join key ${index + 1} right column`}
                        value={pair.rightColumn}
                        onChange={handleJoinKeyFieldChange}
                        placeholder="customer_id"
                        className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                      />
                    </div>
                    <button
                      type="button"
                      data-index={index}
                      disabled={joinKeys.length <= 1}
                      aria-label={`Remove join key ${index + 1}`}
                      onClick={handleRemoveJoinKey}
                      className="p-2 rounded border border-[var(--vscode-border)] text-[var(--vscode-text-muted)] hover:text-rose-400 hover:bg-[var(--vscode-hover)] disabled:opacity-40 disabled:cursor-not-allowed transition-colors cursor-pointer"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                ))}
              </div>

              {hasEmptyJoinKey && (
                <span className="text-xs text-amber-400" role="status">
                  Provide both Left Column and Right Column names for each join key pair.
                </span>
              )}
            </div>
          ) : (
            <PropertyField
              id="joinExpressionInput"
              name="joinExpression"
              label="Join Condition Expression"
              required="Required"
              type="textarea"
              rows={2}
              tooltip="SQL predicate passed to F.expr(...) using aliases l and r for left_df and right_df."
              value={joinExpression}
              onChange={handleTextFieldChange}
              placeholder="l.account_id = r.account_id AND l.event_date >= r.effective_date"
              invalid={isExpressionInvalid}
              error={isExpressionInvalid ? 'Join condition expression is required.' : null}
            />
          )}
        </div>
      )}

      {/* 3. Stream-Stream Event-Time Constraint & Column Disambiguation */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        {!isCrossJoin && (
          <PropertyField
            id="joinStreamTimeConstraint"
            name="streamTimeConstraint"
            label="Stream-Stream Time Constraint (Optional)"
            tooltip="Event-time range predicate referencing aliases l and r. Mandatory for stream-stream outer joins alongside upstream Watermark steps so Spark can evict expired state."
            value={streamTimeConstraint}
            onChange={handleTextFieldChange}
            placeholder="r.event_ts >= l.event_ts AND r.event_ts <= l.event_ts + INTERVAL 10 MINUTES"
          />
        )}

        {!isSemiOrAnti && (
          <>
            <label
              htmlFor="joinDropDuplicateRightKeys"
              className="flex items-start gap-2 text-xs text-[var(--vscode-text-primary)] cursor-pointer select-none"
            >
              <input
                id="joinDropDuplicateRightKeys"
                name="dropDuplicateRightKeys"
                type="checkbox"
                checked={dropDuplicateRightKeys}
                onChange={handleDropDuplicateRightKeysChange}
                className="mt-0.5 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] cursor-pointer"
              />
              <span className="flex items-center flex-wrap gap-1">
                <span className="font-bold">Drop Duplicate Right Join Keys</span>
                <InfoTooltip text="Prevents duplicate join key columns in the output DataFrame so downstream sinks and joins never fail with AMBIGUOUS_REFERENCE." />
              </span>
            </label>

            <PropertyField
              id="joinRightColumnPrefix"
              name="rightColumnPrefix"
              label="Right Column Disambiguation Prefix (Optional)"
              tooltip="Optional prefix applied via right_df.withColumnsRenamed(...) to colliding non-key column names from the right DataFrame."
              value={rightColumnPrefix}
              onChange={handleTextFieldChange}
              placeholder="right_"
            />
          </>
        )}
      </div>

      <SystemInstructionsField
        id="joinSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this Join step."
        placeholder="Enter custom instructions for Join code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
