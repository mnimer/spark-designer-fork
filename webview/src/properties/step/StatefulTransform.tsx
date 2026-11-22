/**
 * @fileoverview Property configuration panel for the Apache Spark Stateful Transform step (StatefulTransform).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React, { useState } from 'react';
import { Cpu, Database, Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * PySpark stateful streaming API strategy for `StatefulTransform`.
 */
export type StatefulApiMode = 'transformWithState' | 'applyInPandasWithState';

/**
 * Spark 4.0 `StatefulProcessorHandle` state primitive type.
 */
export type StatefulPrimitiveType = 'ValueState' | 'ListState' | 'MapState';

/**
 * Timer and watermark clock mode passed to `transformWithState` or `GroupStateTimeout`.
 */
export type StatefulTimeMode = 'EventTime' | 'ProcessingTime' | 'None';

/**
 * Output emission mode supported by arbitrary stateful streaming operators.
 */
export type StatefulOutputMode = 'Append' | 'Update';

/**
 * Persisted properties for the Spark `StatefulTransform` step node.
 */
export interface StatefulTransformStepProperties {
  apiMode?: StatefulApiMode;
  keyColumns?: string[];
  statePrimitive?: StatefulPrimitiveType;
  timeMode?: StatefulTimeMode;
  outputMode?: StatefulOutputMode;
  ttlDuration?: string;
  stateSchemaDDL?: string;
  outputSchemaDDL?: string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link StatefulTransformProperties}.
 */
export interface StatefulTransformNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: StatefulTransformStepProperties;
  };
}

/**
 * Props for the {@link StatefulTransformProperties} component.
 */
export interface StatefulTransformPropertiesProps {
  node: StatefulTransformNode;
  onChange: (properties: StatefulTransformStepProperties) => void;
}

const API_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'transformWithState', label: 'transformWithState (Spark 4.0 StatefulProcessor)' },
  { value: 'applyInPandasWithState', label: 'applyInPandasWithState (Pandas GroupState)' },
];

const STATE_PRIMITIVE_OPTIONS: SelectFieldOption[] = [
  { value: 'ValueState', label: 'ValueState (Single Struct per Key)' },
  { value: 'ListState', label: 'ListState (Appendable List per Key)' },
  { value: 'MapState', label: 'MapState (Key-Value Map per Key)' },
];

const TIME_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'None', label: 'None (No Timers)' },
  { value: 'EventTime', label: 'EventTime (Requires Upstream Watermark)' },
  { value: 'ProcessingTime', label: 'ProcessingTime (Wall-Clock Timers & TTL)' },
];

const OUTPUT_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'Update', label: 'Update' },
  { value: 'Append', label: 'Append' },
];

const VALID_PRIMITIVES = new Set<string>(STATE_PRIMITIVE_OPTIONS.map((opt) => opt.value));
const VALID_TIME_MODES = new Set<string>(TIME_MODE_OPTIONS.map((opt) => opt.value));

/**
 * Normalizes raw grouping key columns into a trimmed string array.
 *
 * @param raw Raw `keyColumns` property value from YAML or UI input.
 * @returns Clean list of grouping key column names.
 */
function normalizeKeyColumns(raw: unknown): string[] {
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
 * Renders the property configuration panel for the Apache Spark Stateful Transform step.
 *
 * @param props Selected StatefulTransform node and property update callback.
 * @returns The rendered StatefulTransform configuration form.
 */
export const StatefulTransformProperties: React.FC<StatefulTransformPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: StatefulTransformStepProperties = node?.data?.properties || {};
  const apiMode: StatefulApiMode =
    properties.apiMode === 'applyInPandasWithState'
      ? 'applyInPandasWithState'
      : 'transformWithState';
  const keyColumns = normalizeKeyColumns(properties.keyColumns);
  const statePrimitive: StatefulPrimitiveType =
    typeof properties.statePrimitive === 'string' && VALID_PRIMITIVES.has(properties.statePrimitive)
      ? (properties.statePrimitive as StatefulPrimitiveType)
      : 'ValueState';
  const timeMode: StatefulTimeMode =
    typeof properties.timeMode === 'string' && VALID_TIME_MODES.has(properties.timeMode)
      ? (properties.timeMode as StatefulTimeMode)
      : 'None';
  const outputMode: StatefulOutputMode = properties.outputMode === 'Append' ? 'Append' : 'Update';
  const ttlDuration = typeof properties.ttlDuration === 'string' ? properties.ttlDuration : '';
  const stateSchemaDDL =
    typeof properties.stateSchemaDDL === 'string' ? properties.stateSchemaDDL : '';
  const outputSchemaDDL =
    typeof properties.outputSchemaDDL === 'string' ? properties.outputSchemaDDL : '';
  const systemInstructions = resolveSystemInstructions(node);

  const [pendingKeyInput, setPendingKeyInput] = useState<string>('');

  const isKeyColumnsEmpty = keyColumns.length === 0;
  const isStateSchemaInvalid = stateSchemaDDL.trim().length === 0;
  const isOutputSchemaInvalid = outputSchemaDDL.trim().length === 0;

  const emitUpdate = (patch: Partial<StatefulTransformStepProperties>) => {
    onChange({
      ...properties,
      apiMode,
      keyColumns,
      statePrimitive,
      timeMode,
      outputMode,
      ttlDuration,
      stateSchemaDDL,
      outputSchemaDDL,
      ...patch,
    });
  };

  const appendPendingKeys = () => {
    const parsed = normalizeKeyColumns(pendingKeyInput);
    if (parsed.length === 0) {
      return;
    }
    const existing = new Set(keyColumns);
    const merged = [...keyColumns];
    for (const col of parsed) {
      if (!existing.has(col)) {
        existing.add(col);
        merged.push(col);
      }
    }
    setPendingKeyInput('');
    emitUpdate({ keyColumns: merged });
  };

  const handleKeyInputKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      appendPendingKeys();
    }
  };

  const handleExistingKeyChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    const updated = [...keyColumns];
    updated[idx] = e.target.value;
    emitUpdate({ keyColumns: normalizeKeyColumns(updated) });
  };

  const handleRemoveKey = (e: React.MouseEvent<HTMLButtonElement>) => {
    const idx = Number(e.currentTarget.dataset.index);
    if (Number.isNaN(idx)) {
      return;
    }
    emitUpdate({ keyColumns: keyColumns.filter((_, i) => i !== idx) });
  };

  const handleSelectChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const { name, value } = e.target;
    emitUpdate({ [name]: value } as Partial<StatefulTransformStepProperties>);
  };

  const handleTextFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    emitUpdate({ [name]: value });
  };

  const handleSystemInstructionsChange = (value: string) => {
    onChange({
      ...properties,
      system_instructions: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* RocksDB & DLQ Architecture Guidance Banner */}
      <div
        data-testid="stateful-rocksdb-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <Database className="w-4 h-4 text-blue-400 shrink-0" />
          <span>RocksDB State Store & DLQ Routing</span>
          <InfoTooltip text="Spark 4.0 transformWithState requires RocksDBStateStoreProvider. Connecting the error handle captures per-key exceptions into an _error column." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          {apiMode === 'transformWithState'
            ? 'Spark 4.0 StatefulProcessor persists per-key state in RocksDBStateStoreProvider. Connect the error output handle on the canvas to route failed state updates to a dead-letter branch.'
            : 'Pandas GroupState processes grouped micro-batch DataFrames via applyInPandasWithState. Connect the error output handle on the canvas to capture exceptions into a dead-letter branch.'}
        </p>
      </div>

      <SelectField
        id="statefulApiMode"
        name="apiMode"
        label="Stateful API Mode"
        tooltip="Choose Spark 4.0 transformWithState (StatefulProcessor with ValueState, ListState, or MapState) or applyInPandasWithState."
        value={apiMode}
        options={API_MODE_OPTIONS}
        onChange={handleSelectChange}
      />

      {/* Grouping Key Columns Section */}
      <div
        data-testid="stateful-key-columns-section"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <label
          htmlFor="statefulKeyAddInput"
          className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]"
        >
          <Cpu className="w-3.5 h-3.5 text-blue-400 shrink-0" />
          <span>Grouping Key Columns</span>
          <span className="text-red-400 font-bold" title="Required">
            *
          </span>
          <InfoTooltip text="Columns passed to df.groupBy(*keyColumns) to partition per-entity streaming state." />
        </label>

        {keyColumns.length > 0 && (
          <div className="flex flex-col gap-2">
            {keyColumns.map((col, index) => (
              <div key={`${index}-${col}`} className="flex items-center gap-2">
                <input
                  type="text"
                  data-index={index}
                  aria-label={`Key column ${index + 1}`}
                  value={col}
                  onChange={handleExistingKeyChange}
                  className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
                />
                <button
                  type="button"
                  data-index={index}
                  aria-label={`Remove key column ${col}`}
                  onClick={handleRemoveKey}
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
            id="statefulKeyAddInput"
            type="text"
            value={pendingKeyInput}
            onChange={(e) => setPendingKeyInput(e.target.value)}
            onKeyDown={handleKeyInputKeyDown}
            placeholder="e.g. user_id, device_id"
            aria-invalid={isKeyColumnsEmpty}
            className="flex-1 bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-sm font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
          />
          <button
            type="button"
            aria-label="Add Key Column"
            onClick={appendPendingKeys}
            className="flex items-center gap-1 px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs font-bold text-[var(--vscode-text-primary)] hover:bg-[var(--vscode-hover)] transition-colors cursor-pointer shrink-0"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Add</span>
          </button>
        </div>

        {isKeyColumnsEmpty && (
          <span className="text-xs text-red-400" role="alert">
            At least one grouping key column is required.
          </span>
        )}
      </div>

      {apiMode === 'transformWithState' && (
        <SelectField
          id="statefulStatePrimitive"
          name="statePrimitive"
          label="State Primitive"
          tooltip="Select the RocksDB-backed StatefulProcessorHandle state collection initialized inside StatefulProcessor.init()."
          value={statePrimitive}
          options={STATE_PRIMITIVE_OPTIONS}
          onChange={handleSelectChange}
        />
      )}

      <div className="grid grid-cols-2 gap-2">
        <SelectField
          id="statefulTimeMode"
          name="timeMode"
          label="Time Mode"
          tooltip="Timer clock semantics passed to transformWithState or applyInPandasWithState. EventTime requires an upstream Watermark step."
          value={timeMode}
          options={TIME_MODE_OPTIONS}
          onChange={handleSelectChange}
        />

        <SelectField
          id="statefulOutputMode"
          name="outputMode"
          label="Output Mode"
          tooltip="Streaming output mode passed to transformWithState or applyInPandasWithState (Update or Append)."
          value={outputMode}
          options={OUTPUT_MODE_OPTIONS}
          onChange={handleSelectChange}
        />
      </div>

      {timeMode === 'EventTime' && (
        <p
          data-testid="stateful-event-time-hint"
          className="text-xs text-[var(--vscode-text-muted)] leading-relaxed px-1"
        >
          EventTime mode requires an upstream Watermark step so Spark can advance event-time timers
          and evict expired state.
        </p>
      )}

      <PropertyField
        id="statefulTtlDuration"
        name="ttlDuration"
        label="State TTL Duration (Optional)"
        tooltip="Optional state time-to-live interval string (such as 24 hours or 30 minutes) used for automatic RocksDB state expiration."
        value={ttlDuration}
        onChange={handleTextFieldChange}
        placeholder="e.g. 24 hours"
      />

      <PropertyField
        id="statefulStateSchemaDDL"
        name="stateSchemaDDL"
        label="State Schema DDL"
        type="textarea"
        rows={2}
        required="Required"
        tooltip="Spark SQL DDL schema describing the persisted state struct (for example: event_count BIGINT, last_seen TIMESTAMP)."
        value={stateSchemaDDL}
        onChange={handleTextFieldChange}
        placeholder="event_count BIGINT, total_amount DOUBLE"
        invalid={isStateSchemaInvalid}
        error={isStateSchemaInvalid ? 'State schema DDL is required.' : null}
      />

      <PropertyField
        id="statefulOutputSchemaDDL"
        name="outputSchemaDDL"
        label="Output Schema DDL"
        type="textarea"
        rows={2}
        required="Required"
        tooltip="Spark SQL DDL schema passed as outputStructType describing rows emitted by the stateful processor."
        value={outputSchemaDDL}
        onChange={handleTextFieldChange}
        placeholder="user_id STRING, event_count BIGINT, total_amount DOUBLE"
        invalid={isOutputSchemaInvalid}
        error={isOutputSchemaInvalid ? 'Output schema DDL is required.' : null}
      />

      <SystemInstructionsField
        id="statefulTransformSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance describing per-key state transitions, timer logic, and alert conditions."
        placeholder="Describe per-key state update logic and timer behavior..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
