/**
 * @fileoverview Property configuration panel for the Apache Spark Watermark step (df.withWatermark).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { Clock } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Standard Spark delay threshold presets for event-time watermarking.
 */
export const WATERMARK_DELAY_PRESETS = [
  '5 seconds',
  '30 seconds',
  '1 minute',
  '5 minutes',
  '10 minutes',
  '1 hour',
] as const;

/**
 * Persisted properties for the Spark Watermark step node.
 */
export interface WatermarkStepProperties {
  eventTimeColumn?: string;
  delayThreshold?: string;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link WatermarkProperties}.
 */
export interface WatermarkNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: WatermarkStepProperties;
  };
}

/**
 * Props for the {@link WatermarkProperties} component.
 */
export interface WatermarkPropertiesProps {
  node: WatermarkNode;
  onChange: (properties: WatermarkStepProperties) => void;
}

/**
 * Builds a concise code generation instruction describing the configured Spark watermark.
 *
 * @param eventTimeColumn Timestamp column name passed to withWatermark.
 * @param delayThreshold Late-data delay threshold interval string.
 * @returns Plain-English instruction string for PySpark code generation.
 */
export function buildWatermarkSystemInstruction(
  eventTimeColumn: string,
  delayThreshold: string
): string {
  const col = eventTimeColumn.trim() || 'event_timestamp';
  const threshold = delayThreshold.trim() || '10 minutes';
  return `Apply df.withWatermark('${col}', '${threshold}') after payload parsing to bound streaming state.`;
}

/**
 * Renders the property configuration panel for the Apache Spark Watermark step.
 *
 * @param props Selected Watermark node and property update callback.
 * @returns The rendered Watermark configuration form.
 */
export const WatermarkProperties: React.FC<WatermarkPropertiesProps> = ({ node, onChange }) => {
  const properties: WatermarkStepProperties = node?.data?.properties || {};
  const eventTimeColumn =
    typeof properties.eventTimeColumn === 'string' ? properties.eventTimeColumn : '';
  const delayThreshold =
    typeof properties.delayThreshold === 'string' ? properties.delayThreshold : '10 minutes';

  const isEventTimeInvalid = eventTimeColumn.trim().length === 0;
  const isDelayInvalid = delayThreshold.trim().length === 0;
  const systemInstructions = resolveSystemInstructions(node);

  const handleEventTimeColumnChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const nextColumn = e.target.value;
    onChange({
      ...properties,
      eventTimeColumn: nextColumn,
      delayThreshold,
      system_instructions: buildWatermarkSystemInstruction(nextColumn, delayThreshold),
    });
  };

  const handleDelayThresholdChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const nextDelay = e.target.value;
    onChange({
      ...properties,
      eventTimeColumn,
      delayThreshold: nextDelay,
      system_instructions: buildWatermarkSystemInstruction(eventTimeColumn, nextDelay),
    });
  };

  const handlePresetClick = (e: React.MouseEvent<HTMLButtonElement>) => {
    const preset = e.currentTarget.dataset.preset;
    if (!preset) {
      return;
    }
    onChange({
      ...properties,
      eventTimeColumn,
      delayThreshold: preset,
      system_instructions: buildWatermarkSystemInstruction(eventTimeColumn, preset),
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
      <div
        data-testid="watermark-placement-banner"
        className="flex flex-col gap-2 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)] text-xs text-[var(--vscode-text-secondary)]"
      >
        <div className="flex items-center gap-2 font-bold uppercase text-[var(--vscode-text-primary)]">
          <Clock className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Event-Time Placement</span>
          <InfoTooltip text="If the source payload is raw JSON, CSV, or Avro bytes, place ParsePayload before Watermark so the event-time column has TimestampType." />
        </div>
        <p className="text-[var(--vscode-text-muted)] leading-relaxed">
          Place Watermark after payload parsing so the event-time column has TimestampType. Spark
          calls <code className="font-mono text-[var(--vscode-text-primary)]">df.withWatermark()</code> to
          bound state for windowed aggregations, deduplication, and stream-stream joins.
        </p>
      </div>

      <PropertyField
        id="watermarkEventTimeColumn"
        name="eventTimeColumn"
        label="Event Time Column"
        required="Required"
        tooltip="Name of the TimestampType column used for event-time tracking in df.withWatermark(eventTimeCol, delayThreshold)."
        value={eventTimeColumn}
        onChange={handleEventTimeColumnChange}
        placeholder="event_timestamp"
        invalid={isEventTimeInvalid}
        error={isEventTimeInvalid ? 'Event time column is required.' : null}
      />

      <div className="flex flex-col gap-2">
        <PropertyField
          id="watermarkDelayThreshold"
          name="delayThreshold"
          label="Delay Threshold"
          required="Required"
          tooltip="Maximum late-arrival threshold interval string passed to df.withWatermark(), such as 10 minutes, 5 seconds, or 1 hour."
          value={delayThreshold}
          onChange={handleDelayThresholdChange}
          placeholder="10 minutes"
          invalid={isDelayInvalid}
          error={isDelayInvalid ? 'Delay threshold is required.' : null}
          helperText="Select a preset interval below or enter a custom Spark calendar interval."
        />

        <div className="flex flex-wrap gap-2" role="group" aria-label="Watermark interval presets">
          {WATERMARK_DELAY_PRESETS.map((preset) => {
            const isSelected = delayThreshold.trim().toLowerCase() === preset.toLowerCase();
            return (
              <button
                key={preset}
                type="button"
                data-preset={preset}
                aria-pressed={isSelected}
                onClick={handlePresetClick}
                className={`px-2 py-1 rounded text-xs font-mono border transition-colors cursor-pointer ${
                  isSelected
                    ? 'bg-blue-500/20 border-blue-400 text-blue-300 font-bold'
                    : 'bg-[var(--vscode-subcard-bg)] border-[var(--vscode-border)] text-[var(--vscode-text-secondary)] hover:bg-[var(--vscode-hover)]'
                }`}
              >
                {preset}
              </button>
            );
          })}
        </div>
      </div>

      <SystemInstructionsField
        id="watermarkSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this Watermark step."
        placeholder="Enter custom instructions for Watermark code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
