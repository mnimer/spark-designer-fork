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
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
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
 * Input representation of the event-time column before `df.withWatermark(...)` runs.
 */
export type WatermarkSourceTimestampType =
  | 'timestamp'
  | 'iso8601'
  | 'epoch_seconds'
  | 'epoch_millis'
  | 'custom_format';

/**
 * Persisted properties for the Spark Watermark step node.
 */
export interface WatermarkStepProperties {
  eventTimeColumn?: string;
  delayThreshold?: string;
  sourceTimestampType?: WatermarkSourceTimestampType;
  timestampFormat?: string;
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

const SOURCE_TIMESTAMP_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'timestamp', label: 'Already TimestampType (No Cast)' },
  { value: 'iso8601', label: 'ISO-8601 String (F.to_timestamp)' },
  { value: 'epoch_seconds', label: 'Unix Epoch Seconds (F.timestamp_seconds)' },
  { value: 'epoch_millis', label: 'Unix Epoch Milliseconds (F.timestamp_millis)' },
  { value: 'custom_format', label: 'Custom Datetime Pattern (F.to_timestamp)' },
];

const VALID_SOURCE_TS_TYPES = new Set<string>(
  SOURCE_TIMESTAMP_TYPE_OPTIONS.map((opt) => opt.value)
);

/**
 * Builds a concise code generation instruction describing the configured Spark watermark.
 *
 * @param eventTimeColumn Timestamp column name passed to withWatermark.
 * @param delayThreshold Late-data delay threshold interval string.
 * @param sourceTimestampType Optional input representation requiring pre-watermark casting.
 * @param timestampFormat Optional datetime pattern when sourceTimestampType is custom_format.
 * @returns Plain-English instruction string for PySpark code generation.
 */
export function buildWatermarkSystemInstruction(
  eventTimeColumn: string,
  delayThreshold: string,
  sourceTimestampType: WatermarkSourceTimestampType = 'timestamp',
  timestampFormat = ''
): string {
  const col = eventTimeColumn.trim() || 'event_timestamp';
  const threshold = delayThreshold.trim() || '10 minutes';

  if (sourceTimestampType === 'iso8601') {
    return `Cast '${col}' from ISO-8601 string to TimestampType with F.to_timestamp(F.col('${col}')) and apply df.withWatermark('${col}', '${threshold}') to bound streaming state.`;
  }
  if (sourceTimestampType === 'epoch_seconds') {
    return `Convert '${col}' from Unix epoch seconds to TimestampType with F.timestamp_seconds(F.col('${col}')) and apply df.withWatermark('${col}', '${threshold}') to bound streaming state.`;
  }
  if (sourceTimestampType === 'epoch_millis') {
    return `Convert '${col}' from Unix epoch milliseconds to TimestampType with F.timestamp_millis(F.col('${col}').cast('bigint')) and apply df.withWatermark('${col}', '${threshold}') to bound streaming state.`;
  }
  if (sourceTimestampType === 'custom_format') {
    const pattern = timestampFormat.trim() || 'yyyy-MM-dd HH:mm:ss';
    return `Parse '${col}' to TimestampType with F.to_timestamp(F.col('${col}'), '${pattern}') and apply df.withWatermark('${col}', '${threshold}') to bound streaming state.`;
  }

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
  const sourceTimestampType: WatermarkSourceTimestampType =
    typeof properties.sourceTimestampType === 'string' &&
    VALID_SOURCE_TS_TYPES.has(properties.sourceTimestampType)
      ? (properties.sourceTimestampType as WatermarkSourceTimestampType)
      : 'timestamp';
  const timestampFormat =
    typeof properties.timestampFormat === 'string' ? properties.timestampFormat : '';

  const isEventTimeInvalid = eventTimeColumn.trim().length === 0;
  const isDelayInvalid = delayThreshold.trim().length === 0;
  const isTimestampFormatInvalid =
    sourceTimestampType === 'custom_format' && timestampFormat.trim().length === 0;
  const systemInstructions = resolveSystemInstructions(node);

  const emitWatermarkChange = (
    nextCol: string,
    nextDelay: string,
    nextSourceType: WatermarkSourceTimestampType,
    nextFormat: string
  ) => {
    const nextProps: WatermarkStepProperties = {
      ...properties,
      eventTimeColumn: nextCol,
      delayThreshold: nextDelay,
      sourceTimestampType: nextSourceType,
      system_instructions: buildWatermarkSystemInstruction(
        nextCol,
        nextDelay,
        nextSourceType,
        nextFormat
      ),
    };
    if (nextSourceType === 'custom_format' || nextFormat.length > 0) {
      nextProps.timestampFormat = nextFormat;
    }
    onChange(nextProps);
  };

  const handleEventTimeColumnChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    emitWatermarkChange(e.target.value, delayThreshold, sourceTimestampType, timestampFormat);
  };

  const handleDelayThresholdChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    emitWatermarkChange(eventTimeColumn, e.target.value, sourceTimestampType, timestampFormat);
  };

  const handlePresetClick = (e: React.MouseEvent<HTMLButtonElement>) => {
    const preset = e.currentTarget.dataset.preset;
    if (!preset) {
      return;
    }
    emitWatermarkChange(eventTimeColumn, preset, sourceTimestampType, timestampFormat);
  };

  const handleSourceTimestampTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextType = e.target.value as WatermarkSourceTimestampType;
    const nextFormat =
      nextType === 'custom_format' && timestampFormat.trim().length === 0
        ? 'yyyy-MM-dd HH:mm:ss'
        : timestampFormat;
    emitWatermarkChange(eventTimeColumn, delayThreshold, nextType, nextFormat);
  };

  const handleTimestampFormatChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    emitWatermarkChange(eventTimeColumn, delayThreshold, sourceTimestampType, e.target.value);
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
          <InfoTooltip text="If the source payload is raw JSON, CSV, or Avro bytes, place ParsePayload before Watermark so the event-time field is extracted as TimestampType." />
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

      <SelectField
        id="watermarkSourceTimestampType"
        name="sourceTimestampType"
        label="Input Timestamp Representation"
        tooltip="Configure optional pre-watermark casting to TimestampType when the upstream field arrives as an ISO-8601 string, Unix epoch integer, or custom datetime string."
        value={sourceTimestampType}
        options={SOURCE_TIMESTAMP_TYPE_OPTIONS}
        onChange={handleSourceTimestampTypeChange}
      />

      {sourceTimestampType === 'custom_format' && (
        <PropertyField
          id="watermarkTimestampFormat"
          name="timestampFormat"
          label="Datetime Pattern"
          required="Required"
          tooltip="Spark DateTimeFormatter pattern passed to F.to_timestamp(col, pattern) before calling df.withWatermark()."
          value={timestampFormat}
          onChange={handleTimestampFormatChange}
          placeholder="yyyy-MM-dd HH:mm:ss.SSS"
          invalid={isTimestampFormatInvalid}
          error={isTimestampFormatInvalid ? 'Datetime pattern is required for custom format.' : null}
        />
      )}

      <div className="flex flex-col gap-2">
        <PropertyField
          id="watermarkDelayThreshold"
          name="delayThreshold"
          label="Delay Threshold"
          required="Required"
          tooltip="Maximum late-arrival interval string passed to df.withWatermark(), such as 10 minutes, 5 seconds, or 1 hour."
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
