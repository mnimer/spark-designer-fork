/**
 * @fileoverview Shared Structured Streaming per-sink settings card for Sink property panels (A9).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import { Radio } from 'lucide-react';
import type { PropertyFormNode } from '../../catalog/propertyFormRegistry';
import { CollapsibleSection } from './CollapsibleSection';
import { PropertyField } from './PropertyField';
import { SelectField, type SelectFieldOption } from './SelectField';
import type {
  PipelineParadigm,
  SparkSinkTriggerSelection,
  SparkStreamingOutputMode,
} from '../../utils/sharedTypes';

/**
 * Props for the {@link StreamingSinkSettingsField} component.
 */
export interface StreamingSinkSettingsFieldProps {
  /** Selected React Flow sink node. */
  node: PropertyFormNode | null | undefined;
  /** Callback invoked with the merged sink properties object. */
  onChange: (properties: Record<string, unknown>) => void;
  /** Active pipeline authoring paradigm ('imperative' default or 'sdp'). */
  paradigm?: PipelineParadigm;
}

const OUTPUT_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'append', label: 'Append (default)' },
  { value: 'complete', label: 'Complete' },
  { value: 'update', label: 'Update' },
];

const SINK_TRIGGER_OPTIONS: SelectFieldOption[] = [
  { value: 'inherit', label: 'Inherit Pipeline Default' },
  { value: 'processingTime', label: 'Processing Time (Interval)' },
  { value: 'availableNow', label: 'Available Now (Incremental Batch)' },
  { value: 'once', label: 'Once (Single Batch)' },
  { value: 'continuous', label: 'Continuous (Low Latency)' },
];

/**
 * Determines whether a node should display the per-sink Structured Streaming overrides section.
 *
 * @param node React Flow node inspected in PropertyForm.
 * @returns True when the node is a streaming sink or already defines streaming sink overrides.
 */
export function shouldShowStreamingSinkSettings(
  node?: PropertyFormNode | null,
  paradigm?: PipelineParadigm
): boolean {
  if (paradigm === 'sdp' || node?.data?.paradigm === 'sdp') {
    return false;
  }
  const nodeType = String(node?.data?.type || '').toUpperCase();
  if (nodeType !== 'SINK') {
    return false;
  }
  return (
    node?.data?.executionMode === 'streaming' ||
    node?.data?.isStreaming === true ||
    Boolean(
      node?.data?.properties?.outputMode ||
        node?.data?.properties?.checkpointLocation ||
        node?.data?.properties?.triggerType
    )
  );
}

/**
 * Renders per-sink Structured Streaming override controls (`outputMode`, `triggerType`,
 * `triggerInterval`, and `checkpointLocation`) inside Sink property panels.
 *
 * @param props Sink node and property update callback.
 * @returns Collapsible streaming sink configuration section, or null for batch or SDP sinks.
 */
export const StreamingSinkSettingsField: React.FC<StreamingSinkSettingsFieldProps> = ({
  node,
  onChange,
  paradigm,
}) => {
  const [isOpen, setIsOpen] = useState<boolean>(true);

  if (!shouldShowStreamingSinkSettings(node, paradigm)) {
    return null;
  }

  const properties = node?.data?.properties || {};
  const nodeId = typeof node?.id === 'string' && node.id.trim().length > 0 ? node.id : 'sink_id';

  const outputMode: SparkStreamingOutputMode =
    properties.outputMode === 'complete' || properties.outputMode === 'update'
      ? properties.outputMode
      : 'append';

  const triggerType: SparkSinkTriggerSelection =
    properties.triggerType === 'processingTime' ||
    properties.triggerType === 'availableNow' ||
    properties.triggerType === 'once' ||
    properties.triggerType === 'continuous'
      ? properties.triggerType
      : 'inherit';

  const showTriggerInterval = triggerType === 'processingTime' || triggerType === 'continuous';
  const triggerInterval =
    typeof properties.triggerInterval === 'string' ? properties.triggerInterval : '';
  const checkpointLocation =
    typeof properties.checkpointLocation === 'string' ? properties.checkpointLocation : '';

  const handleToggleOpen = () => {
    setIsOpen((prev) => !prev);
  };

  const handleOutputModeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    onChange({
      ...properties,
      outputMode: e.target.value as SparkStreamingOutputMode,
    });
  };

  const handleTriggerTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextTrigger = e.target.value as SparkSinkTriggerSelection;
    onChange({
      ...properties,
      triggerType: nextTrigger,
    });
  };

  const handleTriggerIntervalChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    onChange({
      ...properties,
      triggerInterval: e.target.value,
    });
  };

  const handleCheckpointLocationChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    onChange({
      ...properties,
      checkpointLocation: e.target.value,
    });
  };

  return (
    <CollapsibleSection
      id="streaming-sink-settings"
      title="Streaming Sink Settings"
      icon={<Radio className="w-3.5 h-3.5 text-rose-400" />}
      isOpen={isOpen}
      onToggle={handleToggleOpen}
    >
      <SelectField
        id="streamingSinkOutputMode"
        name="outputMode"
        label="Output Mode"
        tooltip="Controls how Spark emits rows in each micro-batch: append new rows, write the complete result table, or emit updated rows."
        value={outputMode}
        options={OUTPUT_MODE_OPTIONS}
        onChange={handleOutputModeChange}
      />

      <SelectField
        id="streamingSinkTriggerType"
        name="triggerType"
        label="Trigger Mode"
        tooltip="Inherits the pipeline trigger from Deployment settings by default, or overrides the trigger for this sink."
        value={triggerType}
        options={SINK_TRIGGER_OPTIONS}
        onChange={handleTriggerTypeChange}
      />

      {showTriggerInterval && (
        <PropertyField
          id="streamingSinkTriggerInterval"
          name="triggerInterval"
          label="Trigger Interval"
          tooltip="Micro-batch or continuous checkpoint interval string for this sink, such as 10 seconds or 1 minute."
          value={triggerInterval}
          onChange={handleTriggerIntervalChange}
          placeholder="10 seconds"
        />
      )}

      <PropertyField
        id="streamingSinkCheckpointLocation"
        name="checkpointLocation"
        label="Checkpoint Location Override"
        tooltip={`Explicit checkpoint storage URI for this streaming sink. If blank, Spark inherits <checkpointRoot>/${nodeId}.`}
        value={checkpointLocation}
        onChange={handleCheckpointLocationChange}
        placeholder={`<checkpointRoot>/${nodeId}`}
        helperText={`Leave blank to inherit <checkpointRoot>/${nodeId} from pipeline streaming defaults.`}
      />
    </CollapsibleSection>
  );
};
