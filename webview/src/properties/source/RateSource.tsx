/**
 * RateSource Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark built-in Rate Streaming Source
 * nodes (`RateSource`). Supports both `.format("rate")` (wall-clock continuous tick generator)
 * and `.format("rate-micro-batch")` (deterministic micro-batch generator emitting `(timestamp, value)` rows).
 *
 * Architectural Decisions:
 * - Streaming-only source (`readMode: 'streaming'`).
 * - Supports `rateFormat` (`'rate' | 'rate-micro-batch'`, default `'rate'`).
 * - Configures `rowsPerSecond` (default `1`), `rampUpTime`, and `numPartitions` when `rateFormat === 'rate'`.
 * - Configures `rowsPerBatch`, `advanceMillisPerBatch`, and `numPartitions` when `rateFormat === 'rate-micro-batch'`.
 * - Uses shared primitives (`PropertyField`, `SelectField`, `SystemInstructionsField`) on the 8pt grid.
 */

import React from 'react';
import { Radio } from 'lucide-react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Rate source format options in Spark Structured Streaming. */
export const RATE_FORMAT_OPTIONS: SelectFieldOption[] = [
  { value: 'rate', label: 'rate (Wall-clock rowsPerSecond)' },
  { value: 'rate-micro-batch', label: 'rate-micro-batch (Deterministic per-batch advancement)' },
];

/** React Flow node shape for the RateSource panel. */
export interface RateSourceNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      readMode?: 'streaming';
      rateFormat?: 'rate' | 'rate-micro-batch';
      rowsPerSecond?: number | string;
      rampUpTime?: string;
      numPartitions?: number | string;
      rowsPerBatch?: number | string;
      advanceMillisPerBatch?: number | string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: any;
    };
  };
}

/** Props for the {@link RateSourceProperties} component. */
export interface RateSourcePropertiesProps {
  node: RateSourceNode;
  onChange: (properties: any) => void;
}

/**
 * RateSourceProperties renders the configuration panel for built-in Spark `.format("rate")` and
 * `.format("rate-micro-batch")` streaming sources.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React component.
 */
export const RateSourceProperties: React.FC<RateSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const rateFormat = properties.rateFormat || 'rate';
  const rowsPerSecond = properties.rowsPerSecond ?? 1;
  const rampUpTime = properties.rampUpTime || '';
  const numPartitions = properties.numPartitions ?? '';
  const rowsPerBatch = properties.rowsPerBatch ?? 100;
  const advanceMillisPerBatch = properties.advanceMillisPerBatch ?? 1000;

  const systemInstructions = resolveSystemInstructions(node);

  /**
   * Handles input and select changes.
   *
   * @param e Form change event from input, select, or textarea.
   */
  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;

    onChange({
      ...properties,
      readMode: 'streaming',
      [name]: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Streaming Mode Indicator Banner */}
      <div className="flex items-center justify-between px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-subcard-bg)] text-xs">
        <span className="flex items-center gap-1.5 font-bold uppercase text-[var(--vscode-text-muted)]">
          <Radio className="w-3.5 h-3.5 text-emerald-400" />
          <span>Execution Mode</span>
        </span>
        <span className="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300 font-mono text-[11px]">
          STREAMING ONLY
        </span>
      </div>

      {/* Rate Format Selector */}
      <SelectField
        id="rateSourceFormat"
        name="rateFormat"
        label="Rate Source Format"
        tooltip="Select .format('rate') for real-time wall-clock row generation or .format('rate-micro-batch') for deterministic micro-batch testing."
        value={rateFormat}
        options={RATE_FORMAT_OPTIONS}
        onChange={handleChange}
      />

      {rateFormat === 'rate' ? (
        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="rateRowsPerSecond"
            name="rowsPerSecond"
            label="Rows Per Second"
            type="number"
            min="1"
            tooltip="Number of rows generated per second across all partitions (option rowsPerSecond, default 1)."
            value={rowsPerSecond}
            onChange={handleChange}
            placeholder="1"
            mono={true}
          />

          <PropertyField
            id="rateRampUpTime"
            name="rampUpTime"
            label="Ramp-Up Time"
            tooltip="Duration before the generator reaches full rowsPerSecond throughput (option rampUpTime, e.g. 5s or 1m)."
            value={rampUpTime}
            onChange={handleChange}
            placeholder="e.g. 5s"
            mono={true}
          />
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="rateRowsPerBatch"
            name="rowsPerBatch"
            label="Rows Per Batch"
            type="number"
            min="1"
            tooltip="Number of rows emitted in each deterministic micro-batch (option rowsPerBatch)."
            value={rowsPerBatch}
            onChange={handleChange}
            placeholder="100"
            mono={true}
          />

          <PropertyField
            id="rateAdvanceMillisPerBatch"
            name="advanceMillisPerBatch"
            label="Advance Millis Per Batch"
            type="number"
            min="1"
            tooltip="Number of milliseconds event time advances on each micro-batch (option advanceMillisPerBatch)."
            value={advanceMillisPerBatch}
            onChange={handleChange}
            placeholder="1000"
            mono={true}
          />
        </div>
      )}

      {/* Number of Partitions */}
      <PropertyField
        id="rateNumPartitions"
        name="numPartitions"
        label="Number of Partitions"
        type="number"
        min="1"
        tooltip="Number of Spark partitions used to generate synthetic rows (option numPartitions, defaults to spark.sql.leafNodeDefaultParallelism)."
        value={numPartitions}
        onChange={handleChange}
        placeholder="e.g. 4"
        mono={true}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="rateSourceSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Rate source node."
        placeholder="e.g. Derive synthetic user_id and amount columns from the generated value column..."
        onChange={(value) =>
          onChange({
            ...properties,
            readMode: 'streaming',
            system_instructions: value,
          })
        }
      />
    </div>
  );
};
