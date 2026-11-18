/**
 * @fileoverview ConsoleSink Properties Component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.x `ConsoleSink` (`LogMessage`)
 * debug and dead-letter inspection nodes. Configures bounded `df.limit(numRows)` structured
 * `logging` summaries in batch mode and `.writeStream.format("console")` output in streaming mode.
 *
 * Architectural Decisions:
 * - Exposes `numRows` (default `20`), `logLevel` (`'INFO' | 'DEBUG' | 'WARNING' | 'ERROR'`, default `'INFO'`),
 *   and `truncate` (`boolean`, default `true`).
 * - Validates that `numRows` is a positive integer greater than or equal to `1`.
 * - Uses shared primitives (`PropertyField`, `SelectField`, `SystemInstructionsField`) on the 8pt grid.
 */

import React from 'react';
import { Terminal } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Python logging level options for batch ConsoleSink summaries. */
const CONSOLE_LOG_LEVEL_OPTIONS: SelectFieldOption[] = [
  { value: 'INFO', label: 'INFO (Standard operational preview)' },
  { value: 'DEBUG', label: 'DEBUG (Verbose diagnostic output)' },
  { value: 'WARNING', label: 'WARNING (Dead-letter or anomaly alert)' },
  { value: 'ERROR', label: 'ERROR (Critical dead-letter alert)' },
];

/** Supported log level union for ConsoleSink. */
export type ConsoleLogLevel = 'INFO' | 'DEBUG' | 'WARNING' | 'ERROR';

/** React Flow node shape for the ConsoleSink panel. */
export interface ConsoleSinkNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      numRows?: number | string;
      truncate?: boolean;
      logLevel?: ConsoleLogLevel;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link ConsoleSinkProperties} component. */
export interface ConsoleSinkPropertiesProps {
  node: ConsoleSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the configuration panel for Spark 4.x `ConsoleSink` debug output.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React element for the ConsoleSink property sheet.
 */
export const ConsoleSinkProperties: React.FC<ConsoleSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data?.properties || {};

  const numRows = properties.numRows ?? 20;
  const truncate = Boolean(properties.truncate ?? true);
  const logLevel: ConsoleLogLevel = properties.logLevel || 'INFO';

  const systemInstructions = resolveSystemInstructions(node);

  const parsedRows = Number(numRows);
  const isNumRowsInvalid =
    String(numRows).trim().length === 0 || Number.isNaN(parsedRows) || parsedRows < 1;

  /**
   * Updates node properties when form inputs, selects, or checkboxes change.
   *
   * @param e Form change event from an input, select, or textarea control.
   */
  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const isCheckbox = type === 'checkbox';
    const checked = isCheckbox ? (e.target as HTMLInputElement).checked : undefined;

    onChange({
      ...properties,
      [name]: isCheckbox ? checked : value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Debug Sink Advisory Banner */}
      <div className="flex items-center justify-between px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-subcard-bg)] text-xs">
        <span className="flex items-center gap-1.5 font-bold uppercase text-[var(--vscode-text-muted)]">
          <Terminal className="w-3.5 h-3.5 text-amber-400" />
          <span>Debug Inspection Sink</span>
        </span>
        <span className="px-2 py-0.5 rounded bg-amber-500/20 text-amber-300 font-mono text-[11px]">
          BOUNDED PREVIEW
        </span>
      </div>

      {/* Number of Rows & Log Level */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="consoleSinkNumRows"
          name="numRows"
          label="Max Preview Rows"
          type="number"
          min="1"
          max="1000"
          tooltip="Maximum number of rows sampled per batch (df.limit(numRows)) or printed per streaming trigger (.option('numRows', numRows))."
          value={numRows}
          onChange={handleChange}
          placeholder="20"
          invalid={isNumRowsInvalid}
          error={isNumRowsInvalid ? 'Enter at least 1 row.' : null}
          mono={true}
        />

        <SelectField
          id="consoleSinkLogLevel"
          name="logLevel"
          label="Log Level"
          tooltip="Python logging severity level used when emitting bounded DataFrame previews in batch mode."
          value={logLevel}
          options={CONSOLE_LOG_LEVEL_OPTIONS}
          onChange={handleChange}
        />
      </div>

      {/* Truncate Toggle */}
      <label htmlFor="consoleSinkTruncate" className="flex items-center gap-2 cursor-pointer">
        <input
          id="consoleSinkTruncate"
          type="checkbox"
          name="truncate"
          checked={truncate}
          onChange={handleChange}
          className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
        />
        <div className="flex items-center">
          <span className="text-sm font-medium text-slate-300">Truncate Long Strings (truncate)</span>
          <InfoTooltip text="Truncate string values longer than 20 characters in console and structured log previews (.option('truncate', 'true'))." />
        </div>
      </label>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="consoleSinkSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Console sink node."
        placeholder="e.g. Log corrupt record counts and sample _corrupt_record payloads at WARNING level."
        onChange={(value) =>
          onChange({
            ...properties,
            system_instructions: value,
          })
        }
      />
    </div>
  );
};
