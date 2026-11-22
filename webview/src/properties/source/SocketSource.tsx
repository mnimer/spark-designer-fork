/**
 * SocketSource Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark built-in Socket Streaming Source
 * nodes (`SocketSource`, `spark.readStream.format("socket")`) used for local debugging and testing.
 *
 * Architectural Decisions:
 * - Streaming-only source (`readMode: 'streaming'`).
 * - Configures `host` (default `'localhost'`), `port` (default `9999`), and `includeTimestamp` (boolean).
 * - Uses shared primitives (`PropertyField`, `InfoTooltip`, `SystemInstructionsField`) on the 8pt grid.
 */

import React from 'react';
import { Radio } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** React Flow node shape for the SocketSource panel. */
export interface SocketSourceNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      readMode?: 'streaming';
      host?: string;
      port?: number | string;
      includeTimestamp?: boolean;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link SocketSourceProperties} component. */
export interface SocketSourcePropertiesProps {
  node: SocketSourceNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * SocketSourceProperties renders the configuration panel for Spark `.format("socket")` debug streams.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React component.
 */
export const SocketSourceProperties: React.FC<SocketSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const host = properties.host !== undefined ? properties.host : 'localhost';
  const port = properties.port !== undefined ? properties.port : 9999;
  const includeTimestamp = properties.includeTimestamp ?? false;

  const systemInstructions = resolveSystemInstructions(node);

  const isHostInvalid = !String(host).trim();
  const portNum = Number(port);
  const isPortInvalid = String(port).trim() === '' || Number.isNaN(portNum) || portNum < 1 || portNum > 65535;

  /**
   * Handles input and checkbox changes.
   *
   * @param e Form change event from input or textarea.
   */
  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const { name, value, type } = e.target;
    const isCheckbox = type === 'checkbox';
    const checked = isCheckbox ? (e.target as HTMLInputElement).checked : undefined;

    onChange({
      ...properties,
      readMode: 'streaming',
      [name]: isCheckbox ? checked : value,
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
          STREAMING ONLY (DEBUG)
        </span>
      </div>

      {/* Host & Port */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="socketHost"
          name="host"
          label="Socket Host"
          required="Socket Host is required."
          tooltip="Hostname or IP address of the TCP socket server to read UTF-8 lines from (option host, default localhost). Required."
          value={host}
          onChange={handleChange}
          placeholder="localhost"
          invalid={isHostInvalid}
          error={isHostInvalid ? 'Socket Host is required.' : null}
          mono={true}
        />

        <PropertyField
          id="socketPort"
          name="port"
          label="Socket Port"
          type="number"
          min="1"
          max="65535"
          required="Valid TCP port (1-65535) is required."
          tooltip="TCP port number to connect to (option port, default 9999). Required."
          value={port}
          onChange={handleChange}
          placeholder="9999"
          invalid={isPortInvalid}
          error={isPortInvalid ? 'Valid TCP port (1-65535) is required.' : null}
          mono={true}
        />
      </div>

      {/* Include Timestamp Checkbox */}
      <div className="p-3 bg-slate-800/40 rounded border border-slate-700/50">
        <label htmlFor="socketIncludeTimestamp" className="flex items-center gap-2 cursor-pointer">
          <input
            id="socketIncludeTimestamp"
            type="checkbox"
            name="includeTimestamp"
            checked={includeTimestamp}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Include Timestamp Column</span>
            <InfoTooltip text="Include a reception timestamp column alongside the value string column in the output DataFrame (option includeTimestamp)." />
          </div>
        </label>
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="socketSourceSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Socket source node."
        placeholder="e.g. Split incoming socket lines into words or parse JSON lines..."
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
