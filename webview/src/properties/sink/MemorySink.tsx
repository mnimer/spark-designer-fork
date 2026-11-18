/**
 * @fileoverview MemorySink Properties Component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.x `MemorySink` debug and
 * interactive SQL nodes. Configures `df.createOrReplaceTempView(viewName)` (or
 * `createOrReplaceGlobalTempView`) in batch mode and `.writeStream.format("memory").queryName(queryName)`
 * in Structured Streaming mode.
 *
 * Architectural Decisions:
 * - Synchronizes `queryName` and `viewName` so downstream planners and YAML consumers can read either key.
 * - Validates that the configured name is non-empty and matches SQL identifier syntax (`^[A-Za-z_][A-Za-z0-9_]*$`).
 * - Exposes `globalTempView` toggle for batch global temporary view registration (`global_temp.<viewName>`).
 * - Displays an explicit driver-memory warning banner for streaming `.format("memory")` execution.
 */

import React from 'react';
import { AlertTriangle } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Valid SQL view and streaming query name pattern. */
const SQL_IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** React Flow node shape for the MemorySink panel. */
export interface MemorySinkNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      queryName?: string;
      viewName?: string;
      globalTempView?: boolean;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link MemorySinkProperties} component. */
export interface MemorySinkPropertiesProps {
  node: MemorySinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the configuration panel for Spark 4.x `MemorySink` temporary views and in-memory streaming tables.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React element for the MemorySink property sheet.
 */
export const MemorySinkProperties: React.FC<MemorySinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data?.properties || {};

  const queryName =
    typeof properties.queryName === 'string'
      ? properties.queryName
      : typeof properties.viewName === 'string'
        ? properties.viewName
        : '';
  const globalTempView = Boolean(properties.globalTempView ?? false);

  const systemInstructions = resolveSystemInstructions(node);

  const trimmedName = queryName.trim();
  const isNameEmpty = trimmedName.length === 0;
  const isIdentifierInvalid = !isNameEmpty && !SQL_IDENTIFIER_PATTERN.test(trimmedName);
  const nameErrorMessage = isNameEmpty
    ? 'Query / View Name is required.'
    : isIdentifierInvalid
      ? 'Must start with a letter or underscore and contain only letters, digits, or underscores.'
      : null;

  /**
   * Updates `queryName` and `viewName` together or toggles `globalTempView`.
   *
   * @param e Form change event from an input control.
   */
  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const { name, value, type } = e.target;
    if (type === 'checkbox') {
      const checked = (e.target as HTMLInputElement).checked;
      onChange({
        ...properties,
        [name]: checked,
      });
      return;
    }

    if (name === 'queryName' || name === 'viewName') {
      onChange({
        ...properties,
        queryName: value,
        viewName: value,
      });
      return;
    }

    onChange({
      ...properties,
      [name]: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Driver Memory Advisory Banner */}
      <div
        role="note"
        className="flex items-start gap-2.5 p-3 rounded border border-amber-500/40 bg-amber-500/10 text-xs text-amber-200"
      >
        <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0 mt-0.5" />
        <span>
          Streaming <code>.format(&quot;memory&quot;)</code> buffers all emitted rows in Spark driver
          memory. Use this sink only for bounded debugging, interactive SQL queries, and unit tests.
        </span>
      </div>

      {/* Query / View Name */}
      <PropertyField
        id="memorySinkQueryName"
        name="queryName"
        label="Query / Temp View Name"
        required="Query / View Name is required."
        tooltip="SQL table name registered via .writeStream.format('memory').queryName(name) in streaming mode or df.createOrReplaceTempView(name) in batch mode."
        value={queryName}
        onChange={handleChange}
        placeholder="debug_metrics_view"
        invalid={Boolean(nameErrorMessage)}
        error={nameErrorMessage}
        mono={true}
        helperText={
          !nameErrorMessage ? (
            <span className="text-[11px] text-[var(--vscode-text-muted)]">
              Queryable in Spark SQL as{' '}
              <code>{globalTempView ? `global_temp.${trimmedName}` : trimmedName}</code>.
            </span>
          ) : null
        }
      />

      {/* Global Temp View Toggle */}
      <label htmlFor="memorySinkGlobalTempView" className="flex items-center gap-2 cursor-pointer">
        <input
          id="memorySinkGlobalTempView"
          type="checkbox"
          name="globalTempView"
          checked={globalTempView}
          onChange={handleChange}
          className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
        />
        <div className="flex items-center">
          <span className="text-sm font-medium text-slate-300">
            Register as Global Temp View (Batch Mode)
          </span>
          <InfoTooltip text="Call df.createOrReplaceGlobalTempView(viewName) in batch mode so the view is accessible across SparkSessions under global_temp.<viewName>." />
        </div>
      </label>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="memorySinkSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Memory sink node."
        placeholder="e.g. Cache the registered temp view for downstream verification assertions."
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
