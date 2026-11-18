/**
 * @fileoverview ForeachBatchSink Properties Component.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.x `ForeachBatchSink`
 * (`DatadogIO`, `SplunkIO`, `RequestResponseIO`) nodes. Configures custom Python
 * `foreachBatch` functions or partition-batched HTTP delivery to REST webhooks,
 * Datadog HTTP Logs Intake v2, and Splunk HTTP Event Collector (HEC).
 *
 * Architectural Decisions:
 * - Supports `sinkMode` (`'custom_python' | 'http_webhook' | 'datadog' | 'splunk_hec'`).
 * - Renders HTTP delivery controls (`endpointUrl`, `httpMethod`, `batchSize`, `timeoutSeconds`,
 *   `maxRetries`, and `SecretManagerSecretField` bound to `tokenSecret`) when an HTTP preset is active.
 * - Validates `endpointUrl` scheme (`http://` or `https://`), required `tokenSecret` for Datadog
 *   and Splunk HEC, and required `customFunctionInstructions` when `sinkMode` equals `'custom_python'`.
 */

import React from 'react';
import { Webhook } from 'lucide-react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { SecretManagerSecretField } from '../../components/common/SecretManagerSecretField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Preset execution modes for ForeachBatchSink. */
const FOREACH_BATCH_SINK_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'custom_python', label: 'Custom Python Function (def write_micro_batch)' },
  { value: 'http_webhook', label: 'Generic Batched REST Webhook (JSON HTTP)' },
  { value: 'datadog', label: 'Datadog HTTP Logs Intake API (v2)' },
  { value: 'splunk_hec', label: 'Splunk HTTP Event Collector (HEC)' },
];

/** Supported HTTP request methods for webhook delivery. */
const FOREACH_BATCH_HTTP_METHOD_OPTIONS: SelectFieldOption[] = [
  { value: 'POST', label: 'POST' },
  { value: 'PUT', label: 'PUT' },
  { value: 'PATCH', label: 'PATCH' },
];

/** Sink mode union type for ForeachBatchSink. */
export type ForeachBatchSinkMode =
  | 'custom_python'
  | 'http_webhook'
  | 'datadog'
  | 'splunk_hec';

/** HTTP method union type for ForeachBatchSink. */
export type ForeachBatchHttpMethod = 'POST' | 'PUT' | 'PATCH';

/** React Flow node shape for the ForeachBatchSink panel. */
export interface ForeachBatchSinkNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      sinkMode?: ForeachBatchSinkMode;
      endpointUrl?: string;
      httpMethod?: ForeachBatchHttpMethod;
      batchSize?: number | string;
      timeoutSeconds?: number | string;
      maxRetries?: number | string;
      tokenSecret?: string;
      customFunctionInstructions?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link ForeachBatchSinkProperties} component. */
export interface ForeachBatchSinkPropertiesProps {
  node: ForeachBatchSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Resolves a mode-specific placeholder URL for HTTP presets.
 *
 * @param mode Selected ForeachBatchSink preset mode.
 * @returns Example endpoint URL.
 */
function getEndpointPlaceholder(mode: ForeachBatchSinkMode): string {
  if (mode === 'datadog') {
    return 'https://http-intake.logs.datadoghq.com/api/v2/logs';
  }
  if (mode === 'splunk_hec') {
    return 'https://splunk-hec.example.com:8088/services/collector/event';
  }
  return 'https://api.example.com/v1/events/batch';
}

/**
 * Renders the configuration panel for Spark 4.x `ForeachBatchSink` nodes.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React element for the ForeachBatchSink property sheet.
 */
export const ForeachBatchSinkProperties: React.FC<ForeachBatchSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data?.properties || {};

  const sinkMode: ForeachBatchSinkMode = properties.sinkMode || 'custom_python';
  const endpointUrl = typeof properties.endpointUrl === 'string' ? properties.endpointUrl : '';
  const httpMethod: ForeachBatchHttpMethod = properties.httpMethod || 'POST';
  const batchSize = properties.batchSize ?? 500;
  const timeoutSeconds = properties.timeoutSeconds ?? 30;
  const maxRetries = properties.maxRetries ?? 3;
  const tokenSecret = typeof properties.tokenSecret === 'string' ? properties.tokenSecret : '';
  const customFunctionInstructions =
    typeof properties.customFunctionInstructions === 'string'
      ? properties.customFunctionInstructions
      : '';

  const systemInstructions = resolveSystemInstructions(node);

  const isHttpMode =
    sinkMode === 'http_webhook' || sinkMode === 'datadog' || sinkMode === 'splunk_hec';
  const requiresTokenSecret = sinkMode === 'datadog' || sinkMode === 'splunk_hec';

  const trimmedUrl = endpointUrl.trim();
  const isUrlEmpty = isHttpMode && trimmedUrl.length === 0;
  const isUrlSchemeInvalid =
    isHttpMode &&
    trimmedUrl.length > 0 &&
    !trimmedUrl.toLowerCase().startsWith('http://') &&
    !trimmedUrl.toLowerCase().startsWith('https://');
  const endpointError = isUrlEmpty
    ? 'Endpoint URL is required.'
    : isUrlSchemeInvalid
      ? 'Endpoint URL must start with https:// or http://.'
      : null;

  const isTokenSecretMissing = requiresTokenSecret && tokenSecret.trim().length === 0;
  const isCustomFunctionMissing =
    sinkMode === 'custom_python' && customFunctionInstructions.trim().length === 0;

  /**
   * Updates node properties when form inputs or selects change.
   *
   * @param e Change event from an input, select, or textarea control.
   */
  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    onChange({
      ...properties,
      [name]: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Execution Strategy Banner */}
      <div className="flex items-center justify-between px-3 py-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-subcard-bg)] text-xs">
        <span className="flex items-center gap-1.5 font-bold uppercase text-[var(--vscode-text-muted)]">
          <Webhook className="w-3.5 h-3.5 text-purple-400" />
          <span>Execution Strategy</span>
        </span>
        <span className="px-2 py-0.5 rounded bg-purple-500/20 text-purple-300 font-mono text-[11px]">
          FOREACHBATCH / PARTITION WRITER
        </span>
      </div>

      {/* Sink Preset Mode */}
      <SelectField
        id="foreachBatchSinkMode"
        name="sinkMode"
        label="Sink Mode"
        tooltip="Choose a custom Python write_micro_batch function or a batched HTTP delivery preset (REST webhook, Datadog Logs v2, or Splunk HEC)."
        value={sinkMode}
        options={FOREACH_BATCH_SINK_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* HTTP Delivery Settings (Visible for http_webhook, datadog, and splunk_hec) */}
      {isHttpMode && (
        <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
          <PropertyField
            id="foreachBatchEndpointUrl"
            name="endpointUrl"
            label="Endpoint URL"
            required="Endpoint URL is required."
            tooltip="Target HTTPS endpoint receiving batched JSON payloads per partition."
            value={endpointUrl}
            onChange={handleChange}
            placeholder={getEndpointPlaceholder(sinkMode)}
            invalid={Boolean(endpointError)}
            error={endpointError}
            mono={true}
          />

          <div className="grid grid-cols-2 gap-3">
            <SelectField
              id="foreachBatchHttpMethod"
              name="httpMethod"
              label="HTTP Method"
              tooltip="HTTP verb used when dispatching batched JSON payloads."
              value={httpMethod}
              options={FOREACH_BATCH_HTTP_METHOD_OPTIONS}
              onChange={handleChange}
            />

            <PropertyField
              id="foreachBatchSize"
              name="batchSize"
              label="Batch Size (Records)"
              type="number"
              min="1"
              tooltip="Maximum number of records grouped into a single HTTP request per partition."
              value={batchSize}
              onChange={handleChange}
              placeholder="500"
              mono={true}
            />
          </div>

          <div className="grid grid-cols-2 gap-3">
            <PropertyField
              id="foreachBatchTimeoutSeconds"
              name="timeoutSeconds"
              label="Timeout (Seconds)"
              type="number"
              min="1"
              tooltip="Per-request HTTP socket timeout in seconds passed to urllib.request.urlopen."
              value={timeoutSeconds}
              onChange={handleChange}
              placeholder="30"
              mono={true}
            />

            <PropertyField
              id="foreachBatchMaxRetries"
              name="maxRetries"
              label="Retry Count"
              type="number"
              min="0"
              tooltip="Maximum exponential-backoff retry attempts on HTTP 429 or 5xx responses."
              value={maxRetries}
              onChange={handleChange}
              placeholder="3"
              mono={true}
            />
          </div>

          <SecretManagerSecretField
            id="foreachBatchTokenSecret"
            name="tokenSecret"
            label={
              sinkMode === 'datadog'
                ? 'Datadog API Key Secret'
                : sinkMode === 'splunk_hec'
                  ? 'Splunk HEC Token Secret'
                  : 'Bearer Token / API Key Secret (Optional)'
            }
            required={requiresTokenSecret ? 'Secret Manager credential reference is required.' : false}
            tooltip="Google Cloud Secret Manager secret ID resolved once via get_secret() for HTTP authentication headers."
            value={tokenSecret}
            onChange={handleChange}
            placeholder={
              sinkMode === 'datadog'
                ? 'e.g. datadog-api-key'
                : sinkMode === 'splunk_hec'
                  ? 'e.g. splunk-hec-token'
                  : 'e.g. webhook-bearer-token'
            }
            invalid={isTokenSecretMissing}
            error={isTokenSecretMissing ? 'Secret Manager credential reference is required.' : null}
          />
        </div>
      )}

      {/* Custom Function or Payload Mapping Instructions */}
      <PropertyField
        id="foreachBatchCustomInstructions"
        name="customFunctionInstructions"
        label={
          sinkMode === 'custom_python'
            ? 'Custom Python Function Logic'
            : 'Custom Payload Mapping (Optional)'
        }
        type="textarea"
        rows={4}
        required={sinkMode === 'custom_python' ? 'Custom Python logic is required.' : false}
        tooltip={
          sinkMode === 'custom_python'
            ? 'Describe the Python logic executed inside def write_micro_batch(batch_df, batch_id) for each batch.'
            : 'Optional instructions describing how DataFrame columns map into the outbound JSON body or headers.'
        }
        value={customFunctionInstructions}
        onChange={handleChange}
        placeholder={
          sinkMode === 'custom_python'
            ? 'e.g. Write high-severity alert rows to an audit table and publish summary metrics.'
            : 'e.g. Nest payload fields under an attributes key and use event_id for deduplication.'
        }
        invalid={isCustomFunctionMissing}
        error={isCustomFunctionMissing ? 'Custom Python logic is required.' : null}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="foreachBatchSinkSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this ForeachBatch sink node."
        placeholder="Enter additional code generation instructions for this ForeachBatch sink..."
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
