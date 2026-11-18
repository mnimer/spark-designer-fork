/**
 * @fileoverview PubSub Sink Properties Component for Apache Spark 4.x pipelines.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud Pub/Sub Sink nodes (`PubSubSink` / `PubSubIO`)
 * in Apache Spark 4.0 pipelines using `DataSourceWriter` and `foreachBatch` with `google-cloud-pubsub`.
 *
 * Architectural Decisions:
 * - Retains live GCP Pub/Sub topic discovery through `useAssetLoader` with inline error feedback.
 * - Configures `topic`, `projectId`, `payloadFormat`, `payloadColumn`, and `attributeColumns`.
 * - Exposes `orderingKeyColumn`, `idempotencyKeyColumn`, `enableMessageOrdering`, and `PublisherClient` batch settings.
 * - Uses shared `PropertyField`, `SelectField`, and `SystemInstructionsField` primitives on the 8pt grid.
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { Sliders } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';
import { useAssetLoader } from '../../hooks/useAssetLoader';

/** Payload serialization format options for PubSubSink. */
export const PUBSUB_PAYLOAD_FORMAT_OPTIONS: SelectFieldOption[] = [
  { value: 'json', label: 'JSON Object (Serialize full row)' },
  { value: 'string', label: 'String Column (UTF-8)' },
  { value: 'binary', label: 'Binary Column (Raw bytes)' },
];

/** React Flow node shape for the PubSubSink panel. */
export interface PubSubSinkNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      topic?: string;
      projectId?: string;
      payloadFormat?: 'json' | 'string' | 'binary';
      payloadColumn?: string;
      attributeColumns?: string;
      orderingKeyColumn?: string;
      idempotencyKeyColumn?: string;
      enableMessageOrdering?: boolean;
      batchMaxMessages?: string | number;
      batchMaxBytes?: string | number;
      batchMaxLatencyMs?: string | number;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link PubSubSinkProperties} component. */
export interface PubSubSinkPropertiesProps {
  node: PubSubSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the Google Cloud Pub/Sub Spark 4.x sink property configuration form.
 *
 * @param props Active node metadata and property change callback.
 * @returns Rendered PubSubSink property form.
 */
export const PubSubSinkProperties: React.FC<PubSubSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const { items: topics, loading: topicsLoading, error: topicError } =
    useAssetLoader<string>('topics');

  const topic = properties.topic || '';
  const projectId = properties.projectId || '';
  const payloadFormat = properties.payloadFormat || 'json';
  const payloadColumn = properties.payloadColumn || '';
  const attributeColumns = properties.attributeColumns || '';
  const orderingKeyColumn = properties.orderingKeyColumn || '';
  const idempotencyKeyColumn = properties.idempotencyKeyColumn || '';
  const enableMessageOrdering = properties.enableMessageOrdering ?? false;
  const batchMaxMessages = properties.batchMaxMessages ?? '100';
  const batchMaxBytes = properties.batchMaxBytes ?? '1048576';
  const batchMaxLatencyMs = properties.batchMaxLatencyMs ?? '10';

  const systemInstructions = resolveSystemInstructions(node);

  const isTopicInvalid = !topic.trim();
  const isPayloadColumnRequired = payloadFormat === 'string' || payloadFormat === 'binary';
  const isPayloadColumnInvalid = isPayloadColumnRequired && !payloadColumn.trim();

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
      {/* Topic Input (Required) */}
      <PropertyField
        id="pubsubSinkTopic"
        name="topic"
        label="Topic"
        required="Topic is required."
        tooltip="Cloud Pub/Sub destination resource path or short ID (for example projects/my-project/topics/my-topic). Required."
        value={topic}
        invalid={isTopicInvalid}
        error={topicError || (isTopicInvalid ? 'Topic is required.' : null)}
        loading={topicsLoading}
        onChange={handleChange}
        placeholder="projects/my-project/topics/my-topic"
        list="topic-options"
        datalist={topics}
        mono={true}
        helperText={
          !isTopicInvalid ? (
            <span>
              Sample: <code className="text-slate-300 bg-slate-800/60 px-1 py-0.5 rounded">projects/my-project/topics/my-topic</code>
            </span>
          ) : null
        }
      />

      {/* GCP Project ID */}
      <PropertyField
        id="pubsubSinkProjectId"
        name="projectId"
        label="GCP Project ID"
        tooltip="Google Cloud project owning the destination Pub/Sub resource. Leave blank if passing a full projects/.../topics/... path."
        value={projectId}
        onChange={handleChange}
        placeholder="e.g. my-gcp-project"
        mono={true}
      />

      {/* Payload Serialization and Column Mapping */}
      <SelectField
        id="pubsubSinkPayloadFormat"
        name="payloadFormat"
        label="Payload Format"
        tooltip="Select how each Spark DataFrame row serializes into Pub/Sub message data bytes."
        value={payloadFormat}
        options={PUBSUB_PAYLOAD_FORMAT_OPTIONS}
        onChange={handleChange}
      />

      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="pubsubSinkPayloadColumn"
          name="payloadColumn"
          label="Payload Column"
          required={isPayloadColumnRequired ? 'Payload Column is required.' : false}
          tooltip="DataFrame column holding the message body. If JSON mode is selected and this field is blank, the full row serializes as JSON."
          value={payloadColumn}
          invalid={isPayloadColumnInvalid}
          error={isPayloadColumnInvalid ? 'Payload Column is required.' : null}
          onChange={handleChange}
          placeholder={payloadFormat === 'json' ? 'Full row (optional)' : 'data'}
          mono={true}
        />

        <PropertyField
          id="pubsubSinkAttributeColumns"
          name="attributeColumns"
          label="Attribute Columns"
          tooltip="Comma-separated DataFrame column names attached as string key-value metadata on each published message."
          value={attributeColumns}
          onChange={handleChange}
          placeholder="e.g. event_type,region"
          mono={true}
        />
      </div>

      {/* Ordering Key and Idempotency */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="pubsubSinkOrderingKeyColumn"
          name="orderingKeyColumn"
          label="Ordering Key Column"
          tooltip="Optional DataFrame column passed as ordering_key so Pub/Sub delivers messages with identical keys in order."
          value={orderingKeyColumn}
          onChange={handleChange}
          placeholder="e.g. customer_id"
          mono={true}
        />

        <PropertyField
          id="pubsubSinkIdempotencyKeyColumn"
          name="idempotencyKeyColumn"
          label="Idempotency Key Column"
          tooltip="Optional deterministic identifier column attached as an event_id attribute to deduplicate retried micro-batches."
          value={idempotencyKeyColumn}
          onChange={handleChange}
          placeholder="e.g. event_id"
          mono={true}
        />
      </div>

      <div className="p-3 bg-slate-800/40 rounded border border-slate-700/50">
        <label htmlFor="pubsubSinkEnableOrdering" className="flex items-center gap-2 cursor-pointer">
          <input
            id="pubsubSinkEnableOrdering"
            type="checkbox"
            name="enableMessageOrdering"
            checked={enableMessageOrdering}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Enable Message Ordering</span>
            <InfoTooltip text="Configure PublisherOptions(enable_message_ordering=True) on PublisherClient for ordered key delivery." />
          </div>
        </label>
      </div>

      {/* Publisher Batch Settings */}
      <div className="p-3 bg-[var(--vscode-subcard-bg)] rounded border border-[var(--vscode-border)] flex flex-col gap-3">
        <div className="flex items-center gap-2 pb-1 border-b border-[var(--vscode-border-light)]">
          <Sliders className="w-4 h-4 text-emerald-400" />
          <span className="text-xs font-bold uppercase tracking-wide text-[var(--vscode-text-primary)]">
            Publisher Batch Settings
          </span>
        </div>

        <div className="grid grid-cols-3 gap-2.5">
          <PropertyField
            id="pubsubSinkBatchMaxMessages"
            name="batchMaxMessages"
            label="Max Messages"
            type="number"
            min="1"
            max="1000"
            tooltip="Maximum number of messages bundled per PublisherClient RPC request (BatchSettings.max_messages, default 100)."
            value={batchMaxMessages}
            onChange={handleChange}
            placeholder="100"
            mono={true}
          />

          <PropertyField
            id="pubsubSinkBatchMaxBytes"
            name="batchMaxBytes"
            label="Max Bytes"
            type="number"
            min="1"
            tooltip="Maximum total byte size bundled per PublisherClient RPC request (BatchSettings.max_bytes, default 1048576)."
            value={batchMaxBytes}
            onChange={handleChange}
            placeholder="1048576"
            mono={true}
          />

          <PropertyField
            id="pubsubSinkBatchMaxLatencyMs"
            name="batchMaxLatencyMs"
            label="Max Latency (ms)"
            type="number"
            min="1"
            tooltip="Maximum delay in milliseconds before flushing an incomplete publish bundle (BatchSettings.max_latency, default 10)."
            value={batchMaxLatencyMs}
            onChange={handleChange}
            placeholder="10"
            mono={true}
          />
        </div>
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="pubsubSinkSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Pub/Sub sink node."
        placeholder="Enter custom instructions for Pub/Sub sink code generation..."
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

/** Alias matching legacy `PubSubIO` sink naming. */
export const PubSubIOSinkProperties = PubSubSinkProperties;
