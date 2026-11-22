/**
 * PulsarIO Source Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark PulsarSource (`PulsarIO`) nodes.
 * Configures streaming (`spark.readStream.format("pulsar")`) and batch (`spark.read.format("pulsar")`)
 * reads using the StreamNative `pulsar-spark-connector`.
 *
 * Architectural Decisions:
 * - Supports `readMode` (`'streaming' | 'batch'`, default `'streaming'`).
 * - Configures `serviceUrl` (`service.url`), `adminUrl` (`admin.url`), `subscriptionMode`
 *   (`'topic' | 'topics' | 'topicsPattern'`), `topic`, `startingOffsets` (`'latest' | 'earliest'`),
 *   conditional `endingOffsets` (in batch mode), and `allowDifferentTopicSchemas`.
 * - Integrates `SecretManagerAuthFields` for Pulsar JWT token / credentials in Google Cloud Secret Manager.
 * - Uses shared primitives on the 8pt grid system.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { SecretManagerAuthFields } from '../../components/common/SecretManagerAuthFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Read mode options for PulsarSource. */
const PULSAR_READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'streaming', label: 'Streaming (spark.readStream)' },
  { value: 'batch', label: 'Batch (spark.read)' },
];

/** Subscription mode options for the Pulsar Spark connector. */
const PULSAR_SUBSCRIPTION_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'topic', label: 'Single Topic (topic)' },
  { value: 'topics', label: 'Multiple Topics (topics)' },
  { value: 'topicsPattern', label: 'Topics Regex Pattern (topicsPattern)' },
];

/** Starting offset options for Pulsar reads. */
const PULSAR_STARTING_OFFSETS_OPTIONS: SelectFieldOption[] = [
  { value: 'latest', label: 'latest (Read latest messages)' },
  { value: 'earliest', label: 'earliest (Read from earliest retained message)' },
];

/** React Flow node shape for the PulsarIO source panel. */
export interface PulsarIOSourceNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      readMode?: 'streaming' | 'batch';
      serviceUrl?: string;
      adminUrl?: string;
      subscriptionMode?: 'topic' | 'topics' | 'topicsPattern';
      topic?: string;
      startingOffsets?: 'latest' | 'earliest' | string;
      endingOffsets?: string;
      allowDifferentTopicSchemas?: boolean;
      authMode?: string;
      secretName?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link PulsarIOSourceProperties} component. */
export interface PulsarIOSourcePropertiesProps {
  node: PulsarIOSourceNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * PulsarIOSourceProperties renders the configuration form for Apache Spark `.format("pulsar")` reads.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React component.
 */
export const PulsarIOSourceProperties: React.FC<PulsarIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const readMode = properties.readMode || 'streaming';
  const serviceUrl = properties.serviceUrl || '';
  const adminUrl = properties.adminUrl || '';
  const subscriptionMode = properties.subscriptionMode || 'topic';
  const topic = properties.topic || '';
  const startingOffsets = properties.startingOffsets || 'latest';
  const endingOffsets = properties.endingOffsets || 'latest';
  const allowDifferentTopicSchemas = properties.allowDifferentTopicSchemas ?? false;
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';

  const systemInstructions = resolveSystemInstructions(node);

  const isServiceUrlInvalid = !serviceUrl.trim();
  const isTopicInvalid = !topic.trim();

  /**
   * Handles generic text, select, and checkbox input changes.
   *
   * @param e Form change event from input, select, or textarea.
   */
  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value, type } = e.target;
    const isCheckbox = type === 'checkbox';
    const checked = isCheckbox ? (e.target as HTMLInputElement).checked : undefined;

    onChange({
      ...properties,
      [name]: isCheckbox ? checked : value,
    });
  };

  const topicLabel =
    subscriptionMode === 'topics'
      ? 'Pulsar Topics (Comma-Separated)'
      : subscriptionMode === 'topicsPattern'
        ? 'Pulsar Topics Pattern (Regex)'
        : 'Pulsar Topic';

  const topicPlaceholder =
    subscriptionMode === 'topics'
      ? 'persistent://public/default/topic1,persistent://public/default/topic2'
      : subscriptionMode === 'topicsPattern'
        ? 'persistent://public/default/events-.*'
        : 'persistent://public/default/telemetry';

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Read Mode Select */}
      <SelectField
        id="pulsarReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select streaming mode (spark.readStream.format('pulsar')) or batch mode (spark.read.format('pulsar'))."
        value={readMode}
        options={PULSAR_READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Broker Service URL (Required) */}
      <PropertyField
        id="pulsarServiceUrl"
        name="serviceUrl"
        label="Pulsar Service URL"
        required="Pulsar Service URL is required."
        tooltip="Pulsar broker service URL (option service.url, e.g. pulsar://localhost:6650 or pulsar+ssl://broker:6651). Required."
        value={serviceUrl}
        onChange={handleChange}
        placeholder="pulsar://localhost:6650"
        invalid={isServiceUrlInvalid}
        error={isServiceUrlInvalid ? 'Pulsar Service URL is required.' : null}
        mono={true}
      />

      {/* Admin HTTP URL */}
      <PropertyField
        id="pulsarAdminUrl"
        name="adminUrl"
        label="Pulsar Admin URL"
        tooltip="Optional Pulsar web admin service URL used for schema and topic metadata lookup (option admin.url, e.g. http://localhost:8080)."
        value={adminUrl}
        onChange={handleChange}
        placeholder="http://localhost:8080"
        mono={true}
      />

      {/* Subscription Mode */}
      <SelectField
        id="pulsarSubscriptionMode"
        name="subscriptionMode"
        label="Subscription Mode"
        tooltip="Choose how Spark subscribes to streams: single destination (topic), comma-separated list (topics), or regular expression (topicsPattern)."
        value={subscriptionMode}
        options={PULSAR_SUBSCRIPTION_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Topic / Topics / Pattern Input */}
      <PropertyField
        id="pulsarTopic"
        name="topic"
        label={topicLabel}
        required="Pulsar topic is required."
        tooltip="Destination URI, comma-separated list of URIs, or regex pattern matching subscriptionMode."
        value={topic}
        onChange={handleChange}
        placeholder={topicPlaceholder}
        invalid={isTopicInvalid}
        error={isTopicInvalid ? 'Pulsar topic is required.' : null}
        mono={true}
      />

      {/* Offsets Configuration */}
      <div className="grid grid-cols-2 gap-3">
        <SelectField
          id="pulsarStartingOffsets"
          name="startingOffsets"
          label="Starting Offsets"
          tooltip="Starting offset position when reading from Pulsar (option startingOffsets: latest or earliest)."
          value={startingOffsets}
          options={PULSAR_STARTING_OFFSETS_OPTIONS}
          onChange={handleChange}
        />

        {readMode === 'batch' && (
          <PropertyField
            id="pulsarEndingOffsets"
            name="endingOffsets"
            label="Ending Offsets (Batch)"
            tooltip="Ending offset position for bounded batch queries (option endingOffsets, default latest)."
            value={endingOffsets}
            onChange={handleChange}
            placeholder="latest"
            datalist={['latest']}
            mono={true}
          />
        )}
      </div>

      {/* Allow Different Topic Schemas Checkbox */}
      <div className="p-3 bg-slate-800/40 rounded border border-slate-700/50">
        <label htmlFor="pulsarAllowDifferentTopicSchemas" className="flex items-center gap-2 cursor-pointer">
          <input
            id="pulsarAllowDifferentTopicSchemas"
            type="checkbox"
            name="allowDifferentTopicSchemas"
            checked={allowDifferentTopicSchemas}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Allow Different Topic Schemas</span>
            <InfoTooltip text="When enabled, reads raw byte payloads across topics without enforcing a single unified Pulsar schema (option allowDifferentTopicSchemas)." />
          </div>
        </label>
      </div>

      {/* Secret Manager Auth */}
      <SecretManagerAuthFields
        idPrefix="pulsarSource"
        datalistId="pulsar-source-secrets-list"
        authMode={authMode}
        secretName={secretName}
        serviceName="Apache Pulsar"
        secretPlaceholder="e.g. pulsar-jwt-token"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="pulsar-instructions"
        value={systemInstructions}
        tooltip="Custom instructions passed to the AI code generator for this Pulsar source node."
        placeholder="e.g. Parse Pulsar payload and extract __publishTime for event-time watermarking."
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

/** Alias matching the canonical Spark transform name `PulsarSource`. */
export const PulsarSourceProperties = PulsarIOSourceProperties;
