/**
 * @fileoverview PulsarIO Sink Properties Component for Apache Spark 4.x pipelines.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark PulsarSink (`PulsarIO`) nodes.
 * Configures streaming (`df.writeStream.format("pulsar")`) and batch (`df.write.format("pulsar")`)
 * writes using the StreamNative `pulsar-spark-connector`.
 *
 * Architectural Decisions:
 * - Configures `serviceUrl` (`service.url`), `adminUrl` (`admin.url`), and `topic` (or dynamic `__topic` column).
 * - Maps DataFrame columns to Pulsar metadata (`__key`, `__eventTime`) and payload serialization modes.
 * - Supports JWT Token and OAuth2 client credentials resolved via Google Cloud Secret Manager (`SecretManagerAuthFields`).
 * - Uses shared `PropertyField`, `SelectField`, `CollapsibleSection`, and `SystemInstructionsField` primitives.
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import { Sliders, ShieldCheck } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { SecretManagerAuthFields } from '../../components/common/SecretManagerAuthFields';
import type { AuthModeOption } from '../../components/common/SecretManagerAuthFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Canonical regex matching full Apache Pulsar persistent or non-persistent topic URIs. */
export const PULSAR_TOPIC_REGEX =
  /^(persistent|non-persistent):\/\/[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+$/;

/** Payload encoding modes for Spark PulsarSink writes. */
export const PULSAR_PAYLOAD_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'json', label: 'JSON Value (F.to_json struct -> value)' },
  { value: 'schema', label: 'Structured Spark Schema (Pulsar Schema)' },
  { value: 'string', label: 'String Column (UTF-8 value)' },
  { value: 'binary', label: 'Binary Column (Raw bytes value)' },
];

/** Producer compression codec options for PulsarSink. */
export const PULSAR_COMPRESSION_OPTIONS: SelectFieldOption[] = [
  { value: 'NONE', label: 'NONE (Uncompressed)' },
  { value: 'LZ4', label: 'LZ4' },
  { value: 'ZSTD', label: 'ZSTD' },
  { value: 'SNAPPY', label: 'SNAPPY' },
  { value: 'ZLIB', label: 'ZLIB' },
];

/** Authentication mode options for Pulsar broker connections. */
export const PULSAR_AUTH_MODE_OPTIONS: AuthModeOption[] = [
  { value: 'none', label: 'No Authentication' },
  { value: 'token', label: 'JWT Token (Google Cloud Secret Manager)' },
  { value: 'oauth2', label: 'OAuth2 Credentials (Google Cloud Secret Manager)' },
];

/** React Flow node shape for the PulsarSink panel. */
export interface PulsarIOSinkNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      serviceUrl?: string;
      clientUrl?: string;
      adminUrl?: string;
      topic?: string;
      topicColumn?: string;
      payloadMode?: 'json' | 'schema' | 'string' | 'binary';
      keyColumn?: string;
      valueColumn?: string;
      eventTimeColumn?: string;
      compressionType?: 'NONE' | 'LZ4' | 'ZSTD' | 'SNAPPY' | 'ZLIB';
      batchingEnabled?: boolean;
      authMode?: 'none' | 'token' | 'oauth2' | string;
      secretName?: string;
      tokenSecretName?: string;
      oauthKeySecretName?: string;
      oauthIssuerUrl?: string;
      oauthAudience?: string;
      producerProperties?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link PulsarIOSinkProperties} component. */
export interface PulsarIOSinkPropertiesProps {
  node: PulsarIOSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the Apache Pulsar `.format("pulsar")` sink property configuration form.
 *
 * @param props Active node metadata and property change callback.
 * @returns Rendered PulsarSink property form.
 */
export const PulsarIOSinkProperties: React.FC<PulsarIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const serviceUrl = properties.serviceUrl ?? properties.clientUrl ?? '';
  const adminUrl = properties.adminUrl || '';
  const topic = properties.topic || '';
  const topicColumn = properties.topicColumn || '';
  const payloadMode = properties.payloadMode || 'json';
  const keyColumn = properties.keyColumn || '';
  const valueColumn = properties.valueColumn || '';
  const eventTimeColumn = properties.eventTimeColumn || '';
  const compressionType = properties.compressionType || 'NONE';
  const batchingEnabled = properties.batchingEnabled ?? true;
  const authMode = properties.authMode || 'none';
  const secretName =
    properties.secretName || properties.tokenSecretName || properties.oauthKeySecretName || '';
  const oauthIssuerUrl = properties.oauthIssuerUrl || '';
  const oauthAudience = properties.oauthAudience || '';
  const producerProperties = properties.producerProperties || '';

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

  const systemInstructions = resolveSystemInstructions(node);

  const isServiceUrlInvalid = !serviceUrl.trim();
  const trimmedTopic = topic.trim();
  const isTopicMissing = !trimmedTopic && !topicColumn.trim();
  const isTopicSchemeInvalid =
    trimmedTopic.includes('://') && !PULSAR_TOPIC_REGEX.test(trimmedTopic);
  const topicError = isTopicMissing
    ? 'Pulsar destination topic is required.'
    : isTopicSchemeInvalid
      ? 'Use persistent://tenant/namespace/topic or non-persistent://tenant/namespace/topic.'
      : null;

  const isValueColumnRequired = payloadMode === 'string' || payloadMode === 'binary';
  const isValueColumnInvalid = isValueColumnRequired && !valueColumn.trim();
  const isOAuth2Mode = authMode === 'oauth2';
  const isOAuthIssuerInvalid = isOAuth2Mode && !oauthIssuerUrl.trim();
  const isOAuthAudienceInvalid = isOAuth2Mode && !oauthAudience.trim();

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

  const activeSecretMode = authMode === 'oauth2' ? 'oauth2' : 'token';

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Broker Service URL (Required) */}
      <PropertyField
        id="pulsarSinkServiceUrl"
        name="serviceUrl"
        label="Pulsar Service URL"
        required="Pulsar Service URL is required."
        tooltip="Broker binary protocol endpoint passed as service.url (for example pulsar://localhost:6650 or pulsar+ssl://broker:6651). Required."
        value={serviceUrl}
        onChange={handleChange}
        placeholder="pulsar://localhost:6650"
        invalid={isServiceUrlInvalid}
        error={isServiceUrlInvalid ? 'Pulsar Service URL is required.' : null}
        mono={true}
      />

      {/* Admin HTTP URL */}
      <PropertyField
        id="pulsarSinkAdminUrl"
        name="adminUrl"
        label="Pulsar Admin URL"
        tooltip="Optional web service endpoint used for topic metadata and schema registry lookups (option admin.url)."
        value={adminUrl}
        onChange={handleChange}
        placeholder="http://localhost:8080"
        mono={true}
      />

      {/* Destination Topic */}
      <PropertyField
        id="pulsarSinkTopic"
        name="topic"
        label="Destination Pulsar Topic"
        required={!topicColumn.trim() ? 'Pulsar destination topic is required.' : false}
        tooltip="Target Pulsar topic URI or short topic name (option topic). Required if Dynamic Topic Column is empty."
        value={topic}
        onChange={handleChange}
        placeholder="persistent://public/default/alerts"
        invalid={isTopicMissing || isTopicSchemeInvalid}
        error={topicError}
        mono={true}
      />

      {/* Payload Serialization Mode */}
      <SelectField
        id="pulsarSinkPayloadMode"
        name="payloadMode"
        label="Payload Serialization"
        tooltip="Select whether rows serialize to a JSON value column, structured Pulsar schema, UTF-8 string, or raw bytes."
        value={payloadMode}
        options={PULSAR_PAYLOAD_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* __key and Value Columns */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="pulsarSinkKeyColumn"
          name="keyColumn"
          label="Message Key Column (__key)"
          tooltip="Optional DataFrame column projected to __key for Pulsar key-shared routing and compaction."
          value={keyColumn}
          onChange={handleChange}
          placeholder="e.g. customer_id"
          mono={true}
        />

        <PropertyField
          id="pulsarSinkValueColumn"
          name="valueColumn"
          label="Value Column"
          required={isValueColumnRequired ? 'Value Column is required.' : false}
          tooltip="Source column mapped to value when String or Binary encoding is selected."
          value={valueColumn}
          onChange={handleChange}
          placeholder={payloadMode === 'string' || payloadMode === 'binary' ? 'value' : 'Optional'}
          invalid={isValueColumnInvalid}
          error={isValueColumnInvalid ? 'Value Column is required.' : null}
          mono={true}
        />
      </div>

      {/* Event Time and Compression */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="pulsarSinkEventTimeColumn"
          name="eventTimeColumn"
          label="Event Time Column"
          tooltip="Optional timestamp column projected to __eventTime on each outgoing Pulsar message."
          value={eventTimeColumn}
          onChange={handleChange}
          placeholder="e.g. event_ts"
          mono={true}
        />

        <SelectField
          id="pulsarSinkCompressionType"
          name="compressionType"
          label="Compression Codec"
          tooltip="Producer compression algorithm applied to Pulsar batches (option pulsar.producer.compressionType)."
          value={compressionType}
          options={PULSAR_COMPRESSION_OPTIONS}
          onChange={handleChange}
        />
      </div>

      {/* Authentication via Secret Manager */}
      <div className="p-3 bg-[var(--vscode-subcard-bg)] rounded border border-[var(--vscode-border)] flex flex-col gap-3">
        <div className="flex items-center gap-2 pb-1 border-b border-[var(--vscode-border-light)]">
          <ShieldCheck className="w-4 h-4 text-emerald-400" />
          <span className="text-xs font-bold uppercase tracking-wide text-[var(--vscode-text-primary)]">
            Pulsar Authentication
          </span>
        </div>

        <SecretManagerAuthFields
          idPrefix="pulsarSink"
          datalistId="pulsar-sink-secrets-list"
          authMode={authMode}
          secretName={secretName}
          serviceName="Apache Pulsar"
          authOptions={PULSAR_AUTH_MODE_OPTIONS}
          secretManagerValue={activeSecretMode}
          secretPlaceholder={
            isOAuth2Mode ? 'e.g. pulsar-oauth2-credentials-json' : 'e.g. pulsar-jwt-token'
          }
          onChange={handleChange}
        />

        {isOAuth2Mode && (
          <div className="grid grid-cols-2 gap-3">
            <PropertyField
              id="pulsarSinkOauthIssuerUrl"
              name="oauthIssuerUrl"
              label="OAuth2 Issuer URL"
              required="OAuth2 Issuer URL is required."
              tooltip="Token issuer URL for Pulsar AuthenticationOAuth2 (for example https://auth.streamnative.cloud/)."
              value={oauthIssuerUrl}
              onChange={handleChange}
              placeholder="https://auth.streamnative.cloud/"
              invalid={isOAuthIssuerInvalid}
              error={isOAuthIssuerInvalid ? 'OAuth2 Issuer URL is required.' : null}
              mono={true}
            />

            <PropertyField
              id="pulsarSinkOauthAudience"
              name="oauthAudience"
              label="OAuth2 Audience"
              required="OAuth2 Audience is required."
              tooltip="Target resource identifier for OAuth2 client credentials (for example urn:sn:pulsar:org:instance)."
              value={oauthAudience}
              onChange={handleChange}
              placeholder="urn:sn:pulsar:org:instance"
              invalid={isOAuthAudienceInvalid}
              error={isOAuthAudienceInvalid ? 'OAuth2 Audience is required.' : null}
              mono={true}
            />
          </div>
        )}
      </div>

      {/* Advanced Pulsar Producer Options */}
      <CollapsibleSection
        title="Advanced Pulsar Producer Options"
        icon={<Sliders className="w-4 h-4 text-emerald-400" />}
        isOpen={isAdvancedOpen}
        onToggle={() => setIsAdvancedOpen((prev) => !prev)}
      >
        <PropertyField
          id="pulsarSinkTopicColumn"
          name="topicColumn"
          label="Dynamic Topic Column (__topic)"
          tooltip="Optional DataFrame column projected to __topic to route records dynamically per row."
          value={topicColumn}
          onChange={handleChange}
          placeholder="e.g. destination_topic"
          mono={true}
        />

        <label htmlFor="pulsarSinkBatchingEnabled" className="flex items-center gap-2 cursor-pointer">
          <input
            id="pulsarSinkBatchingEnabled"
            type="checkbox"
            name="batchingEnabled"
            checked={batchingEnabled}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Enable Producer Batching</span>
            <InfoTooltip text="Group outgoing Pulsar messages into producer batches for higher throughput (option pulsar.producer.batchingEnabled)." />
          </div>
        </label>

        <PropertyField
          id="pulsarSinkProducerProperties"
          name="producerProperties"
          label="Additional Pulsar Options"
          tooltip="Extra pulsar.client.* or pulsar.producer.* options formatted as key=value per line."
          type="textarea"
          value={producerProperties}
          onChange={handleChange}
          placeholder={`pulsar.producer.sendTimeoutMs=30000\npulsar.producer.blockIfQueueFull=true`}
          rows={3}
          mono={true}
        />
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="pulsar-sink-instructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Pulsar sink node."
        placeholder="Enter custom instructions for Pulsar sink code generation..."
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

/** Alias matching the canonical Spark transform name `PulsarSink`. */
export const PulsarSinkProperties = PulsarIOSinkProperties;
