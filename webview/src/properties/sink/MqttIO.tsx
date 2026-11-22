/**
 * @fileoverview MqttIO Sink Properties Component for Apache Spark 4.x pipelines.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark MqttSink (`MqttIO`) nodes.
 * Configures batch (`mapInPandas` / `foreachPartition`) and streaming (`foreachBatch`) MQTT
 * publishers using `paho-mqtt` per Spark partition.
 *
 * Architectural Decisions:
 * - Configures `brokerUrl` (with fallback from legacy `serverUri`), `topic`, and dynamic `topicColumn`.
 * - Warns when `topic` contains MQTT subscription wildcards (`+` or `#`) that cannot be published to.
 * - Exposes selectable `qos` (`0`, `1`, `2`), `retain`, `tlsEnabled`, `clientId`, and `keepAliveSeconds`.
 * - Integrates `SecretManagerAuthFields` for broker passwords and strips legacy cleartext `password` fields.
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { AlertTriangle, ShieldCheck } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { SecretManagerAuthFields } from '../../components/common/SecretManagerAuthFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';
import { MQTT_QOS_OPTIONS } from '../source/MqttIO';

/** Payload encoding modes for MqttSink messages. */
export const MQTT_PAYLOAD_FORMAT_OPTIONS: SelectFieldOption[] = [
  { value: 'json', label: 'JSON Object (Serialize full row)' },
  { value: 'string', label: 'String Column (UTF-8)' },
  { value: 'binary', label: 'Binary Column (Raw bytes)' },
];

/** React Flow node shape for the MqttSink panel. */
export interface MqttIOSinkNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      brokerUrl?: string;
      serverUri?: string;
      topic?: string;
      topicColumn?: string;
      payloadFormat?: 'json' | 'string' | 'binary';
      payloadColumn?: string;
      clientId?: string;
      qos?: 0 | 1 | 2 | '0' | '1' | '2';
      retain?: boolean;
      tlsEnabled?: boolean;
      keepAliveSeconds?: number | string;
      keepAlive?: number | string;
      username?: string;
      password?: string;
      authMode?: string;
      secretName?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link MqttIOSinkProperties} component. */
export interface MqttIOSinkPropertiesProps {
  node: MqttIOSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the MQTT Spark 4.x sink property configuration form.
 *
 * @param props Active node metadata and property change callback.
 * @returns Rendered MqttSink property form.
 */
export const MqttIOSinkProperties: React.FC<MqttIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const brokerUrl = properties.brokerUrl ?? properties.serverUri ?? '';
  const topic = properties.topic || '';
  const topicColumn = properties.topicColumn || '';
  const payloadFormat = properties.payloadFormat || 'json';
  const payloadColumn = properties.payloadColumn || '';
  const clientId = properties.clientId || '';
  const qos = properties.qos !== undefined ? String(properties.qos) : '1';
  const retain = properties.retain ?? false;
  const tlsEnabled = properties.tlsEnabled ?? brokerUrl.startsWith('ssl://');
  const keepAliveSeconds = properties.keepAliveSeconds ?? properties.keepAlive ?? '60';
  const username = properties.username || '';
  const authMode = properties.authMode || (properties.secretName ? 'secretManager' : 'default');
  const secretName = properties.secretName || '';

  const systemInstructions = resolveSystemInstructions(node);

  const isBrokerUrlMissing = !brokerUrl.trim();
  const isTopicMissing = !topic.trim() && !topicColumn.trim();
  const hasWildcardInTopic = topic.includes('+') || topic.includes('#');
  const isPayloadColumnRequired = payloadFormat === 'string' || payloadFormat === 'binary';
  const isPayloadColumnInvalid = isPayloadColumnRequired && !payloadColumn.trim();

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const isCheckbox = type === 'checkbox';
    const checked = isCheckbox ? (e.target as HTMLInputElement).checked : undefined;
    const nextValue = name === 'qos' ? Number(value) : isCheckbox ? checked : value;

    const nextProperties: Record<string, unknown> = {
      ...properties,
      [name]: nextValue,
    };
    delete nextProperties.password;

    onChange(nextProperties);
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Broker URL (Required) */}
      <PropertyField
        id="mqttSinkBrokerUrl"
        name="brokerUrl"
        label="Broker URL"
        required="Broker URL is required."
        tooltip="Connection URI for the destination MQTT broker (for example tcp://broker.hivemq.com:1883 or ssl://broker:8883). Required."
        value={brokerUrl}
        onChange={handleChange}
        placeholder="tcp://localhost:1883"
        invalid={isBrokerUrlMissing}
        error={isBrokerUrlMissing ? 'Broker URL is required.' : null}
        mono={true}
      />

      {/* Destination Publish Topic */}
      <PropertyField
        id="mqttSinkTopic"
        name="topic"
        label="Publish Topic"
        required={!topicColumn.trim() ? 'Publish topic is required.' : false}
        tooltip="Concrete MQTT destination path where records publish. Wildcards (+ or #) are not valid for publishing."
        value={topic}
        onChange={handleChange}
        placeholder="telemetry/device/status"
        invalid={isTopicMissing || hasWildcardInTopic}
        error={isTopicMissing ? 'Publish topic is required.' : null}
        mono={true}
      >
        {hasWildcardInTopic && (
          <div className="p-2.5 rounded bg-amber-500/10 border border-amber-500/30 text-xs text-amber-400 flex items-start gap-2">
            <AlertTriangle size={14} className="shrink-0 mt-0.5" />
            <div>
              <span className="font-bold">Wildcard detected: </span>
              MQTT publishers cannot send messages to wildcard filters (
              <code className="text-amber-300 bg-amber-500/20 px-1 py-0.5 rounded font-mono">+</code>{' '}
              or{' '}
              <code className="text-amber-300 bg-amber-500/20 px-1 py-0.5 rounded font-mono">#</code>
              ). Specify a concrete target path.
            </div>
          </div>
        )}
      </PropertyField>

      {/* Payload Serialization and Columns */}
      <SelectField
        id="mqttSinkPayloadFormat"
        name="payloadFormat"
        label="Payload Format"
        tooltip="Choose how each DataFrame row encodes into outgoing MQTT message bytes."
        value={payloadFormat}
        options={MQTT_PAYLOAD_FORMAT_OPTIONS}
        onChange={handleChange}
      />

      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="mqttSinkPayloadColumn"
          name="payloadColumn"
          label="Payload Column"
          required={isPayloadColumnRequired ? 'Payload Column is required.' : false}
          tooltip="Source column holding the message body. If JSON mode is selected and this field is blank, all columns serialize into JSON."
          value={payloadColumn}
          onChange={handleChange}
          placeholder={payloadFormat === 'json' ? 'Full row (optional)' : 'payload'}
          invalid={isPayloadColumnInvalid}
          error={isPayloadColumnInvalid ? 'Payload Column is required.' : null}
          mono={true}
        />

        <PropertyField
          id="mqttSinkTopicColumn"
          name="topicColumn"
          label="Dynamic Topic Column"
          tooltip="Optional DataFrame string column overriding the destination MQTT topic per row."
          value={topicColumn}
          onChange={handleChange}
          placeholder="e.g. target_topic"
          mono={true}
        />
      </div>

      {/* Quality of Service (QoS) */}
      <SelectField
        id="mqttSinkQos"
        name="qos"
        label="QoS (Quality of Service)"
        tooltip="MQTT delivery assurance level: 0 (At Most Once), 1 (At Least Once), or 2 (Exactly Once)."
        value={qos}
        options={MQTT_QOS_OPTIONS}
        onChange={handleChange}
      />

      {/* Client ID Prefix and Keep Alive */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="mqttSinkClientId"
          name="clientId"
          label="Client ID Prefix"
          tooltip="Prefix combined with a per-partition UUID suffix so concurrent Spark tasks maintain distinct MQTT sessions."
          value={clientId}
          onChange={handleChange}
          placeholder="spark-mqtt-publisher"
          mono={true}
        />

        <PropertyField
          id="mqttSinkKeepAliveSeconds"
          name="keepAliveSeconds"
          label="Keep Alive (s)"
          type="number"
          min="1"
          tooltip="Maximum interval in seconds between MQTT broker ping packets (default 60)."
          value={keepAliveSeconds}
          onChange={handleChange}
          placeholder="60"
          mono={true}
        />
      </div>

      {/* Retain and TLS Toggles */}
      <div className="p-3 bg-slate-800/40 rounded border border-slate-700/50 flex flex-col gap-2.5">
        <label htmlFor="mqttSinkRetain" className="flex items-center gap-2 cursor-pointer">
          <input
            id="mqttSinkRetain"
            type="checkbox"
            name="retain"
            checked={retain}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Retain Messages</span>
            <InfoTooltip text="Instruct the MQTT broker to persist the latest message on each topic as the retained value for new subscribers." />
          </div>
        </label>

        <label htmlFor="mqttSinkTlsEnabled" className="flex items-center gap-2 cursor-pointer">
          <input
            id="mqttSinkTlsEnabled"
            type="checkbox"
            name="tlsEnabled"
            checked={tlsEnabled}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Enable TLS Encryption</span>
            <InfoTooltip text="Configure client.tls_set() on paho-mqtt before connecting to TLS-protected MQTT brokers." />
          </div>
        </label>
      </div>

      {/* Authentication via Secret Manager */}
      <div className="p-3 bg-[var(--vscode-subcard-bg)] rounded border border-[var(--vscode-border)] flex flex-col gap-3">
        <div className="flex items-center gap-2 pb-1 border-b border-[var(--vscode-border-light)]">
          <ShieldCheck className="w-4 h-4 text-emerald-400" />
          <span className="text-xs font-bold uppercase tracking-wide text-[var(--vscode-text-primary)]">
            Broker Authentication
          </span>
        </div>

        <PropertyField
          id="mqttSinkUsername"
          name="username"
          label="Broker Username"
          tooltip="Optional MQTT broker username paired with the password stored in Google Cloud Secret Manager."
          value={username}
          onChange={handleChange}
          placeholder="e.g. iot-publisher"
          mono={true}
        />

        <SecretManagerAuthFields
          idPrefix="mqttSink"
          datalistId="mqtt-sink-secrets-list"
          authMode={authMode}
          secretName={secretName}
          serviceName="MQTT Broker"
          secretPlaceholder="e.g. mqtt-broker-password"
          onChange={handleChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="mqtt-sink-instructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this MQTT sink node."
        placeholder="Enter custom instructions for MQTT sink code generation..."
        onChange={(value) => {
          const nextProperties: Record<string, unknown> = {
            ...properties,
            system_instructions: value,
          };
          delete nextProperties.password;
          onChange(nextProperties);
        }}
      />
    </div>
  );
};

/** Alias matching the canonical Spark transform name `MqttSink`. */
export const MqttSinkProperties = MqttIOSinkProperties;
