/**
 * MqttIO Source Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark MqttSource (`MqttIO`) nodes.
 * Configures streaming reads (`readMode: 'streaming'`) from MQTT brokers using the Spark 4.0
 * Python `DataSourceStreamReader` API (`paho-mqtt`).
 *
 * Architectural Decisions:
 * - Streaming-only source (`readMode: 'streaming'`).
 * - Configures `brokerUrl`, `topic`, `clientId`, `qos` (`0 | 1 | 2`), `cleanSession`,
 *   and `keepAliveSeconds`.
 * - Integrates `SecretManagerAuthFields` to reference broker username/password secrets in
 *   Google Cloud Secret Manager without hardcoding credentials.
 * - Uses shared primitives on the 8pt grid system.
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { Radio } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { SecretManagerAuthFields } from '../../components/common/SecretManagerAuthFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** MQTT Quality of Service options. */
export const MQTT_QOS_OPTIONS: SelectFieldOption[] = [
  { value: '0', label: '0 - At Most Once' },
  { value: '1', label: '1 - At Least Once (Default)' },
  { value: '2', label: '2 - Exactly Once' },
];

/** React Flow node shape for the MqttIO source panel. */
export interface MqttIOSourceNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      readMode?: 'streaming';
      brokerUrl?: string;
      serverUri?: string;
      topic?: string;
      clientId?: string;
      qos?: 0 | 1 | 2 | '0' | '1' | '2';
      cleanSession?: boolean;
      keepAliveSeconds?: number | string;
      authMode?: string;
      secretName?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link MqttIOSourceProperties} component. */
export interface MqttIOSourcePropertiesProps {
  node: MqttIOSourceNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * MqttIOSourceProperties renders the configuration form for Spark 4.0 MQTT streaming reads.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React component.
 */
export const MqttIOSourceProperties: React.FC<MqttIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const brokerUrl = properties.brokerUrl || properties.serverUri || '';
  const topic = properties.topic || '';
  const clientId = properties.clientId || '';
  const qos = properties.qos !== undefined ? String(properties.qos) : '1';
  const cleanSession = properties.cleanSession ?? true;
  const keepAliveSeconds = properties.keepAliveSeconds ?? '60';
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';

  const systemInstructions = resolveSystemInstructions(node);

  const isBrokerUrlMissing = !brokerUrl.trim();
  const isTopicMissing = !topic.trim();

  /**
   * Handles generic text, select, and checkbox input changes.
   *
   * @param e Form change event from input, select, or textarea.
   */
  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value, type } = e.target;
    const isCheckbox = type === 'checkbox';
    const checked = isCheckbox ? (e.target as HTMLInputElement).checked : undefined;
    const nextValue = name === 'qos' ? Number(value) : isCheckbox ? checked : value;

    onChange({
      ...properties,
      readMode: 'streaming',
      [name]: nextValue,
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

      {/* Broker URL (Required) */}
      <PropertyField
        id="mqttBrokerUrl"
        name="brokerUrl"
        label="Broker URL"
        required="Broker URL is required."
        tooltip="Connection URL for the MQTT broker (option brokerUrl, e.g. tcp://broker.hivemq.com:1883 or ssl://broker:8883). Required."
        value={brokerUrl}
        onChange={handleChange}
        placeholder="tcp://localhost:1883"
        invalid={isBrokerUrlMissing}
        error={isBrokerUrlMissing ? 'Broker URL is required.' : null}
        mono={true}
      />

      {/* Subscription Topic (Required) */}
      <PropertyField
        id="mqttTopic"
        name="topic"
        label="Subscription Topic / Filter"
        required="Subscription topic is required."
        tooltip="MQTT topic filter to subscribe to. Supports exact topics (sensors/temp) or MQTT wildcards (+ for single level, # for multi-level). Required."
        value={topic}
        onChange={handleChange}
        placeholder="sensors/+/temperature"
        invalid={isTopicMissing}
        error={isTopicMissing ? 'Subscription topic is required.' : null}
        mono={true}
      />

      {/* Client ID & Keep Alive Seconds */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="mqttClientId"
          name="clientId"
          label="Client ID"
          tooltip="Identifier for the MQTT subscriber session. Leave blank to auto-generate a unique connection identifier per executor."
          value={clientId}
          onChange={handleChange}
          placeholder="spark-mqtt-reader"
          mono={true}
        />

        <PropertyField
          id="mqttKeepAliveSeconds"
          name="keepAliveSeconds"
          label="Keep Alive (s)"
          type="number"
          min="1"
          tooltip="Maximum interval in seconds between ping packets sent to the MQTT broker (option keepAliveSeconds, default 60)."
          value={keepAliveSeconds}
          onChange={handleChange}
          placeholder="60"
          mono={true}
        />
      </div>

      {/* Quality of Service (QoS) */}
      <SelectField
        id="mqttQos"
        name="qos"
        label="QoS (Quality of Service)"
        tooltip="MQTT Quality of Service delivery level: 0 (At Most Once), 1 (At Least Once), or 2 (Exactly Once)."
        value={qos}
        options={MQTT_QOS_OPTIONS}
        onChange={handleChange}
      />

      {/* Clean Session Checkbox */}
      <div className="p-3 bg-slate-800/40 rounded border border-slate-700/50">
        <label htmlFor="mqttCleanSession" className="flex items-center gap-2 cursor-pointer">
          <input
            id="mqttCleanSession"
            type="checkbox"
            name="cleanSession"
            checked={cleanSession}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Clean Session</span>
            <InfoTooltip text="When enabled, the broker discards prior session subscriptions and queued QoS 1/2 messages upon reconnect (option cleanSession)." />
          </div>
        </label>
      </div>

      {/* Secret Manager Auth for Broker Username / Password */}
      <SecretManagerAuthFields
        idPrefix="mqttSource"
        datalistId="mqtt-source-secrets-list"
        authMode={authMode}
        secretName={secretName}
        serviceName="MQTT Broker"
        secretPlaceholder="e.g. mqtt-broker-credentials"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="mqtt-instructions"
        value={systemInstructions}
        tooltip="Custom instructions passed to the AI code generator for this MQTT source node."
        placeholder="e.g. Decode binary payload as UTF-8 JSON and parse sensor telemetry fields."
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

/** Alias matching the canonical Spark transform name `MqttSource`. */
export const MqttSourceProperties = MqttIOSourceProperties;
