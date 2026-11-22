/**
 * @fileoverview EventHubsSink Properties Component for Apache Spark 4.x pipelines.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Azure Event Hubs Sink nodes (`EventHubsSink`)
 * in Apache Spark 4.x pipelines. Publishes batch (`df.write.format("kafka")`) and streaming
 * (`df.writeStream.format("kafka")`) records to `<namespace>.servicebus.windows.net:9093`
 * using SASL_SSL PLAIN `$ConnectionString` authentication.
 *
 * Architectural Decisions:
 * - Reuses `EVENTHUBS_CONNECTOR_MODE_OPTIONS` from `EventHubsSource` for consistent transport selection.
 * - Configures `eventHubNamespace`, `eventHubName`, `partitionKeyColumn`, `valueFormat`, and `valueColumn`.
 * - Exposes `acks`, `requestTimeoutMs` (default `60000` ms per Azure Event Hubs guidance), and header projection.
 * - Integrates `SecretManagerAuthFields` so SAS connection strings resolve via Google Cloud Secret Manager.
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
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';
import { EVENTHUBS_CONNECTOR_MODE_OPTIONS } from '../source/EventHubsSource';

/** Payload serialization format options for EventHubsSink. */
export const EVENTHUBS_VALUE_FORMAT_OPTIONS: SelectFieldOption[] = [
  { value: 'json', label: 'JSON Object (F.to_json struct)' },
  { value: 'string', label: 'String Column (UTF-8)' },
  { value: 'binary', label: 'Binary Column (Raw bytes)' },
];

/** Producer acknowledgment options for EventHubsSink. */
export const EVENTHUBS_ACKS_OPTIONS: SelectFieldOption[] = [
  { value: 'all', label: 'all (-1) - Full Durability' },
  { value: '1', label: '1 - Leader Acknowledgment' },
  { value: '0', label: '0 - Fire and Forget' },
];

/** React Flow node shape for the EventHubsSink panel. */
export interface EventHubsSinkNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      connectorMode?: 'kafka_endpoint' | 'eventhubs';
      eventHubNamespace?: string;
      eventHubName?: string;
      valueFormat?: 'json' | 'string' | 'binary';
      partitionKeyColumn?: string;
      valueColumn?: string;
      acks?: 'all' | '1' | '0';
      requestTimeoutMs?: string | number;
      includeHeaders?: boolean;
      headersColumn?: string;
      authMode?: string;
      secretName?: string;
      producerProperties?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link EventHubsSinkProperties} component. */
export interface EventHubsSinkPropertiesProps {
  node: EventHubsSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the Azure Event Hubs Spark 4.x sink property configuration form.
 *
 * @param props Active node metadata and property change callback.
 * @returns Rendered EventHubsSink property form.
 */
export const EventHubsSinkProperties: React.FC<EventHubsSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data.properties || {};

  const connectorMode = properties.connectorMode || 'kafka_endpoint';
  const eventHubNamespace = properties.eventHubNamespace || '';
  const eventHubName = properties.eventHubName || '';
  const valueFormat = properties.valueFormat || 'json';
  const partitionKeyColumn = properties.partitionKeyColumn || '';
  const valueColumn = properties.valueColumn || '';
  const acks = properties.acks || 'all';
  const requestTimeoutMs = properties.requestTimeoutMs ?? '60000';
  const includeHeaders = properties.includeHeaders ?? false;
  const headersColumn = properties.headersColumn || '';
  const authMode = properties.authMode || 'secretManager';
  const secretName = properties.secretName || '';
  const producerProperties = properties.producerProperties || '';

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

  const systemInstructions = resolveSystemInstructions(node);

  const isNamespaceInvalid = !eventHubNamespace.trim();
  const isEventHubNameInvalid = !eventHubName.trim();
  const isValueColumnRequired = valueFormat === 'string' || valueFormat === 'binary';
  const isValueColumnInvalid = isValueColumnRequired && !valueColumn.trim();

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
      {/* Connector Mode Select */}
      <SelectField
        id="eventHubsSinkConnectorMode"
        name="connectorMode"
        label="Connector Mode"
        tooltip="Publish through the built-in Spark Kafka writer on <namespace>.servicebus.windows.net:9093 or the native Event Hubs format."
        value={connectorMode}
        options={EVENTHUBS_CONNECTOR_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Event Hubs Namespace (Required) */}
      <PropertyField
        id="eventHubSinkNamespace"
        name="eventHubNamespace"
        label="Event Hubs Namespace"
        required="Event Hubs Namespace is required."
        tooltip="Azure Service Bus namespace name or FQDN (for example my-telemetry-ns or my-telemetry-ns.servicebus.windows.net). Required."
        value={eventHubNamespace}
        onChange={handleChange}
        placeholder="my-telemetry-ns"
        invalid={isNamespaceInvalid}
        error={isNamespaceInvalid ? 'Event Hubs Namespace is required.' : null}
        mono={true}
      />

      {/* Event Hub Name (Required) */}
      <PropertyField
        id="eventHubSinkName"
        name="eventHubName"
        label="Event Hub Name (Topic)"
        required="Event Hub Name is required."
        tooltip="Destination Event Hub entity name receiving published events (mapped to Kafka option topic). Required."
        value={eventHubName}
        onChange={handleChange}
        placeholder="device-events"
        invalid={isEventHubNameInvalid}
        error={isEventHubNameInvalid ? 'Event Hub Name is required.' : null}
        mono={true}
      />

      {/* Payload Serialization Format */}
      <SelectField
        id="eventHubsSinkValueFormat"
        name="valueFormat"
        label="Value Serialization"
        tooltip="Choose how Spark DataFrame columns encode into the outgoing Event Hubs payload."
        value={valueFormat}
        options={EVENTHUBS_VALUE_FORMAT_OPTIONS}
        onChange={handleChange}
      />

      {/* Partition Key and Value Columns */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="eventHubsSinkPartitionKeyColumn"
          name="partitionKeyColumn"
          label="Partition Key Column"
          tooltip="Optional DataFrame column projected as the Kafka record key for deterministic Event Hubs partition assignment."
          value={partitionKeyColumn}
          onChange={handleChange}
          placeholder="e.g. device_id"
          mono={true}
        />

        <PropertyField
          id="eventHubsSinkValueColumn"
          name="valueColumn"
          label="Value Column"
          required={isValueColumnRequired ? 'Value Column is required.' : false}
          tooltip="DataFrame column holding the message body. If JSON mode is selected and this field is blank, all columns serialize into JSON."
          value={valueColumn}
          onChange={handleChange}
          placeholder={valueFormat === 'json' ? 'Full row (optional)' : 'value'}
          invalid={isValueColumnInvalid}
          error={isValueColumnInvalid ? 'Value Column is required.' : null}
          mono={true}
        />
      </div>

      {/* Durability and Timeout */}
      <div className="grid grid-cols-2 gap-3">
        <SelectField
          id="eventHubsSinkAcks"
          name="acks"
          label="Acks (Durability)"
          tooltip="Broker acknowledgment level required before marking an Event Hubs publish complete (option kafka.acks)."
          value={acks}
          options={EVENTHUBS_ACKS_OPTIONS}
          onChange={handleChange}
        />

        <PropertyField
          id="eventHubsSinkRequestTimeoutMs"
          name="requestTimeoutMs"
          label="Request Timeout (ms)"
          type="number"
          min="1000"
          tooltip="Producer request timeout in milliseconds recommended by Azure Event Hubs for Kafka (option kafka.request.timeout.ms, default 60000)."
          value={requestTimeoutMs}
          onChange={handleChange}
          placeholder="60000"
          mono={true}
        />
      </div>

      {/* Custom Properties / Headers */}
      <div className="p-3 bg-slate-800/40 rounded border border-slate-700/50 flex flex-col gap-2.5">
        <label htmlFor="eventHubsSinkIncludeHeaders" className="flex items-center gap-2 cursor-pointer">
          <input
            id="eventHubsSinkIncludeHeaders"
            type="checkbox"
            name="includeHeaders"
            checked={includeHeaders}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Include Event Headers</span>
            <InfoTooltip text="Forward an array<struct<key:string, value:binary>> column as Event Hubs user properties (option includeHeaders)." />
          </div>
        </label>

        {includeHeaders && (
          <PropertyField
            id="eventHubsSinkHeadersColumn"
            name="headersColumn"
            label="Headers Column"
            tooltip="DataFrame column containing array<struct<key:string, value:binary>> custom event properties."
            value={headersColumn}
            onChange={handleChange}
            placeholder="headers"
            mono={true}
          />
        )}
      </div>

      {/* SASL_SSL $ConnectionString Auth via Secret Manager */}
      <div className="p-3 bg-[var(--vscode-subcard-bg)] rounded border border-[var(--vscode-border)] flex flex-col gap-3">
        <div className="flex items-center gap-2 pb-1 border-b border-[var(--vscode-border-light)]">
          <ShieldCheck className="w-4 h-4 text-emerald-400" />
          <span className="text-xs font-bold uppercase tracking-wide text-[var(--vscode-text-primary)]">
            SASL_SSL Connection String Authentication
          </span>
        </div>

        <SecretManagerAuthFields
          idPrefix="eventHubsSink"
          datalistId="eventhubs-sink-secrets-list"
          authMode={authMode}
          secretName={secretName}
          serviceName="Azure Event Hubs"
          secretPlaceholder="e.g. eventhubs-connection-string"
          onChange={handleChange}
        />
      </div>

      {/* Advanced Producer Options */}
      <CollapsibleSection
        title="Advanced Event Hubs Options"
        icon={<Sliders className="w-4 h-4 text-emerald-400" />}
        isOpen={isAdvancedOpen}
        onToggle={() => setIsAdvancedOpen((prev) => !prev)}
      >
        <PropertyField
          id="eventHubsSinkProducerProperties"
          name="producerProperties"
          label="Additional Kafka Options"
          tooltip="Extra Kafka producer options formatted as key=value per line (for example kafka.linger.ms=50)."
          type="textarea"
          value={producerProperties}
          onChange={handleChange}
          placeholder={`kafka.linger.ms=50\nkafka.max.request.size=1048576`}
          rows={3}
          mono={true}
        />
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="eventHubsSinkSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Event Hubs sink node."
        placeholder="Enter custom instructions for Event Hubs sink code generation..."
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

/** Alias matching legacy `EventHubsIOSinkProperties` naming. */
export const EventHubsIOSinkProperties = EventHubsSinkProperties;
