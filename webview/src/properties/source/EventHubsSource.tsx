/**
 * EventHubsSource Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Azure Event Hubs Source nodes (`EventHubsSource`)
 * in Apache Spark 4.x pipelines. Supports consuming via the Azure Event Hubs Kafka-compatible
 * endpoint (`.format("kafka")` on `<namespace>.servicebus.windows.net:9093` with SASL_SSL PLAIN
 * `$ConnectionString`) or the native `.format("eventhubs")` connector.
 *
 * Architectural Decisions:
 * - Supports `readMode` (`'streaming' | 'batch'`, default `'streaming'`).
 * - Supports `connectorMode` (`'kafka_endpoint' | 'eventhubs'`, default `'kafka_endpoint'`).
 * - Configures `eventHubNamespace`, `eventHubName`, `consumerGroup` (default `'$Default'`),
 *   `startingOffsets` (`'latest' | 'earliest'`), and `maxOffsetsPerTrigger`.
 * - Integrates `SecretManagerAuthFields` for the Event Hubs Connection String or SAS key stored
 *   in Google Cloud Secret Manager.
 * - Uses shared primitives on the 8pt grid system.
 */

import React from 'react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { SecretManagerAuthFields } from '../../components/common/SecretManagerAuthFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Read mode options for EventHubsSource. */
export const EVENTHUBS_READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'streaming', label: 'Streaming (spark.readStream)' },
  { value: 'batch', label: 'Batch (spark.read)' },
];

/** Connector transport options for Azure Event Hubs in Spark 4.x. */
export const EVENTHUBS_CONNECTOR_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'kafka_endpoint', label: 'Kafka Endpoint (.format("kafka") on port 9093)' },
  { value: 'eventhubs', label: 'Native Event Hubs (.format("eventhubs"))' },
];

/** Starting offsets options for Event Hubs reads. */
export const EVENTHUBS_STARTING_OFFSETS_OPTIONS: SelectFieldOption[] = [
  { value: 'latest', label: 'latest (Read new incoming events)' },
  { value: 'earliest', label: 'earliest (Read from start of retention)' },
];

/** React Flow node shape for the EventHubsSource panel. */
export interface EventHubsSourceNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      readMode?: 'streaming' | 'batch';
      connectorMode?: 'kafka_endpoint' | 'eventhubs';
      eventHubNamespace?: string;
      eventHubName?: string;
      consumerGroup?: string;
      startingOffsets?: 'latest' | 'earliest' | string;
      maxOffsetsPerTrigger?: string | number;
      authMode?: string;
      secretName?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: any;
    };
  };
}

/** Props for the {@link EventHubsSourceProperties} component. */
export interface EventHubsSourcePropertiesProps {
  node: EventHubsSourceNode;
  onChange: (properties: any) => void;
}

/**
 * EventHubsSourceProperties renders the configuration panel for Azure Event Hubs reads in Spark.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React component.
 */
export const EventHubsSourceProperties: React.FC<EventHubsSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const readMode = properties.readMode || 'streaming';
  const connectorMode = properties.connectorMode || 'kafka_endpoint';
  const eventHubNamespace = properties.eventHubNamespace || '';
  const eventHubName = properties.eventHubName || '';
  const consumerGroup = properties.consumerGroup !== undefined ? properties.consumerGroup : '$Default';
  const startingOffsets = properties.startingOffsets || 'latest';
  const maxOffsetsPerTrigger = properties.maxOffsetsPerTrigger ?? '';
  const authMode = properties.authMode || 'secretManager';
  const secretName = properties.secretName || '';

  const systemInstructions = resolveSystemInstructions(node);

  const isNamespaceInvalid = !eventHubNamespace.trim();
  const isEventHubNameInvalid = !eventHubName.trim();

  /**
   * Handles input and select changes.
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

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Read Mode Select */}
      <SelectField
        id="eventHubsReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select streaming mode (spark.readStream) or batch mode (spark.read) for Azure Event Hubs ingestion."
        value={readMode}
        options={EVENTHUBS_READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Connector Mode Select */}
      <SelectField
        id="eventHubsConnectorMode"
        name="connectorMode"
        label="Connector Mode"
        tooltip="Use the built-in Spark Kafka connector against the Event Hubs Kafka surface (<namespace>.servicebus.windows.net:9093) or the native Event Hubs connector."
        value={connectorMode}
        options={EVENTHUBS_CONNECTOR_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Event Hubs Namespace (Required) */}
      <PropertyField
        id="eventHubNamespace"
        name="eventHubNamespace"
        label="Event Hubs Namespace"
        required="Event Hubs Namespace is required."
        tooltip="Azure Service Bus namespace name or FQDN (e.g. my-telemetry-ns or my-telemetry-ns.servicebus.windows.net). Required."
        value={eventHubNamespace}
        onChange={handleChange}
        placeholder="my-telemetry-ns"
        invalid={isNamespaceInvalid}
        error={isNamespaceInvalid ? 'Event Hubs Namespace is required.' : null}
        mono={true}
      />

      {/* Event Hub Name (Required) */}
      <PropertyField
        id="eventHubName"
        name="eventHubName"
        label="Event Hub Name (Topic)"
        required="Event Hub Name is required."
        tooltip="Name of the specific stream entity within the namespace to consume from. Required."
        value={eventHubName}
        onChange={handleChange}
        placeholder="device-events"
        invalid={isEventHubNameInvalid}
        error={isEventHubNameInvalid ? 'Event Hub Name is required.' : null}
        mono={true}
      />

      {/* Consumer Group */}
      <PropertyField
        id="eventHubsConsumerGroup"
        name="consumerGroup"
        label="Consumer Group"
        tooltip="Azure stream reader group identifier (defaults to $Default)."
        value={consumerGroup}
        onChange={handleChange}
        placeholder="$Default"
        mono={true}
      />

      {/* Starting Offsets & Max Offsets Per Trigger */}
      <div className="grid grid-cols-2 gap-3">
        <SelectField
          id="eventHubsStartingOffsets"
          name="startingOffsets"
          label="Starting Offsets"
          tooltip="Starting offset position when no checkpoint exists (latest or earliest)."
          value={startingOffsets}
          options={EVENTHUBS_STARTING_OFFSETS_OPTIONS}
          onChange={handleChange}
        />

        <PropertyField
          id="eventHubsMaxOffsetsPerTrigger"
          name="maxOffsetsPerTrigger"
          label="Max Offsets Per Trigger"
          type="number"
          min="1"
          tooltip="Maximum number of events processed per micro-batch trigger (option maxOffsetsPerTrigger)."
          value={maxOffsetsPerTrigger}
          onChange={handleChange}
          placeholder="e.g. 10000"
          mono={true}
        />
      </div>

      {/* Secret Manager Auth for Event Hubs Connection String / SAS Key */}
      <SecretManagerAuthFields
        idPrefix="eventHubsSource"
        datalistId="eventhubs-source-secrets-list"
        authMode={authMode}
        secretName={secretName}
        serviceName="Azure Event Hubs"
        secretPlaceholder="e.g. eventhubs-connection-string"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="eventHubsSourceSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Event Hubs source node."
        placeholder="Enter custom instructions for Event Hubs source code generation..."
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
