/**
 * @fileoverview KafkaIO Sink Properties Component for Apache Spark 4.x pipelines.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark KafkaSink (`KafkaIO`) nodes.
 * Configures streaming (`df.writeStream.format("kafka")`) and batch (`df.write.format("kafka")`)
 * writes using the `spark-sql-kafka-0-10` connector.
 *
 * Architectural Decisions:
 * - Maps DataFrame columns to the Spark Kafka envelope (`key`, `value`, `topic`, `partition`, and `headers`).
 * - Configures producer reliability (`acks`, `enableIdempotence`) and payload compression (`compressionType`).
 * - Integrates `SecretManagerAuthFields` and SASL or SSL protocol options so credentials resolve via Secret Manager.
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
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';
import {
  KAFKA_SECURITY_PROTOCOL_OPTIONS,
  KAFKA_SASL_MECHANISM_OPTIONS,
} from '../source/KafkaIO';

/** Value serialization mode options for Spark KafkaSink writes. */
export const KAFKA_VALUE_FORMAT_OPTIONS: SelectFieldOption[] = [
  { value: 'json', label: 'JSON Object (F.to_json struct)' },
  { value: 'string', label: 'String Column (UTF-8)' },
  { value: 'binary', label: 'Binary Column (Raw bytes)' },
];

/** Producer acknowledgment durability options for KafkaSink. */
export const KAFKA_ACKS_OPTIONS: SelectFieldOption[] = [
  { value: 'all', label: 'all (-1) - Full ISR Durability' },
  { value: '1', label: '1 - Leader Acknowledgment' },
  { value: '0', label: '0 - No Guarantee (Fire and Forget)' },
];

/** Producer compression codec options for KafkaSink. */
export const KAFKA_COMPRESSION_OPTIONS: SelectFieldOption[] = [
  { value: 'none', label: 'none (Uncompressed)' },
  { value: 'snappy', label: 'snappy' },
  { value: 'lz4', label: 'lz4' },
  { value: 'zstd', label: 'zstd' },
  { value: 'gzip', label: 'gzip' },
];

/** React Flow node shape for the KafkaSink panel. */
export interface KafkaIOSinkNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      bootstrapServers?: string;
      topic?: string;
      topicColumn?: string;
      valueFormat?: 'json' | 'string' | 'binary';
      keyColumn?: string;
      valueColumn?: string;
      partitionColumn?: string;
      includeHeaders?: boolean;
      headersColumn?: string;
      acks?: 'all' | '1' | '0';
      compressionType?: 'none' | 'snappy' | 'lz4' | 'zstd' | 'gzip';
      enableIdempotence?: boolean;
      securityProtocol?: string;
      saslMechanism?: string;
      saslUsername?: string;
      authMode?: string;
      secretName?: string;
      producerProperties?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link KafkaIOSinkProperties} component. */
export interface KafkaIOSinkPropertiesProps {
  node: KafkaIOSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the Apache Spark `.format("kafka")` sink property configuration form.
 *
 * @param props Active node metadata and property change callback.
 * @returns Rendered KafkaSink property form.
 */
export const KafkaIOSinkProperties: React.FC<KafkaIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const bootstrapServers = properties.bootstrapServers || '';
  const topic = properties.topic || '';
  const topicColumn = properties.topicColumn || '';
  const valueFormat = properties.valueFormat || 'json';
  const keyColumn = properties.keyColumn || '';
  const valueColumn = properties.valueColumn || '';
  const partitionColumn = properties.partitionColumn || '';
  const includeHeaders = properties.includeHeaders ?? false;
  const headersColumn = properties.headersColumn || '';
  const acks = properties.acks || 'all';
  const compressionType = properties.compressionType || 'none';
  const enableIdempotence = properties.enableIdempotence ?? true;
  const securityProtocol = properties.securityProtocol || 'PLAINTEXT';
  const saslMechanism = properties.saslMechanism || 'PLAIN';
  const saslUsername = properties.saslUsername || '';
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';
  const producerProperties = properties.producerProperties || '';

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

  const systemInstructions = resolveSystemInstructions(node);

  const isBootstrapServersInvalid = !bootstrapServers.trim();
  const isTopicInvalid = !topic.trim() && !topicColumn.trim();
  const isValueColumnRequired = valueFormat === 'string' || valueFormat === 'binary';
  const isValueColumnInvalid = isValueColumnRequired && !valueColumn.trim();
  const showSaslMechanism = securityProtocol === 'SASL_SSL' || securityProtocol === 'SASL_PLAINTEXT';

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
      {/* Bootstrap Servers Input (Required) */}
      <PropertyField
        id="kafkaSinkBootstrapServers"
        name="bootstrapServers"
        label="Bootstrap Servers"
        required="Bootstrap Servers is required."
        tooltip="Comma-separated host:port pairs for connecting to the Kafka cluster (option kafka.bootstrap.servers). Required."
        value={bootstrapServers}
        invalid={isBootstrapServersInvalid}
        error={isBootstrapServersInvalid ? 'Bootstrap Servers is required.' : null}
        helperText={
          !isBootstrapServersInvalid ? (
            <span>
              Example: <code className="text-slate-300 bg-slate-800/60 px-1 py-0.5 rounded">localhost:9092</code>
            </span>
          ) : null
        }
        placeholder="localhost:9092"
        onChange={handleChange}
        mono={true}
      />

      {/* Target Topic Input */}
      <PropertyField
        id="kafkaSinkTopic"
        name="topic"
        label="Topic"
        required={!topicColumn.trim() ? 'Topic is required.' : false}
        tooltip="Default target Kafka topic for output records (option topic). Required if Topic Column is empty."
        value={topic}
        invalid={isTopicInvalid}
        error={isTopicInvalid ? 'Topic is required.' : null}
        helperText={
          !isTopicInvalid ? (
            <span>
              Example: <code className="text-slate-300 bg-slate-800/60 px-1 py-0.5 rounded">output-events</code>
            </span>
          ) : null
        }
        placeholder="output-topic"
        onChange={handleChange}
        mono={true}
      />

      {/* Value Serialization Format */}
      <SelectField
        id="kafkaSinkValueFormat"
        name="valueFormat"
        label="Value Serialization"
        tooltip="Select how DataFrame columns serialize into the Kafka value payload column."
        value={valueFormat}
        options={KAFKA_VALUE_FORMAT_OPTIONS}
        onChange={handleChange}
      />

      {/* Key and Value Columns */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="kafkaSinkKeyColumn"
          name="keyColumn"
          label="Key Column"
          tooltip="Optional DataFrame column cast to Kafka record key for partition routing and log compaction."
          value={keyColumn}
          onChange={handleChange}
          placeholder="e.g. order_id"
          mono={true}
        />

        <PropertyField
          id="kafkaSinkValueColumn"
          name="valueColumn"
          label="Value Column"
          required={isValueColumnRequired ? 'Value Column is required.' : false}
          tooltip="DataFrame column containing the message payload. If JSON mode is selected and this field is blank, all columns serialize into JSON."
          value={valueColumn}
          invalid={isValueColumnInvalid}
          error={isValueColumnInvalid ? 'Value Column is required.' : null}
          onChange={handleChange}
          placeholder={valueFormat === 'json' ? 'All columns (optional)' : 'value'}
          mono={true}
        />
      </div>

      {/* Producer Durability and Compression */}
      <div className="grid grid-cols-2 gap-3">
        <SelectField
          id="kafkaSinkAcks"
          name="acks"
          label="Acks (Durability)"
          tooltip="Number of broker acknowledgments required before marking a write complete (option kafka.acks)."
          value={acks}
          options={KAFKA_ACKS_OPTIONS}
          onChange={handleChange}
        />

        <SelectField
          id="kafkaSinkCompressionType"
          name="compressionType"
          label="Compression Codec"
          tooltip="Codec applied to Kafka record batches before network transmission (option kafka.compression.type)."
          value={compressionType}
          options={KAFKA_COMPRESSION_OPTIONS}
          onChange={handleChange}
        />
      </div>

      {/* Producer Toggles: Idempotence and Headers */}
      <div className="p-3 bg-slate-800/40 rounded border border-slate-700/50 flex flex-col gap-2.5">
        <label htmlFor="kafkaSinkEnableIdempotence" className="flex items-center gap-2 cursor-pointer">
          <input
            id="kafkaSinkEnableIdempotence"
            type="checkbox"
            name="enableIdempotence"
            checked={enableIdempotence}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Enable Idempotent Producer</span>
            <InfoTooltip text="Prevent duplicate Kafka messages during producer retries (option kafka.enable.idempotence)." />
          </div>
        </label>

        <label htmlFor="kafkaSinkIncludeHeaders" className="flex items-center gap-2 cursor-pointer">
          <input
            id="kafkaSinkIncludeHeaders"
            type="checkbox"
            name="includeHeaders"
            checked={includeHeaders}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Include Kafka Headers</span>
            <InfoTooltip text="Write an array<struct<key:string, value:binary>> column as Kafka record headers (option includeHeaders)." />
          </div>
        </label>

        {includeHeaders && (
          <PropertyField
            id="kafkaSinkHeadersColumn"
            name="headersColumn"
            label="Headers Column"
            tooltip="DataFrame column containing array<struct<key:string, value:binary>> Kafka headers."
            value={headersColumn}
            onChange={handleChange}
            placeholder="headers"
            mono={true}
          />
        )}
      </div>

      {/* Security and Secret Manager Auth */}
      <div className="p-3 bg-[var(--vscode-subcard-bg)] rounded border border-[var(--vscode-border)] flex flex-col gap-3">
        <div className="flex items-center gap-2 pb-1 border-b border-[var(--vscode-border-light)]">
          <ShieldCheck className="w-4 h-4 text-emerald-400" />
          <span className="text-xs font-bold uppercase tracking-wide text-[var(--vscode-text-primary)]">
            Security and Authentication
          </span>
        </div>

        <SelectField
          id="kafkaSinkSecurityProtocol"
          name="securityProtocol"
          label="Security Protocol"
          tooltip="Protocol used to communicate with Kafka brokers (option kafka.security.protocol)."
          value={securityProtocol}
          options={KAFKA_SECURITY_PROTOCOL_OPTIONS}
          onChange={handleChange}
        />

        {showSaslMechanism && (
          <>
            <SelectField
              id="kafkaSinkSaslMechanism"
              name="saslMechanism"
              label="SASL Mechanism"
              tooltip="Authentication mechanism used for SASL broker handshakes (option kafka.sasl.mechanism)."
              value={saslMechanism}
              options={KAFKA_SASL_MECHANISM_OPTIONS}
              onChange={handleChange}
            />

            <PropertyField
              id="kafkaSinkSaslUsername"
              name="saslUsername"
              label="SASL Username"
              tooltip="Optional SASL username paired with the password stored in Secret Manager. Leave blank if the secret stores the full JAAS config."
              value={saslUsername}
              onChange={handleChange}
              placeholder="e.g. spark-producer"
              mono={true}
            />
          </>
        )}

        <SecretManagerAuthFields
          idPrefix="kafkaSink"
          datalistId="kafka-sink-secrets-list"
          authMode={authMode}
          secretName={secretName}
          serviceName="Apache Kafka"
          secretPlaceholder="e.g. kafka-sasl-credentials"
          onChange={handleChange}
        />
      </div>

      {/* Advanced Producer Options */}
      <CollapsibleSection
        title="Advanced Kafka Producer Options"
        icon={<Sliders className="w-4 h-4 text-emerald-400" />}
        isOpen={isAdvancedOpen}
        onToggle={() => setIsAdvancedOpen((prev) => !prev)}
      >
        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="kafkaSinkTopicColumn"
            name="topicColumn"
            label="Topic Column (Dynamic)"
            tooltip="Optional DataFrame string column routing each row to its named Kafka topic."
            value={topicColumn}
            onChange={handleChange}
            placeholder="e.g. target_topic"
            mono={true}
          />

          <PropertyField
            id="kafkaSinkPartitionColumn"
            name="partitionColumn"
            label="Partition Column"
            tooltip="Optional DataFrame integer column specifying the exact Kafka partition per record."
            value={partitionColumn}
            onChange={handleChange}
            placeholder="e.g. partition_id"
            mono={true}
          />
        </div>

        <PropertyField
          id="kafkaProducerProperties"
          name="producerProperties"
          label="Additional Kafka Options"
          tooltip="Extra Spark Kafka producer options formatted as key=value on each line (for example kafka.linger.ms=20)."
          type="textarea"
          value={producerProperties}
          onChange={handleChange}
          placeholder={`kafka.linger.ms=20\nkafka.batch.size=32768`}
          rows={3}
          mono={true}
        />
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="kafkaSinkSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Kafka sink node."
        placeholder="Enter custom instructions for Kafka sink code generation..."
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

/** Alias matching the canonical Spark transform name `KafkaSink`. */
export const KafkaSinkProperties = KafkaIOSinkProperties;
