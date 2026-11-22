/**
 * KafkaIO Source Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark KafkaSource (`KafkaIO`) nodes.
 * Configures streaming (`spark.readStream.format("kafka")`) and batch (`spark.read.format("kafka")`)
 * reads from Apache Kafka clusters using the `spark-sql-kafka-0-10` connector.
 *
 * Architectural Decisions:
 * - Supports both `streaming` (default) and `batch` execution modes (`readMode`).
 * - Supports all three Spark Kafka subscription strategies (`subscribe`, `subscribePattern`, and `assign`).
 * - Conditionally renders `endingOffsets` when `readMode === 'batch'`.
 * - Integrates `SecretManagerAuthFields` and SASL/SSL security configuration to reference credentials
 *   from Google Cloud Secret Manager without hardcoding secrets in pipeline specifications.
 * - Uses shared primitives (`PropertyField`, `SelectField`, `CollapsibleSection`, `SecretManagerAuthFields`,
 *   and `SystemInstructionsField`) on the 8pt grid system.
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

/** Read mode options for KafkaSource. */
const KAFKA_READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'streaming', label: 'Streaming (spark.readStream)' },
  { value: 'batch', label: 'Batch (spark.read)' },
];

/** Subscription mode options for Spark Kafka connector. */
const KAFKA_SUBSCRIPTION_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'subscribe', label: 'Subscribe (Topic List)' },
  { value: 'subscribePattern', label: 'Subscribe Pattern (Regex)' },
  { value: 'assign', label: 'Assign (Explicit Partitions JSON)' },
];

/** Security protocol options for Kafka broker connections. */
export const KAFKA_SECURITY_PROTOCOL_OPTIONS: SelectFieldOption[] = [
  { value: 'PLAINTEXT', label: 'PLAINTEXT (Unencrypted)' },
  { value: 'SSL', label: 'SSL (TLS Encryption)' },
  { value: 'SASL_SSL', label: 'SASL_SSL (SASL over TLS)' },
  { value: 'SASL_PLAINTEXT', label: 'SASL_PLAINTEXT (SASL Unencrypted)' },
];

/** SASL mechanism options for Kafka authentication. */
export const KAFKA_SASL_MECHANISM_OPTIONS: SelectFieldOption[] = [
  { value: 'PLAIN', label: 'PLAIN' },
  { value: 'SCRAM-SHA-256', label: 'SCRAM-SHA-256' },
  { value: 'SCRAM-SHA-512', label: 'SCRAM-SHA-512' },
  { value: 'OAUTHBEARER', label: 'OAUTHBEARER (GCP Managed Kafka / IAM)' },
];

/** React Flow node shape for the KafkaSource panel. */
export interface KafkaIOSourceNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      readMode?: 'streaming' | 'batch';
      bootstrapServers?: string;
      subscriptionMode?: 'subscribe' | 'subscribePattern' | 'assign';
      topics?: string;
      topicPattern?: string;
      assignedPartitions?: string;
      startingOffsets?: string;
      endingOffsets?: string;
      maxOffsetsPerTrigger?: string | number;
      minPartitions?: string | number;
      failOnDataLoss?: boolean;
      includeHeaders?: boolean;
      groupIdPrefix?: string;
      securityProtocol?: string;
      saslMechanism?: string;
      authMode?: string;
      secretName?: string;
      consumerProperties?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link KafkaIOSourceProperties} component. */
export interface KafkaIOSourcePropertiesProps {
  node: KafkaIOSourceNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * KafkaIOSourceProperties renders the configuration form for Apache Spark `.format("kafka")` reads.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React component.
 */
export const KafkaIOSourceProperties: React.FC<KafkaIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const readMode = properties.readMode || 'streaming';
  const bootstrapServers = properties.bootstrapServers || '';
  const subscriptionMode = properties.subscriptionMode || 'subscribe';
  const topics = properties.topics || '';
  const topicPattern = properties.topicPattern || '';
  const assignedPartitions = properties.assignedPartitions || '';
  const startingOffsets = properties.startingOffsets || 'latest';
  const endingOffsets = properties.endingOffsets || 'latest';
  const maxOffsetsPerTrigger = properties.maxOffsetsPerTrigger ?? '';
  const minPartitions = properties.minPartitions ?? '';
  const failOnDataLoss = properties.failOnDataLoss ?? true;
  const includeHeaders = properties.includeHeaders ?? false;
  const groupIdPrefix = properties.groupIdPrefix || '';
  const securityProtocol = properties.securityProtocol || 'PLAINTEXT';
  const saslMechanism = properties.saslMechanism || 'PLAIN';
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';
  const consumerProperties = properties.consumerProperties || '';

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

  const systemInstructions = resolveSystemInstructions(node);

  const isBootstrapServersInvalid = !bootstrapServers.trim();
  const isTopicsInvalid = subscriptionMode === 'subscribe' && !topics.trim();
  const isTopicPatternInvalid = subscriptionMode === 'subscribePattern' && !topicPattern.trim();
  const isAssignedPartitionsInvalid = subscriptionMode === 'assign' && !assignedPartitions.trim();

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

  const showSaslMechanism = securityProtocol === 'SASL_SSL' || securityProtocol === 'SASL_PLAINTEXT';

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Read Mode Select */}
      <SelectField
        id="kafkaReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select streaming mode (spark.readStream.format('kafka')) for continuous micro-batch processing or batch mode (spark.read.format('kafka')) for bounded historical reads."
        value={readMode}
        options={KAFKA_READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Bootstrap Servers Input (Required) */}
      <PropertyField
        id="kafkaBootstrapServers"
        name="bootstrapServers"
        label="Bootstrap Servers"
        required="Bootstrap Servers is required."
        tooltip="Comma-separated list of host:port pairs used to establish the initial connection to the Kafka cluster (option kafka.bootstrap.servers). Required."
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

      {/* Subscription Mode Select */}
      <SelectField
        id="kafkaSubscriptionMode"
        name="subscriptionMode"
        label="Subscription Mode"
        tooltip="Choose how Spark subscribes to Kafka topics: explicit topic list (subscribe), regular expression pattern (subscribePattern), or fixed topic-partitions JSON (assign)."
        value={subscriptionMode}
        options={KAFKA_SUBSCRIPTION_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Conditional Subscription Target Input */}
      {subscriptionMode === 'subscribe' && (
        <PropertyField
          id="kafkaTopics"
          name="topics"
          label="Topic(s)"
          required="Topic(s) is required."
          tooltip="Comma-separated list of Kafka topic names to subscribe to (option subscribe). Required."
          value={topics}
          invalid={isTopicsInvalid}
          error={isTopicsInvalid ? 'Topic(s) is required.' : null}
          helperText={
            !isTopicsInvalid ? (
              <span>
                Example: <code className="text-slate-300 bg-slate-800/60 px-1 py-0.5 rounded">events,orders</code>
              </span>
            ) : null
          }
          placeholder="input-topic"
          onChange={handleChange}
          mono={true}
        />
      )}

      {subscriptionMode === 'subscribePattern' && (
        <PropertyField
          id="kafkaTopicPattern"
          name="topicPattern"
          label="Topic Pattern (Regex)"
          required="Topic Pattern is required."
          tooltip="Java regular expression pattern matching Kafka topics to subscribe to dynamically (option subscribePattern). Required."
          value={topicPattern}
          invalid={isTopicPatternInvalid}
          error={isTopicPatternInvalid ? 'Topic Pattern is required.' : null}
          placeholder="events-.*"
          onChange={handleChange}
          mono={true}
        />
      )}

      {subscriptionMode === 'assign' && (
        <PropertyField
          id="kafkaAssignedPartitions"
          name="assignedPartitions"
          label="Assigned Partitions (JSON)"
          required="Assigned Partitions is required."
          tooltip="JSON string specifying exact topic and partition numbers to consume (option assign). Required."
          value={assignedPartitions}
          invalid={isAssignedPartitionsInvalid}
          error={isAssignedPartitionsInvalid ? 'Assigned Partitions is required.' : null}
          placeholder='{"topicA":[0,1],"topicB":[2,4]}'
          onChange={handleChange}
          mono={true}
        />
      )}

      {/* Offsets Configuration */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="kafkaStartingOffsets"
          name="startingOffsets"
          label="Starting Offsets"
          tooltip="Start point when a query begins: 'latest', 'earliest', or a JSON string mapping topic-partitions to offsets (option startingOffsets)."
          value={startingOffsets}
          onChange={handleChange}
          placeholder="latest"
          datalist={['latest', 'earliest']}
          mono={true}
        />

        {readMode === 'batch' && (
          <PropertyField
            id="kafkaEndingOffsets"
            name="endingOffsets"
            label="Ending Offsets (Batch)"
            tooltip="End point for bounded queries: 'latest' or a JSON string mapping topic-partitions to upper bounds (option endingOffsets)."
            value={endingOffsets}
            onChange={handleChange}
            placeholder="latest"
            datalist={['latest']}
            mono={true}
          />
        )}
      </div>

      {/* Throughput & Partitioning */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="kafkaMaxOffsetsPerTrigger"
          name="maxOffsetsPerTrigger"
          label="Max Offsets Per Trigger"
          type="number"
          min="1"
          tooltip="Rate limit on maximum number of offsets processed per micro-batch trigger (option maxOffsetsPerTrigger)."
          value={maxOffsetsPerTrigger}
          onChange={handleChange}
          placeholder="e.g. 10000"
          mono={true}
        />

        <PropertyField
          id="kafkaMinPartitions"
          name="minPartitions"
          label="Min Partitions"
          type="number"
          min="1"
          tooltip="Desired minimum number of Spark partitions to read from Kafka for increased parallelism (option minPartitions)."
          value={minPartitions}
          onChange={handleChange}
          placeholder="e.g. 8"
          mono={true}
        />
      </div>

      {/* Boolean Options: Fail on Data Loss & Include Headers */}
      <div className="p-3 bg-slate-800/40 rounded border border-slate-700/50 flex flex-col gap-2.5">
        <label htmlFor="kafkaFailOnDataLoss" className="flex items-center gap-2 cursor-pointer">
          <input
            id="kafkaFailOnDataLoss"
            type="checkbox"
            name="failOnDataLoss"
            checked={failOnDataLoss}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Fail on Data Loss</span>
            <InfoTooltip text="Fail the streaming query when data might have been lost because topics were deleted or offsets aged out (option failOnDataLoss, default true)." />
          </div>
        </label>

        <label htmlFor="kafkaIncludeHeaders" className="flex items-center gap-2 cursor-pointer">
          <input
            id="kafkaIncludeHeaders"
            type="checkbox"
            name="includeHeaders"
            checked={includeHeaders}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Include Kafka Headers</span>
            <InfoTooltip text="Include Kafka record headers as an array of struct<key:string, value:binary> column in the DataFrame (option includeHeaders)." />
          </div>
        </label>
      </div>

      {/* Security & Secret Manager Auth */}
      <div className="p-3 bg-[var(--vscode-subcard-bg)] rounded border border-[var(--vscode-border)] flex flex-col gap-3">
        <div className="flex items-center gap-2 pb-1 border-b border-[var(--vscode-border-light)]">
          <ShieldCheck className="w-4 h-4 text-emerald-400" />
          <span className="text-xs font-bold uppercase tracking-wide text-[var(--vscode-text-primary)]">
            Security &amp; Authentication
          </span>
        </div>

        <SelectField
          id="kafkaSecurityProtocol"
          name="securityProtocol"
          label="Security Protocol"
          tooltip="Protocol used to communicate with Kafka brokers (option kafka.security.protocol)."
          value={securityProtocol}
          options={KAFKA_SECURITY_PROTOCOL_OPTIONS}
          onChange={handleChange}
        />

        {showSaslMechanism && (
          <SelectField
            id="kafkaSaslMechanism"
            name="saslMechanism"
            label="SASL Mechanism"
            tooltip="Authentication mechanism used for SASL broker handshakes (option kafka.sasl.mechanism)."
            value={saslMechanism}
            options={KAFKA_SASL_MECHANISM_OPTIONS}
            onChange={handleChange}
          />
        )}

        <SecretManagerAuthFields
          idPrefix="kafkaSource"
          datalistId="kafka-source-secrets-list"
          authMode={authMode}
          secretName={secretName}
          serviceName="Apache Kafka"
          secretPlaceholder="e.g. kafka-sasl-credentials"
          onChange={handleChange}
        />
      </div>

      {/* Advanced Consumer Configuration */}
      <CollapsibleSection
        title="Advanced Kafka Options"
        icon={<Sliders className="w-4 h-4 text-emerald-400" />}
        isOpen={isAdvancedOpen}
        onToggle={() => setIsAdvancedOpen((prev) => !prev)}
      >
        <PropertyField
          id="kafkaGroupIdPrefix"
          name="groupIdPrefix"
          label="Consumer Group ID Prefix"
          tooltip="Prefix for consumer group identifiers generated by Spark Structured Streaming (option groupIdPrefix)."
          value={groupIdPrefix}
          onChange={handleChange}
          placeholder="spark-kafka-source"
          mono={true}
        />

        <PropertyField
          id="kafkaConsumerProperties"
          name="consumerProperties"
          label="Additional Kafka Options"
          tooltip="Additional Kafka reader options formatted as key=value on each line (e.g. kafka.fetch.min.bytes=1024)."
          type="textarea"
          value={consumerProperties}
          onChange={handleChange}
          placeholder={`kafka.fetch.min.bytes=1024\nkafka.max.partition.fetch.bytes=1048576`}
          rows={3}
          mono={true}
        />
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="kafkaSourceSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Kafka source node."
        placeholder="Enter custom instructions for Kafka source code generation..."
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

/** Alias matching the canonical Spark transform name `KafkaSource`. */
export const KafkaSourceProperties = KafkaIOSourceProperties;
