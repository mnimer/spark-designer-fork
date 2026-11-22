/**
 * @fileoverview KinesisIO Sink Properties Component for Apache Spark 4.x pipelines.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark KinesisSink (`KinesisIO`) nodes.
 * Configures batch (`foreachPartition`) and streaming (`foreachBatch`) writes to Amazon Kinesis
 * Data Streams using `boto3` `put_records` (up to 500 records per request with partial failure retries).
 *
 * Architectural Decisions:
 * - Configures `streamName`, `region`, `partitionKeyMode`, `payloadFormat`, and `payloadColumn`.
 * - Enforces AWS Kinesis `PutRecords` batch size bounds (1 to 500 records) and `FailedRecordCount` retries.
 * - Integrates `SecretManagerAuthFields` and optional STS `roleArn` while stripping cleartext AWS keys.
 * - Uses shared `PropertyField`, `SelectField`, `CollapsibleSection`, and `SystemInstructionsField` primitives.
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import { Sliders, ShieldCheck } from 'lucide-react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { SecretManagerAuthFields } from '../../components/common/SecretManagerAuthFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';
import { AWS_REGION_OPTIONS } from '../source/KinesisIO';

/** Partition key routing modes for KinesisSink PutRecords calls. */
export const KINESIS_PARTITION_KEY_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'column', label: 'Column Value (DataFrame Column)' },
  { value: 'hash', label: 'Payload Hash (SHA-256 Distribution)' },
  { value: 'static', label: 'Static Key (Fixed Partition Key)' },
];

/** Payload encoding modes for KinesisSink Data bytes. */
export const KINESIS_PAYLOAD_FORMAT_OPTIONS: SelectFieldOption[] = [
  { value: 'json', label: 'JSON Object (Serialize full row)' },
  { value: 'string', label: 'String Column (UTF-8)' },
  { value: 'binary', label: 'Binary Column (Raw bytes)' },
];

/** React Flow node shape for the KinesisSink panel. */
export interface KinesisIOSinkNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      streamName?: string;
      region?: string;
      partitionKeyMode?: 'column' | 'hash' | 'static';
      partitionKeyColumn?: string;
      staticPartitionKey?: string;
      payloadFormat?: 'json' | 'string' | 'binary';
      payloadColumn?: string;
      maxBatchSize?: string | number;
      maxRetries?: string | number;
      endpointUrl?: string;
      roleArn?: string;
      authMode?: string;
      secretName?: string;
      awsAccessKey?: string;
      awsSecretKey?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link KinesisIOSinkProperties} component. */
export interface KinesisIOSinkPropertiesProps {
  node: KinesisIOSinkNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the Amazon Kinesis Data Streams Spark 4.x sink property configuration form.
 *
 * @param props Active node metadata and property change callback.
 * @returns Rendered KinesisSink property form.
 */
export const KinesisIOSinkProperties: React.FC<KinesisIOSinkPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const streamName = properties.streamName || '';
  const region = properties.region || 'us-east-1';
  const partitionKeyMode = properties.partitionKeyMode || 'column';
  const partitionKeyColumn = properties.partitionKeyColumn || '';
  const staticPartitionKey = properties.staticPartitionKey || '';
  const payloadFormat = properties.payloadFormat || 'json';
  const payloadColumn = properties.payloadColumn || '';
  const maxBatchSize = properties.maxBatchSize ?? '500';
  const maxRetries = properties.maxRetries ?? '5';
  const endpointUrl = properties.endpointUrl || '';
  const roleArn = properties.roleArn || '';
  const authMode = properties.authMode || 'secretManager';
  const secretName = properties.secretName || '';

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

  const systemInstructions = resolveSystemInstructions(node);

  const isStreamNameInvalid = !streamName.trim();
  const isPartitionKeyColumnInvalid =
    partitionKeyMode === 'column' && !partitionKeyColumn.trim();
  const isStaticKeyInvalid = partitionKeyMode === 'static' && !staticPartitionKey.trim();
  const isPayloadColumnRequired = payloadFormat === 'string' || payloadFormat === 'binary';
  const isPayloadColumnInvalid = isPayloadColumnRequired && !payloadColumn.trim();
  const numericBatchSize = Number(maxBatchSize);
  const isBatchSizeInvalid =
    String(maxBatchSize).trim() !== '' &&
    (Number.isNaN(numericBatchSize) || numericBatchSize < 1 || numericBatchSize > 500);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const isCheckbox = type === 'checkbox';
    const checked = isCheckbox ? (e.target as HTMLInputElement).checked : undefined;

    const nextProperties: Record<string, unknown> = {
      ...properties,
      [name]: isCheckbox ? checked : value,
    };
    delete nextProperties.awsAccessKey;
    delete nextProperties.awsSecretKey;

    onChange(nextProperties);
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Stream Name Input (Required) */}
      <PropertyField
        id="kinesisSinkStreamName"
        name="streamName"
        label="Stream Name"
        required="Stream Name is required."
        tooltip="Target Amazon Kinesis Data Stream receiving published records. Required."
        value={streamName}
        onChange={handleChange}
        placeholder="e.g. processed-events"
        invalid={isStreamNameInvalid}
        error={isStreamNameInvalid ? 'Stream Name is required.' : null}
        mono={true}
      />

      {/* AWS Region */}
      <PropertyField
        id="kinesisSinkRegion"
        name="region"
        label="AWS Region"
        tooltip="Cloud region hosting the destination Kinesis stream (for example us-east-1 or us-west-2)."
        value={region}
        onChange={handleChange}
        placeholder="us-east-1"
        datalist={AWS_REGION_OPTIONS}
        mono={true}
      />

      {/* Partition Key Routing */}
      <SelectField
        id="kinesisSinkPartitionKeyMode"
        name="partitionKeyMode"
        label="Partition Key Strategy"
        tooltip="Determine how each record selects its Kinesis shard routing key."
        value={partitionKeyMode}
        options={KINESIS_PARTITION_KEY_MODE_OPTIONS}
        onChange={handleChange}
      />

      {partitionKeyMode === 'column' && (
        <PropertyField
          id="kinesisSinkPartitionKeyColumn"
          name="partitionKeyColumn"
          label="Partition Key Column"
          required="Partition Key Column is required."
          tooltip="DataFrame column converted to UTF-8 string as the Kinesis PartitionKey."
          value={partitionKeyColumn}
          onChange={handleChange}
          placeholder="e.g. device_id"
          invalid={isPartitionKeyColumnInvalid}
          error={isPartitionKeyColumnInvalid ? 'Partition Key Column is required.' : null}
          mono={true}
        />
      )}

      {partitionKeyMode === 'static' && (
        <PropertyField
          id="kinesisSinkStaticPartitionKey"
          name="staticPartitionKey"
          label="Static Partition Key"
          required="Static Partition Key is required."
          tooltip="Constant PartitionKey value assigned to every record in the micro-batch."
          value={staticPartitionKey}
          onChange={handleChange}
          placeholder="e.g. shard-0"
          invalid={isStaticKeyInvalid}
          error={isStaticKeyInvalid ? 'Static Partition Key is required.' : null}
          mono={true}
        />
      )}

      {/* Payload Encoding and Column */}
      <div className="grid grid-cols-2 gap-3">
        <SelectField
          id="kinesisSinkPayloadFormat"
          name="payloadFormat"
          label="Payload Format"
          tooltip="Encoding format used to convert Spark rows into binary Data payloads for put_records."
          value={payloadFormat}
          options={KINESIS_PAYLOAD_FORMAT_OPTIONS}
          onChange={handleChange}
        />

        <PropertyField
          id="kinesisSinkPayloadColumn"
          name="payloadColumn"
          label="Payload Column"
          required={isPayloadColumnRequired ? 'Payload Column is required.' : false}
          tooltip="Source column containing the message body. If JSON mode is selected and this field is blank, all columns serialize to JSON."
          value={payloadColumn}
          onChange={handleChange}
          placeholder={payloadFormat === 'json' ? 'Full row (optional)' : 'data'}
          invalid={isPayloadColumnInvalid}
          error={isPayloadColumnInvalid ? 'Payload Column is required.' : null}
          mono={true}
        />
      </div>

      {/* PutRecords Batch Size and FailedRecordCount Retries */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="kinesisSinkMaxBatchSize"
          name="maxBatchSize"
          label="Max Batch Records"
          type="number"
          min="1"
          max="500"
          tooltip="Maximum records sent per boto3 put_records call (1 to 500, AWS hard limit 500)."
          value={maxBatchSize}
          onChange={handleChange}
          placeholder="500"
          invalid={isBatchSizeInvalid}
          error={isBatchSizeInvalid ? 'Batch size must be between 1 and 500.' : null}
          mono={true}
        />

        <PropertyField
          id="kinesisSinkMaxRetries"
          name="maxRetries"
          label="FailedRecord Retries"
          type="number"
          min="0"
          max="20"
          tooltip="Maximum exponential backoff retries for partial failures when FailedRecordCount exceeds zero."
          value={maxRetries}
          onChange={handleChange}
          placeholder="5"
          mono={true}
        />
      </div>

      {/* AWS Credentials via Secret Manager */}
      <div className="p-3 bg-[var(--vscode-subcard-bg)] rounded border border-[var(--vscode-border)] flex flex-col gap-3">
        <div className="flex items-center gap-2 pb-1 border-b border-[var(--vscode-border-light)]">
          <ShieldCheck className="w-4 h-4 text-emerald-400" />
          <span className="text-xs font-bold uppercase tracking-wide text-[var(--vscode-text-primary)]">
            AWS Authentication
          </span>
        </div>

        <SecretManagerAuthFields
          idPrefix="kinesisSink"
          datalistId="kinesis-sink-secrets-list"
          authMode={authMode}
          secretName={secretName}
          serviceName="Amazon Kinesis"
          secretPlaceholder="e.g. aws-kinesis-credentials"
          onChange={handleChange}
        />
      </div>

      {/* Advanced STS and Endpoint Settings */}
      <CollapsibleSection
        title="Advanced Kinesis Options"
        icon={<Sliders className="w-4 h-4 text-emerald-400" />}
        isOpen={isAdvancedOpen}
        onToggle={() => setIsAdvancedOpen((prev) => !prev)}
      >
        <PropertyField
          id="kinesisSinkRoleArn"
          name="roleArn"
          label="Assume Role ARN"
          tooltip="Optional AWS IAM Role ARN assumed via STS before calling put_records."
          value={roleArn}
          onChange={handleChange}
          placeholder="arn:aws:iam::123456789012:role/SparkKinesisSinkRole"
          mono={true}
        />

        <PropertyField
          id="kinesisSinkEndpointUrl"
          name="endpointUrl"
          label="Custom Endpoint URL"
          tooltip="Optional custom VPC endpoint or LocalStack URL (for example http://localhost:4566)."
          value={endpointUrl}
          onChange={handleChange}
          placeholder="https://kinesis.us-east-1.amazonaws.com"
          mono={true}
        />
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="kinesisSinkSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Kinesis sink node."
        placeholder="Enter custom instructions for Kinesis sink code generation..."
        onChange={(value) => {
          const nextProperties: Record<string, unknown> = {
            ...properties,
            system_instructions: value,
          };
          delete nextProperties.awsAccessKey;
          delete nextProperties.awsSecretKey;
          onChange(nextProperties);
        }}
      />
    </div>
  );
};

/** Alias matching the canonical Spark transform name `KinesisSink`. */
export const KinesisSinkProperties = KinesisIOSinkProperties;
