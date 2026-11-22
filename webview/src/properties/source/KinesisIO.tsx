/**
 * KinesisIO Source Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark KinesisSource (`KinesisIO`) nodes.
 * Configures streaming reads (`readMode: 'streaming'`) from Amazon Kinesis Data Streams using the
 * Spark 4.0 Python `DataSourceStreamReader` (`boto3`) or `.format("kinesis")`.
 *
 * Architectural Decisions:
 * - Streaming-only source (`readMode: 'streaming'`).
 * - Configures `streamName`, `region`, `initialPosition` (`'latest' | 'trim_horizon' | 'at_timestamp'`),
 *   conditional `initialTimestamp`, `endpointUrl`, and `maxRecordsPerFetch`.
 * - Integrates `SecretManagerAuthFields` for AWS credentials stored in Google Cloud Secret Manager.
 * - Uses shared primitives on the 8pt grid system.
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { Radio } from 'lucide-react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { SecretManagerAuthFields } from '../../components/common/SecretManagerAuthFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Preset mapping AWS region label to its standard region code identifier. */
export interface AwsRegionPreset {
  label: string;
  value: string;
}

/** Preset list of AWS regions used by Kinesis source and sink panels. */
const AWS_REGION_PRESETS: AwsRegionPreset[] = [
  { label: 'US East (N. Virginia) [us-east-1]', value: 'us-east-1' },
  { label: 'US East (Ohio) [us-east-2]', value: 'us-east-2' },
  { label: 'US West (N. California) [us-west-1]', value: 'us-west-1' },
  { label: 'US West (Oregon) [us-west-2]', value: 'us-west-2' },
  { label: 'Europe (Ireland) [eu-west-1]', value: 'eu-west-1' },
  { label: 'Europe (Frankfurt) [eu-central-1]', value: 'eu-central-1' },
  { label: 'Europe (London) [eu-west-2]', value: 'eu-west-2' },
  { label: 'Asia Pacific (Tokyo) [ap-northeast-1]', value: 'ap-northeast-1' },
  { label: 'Asia Pacific (Singapore) [ap-southeast-1]', value: 'ap-southeast-1' },
  { label: 'Asia Pacific (Sydney) [ap-southeast-2]', value: 'ap-southeast-2' },
  { label: 'Custom Region...', value: 'custom' },
];

/** Selectable AWS region options for KinesisSource. */
export const AWS_REGION_OPTIONS: SelectFieldOption[] = AWS_REGION_PRESETS.filter(
  (preset) => preset.value !== 'custom'
);

/** Initial shard iterator position options for Kinesis reads. */
const KINESIS_INITIAL_POSITION_OPTIONS: SelectFieldOption[] = [
  { value: 'latest', label: 'latest (Read new incoming records)' },
  { value: 'trim_horizon', label: 'trim_horizon (Read from oldest retained record)' },
  { value: 'at_timestamp', label: 'at_timestamp (Start from specific timestamp)' },
];

/** React Flow node shape for the KinesisIO source panel. */
export interface KinesisIOSourceNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      readMode?: 'streaming';
      streamName?: string;
      region?: string;
      initialPosition?: 'latest' | 'trim_horizon' | 'at_timestamp';
      initialTimestamp?: string;
      endpointUrl?: string;
      maxRecordsPerFetch?: string | number;
      authMode?: string;
      secretName?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link KinesisIOSourceProperties} component. */
export interface KinesisIOSourcePropertiesProps {
  node: KinesisIOSourceNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * KinesisIOSourceProperties renders the configuration panel for reading from Amazon Kinesis Data Streams.
 *
 * @param props Component properties containing the active node data and onChange callback.
 * @returns React component.
 */
export const KinesisIOSourceProperties: React.FC<KinesisIOSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const streamName = properties.streamName || '';
  const region = properties.region || 'us-east-1';
  const initialPosition = properties.initialPosition || 'latest';
  const initialTimestamp = properties.initialTimestamp || '';
  const endpointUrl = properties.endpointUrl || '';
  const maxRecordsPerFetch = properties.maxRecordsPerFetch ?? '1000';
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';

  const systemInstructions = resolveSystemInstructions(node);
  const isStreamNameInvalid = !streamName.trim();
  const isTimestampRequired = initialPosition === 'at_timestamp';
  const isTimestampInvalid = isTimestampRequired && !initialTimestamp.trim();

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
      readMode: 'streaming',
      [name]: isCheckbox ? checked : value,
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

      {/* Stream Name Input (Required) */}
      <PropertyField
        id="kinesisStreamName"
        name="streamName"
        label="Stream Name"
        required="Stream Name is required."
        tooltip="Name of the Amazon Kinesis Data Stream to consume from (option streamName). Required."
        value={streamName}
        onChange={handleChange}
        placeholder="e.g. telemetry-events"
        invalid={isStreamNameInvalid}
        error={isStreamNameInvalid ? 'Stream Name is required.' : null}
        mono={true}
      />

      {/* AWS Region Select */}
      <PropertyField
        id="kinesisRegion"
        name="region"
        label="AWS Region"
        tooltip="Cloud region where the Kinesis Data Stream resides (e.g. us-east-1 or us-west-2)."
        value={region}
        onChange={handleChange}
        placeholder="us-east-1"
        datalist={AWS_REGION_OPTIONS}
        mono={true}
      />

      {/* Initial Shard Iterator Position */}
      <SelectField
        id="kinesisInitialPosition"
        name="initialPosition"
        label="Initial Position"
        tooltip="Shard iterator starting position when no checkpoint exists: latest, trim_horizon, or at_timestamp."
        value={initialPosition}
        options={KINESIS_INITIAL_POSITION_OPTIONS}
        onChange={handleChange}
      />

      {/* Conditional Initial Timestamp */}
      {isTimestampRequired && (
        <PropertyField
          id="kinesisInitialTimestamp"
          name="initialTimestamp"
          label="Initial Timestamp"
          required="Initial Timestamp is required when Initial Position is at_timestamp."
          tooltip="ISO-8601 timestamp string or epoch seconds from which to start reading when Initial Position is at_timestamp."
          value={initialTimestamp}
          onChange={handleChange}
          placeholder="2026-01-01T00:00:00Z"
          invalid={isTimestampInvalid}
          error={isTimestampInvalid ? 'Initial Timestamp is required when Initial Position is at_timestamp.' : null}
          mono={true}
        />
      )}

      {/* Max Records Per Fetch & Endpoint URL */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="kinesisMaxRecordsPerFetch"
          name="maxRecordsPerFetch"
          label="Max Records Per Fetch"
          type="number"
          min="1"
          max="10000"
          tooltip="Maximum number of records fetched per GetRecords API call per shard (1 to 10000, default 1000)."
          value={maxRecordsPerFetch}
          onChange={handleChange}
          placeholder="1000"
          mono={true}
        />

        <PropertyField
          id="kinesisEndpointUrl"
          name="endpointUrl"
          label="Custom Endpoint URL"
          tooltip="Optional custom Kinesis endpoint URL for VPC endpoints or LocalStack testing (e.g. https://kinesis.us-east-1.amazonaws.com)."
          value={endpointUrl}
          onChange={handleChange}
          placeholder="https://kinesis.us-east-1.amazonaws.com"
          mono={true}
        />
      </div>

      {/* AWS Credentials via Secret Manager */}
      <SecretManagerAuthFields
        idPrefix="kinesisSource"
        datalistId="kinesis-source-secrets-list"
        authMode={authMode}
        secretName={secretName}
        serviceName="Amazon Kinesis"
        secretPlaceholder="e.g. aws-kinesis-credentials"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="kinesisSourceSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Kinesis source node."
        placeholder="Enter custom instructions for Kinesis source code generation..."
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

/** Alias matching the canonical Spark transform name `KinesisSource`. */
export const KinesisSourceProperties = KinesisIOSourceProperties;
