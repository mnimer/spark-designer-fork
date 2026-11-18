/**
 * PubSub Source Properties Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud Pub/Sub Source nodes (`PubSubSource` / `PubSubIO`)
 * in Apache Spark 4.0 pipelines using the Python `DataSourceStreamReader` and `DataSourceReader` APIs
 * (`google-cloud-pubsub` `SubscriberClient` synchronous micro-batch pull).
 *
 * Architectural Decisions:
 * - Supports both `streaming` (default) and `batch` execution modes (`readMode`).
 * - Configures `subscription`, `projectId`, `maxMessagesPerBatch`, `ackDeadlineSeconds`, and `includeAttributes`.
 * - Retains live GCP Pub/Sub subscription and topic discovery via `useAssetLoader` with inline error surfacing.
 * - Uses shared `PropertyField`, `SelectField`, and `SystemInstructionsField` primitives on the 8pt grid.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';
import { useAssetLoader } from '../../hooks/useAssetLoader';

/** Read mode options for PubSubSource. */
export const PUBSUB_READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'streaming', label: 'Streaming (DataSourceStreamReader)' },
  { value: 'batch', label: 'Batch (DataSourceReaderBoundedPull)' },
];

/** Props for the {@link PubSubProperties} component. */
export interface PubSubPropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      properties?: {
        readMode?: 'streaming' | 'batch';
        subscription?: string;
        topic?: string;
        projectId?: string;
        maxMessagesPerBatch?: string | number;
        ackDeadlineSeconds?: string | number;
        includeAttributes?: boolean;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: any;
      };
      system_instructions?: string;
      special_instructions?: string;
    };
  };
  onChange: (properties: any) => void;
}

/**
 * Dedicated property panel for the Google Cloud Pub/Sub Spark source node.
 *
 * @param props The selected node and the onChange callback used to persist edits.
 * @returns React component.
 */
export const PubSubProperties: React.FC<PubSubPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};

  const { items: subscriptions, loading: subscriptionsLoading, error: subscriptionError } =
    useAssetLoader<string>('subscriptions');

  const readMode = properties.readMode || 'streaming';
  const subscription = properties.subscription || '';
  const projectId = properties.projectId || '';
  const maxMessagesPerBatch = properties.maxMessagesPerBatch ?? '1000';
  const ackDeadlineSeconds = properties.ackDeadlineSeconds ?? '60';
  const includeAttributes = properties.includeAttributes ?? true;

  const isSubscriptionInvalid = !subscription.trim();

  const systemInstructions = resolveSystemInstructions(node);

  /**
   * Handles input, select, and checkbox changes.
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
        id="pubsubReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select streaming mode (Spark 4.0 DataSourceStreamReader) for continuous micro-batch pulls or batch mode (DataSourceReader) for a bounded pull."
        value={readMode}
        options={PUBSUB_READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Subscription Input (Required) */}
      <PropertyField
        id="pubsubSubscription"
        name="subscription"
        label="Subscription"
        required="Subscription is required."
        tooltip="Cloud Pub/Sub subscription ID or full resource path to pull messages from (e.g. projects/my-project/subscriptions/my-sub or my-sub). Required."
        value={subscription}
        invalid={isSubscriptionInvalid}
        error={subscriptionError || (isSubscriptionInvalid ? 'Subscription is required.' : null)}
        loading={subscriptionsLoading}
        onChange={handleChange}
        placeholder="projects/my-project/subscriptions/my-sub"
        list="subscription-options"
        datalist={subscriptions}
        mono={true}
        helperText={
          !isSubscriptionInvalid ? (
            <span>
              Sample: <code className="text-slate-300 bg-slate-800/60 px-1 py-0.5 rounded">projects/my-project/subscriptions/my-sub</code>
            </span>
          ) : null
        }
      />

      {/* GCP Project ID */}
      <PropertyField
        id="pubsubProjectId"
        name="projectId"
        label="GCP Project ID"
        tooltip="Google Cloud project ID owning the Pub/Sub subscription. Leave blank when passing a full subscription path or using the runtime default project."
        value={projectId}
        onChange={handleChange}
        placeholder="e.g. my-gcp-project"
        mono={true}
      />

      {/* Micro-Batch Pull Limits */}
      <div className="grid grid-cols-2 gap-3">
        <PropertyField
          id="pubsubMaxMessagesPerBatch"
          name="maxMessagesPerBatch"
          label="Max Messages Per Batch"
          type="number"
          min="1"
          tooltip="Maximum number of messages pulled synchronously per Spark micro-batch partition (default 1000)."
          value={maxMessagesPerBatch}
          onChange={handleChange}
          placeholder="1000"
          mono={true}
        />

        <PropertyField
          id="pubsubAckDeadlineSeconds"
          name="ackDeadlineSeconds"
          label="Ack Deadline (s)"
          type="number"
          min="10"
          max="600"
          tooltip="Acknowledgement deadline in seconds for pulled messages before Pub/Sub redelivers them (10 to 600 seconds)."
          value={ackDeadlineSeconds}
          onChange={handleChange}
          placeholder="60"
          mono={true}
        />
      </div>

      {/* Include Attributes Checkbox */}
      <div className="p-3 bg-slate-800/40 rounded border border-slate-700/50">
        <label htmlFor="pubsubIncludeAttributes" className="flex items-center gap-2 cursor-pointer">
          <input
            id="pubsubIncludeAttributes"
            type="checkbox"
            name="includeAttributes"
            checked={includeAttributes}
            onChange={handleChange}
            className="w-4 h-4 rounded border-slate-700 text-blue-600 focus:ring-0 focus:ring-offset-0 bg-[#252836]"
          />
          <div className="flex items-center">
            <span className="text-sm font-medium text-slate-300">Include Message Attributes</span>
            <InfoTooltip text="Include Pub/Sub message attributes as a map<string, string> column alongside message_id, data, and publish_time." />
          </div>
        </label>
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="pubsubSourceSystemInstructions"
        value={systemInstructions}
        tooltip="Custom instructions supplied to the AI code generator for this Pub/Sub source node."
        placeholder="Enter custom instructions for Pub/Sub source code generation..."
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

/** Alias matching the canonical Spark transform name `PubSubSource`. */
export const PubSubSourceProperties = PubSubProperties;
