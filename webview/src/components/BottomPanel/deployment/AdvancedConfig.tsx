/**
 * @fileoverview Advanced job and Structured Streaming defaults configuration for pipeline runners.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React from 'react';
import { InfoTooltip } from '../../InfoTooltip';
import type {
  SparkPipelineStreamingDefaults,
  SparkStreamingTriggerType,
} from '../../../utils/sharedTypes';

/* eslint-disable @typescript-eslint/no-explicit-any */

/**
 * Option descriptor for GCP subnetwork selections.
 */
export interface SubnetworkOption {
  name: string;
  region: string;
  network: string;
  value: string;
}

/**
 * Structure of job configuration fields used in advanced runner and streaming settings.
 */
export interface AdvancedJobConfig extends SparkPipelineStreamingDefaults {
  runner: string;
  machineType?: string;
  minInstances?: number;
  maxInstances?: number;
  gcsStagingPath?: string;
  network?: string;
  subnetwork?: string;
  jobId?: string;
  region?: string;
  projectId?: string;
  [key: string]: any;
}

/**
 * Props for the {@link AdvancedConfig} component.
 */
export interface AdvancedConfigProps {
  /** Current job configuration state. */
  jobConfig: AdvancedJobConfig;
  /** State updater callback for modifying the parent job configuration. */
  setJobConfig: (update: React.SetStateAction<any>) => void;
  /** List of selectable VPC subnetworks. */
  subnetworks: SubnetworkOption[];
}

const STREAMING_TRIGGER_OPTIONS: Array<{ value: SparkStreamingTriggerType; label: string }> = [
  { value: 'processingTime', label: 'Processing Time (Interval)' },
  { value: 'availableNow', label: 'Available Now (Incremental Batch)' },
  { value: 'once', label: 'Once (Single Batch)' },
  { value: 'continuous', label: 'Continuous (Low Latency)' },
];

/**
 * Returns true when the selected Spark trigger strategy requires an interval string.
 *
 * @param triggerType Selected Spark streaming trigger type.
 * @returns True if `triggerType` uses a time interval.
 */
function triggerUsesInterval(triggerType: SparkStreamingTriggerType): boolean {
  return triggerType === 'processingTime' || triggerType === 'continuous';
}

/**
 * Renders contextual runner options and pipeline-level Structured Streaming defaults.
 *
 * @param props Current job configuration, state updater, and discovered subnetworks.
 * @returns The rendered advanced runner and streaming configuration controls.
 */
export const AdvancedConfig: React.FC<AdvancedConfigProps> = ({
  jobConfig,
  setJobConfig,
  subnetworks,
}) => {
  const effectiveTriggerType: SparkStreamingTriggerType =
    jobConfig.defaultTriggerType === 'availableNow' ||
    jobConfig.defaultTriggerType === 'once' ||
    jobConfig.defaultTriggerType === 'continuous'
      ? jobConfig.defaultTriggerType
      : 'processingTime';
  const showTriggerInterval = triggerUsesInterval(effectiveTriggerType);
  const checkpointRootValue = jobConfig.checkpointRoot ?? '';
  const triggerIntervalValue = jobConfig.defaultTriggerInterval ?? '10 seconds';

  const handleMachineTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev: any) => ({ ...prev, machineType: nextValue }));
  };

  const handleMinWorkersChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const nextValue = parseInt(e.target.value, 10) || 1;
    setJobConfig((prev: any) => ({ ...prev, minInstances: nextValue }));
  };

  const handleMaxWorkersChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const nextValue = parseInt(e.target.value, 10) || 1;
    setJobConfig((prev: any) => ({ ...prev, maxInstances: nextValue }));
  };

  const handleStagingPathChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev: any) => ({ ...prev, gcsStagingPath: nextValue }));
  };

  const handleSubnetworkChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev: any) => ({ ...prev, subnetwork: nextValue }));
  };

  const handleCheckpointRootChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev: any) => ({ ...prev, checkpointRoot: nextValue }));
  };

  const handleDefaultTriggerTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextTrigger = e.target.value as SparkStreamingTriggerType;
    setJobConfig((prev: any) => ({
      ...prev,
      defaultTriggerType: nextTrigger,
      ...(triggerUsesInterval(nextTrigger) && prev.defaultTriggerInterval === undefined
        ? { defaultTriggerInterval: '10 seconds' }
        : {}),
    }));
  };

  const handleDefaultTriggerIntervalChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev: any) => ({ ...prev, defaultTriggerInterval: nextValue }));
  };

  return (
    <>
      {/* Contextual Options: Google Cloud Dataflow */}
      {jobConfig.runner === 'Dataflow' && (
        <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-lg border border-[var(--vscode-border)]">
          <div className="text-xs font-bold uppercase tracking-wider text-blue-400">
            Dataflow Options
          </div>

          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedMachineType"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Machine Type</span>
              <InfoTooltip text="Compute Engine machine type used for worker instances." />
            </label>
            <select
              id="advancedMachineType"
              value={jobConfig.machineType || 'n1-standard-1'}
              onChange={handleMachineTypeChange}
              className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs focus:outline-none focus:border-[var(--vscode-highlight)] cursor-pointer"
            >
              <option value="n1-standard-1">n1-standard-1 (1 vCPU, 3.75 GB)</option>
              <option value="n1-standard-2">n1-standard-2 (2 vCPU, 7.5 GB)</option>
              <option value="n1-standard-4">n1-standard-4 (4 vCPU, 15 GB)</option>
              <option value="n2-standard-2">n2-standard-2 (2 vCPU, 8 GB)</option>
              <option value="n2-standard-4">n2-standard-4 (4 vCPU, 16 GB)</option>
            </select>
          </div>

          <div className="grid grid-cols-2 gap-2">
            <div className="flex flex-col gap-1">
              <label
                htmlFor="advancedMinWorkers"
                className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
              >
                <span>Min Workers</span>
                <InfoTooltip text="Minimum worker count allocated when the job starts." />
              </label>
              <input
                id="advancedMinWorkers"
                type="number"
                min={1}
                max={999}
                value={jobConfig.minInstances ?? 1}
                onChange={handleMinWorkersChange}
                className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)] w-full"
              />
            </div>
            <div className="flex flex-col gap-1">
              <label
                htmlFor="advancedMaxWorkers"
                className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
              >
                <span>Max Workers</span>
                <InfoTooltip text="Maximum autoscaling worker ceiling." />
              </label>
              <input
                id="advancedMaxWorkers"
                type="number"
                min={1}
                max={999}
                value={jobConfig.maxInstances ?? 10}
                onChange={handleMaxWorkersChange}
                className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)] w-full"
              />
            </div>
          </div>

          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedStagingPath"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>GCS Staging Path</span>
              <InfoTooltip text="Cloud Storage URI used for temporary staging files." />
            </label>
            <input
              id="advancedStagingPath"
              type="text"
              value={jobConfig.gcsStagingPath || ''}
              onChange={handleStagingPathChange}
              placeholder="gs://<bucket>/staging"
              className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
            />
          </div>

          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedDataflowSubnetwork"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Subnetwork</span>
              <InfoTooltip text="VPC subnetwork attached to worker nodes." />
            </label>
            <select
              id="advancedDataflowSubnetwork"
              value={jobConfig.subnetwork || ''}
              onChange={handleSubnetworkChange}
              className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs focus:outline-none focus:border-[var(--vscode-highlight)] cursor-pointer"
            >
              <option value="">Default Subnetwork</option>
              {subnetworks.map((sub, index) => (
                <option key={index} value={sub.value}>
                  {sub.name} ({sub.region})
                </option>
              ))}
            </select>
          </div>
        </div>
      )}

      {/* Contextual Options: Serverless Spark */}
      {(jobConfig.runner === 'Spark' || jobConfig.runner === 'ServerlessSpark') && (
        <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-lg border border-[var(--vscode-border)]">
          <div className="text-xs font-bold uppercase tracking-wider text-amber-400">
            Spark Options
          </div>
          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedSparkSubnetwork"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Subnetwork</span>
              <InfoTooltip text="VPC subnetwork used for Serverless Spark executor networking." />
            </label>
            <select
              id="advancedSparkSubnetwork"
              value={jobConfig.subnetwork || ''}
              onChange={handleSubnetworkChange}
              className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs focus:outline-none focus:border-[var(--vscode-highlight)] cursor-pointer"
            >
              <option value="">Default Subnetwork</option>
              {subnetworks.map((sub, index) => (
                <option key={index} value={sub.value}>
                  {sub.name} ({sub.region})
                </option>
              ))}
            </select>
          </div>
        </div>
      )}

      {/* Pipeline-Level Structured Streaming Defaults (A9) */}
      <div
        data-testid="pipeline-streaming-defaults"
        className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-lg border border-[var(--vscode-border)]"
      >
        <div className="text-xs font-bold uppercase tracking-wider text-emerald-400">
          Streaming Defaults
        </div>

        <div className="flex flex-col gap-1">
          <label
            htmlFor="advancedCheckpointRoot"
            className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
          >
            <span>Checkpoint Root URI</span>
            <InfoTooltip text="Base storage URI for Spark Structured Streaming checkpoints (such as gs://bucket/checkpoints or /tmp/checkpoints). Streaming sinks inherit <checkpointRoot>/<nodeId> when no override is set." />
          </label>
          <input
            id="advancedCheckpointRoot"
            name="checkpointRoot"
            type="text"
            value={checkpointRootValue}
            onChange={handleCheckpointRootChange}
            placeholder="gs://<bucket>/checkpoints"
            className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
          />
        </div>

        <div className="flex flex-col gap-1">
          <label
            htmlFor="advancedDefaultTriggerType"
            className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
          >
            <span>Default Trigger Type</span>
            <InfoTooltip text="Default Structured Streaming trigger mode inherited by streaming sinks unless overridden on a specific sink." />
          </label>
          <select
            id="advancedDefaultTriggerType"
            name="defaultTriggerType"
            value={effectiveTriggerType}
            onChange={handleDefaultTriggerTypeChange}
            className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)] cursor-pointer"
          >
            {STREAMING_TRIGGER_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </div>

        {showTriggerInterval && (
          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedDefaultTriggerInterval"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Default Trigger Interval</span>
              <InfoTooltip text="Default micro-batch or continuous checkpoint interval string, such as 10 seconds or 1 minute." />
            </label>
            <input
              id="advancedDefaultTriggerInterval"
              name="defaultTriggerInterval"
              type="text"
              value={triggerIntervalValue}
              onChange={handleDefaultTriggerIntervalChange}
              placeholder="10 seconds"
              className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
            />
          </div>
        )}
      </div>
    </>
  );
};
