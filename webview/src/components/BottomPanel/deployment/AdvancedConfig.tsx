/**
 * @fileoverview Advanced job and Structured Streaming defaults configuration for Spark pipeline runners.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React from 'react';
import { InfoTooltip } from '../../InfoTooltip';
import { normalizeSparkRunner } from './RunnerSettings';
import { SdpSettingsPanel } from './SdpSettingsPanel';
import { hasIcebergGraphNodes } from '../../../utils/sdpConfigValidation';
import { useAssetLoader } from '../../../hooks/useAssetLoader';
import type {
  PipelineParadigm,
  SdpJobConfigFields,
  SparkPipelineStreamingDefaults,
  SparkStreamingTriggerType,
} from '../../../utils/sharedTypes';

/* eslint-disable react-refresh/only-export-components */

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
 * Option descriptor for discovered Dataproc clusters.
 */
export interface DataprocClusterOption {
  name: string;
  status?: string;
  region?: string;
}

/**
 * Option descriptor for Spark Connect session templates.
 */
export type SessionTemplateOption =
  | string
  | {
      name: string;
      value?: string;
      description?: string;
    };

/**
 * Structure of job configuration fields used in advanced runner and streaming settings.
 */
export interface AdvancedJobConfig
  extends Omit<SparkPipelineStreamingDefaults, 'defaultTriggerType'>,
    SdpJobConfigFields {
  runner: string;
  defaultTriggerType?: SparkStreamingTriggerType | string;
  useSparkConnect?: boolean;
  sessionTemplate?: string;
  clusterName?: string;
  customJarUris?: string;
  sparkJars?: string;
  runtimeVersion?: '4.0' | '3.0' | string;
  tier?: 'standard' | 'premium' | string;
  serverlessTier?: 'standard' | 'premium' | string;
  depsBucket?: string;
  gcsStagingPath?: string;
  serviceAccount?: string;
  batchTtlMinutes?: number | string;
  machineType?: string;
  minInstances?: number;
  maxInstances?: number;
  network?: string;
  subnetwork?: string;
  jobId?: string;
  region?: string;
  projectId?: string;
  [key: string]: unknown;
}

/**
 * Props for the {@link AdvancedConfig} component.
 */
export interface AdvancedConfigProps {
  /** Current job configuration state. */
  jobConfig: AdvancedJobConfig;
  /** State updater callback for modifying the parent job configuration. */
  setJobConfig: React.Dispatch<React.SetStateAction<AdvancedJobConfig>>;
  /** List of selectable VPC subnetworks. */
  subnetworks: SubnetworkOption[];
  /** Optional list of discovered Dataproc clusters for DataprocCluster mode. */
  clusters?: DataprocClusterOption[];
  /** Optional list of discovered Spark Connect session templates for Local mode. */
  sessionTemplates?: SessionTemplateOption[];
  /** Optional list of discovered GCP regions for DataprocCluster mode. */
  regions?: string[];
  /** Optional pipeline paradigm ('imperative' | 'sdp' | 'notebook'). */
  paradigm?: PipelineParadigm;
  /** Optional graph nodes for SDP Iceberg runtime and storage path checks. */
  nodes?: readonly unknown[];
  /** Optional resolved GCP project identifier fallback. */
  projectId?: string;
}

const STREAMING_TRIGGER_OPTIONS: Array<{ value: SparkStreamingTriggerType; label: string }> = [
  { value: 'processingTime', label: 'Processing Time (Interval)' },
  { value: 'availableNow', label: 'Available Now (Incremental Batch)' },
  { value: 'once', label: 'Once (Single Batch)' },
  { value: 'continuous', label: 'Continuous (Low Latency)' },
];

const COMMON_GCP_REGIONS = [
  'us-central1',
  'us-east1',
  'us-east4',
  'us-west1',
  'europe-west1',
  'europe-west4',
  'asia-east1',
  'asia-northeast1',
];

/**
 * Validates that a Serverless Spark `--deps-bucket` value is a root GCS bucket URI (`gs://<bucket>`).
 *
 * Returns a human-readable warning string when invalid (such as missing `gs://` scheme or
 * containing object path segments like `gs://my-bucket/subfolder`), or `null` when valid or empty.
 *
 * @param value - Raw depsBucket input string.
 * @returns Validation warning message, or null if valid.
 */
export function validateDepsBucketUri(value?: string): string | null {
  if (!value || !value.trim()) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed.startsWith('gs://')) {
    return 'Deps Bucket must start with gs:// (for example gs://my-spark-deps-bucket).';
  }
  const withoutScheme = trimmed.slice('gs://'.length).replace(/\/+$/, '');
  if (!withoutScheme) {
    return 'Deps Bucket must specify a root bucket name (gs://<bucket>).';
  }
  if (withoutScheme.includes('/')) {
    return 'Deps Bucket must be a root bucket URI (gs://<bucket>) without object path segments.';
  }
  if (!/^[a-zA-Z0-9._-]+$/.test(withoutScheme)) {
    return 'Deps Bucket contains invalid GCS bucket characters.';
  }
  return null;
}

/**
 * Returns true when the selected Spark trigger strategy requires an interval string.
 *
 * @param triggerType - Selected Spark streaming trigger type.
 * @returns True if `triggerType` uses a time interval.
 */
function triggerUsesInterval(triggerType: SparkStreamingTriggerType): boolean {
  return triggerType === 'processingTime' || triggerType === 'continuous';
}

/**
 * Renders contextual runner options (`Local`, `DataprocCluster`, `ServerlessSpark`) and
 * pipeline-level Structured Streaming defaults.
 *
 * @param props - Current job configuration, state updater, and discovered GCP assets.
 * @returns The rendered advanced runner and streaming configuration controls.
 */
export const AdvancedConfig: React.FC<AdvancedConfigProps> = ({
  jobConfig,
  setJobConfig,
  subnetworks,
  clusters = [],
  sessionTemplates = [],
  regions = [],
  paradigm,
  nodes = [],
  projectId,
}) => {
  const effectiveRunner = normalizeSparkRunner(jobConfig.runner);
  const isSdpMode = paradigm === 'sdp' || jobConfig.paradigm === 'sdp';
  const hasIcebergNodes = React.useMemo(() => hasIcebergGraphNodes(nodes), [nodes]);
  const clusterRuntimeVersion =
    jobConfig.runtimeVersion === '3.0' ? '3.0' : '4.0';

  const effectiveTriggerType: SparkStreamingTriggerType =
    jobConfig.defaultTriggerType === 'availableNow' ||
    jobConfig.defaultTriggerType === 'once' ||
    jobConfig.defaultTriggerType === 'continuous'
      ? jobConfig.defaultTriggerType
      : 'processingTime';
  const showTriggerInterval = triggerUsesInterval(effectiveTriggerType);
  const checkpointRootValue = jobConfig.checkpointRoot ?? '';
  const triggerIntervalValue = jobConfig.defaultTriggerInterval ?? '10 seconds';

  // Local mode values (R2)
  const useSparkConnectValue = Boolean(jobConfig.useSparkConnect);
  const sessionTemplateValue = jobConfig.sessionTemplate ?? '';

  // DataprocCluster mode values (R3)
  const clusterNameValue = jobConfig.clusterName ?? '';
  const regionValue = jobConfig.region || 'us-central1';
  const customJarUrisValue = jobConfig.customJarUris ?? jobConfig.sparkJars ?? '';

  // Live GCP asset discovery for clusters (D1), session templates (D2), regions & buckets (D5)
  const { items: loadedClusters } = useAssetLoader<DataprocClusterOption | string>(
    'dataprocClusters',
    {
      params: { region: regionValue },
      enabled: effectiveRunner === 'DataprocCluster',
    }
  );
  const { items: loadedSessionTemplates } = useAssetLoader<SessionTemplateOption>(
    'sessionTemplates',
    {
      params: { region: regionValue },
      enabled: effectiveRunner === 'Local',
    }
  );
  const { items: loadedRegions } = useAssetLoader<string | { name?: string; value?: string }>(
    'regions',
    {
      enabled: effectiveRunner === 'DataprocCluster',
    }
  );
  const { items: loadedBuckets } = useAssetLoader<string>('storageBuckets', {
    enabled: effectiveRunner === 'ServerlessSpark',
  });

  const effectiveClusters: DataprocClusterOption[] = React.useMemo(() => {
    const map = new Map<string, DataprocClusterOption>();
    for (const item of [...clusters, ...loadedClusters]) {
      const normalized: DataprocClusterOption =
        typeof item === 'string' ? { name: item } : item;
      if (normalized && normalized.name && !map.has(normalized.name)) {
        map.set(normalized.name, normalized);
      }
    }
    return Array.from(map.values());
  }, [clusters, loadedClusters]);

  const effectiveSessionTemplates: SessionTemplateOption[] = React.useMemo(() => {
    const map = new Map<string, SessionTemplateOption>();
    for (const tpl of [...sessionTemplates, ...loadedSessionTemplates]) {
      const key = typeof tpl === 'string' ? tpl : tpl.value || tpl.name;
      if (key && !map.has(key)) {
        map.set(key, tpl);
      }
    }
    return Array.from(map.values());
  }, [sessionTemplates, loadedSessionTemplates]);

  const effectiveRegions: string[] = React.useMemo(() => {
    const rawList = loadedRegions.length > 0 ? loadedRegions : regions;
    const normalized = rawList
      .map((r) => (typeof r === 'string' ? r : r?.value || r?.name || ''))
      .filter((r): r is string => Boolean(r && r.trim()));
    return normalized.length > 0 ? Array.from(new Set(normalized)) : COMMON_GCP_REGIONS;
  }, [loadedRegions, regions]);

  const depsBucketSuggestions: string[] = React.useMemo(() => {
    return loadedBuckets
      .map((b) => {
        const trimmed = typeof b === 'string' ? b.trim().replace(/\/+$/, '') : '';
        if (!trimmed) return '';
        return trimmed.startsWith('gs://') ? trimmed : `gs://${trimmed}`;
      })
      .filter(Boolean);
  }, [loadedBuckets]);

  // ServerlessSpark mode values (R4)
  const runtimeVersionValue = jobConfig.runtimeVersion === '3.0' ? '3.0' : '4.0';
  const tierValue =
    jobConfig.tier === 'premium' || jobConfig.serverlessTier === 'premium'
      ? 'premium'
      : 'standard';
  const depsBucketValue = jobConfig.depsBucket ?? jobConfig.gcsStagingPath ?? '';
  const depsBucketWarning = validateDepsBucketUri(depsBucketValue);
  const serviceAccountValue = jobConfig.serviceAccount ?? '';
  const batchTtlMinutesValue = jobConfig.batchTtlMinutes ?? '';

  const handleUseSparkConnectChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const checked = e.target.checked;
    setJobConfig((prev) => ({ ...prev, useSparkConnect: checked }));
  };

  const handleSessionTemplateChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>
  ) => {
    const nextValue = e.target.value;
    setJobConfig((prev) => ({ ...prev, sessionTemplate: nextValue }));
  };

  const handleClusterNameChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>
  ) => {
    const nextValue = e.target.value;
    setJobConfig((prev) => ({ ...prev, clusterName: nextValue }));
  };

  const handleRegionChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev) => ({ ...prev, region: nextValue }));
  };

  const handleCustomJarUrisChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev) => ({
      ...prev,
      customJarUris: nextValue,
      sparkJars: nextValue,
    }));
  };

  const handleRuntimeVersionChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev) => ({ ...prev, runtimeVersion: nextValue }));
  };

  const handleTierChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextValue = e.target.value === 'premium' ? 'premium' : 'standard';
    setJobConfig((prev) => ({
      ...prev,
      tier: nextValue,
      serverlessTier: nextValue,
    }));
  };

  const handleDepsBucketChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev) => ({
      ...prev,
      depsBucket: nextValue,
      gcsStagingPath: nextValue,
    }));
  };

  const handleServiceAccountChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev) => ({ ...prev, serviceAccount: nextValue }));
  };

  const handleBatchTtlChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const raw = e.target.value;
    if (raw === '') {
      setJobConfig((prev) => ({ ...prev, batchTtlMinutes: '' }));
      return;
    }
    const parsed = parseInt(raw, 10);
    const nextValue = !Number.isNaN(parsed) && String(parsed) === raw.trim() ? parsed : raw;
    setJobConfig((prev) => ({ ...prev, batchTtlMinutes: nextValue }));
  };

  const handleSubnetworkChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev) => ({ ...prev, subnetwork: nextValue }));
  };

  const handleCheckpointRootChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev) => ({ ...prev, checkpointRoot: nextValue }));
  };

  const handleDefaultTriggerTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextTrigger = e.target.value as SparkStreamingTriggerType;
    setJobConfig((prev) => ({
      ...prev,
      defaultTriggerType: nextTrigger,
      ...(triggerUsesInterval(nextTrigger) && prev.defaultTriggerInterval === undefined
        ? { defaultTriggerInterval: '10 seconds' }
        : {}),
    }));
  };

  const handleDefaultTriggerIntervalChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const nextValue = e.target.value;
    setJobConfig((prev) => ({ ...prev, defaultTriggerInterval: nextValue }));
  };

  return (
    <>
      {/* Contextual Options: Local PySpark / Spark Connect (R2) */}
      {effectiveRunner === 'Local' && (
        <div
          data-testid="local-runner-options"
          className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-lg border border-[var(--vscode-border)]"
        >
          <div className="text-xs font-bold uppercase tracking-wider text-blue-400">
            Local &amp; Spark Connect Options
          </div>

          {isSdpMode && (
            <div
              data-testid="sdp-local-connect-info"
              className="p-2 rounded bg-purple-950/30 border border-purple-700/50 text-[11px] text-purple-200 leading-snug"
            >
              Profile <code className="font-mono">4.2-connect</code> runs{' '}
              <code className="font-mono">spark-pipelines</code> over Spark Connect (
              <code className="font-mono">--remote &quot;local[*]&quot;</code>) on Apache Spark 4.2+.
            </div>
          )}

          {isSdpMode && hasIcebergNodes && (
            <div
              role="alert"
              data-testid="sdp-local-iceberg-warning"
              className="p-2 rounded bg-amber-950/40 border border-amber-600/60 text-[11px] text-amber-200 leading-snug"
            >
              Local Spark 4.2 warning: Maven Central lacks{' '}
              <code className="font-mono">iceberg-spark-runtime-4.2_2.13</code>, and local{' '}
              <code className="font-mono">run</code> and <code className="font-mono">dry-run</code>{' '}
              both load <code className="font-mono">org.apache.iceberg.spark.SparkCatalog</code>.
              Run <code className="font-mono">./run_sdp_local.sh --test</code> for offline{' '}
              <code className="font-mono">pytest</code>, supply{' '}
              <code className="font-mono">SPARK_JARS</code>, or execute on Serverless Spark 4.0 or
              Dataproc 4.0.
            </div>
          )}

          <label
            htmlFor="advancedUseSparkConnect"
            className="flex items-center gap-2 text-xs text-[var(--vscode-text-primary)] cursor-pointer select-none"
          >
            <input
              id="advancedUseSparkConnect"
              name="useSparkConnect"
              type="checkbox"
              checked={useSparkConnectValue}
              onChange={handleUseSparkConnectChange}
              className="rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)] text-blue-500 focus:ring-0 cursor-pointer"
            />
            <span className="font-semibold">Use Spark Connect (ManagedSparkSession)</span>
            <InfoTooltip text="When enabled, initializes DataprocSparkSession instead of a local JVM SparkSession." />
          </label>

          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedSessionTemplate"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Runtime Profile (Session Template)</span>
              <InfoTooltip text="Optional Dataproc template name or resource URI used when connecting remotely." />
            </label>
            <input
              id="advancedSessionTemplate"
              name="sessionTemplate"
              type="text"
              list="advancedSessionTemplatesList"
              value={sessionTemplateValue}
              onChange={handleSessionTemplateChange}
              placeholder="projects/<project>/locations/<region>/sessionTemplates/<name>"
              className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
            />
            <datalist id="advancedSessionTemplatesList">
              {effectiveSessionTemplates.map((tpl, idx) => {
                const value = typeof tpl === 'string' ? tpl : tpl.value || tpl.name;
                const label = typeof tpl === 'string' ? tpl : tpl.name;
                return (
                  <option key={idx} value={value}>
                    {label}
                  </option>
                );
              })}
            </datalist>
          </div>
        </div>
      )}

      {/* Contextual Options: Dataproc Cluster (GCE/GKE) (R3) */}
      {effectiveRunner === 'DataprocCluster' && (
        <div
          data-testid="dataproc-cluster-options"
          className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-lg border border-[var(--vscode-border)]"
        >
          <div className="text-xs font-bold uppercase tracking-wider text-cyan-400">
            Dataproc Cluster Options
          </div>

          {isSdpMode && (
            <>
              <div className="flex flex-col gap-1">
                <label
                  htmlFor="advancedClusterImageVersion"
                  className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
                >
                  <span>Dataproc Image Version</span>
                  <InfoTooltip text="Compute Engine release image. SDP requires image 4.0 (Spark 4.2+)." />
                </label>
                <select
                  id="advancedClusterImageVersion"
                  name="runtimeVersion"
                  value={clusterRuntimeVersion}
                  onChange={handleRuntimeVersionChange}
                  className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs focus:outline-none focus:border-[var(--vscode-highlight)] cursor-pointer"
                >
                  <option value="4.0">4.0 (4.0-debian13 — Spark 4.2)</option>
                  <option value="3.0" disabled>
                    3.0 (Spark 4.1 — Unsupported in SDP)
                  </option>
                </select>
              </div>

              {clusterRuntimeVersion === '3.0' && (
                <div
                  role="alert"
                  data-testid="sdp-runtime-version-warning"
                  className="p-2 rounded bg-amber-950/40 border border-amber-600/60 text-[11px] text-amber-200 leading-snug"
                >
                  SDP_SPARK_42_REQUIRED: Spark Declarative Pipelines requires Dataproc image 4.0+
                  (Apache Spark 4.2). Switch to image 4.0.
                </div>
              )}

              <div
                data-testid="sdp-dataproc-cluster-note"
                className="p-2 rounded bg-cyan-950/30 border border-cyan-700/50 text-[11px] text-cyan-200 leading-snug"
              >
                Dataproc image <code className="font-mono">4.0</code> requires{' '}
                <code className="font-mono">--optional-components=ICEBERG</code> and driver Python
                package <code className="font-mono">zstandard&gt;=0.25.0</code>.
              </div>
            </>
          )}

          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedClusterName"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Cluster Name</span>
              <InfoTooltip text="Target RUNNING Dataproc GCE or GKE target in the selected location." />
            </label>
            <input
              id="advancedClusterName"
              name="clusterName"
              type="text"
              list="advancedDataprocClustersList"
              value={clusterNameValue}
              onChange={handleClusterNameChange}
              placeholder="my-spark-cluster"
              className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
            />
            <datalist id="advancedDataprocClustersList">
              {effectiveClusters.map((cluster, idx) => (
                <option key={idx} value={cluster.name}>
                  {cluster.name}
                  {cluster.status ? ` (${cluster.status})` : ''}
                </option>
              ))}
            </datalist>
          </div>

          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedClusterRegion"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Region</span>
              <InfoTooltip text="GCP location hosting the target Dataproc workload (defaults to us-central1)." />
            </label>
            <input
              id="advancedClusterRegion"
              name="region"
              type="text"
              list="advancedClusterRegionsList"
              value={regionValue}
              onChange={handleRegionChange}
              placeholder="us-central1"
              className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
            />
            <datalist id="advancedClusterRegionsList">
              {effectiveRegions.map((reg) => (
                <option key={reg} value={reg}>
                  {reg}
                </option>
              ))}
            </datalist>
          </div>

          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedClusterSubnetwork"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Subnetwork</span>
              <InfoTooltip text="VPC network path used by worker nodes." />
            </label>
            <select
              id="advancedClusterSubnetwork"
              name="subnetwork"
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

          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedCustomJarUris"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Custom JAR URIs (gs://)</span>
              <InfoTooltip text="Optional comma-separated Cloud Storage archive paths passed via --jars." />
            </label>
            <input
              id="advancedCustomJarUris"
              name="customJarUris"
              type="text"
              value={customJarUrisValue}
              onChange={handleCustomJarUrisChange}
              placeholder="gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar"
              className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)]"
            />
          </div>
        </div>
      )}

      {/* Contextual Options: GCP Serverless Spark (Batch) (R4) */}
      {effectiveRunner === 'ServerlessSpark' && (
        <div
          data-testid="serverless-spark-options"
          className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-lg border border-[var(--vscode-border)]"
        >
          <div className="text-xs font-bold uppercase tracking-wider text-amber-400">
            Serverless Spark Options
          </div>

          <div className="grid grid-cols-2 gap-2">
            <div className="flex flex-col gap-1">
              <label
                htmlFor="advancedRuntimeVersion"
                className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
              >
                <span>Runtime Version</span>
                <InfoTooltip text="Serverless for Apache Spark image release. 4.0 provides Spark 4.2, Python 3.13, and Java 25." />
              </label>
              <select
                id="advancedRuntimeVersion"
                name="runtimeVersion"
                value={runtimeVersionValue}
                onChange={handleRuntimeVersionChange}
                className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs focus:outline-none focus:border-[var(--vscode-highlight)] cursor-pointer"
              >
                <option value="4.0">4.0 (Spark 4.2)</option>
                <option value="3.0" disabled={isSdpMode}>
                  {isSdpMode ? '3.0 (Spark 4.0 — Unsupported in SDP)' : '3.0 (Spark 4.0)'}
                </option>
              </select>
            </div>

            <div className="flex flex-col gap-1">
              <label
                htmlFor="advancedServerlessTier"
                className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
              >
                <span>Tier</span>
                <InfoTooltip text="Serverless compute level. Defaults to standard." />
              </label>
              <select
                id="advancedServerlessTier"
                name="tier"
                value={tierValue}
                onChange={handleTierChange}
                className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs focus:outline-none focus:border-[var(--vscode-highlight)] cursor-pointer"
              >
                <option value="standard">standard</option>
                <option value="premium">premium</option>
              </select>
            </div>
          </div>

          {isSdpMode && runtimeVersionValue === '3.0' && (
            <div
              role="alert"
              data-testid="sdp-runtime-version-warning"
              className="p-2 rounded bg-amber-950/40 border border-amber-600/60 text-[11px] text-amber-200 leading-snug"
            >
              SDP_SPARK_42_REQUIRED: Spark Declarative Pipelines requires Serverless Runtime 4.0+
              (Apache Spark 4.2+). Select Runtime Version 4.0.
            </div>
          )}

          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedDepsBucket"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Deps Bucket (gs://&lt;bucket&gt;)</span>
              <InfoTooltip text="Root Cloud Storage URI used for staging dependencies (--deps-bucket=gs://<bucket>)." />
            </label>
            <input
              id="advancedDepsBucket"
              name="depsBucket"
              type="text"
              list="advancedDepsBucketsList"
              value={depsBucketValue}
              onChange={handleDepsBucketChange}
              placeholder="gs://<bucket>"
              aria-invalid={Boolean(depsBucketWarning)}
              className={`bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border text-xs font-mono focus:outline-none ${
                depsBucketWarning
                  ? 'border-amber-500 focus:border-amber-400'
                  : 'border-[var(--vscode-border)] focus:border-[var(--vscode-highlight)]'
              }`}
            />
            <datalist id="advancedDepsBucketsList">
              {depsBucketSuggestions.map((bucketUri) => (
                <option key={bucketUri} value={bucketUri}>
                  {bucketUri}
                </option>
              ))}
            </datalist>
            {depsBucketWarning && (
              <span
                role="alert"
                data-testid="deps-bucket-warning"
                className="text-[11px] text-amber-400 leading-snug"
              >
                {depsBucketWarning}
              </span>
            )}
          </div>

          <div className="flex flex-col gap-1">
            <label
              htmlFor="advancedSparkSubnetwork"
              className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
            >
              <span>Subnetwork</span>
              <InfoTooltip text="VPC network path used for Serverless Spark executor networking (requires Private Google Access)." />
            </label>
            <select
              id="advancedSparkSubnetwork"
              name="subnetwork"
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

          <div className="grid grid-cols-2 gap-2">
            <div className="flex flex-col gap-1">
              <label
                htmlFor="advancedServiceAccount"
                className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
              >
                <span>Service Account</span>
                <InfoTooltip text="Optional custom IAM workload identity email for Serverless Spark execution." />
              </label>
              <input
                id="advancedServiceAccount"
                name="serviceAccount"
                type="text"
                value={serviceAccountValue}
                onChange={handleServiceAccountChange}
                placeholder="spark-sa@project.iam.gserviceaccount.com"
                className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)] w-full"
              />
            </div>

            <div className="flex flex-col gap-1">
              <label
                htmlFor="advancedBatchTtlMinutes"
                className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
              >
                <span>Batch TTL (minutes)</span>
                <InfoTooltip text="Maximum batch lifetime (--ttl). Serverless Spark defaults to 240m (4h)." />
              </label>
              <input
                id="advancedBatchTtlMinutes"
                name="batchTtlMinutes"
                type="number"
                min={10}
                max={20160}
                value={batchTtlMinutesValue}
                onChange={handleBatchTtlChange}
                placeholder="240"
                className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs font-mono focus:outline-none focus:border-[var(--vscode-highlight)] w-full"
              />
            </div>
          </div>
        </div>
      )}

      {/* Spark Declarative Pipelines (SDP 4.2+) Spec & Catalog Settings (SDP3) */}
      {isSdpMode && (
        <SdpSettingsPanel
          jobConfig={jobConfig}
          setJobConfig={setJobConfig}
          nodes={nodes}
          projectId={projectId ?? jobConfig.projectId}
        />
      )}

      {/* Pipeline-Level Structured Streaming Defaults (Imperative mode only. SDP uses sdpStorage and Trigger.AvailableNow) */}
      {!isSdpMode && (
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
      )}
    </>
  );
};
