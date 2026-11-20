/**
 * @fileoverview Main DeploymentTab component for bottom panel deployment settings and execution sandbox.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
/* eslint-disable @typescript-eslint/no-explicit-any */
import React, { useState } from 'react';
import {
  Play,
  Square,
  Sliders,
  Loader2,
  AlertTriangle,
} from 'lucide-react';
import { RunnerSettings, normalizeSparkRunner } from './deployment/RunnerSettings';
import { AdvancedConfig } from './deployment/AdvancedConfig';
import {
  SdpRefreshControls,
  type SdpRefreshDatasetOption,
} from './deployment/SdpRefreshControls';
import { RuntimeVariablesGrid } from './deployment/RuntimeVariablesGrid';
import { ExecutionTerminal, type LogEntry } from './deployment/ExecutionTerminal';
import { buildGcpConsoleUrl } from './deployment/consoleUrl';
import { useBottomPanel } from './BottomPanelContext';
import { buildSdpPlan, sanitizeSdpIdentifier } from '../../utils/sdpPlanner';

export type { LogEntry };

/**
 * Props for the DeploymentTab component.
 */
export type DeploymentTabProps = Record<string, never>;

const LAKEHOUSE_CATALOG_SINKS = new Set([
  'IcebergSink',
  'DeltaSink',
  'HudiSink',
  'IcebergIO',
]);

/**
 * Sanitizes a raw node label or table name into a valid snake_case SDP dataset identifier.
 *
 * @param raw - Raw candidate identifier string.
 * @param fallback - Fallback identifier when raw sanitizes to empty.
 * @returns Plain snake_case identifier without reserved __spark_autocdc_ prefix.
 */
function fallbackSanitizeSdpName(raw: string, fallback = 'dataset'): string {
  let cleaned = raw
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '');
  if (!cleaned) {
    cleaned = fallback;
  }
  if (/^[0-9]/.test(cleaned)) {
    cleaned = `ds_${cleaned}`;
  }
  if (cleaned.startsWith('__spark_autocdc_')) {
    cleaned = cleaned.replace(/^__spark_autocdc_+/, 'cdc_') || fallback;
  }
  return cleaned;
}

/**
 * Checks whether node properties disable SDP full-refresh resets via pipelines.reset.allowed=false.
 *
 * @param data - Node data record.
 * @param props - Node properties record.
 * @returns False when reset protection is enabled, otherwise true.
 */
function resolveNodeResetAllowed(
  data?: Record<string, unknown>,
  props?: Record<string, unknown>
): boolean {
  if (data?.sdpResetAllowed === false || props?.sdpResetAllowed === false) {
    return false;
  }
  const tableProps = props?.tableProperties;
  if (typeof tableProps === 'string') {
    if (/pipelines\.reset\.allowed\s*[=:]\s*['"]?false['"]?/i.test(tableProps)) {
      return false;
    }
  } else if (tableProps && typeof tableProps === 'object') {
    const val = (tableProps as Record<string, unknown>)['pipelines.reset.allowed'];
    if (val === false || String(val).toLowerCase() === 'false') {
      return false;
    }
  }
  return true;
}

/**
 * Extracts SDP dataset descriptors from canvas nodes using node SDP metadata with fallback to buildSdpPlan.
 *
 * @param nodes - Graph nodes from BottomPanel codegen context.
 * @param jobConfig - Optional pipeline job configuration passed to buildSdpPlan.
 * @returns Deduplicated array of SDP dataset options for refresh controls.
 */
export function extractSdpDatasetsFromGraph(
  nodes?: readonly unknown[],
  jobConfig?: Record<string, unknown>
): SdpRefreshDatasetOption[] {
  if (!Array.isArray(nodes) || nodes.length === 0) {
    return [];
  }

  const plan = buildSdpPlan(nodes as any, [], jobConfig, {
    enforceRuntimeVersion: false,
  });
  const byName = new Map<string, SdpRefreshDatasetOption>();

  for (const rawNode of nodes) {
    if (!rawNode || typeof rawNode !== 'object') {
      continue;
    }
    const record = rawNode as Record<string, unknown>;
    const nodeId = typeof record.id === 'string' && record.id ? record.id : 'node';
    const data =
      record.data && typeof record.data === 'object'
        ? (record.data as Record<string, unknown>)
        : undefined;
    const props =
      (data?.properties && typeof data.properties === 'object'
        ? (data.properties as Record<string, unknown>)
        : record.properties && typeof record.properties === 'object'
          ? (record.properties as Record<string, unknown>)
          : undefined) ?? {};

    const nodeLabel = String(data?.label ?? record.label ?? nodeId);
    const transformRaw = String(
      data?.sparkTransform ??
        data?.spark_transform ??
        record.sparkTransform ??
        record.spark_transform ??
        ''
    );
    const transform = transformRaw.replace(/^(SOURCE|SINK|STEP):/i, '');
    const nodeTypeRaw = String(
      data?.type ?? data?.nodeType ?? record.type ?? ''
    ).toUpperCase();

    const planned = plan.datasetsByNodeId[nodeId];

    const explicitName =
      typeof data?.sdpDatasetName === 'string' && data.sdpDatasetName.trim()
        ? data.sdpDatasetName.trim()
        : typeof props.sdpTableName === 'string' && props.sdpTableName.trim()
          ? sanitizeSdpIdentifier(props.sdpTableName)
          : typeof props.sdpSinkName === 'string' && props.sdpSinkName.trim()
            ? sanitizeSdpIdentifier(props.sdpSinkName)
            : planned?.datasetName
              ? planned.datasetName
              : typeof props.table === 'string' && props.table.trim()
                ? sanitizeSdpIdentifier(props.table)
                : sanitizeSdpIdentifier(nodeLabel, fallbackSanitizeSdpName(nodeId));

    let kind: SdpRefreshDatasetOption['kind'] = planned?.kind ?? 'streaming_table';
    let badge: SdpRefreshDatasetOption['badge'] = planned?.badge ?? 'TABLE';

    if (
      data?.sdpKind === 'temporary_view' ||
      data?.sdpKind === 'materialized_view' ||
      data?.sdpKind === 'streaming_table' ||
      data?.sdpKind === 'append_flow' ||
      data?.sdpKind === 'auto_cdc' ||
      data?.sdpKind === 'external_sink'
    ) {
      kind = data.sdpKind;
    } else if (!planned) {
      if (props.sdpDatasetType === 'temporary_view') {
        kind = 'temporary_view';
      } else if (props.sdpDatasetType === 'materialized_view') {
        kind = 'materialized_view';
      } else if (transform === 'MergeCdcScd') {
        kind = 'auto_cdc';
      } else if (
        nodeTypeRaw.includes('SINK') ||
        transformRaw.startsWith('SINK:') ||
        transform.endsWith('Sink')
      ) {
        if (
          props.sdpSinkKind === 'external_sink' ||
          (!LAKEHOUSE_CATALOG_SINKS.has(transform) && props.sdpSinkKind !== 'catalog_table')
        ) {
          kind = 'external_sink';
        } else {
          kind = 'streaming_table';
        }
      } else if (transform === 'GroupByAgg' || transform === 'RollupCube') {
        kind = 'materialized_view';
      } else if (
        (nodeTypeRaw.includes('SOURCE') ||
          transformRaw.startsWith('SOURCE:') ||
          transform.endsWith('Source')) &&
        props.readMode === 'batch'
      ) {
        kind = 'temporary_view';
      }
    }

    if (
      data?.sdpBadge === 'VIEW' ||
      data?.sdpBadge === 'MV' ||
      data?.sdpBadge === 'TABLE' ||
      data?.sdpBadge === 'CDC' ||
      data?.sdpBadge === 'SINK'
    ) {
      badge = data.sdpBadge;
    } else if (!planned) {
      badge =
        kind === 'temporary_view'
          ? 'VIEW'
          : kind === 'materialized_view'
            ? 'MV'
            : kind === 'auto_cdc'
              ? 'CDC'
              : kind === 'external_sink'
                ? 'SINK'
                : 'TABLE';
    }

    const resetAllowed =
      kind === 'external_sink'
        ? true
        : resolveNodeResetAllowed(data, props) && (planned?.resetAllowed ?? true);

    const existing = byName.get(explicitName);
    if (existing) {
      if (kind === 'auto_cdc') {
        existing.kind = 'auto_cdc';
        existing.badge = 'CDC';
      }
      if (!resetAllowed && existing.kind !== 'external_sink') {
        existing.resetAllowed = false;
      }
    } else {
      byName.set(explicitName, {
        nodeId,
        nodeLabel,
        datasetName: explicitName,
        kind,
        badge,
        resetAllowed,
      });
    }

    // Include quarantine dataset generated for DataQualityAssert
    const quarantineName =
      typeof data?.quarantineDatasetName === 'string' && data.quarantineDatasetName.trim()
        ? data.quarantineDatasetName.trim()
        : planned?.quarantineDatasetName
          ? planned.quarantineDatasetName
          : transform === 'DataQualityAssert'
            ? `${explicitName}_quarantine`
            : undefined;
    if (quarantineName && !byName.has(quarantineName)) {
      byName.set(quarantineName, {
        nodeId: `${nodeId}-quarantine`,
        nodeLabel: `${nodeLabel} Quarantine`,
        datasetName: quarantineName,
        kind: 'streaming_table',
        badge: 'TABLE',
        resetAllowed: true,
      });
    }
  }

  return Array.from(byName.values());
}

/**
 * Builds the default command line preview string for the selected Spark target environment.
 *
 * @param runner - Selected runner identifier.
 * @param jobConfig - Current job configuration object.
 * @param effectiveProjectId - Resolved GCP Project ID.
 * @param effectiveRegion - Resolved GCP region.
 * @param paradigm - Optional pipeline paradigm ('imperative' | 'sdp').
 * @returns Multi-line shell command preview string.
 */
export function buildDefaultCommandPreview(
  runner: string,
  jobConfig: Record<string, unknown>,
  effectiveProjectId: string,
  effectiveRegion: string,
  paradigm?: 'imperative' | 'sdp'
): string {
  const normalized = normalizeSparkRunner(runner);
  const isSdp = paradigm === 'sdp' || jobConfig.paradigm === 'sdp';

  if (isSdp) {
    const sdpCommand = jobConfig.sdpCommand === 'dry-run' ? 'dry-run' : 'run';
    const refreshMode =
      typeof jobConfig.sdpRefreshMode === 'string' ? jobConfig.sdpRefreshMode : 'incremental';
    const refreshList = Array.isArray(jobConfig.sdpRefreshDatasets)
      ? jobConfig.sdpRefreshDatasets.filter((d): d is string => typeof d === 'string' && Boolean(d))
      : [];
    const fullRefreshList = Array.isArray(jobConfig.sdpFullRefreshDatasets)
      ? jobConfig.sdpFullRefreshDatasets.filter(
          (d): d is string => typeof d === 'string' && Boolean(d)
        )
      : [];

    const envParts: string[] = [];
    let cliFlag = '';
    if (sdpCommand === 'dry-run') {
      envParts.push('SDP_COMMAND=dry-run');
    } else {
      if (refreshMode === 'refresh' && refreshList.length > 0) {
        const joined = refreshList.join(',');
        envParts.push(`SDP_REFRESH=${joined}`);
        cliFlag = ` --refresh ${joined}`;
      } else if (refreshMode === 'full_refresh' && fullRefreshList.length > 0) {
        const joined = fullRefreshList.join(',');
        envParts.push(`SDP_FULL_REFRESH=${joined}`);
        cliFlag = ` --full-refresh ${joined}`;
      } else if (refreshMode === 'full_refresh_all') {
        envParts.push('SDP_FULL_REFRESH_ALL=true');
        cliFlag = ' --full-refresh-all';
      }
    }

    const cliPreview = `spark-pipelines ${sdpCommand} --spec spark-pipeline.yml${cliFlag}`;
    const envPrefix = envParts.length > 0 ? `${envParts.join(' ')} ` : '';

    if (normalized === 'DataprocCluster') {
      const clusterName =
        typeof jobConfig.clusterName === 'string' && jobConfig.clusterName.trim()
          ? jobConfig.clusterName.trim()
          : '<cluster>';
      return `# Submitting Spark Declarative Pipeline to Dataproc 4.0 Cluster (${cliPreview})\nPROJECT_ID=${effectiveProjectId} REGION=${effectiveRegion} CLUSTER_NAME=${clusterName} ${envPrefix}./run_sdp_dataproc.sh`;
    }
    if (normalized === 'ServerlessSpark') {
      return `# Managed Data Pipelines sdp Stage (sdp_stage.pipeline.yaml -> ${cliPreview})\nPROJECT_ID=${effectiveProjectId} REGION=${effectiveRegion} ${envPrefix}./run_sdp_local.sh`;
    }
    return `# Running Spark Declarative Pipeline over Local Spark 4.2 Connect (${cliPreview})\n${envPrefix}./run_sdp_local.sh`;
  }

  if (normalized === 'DataprocCluster') {
    const clusterName =
      typeof jobConfig.clusterName === 'string' && jobConfig.clusterName.trim()
        ? jobConfig.clusterName.trim()
        : '<cluster>';
    return `# Submitting PySpark Job to Google Cloud Dataproc Cluster\nPROJECT_ID=${effectiveProjectId} REGION=${effectiveRegion} CLUSTER_NAME=${clusterName} ./run_dataproc_cluster.sh`;
  }
  if (normalized === 'ServerlessSpark') {
    const depsBucket =
      (typeof jobConfig.depsBucket === 'string' && jobConfig.depsBucket.trim()) ||
      (typeof jobConfig.gcsStagingPath === 'string' && jobConfig.gcsStagingPath.trim()) ||
      'gs://<bucket>';
    return `# Submitting PySpark Batch to GCP Serverless for Apache Spark\nPROJECT_ID=${effectiveProjectId} REGION=${effectiveRegion} DEPS_BUCKET=${depsBucket} ./run_serverless_spark.sh`;
  }
  if (jobConfig.useSparkConnect) {
    return `# Running PySpark Pipeline via Spark Connect (ManagedSparkSession)\nUSE_SPARK_CONNECT=true ./run_local.sh`;
  }
  return `# Running Local PySpark 4.x Pipeline (local[*])\n./run_local.sh`;
}

/**
 * Deployment panel rendering pipeline execution configuration controls, runtime variables, and the live log terminal.
 *
 * @returns The rendered deployment tab split view.
 */
export const DeploymentTab: React.FC<DeploymentTabProps> = () => {
  const {
    execution,
    jobConfig: jobConfigSlice,
    runtimeVariables: rvSlice,
    pipelineConfig,
    codegen,
  } = useBottomPanel();

  const [cancelRemoteJob, setCancelRemoteJob] = useState<boolean>(true);
  const [showFullRefreshConfirm, setShowFullRefreshConfirm] = useState<boolean>(false);

  const jobConfig = React.useMemo(
    () => jobConfigSlice.value ?? { runner: 'Local' },
    [jobConfigSlice.value]
  );
  const setJobConfig = jobConfigSlice.set;
  const runtimeVariables = rvSlice.variables;
  const addVariable = rvSlice.addVariable;
  const updateVariable = rvSlice.updateVariable;
  const deleteVariable = rvSlice.deleteVariable;
  const isJobIdValid = jobConfigSlice.isJobIdValid;
  const onRunNow = execution.run;
  const onStopPipeline = execution.stop;
  const subnetworks = jobConfigSlice.subnetworks;
  const clusters = jobConfigSlice.clusters;
  const sessionTemplates = jobConfigSlice.sessionTemplates;
  const needsRebuild = pipelineConfig.config.needsRebuild;
  const paradigm: 'imperative' | 'sdp' =
    pipelineConfig.config.paradigm === 'sdp' || jobConfig.paradigm === 'sdp'
      ? 'sdp'
      : 'imperative';
  const isSdpMode = paradigm === 'sdp';
  const graphNodes = React.useMemo(() => codegen.nodes ?? [], [codegen.nodes]);
  const pipelineLogs = execution.logs;
  const pipelineStatus = execution.status;
  const runnerCommand = execution.runnerCommand;
  const onClearLogs = execution.clear;
  const onOpenConsole = execution.openConsole;
  const projectId = jobConfigSlice.projectId ?? 'YOUR_PROJECT_ID';

  const sdpDatasets = React.useMemo(
    () => (isSdpMode ? extractSdpDatasetsFromGraph(graphNodes, jobConfig) : []),
    [isSdpMode, graphNodes, jobConfig]
  );

  const persistedSdpDatasets = React.useMemo(
    () => sdpDatasets.filter((d) => d.kind !== 'temporary_view' && d.badge !== 'VIEW'),
    [sdpDatasets]
  );

  const sdpCommand: 'run' | 'dry-run' =
    jobConfig.sdpCommand === 'dry-run' ? 'dry-run' : 'run';
  const sdpRefreshMode: 'incremental' | 'refresh' | 'full_refresh' | 'full_refresh_all' =
    jobConfig.sdpRefreshMode === 'refresh' ||
    jobConfig.sdpRefreshMode === 'full_refresh' ||
    jobConfig.sdpRefreshMode === 'full_refresh_all'
      ? jobConfig.sdpRefreshMode
      : 'incremental';

  const sdpRefreshList = React.useMemo<string[]>(
    () =>
      Array.isArray(jobConfig.sdpRefreshDatasets)
        ? jobConfig.sdpRefreshDatasets.filter((d: unknown): d is string => typeof d === 'string')
        : [],
    [jobConfig.sdpRefreshDatasets]
  );
  const sdpFullRefreshList = React.useMemo<string[]>(
    () =>
      Array.isArray(jobConfig.sdpFullRefreshDatasets)
        ? jobConfig.sdpFullRefreshDatasets.filter(
            (d: unknown): d is string => typeof d === 'string'
          )
        : [],
    [jobConfig.sdpFullRefreshDatasets]
  );

  // Datasets targeted for destructive reset when full_refresh or full_refresh_all is active
  const resetTargetDatasets = React.useMemo(() => {
    if (!isSdpMode || sdpCommand !== 'run') {
      return [];
    }
    if (sdpRefreshMode === 'full_refresh') {
      return persistedSdpDatasets.filter(
        (d) =>
          sdpFullRefreshList.includes(d.datasetName) &&
          (d.resetAllowed || d.kind === 'external_sink')
      );
    }
    if (sdpRefreshMode === 'full_refresh_all') {
      return persistedSdpDatasets.filter((d) => d.resetAllowed || d.kind === 'external_sink');
    }
    return [];
  }, [isSdpMode, sdpCommand, sdpRefreshMode, persistedSdpDatasets, sdpFullRefreshList]);

  const resetAutoCdcTargets = React.useMemo(
    () => resetTargetDatasets.filter((d) => d.kind === 'auto_cdc' || d.badge === 'CDC'),
    [resetTargetDatasets]
  );

  const resetExternalSinks = React.useMemo(
    () => resetTargetDatasets.filter((d) => d.kind === 'external_sink' || d.badge === 'SINK'),
    [resetTargetDatasets]
  );

  const hasConflictingSdpFlags =
    isSdpMode &&
    sdpCommand === 'run' &&
    ((sdpRefreshMode === 'full_refresh_all' &&
      (sdpRefreshList.length > 0 || sdpFullRefreshList.length > 0)) ||
      sdpRefreshList.some((name) => sdpFullRefreshList.includes(name)));

  const hasNonResettableSelection =
    isSdpMode &&
    sdpCommand === 'run' &&
    sdpRefreshMode === 'full_refresh' &&
    persistedSdpDatasets.some(
      (d) =>
        !d.resetAllowed &&
        d.kind !== 'external_sink' &&
        sdpFullRefreshList.includes(d.datasetName)
    );

  const isMissingSelectiveSelection =
    isSdpMode &&
    sdpCommand === 'run' &&
    ((sdpRefreshMode === 'refresh' && sdpRefreshList.length === 0) ||
      (sdpRefreshMode === 'full_refresh' && resetTargetDatasets.length === 0));

  const isRunDisabled =
    needsRebuild ||
    pipelineStatus === 'running' ||
    hasConflictingSdpFlags ||
    hasNonResettableSelection ||
    isMissingSelectiveSelection;

  const requiresFullRefreshConfirm =
    isSdpMode &&
    sdpCommand === 'run' &&
    (sdpRefreshMode === 'full_refresh' || sdpRefreshMode === 'full_refresh_all');

  const handlePrimaryRunClick = () => {
    if (isRunDisabled) {
      return;
    }
    if (requiresFullRefreshConfirm) {
      setShowFullRefreshConfirm(true);
      return;
    }
    onRunNow();
  };

  const handleConfirmFullRefreshRun = () => {
    setShowFullRefreshConfirm(false);
    onRunNow();
  };

  // Compute GCP Cloud Console link when jobId is available
  const effectiveProjectId = jobConfig.projectId || projectId || 'YOUR_PROJECT_ID';
  const effectiveRegion = jobConfig.region || 'us-central1';
  const gcpConsoleUrl = buildGcpConsoleUrl(
    jobConfig.runner,
    jobConfig.jobId,
    effectiveRegion,
    effectiveProjectId
  );

  const normalizedRunner = normalizeSparkRunner(jobConfig.runner);
  const isRemoteRunner =
    normalizedRunner === 'DataprocCluster' || normalizedRunner === 'ServerlessSpark';

  // Format displayed runner command preview
  const defaultCommandPreview = buildDefaultCommandPreview(
    jobConfig.runner,
    jobConfig,
    effectiveProjectId,
    effectiveRegion,
    paradigm
  );
  const displayedCommand = runnerCommand || defaultCommandPreview;

  // Compute status badge styling
  const renderStatusBadge = () => {
    switch (pipelineStatus) {
      case 'running':
        return (
          <span className="px-2 py-0.5 text-[10px] font-bold rounded-full bg-amber-500/20 text-amber-300 border border-amber-500/40 flex items-center gap-1 animate-pulse">
            <Loader2 className="w-2.5 h-2.5 animate-spin" />
            Running
          </span>
        );
      case 'completed':
        return (
          <span className="px-2 py-0.5 text-[10px] font-bold rounded-full bg-emerald-500/20 text-emerald-300 border border-emerald-500/40">
            Success
          </span>
        );
      case 'failed':
        return (
          <span className="px-2 py-0.5 text-[10px] font-bold rounded-full bg-rose-500/20 text-rose-300 border border-rose-500/40">
            Failed
          </span>
        );
      case 'stopped':
        return (
          <span className="px-2 py-0.5 text-[10px] font-bold rounded-full bg-slate-500/20 text-slate-300 border border-slate-500/40">
            Stopped
          </span>
        );
      default:
        return (
          <span className="px-2 py-0.5 text-[10px] font-bold rounded-full bg-blue-500/20 text-blue-300 border border-blue-500/40">
            Ready
          </span>
        );
    }
  };

  return (
    <div className="flex gap-4 h-full text-slate-300 relative">
      {/* ========================================================================= */}
      {/* Left Panel (25%-30% width): Deployment & Runner Settings */}
      {/* ========================================================================= */}
      <div className="w-[30%] min-w-[300px] max-w-[380px] flex flex-col bg-[#161922] rounded-xl border border-slate-700/80 overflow-hidden shadow-lg">
        {/* Panel Header */}
        <div className="border-b border-slate-700/80 bg-[#11141c] py-2.5 px-4 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Sliders className="w-3.5 h-3.5 text-blue-400 shrink-0" />
            <span className="text-xs font-bold tracking-wider text-slate-200 uppercase">
              DEPLOYMENT &amp; RUNNER
            </span>
          </div>
          {renderStatusBadge()}
        </div>

        {/* Panel Content */}
        <div className="flex-1 flex flex-col p-4 overflow-y-auto justify-between gap-4">
          <div className="flex flex-col gap-4">
            <RunnerSettings
              runner={jobConfig.runner}
              setJobConfig={setJobConfig}
              paradigm={paradigm}
            />

            {isSdpMode && (
              <SdpRefreshControls
                jobConfig={jobConfig}
                setJobConfig={setJobConfig}
                datasets={sdpDatasets}
              />
            )}

            <AdvancedConfig
              jobConfig={jobConfig}
              setJobConfig={setJobConfig}
              subnetworks={subnetworks}
              clusters={clusters}
              sessionTemplates={sessionTemplates}
              paradigm={paradigm}
              nodes={graphNodes}
              projectId={effectiveProjectId}
            />

            <RuntimeVariablesGrid
              runtimeVariables={runtimeVariables}
              addVariable={addVariable}
              updateVariable={updateVariable}
              deleteVariable={deleteVariable}
              isJobIdValid={isJobIdValid}
            />
          </div>

          {/* Action Buttons: Run Pipeline & Stop / Cancel Job */}
          <div className="pt-3 border-t border-slate-800/80 flex flex-col gap-2">
            {isRemoteRunner && (
              <label
                htmlFor="cancelRemoteDataprocJob"
                className="flex items-center gap-2 text-[11px] text-slate-400 cursor-pointer select-none"
              >
                <input
                  id="cancelRemoteDataprocJob"
                  type="checkbox"
                  checked={cancelRemoteJob}
                  onChange={(e) => setCancelRemoteJob(e.target.checked)}
                  className="rounded border-slate-700 bg-[#0d1017] text-blue-500 focus:ring-0 cursor-pointer"
                />
                <span>Cancel remote Dataproc job on Stop</span>
              </label>
            )}
            <div className="flex gap-2.5">
              <button
                type="button"
                onClick={handlePrimaryRunClick}
                disabled={isRunDisabled}
                className={`flex items-center justify-center gap-2 py-2.5 px-4 rounded-lg text-xs font-bold flex-1 cursor-pointer transition shadow-lg active:scale-[0.98] ${
                  isRunDisabled
                    ? 'bg-slate-800 text-slate-500 cursor-not-allowed shadow-none'
                    : 'bg-emerald-600 hover:bg-emerald-500 active:bg-emerald-700 text-white shadow-emerald-950/40'
                }`}
              >
                {pipelineStatus === 'running' ? (
                  <>
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                    <span>RUNNING...</span>
                  </>
                ) : (
                  <>
                    <Play className="w-3.5 h-3.5 fill-current" />
                    <span>{isSdpMode && sdpCommand === 'dry-run' ? 'DRY-RUN SDP' : 'RUN PIPELINE'}</span>
                  </>
                )}
              </button>
              <button
                type="button"
                onClick={() => onStopPipeline?.({ cancelRemoteJob })}
                className="bg-[#881337] hover:bg-[#9f1239] active:bg-[#4c0519] text-rose-200 hover:text-white px-3 py-2.5 rounded-lg text-xs font-bold w-1/4 transition cursor-pointer flex items-center justify-center gap-1.5 border border-rose-700/60 shadow-lg shadow-rose-950/30 active:scale-[0.98]"
                title="Stop running pipeline process and optionally cancel remote Dataproc job"
              >
                <Square className="w-3 h-3 fill-current" />
                <span>STOP</span>
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* ========================================================================= */}
      {/* Right Panel (70%-75% width): Live Execution Sandbox & Log Output */}
      {/* ========================================================================= */}
      <ExecutionTerminal
        runner={jobConfig.runner}
        displayedCommand={displayedCommand}
        pipelineLogs={pipelineLogs}
        gcpConsoleUrl={gcpConsoleUrl}
        onOpenConsole={onOpenConsole}
        onClearLogs={onClearLogs}
      />

      {/* ========================================================================= */}
      {/* Explicit SDP Full-Refresh Destructive Confirmation Gate (SDP9) */}
      {/* ========================================================================= */}
      {showFullRefreshConfirm && (
        <div
          role="dialog"
          aria-modal="true"
          aria-labelledby="sdpFullRefreshModalTitle"
          data-testid="sdp-full-refresh-confirm-modal"
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
        >
          <div className="w-full max-w-md rounded-xl bg-[#161922] border border-amber-500/60 shadow-2xl p-4 flex flex-col gap-3 text-slate-200">
            <div className="flex items-center gap-2 text-amber-300 font-bold text-sm">
              <AlertTriangle className="w-4 h-4 shrink-0" aria-hidden="true" />
              <span id="sdpFullRefreshModalTitle">Confirm SDP Full Refresh Reset</span>
            </div>

            <p className="text-xs text-slate-300 leading-relaxed">
              This full refresh empties streaming tables and drops Auto CDC{' '}
              <code className="font-mono text-amber-200">__spark_autocdc_aux_state_*</code> state
              tables, then rebuilds each target{' '}
              <strong className="text-white">only from source data still available</strong>. Any
              expired retention records or deleted source files will be permanently lost.
            </p>

            <div className="flex flex-col gap-1.5 p-3 rounded-lg bg-[#0d1017] border border-slate-800">
              <span className="text-[11px] font-bold uppercase tracking-wider text-slate-400">
                Datasets Being Reset ({resetTargetDatasets.length})
              </span>
              {resetTargetDatasets.length > 0 ? (
                <ul
                  data-testid="sdp-reset-dataset-list"
                  className="flex flex-col gap-1 text-xs font-mono text-amber-200 list-disc list-inside"
                >
                  {resetTargetDatasets.map((ds) => (
                    <li key={ds.datasetName}>
                      {ds.datasetName} ({ds.badge})
                      {(ds.kind === 'auto_cdc' || ds.badge === 'CDC') &&
                        ` + __spark_autocdc_aux_state_${ds.datasetName}`}
                    </li>
                  ))}
                </ul>
              ) : (
                <div
                  data-testid="sdp-reset-dataset-list"
                  className="text-xs text-slate-400 italic"
                >
                  All resettable pipeline datasets declared in spark-pipeline.yml.
                </div>
              )}
            </div>

            {resetAutoCdcTargets.length > 0 && (
              <div className="text-[11px] text-amber-200/90 leading-snug">
                Auto CDC state tables dropped:{' '}
                <code className="font-mono">
                  {resetAutoCdcTargets
                    .map((t) => `__spark_autocdc_aux_state_${t.datasetName}`)
                    .join(', ')}
                </code>
                .
              </div>
            )}

            {resetExternalSinks.length > 0 && (
              <div
                data-testid="sdp-modal-sink-duplicate-warning"
                className="p-2.5 rounded bg-rose-950/50 border border-rose-600/60 text-[11px] text-rose-200 leading-snug"
              >
                <strong>Duplicate Delivery Warning:</strong> External{' '}
                <code className="font-mono">dp.create_sink</code> targets (
                <span className="font-mono font-semibold">
                  {resetExternalSinks.map((s) => s.datasetName).join(', ')}
                </span>
                ) cannot be protected by <code className="font-mono">pipelines.reset.allowed</code>
                . All available source data will be re-sent as duplicates.
              </div>
            )}

            <div className="flex justify-end gap-2 pt-2 border-t border-slate-800">
              <button
                type="button"
                data-testid="cancel-sdp-full-refresh-btn"
                onClick={() => setShowFullRefreshConfirm(false)}
                className="px-3 py-2 rounded-lg text-xs font-semibold bg-slate-800 hover:bg-slate-700 text-slate-200 cursor-pointer transition"
              >
                Cancel
              </button>
              <button
                type="button"
                data-testid="confirm-sdp-full-refresh-btn"
                onClick={handleConfirmFullRefreshRun}
                className="px-3 py-2 rounded-lg text-xs font-bold bg-amber-600 hover:bg-amber-500 text-white cursor-pointer transition"
              >
                Confirm Full Refresh &amp; Run
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
