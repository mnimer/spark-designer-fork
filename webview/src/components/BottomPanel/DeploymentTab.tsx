/**
 * @fileoverview Main DeploymentTab component for bottom panel deployment settings and execution sandbox.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React, { useState } from 'react';
import {
  Play,
  Square,
  Sliders,
  Loader2,
} from 'lucide-react';
import { RunnerSettings, normalizeSparkRunner } from './deployment/RunnerSettings';
import { AdvancedConfig } from './deployment/AdvancedConfig';
import { RuntimeVariablesGrid } from './deployment/RuntimeVariablesGrid';
import { ExecutionTerminal, type LogEntry } from './deployment/ExecutionTerminal';
import { buildGcpConsoleUrl } from './deployment/consoleUrl';
import { useBottomPanel } from './BottomPanelContext';

export type { LogEntry };

/**
 * Props for the DeploymentTab component.
 */
export type DeploymentTabProps = Record<string, never>;

/**
 * Builds the default command line preview string for the selected Spark target environment.
 *
 * @param runner - Selected runner identifier.
 * @param jobConfig - Current job configuration object.
 * @param effectiveProjectId - Resolved GCP Project ID.
 * @param effectiveRegion - Resolved GCP region.
 * @returns Multi-line shell command preview string.
 */
export function buildDefaultCommandPreview(
  runner: string,
  jobConfig: Record<string, unknown>,
  effectiveProjectId: string,
  effectiveRegion: string
): string {
  const normalized = normalizeSparkRunner(runner);
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
  } = useBottomPanel();

  const [cancelRemoteJob, setCancelRemoteJob] = useState<boolean>(true);

  const jobConfig = jobConfigSlice.value ?? { runner: 'Local' };
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
  const pipelineLogs = execution.logs;
  const pipelineStatus = execution.status;
  const runnerCommand = execution.runnerCommand;
  const onClearLogs = execution.clear;
  const onOpenConsole = execution.openConsole;
  const projectId = jobConfigSlice.projectId ?? 'YOUR_PROJECT_ID';

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
    effectiveRegion
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
    <div className="flex gap-4 h-full text-slate-300">
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
            />

            <AdvancedConfig
              jobConfig={jobConfig}
              setJobConfig={setJobConfig}
              subnetworks={subnetworks}
              clusters={clusters}
              sessionTemplates={sessionTemplates}
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
                onClick={onRunNow}
                disabled={needsRebuild || pipelineStatus === 'running'}
                className={`flex items-center justify-center gap-2 py-2.5 px-4 rounded-lg text-xs font-bold flex-1 cursor-pointer transition shadow-lg active:scale-[0.98] ${
                  needsRebuild || pipelineStatus === 'running'
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
                    <span>RUN PIPELINE</span>
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
    </div>
  );
};
