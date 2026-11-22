/**
 * @fileoverview Runner settings dropdown component for selecting the target Spark environment.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import {
  normalizeSparkRunner,
  type SparkRunnerType,
} from '../../../utils/sharedTypes';
import type { BottomPanelContextValue } from '../BottomPanelContext';

export { normalizeSparkRunner };
export type { SparkRunnerType };

type JobConfigValue = BottomPanelContextValue['jobConfig']['value'];

/**
 * Props for the RunnerSettings component.
 */
export interface RunnerSettingsProps {
  /** Currently active runner identifier ('Local' | 'DataprocCluster' | 'ServerlessSpark'). */
  runner?: string;
  /** State updater callback for modifying the parent job configuration. */
  setJobConfig: BottomPanelContextValue['jobConfig']['set'];
  /** Optional pipeline paradigm ('imperative' | 'sdp'). */
  paradigm?: 'imperative' | 'sdp';
}

/**
 * Target runner environment selector dropdown.
 */
export const RunnerSettings: React.FC<RunnerSettingsProps> = ({
  runner,
  setJobConfig,
  paradigm = 'imperative',
}) => {
  const effectiveRunner = normalizeSparkRunner(runner);
  const isSdp = paradigm === 'sdp';
  return (
    <div className="flex flex-col gap-1.5">
      <label
        htmlFor="targetEnvironmentRunner"
        className="text-xs font-bold uppercase tracking-wider text-slate-400"
      >
        Target Environment
      </label>
      <select
        id="targetEnvironmentRunner"
        value={effectiveRunner}
        onChange={(e) => {
          const nextValue = normalizeSparkRunner(e.target.value);
          setJobConfig((prev: JobConfigValue) => {
            const prevRecord =
              prev && typeof prev === 'object' ? (prev as Record<string, unknown>) : {};
            if (isSdp) {
              const currentVersion =
                typeof prevRecord.runtimeVersion === 'string'
                  ? prevRecord.runtimeVersion
                  : '';
              const nextRuntimeVersion =
                nextValue === 'Local'
                  ? '4.2-connect'
                  : currentVersion === '3.0' ||
                      currentVersion === '4.2-connect' ||
                      !currentVersion
                    ? '4.0'
                    : currentVersion;
              return {
                ...prev,
                runner: nextValue,
                runtimeVersion: nextRuntimeVersion,
                ...(nextValue === 'Local' ? { useSparkConnect: true } : {}),
              };
            }
            return { ...prev, runner: nextValue };
          });
        }}
        className="bg-[#0d1017] text-white px-3 py-2 rounded-lg border border-slate-800 focus:outline-none focus:border-blue-500 text-xs shadow-inner transition cursor-pointer"
      >
        <option value="Local">
          {isSdp ? 'Local Spark 4.2 Connect (4.2-connect)' : 'Local PySpark / Spark Connect'}
        </option>
        <option value="DataprocCluster">
          {isSdp ? 'Dataproc Cluster 4.0 (Spark 4.2)' : 'Dataproc Cluster (GCE/GKE)'}
        </option>
        <option value="ServerlessSpark">
          {isSdp
            ? 'GCP Serverless Spark 4.0 / Managed SDP'
            : 'GCP Serverless Spark (Batch)'}
        </option>
      </select>
    </div>
  );
};
