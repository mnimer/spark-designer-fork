/**
 * @fileoverview Runner settings dropdown component for selecting the target Spark environment.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React from 'react';

/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable react-refresh/only-export-components */

/**
 * Supported Spark target execution environments.
 */
export type SparkRunnerType = 'Local' | 'DataprocCluster' | 'ServerlessSpark';

/**
 * Normalizes a raw or legacy runner string into one of the three supported Spark environments.
 *
 * - Unset, empty, `'Local'`, or legacy `'DirectRunner'` resolves to `'Local'`.
 * - `'DataprocCluster'` resolves to `'DataprocCluster'`.
 * - `'ServerlessSpark'` or legacy `'Spark'` resolves to `'ServerlessSpark'`.
 *
 * @param runner - Raw runner string from jobConfig.
 * @returns Normalized Spark runner identifier.
 */
export function normalizeSparkRunner(runner?: string): SparkRunnerType {
  if (runner === 'DataprocCluster') {
    return 'DataprocCluster';
  }
  if (runner === 'ServerlessSpark' || runner === 'Spark') {
    return 'ServerlessSpark';
  }
  return 'Local';
}

/**
 * Props for the RunnerSettings component.
 */
export interface RunnerSettingsProps {
  /** Currently active runner identifier ('Local' | 'DataprocCluster' | 'ServerlessSpark'). */
  runner: string;
  /** State updater callback for modifying the parent job configuration. */
  setJobConfig: (update: React.SetStateAction<any>) => void;
}

/**
 * Target runner environment selector dropdown.
 */
export const RunnerSettings: React.FC<RunnerSettingsProps> = ({
  runner,
  setJobConfig,
}) => {
  const effectiveRunner = normalizeSparkRunner(runner);
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
          const nextValue = e.target.value;
          setJobConfig((prev: any) => ({ ...prev, runner: nextValue }));
        }}
        className="bg-[#0d1017] text-white px-3 py-2 rounded-lg border border-slate-800 focus:outline-none focus:border-blue-500 text-xs shadow-inner transition cursor-pointer"
      >
        <option value="Local">Local PySpark / Spark Connect</option>
        <option value="DataprocCluster">Dataproc Cluster (GCE/GKE)</option>
        <option value="ServerlessSpark">GCP Serverless Spark (Batch)</option>
      </select>
    </div>
  );
};
