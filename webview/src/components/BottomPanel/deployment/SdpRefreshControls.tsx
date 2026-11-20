/**
 * @fileoverview Spark Declarative Pipelines execution command, refresh dataset multi-select, and safety banners (SDP9).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import React from 'react';
import { AlertTriangle, Info } from 'lucide-react';
import { InfoTooltip } from '../../InfoTooltip';

/* eslint-disable @typescript-eslint/no-explicit-any */

/**
 * Resolved SDP dataset or external sink descriptor extracted from the pipeline graph.
 */
export interface SdpRefreshDatasetOption {
  /** Canvas node identifier. */
  nodeId: string;
  /** Human-readable canvas node label. */
  nodeLabel: string;
  /** Exact SDP dataset or sink identifier passed to --refresh or --full-refresh. */
  datasetName: string;
  /** SDP persistence classification. */
  kind:
    | 'temporary_view'
    | 'materialized_view'
    | 'streaming_table'
    | 'append_flow'
    | 'auto_cdc'
    | 'external_sink';
  /** Compact dataset badge ('VIEW' | 'MV' | 'TABLE' | 'CDC' | 'SINK'). */
  badge: 'VIEW' | 'MV' | 'TABLE' | 'CDC' | 'SINK';
  /** False when table_properties sets pipelines.reset.allowed to false. */
  resetAllowed: boolean;
}

/**
 * Props for the {@link SdpRefreshControls} component.
 */
export interface SdpRefreshControlsProps {
  /** Active pipeline job configuration state. */
  jobConfig: Record<string, any>;
  /** Functional updater for modifying jobConfig without clobbering sibling fields. */
  setJobConfig: (update: React.SetStateAction<any>) => void;
  /** Resolved SDP datasets and sinks from the current graph. */
  datasets: readonly SdpRefreshDatasetOption[];
}

/**
 * Renders SDP command mode, graph-derived dataset refresh multi-select checkboxes,
 * external sink duplicate warnings, and materialized view truncation operational notes.
 *
 * @param props - Job config, functional updater, and graph SDP datasets.
 * @returns Rendered SDP refresh controls card.
 */
export const SdpRefreshControls: React.FC<SdpRefreshControlsProps> = ({
  jobConfig,
  setJobConfig,
  datasets,
}) => {
  const sdpCommand: 'run' | 'dry-run' =
    jobConfig.sdpCommand === 'dry-run' ? 'dry-run' : 'run';
  const sdpRefreshMode: 'incremental' | 'refresh' | 'full_refresh' | 'full_refresh_all' =
    jobConfig.sdpRefreshMode === 'refresh' ||
    jobConfig.sdpRefreshMode === 'full_refresh' ||
    jobConfig.sdpRefreshMode === 'full_refresh_all'
      ? jobConfig.sdpRefreshMode
      : 'incremental';

  const rawRefreshList = React.useMemo<string[]>(
    () =>
      Array.isArray(jobConfig.sdpRefreshDatasets)
        ? jobConfig.sdpRefreshDatasets.filter((d: unknown): d is string => typeof d === 'string')
        : [],
    [jobConfig.sdpRefreshDatasets]
  );
  const rawFullRefreshList = React.useMemo<string[]>(
    () =>
      Array.isArray(jobConfig.sdpFullRefreshDatasets)
        ? jobConfig.sdpFullRefreshDatasets.filter(
            (d: unknown): d is string => typeof d === 'string'
          )
        : [],
    [jobConfig.sdpFullRefreshDatasets]
  );

  // Only persisted datasets and sinks can be refreshed (temporary_view is non-persisted)
  const persistedDatasets = React.useMemo(
    () => datasets.filter((d) => d.kind !== 'temporary_view' && d.badge !== 'VIEW'),
    [datasets]
  );

  const externalSinks = React.useMemo(
    () =>
      persistedDatasets.filter((d) => d.kind === 'external_sink' || d.badge === 'SINK'),
    [persistedDatasets]
  );

  const materializedViews = React.useMemo(
    () =>
      persistedDatasets.filter(
        (d) => d.kind === 'materialized_view' || d.badge === 'MV'
      ),
    [persistedDatasets]
  );

  const overlappingDatasets = React.useMemo(
    () => rawRefreshList.filter((name) => rawFullRefreshList.includes(name)),
    [rawRefreshList, rawFullRefreshList]
  );

  const hasFullRefreshAllConflict =
    sdpRefreshMode === 'full_refresh_all' &&
    (rawRefreshList.length > 0 || rawFullRefreshList.length > 0);

  const protectedSelectedDatasets = React.useMemo(
    () =>
      persistedDatasets.filter(
        (d) =>
          !d.resetAllowed &&
          d.kind !== 'external_sink' &&
          rawFullRefreshList.includes(d.datasetName)
      ),
    [persistedDatasets, rawFullRefreshList]
  );

  const affectedExternalSinks = React.useMemo(() => {
    if (sdpCommand !== 'run') {
      return [];
    }
    if (sdpRefreshMode === 'full_refresh_all') {
      return externalSinks;
    }
    if (sdpRefreshMode === 'full_refresh') {
      return externalSinks.filter((sink) => rawFullRefreshList.includes(sink.datasetName));
    }
    return [];
  }, [sdpCommand, sdpRefreshMode, externalSinks, rawFullRefreshList]);

  const handleToggleRefreshDataset = (datasetName: string, checked: boolean) => {
    setJobConfig((prev: any) => {
      const prevRefresh: string[] = Array.isArray(prev?.sdpRefreshDatasets)
        ? prev.sdpRefreshDatasets
        : [];
      const prevFull: string[] = Array.isArray(prev?.sdpFullRefreshDatasets)
        ? prev.sdpFullRefreshDatasets
        : [];
      const nextRefresh = checked
        ? Array.from(new Set([...prevRefresh, datasetName]))
        : prevRefresh.filter((d) => d !== datasetName);
      const nextFull = checked
        ? prevFull.filter((d) => d !== datasetName)
        : prevFull;
      return {
        ...prev,
        sdpRefreshDatasets: nextRefresh,
        sdpFullRefreshDatasets: nextFull,
      };
    });
  };

  const handleToggleFullRefreshDataset = (datasetName: string, checked: boolean) => {
    setJobConfig((prev: any) => {
      const prevFull: string[] = Array.isArray(prev?.sdpFullRefreshDatasets)
        ? prev.sdpFullRefreshDatasets
        : [];
      const prevRefresh: string[] = Array.isArray(prev?.sdpRefreshDatasets)
        ? prev.sdpRefreshDatasets
        : [];
      const nextFull = checked
        ? Array.from(new Set([...prevFull, datasetName]))
        : prevFull.filter((d) => d !== datasetName);
      const nextRefresh = checked
        ? prevRefresh.filter((d) => d !== datasetName)
        : prevRefresh;
      return {
        ...prev,
        sdpFullRefreshDatasets: nextFull,
        sdpRefreshDatasets: nextRefresh,
      };
    });
  };

  return (
    <div
      data-testid="sdp-refresh-controls"
      className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-lg border border-[var(--vscode-border)]"
    >
      <div className="text-xs font-bold uppercase tracking-wider text-emerald-400">
        SDP Execution &amp; Refresh
      </div>

      {/* Command Mode: run vs dry-run */}
      <div className="flex flex-col gap-1">
        <label
          htmlFor="sdpCommandMode"
          className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
        >
          <span>SDP Command</span>
          <InfoTooltip text="Choose triggered AvailableNow execution or graph-only validation without writing data." />
        </label>
        <select
          id="sdpCommandMode"
          name="sdpCommand"
          value={sdpCommand}
          onChange={(e) => {
            const nextCmd = e.target.value === 'dry-run' ? 'dry-run' : 'run';
            setJobConfig((prev: any) => ({ ...prev, sdpCommand: nextCmd }));
          }}
          className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs focus:outline-none focus:border-[var(--vscode-highlight)] cursor-pointer"
        >
          <option value="run">Triggered Update (spark-pipelines run)</option>
          <option value="dry-run">Validate Graph Only (spark-pipelines dry-run)</option>
        </select>
      </div>

      {sdpCommand === 'dry-run' && (
        <div
          data-testid="sdp-dry-run-note"
          className="p-2 rounded bg-blue-950/30 border border-blue-700/50 text-[11px] text-blue-200 leading-snug"
        >
          <code className="font-mono">spark-pipelines dry-run</code> resolves and validates the
          dataset graph without writing data. It does not check whether the target database exists
          or verify schema compatibility with existing tables.
        </div>
      )}

      {/* Refresh Strategy (run mode only) */}
      {sdpCommand === 'run' && (
        <div className="flex flex-col gap-1">
          <label
            htmlFor="sdpRefreshMode"
            className="flex items-center gap-1 text-xs font-bold uppercase tracking-wider text-[var(--vscode-text-muted)]"
          >
            <span>Refresh Strategy</span>
            <InfoTooltip text="Controls incremental processing versus selective or full checkpoint and table resets." />
          </label>
          <select
            id="sdpRefreshMode"
            name="sdpRefreshMode"
            value={sdpRefreshMode}
            onChange={(e) => {
              const nextMode = e.target.value as
                | 'incremental'
                | 'refresh'
                | 'full_refresh'
                | 'full_refresh_all';
              setJobConfig((prev: any) => ({
                ...prev,
                sdpRefreshMode: nextMode,
                ...(nextMode === 'full_refresh_all'
                  ? { sdpRefreshDatasets: [], sdpFullRefreshDatasets: [] }
                  : {}),
              }));
            }}
            className="bg-[var(--vscode-input-bg)] text-[var(--vscode-text-primary)] p-2 rounded border border-[var(--vscode-border)] text-xs focus:outline-none focus:border-[var(--vscode-highlight)] cursor-pointer"
          >
            <option value="incremental">Incremental Update (default)</option>
            <option value="refresh">Selective Refresh (--refresh)</option>
            <option value="full_refresh">Selective Full Refresh (--full-refresh)</option>
            <option value="full_refresh_all">Full Refresh All (--full-refresh-all)</option>
          </select>
        </div>
      )}

      {/* Multi-select Checkboxes for --refresh */}
      {sdpCommand === 'run' && sdpRefreshMode === 'refresh' && (
        <div
          data-testid="sdp-refresh-dataset-picker"
          className="flex flex-col gap-2 p-2 rounded bg-[#0d1017] border border-slate-800"
        >
          <div className="text-[11px] text-slate-400 leading-snug">
            Select persisted datasets from the canvas graph (SDP silently ignores unknown names):
          </div>
          {persistedDatasets.length === 0 ? (
            <div className="text-[11px] text-slate-500 italic">
              No persisted SDP datasets found on the canvas.
            </div>
          ) : (
            persistedDatasets.map((ds) => {
              const checked = rawRefreshList.includes(ds.datasetName);
              return (
                <label
                  key={ds.datasetName}
                  htmlFor={`sdp-refresh-${ds.datasetName}`}
                  className="flex items-center justify-between gap-2 text-xs text-slate-200 cursor-pointer select-none py-0.5"
                >
                  <span className="flex items-center gap-2 min-w-0">
                    <input
                      id={`sdp-refresh-${ds.datasetName}`}
                      data-testid={`sdp-refresh-checkbox-${ds.datasetName}`}
                      type="checkbox"
                      checked={checked}
                      onChange={(e) =>
                        handleToggleRefreshDataset(ds.datasetName, e.target.checked)
                      }
                      className="rounded border-slate-700 bg-[#161922] text-emerald-500 focus:ring-0 cursor-pointer"
                    />
                    <span className="font-mono truncate">{ds.datasetName}</span>
                  </span>
                  <span className="px-1.5 py-0.5 text-[10px] font-bold rounded bg-slate-800 text-slate-300 shrink-0">
                    {ds.badge}
                  </span>
                </label>
              );
            })
          )}
        </div>
      )}

      {/* Multi-select Checkboxes for --full-refresh */}
      {sdpCommand === 'run' && sdpRefreshMode === 'full_refresh' && (
        <div
          data-testid="sdp-full-refresh-dataset-picker"
          className="flex flex-col gap-2 p-2 rounded bg-[#0d1017] border border-slate-800"
        >
          <div className="text-[11px] text-slate-400 leading-snug">
            Select persisted datasets to empty and rebuild from available source data:
          </div>
          {persistedDatasets.length === 0 ? (
            <div className="text-[11px] text-slate-500 italic">
              No persisted SDP datasets found on the canvas.
            </div>
          ) : (
            persistedDatasets.map((ds) => {
              const isNonResettable = !ds.resetAllowed && ds.kind !== 'external_sink';
              const checked = rawFullRefreshList.includes(ds.datasetName);
              return (
                <label
                  key={ds.datasetName}
                  htmlFor={`sdp-full-refresh-${ds.datasetName}`}
                  className={`flex items-center justify-between gap-2 text-xs select-none py-0.5 ${
                    isNonResettable
                      ? 'text-slate-500 cursor-not-allowed opacity-60'
                      : 'text-slate-200 cursor-pointer'
                  }`}
                >
                  <span className="flex items-center gap-2 min-w-0">
                    <input
                      id={`sdp-full-refresh-${ds.datasetName}`}
                      data-testid={`sdp-full-refresh-checkbox-${ds.datasetName}`}
                      type="checkbox"
                      disabled={isNonResettable}
                      checked={checked && !isNonResettable}
                      onChange={(e) =>
                        handleToggleFullRefreshDataset(ds.datasetName, e.target.checked)
                      }
                      className="rounded border-slate-700 bg-[#161922] text-amber-500 focus:ring-0"
                    />
                    <span className="font-mono truncate">{ds.datasetName}</span>
                  </span>
                  <span className="flex items-center gap-1 shrink-0">
                    {isNonResettable && (
                      <span
                        data-testid={`sdp-not-resettable-tag-${ds.datasetName}`}
                        className="px-1.5 py-0.5 text-[9px] font-bold rounded bg-amber-950/60 text-amber-300 border border-amber-700/50"
                      >
                        TABLE_NOT_RESETTABLE
                      </span>
                    )}
                    <span className="px-1.5 py-0.5 text-[10px] font-bold rounded bg-slate-800 text-slate-300">
                      {ds.badge}
                    </span>
                  </span>
                </label>
              );
            })
          )}
        </div>
      )}

      {/* Conflicting Refresh Options Guard */}
      {(overlappingDatasets.length > 0 || hasFullRefreshAllConflict) && (
        <div
          role="alert"
          data-testid="sdp-conflicting-refresh-warning"
          className="p-2 rounded bg-rose-950/40 border border-rose-700/60 text-[11px] text-rose-200 leading-snug"
        >
          CONFLICTING_PIPELINE_REFRESH_OPTIONS:{' '}
          {hasFullRefreshAllConflict
            ? '--full-refresh-all cannot be combined with --refresh or --full-refresh dataset lists.'
            : `Datasets (${overlappingDatasets.join(', ')}) cannot appear in both --refresh and --full-refresh.`}
        </div>
      )}

      {/* Non-Resettable Table Selection Warning */}
      {protectedSelectedDatasets.length > 0 && (
        <div
          role="alert"
          data-testid="sdp-table-not-resettable-warning"
          className="p-2 rounded bg-amber-950/40 border border-amber-600/60 text-[11px] text-amber-200 leading-snug"
        >
          TABLE_NOT_RESETTABLE: Protected datasets (
          {protectedSelectedDatasets.map((d) => d.datasetName).join(', ')}) set{' '}
          <code className="font-mono">pipelines.reset.allowed=false</code> and cannot be reset via{' '}
          <code className="font-mono">--full-refresh</code>.
        </div>
      )}

      {/* External Sink (dp.create_sink) Duplicate Warning */}
      {affectedExternalSinks.length > 0 && (
        <div
          role="alert"
          data-testid="sdp-external-sink-duplicate-warning"
          className="p-2.5 rounded bg-amber-950/50 border border-amber-500/70 text-[11px] text-amber-100 leading-snug flex gap-2 items-start"
        >
          <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0 mt-0.5" aria-hidden="true" />
          <div>
            <span className="font-bold text-amber-300">External Sink Duplicate Warning: </span>
            <code className="font-mono">pipelines.reset.allowed</code> cannot protect{' '}
            <code className="font-mono">dp.create_sink</code> external sinks (
            <span className="font-mono font-semibold">
              {affectedExternalSinks.map((s) => s.datasetName).join(', ')}
            </span>
            ). Resetting their flow checkpoints re-sends all available source data to downstream
            targets such as Kafka or BigQuery as duplicates.
          </div>
        </div>
      )}

      {/* Materialized View Truncation Operational Note */}
      {materializedViews.length > 0 && (
        <div
          data-testid="sdp-mv-truncation-note"
          className="p-2.5 rounded bg-slate-900/80 border border-slate-700/80 text-[11px] text-slate-300 leading-snug flex gap-2 items-start"
        >
          <Info className="w-4 h-4 text-blue-400 shrink-0 mt-0.5" aria-hidden="true" />
          <div>
            <span className="font-bold text-blue-300">Materialized View Behavior: </span>
            Materialized views (
            <span className="font-mono">
              {materializedViews.map((m) => m.datasetName).join(', ')}
            </span>
            ) execute <code className="font-mono">TRUNCATE TABLE</code> at the start of every run.
            Readers see empty materialized views during a run and after a failed run until the next
            successful run completes.
          </div>
        </div>
      )}
    </div>
  );
};
