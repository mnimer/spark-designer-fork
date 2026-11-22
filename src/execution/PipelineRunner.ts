/**
 * @fileoverview Pipeline runner: locates the generated runner script, builds the
 * sanitized child environment, resolves the target project and region, spawns the
 * script as a process-group leader, and tracks Spark batch and job IDs in its output.
 *
 * Extracted from `handleRunPipeline` so the IPC handler only orchestrates
 * validation and webview messaging, while the process mechanics here are
 * individually unit-testable.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import * as fs from 'fs';
import * as path from 'path';
import { execFile, spawn, type ChildProcess, type ExecFileException } from 'child_process';
import { findGcloudPath } from '../cli/cliResolver';
import { getGcloudProject } from '../cli/gcloudClient';
import { applyRuntimeVariables, type RuntimeVariable } from '../security/envSanitizer';
import type { PipelineParadigm, SdpJobConfigFields } from '../shared/types';
import { detectSparkJob } from './jobIdDetector';

/** Project placeholder used by generated configs before a real project is chosen. */
export const PLACEHOLDER_PROJECT_ID = 'YOUR_PROJECT_ID';

/** Region used when the job config does not specify one. */
export const DEFAULT_RUN_REGION = 'us-central1';

/** Job configuration fields the runner reads from the webview's run request. */
export interface RunnerJobConfig extends SdpJobConfigFields {
    /** Runner name: 'Local' | 'DataprocCluster' | 'ServerlessSpark'. */
    runner?: string;
    /** Explicit GCP project, taking precedence over the gcloud default (F6). */
    projectId?: string;
    /** GCP region for job submission and Console links. */
    region?: string;
    /** Serverless Spark staging bucket (`gs://<bucket>`), exported as DEPS_BUCKET. */
    depsBucket?: string;
    /** Legacy staging path, exported as GCS_STAGING_PATH and fallback for DEPS_BUCKET. */
    gcsStagingPath?: string;
    /** Structured Streaming checkpoint root URI (`gs://...` or local path), exported as CHECKPOINT_ROOT. */
    checkpointRoot?: string;
    /** Target Dataproc cluster name for DataprocCluster runs, exported as CLUSTER_NAME. */
    clusterName?: string;
    /** Serverless Spark runtime version ('4.0' | '3.0'), exported as RUNTIME_VERSION. */
    runtimeVersion?: string;
    /** Serverless Spark compute tier ('standard' | 'premium'), exported as SERVERLESS_TIER. */
    tier?: string;
    /** Alias for `tier`, exported as SERVERLESS_TIER. */
    serverlessTier?: string;
    /** VPC subnetwork name or URI, exported as SUBNET and SUBNETWORK. */
    subnetwork?: string;
    /** Alias for `subnetwork`, exported as SUBNET and SUBNETWORK. */
    subnet?: string;
    /** Custom workload service account email, exported as SERVICE_ACCOUNT. */
    serviceAccount?: string;
    /** Batch time-to-live in minutes (or duration string), formatted and exported as BATCH_TTL. */
    batchTtlMinutes?: number | string;
    /** Raw batch TTL string fallback, exported as BATCH_TTL. */
    batchTtl?: string;
    /** Comma-separated custom `gs://` JAR URIs, exported as SPARK_JARS. */
    customJarUris?: string;
    /** Alias for `customJarUris`, exported as SPARK_JARS. */
    sparkJars?: string;
    /** Whether Local mode should run via ManagedSparkSession (Spark Connect), exported as USE_SPARK_CONNECT. */
    useSparkConnect?: boolean;
    /** Spark Connect session template resource name or ID, exported as SPARK_CONNECT_SESSION_TEMPLATE. */
    sessionTemplate?: string;
    /** Pipeline paradigm ('imperative' by default, or 'sdp' for Spark Declarative Pipelines). */
    paradigm?: PipelineParadigm;
}

/** Plain Spark / SDP identifier pattern (`[A-Za-z_][A-Za-z0-9_]*`). */
const SAFE_SDP_IDENTIFIER_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Sanitizes a list of SDP dataset names for `--refresh` / `--full-refresh` exports.
 * Drops blank strings, invalid identifiers, `__spark_autocdc_` internal tables, and duplicates.
 *
 * @param datasets Raw dataset list from `jobConfig`.
 * @returns Sanitized unique dataset identifiers.
 */
function sanitizeSdpDatasetList(datasets: readonly unknown[] | undefined): string[] {
    if (!Array.isArray(datasets)) {
        return [];
    }
    const seen = new Set<string>();
    const out: string[] = [];
    for (const entry of datasets) {
        if (typeof entry !== 'string') {
            continue;
        }
        const trimmed = entry.trim();
        if (
            !trimmed ||
            !SAFE_SDP_IDENTIFIER_REGEX.test(trimmed) ||
            trimmed.toLowerCase().startsWith('__spark_autocdc_') ||
            seen.has(trimmed)
        ) {
            continue;
        }
        seen.add(trimmed);
        out.push(trimmed);
    }
    return out;
}

/**
 * Serializes `sdpStaticSparkConf` (`Record<string, string>` or comma/newline string) into a
 * safe comma-separated `key=value` string without control characters.
 *
 * @param raw Static Spark configuration from `jobConfig`.
 * @returns Comma-separated `key=value` entries, or undefined when empty.
 */
function formatSdpStaticSparkConf(raw: Record<string, string> | string | undefined): string | undefined {
    if (!raw) {
        return undefined;
    }
    const pairs: string[] = [];
    if (typeof raw === 'string') {
        for (const piece of raw.split(/[,\n]+/)) {
            const trimmed = piece.trim();
            if (trimmed && trimmed.includes('=') && !/[\r\n\0]/.test(trimmed)) {
                pairs.push(trimmed);
            }
        }
    } else if (typeof raw === 'object' && !Array.isArray(raw)) {
        for (const [k, v] of Object.entries(raw)) {
            const key = k.trim();
            const val = String(v ?? '').trim();
            if (key && val && !/[\r\n\0,]/.test(key) && !/[\r\n\0,]/.test(val)) {
                pairs.push(`${key}=${val}`);
            }
        }
    }
    return pairs.length > 0 ? pairs.join(',') : undefined;
}

/**
 * Returns the runner script filenames to look for, in priority order.
 *
 * @param runner Runner name from the job config.
 * @param paradigm Optional pipeline paradigm (`'imperative'` | `'sdp'`).
 * @returns Candidate script names (empty for unknown runners).
 */
export function runnerScriptCandidates(runner: string, paradigm?: string): string[] {
    if (paradigm === 'sdp') {
        if (runner === 'Local') {
            return ['run_sdp_local.sh', 'run_local.sh'];
        }
        if (runner === 'DataprocCluster') {
            return ['run_sdp_dataproc.sh', 'run_dataproc_cluster.sh'];
        }
        if (runner === 'ServerlessSpark') {
            return ['run_sdp_serverless.sh', 'run_sdp_local.sh', 'run_serverless_spark.sh'];
        }
        return [];
    }
    if (runner === 'Local') {
        return ['run_local.sh'];
    }
    if (runner === 'DataprocCluster') {
        return ['run_dataproc_cluster.sh'];
    }
    if (runner === 'ServerlessSpark') {
        return ['run_serverless_spark.sh'];
    }
    return [];
}

/**
 * Finds the first candidate runner script that exists in `folderPath`.
 *
 * @param folderPath Generated pipeline output folder.
 * @param candidates Script names from {@link runnerScriptCandidates}.
 * @param exists Existence check (injectable for tests).
 * @returns The script filename, or null when none exists.
 */
export function findRunnerScript(
    folderPath: string,
    candidates: readonly string[],
    exists: (p: string) => boolean = (p) => fs.existsSync(p)
): string | null {
    for (const name of candidates) {
        if (exists(path.join(folderPath, name))) {
            return name;
        }
    }
    return null;
}

/**
 * Human-readable comment line describing how the runner executes, shown in the
 * execution log header.
 *
 * @param runner Runner name.
 * @param paradigm Optional pipeline paradigm (`'imperative'` | `'sdp'`).
 * @returns A shell-comment description line.
 */
export function describeRunner(runner: string, paradigm?: string): string {
    if (paradigm === 'sdp') {
        if (runner === 'Local') {
            return '# Running Spark Declarative Pipeline via Local Spark 4.2 Connect';
        }
        if (runner === 'DataprocCluster') {
            return '# Submitting Spark Declarative Pipeline to Google Cloud Dataproc 4.0 Cluster';
        }
        return '# Running Spark Declarative Pipeline Stage (Managed Data Pipelines / Serverless Spark 4.0)';
    }
    if (runner === 'Local') {
        return '# Using Local PySpark / Spark Connect Runner (Local Machine)';
    }
    if (runner === 'DataprocCluster') {
        return '# Submitting PySpark Job to Google Cloud Dataproc Cluster (GCE/GKE)';
    }
    return '# Submitting PySpark Batch Job to GCP Serverless Spark (Dataproc)';
}

/**
 * Formats a batch TTL value (numeric minutes or suffixed duration string) into a
 * gcloud-compatible duration string (for example `14400s` for `240` minutes).
 *
 * @param rawValue Numeric minutes or duration string from `jobConfig.batchTtlMinutes`.
 * @returns Formatted duration string, or undefined when empty/invalid.
 */
export function formatBatchTtl(rawValue: number | string | undefined): string | undefined {
    if (rawValue === undefined || rawValue === null) {
        return undefined;
    }
    if (typeof rawValue === 'number') {
        if (!Number.isFinite(rawValue) || rawValue <= 0) {
            return undefined;
        }
        return `${Math.round(rawValue * 60)}s`;
    }
    const trimmed = String(rawValue).trim();
    if (!trimmed) {
        return undefined;
    }
    if (/^\d+(?:\.\d+)?$/.test(trimmed)) {
        const mins = Number(trimmed);
        if (!Number.isFinite(mins) || mins <= 0) {
            return undefined;
        }
        return `${Math.round(mins * 60)}s`;
    }
    if (/^\d+[smhd]$/i.test(trimmed)) {
        return trimmed.toLowerCase();
    }
    return undefined;
}

/** Options for {@link buildRunnerEnvironment}. */
export interface RunnerEnvironmentOptions {
    /** Base environment, normally `process.env`. */
    baseEnv: Record<string, string | undefined>;
    /** Runtime variables from the `.spark` document (sanitized, S1/S9). */
    runtimeVariables?: readonly RuntimeVariable[];
    /** Job config supplying Spark runner environment exports. */
    jobConfig?: RunnerJobConfig;
    /** Existence check for candidate PATH directories (injectable for tests). */
    exists?: (p: string) => boolean;
}

/** Result of {@link buildRunnerEnvironment}. */
export interface RunnerEnvironment {
    /** Environment passed to `spawn`. */
    env: Record<string, string>;
    /** Runtime variable keys refused by the env sanitizer (S1/S9). */
    rejectedKeys: string[];
}

/**
 * Builds the child-process environment: prepends common tool directories
 * (Homebrew, gcloud SDK, pyenv, sdkman…) that exist on disk to PATH, forces
 * unbuffered/uncolored output, applies sanitized runtime variables, and exports
 * the Spark runtime configuration from the job config.
 *
 * @param options Environment inputs.
 * @returns The environment and any rejected runtime variable keys.
 */
export function buildRunnerEnvironment(options: RunnerEnvironmentOptions): RunnerEnvironment {
    const { baseEnv, runtimeVariables, jobConfig } = options;
    const exists = options.exists ?? ((p: string) => fs.existsSync(p));
    const home = baseEnv.HOME || '';
    const extraPathDirs = [
        '/opt/homebrew/bin',
        '/opt/homebrew/sbin',
        '/usr/local/bin',
        '/usr/bin',
        '/bin',
        '/usr/sbin',
        '/sbin',
        path.join(home, 'google-cloud-sdk/bin'),
        path.join(home, '.local/bin'),
        path.join(home, '.pyenv/shims'),
        path.join(home, '.sdkman/candidates/maven/current/bin'),
        path.join(home, '.sdkman/candidates/java/current/bin')
    ];
    // NOTE: An empty PATH entry makes the shell search the current folder. Drop empty entries.
    const existingPaths = (baseEnv.PATH || '').split(path.delimiter).filter((p) => p !== '');
    const mergedPaths = Array.from(new Set([...extraPathDirs.filter(d => exists(d)), ...existingPaths]));

    const env: Record<string, string> = {
        ...baseEnv as Record<string, string>,
        PATH: mergedPaths.join(path.delimiter),
        PYTHONUNBUFFERED: '1',
        FORCE_COLOR: '0'
    };

    // S1/S9: runtime variables come from the .spark file; refuse keys that
    // would let it hijack bash, the dynamic loader or the JVM/PySpark runtime.
    const rejectedKeys = applyRuntimeVariables(env, runtimeVariables);
    if (jobConfig) {
        if (jobConfig.projectId && jobConfig.projectId !== PLACEHOLDER_PROJECT_ID) {
            env['PROJECT_ID'] = jobConfig.projectId;
        }
        if (jobConfig.region) {
            env['REGION'] = jobConfig.region;
        }
        const depsBucket = jobConfig.depsBucket || jobConfig.gcsStagingPath;
        if (depsBucket) {
            env['DEPS_BUCKET'] = depsBucket;
            env['GCS_STAGING_PATH'] = jobConfig.gcsStagingPath || depsBucket;
        }
        if (jobConfig.checkpointRoot) {
            env['CHECKPOINT_ROOT'] = jobConfig.checkpointRoot;
        }
        if (jobConfig.clusterName) {
            env['CLUSTER_NAME'] = jobConfig.clusterName;
        }
        if (jobConfig.runtimeVersion) {
            env['RUNTIME_VERSION'] = jobConfig.runtimeVersion;
        }
        const serverlessTier = jobConfig.tier || jobConfig.serverlessTier;
        if (serverlessTier) {
            env['SERVERLESS_TIER'] = serverlessTier;
        }
        const subnet = jobConfig.subnetwork || jobConfig.subnet;
        if (subnet) {
            env['SUBNET'] = subnet;
            env['SUBNETWORK'] = subnet;
        }
        if (jobConfig.serviceAccount) {
            env['SERVICE_ACCOUNT'] = jobConfig.serviceAccount;
        }
        const batchTtl = formatBatchTtl(jobConfig.batchTtlMinutes) ?? formatBatchTtl(jobConfig.batchTtl);
        if (batchTtl) {
            env['BATCH_TTL'] = batchTtl;
        }
        const sparkJars = jobConfig.customJarUris || jobConfig.sparkJars;
        if (sparkJars) {
            env['SPARK_JARS'] = sparkJars;
        }
        if (jobConfig.useSparkConnect === true) {
            env['USE_SPARK_CONNECT'] = 'true';
        } else if (jobConfig.useSparkConnect === false) {
            env['USE_SPARK_CONNECT'] = 'false';
        }
        if (jobConfig.sessionTemplate) {
            env['SPARK_CONNECT_SESSION_TEMPLATE'] = jobConfig.sessionTemplate;
        }

        // Spark Declarative Pipelines (SDP) sanitized environment exports (SDP8).
        if (jobConfig.sdpCommand === 'run' || jobConfig.sdpCommand === 'dry-run') {
            env['SDP_COMMAND'] = jobConfig.sdpCommand;
        }
        if (typeof jobConfig.sdpStorage === 'string') {
            const trimmedStorage = jobConfig.sdpStorage.trim();
            if (trimmedStorage && !/[\r\n\0]/.test(trimmedStorage)) {
                env['SDP_STORAGE'] = trimmedStorage;
            }
        }
        if (typeof jobConfig.sdpCatalog === 'string') {
            const trimmedCatalog = jobConfig.sdpCatalog.trim();
            if (SAFE_SDP_IDENTIFIER_REGEX.test(trimmedCatalog)) {
                env['SDP_CATALOG'] = trimmedCatalog;
            }
        }
        if (typeof jobConfig.sdpDatabase === 'string') {
            const trimmedDatabase = jobConfig.sdpDatabase.trim();
            if (SAFE_SDP_IDENTIFIER_REGEX.test(trimmedDatabase)) {
                env['SDP_DATABASE'] = trimmedDatabase;
            }
        }
        if (typeof jobConfig.sdpCreateNamespace === 'boolean') {
            env['SDP_CREATE_NAMESPACE'] = jobConfig.sdpCreateNamespace ? 'true' : 'false';
        }
        const staticConf = formatSdpStaticSparkConf(jobConfig.sdpStaticSparkConf);
        if (staticConf) {
            env['SPARK_PROPERTIES'] = env['SPARK_PROPERTIES']
                ? `${env['SPARK_PROPERTIES']},${staticConf}`
                : staticConf;
        }

        // Refresh flags apply only to `run` (never `dry-run`) and prevent conflicting combinations.
        if (jobConfig.sdpCommand !== 'dry-run') {
            if (jobConfig.sdpRefreshMode === 'full_refresh_all') {
                env['SDP_FULL_REFRESH_ALL'] = 'true';
                delete env['SDP_REFRESH'];
                delete env['SDP_FULL_REFRESH'];
            } else if (jobConfig.sdpRefreshMode === 'refresh') {
                const refreshDatasets = sanitizeSdpDatasetList(jobConfig.sdpRefreshDatasets);
                if (refreshDatasets.length > 0) {
                    env['SDP_REFRESH'] = refreshDatasets.join(',');
                }
            } else if (jobConfig.sdpRefreshMode === 'full_refresh') {
                const fullRefreshDatasets = sanitizeSdpDatasetList(jobConfig.sdpFullRefreshDatasets);
                if (fullRefreshDatasets.length > 0) {
                    env['SDP_FULL_REFRESH'] = fullRefreshDatasets.join(',');
                }
            } else if (jobConfig.sdpRefreshMode === undefined) {
                const fullSet = new Set(sanitizeSdpDatasetList(jobConfig.sdpFullRefreshDatasets));
                const refreshOnly = sanitizeSdpDatasetList(jobConfig.sdpRefreshDatasets).filter(
                    (name) => !fullSet.has(name)
                );
                if (refreshOnly.length > 0) {
                    env['SDP_REFRESH'] = refreshOnly.join(',');
                }
                if (fullSet.size > 0) {
                    env['SDP_FULL_REFRESH'] = [...fullSet].join(',');
                }
            }
        }
    }
    return { env, rejectedKeys };
}

/** Project and region that detected job IDs are reported against. */
export interface RunTarget {
    /** GCP project ID (may remain the placeholder when gcloud has no default). */
    project: string;
    /** GCP region. */
    region: string;
}

/**
 * Resolves the project and region for a run BEFORE spawning (F6), so a job ID
 * emitted early is never reported against the placeholder project. An explicit
 * projectId always wins over the gcloud default.
 *
 * @param jobConfig Job config from the run request.
 * @param getProject gcloud default-project lookup (injectable for tests).
 * @returns The resolved run target.
 */
export async function resolveRunTarget(
    jobConfig: RunnerJobConfig | undefined,
    getProject: () => Promise<string | undefined> = getGcloudProject
): Promise<RunTarget> {
    let project = jobConfig?.projectId || PLACEHOLDER_PROJECT_ID;
    if (project === PLACEHOLDER_PROJECT_ID) {
        const resolvedProject = await getProject().catch(() => undefined);
        if (resolvedProject) {
            project = resolvedProject;
        }
    }
    return { project, region: jobConfig?.region || DEFAULT_RUN_REGION };
}

/** A job ID detected in runner output. */
export interface DetectedRunnerJob {
    /** Serverless Spark batch ID or Dataproc cluster job ID. */
    jobId: string;
    /** Which service the job belongs to. */
    runner: 'ServerlessSpark' | 'DataprocCluster';
    /** Project to link the job against. */
    projectId: string;
    /** Region to link the job against. */
    region: string;
}

/**
 * Creates a stateful detector that reports the FIRST Serverless Spark batch ID
 * or Dataproc cluster job ID seen across output chunks, then ignores all further
 * chunks. Region and project parsed from a Console URL or full GCP resource path
 * take precedence over `target`.
 *
 * @param target Default project and region for reported jobs.
 * @returns A function to feed each output chunk that returns the job once, else null.
 */
export function createJobIdTracker(target: RunTarget): (chunk: string) => DetectedRunnerJob | null {
    let detected = false;
    return (chunk: string) => {
        if (detected) return null;

        const sparkJob = detectSparkJob(chunk);
        if (sparkJob) {
            detected = true;
            return {
                jobId: sparkJob.jobId,
                runner: sparkJob.runner,
                projectId: sparkJob.projectId || target.project,
                region: sparkJob.region || target.region
            };
        }

        return null;
    };
}

/** Safe identifier pattern for remote batch/job IDs passed to gcloud. */
const SAFE_REMOTE_JOB_ID_REGEX = /^[a-zA-Z0-9][a-zA-Z0-9_-]{1,127}$/;

/** Safe GCP region identifier pattern. */
const SAFE_REGION_REGEX = /^[a-zA-Z0-9-]+$/;

/** Safe GCP project ID pattern. */
const SAFE_PROJECT_ID_REGEX = /^[a-zA-Z0-9.:_-]+$/;

/** Options for {@link cancelRemoteSparkJob}. */
export interface CancelRemoteSparkJobOptions {
    /** Runner type: 'ServerlessSpark' | 'DataprocCluster'. */
    runner: string;
    /** Remote Serverless batch ID or Dataproc cluster job ID to cancel. */
    jobId: string;
    /** GCP project ID (resolved via gcloud when omitted or placeholder). */
    projectId?: string;
    /** GCP region (defaults to DEFAULT_RUN_REGION). */
    region?: string;
    /** Optional gcloud executable override for tests. */
    gcloudBin?: string;
    /** Optional project lookup override for tests. */
    getProject?: () => Promise<string | undefined>;
}

/** Result of {@link cancelRemoteSparkJob}. */
export interface CancelRemoteSparkJobResult {
    /** True when the gcloud cancellation command exited with code 0. */
    cancelled: boolean;
    /** The full gcloud command line executed (for logging). */
    commandLine: string;
    /** Standard output from gcloud, if any. */
    stdout?: string;
    /** Error message when validation or execution failed. */
    error?: string;
}

/**
 * Cancels an active remote Serverless Spark batch or kills a running Dataproc
 * Cluster job via `gcloud`:
 * - `ServerlessSpark`:
 *   `gcloud dataproc batches cancel <batchId> --region=<region> --project=<projectId> --quiet`
 * - `DataprocCluster`:
 *   `gcloud dataproc jobs kill <jobId> --region=<region> --project=<projectId> --quiet`
 *
 * @param options Cancellation target and optional test overrides.
 * @returns Cancellation outcome and command line string.
 */
export async function cancelRemoteSparkJob(
    options: CancelRemoteSparkJobOptions
): Promise<CancelRemoteSparkJobResult> {
    const jobId = options.jobId?.trim() || '';
    if (!SAFE_REMOTE_JOB_ID_REGEX.test(jobId)) {
        return {
            cancelled: false,
            commandLine: '',
            error: `Invalid remote Spark job/batch ID: '${options.jobId ?? ''}'`,
        };
    }

    const target = await resolveRunTarget(
        { projectId: options.projectId, region: options.region },
        options.getProject
    );
    const region = target.region.trim();
    if (!SAFE_REGION_REGEX.test(region)) {
        return {
            cancelled: false,
            commandLine: '',
            error: `Invalid GCP region: '${region}'`,
        };
    }

    const projectId = target.project.trim();
    if (!projectId || projectId === PLACEHOLDER_PROJECT_ID || !SAFE_PROJECT_ID_REGEX.test(projectId)) {
        return {
            cancelled: false,
            commandLine: '',
            error: `Valid GCP project ID is required to cancel remote job '${jobId}'.`,
        };
    }

    let args: string[];
    if (options.runner === 'ServerlessSpark') {
        args = [
            'dataproc',
            'batches',
            'cancel',
            jobId,
            `--region=${region}`,
            `--project=${projectId}`,
            '--quiet',
        ];
    } else if (options.runner === 'DataprocCluster') {
        args = [
            'dataproc',
            'jobs',
            'kill',
            jobId,
            `--region=${region}`,
            `--project=${projectId}`,
            '--quiet',
        ];
    } else {
        return {
            cancelled: false,
            commandLine: '',
            error: `Runner '${options.runner}' does not support remote Spark job cancellation.`,
        };
    }

    const gcloudBin = options.gcloudBin || findGcloudPath();
    const commandLine = `${gcloudBin} ${args.join(' ')}`;

    return new Promise((resolve) => {
        execFile(
            gcloudBin,
            args,
            { timeout: 15000 },
            (err: ExecFileException | null, stdout: string, stderr: string) => {
                if (err) {
                    const errText = stderr?.trim() || err.message;
                    resolve({
                        cancelled: false,
                        commandLine,
                        error: errText,
                    });
                    return;
                }
                resolve({
                    cancelled: true,
                    commandLine,
                    ...(stdout?.trim() ? { stdout: stdout.trim() } : {}),
                });
            }
        );
    });
}

/** Callbacks wired to the spawned runner process. */
export interface RunnerProcessCallbacks {
    /** A stdout/stderr chunk was produced. */
    onOutput: (text: string, stream: 'stdout' | 'stderr') => void;
    /** The process could not be started or crashed at the OS level. */
    onError: (err: Error) => void;
    /** The process exited. */
    onClose: (code: number | null, signal: string | null) => void;
}

/**
 * Spawns the runner script. On POSIX it runs via `bash` as a detached
 * process-group leader so Stop can signal the whole tree (bash plus the
 * JVM/python it launched); on Windows the script is executed directly.
 *
 * @param folderPath Working directory (the generated pipeline folder).
 * @param scriptFullPath Absolute path of the runner script.
 * @param env Environment from {@link buildRunnerEnvironment}.
 * @param callbacks Output, error and exit callbacks.
 * @returns The spawned child process.
 */
export function spawnRunnerScript(
    folderPath: string,
    scriptFullPath: string,
    env: Record<string, string>,
    callbacks: RunnerProcessCallbacks
): ChildProcess {
    const isWindows = process.platform === 'win32';
    const spawnCmd = isWindows ? scriptFullPath : 'bash';
    const spawnArgs = isWindows ? [] : [scriptFullPath];

    const child = spawn(spawnCmd, spawnArgs, {
        cwd: folderPath,
        env,
        detached: !isWindows
    });

    child.stdout?.on('data', (data: Buffer | string) => callbacks.onOutput(data.toString(), 'stdout'));
    child.stderr?.on('data', (data: Buffer | string) => callbacks.onOutput(data.toString(), 'stderr'));
    child.on('error', (err: Error) => callbacks.onError(err));
    child.on('close', (code: number | null, signal: string | null) => callbacks.onClose(code, signal));
    return child;
}
