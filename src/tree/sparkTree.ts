/** @fileoverview Provides the sidebar tree view for the Apache Spark Explorer.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import * as vscode from 'vscode';
import * as path from 'path';
import { execFile } from 'child_process';
import { findGcloudPath } from '../cli/cliResolver';
import { getGcloudProject } from '../cli/gcloudClient';
import { PIPELINE_SEARCH_EXCLUDE_PATTERN, SPARK_EDITOR_VIEW_TYPE } from '../core/constants';
import { isValidGcpRegion } from '../security/pathGuard';

/**
 * Interface representing a normalized Spark / Dataproc job or batch item.
 */
export interface SparkJob {
    id: string;
    name: string;
    state: string;
    region?: string;
    location?: string;
    jobType?: 'serverless-batch' | 'cluster-job';
    category?: 'batch' | 'cluster-job';
    clusterName?: string;
    runtimeVersion?: string;
    type?: string;
    uuid?: string;
    createTime?: string;
    creationTime?: string;
    startTime?: string;
    stateTime?: string;
    stateStartTime?: string;
    statusHistory?: Array<{ state?: string; stateStartTime?: string }>;
}

/**
 * Raw Dataproc Serverless Batch object returned by `gcloud dataproc batches list --format=json`.
 */
export interface RawServerlessBatch {
    name?: string;
    uuid?: string;
    state?: string;
    createTime?: string;
    stateTime?: string;
    runtimeConfig?: {
        version?: string;
        properties?: Record<string, string>;
    };
    pysparkBatch?: {
        mainPythonFileUri?: string;
    };
    labels?: Record<string, string>;
    [key: string]: unknown;
}

/**
 * Raw Dataproc Cluster Job object returned by `gcloud dataproc jobs list --format=json`.
 */
export interface RawClusterJob {
    reference?: {
        jobId?: string;
        projectId?: string;
    };
    placement?: {
        clusterName?: string;
        clusterUuid?: string;
    };
    status?: {
        state?: string;
        stateStartTime?: string;
        details?: string;
    };
    statusHistory?: Array<{
        state?: string;
        stateStartTime?: string;
    }>;
    pysparkJob?: {
        mainPythonFileUri?: string;
    };
    labels?: Record<string, string>;
    [key: string]: unknown;
}

/**
 * Categorizes a Spark batch or cluster job state into one of the three standard groups:
 * 'Running' (1st), 'Cancelled' (2nd), or 'Completed' (3rd).
 *
 * Serverless Batch states:
 * - Running: PENDING, RUNNING, CANCELLING
 * - Cancelled: CANCELLED
 * - Completed: SUCCEEDED, FAILED
 *
 * Dataproc Cluster Job states:
 * - Running: PENDING, SETUP_DONE, RUNNING, CANCEL_PENDING, CANCEL_STARTED
 * - Cancelled: CANCELLED
 * - Completed: DONE, ERROR, ATTEMPT_FAILURE
 *
 * @param state Raw state string from Dataproc API / gcloud CLI.
 * @returns 'Running' | 'Cancelled' | 'Completed'
 */
export function categorizeJobStatus(state: string): 'Running' | 'Cancelled' | 'Completed' {
    const normalized = (state || '').trim().toUpperCase();

    // Active / in-progress states (including CANCELLING, CANCEL_PENDING, CANCEL_STARTED, SETUP_DONE)
    if (
        normalized === 'PENDING' ||
        normalized === 'RUNNING' ||
        normalized === 'CANCELLING' ||
        normalized === 'SETUP_DONE' ||
        normalized === 'CANCEL_PENDING' ||
        normalized === 'CANCEL_STARTED' ||
        normalized.includes('RUNNING') ||
        normalized.includes('PENDING') ||
        normalized.includes('QUEUED') ||
        normalized.includes('CANCELLING') ||
        normalized.includes('SETUP_DONE') ||
        normalized.includes('RESOURCE_CLEANING_UP')
    ) {
        return 'Running';
    }

    // Terminal cancelled / stopped states
    if (
        normalized === 'CANCELLED' ||
        normalized === 'CANCELED' ||
        normalized.includes('CANCELLED') ||
        normalized.includes('CANCELED') ||
        normalized.includes('DRAIN') ||
        normalized.includes('STOPPED')
    ) {
        return 'Cancelled';
    }

    // Completed includes SUCCEEDED, FAILED, DONE, ERROR, ATTEMPT_FAILURE, etc.
    return 'Completed';
}

/**
 * Parses a timestamp string into milliseconds since epoch, returning 0 if invalid or missing.
 *
 * @param timeStr ISO timestamp string.
 * @returns Timestamp in milliseconds, or 0.
 */
function parseTimestamp(timeStr?: string): number {
    if (!timeStr) {
        return 0;
    }
    const t = new Date(timeStr).getTime();
    return isNaN(t) ? 0 : t;
}

/**
 * Extracts a numeric timestamp for sorting Spark batches or cluster jobs.
 *
 * - For Serverless Batches: uses `createTime` (falling back to `creationTime`, `startTime`, `stateTime`).
 * - For Dataproc Cluster Jobs: uses `status.stateStartTime` / `stateStartTime` (falling back to latest `statusHistory` start time, `startTime`, `createTime`, or `0`).
 *
 * @param job SparkJob or raw job/batch object.
 * @returns Timestamp in milliseconds.
 */
export function getJobStartTime(job: SparkJob | RawClusterJob | RawServerlessBatch): number {
    const anyJob = job as Record<string, unknown>;
    const statusObj = typeof anyJob.status === 'object' && anyJob.status !== null
        ? (anyJob.status as Record<string, unknown>)
        : undefined;

    // Check cluster job status.stateStartTime or normalized stateStartTime
    const directStateStart =
        (typeof anyJob.stateStartTime === 'string' && anyJob.stateStartTime) ||
        (typeof statusObj?.stateStartTime === 'string' && statusObj.stateStartTime) ||
        undefined;
    if (directStateStart) {
        const parsed = parseTimestamp(directStateStart);
        if (parsed > 0) {
            return parsed;
        }
    }

    // Check statusHistory if present (find latest stateStartTime)
    if (Array.isArray(anyJob.statusHistory) && anyJob.statusHistory.length > 0) {
        let maxHistoryTime = 0;
        for (const entry of anyJob.statusHistory) {
            const entryObj = typeof entry === 'object' && entry !== null ? (entry as Record<string, unknown>) : undefined;
            const entryTime = typeof entryObj?.stateStartTime === 'string' ? entryObj.stateStartTime : undefined;
            const t = parseTimestamp(entryTime);
            if (t > maxHistoryTime) {
                maxHistoryTime = t;
            }
        }
        if (maxHistoryTime > 0) {
            return maxHistoryTime;
        }
    }

    // Check createTime / startTime / creationTime / stateTime
    const fallbackStr =
        (typeof anyJob.createTime === 'string' && anyJob.createTime) ||
        (typeof anyJob.startTime === 'string' && anyJob.startTime) ||
        (typeof anyJob.creationTime === 'string' && anyJob.creationTime) ||
        (typeof anyJob.stateTime === 'string' && anyJob.stateTime) ||
        undefined;
    return parseTimestamp(fallbackStr);
}

/**
 * Normalizes a raw Dataproc Serverless Batch object into a SparkJob.
 *
 * @param raw Raw batch entry from `gcloud dataproc batches list` or an already-normalized SparkJob.
 * @param defaultRegion Fallback region when not embedded in the resource name.
 * @returns Normalized SparkJob object.
 */
export function normalizeServerlessBatch(raw: RawServerlessBatch | SparkJob, defaultRegion = 'us-central1'): SparkJob {
    const anyRaw = raw as Record<string, unknown>;
    const rawName = typeof anyRaw.name === 'string' ? anyRaw.name : '';

    // Parse resource name: projects/<project>/locations/<region>/batches/<batchId>
    let batchId = typeof anyRaw.id === 'string' && anyRaw.id ? anyRaw.id : '';
    let region = typeof anyRaw.region === 'string' && anyRaw.region
        ? anyRaw.region
        : (typeof anyRaw.location === 'string' && anyRaw.location ? anyRaw.location : defaultRegion);

    const match = rawName.match(/locations\/([^/]+)\/batches\/([^/]+)$/);
    if (match) {
        region = match[1];
        batchId = match[2];
    } else if (!batchId && rawName) {
        const parts = rawName.split('/');
        batchId = parts[parts.length - 1] || rawName;
    }

    if (!batchId && typeof anyRaw.uuid === 'string') {
        batchId = anyRaw.uuid;
    }

    const displayName = batchId || rawName || 'unknown-batch';
    const state = typeof anyRaw.state === 'string' ? anyRaw.state : 'UNKNOWN';
    const createTime = typeof anyRaw.createTime === 'string'
        ? anyRaw.createTime
        : (typeof anyRaw.creationTime === 'string' ? anyRaw.creationTime : undefined);
    const stateTime = typeof anyRaw.stateTime === 'string' ? anyRaw.stateTime : undefined;
    const runtimeConfigObj = typeof anyRaw.runtimeConfig === 'object' && anyRaw.runtimeConfig !== null
        ? (anyRaw.runtimeConfig as Record<string, unknown>)
        : undefined;
    const runtimeVersion = typeof anyRaw.runtimeVersion === 'string'
        ? anyRaw.runtimeVersion
        : (typeof runtimeConfigObj?.version === 'string' ? runtimeConfigObj.version : undefined);

    return {
        id: batchId,
        name: displayName,
        state,
        region,
        location: region,
        jobType: 'serverless-batch',
        category: 'batch',
        uuid: typeof anyRaw.uuid === 'string' ? anyRaw.uuid : undefined,
        createTime,
        creationTime: createTime,
        stateTime,
        runtimeVersion,
        type: 'Serverless Batch',
    };
}

/**
 * Normalizes a raw Dataproc Cluster Job object into a SparkJob.
 *
 * @param raw Raw job entry from `gcloud dataproc jobs list` or an already-normalized SparkJob.
 * @param defaultRegion Region used when querying cluster jobs.
 * @returns Normalized SparkJob object.
 */
export function normalizeClusterJob(raw: RawClusterJob | SparkJob, defaultRegion = 'us-central1'): SparkJob {
    const anyRaw = raw as Record<string, unknown>;
    const refObj = typeof anyRaw.reference === 'object' && anyRaw.reference !== null
        ? (anyRaw.reference as Record<string, unknown>)
        : undefined;
    const placementObj = typeof anyRaw.placement === 'object' && anyRaw.placement !== null
        ? (anyRaw.placement as Record<string, unknown>)
        : undefined;
    const statusObj = typeof anyRaw.status === 'object' && anyRaw.status !== null
        ? (anyRaw.status as Record<string, unknown>)
        : undefined;

    const jobId =
        (typeof refObj?.jobId === 'string' && refObj.jobId) ||
        (typeof anyRaw.id === 'string' && anyRaw.id) ||
        (typeof anyRaw.name === 'string' && anyRaw.name) ||
        'unknown-job';

    const clusterName =
        (typeof placementObj?.clusterName === 'string' && placementObj.clusterName) ||
        (typeof anyRaw.clusterName === 'string' && anyRaw.clusterName) ||
        undefined;

    const state =
        (typeof statusObj?.state === 'string' && statusObj.state) ||
        (typeof anyRaw.state === 'string' && anyRaw.state) ||
        'UNKNOWN';

    const region =
        (typeof anyRaw.region === 'string' && anyRaw.region) ||
        (typeof anyRaw.location === 'string' && anyRaw.location) ||
        defaultRegion;

    const statusHistory = Array.isArray(anyRaw.statusHistory)
        ? (anyRaw.statusHistory as Array<{ state?: string; stateStartTime?: string }>)
        : undefined;

    let stateStartTime =
        (typeof statusObj?.stateStartTime === 'string' && statusObj.stateStartTime) ||
        (typeof anyRaw.stateStartTime === 'string' && anyRaw.stateStartTime) ||
        undefined;

    if (!stateStartTime && statusHistory && statusHistory.length > 0) {
        let latestTime = 0;
        let latestStr: string | undefined;
        for (const entry of statusHistory) {
            const t = parseTimestamp(entry?.stateStartTime);
            if (t > latestTime && entry?.stateStartTime) {
                latestTime = t;
                latestStr = entry.stateStartTime;
            }
        }
        stateStartTime = latestStr;
    }

    const displayName =
        typeof anyRaw.name === 'string' && anyRaw.name && !refObj?.jobId
            ? anyRaw.name
            : jobId;

    return {
        id: jobId,
        name: displayName,
        state,
        region,
        location: region,
        jobType: 'cluster-job',
        category: 'cluster-job',
        clusterName,
        stateStartTime,
        startTime: stateStartTime || (typeof anyRaw.startTime === 'string' ? anyRaw.startTime : undefined),
        createTime: typeof anyRaw.createTime === 'string' ? anyRaw.createTime : (typeof anyRaw.creationTime === 'string' ? anyRaw.creationTime : undefined),
        creationTime: typeof anyRaw.creationTime === 'string' ? anyRaw.creationTime : undefined,
        statusHistory,
        type: 'Cluster Job',
    };
}

/**
 * Provides the sidebar tree view for the Spark Explorer, including workspace `.spark`
 * pipeline files, GCP Serverless Spark Batches, and Dataproc Cluster Jobs.
 */
export class SparkTreeProvider implements vscode.TreeDataProvider<DataItem> {
    private _onDidChangeTreeData: vscode.EventEmitter<DataItem | undefined | null | void> = new vscode.EventEmitter<DataItem | undefined | null | void>();
    readonly onDidChangeTreeData: vscode.Event<DataItem | undefined | null | void> = this._onDidChangeTreeData.event;

    private pipelines: DataItem[] = [];
    private defaultRegion: string;

    /**
     * Initializes the SparkTreeProvider and schedules an initial workspace scan.
     *
     * @param defaultRegion Default GCP region for querying batches and cluster jobs (defaults to 'us-central1').
     */
    constructor(defaultRegion = 'us-central1') {
        this.defaultRegion = defaultRegion;
        this.refresh();
    }

    /**
     * Refreshes the tree provider by rescanning `.spark` files and notifying the VS Code tree view.
     */
    refresh(): void {
        void this.scanPipelines();
    }

    /**
     * Scans the workspace for `*.spark` files and builds the tree hierarchy.
     * Excludes dependency, build, and VCS directories using PIPELINE_SEARCH_EXCLUDE_PATTERN.
     */
    private async scanPipelines(): Promise<void> {
        const files = await vscode.workspace.findFiles('**/*.spark', PIPELINE_SEARCH_EXCLUDE_PATTERN);
        this.pipelines = this.buildTree(files);
        this._onDidChangeTreeData.fire();
    }

    /**
     * Resolves the active GCP region to use when querying Dataproc batches and jobs.
     *
     * @param regionOverride Optional region override.
     * @returns Validated GCP region string (fallback: 'us-central1').
     */
    private resolveRegion(regionOverride?: string): string {
        if (regionOverride && isValidGcpRegion(regionOverride)) {
            return regionOverride;
        }
        const config = vscode.workspace.getConfiguration?.('sparkDesigner');
        const configuredRegion = config?.get<string>('region');
        if (configuredRegion && isValidGcpRegion(configuredRegion)) {
            return configuredRegion;
        }
        return isValidGcpRegion(this.defaultRegion) ? this.defaultRegion : 'us-central1';
    }

    /**
     * Builds hierarchical tree nodes from a list of `.spark` file URIs.
     *
     * @param files List of Uri objects for discovered `.spark` files.
     * @returns Array of DataItem representing root folders and files.
     */
    public buildTree(files: vscode.Uri[]): DataItem[] {
        const root: DataItem[] = [];
        const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
        if (!workspaceFolder) {
            return [];
        }

        const workspacePath = workspaceFolder.uri.fsPath;

        files.forEach(file => {
            const relativePath = path.relative(workspacePath, file.fsPath);
            const parts = relativePath.split(path.sep);

            let currentLevel = root;

            for (let i = 0; i < parts.length - 1; i++) {
                const part = parts[i];
                let folder = currentLevel.find(item => item.label === part && item.contextValue === 'folder');
                if (!folder) {
                    const children: DataItem[] = [];
                    folder = new DataItem(part, vscode.TreeItemCollapsibleState.Collapsed, 'folder', children);
                    currentLevel.push(folder);
                    currentLevel = children;
                } else {
                    currentLevel = folder.children ?? [];
                }
            }

            const fileName = parts[parts.length - 1];
            currentLevel.push(new DataItem(fileName, vscode.TreeItemCollapsibleState.None, 'pipeline-edit', undefined, file));
        });

        return root;
    }

    /**
     * Queries GCP Dataproc Serverless Batches via the gcloud CLI (`R9`).
     * Executes `gcloud dataproc batches list --region=<REGION> --limit=100 --format=json`.
     *
     * @param projectId Optional GCP project ID override.
     * @param region Optional GCP region override (defaults to 'us-central1').
     * @returns Promise resolving to normalized batches, projectId, region, and optional error.
     */
    public async fetchServerlessBatches(
        projectId?: string,
        region?: string
    ): Promise<{ batches: SparkJob[]; projectId?: string; region: string; error?: string }> {
        const targetProject = projectId || (await getGcloudProject());
        const targetRegion = this.resolveRegion(region);
        const gcloudBin = findGcloudPath();
        const gcloudArgs = [
            'dataproc',
            'batches',
            'list',
            `--region=${targetRegion}`,
            '--limit=100',
            '--format=json'
        ];

        if (targetProject && targetProject !== 'YOUR_PROJECT_ID') {
            gcloudArgs.push(`--project=${targetProject}`);
        }

        return new Promise((resolve) => {
            execFile(gcloudBin, gcloudArgs, { timeout: 15000, maxBuffer: 1024 * 1024 * 10 }, (error, stdout, stderr) => {
                if (error) {
                    const errMsg = error.killed
                        ? 'Request timed out waiting for gcloud CLI response.'
                        : (stderr || error.message);
                    resolve({ batches: [], projectId: targetProject, region: targetRegion, error: errMsg });
                    return;
                }
                try {
                    const parsed = JSON.parse(stdout || '[]');
                    const rawList: RawServerlessBatch[] = Array.isArray(parsed)
                        ? parsed
                        : (Array.isArray(parsed?.batches) ? parsed.batches : []);
                    const batches = rawList.map(item => normalizeServerlessBatch(item, targetRegion));
                    resolve({ batches, projectId: targetProject, region: targetRegion });
                } catch (e: unknown) {
                    const message = e instanceof Error ? e.message : String(e);
                    resolve({
                        batches: [],
                        projectId: targetProject,
                        region: targetRegion,
                        error: `Failed to parse gcloud output: ${message}`
                    });
                }
            });
        });
    }

    /**
     * Queries GCP Dataproc Cluster Jobs via the gcloud CLI (`R10`).
     * Executes `gcloud dataproc jobs list --region=<REGION> --limit=100 --format=json`.
     *
     * @param projectId Optional GCP project ID override.
     * @param region Optional GCP region override (defaults to 'us-central1').
     * @returns Promise resolving to normalized cluster jobs, projectId, region, and optional error.
     */
    public async fetchClusterJobs(
        projectId?: string,
        region?: string
    ): Promise<{ jobs: SparkJob[]; projectId?: string; region: string; error?: string }> {
        const targetProject = projectId || (await getGcloudProject());
        const targetRegion = this.resolveRegion(region);
        const gcloudBin = findGcloudPath();
        const gcloudArgs = [
            'dataproc',
            'jobs',
            'list',
            `--region=${targetRegion}`,
            '--limit=100',
            '--format=json'
        ];

        if (targetProject && targetProject !== 'YOUR_PROJECT_ID') {
            gcloudArgs.push(`--project=${targetProject}`);
        }

        return new Promise((resolve) => {
            execFile(gcloudBin, gcloudArgs, { timeout: 15000, maxBuffer: 1024 * 1024 * 10 }, (error, stdout, stderr) => {
                if (error) {
                    const errMsg = error.killed
                        ? 'Request timed out waiting for gcloud CLI response.'
                        : (stderr || error.message);
                    resolve({ jobs: [], projectId: targetProject, region: targetRegion, error: errMsg });
                    return;
                }
                try {
                    const parsed = JSON.parse(stdout || '[]');
                    const rawList: RawClusterJob[] = Array.isArray(parsed)
                        ? parsed
                        : (Array.isArray(parsed?.jobs) ? parsed.jobs : []);
                    const jobs = rawList.map(item => normalizeClusterJob(item, targetRegion));
                    resolve({ jobs, projectId: targetProject, region: targetRegion });
                } catch (e: unknown) {
                    const message = e instanceof Error ? e.message : String(e);
                    resolve({
                        jobs: [],
                        projectId: targetProject,
                        region: targetRegion,
                        error: `Failed to parse gcloud output: ${message}`
                    });
                }
            });
        });
    }

    /**
     * Backward-compatible alias that delegates to `fetchServerlessBatches`.
     *
     * @param projectId Optional GCP project ID override.
     * @param region Optional GCP region override.
     * @returns Promise resolving to jobs array, projectId, and optional error.
     */
    public async fetchJobs(
        projectId?: string,
        region?: string
    ): Promise<{ jobs: SparkJob[]; projectId?: string; error?: string }> {
        const result = await this.fetchServerlessBatches(projectId, region);
        return {
            jobs: result.batches,
            projectId: result.projectId,
            error: result.error
        };
    }

    /**
     * Builds group tree items (`Running`, `Cancelled`, `Completed`) for Serverless Batches (`R9`).
     * Sorts batches within each group by `createTime` descending (newest first).
     *
     * @param batches List of raw or normalized Serverless Batch items.
     * @param projectId Active GCP project ID.
     * @param defaultRegion Default GCP region if not present on an item.
     * @returns Array of DataItem representing the 3 ordered status groups.
     */
    public buildBatchGroups(
        batches: Array<SparkJob | RawServerlessBatch>,
        projectId?: string,
        defaultRegion = 'us-central1'
    ): DataItem[] {
        const normalized = batches.map(b => normalizeServerlessBatch(b, defaultRegion));
        return this.buildGroupedTreeItems(normalized, projectId, 'serverless-batch');
    }

    /**
     * Builds group tree items (`Running`, `Cancelled`, `Completed`) for Dataproc Cluster Jobs (`R10`).
     * Sorts jobs within each group by `status.stateStartTime` descending (falling back to latest `statusHistory` start time or `0`).
     *
     * @param jobs List of raw or normalized Dataproc Cluster Job items.
     * @param projectId Active GCP project ID.
     * @param defaultRegion Default GCP region if not present on an item.
     * @returns Array of DataItem representing the 3 ordered status groups.
     */
    public buildClusterJobGroups(
        jobs: Array<SparkJob | RawClusterJob>,
        projectId?: string,
        defaultRegion = 'us-central1'
    ): DataItem[] {
        const normalized = jobs.map(j => normalizeClusterJob(j, defaultRegion));
        return this.buildGroupedTreeItems(normalized, projectId, 'cluster-job');
    }

    /**
     * Builds group tree items (`Running`, `Cancelled`, `Completed`) for generic Spark jobs.
     * Preserves each item's `jobType` (`'serverless-batch'` or `'cluster-job'`, defaulting to `'serverless-batch'`).
     *
     * @param jobs List of SparkJob items.
     * @param projectId Active GCP project ID.
     * @param defaultJobType Default job type when not specified on individual items.
     * @returns Array of DataItem representing the 3 status groups.
     */
    public buildJobGroups(
        jobs: SparkJob[],
        projectId?: string,
        defaultJobType: 'serverless-batch' | 'cluster-job' = 'serverless-batch'
    ): DataItem[] {
        const normalized = jobs.map(job => {
            const jobType = job.jobType || (job.category === 'cluster-job' ? 'cluster-job' : defaultJobType);
            if (jobType === 'cluster-job') {
                return normalizeClusterJob(job, job.region || job.location || this.defaultRegion);
            }
            return normalizeServerlessBatch(job, job.region || job.location || this.defaultRegion);
        });
        return this.buildGroupedTreeItems(normalized, projectId, defaultJobType);
    }

    /**
     * Internal helper that groups normalized `SparkJob` items into `Running`, `Cancelled`, and `Completed`,
     * sorts them descending by timestamp, and constructs `DataItem` nodes wired to `openJobInConsole`.
     *
     * @param jobs Normalized SparkJob list.
     * @param projectId Active GCP project ID.
     * @param defaultJobType Fallback jobType.
     * @returns Array of 3 group DataItems in strict order: 1) Running, 2) Cancelled, 3) Completed.
     */
    private buildGroupedTreeItems(
        jobs: SparkJob[],
        projectId?: string,
        defaultJobType: 'serverless-batch' | 'cluster-job' = 'serverless-batch'
    ): DataItem[] {
        const runningJobs: SparkJob[] = [];
        const cancelledJobs: SparkJob[] = [];
        const completedJobs: SparkJob[] = [];

        // 1. Group each job into Running, Cancelled, or Completed
        for (const job of jobs) {
            const category = categorizeJobStatus(job.state);
            if (category === 'Running') {
                runningJobs.push(job);
            } else if (category === 'Cancelled') {
                cancelledJobs.push(job);
            } else {
                completedJobs.push(job);
            }
        }

        // 2. Sort jobs in each group by timestamp descending (newest first)
        const sortByTimeDesc = (a: SparkJob, b: SparkJob) => getJobStartTime(b) - getJobStartTime(a);
        runningJobs.sort(sortByTimeDesc);
        cancelledJobs.sort(sortByTimeDesc);
        completedJobs.sort(sortByTimeDesc);

        // 3. Create tree items for individual batches / cluster jobs
        const createJobItem = (job: SparkJob): DataItem => {
            const jobType = job.jobType || defaultJobType;
            const dateStr = jobType === 'cluster-job'
                ? (job.stateStartTime || job.startTime || job.createTime || job.creationTime)
                : (job.createTime || job.creationTime || job.startTime || job.stateTime);
            const formattedDate = dateStr ? new Date(dateStr).toLocaleString() : '';
            const clusterPrefix = job.clusterName ? `${job.clusterName} • ` : '';
            const description = `${clusterPrefix}${job.state || 'Unknown'}${formattedDate ? ` • ${formattedDate}` : ''}`;

            const itemContextValue = jobType === 'cluster-job' ? 'cluster-job' : 'serverless-batch';
            const item = new DataItem(
                job.name || job.id,
                vscode.TreeItemCollapsibleState.None,
                itemContextValue
            );
            item.description = description;

            const region = job.region || job.location || this.defaultRegion;
            const jobPayload: SparkJob = {
                ...job,
                region,
                location: region,
                jobType,
                category: jobType === 'cluster-job' ? 'cluster-job' : 'batch'
            };

            const extraLines = jobType === 'cluster-job'
                ? (job.clusterName ? `- **Cluster:** \`${job.clusterName}\`\n` : '')
                : (job.runtimeVersion ? `- **Runtime Version:** ${job.runtimeVersion}\n` : '');

            item.tooltip = new vscode.MarkdownString(
                `**${job.name || job.id}**\n\n` +
                `- **ID:** \`${job.id}\`\n` +
                `- **State:** ${job.state}\n` +
                `- **Type:** ${job.type || (jobType === 'cluster-job' ? 'Cluster Job' : 'Serverless Batch')}\n` +
                extraLines +
                `- **Region:** ${region}\n` +
                `- **Timestamp:** ${formattedDate || '-'}\n\n` +
                `*Click to open in Google Cloud Console*`
            );

            // Assign status icon
            const normalizedState = (job.state || '').toUpperCase();
            if (normalizedState.includes('RUNNING') || normalizedState === 'SETUP_DONE') {
                item.iconPath = new vscode.ThemeIcon('play-circle', new vscode.ThemeColor('testing.iconPassed'));
            } else if (
                normalizedState.includes('PENDING') ||
                normalizedState.includes('QUEUED') ||
                normalizedState.includes('CANCELLING') ||
                normalizedState.includes('CANCEL_STARTED')
            ) {
                item.iconPath = new vscode.ThemeIcon('clock', new vscode.ThemeColor('testing.iconQueued'));
            } else if (normalizedState.includes('CANCEL') || normalizedState.includes('STOPPED')) {
                item.iconPath = new vscode.ThemeIcon('circle-slash', new vscode.ThemeColor('testing.iconSkipped'));
            } else if (
                normalizedState.includes('FAIL') ||
                normalizedState.includes('ERROR')
            ) {
                item.iconPath = new vscode.ThemeIcon('error', new vscode.ThemeColor('testing.iconFailed'));
            } else {
                item.iconPath = new vscode.ThemeIcon('check', new vscode.ThemeColor('testing.iconPassed'));
            }

            // Clicking opens in Google Cloud Console (R11)
            item.command = {
                command: 'spark-pipeline-builder.openJobInConsole',
                title: 'Open Job in Cloud Console',
                arguments: [jobPayload, projectId]
            };

            return item;
        };

        const runningItems = runningJobs.map(createJobItem);
        const cancelledItems = cancelledJobs.map(createJobItem);
        const completedItems = completedJobs.map(createJobItem);

        // 4. Return the 3 groups in strict order: 1) Running, 2) Cancelled, 3) Completed
        const groups: DataItem[] = [
            new DataItem(
                `Running (${runningJobs.length})`,
                runningJobs.length > 0 ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.Collapsed,
                'job-group-running',
                runningItems
            ),
            new DataItem(
                `Cancelled (${cancelledJobs.length})`,
                vscode.TreeItemCollapsibleState.Collapsed,
                'job-group-cancelled',
                cancelledItems
            ),
            new DataItem(
                `Completed (${completedJobs.length})`,
                vscode.TreeItemCollapsibleState.Collapsed,
                'job-group-completed',
                completedItems
            )
        ];

        // Apply icons to the group header nodes
        groups[0].iconPath = new vscode.ThemeIcon('run');
        groups[1].iconPath = new vscode.ThemeIcon('stop-circle');
        groups[2].iconPath = new vscode.ThemeIcon('check-all');

        return groups;
    }

    /**
     * Returns the TreeItem for display in the VS Code sidebar.
     *
     * @param element The tree item element.
     * @returns The VS Code TreeItem instance.
     */
    getTreeItem(element: DataItem): vscode.TreeItem {
        return element;
    }

    /**
     * Returns the children for a given tree item or root level (`R8`).
     *
     * Root level features 3 sections:
     * 1. `Pipelines` (`contextValue: 'category-pipelines'`)
     * 2. `Serverless Batches` (`contextValue: 'category-serverless-batches'`)
     * 3. `Dataproc Cluster Jobs` (`contextValue: 'category-cluster-jobs'`)
     *
     * @param element Optional parent tree item element.
     * @returns Promise resolving to an array of child DataItem nodes.
     */
    async getChildren(element?: DataItem): Promise<DataItem[]> {
        // Root Level: Return "Pipelines", "Serverless Batches", and "Dataproc Cluster Jobs"
        if (!element) {
            const pipelineChildren = this.pipelines.length > 0
                ? this.pipelines
                : [new DataItem('No pipelines found', vscode.TreeItemCollapsibleState.None, 'empty')];

            const pipelinesCategory = new DataItem(
                'Pipelines',
                vscode.TreeItemCollapsibleState.Expanded,
                'category-pipelines',
                pipelineChildren
            );
            pipelinesCategory.iconPath = new vscode.ThemeIcon('repo');

            const serverlessBatchesCategory = new DataItem(
                'Serverless Batches',
                vscode.TreeItemCollapsibleState.Collapsed,
                'category-serverless-batches'
            );
            serverlessBatchesCategory.iconPath = new vscode.ThemeIcon('flame');

            const clusterJobsCategory = new DataItem(
                'Dataproc Cluster Jobs',
                vscode.TreeItemCollapsibleState.Collapsed,
                'category-cluster-jobs'
            );
            clusterJobsCategory.iconPath = new vscode.ThemeIcon('server-process');

            return [pipelinesCategory, serverlessBatchesCategory, clusterJobsCategory];
        }

        // Expanded "Pipelines" Category
        if (element.contextValue === 'category-pipelines' || element.label === 'Pipelines') {
            return element.children || [];
        }

        // Expanded Folder inside Pipelines
        if (element.contextValue === 'folder') {
            return element.children || [];
        }

        // Expanded "Serverless Batches" Category (R9) -> dynamically query GCP Dataproc Batches
        if (element.contextValue === 'category-serverless-batches' || element.contextValue === 'category-running-jobs') {
            const { batches, projectId, region, error } = await this.fetchServerlessBatches();
            if (error) {
                const errorItem = new DataItem(
                    `Error: ${error}`,
                    vscode.TreeItemCollapsibleState.None,
                    'job-error'
                );
                errorItem.iconPath = new vscode.ThemeIcon('error', new vscode.ThemeColor('testing.iconFailed'));
                return [errorItem];
            }

            if (batches.length === 0) {
                const emptyItem = new DataItem(
                    'No Serverless Spark batches found',
                    vscode.TreeItemCollapsibleState.None,
                    'job-empty'
                );
                emptyItem.iconPath = new vscode.ThemeIcon('info');
                return [emptyItem];
            }

            return this.buildBatchGroups(batches, projectId, region);
        }

        // Expanded "Dataproc Cluster Jobs" Category (R10) -> dynamically query GCP Dataproc Jobs
        if (element.contextValue === 'category-cluster-jobs') {
            const { jobs, projectId, region, error } = await this.fetchClusterJobs();
            if (error) {
                const errorItem = new DataItem(
                    `Error: ${error}`,
                    vscode.TreeItemCollapsibleState.None,
                    'job-error'
                );
                errorItem.iconPath = new vscode.ThemeIcon('error', new vscode.ThemeColor('testing.iconFailed'));
                return [errorItem];
            }

            if (jobs.length === 0) {
                const emptyItem = new DataItem(
                    'No Dataproc cluster jobs found',
                    vscode.TreeItemCollapsibleState.None,
                    'job-empty'
                );
                emptyItem.iconPath = new vscode.ThemeIcon('info');
                return [emptyItem];
            }

            return this.buildClusterJobGroups(jobs, projectId, region);
        }

        // Expanded Job Group (Running, Cancelled, Completed)
        if (element.contextValue?.startsWith('job-group-')) {
            return element.children || [];
        }

        return [];
    }
}

/**
 * Represents individual items (categories, folders, pipeline files, and batch/job nodes)
 * within the Apache Spark Explorer tree view.
 */
export class DataItem extends vscode.TreeItem {
    /**
     * Creates a new DataItem tree item.
     *
     * @param label Display label for the tree item.
     * @param collapsibleState Collapsed, expanded, or none state.
     * @param contextValue Categorical identifier ('category-pipelines', 'folder', 'pipeline-edit', 'serverless-batch', 'cluster-job', etc.).
     * @param children Optional nested child tree items.
     * @param resourceUri Optional Uri pointing to the `.spark` file.
     */
    constructor(
        public readonly label: string,
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        public readonly contextValue: string,
        public readonly children?: DataItem[],
        public readonly resourceUri?: vscode.Uri
    ) {
        super(label, collapsibleState);

        if (contextValue === 'pipeline-edit') {
            this.command = {
                command: 'vscode.openWith',
                title: 'Open Pipeline',
                arguments: [this.resourceUri, SPARK_EDITOR_VIEW_TYPE]
            };
            this.iconPath = new vscode.ThemeIcon('file');
        } else if (contextValue === 'folder') {
            this.iconPath = new vscode.ThemeIcon('folder');
        }
    }
}

/**
 * Alias for `DataItem` representing a Spark Explorer tree item.
 */
export { DataItem as SparkTreeItem };
