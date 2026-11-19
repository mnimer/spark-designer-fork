/**
 * SparkTreeProvider Unit Tests (R8, R9, R10, R11)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating SparkTreeProvider, Serverless Batch & Dataproc Cluster Job
 * categorization, sorting by createTime / stateStartTime, CLI argument construction,
 * sidebar tree hierarchy generation, and Cloud Console navigation via openJobInConsole.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import * as vscode from 'vscode';

const { mockExecFile, mockGetGcloudProject, mockFindGcloudPath } = vi.hoisted(() => ({
    mockExecFile: vi.fn(),
    mockGetGcloudProject: vi.fn().mockResolvedValue('my-spark-project'),
    mockFindGcloudPath: vi.fn().mockReturnValue('/usr/bin/gcloud')
}));

vi.mock('child_process', () => ({
    execFile: mockExecFile
}));

vi.mock('../../cli/gcloudClient', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../cli/gcloudClient')>();
    return {
        ...actual,
        getGcloudProject: mockGetGcloudProject
    };
});

vi.mock('../../cli/cliResolver', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../cli/cliResolver')>();
    return {
        ...actual,
        findGcloudPath: mockFindGcloudPath
    };
});

// Mock vscode module
vi.mock('vscode', async () => (await import('./helpers/vscodeMock')).createVscodeMock({
    workspace: {
        onDidRenameFiles: vi.fn().mockReturnValue({ dispose: vi.fn() })
    }
}));

import { activate, SPARK_EDITOR_VIEW_TYPE } from '../../extension';
import {
    SparkTreeProvider,
    DataFlowTreeProvider,
    DataItem,
    SparkTreeItem,
    categorizeJobStatus,
    getJobStartTime,
    normalizeServerlessBatch,
    normalizeClusterJob,
    type SparkJob,
    type RawServerlessBatch,
    type RawClusterJob
} from '../../tree/sparkTree';
import { PIPELINE_SEARCH_EXCLUDE_PATTERN } from '../../core/constants';

describe('Job & Batch Status Categorization and Timestamp Sorting (R9 & R10)', () => {
    it('categorizes Serverless Batch states into Running, Cancelled, and Completed (R9)', () => {
        // Running: PENDING, RUNNING, CANCELLING
        expect(categorizeJobStatus('PENDING')).toBe('Running');
        expect(categorizeJobStatus('RUNNING')).toBe('Running');
        expect(categorizeJobStatus('CANCELLING')).toBe('Running');

        // Cancelled: CANCELLED
        expect(categorizeJobStatus('CANCELLED')).toBe('Cancelled');

        // Completed: SUCCEEDED, FAILED
        expect(categorizeJobStatus('SUCCEEDED')).toBe('Completed');
        expect(categorizeJobStatus('FAILED')).toBe('Completed');
    });

    it('categorizes Dataproc Cluster Job states into Running, Cancelled, and Completed (R10)', () => {
        // Running: PENDING, SETUP_DONE, RUNNING, CANCEL_PENDING, CANCEL_STARTED
        expect(categorizeJobStatus('PENDING')).toBe('Running');
        expect(categorizeJobStatus('SETUP_DONE')).toBe('Running');
        expect(categorizeJobStatus('RUNNING')).toBe('Running');
        expect(categorizeJobStatus('CANCEL_PENDING')).toBe('Running');
        expect(categorizeJobStatus('CANCEL_STARTED')).toBe('Running');

        // Cancelled: CANCELLED
        expect(categorizeJobStatus('CANCELLED')).toBe('Cancelled');

        // Completed: DONE, ERROR, ATTEMPT_FAILURE
        expect(categorizeJobStatus('DONE')).toBe('Completed');
        expect(categorizeJobStatus('ERROR')).toBe('Completed');
        expect(categorizeJobStatus('ATTEMPT_FAILURE')).toBe('Completed');
    });

    it('extracts timestamps from createTime, stateStartTime, and statusHistory', () => {
        const batchItem: SparkJob = {
            id: 'batch-1',
            name: 'batch-1',
            state: 'RUNNING',
            createTime: '2026-08-26T10:00:00Z'
        };
        const clusterJobDirect: RawClusterJob = {
            reference: { jobId: 'job-1' },
            status: { state: 'RUNNING', stateStartTime: '2026-08-26T12:00:00Z' }
        };
        const clusterJobFromHistory: RawClusterJob = {
            reference: { jobId: 'job-2' },
            status: { state: 'DONE' },
            statusHistory: [
                { state: 'PENDING', stateStartTime: '2026-08-26T09:00:00Z' },
                { state: 'RUNNING', stateStartTime: '2026-08-26T11:30:00Z' }
            ]
        };
        const jobWithoutTime: SparkJob = {
            id: 'job-3',
            name: 'job-3',
            state: 'CANCELLED'
        };

        expect(getJobStartTime(batchItem)).toBe(new Date('2026-08-26T10:00:00Z').getTime());
        expect(getJobStartTime(clusterJobDirect)).toBe(new Date('2026-08-26T12:00:00Z').getTime());
        expect(getJobStartTime(clusterJobFromHistory)).toBe(new Date('2026-08-26T11:30:00Z').getTime());
        expect(getJobStartTime(jobWithoutTime)).toBe(0);
    });

    it('normalizes raw Serverless Batch items from resource name paths (R9)', () => {
        const raw: RawServerlessBatch = {
            name: 'projects/my-proj/locations/us-east1/batches/etl-batch-2026',
            uuid: 'uuid-1234',
            state: 'SUCCEEDED',
            createTime: '2026-08-26T10:00:00Z',
            stateTime: '2026-08-26T10:15:00Z',
            runtimeConfig: { version: '2.2' }
        };

        const normalized = normalizeServerlessBatch(raw, 'us-central1');
        expect(normalized.id).toBe('etl-batch-2026');
        expect(normalized.name).toBe('etl-batch-2026');
        expect(normalized.region).toBe('us-east1');
        expect(normalized.location).toBe('us-east1');
        expect(normalized.state).toBe('SUCCEEDED');
        expect(normalized.jobType).toBe('serverless-batch');
        expect(normalized.category).toBe('batch');
        expect(normalized.runtimeVersion).toBe('2.2');
    });

    it('normalizes raw Dataproc Cluster Job items with reference and placement (R10)', () => {
        const raw: RawClusterJob = {
            reference: { jobId: 'spark-job-99', projectId: 'my-proj' },
            placement: { clusterName: 'analytics-cluster' },
            status: { state: 'RUNNING', stateStartTime: '2026-08-26T14:00:00Z' }
        };

        const normalized = normalizeClusterJob(raw, 'europe-west1');
        expect(normalized.id).toBe('spark-job-99');
        expect(normalized.name).toBe('spark-job-99');
        expect(normalized.clusterName).toBe('analytics-cluster');
        expect(normalized.state).toBe('RUNNING');
        expect(normalized.stateStartTime).toBe('2026-08-26T14:00:00Z');
        expect(normalized.region).toBe('europe-west1');
        expect(normalized.jobType).toBe('cluster-job');
        expect(normalized.category).toBe('cluster-job');
    });
});

describe('SparkTreeProvider Root Sections & Pipelines (R8)', () => {
    let provider: SparkTreeProvider;

    beforeEach(() => {
        vi.clearAllMocks();
        mockGetGcloudProject.mockResolvedValue('my-spark-project');
        mockFindGcloudPath.mockReturnValue('/usr/bin/gcloud');
        provider = new SparkTreeProvider();
    });

    it('exposes DataFlowTreeProvider and SparkTreeItem aliases for backward compatibility', () => {
        expect(DataFlowTreeProvider).toBe(SparkTreeProvider);
        expect(SparkTreeItem).toBe(DataItem);
    });

    it('returns Pipelines, Serverless Batches, and Dataproc Cluster Jobs as the 3 root sections (R8)', async () => {
        const rootItems = await provider.getChildren();

        expect(rootItems.length).toBe(3);

        expect(rootItems[0].label).toBe('Pipelines');
        expect(rootItems[0].contextValue).toBe('category-pipelines');
        expect(rootItems[0].collapsibleState).toBe(vscode.TreeItemCollapsibleState.Expanded);

        expect(rootItems[1].label).toBe('Serverless Batches');
        expect(rootItems[1].contextValue).toBe('category-serverless-batches');
        expect(rootItems[1].collapsibleState).toBe(vscode.TreeItemCollapsibleState.Collapsed);

        expect(rootItems[2].label).toBe('Dataproc Cluster Jobs');
        expect(rootItems[2].contextValue).toBe('category-cluster-jobs');
        expect(rootItems[2].collapsibleState).toBe(vscode.TreeItemCollapsibleState.Collapsed);
    });

    it('builds nested folder and file nodes for .spark files and wires vscode.openWith + SPARK_EDITOR_VIEW_TYPE (R8)', () => {
        const files = [
            vscode.Uri.file('/workspace/etl/daily.spark'),
            vscode.Uri.file('/workspace/root-pipeline.spark')
        ];

        const tree = provider.buildTree(files);
        expect(tree.length).toBe(2);

        const folderNode = tree[0];
        expect(folderNode.label).toBe('etl');
        expect(folderNode.contextValue).toBe('folder');
        expect(folderNode.children?.length).toBe(1);

        const nestedFileNode = folderNode.children?.[0];
        expect(nestedFileNode?.label).toBe('daily.spark');
        expect(nestedFileNode?.contextValue).toBe('pipeline-edit');
        expect(nestedFileNode?.command).toEqual({
            command: 'vscode.openWith',
            title: 'Open Pipeline',
            arguments: [files[0], SPARK_EDITOR_VIEW_TYPE]
        });

        const rootFileNode = tree[1];
        expect(rootFileNode.label).toBe('root-pipeline.spark');
        expect(rootFileNode.command).toEqual({
            command: 'vscode.openWith',
            title: 'Open Pipeline',
            arguments: [files[1], SPARK_EDITOR_VIEW_TYPE]
        });
    });
});

describe('Serverless Batches Fetching and Grouping (R9)', () => {
    let provider: SparkTreeProvider;

    beforeEach(() => {
        vi.clearAllMocks();
        mockGetGcloudProject.mockResolvedValue('my-spark-project');
        mockFindGcloudPath.mockReturnValue('/usr/bin/gcloud');
        provider = new SparkTreeProvider();
    });

    it('executes gcloud dataproc batches list --region=us-central1 --limit=100 --format=json --project=<PROJECT>', async () => {
        const rawBatches: RawServerlessBatch[] = [
            {
                name: 'projects/my-spark-project/locations/us-central1/batches/batch-succeeded-old',
                uuid: 'u1',
                state: 'SUCCEEDED',
                createTime: '2026-08-26T08:00:00Z'
            },
            {
                name: 'projects/my-spark-project/locations/us-central1/batches/batch-running-new',
                uuid: 'u2',
                state: 'RUNNING',
                createTime: '2026-08-26T12:00:00Z'
            },
            {
                name: 'projects/my-spark-project/locations/us-central1/batches/batch-pending-old',
                uuid: 'u3',
                state: 'PENDING',
                createTime: '2026-08-26T10:00:00Z'
            },
            {
                name: 'projects/my-spark-project/locations/us-central1/batches/batch-cancelling',
                uuid: 'u4',
                state: 'CANCELLING',
                createTime: '2026-08-26T11:00:00Z'
            },
            {
                name: 'projects/my-spark-project/locations/us-central1/batches/batch-cancelled',
                uuid: 'u5',
                state: 'CANCELLED',
                createTime: '2026-08-26T09:00:00Z'
            },
            {
                name: 'projects/my-spark-project/locations/us-central1/batches/batch-failed-new',
                uuid: 'u6',
                state: 'FAILED',
                createTime: '2026-08-26T11:30:00Z'
            }
        ];

        mockExecFile.mockImplementation((_bin: string, _args: string[], _opts: unknown, cb: (err: Error | null, stdout: string, stderr: string) => void) => {
            cb(null, JSON.stringify(rawBatches), '');
        });

        const categoryNode = new DataItem(
            'Serverless Batches',
            vscode.TreeItemCollapsibleState.Collapsed,
            'category-serverless-batches'
        );

        const groups = await provider.getChildren(categoryNode);

        expect(mockExecFile).toHaveBeenCalledTimes(1);
        expect(mockExecFile.mock.calls[0][0]).toBe('/usr/bin/gcloud');
        expect(mockExecFile.mock.calls[0][1]).toEqual([
            'dataproc',
            'batches',
            'list',
            '--region=us-central1',
            '--limit=100',
            '--format=json',
            '--project=my-spark-project'
        ]);

        // Strict order: 1) Running, 2) Cancelled, 3) Completed
        expect(groups.length).toBe(3);
        expect(groups[0].label).toBe('Running (3)');
        expect(groups[0].contextValue).toBe('job-group-running');
        expect(groups[0].children?.map(c => c.label)).toEqual([
            'batch-running-new',
            'batch-cancelling',
            'batch-pending-old'
        ]);

        expect(groups[1].label).toBe('Cancelled (1)');
        expect(groups[1].contextValue).toBe('job-group-cancelled');
        expect(groups[1].children?.map(c => c.label)).toEqual(['batch-cancelled']);

        expect(groups[2].label).toBe('Completed (2)');
        expect(groups[2].contextValue).toBe('job-group-completed');
        expect(groups[2].children?.map(c => c.label)).toEqual([
            'batch-failed-new',
            'batch-succeeded-old'
        ]);

        // Verify R11 command wiring on batch item
        const batchNode = groups[0].children?.[0];
        expect(batchNode?.contextValue).toBe('serverless-batch');
        expect(batchNode?.command).toEqual({
            command: 'spark-pipeline-builder.openJobInConsole',
            title: 'Open Job in Cloud Console',
            arguments: [
                expect.objectContaining({
                    id: 'batch-running-new',
                    region: 'us-central1',
                    jobType: 'serverless-batch',
                    category: 'batch'
                }),
                'my-spark-project'
            ]
        });
    });

    it('returns error node when fetchServerlessBatches fails', async () => {
        vi.spyOn(provider, 'fetchServerlessBatches').mockResolvedValue({
            batches: [],
            region: 'us-central1',
            error: 'GCP authentication failed'
        });

        const categoryNode = new DataItem(
            'Serverless Batches',
            vscode.TreeItemCollapsibleState.Collapsed,
            'category-serverless-batches'
        );

        const children = await provider.getChildren(categoryNode);
        expect(children.length).toBe(1);
        expect(children[0].label).toContain('Error: GCP authentication failed');
        expect(children[0].contextValue).toBe('job-error');
    });

    it('returns empty notice when no Serverless Spark batches are found', async () => {
        vi.spyOn(provider, 'fetchServerlessBatches').mockResolvedValue({
            batches: [],
            region: 'us-central1'
        });

        const categoryNode = new DataItem(
            'Serverless Batches',
            vscode.TreeItemCollapsibleState.Collapsed,
            'category-serverless-batches'
        );

        const children = await provider.getChildren(categoryNode);
        expect(children.length).toBe(1);
        expect(children[0].label).toBe('No Serverless Spark batches found');
        expect(children[0].contextValue).toBe('job-empty');
    });
});

describe('Dataproc Cluster Jobs Fetching and Grouping (R10)', () => {
    let provider: SparkTreeProvider;

    beforeEach(() => {
        vi.clearAllMocks();
        mockGetGcloudProject.mockResolvedValue('my-spark-project');
        mockFindGcloudPath.mockReturnValue('/usr/bin/gcloud');
        provider = new SparkTreeProvider();
    });

    it('executes gcloud dataproc jobs list --region=us-central1 --limit=100 --format=json --project=<PROJECT>', async () => {
        const rawJobs: RawClusterJob[] = [
            {
                reference: { jobId: 'job-done-old' },
                placement: { clusterName: 'cluster-a' },
                status: { state: 'DONE', stateStartTime: '2026-08-26T07:00:00Z' }
            },
            {
                reference: { jobId: 'job-running-new' },
                placement: { clusterName: 'cluster-a' },
                status: { state: 'RUNNING', stateStartTime: '2026-08-26T14:00:00Z' }
            },
            {
                reference: { jobId: 'job-setup-done' },
                placement: { clusterName: 'cluster-b' },
                status: { state: 'SETUP_DONE', stateStartTime: '2026-08-26T13:00:00Z' }
            },
            {
                reference: { jobId: 'job-cancel-pending' },
                placement: { clusterName: 'cluster-b' },
                status: { state: 'CANCEL_PENDING', stateStartTime: '2026-08-26T12:00:00Z' }
            },
            {
                reference: { jobId: 'job-cancelled' },
                placement: { clusterName: 'cluster-a' },
                status: { state: 'CANCELLED', stateStartTime: '2026-08-26T09:00:00Z' }
            },
            {
                reference: { jobId: 'job-error-new' },
                placement: { clusterName: 'cluster-c' },
                status: { state: 'ERROR' },
                statusHistory: [
                    { state: 'RUNNING', stateStartTime: '2026-08-26T10:30:00Z' }
                ]
            }
        ];

        mockExecFile.mockImplementation((_bin: string, _args: string[], _opts: unknown, cb: (err: Error | null, stdout: string, stderr: string) => void) => {
            cb(null, JSON.stringify(rawJobs), '');
        });

        const categoryNode = new DataItem(
            'Dataproc Cluster Jobs',
            vscode.TreeItemCollapsibleState.Collapsed,
            'category-cluster-jobs'
        );

        const groups = await provider.getChildren(categoryNode);

        expect(mockExecFile).toHaveBeenCalledTimes(1);
        expect(mockExecFile.mock.calls[0][0]).toBe('/usr/bin/gcloud');
        expect(mockExecFile.mock.calls[0][1]).toEqual([
            'dataproc',
            'jobs',
            'list',
            '--region=us-central1',
            '--limit=100',
            '--format=json',
            '--project=my-spark-project'
        ]);

        // Strict order: 1) Running, 2) Cancelled, 3) Completed
        expect(groups.length).toBe(3);
        expect(groups[0].label).toBe('Running (3)');
        expect(groups[0].children?.map(c => c.label)).toEqual([
            'job-running-new',
            'job-setup-done',
            'job-cancel-pending'
        ]);

        expect(groups[1].label).toBe('Cancelled (1)');
        expect(groups[1].children?.map(c => c.label)).toEqual(['job-cancelled']);

        expect(groups[2].label).toBe('Completed (2)');
        expect(groups[2].children?.map(c => c.label)).toEqual([
            'job-error-new',
            'job-done-old'
        ]);

        // Verify R11 command wiring on cluster job item
        const jobNode = groups[0].children?.[0];
        expect(jobNode?.contextValue).toBe('cluster-job');
        expect(jobNode?.description).toContain('cluster-a');
        expect(jobNode?.command).toEqual({
            command: 'spark-pipeline-builder.openJobInConsole',
            title: 'Open Job in Cloud Console',
            arguments: [
                expect.objectContaining({
                    id: 'job-running-new',
                    clusterName: 'cluster-a',
                    region: 'us-central1',
                    jobType: 'cluster-job',
                    category: 'cluster-job'
                }),
                'my-spark-project'
            ]
        });
    });

    it('returns error node when fetchClusterJobs fails', async () => {
        vi.spyOn(provider, 'fetchClusterJobs').mockResolvedValue({
            jobs: [],
            region: 'us-central1',
            error: 'Cluster API disabled'
        });

        const categoryNode = new DataItem(
            'Dataproc Cluster Jobs',
            vscode.TreeItemCollapsibleState.Collapsed,
            'category-cluster-jobs'
        );

        const children = await provider.getChildren(categoryNode);
        expect(children.length).toBe(1);
        expect(children[0].label).toContain('Error: Cluster API disabled');
        expect(children[0].contextValue).toBe('job-error');
    });

    it('returns empty notice when no Dataproc cluster jobs are found', async () => {
        vi.spyOn(provider, 'fetchClusterJobs').mockResolvedValue({
            jobs: [],
            region: 'us-central1'
        });

        const categoryNode = new DataItem(
            'Dataproc Cluster Jobs',
            vscode.TreeItemCollapsibleState.Collapsed,
            'category-cluster-jobs'
        );

        const children = await provider.getChildren(categoryNode);
        expect(children.length).toBe(1);
        expect(children[0].label).toBe('No Dataproc cluster jobs found');
        expect(children[0].contextValue).toBe('job-empty');
    });
});

describe('openJobInConsole Command URL Routing (R11)', () => {
    let openJobCallback: ((job: Partial<SparkJob>, project?: string) => Promise<void>) | undefined;

    beforeEach(() => {
        vi.clearAllMocks();
        openJobCallback = undefined;
        vi.mocked(vscode.commands.registerCommand).mockImplementation((command: string, callback: (...args: unknown[]) => unknown) => {
            if (command === 'spark-pipeline-builder.openJobInConsole') {
                openJobCallback = callback as (job: Partial<SparkJob>, project?: string) => Promise<void>;
            }
            return { dispose: vi.fn() };
        });

        const mockContext = {
            subscriptions: []
        } as unknown as vscode.ExtensionContext;
        activate(mockContext);
    });

    it('opens Serverless Batch URL in Google Cloud Console when jobType is serverless-batch', async () => {
        expect(openJobCallback).toBeDefined();
        await openJobCallback?.(
            {
                id: 'batch-abc-123',
                region: 'us-east1',
                jobType: 'serverless-batch'
            },
            'my-gcp-project'
        );

        expect(vscode.env.openExternal).toHaveBeenCalledTimes(1);
        const parsedUri = vi.mocked(vscode.Uri.parse).mock.calls[0][0];
        expect(parsedUri).toBe(
            'https://console.cloud.google.com/dataproc/batches/us-east1/batch-abc-123/monitoring?project=my-gcp-project'
        );
    });

    it('opens Dataproc Cluster Job URL in Google Cloud Console when jobType is cluster-job', async () => {
        expect(openJobCallback).toBeDefined();
        await openJobCallback?.(
            {
                id: 'job-xyz-789',
                region: 'europe-west1',
                jobType: 'cluster-job'
            },
            'my-gcp-project'
        );

        expect(vscode.env.openExternal).toHaveBeenCalledTimes(1);
        const parsedUri = vi.mocked(vscode.Uri.parse).mock.calls[0][0];
        expect(parsedUri).toBe(
            'https://console.cloud.google.com/dataproc/jobs/job-xyz-789?region=europe-west1&project=my-gcp-project'
        );
    });

    it('opens Dataproc Cluster Job URL when category === cluster-job and resolves project via getGcloudProject when omitted', async () => {
        mockGetGcloudProject.mockResolvedValue('resolved-proj-42');
        expect(openJobCallback).toBeDefined();
        await openJobCallback?.({
            id: 'job-999',
            location: 'us-west1',
            category: 'cluster-job'
        });

        expect(vscode.env.openExternal).toHaveBeenCalledTimes(1);
        const parsedUri = vi.mocked(vscode.Uri.parse).mock.calls[0][0];
        expect(parsedUri).toBe(
            'https://console.cloud.google.com/dataproc/jobs/job-999?region=us-west1&project=resolved-proj-42'
        );
    });
});

/** Signature of the mocked execFile callback. */
type ExecCallback = (err: (Error & { killed?: boolean }) | null, stdout: string, stderr: string) => void;

/**
 * Makes the mocked execFile answer every call with one result.
 *
 * @param err Error passed to the callback, or null.
 * @param stdout Standard output text.
 * @param stderr Standard error text.
 */
function answerExecFile(err: (Error & { killed?: boolean }) | null, stdout = '', stderr = ''): void {
    mockExecFile.mockImplementation((_bin: string, _args: string[], _opts: unknown, cb: ExecCallback) => {
        cb(err, stdout, stderr);
    });
}

/**
 * Returns the theme icon id of a tree item.
 *
 * @param item Tree item.
 * @returns Icon id, or undefined.
 */
function iconId(item: DataItem | undefined): string | undefined {
    return (item?.iconPath as { id?: string } | undefined)?.id;
}

describe('SparkTreeProvider boundary and failure cases (T5)', () => {
    let provider: SparkTreeProvider;

    beforeEach(() => {
        vi.clearAllMocks();
        mockGetGcloudProject.mockResolvedValue('my-spark-project');
        mockFindGcloudPath.mockReturnValue('/usr/bin/gcloud');
        vi.mocked(vscode.workspace.getConfiguration).mockReturnValue({
            get: vi.fn(),
            has: vi.fn(),
            inspect: vi.fn(),
            update: vi.fn()
        } as unknown as vscode.WorkspaceConfiguration);
        provider = new SparkTreeProvider();
    });

    describe('categorizeJobStatus', () => {
        it.each([
            ['', 'Completed'],
            ['UNKNOWN', 'Completed'],
            ['running', 'Running'],
            ['  pending  ', 'Running'],
            ['STATE_QUEUED', 'Running'],
            ['RESOURCE_CLEANING_UP', 'Running'],
            ['CANCELED', 'Cancelled'],
            ['JOB_STATE_DRAINING', 'Cancelled'],
            ['STOPPED', 'Cancelled'],
            ['ATTEMPT_FAILURE', 'Completed'],
            ['done', 'Completed']
        ])('maps state %j to %s', (state, expected) => {
            expect(categorizeJobStatus(state)).toBe(expected);
        });

        it('treats a missing state as Completed', () => {
            expect(categorizeJobStatus(undefined as unknown as string)).toBe('Completed');
        });
    });

    describe('getJobStartTime', () => {
        it('returns 0 when no timestamp is present or every timestamp is invalid', () => {
            expect(getJobStartTime({} as RawServerlessBatch)).toBe(0);
            expect(getJobStartTime({ createTime: 'not-a-date' } as RawServerlessBatch)).toBe(0);
        });

        it('skips an invalid stateStartTime and uses the latest valid statusHistory entry', () => {
            const job = {
                status: { stateStartTime: 'garbage' },
                statusHistory: [
                    { stateStartTime: '2026-01-01T00:00:00Z' },
                    null,
                    { stateStartTime: 'bad' },
                    { stateStartTime: '2026-03-01T00:00:00Z' }
                ]
            } as unknown as RawClusterJob;
            expect(getJobStartTime(job)).toBe(Date.parse('2026-03-01T00:00:00Z'));
        });

        it('falls back from createTime to startTime, creationTime, and stateTime in order', () => {
            expect(getJobStartTime({ stateTime: '2026-05-01T00:00:00Z' } as RawServerlessBatch)).toBe(Date.parse('2026-05-01T00:00:00Z'));
            expect(
                getJobStartTime({ creationTime: '2026-04-01T00:00:00Z', stateTime: '2026-05-01T00:00:00Z' } as RawServerlessBatch)
            ).toBe(Date.parse('2026-04-01T00:00:00Z'));
        });
    });

    describe('normalizeServerlessBatch', () => {
        it('uses safe defaults for an empty batch', () => {
            expect(normalizeServerlessBatch({} as RawServerlessBatch)).toMatchObject({
                id: '',
                name: 'unknown-batch',
                state: 'UNKNOWN',
                region: 'us-central1',
                location: 'us-central1',
                jobType: 'serverless-batch',
                category: 'batch',
                type: 'Serverless Batch'
            });
        });

        it('uses the last name segment, then the uuid, as the batch ID', () => {
            expect(normalizeServerlessBatch({ name: 'some/path/batch-x' } as RawServerlessBatch).id).toBe('batch-x');
            expect(normalizeServerlessBatch({ uuid: 'uuid-1' } as RawServerlessBatch).id).toBe('uuid-1');
        });

        it('prefers the resource name region over the location field and the default region', () => {
            const batch = normalizeServerlessBatch(
                { name: 'projects/p/locations/asia-east1/batches/b-1', location: 'europe-west1' } as RawServerlessBatch,
                'us-west1'
            );
            expect(batch.region).toBe('asia-east1');
            expect(normalizeServerlessBatch({ id: 'b-2', location: 'europe-west1' } as RawServerlessBatch, 'us-west1').region).toBe('europe-west1');
        });

        it('reads the runtime version from runtimeConfig', () => {
            expect(normalizeServerlessBatch({ id: 'b', runtimeConfig: { version: '2.3' } } as unknown as RawServerlessBatch).runtimeVersion).toBe('2.3');
        });
    });

    describe('normalizeClusterJob', () => {
        it('uses safe defaults for an empty job', () => {
            expect(normalizeClusterJob({} as RawClusterJob, 'us-east4')).toMatchObject({
                id: 'unknown-job',
                name: 'unknown-job',
                state: 'UNKNOWN',
                region: 'us-east4',
                jobType: 'cluster-job',
                category: 'cluster-job',
                type: 'Cluster Job'
            });
        });

        it('takes the latest statusHistory start time when status has none', () => {
            const job = normalizeClusterJob({
                reference: { jobId: 'j-1' },
                statusHistory: [
                    { state: 'PENDING', stateStartTime: '2026-01-01T00:00:00Z' },
                    { state: 'RUNNING', stateStartTime: '2026-01-02T00:00:00Z' }
                ]
            } as RawClusterJob);
            expect(job.stateStartTime).toBe('2026-01-02T00:00:00Z');
            expect(job.startTime).toBe('2026-01-02T00:00:00Z');
        });

        it('uses the raw name as display name only when no reference jobId exists', () => {
            expect(normalizeClusterJob({ name: 'display', id: 'id-1' } as unknown as RawClusterJob).name).toBe('display');
            expect(normalizeClusterJob({ name: 'display', reference: { jobId: 'ref-1' } } as unknown as RawClusterJob).name).toBe('ref-1');
        });
    });

    describe('buildTree and pipeline scanning', () => {
        it('returns no nodes when no workspace folder is open', () => {
            const original = vscode.workspace.workspaceFolders;
            Object.defineProperty(vscode.workspace, 'workspaceFolders', { value: undefined, configurable: true, writable: true });
            try {
                expect(provider.buildTree([vscode.Uri.file('/workspace/a.spark')])).toEqual([]);
            } finally {
                Object.defineProperty(vscode.workspace, 'workspaceFolders', { value: original, configurable: true, writable: true });
            }
        });

        it('returns no nodes for an empty file list', () => {
            expect(provider.buildTree([])).toEqual([]);
        });

        it('merges files that share a folder and nests deep folders', () => {
            const tree = provider.buildTree([
                vscode.Uri.file('/workspace/etl/a.spark'),
                vscode.Uri.file('/workspace/etl/b.spark'),
                vscode.Uri.file('/workspace/etl/nightly/c.spark')
            ]);
            expect(tree).toHaveLength(1);
            expect(tree[0].label).toBe('etl');
            expect(iconId(tree[0])).toBe('folder');
            expect(tree[0].children?.map((c) => c.label)).toEqual(['a.spark', 'b.spark', 'nightly']);
            const nightly = tree[0].children?.[2];
            expect(nightly?.children?.map((c) => c.label)).toEqual(['c.spark']);
            expect(iconId(nightly?.children?.[0])).toBe('file');
        });

        it('scans .spark files with the exclude pattern and fires a tree change', async () => {
            vi.mocked(vscode.workspace.findFiles).mockResolvedValue([vscode.Uri.file('/workspace/x.spark')] as never);
            const scanned = new SparkTreeProvider();
            await vi.waitFor(() => expect(vscode.workspace.findFiles).toHaveBeenCalledWith('**/*.spark', PIPELINE_SEARCH_EXCLUDE_PATTERN));
            const emitter = (scanned as unknown as { _onDidChangeTreeData: { fire: ReturnType<typeof vi.fn> } })._onDidChangeTreeData;
            await vi.waitFor(() => expect(emitter.fire).toHaveBeenCalled());
            const [pipelines] = await scanned.getChildren();
            expect(pipelines.children?.map((c) => c.label)).toEqual(['x.spark']);
            vi.mocked(vscode.workspace.findFiles).mockResolvedValue([] as never);
        });
    });

    describe('getChildren routing', () => {
        it('shows a placeholder under Pipelines when no pipeline exists', async () => {
            const [pipelines] = await provider.getChildren();
            expect(pipelines.children).toHaveLength(1);
            expect(pipelines.children?.[0].label).toBe('No pipelines found');
            expect(pipelines.children?.[0].contextValue).toBe('empty');
        });

        it('assigns valid lowercase codicon ids to the three root sections', async () => {
            const roots = await provider.getChildren();
            expect(roots.map(iconId)).toEqual(['repo', 'flame', 'server-process']);
        });

        it('routes the legacy category-running-jobs node to the Serverless batch query', async () => {
            const spy = vi.spyOn(provider, 'fetchServerlessBatches').mockResolvedValue({ batches: [], region: 'us-central1' });
            const children = await provider.getChildren(new DataItem('Running Jobs', vscode.TreeItemCollapsibleState.Collapsed, 'category-running-jobs'));
            expect(spy).toHaveBeenCalledTimes(1);
            expect(children[0].contextValue).toBe('job-empty');
        });

        it('returns the stored children of folder, job group, and Pipelines-labelled nodes', async () => {
            const child = new DataItem('c', vscode.TreeItemCollapsibleState.None, 'pipeline-edit');
            for (const contextValue of ['folder', 'job-group-running', 'job-group-cancelled', 'job-group-completed']) {
                expect(await provider.getChildren(new DataItem('x', vscode.TreeItemCollapsibleState.Collapsed, contextValue, [child]))).toEqual([child]);
                expect(await provider.getChildren(new DataItem('x', vscode.TreeItemCollapsibleState.Collapsed, contextValue))).toEqual([]);
            }
            expect(await provider.getChildren(new DataItem('Pipelines', vscode.TreeItemCollapsibleState.Collapsed, 'other', [child]))).toEqual([child]);
        });

        it('returns no children for leaf and unknown nodes', async () => {
            expect(await provider.getChildren(new DataItem('leaf', vscode.TreeItemCollapsibleState.None, 'serverless-batch'))).toEqual([]);
            expect(await provider.getChildren(new DataItem('?', vscode.TreeItemCollapsibleState.None, 'mystery'))).toEqual([]);
        });

        it('returns the same element from getTreeItem', () => {
            const item = new DataItem('x', vscode.TreeItemCollapsibleState.None, 'empty');
            expect(provider.getTreeItem(item)).toBe(item);
        });
    });

    describe('fetchServerlessBatches and fetchClusterJobs', () => {
        it('passes a 15 second timeout and a 10 MB output buffer to gcloud', async () => {
            answerExecFile(null, '[]');
            await provider.fetchServerlessBatches();
            expect(mockExecFile.mock.calls[0][2]).toEqual({ timeout: 15000, maxBuffer: 10 * 1024 * 1024 });
        });

        it('omits --project when the project is the placeholder or gcloud has no default', async () => {
            answerExecFile(null, '[]');
            await provider.fetchServerlessBatches('YOUR_PROJECT_ID');
            expect(mockExecFile.mock.calls[0][1]).not.toContainEqual(expect.stringMatching(/^--project=/));

            mockGetGcloudProject.mockResolvedValue(undefined);
            const result = await provider.fetchClusterJobs();
            expect(mockExecFile.mock.calls[1][1]).not.toContainEqual(expect.stringMatching(/^--project=/));
            expect(result.projectId).toBeUndefined();
        });

        it('uses a valid region override, then the configured region, then the constructor default', async () => {
            answerExecFile(null, '[]');
            expect((await provider.fetchServerlessBatches('p', 'europe-west4')).region).toBe('europe-west4');

            vi.mocked(vscode.workspace.getConfiguration).mockReturnValue({
                get: vi.fn(() => 'asia-south1')
            } as unknown as vscode.WorkspaceConfiguration);
            expect((await provider.fetchServerlessBatches('p', 'bad region;rm')).region).toBe('asia-south1');

            vi.mocked(vscode.workspace.getConfiguration).mockReturnValue({
                get: vi.fn(() => '../etc')
            } as unknown as vscode.WorkspaceConfiguration);
            expect((await new SparkTreeProvider('me-west1').fetchClusterJobs('p')).region).toBe('me-west1');
            expect((await new SparkTreeProvider('not a region').fetchClusterJobs('p')).region).toBe('us-central1');
        });

        it('reports a timeout message when gcloud is killed', async () => {
            answerExecFile(Object.assign(new Error('killed'), { killed: true }), '', 'partial');
            expect((await provider.fetchServerlessBatches('p')).error).toBe('Request timed out waiting for gcloud CLI response.');
            expect((await provider.fetchClusterJobs('p')).error).toBe('Request timed out waiting for gcloud CLI response.');
        });

        it('prefers stderr over the error message and falls back to the message', async () => {
            answerExecFile(new Error('exit 1'), '', 'ERROR: PERMISSION_DENIED');
            expect((await provider.fetchServerlessBatches('p')).error).toBe('ERROR: PERMISSION_DENIED');
            answerExecFile(new Error('exit 2'), '', '');
            expect((await provider.fetchClusterJobs('p')).error).toBe('exit 2');
        });

        it('reports a parse error for malformed JSON output', async () => {
            answerExecFile(null, '{not json');
            const batches = await provider.fetchServerlessBatches('p');
            expect(batches.batches).toEqual([]);
            expect(batches.error).toMatch(/^Failed to parse gcloud output: /);
            const jobs = await provider.fetchClusterJobs('p');
            expect(jobs.jobs).toEqual([]);
            expect(jobs.error).toMatch(/^Failed to parse gcloud output: /);
        });

        it('accepts wrapped object output and treats empty or unknown shapes as no items', async () => {
            answerExecFile(null, JSON.stringify({ batches: [{ id: 'b-1', state: 'RUNNING' }] }));
            expect((await provider.fetchServerlessBatches('p')).batches.map((b) => b.id)).toEqual(['b-1']);

            answerExecFile(null, JSON.stringify({ jobs: [{ reference: { jobId: 'j-1' } }] }));
            expect((await provider.fetchClusterJobs('p')).jobs.map((j) => j.id)).toEqual(['j-1']);

            answerExecFile(null, '');
            expect((await provider.fetchServerlessBatches('p')).batches).toEqual([]);
            answerExecFile(null, JSON.stringify({ unexpected: true }));
            expect((await provider.fetchClusterJobs('p')).jobs).toEqual([]);
        });

        it('maps fetchJobs onto the Serverless batch result', async () => {
            answerExecFile(null, JSON.stringify([{ id: 'b-9', state: 'DONE' }]));
            const result = await provider.fetchJobs('p');
            expect(result).toEqual({ jobs: [expect.objectContaining({ id: 'b-9' })], projectId: 'p', error: undefined });
        });
    });

    describe('grouped job items', () => {
        it('returns three empty groups, with Running collapsed, for no jobs', () => {
            const groups = provider.buildBatchGroups([]);
            expect(groups.map((g) => g.label)).toEqual(['Running (0)', 'Cancelled (0)', 'Completed (0)']);
            expect(groups.map((g) => g.collapsibleState)).toEqual([
                vscode.TreeItemCollapsibleState.Collapsed,
                vscode.TreeItemCollapsibleState.Collapsed,
                vscode.TreeItemCollapsibleState.Collapsed
            ]);
            expect(groups.map(iconId)).toEqual(['run', 'stop-circle', 'check-all']);
        });

        it.each([
            ['RUNNING', 'play-circle'],
            ['SETUP_DONE', 'play-circle'],
            ['PENDING', 'clock'],
            ['CANCEL_STARTED', 'clock'],
            ['CANCELLED', 'circle-slash'],
            ['FAILED', 'error'],
            ['ERROR', 'error'],
            ['ATTEMPT_FAILURE', 'error'],
            ['DONE', 'check'],
            ['SUCCEEDED', 'check']
        ])('assigns the %s state the %s icon', (state, icon) => {
            const groups = provider.buildClusterJobGroups([{ reference: { jobId: 'j-1' }, status: { state } } as RawClusterJob]);
            const item = groups.flatMap((g) => g.children ?? [])[0];
            expect(iconId(item)).toBe(icon);
        });

        it('adds the cluster name to the description and tooltip of a cluster job', () => {
            const groups = provider.buildClusterJobGroups([
                { reference: { jobId: 'j-1' }, placement: { clusterName: 'c-1' }, status: { state: 'DONE' } } as RawClusterJob
            ]);
            const item = groups[2].children?.[0];
            expect(item?.description).toBe('c-1 • DONE');
            expect((item?.tooltip as { value: string }).value).toContain('- **Cluster:** `c-1`');
            expect((item?.tooltip as { value: string }).value).toContain('- **Timestamp:** -');
        });

        it('adds the runtime version to the tooltip of a Serverless batch', () => {
            const groups = provider.buildBatchGroups([{ id: 'b-1', state: 'SUCCEEDED', runtimeVersion: '2.3' } as RawServerlessBatch]);
            const tooltip = (groups[2].children?.[0].tooltip as { value: string }).value;
            expect(tooltip).toContain('- **Runtime Version:** 2.3');
            expect(tooltip).toContain('- **Type:** Serverless Batch');
        });

        it('keeps each job type in buildJobGroups and passes the project to the console command', () => {
            const groups = provider.buildJobGroups(
                [
                    { id: 'b-1', name: 'b-1', state: 'RUNNING', jobType: 'serverless-batch' } as SparkJob,
                    { id: 'j-1', name: 'j-1', state: 'RUNNING', category: 'cluster-job', region: 'us-east1' } as SparkJob
                ],
                'proj-x'
            );
            const items = groups[0].children ?? [];
            expect(items.map((i) => i.contextValue).sort()).toEqual(['cluster-job', 'serverless-batch']);
            const cluster = items.find((i) => i.contextValue === 'cluster-job');
            expect(cluster?.command?.arguments).toEqual([
                expect.objectContaining({ id: 'j-1', region: 'us-east1', jobType: 'cluster-job', category: 'cluster-job' }),
                'proj-x'
            ]);
        });
    });
});
