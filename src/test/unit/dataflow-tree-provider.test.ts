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

import {
    activate,
    SparkTreeProvider,
    DataFlowTreeProvider,
    DataItem,
    SparkTreeItem,
    categorizeJobStatus,
    getJobStartTime,
    normalizeServerlessBatch,
    normalizeClusterJob,
    SPARK_EDITOR_VIEW_TYPE,
    type SparkJob,
    type RawServerlessBatch,
    type RawClusterJob
} from '../../extension';

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
