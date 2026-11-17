/**
 * @fileoverview Unit tests for the PipelineRunner helpers extracted from handleRunPipeline.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import * as path from 'path';

const execFileCalls = vi.hoisted(() => ({
    calls: [] as Array<{ bin: string; args: string[]; opts: any }>,
    nextError: null as Error | null,
    nextStdout: '',
    nextStderr: '',
}));

vi.mock('child_process', async (importOriginal) => {
    const actual = await importOriginal<typeof import('child_process')>();
    return {
        ...actual,
        execFile: vi.fn((bin: string, args: string[], opts: any, cb: any) => {
            const callback = typeof opts === 'function' ? opts : cb;
            execFileCalls.calls.push({ bin, args, opts });
            callback(execFileCalls.nextError, execFileCalls.nextStdout, execFileCalls.nextStderr);
        }),
    };
});

// gcloudClient and cliResolver pull in the vscode API; the tests inject their own lookups.
vi.mock('../../cli/gcloudClient', () => ({ getGcloudProject: vi.fn() }));
vi.mock('../../cli/cliResolver', () => ({ findGcloudPath: vi.fn(() => '/usr/bin/gcloud') }));

import {
    DEFAULT_RUN_REGION,
    PLACEHOLDER_PROJECT_ID,
    buildRunnerEnvironment,
    cancelRemoteSparkJob,
    createJobIdTracker,
    describeRunner,
    findRunnerScript,
    formatBatchTtl,
    resolveRunTarget,
    runnerScriptCandidates,
} from '../../execution/PipelineRunner';

describe('runnerScriptCandidates', () => {
    it('lists scripts per runner in priority order', () => {
        expect(runnerScriptCandidates('Local')).toEqual(['run_local.sh', 'run_directrunner.sh']);
        expect(runnerScriptCandidates('DirectRunner')).toEqual(['run_local.sh', 'run_directrunner.sh']);
        expect(runnerScriptCandidates('DataprocCluster')).toEqual(['run_dataproc_cluster.sh']);
        expect(runnerScriptCandidates('ServerlessSpark')).toEqual([
            'run_serverless_spark.sh',
            'run_sparkrunner.sh',
            'run_spark.sh',
        ]);
        expect(runnerScriptCandidates('Spark')).toEqual([
            'run_serverless_spark.sh',
            'run_sparkrunner.sh',
            'run_spark.sh',
        ]);
        expect(runnerScriptCandidates('Dataflow')).toEqual(['run_dataflowrunner.sh', 'run_dataflow.sh']);
        expect(runnerScriptCandidates('Flink')).toEqual([]);
    });
});

describe('findRunnerScript', () => {
    it('returns the first existing candidate', () => {
        const existing = new Set([path.join('/p', 'run_dataflow.sh')]);
        expect(findRunnerScript('/p', runnerScriptCandidates('Dataflow'), (p) => existing.has(p))).toBe('run_dataflow.sh');
    });

    it('prefers the earlier candidate when both exist', () => {
        expect(findRunnerScript('/p', runnerScriptCandidates('Local'), () => true)).toBe('run_local.sh');
        expect(findRunnerScript('/p', runnerScriptCandidates('ServerlessSpark'), () => true)).toBe('run_serverless_spark.sh');
        expect(findRunnerScript('/p', runnerScriptCandidates('Dataflow'), () => true)).toBe('run_dataflowrunner.sh');
    });

    it('returns null when no candidate exists', () => {
        expect(findRunnerScript('/p', runnerScriptCandidates('ServerlessSpark'), () => false)).toBeNull();
    });
});

describe('describeRunner', () => {
    it('describes each runner', () => {
        expect(describeRunner('Local')).toContain('Local PySpark / Spark Connect');
        expect(describeRunner('DirectRunner')).toContain('Direct Runner');
        expect(describeRunner('DataprocCluster')).toContain('Dataproc Cluster');
        expect(describeRunner('ServerlessSpark')).toContain('Serverless Spark');
        expect(describeRunner('Spark')).toContain('Spark');
        expect(describeRunner('Dataflow')).toContain('Dataflow');
    });
});

describe('formatBatchTtl', () => {
    it('converts numeric minutes to seconds and preserves duration strings', () => {
        expect(formatBatchTtl(60)).toBe('3600s');
        expect(formatBatchTtl('240')).toBe('14400s');
        expect(formatBatchTtl('14400s')).toBe('14400s');
        expect(formatBatchTtl('4h')).toBe('4h');
        expect(formatBatchTtl(0)).toBeUndefined();
        expect(formatBatchTtl('')).toBeUndefined();
        expect(formatBatchTtl(undefined)).toBeUndefined();
    });
});

describe('buildRunnerEnvironment', () => {
    it('prepends existing tool dirs to PATH and forces unbuffered, uncolored output', () => {
        const { env } = buildRunnerEnvironment({
            baseEnv: { HOME: '/home/u', PATH: ['/usr/bin', '/custom'].join(path.delimiter) },
            exists: (d) => d === '/opt/homebrew/bin' || d === '/usr/bin',
        });
        expect(env.PATH.split(path.delimiter)).toEqual(['/opt/homebrew/bin', '/usr/bin', '/custom']);
        expect(env.PYTHONUNBUFFERED).toBe('1');
        expect(env.FORCE_COLOR).toBe('0');
        expect(env.HOME).toBe('/home/u');
    });

    it('applies safe runtime variables and reports rejected keys', () => {
        const { env, rejectedKeys } = buildRunnerEnvironment({
            baseEnv: {},
            exists: () => false,
            runtimeVariables: [
                { key: 'MY_VAR', value: 'x' },
                { key: 'LD_PRELOAD', value: 'evil.so' },
                { key: 'PYSPARK_PYTHON', value: '/tmp/evil-py' },
            ],
        });
        expect(env.MY_VAR).toBe('x');
        expect(env.LD_PRELOAD).toBeUndefined();
        expect(env.PYSPARK_PYTHON).toBeUndefined();
        expect(rejectedKeys).toEqual(['LD_PRELOAD', 'PYSPARK_PYTHON']);
    });

    it('exports staging path and subnetwork from the job config', () => {
        const { env } = buildRunnerEnvironment({
            baseEnv: {},
            exists: () => false,
            jobConfig: { gcsStagingPath: 'gs://b/stage', subnetwork: 'sub-1' },
        });
        expect(env.GCS_STAGING_PATH).toBe('gs://b/stage');
        expect(env.DEPS_BUCKET).toBe('gs://b/stage');
        expect(env.SUBNET).toBe('sub-1');
        expect(env.SUBNETWORK).toBe('sub-1');
    });

    it('exports all Spark 4.x runner environment variables from jobConfig', () => {
        const { env } = buildRunnerEnvironment({
            baseEnv: {},
            exists: () => false,
            jobConfig: {
                runner: 'ServerlessSpark',
                projectId: 'spark-prod-123',
                region: 'us-east1',
                depsBucket: 'gs://spark-deps-bucket',
                checkpointRoot: 'gs://spark-checkpoints/job1',
                clusterName: 'analytics-cluster-01',
                runtimeVersion: '4.0',
                tier: 'standard',
                subnetwork: 'regions/us-east1/subnetworks/private-spark',
                serviceAccount: 'spark-sa@spark-prod-123.iam.gserviceaccount.com',
                batchTtlMinutes: 120,
                customJarUris: 'gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar',
                useSparkConnect: true,
                sessionTemplate: 'projects/spark-prod-123/locations/us-east1/sessionTemplates/interactive',
            },
        });
        expect(env.PROJECT_ID).toBe('spark-prod-123');
        expect(env.REGION).toBe('us-east1');
        expect(env.DEPS_BUCKET).toBe('gs://spark-deps-bucket');
        expect(env.GCS_STAGING_PATH).toBe('gs://spark-deps-bucket');
        expect(env.CHECKPOINT_ROOT).toBe('gs://spark-checkpoints/job1');
        expect(env.CLUSTER_NAME).toBe('analytics-cluster-01');
        expect(env.RUNTIME_VERSION).toBe('4.0');
        expect(env.SERVERLESS_TIER).toBe('standard');
        expect(env.SUBNET).toBe('regions/us-east1/subnetworks/private-spark');
        expect(env.SUBNETWORK).toBe('regions/us-east1/subnetworks/private-spark');
        expect(env.SERVICE_ACCOUNT).toBe('spark-sa@spark-prod-123.iam.gserviceaccount.com');
        expect(env.BATCH_TTL).toBe('7200s');
        expect(env.SPARK_JARS).toBe('gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar');
        expect(env.USE_SPARK_CONNECT).toBe('true');
        expect(env.SPARK_CONNECT_SESSION_TEMPLATE).toBe(
            'projects/spark-prod-123/locations/us-east1/sessionTemplates/interactive'
        );
    });

    it('does not export PROJECT_ID when projectId is the placeholder', () => {
        const { env } = buildRunnerEnvironment({
            baseEnv: {},
            exists: () => false,
            jobConfig: { projectId: PLACEHOLDER_PROJECT_ID },
        });
        expect(env.PROJECT_ID).toBeUndefined();
    });
});

describe('resolveRunTarget', () => {
    it('keeps an explicit projectId without calling gcloud', async () => {
        let called = false;
        const target = await resolveRunTarget({ projectId: 'explicit', region: 'europe-west1' }, async () => {
            called = true;
            return 'gcloud-default';
        });
        expect(target).toEqual({ project: 'explicit', region: 'europe-west1' });
        expect(called).toBe(false);
    });

    it('falls back to the gcloud default project and default region', async () => {
        expect(await resolveRunTarget(undefined, async () => 'gcloud-default'))
            .toEqual({ project: 'gcloud-default', region: DEFAULT_RUN_REGION });
    });

    it('keeps the placeholder when gcloud fails or has no default', async () => {
        expect((await resolveRunTarget({}, async () => { throw new Error('no gcloud'); })).project).toBe(PLACEHOLDER_PROJECT_ID);
        expect((await resolveRunTarget({}, async () => undefined)).project).toBe(PLACEHOLDER_PROJECT_ID);
    });
});

describe('createJobIdTracker', () => {
    const target = { project: 'p1', region: 'r1' };

    it('reports a Dataflow job once, using the default target', () => {
        const track = createJobIdTracker(target);
        expect(track('noise')).toBeNull();
        expect(track('Submitted job: 2026-01-01_00_00_00-1234567890123\n')).toEqual({
            jobId: '2026-01-01_00_00_00-1234567890123', runner: 'Dataflow', projectId: 'p1', region: 'r1',
        });
        expect(track('Submitted job: 2026-01-02_00_00_00-9999999999999\n')).toBeNull();
    });

    it('prefers project/region parsed from a Console URL', () => {
        const job = createJobIdTracker(target)(
            'https://console.cloud.google.com/dataflow/jobs/europe-west4/2026-09-30_02_00_00-1234567890123456?project=url-proj'
        );
        expect(job).toMatchObject({ runner: 'Dataflow', projectId: 'url-proj', region: 'europe-west4' });
    });

    it('reports a ServerlessSpark batch ID but ignores status words', () => {
        const track = createJobIdTracker(target);
        expect(track('Batch [running]')).toBeNull();
        expect(track('Batch ID: batch-abc123')).toEqual({
            jobId: 'batch-abc123',
            runner: 'ServerlessSpark',
            projectId: 'p1',
            region: 'r1',
        });
    });

    it('reports a DataprocCluster job ID from gcloud output and Console URLs', () => {
        const track = createJobIdTracker(target);
        expect(track('Job [cluster-job-456] submitted.')).toEqual({
            jobId: 'cluster-job-456',
            runner: 'DataprocCluster',
            projectId: 'p1',
            region: 'r1',
        });

        const urlJob = createJobIdTracker(target)(
            'https://console.cloud.google.com/dataproc/jobs/job-999?region=us-west1&project=cluster-proj'
        );
        expect(urlJob).toEqual({
            jobId: 'job-999',
            runner: 'DataprocCluster',
            projectId: 'cluster-proj',
            region: 'us-west1',
        });
    });
});

describe('cancelRemoteSparkJob', () => {
    beforeEach(() => {
        execFileCalls.calls.length = 0;
        execFileCalls.nextError = null;
        execFileCalls.nextStdout = 'Cancelled.';
        execFileCalls.nextStderr = '';
    });

    it('cancels a ServerlessSpark batch via gcloud dataproc batches cancel', async () => {
        const result = await cancelRemoteSparkJob({
            runner: 'ServerlessSpark',
            jobId: 'batch-12345',
            projectId: 'my-spark-proj',
            region: 'us-central1',
            gcloudBin: '/opt/gcloud',
        });

        expect(result.cancelled).toBe(true);
        expect(execFileCalls.calls).toHaveLength(1);
        expect(execFileCalls.calls[0].bin).toBe('/opt/gcloud');
        expect(execFileCalls.calls[0].args).toEqual([
            'dataproc',
            'batches',
            'cancel',
            'batch-12345',
            '--region=us-central1',
            '--project=my-spark-proj',
            '--quiet',
        ]);
    });

    it('kills a DataprocCluster job via gcloud dataproc jobs kill', async () => {
        const result = await cancelRemoteSparkJob({
            runner: 'DataprocCluster',
            jobId: 'job-abcdef12',
            projectId: 'my-cluster-proj',
            region: 'europe-west1',
            gcloudBin: '/opt/gcloud',
        });

        expect(result.cancelled).toBe(true);
        expect(execFileCalls.calls).toHaveLength(1);
        expect(execFileCalls.calls[0].args).toEqual([
            'dataproc',
            'jobs',
            'kill',
            'job-abcdef12',
            '--region=europe-west1',
            '--project=my-cluster-proj',
            '--quiet',
        ]);
    });

    it('rejects unsafe job IDs or unsupported runners without invoking gcloud', async () => {
        const badId = await cancelRemoteSparkJob({
            runner: 'ServerlessSpark',
            jobId: 'bad;rm -rf /',
            projectId: 'my-proj',
        });
        expect(badId.cancelled).toBe(false);
        expect(execFileCalls.calls).toHaveLength(0);

        const localRunner = await cancelRemoteSparkJob({
            runner: 'Local',
            jobId: 'job-1234',
            projectId: 'my-proj',
        });
        expect(localRunner.cancelled).toBe(false);
        expect(execFileCalls.calls).toHaveLength(0);
    });

    it('reports errors when gcloud exits with a non-zero code', async () => {
        execFileCalls.nextError = new Error('Command failed');
        execFileCalls.nextStderr = 'ERROR: (gcloud.dataproc.batches.cancel) NOT_FOUND';

        const result = await cancelRemoteSparkJob({
            runner: 'ServerlessSpark',
            jobId: 'batch-missing',
            projectId: 'my-proj',
            region: 'us-central1',
        });
        expect(result.cancelled).toBe(false);
        expect(result.error).toContain('NOT_FOUND');
    });
});
