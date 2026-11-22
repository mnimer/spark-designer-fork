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

/** Minimal fake child process recorded by the spawn mock. */
interface FakeChild {
    stdout: { emit: (event: string, ...args: unknown[]) => boolean };
    stderr: { emit: (event: string, ...args: unknown[]) => boolean };
    emit: (event: string, ...args: unknown[]) => boolean;
}

const spawnCalls = vi.hoisted(() => ({
    calls: [] as Array<{ cmd: string; args: string[]; opts: Record<string, unknown>; child: FakeChild }>,
}));

vi.mock('child_process', async (importOriginal) => {
    const actual = await importOriginal<typeof import('child_process')>();
    const { EventEmitter } = await import('events');
    return {
        ...actual,
        execFile: vi.fn((bin: string, args: string[], opts: any, cb: any) => {
            const callback = typeof opts === 'function' ? opts : cb;
            execFileCalls.calls.push({ bin, args, opts });
            callback(execFileCalls.nextError, execFileCalls.nextStdout, execFileCalls.nextStderr);
        }),
        spawn: vi.fn((cmd: string, args: string[], opts: Record<string, unknown>) => {
            type Emitter = InstanceType<typeof EventEmitter>;
            const child = new EventEmitter() as Emitter & { stdout: Emitter; stderr: Emitter };
            child.stdout = new EventEmitter();
            child.stderr = new EventEmitter();
            spawnCalls.calls.push({ cmd, args, opts, child });
            return child;
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
    spawnRunnerScript,
} from '../../execution/PipelineRunner';
import { findGcloudPath } from '../../cli/cliResolver';

describe('runnerScriptCandidates', () => {
    it('lists scripts per runner in priority order', () => {
        expect(runnerScriptCandidates('Local')).toEqual(['run_local.sh']);
        expect(runnerScriptCandidates('DataprocCluster')).toEqual(['run_dataproc_cluster.sh']);
        expect(runnerScriptCandidates('ServerlessSpark')).toEqual(['run_serverless_spark.sh']);
        expect(runnerScriptCandidates('Local', 'sdp')).toEqual(['run_sdp_local.sh', 'run_local.sh']);
        expect(runnerScriptCandidates('DataprocCluster', 'sdp')).toEqual([
            'run_sdp_dataproc.sh',
            'run_dataproc_cluster.sh',
        ]);
        expect(runnerScriptCandidates('ServerlessSpark', 'sdp')).toEqual([
            'run_sdp_serverless.sh',
            'run_sdp_local.sh',
            'run_serverless_spark.sh',
        ]);
        expect(runnerScriptCandidates('Spark')).toEqual([]);
        expect(runnerScriptCandidates('Flink')).toEqual([]);
    });
});

describe('findRunnerScript', () => {
    it('returns the first existing candidate', () => {
        const existing = new Set([path.join('/p', 'run_local.sh')]);
        expect(findRunnerScript('/p', runnerScriptCandidates('Local', 'sdp'), (p) => existing.has(p))).toBe('run_local.sh');
    });

    it('prefers the earlier candidate when both exist', () => {
        expect(findRunnerScript('/p', runnerScriptCandidates('Local'), () => true)).toBe('run_local.sh');
        expect(findRunnerScript('/p', runnerScriptCandidates('ServerlessSpark'), () => true)).toBe('run_serverless_spark.sh');
        expect(findRunnerScript('/p', runnerScriptCandidates('Local', 'sdp'), () => true)).toBe('run_sdp_local.sh');
    });

    it('returns null when no candidate exists', () => {
        expect(findRunnerScript('/p', runnerScriptCandidates('ServerlessSpark'), () => false)).toBeNull();
    });
});

describe('describeRunner', () => {
    it('describes each Spark runner in imperative and SDP modes', () => {
        expect(describeRunner('Local')).toContain('Local PySpark / Spark Connect');
        expect(describeRunner('DataprocCluster')).toContain('Dataproc Cluster');
        expect(describeRunner('ServerlessSpark')).toContain('Serverless Spark');
        expect(describeRunner('Local', 'sdp')).toContain('Spark Declarative Pipeline via Local Spark 4.2 Connect');
        expect(describeRunner('DataprocCluster', 'sdp')).toContain('Dataproc 4.0 Cluster');
        expect(describeRunner('ServerlessSpark', 'sdp')).toContain('Serverless Spark 4.0');
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

    it('reports a ServerlessSpark batch once, using the default target', () => {
        const track = createJobIdTracker(target);
        expect(track('noise')).toBeNull();
        expect(track('Batch [batch-2026-001] submitted.\n')).toEqual({
            jobId: 'batch-2026-001',
            runner: 'ServerlessSpark',
            projectId: 'p1',
            region: 'r1',
        });
        expect(track('Batch [batch-2026-002] submitted.\n')).toBeNull();
    });

    it('prefers project and region parsed from a Console URL', () => {
        const job = createJobIdTracker(target)(
            'https://console.cloud.google.com/dataproc/batches/europe-west4/batch-euro-99/monitoring?project=url-proj'
        );
        expect(job).toMatchObject({
            jobId: 'batch-euro-99',
            runner: 'ServerlessSpark',
            projectId: 'url-proj',
            region: 'europe-west4',
        });
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

describe('PipelineRunner boundary and failure cases (T5)', () => {
    beforeEach(() => {
        execFileCalls.calls.length = 0;
        execFileCalls.nextError = null;
        execFileCalls.nextStdout = '';
        execFileCalls.nextStderr = '';
        spawnCalls.calls.length = 0;
    });

    describe('runnerScriptCandidates and describeRunner', () => {
        it('matches runner names case-sensitively', () => {
            expect(runnerScriptCandidates('local')).toEqual([]);
            expect(runnerScriptCandidates('serverlessspark')).toEqual([]);
            expect(runnerScriptCandidates('')).toEqual([]);
        });

        it('returns a fresh array so callers cannot mutate the next lookup', () => {
            const first = runnerScriptCandidates('Local');
            first.push('evil.sh');
            expect(runnerScriptCandidates('Local')).toEqual(['run_local.sh']);
        });

        it('falls back to the Serverless Spark description for an unknown runner', () => {
            expect(describeRunner('Flink')).toBe('# Submitting PySpark Batch Job to GCP Serverless Spark (Dataproc)');
            expect(describeRunner('')).toContain('Serverless Spark');
        });

        it('returns null from findRunnerScript for an empty candidate list without checking the disk', () => {
            const exists = vi.fn(() => true);
            expect(findRunnerScript('/p', [], exists)).toBeNull();
            expect(exists).not.toHaveBeenCalled();
        });
    });

    describe('formatBatchTtl', () => {
        it('rounds fractional minutes to whole seconds', () => {
            expect(formatBatchTtl(1.5)).toBe('90s');
            expect(formatBatchTtl('0.5')).toBe('30s');
            expect(formatBatchTtl(0.001)).toBe('0s');
        });

        it('trims and lower-cases suffixed duration strings', () => {
            expect(formatBatchTtl(' 30 ')).toBe('1800s');
            expect(formatBatchTtl('2D')).toBe('2d');
            expect(formatBatchTtl('90M')).toBe('90m');
        });

        it.each([-5, Number.NaN, Number.POSITIVE_INFINITY, '-5', '0', '0.0', '1w', '4 h', 'abc', '1.5h'])(
            'rejects invalid TTL %j',
            (value) => {
                expect(formatBatchTtl(value as number | string)).toBeUndefined();
            }
        );

        it('rejects null', () => {
            expect(formatBatchTtl(null as unknown as undefined)).toBeUndefined();
        });
    });

    describe('buildRunnerEnvironment', () => {
        it('never adds an empty PATH entry, so the current folder is not searched', () => {
            const { env } = buildRunnerEnvironment({ baseEnv: {}, exists: (d) => d === '/usr/bin' });
            expect(env.PATH).toBe('/usr/bin');
            expect(env.PATH.split(path.delimiter)).not.toContain('');

            const empty = buildRunnerEnvironment({ baseEnv: {}, exists: () => false });
            expect(empty.env.PATH).toBe('');
        });

        it('drops empty segments from an inherited PATH and removes duplicates', () => {
            const { env } = buildRunnerEnvironment({
                baseEnv: { PATH: ['/usr/bin', '', '/custom', '/usr/bin', ''].join(path.delimiter) },
                exists: (d) => d === '/usr/bin',
            });
            expect(env.PATH.split(path.delimiter)).toEqual(['/usr/bin', '/custom']);
        });

        it('resolves home-relative tool folders against HOME', () => {
            const sdkBin = path.join('/home/u', 'google-cloud-sdk/bin');
            const { env } = buildRunnerEnvironment({ baseEnv: { HOME: '/home/u' }, exists: (d) => d === sdkBin });
            expect(env.PATH).toBe(sdkBin);
        });

        it('exports no Spark variables when the job config is absent', () => {
            const { env, rejectedKeys } = buildRunnerEnvironment({ baseEnv: { HOME: '/h' }, exists: () => false });
            for (const key of ['PROJECT_ID', 'REGION', 'DEPS_BUCKET', 'SPARK_JARS', 'BATCH_TTL', 'USE_SPARK_CONNECT']) {
                expect(env[key], key).toBeUndefined();
            }
            expect(rejectedKeys).toEqual([]);
        });

        it('prefers the primary job config field over its alias', () => {
            const { env } = buildRunnerEnvironment({
                baseEnv: {},
                exists: () => false,
                jobConfig: {
                    depsBucket: 'gs://deps',
                    gcsStagingPath: 'gs://staging',
                    tier: 'premium',
                    serverlessTier: 'standard',
                    subnetwork: 'primary-subnet',
                    subnet: 'alias-subnet',
                    customJarUris: 'gs://a.jar',
                    sparkJars: 'gs://b.jar',
                },
            });
            expect(env.DEPS_BUCKET).toBe('gs://deps');
            expect(env.GCS_STAGING_PATH).toBe('gs://staging');
            expect(env.SERVERLESS_TIER).toBe('premium');
            expect(env.SUBNET).toBe('primary-subnet');
            expect(env.SUBNETWORK).toBe('primary-subnet');
            expect(env.SPARK_JARS).toBe('gs://a.jar');
        });

        it('uses each alias field when the primary field is empty', () => {
            const { env } = buildRunnerEnvironment({
                baseEnv: {},
                exists: () => false,
                jobConfig: {
                    gcsStagingPath: 'gs://staging',
                    serverlessTier: 'standard',
                    subnet: 'alias-subnet',
                    sparkJars: 'gs://b.jar',
                    batchTtlMinutes: 'not-a-ttl',
                    batchTtl: '2h',
                },
            });
            expect(env.DEPS_BUCKET).toBe('gs://staging');
            expect(env.GCS_STAGING_PATH).toBe('gs://staging');
            expect(env.SERVERLESS_TIER).toBe('standard');
            expect(env.SUBNET).toBe('alias-subnet');
            expect(env.SPARK_JARS).toBe('gs://b.jar');
            expect(env.BATCH_TTL).toBe('2h');
        });

        it('exports USE_SPARK_CONNECT for explicit booleans and omits it otherwise', () => {
            const explicitFalse = buildRunnerEnvironment({
                baseEnv: {},
                exists: () => false,
                jobConfig: { useSparkConnect: false },
            });
            expect(explicitFalse.env.USE_SPARK_CONNECT).toBe('false');

            for (const flag of [undefined, 'true' as unknown as boolean]) {
                const { env } = buildRunnerEnvironment({ baseEnv: {}, exists: () => false, jobConfig: { useSparkConnect: flag } });
                expect(env.USE_SPARK_CONNECT).toBeUndefined();
            }
        });

        it('lets the job config override a runtime variable with the same name', () => {
            const { env } = buildRunnerEnvironment({
                baseEnv: {},
                exists: () => false,
                runtimeVariables: [{ key: 'PROJECT_ID', value: 'from-runtime' }],
                jobConfig: { projectId: 'from-config' },
            });
            expect(env.PROJECT_ID).toBe('from-config');
        });

        it('does not mutate the base environment object', () => {
            const baseEnv = { HOME: '/h', PATH: '/usr/bin' };
            buildRunnerEnvironment({ baseEnv, exists: () => true, runtimeVariables: [{ key: 'X_VAR', value: '1' }] });
            expect(baseEnv).toEqual({ HOME: '/h', PATH: '/usr/bin' });
        });
    });

    describe('resolveRunTarget', () => {
        it('treats an empty projectId and region as missing', async () => {
            expect(await resolveRunTarget({ projectId: '', region: '' }, async () => 'g-proj')).toEqual({
                project: 'g-proj',
                region: DEFAULT_RUN_REGION,
            });
        });

        it('asks gcloud when the projectId is the placeholder', async () => {
            const getProject = vi.fn(async () => 'g-proj');
            expect((await resolveRunTarget({ projectId: PLACEHOLDER_PROJECT_ID }, getProject)).project).toBe('g-proj');
            expect(getProject).toHaveBeenCalledTimes(1);
        });
    });

    describe('createJobIdTracker', () => {
        it('stops after the first job even when a later chunk holds another runner type', () => {
            const track = createJobIdTracker({ project: 'p', region: 'r' });
            expect(track('Batch ID: batch-first-1')).toMatchObject({ runner: 'ServerlessSpark', jobId: 'batch-first-1' });
            expect(track('Job [cluster-job-2] submitted.')).toBeNull();
            expect(track('Submitted job: 2026-01-01_00_00_00-1234567890123')).toBeNull();
        });

        it('keeps returning null for empty chunks', () => {
            const track = createJobIdTracker({ project: 'p', region: 'r' });
            expect(track('')).toBeNull();
            expect(track('\n')).toBeNull();
            expect(track('Job [cluster-job-3]')).toMatchObject({ jobId: 'cluster-job-3' });
        });

        it('uses project and region from a full Serverless batch resource path', () => {
            expect(
                createJobIdTracker({ project: 'p', region: 'r' })('projects/res-proj/locations/asia-south1/batches/batch-res-1')
            ).toEqual({ jobId: 'batch-res-1', runner: 'ServerlessSpark', projectId: 'res-proj', region: 'asia-south1' });
        });
    });

    describe('cancelRemoteSparkJob', () => {
        it('trims the job ID and runs gcloud with a 15 second timeout', async () => {
            const result = await cancelRemoteSparkJob({
                runner: 'ServerlessSpark',
                jobId: '  batch-trim-1  ',
                projectId: 'proj',
                region: 'us-east1',
                gcloudBin: '/g',
            });
            expect(result).toEqual({
                cancelled: true,
                commandLine: '/g dataproc batches cancel batch-trim-1 --region=us-east1 --project=proj --quiet',
            });
            expect(execFileCalls.calls[0].opts).toEqual({ timeout: 15000 });
        });

        it('returns trimmed stdout only when gcloud prints output', async () => {
            execFileCalls.nextStdout = '  Cancelled batch.  \n';
            const result = await cancelRemoteSparkJob({ runner: 'ServerlessSpark', jobId: 'batch-1', projectId: 'p', region: 'r' });
            expect(result.stdout).toBe('Cancelled batch.');
        });

        it('uses findGcloudPath and the default region when neither is given', async () => {
            const result = await cancelRemoteSparkJob({ runner: 'DataprocCluster', jobId: 'job-1', projectId: 'p' });
            expect(findGcloudPath).toHaveBeenCalled();
            expect(execFileCalls.calls[0].bin).toBe('/usr/bin/gcloud');
            expect(execFileCalls.calls[0].args).toContain(`--region=${DEFAULT_RUN_REGION}`);
            expect(result.cancelled).toBe(true);
        });

        it('resolves the project through gcloud when the projectId is missing', async () => {
            const result = await cancelRemoteSparkJob({
                runner: 'ServerlessSpark',
                jobId: 'batch-1',
                region: 'r',
                gcloudBin: '/g',
                getProject: async () => 'g-default',
            });
            expect(result.cancelled).toBe(true);
            expect(execFileCalls.calls[0].args).toContain('--project=g-default');
        });

        it('fails without gcloud when no real project is available', async () => {
            const result = await cancelRemoteSparkJob({
                runner: 'ServerlessSpark',
                jobId: 'batch-1',
                projectId: PLACEHOLDER_PROJECT_ID,
                getProject: async () => undefined,
            });
            expect(result).toEqual({
                cancelled: false,
                commandLine: '',
                error: "Valid GCP project ID is required to cancel remote job 'batch-1'.",
            });
            expect(execFileCalls.calls).toHaveLength(0);
        });

        it.each(['us central1', 'us-east1;rm', 'r/../x'])('rejects unsafe region %j without gcloud', async (region) => {
            const result = await cancelRemoteSparkJob({ runner: 'ServerlessSpark', jobId: 'batch-1', projectId: 'p', region });
            expect(result.cancelled).toBe(false);
            expect(result.error).toContain('Invalid GCP region');
            expect(execFileCalls.calls).toHaveLength(0);
        });

        it.each(['p roj', 'p;rm', '$(id)'])('rejects unsafe project ID %j without gcloud', async (projectId) => {
            const result = await cancelRemoteSparkJob({ runner: 'ServerlessSpark', jobId: 'batch-1', projectId, region: 'r' });
            expect(result.cancelled).toBe(false);
            expect(result.error).toContain('Valid GCP project ID is required');
            expect(execFileCalls.calls).toHaveLength(0);
        });

        it('enforces the 2 to 128 character job ID length limits', async () => {
            const ok = async (jobId: string) =>
                (await cancelRemoteSparkJob({ runner: 'ServerlessSpark', jobId, projectId: 'p', region: 'r', gcloudBin: '/g' })).cancelled;
            expect(await ok('a')).toBe(false);
            expect(await ok('ab')).toBe(true);
            expect(await ok('a'.repeat(128))).toBe(true);
            expect(await ok('a'.repeat(129))).toBe(false);
            expect(await ok('-ab')).toBe(false);
        });

        it('reports a missing job ID with an empty quoted value', async () => {
            const result = await cancelRemoteSparkJob({
                runner: 'ServerlessSpark',
                jobId: undefined as unknown as string,
                projectId: 'p',
            });
            expect(result.error).toBe("Invalid remote Spark job/batch ID: ''");
        });

        it('falls back to the error message when gcloud writes nothing to stderr', async () => {
            execFileCalls.nextError = new Error('spawn gcloud ENOENT');
            const result = await cancelRemoteSparkJob({ runner: 'DataprocCluster', jobId: 'job-1', projectId: 'p', region: 'r', gcloudBin: '/g' });
            expect(result).toEqual({
                cancelled: false,
                commandLine: '/g dataproc jobs kill job-1 --region=r --project=p --quiet',
                error: 'spawn gcloud ENOENT',
            });
        });
    });

    describe('spawnRunnerScript', () => {
        it('runs the script through bash as a detached group leader on POSIX and forwards events', () => {
            if (process.platform === 'win32') {
                return;
            }
            const onOutput = vi.fn();
            const onError = vi.fn();
            const onClose = vi.fn();
            const env = { PATH: '/usr/bin' };
            const child = spawnRunnerScript('/work', '/work/run_local.sh', env, { onOutput, onError, onClose });

            expect(spawnCalls.calls).toHaveLength(1);
            const call = spawnCalls.calls[0];
            expect(call.cmd).toBe('bash');
            expect(call.args).toEqual(['/work/run_local.sh']);
            expect(call.opts).toEqual({ cwd: '/work', env, detached: true });
            expect(child).toBe(call.child);

            call.child.stdout.emit('data', Buffer.from('hello'));
            call.child.stderr.emit('data', 'warn');
            const error = new Error('boom');
            call.child.emit('error', error);
            call.child.emit('close', 1, null);
            call.child.emit('close', null, 'SIGTERM');

            expect(onOutput).toHaveBeenNthCalledWith(1, 'hello', 'stdout');
            expect(onOutput).toHaveBeenNthCalledWith(2, 'warn', 'stderr');
            expect(onError).toHaveBeenCalledWith(error);
            expect(onClose).toHaveBeenNthCalledWith(1, 1, null);
            expect(onClose).toHaveBeenNthCalledWith(2, null, 'SIGTERM');
        });

        it('runs the script directly without detaching on Windows', () => {
            const original = Object.getOwnPropertyDescriptor(process, 'platform');
            Object.defineProperty(process, 'platform', { value: 'win32' });
            try {
                spawnRunnerScript('C:\\work', 'C:\\work\\run_local.sh', {}, {
                    onOutput: vi.fn(),
                    onError: vi.fn(),
                    onClose: vi.fn(),
                });
            } finally {
                if (original) {
                    Object.defineProperty(process, 'platform', original);
                }
            }
            const call = spawnCalls.calls[0];
            expect(call.cmd).toBe('C:\\work\\run_local.sh');
            expect(call.args).toEqual([]);
            expect(call.opts.detached).toBe(false);
        });
    });
});
