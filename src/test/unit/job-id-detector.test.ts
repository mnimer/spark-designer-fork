/**
 * Unit tests for Serverless Spark batch ID, Dataproc cluster job ID, and Dataflow job ID detectors
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect } from 'vitest';
import {
    detectClusterJobId,
    detectDataflowJob,
    detectServerlessBatchId,
    detectSparkJob,
} from '../../execution/jobIdDetector';

describe('Serverless Spark Batch ID Detector (detectServerlessBatchId)', () => {
    it('detects batch ID from "batches/<id>" and full resource name', () => {
        expect(detectServerlessBatchId('Submitted batches/batch-etl-001 to Dataproc')).toEqual({
            jobId: 'batch-etl-001',
            runner: 'ServerlessSpark',
        });

        expect(
            detectServerlessBatchId('name: projects/my-spark-proj/locations/us-central1/batches/spark-batch-99')
        ).toEqual({
            jobId: 'spark-batch-99',
            runner: 'ServerlessSpark',
            projectId: 'my-spark-proj',
            region: 'us-central1',
        });
    });

    it('detects batch ID from "Batch [<id>]" and "Batch ID: <id>"', () => {
        expect(detectServerlessBatchId('Batch [my-batch-42] submitted.')).toEqual({
            jobId: 'my-batch-42',
            runner: 'ServerlessSpark',
        });
        expect(detectServerlessBatchId('Batch ID: batch-abc123')).toEqual({
            jobId: 'batch-abc123',
            runner: 'ServerlessSpark',
        });
    });

    it('extracts batchId, region, and projectId from Cloud Console URLs', () => {
        expect(
            detectServerlessBatchId(
                'Monitor at https://console.cloud.google.com/dataproc/batches/us-east1/batch-2026-xyz/monitoring?project=analytics-prod'
            )
        ).toEqual({
            jobId: 'batch-2026-xyz',
            runner: 'ServerlessSpark',
            region: 'us-east1',
            projectId: 'analytics-prod',
        });

        expect(
            detectServerlessBatchId(
                'https://console.cloud.google.com/dataproc/batches/locations/europe-west1/batches/euro-batch-1?project=euro-proj'
            )
        ).toEqual({
            jobId: 'euro-batch-1',
            runner: 'ServerlessSpark',
            region: 'europe-west1',
            projectId: 'euro-proj',
        });
    });

    it('ignores status words after Batch', () => {
        expect(detectServerlessBatchId('Batch [running]')).toBeNull();
        expect(detectServerlessBatchId('Batch [pending]')).toBeNull();
        expect(detectServerlessBatchId('Batch [created]')).toBeNull();
        expect(detectServerlessBatchId('Batch [failed]')).toBeNull();
    });
});

describe('Dataproc Cluster Job ID Detector (detectClusterJobId)', () => {
    it('detects job ID from "Job [<id>] submitted" and "Job [<id>]"', () => {
        expect(detectClusterJobId('Job [a1b2c3d4-e5f6-7890] submitted.')).toEqual({
            jobId: 'a1b2c3d4-e5f6-7890',
            runner: 'DataprocCluster',
        });
        expect(detectClusterJobId('Waiting for Job [cluster-job-101] output...')).toEqual({
            jobId: 'cluster-job-101',
            runner: 'DataprocCluster',
        });
    });

    it('detects job ID from "jobs/<id>", full resource path, and "jobId: <id>"', () => {
        expect(detectClusterJobId('Submitted jobs/pyspark-job-777')).toEqual({
            jobId: 'pyspark-job-777',
            runner: 'DataprocCluster',
        });
        expect(
            detectClusterJobId('projects/p-cluster/regions/us-west1/jobs/job-uuid-9876')
        ).toEqual({
            jobId: 'job-uuid-9876',
            runner: 'DataprocCluster',
            projectId: 'p-cluster',
            region: 'us-west1',
        });
        expect(detectClusterJobId('jobId: "job-from-yaml-123"')).toEqual({
            jobId: 'job-from-yaml-123',
            runner: 'DataprocCluster',
        });
    });

    it('extracts jobId, region, and projectId from Dataproc Cluster Job Console URLs', () => {
        expect(
            detectClusterJobId(
                'https://console.cloud.google.com/dataproc/jobs/job-cluster-555?region=us-central1&project=my-cluster-proj'
            )
        ).toEqual({
            jobId: 'job-cluster-555',
            runner: 'DataprocCluster',
            region: 'us-central1',
            projectId: 'my-cluster-proj',
        });

        expect(
            detectClusterJobId(
                'https://console.cloud.google.com/dataproc/jobs/asia-east1/job-asia-999?project=asia-proj'
            )
        ).toEqual({
            jobId: 'job-asia-999',
            runner: 'DataprocCluster',
            region: 'asia-east1',
            projectId: 'asia-proj',
        });
    });

    it('does not misclassify Dataflow Console URLs or status words as Dataproc Cluster jobs', () => {
        expect(
            detectClusterJobId(
                'https://console.cloud.google.com/dataflow/jobs/us-central1/2026-09-30_02_00_00-1234567890123456?project=my-gcp-project'
            )
        ).toBeNull();
        expect(detectClusterJobId('Job [running]')).toBeNull();
        expect(detectClusterJobId('Job [submitted]')).toBeNull();
    });
});

describe('Unified Spark Job Detector (detectSparkJob)', () => {
    it('distinguishes ServerlessSpark batches from DataprocCluster jobs', () => {
        expect(detectSparkJob('Batch [serverless-batch-01] created.')).toEqual({
            jobId: 'serverless-batch-01',
            runner: 'ServerlessSpark',
        });
        expect(detectSparkJob('Job [dataproc-job-02] submitted.')).toEqual({
            jobId: 'dataproc-job-02',
            runner: 'DataprocCluster',
        });
        expect(detectSparkJob('Normal Spark log line without IDs')).toBeNull();
    });
});

describe('Dataflow Job ID Detector (detectDataflowJob)', () => {
    describe('Standard Beam Runner Output', () => {
        it('detects job ID from "Submitted job: <jobId>" format', () => {
            const line = 'Submitted job: 2026-01-01_00_00_00-1234567890123\n';
            const result = detectDataflowJob(line);

            expect(result).not.toBeNull();
            expect(result?.jobId).toBe('2026-01-01_00_00_00-1234567890123');
            expect(result?.region).toBeUndefined();
            expect(result?.projectId).toBeUndefined();
        });

        it('detects job ID embedded in logging timestamps and log level prefixes', () => {
            const line = '[2026-09-30 02:00:00,123] [INFO] Submitted job: 2026-09-30_02_00_00-9876543210123456789';
            const result = detectDataflowJob(line);

            expect(result).not.toBeNull();
            expect(result?.jobId).toBe('2026-09-30_02_00_00-9876543210123456789');
        });

        it('detects bare job ID token in runner output', () => {
            const line = 'Job 2026-05-10_10_20_30-abcdef1234567890 is currently running.';
            const result = detectDataflowJob(line);

            expect(result).not.toBeNull();
            expect(result?.jobId).toBe('2026-05-10_10_20_30-abcdef1234567890');
        });
    });

    describe('CLI and Structured Output', () => {
        it('detects job ID from "id: \'<jobId>\'" single-quoted format', () => {
            const line = "id: '2026-03-15_14_22_33-abcdef1234567890'";
            const result = detectDataflowJob(line);

            expect(result).not.toBeNull();
            expect(result?.jobId).toBe('2026-03-15_14_22_33-abcdef1234567890');
        });

        it('detects job ID from "id: \\"<jobId>\\"" double-quoted format', () => {
            const line = 'id: "2026-03-15_14_22_33-abcdef1234567890"';
            const result = detectDataflowJob(line);

            expect(result).not.toBeNull();
            expect(result?.jobId).toBe('2026-03-15_14_22_33-abcdef1234567890');
        });

        it('detects job ID from unquoted "id: <jobId>" format', () => {
            const line = 'id: 2026-03-15_14_22_33-abcdef1234567890';
            const result = detectDataflowJob(line);

            expect(result).not.toBeNull();
            expect(result?.jobId).toBe('2026-03-15_14_22_33-abcdef1234567890');
        });
    });

    describe('Cloud Console URLs', () => {
        it('extracts jobId, region, and projectId from standard Cloud Console URL', () => {
            const line = 'To access the documentation on Dataflow jobs: https://console.cloud.google.com/dataflow/jobs/us-central1/2026-09-30_02_00_00-1234567890123456?project=my-gcp-project';
            const result = detectDataflowJob(line);

            expect(result).not.toBeNull();
            expect(result?.jobId).toBe('2026-09-30_02_00_00-1234567890123456');
            expect(result?.region).toBe('us-central1');
            expect(result?.projectId).toBe('my-gcp-project');
        });

        it('extracts jobId, region, and projectId from locations/<region>/jobs format', () => {
            const line = 'Console URL: https://console.cloud.google.com/dataflow/jobs/locations/europe-west1/jobs/2026-09-30_02_00_00-1234567890123456?project=euro-prod-123';
            const result = detectDataflowJob(line);

            expect(result).not.toBeNull();
            expect(result?.jobId).toBe('2026-09-30_02_00_00-1234567890123456');
            expect(result?.region).toBe('europe-west1');
            expect(result?.projectId).toBe('euro-prod-123');
        });

        it('handles Console URL without query parameters', () => {
            const line = 'https://console.cloud.google.com/dataflow/jobs/us-east1/2026-09-30_02_00_00-1234567890123456';
            const result = detectDataflowJob(line);

            expect(result).not.toBeNull();
            expect(result?.jobId).toBe('2026-09-30_02_00_00-1234567890123456');
            expect(result?.region).toBe('us-east1');
            expect(result?.projectId).toBeUndefined();
        });
    });

    describe('Edge Cases and Non-matches', () => {
        it('returns null for empty string or whitespace', () => {
            expect(detectDataflowJob('')).toBeNull();
            expect(detectDataflowJob('   ')).toBeNull();
        });

        it('returns null for non-string input safely', () => {
            expect(detectDataflowJob(null as unknown as string)).toBeNull();
            expect(detectDataflowJob(undefined as unknown as string)).toBeNull();
        });

        it('returns null for generic build and execution logs', () => {
            expect(detectDataflowJob('Running DirectRunner...')).toBeNull();
            expect(detectDataflowJob('Build succeeded with 0 errors.')).toBeNull();
            expect(detectDataflowJob('2026-09-30 12:00:00 Starting pipeline')).toBeNull();
        });

        it('returns null when ID token is too short or malformed', () => {
            expect(detectDataflowJob('Submitted job: 2026-01-01_123')).toBeNull();
            expect(detectDataflowJob('id: 1234567890')).toBeNull();
        });
    });
});
