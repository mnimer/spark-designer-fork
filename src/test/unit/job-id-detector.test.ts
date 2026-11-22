/**
 * Unit tests for Serverless Spark batch ID and Dataproc cluster job ID detectors.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect } from 'vitest';
import {
    detectClusterJobId,
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

    it('does not misclassify unrelated service Console URLs or status words as Dataproc Cluster jobs', () => {
        expect(
            detectClusterJobId(
                'https://console.cloud.google.com/compute/jobs/us-central1/2026-09-30_02_00_00-1234567890123456?project=my-gcp-project'
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

describe('Spark job detectors: boundary and failure cases (T5)', () => {
    it('returns null for empty, whitespace, and non-string input in every Spark detector', () => {
        for (const detector of [detectServerlessBatchId, detectClusterJobId, detectSparkJob]) {
            expect(detector('')).toBeNull();
            expect(detector('   ')).toBeNull();
            expect(detector(undefined as unknown as string)).toBeNull();
            expect(detector(null as unknown as string)).toBeNull();
            expect(detector(123 as unknown as string)).toBeNull();
        }
    });

    it('requires at least four characters in a batch or job ID', () => {
        expect(detectServerlessBatchId('batches/abc')).toBeNull();
        expect(detectServerlessBatchId('batches/abcd')).toEqual({ jobId: 'abcd', runner: 'ServerlessSpark' });
        expect(detectClusterJobId('Job [abc] submitted.')).toBeNull();
        expect(detectClusterJobId('Job [abcd] submitted.')).toEqual({ jobId: 'abcd', runner: 'DataprocCluster' });
    });

    it('rejects IDs that start with a separator character', () => {
        expect(detectServerlessBatchId('Batch [-batch-001]')).toBeNull();
        expect(detectClusterJobId('Job [_job-001]')).toBeNull();
    });

    it('detects batchId and batch_id properties with quotes', () => {
        expect(detectServerlessBatchId('batchId: "etl-batch-7"')).toEqual({ jobId: 'etl-batch-7', runner: 'ServerlessSpark' });
        expect(detectServerlessBatchId("batch_id: 'etl-batch-8'")).toEqual({ jobId: 'etl-batch-8', runner: 'ServerlessSpark' });
    });

    it('reads the region from the query string when the batch console URL has no region segment', () => {
        expect(
            detectServerlessBatchId(
                'https://console.cloud.google.com/dataproc/batches/nightly-batch/monitoring?region=asia-east1&project=p-1'
            )
        ).toEqual({ jobId: 'nightly-batch', runner: 'ServerlessSpark', region: 'asia-east1', projectId: 'p-1' });
    });

    it('omits projectId and region when the console URL has no query string', () => {
        expect(detectServerlessBatchId('https://console.cloud.google.com/dataproc/batches/us-west1/batch-no-query')).toEqual({
            jobId: 'batch-no-query',
            runner: 'ServerlessSpark',
            region: 'us-west1',
        });
        expect(detectClusterJobId('https://console.cloud.google.com/dataproc/jobs/job-no-query')).toEqual({
            jobId: 'job-no-query',
            runner: 'DataprocCluster',
        });
    });

    it('accepts domain-scoped project IDs in console URLs and resource paths', () => {
        expect(
            detectClusterJobId('https://console.cloud.google.com/dataproc/jobs/job-scoped-1?region=us-central1&project=example.com:my-proj')
        ).toEqual({ jobId: 'job-scoped-1', runner: 'DataprocCluster', region: 'us-central1', projectId: 'example.com:my-proj' });
        expect(detectClusterJobId('projects/example.com:p/regions/us-central1/jobs/job-xyz-9')).toEqual({
            jobId: 'job-xyz-9',
            runner: 'DataprocCluster',
            projectId: 'example.com:p',
            region: 'us-central1',
        });
    });

    it('detects cluster jobs from the locations and regions console URL forms', () => {
        expect(
            detectClusterJobId('https://console.cloud.google.com/dataproc/jobs/regions/us-east4/jobs/cluster-job-77?project=proj-a')
        ).toEqual({ jobId: 'cluster-job-77', runner: 'DataprocCluster', region: 'us-east4', projectId: 'proj-a' });
    });

    it('ignores status words in every Spark ID position', () => {
        expect(detectServerlessBatchId('batches/running')).toBeNull();
        expect(detectServerlessBatchId('Batch ID: SUCCEEDED')).toBeNull();
        expect(detectClusterJobId('jobs/cancelled')).toBeNull();
        expect(detectClusterJobId('jobId: Pending')).toBeNull();
        expect(detectSparkJob('Batch [status] Job [error]')).toBeNull();
    });

    it('returns null from detectSparkJob for non-Dataproc console URLs and generic Spark logs', () => {
        expect(
            detectSparkJob('https://console.cloud.google.com/compute/jobs/us-central1/2026-09-30_02_00_00-1234567890123456?project=p')
        ).toBeNull();
        expect(detectSparkJob('INFO SparkContext: Running Spark version 4.0.0')).toBeNull();
        expect(detectSparkJob('WARN NativeCodeLoader: Unable to load native-hadoop library')).toBeNull();
    });

    it('prefers a Serverless batch over a cluster job when one line holds both', () => {
        expect(detectSparkJob('batches/batch-first-1 then Job [cluster-second-2]')).toEqual({
            jobId: 'batch-first-1',
            runner: 'ServerlessSpark',
        });
    });

    it('detects a job ID inside a multi-line output chunk', () => {
        const chunk = 'Waiting for job output...\nJob [multi-line-job-5] submitted.\nDone.';
        expect(detectClusterJobId(chunk)).toEqual({ jobId: 'multi-line-job-5', runner: 'DataprocCluster' });
    });
});
