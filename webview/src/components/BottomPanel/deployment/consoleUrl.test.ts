/**
 * @fileoverview Unit tests for Google Cloud Console URL builder (R7).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect } from 'vitest';
import {
  buildDataflowConsoleUrl,
  buildDataprocConsoleUrl,
  buildServerlessBatchConsoleUrl,
  buildDataprocJobConsoleUrl,
  buildGcpConsoleUrl,
} from './consoleUrl';

describe('consoleUrl (R7)', () => {
  describe('buildServerlessBatchConsoleUrl & buildDataprocConsoleUrl', () => {
    it('builds monitoring URL for Serverless Spark batch with normal IDs', () => {
      const url = buildServerlessBatchConsoleUrl(
        'my-test-project',
        'us-central1',
        'batch-12345'
      );
      expect(url).toBe(
        'https://console.cloud.google.com/dataproc/batches/us-central1/batch-12345/monitoring?project=my-test-project'
      );
      expect(buildDataprocConsoleUrl('my-test-project', 'us-central1', 'batch-12345')).toBe(url);
    });

    it('encodes special characters such as / and ? in batchId, region, and projectId', () => {
      const url = buildServerlessBatchConsoleUrl(
        'proj?id=1',
        'region/zone',
        'batch/id?extra=true'
      );
      expect(url).toBe(
        'https://console.cloud.google.com/dataproc/batches/region%2Fzone/batch%2Fid%3Fextra%3Dtrue/monitoring?project=proj%3Fid%3D1'
      );
      expect(url).not.toContain('batch/id?extra=true');
      expect(url).toContain('batch%2Fid%3Fextra%3Dtrue');
    });
  });

  describe('buildDataprocJobConsoleUrl', () => {
    it('builds Dataproc Cluster Job URL with normal IDs', () => {
      const url = buildDataprocJobConsoleUrl(
        'my-test-project',
        'us-central1',
        'job-abc-999'
      );
      expect(url).toBe(
        'https://console.cloud.google.com/dataproc/jobs/job-abc-999?region=us-central1&project=my-test-project'
      );
    });

    it('encodes special characters in jobId, region, and projectId', () => {
      const url = buildDataprocJobConsoleUrl(
        'proj/1?a=b',
        'us-central1/a',
        'job/123?foo=bar&baz=1'
      );
      expect(url).toBe(
        'https://console.cloud.google.com/dataproc/jobs/job%2F123%3Ffoo%3Dbar%26baz%3D1?region=us-central1%2Fa&project=proj%2F1%3Fa%3Db'
      );
    });
  });

  describe('buildDataflowConsoleUrl', () => {
    it('preserves normal IDs unchanged', () => {
      const url = buildDataflowConsoleUrl(
        'my-test-project',
        'us-central1',
        '2026-09-12_12_34_56-123456789'
      );
      expect(url).toBe(
        'https://console.cloud.google.com/dataflow/jobs/us-central1/2026-09-12_12_34_56-123456789?project=my-test-project'
      );
    });
  });

  describe('buildGcpConsoleUrl', () => {
    it('returns empty string if jobId is undefined or empty', () => {
      expect(buildGcpConsoleUrl('ServerlessSpark', undefined)).toBe('');
      expect(buildGcpConsoleUrl('DataprocCluster', '')).toBe('');
    });

    it('returns empty string for Local or unsupported runner types', () => {
      expect(buildGcpConsoleUrl('Local', 'job-123')).toBe('');
      expect(buildGcpConsoleUrl('DirectRunner', 'job-123')).toBe('');
      expect(buildGcpConsoleUrl('Flink', 'job-123')).toBe('');
    });

    it('builds Serverless Batch monitoring URL when runner is ServerlessSpark or Spark', () => {
      const expected =
        'https://console.cloud.google.com/dataproc/batches/us-west1/batch-12345/monitoring?project=my-spark-project';
      expect(
        buildGcpConsoleUrl('ServerlessSpark', 'batch-12345', 'us-west1', 'my-spark-project')
      ).toBe(expected);
      expect(
        buildGcpConsoleUrl('Spark', 'batch-12345', 'us-west1', 'my-spark-project')
      ).toBe(expected);
    });

    it('builds Dataproc Cluster Job URL when runner is DataprocCluster', () => {
      const url = buildGcpConsoleUrl(
        'DataprocCluster',
        'cluster-job-42',
        'europe-west1',
        'my-spark-project'
      );
      expect(url).toBe(
        'https://console.cloud.google.com/dataproc/jobs/cluster-job-42?region=europe-west1&project=my-spark-project'
      );
    });

    it('uses default region and projectId when not specified', () => {
      const url = buildGcpConsoleUrl('ServerlessSpark', 'batch-123');
      expect(url).toBe(
        'https://console.cloud.google.com/dataproc/batches/us-central1/batch-123/monitoring?project=YOUR_PROJECT_ID'
      );
    });
  });
});
