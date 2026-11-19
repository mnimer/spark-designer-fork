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

describe('consoleUrl boundary and failure cases (T5)', () => {
  it('encodes special characters in every Dataflow URL part', () => {
    expect(buildDataflowConsoleUrl('proj&x=1', 'us central1', 'job/../id?y')).toBe(
      'https://console.cloud.google.com/dataflow/jobs/us%20central1/job%2F..%2Fid%3Fy?project=proj%26x%3D1'
    );
  });

  it('builds the Dataflow job URL through buildGcpConsoleUrl', () => {
    expect(buildGcpConsoleUrl('Dataflow', '2026-01-01_00_00_00-1', 'asia-east1', 'df-proj')).toBe(
      'https://console.cloud.google.com/dataflow/jobs/asia-east1/2026-01-01_00_00_00-1?project=df-proj'
    );
  });

  it('applies the default region and project to the cluster job URL', () => {
    expect(buildGcpConsoleUrl('DataprocCluster', 'job-1')).toBe(
      'https://console.cloud.google.com/dataproc/jobs/job-1?region=us-central1&project=YOUR_PROJECT_ID'
    );
  });

  it('treats empty region and project strings as missing', () => {
    expect(buildGcpConsoleUrl('ServerlessSpark', 'batch-1', '', '')).toBe(
      'https://console.cloud.google.com/dataproc/batches/us-central1/batch-1/monitoring?project=YOUR_PROJECT_ID'
    );
  });

  it('matches runner names case-sensitively and returns an empty string for other spellings', () => {
    expect(buildGcpConsoleUrl('serverlessspark', 'batch-1', 'us-east1', 'p')).toBe('');
    expect(buildGcpConsoleUrl('dataproccluster', 'job-1', 'us-east1', 'p')).toBe('');
    expect(buildGcpConsoleUrl('', 'job-1', 'us-east1', 'p')).toBe('');
  });

  it('encodes a whitespace-only job ID instead of dropping it', () => {
    expect(buildGcpConsoleUrl('DataprocCluster', ' ', 'us-east1', 'p')).toBe(
      'https://console.cloud.google.com/dataproc/jobs/%20?region=us-east1&project=p'
    );
  });

  it('returns the same URL from the Dataproc alias and the Serverless batch builder', () => {
    expect(buildDataprocConsoleUrl('p', 'r', 'b#1')).toBe(buildServerlessBatchConsoleUrl('p', 'r', 'b#1'));
    expect(buildServerlessBatchConsoleUrl('p', 'r', 'b#1')).toContain('/b%231/monitoring');
  });

  it('keeps the cluster job ID in the path and the region in the query string', () => {
    const url = new URL(buildDataprocJobConsoleUrl('my:proj', 'eu/west', 'job 1'));
    expect(url.pathname).toBe('/dataproc/jobs/job%201');
    expect(url.searchParams.get('region')).toBe('eu/west');
    expect(url.searchParams.get('project')).toBe('my:proj');
  });
});
