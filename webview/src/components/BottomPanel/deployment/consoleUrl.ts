/**
 * @fileoverview URL builder helpers for Google Cloud Console deep links.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/**
 * Builds a Google Cloud Console URL for a Dataflow job (retained for legacy compatibility).
 *
 * All path and query parameters are encoded with encodeURIComponent.
 *
 * @param projectId - Google Cloud Project ID.
 * @param region - GCP region (e.g. 'us-central1').
 * @param jobId - Dataflow Job ID.
 * @returns Fully qualified Cloud Console URL for the Dataflow job.
 */
export function buildDataflowConsoleUrl(
  projectId: string,
  region: string,
  jobId: string
): string {
  const encRegion = encodeURIComponent(region);
  const encJobId = encodeURIComponent(jobId);
  const encProjectId = encodeURIComponent(projectId);
  return `https://console.cloud.google.com/dataflow/jobs/${encRegion}/${encJobId}?project=${encProjectId}`;
}

/**
 * Builds a Google Cloud Console monitoring URL for a GCP Serverless for Apache Spark batch.
 *
 * All path and query parameters are encoded with encodeURIComponent.
 *
 * @param projectId - Google Cloud Project ID.
 * @param region - GCP region (e.g. 'us-central1').
 * @param batchId - Dataproc Serverless Batch ID.
 * @returns Fully qualified Cloud Console monitoring URL for the Serverless Spark batch.
 */
export function buildServerlessBatchConsoleUrl(
  projectId: string,
  region: string,
  batchId: string
): string {
  const encRegion = encodeURIComponent(region);
  const encBatchId = encodeURIComponent(batchId);
  const encProjectId = encodeURIComponent(projectId);
  return `https://console.cloud.google.com/dataproc/batches/${encRegion}/${encBatchId}/monitoring?project=${encProjectId}`;
}

/**
 * Alias for {@link buildServerlessBatchConsoleUrl} for Serverless Spark batches.
 *
 * @param projectId - Google Cloud Project ID.
 * @param region - GCP region (e.g. 'us-central1').
 * @param batchId - Dataproc Serverless Batch ID.
 * @returns Fully qualified Cloud Console monitoring URL for the Serverless Spark batch.
 */
export function buildDataprocConsoleUrl(
  projectId: string,
  region: string,
  batchId: string
): string {
  return buildServerlessBatchConsoleUrl(projectId, region, batchId);
}

/**
 * Builds a Google Cloud Console URL for a Dataproc Cluster job.
 *
 * All path and query parameters are encoded with encodeURIComponent.
 *
 * @param projectId - Google Cloud Project ID.
 * @param region - GCP region (e.g. 'us-central1').
 * @param jobId - Dataproc Cluster Job ID.
 * @returns Fully qualified Cloud Console URL for the Dataproc Cluster job.
 */
export function buildDataprocJobConsoleUrl(
  projectId: string,
  region: string,
  jobId: string
): string {
  const encJobId = encodeURIComponent(jobId);
  const encRegion = encodeURIComponent(region);
  const encProjectId = encodeURIComponent(projectId);
  return `https://console.cloud.google.com/dataproc/jobs/${encJobId}?region=${encRegion}&project=${encProjectId}`;
}

/**
 * Builds a Google Cloud Console URL based on the selected runner and detected job/batch ID.
 *
 * @param runner - Selected pipeline runner ('ServerlessSpark', 'Spark', 'DataprocCluster', 'Local', etc.).
 * @param jobId - Job ID or Batch ID. If omitted or empty, returns empty string.
 * @param region - GCP region (defaults to 'us-central1').
 * @param projectId - Google Cloud Project ID (defaults to 'YOUR_PROJECT_ID').
 * @returns Formatted Cloud Console URL or an empty string if no jobId or local/unrecognized runner.
 */
export function buildGcpConsoleUrl(
  runner: string,
  jobId?: string,
  region?: string,
  projectId?: string
): string {
  if (!jobId) {
    return '';
  }

  const effectiveRegion = region || 'us-central1';
  const effectiveProjectId = projectId || 'YOUR_PROJECT_ID';

  if (runner === 'ServerlessSpark' || runner === 'Spark') {
    return buildServerlessBatchConsoleUrl(effectiveProjectId, effectiveRegion, jobId);
  }

  if (runner === 'DataprocCluster') {
    return buildDataprocJobConsoleUrl(effectiveProjectId, effectiveRegion, jobId);
  }

  if (runner === 'Dataflow') {
    return buildDataflowConsoleUrl(effectiveProjectId, effectiveRegion, jobId);
  }

  return '';
}
