/**
 * @fileoverview Pure extraction of Serverless Spark batch IDs, Dataproc cluster job IDs,
 * and legacy Dataflow job IDs (plus region/project from Console URLs) from runner output.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/**
 * Result of detecting a Spark batch or cluster job in execution output.
 */
export interface DetectedSparkJob {
    /** The Serverless Spark batch ID or Dataproc cluster job ID. */
    jobId: string;
    /** Target Spark execution platform. */
    runner: 'ServerlessSpark' | 'DataprocCluster';
    /** Optional GCP region extracted from the log line or console URL. */
    region?: string;
    /** Optional GCP project ID extracted from the log line or console URL. */
    projectId?: string;
}

/**
 * Result of detecting a Dataflow job in execution output (retained for backward compatibility).
 */
export interface DetectedDataflowJob {
    /** The Dataflow job ID string (format: YYYY-MM-DD_HH_MM_SS-...). */
    jobId: string;
    /** Optional GCP region extracted from the log line or console URL. */
    region?: string;
    /** Optional GCP project ID extracted from the log line or console URL. */
    projectId?: string;
}

/** Status or noise words that may follow "Batch" or "Job" and must not be mistaken for IDs. */
const SPARK_STATUS_WORDS: ReadonlySet<string> = new Set([
    'created',
    'running',
    'pending',
    'failed',
    'succeeded',
    'cancelled',
    'cancelling',
    'canceled',
    'submitted',
    'done',
    'completed',
    'starting',
    'stopping',
    'stopped',
    'error',
    'unknown',
    'active',
    'queued',
    'state',
    'status',
    'output',
    'monitoring',
    'details',
    'configuration',
    'logs',
]);

/** Console URL path sub-pages that are not job/batch IDs. */
const CONSOLE_SUBPAGE_SEGMENTS: ReadonlySet<string> = new Set([
    'monitoring',
    'details',
    'configuration',
    'logs',
]);

/**
 * Extracts `project` and `region` query parameters from a URL query string.
 *
 * @param queryString Raw URL query string without leading `?`.
 * @returns Extracted projectId and region when present.
 */
function parseConsoleQueryParams(queryString: string | undefined): { projectId?: string; region?: string } {
    if (!queryString) {
        return {};
    }
    const projectMatch = queryString.match(/(?:^|&)project=([a-z0-9.:_-]+)/i);
    const regionMatch = queryString.match(/(?:^|&)region=([a-z0-9-]+)/i);
    return {
        ...(projectMatch?.[1] ? { projectId: projectMatch[1] } : {}),
        ...(regionMatch?.[1] ? { region: regionMatch[1] } : {}),
    };
}

/**
 * Pure detector for Google Cloud Serverless for Apache Spark batch IDs from runner output.
 *
 * Supports:
 * 1. Cloud Console URLs:
 *    - `https://console.cloud.google.com/dataproc/batches/<region>/<batchId>/monitoring?project=<project>`
 *    - `https://console.cloud.google.com/dataproc/batches/<region>/<batchId>?project=<project>`
 *    - `https://console.cloud.google.com/dataproc/batches/locations/<region>/batches/<batchId>?project=<project>`
 * 2. Full resource names: `projects/<project>/locations/<region>/batches/<batchId>`
 * 3. CLI / log tokens: `batches/<batchId>`, `Batch [<batchId>]`, `Batch ID: <batchId>`, `batchId: <batchId>`
 *
 * @param line Chunk or single line of output from stdout or stderr.
 * @returns DetectedSparkJob with `runner: 'ServerlessSpark'` if found, or null otherwise.
 */
export function detectServerlessBatchId(line: string): DetectedSparkJob | null {
    if (!line || typeof line !== 'string') {
        return null;
    }

    // Pattern 1a: Console URL with /locations/<region>/batches/<batchId>
    const locUrlMatch = line.match(
        /https:\/\/console\.cloud\.google\.com\/dataproc\/batches\/locations\/([a-z0-9-]+)\/batches\/([a-zA-Z0-9][a-zA-Z0-9_-]{3,})(?:\/[a-z]+)?(?:\?([^\s"'<>]*))?/i
    );
    if (locUrlMatch) {
        const region = locUrlMatch[1];
        const jobId = locUrlMatch[2];
        if (!SPARK_STATUS_WORDS.has(jobId.toLowerCase())) {
            const { projectId } = parseConsoleQueryParams(locUrlMatch[3]);
            return {
                jobId,
                runner: 'ServerlessSpark',
                ...(region ? { region } : {}),
                ...(projectId ? { projectId } : {}),
            };
        }
    }

    // Pattern 1b: Standard Console URL /dataproc/batches/<region>/<batchId>[/monitoring][?project=...]
    const urlMatch = line.match(
        /https:\/\/console\.cloud\.google\.com\/dataproc\/batches\/([a-z0-9-]+)\/([a-zA-Z0-9][a-zA-Z0-9_-]{3,})(?:\/(?:monitoring|details|configuration|logs))?(?:\?([^\s"'<>]*))?/i
    );
    if (urlMatch) {
        const seg1 = urlMatch[1];
        const seg2 = urlMatch[2];
        const queryParams = parseConsoleQueryParams(urlMatch[3]);
        // If seg2 is a subpage (e.g. /dataproc/batches/<batchId>/monitoring?region=...), seg1 is the batchId
        const isSeg2Subpage = CONSOLE_SUBPAGE_SEGMENTS.has(seg2.toLowerCase());
        const jobId = isSeg2Subpage ? seg1 : seg2;
        const region = isSeg2Subpage ? queryParams.region : seg1;
        if (!SPARK_STATUS_WORDS.has(jobId.toLowerCase())) {
            return {
                jobId,
                runner: 'ServerlessSpark',
                ...(region ? { region } : {}),
                ...(queryParams.projectId ? { projectId: queryParams.projectId } : {}),
            };
        }
    }

    // Pattern 2: Full resource path projects/<project>/locations/<region>/batches/<batchId>
    const resourceMatch = line.match(
        /projects\/([a-z0-9.:_-]+)\/locations\/([a-z0-9-]+)\/batches\/([a-zA-Z0-9][a-zA-Z0-9_-]{3,})/i
    );
    if (resourceMatch) {
        const projectId = resourceMatch[1];
        const region = resourceMatch[2];
        const jobId = resourceMatch[3];
        if (!SPARK_STATUS_WORDS.has(jobId.toLowerCase())) {
            return {
                jobId,
                runner: 'ServerlessSpark',
                projectId,
                region,
            };
        }
    }

    // Pattern 3: CLI / log patterns: batches/<id>, Batch [<id>], Batch ID: <id>, batchId: <id>
    const tokenMatch = line.match(
        /(?:batches\/|Batch\s*\[|Batch ID:\s*['"]?|batchId:\s*['"]?|batch_id:\s*['"]?)([a-zA-Z0-9][a-zA-Z0-9_-]{3,})/i
    );
    if (tokenMatch && tokenMatch[1]) {
        const jobId = tokenMatch[1];
        if (!SPARK_STATUS_WORDS.has(jobId.toLowerCase())) {
            return {
                jobId,
                runner: 'ServerlessSpark',
            };
        }
    }

    return null;
}

/**
 * Pure detector for Google Cloud Dataproc Cluster job IDs from runner output.
 *
 * Supports:
 * 1. Cloud Console URLs:
 *    - `https://console.cloud.google.com/dataproc/jobs/<jobId>?region=<region>&project=<project>`
 *    - `https://console.cloud.google.com/dataproc/jobs/<region>/<jobId>?project=<project>`
 * 2. Full resource names: `projects/<project>/regions/<region>/jobs/<jobId>`
 * 3. CLI / log tokens: `Job [<jobId>] submitted`, `Job [<jobId>]`, `jobs/<jobId>`, `jobId: <jobId>`
 *
 * @param line Chunk or single line of output from stdout or stderr.
 * @returns DetectedSparkJob with `runner: 'DataprocCluster'` if found, or null otherwise.
 */
export function detectClusterJobId(line: string): DetectedSparkJob | null {
    if (!line || typeof line !== 'string') {
        return null;
    }

    // Ignore Dataflow URLs so /dataflow/jobs/... is never misclassified as a Dataproc cluster job
    if (/console\.cloud\.google\.com\/dataflow\/jobs/i.test(line)) {
        return null;
    }

    // Pattern 1a: Console URL with /locations/<region>/jobs/<jobId> or /regions/<region>/jobs/<jobId>
    const locUrlMatch = line.match(
        /https:\/\/console\.cloud\.google\.com\/dataproc\/jobs\/(?:locations|regions)\/([a-z0-9-]+)\/jobs\/([a-zA-Z0-9][a-zA-Z0-9_-]{3,})(?:\/[a-z]+)?(?:\?([^\s"'<>]*))?/i
    );
    if (locUrlMatch) {
        const region = locUrlMatch[1];
        const jobId = locUrlMatch[2];
        if (!SPARK_STATUS_WORDS.has(jobId.toLowerCase())) {
            const { projectId } = parseConsoleQueryParams(locUrlMatch[3]);
            return {
                jobId,
                runner: 'DataprocCluster',
                ...(region ? { region } : {}),
                ...(projectId ? { projectId } : {}),
            };
        }
    }

    // Pattern 1b: Console URL /dataproc/jobs/<jobId>?region=...&project=... or /dataproc/jobs/<region>/<jobId>?project=...
    const urlMatch = line.match(
        /https:\/\/console\.cloud\.google\.com\/dataproc\/jobs\/([a-zA-Z0-9][a-zA-Z0-9_-]{3,})(?:\/([a-zA-Z0-9][a-zA-Z0-9_-]{3,}))?(?:\/(?:monitoring|details|configuration|logs))?(?:\?([^\s"'<>]*))?/i
    );
    if (urlMatch) {
        const seg1 = urlMatch[1];
        const seg2 = urlMatch[2];
        const queryParams = parseConsoleQueryParams(urlMatch[3]);
        const hasSecondIdSegment = Boolean(seg2 && !CONSOLE_SUBPAGE_SEGMENTS.has(seg2.toLowerCase()));
        const jobId = hasSecondIdSegment && seg2 ? seg2 : seg1;
        const region = hasSecondIdSegment ? seg1 : queryParams.region;
        if (!SPARK_STATUS_WORDS.has(jobId.toLowerCase())) {
            return {
                jobId,
                runner: 'DataprocCluster',
                ...(region ? { region } : {}),
                ...(queryParams.projectId ? { projectId: queryParams.projectId } : {}),
            };
        }
    }

    // Pattern 2: Full resource path projects/<project>/regions/<region>/jobs/<jobId>
    const resourceMatch = line.match(
        /projects\/([a-z0-9.:_-]+)\/(?:regions|locations)\/([a-z0-9-]+)\/jobs\/([a-zA-Z0-9][a-zA-Z0-9_-]{3,})/i
    );
    if (resourceMatch) {
        const projectId = resourceMatch[1];
        const region = resourceMatch[2];
        const jobId = resourceMatch[3];
        if (!SPARK_STATUS_WORDS.has(jobId.toLowerCase())) {
            return {
                jobId,
                runner: 'DataprocCluster',
                projectId,
                region,
            };
        }
    }

    // Pattern 3: CLI / log patterns:
    // - "Job [<id>] submitted." / "Job [<id>]"
    // - "jobs/<id>" or "dataproc/jobs/<id>"
    // - "jobId: <id>" or "job_id: <id>"
    const bracketMatch = line.match(/Job\s*\[([a-zA-Z0-9][a-zA-Z0-9_-]{3,})\](?:\s+submitted)?/i);
    if (bracketMatch && bracketMatch[1] && !SPARK_STATUS_WORDS.has(bracketMatch[1].toLowerCase())) {
        return {
            jobId: bracketMatch[1],
            runner: 'DataprocCluster',
        };
    }

    const jobsPathMatch = line.match(/(?:^|[\s"'=(]|dataproc\/)jobs\/([a-zA-Z0-9][a-zA-Z0-9_-]{3,})/i);
    if (jobsPathMatch && jobsPathMatch[1] && !SPARK_STATUS_WORDS.has(jobsPathMatch[1].toLowerCase())) {
        return {
            jobId: jobsPathMatch[1],
            runner: 'DataprocCluster',
        };
    }

    const jobIdPropMatch = line.match(/(?:jobId|job_id):\s*['"]?([a-zA-Z0-9][a-zA-Z0-9_-]{3,})/i);
    if (jobIdPropMatch && jobIdPropMatch[1] && !SPARK_STATUS_WORDS.has(jobIdPropMatch[1].toLowerCase())) {
        return {
            jobId: jobIdPropMatch[1],
            runner: 'DataprocCluster',
        };
    }

    return null;
}

/**
 * Detects either a Serverless Spark batch ID or a Dataproc Cluster job ID from runner output.
 *
 * @param line Chunk or single line of output from stdout or stderr.
 * @returns DetectedSparkJob if a batch or cluster job ID was found, or null otherwise.
 */
export function detectSparkJob(line: string): DetectedSparkJob | null {
    return detectServerlessBatchId(line) ?? detectClusterJobId(line);
}

/**
 * Pure detector for identifying Google Cloud Dataflow job IDs from command-line runner output.
 *
 * Supports:
 * 1. Cloud Console URLs:
 *    e.g. https://console.cloud.google.com/dataflow/jobs/us-central1/2026-09-30_02_00_00-1234567890123456?project=my-project
 *    or https://console.cloud.google.com/dataflow/jobs/locations/us-central1/jobs/2026-09-30_02_00_00-1234567890123456?project=my-project
 * 2. "Submitted job: <jobId>" standard Beam log output
 * 3. "id: '<jobId>'" gcloud / JSON / YAML output
 * 4. Raw job ID tokens matching the pattern YYYY-MM-DD_...
 *
 * @param line - Chunk or single line of output from stdout or stderr.
 * @returns DetectedDataflowJob if found, or null otherwise.
 */
export function detectDataflowJob(line: string): DetectedDataflowJob | null {
    if (!line || typeof line !== 'string') {
        return null;
    }

    // Pattern 1: Google Cloud Console URL
    // e.g. https://console.cloud.google.com/dataflow/jobs/<region>/<jobId>?project=<project>
    // or .../dataflow/jobs/locations/<region>/jobs/<jobId>?project=<project>
    const urlMatch = line.match(
        /https:\/\/console\.cloud\.google\.com\/dataflow\/jobs(?:\/locations\/([a-z0-9-]+)\/jobs|\/([a-z0-9-]+))?\/([0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9a-zA-Z_-]{10,})(?:\?([^\s"'<>]*))?/i
    );
    if (urlMatch) {
        const region = urlMatch[1] || urlMatch[2];
        const jobId = urlMatch[3];
        const queryString = urlMatch[4];
        let projectId: string | undefined;
        if (queryString) {
            const projectMatch = queryString.match(/(?:^|&)project=([a-z0-9-_]+)/i);
            if (projectMatch) {
                projectId = projectMatch[1];
            }
        }
        return {
            jobId,
            ...(region ? { region } : {}),
            ...(projectId ? { projectId } : {})
        };
    }

    // Pattern 2: Standard log / CLI format
    // Matches "Submitted job: 2026-...", "id: '2026-...", or bare job ID token
    const match = line.match(/(?:id:\s*['"]?|Submitted job:\s*|\b)([0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9a-zA-Z_-]{10,})/i);
    if (match && match[1]) {
        return {
            jobId: match[1]
        };
    }

    return null;
}
