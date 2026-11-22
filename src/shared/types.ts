/**
 * @fileoverview Pure TypeScript data contracts and allowlist helpers crossing the host<->webview IPC boundary.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Rules:
 * - Pure interfaces, types, and deterministic helpers only (no enums).
 * - Zero dependencies on vscode, node, or react.
 */

/**
 * Canonical allowlist of supported Apache Spark runner identifiers.
 */
export const SUPPORTED_SPARK_RUNNERS = [
    'Local',
    'DataprocCluster',
    'ServerlessSpark',
] as const;

/**
 * Supported Spark target execution environments.
 */
export type SparkRunnerType = (typeof SUPPORTED_SPARK_RUNNERS)[number];

/**
 * Normalizes a candidate runner value against the supported Spark runner allowlist.
 * If the value is not an exact match in `SUPPORTED_SPARK_RUNNERS`, return `'Local'`.
 *
 * @param runner Raw runner value from serialized pipeline configuration or IPC payload.
 * @returns Canonical Spark runner identifier (`'Local'` | `'DataprocCluster'` | `'ServerlessSpark'`).
 */
export function normalizeSparkRunner(runner?: unknown): SparkRunnerType {
    if (
        typeof runner === 'string' &&
        (SUPPORTED_SPARK_RUNNERS as readonly string[]).includes(runner)
    ) {
        return runner as SparkRunnerType;
    }
    return 'Local';
}

/**
 * Token usage telemetry from Gemini code generation runs.
 */
export interface TokenUsage {
    /** Number of input prompt tokens consumed. */
    inputTokens: number;
    /** Number of candidate output tokens generated. */
    outputTokens: number;
    /** Optional total tokens consumed across the request. */
    totalTokens?: number;
}

/**
 * Built-in sample pipeline catalog entry.
 */
export interface SamplePipeline {
    /** Unique identifier for the sample (e.g. 'gcs-parquet-etl'). */
    id: string;
    /** Bundled .spark file name (e.g. 'gcs-parquet-etl.spark'). */
    filename: string;
    /** Human-readable title of the sample (e.g. 'GCS Parquet ETL'). */
    title: string;
    /** Summary description or flow of what the sample demonstrates. */
    description: string;
    /** Optional category group (e.g. 'Samples'). */
    category?: string;
    /** Optional visual badge text (e.g. 'PySpark'). */
    badge?: string;
    /** Optional normalized pipeline language ('PySpark'). */
    language?: string;
    /** Optional recommended runner name ('Local' | 'DataprocCluster' | 'ServerlessSpark'). */
    runner?: string;
    /** Optional number of nodes in the sample pipeline DAG. */
    nodesCount?: number;
    /** Optional full Markdown README documentation for the sample pipeline. */
    readmeContent?: string;
    /** Raw .spark YAML text content. */
    content: string;
}

/**
 * Log stream identifier for pipeline execution output.
 */
export type LogStream = 'stdout' | 'stderr';

/**
 * Supported target languages for Spark code generation.
 * Scala support is deferred to Phase 2 (V2.2).
 */
export type SupportedLanguage = 'PySpark';

/**
 * Default target language for new and existing Spark pipelines.
 */
export const DEFAULT_LANGUAGE: SupportedLanguage = 'PySpark';

/**
 * Default Apache Spark runtime version baseline.
 */
export const DEFAULT_SPARK_VERSION = '4.0';

/**
 * Trigger execution strategies supported by Spark Structured Streaming queries.
 */
export type SparkStreamingTriggerType =
    | 'processingTime'
    | 'availableNow'
    | 'once'
    | 'continuous';

/**
 * Trigger selector values for per-sink overrides, including pipeline inheritance.
 */
export type SparkSinkTriggerSelection = 'inherit' | SparkStreamingTriggerType;

/**
 * Output modes supported by Spark Structured Streaming DataStreamWriter sinks.
 */
export type SparkStreamingOutputMode = 'append' | 'complete' | 'update';

/**
 * Per-sink Structured Streaming configuration overrides stored in sink node properties.
 */
export interface SparkStreamingSinkOverrides {
    /** Structured Streaming output mode ('append' default, 'complete', or 'update'). */
    outputMode?: SparkStreamingOutputMode;
    /** Trigger mode override, or 'inherit' to use the pipeline default trigger. */
    triggerType?: SparkSinkTriggerSelection;
    /** Trigger interval expression when triggerType uses an interval (for example '10 seconds'). */
    triggerInterval?: string;
    /** Explicit checkpoint URI override. If blank, Spark inherits '<checkpointRoot>/<nodeId>'. */
    checkpointLocation?: string;
}

/**
 * Pipeline-level Structured Streaming defaults stored in jobConfig.
 */
export interface SparkPipelineStreamingDefaults {
    /** Base storage URI for streaming query checkpoints (for example 'gs://bucket/checkpoints'). */
    checkpointRoot?: string;
    /** Default trigger strategy inherited by streaming sinks ('processingTime' default). */
    defaultTriggerType?: SparkStreamingTriggerType;
    /** Default trigger interval inherited when defaultTriggerType uses an interval. */
    defaultTriggerInterval?: string;
}

/**
 * Canonical allowlist of supported pipeline authoring and code generation paradigms.
 */
export const SUPPORTED_PIPELINE_PARADIGMS = [
    'imperative',
    'sdp',
    'notebook',
] as const;

/**
 * Pipeline authoring and code generation paradigm.
 */
export type PipelineParadigm = (typeof SUPPORTED_PIPELINE_PARADIGMS)[number];

/**
 * Default pipeline paradigm for new and existing Spark pipelines.
 */
export const DEFAULT_PIPELINE_PARADIGM: PipelineParadigm = 'imperative';

/**
 * Normalizes a candidate paradigm identifier to `'imperative'`, `'sdp'`, or `'notebook'`.
 * If the value is not a recognized paradigm, return `'imperative'`.
 *
 * @param paradigm Candidate paradigm identifier from serialized pipeline config or IPC payload.
 * @returns Canonical `PipelineParadigm` value.
 */
export function normalizePipelineParadigm(paradigm?: unknown): PipelineParadigm {
    if (typeof paradigm === 'string') {
        const normalized = paradigm.trim().toLowerCase();
        if (normalized === 'sdp') {
            return 'sdp';
        }
        if (normalized === 'notebook') {
            return 'notebook';
        }
    }
    return DEFAULT_PIPELINE_PARADIGM;
}

/**
 * Dataset micro-badge rendered on canvas nodes in Spark Declarative Pipelines mode.
 */
export type SdpDatasetBadge = 'VIEW' | 'MV' | 'TABLE' | 'CDC' | 'SINK';

/**
 * Canonical Spark Declarative Pipelines dataset or flow registration kind.
 */
export type SdpDatasetKind =
    | 'temporary_view'
    | 'materialized_view'
    | 'streaming_table'
    | 'append_flow'
    | 'auto_cdc'
    | 'external_sink';

/**
 * BigLake Iceberg REST catalog integration mode for SDP pipelines.
 */
export type SdpBigLakeMode = 'lakehouse' | 'gcs_bucket' | 'custom' | 'none';

/**
 * Dataset refresh execution mode passed to the spark-pipelines CLI.
 */
export type SdpRefreshMode = 'incremental' | 'refresh' | 'full_refresh' | 'full_refresh_all';

/**
 * Subcommand mode executed by the spark-pipelines CLI.
 */
export type SdpCommandMode = 'run' | 'dry-run';

/**
 * Pipeline-level Spark Declarative Pipelines configuration fields stored in jobConfig.
 */
export interface SdpJobConfigFields {
    /** Checkpoint and state root URI required by the SDP pipeline spec. */
    sdpStorage?: string;
    /** Plain Spark SQL catalog alias used by SDP datasets. */
    sdpCatalog?: string;
    /** Target database or namespace name inside the configured catalog. */
    sdpDatabase?: string;
    /** BigLake Iceberg REST catalog configuration mode. */
    sdpBigLakeMode?: SdpBigLakeMode;
    /** Google Cloud project ID hosting the BigLake catalog. */
    sdpBigLakeProject?: string;
    /** BigLake Lakehouse catalog identifier for downstream four-part references. */
    sdpBigLakeCatalogId?: string;
    /** Bucket-root URI for Cloud Storage BigLake REST catalogs. */
    sdpBigLakeWarehouseBucket?: string;
    /** Static Spark properties routed to runner config instead of the pipeline spec. */
    sdpStaticSparkConf?: Record<string, string> | string;
    /** CLI command mode ('run' or 'dry-run'). */
    sdpCommand?: SdpCommandMode;
    /** Dataset refresh strategy for triggered SDP runs. */
    sdpRefreshMode?: SdpRefreshMode;
    /** Persisted dataset names selected for targeted refresh. */
    sdpRefreshDatasets?: string[];
    /** Persisted dataset names selected for full reset and recomputation. */
    sdpFullRefreshDatasets?: string[];
    /** True when standalone runners execute CREATE NAMESPACE IF NOT EXISTS before running. */
    sdpCreateNamespace?: boolean;
}

/**
 * Tier classification for dynamically discovered Gemini code-generation models.
 */
export type CodegenModelTier = 'flash' | 'pro' | 'flash-lite' | 'other';

/**
 * Metadata for an available Gemini code-generation model discovered at runtime.
 */
export interface CodegenModelOption {
    /** Short canonical model identifier passed to Gemini (for example 'gemini-4-hydrogen' or 'gemini-3.8-flash'). */
    id: string;
    /** Human-readable display label (for example 'Gemini 4 Hydrogen' or 'Gemini 3.8 Flash'). */
    displayName: string;
    /** Optional model description from the provider catalog. */
    description?: string;
    /** Model tier classification used for Flash-default ranking. */
    tier: CodegenModelTier;
    /** Optional parsed version label (for example '4.0' or '3.8'). */
    version?: string;
    /** True when this model is the highest-ranked Flash-class default. */
    isDefault?: boolean;
}

