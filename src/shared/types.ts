/**
 * @fileoverview Pure TypeScript data contracts crossing the host<->webview IPC boundary.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Rules:
 * - Pure interfaces / types only (zero runtime overhead, no enums).
 * - Zero dependencies on vscode, node, or react.
 */

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
