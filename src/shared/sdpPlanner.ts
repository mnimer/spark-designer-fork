/**
 * @fileoverview Deterministic Compatibility Gate and Dataset Planner for Spark Declarative Pipelines.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * NOTE: Keep this module free of host or browser imports such as vscode, fs,
 * path, child_process, os, react, or node built-ins. Both the VS Code extension
 * host and the Vite webview bundle compile this file directly.
 *
 * Purpose:
 * Evaluates Apache Spark pipeline graphs against Apache Spark 4.2 Spark Declarative
 * Pipelines (pyspark.pipelines) rules and maps compatible nodes to deterministic
 * dataset and flow constructs: @dp.temporary_view, @dp.materialized_view, @dp.table,
 * dp.create_streaming_table with @dp.append_flow, dp.create_auto_cdc_flow, and
 * dp.create_sink.
 */

import type { DagEdge, DagNode } from './dagValidator';
import {
    computeStreamingNodeSet,
    getNodeCategory,
    getNodeSparkTransform,
    isJoinNode,
    isMergeCdcScdNode,
} from './sparkDagRules';
import { getNodeCapabilities, SPARK_NODE_CAPABILITIES } from './sparkNodeCapabilities';
import {
    DEFAULT_PIPELINE_PARADIGM,
    type PipelineParadigm,
    type SdpDatasetBadge,
    type SdpDatasetKind,
} from './types';
import {
    isRuntimeSdpCapable,
    resolvePipelineRuntimeArtifacts,
    type PipelineRuntimeArtifacts,
} from '../spark/versionMatrix';

export { DEFAULT_PIPELINE_PARADIGM };
export type {
    PipelineParadigm,
    PipelineRuntimeArtifacts,
    SdpDatasetBadge,
    SdpDatasetKind,
};

/**
 * Blocking compatibility issue preventing execution or generation in SDP mode.
 */
export interface SdpBlockingIssue {
    /** Stable machine-readable SDP validation error code. */
    code: string;
    /** Human-readable explanation describing why SDP rejects the construct. */
    message: string;
    /** Optional graph node identifier responsible for the issue. */
    nodeId?: string;
    /** Optional human-readable node label shown in compatibility dialogs. */
    nodeLabel?: string;
    /** Optional canonical Spark transform name on the blocking node. */
    sparkTransform?: string;
}

/**
 * Result of checking whether a single palette or graph node is supported in SDP mode.
 */
export interface SdpNodeCompatibility {
    /** True when Spark Declarative Pipelines can express the node. */
    supported: boolean;
    /** Explanatory tooltip or error reason when supported is false. */
    reason?: string;
    /** Stable error code when supported is false. */
    code?: string;
}

/**
 * Deterministic SDP dataset or flow assignment for a single pipeline node.
 */
export interface SdpPlannedDataset {
    /** Graph node identifier. */
    nodeId: string;
    /** Human-readable node label from the canvas. */
    nodeLabel: string;
    /** Canonical Spark transform identifier. */
    sparkTransform: string;
    /** Unique snake_case SDP dataset, flow, or sink identifier. */
    datasetName: string;
    /** SDP registration construct assigned to this node. */
    kind: SdpDatasetKind;
    /** Compact micro-badge rendered on the canvas node in SDP mode. */
    badge: SdpDatasetBadge;
    /** True when the dataset or flow consumes a streaming DataFrame. */
    isStreaming: boolean;
    /** Storage or connector format passed to SDP decorators or dp.create_sink. */
    format?: string;
    /** Upstream SDP dataset names read by this node. */
    upstreamDatasets: string[];
    /** Target streaming table or sink name for append_flow and auto_cdc nodes. */
    targetDataset?: string;
    /** Companion quarantine dataset name generated for DataQualityAssert nodes. */
    quarantineDatasetName?: string;
    /** False when pipelines.reset.allowed=false protects this streaming table. */
    resetAllowed: boolean;
    /** Optional identity partition columns configured on the dataset. */
    partitionCols?: string[];
    /** Optional clustering columns configured on the dataset. */
    clusterBy?: string[];
}

/**
 * Complete deterministic Spark Declarative Pipelines execution plan and validation report.
 */
export interface SdpExecutionPlan {
    /** True when zero blocking issues exist for SDP execution. */
    isCompatible: boolean;
    /** Ordered blocking compatibility issues. */
    blockingIssues: SdpBlockingIssue[];
    /** Non-blocking operational warnings for SDP mode. */
    warnings: string[];
    /** Topologically ordered dataset plans across the pipeline graph. */
    datasets: SdpPlannedDataset[];
    /** Lookup map from node identifier to planned SDP dataset metadata. */
    datasetsByNodeId: Record<string, SdpPlannedDataset>;
    /** Topologically sorted node identifiers. */
    sortedNodeIds: string[];
    /** Resolved Spark runtime profile, connectors, and dependency artifacts. */
    runtimeArtifacts: PipelineRuntimeArtifacts;
    /** True when at least one node maps to dp.create_sink. */
    hasExternalSinks: boolean;
    /** Dataset names of all external dp.create_sink targets. */
    externalSinkNames: string[];
    /** Dataset names of all @dp.materialized_view datasets. */
    materializedViewNames: string[];
    /** Dataset names of all persisted streaming tables. */
    streamingTableNames: string[];
}

/**
 * Options controlling SDP compatibility and planning checks.
 */
export interface SdpPlannerOptions {
    /** Enforce Spark 4.2+ runtime profile validation. */
    enforceRuntimeVersion?: boolean;
}

const IMPERATIVE_ML_TRANSFORMS = new Set<string>([
    'FeaturePrep',
    'SparkMlAndVertexAi',
    'SparkMlInference',
    'VertexAiInference',
]);

const LAKEHOUSE_CATALOG_SINKS = new Set<string>([
    'IcebergSink',
    'DeltaSink',
    'HudiSink',
    'IcebergIO',
]);

const MERGE_CAPABLE_SINKS = new Set<string>([
    'IcebergSink',
    'DeltaSink',
    'IcebergIO',
]);

const MERGE_CAPABLE_FORMATS = new Set<string>(['iceberg', 'delta']);

const BATCH_MATERIALIZED_VIEW_STEPS = new Set<string>([
    'GroupByAgg',
    'RollupCube',
    'TopN',
    'WindowOver',
    'Deduplicate',
    'SetOperation',
]);

const EXTERNAL_SINK_FORMAT_BY_TRANSFORM: Readonly<Record<string, string>> = {
    KafkaSink: 'kafka',
    KafkaIO: 'kafka',
    EventHubsSink: 'kafka',
    PulsarSink: 'pulsar',
    PulsarIO: 'pulsar',
    BigQuerySink: 'bigquery',
    BigQueryIO: 'bigquery',
    ParquetSink: 'parquet',
    ParquetIO: 'parquet',
    OrcSink: 'orc',
    AvroSink: 'avro',
    AvroIO: 'avro',
    CsvSink: 'csv',
    CsvIO: 'csv',
    JsonSink: 'json',
    JsonIO: 'json',
    TextSink: 'text',
    TextIO: 'text',
    ConsoleSink: 'console',
    MemorySink: 'memory',
    MongoDbSink: 'mongodb',
    MongoDbIO: 'mongodb',
    CassandraSink: 'cassandra',
    CassandraIO: 'cassandra',
    ElasticsearchSink: 'es',
    ElasticsearchIO: 'es',
    Neo4jSink: 'neo4j',
    Neo4jIO: 'neo4j',
};

const LAKEHOUSE_FORMAT_BY_TRANSFORM: Readonly<Record<string, string>> = {
    IcebergSink: 'iceberg',
    IcebergSource: 'iceberg',
    IcebergIO: 'iceberg',
    DeltaSink: 'delta',
    DeltaSource: 'delta',
    HudiSink: 'hudi',
    HudiSource: 'hudi',
};

const STREAMING_STATEFUL_DOWNSTREAM_STEPS = new Set<string>([
    'Join',
    'Deduplicate',
    'StatefulTransform',
    'GroupByAgg',
    'RollupCube',
]);

interface NormalizedEdge {
    id?: string;
    source: string;
    target: string;
    sourceHandle?: string | null;
    targetHandle?: string | null;
}

/**
 * Narrows an unknown value to a plain key-value record.
 *
 * @param value Candidate value to inspect.
 * @returns Plain object record or undefined.
 */
function asRecord(value: unknown): Record<string, unknown> | undefined {
    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
        return value as Record<string, unknown>;
    }
    return undefined;
}

/**
 * Reads a trimmed non-empty string from an unknown value.
 *
 * @param value Candidate string value.
 * @returns Trimmed non-empty string or undefined.
 */
function readNonEmptyString(value: unknown): string | undefined {
    if (typeof value === 'string') {
        const trimmed = value.trim();
        if (trimmed.length > 0) {
            return trimmed;
        }
    }
    return undefined;
}

/**
 * Extracts configuration properties from a React Flow or YAML-shaped DAG node.
 *
 * @param node Pipeline DAG node.
 * @returns Node properties record or empty object.
 */
function getNodeProperties(node: DagNode): Record<string, unknown> {
    const dataRecord = asRecord(node.data);
    const nodeRecord = asRecord(node);
    return asRecord(dataRecord?.properties) ?? asRecord(nodeRecord?.properties) ?? {};
}

/**
 * Extracts a human-readable display label from a DAG node.
 *
 * @param node Pipeline DAG node.
 * @returns Trimmed node label or stringified node identifier.
 */
function getNodeLabel(node: DagNode): string {
    const dataRecord = asRecord(node.data);
    return (
        readNonEmptyString(dataRecord?.label) ??
        readNonEmptyString((node as Record<string, unknown>).label) ??
        String(node.id)
    );
}

/**
 * Strips optional SOURCE:, SINK:, or STEP: prefixes from a transform identifier.
 *
 * @param rawTransform Raw transform string.
 * @returns Bare transform name and optional prefix category.
 */
function splitTransformPrefix(rawTransform?: string): {
    bareTransform: string;
    prefixType?: 'SOURCE' | 'STEP' | 'SINK';
} {
    const trimmed = (rawTransform ?? '').trim();
    const colonIdx = trimmed.indexOf(':');
    if (colonIdx > 0) {
        const prefix = trimmed.slice(0, colonIdx).trim().toUpperCase();
        if (prefix === 'SOURCE' || prefix === 'STEP' || prefix === 'SINK') {
            return {
                bareTransform: trimmed.slice(colonIdx + 1).trim(),
                prefixType: prefix,
            };
        }
    }
    return { bareTransform: trimmed };
}

/**
 * Parses a string array or comma-separated string into trimmed non-empty column names.
 *
 * @param raw Candidate array or comma-delimited string.
 * @returns Array of trimmed non-empty strings.
 */
function parseStringList(raw: unknown): string[] {
    if (Array.isArray(raw)) {
        return raw
            .map((entry) => String(entry ?? '').trim())
            .filter((entry) => entry.length > 0);
    }
    if (typeof raw === 'string' && raw.trim().length > 0) {
        return raw
            .split(',')
            .map((part) => part.trim())
            .filter((part) => part.length > 0);
    }
    return [];
}

/**
 * Sanitizes an arbitrary label or table name into a valid SDP snake_case identifier.
 * Strips invalid characters and removes the reserved __spark_autocdc_ prefix.
 *
 * @param raw Raw candidate identifier string.
 * @param fallback Optional fallback identifier when raw sanitizes to empty.
 * @returns Valid plain snake_case identifier matching [a-z_][a-z0-9_]*.
 */
export function sanitizeSdpIdentifier(raw: string, fallback = 'dataset'): string {
    const cleanCandidate = (input: string): string => {
        let current = input
            .trim()
            .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
            .toLowerCase();

        // Strip reserved Auto CDC prefix before and after normalizing separators.
        while (/^_*spark_autocdc_*/.test(current)) {
            current = current.replace(/^_*spark_autocdc_*/, '');
        }

        current = current
            .replace(/[^a-z0-9_]+/g, '_')
            .replace(/_+/g, '_')
            .replace(/^_+|_+$/g, '');

        while (/^_*spark_autocdc_*/.test(current)) {
            current = current.replace(/^_*spark_autocdc_*/, '').replace(/^_+|_+$/g, '');
        }

        if (current.length === 0) {
            return '';
        }
        if (/^[0-9]/.test(current)) {
            current = `_${current}`;
        }
        return current;
    };

    const primary = cleanCandidate(typeof raw === 'string' ? raw : '');
    if (primary.length > 0) {
        return primary;
    }

    const secondary = cleanCandidate(typeof fallback === 'string' ? fallback : '');
    return secondary.length > 0 ? secondary : 'dataset';
}

/**
 * Checks whether a palette item or configured node is supported in SDP mode.
 *
 * @param sparkTransform Spark transform identifier on the node or palette item.
 * @param nodeType Optional node category ('SOURCE', 'STEP', or 'SINK').
 * @param properties Optional persisted node properties when inspecting a canvas node.
 * @returns Compatibility result with reason and error code when unsupported.
 */
export function checkNodeSdpCompatibility(
    sparkTransform?: string,
    nodeType?: string,
    properties?: Record<string, unknown>
): SdpNodeCompatibility {
    const { bareTransform, prefixType } = splitTransformPrefix(sparkTransform);
    if (!bareTransform) {
        return { supported: true };
    }

    const explicitType = nodeType?.trim().toUpperCase();
    const effectiveType =
        explicitType === 'SOURCE' || explicitType === 'STEP' || explicitType === 'SINK'
            ? explicitType
            : prefixType;

    // 1. Block Spark ML and imperative-only caching steps.
    if (IMPERATIVE_ML_TRANSFORMS.has(bareTransform)) {
        return {
            supported: false,
            code: 'SDP_SPARK_ML_UNSUPPORTED',
            reason: `${bareTransform} requires imperative Spark ML or model execution, which Spark Declarative Pipelines does not support.`,
        };
    }

    if (bareTransform === 'CachePersist') {
        return {
            supported: false,
            code: 'SDP_CACHE_PERSIST_UNSUPPORTED',
            reason:
                'CachePersist uses imperative DataFrame caching, whereas Spark Declarative Pipelines manages persistence automatically.',
        };
    }

    // 2. Block foreachBatch-only sinks.
    const isKnownSinkOnly =
        effectiveType === 'SINK' ||
        (!effectiveType &&
            (bareTransform.endsWith('Sink') ||
                (SPARK_NODE_CAPABILITIES[bareTransform]?.streamingSinkMode === 'foreachBatch' &&
                    !SPARK_NODE_CAPABILITIES[bareTransform]?.supportedReadModes)));

    if (isKnownSinkOnly) {
        const scopedKey = effectiveType ? `${effectiveType}:${bareTransform}` : bareTransform;
        const knownEntry =
            SPARK_NODE_CAPABILITIES[scopedKey] ??
            SPARK_NODE_CAPABILITIES[sparkTransform ?? ''] ??
            SPARK_NODE_CAPABILITIES[bareTransform];
        if (knownEntry && getNodeCapabilities(sparkTransform, 'SINK').streamingSinkMode === 'foreachBatch') {
            return {
                supported: false,
                code: 'SDP_FOREACH_BATCH_SINK_UNSUPPORTED',
                reason: `${bareTransform} relies on imperative foreachBatch micro-batch writes, which Spark Declarative Pipelines does not support.`,
            };
        }
    }

    // 3. Block MergeCdcScd only when configured for SCD Type 2.
    if (bareTransform === 'MergeCdcScd' && properties !== undefined) {
        const scdType = readNonEmptyString(properties.scdType)?.toLowerCase();
        const storedAsScd = properties.storedAsScdType;
        if (scdType === 'scd2' || storedAsScd === 2 || storedAsScd === '2') {
            return {
                supported: false,
                code: 'AUTOCDC_SCD2_NOT_SUPPORTED',
                reason:
                    'Apache Spark 4.2 Auto CDC supports SCD Type 1 only (AUTOCDC_SCD2_NOT_SUPPORTED). Switch to Imperative mode for SCD Type 2.',
            };
        }
    }

    // 4. Block PivotUnpivot when operation is pivot and explicit pivotValues are missing.
    if (bareTransform === 'PivotUnpivot' && properties !== undefined) {
        const operation = (readNonEmptyString(properties.operation) ?? 'pivot').toLowerCase();
        if (operation === 'pivot') {
            const pivotValues = parseStringList(properties.pivotValues ?? properties.values);
            if (pivotValues.length === 0) {
                return {
                    supported: false,
                    code: 'SDP_PIVOT_VALUES_REQUIRED',
                    reason:
                        'Pivot in Spark Declarative Pipelines requires explicit pivotValues because dataset functions cannot run eager distinct queries.',
                };
            }
        }
    }

    return { supported: true };
}

/**
 * Resolves whether a streaming table permits full-refresh resets.
 *
 * @param props Node configuration properties.
 * @returns False when pipelines.reset.allowed is explicitly disabled, otherwise true.
 */
function resolveResetAllowed(props: Record<string, unknown>): boolean {
    if (props.sdpResetAllowed === false || props.sdpResetAllowed === 'false') {
        return false;
    }
    if (props.resetAllowed === false || props.resetAllowed === 'false') {
        return false;
    }

    for (const key of ['tableProperties', 'sdpTableProperties']) {
        const rawProps = props[key];
        if (typeof rawProps === 'string') {
            if (/['"]?pipelines\.reset\.allowed['"]?\s*[:=]\s*['"]?false['"]?/i.test(rawProps)) {
                return false;
            }
        } else {
            const record = asRecord(rawProps);
            if (record) {
                const resetVal = record['pipelines.reset.allowed'];
                if (resetVal === false || String(resetVal ?? '').trim().toLowerCase() === 'false') {
                    return false;
                }
            }
        }
    }

    return true;
}

/**
 * Extracts the unqualified table segment when a user enters db.table or catalog.db.table.
 *
 * @param raw Raw table name property.
 * @returns Final table segment string.
 */
function extractTableLeaf(raw: string): string {
    const parts = raw
        .split('.')
        .map((segment) => segment.trim())
        .filter((segment) => segment.length > 0);
    return parts.length > 0 ? parts[parts.length - 1] : raw;
}

/**
 * Allocates a unique SDP dataset identifier within the pipeline namespace.
 *
 * @param preferred Preferred raw identifier string.
 * @param fallback Fallback raw identifier string.
 * @param used Set of already allocated identifiers.
 * @returns Unique sanitized snake_case dataset identifier.
 */
function allocateUniqueDatasetName(
    preferred: string,
    fallback: string,
    used: Set<string>
): string {
    const base = sanitizeSdpIdentifier(preferred, fallback);
    if (!used.has(base)) {
        used.add(base);
        return base;
    }
    let counter = 2;
    while (used.has(`${base}_${counter}`)) {
        counter += 1;
    }
    const disambiguated = `${base}_${counter}`;
    used.add(disambiguated);
    return disambiguated;
}

/**
 * Determines the preferred base dataset name for a node before collision disambiguation.
 *
 * @param node Pipeline DAG node.
 * @param role Node role ('SOURCE' | 'STEP' | 'SINK').
 * @param sparkTransform Canonical Spark transform name.
 * @returns Raw candidate dataset name.
 */
function getPreferredDatasetCandidate(
    node: DagNode,
    role: 'SOURCE' | 'STEP' | 'SINK',
    sparkTransform: string
): string {
    const props = getNodeProperties(node);
    const explicitSdp =
        readNonEmptyString(props.sdpDatasetName) ??
        readNonEmptyString(props.sdpTableName) ??
        readNonEmptyString(props.sdpSinkName);
    if (explicitSdp) {
        return extractTableLeaf(explicitSdp);
    }

    if (role === 'SINK') {
        const sinkTable =
            readNonEmptyString(props.table) ??
            readNonEmptyString(props.tableName) ??
            readNonEmptyString(props.queryName) ??
            readNonEmptyString(props.topic);
        if (sinkTable) {
            return extractTableLeaf(sinkTable);
        }
    }

    const label = getNodeLabel(node);
    if (label.length > 0) {
        return label;
    }

    return sparkTransform || String(node.id);
}

/**
 * Resolves the SDP storage or sink format string for a planned node.
 *
 * @param sparkTransform Canonical Spark transform name.
 * @param kind Assigned SDP dataset kind.
 * @param props Node configuration properties.
 * @param defaultTableFormat Default lakehouse table format when Iceberg or BigLake is active.
 * @returns Normalized format string or undefined for temporary views and standard Spark tables.
 */
function resolveDatasetFormat(
    sparkTransform: string,
    kind: SdpDatasetKind,
    props: Record<string, unknown>,
    defaultTableFormat?: string
): string | undefined {
    if (kind === 'temporary_view') {
        return undefined;
    }

    const explicitFormat = readNonEmptyString(props.sdpFormat);
    if (explicitFormat) {
        return explicitFormat.toLowerCase();
    }

    const { bareTransform } = splitTransformPrefix(sparkTransform);
    if (kind === 'external_sink') {
        return (
            EXTERNAL_SINK_FORMAT_BY_TRANSFORM[bareTransform] ??
            readNonEmptyString(props.format)?.toLowerCase() ??
            'parquet'
        );
    }

    return LAKEHOUSE_FORMAT_BY_TRANSFORM[bareTransform] ?? defaultTableFormat;
}

/**
 * Maps an SDP dataset kind to its canvas micro-badge label.
 *
 * @param kind Planned SDP dataset kind.
 * @returns Corresponding SdpDatasetBadge token.
 */
function badgeForDatasetKind(kind: SdpDatasetKind): SdpDatasetBadge {
    switch (kind) {
        case 'temporary_view':
            return 'VIEW';
        case 'materialized_view':
            return 'MV';
        case 'streaming_table':
        case 'append_flow':
            return 'TABLE';
        case 'auto_cdc':
            return 'CDC';
        case 'external_sink':
            return 'SINK';
    }
}

/**
 * Normalizes default runtime settings for Spark Declarative Pipelines.
 * If the runtime version is unset or uses a default placeholder, upgrade it to the matching Spark 4.2 profile.
 * Explicit pre-4.2 versions remain unchanged so compatibility checks reject downgrades.
 *
 * @param jobConfig Optional pipeline deployment configuration map.
 * @returns Normalized copy of the deployment configuration for SDP mode.
 */
export function normalizeSdpJobConfig<T extends Record<string, unknown> = Record<string, unknown>>(
    jobConfig?: T
): T {
    const base: Record<string, unknown> = jobConfig ? { ...jobConfig } : {};
    const rawRunner = readNonEmptyString(base.runner) ?? '';
    const lowerRunner = rawRunner.toLowerCase();
    const rawVersion =
        readNonEmptyString(base.runtimeVersion) ??
        readNonEmptyString(base.version) ??
        readNonEmptyString(base.imageVersion) ??
        '';
    const lowerVersion = rawVersion.toLowerCase();

    if (
        lowerRunner === 'serverless_4.0' ||
        lowerRunner === 'serverless_3.0' ||
        lowerRunner === 'cluster_4.0' ||
        lowerRunner === 'cluster_3.0' ||
        lowerRunner === 'local_4.2_connect'
    ) {
        return base as unknown as T;
    }

    if (
        lowerRunner === 'dataproccluster' ||
        lowerRunner === 'dataproc_cluster' ||
        lowerRunner === 'dataproc' ||
        lowerRunner === 'cluster'
    ) {
        if (
            lowerVersion === '' ||
            lowerVersion === '4.2-connect' ||
            lowerVersion === '4.2_connect'
        ) {
            return {
                ...base,
                runtimeVersion: '4.0',
            } as unknown as T;
        }
        return base as unknown as T;
    }

    if (
        lowerRunner === 'serverlessspark' ||
        lowerRunner === 'serverless_spark' ||
        lowerRunner === 'serverless'
    ) {
        if (
            lowerVersion === '' ||
            lowerVersion === '4.2-connect' ||
            lowerVersion === '4.2_connect'
        ) {
            return {
                ...base,
                runtimeVersion: '4.0',
            } as unknown as T;
        }
        return base as unknown as T;
    }

    const canonicalLocalRunner = lowerRunner === 'local' ? rawRunner : 'Local';
    if (lowerVersion === '' || lowerVersion === '4.0') {
        return {
            ...base,
            runner: canonicalLocalRunner,
            runtimeVersion: '4.2-connect',
            useSparkConnect: true,
        } as unknown as T;
    }
    if (lowerVersion === '4.2-connect' || lowerVersion === '4.2_connect') {
        return {
            ...base,
            runner: canonicalLocalRunner,
            runtimeVersion: '4.2-connect',
            useSparkConnect: true,
        } as unknown as T;
    }
    if (lowerRunner !== '' && lowerRunner !== 'local') {
        return {
            ...base,
            runner: 'Local',
        } as unknown as T;
    }
    return base as unknown as T;
}

/**
 * Evaluates whether a pipeline graph can switch to or run in SDP mode.
 * Defaults enforceRuntimeVersion to false so switching paradigms on the canvas inspects
 * graph semantics before upgrading runner settings.
 *
 * @param nodes Pipeline DAG nodes.
 * @param edges Pipeline DAG edges.
 * @param jobConfig Optional pipeline deployment and SDP settings.
 * @param options Optional compatibility evaluation flags.
 * @returns Compatibility flag, blocking issues, and operational warnings.
 */
export function evaluateSdpCompatibility(
    nodes: readonly DagNode[],
    edges: readonly DagEdge[],
    jobConfig?: Record<string, unknown>,
    options?: SdpPlannerOptions
): {
    isCompatible: boolean;
    blockingIssues: SdpBlockingIssue[];
    warnings: string[];
} {
    const plan = buildSdpPlan(nodes, edges, jobConfig, {
        enforceRuntimeVersion: options?.enforceRuntimeVersion ?? false,
    });
    return {
        isCompatible: plan.isCompatible,
        blockingIssues: plan.blockingIssues,
        warnings: plan.warnings,
    };
}

/**
 * Computes a deterministic Spark Declarative Pipelines execution plan and compatibility report.
 * Enforces Spark 4.2+ runtime checks by default whenever jobConfig specifies a runner or version.
 *
 * @param nodes Pipeline DAG nodes.
 * @param edges Pipeline DAG edges.
 * @param jobConfig Optional pipeline deployment and SDP configuration.
 * @param options Optional planner flags.
 * @returns Complete SdpExecutionPlan with blocking issues, warnings, and dataset mappings.
 */
export function buildSdpPlan(
    nodes: readonly DagNode[],
    edges: readonly DagEdge[],
    jobConfig?: Record<string, unknown>,
    options?: SdpPlannerOptions
): SdpExecutionPlan {
    const validNodes = (nodes ?? []).filter(
        (node): node is DagNode => Boolean(node && node.id !== undefined && node.id !== null)
    );
    const nodeMap = new Map<string, DagNode>();
    const nodeIds: string[] = [];
    for (const node of validNodes) {
        const id = String(node.id);
        nodeMap.set(id, node);
        nodeIds.push(id);
    }

    const inDegree = new Map<string, number>();
    const incomingEdges = new Map<string, NormalizedEdge[]>();
    const outgoingEdges = new Map<string, NormalizedEdge[]>();
    const validEdges: DagEdge[] = [];

    for (const id of nodeIds) {
        inDegree.set(id, 0);
        incomingEdges.set(id, []);
        outgoingEdges.set(id, []);
    }

    for (const edge of edges ?? []) {
        if (!edge || edge.source === undefined || edge.target === undefined) {
            continue;
        }
        const src = String(edge.source);
        const tgt = String(edge.target);
        if (!nodeMap.has(src) || !nodeMap.has(tgt)) {
            continue;
        }
        validEdges.push(edge);
        inDegree.set(tgt, (inDegree.get(tgt) ?? 0) + 1);
        const normalized: NormalizedEdge = {
            id: edge.id !== undefined && edge.id !== null ? String(edge.id) : undefined,
            source: src,
            target: tgt,
            sourceHandle: edge.sourceHandle,
            targetHandle: edge.targetHandle,
        };
        outgoingEdges.get(src)?.push(normalized);
        incomingEdges.get(tgt)?.push(normalized);
    }

    // 1. Topological sort (Kahn's algorithm) with terminal sinks ordered last.
    const queue: string[] = [];
    for (const id of nodeIds) {
        if ((inDegree.get(id) ?? 0) === 0) {
            queue.push(id);
        }
    }

    const rawSortedIds: string[] = [];
    let qHead = 0;
    while (qHead < queue.length) {
        const curr = queue[qHead];
        qHead += 1;
        rawSortedIds.push(curr);
        for (const edge of outgoingEdges.get(curr) ?? []) {
            const nextDeg = (inDegree.get(edge.target) ?? 0) - 1;
            inDegree.set(edge.target, nextDeg);
            if (nextDeg === 0) {
                queue.push(edge.target);
            }
        }
    }

    const hasCycles = rawSortedIds.length < nodeIds.length;
    const sortedSet = new Set(rawSortedIds);
    const nonTerminalIds: string[] = [];
    const terminalSinkIds: string[] = [];
    for (const id of rawSortedIds) {
        const node = nodeMap.get(id);
        const outCount = outgoingEdges.get(id)?.length ?? 0;
        if (node && getNodeCategory(node) === 'SINK' && outCount === 0) {
            terminalSinkIds.push(id);
        } else {
            nonTerminalIds.push(id);
        }
    }
    const sortedNodeIds = hasCycles ? [] : [...nonTerminalIds, ...terminalSinkIds];
    const evaluationOrderIds = hasCycles
        ? [...rawSortedIds, ...nodeIds.filter((id) => !sortedSet.has(id))]
        : sortedNodeIds;

    // 2. Resolve runtime profile and artifacts.
    const runtimeInputNodes = evaluationOrderIds.map((id) => {
        const node = nodeMap.get(id);
        return {
            sparkTransform: node ? getNodeSparkTransform(node) : undefined,
            properties: node ? getNodeProperties(node) : undefined,
        };
    });
    const runtimeArtifacts = resolvePipelineRuntimeArtifacts(runtimeInputNodes, {
        ...(jobConfig ?? {}),
        paradigm: 'sdp',
    });

    const blockingIssues: SdpBlockingIssue[] = [];
    const warnings: string[] = [];

    const pushBlockingIssue = (issue: SdpBlockingIssue): void => {
        const exists = blockingIssues.some(
            (existing) =>
                existing.code === issue.code &&
                existing.nodeId === issue.nodeId &&
                existing.message === issue.message
        );
        if (!exists) {
            blockingIssues.push(issue);
        }
    };

    const pushWarning = (warning: string): void => {
        if (!warnings.includes(warning)) {
            warnings.push(warning);
        }
    };

    if (hasCycles) {
        pushBlockingIssue({
            code: 'SDP_CYCLE_DETECTED',
            message:
                'Circular dependencies are not supported in Spark Declarative Pipelines.',
        });
    }

    // 3. Enforce Spark 4.2+ runtime support when requested or when runner/version is configured.
    const hasConfiguredRuntime = Boolean(
        readNonEmptyString(jobConfig?.runner) ||
            readNonEmptyString(jobConfig?.runtimeVersion) ||
            readNonEmptyString(jobConfig?.version) ||
            readNonEmptyString(jobConfig?.imageVersion)
    );
    const shouldEnforceRuntime = options?.enforceRuntimeVersion ?? hasConfiguredRuntime;

    if (shouldEnforceRuntime && !isRuntimeSdpCapable(runtimeArtifacts.profile)) {
        pushBlockingIssue({
            code: 'SDP_SPARK_42_REQUIRED',
            message: `Spark Declarative Pipelines requires Apache Spark 4.2 or newer (Serverless Spark 4.0, Dataproc Cluster 4.0, or Local 4.2 Spark Connect). Selected runtime "${runtimeArtifacts.profile.id}" runs Spark ${runtimeArtifacts.profile.sparkVersion}.`,
        });
    }

    if (runtimeArtifacts.missingLocalIcebergRuntime) {
        pushWarning(
            'MISSING_LOCAL_ICEBERG_RUNTIME: Local Spark 4.2 Spark Connect lacks iceberg-spark-runtime-4.2_2.13 on Maven Central. Run Iceberg SDP pipelines on Serverless Spark 4.0 or Dataproc Cluster 4.0.'
        );
    }

    // 4. Block continuous triggers at pipeline level.
    const defaultTrigger = readNonEmptyString(jobConfig?.defaultTriggerType)?.toLowerCase();
    if (defaultTrigger === 'continuous') {
        pushBlockingIssue({
            code: 'SDP_CONTINUOUS_TRIGGER_UNSUPPORTED',
            message:
                'Spark Declarative Pipelines runs triggered flows and does not support pipeline defaultTriggerType="continuous".',
        });
    }

    // 5. Compute initial streaming propagation for watermark and CDC checks.
    const streaming = computeStreamingNodeSet(validNodes, validEdges);

    const branchHasWatermark = (branchTipId: string): boolean => {
        if (!streaming.streamingNodeIds.has(branchTipId)) {
            return false;
        }
        const visited = new Set<string>();
        const walk: string[] = [branchTipId];
        let idx = 0;
        let sawWatermark = false;
        let reachedUnwatermarkedSource = false;

        while (idx < walk.length) {
            const currId = walk[idx];
            idx += 1;
            if (visited.has(currId)) {
                continue;
            }
            visited.add(currId);
            const currNode = nodeMap.get(currId);
            if (!currNode) {
                continue;
            }
            const currTransform = splitTransformPrefix(getNodeSparkTransform(currNode)).bareTransform;
            if (currTransform === 'Watermark') {
                sawWatermark = true;
                continue;
            }
            if (getNodeCategory(currNode) === 'SOURCE') {
                reachedUnwatermarkedSource = true;
                continue;
            }
            for (const parentEdge of incomingEdges.get(currId) ?? []) {
                if (
                    streaming.streamingNodeIds.has(parentEdge.source) &&
                    !visited.has(parentEdge.source)
                ) {
                    walk.push(parentEdge.source);
                }
            }
        }

        return sawWatermark && !reachedUnwatermarkedSource;
    };

    // 6. Validate per-node compatibility, triggers, partitioning/clustering, watermarks, and Auto CDC.
    const autoCdcSinkIds = new Set<string>();
    const autoCdcBySinkId = new Map<string, string[]>();

    for (const nodeId of evaluationOrderIds) {
        const node = nodeMap.get(nodeId);
        if (!node) {
            continue;
        }
        const role = getNodeCategory(node) ?? 'STEP';
        const rawTransform = getNodeSparkTransform(node) ?? '';
        const { bareTransform } = splitTransformPrefix(rawTransform);
        const props = getNodeProperties(node);
        const nodeLabel = getNodeLabel(node);

        // 6a. Node-level compatibility gate.
        const compat = checkNodeSdpCompatibility(rawTransform, role, props);
        if (!compat.supported) {
            pushBlockingIssue({
                code: compat.code ?? 'SDP_NODE_UNSUPPORTED',
                message:
                    compat.reason ??
                    `Node "${nodeLabel}" (${bareTransform || role}) is not supported in Spark Declarative Pipelines.`,
                nodeId,
                nodeLabel,
                sparkTransform: bareTransform || rawTransform,
            });
        }

        // 6b. Per-node continuous trigger check.
        const nodeTrigger = readNonEmptyString(props.triggerType)?.toLowerCase();
        if (nodeTrigger === 'continuous') {
            pushBlockingIssue({
                code: 'SDP_CONTINUOUS_TRIGGER_UNSUPPORTED',
                message: `Node "${nodeLabel}" sets triggerType="continuous", which Spark Declarative Pipelines does not support.`,
                nodeId,
                nodeLabel,
                sparkTransform: bareTransform || rawTransform,
            });
        }

        // 6c. Mutually exclusive partition_cols and cluster_by check.
        const partitionCols = parseStringList(props.sdpPartitionCols ?? props.partitionCols);
        const clusterBy = parseStringList(props.sdpClusterBy ?? props.clusterBy);
        if (partitionCols.length > 0 && clusterBy.length > 0) {
            pushBlockingIssue({
                code: 'SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED',
                message: `Node "${nodeLabel}" specifies both partition_cols and cluster_by (SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED).`,
                nodeId,
                nodeLabel,
                sparkTransform: bareTransform || rawTransform,
            });
        }

        // 6d. Streaming aggregations, Deduplicate, and stream-stream Joins require upstream Watermark.
        const inbound = incomingEdges.get(nodeId) ?? [];
        const explicitDatasetType = readNonEmptyString(props.sdpDatasetType)?.toLowerCase();

        if (
            (bareTransform === 'GroupByAgg' || bareTransform === 'RollupCube') &&
            streaming.streamingNodeIds.has(nodeId) &&
            explicitDatasetType !== 'materialized_view'
        ) {
            const streamingParents = inbound.filter((e) =>
                streaming.streamingNodeIds.has(e.source)
            );
            if (
                streamingParents.length > 0 &&
                streamingParents.some((e) => !branchHasWatermark(e.source))
            ) {
                pushBlockingIssue({
                    code: 'SDP_STREAMING_WATERMARK_REQUIRED',
                    message: `Streaming ${bareTransform} node "${nodeLabel}" requires an upstream Watermark step in Spark Declarative Pipelines.`,
                    nodeId,
                    nodeLabel,
                    sparkTransform: bareTransform,
                });
            }
        }

        if (
            bareTransform === 'Deduplicate' &&
            streaming.streamingNodeIds.has(nodeId) &&
            explicitDatasetType !== 'materialized_view'
        ) {
            const streamingParents = inbound.filter((e) =>
                streaming.streamingNodeIds.has(e.source)
            );
            if (
                streamingParents.length > 0 &&
                streamingParents.some((e) => !branchHasWatermark(e.source))
            ) {
                pushBlockingIssue({
                    code: 'SDP_STREAMING_WATERMARK_REQUIRED',
                    message: `Streaming Deduplicate node "${nodeLabel}" requires an upstream Watermark step to bound state in Spark Declarative Pipelines.`,
                    nodeId,
                    nodeLabel,
                    sparkTransform: bareTransform,
                });
            }
        }

        if (isJoinNode(node) && streaming.joinModes.get(nodeId) === 'stream_stream') {
            const leftEdges = inbound.filter((e) => e.targetHandle === 'left');
            const rightEdges = inbound.filter((e) => e.targetHandle === 'right');
            const candidates =
                leftEdges.length > 0 || rightEdges.length > 0
                    ? [...leftEdges, ...rightEdges]
                    : inbound;
            const missingWatermark = candidates.some(
                (edge) =>
                    streaming.streamingNodeIds.has(edge.source) &&
                    !branchHasWatermark(edge.source)
            );
            if (missingWatermark) {
                pushBlockingIssue({
                    code: 'SDP_STREAMING_WATERMARK_REQUIRED',
                    message: `Stream-stream Join node "${nodeLabel}" requires an upstream Watermark step on both streaming inputs in Spark Declarative Pipelines.`,
                    nodeId,
                    nodeLabel,
                    sparkTransform: 'Join',
                });
            }
        }

        // 6e. MergeCdcScd Auto CDC source, column identifier, and MERGE target rules.
        if (isMergeCdcScdNode(node)) {
            const leftEdges = inbound.filter((e) => e.targetHandle === 'left');
            const rightEdges = inbound.filter((e) => e.targetHandle === 'right');
            const untypedEdges = inbound.filter(
                (e) => e.targetHandle !== 'left' && e.targetHandle !== 'right'
            );
            const cdcSourceEdges = leftEdges.length > 0 ? leftEdges : untypedEdges;

            if (cdcSourceEdges.length === 0) {
                pushBlockingIssue({
                    code: 'AUTOCDC_MISSING_STREAMING_SOURCE',
                    message: `MergeCdcScd node "${nodeLabel}" requires a streaming CDC source connected to its "left" input handle.`,
                    nodeId,
                    nodeLabel,
                    sparkTransform: 'MergeCdcScd',
                });
            } else if (
                cdcSourceEdges.some((edge) => !streaming.streamingNodeIds.has(edge.source))
            ) {
                pushBlockingIssue({
                    code: 'AUTOCDC_SOURCE_MUST_BE_STREAMING',
                    message: `MergeCdcScd node "${nodeLabel}" requires a streaming input on its "left" handle because dp.create_auto_cdc_flow reads streaming sources only.`,
                    nodeId,
                    nodeLabel,
                    sparkTransform: 'MergeCdcScd',
                });
            }

            if (rightEdges.length > 0) {
                pushWarning(
                    `MergeCdcScd node "${nodeLabel}" ignores its "right" target snapshot input in SDP mode because dp.create_auto_cdc_flow manages the target streaming table directly.`
                );
            }

            const keyCols = parseStringList(props.keyColumns);
            const seqCol = readNonEmptyString(props.sequenceColumn);
            const checkColumns = [...keyCols, ...(seqCol ? [seqCol] : [])];
            for (const colName of checkColumns) {
                if (colName.includes('.')) {
                    pushBlockingIssue({
                        code: 'AUTOCDC_MULTIPART_COLUMN_IDENTIFIER',
                        message: `MergeCdcScd node "${nodeLabel}" column "${colName}" must be an unqualified column identifier (AUTOCDC_MULTIPART_COLUMN_IDENTIFIER).`,
                        nodeId,
                        nodeLabel,
                        sparkTransform: 'MergeCdcScd',
                    });
                }
                if (colName.toLowerCase().startsWith('__spark_autocdc_')) {
                    pushBlockingIssue({
                        code: 'AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT',
                        message: `MergeCdcScd node "${nodeLabel}" column "${colName}" cannot start with reserved prefix "__spark_autocdc_".`,
                        nodeId,
                        nodeLabel,
                        sparkTransform: 'MergeCdcScd',
                    });
                }
            }

            const outbound = outgoingEdges.get(nodeId) ?? [];
            const sinkTargets = outbound
                .map((e) => nodeMap.get(e.target))
                .filter((targetNode): targetNode is DagNode => Boolean(targetNode));

            if (sinkTargets.length === 0) {
                pushBlockingIssue({
                    code: 'AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE',
                    message: `MergeCdcScd node "${nodeLabel}" must connect to an IcebergSink or DeltaSink target table supporting MERGE (AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE).`,
                    nodeId,
                    nodeLabel,
                    sparkTransform: 'MergeCdcScd',
                });
            } else {
                for (const targetNode of sinkTargets) {
                    const targetRole = getNodeCategory(targetNode) ?? 'STEP';
                    const targetTransform = splitTransformPrefix(
                        getNodeSparkTransform(targetNode)
                    ).bareTransform;
                    const targetProps = getNodeProperties(targetNode);
                    const targetLabel = getNodeLabel(targetNode);
                    const targetExplicitFormat = readNonEmptyString(
                        targetProps.sdpFormat
                    )?.toLowerCase();
                    const isExternalOverride =
                        readNonEmptyString(targetProps.sdpSinkKind)?.toLowerCase() ===
                        'external_sink';

                    const supportsMerge =
                        targetRole === 'SINK' &&
                        !isExternalOverride &&
                        MERGE_CAPABLE_SINKS.has(targetTransform) &&
                        (!targetExplicitFormat || MERGE_CAPABLE_FORMATS.has(targetExplicitFormat));

                    if (!supportsMerge) {
                        pushBlockingIssue({
                            code: 'AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE',
                            message: `Auto CDC target "${targetLabel}" (${targetTransform || targetRole}) does not support MERGE (AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE). Connect MergeCdcScd to IcebergSink or DeltaSink.`,
                            nodeId: String(targetNode.id),
                            nodeLabel: targetLabel,
                            sparkTransform: targetTransform,
                        });
                    } else {
                        const sinkId = String(targetNode.id);
                        autoCdcSinkIds.add(sinkId);
                        const existingFlows = autoCdcBySinkId.get(sinkId) ?? [];
                        existingFlows.push(nodeId);
                        autoCdcBySinkId.set(sinkId, existingFlows);
                    }
                }
            }
        }
    }

    // 6f. Check AUTOCDC_MULTIPLE_FLOWS_TO_TARGET on Auto CDC target sinks.
    for (const sinkId of autoCdcSinkIds) {
        const sinkNode = nodeMap.get(sinkId);
        if (!sinkNode) {
            continue;
        }
        const sinkInbound = incomingEdges.get(sinkId) ?? [];
        const cdcFlows = autoCdcBySinkId.get(sinkId) ?? [];
        if (sinkInbound.length > 1 || cdcFlows.length > 1) {
            const sinkLabel = getNodeLabel(sinkNode);
            const sinkTransform = splitTransformPrefix(
                getNodeSparkTransform(sinkNode)
            ).bareTransform;
            pushBlockingIssue({
                code: 'AUTOCDC_MULTIPLE_FLOWS_TO_TARGET',
                message: `Auto CDC target table "${sinkLabel}" receives multiple incoming flows (AUTOCDC_MULTIPLE_FLOWS_TO_TARGET). Keep exactly one create_auto_cdc_flow per target table.`,
                nodeId: sinkId,
                nodeLabel: sinkLabel,
                sparkTransform: sinkTransform,
            });
        }
    }

    // 7. Deterministically map each node to its SDP dataset kind, name, format, and execution mode.
    const rawBigLakeMode = readNonEmptyString(jobConfig?.sdpBigLakeMode)?.toLowerCase();
    const hasExplicitBigLakeCatalog =
        rawBigLakeMode === 'lakehouse' ||
        rawBigLakeMode === 'gcs_bucket' ||
        rawBigLakeMode === 'custom';
    const graphHasIcebergNode = validNodes.some((n) => {
        const bt = splitTransformPrefix(getNodeSparkTransform(n)).bareTransform;
        if (
            bt === 'IcebergSource' ||
            bt === 'IcebergSink' ||
            bt === 'IcebergIO' ||
            /^Iceberg/i.test(bt)
        ) {
            return true;
        }
        const p = getNodeProperties(n);
        return readNonEmptyString(p.sdpFormat)?.toLowerCase() === 'iceberg';
    });
    const defaultLakehouseTableFormat =
        hasExplicitBigLakeCatalog || graphHasIcebergNode ? 'iceberg' : undefined;

    const usedDatasetNames = new Set<string>();
    const datasets: SdpPlannedDataset[] = [];
    const datasetsByNodeId: Record<string, SdpPlannedDataset> = {};
    const effectiveNodeStreaming = new Map<string, boolean>();

    // Pre-allocate dataset names so MergeCdcScd can reference its downstream target sink name.
    const preallocatedNameByNodeId = new Map<string, string>();
    const preallocatedQuarantineByNodeId = new Map<string, string>();

    for (const nodeId of evaluationOrderIds) {
        const node = nodeMap.get(nodeId);
        if (!node) {
            continue;
        }
        const role = getNodeCategory(node) ?? 'STEP';
        const rawTransform = getNodeSparkTransform(node) ?? '';
        const { bareTransform } = splitTransformPrefix(rawTransform);
        const preferred = getPreferredDatasetCandidate(node, role, bareTransform || rawTransform);
        const datasetName = allocateUniqueDatasetName(preferred, nodeId, usedDatasetNames);
        preallocatedNameByNodeId.set(nodeId, datasetName);

        if (bareTransform === 'DataQualityAssert') {
            const quarantineName = allocateUniqueDatasetName(
                `${datasetName}_quarantine`,
                `${nodeId}_quarantine`,
                usedDatasetNames
            );
            preallocatedQuarantineByNodeId.set(nodeId, quarantineName);
        }
    }

    // Detect duplicate explicit target table names across separate Auto CDC sink nodes.
    const seenExplicitAutoCdcTables = new Map<string, string>();
    for (const sinkId of autoCdcSinkIds) {
        const sinkNode = nodeMap.get(sinkId);
        if (!sinkNode) {
            continue;
        }
        const props = getNodeProperties(sinkNode);
        const explicitTable =
            readNonEmptyString(props.sdpTableName) ??
            readNonEmptyString(props.table) ??
            readNonEmptyString(props.tableName);
        if (explicitTable) {
            const normalized = sanitizeSdpIdentifier(extractTableLeaf(explicitTable));
            const firstSinkId = seenExplicitAutoCdcTables.get(normalized);
            if (firstSinkId && firstSinkId !== sinkId) {
                const sinkLabel = getNodeLabel(sinkNode);
                const sinkTransform = splitTransformPrefix(
                    getNodeSparkTransform(sinkNode)
                ).bareTransform;
                pushBlockingIssue({
                    code: 'AUTOCDC_MULTIPLE_FLOWS_TO_TARGET',
                    message: `Auto CDC target table "${normalized}" is targeted by multiple Auto CDC sinks (AUTOCDC_MULTIPLE_FLOWS_TO_TARGET).`,
                    nodeId: sinkId,
                    nodeLabel: sinkLabel,
                    sparkTransform: sinkTransform,
                });
            } else {
                seenExplicitAutoCdcTables.set(normalized, sinkId);
            }
        }
    }

    for (const nodeId of evaluationOrderIds) {
        const node = nodeMap.get(nodeId);
        if (!node) {
            continue;
        }
        const role = getNodeCategory(node) ?? 'STEP';
        const rawTransform = getNodeSparkTransform(node) ?? '';
        const { bareTransform } = splitTransformPrefix(rawTransform);
        const props = getNodeProperties(node);
        const nodeLabel = getNodeLabel(node);
        const inbound = incomingEdges.get(nodeId) ?? [];
        const outbound = outgoingEdges.get(nodeId) ?? [];

        // Compute whether upstream SDP datasets feed this node as a stream or as batch.
        // Reading an upstream materialized_view always produces a batch DataFrame in SDP.
        let inputIsStreaming = false;
        if (role === 'SOURCE') {
            inputIsStreaming = streaming.streamingNodeIds.has(nodeId);
        } else if (isMergeCdcScdNode(node)) {
            const leftEdges = inbound.filter((e) => e.targetHandle === 'left');
            const cdcEdges =
                leftEdges.length > 0
                    ? leftEdges
                    : inbound.filter((e) => e.targetHandle !== 'right');
            inputIsStreaming = cdcEdges.some(
                (e) =>
                    effectiveNodeStreaming.get(e.source) ??
                    streaming.streamingNodeIds.has(e.source)
            );
        } else {
            inputIsStreaming = inbound.some(
                (e) =>
                    effectiveNodeStreaming.get(e.source) ??
                    streaming.streamingNodeIds.has(e.source)
            );
        }

        const explicitDatasetType = readNonEmptyString(props.sdpDatasetType)?.toLowerCase();
        const explicitSinkKind = readNonEmptyString(props.sdpSinkKind)?.toLowerCase();

        let kind: SdpDatasetKind;
        let nodeIsStreaming = inputIsStreaming;
        let targetDataset: string | undefined;

        if (role === 'SOURCE') {
            if (explicitDatasetType === 'temporary_view') {
                kind = 'temporary_view';
            } else if (explicitDatasetType === 'materialized_view' && !inputIsStreaming) {
                kind = 'materialized_view';
                nodeIsStreaming = false;
            } else if (inputIsStreaming) {
                kind = 'streaming_table';
                nodeIsStreaming = true;
            } else {
                kind = 'temporary_view';
                nodeIsStreaming = false;
            }
        } else if (role === 'SINK') {
            if (autoCdcSinkIds.has(nodeId)) {
                kind = 'auto_cdc';
                nodeIsStreaming = true;
            } else if (
                LAKEHOUSE_CATALOG_SINKS.has(bareTransform) &&
                explicitSinkKind !== 'external_sink'
            ) {
                if (!inputIsStreaming) {
                    kind = 'materialized_view';
                    nodeIsStreaming = false;
                } else if (inbound.length > 1) {
                    kind = 'append_flow';
                    nodeIsStreaming = true;
                    targetDataset = preallocatedNameByNodeId.get(nodeId);
                } else {
                    kind = 'streaming_table';
                    nodeIsStreaming = true;
                }
            } else {
                kind = 'external_sink';
                nodeIsStreaming = true;
                targetDataset = preallocatedNameByNodeId.get(nodeId);
                if (!inputIsStreaming) {
                    pushWarning(
                        `External sink "${nodeLabel}" maps to dp.create_sink, which expects a streaming flow input in Spark Declarative Pipelines.`
                    );
                }
            }
        } else {
            // STEP node classification.
            if (isMergeCdcScdNode(node)) {
                kind = 'auto_cdc';
                nodeIsStreaming = true;
                const firstSinkEdge = outbound.find((e) => autoCdcSinkIds.has(e.target));
                if (firstSinkEdge) {
                    targetDataset = preallocatedNameByNodeId.get(firstSinkEdge.target);
                }
            } else if (explicitDatasetType === 'temporary_view') {
                kind = 'temporary_view';
            } else if (explicitDatasetType === 'materialized_view') {
                kind = 'materialized_view';
                nodeIsStreaming = false;
            } else if (explicitDatasetType === 'streaming_table' && inputIsStreaming) {
                kind = 'streaming_table';
                nodeIsStreaming = true;
            } else if (bareTransform === 'UnionByName' && inputIsStreaming) {
                kind = 'append_flow';
                nodeIsStreaming = true;
                targetDataset = preallocatedNameByNodeId.get(nodeId);
            } else if (bareTransform === 'GroupByAgg' || bareTransform === 'RollupCube') {
                const windowType = readNonEmptyString(props.windowType)?.toLowerCase();
                const hasStreamingWindow =
                    Boolean(windowType) && windowType !== 'none' && inputIsStreaming;
                if (hasStreamingWindow) {
                    kind = 'streaming_table';
                    nodeIsStreaming = true;
                } else {
                    kind = 'materialized_view';
                    nodeIsStreaming = false;
                }
            } else if (!inputIsStreaming) {
                if (
                    BATCH_MATERIALIZED_VIEW_STEPS.has(bareTransform) ||
                    isJoinNode(node) ||
                    (bareTransform === 'PivotUnpivot' &&
                        (readNonEmptyString(props.operation) ?? 'pivot').toLowerCase() === 'pivot')
                ) {
                    kind = 'materialized_view';
                } else {
                    kind = 'temporary_view';
                }
                nodeIsStreaming = false;
            } else if (bareTransform === 'Watermark') {
                const feedsStatefulStreamStep = outbound.some((edge) => {
                    const targetNode = nodeMap.get(edge.target);
                    if (!targetNode) {
                        return false;
                    }
                    const targetTransform = splitTransformPrefix(
                        getNodeSparkTransform(targetNode)
                    ).bareTransform;
                    if (targetTransform === 'GroupByAgg' || targetTransform === 'RollupCube') {
                        const targetProps = getNodeProperties(targetNode);
                        const wType = readNonEmptyString(targetProps.windowType)?.toLowerCase();
                        return Boolean(wType) && wType !== 'none';
                    }
                    return STREAMING_STATEFUL_DOWNSTREAM_STEPS.has(targetTransform);
                });
                kind = feedsStatefulStreamStep ? 'temporary_view' : 'streaming_table';
                nodeIsStreaming = true;
            } else {
                kind = 'streaming_table';
                nodeIsStreaming = true;
            }
        }

        effectiveNodeStreaming.set(nodeId, nodeIsStreaming);

        const datasetName = preallocatedNameByNodeId.get(nodeId) ?? sanitizeSdpIdentifier(nodeId);
        let format = resolveDatasetFormat(
            bareTransform || rawTransform,
            kind,
            props,
            defaultLakehouseTableFormat
        );
        if (isMergeCdcScdNode(node)) {
            const firstSinkEdge = outbound.find((e) => autoCdcSinkIds.has(e.target));
            const sinkNode = firstSinkEdge ? nodeMap.get(firstSinkEdge.target) : undefined;
            if (sinkNode) {
                const sinkTransform =
                    getNodeSparkTransform(sinkNode) ?? 'IcebergSink';
                format = resolveDatasetFormat(
                    sinkTransform,
                    'auto_cdc',
                    getNodeProperties(sinkNode),
                    defaultLakehouseTableFormat
                );
            }
        }

        const upstreamDatasets: string[] = [];
        const upstreamEdges = isMergeCdcScdNode(node)
            ? inbound.filter((e) => e.targetHandle !== 'right')
            : inbound;
        for (const edge of upstreamEdges) {
            const upName = preallocatedNameByNodeId.get(edge.source);
            if (upName && !upstreamDatasets.includes(upName)) {
                upstreamDatasets.push(upName);
            }
        }

        const partitionCols = parseStringList(props.sdpPartitionCols ?? props.partitionCols);
        const clusterBy = parseStringList(props.sdpClusterBy ?? props.clusterBy);
        const resetAllowed = kind === 'external_sink' ? true : resolveResetAllowed(props);
        const quarantineDatasetName = preallocatedQuarantineByNodeId.get(nodeId);

        const planned: SdpPlannedDataset = {
            nodeId,
            nodeLabel,
            sparkTransform: bareTransform || rawTransform,
            datasetName,
            kind,
            badge: badgeForDatasetKind(kind),
            isStreaming: nodeIsStreaming,
            ...(format ? { format } : {}),
            upstreamDatasets,
            ...(targetDataset ? { targetDataset } : {}),
            ...(quarantineDatasetName ? { quarantineDatasetName } : {}),
            resetAllowed,
            ...(partitionCols.length > 0 ? { partitionCols } : {}),
            ...(clusterBy.length > 0 ? { clusterBy } : {}),
        };

        datasets.push(planned);
        datasetsByNodeId[nodeId] = planned;
    }

    // 8. Validate INVALID_RESETTABLE_DEPENDENCY across persisted streaming tables.
    const isPersistedStreamingDataset = (ds: SdpPlannedDataset | undefined): boolean => {
        if (!ds) {
            return false;
        }
        return (
            ds.kind === 'streaming_table' ||
            ds.kind === 'append_flow' ||
            (ds.kind === 'auto_cdc' && !ds.targetDataset)
        );
    };

    for (const dataset of datasets) {
        const isProtectedStreamingTable =
            (dataset.kind === 'streaming_table' ||
                dataset.kind === 'append_flow' ||
                dataset.kind === 'auto_cdc') &&
            !dataset.resetAllowed;
        if (!isProtectedStreamingTable) {
            continue;
        }

        // Walk upstream across streaming edges (including through temporary views) to find resettable tables.
        const visited = new Set<string>();
        const walkQueue: string[] = [dataset.nodeId];
        let head = 0;

        while (head < walkQueue.length) {
            const currId = walkQueue[head];
            head += 1;
            const currNode = nodeMap.get(currId);
            const currInbound = incomingEdges.get(currId) ?? [];
            const relevantEdges =
                currNode && isMergeCdcScdNode(currNode)
                    ? currInbound.filter((e) => e.targetHandle !== 'right')
                    : currInbound;

            for (const edge of relevantEdges) {
                const upId = edge.source;
                if (visited.has(upId)) {
                    continue;
                }
                visited.add(upId);
                const upDataset = datasetsByNodeId[upId];
                if (!upDataset || !upDataset.isStreaming) {
                    continue;
                }
                if (isPersistedStreamingDataset(upDataset)) {
                    if (upDataset.resetAllowed) {
                        pushBlockingIssue({
                            code: 'INVALID_RESETTABLE_DEPENDENCY',
                            message: `Streaming table "${dataset.datasetName}" sets pipelines.reset.allowed=false while reading stream from resettable upstream streaming table "${upDataset.datasetName}" (INVALID_RESETTABLE_DEPENDENCY).`,
                            nodeId: dataset.nodeId,
                            nodeLabel: dataset.nodeLabel,
                            sparkTransform: dataset.sparkTransform,
                        });
                    }
                } else if (upDataset.kind === 'temporary_view') {
                    walkQueue.push(upId);
                }
            }
        }
    }

    // 9. Aggregate external sink, materialized view, and streaming table name lists.
    const externalSinkNames: string[] = [];
    const materializedViewNames: string[] = [];
    const streamingTableNames: string[] = [];

    const appendUnique = (target: string[], name: string): void => {
        if (!target.includes(name)) {
            target.push(name);
        }
    };

    for (const dataset of datasets) {
        if (dataset.kind === 'external_sink') {
            appendUnique(externalSinkNames, dataset.datasetName);
        } else if (dataset.kind === 'materialized_view') {
            appendUnique(materializedViewNames, dataset.datasetName);
        } else if (dataset.kind === 'streaming_table' || dataset.kind === 'append_flow') {
            appendUnique(streamingTableNames, dataset.datasetName);
        } else if (dataset.kind === 'auto_cdc') {
            appendUnique(streamingTableNames, dataset.targetDataset ?? dataset.datasetName);
        }
    }

    return {
        isCompatible: blockingIssues.length === 0,
        blockingIssues,
        warnings,
        datasets,
        datasetsByNodeId,
        sortedNodeIds,
        runtimeArtifacts,
        hasExternalSinks: externalSinkNames.length > 0,
        externalSinkNames,
        materializedViewNames,
        streamingTableNames,
    };
}
