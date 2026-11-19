/**
 * Pipeline Feature Detector
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Reads the parsed `.spark` pipeline (React Flow `nodes` + `edges`) and returns the facts
 * that drive skill selection: the language, the runners that the project must support,
 * the set of `spark_transform` values, and the pipeline features (side inputs, multi-output,
 * streaming, custom instructions, Python 4.0 DataSource API, cross-language transforms).
 *
 * Architectural decisions / notes for future sessions:
 * - The detector receives the ALREADY-PARSED YAML. It never parses YAML a second time.
 * - Selection keys on `data.spark_transform` (with fallback to `data.sparkTransform` or
 *   legacy `beam_transform` metadata). A node without a transform is skipped with a warning.
 *   The detector never maps node labels to transforms (labels are user-editable).
 *   Fix the `.spark` file instead.
 * - Legacy `spark_transform` spellings (`ReadFromKafka`) are resolved to the catalog value
 *   (`KafkaSource`) by the optional `resolveTransform` callback (the skill catalog). Each
 *   alias and each value without a skill produces a warning.
 * - Streaming detection uses `computeStreamingNodeSet`, explicit `readMode: 'streaming'` on
 *   any SOURCE node, `STREAMING_SOURCE_TRANSFORMS` (unless `readMode === 'batch'`), or
 *   streaming step transforms (`Watermark`, `StatefulTransform`, `WindowInto`).
 * - Python 4.0 DataSource API detection inspects `PYTHON_DATASOURCE_TRANSFORMS` and
 *   `resolveConnectorForRuntime` from `src/spark/versionMatrix.ts`.
 * - The language comes from the caller (the Generate request), not from the YAML. The
 *   extension already resolved it, and `pipelineConfig.language` can be stale.
 * - Runners: every runner that the language supports (parity rule in `codegenConfig.ts`).
 * - Every rule is a constant in `codegenConfig.ts`. This file holds only logic.
 */

import type { DagEdge, DagNode } from '../shared/dagValidator';
import { computeStreamingNodeSet } from '../shared/sparkDagRules';
import type { SupportedLanguage } from '../shared/types';
import { resolveConnectorForRuntime } from '../spark/versionMatrix';
import {
    CUSTOM_INSTRUCTION_KEYS,
    EDGE_HANDLES,
    LANGUAGE_RUNNERS,
    PIPELINE_FEATURES,
    PipelineFeature,
    PYTHON_DATASOURCE_TRANSFORMS,
    STREAMING_SOURCE_TRANSFORMS,
    STREAMING_STEP_TRANSFORMS,
    SkillLanguage,
    SkillRunner,
    XLANG_TRANSFORMS,
    toSkillLanguage,
} from './codegenConfig';

/** Node type of a source node in the `.spark` YAML. */
const SOURCE_NODE_TYPE = 'SOURCE';

/** Result of feature detection. */
export interface DetectedPipeline {
    /** Skill language id. */
    language: SkillLanguage;
    /** Every runner that the project must support. */
    runners: Set<SkillRunner>;
    /** Every node `data.spark_transform` value. */
    transforms: Set<string>;
    /** Detected features. */
    features: Set<PipelineFeature>;
    /** One message per node that has no `spark_transform` or uses an alias / unknown value. */
    warnings: string[];
}

/** Minimal shape of a node `data` object that the detector reads. */
interface NodeData {
    label?: unknown;
    type?: unknown;
    spark_transform?: unknown;
    sparkTransform?: unknown;
    beam_transform?: unknown;
    beamTransform?: unknown;
    system_instructions?: unknown;
    properties?: unknown;
}

/**
 * Returns the value when it is a non-null object, otherwise undefined.
 *
 * @param value Any value.
 * @returns The value as a record, or undefined.
 */
function asRecord(value: unknown): Record<string, unknown> | undefined {
    return value !== null && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : undefined;
}

/**
 * Returns the value when it is an array, otherwise an empty array.
 *
 * @param value Any value.
 * @returns The array, or an empty array.
 */
function asArray(value: unknown): unknown[] {
    return Array.isArray(value) ? value : [];
}

/**
 * Checks whether a value is a string with non-whitespace content.
 *
 * @param value Any value.
 * @returns True for a non-blank string.
 */
function isNonBlankString(value: unknown): boolean {
    return typeof value === 'string' && value.trim().length > 0;
}

/**
 * Checks whether a node carries custom code instructions for the model.
 *
 * @param data Node `data` object.
 * @returns True when `system_instructions` or a custom-logic property is non-blank.
 */
function hasCustomInstructions(data: NodeData): boolean {
    if (isNonBlankString(data.system_instructions)) {
        return true;
    }
    const properties = asRecord(data.properties);
    return properties !== undefined && CUSTOM_INSTRUCTION_KEYS.propertyKeys.some((key) => isNonBlankString(properties[key]));
}

/**
 * Extracts the raw transform string from node data, supporting `spark_transform`,
 * `sparkTransform`, and legacy `beam_transform` / `beamTransform` fallbacks.
 *
 * @param data Node `data` record.
 * @param nodeRecord Outer node record.
 * @returns Raw transform value, if present.
 */
function extractRawTransform(data: NodeData, nodeRecord?: Record<string, unknown>): unknown {
    const { beam_transform: legacySnakeTransform, beamTransform: legacyCamelTransform } = data;
    return (
        data.spark_transform ??
        data.sparkTransform ??
        legacySnakeTransform ??
        legacyCamelTransform ??
        nodeRecord?.spark_transform ??
        nodeRecord?.sparkTransform
    );
}

/**
 * Checks whether a node transform uses the Spark 4.0 Python DataSource API or Python
 * foreachBatch SDK connector according to `PYTHON_DATASOURCE_TRANSFORMS` or `versionMatrix.ts`.
 *
 * @param transformName Canonical transform name.
 * @param rawTransform Original un-aliased transform name.
 * @param properties Optional node property map.
 * @returns True when `core-python-datasource` should be preloaded.
 */
function usesPythonDataSource(
    transformName: string,
    rawTransform: string,
    properties?: Record<string, unknown>
): boolean {
    if (PYTHON_DATASOURCE_TRANSFORMS.has(transformName) || PYTHON_DATASOURCE_TRANSFORMS.has(rawTransform)) {
        return true;
    }
    const resolution = resolveConnectorForRuntime(transformName, 'serverless_4.0', properties);
    return resolution.mechanism === 'python_datasource' || Boolean(resolution.pythonDataSourceClass);
}

/**
 * Detects the skill-selection facts of a parsed pipeline.
 *
 * @param parsedPipeline The parsed `.spark` YAML (any shape; missing parts are tolerated).
 * @param language The resolved generation language.
 * @param resolveTransform Optional skill catalog transform resolver.
 * @returns Language, runners, transforms, features, and warnings.
 */
export function detectPipelineFeatures(
    parsedPipeline: unknown,
    language: SupportedLanguage,
    resolveTransform?: (raw: string) => { canonical: string; aliased: boolean; known: boolean }
): DetectedPipeline {
    const skillLanguage = toSkillLanguage(language);
    const root = asRecord(parsedPipeline) ?? {};
    const transforms = new Set<string>();
    const features = new Set<PipelineFeature>();
    const warnings: string[] = [];

    const rawNodes = asArray(root.nodes);
    const rawEdges = asArray(root.edges);

    // 1. Propagate streaming across the DAG via sparkDagRules when valid node IDs exist.
    const dagNodes = rawNodes.filter((n): n is DagNode => asRecord(n) !== undefined);
    const dagEdges = rawEdges.filter((e): e is DagEdge => asRecord(e) !== undefined);
    const streamingPropagation = computeStreamingNodeSet(dagNodes, dagEdges);
    if (streamingPropagation.hasStreamingSource || streamingPropagation.streamingNodeIds.size > 0) {
        features.add(PIPELINE_FEATURES.streaming);
    }

    // 2. Nodes: collect spark_transform values and node-level features.
    for (const node of rawNodes) {
        const nodeRecord = asRecord(node);
        const data: NodeData = asRecord(nodeRecord?.data) ?? {};
        const properties = asRecord(data.properties);
        const transform = extractRawTransform(data, nodeRecord);

        // 2a. Custom instructions apply even when the transform is missing.
        if (hasCustomInstructions(data)) {
            features.add(PIPELINE_FEATURES.customInstructions);
        }

        // 2b. Explicit readMode: 'streaming' on any node marks the pipeline as streaming.
        if (properties?.readMode === 'streaming') {
            features.add(PIPELINE_FEATURES.streaming);
        }

        // 2c. Skip (with one warning) nodes that have no spark_transform.
        if (!isNonBlankString(transform)) {
            const nodeLabel = typeof data.label === 'string' ? data.label : '(no label)';
            const id = typeof nodeRecord?.id === 'string' ? nodeRecord.id : '(no id)';
            warnings.push(`Node '${nodeLabel}' (${id}) has no spark_transform. No connector skill is loaded for it.`);
            continue;
        }

        // 2d. Resolve legacy spellings (`ReadFromKafka` -> `KafkaSource`) to the catalog value.
        const rawTrimmed = (transform as string).trim();
        const label = typeof data.label === 'string' ? data.label : '(no label)';
        const resolution = resolveTransform?.(rawTrimmed);
        const transformName = resolution ? resolution.canonical : rawTrimmed;
        if (resolution?.aliased) {
            warnings.push(`Node '${label}': spark_transform '${rawTrimmed}' is treated as '${transformName}'.`);
        } else if (resolution && !resolution.known) {
            warnings.push(`Node '${label}': spark_transform '${transformName}' has no codegen skill.`);
        }
        transforms.add(transformName);

        // 2e. Streaming: a streaming SOURCE node (unless explicitly configured as batch),
        //     or a streaming step node (`Watermark`, `StatefulTransform`, `WindowInto`).
        const isSourceNode = data.type === SOURCE_NODE_TYPE || nodeRecord?.type === SOURCE_NODE_TYPE;
        const explicitlyBatch = properties?.readMode === 'batch';
        const isStreamingSource =
            isSourceNode &&
            !explicitlyBatch &&
            (STREAMING_SOURCE_TRANSFORMS.has(transformName) ||
                STREAMING_SOURCE_TRANSFORMS.has(rawTrimmed) ||
                properties?.readMode === 'streaming');
        if (
            isStreamingSource ||
            STREAMING_STEP_TRANSFORMS.has(transformName) ||
            STREAMING_STEP_TRANSFORMS.has(rawTrimmed)
        ) {
            features.add(PIPELINE_FEATURES.streaming);
        }

        // 2f. Spark 4.0 Python DataSource API or foreachBatch SDK connector.
        if (usesPythonDataSource(transformName, rawTrimmed, properties)) {
            features.add(PIPELINE_FEATURES.pythonDatasource);
        }

        // 2g. Cross-language: the transform needs an expansion service in this language.
        if (XLANG_TRANSFORMS[skillLanguage].has(transformName)) {
            features.add(PIPELINE_FEATURES.xlang);
        }
    }

    // 3. Edges: side inputs and multi-output routing.
    for (const edge of rawEdges) {
        const edgeRecord = asRecord(edge) ?? {};
        if (edgeRecord.targetHandle === EDGE_HANDLES.sideInput) {
            features.add(PIPELINE_FEATURES.sideInputs);
        }
        if (typeof edgeRecord.sourceHandle === 'string' && EDGE_HANDLES.multiOutput.includes(edgeRecord.sourceHandle)) {
            features.add(PIPELINE_FEATURES.multiOutput);
        }
    }

    // 4. Runners: every runner of the language (parity), independent of jobConfig.runner.
    return {
        language: skillLanguage,
        runners: new Set<SkillRunner>(LANGUAGE_RUNNERS[skillLanguage]),
        transforms,
        features,
        warnings,
    };
}
