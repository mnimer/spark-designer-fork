/**
 * Pipeline Feature Detector
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Reads the parsed `.beam` pipeline (React Flow `nodes` + `edges`) and returns the facts
 * that drive skill selection: the language, the runners that the project must support,
 * the set of `spark_transform` values, and the pipeline features (side inputs, multi-output,
 * streaming, custom instructions, cross-language transforms).
 *
 * Architectural decisions / notes for future sessions:
 * - The detector receives the ALREADY-PARSED YAML. It never parses YAML a second time.
 * - Selection keys on `data.spark_transform` only. A node without it is skipped with a
 *   warning. The detector never maps node labels to transforms (labels are user-editable).
 *   Fix the `.beam` file instead.
 * - Legacy `spark_transform` spellings (`ReadFromKafka`) are resolved to the catalog value
 *   (`KafkaIO`) by the optional `resolveTransform` callback (the skill catalog). Each alias
 *   and each value without a skill produces a warning.
 * - The language comes from the caller (the Generate request), not from the YAML. The
 *   extension already resolved it, and `pipelineConfig.language` can be stale.
 * - Runners: every runner that the language supports (parity rule in `codegenConfig.ts`).
 * - Every rule is a constant in `codegenConfig.ts`. This file holds only logic.
 */

import type { SupportedLanguage } from '../shared/types';
import {
    CUSTOM_INSTRUCTION_KEYS,
    EDGE_HANDLES,
    LANGUAGE_RUNNERS,
    PIPELINE_FEATURES,
    PipelineFeature,
    STREAMING_SOURCE_TRANSFORMS,
    SkillLanguage,
    SkillRunner,
    WINDOW_INTO_TRANSFORM,
    XLANG_TRANSFORMS,
    toSkillLanguage,
} from './codegenConfig';

/** Node type of a source node in the `.beam` YAML. */
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
    /** One message per node that has no `spark_transform`. */
    warnings: string[];
}

/** Minimal shape of a node `data` object that the detector reads. */
interface NodeData {
    label?: unknown;
    type?: unknown;
    spark_transform?: unknown;
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
 * Detects the skill-selection facts of a parsed pipeline.
 *
 * @param parsedPipeline The parsed `.beam` YAML (any shape; missing parts are tolerated).
 * @param language The resolved generation language.
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

    // 1. Nodes: collect spark_transform values and node-level features.
    for (const node of asArray(root.nodes)) {
        const nodeRecord = asRecord(node);
        const data: NodeData = asRecord(nodeRecord?.data) ?? {};
        const transform = data.spark_transform;

        // 1a. Custom instructions apply even when the transform is missing.
        if (hasCustomInstructions(data)) {
            features.add(PIPELINE_FEATURES.customInstructions);
        }

        // 1b. Skip (with one warning) nodes that have no spark_transform.
        if (!isNonBlankString(transform)) {
            const nodeLabel = typeof data.label === 'string' ? data.label : '(no label)';
            const id = typeof nodeRecord?.id === 'string' ? nodeRecord.id : '(no id)';
            warnings.push(`Node '${nodeLabel}' (${id}) has no spark_transform. No connector skill is loaded for it.`);
            continue;
        }
        // 1c. Resolve legacy spellings (`ReadFromKafka` -> `KafkaIO`) to the catalog value.
        const label = typeof data.label === 'string' ? data.label : '(no label)';
        const resolution = resolveTransform?.(transform as string);
        const transformName = resolution ? resolution.canonical : (transform as string).trim();
        if (resolution?.aliased) {
            warnings.push(`Node '${label}': spark_transform '${(transform as string).trim()}' is treated as '${transformName}'.`);
        } else if (resolution && !resolution.known) {
            warnings.push(`Node '${label}': spark_transform '${transformName}' has no codegen skill.`);
        }
        transforms.add(transformName);

        // 1d. Streaming: an unbounded SOURCE node, or any WindowInto node.
        const isStreamingSource = data.type === SOURCE_NODE_TYPE && STREAMING_SOURCE_TRANSFORMS.has(transformName);
        if (isStreamingSource || transformName === WINDOW_INTO_TRANSFORM) {
            features.add(PIPELINE_FEATURES.streaming);
        }

        // 1e. Cross-language: the transform needs an expansion service in this language.
        if (XLANG_TRANSFORMS[skillLanguage].has(transformName)) {
            features.add(PIPELINE_FEATURES.xlang);
        }
    }

    // 2. Edges: side inputs and multi-output routing.
    for (const edge of asArray(root.edges)) {
        const edgeRecord = asRecord(edge) ?? {};
        if (edgeRecord.targetHandle === EDGE_HANDLES.sideInput) {
            features.add(PIPELINE_FEATURES.sideInputs);
        }
        if (typeof edgeRecord.sourceHandle === 'string' && EDGE_HANDLES.multiOutput.includes(edgeRecord.sourceHandle)) {
            features.add(PIPELINE_FEATURES.multiOutput);
        }
    }

    // 3. Runners: every runner of the language (parity), independent of jobConfig.runner.
    return {
        language: skillLanguage,
        runners: new Set<SkillRunner>(LANGUAGE_RUNNERS[skillLanguage]),
        transforms,
        features,
        warnings,
    };
}
