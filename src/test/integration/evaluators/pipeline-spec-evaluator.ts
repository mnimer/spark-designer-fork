/**
 * Pipeline Specification & Property Conformance Evaluator Sub-Agent.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Audits a generated PySpark 4.x repository against the visual `.spark` DAG specification
 * (YAML) and its deterministic ExecutionPlan using a formal Requirement Traceability Matrix (RTM).
 *
 * Architectural decisions:
 * - Generates explicit requirement IDs (`REQ-NODE-*`, `REQ-INSTR-*`, `REQ-VAR-*`, `REQ-CONFIG-*`,
 *   `REQ-EDGE-*`, and `REQ-PLAN-*` when an ExecutionPlan is available).
 * - Runs deterministic audits before the LLM call:
 *   - `spark_transform` audit: every node declares a known, canonical Spark 4.x transform that
 *     matches its node type, with no legacy Beam-era alias.
 *   - ExecutionPlan compliance: dfs keys per node, auto-cache `.cache()` / `.unpersist()`,
 *     `foreachBatch` sink grouping with `txnVersion` / `txnAppId`, checkpoints, and DLQ splits.
 * - Merges deterministic findings with the LLM findings. Any deterministic CRITICAL or MAJOR
 *   finding fails the agent.
 * - Mandates that the evaluator output a verification record for every requirement ID
 *   with status (`VERIFIED` | `PARTIAL` | `MISSING`) and verbatim line-numbered code evidence.
 * - Formats all generated files with 1-indexed line numbers.
 */

import * as yaml from 'yaml';
import { GoogleGenAI } from '@google/genai';
import { DEFAULT_GEMINI_MODEL } from '../../../constants';
import { buildExecutionPlan, type ExecutionPlan } from '../../../shared/executionPlanner';
import type { DagEdge, DagNode } from '../../../shared/dagValidator';
import { SPARK_NODE_CAPABILITIES } from '../../../shared/sparkNodeCapabilities';
import { EvaluatorFinding, SpecialistAgentResult, generateContentWithRetry } from './types';
import type { GeneratedFileCandidate } from './deterministic-gate';
import { formatFilesWithLineNumbers } from './format-utils';

/**
 * Structured representation of a parsed pipeline node for specification audit.
 */
export interface ParsedPipelineNode {
    id: string;
    label: string;
    type: 'SOURCE' | 'STEP' | 'SINK';
    sparkTransform?: string;
    properties: Record<string, any>;
    systemInstructions?: string;
    codeGenerationPrompt?: string;
}

/**
 * Structured representation of the full pipeline specification parsed from YAML.
 */
export interface ParsedPipelineSpec {
    nodes: ParsedPipelineNode[];
    runtimeVariables: { key: string; value: any }[];
    jobConfig: Record<string, any>;
    pipelineConfig: Record<string, any>;
    edges: { id?: string; source: string; target: string; sourceHandle?: string; targetHandle?: string }[];
}

export interface SpecRequirementItem {
    reqId: string;
    category: string;
    description: string;
    expected: string;
}

/**
 * Safely parses the pipeline YAML string into a structured specification model.
 *
 * @param yamlContent - Raw pipeline YAML content.
 * @returns Structured ParsedPipelineSpec object.
 */
export function parsePipelineSpec(yamlContent: string): ParsedPipelineSpec {
    if (!yamlContent || !yamlContent.trim()) {
        return {
            nodes: [],
            runtimeVariables: [],
            jobConfig: {},
            pipelineConfig: {},
            edges: []
        };
    }

    try {
        const parsed = yaml.parse(yamlContent);
        if (!parsed || typeof parsed !== 'object') {
            return {
                nodes: [],
                runtimeVariables: [],
                jobConfig: {},
                pipelineConfig: {},
                edges: []
            };
        }

        const rawNodes = Array.isArray(parsed.nodes) ? parsed.nodes : [];
        const nodes: ParsedPipelineNode[] = rawNodes.map((n: any) => {
            const data = n.data || {};
            const props = typeof data.properties === 'object' && data.properties !== null ? data.properties : {};
            return {
                id: String(n.id || ''),
                label: String(data.label || 'Unnamed Node'),
                type: (data.type === 'SOURCE' || data.type === 'SINK') ? data.type : 'STEP',
                sparkTransform: data.spark_transform || data.sparkTransform,
                properties: props,
                systemInstructions: data.system_instructions || data.systemInstructions || props.system_instructions || props.systemInstructions,
                codeGenerationPrompt: data.codeGenerationPrompt || props.codeGenerationPrompt
            };
        });

        const rawVars = Array.isArray(parsed.runtimeVariables) ? parsed.runtimeVariables : [];
        const runtimeVariables = rawVars.map((v: any) => ({
            key: String(v.key || ''),
            value: v.value
        })).filter((v: any) => v.key.length > 0);

        const jobConfig = typeof parsed.jobConfig === 'object' && parsed.jobConfig !== null ? parsed.jobConfig : {};
        const pipelineConfig = typeof parsed.pipelineConfig === 'object' && parsed.pipelineConfig !== null ? parsed.pipelineConfig : {};

        const rawEdges = Array.isArray(parsed.edges) ? parsed.edges : [];
        const edges = rawEdges.map((e: any) => ({
            id: e.id,
            source: String(e.source || ''),
            target: String(e.target || ''),
            sourceHandle: e.sourceHandle,
            targetHandle: e.targetHandle
        }));

        return {
            nodes,
            runtimeVariables,
            jobConfig,
            pipelineConfig,
            edges
        };
    } catch {
        return {
            nodes: [],
            runtimeVariables: [],
            jobConfig: {},
            pipelineConfig: {},
            edges: []
        };
    }
}

/**
 * Normalizes a generated filename to forward slashes without a leading `./`.
 * This module keeps its own copy so it has no runtime dependency on the deterministic gate.
 *
 * @param filename - Raw filename from the generation payload.
 * @returns Normalized relative filename.
 */
export function normalizeRepoPath(filename: string): string {
    return filename.replace(/\\/g, '/').replace(/^\.\//, '');
}

/**
 * Builds the deterministic ExecutionPlan for a `.spark` pipeline YAML document.
 *
 * @param yamlContent - Raw pipeline YAML content.
 * @returns ExecutionPlan, or null if the YAML is empty, malformed, or has no nodes.
 */
export function buildSpecExecutionPlan(yamlContent: string): ExecutionPlan | null {
    if (!yamlContent || !yamlContent.trim()) {
        return null;
    }
    try {
        const parsed = yaml.parse(yamlContent);
        if (!parsed || typeof parsed !== 'object' || !Array.isArray(parsed.nodes) || parsed.nodes.length === 0) {
            return null;
        }
        const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null;
        const nodes: DagNode[] = (parsed.nodes as unknown[])
            .filter((n): n is Record<string, unknown> => isRecord(n) && n.id !== undefined && n.id !== null)
            .map(n => ({ ...n, id: String(n.id) }));
        const edges: DagEdge[] = (Array.isArray(parsed.edges) ? (parsed.edges as unknown[]) : [])
            .filter(isRecord)
            .map(e => ({ ...e, source: String(e.source ?? ''), target: String(e.target ?? '') }));
        const jobConfig = typeof parsed.jobConfig === 'object' && parsed.jobConfig !== null ? parsed.jobConfig : undefined;
        return buildExecutionPlan(nodes, edges, jobConfig);
    } catch {
        return null;
    }
}

/**
 * Beam primitive names that have no canonical Spark 4.x catalog equivalent.
 */
const BEAM_PRIMITIVE_TRANSFORMS: ReadonlySet<string> = new Set([
    'ParDo', 'DoFn', 'Flatten', 'GroupByKey', 'CoGroupByKey', 'Combine', 'CombinePerKey', 'Reshuffle', 'Create'
]);

/**
 * Returns true if a transform identifier is a legacy Beam-era alias.
 *
 * @param transform - Raw `spark_transform` value.
 * @returns True for `*IO`, `*FileSystem`, scoped `SOURCE:`/`SINK:` names, and Beam primitives.
 */
export function isLegacyBeamTransform(transform: string): boolean {
    return /IO$/.test(transform)
        || /FileSystem$/.test(transform)
        || transform.includes(':')
        || BEAM_PRIMITIVE_TRANSFORMS.has(transform);
}

/**
 * Audits the `spark_transform` property of every node in the specification.
 *
 * Logic flow:
 * - If a node has no `spark_transform`, report a CRITICAL finding.
 * - If the transform is a legacy Beam-era alias, report a MAJOR finding.
 * - If the transform is not in the Spark capability map, report a MAJOR finding.
 * - If a SOURCE node does not use a `*Source` transform, or a SINK node does not use a `*Sink`
 *   transform, or a STEP node uses either, report a MAJOR finding.
 *
 * @param spec - Parsed pipeline specification.
 * @returns Specification findings.
 */
export function auditSparkTransformProperties(spec: ParsedPipelineSpec): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];
    const file = 'pipeline.spark';

    for (const node of spec.nodes) {
        const transform = (node.sparkTransform ?? '').trim();
        const nodeRef = `Node "${node.label}" (${node.id})`;

        if (!transform) {
            findings.push({
                id: `SPEC-TRANSFORM-MISSING-${node.id}`,
                category: 'SPECIFICATION',
                severity: 'CRITICAL',
                file,
                description: `${nodeRef} has no spark_transform property`,
                rationale: 'Code generation resolves the codegen skill and ExecutionPlan behavior from spark_transform. A node without it cannot be generated faithfully.'
            });
            continue;
        }

        if (isLegacyBeamTransform(transform)) {
            findings.push({
                id: `SPEC-TRANSFORM-LEGACY-${node.id}`,
                category: 'SPECIFICATION',
                severity: 'MAJOR',
                file,
                evidence: `spark_transform: ${transform}`,
                description: `${nodeRef} uses the legacy Beam-era transform alias '${transform}'`,
                rationale: 'Spark 4.x pipelines must use canonical catalog transforms (for example KafkaSource, JdbcSink, Filter) so the matching codegen skill applies.'
            });
            continue;
        }

        if (!Object.prototype.hasOwnProperty.call(SPARK_NODE_CAPABILITIES, transform)) {
            findings.push({
                id: `SPEC-TRANSFORM-UNKNOWN-${node.id}`,
                category: 'SPECIFICATION',
                severity: 'MAJOR',
                file,
                evidence: `spark_transform: ${transform}`,
                description: `${nodeRef} uses unknown spark_transform '${transform}'`,
                rationale: 'The transform is not in the Spark node capability map, so no codegen skill or ExecutionPlan rule covers it.'
            });
            continue;
        }

        const isSourceName = transform.endsWith('Source');
        const isSinkName = transform.endsWith('Sink');
        const typeMatches = node.type === 'SOURCE' ? isSourceName
            : node.type === 'SINK' ? isSinkName
                : !isSourceName && !isSinkName;
        if (!typeMatches) {
            findings.push({
                id: `SPEC-TRANSFORM-TYPE-${node.id}`,
                category: 'SPECIFICATION',
                severity: 'MAJOR',
                file,
                evidence: `type: ${node.type}, spark_transform: ${transform}`,
                description: `${nodeRef} has type ${node.type} but spark_transform '${transform}' belongs to another node category`,
                rationale: 'A node type that disagrees with its transform produces wrong read_sources / transform / write_sinks placement.'
            });
        }
    }

    return findings;
}

/**
 * Returns true if the code contains the quoted string literal.
 *
 * @param code - Source code.
 * @param literal - Literal text without quotes.
 * @returns True if `"literal"` or `'literal'` appears.
 */
function containsQuoted(code: string, literal: string): boolean {
    return code.includes(`"${literal}"`) || code.includes(`'${literal}'`);
}

/**
 * Audits generated `src/pipeline.py` against the deterministic ExecutionPlan.
 *
 * Logic flow:
 * - If `src/pipeline.py` is absent, return no findings. The deterministic gate reports it.
 * - Every SOURCE and STEP node id must appear as a quoted `dfs` key.
 * - Cache directives require `.cache()` or `.persist(...)`, and unpersist points require `.unpersist()`.
 * - `foreachBatch` sink groups require `.foreachBatch(...)`. Idempotent groups require
 *   `txnVersion` and `txnAppId`. Multi-sink groups require batch caching and unpersist.
 * - Native streaming groups require `.writeStream`. Every streaming group requires `checkpointLocation`.
 * - Every DLQ split requires a `<node_id>__error` dfs key. A `_corrupt_record` split requires that column.
 *
 * @param plan - Deterministic ExecutionPlan for the pipeline.
 * @param files - All generated files.
 * @returns Specification findings.
 */
export function auditExecutionPlanCompliance(plan: ExecutionPlan, files: GeneratedFileCandidate[]): EvaluatorFinding[] {
    const pipelineFile = files.find(f => normalizeRepoPath(f.filename) === 'src/pipeline.py');
    if (!pipelineFile) {
        return [];
    }
    const code = pipelineFile.content;
    const file = pipelineFile.filename;
    const findings: EvaluatorFinding[] = [];
    const push = (id: string, severity: EvaluatorFinding['severity'], description: string, rationale: string): void => {
        findings.push({ id, category: 'SPECIFICATION', severity, file, description, rationale });
    };

    // 1. Node id dfs keys.
    for (const nodeId of plan.sortedNodeIds) {
        const nodeType = plan.nodes[nodeId]?.nodeType;
        if (nodeType === 'SINK') {
            continue;
        }
        if (!containsQuoted(code, nodeId)) {
            push(`PLAN-NODE-KEY-${nodeId}`, 'MAJOR',
                `Node '${nodeId}' is never referenced as a dfs key in src/pipeline.py`,
                'read_sources and transform must store every SOURCE and STEP DataFrame in dfs[<node_id>] so tests and sinks can find it.');
        }
    }

    // 2. Auto-cache and unpersist.
    const hasCache = /\.cache\(\s*\)|\.persist\(/.test(code);
    const hasUnpersist = /\.unpersist\(/.test(code);
    if (plan.cacheDirectives.length > 0) {
        if (!hasCache) {
            push('PLAN-CACHE-MISSING', 'MAJOR',
                `ExecutionPlan caches ${plan.cacheDirectives.map(d => `'${d.nodeId}' (${d.reason})`).join(', ')} but src/pipeline.py never calls .cache() or .persist()`,
                'Batch fan-out without caching recomputes the full upstream lineage once per branch.');
        }
        if (plan.cacheDirectives.some(d => d.unpersistAfterNodeIds.length > 0) && !hasUnpersist) {
            push('PLAN-UNPERSIST-MISSING', 'MAJOR',
                'ExecutionPlan defines unpersist points but src/pipeline.py never calls .unpersist()',
                'Cached DataFrames must be released at ExecutionPlan.unpersistPoints after the downstream writes complete.');
        }
    }

    // 3. Streaming sink groups.
    for (const group of plan.streamingSinkGroups) {
        if (group.mode === 'foreachBatch') {
            if (!/\.foreachBatch\(/.test(code)) {
                push(`PLAN-FOREACHBATCH-${group.id}`, 'MAJOR',
                    `Streaming sink group '${group.id}' (sinks: ${group.sinkIds.join(', ')}) requires foreachBatch but src/pipeline.py never calls .foreachBatch()`,
                    'Batch-only connectors and multi-sink fan-outs must share one foreachBatch micro-batch writer.');
            }
            if (group.idempotentTxnOptions && !(code.includes('txnVersion') && code.includes('txnAppId'))) {
                push(`PLAN-TXN-${group.id}`, 'MAJOR',
                    `Streaming sink group '${group.id}' requires idempotent txnVersion / txnAppId write options`,
                    `Set txnVersion=batch_id and txnAppId (planned value '${group.txnAppId}') so replayed micro-batches do not write duplicates.`);
            }
            if (group.requiresBatchCache && !(hasCache && hasUnpersist)) {
                push(`PLAN-BATCH-CACHE-${group.id}`, 'MAJOR',
                    `Streaming sink group '${group.id}' writes to several sinks but does not cache and unpersist batch_df`,
                    'Multi-sink micro-batches must call batch_df.cache() and release it in a finally block with batch_df.unpersist().');
            }
        } else if (!/\.writeStream\b/.test(code)) {
            push(`PLAN-WRITESTREAM-${group.id}`, 'MAJOR',
                `Streaming sink group '${group.id}' requires a native writeStream query but src/pipeline.py never uses .writeStream`,
                'Streaming sinks must start a Structured Streaming query.');
        }
    }
    if (plan.streamingSinkGroups.length > 0 && !code.includes('checkpointLocation')) {
        push('PLAN-CHECKPOINT-MISSING', 'MAJOR',
            'Streaming sink groups exist but src/pipeline.py never sets checkpointLocation',
            'Structured Streaming queries need a checkpointLocation for fault tolerance and exactly-once recovery.');
    }

    // 4. DLQ splits.
    for (const split of plan.dlqSplitPoints) {
        if (!containsQuoted(code, `${split.nodeId}__error`)) {
            push(`PLAN-DLQ-${split.nodeId}`, 'MAJOR',
                `DLQ split on '${split.nodeId}' (${split.sparkTransform}, ${split.mechanism}) has no dfs["${split.nodeId}__error"] branch`,
                `Error rows must be stored in dfs["<node_id>__error"] and routed to ${split.errorTargetIds.join(', ') || 'the error targets'}.`);
        }
        if (split.mechanism === '_corrupt_record' && !code.includes('_corrupt_record')) {
            push(`PLAN-DLQ-CORRUPT-${split.nodeId}`, 'MAJOR',
                `DLQ split on '${split.nodeId}' uses the _corrupt_record mechanism but src/pipeline.py never references _corrupt_record`,
                'PERMISSIVE parsing must capture malformed payloads in the _corrupt_record column before the split.');
        }
    }

    return findings;
}

/**
 * Extracts a flattened Requirement Traceability Matrix (RTM) from the specification.
 *
 * @param spec - Parsed pipeline specification.
 * @param plan - Optional deterministic ExecutionPlan. If present, `REQ-PLAN-*` items are added.
 * @returns Array of explicit requirement items with deterministic IDs.
 */
export function generateRequirementMatrix(spec: ParsedPipelineSpec, plan?: ExecutionPlan | null): SpecRequirementItem[] {
    const items: SpecRequirementItem[] = [];

    // 1. Node properties
    spec.nodes.forEach((node, nodeIdx) => {
        const nodeNum = nodeIdx + 1;
        const transformDesc = node.sparkTransform ? ` (${node.sparkTransform})` : '';

        Object.entries(node.properties).forEach(([k, v]) => {
            items.push({
                reqId: `REQ-NODE-${nodeNum}-${k.toUpperCase()}`,
                category: 'NODE_PROPERTY',
                description: `Node "${node.label}"${transformDesc} property: ${k}`,
                expected: JSON.stringify(v)
            });
        });

        if (node.systemInstructions) {
            items.push({
                reqId: `REQ-INSTR-${nodeNum}`,
                category: 'CUSTOM_INSTRUCTIONS',
                description: `Node "${node.label}" custom system instructions`,
                expected: node.systemInstructions
            });
        }

        if (node.codeGenerationPrompt) {
            items.push({
                reqId: `REQ-PROMPT-${nodeNum}`,
                category: 'CUSTOM_PROMPT',
                description: `Node "${node.label}" custom code generation prompt`,
                expected: node.codeGenerationPrompt
            });
        }
    });

    // 2. Runtime variables
    spec.runtimeVariables.forEach(rv => {
        items.push({
            reqId: `REQ-VAR-${rv.key.toUpperCase()}`,
            category: 'RUNTIME_VARIABLE',
            description: `Runtime variable ${rv.key}`,
            expected: JSON.stringify(rv.value)
        });
    });

    // 3. Job configuration
    Object.entries(spec.jobConfig).forEach(([k, v]) => {
        if (v !== '' && v !== null && v !== undefined) {
            items.push({
                reqId: `REQ-CONFIG-${k.toUpperCase()}`,
                category: 'JOB_CONFIG',
                description: `Job configuration ${k}`,
                expected: JSON.stringify(v)
            });
        }
    });

    // 4. Edges and DLQ routing
    spec.edges.forEach((edge, edgeIdx) => {
        const handle = edge.sourceHandle ? ` via handle '${edge.sourceHandle}'` : '';
        items.push({
            reqId: `REQ-EDGE-${edgeIdx + 1}`,
            category: 'TOPOLOGY_EDGE',
            description: `Edge from ${edge.source} to ${edge.target}${handle}`,
            expected: `Connects output to input${edge.sourceHandle ? ` with output tag '${edge.sourceHandle}'` : ''}`
        });
    });

    // 5. ExecutionPlan directives
    if (plan) {
        plan.cacheDirectives.forEach(d => {
            items.push({
                reqId: `REQ-PLAN-CACHE-${d.nodeId}`,
                category: 'EXECUTION_PLAN',
                description: `Cache node ${d.nodeId} (${d.reason})`,
                expected: `.cache() before branching, .unpersist() after ${d.unpersistAfterNodeIds.join(', ') || 'downstream writes'}`
            });
        });
        plan.streamingSinkGroups.forEach(g => {
            items.push({
                reqId: `REQ-PLAN-SINKGROUP-${g.id}`,
                category: 'EXECUTION_PLAN',
                description: `Streaming sink group ${g.id} (${g.mode}) for sinks ${g.sinkIds.join(', ')}`,
                expected: g.mode === 'foreachBatch'
                    ? `writeStream.foreachBatch(...)${g.idempotentTxnOptions ? ` with txnVersion=batch_id, txnAppId=${g.txnAppId}` : ''}${g.requiresBatchCache ? ', batch_df.cache()/unpersist()' : ''}`
                    : 'native writeStream query with checkpointLocation'
            });
        });
        plan.dlqSplitPoints.forEach(s => {
            items.push({
                reqId: `REQ-PLAN-DLQ-${s.nodeId}`,
                category: 'EXECUTION_PLAN',
                description: `DLQ split on ${s.nodeId} (${s.sparkTransform}, ${s.mechanism})`,
                expected: `valid -> ${s.validTargetIds.join(', ') || 'none'}; error dfs["${s.nodeId}__error"] -> ${s.errorTargetIds.join(', ') || 'none'}`
            });
        });
        const { gcsJars, mavenPackages } = plan.runtimeArtifacts;
        if (gcsJars.length > 0 || mavenPackages.length > 0) {
            items.push({
                reqId: 'REQ-PLAN-CONNECTOR-JARS',
                category: 'EXECUTION_PLAN',
                description: `Connector artifacts for runtime ${plan.runtimeArtifacts.profile.id}`,
                expected: [...gcsJars.map(j => `--jars ${j}`), ...mavenPackages.map(p => `spark.jars.packages ${p}`)].join('; ')
            });
        }
    }

    return items;
}

/**
 * Formats a parsed pipeline specification into a human-readable checklist and RTM table.
 *
 * @param spec - Structured ParsedPipelineSpec.
 * @param plan - Optional deterministic ExecutionPlan whose directives join the RTM.
 * @returns Formatted checklist string.
 */
export function formatSpecChecklist(spec: ParsedPipelineSpec, plan?: ExecutionPlan | null): string {
    const lines: string[] = [];
    const rtm = generateRequirementMatrix(spec, plan);

    lines.push(`Total Nodes: ${spec.nodes.length}`);
    if (spec.nodes.length === 0 && rtm.length === 0) {
        return lines.join('\n');
    }

    for (const [idx, node] of spec.nodes.entries()) {
        lines.push(`\n[Node ${idx + 1}] ID: "${node.id}" | Label: "${node.label}" | Type: ${node.type} | Transform: ${node.sparkTransform || 'N/A'}`);

        const propKeys = Object.keys(node.properties);
        if (propKeys.length > 0) {
            lines.push(`  Configured Properties (${propKeys.length}):`);
            for (const key of propKeys) {
                const val = JSON.stringify(node.properties[key]);
                lines.push(`    - ${key}: ${val}`);
            }
        } else {
            lines.push(`  Configured Properties: (none)`);
        }

        if (node.systemInstructions) {
            lines.push(`  Custom System Instructions: "${node.systemInstructions}"`);
        }
        if (node.codeGenerationPrompt) {
            lines.push(`  Custom Code Generation Prompt: "${node.codeGenerationPrompt}"`);
        }
    }

    if (spec.runtimeVariables.length > 0) {
        lines.push(`\nRuntime Variables (${spec.runtimeVariables.length}):`);
        for (const rv of spec.runtimeVariables) {
            lines.push(`  - ${rv.key}: ${JSON.stringify(rv.value)}`);
        }
    }

    const jobKeys = Object.keys(spec.jobConfig);
    if (jobKeys.length > 0) {
        lines.push(`\nJob Configuration:`);
        for (const k of jobKeys) {
            lines.push(`  - ${k}: ${JSON.stringify(spec.jobConfig[k])}`);
        }
    }

    if (spec.edges.length > 0) {
        lines.push(`\nEdges / Connections (${spec.edges.length}):`);
        for (const edge of spec.edges) {
            const handleInfo = edge.sourceHandle ? ` [handle: ${edge.sourceHandle}]` : '';
            lines.push(`  - Source "${edge.source}" -> Target "${edge.target}"${handleInfo}`);
        }
    }

    lines.push(`\nTotal Traceable Requirements: ${rtm.length}`);
    lines.push('');
    lines.push('| Requirement ID | Category | Description | Expected Value / Behavior |');
    lines.push('| :--- | :--- | :--- | :--- |');

    for (const item of rtm) {
        const cleanExpected = item.expected.replace(/\n/g, ' ').substring(0, 100);
        lines.push(`| \`${item.reqId}\` | ${item.category} | ${item.description} | ${cleanExpected} |`);
    }

    return lines.join('\n');
}

/**
 * Runs the Pipeline Specification & Property Conformance Evaluator sub-agent.
 *
 * Logic flow:
 * - Parse the YAML and build the deterministic ExecutionPlan.
 * - Run the deterministic `spark_transform` and ExecutionPlan compliance audits.
 * - Ask the LLM to verify every RTM requirement against the line-numbered files.
 * - Merge both finding lists. Deterministic CRITICAL or MAJOR findings fail the agent.
 *
 * @param ai - Initialized GoogleGenAI client.
 * @param language - Target language. The Spark designer generates PySpark only.
 * @param yamlContent - Original `.spark` pipeline YAML.
 * @param files - All generated files.
 * @returns Specialist evaluation result with category 'SPECIFICATION'.
 */
export async function evaluatePipelineSpecification(
    ai: GoogleGenAI,
    language: 'Java' | 'Python',
    yamlContent: string,
    files: GeneratedFileCandidate[]
): Promise<SpecialistAgentResult> {
    const spec = parsePipelineSpec(yamlContent);
    const plan = buildSpecExecutionPlan(yamlContent);
    const checklistText = formatSpecChecklist(spec, plan);
    const deterministicFindings = [
        ...auditSparkTransformProperties(spec),
        ...(plan ? auditExecutionPlanCompliance(plan, files) : [])
    ];

    const prompt = `You are the Principal Pipeline Specification & Property Conformance Evaluator Agent.
Your responsibility is auditing a generated Apache Spark 4.x PySpark repository to verify that it FAITHFULLY IMPLEMENTS 100% OF THE SPECIFICATION.
You must verify each requirement in the Requirement Traceability Matrix (RTM) below against the line-numbered code.

=== TARGET LANGUAGE ===
${language} (PySpark 4.x DataFrame, Catalyst SQL, and Structured Streaming)
=======================

=== REPOSITORY CONTRACT ===
src/pipeline.py defines read_sources(spark, config), a pure transform(dfs, config), and write_sinks(dfs, config).
Every SOURCE and STEP DataFrame is stored in dfs[<node_id>]. DLQ error rows are stored in dfs["<node_id>__error"].
===========================

=== REQUIREMENT TRACEABILITY MATRIX (RTM) ===
${checklistText}
=============================================

=== DETERMINISTIC PRE-AUDIT FINDINGS (already confirmed, do not repeat) ===
${deterministicFindings.length > 0 ? deterministicFindings.map(f => `- [${f.severity}] ${f.id}: ${f.description}`).join('\n') : 'none'}
==========================================================================

=== FULL INPUT PIPELINE YAML ===
${yamlContent}
================================

=== GENERATED SOURCE FILES & ARTIFACTS (LINE NUMBERED) ===
${formatFilesWithLineNumbers(files)}
==========================================================

Instructions:
1. Verify EVERY requirement in the RTM. For each \`reqId\`, determine whether it is:
   - \`VERIFIED\`: Fully implemented and faithfully utilized.
   - \`PARTIAL\`: Partially implemented or substituted with an unintended default value.
   - \`MISSING\`: Completely omitted or silently ignored in the generated code.
2. Provide verbatim line-numbered code evidence for each verification.
3. For \`REQ-PLAN-*\` items, confirm that the code follows the ExecutionPlan: cache and unpersist points,
   foreachBatch grouping with txnVersion / txnAppId, DLQ routing, and connector JARs or packages in the runner scripts.
4. For any requirement marked \`MISSING\` or \`PARTIAL\`, emit a corresponding finding in \`findings[]\`.
   - CRITICAL: Omission of functional transform parameters (e.g. table, topic, path, SQL query, join keys, custom system_instructions logic, or DLQ routing).
   - MAJOR: Substitution of configured property with arbitrary default, missing runtime variable, omitted jobConfig parameter, or ExecutionPlan deviation.
   - MINOR: Optional cosmetic discrepancy.`;

    const schema = {
        type: "object",
        properties: {
            passed: {
                type: "boolean",
                description: "True if all requirements are VERIFIED without any CRITICAL or MAJOR omissions"
            },
            summary: {
                type: "string",
                description: "Specification conformance assessment summary"
            },
            requirementVerifications: {
                type: "array",
                description: "Full audit record verifying each requirement in the RTM",
                items: {
                    type: "object",
                    properties: {
                        reqId: { type: "string" },
                        category: { type: "string" },
                        description: { type: "string" },
                        status: { type: "string", enum: ["VERIFIED", "PARTIAL", "MISSING"] },
                        evidence: { type: "string" }
                    },
                    required: ["reqId", "category", "description", "status"]
                }
            },
            findings: {
                type: "array",
                items: {
                    type: "object",
                    properties: {
                        id: { type: "string" },
                        category: { type: "string", enum: ["SPECIFICATION"] },
                        severity: { type: "string", enum: ["CRITICAL", "MAJOR", "MINOR"] },
                        file: { type: "string" },
                        line: { type: "integer" },
                        evidence: { type: "string" },
                        description: { type: "string" },
                        rationale: { type: "string" }
                    },
                    required: ["id", "category", "severity", "file", "description", "rationale"]
                }
            }
        },
        required: ["passed", "summary", "findings", "requirementVerifications"]
    };

    const response = await generateContentWithRetry(ai, {
        model: DEFAULT_GEMINI_MODEL,
        contents: prompt,
        config: {
            responseMimeType: 'application/json',
            responseSchema: schema,
            temperature: 0.1
        }
    });

    if (!response.text) {
        throw new Error('Empty response from PipelineSpecificationAgent');
    }

    const parsed = JSON.parse(response.text);
    const blocking = deterministicFindings.filter(f => f.severity === 'CRITICAL' || f.severity === 'MAJOR');
    const summary = deterministicFindings.length > 0
        ? `${parsed.summary} Deterministic audit: ${deterministicFindings.length} finding(s).`
        : parsed.summary;
    return {
        agentName: 'PipelineSpecificationAgent',
        passed: Boolean(parsed.passed) && blocking.length === 0,
        summary,
        findings: [...deterministicFindings, ...(parsed.findings || [])],
        requirementVerifications: parsed.requirementVerifications || []
    };
}
