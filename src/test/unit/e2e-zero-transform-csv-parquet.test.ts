/**
 * Unit tests for the zero-transform CSV to Parquet E2E pipeline specification (I5).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies that the zero-transform-csv-to-parquet pipeline loads through the integration
 * runner, satisfies DAG topological and Spark semantic rules with zero intermediate steps,
 * produces a batch ExecutionPlan without caching, and selects the CSV and Parquet skills.
 */

import * as path from 'path';
import { describe, expect, it } from 'vitest';
import * as yaml from 'yaml';
import { buildPromptFromPipelineYaml, renderExecutionPlan } from '../../codegen/CodegenPromptBuilder';
import { CODEGEN_RESOURCE_PATHS } from '../../codegen/codegenConfig';
import { validateDagTopology, type DagEdge, type DagNode } from '../../shared/dagValidator';
import { buildExecutionPlan } from '../../shared/executionPlanner';
import { getResourcesRoot, loadSparkPipeline } from '../integration/codegen-runner';
import {
    auditSparkTransformProperties,
    parsePipelineSpec,
} from '../integration/evaluators/pipeline-spec-evaluator';

const E2E_PIPELINE_PATH = 'e2e/zero-transform-csv-to-parquet/zero-transform-csv-to-parquet.spark';

interface ParsedSparkDocument {
    nodes: DagNode[];
    edges: DagEdge[];
    runtimeVariables?: Array<{ key: string; value: string }>;
    jobConfig?: Record<string, unknown>;
    pipelineConfig?: Record<string, unknown>;
}

/**
 * Loads and parses the zero-transform CSV to Parquet pipeline YAML document.
 *
 * @returns Raw YAML text and parsed pipeline object.
 */
function loadParsedPipeline(): { rawYaml: string; doc: ParsedSparkDocument } {
    const rawYaml = loadSparkPipeline(E2E_PIPELINE_PATH);
    const doc = yaml.parse(rawYaml) as ParsedSparkDocument;
    return { rawYaml, doc };
}

/**
 * Extracts the serialized JSON ExecutionPlan payload from a rendered section.
 *
 * @param renderedSection Rendered Execution Plan markdown block.
 * @returns Parsed JSON object embedded in the fenced block.
 */
function parseRenderedExecutionPlanJson(renderedSection: string): Record<string, unknown> {
    const match = /```json\n([\s\S]*?)\n```/.exec(renderedSection);
    if (!match) {
        throw new Error('Expected a fenced JSON block inside the rendered Execution Plan.');
    }
    return JSON.parse(match[1]) as Record<string, unknown>;
}

describe('E2E zero-transform-csv-to-parquet pipeline (I5)', () => {
    describe('pipeline loading and property contract', () => {
        it('locates the pipeline file with or without the explicit file extension', () => {
            const withExt = loadSparkPipeline(E2E_PIPELINE_PATH);
            const withoutExt = loadSparkPipeline(
                'e2e/zero-transform-csv-to-parquet/zero-transform-csv-to-parquet'
            );

            expect(withExt.length).toBeGreaterThan(0);
            expect(withoutExt).toBe(withExt);
        });

        it('throws a descriptive error when the requested E2E pipeline file does not exist', () => {
            expect(() =>
                loadSparkPipeline('e2e/zero-transform-csv-to-parquet/missing-fixture.spark')
            ).toThrow(/Could not locate \.spark pipeline file/);
        });

        it('defines a direct CsvSource to ParquetSink edge with zero STEP nodes and complete storage properties', () => {
            const { rawYaml, doc } = loadParsedPipeline();

            expect(doc.pipelineConfig?.language).toBe('PySpark');
            expect(doc.jobConfig).toMatchObject({
                runner: 'ServerlessSpark',
                runtimeVersion: '4.0',
                tier: 'standard',
            });

            expect(doc.nodes).toHaveLength(2);
            const stepNodes = doc.nodes.filter((n) => n.data?.type === 'STEP');
            expect(stepNodes).toHaveLength(0);

            const csvNode = doc.nodes.find((n) => n.data?.spark_transform === 'CsvSource');
            const parquetNode = doc.nodes.find((n) => n.data?.spark_transform === 'ParquetSink');
            expect(csvNode).toBeDefined();
            expect(parquetNode).toBeDefined();

            expect(csvNode?.data?.type).toBe('SOURCE');
            expect(csvNode?.data?.properties).toMatchObject({
                readMode: 'batch',
                header: true,
                inferSchema: true,
                storageBackend: 'gcs',
            });
            expect(String(csvNode?.data?.properties?.path ?? '')).toMatch(/^gs:\/\//);

            expect(parquetNode?.data?.type).toBe('SINK');
            expect(parquetNode?.data?.properties).toMatchObject({
                storageBackend: 'gcs',
                saveMode: 'overwrite',
                compression: 'snappy',
            });
            expect(String(parquetNode?.data?.properties?.path ?? '')).toMatch(/^gs:\/\//);

            expect(doc.edges).toEqual([
                expect.objectContaining({
                    source: csvNode?.id,
                    target: parquetNode?.id,
                }),
            ]);

            // Verify canonical transform audit produces zero specification findings.
            const specFindings = auditSparkTransformProperties(parsePipelineSpec(rawYaml));
            expect(specFindings).toEqual([]);
        });
    });

    describe('DAG topology and deterministic ExecutionPlan', () => {
        it('passes validateDagTopology with zero errors, zero warnings, and zero cycles', () => {
            const { doc } = loadParsedPipeline();
            const validation = validateDagTopology(doc.nodes, doc.edges, {
                jobConfig: doc.jobConfig,
            });

            expect(validation.isValid).toBe(true);
            expect(validation.hasCycles).toBe(false);
            expect(validation.orphanNodeIds).toEqual([]);
            expect(validation.errors).toEqual([]);
            expect(validation.warnings).toEqual([]);
            expect(validation.sortedNodeIds).toEqual(['csv_source', 'parquet_sink']);
        });

        it('orders CsvSource before ParquetSink in batch mode with empty cache directives', () => {
            const { doc } = loadParsedPipeline();
            const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);

            expect(plan.sortedNodeIds).toEqual(['csv_source', 'parquet_sink']);
            expect(plan.pipelineMode).toBe('batch');
            expect(plan.streamingNodeIds).toEqual([]);
            expect(plan.cacheDirectives).toEqual([]);
            expect(plan.cachedNodeIds).toEqual([]);
            expect(plan.unpersistPoints).toEqual({});
            expect(plan.streamingSinkGroups).toEqual([]);
            expect(plan.nodes.csv_source.executionMode).toBe('batch');
            expect(plan.nodes.parquet_sink.executionMode).toBe('batch');
            expect(plan.runtimeArtifacts.preinstalledConnectors).toContain('gcs-connector');
            expect(plan.runtimeArtifacts.mavenPackages).toEqual([]);

            const renderedJson = parseRenderedExecutionPlanJson(renderExecutionPlan(plan));
            expect(renderedJson.isStreaming).toBe(false);
            expect(renderedJson.cacheNodes).toEqual([]);
            expect(renderedJson.sortedNodeIds).toEqual(['csv_source', 'parquet_sink']);
        });

        it('propagates streaming mode to ParquetSink when CsvSource switches to streaming (boundary)', () => {
            const { doc } = loadParsedPipeline();
            const streamingNodes = doc.nodes.map((node) =>
                node.id === 'csv_source'
                    ? {
                          ...node,
                          data: {
                              ...node.data,
                              properties: {
                                  ...node.data?.properties,
                                  readMode: 'streaming',
                              },
                          },
                      }
                    : node
            );

            const streamingPlan = buildExecutionPlan(streamingNodes, doc.edges, {
                ...doc.jobConfig,
                checkpointRoot: 'gs://acme-spark-staging/checkpoints',
            });
            expect(streamingPlan.pipelineMode).toBe('streaming');
            expect(streamingPlan.streamingNodeIds).toEqual(['csv_source', 'parquet_sink']);
            expect(streamingPlan.streamingSinkGroups).toHaveLength(1);
            expect(streamingPlan.streamingSinkGroups[0].mode).toBe('native');
        });
    });

    describe('prompt assembly and skill selection', () => {
        it('selects io-csv and io-parquet and injects the deterministic Execution Plan section', () => {
            const { rawYaml } = loadParsedPipeline();
            const skillsRoot = path.join(getResourcesRoot(), CODEGEN_RESOURCE_PATHS.skillsDir);

            const built = buildPromptFromPipelineYaml({
                skillsRoot,
                yamlText: rawYaml,
                language: 'PySpark',
            });

            expect(built.redactedPaths).toEqual([]);
            expect(built.detection.warnings).toEqual([]);
            expect(built.selectedSkills).toContain('io-csv');
            expect(built.selectedSkills).toContain('io-parquet');
            expect(built.selectedSkills).not.toContain('core-streaming');

            expect(built.prompt).toContain('## Execution Plan');
            expect(built.prompt).toContain('`csv_source` -> `parquet_sink`');
            expect(built.prompt).toContain('`isStreaming: false`');
            expect(built.prompt).toContain('"cacheNodes": []');
        });
    });
});
