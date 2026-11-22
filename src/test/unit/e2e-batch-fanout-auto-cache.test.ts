/**
 * Unit tests for the batch fan-out auto-cache E2E pipeline specification (I6).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies that the batch-fanout-auto-cache pipeline satisfies DAG topological rules,
 * emits a batch_fanout cache directive with outDegree 2 on the WithColumns step,
 * schedules unpersist after the final downstream sink, resolves the Cloud Spanner
 * Spark 4.0 connector JAR, and injects .cache() and .unpersist() guidance into the prompt.
 */

import * as path from 'path';
import { describe, expect, it } from 'vitest';
import * as yaml from 'yaml';
import { buildPromptFromPipelineYaml, renderExecutionPlan } from '../../codegen/CodegenPromptBuilder';
import { CODEGEN_RESOURCE_PATHS } from '../../codegen/codegenConfig';
import { validateDagTopology, type DagEdge, type DagNode } from '../../shared/dagValidator';
import { buildExecutionPlan, type CacheDirective } from '../../shared/executionPlanner';
import { getResourcesRoot, loadSparkPipeline } from '../integration/codegen-runner';
import {
    auditSparkTransformProperties,
    parsePipelineSpec,
} from '../integration/evaluators/pipeline-spec-evaluator';

const E2E_PIPELINE_PATH = 'e2e/batch-fanout-auto-cache/batch-fanout-auto-cache.spark';
const SPANNER_SPARK_40_JAR = 'gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar';

interface ParsedSparkDocument {
    nodes: DagNode[];
    edges: DagEdge[];
    runtimeVariables?: Array<{ key: string; value: string }>;
    jobConfig?: Record<string, unknown>;
    pipelineConfig?: Record<string, unknown>;
}

/**
 * Loads and parses the batch fan-out auto-cache pipeline YAML document.
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

describe('E2E batch-fanout-auto-cache pipeline (I6)', () => {
    describe('pipeline loading and connector property contract', () => {
        it('locates the pipeline file via loadSparkPipeline and rejects missing paths', () => {
            const content = loadSparkPipeline(E2E_PIPELINE_PATH);
            expect(content.length).toBeGreaterThan(0);
            expect(loadSparkPipeline('e2e/batch-fanout-auto-cache/batch-fanout-auto-cache')).toBe(
                content
            );

            expect(() =>
                loadSparkPipeline('e2e/batch-fanout-auto-cache/does-not-exist.spark')
            ).toThrow(/Could not locate \.spark pipeline file/);
        });

        it('configures complete valid properties on BigQuerySource, WithColumns, IcebergSink, and SpannerSink', () => {
            const { rawYaml, doc } = loadParsedPipeline();

            expect(doc.pipelineConfig?.language).toBe('PySpark');
            expect(doc.nodes).toHaveLength(4);
            expect(doc.edges).toHaveLength(3);

            const fanoutEdges = doc.edges.filter((e) => e.source === 'enrich_orders');
            expect(fanoutEdges).toEqual([
                expect.objectContaining({
                    source: 'enrich_orders',
                    sourceHandle: 'valid',
                    target: 'iceberg_orders_sink',
                }),
                expect.objectContaining({
                    source: 'enrich_orders',
                    sourceHandle: 'valid',
                    target: 'spanner_orders_sink',
                }),
            ]);

            const bqNode = doc.nodes.find((n) => n.data?.spark_transform === 'BigQuerySource');
            const withColsNode = doc.nodes.find((n) => n.data?.spark_transform === 'WithColumns');
            const icebergNode = doc.nodes.find((n) => n.data?.spark_transform === 'IcebergSink');
            const spannerNode = doc.nodes.find((n) => n.data?.spark_transform === 'SpannerSink');

            expect(bqNode?.data?.properties).toMatchObject({
                readModeType: 'table',
                projectId: 'acme-analytics-prod',
                dataset: 'commerce',
                table: 'acme-analytics-prod.commerce.customer_orders',
            });

            const columns = withColsNode?.data?.properties?.columns as Array<Record<string, string>>;
            expect(Array.isArray(columns)).toBe(true);
            expect(columns.length).toBeGreaterThanOrEqual(2);
            for (const col of columns) {
                expect(col.name.trim().length).toBeGreaterThan(0);
                expect(col.expression.trim().length).toBeGreaterThan(0);
            }

            expect(icebergNode?.data?.properties).toMatchObject({
                writeOperation: 'append',
                catalogName: 'biglake',
                catalogType: 'biglake',
                namespace: 'curated_sales',
                table: 'biglake.curated_sales.enriched_orders',
                warehouseLocation: 'gs://acme-lakehouse-prod/iceberg/warehouse',
                fanoutEnabled: true,
            });

            expect(spannerNode?.data?.properties).toMatchObject({
                projectId: 'acme-analytics-prod',
                instanceId: 'orders-serving-prod',
                databaseId: 'commerce_ledger',
                table: 'EnrichedOrders',
                saveMode: 'append',
                mutationMode: 'insert_or_update',
            });

            const specFindings = auditSparkTransformProperties(parsePipelineSpec(rawYaml));
            expect(specFindings).toEqual([]);
        });
    });

    describe('DAG topology validation', () => {
        it('validates the fan-out DAG with zero errors, zero warnings, and zero orphans', () => {
            const { doc } = loadParsedPipeline();
            const result = validateDagTopology(doc.nodes, doc.edges, {
                jobConfig: doc.jobConfig,
            });

            expect(result.isValid).toBe(true);
            expect(result.hasCycles).toBe(false);
            expect(result.orphanNodeIds).toEqual([]);
            expect(result.errors).toEqual([]);
            expect(result.warnings).toEqual([]);
        });

        it('fails validation when a disconnected orphan sink is added (failure condition)', () => {
            const { doc } = loadParsedPipeline();
            const invalidNodes: DagNode[] = [
                ...doc.nodes,
                {
                    id: 'orphan_sink',
                    type: 'pipeline',
                    data: {
                        label: 'Orphan Sink',
                        type: 'SINK',
                        spark_transform: 'ConsoleSink',
                    },
                },
            ];

            const result = validateDagTopology(invalidNodes, doc.edges);
            expect(result.isValid).toBe(false);
            expect(result.orphanNodeIds).toEqual(['orphan_sink']);
            expect(result.errors.length).toBeGreaterThan(0);
        });
    });

    describe('ExecutionPlan batch fan-out caching and Spanner JAR resolution', () => {
        it('emits a batch_fanout cache directive on WithColumns with outDegree 2 and schedules unpersist after both sinks finish', () => {
            const { doc } = loadParsedPipeline();
            const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);

            expect(plan.pipelineMode).toBe('batch');
            expect(plan.streamingNodeIds).toEqual([]);
            expect(plan.sortedNodeIds).toEqual([
                'bq_orders_source',
                'enrich_orders',
                'iceberg_orders_sink',
                'spanner_orders_sink',
            ]);

            expect(plan.cachedNodeIds).toEqual(['enrich_orders']);
            expect(plan.nodes.enrich_orders.isCached).toBe(true);
            expect(plan.nodes.bq_orders_source.isCached).toBe(false);

            // SpannerSink executes after IcebergSink in topological order, so unpersist runs after SpannerSink.
            expect(plan.cacheDirectives).toEqual([
                {
                    nodeId: 'enrich_orders',
                    reason: 'batch_fanout',
                    outDegree: 2,
                    unpersistAfterNodeIds: ['spanner_orders_sink'],
                },
            ]);
            expect(plan.unpersistPoints).toEqual({
                spanner_orders_sink: ['enrich_orders'],
            });

            const renderedJson = parseRenderedExecutionPlanJson(renderExecutionPlan(plan));
            expect(renderedJson.isStreaming).toBe(false);
            const cacheNodes = renderedJson.cacheNodes as CacheDirective[];
            expect(cacheNodes).toEqual([
                {
                    nodeId: 'enrich_orders',
                    reason: 'batch_fanout',
                    outDegree: 2,
                    unpersistAfterNodeIds: ['spanner_orders_sink'],
                },
            ]);

            // Reversing sink edge order places IcebergSink last and moves unpersistAfterNodeIds to IcebergSink.
            const reversedEdges: DagEdge[] = [
                doc.edges[0],
                doc.edges[2],
                doc.edges[1],
            ];
            const reversedPlan = buildExecutionPlan(doc.nodes, reversedEdges, doc.jobConfig);
            expect(reversedPlan.sortedNodeIds).toEqual([
                'bq_orders_source',
                'enrich_orders',
                'spanner_orders_sink',
                'iceberg_orders_sink',
            ]);
            expect(reversedPlan.cacheDirectives[0].unpersistAfterNodeIds).toEqual([
                'iceberg_orders_sink',
            ]);
        });

        it('suppresses automatic caching when cacheOutput is explicitly disabled on WithColumns (boundary)', () => {
            const { doc } = loadParsedPipeline();
            const suppressedNodes = doc.nodes.map((node) =>
                node.id === 'enrich_orders'
                    ? {
                          ...node,
                          data: {
                              ...node.data,
                              properties: {
                                  ...node.data?.properties,
                                  cacheOutput: false,
                              },
                          },
                      }
                    : node
            );

            const plan = buildExecutionPlan(suppressedNodes, doc.edges, doc.jobConfig);
            expect(plan.cacheDirectives).toEqual([]);
            expect(plan.cachedNodeIds).toEqual([]);
            expect(plan.nodes.enrich_orders.isCached).toBe(false);
        });

        it('resolves the Cloud Spanner Spark 4.0 JAR and Managed Spark pre-installed connectors', () => {
            const { doc } = loadParsedPipeline();
            const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);

            expect(plan.runtimeArtifacts.gcsJars).toContain(SPANNER_SPARK_40_JAR);
            expect(plan.runtimeArtifacts.preinstalledConnectors).toEqual(
                expect.arrayContaining(['spark-bigquery', 'iceberg-spark-runtime'])
            );
            expect(plan.runtimeArtifacts.mavenPackages).toEqual([]);

            // Omitting jarUri on SpannerSink still resolves the default Spark 4.0 Spanner JAR.
            const nodesWithoutExplicitJar = doc.nodes.map((node) => {
                if (node.id !== 'spanner_orders_sink') {
                    return node;
                }
                const nextProps = { ...node.data?.properties };
                delete nextProps.jarUri;
                return {
                    ...node,
                    data: {
                        ...node.data,
                        properties: nextProps,
                    },
                };
            });
            const defaultJarPlan = buildExecutionPlan(
                nodesWithoutExplicitJar,
                doc.edges,
                doc.jobConfig
            );
            expect(defaultJarPlan.runtimeArtifacts.gcsJars).toContain(SPANNER_SPARK_40_JAR);
        });
    });

    describe('prompt assembly and skill selection', () => {
        it('selects BigQuery, WithColumns, Iceberg, and Spanner skills and injects .cache() and .unpersist() directives', () => {
            const { rawYaml } = loadParsedPipeline();
            const skillsRoot = path.join(getResourcesRoot(), CODEGEN_RESOURCE_PATHS.skillsDir);

            const built = buildPromptFromPipelineYaml({
                skillsRoot,
                yamlText: rawYaml,
                language: 'PySpark',
            });

            expect(built.redactedPaths).toEqual([]);
            expect(built.detection.warnings).toEqual([]);
            expect(built.selectedSkills).toEqual(
                expect.arrayContaining([
                    'io-bigquery',
                    'transform-with-columns',
                    'io-iceberg',
                    'io-spanner',
                    'core-catalyst-optimization',
                ])
            );
            expect(built.selectedSkills).not.toContain('core-streaming');

            // Extract the ## Execution Plan section and verify .cache(), .unpersist(), cacheNodes, and runtimeJars.
            const execPlanStart = built.prompt.indexOf('## Execution Plan');
            expect(execPlanStart).toBeGreaterThan(-1);
            const nextSkillStart = built.prompt.indexOf('## Skill:', execPlanStart);
            const execPlanSection = built.prompt.slice(execPlanStart, nextSkillStart);

            expect(execPlanSection).toContain('.cache()');
            expect(execPlanSection).toContain('.unpersist()');
            expect(execPlanSection).toContain('`enrich_orders`');
            expect(execPlanSection).toContain('`batch_fanout`');
            expect(execPlanSection).toContain(SPANNER_SPARK_40_JAR);
        });
    });
});
