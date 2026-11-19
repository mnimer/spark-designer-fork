/**
 * @fileoverview Unit tests for the multi-input Spark SQL query E2E pipeline specification (I9).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Verifies pipeline discovery, three-source DAG topology validation, boundary and failure
 * arity rules on SparkSqlQuery, ExecutionPlan ordering, JDBC JAR resolution, and skill selection.
 */

import { describe, expect, it } from 'vitest';
import * as yaml from 'yaml';
import { buildPromptFromPipelineYaml } from '../../codegen/CodegenPromptBuilder';
import { validateDagTopology, type DagEdge, type DagNode } from '../../shared/dagValidator';
import { buildExecutionPlan } from '../../shared/executionPlanner';
import { loadSparkPipeline } from '../integration/codegen-runner';
import {
    auditSparkTransformProperties,
    parsePipelineSpec,
} from '../integration/evaluators/pipeline-spec-evaluator';
import { SKILLS_ROOT } from './helpers/codegenSkillsCorpus';

/**
 * Parsed structure of a serialized Spark pipeline document.
 */
interface ParsedSparkDocument {
    pipelineConfig?: Record<string, unknown>;
    jobConfig?: Record<string, unknown>;
    runtimeVariables?: { key: string; value: string }[];
    nodes: DagNode[];
    edges: DagEdge[];
}

/**
 * Loads and parses the multi-input Spark SQL pipeline specification from disk.
 *
 * @returns Raw YAML text and parsed pipeline document.
 */
function loadMultiInputSqlFixture(): { sparkContent: string; doc: ParsedSparkDocument } {
    const sparkContent = loadSparkPipeline(
        'e2e/multi-input-spark-sql/multi-input-spark-sql.spark'
    );
    const doc = yaml.parse(sparkContent) as ParsedSparkDocument;
    return { sparkContent, doc };
}

/**
 * Offline verification suite for Task I9 (multi-input-spark-sql).
 */
describe('E2E Spec: multi-input-spark-sql (I9)', () => {
    it('loads the pipeline YAML with 3 batch sources, 1 SparkSqlQuery step, and 1 IcebergSink', () => {
        const { sparkContent, doc } = loadMultiInputSqlFixture();

        expect(sparkContent.length).toBeGreaterThan(0);
        expect(doc.pipelineConfig?.language).toBe('PySpark');
        expect(doc.nodes).toHaveLength(5);
        expect(doc.edges).toHaveLength(4);
        for (const node of doc.nodes) {
            expect(node.type).toBe('pipeline');
            expect(typeof node.data?.iconName).toBe('string');
        }
        const sqlToSinkEdge = doc.edges.find((e) => e.id === 'edge_sql_to_iceberg');
        expect(sqlToSinkEdge?.sourceHandle).toBe('valid');

        const specFindings = auditSparkTransformProperties(parsePipelineSpec(sparkContent));
        expect(specFindings).toEqual([]);

        const jdbcNode = doc.nodes.find((n) => n.id === 'src_customers_jdbc');
        const jdbcProps = (jdbcNode?.data?.properties ?? {}) as Record<string, unknown>;
        expect(jdbcProps.authMode).toBe('secretManager');
        expect(jdbcProps.secretName).toBe(
            'projects/analytics-prod-project/secrets/commerce-postgres-reader-pw/versions/latest'
        );
        expect(jdbcProps.password).toBeUndefined();
        expect(jdbcProps.driverJarUri).toBe(
            'gs://analytics-lakehouse-artifacts/jars/postgresql-42.7.4.jar'
        );

        const sqlNode = doc.nodes.find((n) => n.id === 'step_enrich_sql');
        const sqlProps = (sqlNode?.data?.properties ?? {}) as Record<string, unknown>;
        expect(sqlNode?.data?.inputArity).toBe('n');
        expect(sqlProps.inputViewAliases).toEqual([
            { inputIndex: 0, viewName: 'orders_view' },
            { inputIndex: 1, viewName: 'attribution_view' },
            { inputIndex: 2, viewName: 'customers_view' },
        ]);
        expect(String(sqlProps.sqlQuery)).toContain('INNER JOIN customers_view');
        expect(String(sqlProps.sqlQuery)).toContain('LEFT JOIN attribution_view');
    });

    it('validates 3 inbound edges on SparkSqlQuery with zero errors and zero warnings', () => {
        const { doc } = loadMultiInputSqlFixture();
        const inboundToSql = doc.edges.filter((edge) => edge.target === 'step_enrich_sql');
        expect(inboundToSql).toHaveLength(3);

        const validation = validateDagTopology(doc.nodes, doc.edges, {
            jobConfig: doc.jobConfig,
        });
        expect(validation.isValid).toBe(true);
        expect(validation.hasCycles).toBe(false);
        expect(validation.orphanNodeIds).toEqual([]);
        expect(validation.errors).toEqual([]);
        expect(validation.warnings).toEqual([]);
    });

    it('accepts a single-source boundary input on SparkSqlQuery and rejects zero upstream inputs', () => {
        const { doc } = loadMultiInputSqlFixture();

        // Boundary condition: N-arity SparkSqlQuery remains valid with 1 connected source.
        const singleSourceNodes = doc.nodes.filter(
            (node) =>
                node.id === 'src_orders_parquet' ||
                node.id === 'step_enrich_sql' ||
                node.id === 'snk_revenue_iceberg'
        );
        const singleSourceEdges = doc.edges.filter(
            (edge) =>
                edge.source === 'src_orders_parquet' || edge.source === 'step_enrich_sql'
        );
        const singleInputResult = validateDagTopology(singleSourceNodes, singleSourceEdges, {
            jobConfig: doc.jobConfig,
        });
        expect(singleInputResult.isValid).toBe(true);
        expect(singleInputResult.errors).toEqual([]);

        // Failure condition: SparkSqlQuery with 0 upstream inputs fails validation.
        const missingUpstreamNodes = doc.nodes.filter(
            (node) => node.id === 'step_enrich_sql' || node.id === 'snk_revenue_iceberg'
        );
        const missingUpstreamEdges = doc.edges.filter(
            (edge) => edge.source === 'step_enrich_sql'
        );
        const zeroInputResult = validateDagTopology(missingUpstreamNodes, missingUpstreamEdges, {
            jobConfig: doc.jobConfig,
        });
        expect(zeroInputResult.isValid).toBe(false);
        expect(zeroInputResult.missingUpstreamNodeIds).toContain('step_enrich_sql');
    });

    it('orders all 3 upstream sources before SparkSqlQuery and places IcebergSink last in ExecutionPlan', () => {
        const { doc } = loadMultiInputSqlFixture();
        const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);

        expect(plan.pipelineMode).toBe('batch');
        expect(plan.streamingNodeIds).toEqual([]);
        expect(plan.cacheDirectives).toEqual([]);
        expect(plan.sortedNodeIds).toHaveLength(5);

        const sqlIdx = plan.sortedNodeIds.indexOf('step_enrich_sql');
        const sinkIdx = plan.sortedNodeIds.indexOf('snk_revenue_iceberg');
        expect(plan.sortedNodeIds.indexOf('src_orders_parquet')).toBeLessThan(sqlIdx);
        expect(plan.sortedNodeIds.indexOf('src_clickstream_json')).toBeLessThan(sqlIdx);
        expect(plan.sortedNodeIds.indexOf('src_customers_jdbc')).toBeLessThan(sqlIdx);
        expect(sqlIdx).toBe(3);
        expect(sinkIdx).toBe(4);

        expect(plan.runtimeArtifacts.profile.id).toBe('serverless_4.0');
        expect(plan.runtimeArtifacts.tier).toBe('standard');
        expect(plan.runtimeArtifacts.gcsJars).toContain(
            'gs://analytics-lakehouse-artifacts/jars/postgresql-42.7.4.jar'
        );
        expect(plan.runtimeArtifacts.preinstalledConnectors).toEqual(
            expect.arrayContaining(['gcs-connector', 'iceberg-spark-runtime'])
        );
    });

    it('selects io-parquet, io-json, io-jdbc, transform-spark-sql-query, and io-iceberg with zero redacted secrets', () => {
        const { sparkContent } = loadMultiInputSqlFixture();
        const built = buildPromptFromPipelineYaml({
            skillsRoot: SKILLS_ROOT,
            yamlText: sparkContent,
            language: 'PySpark',
        });

        expect(built.redactedPaths).toEqual([]);
        expect(built.detection.warnings).toEqual([]);
        expect(built.selectedSkills).toEqual(
            expect.arrayContaining([
                'io-parquet',
                'io-json',
                'io-jdbc',
                'transform-spark-sql-query',
                'io-iceberg',
            ])
        );
        expect(built.prompt).toContain('## Execution Plan');
        expect(built.prompt).toContain(
            'gs://analytics-lakehouse-artifacts/jars/postgresql-42.7.4.jar'
        );
    });
});
