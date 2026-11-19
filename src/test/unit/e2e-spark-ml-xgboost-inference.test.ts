/**
 * @fileoverview Unit tests for the Spark ML XGBoost inference E2E pipeline specification (I10).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Verifies pipeline discovery, DAG topology validation, ConditionalSplit handle routing,
 * batch fan-out caching, XGBoost runtime dependency resolution, and codegen skill selection.
 */

import { describe, expect, it } from 'vitest';
import * as yaml from 'yaml';
import { buildPromptFromPipelineYaml } from '../../codegen/CodegenPromptBuilder';
import { validateDagTopology, type DagEdge, type DagNode } from '../../shared/dagValidator';
import { buildExecutionPlan } from '../../shared/executionPlanner';
import { getConditionalSplitHandles } from '../../shared/sparkDagRules';
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
 * Loads and parses the Spark ML XGBoost inference pipeline specification from disk.
 *
 * @returns Raw YAML text and parsed pipeline document.
 */
function loadXgboostInferenceFixture(): { sparkContent: string; doc: ParsedSparkDocument } {
    const sparkContent = loadSparkPipeline(
        'e2e/spark-ml-xgboost-inference/spark-ml-xgboost-inference.spark'
    );
    const doc = yaml.parse(sparkContent) as ParsedSparkDocument;
    return { sparkContent, doc };
}

/**
 * Offline verification suite for Task I10 (spark-ml-xgboost-inference).
 */
describe('E2E Spec: spark-ml-xgboost-inference (I10)', () => {
    it('loads the pipeline YAML with IcebergSource, FeaturePrep, SparkMlAndVertexAi, ConditionalSplit, and BigQuerySinks', () => {
        const { sparkContent, doc } = loadXgboostInferenceFixture();

        expect(sparkContent.length).toBeGreaterThan(0);
        expect(doc.pipelineConfig?.language).toBe('PySpark');
        expect(doc.nodes).toHaveLength(6);
        expect(doc.edges).toHaveLength(5);
        for (const node of doc.nodes) {
            expect(node.type).toBe('pipeline');
            expect(typeof node.data?.iconName).toBe('string');
        }
        expect(doc.edges.find((e) => e.id === 'edge_features_to_xgboost')?.sourceHandle).toBe(
            'valid'
        );
        expect(doc.edges.find((e) => e.id === 'edge_xgboost_to_split')?.sourceHandle).toBe('valid');

        const specFindings = auditSparkTransformProperties(parsePipelineSpec(sparkContent));
        expect(specFindings).toEqual([]);

        const mlNode = doc.nodes.find((n) => n.id === 'step_xgboost_score');
        const mlProps = (mlNode?.data?.properties ?? {}) as Record<string, unknown>;
        expect(mlProps.engine).toBe('xgboost');
        expect(mlProps.useXgboost).toBe(true);
        expect(mlProps.xgboostTask).toBe('classification');
        expect(mlProps.convertVectorsToArray).toBe(true);

        const splitNode = doc.nodes.find((n) => n.id === 'step_risk_split');
        if (!splitNode) {
            throw new Error('Expected step_risk_split node in spark-ml-xgboost-inference fixture');
        }
        const splitHandles = getConditionalSplitHandles(splitNode);
        expect(splitHandles).toEqual([
            {
                id: 'high_risk',
                label: 'high_risk',
                condition: 'size(probability) > 1 AND probability[1] >= 0.8',
            },
            {
                id: 'else',
                label: 'else',
            },
        ]);
    });

    it('validates the DAG topology with zero cycles, zero orphans, zero errors, and zero warnings', () => {
        const { doc } = loadXgboostInferenceFixture();

        const validation = validateDagTopology(doc.nodes, doc.edges, {
            jobConfig: doc.jobConfig,
        });
        expect(validation.isValid).toBe(true);
        expect(validation.hasCycles).toBe(false);
        expect(validation.orphanNodeIds).toEqual([]);
        expect(validation.errors).toEqual([]);
        expect(validation.warnings).toEqual([]);
    });

    it('resolves ConditionalSplit branch targets, batch fan-out caching, and xgboost runtime packages in ExecutionPlan', () => {
        const { doc } = loadXgboostInferenceFixture();
        const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);

        expect(plan.pipelineMode).toBe('batch');
        expect(plan.streamingNodeIds).toEqual([]);
        expect(plan.sortedNodeIds).toEqual([
            'src_transactions_iceberg',
            'step_feature_prep',
            'step_xgboost_score',
            'step_risk_split',
            'snk_high_risk_bq',
            'snk_standard_bq',
        ]);

        expect(plan.conditionalSplits).toHaveLength(1);
        expect(plan.conditionalSplits[0]).toEqual({
            nodeId: 'step_risk_split',
            branches: [
                {
                    handleId: 'high_risk',
                    label: 'high_risk',
                    condition: 'size(probability) > 1 AND probability[1] >= 0.8',
                    isFallback: false,
                    targetNodeIds: ['snk_high_risk_bq'],
                },
                {
                    handleId: 'else',
                    label: 'else',
                    isFallback: true,
                    targetNodeIds: ['snk_standard_bq'],
                },
            ],
        });

        expect(plan.cacheDirectives).toEqual([
            {
                nodeId: 'step_risk_split',
                reason: 'batch_fanout',
                outDegree: 2,
                unpersistAfterNodeIds: ['snk_standard_bq'],
            },
        ]);
        expect(plan.cachedNodeIds).toEqual(['step_risk_split']);
        expect(plan.unpersistPoints).toEqual({
            snk_standard_bq: ['step_risk_split'],
        });

        expect(plan.runtimeArtifacts.profile.id).toBe('serverless_4.0');
        expect(plan.runtimeArtifacts.pipPackages).toContain('xgboost');
        expect(plan.runtimeArtifacts.preinstalledConnectors).toEqual([
            'iceberg-spark-runtime',
            'spark-bigquery',
        ]);
    });

    it('handles single-branch ConditionalSplit boundary and detects disconnected sink orphans', () => {
        const { doc } = loadXgboostInferenceFixture();

        // Boundary condition: Wiring only high_risk to a single BigQuerySink skips fan-out caching.
        const singleBranchNodes = doc.nodes.filter((node) => node.id !== 'snk_standard_bq');
        const singleBranchEdges = doc.edges.filter((edge) => edge.target !== 'snk_standard_bq');
        const singleBranchValidation = validateDagTopology(singleBranchNodes, singleBranchEdges, {
            jobConfig: doc.jobConfig,
        });
        expect(singleBranchValidation.isValid).toBe(true);

        const singleBranchPlan = buildExecutionPlan(
            singleBranchNodes,
            singleBranchEdges,
            doc.jobConfig
        );
        expect(singleBranchPlan.cacheDirectives).toEqual([]);
        expect(singleBranchPlan.conditionalSplits[0].branches).toEqual([
            {
                handleId: 'high_risk',
                label: 'high_risk',
                condition: 'size(probability) > 1 AND probability[1] >= 0.8',
                isFallback: false,
                targetNodeIds: ['snk_high_risk_bq'],
            },
            {
                handleId: 'else',
                label: 'else',
                isFallback: true,
                targetNodeIds: [],
            },
        ]);

        // Failure condition: Leaving snk_standard_bq in nodes without an incoming edge flags an orphan.
        const orphanValidation = validateDagTopology(doc.nodes, singleBranchEdges, {
            jobConfig: doc.jobConfig,
        });
        expect(orphanValidation.isValid).toBe(false);
        expect(orphanValidation.orphanNodeIds).toEqual(['snk_standard_bq']);
    });

    it('selects io-iceberg, transform-feature-prep, transform-spark-ml-vertex-ai, transform-conditional-split, and io-bigquery', () => {
        const { sparkContent } = loadXgboostInferenceFixture();
        const built = buildPromptFromPipelineYaml({
            skillsRoot: SKILLS_ROOT,
            yamlText: sparkContent,
            language: 'PySpark',
        });

        expect(built.redactedPaths).toEqual([]);
        expect(built.detection.warnings).toEqual([]);
        expect(built.selectedSkills).toEqual(
            expect.arrayContaining([
                'io-iceberg',
                'transform-feature-prep',
                'transform-spark-ml-vertex-ai',
                'transform-conditional-split',
                'io-bigquery',
            ])
        );
        expect(built.prompt).toContain('## Execution Plan');
        expect(built.prompt).toContain('batch_fanout');
        expect(built.prompt).toContain('xgboost');
    });
});
