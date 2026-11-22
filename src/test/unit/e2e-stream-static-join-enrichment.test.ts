/**
 * Unit Tests for the Stream-Static Broadcast Join Enrichment E2E Pipeline Specification (I8).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Verifies DAG topology, Join handle wiring, and asymmetric stream-static mode propagation.
 * Checks broadcast lookup configuration and codegen skill selection for pipeline I8.
 */

import * as fs from 'fs';
import * as path from 'path';
import { describe, expect, it } from 'vitest';
import YAML from 'yaml';
import { buildPromptFromPipelineYaml } from '../../codegen/CodegenPromptBuilder';
import { validateDagTopology, type DagEdge, type DagNode } from '../../shared/dagValidator';
import { buildExecutionPlan } from '../../shared/executionPlanner';
import { computeStreamingNodeSet } from '../../shared/sparkDagRules';
import { loadSparkPipeline } from '../integration/codegen-runner';
import {
    auditSparkTransformProperties,
    parsePipelineSpec,
} from '../integration/evaluators/pipeline-spec-evaluator';

/**
 * Parsed shape of a serialized E2E pipeline YAML document.
 */
interface ParsedSparkPipeline {
    nodes: DagNode[];
    edges: DagEdge[];
    runtimeVariables?: { key: string; value: string }[];
    jobConfig?: Record<string, unknown>;
    pipelineConfig?: Record<string, unknown>;
}

const E2E_REL_PATH =
    'e2e/stream-static-join-enrichment/stream-static-join-enrichment.spark';
const SKILLS_ROOT = path.resolve(__dirname, '../../../resources/codegen-skills');

/**
 * Loads and parses the stream-static-join-enrichment pipeline YAML from disk.
 *
 * @returns Raw YAML text and parsed pipeline document.
 */
function readStreamStaticJoinPipeline(): { rawYaml: string; doc: ParsedSparkPipeline } {
    const filePath = path.resolve(__dirname, '../integration', E2E_REL_PATH);
    expect(fs.existsSync(filePath)).toBe(true);
    const rawYaml = loadSparkPipeline(E2E_REL_PATH);
    const doc = YAML.parse(rawYaml) as ParsedSparkPipeline;
    return { rawYaml, doc };
}

describe('E2E Pipeline Specification: stream-static-join-enrichment (I8)', () => {
    it('loads via loadSparkPipeline with explicit left and right Join handles and canonical transforms', () => {
        const { rawYaml, doc } = readStreamStaticJoinPipeline();

        expect(doc.nodes.map((n) => n.data?.spark_transform)).toEqual([
            'PubSubSource',
            'BigQuerySource',
            'Join',
            'BigQuerySink',
        ]);

        const leftEdge = doc.edges.find(
            (e) => e.target === 'step_join_enrich' && e.targetHandle === 'left'
        );
        const rightEdge = doc.edges.find(
            (e) => e.target === 'step_join_enrich' && e.targetHandle === 'right'
        );
        expect(leftEdge?.source).toBe('src_pubsub');
        expect(rightEdge?.source).toBe('src_bq_lookup');

        const joinNode = doc.nodes.find((n) => n.id === 'step_join_enrich');
        expect(joinNode?.data?.properties?.broadcastRight).toBe(true);
        expect(joinNode?.data?.properties?.joinType).toBe('left');

        const spec = parsePipelineSpec(rawYaml);
        const transformFindings = auditSparkTransformProperties(spec);
        expect(transformFindings).toEqual([]);
    });

    it('passes validateDagTopology with requireCheckpoints enabled and rejects unsupported stream-static outer joins', () => {
        const { doc } = readStreamStaticJoinPipeline();

        const validation = validateDagTopology(doc.nodes, doc.edges, {
            jobConfig: doc.jobConfig,
            requireCheckpoints: true,
        });

        expect(validation.isValid).toBe(true);
        expect(validation.hasCycles).toBe(false);
        expect(validation.orphanNodeIds).toEqual([]);
        expect(validation.errors).toEqual([]);
        expect(validation.warnings).toEqual([]);

        // Verify standalone topology validation without jobConfig also emits zero warnings.
        const standaloneValidation = validateDagTopology(doc.nodes, doc.edges);
        expect(standaloneValidation.isValid).toBe(true);
        expect(standaloneValidation.errors).toEqual([]);
        expect(standaloneValidation.warnings).toEqual([]);

        // Verify failure condition when Join switches to an unsupported right_outer stream-static join.
        const invalidNodes = doc.nodes.map((node) =>
            node.id === 'step_join_enrich'
                ? {
                      ...node,
                      data: {
                          ...node.data,
                          properties: { ...node.data?.properties, joinType: 'right_outer' },
                      },
                  }
                : node
        );
        const rejectedValidation = validateDagTopology(invalidNodes, doc.edges, {
            jobConfig: doc.jobConfig,
            requireCheckpoints: true,
        });
        expect(rejectedValidation.isValid).toBe(false);
        expect(rejectedValidation.errors[0]).toContain('Stream-static Join');
    });

    it('marks PubSubSource, Join, and BigQuerySink as streaming while keeping BigQuerySource static in ExecutionPlan', () => {
        const { doc } = readStreamStaticJoinPipeline();

        const propagation = computeStreamingNodeSet(doc.nodes, doc.edges);
        expect(propagation.streamingNodeIds.has('src_pubsub')).toBe(true);
        expect(propagation.streamingNodeIds.has('step_join_enrich')).toBe(true);
        expect(propagation.streamingNodeIds.has('snk_bigquery')).toBe(true);
        expect(propagation.streamingNodeIds.has('src_bq_lookup')).toBe(false);
        expect(propagation.joinModes.get('step_join_enrich')).toBe('stream_static');

        const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);
        expect(plan.pipelineMode).toBe('streaming');
        expect(plan.streamingNodeIds).toEqual([
            'src_pubsub',
            'step_join_enrich',
            'snk_bigquery',
        ]);
        expect(plan.streamingNodeIds).not.toContain('src_bq_lookup');

        expect(plan.nodes.src_pubsub.executionMode).toBe('streaming');
        expect(plan.nodes.src_bq_lookup.executionMode).toBe('batch');
        expect(plan.nodes.step_join_enrich.executionMode).toBe('streaming');
        expect(plan.nodes.step_join_enrich.joinMode).toBe('stream_static');
        expect(plan.nodes.snk_bigquery.executionMode).toBe('streaming');

        // Single native BigQuery streaming sink runs in native DataStreamWriter mode.
        expect(plan.streamingSinkGroups).toHaveLength(1);
        expect(plan.streamingSinkGroups[0].mode).toBe('native');
        expect(plan.streamingSinkGroups[0].sinkIds).toEqual(['snk_bigquery']);
        expect(plan.streamingSinkGroups[0].settings.checkpointLocation).toBeTruthy();

        expect(plan.runtimeArtifacts.pythonDataSources).toContain('PubSubDataSource');
        expect(plan.runtimeArtifacts.preinstalledConnectors).toContain('spark-bigquery');
    });

    it('selects core-streaming, core-python-datasource, io-pubsub, io-bigquery, and transform-join skills', () => {
        const { rawYaml } = readStreamStaticJoinPipeline();
        const built = buildPromptFromPipelineYaml({
            skillsRoot: SKILLS_ROOT,
            yamlText: rawYaml,
            language: 'PySpark',
        });

        expect(built.redactedPaths).toEqual([]);
        expect(built.detection.warnings).toEqual([]);
        expect(built.selectedSkills).toEqual(
            expect.arrayContaining([
                'core-streaming',
                'core-python-datasource',
                'io-pubsub',
                'io-bigquery',
                'transform-join',
            ])
        );

        expect(built.prompt).toContain('PubSubDataSource');
        expect(built.prompt).toContain('F.broadcast');
        expect(built.prompt).toContain('stream_static');
    });
});
