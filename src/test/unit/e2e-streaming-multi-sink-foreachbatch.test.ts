/**
 * Unit Tests for the Streaming Multi-Sink foreachBatch E2E Pipeline Specification (I7).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Verifies DAG topology, checkpoint enforcement, and unified foreachBatch sink grouping.
 * Checks micro-batch caching flags, IAM credential safety, and skill selection for I7.
 */

import * as fs from 'fs';
import * as path from 'path';
import { describe, expect, it } from 'vitest';
import YAML from 'yaml';
import { buildPromptFromPipelineYaml } from '../../codegen/CodegenPromptBuilder';
import { validateDagTopology, type DagEdge, type DagNode } from '../../shared/dagValidator';
import { buildExecutionPlan } from '../../shared/executionPlanner';
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
    'e2e/streaming-multi-sink-foreachbatch/streaming-multi-sink-foreachbatch.spark';
const SKILLS_ROOT = path.resolve(__dirname, '../../../resources/codegen-skills');

/**
 * Loads and parses the streaming-multi-sink-foreachbatch pipeline YAML from disk.
 *
 * @returns Raw YAML text and parsed pipeline document.
 */
function readMultiSinkPipeline(): { rawYaml: string; doc: ParsedSparkPipeline } {
    const filePath = path.resolve(__dirname, '../integration', E2E_REL_PATH);
    expect(fs.existsSync(filePath)).toBe(true);
    const rawYaml = loadSparkPipeline(E2E_REL_PATH);
    const doc = YAML.parse(rawYaml) as ParsedSparkPipeline;
    return { rawYaml, doc };
}

describe('E2E Pipeline Specification: streaming-multi-sink-foreachbatch (I7)', () => {
    it('loads via loadSparkPipeline and passes canonical spark_transform specification audits', () => {
        const { rawYaml, doc } = readMultiSinkPipeline();

        expect(doc.nodes.map((n) => n.data?.spark_transform)).toEqual([
            'KafkaSource',
            'ParsePayload',
            'DeltaSink',
            'CloudSqlSink',
        ]);

        const spec = parsePipelineSpec(rawYaml);
        const transformFindings = auditSparkTransformProperties(spec);
        expect(transformFindings).toEqual([]);

        const cloudSqlNode = doc.nodes.find((n) => n.id === 'snk_cloudsql');
        expect(cloudSqlNode?.data?.properties?.authMode).toBe('iam');
        expect(cloudSqlNode?.data?.properties?.password).toBeUndefined();
    });

    it('passes validateDagTopology with requireCheckpoints enabled and zero errors or warnings', () => {
        const { doc } = readMultiSinkPipeline();

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
    });

    it('groups DeltaSink and CloudSqlSink into one foreachBatch group with batch caching and idempotent txn options', () => {
        const { doc } = readMultiSinkPipeline();
        const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);

        expect(plan.pipelineMode).toBe('streaming');
        expect(plan.sortedNodeIds).toEqual([
            'src_kafka',
            'step_parse_payload',
            'snk_delta',
            'snk_cloudsql',
        ]);
        expect(plan.streamingNodeIds).toEqual([
            'src_kafka',
            'step_parse_payload',
            'snk_delta',
            'snk_cloudsql',
        ]);

        // Streaming fan-out caches micro-batch DataFrames inside foreachBatch rather than batch lineage.
        expect(plan.cacheDirectives).toEqual([]);
        expect(plan.cachedNodeIds).toEqual([]);

        expect(plan.streamingSinkGroups).toHaveLength(1);
        const [group] = plan.streamingSinkGroups;
        expect(group.id).toBe('foreach_batch_1');
        expect(group.mode).toBe('foreachBatch');
        expect(group.sourceSubqueryNodeId).toBe('step_parse_payload');
        expect(group.sinkIds).toEqual(['snk_delta', 'snk_cloudsql']);
        expect(group.requiresBatchCache).toBe(true);
        expect(group.idempotentTxnOptions).toBe(true);
        expect(group.txnAppId).toBe('streaming-multi-sink-foreachbatch_step_parse_payload');
        expect(group.settings.outputMode).toBe('append');
        expect(group.settings.triggerType).toBe('processingTime');
        expect(group.settings.checkpointLocation).toBeTruthy();

        expect(plan.foreachBatchSinkIds).toEqual(['snk_delta', 'snk_cloudsql']);
        expect(plan.nodes.snk_delta.isForeachBatchSink).toBe(true);
        expect(plan.nodes.snk_cloudsql.isForeachBatchSink).toBe(true);

        // Verify failure condition when checkpoints are stripped and requireCheckpoints is enforced.
        const strippedNodes = doc.nodes.map((node) =>
            node.data?.type === 'SINK'
                ? {
                      ...node,
                      data: {
                          ...node.data,
                          properties: { ...node.data.properties, checkpointLocation: '' },
                      },
                  }
                : node
        );
        const missingCkptValidation = validateDagTopology(strippedNodes, doc.edges, {
            jobConfig: { ...doc.jobConfig, checkpointRoot: '' },
            requireCheckpoints: true,
        });
        expect(missingCkptValidation.isValid).toBe(false);
        expect(missingCkptValidation.errors.length).toBe(2);
    });

    it('selects streaming, Kafka, ParsePayload, Delta, and Cloud SQL skills with foreachBatch and txnVersion guidance', () => {
        const { rawYaml } = readMultiSinkPipeline();
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
                'io-kafka',
                'transform-parse-payload',
                'io-delta',
                'io-cloudsql',
            ])
        );

        expect(built.prompt).toContain('foreachBatch');
        expect(built.prompt).toContain('batch_df.cache()');
        expect(built.prompt).toContain('batch_df.unpersist()');
        expect(built.prompt).toContain('txnVersion');
        expect(built.prompt).toContain('txnAppId');
    });
});
