/**
 * @fileoverview Unit tests for the bundled Kafka Streaming to BigLake Iceberg sample (I2).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies sample discovery, companion documentation structure, topological and checkpoint
 * validation, ExecutionPlan DLQ and streaming sink grouping, Managed Spark classpath resolution,
 * and codegen skill selection for kafka-streaming-to-iceberg.spark.
 */

import { describe, it, expect, vi } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import YAML from 'yaml';
import type * as vscode from 'vscode';
import { getSamplePipelines } from '../../samples/samplePipelineService';
import { validateDagTopology, type DagEdge, type DagNode } from '../../shared/dagValidator';
import { buildExecutionPlan } from '../../shared/executionPlanner';
import { buildPromptFromPipelineYaml } from '../../codegen/CodegenPromptBuilder';
import { clearCodegenSkillCatalogCache } from '../../codegen/CodegenSkillCatalog';

vi.mock('vscode', async () => (await import('./helpers/vscodeMock')).createVscodeMock());

const REPO_ROOT = path.resolve(__dirname, '../../..');
const SAMPLE_SPARK_PATH = path.join(REPO_ROOT, 'resources', 'samples', 'kafka-streaming-to-iceberg.spark');
const SAMPLE_MD_PATH = path.join(REPO_ROOT, 'resources', 'samples', 'kafka-streaming-to-iceberg.md');
const SKILLS_ROOT = path.join(REPO_ROOT, 'resources', 'codegen-skills');

/**
 * Parsed representation of a bundled Spark sample document.
 */
interface ParsedSampleDocument {
    pipelineConfig?: Record<string, unknown>;
    jobConfig?: Record<string, unknown>;
    runtimeVariables?: Array<{ key: string; value: string }>;
    nodes: DagNode[];
    edges: DagEdge[];
}

/**
 * Reads and parses the bundled Kafka to BigLake Iceberg YAML specification from disk.
 *
 * @returns Parsed sample pipeline document and raw YAML string.
 */
function loadKafkaIcebergSample(): { rawYaml: string; doc: ParsedSampleDocument } {
    const rawYaml = fs.readFileSync(SAMPLE_SPARK_PATH, 'utf-8');
    const doc = YAML.parse(rawYaml) as ParsedSampleDocument;
    return { rawYaml, doc };
}

describe('Bundled Sample I2: kafka-streaming-to-iceberg', () => {
    it('discovers the sample via getSamplePipelines and loads its companion ASD-STE100 README', async () => {
        const samples = await getSamplePipelines({
            extensionPath: REPO_ROOT,
        } as unknown as vscode.ExtensionContext);

        const sample = samples.find((entry) => entry.id === 'kafka-streaming-to-iceberg');
        expect(sample).toBeDefined();
        expect(sample).toMatchObject({
            id: 'kafka-streaming-to-iceberg',
            filename: 'kafka-streaming-to-iceberg.spark',
            title: 'Kafka Streaming To Iceberg',
            language: 'PySpark',
            runner: 'ServerlessSpark',
            nodesCount: 6,
            description:
                'Pipeline flow: Kafka Event Stream ➔ Parse Order JSON ➔ [Event-Time Watermark, Corrupt Payload DLQ] ➔ Tumbling Window Metrics ➔ BigLake Iceberg Sink',
        });

        const diskReadme = fs.readFileSync(SAMPLE_MD_PATH, 'utf-8');
        expect(sample?.readmeContent).toBe(diskReadme);
        expect(diskReadme).toContain('# Streaming Kafka to BigLake Iceberg Pipeline');
        expect(diskReadme).toContain('## Overview');
        expect(diskReadme).toContain('## Pipeline Architecture & Data Flow');
        expect(diskReadme).toContain('## Runtime Parameters');
        expect(diskReadme).toContain('## Code Generation & Execution');
        expect(diskReadme).not.toContain('## Pipeline Nodes & Transforms');
        expect(diskReadme).not.toContain(';');
    });

    it('passes DAG topology, watermark, and strict checkpoint validation with zero errors or warnings', () => {
        const { doc } = loadKafkaIcebergSample();

        const validation = validateDagTopology(doc.nodes, doc.edges, {
            jobConfig: doc.jobConfig,
            requireCheckpoints: true,
        });

        expect(validation.isValid).toBe(true);
        expect(validation.hasCycles).toBe(false);
        expect(validation.orphanNodeIds).toEqual([]);
        expect(validation.errors).toEqual([]);
        expect(validation.warnings).toEqual([]);
    });

    it('emits expected validation diagnostics when watermark or checkpoints are removed (boundary and failure)', () => {
        const { doc } = loadKafkaIcebergSample();

        // Boundary case: bypass Watermark so ParsePayload feeds GroupByAgg directly.
        const nodesWithoutWatermark = doc.nodes.filter((n) => n.id !== 'step_watermark');
        const edgesWithoutWatermark = doc.edges
            .filter((e) => e.source !== 'step_watermark' && e.target !== 'step_watermark')
            .concat({
                id: 'e_direct_unwatermarked',
                source: 'step_parse_payload',
                sourceHandle: 'valid',
                target: 'step_window_agg',
            });

        const unwatermarkedResult = validateDagTopology(nodesWithoutWatermark, edgesWithoutWatermark, {
            jobConfig: doc.jobConfig,
            requireCheckpoints: true,
        });
        expect(unwatermarkedResult.isValid).toBe(true);
        expect(unwatermarkedResult.warnings.some((w) => w.includes('Watermark'))).toBe(true);

        // Failure case: strip both sink checkpointLocation and jobConfig.checkpointRoot.
        const nodesWithoutCheckpoints = doc.nodes.map((n) =>
            n.data?.type === 'SINK'
                ? {
                      ...n,
                      data: {
                          ...n.data,
                          properties: {
                              ...(n.data.properties as Record<string, unknown>),
                              checkpointLocation: '',
                          },
                      },
                  }
                : n
        );
        const missingCheckpointResult = validateDagTopology(nodesWithoutCheckpoints, doc.edges, {
            jobConfig: { ...doc.jobConfig, checkpointRoot: '' },
            requireCheckpoints: true,
        });
        expect(missingCheckpointResult.isValid).toBe(false);
        expect(missingCheckpointResult.errors.length).toBeGreaterThanOrEqual(2);
    });

    it('builds a streaming ExecutionPlan with _corrupt_record DLQ routing, native sink groups, and Managed Spark artifacts', () => {
        const { doc } = loadKafkaIcebergSample();
        const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);

        expect(plan.pipelineMode).toBe('streaming');
        expect(plan.streamingNodeIds).toEqual(
            expect.arrayContaining([
                'src_kafka',
                'step_parse_payload',
                'step_watermark',
                'step_window_agg',
                'sink_dlq_console',
                'sink_iceberg',
            ])
        );
        expect(plan.cachedNodeIds).toEqual([]);

        expect(plan.dlqSplitPoints).toEqual([
            {
                nodeId: 'step_parse_payload',
                sparkTransform: 'ParsePayload',
                mechanism: '_corrupt_record',
                validTargetIds: ['step_watermark'],
                errorTargetIds: ['sink_dlq_console'],
            },
        ]);

        expect(plan.streamingSinkGroups).toHaveLength(2);
        const dlqGroup = plan.streamingSinkGroups.find((g) => g.sinkIds.includes('sink_dlq_console'));
        const icebergGroup = plan.streamingSinkGroups.find((g) => g.sinkIds.includes('sink_iceberg'));

        expect(dlqGroup).toMatchObject({
            mode: 'native',
            sourceSubqueryNodeId: 'step_parse_payload',
            sinkIds: ['sink_dlq_console'],
            settings: {
                outputMode: 'append',
                triggerType: 'processingTime',
                triggerInterval: '30 seconds',
                checkpointLocation:
                    'gs://my-lakehouse-bucket/checkpoints/kafka-to-iceberg/sink_dlq_console',
            },
        });

        expect(icebergGroup).toMatchObject({
            mode: 'native',
            sourceSubqueryNodeId: 'step_window_agg',
            sinkIds: ['sink_iceberg'],
            settings: {
                outputMode: 'append',
                triggerType: 'processingTime',
                triggerInterval: '30 seconds',
                checkpointLocation:
                    'gs://my-lakehouse-bucket/checkpoints/kafka-to-iceberg/sink_iceberg',
            },
        });

        expect(plan.runtimeArtifacts.profile.id).toBe('serverless_4.0');
        expect(plan.runtimeArtifacts.tier).toBe('standard');
        expect(plan.runtimeArtifacts.mavenPackages).toContain(
            'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0'
        );
        expect(
            plan.runtimeArtifacts.mavenPackages.some((pkg) => pkg.includes('iceberg-spark-runtime'))
        ).toBe(false);
        expect(plan.runtimeArtifacts.preinstalledConnectors).toContain('iceberg-spark-runtime');
    });

    it('selects core-streaming and every expected IO and transform skill without credential redaction', () => {
        const { rawYaml } = loadKafkaIcebergSample();
        clearCodegenSkillCatalogCache();

        const built = buildPromptFromPipelineYaml({
            skillsRoot: SKILLS_ROOT,
            yamlText: rawYaml,
            language: 'PySpark',
            sparkVersion: '4.2.0',
        });

        expect(built.redactedPaths).toEqual([]);
        expect(built.selectedSkills).toEqual(
            expect.arrayContaining([
                'codegen-common',
                'core-pyspark',
                'core-streaming',
                'core-catalyst-optimization',
                'io-kafka',
                'io-console',
                'io-iceberg',
                'transform-parse-payload',
                'transform-watermark',
                'transform-groupby-agg',
                'runner-serverless-spark',
                'verification-gate',
            ])
        );
        expect(built.prompt).toContain('## Execution Plan');
        expect(built.prompt).not.toContain('## Redacted Credentials');
    });
});
