/**
 * @fileoverview Unit tests for the bundled Local Rate Stream to Delta Lake sample (I4).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies sample discovery, companion ASD-STE100 documentation, local checkpoint and
 * watermark topology validation, ExecutionPlan native streaming sink configuration,
 * Local runner Delta Lake Maven artifact resolution, and codegen skill selection for
 * rate-stream-to-delta.spark.
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
const SAMPLE_SPARK_PATH = path.join(REPO_ROOT, 'resources', 'samples', 'rate-stream-to-delta.spark');
const SAMPLE_MD_PATH = path.join(REPO_ROOT, 'resources', 'samples', 'rate-stream-to-delta.md');
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
 * Reads and parses the bundled Rate Stream to Delta Lake YAML specification from disk.
 *
 * @returns Parsed sample pipeline document and raw YAML string.
 */
function loadRateDeltaSample(): { rawYaml: string; doc: ParsedSampleDocument } {
    const rawYaml = fs.readFileSync(SAMPLE_SPARK_PATH, 'utf-8');
    const doc = YAML.parse(rawYaml) as ParsedSampleDocument;
    return { rawYaml, doc };
}

describe('Bundled Sample I4: rate-stream-to-delta', () => {
    it('discovers the sample via getSamplePipelines and loads its companion ASD-STE100 README', async () => {
        const samples = await getSamplePipelines({
            extensionPath: REPO_ROOT,
        } as unknown as vscode.ExtensionContext);

        const sample = samples.find((entry) => entry.id === 'rate-stream-to-delta');
        expect(sample).toBeDefined();
        expect(sample).toMatchObject({
            id: 'rate-stream-to-delta',
            filename: 'rate-stream-to-delta.spark',
            title: 'Rate Stream To Delta',
            language: 'PySpark',
            runner: 'Local',
            nodesCount: 5,
            description:
                'Pipeline flow: Synthetic Rate Stream ➔ Derive Event Columns ➔ Event-Time Watermark ➔ Watermark Key Deduplicate ➔ Local Delta Lake Sink',
        });

        const diskReadme = fs.readFileSync(SAMPLE_MD_PATH, 'utf-8');
        expect(sample?.readmeContent).toBe(diskReadme);
        expect(diskReadme).toContain('# Local Streaming Rate to Delta Lake Pipeline');
        expect(diskReadme).toContain('## Overview');
        expect(diskReadme).toContain('## Pipeline Architecture & Data Flow');
        expect(diskReadme).toContain('## Runtime Parameters');
        expect(diskReadme).toContain('## Code Generation & Execution');
        expect(diskReadme).not.toContain('## Pipeline Nodes & Transforms');
        expect(diskReadme).not.toContain(';');
    });

    it('passes DAG topology, watermark, and local checkpoint validation with zero errors or warnings', () => {
        const { doc } = loadRateDeltaSample();

        expect(doc.jobConfig?.runner).toBe('Local');

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

    it('emits expected watermark warning and invalid checkpoint scheme error on mutated copies (boundary and failure)', () => {
        const { doc } = loadRateDeltaSample();

        // Boundary case: bypass Watermark before Deduplicate.
        const nodesWithoutWatermark = doc.nodes.filter((n) => n.id !== 'step_watermark');
        const edgesWithoutWatermark = doc.edges
            .filter((e) => e.source !== 'step_watermark' && e.target !== 'step_watermark')
            .concat({
                id: 'e_enrich_to_dedup_direct',
                source: 'step_enrich_columns',
                sourceHandle: 'valid',
                target: 'step_deduplicate',
            });

        const missingWatermark = validateDagTopology(nodesWithoutWatermark, edgesWithoutWatermark, {
            jobConfig: doc.jobConfig,
            requireCheckpoints: true,
        });
        expect(missingWatermark.isValid).toBe(true);
        expect(
            missingWatermark.warnings.some((w) => w.includes('dropDuplicatesWithinWatermark'))
        ).toBe(true);

        // Failure case: unsupported checkpoint URI scheme on DeltaSink.
        const badCheckpointNodes = doc.nodes.map((n) =>
            n.id === 'sink_delta'
                ? {
                      ...n,
                      data: {
                          ...n.data,
                          properties: {
                              ...(n.data?.properties as Record<string, unknown>),
                              checkpointLocation: 'http://localhost/checkpoints',
                          },
                      },
                  }
                : n
        );
        const badCheckpointResult = validateDagTopology(badCheckpointNodes, doc.edges, {
            jobConfig: doc.jobConfig,
            requireCheckpoints: true,
        });
        expect(badCheckpointResult.isValid).toBe(false);
        expect(badCheckpointResult.errors.some((e) => e.includes('Unsupported checkpoint URI'))).toBe(
            true
        );
    });

    it('builds a Local streaming ExecutionPlan with native DeltaSink query and Delta Maven coordinate', () => {
        const { doc } = loadRateDeltaSample();
        const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);

        expect(plan.pipelineMode).toBe('streaming');
        expect(plan.sortedNodeIds).toEqual([
            'src_rate',
            'step_enrich_columns',
            'step_watermark',
            'step_deduplicate',
            'sink_delta',
        ]);
        expect(plan.streamingNodeIds).toEqual([
            'src_rate',
            'step_enrich_columns',
            'step_watermark',
            'step_deduplicate',
            'sink_delta',
        ]);
        expect(plan.dlqSplitPoints).toEqual([]);
        expect(plan.cachedNodeIds).toEqual([]);

        expect(plan.streamingSinkGroups).toHaveLength(1);
        expect(plan.streamingSinkGroups[0]).toMatchObject({
            mode: 'native',
            sourceSubqueryNodeId: 'step_deduplicate',
            sinkIds: ['sink_delta'],
            settings: {
                outputMode: 'append',
                triggerType: 'processingTime',
                triggerInterval: '5 seconds',
                checkpointLocation:
                    'file:///tmp/spark-pipeline-builder/rate-stream-to-delta/checkpoints/sink_delta',
            },
        });

        expect(plan.runtimeArtifacts.profile.id).toBe('local');
        expect(plan.runtimeArtifacts.profile.runner).toBe('Local');
        expect(plan.runtimeArtifacts.mavenPackages).toEqual(['io.delta:delta-spark_2.13:4.0.0']);
        expect(plan.runtimeArtifacts.gcsJars).toEqual([]);
        expect(plan.runtimeArtifacts.preinstalledConnectors).toEqual([]);
    });

    it('selects core-streaming and every expected IO and transform skill without credential redaction', () => {
        const { rawYaml } = loadRateDeltaSample();
        clearCodegenSkillCatalogCache();

        const built = buildPromptFromPipelineYaml({
            skillsRoot: SKILLS_ROOT,
            yamlText: rawYaml,
            language: 'PySpark',
            sparkVersion: '4.0.1',
        });

        expect(built.redactedPaths).toEqual([]);
        expect(built.selectedSkills).toEqual(
            expect.arrayContaining([
                'codegen-common',
                'core-pyspark',
                'core-streaming',
                'core-catalyst-optimization',
                'io-rate',
                'io-delta',
                'transform-with-columns',
                'transform-watermark',
                'transform-deduplicate',
                'runner-local',
                'verification-gate',
            ])
        );
        expect(built.prompt).toContain('## Execution Plan');
        expect(built.prompt).not.toContain('## Redacted Credentials');
    });
});
