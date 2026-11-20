/**
 * @fileoverview Unit tests for the bundled Generate Mock Data sample.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies sample discovery, companion ASD-STE100 documentation, local checkpoint and
 * topology validation, credential redaction cleanliness, ExecutionPlan native streaming
 * console configuration with zero external JARs, and codegen skill selection for
 * generate-mock-data.spark.
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
import { redactPipelineDocument } from '../../credentialRedaction';

vi.mock('vscode', async () => (await import('./helpers/vscodeMock')).createVscodeMock());

const REPO_ROOT = path.resolve(__dirname, '../../..');
const SAMPLE_SPARK_PATH = path.join(REPO_ROOT, 'resources', 'samples', 'generate-mock-data.spark');
const SAMPLE_MD_PATH = path.join(REPO_ROOT, 'resources', 'samples', 'generate-mock-data.md');
const SKILLS_ROOT = path.join(REPO_ROOT, 'resources', 'codegen-skills');

/**
 * Parsed representation of a bundled Spark sample document.
 */
interface ParsedSampleDocument {
    paradigm?: 'imperative' | 'sdp';
    pipelineConfig?: Record<string, unknown>;
    jobConfig?: Record<string, unknown>;
    runtimeVariables?: Array<{ key: string; value: string }>;
    nodes: DagNode[];
    edges: DagEdge[];
}

/**
 * Reads and parses the bundled Generate Mock Data YAML specification from disk.
 *
 * @returns Parsed sample pipeline document and raw YAML string.
 */
function loadGenerateMockDataSample(): { rawYaml: string; doc: ParsedSampleDocument } {
    const rawYaml = fs.readFileSync(SAMPLE_SPARK_PATH, 'utf-8');
    const doc = YAML.parse(rawYaml) as ParsedSampleDocument;
    return { rawYaml, doc };
}

describe('Bundled Sample: generate-mock-data', () => {
    it('discovers the sample via getSamplePipelines and loads its companion ASD-STE100 README', async () => {
        const samples = await getSamplePipelines({
            extensionPath: REPO_ROOT,
        } as unknown as vscode.ExtensionContext);

        const sample = samples.find((entry) => entry.id === 'generate-mock-data');
        expect(sample).toBeDefined();
        expect(sample).toMatchObject({
            id: 'generate-mock-data',
            filename: 'generate-mock-data.spark',
            title: 'Generate Mock Data',
            language: 'PySpark',
            runner: 'Local',
            nodesCount: 3,
            description:
                'Pipeline flow: Synthetic Rate Stream ➔ Generate Faker Customers ➔ Console Log Sink',
        });

        const diskReadme = fs.readFileSync(SAMPLE_MD_PATH, 'utf-8');
        expect(sample?.readmeContent).toBe(diskReadme);
        expect(diskReadme).toContain('# Generate Mock Data Pipeline');
        expect(diskReadme).toContain('## Overview');
        expect(diskReadme).toContain('## Pipeline Architecture & Data Flow');
        expect(diskReadme).toContain('## Runtime Parameters');
        expect(diskReadme).toContain('## Code Generation & Execution');
        expect(diskReadme).not.toContain('## Pipeline Nodes & Transforms');
        expect(diskReadme).not.toContain(';');
        expect(diskReadme).not.toContain('.beam');
    });

    it('passes DAG topology and local checkpoint validation with zero errors or warnings', () => {
        const { doc } = loadGenerateMockDataSample();

        expect(doc.paradigm).toBe('imperative');
        expect(doc.jobConfig?.runner).toBe('Local');
        expect(doc.jobConfig?.defaultTriggerType).toBe('processingTime');
        expect(doc.jobConfig?.defaultTriggerInterval).toBe('1 second');

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

    it('fails validation on mutated copies with missing or unsupported checkpoint URIs (boundary and failure)', () => {
        const { doc } = loadGenerateMockDataSample();

        // Failure case: unsupported checkpoint URI scheme on ConsoleSink.
        const badCheckpointNodes = doc.nodes.map((n) =>
            n.id === 'sink_console'
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

        // Boundary case: missing checkpointLocation and missing checkpointRoot.
        const noCheckpointNodes = doc.nodes.map((n) => {
            if (n.id !== 'sink_console') return n;
            const props = { ...(n.data?.properties as Record<string, unknown>) };
            delete props.checkpointLocation;
            return { ...n, data: { ...n.data, properties: props } };
        });
        const jobConfigWithoutRoot = { ...(doc.jobConfig || {}) };
        delete jobConfigWithoutRoot.checkpointRoot;
        const missingCheckpointResult = validateDagTopology(noCheckpointNodes, doc.edges, {
            jobConfig: jobConfigWithoutRoot,
            requireCheckpoints: true,
        });
        expect(missingCheckpointResult.isValid).toBe(false);
    });

    it('builds a Local streaming ExecutionPlan with native ConsoleSink query and zero external JARs', () => {
        const { doc } = loadGenerateMockDataSample();
        const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);

        expect(plan.pipelineMode).toBe('streaming');
        expect(plan.sortedNodeIds).toEqual([
            'src_rate',
            'step_generate_customers',
            'sink_console',
        ]);
        expect(plan.streamingNodeIds).toEqual([
            'src_rate',
            'step_generate_customers',
            'sink_console',
        ]);
        expect(plan.dlqSplitPoints).toEqual([]);
        expect(plan.cachedNodeIds).toEqual([]);

        expect(plan.streamingSinkGroups).toHaveLength(1);
        expect(plan.streamingSinkGroups[0]).toMatchObject({
            mode: 'native',
            sourceSubqueryNodeId: 'step_generate_customers',
            sinkIds: ['sink_console'],
            settings: {
                outputMode: 'append',
                triggerType: 'processingTime',
                triggerInterval: '1 second',
                checkpointLocation:
                    'file:///tmp/spark-pipeline-builder/generate-mock-data/checkpoints/sink_console',
            },
        });

        expect(plan.runtimeArtifacts.profile.id).toBe('local');
        expect(plan.runtimeArtifacts.profile.runner).toBe('Local');
        expect(plan.runtimeArtifacts.mavenPackages).toEqual([]);
        expect(plan.runtimeArtifacts.gcsJars).toEqual([]);
        expect(plan.runtimeArtifacts.preinstalledConnectors).toEqual([]);
    });

    it('passes redactPipelineDocument cleanly and selects expected skills for code generation', () => {
        const { rawYaml } = loadGenerateMockDataSample();
        const redacted = redactPipelineDocument(rawYaml);
        expect(redacted.redactedPaths).toEqual([]);

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
                'io-console',
                'transform-pandas-udf',
                'runner-local',
                'verification-gate',
            ])
        );
        expect(built.prompt).toContain('latest faker library');
        expect(built.prompt).toContain('log level to WARN');
        expect(built.prompt).toContain('## Execution Plan');
        expect(built.prompt).not.toContain('## Redacted Credentials');
    });
});
