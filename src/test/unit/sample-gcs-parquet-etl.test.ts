/**
 * @fileoverview Unit tests for the bundled Batch GCS Parquet ETL starter sample (Task I1).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies sample pipeline discovery, companion Markdown README loading, DAG topology
 * validation, deterministic batch execution planning, and modular PySpark skill selection
 * for `resources/samples/gcs-parquet-etl.spark`.
 */

import * as fs from 'fs';
import * as path from 'path';
import type * as vscode from 'vscode';
import { describe, expect, it, vi } from 'vitest';
import YAML from 'yaml';
import { clearCodegenSkillCatalogCache } from '../../codegen/CodegenSkillCatalog';
import { buildPromptFromPipelineYaml } from '../../codegen/CodegenPromptBuilder';
import { getSamplePipelines } from '../../samples/samplePipelineService';
import { validateDagTopology, type DagEdge, type DagNode } from '../../shared/dagValidator';
import { buildExecutionPlan } from '../../shared/executionPlanner';

vi.mock('vscode', async () => (await import('./helpers/vscodeMock')).createVscodeMock());

interface ParsedSampleDocument {
    pipelineConfig?: Record<string, unknown>;
    jobConfig?: Record<string, unknown>;
    runtimeVariables?: Array<{ key: string; value: string }>;
    nodes: DagNode[];
    edges: DagEdge[];
}

const REPO_ROOT = path.resolve(__dirname, '../../..');
const SAMPLE_SPARK_PATH = path.join(REPO_ROOT, 'resources', 'samples', 'gcs-parquet-etl.spark');
const SAMPLE_README_PATH = path.join(REPO_ROOT, 'resources', 'samples', 'gcs-parquet-etl.md');
const SKILLS_ROOT = path.join(REPO_ROOT, 'resources', 'codegen-skills');

/**
 * Loads the raw YAML text and parsed document for `gcs-parquet-etl.spark`.
 *
 * @returns Raw YAML text and parsed pipeline document.
 */
function loadGcsParquetSample(): { yamlText: string; doc: ParsedSampleDocument } {
    const yamlText = fs.readFileSync(SAMPLE_SPARK_PATH, 'utf-8');
    const doc = YAML.parse(yamlText) as ParsedSampleDocument;
    return { yamlText, doc };
}

describe('Bundled Sample I1 — gcs-parquet-etl.spark', () => {
    it('discovers gcs-parquet-etl.spark and loads its companion Markdown README via getSamplePipelines', async () => {
        const expectedReadme = fs.readFileSync(SAMPLE_README_PATH, 'utf-8');
        const samples = await getSamplePipelines({
            extensionPath: REPO_ROOT,
        } as unknown as vscode.ExtensionContext);

        const sample = samples.find((entry) => entry.filename === 'gcs-parquet-etl.spark');
        expect(sample).toBeDefined();
        expect(sample).toMatchObject({
            id: 'gcs-parquet-etl',
            filename: 'gcs-parquet-etl.spark',
            title: 'Gcs Parquet Etl',
            language: 'PySpark',
            runner: 'ServerlessSpark',
            nodesCount: 6,
            description:
                'Pipeline flow: Raw Orders Parquet ➔ Filter Completed Orders ➔ Derive Revenue Columns ➔ Aggregate Daily Revenue ➔ Coalesce Partitions ➔ Curated Revenue Parquet',
        });
        expect(sample?.readmeContent).toBe(expectedReadme);
        expect(sample?.readmeContent).toContain('# Batch GCS Parquet ETL Pipeline');
        expect(sample?.readmeContent).toContain('## Overview');
        expect(sample?.readmeContent).toContain('## Pipeline Architecture & Data Flow');
        expect(sample?.readmeContent).toContain('## Runtime Parameters');
        expect(sample?.readmeContent).toContain('## Code Generation & Execution');
        expect(sample?.readmeContent).not.toContain('## Pipeline Nodes & Transforms');
        expect(sample?.readmeContent).not.toContain(';');
    });

    it('passes validateDagTopology with zero errors, zero warnings, and clean left-to-right node layout', () => {
        const { doc } = loadGcsParquetSample();

        expect(doc.pipelineConfig).toMatchObject({ language: 'PySpark' });
        expect(doc.jobConfig).toMatchObject({
            runner: 'ServerlessSpark',
            runtimeVersion: '4.0',
            tier: 'standard',
        });
        expect(Array.isArray(doc.runtimeVariables)).toBe(true);
        expect((doc.runtimeVariables ?? []).length).toBeGreaterThanOrEqual(4);

        const validation = validateDagTopology(doc.nodes, doc.edges, {
            jobConfig: doc.jobConfig,
        });
        expect(validation.isValid).toBe(true);
        expect(validation.hasCycles).toBe(false);
        expect(validation.orphanNodeIds).toEqual([]);
        expect(validation.errors).toEqual([]);
        expect(validation.warnings).toEqual([]);
        expect(validation.sortedNodeIds).toEqual([
            'src_raw_orders',
            'step_filter_completed',
            'step_derive_columns',
            'step_daily_region_agg',
            'step_coalesce_files',
            'snk_curated_parquet',
        ]);

        // Verify left-to-right canvas X coordinates and expected node transforms
        const xPositions = doc.nodes.map((node) => Number(node.position?.x ?? 0));
        for (let idx = 1; idx < xPositions.length; idx += 1) {
            expect(xPositions[idx]).toBeGreaterThan(xPositions[idx - 1]);
        }

        const transforms = doc.nodes.map((node) => node.data?.spark_transform);
        expect(transforms).toEqual([
            'ParquetSource',
            'Filter',
            'WithColumns',
            'GroupByAgg',
            'RepartitionCoalesce',
            'ParquetSink',
        ]);

        const sourceProps = doc.nodes[0]?.data?.properties ?? {};
        expect(sourceProps.readMode).toBe('batch');
        expect(sourceProps.storageBackend).toBe('gcs');
        expect(String(sourceProps.path)).toMatch(/^gs:\/\//);

        const groupByProps = doc.nodes[3]?.data?.properties ?? {};
        expect(groupByProps.windowType).toBe('None');
        expect(groupByProps.groupByColumns).toEqual(['event_date', 'normalized_region']);

        const coalesceProps = doc.nodes[4]?.data?.properties ?? {};
        expect(coalesceProps.operation).toBe('coalesce');
        expect(coalesceProps.numPartitions).toBe(4);

        const sinkProps = doc.nodes[5]?.data?.properties ?? {};
        expect(sinkProps.storageBackend).toBe('gcs');
        expect(String(sinkProps.path)).toMatch(/^gs:\/\//);
        expect(sinkProps.saveMode).toBe('overwrite');
        expect(String(sinkProps.partitionBy)).toContain('event_date');
    });

    it('rejects disconnected orphan mutations on the sample graph (failure boundary)', () => {
        const { doc } = loadGcsParquetSample();
        const brokenEdges = doc.edges.filter(
            (edge) => edge.source !== 'step_coalesce_files' && edge.target !== 'step_coalesce_files'
        );

        const validation = validateDagTopology(doc.nodes, brokenEdges, {
            jobConfig: doc.jobConfig,
        });
        expect(validation.isValid).toBe(false);
        expect(validation.orphanNodeIds).toContain('step_coalesce_files');
    });

    it('produces deterministic batch flags, topological order, and Serverless 4.0 GCS artifacts in buildExecutionPlan', () => {
        const { doc } = loadGcsParquetSample();
        const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);

        expect(plan.pipelineMode).toBe('batch');
        expect(plan.streamingNodeIds).toEqual([]);
        expect(plan.cachedNodeIds).toEqual([]);
        expect(plan.cacheDirectives).toEqual([]);
        expect(plan.dlqSplitPoints).toEqual([]);
        expect(plan.streamingSinkGroups).toEqual([]);
        expect(plan.foreachBatchSinkIds).toEqual([]);
        expect(plan.sortedNodeIds).toEqual([
            'src_raw_orders',
            'step_filter_completed',
            'step_derive_columns',
            'step_daily_region_agg',
            'step_coalesce_files',
            'snk_curated_parquet',
        ]);

        for (const nodeId of plan.sortedNodeIds) {
            expect(plan.nodes[nodeId]?.executionMode).toBe('batch');
            expect(plan.nodes[nodeId]?.isCached).toBe(false);
            expect(plan.nodes[nodeId]?.isForeachBatchSink).toBe(false);
        }

        expect(plan.runtimeArtifacts.profile.id).toBe('serverless_4.0');
        expect(plan.runtimeArtifacts.profile.sparkVersion).toBe('4.2.0');
        expect(plan.runtimeArtifacts.tier).toBe('standard');
        expect(plan.runtimeArtifacts.preinstalledConnectors).toContain('gcs-connector');
        expect(plan.runtimeArtifacts.mavenPackages).toEqual([]);
        expect(plan.runtimeArtifacts.gcsJars).toEqual([]);
    });

    it('selects all expected io and transform skills cleanly via buildPromptFromPipelineYaml', () => {
        const { yamlText } = loadGcsParquetSample();
        clearCodegenSkillCatalogCache();

        const built = buildPromptFromPipelineYaml({
            skillsRoot: SKILLS_ROOT,
            yamlText,
            language: 'PySpark',
        });

        expect(built.redactedPaths).toEqual([]);
        expect(built.detection.warnings).toEqual([]);
        expect(built.selectedSkills).toEqual([
            'codegen-common',
            'core-pyspark',
            'runner-dataproc-cluster',
            'runner-local',
            'runner-serverless-spark',
            'io-parquet',
            'transform-filter',
            'transform-groupby-agg',
            'transform-repartition-coalesce',
            'transform-with-columns',
            'core-catalyst-optimization',
            'verification-gate',
        ]);
        expect(built.selectedSkills).not.toContain('core-streaming');
        expect(built.selectedSkills).not.toContain('core-python-datasource');
        expect(built.prompt).toContain('## Execution Plan');
    });
});
