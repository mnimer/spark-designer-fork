/**
 * @fileoverview Unit tests for the BigQuery and Cloud SQL Broadcast Join starter sample (Task I3).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies sample pipeline discovery, companion Markdown README loading, credential redaction
 * cleanliness, Join handle topology validation, batch execution planning (`batch_batch`), and
 * PySpark skill selection for `resources/samples/bigquery-cloudsql-broadcast-join.spark`.
 */

import * as fs from 'fs';
import * as path from 'path';
import type * as vscode from 'vscode';
import { describe, expect, it, vi } from 'vitest';
import YAML from 'yaml';
import { clearCodegenSkillCatalogCache } from '../../codegen/CodegenSkillCatalog';
import { buildPromptFromPipelineYaml } from '../../codegen/CodegenPromptBuilder';
import { redactPipelineYaml } from '../../credentialRedaction';
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
const SAMPLE_SPARK_PATH = path.join(
    REPO_ROOT,
    'resources',
    'samples',
    'bigquery-cloudsql-broadcast-join.spark'
);
const SAMPLE_README_PATH = path.join(
    REPO_ROOT,
    'resources',
    'samples',
    'bigquery-cloudsql-broadcast-join.md'
);
const SKILLS_ROOT = path.join(REPO_ROOT, 'resources', 'codegen-skills');

/**
 * Loads the raw YAML text and parsed document for `bigquery-cloudsql-broadcast-join.spark`.
 *
 * @returns Raw YAML text and parsed pipeline document.
 */
function loadBqCloudSqlSample(): { yamlText: string; doc: ParsedSampleDocument } {
    const yamlText = fs.readFileSync(SAMPLE_SPARK_PATH, 'utf-8');
    const doc = YAML.parse(yamlText) as ParsedSampleDocument;
    return { yamlText, doc };
}

describe('Bundled Sample I3 — bigquery-cloudsql-broadcast-join.spark', () => {
    it('discovers bigquery-cloudsql-broadcast-join.spark and loads its companion Markdown README', async () => {
        const expectedReadme = fs.readFileSync(SAMPLE_README_PATH, 'utf-8');
        const samples = await getSamplePipelines({
            extensionPath: REPO_ROOT,
        } as unknown as vscode.ExtensionContext);

        const sample = samples.find(
            (entry) => entry.filename === 'bigquery-cloudsql-broadcast-join.spark'
        );
        expect(sample).toBeDefined();
        expect(sample).toMatchObject({
            id: 'bigquery-cloudsql-broadcast-join',
            filename: 'bigquery-cloudsql-broadcast-join.spark',
            title: 'Bigquery Cloudsql Broadcast Join',
            language: 'PySpark',
            runner: 'ServerlessSpark',
            nodesCount: 5,
            description:
                'Pipeline flow: [BigQuery Transactions Fact, Cloud SQL Merchant Accounts] ➔ Enrich Merchant Tier ➔ Rank Merchant Transactions ➔ BigQuery Enriched Ledger',
        });
        expect(sample?.readmeContent).toBe(expectedReadme);
        expect(sample?.readmeContent).toContain('# BigQuery and Cloud SQL Broadcast Join Pipeline');
        expect(sample?.readmeContent).toContain('## Overview');
        expect(sample?.readmeContent).toContain('## Pipeline Architecture & Data Flow');
        expect(sample?.readmeContent).toContain('## Runtime Parameters');
        expect(sample?.readmeContent).toContain('## Code Generation & Execution');
        expect(sample?.readmeContent).not.toContain('## Pipeline Nodes & Transforms');
        expect(sample?.readmeContent).not.toContain(';');
    });

    it('passes credentialRedaction with zero redacted secrets and uses IAM authentication', () => {
        const { yamlText, doc } = loadBqCloudSqlSample();
        const redaction = redactPipelineYaml(yamlText);
        expect(redaction.redactedPaths).toEqual([]);
        expect(redaction.value).toBe(yamlText);

        const cloudSqlNode = doc.nodes.find((node) => node.id === 'src_cloudsql_accounts');
        const cloudSqlProps = cloudSqlNode?.data?.properties ?? {};
        expect(cloudSqlProps.dbEngine).toBe('postgresql');
        expect(cloudSqlProps.authMode).toBe('iam');
        expect(cloudSqlProps.dynamicBounds).toBe(true);
        expect(cloudSqlProps.partitionColumn).toBe('merchant_id');
        expect(cloudSqlProps.password).toBeUndefined();
    });

    it('passes validateDagTopology with explicit Join left and right handles, broadcastRight, WindowOver, and direct BigQuerySink', () => {
        const { doc } = loadBqCloudSqlSample();

        const validation = validateDagTopology(doc.nodes, doc.edges, {
            jobConfig: doc.jobConfig,
        });
        expect(validation.isValid).toBe(true);
        expect(validation.hasCycles).toBe(false);
        expect(validation.orphanNodeIds).toEqual([]);
        expect(validation.errors).toEqual([]);
        expect(validation.warnings).toEqual([]);

        const leftEdge = doc.edges.find(
            (edge) => edge.target === 'step_broadcast_join' && edge.targetHandle === 'left'
        );
        const rightEdge = doc.edges.find(
            (edge) => edge.target === 'step_broadcast_join' && edge.targetHandle === 'right'
        );
        expect(leftEdge?.source).toBe('src_bq_transactions');
        expect(rightEdge?.source).toBe('src_cloudsql_accounts');

        const joinNode = doc.nodes.find((node) => node.id === 'step_broadcast_join');
        expect(joinNode?.data?.properties).toMatchObject({
            joinType: 'left_outer',
            broadcastRight: true,
        });

        const windowNode = doc.nodes.find((node) => node.id === 'step_latest_tx_window');
        expect(windowNode?.data?.properties).toMatchObject({
            partitionByColumns: ['merchant_id'],
            windowMetrics: [{ fn: 'row_number', alias: 'merchant_tx_rank' }],
        });

        const sinkNode = doc.nodes.find((node) => node.id === 'snk_bq_enriched');
        expect(sinkNode?.data?.properties).toMatchObject({
            writeMethod: 'direct',
        });
    });

    it('rejects untyped Join edges when targetHandle is stripped (failure boundary)', () => {
        const { doc } = loadBqCloudSqlSample();
        const invalidEdges = doc.edges.map((edge) =>
            edge.id === 'e_sql_to_join_right' ? { ...edge, targetHandle: undefined } : edge
        );

        const validation = validateDagTopology(doc.nodes, invalidEdges, {
            jobConfig: doc.jobConfig,
        });
        expect(validation.isValid).toBe(false);
        expect(validation.errors.some((err) => err.includes('"left" and "right" handles'))).toBe(
            true
        );
    });

    it('produces batch_batch joinMode, topological order, and Cloud SQL + BigQuery runtime artifacts in buildExecutionPlan', () => {
        const { doc } = loadBqCloudSqlSample();
        const plan = buildExecutionPlan(doc.nodes, doc.edges, doc.jobConfig);

        expect(plan.pipelineMode).toBe('batch');
        expect(plan.streamingNodeIds).toEqual([]);
        expect(plan.cachedNodeIds).toEqual([]);
        expect(plan.dlqSplitPoints).toEqual([]);
        expect(plan.streamingSinkGroups).toEqual([]);
        expect(plan.nodes.step_broadcast_join?.joinMode).toBe('batch_batch');

        const bqIdx = plan.sortedNodeIds.indexOf('src_bq_transactions');
        const sqlIdx = plan.sortedNodeIds.indexOf('src_cloudsql_accounts');
        const joinIdx = plan.sortedNodeIds.indexOf('step_broadcast_join');
        const winIdx = plan.sortedNodeIds.indexOf('step_latest_tx_window');
        const sinkIdx = plan.sortedNodeIds.indexOf('snk_bq_enriched');

        expect(bqIdx).toBeGreaterThanOrEqual(0);
        expect(sqlIdx).toBeGreaterThanOrEqual(0);
        expect(bqIdx).toBeLessThan(joinIdx);
        expect(sqlIdx).toBeLessThan(joinIdx);
        expect(joinIdx).toBeLessThan(winIdx);
        expect(winIdx).toBeLessThan(sinkIdx);

        for (const nodeId of plan.sortedNodeIds) {
            expect(plan.nodes[nodeId]?.executionMode).toBe('batch');
        }

        expect(plan.runtimeArtifacts.profile.id).toBe('serverless_4.0');
        expect(plan.runtimeArtifacts.tier).toBe('standard');
        expect(plan.runtimeArtifacts.preinstalledConnectors).toContain('spark-bigquery');
        expect(plan.runtimeArtifacts.gcsJars).toEqual([
            'gs://example-spark-deps/jars/postgresql-42.7.4.jar',
            'gs://example-spark-deps/jars/postgres-socket-factory-1.21.0-jar-with-dependencies.jar',
        ]);
        expect(plan.runtimeArtifacts.mavenPackages).toEqual([]);
    });

    it('selects io-bigquery, io-cloudsql, transform-join, and transform-window-over cleanly via buildPromptFromPipelineYaml', () => {
        const { yamlText } = loadBqCloudSqlSample();
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
            'io-bigquery',
            'io-cloudsql',
            'transform-join',
            'transform-window-over',
            'core-catalyst-optimization',
            'verification-gate',
        ]);
        expect(built.selectedSkills).not.toContain('core-streaming');
        expect(built.selectedSkills).not.toContain('core-python-datasource');
        expect(built.prompt).toContain('## Execution Plan');
    });
});
