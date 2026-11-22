/**
 * Unit Tests for Bundled Spark Declarative Pipelines Samples (SDP11).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies both bundled SDP sample pipelines (`sdp-biglake-iceberg-medallion` and
 * `sdp-auto-cdc-scd1`) and their companion Markdown guides:
 * - Parse cleanly with `paradigm: 'sdp'` and `pipelineConfig.language: 'PySpark'`.
 * - Pass `buildSdpPlan` with `isCompatible: true` and `blockingIssues: []`.
 * - Pass `validateDagTopology` in SDP mode with `isValid: true`, `errors: []`, and `warnings: []`.
 * - Pass `redactPipelineDocument` with `redactedPaths: []` (zero hardcoded credentials).
 * - Expose `badge: 'SDP (4.2+)'` in `getSamplePipelines` and `DEFAULT_SAMPLES`.
 * - Satisfy ASD-STE100 companion Markdown rules (zero semicolons in prose, required sections).
 */

import * as fs from 'fs';
import * as path from 'path';
import { describe, it, expect, vi } from 'vitest';
import * as YAML from 'yaml';
import type * as vscode from 'vscode';
import { redactPipelineDocument } from '../../../credentialRedaction';
import { buildPromptFromPipelineYaml } from '../../../codegen/CodegenPromptBuilder';
import { getSamplePipelines, computePipelineFlow } from '../../../samples/samplePipelineService';
import { validateDagTopology } from '../../../shared/dagValidator';
import { buildSdpPlan } from '../../../shared/sdpPlanner';

vi.mock('vscode', async () => (await import('../helpers/vscodeMock')).createVscodeMock());

const REPO_ROOT = path.resolve(__dirname, '../../../../');
const SAMPLES_DIR = path.join(REPO_ROOT, 'resources', 'samples');
const WEBVIEW_SAMPLES_CATALOG_PATH = path.join(REPO_ROOT, 'webview', 'src', 'catalog', 'samples.ts');

const SDP_SAMPLE_SLUGS = [
    'sdp-biglake-iceberg-medallion',
    'sdp-auto-cdc-scd1',
] as const;

describe('Bundled SDP Sample Pipelines (SDP11)', () => {
    it.each(SDP_SAMPLE_SLUGS)(
        '%s parses cleanly, passes buildSdpPlan, validateDagTopology, and credential redaction',
        (slug) => {
            const sparkPath = path.join(SAMPLES_DIR, `${slug}.spark`);
            expect(fs.existsSync(sparkPath)).toBe(true);

            const rawYaml = fs.readFileSync(sparkPath, 'utf-8');
            const redaction = redactPipelineDocument(rawYaml);
            expect(redaction.redactedPaths).toEqual([]);

            const parsed = YAML.parse(rawYaml) as Record<string, any>;
            expect(parsed.paradigm).toBe('sdp');
            expect(parsed.pipelineConfig?.language).toBe('PySpark');
            expect(parsed.jobConfig?.runner).toBe('ServerlessSpark');
            expect(parsed.jobConfig?.runtimeVersion).toBe('4.0');
            expect(parsed.jobConfig?.sdpCatalog).toBe('lake');
            expect(String(parsed.jobConfig?.sdpStorage)).toMatch(/^gs:\/\//);

            const dagResult = validateDagTopology(parsed.nodes, parsed.edges, {
                jobConfig: parsed.jobConfig,
                requireCheckpoints: true,
                paradigm: 'sdp',
            });
            expect(dagResult.isValid).toBe(true);
            expect(dagResult.errors).toEqual([]);
            expect(dagResult.warnings).toEqual([]);
            expect(dagResult.orphanNodeIds).toEqual([]);

            const sdpPlan = buildSdpPlan(parsed.nodes, parsed.edges, parsed.jobConfig, {
                enforceRuntimeVersion: true,
            });
            expect(sdpPlan.isCompatible).toBe(true);
            expect(sdpPlan.blockingIssues).toEqual([]);
            expect(sdpPlan.datasets.length).toBe(parsed.nodes.length);
        }
    );

    it('maps sdp-biglake-iceberg-medallion to streaming bronze/silver tables and a gold materialized view', () => {
        const rawYaml = fs.readFileSync(
            path.join(SAMPLES_DIR, 'sdp-biglake-iceberg-medallion.spark'),
            'utf-8'
        );
        const parsed = YAML.parse(rawYaml) as Record<string, any>;
        const plan = buildSdpPlan(parsed.nodes, parsed.edges, parsed.jobConfig);

        expect(plan.datasetsByNodeId.src_orders_csv.kind).toBe('streaming_table');
        expect(plan.datasetsByNodeId.src_orders_csv.datasetName).toBe('bronze_orders');
        expect(plan.datasetsByNodeId.step_bronze_clean.kind).toBe('streaming_table');
        expect(plan.datasetsByNodeId.step_silver_validate.kind).toBe('streaming_table');
        expect(plan.datasetsByNodeId.step_silver_validate.datasetName).toBe('silver_orders');
        expect(plan.datasetsByNodeId.step_gold_agg.kind).toBe('materialized_view');
        expect(plan.datasetsByNodeId.sink_gold_iceberg.kind).toBe('materialized_view');
        expect(plan.datasetsByNodeId.sink_gold_iceberg.datasetName).toBe('gold_store_revenue');
        expect(plan.datasetsByNodeId.sink_gold_iceberg.format).toBe('iceberg');
    });

    it('maps sdp-auto-cdc-scd1 to dp.create_auto_cdc_flow targeting silver_customers', () => {
        const rawYaml = fs.readFileSync(
            path.join(SAMPLES_DIR, 'sdp-auto-cdc-scd1.spark'),
            'utf-8'
        );
        const parsed = YAML.parse(rawYaml) as Record<string, any>;
        const plan = buildSdpPlan(parsed.nodes, parsed.edges, parsed.jobConfig);

        expect(plan.datasetsByNodeId.step_auto_cdc_scd1.kind).toBe('auto_cdc');
        expect(plan.datasetsByNodeId.step_auto_cdc_scd1.badge).toBe('CDC');
        expect(plan.datasetsByNodeId.step_auto_cdc_scd1.targetDataset).toBe('silver_customers');
        expect(plan.datasetsByNodeId.sink_silver_customers.kind).toBe('auto_cdc');
        expect(plan.datasetsByNodeId.sink_silver_customers.datasetName).toBe('silver_customers');
        expect(plan.datasetsByNodeId.sink_silver_customers.format).toBe('iceberg');
    });

    it.each(SDP_SAMPLE_SLUGS)(
        '%s companion Markdown follows ASD-STE100 rules with required headings and zero semicolons in prose',
        (slug) => {
            const mdPath = path.join(SAMPLES_DIR, `${slug}.md`);
            expect(fs.existsSync(mdPath)).toBe(true);
            const md = fs.readFileSync(mdPath, 'utf-8');

            expect(md).toMatch(/^#\s+\S+/m);
            expect(md).toContain('## Overview');
            expect(md).toContain('## Pipeline Architecture & Data Flow');
            expect(md).toContain('## Runtime Parameters');
            expect(md).toContain('## Code Generation & Execution');
            expect(md).not.toContain('## Pipeline Nodes & Transforms');

            // Strip fenced code blocks and check that zero semicolons appear in prose.
            const proseOnly = md.replace(/```[\s\S]*?```/g, '');
            expect(proseOnly).not.toContain(';');
            const legacyExt = ['.', 'beam'].join('');
            expect(md.toLowerCase()).not.toContain(legacyExt);
        }
    );

    it('surfaces badge "SDP (4.2+)" in both getSamplePipelines and webview DEFAULT_SAMPLES', async () => {
        const discovered = await getSamplePipelines({
            extensionPath: REPO_ROOT,
        } as unknown as vscode.ExtensionContext);
        const webviewCatalogSource = fs.readFileSync(WEBVIEW_SAMPLES_CATALOG_PATH, 'utf-8');
        expect(webviewCatalogSource).toContain("badge: 'SDP (4.2+)'");

        for (const slug of SDP_SAMPLE_SLUGS) {
            const hostEntry = discovered.find((s) => s.id === slug);
            expect(hostEntry).toBeDefined();
            if (!hostEntry) {
                throw new Error(`Missing discovered sample ${slug}`);
            }
            expect(hostEntry.badge).toBe('SDP (4.2+)');
            expect(hostEntry.runner).toBe('ServerlessSpark');

            const parsed = YAML.parse(hostEntry.content);
            const expectedFlow = `Pipeline flow: ${computePipelineFlow(parsed.nodes, parsed.edges)}`;
            expect(hostEntry.description).toBe(expectedFlow);
            expect(webviewCatalogSource).toContain(`id: '${slug}'`);
            expect(webviewCatalogSource).toContain(expectedFlow);
        }
    });

    it.each(SDP_SAMPLE_SLUGS)(
        '%s builds an SDP codegen prompt cleanly via CodegenPromptBuilder with zero warnings',
        (slug) => {
            const rawYaml = fs.readFileSync(path.join(SAMPLES_DIR, `${slug}.spark`), 'utf-8');
            const skillsRoot = path.join(REPO_ROOT, 'resources', 'codegen-skills');
            const built = buildPromptFromPipelineYaml({
                skillsRoot,
                yamlText: rawYaml,
                language: 'PySpark',
            });

            expect(built.detection.paradigm).toBe('sdp');
            expect(built.detection.warnings).toEqual([]);
            expect(built.redactedPaths).toEqual([]);
            expect(built.sdpPlan?.isCompatible).toBe(true);
            expect(built.sdpPlan?.blockingIssues).toEqual([]);
            expect(built.sdpPlan?.warnings).toEqual([]);
            expect(built.selectedSkills).toContain('sdp-core');
            expect(built.selectedSkills).toContain('sdp-spec');
            expect(built.selectedSkills).toContain('runner-sdp');
            expect(built.selectedSkills).toContain('io-iceberg');
            expect(built.selectedSkills).not.toContain('runner-local');
            expect(built.selectedSkills).not.toContain('runner-serverless-spark');
            expect(built.selectedSkills).not.toContain('core-streaming');
            if (slug === 'sdp-auto-cdc-scd1') {
                expect(built.selectedSkills).toContain('sdp-auto-cdc');
            }
            expect(built.prompt).toContain('## SDP Execution Plan');
            expect(built.prompt).toContain('spark-pipeline.yml');
        }
    );
});
