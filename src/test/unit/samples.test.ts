/**
 * Sample Pipelines Unit Tests
 * 
 * Author: Mike Nimer <mikenimer@google.com>
 * 
 * Purpose:
 * Unit tests validating sample pipeline discovery, metadata extraction,
 * title formatting, and fallback handling.
 */

import { afterEach, beforeEach, describe, it, expect, vi } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import type * as vscode from 'vscode';
import { formatSampleTitle, getSamplePipelines, computePipelineFlow } from '../../extension';
import { buildSampleReadmeMarkdown } from '../../samples/samplePipelineService';

// Mock vscode module
vi.mock('vscode', async () => (await import('./helpers/vscodeMock')).createVscodeMock());

describe('computePipelineFlow', () => {
    it('returns empty string for empty or missing nodes', () => {
        expect(computePipelineFlow([], [])).toBe('');
        expect(computePipelineFlow(null as any, [])).toBe('');
    });

    it('computes linear flow for single path DAG', () => {
        const nodes = [
            { id: '1', data: { label: 'Trigger', type: 'SOURCE' }, position: { x: 100, y: 100 } },
            { id: '2', data: { label: 'Generate Data', type: 'STEP' }, position: { x: 300, y: 100 } },
            { id: '3', data: { label: 'Log Message', type: 'SINK' }, position: { x: 500, y: 100 } }
        ];
        const edges = [
            { source: '1', target: '2' },
            { source: '2', target: '3' }
        ];

        expect(computePipelineFlow(nodes, edges)).toBe('Trigger ➔ Generate Data ➔ Log Message');
    });

    it('computes branched sink flow correctly (like pubsub-to-bigquery)', () => {
        const nodes = [
            { id: '1', data: { label: 'PubSub Connector', type: 'SOURCE' }, position: { x: 80, y: 150 } },
            { id: '2', data: { label: 'BigQuery', type: 'SINK' }, position: { x: 680, y: 90 } },
            { id: '3', data: { label: 'Parse Message', type: 'STEP' }, position: { x: 380, y: 150 } },
            { id: '4', data: { label: 'Log Message', type: 'SINK' }, position: { x: 680, y: 210 } }
        ];
        const edges = [
            { source: '1', target: '3' },
            { source: '3', target: '2' },
            { source: '3', target: '4' }
        ];

        expect(computePipelineFlow(nodes, edges)).toBe('PubSub Connector ➔ Parse Message ➔ [BigQuery, Log Message]');
    });

    it('computes multi-source joins correctly', () => {
        const nodes = [
            { id: '1', data: { label: 'Kafka Source', type: 'SOURCE' }, position: { x: 50, y: 80 } },
            { id: '2', data: { label: 'PubSub Source', type: 'SOURCE' }, position: { x: 50, y: 200 } },
            { id: '3', data: { label: 'Join Streams', type: 'STEP' }, position: { x: 300, y: 140 } },
            { id: '4', data: { label: 'BigQuery Sink', type: 'SINK' }, position: { x: 550, y: 140 } }
        ];
        const edges = [
            { source: '1', target: '3' },
            { source: '2', target: '3' },
            { source: '3', target: '4' }
        ];

        expect(computePipelineFlow(nodes, edges)).toBe('[Kafka Source, PubSub Source] ➔ Join Streams ➔ BigQuery Sink');
    });
});

describe('formatSampleTitle', () => {
    it('formats kebab-case names into title case words', () => {
        expect(formatSampleTitle('generate-mock-data')).toBe('Generate Mock Data');
        expect(formatSampleTitle('pubsub-to-bigquery-streaming')).toBe('Pubsub To Bigquery Streaming');
    });

    it('formats snake_case names into title case words', () => {
        expect(formatSampleTitle('kafka_to_jdbc')).toBe('Kafka To Jdbc');
    });

    it('handles empty or single word strings gracefully', () => {
        expect(formatSampleTitle('')).toBe('');
        expect(formatSampleTitle('simple')).toBe('Simple');
    });
});

describe('getSamplePipelines', () => {
    let extensionRoot: string;

    beforeEach(() => {
        extensionRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'spark-samples-'));
    });

    afterEach(() => {
        fs.rmSync(extensionRoot, { recursive: true, force: true });
    });

    /**
     * Writes a file under the temporary extension root and creates parent folders.
     *
     * @param relativePath Path relative to the extension root.
     * @param content File content to write.
     */
    const writeFixture = (relativePath: string, content: string): void => {
        const fullPath = path.join(extensionRoot, relativePath);
        fs.mkdirSync(path.dirname(fullPath), { recursive: true });
        fs.writeFileSync(fullPath, content, 'utf-8');
    };

    it('discovers and parses .spark samples with companion and synthesized READMEs', async () => {
        writeFixture(
            'resources/samples/kafka-to-iceberg.spark',
            [
                'pipelineConfig: { language: PySpark }',
                'jobConfig: { runner: ServerlessSpark }',
                'nodes:',
                '  - { id: "1", data: { label: Kafka Source, type: SOURCE, spark_transform: KafkaSource }, position: { x: 0, y: 0 } }',
                '  - { id: "2", data: { label: Parse Payload, type: STEP, spark_transform: ParsePayload }, position: { x: 200, y: 0 } }',
                '  - { id: "3", data: { label: Iceberg Sink, type: SINK, spark_transform: IcebergSink }, position: { x: 400, y: 0 } }',
                'edges:',
                '  - { source: "1", target: "2" }',
                '  - { source: "2", target: "3" }',
                '',
            ].join('\n')
        );
        writeFixture('resources/samples/kafka-to-iceberg.md', '# Kafka to Apache Iceberg Lakehouse Pipeline\n');
        writeFixture(
            'resources/samples/generate-mock-data.spark',
            [
                'nodes:',
                '  - { id: a, data: { label: Rate Source, type: SOURCE, spark_transform: RateSource }, position: { x: 0, y: 0 } }',
                '  - { id: b, data: { label: Console Sink, type: SINK, spark_transform: ConsoleSink }, position: { x: 200, y: 0 } }',
                'edges:',
                '  - { source: a, target: b }',
                '',
            ].join('\n')
        );
        writeFixture('resources/samples/legacy-pipeline.yaml', 'nodes: []\n');
        writeFixture('resources/samples/notes.txt', 'ignored\n');

        const samples = await getSamplePipelines({ extensionPath: extensionRoot } as unknown as vscode.ExtensionContext);
        expect(samples.map((s) => s.filename).sort()).toEqual(['generate-mock-data.spark', 'kafka-to-iceberg.spark']);

        const kafka = samples.find((s) => s.filename === 'kafka-to-iceberg.spark');
        expect(kafka).toMatchObject({
            id: 'kafka-to-iceberg',
            title: 'Kafka To Iceberg',
            language: 'PySpark',
            runner: 'ServerlessSpark',
            nodesCount: 3,
            description: 'Pipeline flow: Kafka Source ➔ Parse Payload ➔ Iceberg Sink',
        });
        expect(kafka?.readmeContent).toBe('# Kafka to Apache Iceberg Lakehouse Pipeline\n');

        const mock = samples.find((s) => s.filename === 'generate-mock-data.spark');
        expect(mock).toMatchObject({
            id: 'generate-mock-data',
            title: 'Generate Mock Data',
            language: 'PySpark',
            runner: 'Local',
            nodesCount: 2,
            description: 'Pipeline flow: Rate Source ➔ Console Sink',
        });
        expect(mock?.readmeContent).toContain('# Generate Mock Data');
        expect(mock?.readmeContent).toContain('## Overview');
        expect(mock?.readmeContent).toContain('## Pipeline Architecture & Data Flow');
        expect(mock?.readmeContent).not.toContain('## Pipeline Nodes & Transforms');
    });

    it('returns an empty list when no samples directory exists', async () => {
        const samples = await getSamplePipelines({ extensionPath: extensionRoot } as unknown as vscode.ExtensionContext);
        expect(samples).toEqual([]);
    });
});

describe('buildSampleReadmeMarkdown', () => {
    it('synthesizes Apache Spark 4.x PySpark fallback markdown with flow and runtime variables', () => {
        const markdown = buildSampleReadmeMarkdown(
            'Orders Etl',
            'orders-etl.spark',
            'Parquet Source ➔ Filter ➔ Parquet Sink',
            'ServerlessSpark',
            {
                nodes: [
                    { id: 'n1', data: { label: 'Parquet Source', type: 'SOURCE', spark_transform: 'ParquetSource' } },
                    { id: 'n2', data: { label: 'Filter', type: 'STEP', spark_transform: 'Filter' } },
                    { id: 'n3', data: { label: 'Parquet Sink', type: 'SINK', spark_transform: 'ParquetSink' } },
                ],
                runtimeVariables: [
                    { key: 'INPUT_URI', value: 'gs://bucket/orders' },
                    { key: 'OPTIONAL_TAG', value: '' },
                ],
            }
        );

        expect(markdown).toContain('# Orders Etl');
        expect(markdown).toContain('## Overview');
        expect(markdown).toContain('Apache Spark 4.x (PySpark)');
        expect(markdown).toContain('`orders-etl.spark`');
        expect(markdown).toContain('## Pipeline Architecture & Data Flow');
        expect(markdown).not.toContain('## Pipeline Nodes & Transforms');
        expect(markdown).toContain('## Runtime Parameters');
        expect(markdown).toContain('| `INPUT_URI` | `gs://bucket/orders` |');
        expect(markdown).toContain('| `OPTIONAL_TAG` | *(unset)* |');
        expect(markdown).toContain('./run_local.sh');
        expect(markdown).not.toContain('Apache Beam');
    });

    it('falls back to runner summary when flow and nodes are empty', () => {
        const markdown = buildSampleReadmeMarkdown('Empty Sample', 'empty.spark', '', 'Local', {});
        expect(markdown).toContain('# Empty Sample');
        expect(markdown).toContain('configured for the **Local** runner');
        expect(markdown).toContain('./run_local.sh');
    });
});

describe('bundled resources/samples catalog (Phase 9 I1-I4 & Phase 12 SDP11)', () => {
    const repoRoot = path.resolve(__dirname, '../../../');
    const bundledSamplesDir = path.join(repoRoot, 'resources', 'samples');
    const expectedSlugs = [
        'bigquery-cloudsql-broadcast-join',
        'gcs-parquet-etl',
        'generate-mock-data',
        'kafka-streaming-to-iceberg',
        'sdp-auto-cdc-scd1',
        'sdp-biglake-iceberg-medallion',
    ];
    const sdpSlugs = new Set(['sdp-auto-cdc-scd1', 'sdp-biglake-iceberg-medallion']);

    it('ships all six bundled .spark and .md sample pairs and zero unexpected files', () => {
        expect(fs.existsSync(bundledSamplesDir)).toBe(true);
        const entries = fs.readdirSync(bundledSamplesDir).sort();

        expect(entries.filter((f) => !f.endsWith('.spark') && !f.endsWith('.md'))).toEqual([]);
        expect(entries.filter((f) => f.endsWith('.spark'))).toEqual(
            expectedSlugs.map((slug) => `${slug}.spark`)
        );
        expect(entries.filter((f) => f.endsWith('.md'))).toEqual(
            expectedSlugs.map((slug) => `${slug}.md`)
        );
    });

    it('discovers all six bundled samples cleanly through getSamplePipelines', async () => {
        const samples = await getSamplePipelines({ extensionPath: repoRoot } as unknown as vscode.ExtensionContext);
        expect(samples.map((s) => s.id).sort()).toEqual(expectedSlugs);

        for (const sample of samples) {
            expect(sample.filename).toBe(`${sample.id}.spark`);
            expect(sample.language).toBe('PySpark');
            expect(sample.badge).toBe(sdpSlugs.has(sample.id) ? 'SDP (4.2+)' : 'PySpark');
            expect(['ServerlessSpark', 'Local']).toContain(sample.runner);
            expect(sample.nodesCount).toBeGreaterThanOrEqual(3);
            expect(sample.description).toMatch(/^Pipeline flow: /);
            expect(sample.readmeContent).toContain('## Overview');
            expect(sample.readmeContent).toContain('## Pipeline Architecture & Data Flow');
            expect(sample.readmeContent).not.toContain('## Pipeline Nodes & Transforms');
        }
    });
});
