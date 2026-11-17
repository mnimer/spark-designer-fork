/**
 * Unit tests for the pipeline feature detector (`src/codegen/PipelineFeatureDetector.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Uses small YAML fixtures. Covers side-input edges, multi-output edges, streaming
 * sources (`STREAMING_SOURCE_TRANSFORMS`, `readMode: 'streaming'`, `computeStreamingNodeSet`),
 * `Watermark` and `StatefulTransform` steps, Python 4.0 `DataSource` transforms,
 * system instructions and custom-logic properties, missing jobConfig, unknown and missing
 * `spark_transform` values, empty graphs, and malformed input shapes.
 */

import { describe, it, expect } from 'vitest';
import YAML from 'yaml';
import { detectPipelineFeatures } from '../../../codegen/PipelineFeatureDetector';
import { PYTHON_DATASOURCE_TRANSFORMS, STREAMING_SOURCE_TRANSFORMS } from '../../../codegen/codegenConfig';

/**
 * Parses a YAML fixture.
 *
 * @param text YAML text.
 * @returns Parsed value.
 */
function parse(text: string): unknown {
    return YAML.parse(text);
}

describe('detectPipelineFeatures', () => {
    it('collects transforms and the language runners for a batch graph (simple)', () => {
        const result = detectPipelineFeatures(
            parse(`
nodes:
  - id: a
    data: { type: SOURCE, label: Parquet, spark_transform: ParquetSource }
  - id: b
    data: { type: SINK, label: BigQuery, spark_transform: BigQuerySink }
edges:
  - { source: a, target: b }
jobConfig: { runner: ServerlessSpark }
`),
            'PySpark'
        );
        expect(result.language).toBe('python');
        expect([...result.transforms].sort()).toEqual(['BigQuerySink', 'ParquetSource']);
        expect([...result.runners]).toEqual(['local', 'dataproc-cluster', 'serverless-spark']);
        expect(result.features.size).toBe(0);
        expect(result.warnings).toEqual([]);
    });

    it('returns every PySpark runner regardless of jobConfig.runner (simple)', () => {
        const result = detectPipelineFeatures(parse('nodes: []\njobConfig: { runner: Local }'), 'PySpark');
        expect([...result.runners]).toEqual(['local', 'dataproc-cluster', 'serverless-spark']);
    });

    it('detects side inputs from targetHandle side_input (simple)', () => {
        const result = detectPipelineFeatures(
            parse('edges:\n  - { source: a, target: b, targetHandle: side_input }'),
            'PySpark'
        );
        expect(result.features.has('side_inputs')).toBe(true);
    });

    it('detects multi-output from sourceHandle valid or error (simple)', () => {
        expect(detectPipelineFeatures(parse('edges:\n  - { sourceHandle: valid }'), 'PySpark').features.has('multi_output')).toBe(true);
        expect(detectPipelineFeatures(parse('edges:\n  - { sourceHandle: error }'), 'PySpark').features.has('multi_output')).toBe(true);
        expect(detectPipelineFeatures(parse('edges:\n  - { sourceHandle: other, targetHandle: null }'), 'PySpark').features.size).toBe(0);
    });

    it('detects streaming from every STREAMING_SOURCE_TRANSFORMS entry unless readMode is batch (simple)', () => {
        for (const sourceTransform of STREAMING_SOURCE_TRANSFORMS) {
            const source = detectPipelineFeatures(
                parse(`nodes:\n  - id: s1\n    data: { type: SOURCE, spark_transform: ${sourceTransform} }`),
                'PySpark'
            );
            expect(source.features.has('streaming'), sourceTransform).toBe(true);
        }

        // Explicitly setting readMode: 'batch' on a dual-mode source keeps a batch-only graph in batch mode.
        const batchKafka = detectPipelineFeatures(
            parse('nodes:\n  - id: s1\n    data: { type: SOURCE, spark_transform: KafkaSource, properties: { readMode: batch } }'),
            'PySpark'
        );
        expect(batchKafka.features.has('streaming')).toBe(false);

        // Any file/lakehouse source with readMode: 'streaming' enables streaming.
        const streamingParquet = detectPipelineFeatures(
            parse('nodes:\n  - id: s1\n    data: { type: SOURCE, spark_transform: ParquetSource, properties: { readMode: streaming } }'),
            'PySpark'
        );
        expect(streamingParquet.features.has('streaming')).toBe(true);
    });

    it('detects streaming from Watermark, StatefulTransform, or legacy WindowInto steps (simple)', () => {
        for (const stepTransform of ['Watermark', 'StatefulTransform', 'WindowInto']) {
            const result = detectPipelineFeatures(
                parse(`nodes:\n  - data: { type: STEP, spark_transform: ${stepTransform} }`),
                'PySpark'
            );
            expect(result.features.has('streaming'), stepTransform).toBe(true);
        }
    });

    it('detects python_datasource feature for all Spark 4.0 Python DataSource transforms (simple)', () => {
        for (const dsTransform of PYTHON_DATASOURCE_TRANSFORMS) {
            const result = detectPipelineFeatures(
                parse(`nodes:\n  - id: n1\n    data: { type: SOURCE, spark_transform: ${dsTransform} }`),
                'PySpark'
            );
            expect(result.features.has('python_datasource'), dsTransform).toBe(true);
        }

        const nonPythonDs = detectPipelineFeatures(
            parse('nodes:\n  - id: n1\n    data: { type: SOURCE, spark_transform: BigQuerySource }'),
            'PySpark'
        );
        expect(nonPythonDs.features.has('python_datasource')).toBe(false);
    });

    it('detects custom instructions from system_instructions or custom-logic properties (simple)', () => {
        const fromData = detectPipelineFeatures(
            parse('nodes:\n  - data: { spark_transform: CustomTransform, system_instructions: "uppercase it" }'),
            'PySpark'
        );
        expect(fromData.features.has('custom_instructions')).toBe(true);
        const fromProps = detectPipelineFeatures(
            parse('nodes:\n  - data: { spark_transform: CustomTransform, properties: { codeGenerationPrompt: "x" } }'),
            'PySpark'
        );
        expect(fromProps.features.has('custom_instructions')).toBe(true);
    });

    it('ignores blank instructions and non-object properties (extreme)', () => {
        const result = detectPipelineFeatures(
            parse(
                'nodes:\n  - data: { spark_transform: Select, system_instructions: "   ", properties: null }\n  - data: { spark_transform: Filter, properties: { prompt: "" } }'
            ),
            'PySpark'
        );
        expect(result.features.has('custom_instructions')).toBe(false);
    });

    it('skips nodes without spark_transform and warns once per node (failure)', () => {
        const result = detectPipelineFeatures(
            parse(
                'nodes:\n  - id: "1"\n    data: { type: SOURCE, label: PubSub Connector, system_instructions: do it }\n  - data: { spark_transform: "  " }\n  - {}'
            ),
            'PySpark'
        );
        expect(result.transforms.size).toBe(0);
        expect(result.warnings).toEqual([
            "Node 'PubSub Connector' (1) has no spark_transform. No connector skill is loaded for it.",
            "Node '(no label)' ((no id)) has no spark_transform. No connector skill is loaded for it.",
            "Node '(no label)' ((no id)) has no spark_transform. No connector skill is loaded for it.",
        ]);
        // The custom instruction still counts even though the transform is missing.
        expect(result.features.has('custom_instructions')).toBe(true);
    });

    it('keeps unknown spark_transform values and trims whitespace (extreme)', () => {
        const result = detectPipelineFeatures(
            parse('nodes:\n  - data: { spark_transform: " NotInCatalog " }'),
            'PySpark'
        );
        expect([...result.transforms]).toEqual(['NotInCatalog']);
    });

    it('tolerates an empty graph, missing jobConfig, and non-object input (extreme)', () => {
        for (const input of [parse('{}'), null, undefined, 'text', 42, [], parse('nodes: notalist\nedges: 7')]) {
            const result = detectPipelineFeatures(input, 'PySpark');
            expect(result.transforms.size).toBe(0);
            expect(result.features.size).toBe(0);
            expect(result.warnings).toEqual([]);
        }
    });

    it('tolerates non-object nodes and edges (failure)', () => {
        const result = detectPipelineFeatures(
            { nodes: [null, 'x', { data: [] }], edges: [null, 3] },
            'PySpark'
        );
        expect(result.transforms.size).toBe(0);
        expect(result.warnings).toHaveLength(3);
    });

    it('resolves legacy spark_transform values before streaming/python_datasource checks and warns (regression: ReadFromKafka)', () => {
        const resolver = (raw: string) =>
            raw === 'ReadFromKafka'
                ? { canonical: 'KafkaSource', aliased: true, known: true }
                : { canonical: raw.trim(), aliased: false, known: raw.trim() !== 'MadeUp' };
        const result = detectPipelineFeatures(
            parse(
                'nodes:\n  - data: { type: SOURCE, label: Kafka, spark_transform: ReadFromKafka }\n  - data: { type: SINK, label: X, spark_transform: MadeUp }\n  - data: { type: STEP, spark_transform: Select }'
            ),
            'PySpark',
            resolver
        );
        expect([...result.transforms].sort()).toEqual(['KafkaSource', 'MadeUp', 'Select']);
        expect(result.features.has('streaming')).toBe(true);
        expect(result.warnings).toEqual([
            "Node 'Kafka': spark_transform 'ReadFromKafka' is treated as 'KafkaSource'.",
            "Node 'X': spark_transform 'MadeUp' has no codegen skill.",
        ]);
    });
});
