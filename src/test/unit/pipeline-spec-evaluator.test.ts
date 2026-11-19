/**
 * Unit Tests for Pipeline Specification & Property Conformance Evaluator.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Tests the YAML specification parser, human-readable checklist formatter, ExecutionPlan
 * builder, deterministic spark_transform and ExecutionPlan compliance audits, and the
 * PipelineSpecificationAgent sub-agent under normal, boundary, and failure conditions.
 * The LLM client is mocked, so the tests run offline.
 */

import { describe, it, expect, vi } from 'vitest';
import {
    parsePipelineSpec,
    formatSpecChecklist,
    generateRequirementMatrix,
    buildSpecExecutionPlan,
    isLegacyBeamTransform,
    auditSparkTransformProperties,
    auditExecutionPlanCompliance,
    evaluatePipelineSpecification
} from '../integration/evaluators/pipeline-spec-evaluator';
import type { ExecutionPlan } from '../../shared/executionPlanner';

/**
 * Builds the ExecutionPlan for a YAML fixture and fails the test if none is produced.
 *
 * @param yamlContent - Pipeline YAML fixture.
 * @returns ExecutionPlan.
 */
function mustPlan(yamlContent: string): ExecutionPlan {
    const plan = buildSpecExecutionPlan(yamlContent);
    if (!plan) {
        throw new Error('Expected an ExecutionPlan for the YAML fixture');
    }
    return plan;
}

const STREAMING_DLQ_YAML = `
nodes:
  - id: src_kafka
    data: { label: Kafka, type: SOURCE, spark_transform: KafkaSource, properties: { topic: events } }
  - id: parse
    data: { label: Parse, type: STEP, spark_transform: ParsePayload, properties: { format: json } }
  - id: sink_bq
    data: { label: BQ, type: SINK, spark_transform: BigQuerySink, properties: { table: ds.events } }
  - id: sink_spanner
    data: { label: Spanner, type: SINK, spark_transform: SpannerSink, properties: { table: events } }
  - id: sink_dlq
    data: { label: DLQ, type: SINK, spark_transform: ConsoleSink }
edges:
  - { source: src_kafka, target: parse }
  - { source: parse, target: sink_bq, sourceHandle: output }
  - { source: parse, target: sink_spanner, sourceHandle: output }
  - { source: parse, target: sink_dlq, sourceHandle: error }
jobConfig: { runner: ServerlessSpark }
`;

const BATCH_FANOUT_YAML = `
nodes:
  - id: src
    data: { label: Orders, type: SOURCE, spark_transform: ParquetSource }
  - id: flt
    data: { label: Filter, type: STEP, spark_transform: Filter }
  - id: s1
    data: { label: Lake, type: SINK, spark_transform: ParquetSink }
  - id: s2
    data: { label: Spanner, type: SINK, spark_transform: SpannerSink }
edges:
  - { source: src, target: flt }
  - { source: flt, target: s1 }
  - { source: flt, target: s2 }
jobConfig: { runner: DataprocCluster }
`;

const COMPLIANT_STREAMING_PIPELINE = [
    'def read_sources(spark, config):',
    '    return {"src_kafka": spark.readStream.format("kafka").load()}',
    '',
    'def transform(dfs, config):',
    '    parsed = dfs["src_kafka"].selectExpr("CAST(value AS STRING) AS _corrupt_record")',
    '    dfs["parse"] = parsed.filter("_corrupt_record IS NULL")',
    '    dfs["parse__error"] = parsed.filter("_corrupt_record IS NOT NULL")',
    '    return dfs',
    '',
    'def write_sinks(dfs, config):',
    '    def write_micro_batch(batch_df, batch_id):',
    '        batch_df.cache()',
    '        try:',
    '            batch_df.write.option("txnVersion", batch_id).option("txnAppId", "spark_pipeline_parse").save()',
    '        finally:',
    '            batch_df.unpersist()',
    '    dfs["parse"].writeStream.foreachBatch(write_micro_batch).option("checkpointLocation", config["ckpt"]).start()',
    ''
].join('\n');

describe('Pipeline Specification Evaluator (pipeline-spec-evaluator.ts)', () => {
    describe('parsePipelineSpec', () => {
        it('parses valid pipeline YAML into structured nodes, properties, and configs (simple condition)', () => {
            const yamlContent = `
nodes:
  - id: "node-1"
    type: pipeline
    data:
      label: JDBC Source
      type: SOURCE
      spark_transform: JdbcSource
      properties:
        driverClassName: org.postgresql.Driver
        url: jdbc:postgresql://localhost:5432/mydb
        username: dbuser
        password: dbpassword
        tableName: users_table
        outputParallelization: true
  - id: "node-2"
    type: pipeline
    data:
      label: Transform Step
      type: STEP
      spark_transform: CustomTransform
      properties:
        codeGenerationPrompt: "Parse JSON payload"
      system_instructions: "Wrap in try-catch and route errors to DLQ"
  - id: "node-3"
    type: pipeline
    data:
      label: Kafka Sink
      type: SINK
      spark_transform: KafkaSink
      properties:
        topic: output-events
        bootstrapServers: localhost:9092
        useEOS: true
edges:
  - id: "e1"
    source: "node-1"
    target: "node-2"
  - id: "e2"
    source: "node-2"
    target: "node-3"
    sourceHandle: "valid"
runtimeVariables:
  - key: JOB_ID
    value: test-job-123
jobConfig:
  runner: ServerlessSpark
  machineType: n1-standard-1
  minInstances: 1
  maxInstances: 5
pipelineConfig:
  language: Python
  outputFolder: output/test
`;

            const spec = parsePipelineSpec(yamlContent);

            expect(spec.nodes.length).toBe(3);

            // Node 1 (Source)
            expect(spec.nodes[0].id).toBe('node-1');
            expect(spec.nodes[0].label).toBe('JDBC Source');
            expect(spec.nodes[0].type).toBe('SOURCE');
            expect(spec.nodes[0].sparkTransform).toBe('JdbcSource');
            expect(spec.nodes[0].properties.driverClassName).toBe('org.postgresql.Driver');
            expect(spec.nodes[0].properties.tableName).toBe('users_table');
            expect(spec.nodes[0].properties.outputParallelization).toBe(true);

            // Node 2 (Step with instructions)
            expect(spec.nodes[1].id).toBe('node-2');
            expect(spec.nodes[1].type).toBe('STEP');
            expect(spec.nodes[1].systemInstructions).toBe('Wrap in try-catch and route errors to DLQ');
            expect(spec.nodes[1].codeGenerationPrompt).toBe('Parse JSON payload');

            // Node 3 (Sink)
            expect(spec.nodes[2].id).toBe('node-3');
            expect(spec.nodes[2].type).toBe('SINK');
            expect(spec.nodes[2].properties.topic).toBe('output-events');
            expect(spec.nodes[2].properties.useEOS).toBe(true);

            // Runtime Variables
            expect(spec.runtimeVariables.length).toBe(1);
            expect(spec.runtimeVariables[0].key).toBe('JOB_ID');
            expect(spec.runtimeVariables[0].value).toBe('test-job-123');

            // Job & Pipeline Configs
            expect(spec.jobConfig.runner).toBe('ServerlessSpark');
            expect(spec.jobConfig.maxInstances).toBe(5);
            expect(spec.pipelineConfig.language).toBe('Python');

            // Edges
            expect(spec.edges.length).toBe(2);
            expect(spec.edges[1].sourceHandle).toBe('valid');
        });

        it('handles empty string or whitespace-only YAML gracefully (extreme condition)', () => {
            const emptySpec = parsePipelineSpec('');
            expect(emptySpec.nodes).toEqual([]);
            expect(emptySpec.runtimeVariables).toEqual([]);
            expect(emptySpec.jobConfig).toEqual({});
            expect(emptySpec.edges).toEqual([]);

            const whitespaceSpec = parsePipelineSpec('   \n  \t  ');
            expect(whitespaceSpec.nodes).toEqual([]);
        });

        it('handles malformed YAML syntax without throwing (failure condition)', () => {
            const corruptYaml = `::: invalid [yaml mapping :::`;
            const spec = parsePipelineSpec(corruptYaml);
            expect(spec.nodes).toEqual([]);
            expect(spec.runtimeVariables).toEqual([]);
            expect(spec.jobConfig).toEqual({});
            expect(spec.edges).toEqual([]);
        });

        it('handles YAML with non-object root or missing properties safely (extreme condition)', () => {
            const scalarYaml = `just a plain string`;
            const spec = parsePipelineSpec(scalarYaml);
            expect(spec.nodes).toEqual([]);

            const partialYaml = `
nodes:
  - id: "n1"
`;
            const partialSpec = parsePipelineSpec(partialYaml);
            expect(partialSpec.nodes.length).toBe(1);
            expect(partialSpec.nodes[0].id).toBe('n1');
            expect(partialSpec.nodes[0].label).toBe('Unnamed Node');
            expect(partialSpec.nodes[0].properties).toEqual({});
        });
    });

    describe('formatSpecChecklist', () => {
        it('formats structured checklist text with all properties and custom instructions (simple condition)', () => {
            const spec = {
                nodes: [
                    {
                        id: 'n1',
                        label: 'Postgres Reader',
                        type: 'SOURCE' as const,
                        sparkTransform: 'JdbcSource',
                        properties: {
                            driverClassName: 'org.postgresql.Driver',
                            url: 'jdbc:postgresql://localhost:5432/mydb'
                        },
                        systemInstructions: 'Log row counts on start',
                        codeGenerationPrompt: 'Generate connection pool'
                    }
                ],
                runtimeVariables: [{ key: 'JOB_ID', value: 'sample-job' }],
                jobConfig: { runner: 'ServerlessSpark' },
                pipelineConfig: { language: 'Python' },
                edges: [{ source: 'n1', target: 'n2', sourceHandle: 'valid' }]
            };

            const checklist = formatSpecChecklist(spec);

            expect(checklist).toContain('Total Nodes: 1');
            expect(checklist).toContain('[Node 1] ID: "n1" | Label: "Postgres Reader" | Type: SOURCE | Transform: JdbcSource');
            expect(checklist).toContain('driverClassName: "org.postgresql.Driver"');
            expect(checklist).toContain('url: "jdbc:postgresql://localhost:5432/mydb"');
            expect(checklist).toContain('Custom System Instructions: "Log row counts on start"');
            expect(checklist).toContain('Custom Code Generation Prompt: "Generate connection pool"');
            expect(checklist).toContain('Runtime Variables (1):');
            expect(checklist).toContain('JOB_ID: "sample-job"');
            expect(checklist).toContain('Job Configuration:');
            expect(checklist).toContain('runner: "ServerlessSpark"');
            expect(checklist).toContain('Edges / Connections (1):');
            expect(checklist).toContain('Source "n1" -> Target "n2" [handle: valid]');
        });

        it('formats empty specification checklist cleanly (extreme condition)', () => {
            const emptySpec = {
                nodes: [],
                runtimeVariables: [],
                jobConfig: {},
                pipelineConfig: {},
                edges: []
            };

            const checklist = formatSpecChecklist(emptySpec);
            expect(checklist).toBe('Total Nodes: 0');
        });
    });

    describe('buildSpecExecutionPlan', () => {
        it('builds a plan with foreachBatch grouping, DLQ split, and connector JARs (normal condition)', () => {
            const plan = mustPlan(STREAMING_DLQ_YAML);
            expect(plan.sortedNodeIds[0]).toBe('src_kafka');
            expect(plan.streamingSinkGroups[0].mode).toBe('foreachBatch');
            expect(plan.dlqSplitPoints[0].nodeId).toBe('parse');
            expect(plan.runtimeArtifacts.profile.runner).toBe('ServerlessSpark');
            expect(plan.runtimeArtifacts.gcsJars.some(j => j.includes('spanner'))).toBe(true);
        });

        it('returns null for empty, malformed, or node-less YAML (boundary and failure conditions)', () => {
            expect(buildSpecExecutionPlan('')).toBeNull();
            expect(buildSpecExecutionPlan('::: invalid [yaml :::')).toBeNull();
            expect(buildSpecExecutionPlan('nodes: []')).toBeNull();
            expect(buildSpecExecutionPlan('just text')).toBeNull();
        });

        it('ignores nodes without ids and edges that are not objects (boundary condition)', () => {
            const plan = mustPlan('nodes:\n  - data: { type: SOURCE }\n  - id: 7\n    data: { type: SOURCE, spark_transform: RateSource }\nedges:\n  - null\n');
            expect(plan.sortedNodeIds).toEqual(['7']);
        });
    });

    describe('generateRequirementMatrix with ExecutionPlan', () => {
        it('adds REQ-PLAN items for sink groups, DLQ splits, and connector JARs (normal condition)', () => {
            const spec = parsePipelineSpec(STREAMING_DLQ_YAML);
            const plan = buildSpecExecutionPlan(STREAMING_DLQ_YAML);
            const ids = generateRequirementMatrix(spec, plan).map(r => r.reqId);
            expect(ids).toContain('REQ-PLAN-SINKGROUP-foreach_batch_1');
            expect(ids).toContain('REQ-PLAN-DLQ-parse');
            expect(ids).toContain('REQ-PLAN-CONNECTOR-JARS');
            const group = generateRequirementMatrix(spec, plan).find(r => r.reqId === 'REQ-PLAN-SINKGROUP-foreach_batch_1');
            expect(group?.expected).toContain('txnVersion=batch_id');
            expect(group?.expected).toContain('batch_df.cache()/unpersist()');
        });

        it('adds REQ-PLAN-CACHE items for batch fan-out and lists them in the checklist', () => {
            const spec = parsePipelineSpec(BATCH_FANOUT_YAML);
            const plan = buildSpecExecutionPlan(BATCH_FANOUT_YAML);
            const checklist = formatSpecChecklist(spec, plan);
            expect(checklist).toContain('REQ-PLAN-CACHE-flt');
            expect(checklist).toContain('EXECUTION_PLAN');
        });

        it('adds no REQ-PLAN items without a plan (boundary condition)', () => {
            const spec = parsePipelineSpec(STREAMING_DLQ_YAML);
            expect(generateRequirementMatrix(spec).some(r => r.reqId.startsWith('REQ-PLAN'))).toBe(false);
            expect(generateRequirementMatrix(spec, null).some(r => r.reqId.startsWith('REQ-PLAN'))).toBe(false);
        });
    });

    describe('auditSparkTransformProperties', () => {
        it('accepts canonical Spark transforms that match their node types (normal condition)', () => {
            expect(auditSparkTransformProperties(parsePipelineSpec(STREAMING_DLQ_YAML))).toEqual([]);
        });

        it('classifies legacy Beam-era aliases', () => {
            expect(isLegacyBeamTransform('JdbcIO')).toBe(true);
            expect(isLegacyBeamTransform('GcsFileSystem')).toBe(true);
            expect(isLegacyBeamTransform('SOURCE:KafkaIO')).toBe(true);
            expect(isLegacyBeamTransform('ParDo')).toBe(true);
            expect(isLegacyBeamTransform('KafkaSource')).toBe(false);
            expect(isLegacyBeamTransform('WindowOver')).toBe(false);
        });

        it('flags missing, legacy, unknown, and mismatched transforms (failure condition)', () => {
            const spec = parsePipelineSpec(`
nodes:
  - id: a
    data: { label: NoTransform, type: SOURCE }
  - id: b
    data: { label: Legacy, type: SOURCE, spark_transform: JdbcIO }
  - id: c
    data: { label: Unknown, type: STEP, spark_transform: MagicStep }
  - id: d
    data: { label: Mismatch, type: SINK, spark_transform: KafkaSource }
  - id: e
    data: { label: StepAsSink, type: STEP, spark_transform: ConsoleSink }
  - id: f
    data: { label: Beam, type: STEP, spark_transform: ParDo }
`);
            const findings = auditSparkTransformProperties(spec);
            expect(findings.map(f => f.id)).toEqual([
                'SPEC-TRANSFORM-MISSING-a',
                'SPEC-TRANSFORM-LEGACY-b',
                'SPEC-TRANSFORM-UNKNOWN-c',
                'SPEC-TRANSFORM-TYPE-d',
                'SPEC-TRANSFORM-TYPE-e',
                'SPEC-TRANSFORM-LEGACY-f'
            ]);
            expect(findings[0].severity).toBe('CRITICAL');
            expect(findings.slice(1).every(f => f.severity === 'MAJOR')).toBe(true);
            expect(findings.every(f => f.category === 'SPECIFICATION')).toBe(true);
        });

        it('treats a whitespace-only transform as missing (boundary condition)', () => {
            const findings = auditSparkTransformProperties({
                nodes: [{ id: 'x', label: 'X', type: 'STEP', sparkTransform: '   ', properties: {} }],
                runtimeVariables: [], jobConfig: {}, pipelineConfig: {}, edges: []
            });
            expect(findings[0].id).toBe('SPEC-TRANSFORM-MISSING-x');
        });
    });

    describe('auditExecutionPlanCompliance', () => {
        it('accepts a pipeline that follows the foreachBatch, txn, checkpoint, and DLQ plan (normal condition)', () => {
            const plan = mustPlan(STREAMING_DLQ_YAML);
            const findings = auditExecutionPlanCompliance(plan, [{ filename: 'src/pipeline.py', content: COMPLIANT_STREAMING_PIPELINE }]);
            expect(findings).toEqual([]);
        });

        it('returns no findings when src/pipeline.py is absent (boundary condition)', () => {
            const plan = mustPlan(STREAMING_DLQ_YAML);
            expect(auditExecutionPlanCompliance(plan, [{ filename: 'README.md', content: '' }])).toEqual([]);
        });

        it('flags missing foreachBatch, txn options, batch cache, checkpoint, and DLQ branches (failure condition)', () => {
            const plan = mustPlan(STREAMING_DLQ_YAML);
            const code = 'def transform(dfs, config):\n    dfs["parse"] = dfs["src_kafka"]\n    return dfs\n\ndef write_sinks(dfs, config):\n    dfs["parse"].writeStream.format("console").start()\n';
            const ids = auditExecutionPlanCompliance(plan, [{ filename: 'src/pipeline.py', content: code }]).map(f => f.id);
            expect(ids).toEqual([
                'PLAN-FOREACHBATCH-foreach_batch_1',
                'PLAN-TXN-foreach_batch_1',
                'PLAN-BATCH-CACHE-foreach_batch_1',
                'PLAN-CHECKPOINT-MISSING',
                'PLAN-DLQ-parse',
                'PLAN-DLQ-CORRUPT-parse'
            ]);
        });

        it('flags dfs keys that never mention a SOURCE or STEP node id (failure condition)', () => {
            const plan = mustPlan(BATCH_FANOUT_YAML);
            const code = 'dfs["src"] = x\nout = dfs["src"].cache()\nout.unpersist()\n';
            const ids = auditExecutionPlanCompliance(plan, [{ filename: 'src/pipeline.py', content: code }]).map(f => f.id);
            expect(ids).toEqual(['PLAN-NODE-KEY-flt']);
        });

        it('flags missing cache and unpersist calls for batch fan-out (failure condition)', () => {
            const plan = mustPlan(BATCH_FANOUT_YAML);
            const code = "dfs['src'] = a\ndfs['flt'] = dfs['src'].filter('x > 1')\n";
            const findings = auditExecutionPlanCompliance(plan, [{ filename: 'src/pipeline.py', content: code }]);
            expect(findings.map(f => f.id)).toEqual(['PLAN-CACHE-MISSING', 'PLAN-UNPERSIST-MISSING']);
            expect(findings[0].description).toContain("'flt' (batch_fanout)");
        });

        it('accepts .persist() as a cache call and flags a missing native writeStream', () => {
            const batchPlan = mustPlan(BATCH_FANOUT_YAML);
            const persisted = 'dfs["src"] = a\ndfs["flt"] = dfs["src"].persist(StorageLevel.MEMORY_AND_DISK)\ndfs["flt"].unpersist()\n';
            expect(auditExecutionPlanCompliance(batchPlan, [{ filename: 'src/pipeline.py', content: persisted }])).toEqual([]);

            const nativePlan = mustPlan(`
nodes:
  - id: r
    data: { type: SOURCE, spark_transform: RateSource }
  - id: k
    data: { type: SINK, spark_transform: KafkaSink }
edges:
  - { source: r, target: k }
`);
            expect(nativePlan.streamingSinkGroups[0].mode).toBe('native');
            const ids = auditExecutionPlanCompliance(nativePlan, [{ filename: 'src/pipeline.py', content: 'dfs["r"] = x\n' }]).map(f => f.id);
            expect(ids).toEqual([`PLAN-WRITESTREAM-${nativePlan.streamingSinkGroups[0].id}`, 'PLAN-CHECKPOINT-MISSING']);
        });
    });

    describe('evaluatePipelineSpecification', () => {
        /**
         * Builds a mocked GoogleGenAI client that returns one JSON payload.
         *
         * @param payload - Model response object or raw text.
         * @returns Mock client.
         */
        const mockClient = (payload: unknown) => {
            const generateContent = vi.fn().mockResolvedValue({
                text: typeof payload === 'string' ? payload : JSON.stringify(payload)
            });
            const client = { models: { generateContent } };
            return client as typeof client & Parameters<typeof evaluatePipelineSpecification>[0];
        };

        it('passes when the model and the deterministic audits find no defects (normal condition)', async () => {
            const mockAi = mockClient({ passed: true, summary: 'All properties implemented.', findings: [], requirementVerifications: [] });
            const files = [{ filename: 'src/pipeline.py', content: COMPLIANT_STREAMING_PIPELINE }];

            const result = await evaluatePipelineSpecification(mockAi, 'Python', STREAMING_DLQ_YAML, files);

            expect(result.agentName).toBe('PipelineSpecificationAgent');
            expect(result.passed).toBe(true);
            expect(result.findings).toEqual([]);
            expect(result.summary).toBe('All properties implemented.');
            expect(mockAi.models.generateContent).toHaveBeenCalledTimes(1);
            const request = mockAi.models.generateContent.mock.calls[0][0];
            expect(request.contents).toContain('PySpark 4.x');
            expect(request.contents).toContain('REQ-PLAN-SINKGROUP-foreach_batch_1');
            expect(request.contents).not.toMatch(/Apache Beam/);
        });

        it('fails and merges deterministic findings even when the model passes (failure condition)', async () => {
            const mockAi = mockClient({ passed: true, summary: 'Looks fine.', findings: [] });
            const files = [{ filename: 'src/pipeline.py', content: 'def transform(dfs, config):\n    return dfs\n' }];

            const result = await evaluatePipelineSpecification(mockAi, 'Python', STREAMING_DLQ_YAML, files);

            expect(result.passed).toBe(false);
            expect(result.findings.map(f => f.id)).toContain('PLAN-FOREACHBATCH-foreach_batch_1');
            expect(result.summary).toMatch(/Deterministic audit: \d+ finding\(s\)\./);
            const request = mockAi.models.generateContent.mock.calls[0][0];
            expect(request.contents).toContain('[MAJOR] PLAN-FOREACHBATCH-foreach_batch_1');
        });

        it('fails on model findings when the deterministic audits are clean (boundary condition)', async () => {
            const mockAi = mockClient({
                passed: false,
                summary: 'Configured table property was ignored.',
                findings: [{
                    id: 'SPEC-1',
                    category: 'SPECIFICATION',
                    severity: 'CRITICAL',
                    file: 'src/pipeline.py',
                    description: 'Node BQ configured table "ds.events", but code writes to a default table',
                    rationale: 'Data lands in the wrong table'
                }]
            });
            const files = [{ filename: 'src/pipeline.py', content: COMPLIANT_STREAMING_PIPELINE }];

            const result = await evaluatePipelineSpecification(mockAi, 'Python', STREAMING_DLQ_YAML, files);

            expect(result.passed).toBe(false);
            expect(result.findings).toHaveLength(1);
            expect(result.findings[0].severity).toBe('CRITICAL');
            expect(result.requirementVerifications).toEqual([]);
        });

        it('flags legacy Beam transforms from the YAML without generated files (failure condition)', async () => {
            const mockAi = mockClient({ passed: true, summary: 'ok', findings: [] });
            const yamlContent = 'nodes:\n  - id: "1"\n    data:\n      label: JDBC\n      type: SOURCE\n      spark_transform: JdbcIO\n';
            const result = await evaluatePipelineSpecification(mockAi, 'Python', yamlContent, []);
            expect(result.passed).toBe(false);
            expect(result.findings[0].id).toBe('SPEC-TRANSFORM-LEGACY-1');
        });

        it('throws when the model returns empty text (failure condition)', async () => {
            const mockAi = mockClient('');
            await expect(
                evaluatePipelineSpecification(mockAi, 'Python', 'nodes: []', [])
            ).rejects.toThrow('Empty response from PipelineSpecificationAgent');
        });
    });
});
