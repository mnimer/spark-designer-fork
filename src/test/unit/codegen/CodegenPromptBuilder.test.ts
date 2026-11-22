/**
 * Unit tests for the code generation prompt builder (`src/codegen/CodegenPromptBuilder.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Uses an in-memory fixture catalog. Covers the section order (`codegen-common`,
 * `## Pipeline YAML`, `## Execution Plan`, redaction notice, skills with `verification-gate`
 * last), the YAML fence, `ExecutionPlan` serialization (`sortedNodeIds`, `isStreaming`,
 * `cacheNodes`, `streamingSinkGroups`, `dlqRoutes`, `runtimeJars`, `runtimePackages`,
 * `sparkProperties`), `{{spark_version}}` and `{{beam_version}}` handling in skill text only
 * (fix F7 regression), the guarantee that the user YAML is never changed (including `$&`
 * sequences), language specifics, template inlining, the fallback Spark version, and the
 * missing-common failure.
 */

import * as path from 'path';
import { describe, it, expect, vi } from 'vitest';

vi.mock('vscode', async () =>
    (await import('../helpers/vscodeMock')).createVscodeMock({
        workspace: { workspaceFolders: [] },
    })
);

import { handleRegenerateCode, PYTHON_STRATEGY } from '../../../codeGeneration';
import YAML from 'yaml';
import { CodegenSkillCatalog, buildSkill, clearCodegenSkillCatalogCache } from '../../../codegen/CodegenSkillCatalog';
import {
    CodegenPromptBuilder,
    buildPromptFromPipelineYaml,
    fence,
    renderExecutionPlan,
    resolveBeamVersion,
    resolveSparkVersion,
} from '../../../codegen/CodegenPromptBuilder';
import { DEFAULT_SPARK_VERSION } from '../../../shared/types';
import { REDACTION_PLACEHOLDER } from '../../../credentialRedaction';

/**
 * Builds a fixture skill.
 *
 * @param name Skill name.
 * @param kind Skill kind.
 * @param body Body text.
 * @param meta Extra metadata lines.
 * @param languageFiles Language reference files.
 * @param templates Runner templates.
 * @returns The skill.
 */
function skill(
    name: string,
    kind: string,
    body: string,
    meta = '',
    languageFiles: Record<string, string> = {},
    templates: Record<string, Array<{ fileName: string; content: string }>> = {}
) {
    const content = [
        '---',
        `name: ${name}`,
        `description: Fixture ${name}.`,
        'metadata:',
        `  kind: ${kind}`,
        '  usedBy: [generator, reviewer]',
        meta.includes('languages:') ? '' : '  languages: [all]',
        meta.includes('runners:') ? '' : '  runners: [all]',
        meta,
        '---',
        body,
    ]
        .filter((line) => line !== '')
        .join('\n');
    return buildSkill(content, name, { languageFiles, templates });
}

/** Fixture catalog that mirrors the real Spark skill kinds. */
const CATALOG = CodegenSkillCatalog.fromSkills([
    skill('codegen-common', 'common', 'ROLE. Use Spark {{spark_version}}.', '', { python: 'common py' }),
    skill('core-pyspark', 'language', 'CORE PYSPARK', '  languages: [python]'),
    skill('runner-serverless-spark', 'runner', 'Produce Dockerfile.', '  runners: [serverless-spark]', {}, {
        python: [{ fileName: 'Dockerfile', content: 'FROM spark:{{spark_version}}\n' }],
    }),
    skill('io-kafka', 'io', 'KAFKA RULES', '  transforms: [KafkaSource]', { python: 'kafka py' }),
    skill('core-streaming', 'crosscutting', 'STREAMING RULES', '  preloadWhen: [streaming]'),
    skill('core-python-datasource', 'crosscutting', 'PYTHON DS RULES', '  preloadWhen: [python_datasource]'),
    skill('verification-gate', 'gate', 'CHECKLIST'),
]);

/** Pipeline YAML with a version-like placeholder and `$` sequences inside a property. */
const YAML_TEXT = [
    'nodes:',
    '  - id: k',
    '    data:',
    '      type: SOURCE',
    '      spark_transform: KafkaSource',
    '      properties:',
    '        note: "keep {{spark_version}} and {{beam_version}} and 4.0 and $& and $\' literal"',
    'edges: []',
    '',
].join('\n');

/**
 * Builds a prompt with defaults.
 *
 * @param overrides Input overrides.
 * @returns Built prompt.
 */
function build(overrides: Partial<Parameters<CodegenPromptBuilder['build']>[0]> = {}) {
    return new CodegenPromptBuilder(CATALOG).build({
        parsedPipeline: YAML.parse(YAML_TEXT),
        redactedYaml: YAML_TEXT,
        redactedPaths: [],
        language: 'PySpark',
        sparkVersion: '4.2.0',
        ...overrides,
    });
}

describe('CodegenPromptBuilder.build', () => {
    it('emits common, YAML, Execution Plan, then skills in kind order with the gate last (simple)', () => {
        const { prompt, selectedSkills, executionPlan } = build();
        expect(selectedSkills).toEqual([
            'codegen-common',
            'core-pyspark',
            'runner-serverless-spark',
            'io-kafka',
            'core-streaming',
            'verification-gate',
        ]);
        expect(executionPlan.pipelineMode).toBe('streaming');
        expect(executionPlan.sortedNodeIds).toEqual(['k']);

        const markers = [
            'ROLE.',
            '## Pipeline YAML',
            '## Execution Plan',
            '## Skill: core-pyspark',
            '## Skill: runner-serverless-spark',
            '## Skill: io-kafka',
            '## Skill: core-streaming',
            '## Skill: verification-gate',
        ];
        const positions = markers.map((m) => prompt.indexOf(m));
        expect(positions.every((p) => p >= 0)).toBe(true);
        expect([...positions].sort((a, b) => a - b)).toEqual(positions);
        expect(prompt.startsWith('ROLE.')).toBe(true);
        expect(prompt.trimEnd().endsWith('CHECKLIST')).toBe(true);
        // codegen-common has no "## Skill:" heading.
        expect(prompt).not.toContain('## Skill: codegen-common');
    });

    it('injects deterministic ExecutionPlan summary and JSON for caching, foreachBatch, DLQ splits, and runtime artifacts (G2)', () => {
        const complexPipeline = YAML.parse(`
nodes:
  - id: src_bq
    data:
      type: SOURCE
      spark_transform: BigQuerySource
      properties:
        table: proj.ds.events
  - id: step_parse
    data:
      type: STEP
      spark_transform: ParsePayload
      properties:
        format: json
  - id: sink_iceberg
    data:
      type: SINK
      spark_transform: IcebergSink
  - id: sink_spanner
    data:
      type: SINK
      spark_transform: SpannerSink
edges:
  - id: e1
    source: src_bq
    target: step_parse
  - id: e2
    source: step_parse
    target: sink_iceberg
    sourceHandle: valid
  - id: e3
    source: step_parse
    target: sink_spanner
    sourceHandle: error
jobConfig:
  runner: ServerlessSpark
  runtimeVersion: "4.0"
`);
        const result = build({ parsedPipeline: complexPipeline });
        expect(result.executionPlan.pipelineMode).toBe('batch');
        expect(result.executionPlan.cachedNodeIds).toContain('step_parse');
        expect(result.executionPlan.dlqSplitPoints).toHaveLength(1);
        expect(result.executionPlan.dlqSplitPoints[0].mechanism).toBe('_corrupt_record');
        expect(result.executionPlan.runtimeArtifacts.gcsJars).toContain(
            'gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar'
        );

        expect(result.prompt).toContain('## Execution Plan');
        expect(result.prompt).toContain('`src_bq` -> `step_parse` -> `sink_iceberg` -> `sink_spanner`');
        expect(result.prompt).toContain('`step_parse` (reason: `dlq_split`');
        expect(result.prompt).toContain('_corrupt_record');
        expect(result.prompt).toContain('gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar');
        expect(result.prompt).toContain('"isStreaming": false');
        expect(result.prompt).toContain('"cacheNodes": [');
        expect(result.prompt).toContain('"dlqRoutes": [');
        expect(result.prompt).toContain('"runtimeJars": [');
    });

    it('renders foreachBatch streamingSinkGroups in Execution Plan when streaming sinks fan out (G2)', () => {
        const streamingFanout = YAML.parse(`
nodes:
  - id: src_kafka
    data:
      type: SOURCE
      spark_transform: KafkaSource
  - id: sink_delta
    data:
      type: SINK
      spark_transform: DeltaSink
  - id: sink_sql
    data:
      type: SINK
      spark_transform: CloudSqlSink
      properties:
        connectionMode: socket_factory
        authType: iam
edges:
  - { id: e1, source: src_kafka, target: sink_delta }
  - { id: e2, source: src_kafka, target: sink_sql }
jobConfig:
  jobName: orders-stream
  checkpointRoot: gs://my-bucket/checkpoints
`);
        const result = build({ parsedPipeline: streamingFanout });
        expect(result.executionPlan.pipelineMode).toBe('streaming');
        expect(result.executionPlan.streamingSinkGroups).toHaveLength(1);
        expect(result.executionPlan.streamingSinkGroups[0].mode).toBe('foreachBatch');
        expect(result.executionPlan.streamingSinkGroups[0].requiresBatchCache).toBe(true);

        const renderedPlan = renderExecutionPlan(result.executionPlan);
        expect(renderedPlan).toContain('`foreach_batch_1`');
        expect(renderedPlan).toContain('requiresBatchCache: true');
        expect(renderedPlan).toContain('orders-stream_src_kafka');
    });

    it('fences the YAML and inserts it byte-for-byte, never substituting inside it (F7 regression)', () => {
        const { prompt } = build();
        expect(prompt).toContain('```yaml\n' + YAML_TEXT + '```');
        expect(prompt).toContain("keep {{spark_version}} and {{beam_version}} and 4.0 and $& and $' literal");
    });

    it('resolves {{spark_version}} in skill text and templates (simple)', () => {
        const { prompt } = build();
        expect(prompt).toContain('Use Spark 4.2.0.');
        expect(prompt).toContain('#### Template: Dockerfile\n```\nFROM spark:4.2.0\n```');
        // The only remaining placeholder is the one inside the user YAML.
        expect(prompt.match(/\{\{spark_version\}\}/g)).toHaveLength(1);
    });

    it('falls back to DEFAULT_SPARK_VERSION when no version is given (extreme)', () => {
        const { prompt } = build({ sparkVersion: undefined, beamVersion: undefined });
        expect(prompt).toContain(`Use Spark ${DEFAULT_SPARK_VERSION}.`);
    });

    it('emits only the pipeline language specifics (simple)', () => {
        const python = build().prompt;
        expect(python).toContain('### Python specifics\n\ncommon py');
        expect(python).toContain('### Python specifics\n\nkafka py');
    });

    it('adds the redaction notice after Execution Plan and before skills only when values were redacted (simple)', () => {
        expect(build().prompt).not.toContain('## Redacted Credentials');
        const { prompt } = build({ redactedPaths: ['nodes[0].data.properties.password'] });
        const planHeading = prompt.indexOf('## Execution Plan');
        const notice = prompt.indexOf('## Redacted Credentials');
        expect(notice).toBeGreaterThan(planHeading);
        expect(notice).toBeLessThan(prompt.indexOf('## Skill: core-pyspark'));
        expect(prompt).toContain(REDACTION_PLACEHOLDER);
        expect(prompt).toContain('nodes[0].data.properties.password');
    });

    it('selects feature-gated skills from the parsed pipeline (simple)', () => {
        const parsed = {
            nodes: [{ id: 'p', data: { type: 'SOURCE', spark_transform: 'PubSubSource' } }],
            edges: [],
        };
        const result = build({ parsedPipeline: parsed });
        expect(result.selectedSkills).toContain('core-python-datasource');
        expect(result.selectedSkills).toContain('core-streaming');
        expect(result.selectedSkills).not.toContain('io-kafka');
        expect(result.detection.features.has('python_datasource')).toBe(true);
    });

    it('throws when the catalog has no codegen-common skill (failure)', () => {
        const catalog = CodegenSkillCatalog.fromSkills([skill('verification-gate', 'gate', 'X')]);
        expect(() =>
            new CodegenPromptBuilder(catalog).build({
                parsedPipeline: {},
                redactedYaml: '',
                redactedPaths: [],
                language: 'PySpark',
            })
        ).toThrow(/Skill 'codegen-common' is missing for PySpark/);
    });

    it('renders a skill without a body (extreme)', () => {
        const catalog = CodegenSkillCatalog.fromSkills([
            skill('codegen-common', 'common', ''),
            skill('verification-gate', 'gate', ''),
        ]);
        const { prompt } = new CodegenPromptBuilder(catalog).build({
            parsedPipeline: {},
            redactedYaml: 'a: 1',
            redactedPaths: [],
            language: 'PySpark',
        });
        expect(prompt.startsWith('## Pipeline YAML')).toBe(true);
        expect(prompt.endsWith('## Skill: verification-gate')).toBe(true);
    });

    it('builds end-to-end prompt from bundled Phase 2 skills with ExecutionPlan injection (G1-G10)', () => {
        const bundledSkillsRoot = path.join(__dirname, '..', '..', '..', '..', 'resources', 'codegen-skills');
        clearCodegenSkillCatalogCache();
        const yamlText = [
            'nodes:',
            '  - id: src_pubsub',
            '    data:',
            '      type: SOURCE',
            '      spark_transform: PubSubSource',
            '      properties:',
            '        subscription: projects/my-proj/subscriptions/events-sub',
            '        readMode: streaming',
            '  - id: step_wm',
            '    data:',
            '      type: STEP',
            '      spark_transform: Watermark',
            '      properties:',
            '        eventTimeColumn: publish_time',
            '        delayThreshold: 10 minutes',
            '  - id: sink_iceberg',
            '    data:',
            '      type: SINK',
            '      spark_transform: IcebergSink',
            '      properties:',
            '        table: biglake.sales.events',
            'edges:',
            '  - { id: e1, source: src_pubsub, target: step_wm }',
            '  - { id: e2, source: step_wm, target: sink_iceberg }',
            'jobConfig:',
            '  runner: ServerlessSpark',
            '  runtimeVersion: "4.0"',
            '  checkpointRoot: gs://my-bucket/checkpoints',
            '',
        ].join('\n');

        const built = buildPromptFromPipelineYaml({
            skillsRoot: bundledSkillsRoot,
            yamlText,
            language: 'PySpark',
            sparkVersion: '4.2.0',
        });

        expect(built.selectedSkills).toEqual([
            'codegen-common',
            'core-pyspark',
            'runner-dataproc-cluster',
            'runner-local',
            'runner-serverless-spark',
            'io-iceberg',
            'io-pubsub',
            'transform-watermark',
            'core-catalyst-optimization',
            'core-python-datasource',
            'core-streaming',
            'verification-gate',
        ]);
        expect(built.prompt).toContain('## Pipeline YAML');
        expect(built.prompt).toContain('## Execution Plan');
        expect(built.prompt).toContain('## Skill: core-pyspark');
        expect(built.prompt).toContain('## Skill: transform-watermark');
        expect(built.prompt).toContain('## Skill: core-catalyst-optimization');
        expect(built.prompt).toContain('## Skill: core-streaming');
        expect(built.prompt).toContain('## Skill: core-python-datasource');
        expect(built.prompt).toContain('## Skill: runner-local');
        expect(built.prompt).toContain('## Skill: runner-dataproc-cluster');
        expect(built.prompt).toContain('## Skill: runner-serverless-spark');
        expect(built.prompt).toContain('## Skill: verification-gate');
        expect(built.prompt).toContain('#### Template: run_local.sh');
        expect(built.prompt).toContain('#### Template: run_dataproc_cluster.sh');
        expect(built.prompt).toContain('#### Template: run_serverless_spark.sh');
        expect(built.prompt).toContain('#### Template: Dockerfile');
    });

    it('builds SDP prompt with ## SDP Execution Plan, quarantine datasets, Auto CDC SCD1, and SDP skills (SDP7)', () => {
        const bundledSkillsRoot = path.join(__dirname, '..', '..', '..', '..', 'resources', 'codegen-skills');
        clearCodegenSkillCatalogCache();
        const sdpYaml = [
            'paradigm: sdp',
            'nodes:',
            '  - id: src_cdc',
            '    data:',
            '      type: SOURCE',
            '      label: Customer CDC JSON',
            '      spark_transform: JsonSource',
            '      properties:',
            '        readMode: streaming',
            '        path: gs://raw-bucket/customers/',
            '  - id: step_dq',
            '    data:',
            '      type: STEP',
            '      label: Validate Customers',
            '      spark_transform: DataQualityAssert',
            '      properties:',
            '        failurePolicy: route_to_dlq',
            '        expectations:',
            '          - { name: id_present, column: customer_id, checkType: not_null }',
            '  - id: step_cdc',
            '    data:',
            '      type: STEP',
            '      label: Customer Upsert',
            '      spark_transform: MergeCdcScd',
            '      properties:',
            '        scdType: scd1',
            '        keyColumns: [customer_id]',
            '        sequenceColumn: updated_at',
            '  - id: sink_silver',
            '    data:',
            '      type: SINK',
            '      label: Silver Customers',
            '      spark_transform: IcebergSink',
            '      properties:',
            '        sdpTableName: silver_customers',
            '        sdpFormat: iceberg',
            'edges:',
            '  - { id: e1, source: src_cdc, target: step_dq }',
            '  - { id: e2, source: step_dq, target: step_cdc, sourceHandle: valid, targetHandle: left }',
            '  - { id: e3, source: step_cdc, target: sink_silver }',
            'jobConfig:',
            '  runner: ServerlessSpark',
            '  runtimeVersion: "4.0"',
            '  projectId: retail-prod',
            '  sdpStorage: gs://retail-sdp/state/customers',
            '  sdpCatalog: lake',
            '  sdpDatabase: crm',
            '  sdpBigLakeMode: lakehouse',
            '  sdpBigLakeProject: retail-prod',
            '  sdpBigLakeCatalogId: crm_catalog',
            '',
        ].join('\n');

        const built = buildPromptFromPipelineYaml({
            skillsRoot: bundledSkillsRoot,
            yamlText: sdpYaml,
            language: 'PySpark',
        });

        expect(built.detection.paradigm).toBe('sdp');
        expect(built.sdpPlan).toBeDefined();
        expect(built.sdpPlan?.isCompatible).toBe(true);
        expect(built.selectedSkills).toContain('sdp-core');
        expect(built.selectedSkills).toContain('sdp-spec');
        expect(built.selectedSkills).toContain('sdp-auto-cdc');
        expect(built.selectedSkills).toContain('sdp-quality-quarantine');
        expect(built.selectedSkills).not.toContain('core-streaming');
        expect(built.selectedSkills).not.toContain('runner-local');
        expect(built.selectedSkills).not.toContain('runner-serverless-spark');

        expect(built.prompt).toContain('## SDP Execution Plan');
        expect(built.prompt).not.toContain('## Execution Plan\n');
        expect(built.prompt).toContain('storage: gs://retail-sdp/state/customers');
        expect(built.prompt).toContain('bl://projects/retail-prod/catalogs/crm_catalog');
        expect(built.prompt).toContain('_quarantine');
        expect(built.prompt).toContain('dp.create_auto_cdc_flow');
        expect(built.prompt).toContain('stored_as_scd_type=1');
        expect(built.prompt).toContain('src/transforms.py');
        expect(built.prompt).toContain('tests/test_transforms.py');
    });

    it('preserves SDP file paths in PYTHON_STRATEGY and blocks incompatible SDP pipelines before calling Gemini (SDP7)', async () => {
        expect(
            PYTHON_STRATEGY.routeFilename(
                'transformations/pipeline.py',
                'pipeline.py',
                'transformations',
                'retail_sdp'
            )
        ).toBe('transformations/pipeline.py');
        expect(
            PYTHON_STRATEGY.routeFilename('spark-pipeline.yml', 'spark-pipeline.yml', '.', 'retail_sdp')
        ).toBe('spark-pipeline.yml');
        expect(
            PYTHON_STRATEGY.routeFilename('sdp_stage.pipeline.yaml', 'sdp_stage.pipeline.yaml', '.', 'retail_sdp')
        ).toBe('sdp_stage.pipeline.yaml');
        expect(PYTHON_STRATEGY.routeFilename('transforms.py', 'transforms.py', '.', 'retail_sdp')).toBe(
            'src/transforms.py'
        );
        expect(
            PYTHON_STRATEGY.routeFilename('test_transforms.py', 'test_transforms.py', '.', 'retail_sdp')
        ).toBe('tests/test_transforms.py');
        expect(
            PYTHON_STRATEGY.isMainPipelineFile('transformations/pipeline.py', 'retail_sdp', 'existing_transforms')
        ).toBe(true);
        expect(
            PYTHON_STRATEGY.isMainPipelineFile('src/transforms.py', 'retail_sdp', 'from pyspark import pipelines as dp')
        ).toBe(false);

        const postToWebview = vi.fn().mockResolvedValue(true);
        const incompatibleSdpYaml = [
            'paradigm: sdp',
            'nodes:',
            '  - id: s1',
            '    data: { type: SOURCE, label: Kafka, spark_transform: KafkaSource }',
            '  - id: k1',
            '    data: { type: SINK, label: Spanner, spark_transform: SpannerSink }',
            'edges:',
            '  - { id: e1, source: s1, target: k1 }',
            'jobConfig:',
            '  runner: ServerlessSpark',
            '  runtimeVersion: "4.0"',
            '',
        ].join('\n');

        await handleRegenerateCode({
            message: {
                data: incompatibleSdpYaml,
                filename: 'bad_sdp.spark',
                language: 'PySpark',
                outputFolder: 'out',
            },
            context: {} as never,
            uri: { fsPath: '/tmp/bad_sdp.spark' } as never,
            postToWebview,
            outputChannel: { appendLine: vi.fn() } as never,
        });

        expect(postToWebview).toHaveBeenCalledWith(
            expect.objectContaining({
                command: 'codeRegenerated',
                success: false,
                error: expect.stringContaining('Spanner'),
            })
        );
    });

    it('normalizes default Local and DataprocCluster SDP jobConfig to Spark 4.2 profiles while blocking explicit pre-4.2 versions', async () => {
        const bundledSkillsRoot = path.join(__dirname, '..', '..', '..', '..', 'resources', 'codegen-skills');
        clearCodegenSkillCatalogCache();

        const makeSdpYaml = (jobConfigBlock: string) =>
            [
                'paradigm: sdp',
                'nodes:',
                '  - id: s1',
                '    data: { type: SOURCE, label: Orders CSV, spark_transform: CsvSource, properties: { readMode: streaming } }',
                '  - id: k1',
                '    data: { type: SINK, label: Bronze Parquet, spark_transform: ParquetSink }',
                'edges:',
                '  - { id: e1, source: s1, target: k1 }',
                jobConfigBlock,
                '',
            ].join('\n');

        // 1. Local without runtimeVersion normalizes to local_4.2_connect and omits Iceberg defaults for non-Iceberg graphs
        const localUnset = buildPromptFromPipelineYaml({
            skillsRoot: bundledSkillsRoot,
            yamlText: makeSdpYaml('jobConfig:\n  runner: Local'),
            language: 'PySpark',
            sparkVersion: '4.0',
        });
        expect(localUnset.sdpPlan?.isCompatible).toBe(true);
        expect(localUnset.sdpPlan?.blockingIssues).toEqual([]);
        expect(localUnset.sdpPlan?.runtimeArtifacts.profile.id).toBe('local_4.2_connect');
        expect(localUnset.prompt).toContain('Profile: `local_4.2_connect`');
        expect(localUnset.prompt).toContain('pyspark[connect]==4.2.*');
        expect(localUnset.prompt).toContain('pyyaml>=6.0');
        expect(localUnset.prompt).not.toContain('pyspark[connect]==4.0.*');
        expect(localUnset.prompt).toContain('"usesIceberg": false');
        expect(localUnset.prompt).toContain('"bigLakeMode": "none"');
        expect(localUnset.prompt).toContain('"catalog": "spark_catalog"');
        expect(localUnset.prompt).toContain('"staticRunnerProperties": {}');

        // 1b. Iceberg SDP pipeline includes IcebergSparkSessionExtensions and defaults bigLakeMode to lakehouse
        const icebergLocal = buildPromptFromPipelineYaml({
            skillsRoot: bundledSkillsRoot,
            yamlText: [
                'paradigm: sdp',
                'nodes:',
                '  - id: s1',
                '    data: { type: SOURCE, label: Orders Iceberg, spark_transform: IcebergSource }',
                '  - id: k1',
                '    data: { type: SINK, label: Silver Iceberg, spark_transform: IcebergSink }',
                'edges:',
                '  - { id: e1, source: s1, target: k1 }',
                'jobConfig:',
                '  runner: UnknownLegacyRunner',
                '',
            ].join('\n'),
            language: 'PySpark',
            sparkVersion: '4.0',
        });
        expect(icebergLocal.sdpPlan?.isCompatible).toBe(true);
        expect(icebergLocal.sdpPlan?.runtimeArtifacts.profile.id).toBe('local_4.2_connect');
        expect(icebergLocal.sdpPlan?.runtimeArtifacts.profile.runner).toBe('Local');
        expect(icebergLocal.prompt).toContain('"usesIceberg": true');
        expect(icebergLocal.prompt).toContain('"bigLakeMode": "lakehouse"');
        expect(icebergLocal.prompt).toContain('"catalog": "lake"');
        expect(icebergLocal.prompt).toContain(
            '"spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"'
        );
        expect(icebergLocal.prompt).toContain('"pyspark[connect]==4.2.*"');
        expect(icebergLocal.prompt).toContain('"pyyaml>=6.0"');
        expect(icebergLocal.prompt).toContain('"zstandard>=0.25.0"');
        expect(icebergLocal.prompt).toContain('"pytest>=8.0.0"');

        // 2. Local with default webview placeholder runtimeVersion: "4.0" normalizes to local_4.2_connect
        const localPlaceholder = buildPromptFromPipelineYaml({
            skillsRoot: bundledSkillsRoot,
            yamlText: makeSdpYaml('jobConfig:\n  runner: Local\n  runtimeVersion: "4.0"'),
            language: 'PySpark',
        });
        expect(localPlaceholder.sdpPlan?.isCompatible).toBe(true);
        expect(localPlaceholder.sdpPlan?.blockingIssues).toEqual([]);
        expect(localPlaceholder.sdpPlan?.runtimeArtifacts.profile.id).toBe('local_4.2_connect');

        // 3. DataprocCluster without runtimeVersion normalizes to cluster_4.0
        const clusterUnset = buildPromptFromPipelineYaml({
            skillsRoot: bundledSkillsRoot,
            yamlText: makeSdpYaml('jobConfig:\n  runner: DataprocCluster'),
            language: 'PySpark',
        });
        expect(clusterUnset.sdpPlan?.isCompatible).toBe(true);
        expect(clusterUnset.sdpPlan?.runtimeArtifacts.profile.id).toBe('cluster_4.0');

        // 4. Explicit pre-4.2 versions still fail with SDP_SPARK_42_REQUIRED
        for (const badBlock of [
            'jobConfig:\n  runner: ServerlessSpark\n  runtimeVersion: "3.0"',
            'jobConfig:\n  runner: DataprocCluster\n  runtimeVersion: "3.0"',
            'jobConfig:\n  runner: Local\n  runtimeVersion: "4.0.1"',
        ]) {
            const blocked = buildPromptFromPipelineYaml({
                skillsRoot: bundledSkillsRoot,
                yamlText: makeSdpYaml(badBlock),
                language: 'PySpark',
            });
            expect(blocked.sdpPlan?.isCompatible).toBe(false);
            expect(blocked.sdpPlan?.blockingIssues.map((i) => i.code)).toContain(
                'SDP_SPARK_42_REQUIRED'
            );
        }

        // 5. handleRegenerateCode pre-flight passes runner: Local without SDP_SPARK_42_REQUIRED while rejecting explicit 3.0
        const localPreflightPost = vi.fn().mockResolvedValue(true);
        await handleRegenerateCode({
            message: {
                data: makeSdpYaml(
                    'jobConfig:\n  runner: Local\n  sdpStorage: gs://my-sdp/checkpoints'
                ),
                filename: 'local_sdp.spark',
                language: 'PySpark',
                outputFolder: '',
            },
            context: {} as never,
            postToWebview: localPreflightPost,
            outputChannel: { appendLine: vi.fn() } as never,
        });
        expect(localPreflightPost).toHaveBeenCalledWith(
            expect.objectContaining({
                command: 'codeRegenerated',
                success: false,
                error: 'Could not determine output folder.',
            })
        );

        const explicitDowngradePost = vi.fn().mockResolvedValue(true);
        await handleRegenerateCode({
            message: {
                data: makeSdpYaml(
                    'jobConfig:\n  runner: ServerlessSpark\n  runtimeVersion: "3.0"\n  sdpStorage: gs://my-sdp/checkpoints'
                ),
                filename: 'downgraded_sdp.spark',
                language: 'PySpark',
                outputFolder: 'out',
            },
            context: {} as never,
            uri: { fsPath: '/tmp/downgraded_sdp.spark' } as never,
            postToWebview: explicitDowngradePost,
            outputChannel: { appendLine: vi.fn() } as never,
        });
        expect(explicitDowngradePost).toHaveBeenCalledWith(
            expect.objectContaining({
                command: 'codeRegenerated',
                success: false,
                error: expect.stringContaining('SDP_SPARK_42_REQUIRED'),
            })
        );
    });
});

describe('fence and resolveSparkVersion', () => {
    it('uses a longer fence when the text contains a fence (extreme)', () => {
        expect(fence('a\n```\nb', 'md')).toBe('````md\na\n```\nb\n````');
        expect(fence('plain\n')).toBe('```\nplain\n```');
    });

    it('treats $ sequences in the version literally (extreme)', () => {
        expect(resolveSparkVersion('v={{spark_version}} {{beam_version}}', "$&$'")).toBe("v=$&$' $&$'");
        expect(resolveBeamVersion('v={{beam_version}}', "$&$'")).toBe("v=$&$'");
    });
});
