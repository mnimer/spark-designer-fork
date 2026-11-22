/**
 * @fileoverview Unit tests verifying ARCHITECTURE.md completeness, contract sync, and style rules.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies that ARCHITECTURE.md documents every command in WEBVIEW_TO_HOST_COMMANDS,
 * every HostToWebviewMessage variant, every SparkRuntimeTarget profile and connector
 * mechanism in versionMatrix.ts, deterministic executionPlanner.ts and sdpPlanner.ts
 * contracts, dynamic Gemini model discovery, single Author credit, ASD-STE100 prose
 * limits, zero prose semicolons, and zero banned legacy tokens.
 */

import * as fs from 'fs';
import * as path from 'path';
import { describe, expect, it } from 'vitest';
import {
    WEBVIEW_TO_HOST_COMMANDS,
    type AssetResponseCommand,
    type HostToWebviewCommand,
} from '../../../shared/protocol';
import {
    DEFAULT_SERVERLESS_TIER,
    RUNTIME_PROFILES,
    type ConnectorDeliveryMechanism,
    type SparkRuntimeTarget,
} from '../../../spark/versionMatrix';

const REPO_ROOT = path.resolve(__dirname, '../../../..');
const ARCHITECTURE_DOC_PATH = path.join(REPO_ROOT, 'ARCHITECTURE.md');
const THIS_TEST_FILE_PATH = path.join(
    REPO_ROOT,
    'src/test/unit/docs/architecture-doc.test.ts'
);

/**
 * Strips fenced Markdown code blocks while preserving line numbers with empty lines.
 *
 * @param markdown Raw Markdown document content.
 * @returns Markdown text with fenced code block interiors blanked out.
 */
function stripFencedCodeBlocks(markdown: string): string {
    const lines = markdown.split(/\r?\n/);
    const output: string[] = [];
    let inFence = false;

    for (const line of lines) {
        if (line.trim().startsWith('```')) {
            inFence = !inFence;
            output.push('');
            continue;
        }
        output.push(inFence ? '' : line);
    }

    return output.join('\n');
}

/**
 * Splits a Markdown prose line into individual sentences without splitting inline code or version numbers.
 *
 * @param rawLine Trimmed prose or list-item text with list markers removed.
 * @returns Array of non-empty sentence strings.
 */
function splitMarkdownSentences(rawLine: string): string[] {
    const masked = rawLine
        .replace(/`[^`]*`/g, (span) => span.replace(/\./g, '__DOT__'))
        .replace(/\b\d+\.\d+(?:\.\d+)?|\b\d+\.x\b|\bvs\./g, (token) =>
            token.replace(/\./g, '__DOT__')
        );

    return masked
        .split(/\.(?:\s+|$)/)
        .map((part) => part.replace(/__DOT__/g, '.').trim())
        .filter((part) => part.length > 0);
}

/**
 * Counts whitespace-delimited words in a sentence.
 *
 * @param sentence Candidate sentence text.
 * @returns Word count for the sentence.
 */
function countWords(sentence: string): number {
    return sentence.trim().split(/\s+/).filter(Boolean).length;
}

/**
 * Extracts numbered procedural steps and descriptive prose sentences outside code fences and tables.
 *
 * @param markdown Raw Markdown document content.
 * @returns Extracted numbered-step sentences and descriptive prose sentences with line metadata.
 */
function extractStyleSentences(markdown: string): {
    numberedSteps: Array<{ line: number; sentence: string; words: number }>;
    descriptiveProse: Array<{ line: number; sentence: string; words: number }>;
} {
    const stripped = stripFencedCodeBlocks(markdown);
    const lines = stripped.split('\n');
    const numberedSteps: Array<{ line: number; sentence: string; words: number }> = [];
    const descriptiveProse: Array<{ line: number; sentence: string; words: number }> = [];

    lines.forEach((rawLine, index) => {
        const trimmed = rawLine.trim();
        if (
            !trimmed ||
            trimmed.startsWith('#') ||
            trimmed.startsWith('|') ||
            trimmed.startsWith('>') ||
            trimmed === '---'
        ) {
            return;
        }

        const lineNumber = index + 1;
        if (/^\d+\.\s+/.test(trimmed)) {
            const body = trimmed.replace(/^\d+\.\s+/, '');
            for (const sentence of splitMarkdownSentences(body)) {
                numberedSteps.push({
                    line: lineNumber,
                    sentence,
                    words: countWords(sentence),
                });
            }
            return;
        }

        const body = trimmed.replace(/^[-*]\s+/, '');
        for (const sentence of splitMarkdownSentences(body)) {
            descriptiveProse.push({
                line: lineNumber,
                sentence,
                words: countWords(sentence),
            });
        }
    });

    return { numberedSteps, descriptiveProse };
}

/**
 * Builds dynamic regular expressions for banned legacy tokens without embedding literal substrings.
 *
 * @returns Named regular expressions for forbidden tokens.
 */
function buildBannedTokenPatterns(): Array<{ label: string; pattern: RegExp }> {
    const legacyWord = ['be', 'am'].join('');
    const modelWord = ['gem', 'ini'].join('');

    return [
        { label: 'legacy pipeline extension', pattern: new RegExp(`\\.${legacyWord}\\b`, 'i') },
        { label: 'legacy manifest token', pattern: new RegExp(`\\.?${legacyWord}gen\\b`, 'i') },
        {
            label: 'legacy snake transform key',
            pattern: new RegExp(`${legacyWord}_transform\\b`, 'i'),
        },
        {
            label: 'legacy camel transform key',
            pattern: new RegExp(`${legacyWord}Transform\\b`),
        },
        { label: 'deprecated 2.5 model', pattern: new RegExp(`${modelWord}-2\\.5`, 'i') },
        { label: 'deprecated 1.x model prefix', pattern: new RegExp(`${modelWord}-1\\.`, 'i') },
        { label: 'deprecated 2.x model prefix', pattern: new RegExp(`${modelWord}-2\\.`, 'i') },
        { label: 'forbidden 3.8 model label', pattern: new RegExp(`${modelWord}[- ]3\\.8`, 'i') },
    ];
}

const ALL_ASSET_RESPONSE_COMMANDS: readonly AssetResponseCommand[] = [
    'datasetsLoaded',
    'tablesLoaded',
    'topicsLoaded',
    'subscriptionsLoaded',
    'networksLoaded',
    'subnetworksLoaded',
    'storageBucketsLoaded',
    'secretsLoaded',
    'healthcareDatasetsLoaded',
    'awsQueuesLoaded',
    'computeInstancesLoaded',
    'bigtableInstancesLoaded',
    'bigtableTablesLoaded',
    'sqlInstancesLoaded',
    'sqlDatabasesLoaded',
    'projectsLoaded',
    'dataprocClustersLoaded',
    'sessionTemplatesLoaded',
    'bigLakeCatalogsLoaded',
    'regionsLoaded',
    'codegenModelsLoaded',
];

const ALL_NON_ASSET_HOST_COMMANDS: readonly Exclude<
    HostToWebviewCommand,
    AssetResponseCommand
>[] = [
    'loadPipeline',
    'projectIdResolved',
    'samplesLoaded',
    'codeRegenerated',
    'codeGenerationStopped',
    'codeGenerationProgress',
    'pipelineLog',
    'pipelineStarted',
    'pipelineFinished',
    'pipelineStopped',
    'pipelineJobIdDetected',
    'runningJobsLoaded',
    'folderSelected',
    'folderFilesRead',
    'fileContentLoaded',
    'fileContentError',
];

const ALL_CONNECTOR_MECHANISMS: readonly ConnectorDeliveryMechanism[] = [
    'builtin',
    'preinstalled',
    'gcs_jar',
    'maven_package',
    'python_datasource',
    'foreach_batch_sdk',
];

describe('DOC1 — ARCHITECTURE.md specification & contract parity', () => {
    const architectureDoc = fs.readFileSync(ARCHITECTURE_DOC_PATH, 'utf-8');
    const proseOnlyDoc = stripFencedCodeBlocks(architectureDoc);

    describe('author credit, banned legacy tokens, and ASD-STE100 style rules', () => {
        it('contains exactly one header credit in ARCHITECTURE.md and this test file', () => {
            const authorPrefixPattern = new RegExp('Auth' + 'or:', 'g');
            const expectedCredit = 'Auth' + 'or: Mike Nimer <mikenimer@google.com>';

            const docAuthorMatches = architectureDoc.match(authorPrefixPattern) ?? [];
            expect(docAuthorMatches).toHaveLength(1);
            expect(architectureDoc).toContain(expectedCredit);

            const testSource = fs.readFileSync(THIS_TEST_FILE_PATH, 'utf-8');
            const testAuthorMatches = testSource.match(authorPrefixPattern) ?? [];
            expect(testAuthorMatches).toHaveLength(1);
            expect(testSource).toContain(expectedCredit);
        });

        it('contains zero banned legacy extensions, transform keys, or deprecated model tokens', () => {
            const testSource = fs.readFileSync(THIS_TEST_FILE_PATH, 'utf-8');
            for (const { label, pattern } of buildBannedTokenPatterns()) {
                expect(architectureDoc, `ARCHITECTURE.md matched ${label}`).not.toMatch(pattern);
                expect(testSource, `architecture-doc.test.ts matched ${label}`).not.toMatch(
                    pattern
                );
            }
        });

        it('contains zero semicolons in prose outside fenced code blocks', () => {
            expect(proseOnlyDoc).not.toContain(';');
        });

        it('enforces <= 20 words per numbered step and <= 25 words per descriptive prose sentence', () => {
            const { numberedSteps, descriptiveProse } = extractStyleSentences(architectureDoc);

            expect(numberedSteps.length).toBeGreaterThan(0);
            expect(descriptiveProse.length).toBeGreaterThan(0);

            const overlengthSteps = numberedSteps.filter((entry) => entry.words > 20);
            expect(overlengthSteps).toEqual([]);

            const overlengthProse = descriptiveProse.filter((entry) => entry.words > 25);
            expect(overlengthProse).toEqual([]);
        });

        it('validates style helper boundary and failure conditions on synthetic Markdown inputs', () => {
            const synthetic = [
                '```python',
                'x = 1; y = 2',
                '```',
                '1. Keep each numbered procedural step direct and concise with `src/extension.ts` on Spark 4.2.0.',
                '2. Do one thing and also add way too many extra filler words inside a single numbered procedural step so that it exceeds twenty words easily.',
                '- Provide a descriptive sentence that has way too many redundant words in a single bullet clause so the prose checker flags sentences exceeding twenty five words.',
            ].join('\n');

            expect(stripFencedCodeBlocks(synthetic)).not.toContain(';');
            const { numberedSteps, descriptiveProse } = extractStyleSentences(synthetic);
            expect(numberedSteps[0].words).toBeLessThanOrEqual(20);
            expect(numberedSteps[1].words).toBeGreaterThan(20);
            expect(descriptiveProse[0].words).toBeGreaterThan(25);
        });
    });

    describe('Pillar 1 — Spark 4.x component topology', () => {
        it('documents Extension Host, Cross-Boundary Shared, and Webview modules plus Mermaid diagrams', () => {
            expect(architectureDoc).toContain('```mermaid\nflowchart TD');
            expect(architectureDoc).toContain('```mermaid\nflowchart LR');
            expect(architectureDoc).toContain('```mermaid\nsequenceDiagram');

            const requiredTopologyTokens = [
                'src/extension.ts',
                'SparkEditorProvider.ts',
                'EditorSession.ts',
                'src/ipc/messageRouter.ts',
                'cliResolver.ts',
                'gcloudClient.ts',
                'resourceMappers.ts',
                'CodegenPromptBuilder.ts',
                'ReadOnlyCheckRunner.ts',
                'outputFolderGate.ts',
                'stagedWriter.ts',
                '.sparkgen.json',
                'PipelineRunner.ts',
                'jobIdDetector.ts',
                'envSanitizer.ts',
                'pathGuard.ts',
                'scriptGuard.ts',
                'uriGuard.ts',
                'samplePipelineService.ts',
                'sparkTree.ts',
                'src/shared/protocol.ts',
                'src/shared/dagValidator.ts',
                'src/shared/sparkDagRules.ts',
                'src/shared/sparkNodeCapabilities.ts',
                'src/shared/executionPlanner.ts',
                'src/shared/sdpPlanner.ts',
                'src/spark/versionMatrix.ts',
                'App.tsx',
                'CanvasToolbar.tsx',
                'PipelineNode.tsx',
                'NodesCard.tsx',
                'PropertyPane.tsx',
                'PropertyForm.tsx',
                'BottomPanel/',
                'SourceCodeTab',
                'DeploymentTab',
                'useVsCodeBridge.ts',
                'useAssetLoader.ts',
            ];

            for (const token of requiredTopologyTokens) {
                expect(architectureDoc).toContain(token);
            }
        });
    });

    describe('Pillar 2 — deterministic executionPlanner.ts flow', () => {
        it('documents buildExecutionPlan stages, Join modes, CacheDirective reasons, streamingSinkGroups, and DLQ splits', () => {
            const requiredPlannerTokens = [
                'buildExecutionPlan',
                'alignStageTopologicalOrder',
                'computeStreamingNodeSet',
                'batch_batch',
                'stream_static',
                'stream_stream',
                'CacheDirective',
                'batch_fanout',
                'dlq_split',
                'manual_override',
                'cache_persist_step',
                'unpersistAfterNodeIds',
                'streamingSinkGroups',
                'foreachBatch',
                'batch_df.cache()',
                'txnVersion',
                'txnAppId',
                'batch_df.unpersist()',
                'DlqSplitPoint',
                '_corrupt_record',
                'negated_predicate',
                'expectation_violation',
                'try_except_error_col',
                'ConditionalSplitPlan',
                'PipelineRuntimeArtifacts',
            ];

            for (const token of requiredPlannerTokens) {
                expect(architectureDoc).toContain(token);
            }
        });
    });

    describe('Pillar 3 — runtime version & classpath matrix (versionMatrix.ts)', () => {
        it('documents all 6 SparkRuntimeTarget profiles with exact Spark, Python, Java, and Scala versions', () => {
            const targets = Object.keys(RUNTIME_PROFILES) as SparkRuntimeTarget[];
            expect(targets).toHaveLength(6);

            for (const target of targets) {
                const profile = RUNTIME_PROFILES[target];
                const escapedTarget = target.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                const rowRegex = new RegExp(
                    `\\|\\s*\`${escapedTarget}\`\\s*\\|\\s*\`${profile.runner}\`\\s*\\|\\s*\`${profile.runtimeVersion}\`\\s*\\|\\s*\`${profile.sparkVersion}\`\\s*\\|\\s*\`${profile.pythonVersion}\`\\s*\\|\\s*\`${profile.javaVersion}\`\\s*\\|\\s*\`${profile.scalaVersion}\`\\s*\\|.*\\|\\s*\`${String(profile.sdpSupported)}\`\\s*\\|`
                );
                expect(architectureDoc, `Missing or mismatched matrix row for ${target}`).toMatch(
                    rowRegex
                );
            }

            expect(architectureDoc).toContain(`\`${DEFAULT_SERVERLESS_TIER}\``);
            expect(architectureDoc).toContain('missingLocalIcebergRuntime');
        });

        it('documents all 5 pre-installed Managed Spark artifacts and all 6 ConnectorDeliveryMechanism strategies', () => {
            for (const preinstalled of RUNTIME_PROFILES['serverless_4.0'].preinstalledConnectors) {
                expect(architectureDoc).toContain(`\`${preinstalled}\``);
            }

            for (const mechanism of ALL_CONNECTOR_MECHANISMS) {
                expect(architectureDoc).toContain(`\`${mechanism}\``);
            }

            expect(architectureDoc).toContain('dataproc.artifacts.remove=conscrypt');
        });
    });

    describe('Pillar 4 — typed IPC message registry & four-registry lockstep', () => {
        it('documents every WebviewToHostCommand in WEBVIEW_TO_HOST_COMMANDS', () => {
            for (const command of WEBVIEW_TO_HOST_COMMANDS) {
                expect(
                    architectureDoc,
                    `Missing WEBVIEW_TO_HOST_COMMANDS entry: ${command}`
                ).toContain(`\`${command}\``);
            }
        });

        it('documents every AssetResponseCommand and non-asset HostToWebviewMessage command', () => {
            for (const responseCommand of ALL_ASSET_RESPONSE_COMMANDS) {
                expect(
                    architectureDoc,
                    `Missing AssetResponseCommand entry: ${responseCommand}`
                ).toContain(`\`${responseCommand}\``);
            }

            for (const hostCommand of ALL_NON_ASSET_HOST_COMMANDS) {
                expect(
                    architectureDoc,
                    `Missing HostToWebviewMessage command: ${hostCommand}`
                ).toContain(`\`${hostCommand}\``);
            }
        });

        it('documents the four-registry lockstep files and requestParams correlation', () => {
            expect(architectureDoc).toContain('src/shared/protocol.ts');
            expect(architectureDoc).toContain('src/ipc/messageRouter.ts');
            expect(architectureDoc).toContain('webview/src/hooks/useAssetLoader.ts');
            expect(architectureDoc).toContain('src/extension.ts');
            expect(architectureDoc).toContain('requestParams');
            expect(architectureDoc).toContain('ASSET_REQUEST_COMMANDS');
            expect(architectureDoc).toContain('ASSET_CONFIGS');
        });
    });

    describe('Phase 12 SDP12, Phase 14 MOD1, and Multi-Agent Evaluator preservation', () => {
        it('preserves SDP decision guide, 9 SDP ceilings, triggered execution, dataset mappings, and BigLake contract', () => {
            const requiredSdpTokens = [
                'evaluateSdpCompatibility',
                'AUTOCDC_SCD2_NOT_SUPPORTED',
                'SDP_FOREACH_BATCH_SINK_UNSUPPORTED',
                'SDP_SPARK_ML_UNSUPPORTED',
                'SDP_CACHE_PERSIST_UNSUPPORTED',
                'SDP_PIVOT_VALUES_REQUIRED',
                'SDP_SPARK_42_REQUIRED',
                'SDP_STREAMING_WATERMARK_REQUIRED',
                'SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED',
                'INVALID_RESETTABLE_DEPENDENCY',
                'AvailableNow',
                'TRUNCATE TABLE',
                '--full-refresh-all',
                'buildSdpPlan',
                'temporary_view',
                'materialized_view',
                'streaming_table',
                'append_flow',
                'auto_cdc',
                'external_sink',
                'sdp_stage.pipeline.yaml',
                'https://biglake.googleapis.com/iceberg/v1/restcatalog',
                'run_sdp_dataproc.sh',
                'run_sdp_local.sh',
            ];

            for (const token of requiredSdpTokens) {
                expect(architectureDoc).toContain(token);
            }
        });

        it('preserves dynamic modelDiscovery.ts routing and multi-agent evaluation harness sections', () => {
            const requiredModelAndEvalTokens = [
                'src/codegen/modelDiscovery.ts',
                'discoverCodegenModels',
                'filterAndRankCodegenModels',
                'gemini-4-hydrogen',
                'DEFAULT_GEMINI_MODEL',
                "location: 'global'",
                'executeCodeGeneration',
                'runReviewerPass',
                'deterministic-gate.ts',
                'SparkArchitectureAgent',
                'PipelineSpecificationAgent',
                'SecurityAndHardeningAgent',
                'PackagingAndDeployabilityAgent',
                'arbiter-evaluator.ts',
                'pytest-runner.ts',
            ];

            for (const token of requiredModelAndEvalTokens) {
                expect(architectureDoc).toContain(token);
            }
        });
    });
});
