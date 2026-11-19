/**
 * Unit Tests for the Multi-Agent Evaluation Harness.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Tests the baseline scoring engine, hard veto rules, false-positive
 * reconciliation, security model fallback, language synchronization,
 * report formatting, prompt remediation, and end-to-end orchestration
 * with the SparkArchitectureAgent. Deterministic gate coverage lives in
 * its own suite.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
    calculateBaselineScorecard,
    synthesizeEvaluation
} from '../integration/evaluators/arbiter-evaluator';
import {
    formatEvaluationReport,
    runMultiAgentEvaluation
} from '../integration/evaluators/multi-agent-evaluator';
import {
    resolveSecurityModel,
    resetResolvedSecurityModelForTesting
} from '../integration/evaluators/security-evaluator';
import { formatFilesWithLineNumbers } from '../integration/evaluators/format-utils';
import { synchronizePipelineLanguage, parseGcloudProjectFromIni } from '../integration/codegen-runner';
import {
    DeterministicGateResult,
    EvaluatorFinding,
    MultiAgentEvaluationResult,
    SpecialistAgentResult
} from '../integration/evaluators/types';
import { DEFAULT_GEMINI_MODEL } from '../../constants';
import { cleanSparkFiles } from './helpers/sparkPipelineFixtures';
import { evaluatePackagingAndDeployability } from '../integration/evaluators/packaging-evaluator';

// NOTE: The deterministic gate has its own suite. Stub it here so orchestration tests stay independent of gate rules.
vi.mock('../integration/evaluators/deterministic-gate', () => ({
    runDeterministicGate: vi.fn(async () => ({
        syntaxValid: true,
        syntaxErrors: [],
        secretScanPassed: true,
        secretFindings: [],
        missingDeliverables: []
    }))
}));

// NOTE: Wrap the real packaging agent so tests can assert the arguments it receives.
vi.mock('../integration/evaluators/packaging-evaluator', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../integration/evaluators/packaging-evaluator')>();
    return { ...actual, evaluatePackagingAndDeployability: vi.fn(actual.evaluatePackagingAndDeployability) };
});

describe('Format Utils (formatFilesWithLineNumbers)', () => {
    it('prefixes source code with 1-indexed line numbers (simple condition)', () => {
        const files = [
            {
                filename: 'test.py',
                content: 'line one\nline two\nline three'
            }
        ];
        const formatted = formatFilesWithLineNumbers(files);
        expect(formatted).toContain('--- FILE: test.py ---');
        expect(formatted).toContain('1: line one');
        expect(formatted).toContain('2: line two');
        expect(formatted).toContain('3: line three');
    });

    it('handles empty content gracefully (extreme condition)', () => {
        const files = [{ filename: 'empty.txt', content: '' }];
        const formatted = formatFilesWithLineNumbers(files);
        expect(formatted).toContain('1: ');
    });
});

describe('Baseline Scoring Engine & Hard Veto Rules (calculateBaselineScorecard)', () => {
    it('awards 100/100 and no hard veto on completely clean findings (simple condition)', () => {
        const cleanGate: DeterministicGateResult = {
            syntaxValid: true,
            syntaxErrors: [],
            secretScanPassed: true,
            secretFindings: []
        };
        const baseline = calculateBaselineScorecard(cleanGate, []);
        expect(baseline.scorecard.overallScore).toBe(100);
        expect(baseline.hardVetoTriggered).toBe(false);
        expect(baseline.hardVetoReasons.length).toBe(0);
    });

    it('triggers a hard veto when deterministic syntax checks fail (failure condition)', () => {
        const brokenGate: DeterministicGateResult = {
            syntaxValid: false,
            syntaxErrors: ['Syntax error in pipeline.py: line 4'],
            secretScanPassed: true,
            secretFindings: []
        };
        const baseline = calculateBaselineScorecard(brokenGate, []);
        expect(baseline.hardVetoTriggered).toBe(true);
        expect(baseline.hardVetoReasons).toContain('Syntax error in pipeline.py: line 4');
        expect(baseline.scorecard.packagingScore).toBe(50);
    });

    it('triggers a hard veto and resets security score to 0 on secret leak (failure condition)', () => {
        const secretGate: DeterministicGateResult = {
            syntaxValid: true,
            syntaxErrors: [],
            secretScanPassed: false,
            secretFindings: [
                {
                    id: 'SEC-1',
                    category: 'SECURITY',
                    severity: 'CRITICAL',
                    file: 'pipeline.py',
                    description: 'Plaintext secret leaked',
                    rationale: 'Secret key detected'
                }
            ]
        };
        const baseline = calculateBaselineScorecard(secretGate, secretGate.secretFindings);
        expect(baseline.hardVetoTriggered).toBe(true);
        expect(baseline.scorecard.securityScore).toBe(0);
    });

    it('triggers hard veto on CRITICAL architectural flaws (e.g. silent drop bug)', () => {
        const cleanGate: DeterministicGateResult = {
            syntaxValid: true,
            syntaxErrors: [],
            secretScanPassed: true,
            secretFindings: []
        };
        const criticalArchFinding: EvaluatorFinding = {
            id: 'ARCH-1',
            category: 'ARCHITECTURE',
            severity: 'CRITICAL',
            file: 'pipeline.py',
            description: 'foreachBatch Delta write omits txnVersion and duplicates retried micro-batches',
            rationale: 'Spark idempotency bug'
        };
        const baseline = calculateBaselineScorecard(cleanGate, [criticalArchFinding]);
        expect(baseline.hardVetoTriggered).toBe(true);
        expect(baseline.hardVetoReasons[0]).toContain('Critical Architecture Defect');
        expect(baseline.scorecard.architectureScore).toBe(65);
    });
});

describe('Security Model Fallback (resolveSecurityModel)', () => {
    beforeEach(() => {
        resetResolvedSecurityModelForTesting();
        delete process.env.SECURITY_EVALUATOR_MODEL;
    });

    it('resolves preferred gemini-3.8-flash-cyber model when probing succeeds', async () => {
        const mockAi: any = {
            models: {
                generateContent: vi.fn().mockResolvedValue({ text: 'pong' })
            }
        };
        const model = await resolveSecurityModel(mockAi);
        expect(model).toBe('gemini-3.8-flash-cyber');
    });

    it('falls back to gemini-3.8-flash when cyber model throws 404/403', async () => {
        const mockAi: any = {
            models: {
                generateContent: vi.fn()
                    .mockRejectedValueOnce(new Error('Model not found: 404'))
                    .mockResolvedValueOnce({ text: 'pong' })
            }
        };
        const model = await resolveSecurityModel(mockAi);
        expect(model).toBe('gemini-3.8-flash');
    });

    it('falls back to DEFAULT_GEMINI_MODEL when both cyber and flash fail', async () => {
        const mockAi: any = {
            models: {
                generateContent: vi.fn()
                    .mockRejectedValueOnce(new Error('Model not found: 404'))
                    .mockRejectedValueOnce(new Error('Rate limit exceeded: 429'))
            }
        };
        const model = await resolveSecurityModel(mockAi);
        expect(model).toBe(DEFAULT_GEMINI_MODEL);
    });
});

describe('Pipeline Language Synchronization (synchronizePipelineLanguage)', () => {
    it('updates pipelineConfig.language to target language in YAML', () => {
        const javaYaml = `
pipelineConfig:
  name: test-pipe
  language: Java
nodes: []
`;
        const pythonYaml = synchronizePipelineLanguage(javaYaml, 'Python');
        expect(pythonYaml).toContain('language: Python');
        expect(pythonYaml).not.toContain('language: Java');
    });

    it('extracts project ID synchronously from gcloud config_default INI content', () => {
        const iniContent = `[core]\naccount = user@google.com\nproject = my-gcp-vertex-project\n\n[compute]\nregion = us-central1\n`;
        expect(parseGcloudProjectFromIni(iniContent)).toBe('my-gcp-vertex-project');
        expect(parseGcloudProjectFromIni('[core]\naccount = user@google.com\n')).toBeUndefined();
    });
});

describe('Arbiter False-Positive Reconciliation', () => {
    it('clears hard veto when Arbiter rejects a specialist finding as false positive', async () => {
        const mockAi: any = {
            models: {
                generateContent: vi.fn().mockResolvedValue({
                    text: JSON.stringify({
                        isGood: true,
                        explanation: 'Specialist reported missing Secret Manager, but code verified at lines 15-20.',
                        rejectedFindingIds: ['SPEC-FP-1'],
                        rejectedFindings: [
                            {
                                id: 'SPEC-FP-1',
                                reason: 'False positive: Secret Manager is initialized in setup() on line 18.'
                            }
                        ],
                        promptRemediationDirectives: []
                    })
                })
            }
        };

        const cleanGate: DeterministicGateResult = {
            syntaxValid: true,
            syntaxErrors: [],
            secretScanPassed: true,
            secretFindings: []
        };

        const falsePositiveFinding: EvaluatorFinding = {
            id: 'SPEC-FP-1',
            category: 'SPECIFICATION',
            severity: 'CRITICAL',
            file: 'src/pipeline.py',
            description: 'Missing Secret Manager integration',
            rationale: 'Specialist did not see dynamic resolution'
        };

        const specialistResults: Record<string, SpecialistAgentResult> = {
            PipelineSpecificationAgent: {
                agentName: 'PipelineSpecificationAgent',
                passed: false,
                summary: 'Failed due to missing secret manager',
                findings: [falsePositiveFinding]
            }
        };

        const files = [{ filename: 'src/pipeline.py', content: 'def setup():\n  client = secretmanager.Client()' }];

        const result = await synthesizeEvaluation(
            mockAi,
            'Python',
            'nodes: []',
            files,
            cleanGate,
            specialistResults,
            'Prompt'
        );

        expect(result.isGood).toBe(true);
        expect(result.hardVetoTriggered).toBe(false);
        expect(result.rejectedFindings?.length).toBe(1);
        expect(result.confirmedFindings?.length).toBe(0);
    });
});

describe('Report Formatting and Prompt Remediation Directives', () => {
    it('formats a clean report with scorecard, findings, and prompt directives', () => {
        const sampleResult: MultiAgentEvaluationResult = {
            isGood: false,
            scorecard: {
                overallScore: 68,
                architectureScore: 65,
                securityScore: 100,
                packagingScore: 85,
                maintainabilityScore: 90
            },
            hardVetoTriggered: true,
            hardVetoReasons: ['Critical Architecture Defect: Scala 2.12 connector JAR on Spark 4.x'],
            allFindings: [
                {
                    id: 'ARCH-1',
                    category: 'ARCHITECTURE',
                    severity: 'CRITICAL',
                    file: 'pipeline.py',
                    line: 42,
                    evidence: 'rows = df.collect()',
                    description: 'Driver collection of a large DataFrame',
                    rationale: 'Collect causes driver OOM on large data'
                }
            ],
            specialistResults: {},
            skillRemediationDirectives: [],
            promptRemediationDirectives: [
                {
                    targetSkill: 'resources/codegen-skills/core-catalyst-optimization/SKILL.md',
                    targetSection: '## 2. No Unbounded Driver Collection',
                    recommendedRuleText: 'Never call .collect() in transform(). Keep data in lazy DataFrame operations.',
                    rationale: 'Prevents driver OOM in PySpark pipelines.'
                }
            ],
            explanation: 'Pipeline fails due to driver collection in transform().'
        };

        const report = formatEvaluationReport(sampleResult, 'Python');
        expect(report).toContain('EVALUATION SCORECARD (Python)');
        expect(report).toContain('Status:       ❌ FAILED');
        expect(report).toContain('Overall:      68 / 100');
        expect(report).toContain('HARD VETO TRIGGERED:');
        expect(report).toContain('pipeline.py:42');
        expect(report).toContain('Evidence: rows = df.collect()');
        expect(report).toContain('PROMPT REMEDIATION DIRECTIVES (Targeting Generator Prompts)');
        expect(report).toContain('Directive #1 for resources/codegen-skills/core-catalyst-optimization/SKILL.md');
    });

    it('prefers Skill Remediation Directives with node, findings, and edit instruction', () => {
        const result: MultiAgentEvaluationResult = {
            isGood: false,
            scorecard: { overallScore: 70, architectureScore: 85, securityScore: 100, packagingScore: 100, maintainabilityScore: 100 },
            hardVetoTriggered: true,
            hardVetoReasons: [],
            allFindings: [],
            specialistResults: {},
            skillRemediationDirectives: [
                {
                    skillId: 'io-kafka',
                    skillFile: 'resources/codegen-skills/io-kafka/SKILL.md',
                    nodeId: 'src_kafka',
                    sparkTransform: 'KafkaSource',
                    findingIds: ['SPEC-1', 'ARCH-2'],
                    finding: 'Kafka offsets are hardcoded.',
                    targetSection: '## 2. Batch and Streaming Read Modes',
                    editInstruction: 'Read startingOffsets from the node property.\nNever hardcode offsets.',
                    rationale: 'Keeps replay behavior configurable.'
                }
            ],
            promptRemediationDirectives: [
                { targetSkill: 'resources/codegen-skills/io-kafka/SKILL.md', targetSection: 's', recommendedRuleText: 'legacy', rationale: 'r' }
            ],
            explanation: 'Kafka offsets defect.'
        };
        const report = formatEvaluationReport(result, 'Python');
        expect(report).toContain('SKILL REMEDIATION DIRECTIVES (Targeting Codegen Skills)');
        expect(report).toContain('Directive #1 for resources/codegen-skills/io-kafka/SKILL.md');
        expect(report).toContain('Node: src_kafka (sparkTransform: KafkaSource)');
        expect(report).toContain('Findings: SPEC-1, ARCH-2');
        expect(report).toContain('     Never hardcode offsets.');
        expect(report).not.toContain('PROMPT REMEDIATION DIRECTIVES');
    });
});

describe('End-to-End Orchestration with Mocked Model Calls', () => {
    beforeEach(() => {
        resetResolvedSecurityModelForTesting();
    });

    it('runs runMultiAgentEvaluation concurrently across subagents', async () => {
        const mockAi: any = {
            models: {
                generateContent: vi.fn().mockImplementation((args: any) => {
                    const promptText = args.contents;
                    if (promptText === 'ping') {
                        return Promise.resolve({ text: 'pong' });
                    }
                    if (promptText.includes('Spark Architecture Evaluator Agent')) {
                        return Promise.resolve({
                            text: JSON.stringify({
                                passed: true,
                                summary: 'Spark architecture clean',
                                findings: []
                            })
                        });
                    }
                    if (promptText.includes('Pipeline Specification & Property Conformance')) {
                        return Promise.resolve({
                            text: JSON.stringify({
                                passed: true,
                                summary: 'Specification and properties clean',
                                findings: []
                            })
                        });
                    }
                    if (promptText.includes('Security & Container Hardening')) {
                        return Promise.resolve({
                            text: JSON.stringify({
                                passed: true,
                                summary: 'Security clean',
                                findings: []
                            })
                        });
                    }
                    if (promptText.includes('Packaging & Runtime Deployability')) {
                        return Promise.resolve({
                            text: JSON.stringify({
                                passed: true,
                                summary: 'Packaging clean',
                                findings: []
                            })
                        });
                    }
                    if (promptText.includes('Lead Arbiter')) {
                        return Promise.resolve({
                            text: JSON.stringify({
                                isGood: true,
                                explanation: 'All criteria satisfied.',
                                promptRemediationDirectives: []
                            })
                        });
                    }
                    return Promise.resolve({ text: '{}' });
                })
            }
        };

        const files = cleanSparkFiles();

        const result = await runMultiAgentEvaluation(
            mockAi,
            'Python',
            'nodes: []',
            'system prompt',
            files
        );

        expect(result.isGood).toBe(true);
        expect(result.hardVetoTriggered).toBe(false);
        expect(result.scorecard.overallScore).toBe(100);
        expect(result.specialistResults.SparkArchitectureAgent).toBeDefined();
        expect(result.specialistResults.SparkArchitectureAgent.passed).toBe(true);
        expect(result.specialistResults.PipelineSpecificationAgent).toBeDefined();
        expect(result.specialistResults.SecurityAndHardeningAgent).toBeDefined();
        expect(result.specialistResults.PackagingAndDeployabilityAgent).toBeDefined();
        // The packaging agent receives the pipeline YAML so it can audit ExecutionPlan artifacts.
        expect(vi.mocked(evaluatePackagingAndDeployability)).toHaveBeenLastCalledWith(mockAi, 'Python', files, 'nodes: []');
        // Only the 4 specialist sub-agents should be invoked in parallel on a clean pass (no ping calls, no redundant 5th arbiter LLM call)
        expect(mockAi.models.generateContent).toHaveBeenCalledTimes(4);
    });

    it('invokes Lead Arbiter LLM when a specialist reports findings', async () => {
        const mockAi: any = {
            models: {
                generateContent: vi.fn().mockImplementation((args: any) => {
                    const promptText = args.contents;
                    if (promptText.includes('Spark Architecture Evaluator Agent')) {
                        return Promise.resolve({
                            text: JSON.stringify({
                                passed: false,
                                summary: 'Watermark placed after aggregation',
                                findings: [
                                    {
                                        id: 'ARCH-WM-1',
                                        category: 'ARCHITECTURE',
                                        severity: 'CRITICAL',
                                        file: 'src/pipeline.py',
                                        description: 'withWatermark is applied after the windowed groupBy',
                                        rationale: 'Late-data state grows without bound'
                                    }
                                ]
                            })
                        });
                    }
                    if (promptText.includes('Pipeline Specification & Property Conformance')) {
                        return Promise.resolve({
                            text: JSON.stringify({
                                passed: true,
                                summary: 'Spec clean',
                                requirementVerifications: [],
                                findings: []
                            })
                        });
                    }
                    if (promptText.includes('Security & Container Hardening')) {
                        return Promise.resolve({
                            text: JSON.stringify({
                                passed: true,
                                summary: 'Security clean',
                                findings: []
                            })
                        });
                    }
                    if (promptText.includes('Packaging & Runtime Deployability')) {
                        return Promise.resolve({
                            text: JSON.stringify({
                                passed: true,
                                summary: 'Packaging clean',
                                findings: []
                            })
                        });
                    }
                    if (promptText.includes('Lead Arbiter')) {
                        return Promise.resolve({
                            text: JSON.stringify({
                                isGood: false,
                                explanation: 'Critical missing windowing confirmed.',
                                rejectedFindingIds: [],
                                rejectedFindings: [],
                                skillRemediationDirectives: [
                                    {
                                        skillId: 'resources/codegen-skills/core-streaming/SKILL.md',
                                        findingIds: ['ARCH-WM-1'],
                                        finding: 'withWatermark runs after the windowed groupBy.',
                                        targetSection: '## 1. Streaming Sources & Watermark Placement (`Watermark`)',
                                        editInstruction: 'Apply withWatermark before every windowed groupBy.',
                                        rationale: 'Prevents unbounded state growth.'
                                    }
                                ]
                            })
                        });
                    }
                    return Promise.resolve({ text: '{}' });
                })
            }
        };

        const files = cleanSparkFiles();

        const result = await runMultiAgentEvaluation(
            mockAi,
            'Python',
            'nodes: []',
            'system prompt',
            files
        );

        expect(result.isGood).toBe(false);
        expect(result.hardVetoTriggered).toBe(true);
        expect(result.skillRemediationDirectives).toEqual([
            expect.objectContaining({
                skillId: 'core-streaming',
                skillFile: 'resources/codegen-skills/core-streaming/SKILL.md',
                findingIds: ['ARCH-WM-1']
            })
        ]);
        expect(result.promptRemediationDirectives).toEqual([
            expect.objectContaining({ targetSkill: 'resources/codegen-skills/core-streaming/SKILL.md' })
        ]);
        // 4 specialist sub-agents + 1 Lead Arbiter call = 5 calls total
        expect(mockAi.models.generateContent).toHaveBeenCalledTimes(5);
    });

    it('keeps static SparkArchitectureAgent findings when its LLM call fails (failure condition)', async () => {
        const generateContent = vi.fn().mockImplementation((args: any) => {
            const promptText: string = args.contents;
            if (promptText.includes('Spark Architecture Evaluator Agent')) {
                return Promise.reject(new Error('503 Service Unavailable'));
            }
            if (promptText.includes('Lead Arbiter')) {
                return Promise.resolve({
                    text: JSON.stringify({ isGood: false, explanation: 'Scala 2.12 JAR confirmed.', rejectedFindingIds: [], skillRemediationDirectives: [], promptRemediationDirectives: [] })
                });
            }
            return Promise.resolve({ text: JSON.stringify({ passed: true, summary: 'clean', requirementVerifications: [], findings: [] }) });
        });
        const files = cleanSparkFiles().map(f => f.filename === 'run_dataproc_cluster.sh'
            ? { ...f, content: f.content.replace('delta-spark_2.13:4.0.0', 'delta-core_2.12:2.4.0') }
            : f);

        const result = await runMultiAgentEvaluation({ models: { generateContent } } as any, 'Python', 'nodes: []', 'system prompt', files);

        const arch = result.specialistResults.SparkArchitectureAgent;
        expect(arch.passed).toBe(false);
        expect(arch.summary).toContain('503');
        expect(arch.findings.some(f => f.id.startsWith('SPARK-ARCH-CLASSPATH') && f.severity === 'CRITICAL')).toBe(true);
        expect(result.isGood).toBe(false);
        expect(result.hardVetoReasons.some(r => r.includes('Scala 2.11 or 2.12'))).toBe(true);
    });
});
