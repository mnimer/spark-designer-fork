/**
 * Unit Tests for the Lead Arbiter Skill Remediation Directives.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies skill resolution from node `sparkTransform` values, directive normalization,
 * the legacy directive view, the arbiter prompt, the response schema, and the arbiter
 * entry point with a mocked model client. All tests run offline against the bundled
 * codegen skill catalog.
 */

import { describe, it, expect, vi } from 'vitest';
import {
    ARBITER_RESPONSE_SCHEMA,
    CODEGEN_SKILLS_REPO_DIR,
    NodeSkillTarget,
    SkillCatalogLike,
    buildArbiterPrompt,
    calculateBaselineScorecard,
    defaultSkillsRoot,
    formatSkillTargetTable,
    normalizeSkillRemediationDirectives,
    resolveNodeSkillTargets,
    resolveSkillForTransform,
    skillFilePath,
    synthesizeEvaluation,
    toPromptRemediationDirectives,
} from '../integration/evaluators/arbiter-evaluator';
import { getCodegenSkillCatalog } from '../../codegen/CodegenSkillCatalog';
import { DeterministicGateResult, EvaluatorFinding, SpecialistAgentResult } from '../integration/evaluators/types';
import { cleanSparkFiles } from './helpers/sparkPipelineFixtures';

const catalog = getCodegenSkillCatalog(defaultSkillsRoot());
const skillIds = new Set(catalog.all().map(s => s.name));

const PIPELINE_YAML = [
    'nodes:',
    '  - id: src_kafka',
    '    data:',
    '      type: SOURCE',
    '      label: Orders Stream',
    '      spark_transform: KafkaSource',
    '  - id: step_wm',
    '    data:',
    '      type: STEP',
    '      label: Event Time',
    '      sparkTransform: Watermark',
    '  - id: step_unknown',
    '    data:',
    '      type: STEP',
    '      label: Mystery',
    '      spark_transform: NotARealTransform',
    'edges:',
    '  - source: src_kafka',
    '    target: step_wm',
    '',
].join('\n');

const CLEAN_GATE: DeterministicGateResult = {
    syntaxValid: true,
    syntaxErrors: [],
    secretScanPassed: true,
    secretFindings: [],
};

const KAFKA_FINDING: EvaluatorFinding = {
    id: 'SPEC-KAFKA-1',
    category: 'SPECIFICATION',
    severity: 'MAJOR',
    file: 'src/pipeline.py',
    description: 'startingOffsets is hardcoded to earliest',
    rationale: 'Node property says latest',
};

describe('skill path helpers', () => {
    it('builds the repository-relative SKILL.md path', () => {
        expect(CODEGEN_SKILLS_REPO_DIR).toBe('resources/codegen-skills');
        expect(skillFilePath('io-kafka')).toBe('resources/codegen-skills/io-kafka/SKILL.md');
    });

    it('points the default skills root at the bundled catalog', () => {
        expect(defaultSkillsRoot().replace(/\\/g, '/')).toMatch(/\/resources\/codegen-skills$/);
        expect(skillIds.has('core-pyspark')).toBe(true);
    });
});

describe('resolveSkillForTransform', () => {
    it('resolves exact catalog transforms to their owning skill (simple condition)', () => {
        expect(resolveSkillForTransform(catalog, 'KafkaSource')).toBe('io-kafka');
        expect(resolveSkillForTransform(catalog, 'KafkaSink')).toBe('io-kafka');
        expect(resolveSkillForTransform(catalog, 'StatefulTransform')).toBe('transform-stateful-transform');
    });

    it('prefers the transform skill over a cross-cutting skill that also lists the transform', () => {
        expect(catalog.all().filter(s => s.transforms.includes('Watermark')).map(s => s.name).sort()).toEqual(['core-streaming', 'transform-watermark']);
        expect(resolveSkillForTransform(catalog, 'Watermark')).toBe('transform-watermark');
    });

    it('resolves legacy aliases through the catalog rules (boundary condition)', () => {
        expect(resolveSkillForTransform(catalog, '  KafkaSource  ')).toBe('io-kafka');
    });

    it('returns undefined for unknown, empty, or missing transforms (failure condition)', () => {
        expect(resolveSkillForTransform(catalog, 'NotARealTransform')).toBeUndefined();
        expect(resolveSkillForTransform(catalog, '')).toBeUndefined();
        expect(resolveSkillForTransform(catalog, '   ')).toBeUndefined();
        expect(resolveSkillForTransform(catalog, undefined)).toBeUndefined();
    });

    it('works with any catalog-like object', () => {
        const fake: SkillCatalogLike = {
            all: () => [
                { name: 'core-fake', kind: 'crosscutting', transforms: ['FakeSource'] } as never,
                { name: 'io-fake', kind: 'io', transforms: ['FakeSource'] } as never,
            ],
            resolveTransform: (raw: string) => ({ canonical: raw.trim(), aliased: false, known: raw.trim() === 'FakeSource' }),
        };
        expect(resolveSkillForTransform(fake, 'FakeSource')).toBe('io-fake');
        expect(resolveSkillForTransform(fake, 'Other')).toBeUndefined();
    });
});

describe('resolveNodeSkillTargets', () => {
    it('maps every node to its owning skill, reading spark_transform or sparkTransform', () => {
        expect(resolveNodeSkillTargets(PIPELINE_YAML, catalog)).toEqual([
            { nodeId: 'src_kafka', label: 'Orders Stream', sparkTransform: 'KafkaSource', skillId: 'io-kafka' },
            { nodeId: 'step_wm', label: 'Event Time', sparkTransform: 'Watermark', skillId: 'transform-watermark' },
            { nodeId: 'step_unknown', label: 'Mystery', sparkTransform: 'NotARealTransform', skillId: undefined },
        ]);
    });

    it('handles nodes without data or label (boundary condition)', () => {
        const targets = resolveNodeSkillTargets('nodes:\n  - id: 7\n  - data: {}\n', catalog);
        expect(targets).toEqual([{ nodeId: '7', label: '7', sparkTransform: undefined, skillId: undefined }]);
    });

    it('returns an empty list for empty, invalid, or node-less YAML (failure condition)', () => {
        expect(resolveNodeSkillTargets('', catalog)).toEqual([]);
        expect(resolveNodeSkillTargets('nodes: [unclosed', catalog)).toEqual([]);
        expect(resolveNodeSkillTargets('pipelineConfig: {}', catalog)).toEqual([]);
        expect(resolveNodeSkillTargets('just a string', catalog)).toEqual([]);
    });
});

describe('formatSkillTargetTable', () => {
    it('lists each node with its skill file and the sorted valid skill ids', () => {
        const table = formatSkillTargetTable(resolveNodeSkillTargets(PIPELINE_YAML, catalog), ['io-kafka', 'core-streaming']);
        expect(table).toContain('nodeId=src_kafka | label=Orders Stream | sparkTransform=KafkaSource | skill=resources/codegen-skills/io-kafka/SKILL.md');
        expect(table).toContain('nodeId=step_unknown | label=Mystery | sparkTransform=NotARealTransform | skill=(no owning skill)');
        expect(table).toContain('Valid skill ids: core-streaming, io-kafka');
    });

    it('marks an empty pipeline (extreme condition)', () => {
        expect(formatSkillTargetTable([], [])).toContain('- (no pipeline nodes)');
    });
});

describe('normalizeSkillRemediationDirectives', () => {
    const targets: NodeSkillTarget[] = resolveNodeSkillTargets(PIPELINE_YAML, catalog);
    const base = {
        finding: 'Offsets are hardcoded.',
        targetSection: '## 2. Batch and Streaming Read Modes',
        editInstruction: 'Read startingOffsets from the node property.',
        rationale: 'Keeps replay configurable.',
    };

    it('resolves the skill from the node sparkTransform and overrides a wrong model skill', () => {
        const { directives, dropped } = normalizeSkillRemediationDirectives(
            [{ ...base, nodeId: 'src_kafka', skillId: 'core-pyspark', findingIds: ['SPEC-KAFKA-1'] }],
            targets,
            skillIds,
            new Set(['SPEC-KAFKA-1'])
        );
        expect(dropped).toEqual([]);
        expect(directives).toEqual([{
            skillId: 'io-kafka',
            skillFile: 'resources/codegen-skills/io-kafka/SKILL.md',
            nodeId: 'src_kafka',
            sparkTransform: 'KafkaSource',
            findingIds: ['SPEC-KAFKA-1'],
            ...base,
        }]);
    });

    it('accepts cross-cutting skills by bare id or by skill path without a node', () => {
        const { directives } = normalizeSkillRemediationDirectives(
            [
                { ...base, skillId: 'core-catalyst-optimization', editInstruction: 'Never call collect() in transform().' },
                { ...base, skillId: 'resources/codegen-skills/core-pyspark/SKILL.md' },
            ],
            targets,
            skillIds
        );
        expect(directives.map(d => d.skillFile)).toEqual([
            'resources/codegen-skills/core-catalyst-optimization/SKILL.md',
            'resources/codegen-skills/core-pyspark/SKILL.md',
        ]);
        expect(directives.every(d => d.nodeId === undefined)).toBe(true);
    });

    it('falls back to the model skill when the node has no owning skill (boundary condition)', () => {
        const { directives } = normalizeSkillRemediationDirectives(
            [{ ...base, nodeId: 'step_unknown', skillId: 'core-pyspark' }],
            targets,
            skillIds
        );
        expect(directives).toHaveLength(1);
        expect(directives[0].skillId).toBe('core-pyspark');
        expect(directives[0].nodeId).toBeUndefined();
    });

    it('merges duplicate directives and keeps only known finding ids', () => {
        const { directives } = normalizeSkillRemediationDirectives(
            [
                { ...base, nodeId: 'src_kafka', findingIds: ['SPEC-KAFKA-1', 'GHOST-1'] },
                { ...base, skillId: 'io-kafka', editInstruction: base.editInstruction.toUpperCase(), findingIds: ['ARCH-2'] },
            ],
            targets,
            skillIds,
            new Set(['SPEC-KAFKA-1', 'ARCH-2'])
        );
        expect(directives).toHaveLength(1);
        expect(directives[0].findingIds).toEqual(['SPEC-KAFKA-1', 'ARCH-2']);
    });

    it('drops unknown skills, missing text, and non-object entries with reasons (failure condition)', () => {
        const { directives, dropped } = normalizeSkillRemediationDirectives(
            [
                { ...base, skillId: 'io-beam-kafka' },
                { ...base, skillId: '' },
                { ...base, skillId: 'io-kafka', editInstruction: '   ' },
                { ...base, skillId: 'io-kafka', finding: undefined },
                'not-an-object',
                null,
            ],
            targets,
            skillIds
        );
        expect(directives).toEqual([]);
        expect(dropped).toHaveLength(6);
        expect(dropped[0].reason).toContain("Unknown skill 'io-beam-kafka'");
        expect(dropped[2].reason).toContain('no edit instruction');
        expect(dropped[4].reason).toContain('not an object');
    });

    it('returns no directives for a non-array value (extreme condition)', () => {
        expect(normalizeSkillRemediationDirectives(undefined, targets, skillIds)).toEqual({ directives: [], dropped: [] });
        expect(normalizeSkillRemediationDirectives({ skillId: 'io-kafka' }, targets, skillIds)).toEqual({ directives: [], dropped: [] });
    });
});

describe('toPromptRemediationDirectives', () => {
    it('maps skill directives to the legacy reporter shape', () => {
        expect(toPromptRemediationDirectives([{
            skillId: 'io-kafka',
            skillFile: 'resources/codegen-skills/io-kafka/SKILL.md',
            findingIds: [],
            finding: 'f',
            targetSection: 's',
            editInstruction: 'rule',
            rationale: 'r',
        }])).toEqual([{ targetSkill: 'resources/codegen-skills/io-kafka/SKILL.md', targetSection: 's', recommendedRuleText: 'rule', rationale: 'r' }]);
        expect(toPromptRemediationDirectives([])).toEqual([]);
    });
});

describe('buildArbiterPrompt and ARBITER_RESPONSE_SCHEMA', () => {
    it('includes the node-to-skill map, Spark grounding, and the skill directive contract', () => {
        const prompt = buildArbiterPrompt({
            language: 'Python',
            yamlContent: PIPELINE_YAML,
            files: cleanSparkFiles(),
            deterministic: CLEAN_GATE,
            specialistResults: {},
            codeGenPromptTemplate: 'PROMPT-TEMPLATE',
            baseline: calculateBaselineScorecard(CLEAN_GATE, []),
            skillTargetTable: 'TABLE-MARKER',
            sparkVersion: '4.0.1',
        });
        expect(prompt).toContain('Lead Arbiter');
        expect(prompt).toContain('=== NODE -> SKILL MAP ===\nTABLE-MARKER');
        expect(prompt).toContain('`4.0.1`');
        expect(prompt).toContain('resources/codegen-skills/<skillId>/SKILL.md');
        expect(prompt).toContain('SparkArchitectureAgent');
        expect(prompt).toContain('PROMPT-TEMPLATE');
        expect(prompt).not.toMatch(/Beam|DoFn|python\.md|java\.md/);
    });

    it('requires skill directives with an edit instruction in the schema', () => {
        expect(ARBITER_RESPONSE_SCHEMA.required).toContain('skillRemediationDirectives');
        const items = ARBITER_RESPONSE_SCHEMA.properties.skillRemediationDirectives.items;
        expect(items.required).toEqual(['skillId', 'finding', 'targetSection', 'editInstruction', 'rationale']);
        expect(Object.keys(items.properties)).toContain('nodeId');
    });
});

describe('calculateBaselineScorecard with Spark categories', () => {
    it('deducts IDIOMATIC_SPARK findings from the architecture score without a veto for MINOR', () => {
        const baseline = calculateBaselineScorecard(CLEAN_GATE, [{
            id: 'SPARK-ARCH-PYTHON-UDF-1', category: 'IDIOMATIC_SPARK', severity: 'MINOR', file: 'src/pipeline.py', description: 'udf', rationale: 'r',
        }]);
        expect(baseline.scorecard.architectureScore).toBe(95);
        expect(baseline.hardVetoReasons).toEqual([]);
    });

    it('scores non-secret gate findings by category instead of as secret leaks', () => {
        const structure: EvaluatorFinding = {
            id: 'GATE-STRUCT-1', category: 'ARCHITECTURE', severity: 'CRITICAL', file: 'src/pipeline.py', description: 'transform(dfs, config) missing', rationale: 'D3',
        };
        const gate: DeterministicGateResult = { ...CLEAN_GATE, secretScanPassed: false, secretFindings: [structure] };
        const baseline = calculateBaselineScorecard(gate, [structure]);
        expect(baseline.scorecard.securityScore).toBe(100);
        expect(baseline.scorecard.architectureScore).toBe(65);
        expect(baseline.hardVetoReasons).toEqual(['Critical Architecture Defect: transform(dfs, config) missing (src/pipeline.py)']);
    });

    it('still zeroes security and vetoes on a real SECURITY leak mixed with other gate findings (failure condition)', () => {
        const leak: EvaluatorFinding = { id: 'SEC-1', category: 'SECURITY', severity: 'CRITICAL', file: 'run_local.sh', description: 'AWS key', rationale: 'leak' };
        const banned: EvaluatorFinding = { id: 'BAN-1', category: 'PACKAGING', severity: 'MAJOR', file: 'src/pipeline.py', description: 'local master', rationale: 'r' };
        const gate: DeterministicGateResult = { ...CLEAN_GATE, secretScanPassed: false, secretFindings: [leak, banned] };
        const baseline = calculateBaselineScorecard(gate, [leak, banned]);
        expect(baseline.scorecard.securityScore).toBe(0);
        expect(baseline.scorecard.packagingScore).toBe(85);
        expect(baseline.hardVetoReasons).toContain('Deterministic Secret Leak: AWS key (run_local.sh)');
        expect(baseline.hardVetoReasons.filter(r => r.includes('Secret Leak'))).toHaveLength(1);
    });

    it('zeroes security when the scan fails without any finding (extreme condition)', () => {
        const baseline = calculateBaselineScorecard({ ...CLEAN_GATE, secretScanPassed: false }, []);
        expect(baseline.scorecard.securityScore).toBe(0);
    });
});

describe('synthesizeEvaluation skill directives (mocked model client)', () => {
    const specialists: Record<string, SpecialistAgentResult> = {
        PipelineSpecificationAgent: { agentName: 'PipelineSpecificationAgent', passed: false, summary: 'offsets', findings: [KAFKA_FINDING] },
    };

    it('emits resolved skill directives and the legacy view', async () => {
        const generateContent = vi.fn().mockResolvedValue({
            text: JSON.stringify({
                isGood: false,
                explanation: 'Offsets defect confirmed.',
                rejectedFindingIds: [],
                skillRemediationDirectives: [
                    { skillId: 'core-pyspark', nodeId: 'src_kafka', findingIds: ['SPEC-KAFKA-1'], finding: 'Offsets are hardcoded.', targetSection: '## 2. Batch and Streaming Read Modes', editInstruction: 'Read startingOffsets from the node property.', rationale: 'r' },
                    { skillId: 'io-unknown', finding: 'x', targetSection: 's', editInstruction: 'y', rationale: 'z' },
                ],
            }),
        });
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
        const result = await synthesizeEvaluation({ models: { generateContent } } as never, 'Python', PIPELINE_YAML, cleanSparkFiles(), CLEAN_GATE, specialists, 'PROMPT');

        expect(result.skillRemediationDirectives).toHaveLength(1);
        expect(result.skillRemediationDirectives[0]).toMatchObject({
            skillId: 'io-kafka',
            skillFile: 'resources/codegen-skills/io-kafka/SKILL.md',
            nodeId: 'src_kafka',
            sparkTransform: 'KafkaSource',
            findingIds: ['SPEC-KAFKA-1'],
        });
        expect(result.promptRemediationDirectives[0].targetSkill).toBe('resources/codegen-skills/io-kafka/SKILL.md');
        const args = generateContent.mock.calls[0][0];
        expect(args.config.responseSchema).toBe(ARBITER_RESPONSE_SCHEMA);
        expect(args.contents).toContain('nodeId=src_kafka');
        expect(warn).toHaveBeenCalledWith(expect.stringContaining("Unknown skill 'io-unknown'"));
        warn.mockRestore();
    });

    it('skips the model and the catalog on a clean pass (boundary condition)', async () => {
        const generateContent = vi.fn();
        const all = vi.fn(() => { throw new Error('catalog must not load'); });
        const result = await synthesizeEvaluation(
            { models: { generateContent } } as never,
            'Python', PIPELINE_YAML, cleanSparkFiles(), CLEAN_GATE,
            { SparkArchitectureAgent: { agentName: 'SparkArchitectureAgent', passed: true, summary: 'ok', findings: [] } },
            'PROMPT',
            { all, resolveTransform: vi.fn() } as never
        );
        expect(result.isGood).toBe(true);
        expect(result.skillRemediationDirectives).toEqual([]);
        expect(generateContent).not.toHaveBeenCalled();
        expect(all).not.toHaveBeenCalled();
    });

    it('returns no directives when the model omits them', async () => {
        const generateContent = vi.fn().mockResolvedValue({ text: JSON.stringify({ isGood: false, explanation: 'x' }) });
        const result = await synthesizeEvaluation({ models: { generateContent } } as never, 'Python', PIPELINE_YAML, [], CLEAN_GATE, specialists, 'PROMPT');
        expect(result.skillRemediationDirectives).toEqual([]);
        expect(result.promptRemediationDirectives).toEqual([]);
    });

    it('throws on an empty model response (failure condition)', async () => {
        const generateContent = vi.fn().mockResolvedValue({ text: '' });
        await expect(
            synthesizeEvaluation({ models: { generateContent } } as never, 'Python', PIPELINE_YAML, [], CLEAN_GATE, specialists, 'PROMPT')
        ).rejects.toThrow('Empty response from Lead Arbiter Agent');
    });
});
