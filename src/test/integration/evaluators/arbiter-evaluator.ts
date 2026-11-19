/**
 * Lead Arbiter & Synthesizer Evaluator Agent.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Synthesizes reports from the deterministic pre-flight gate and specialized sub-agents.
 * Validates findings against ground-truth line-numbered code, reconciles false positives,
 * produces a multi-dimensional scorecard, checks hard veto rules, and generates
 * actionable Skill Remediation Directives whenever an evaluation fails. Each directive
 * targets one `resources/codegen-skills/<skill>/SKILL.md` file.
 *
 * Architectural decisions:
 * - Does NOT patch the generated code when a test fails. Integration tests validate
 *   the LLM Code Generator itself; therefore, remediation consists of formulating
 *   exact prompt rules to update the prompt templates so the generator ceases
 *   making the error.
 * - Inspects full line-numbered generated source files to verify or reject sub-agent claims.
 * - Filters rejected false-positive findings and recalculates the baseline scorecard
 *   and hard vetoes on confirmed findings only, resolving the false-positive veto trap.
 * - Receives the full generator prompt template (without artificial truncation) so
 *   remediation directives cite real prompt sections and existing rules accurately.
 * - Resolves the target skill deterministically. If a directive names a pipeline node, the
 *   node `sparkTransform` selects the owning skill through the codegen skill catalog, and
 *   that resolution overrides the model's choice. Directives that name an unknown skill
 *   or omit the edit instruction are dropped, not guessed.
 */

import * as path from 'path';
import * as yaml from 'yaml';
import { GoogleGenAI } from '@google/genai';
import { DEFAULT_GEMINI_MODEL } from '../../../constants';
import { DEFAULT_SPARK_VERSION } from '../../../shared/types';
import { CodegenSkillCatalog, getCodegenSkillCatalog } from '../../../codegen/CodegenSkillCatalog';
import { CODEGEN_RESOURCE_PATHS } from '../../../codegen/codegenConfig';
import {
    DeterministicGateResult,
    SpecialistAgentResult,
    EvaluatorFinding,
    MultiAgentEvaluationResult,
    EvaluationScorecard,
    RejectedFinding,
    PromptRemediationDirective,
    SkillRemediationDirective,
    generateContentWithRetry
} from './types';
import { GeneratedFileCandidate } from './deterministic-gate';
import { formatFilesWithLineNumbers } from './format-utils';

/** Repository-relative folder that holds the codegen skills. */
export const CODEGEN_SKILLS_REPO_DIR = `resources/${CODEGEN_RESOURCE_PATHS.skillsDir}`;

/** Catalog methods that the arbiter needs. Tests pass a catalog built from in-memory skills. */
export type SkillCatalogLike = Pick<CodegenSkillCatalog, 'all' | 'resolveTransform'>;

/** One pipeline node and the skill that owns its `sparkTransform`. */
export interface NodeSkillTarget {
    nodeId: string;
    label: string;
    sparkTransform?: string;
    /** Owning skill id, or undefined when no skill owns the transform. */
    skillId?: string;
}

/** A model directive that normalization rejected, with the reason. */
export interface DroppedSkillDirective {
    reason: string;
    raw: unknown;
}

/**
 * Returns the repository-relative `SKILL.md` path of a skill.
 *
 * @param skillId - Skill id (folder name).
 * @returns `resources/codegen-skills/<skillId>/SKILL.md`.
 */
export function skillFilePath(skillId: string): string {
    return `${CODEGEN_SKILLS_REPO_DIR}/${skillId}/${CODEGEN_RESOURCE_PATHS.skillFileName}`;
}

/**
 * Returns the absolute skills folder of this repository.
 *
 * @returns Absolute path of `resources/codegen-skills`.
 */
export function defaultSkillsRoot(): string {
    // NOTE: This file lives in src/test/integration/evaluators (or the same depth under out/).
    return path.resolve(__dirname, '..', '..', '..', '..', 'resources', CODEGEN_RESOURCE_PATHS.skillsDir);
}

/** Skill kinds that own one node type. Cross-cutting skills may also list a transform as a trigger. */
const NODE_OWNER_KINDS: ReadonlySet<string> = new Set(['io', 'transform']);

/**
 * Resolves the skill that owns a `sparkTransform` value. Accepts legacy spellings through
 * the catalog alias rules. If several skills list the transform, the `io` or `transform`
 * skill wins over a cross-cutting skill that only uses the transform as a trigger.
 *
 * @param catalog - Skill catalog.
 * @param sparkTransform - Raw `spark_transform` value.
 * @returns Owning skill id, or undefined when no skill owns the value.
 */
export function resolveSkillForTransform(catalog: SkillCatalogLike, sparkTransform: string | undefined): string | undefined {
    if (!sparkTransform || !sparkTransform.trim()) {
        return undefined;
    }
    const resolution = catalog.resolveTransform(sparkTransform);
    if (!resolution.known) {
        return undefined;
    }
    const candidates = catalog.all().filter(skill => skill.transforms.includes(resolution.canonical));
    return (candidates.find(skill => NODE_OWNER_KINDS.has(skill.kind)) ?? candidates[0])?.name;
}

/**
 * Parses the pipeline YAML and resolves the owning skill of every node.
 *
 * @param yamlContent - Pipeline YAML.
 * @param catalog - Skill catalog.
 * @returns One target per node. Returns an empty list when the YAML has no nodes or does not parse.
 */
export function resolveNodeSkillTargets(yamlContent: string, catalog: SkillCatalogLike): NodeSkillTarget[] {
    let parsed: unknown;
    try {
        parsed = yaml.parse(yamlContent || '');
    } catch {
        return [];
    }
    const nodes = parsed && typeof parsed === 'object' ? (parsed as { nodes?: unknown }).nodes : undefined;
    if (!Array.isArray(nodes)) {
        return [];
    }
    return nodes
        .filter((n): n is Record<string, unknown> => !!n && typeof n === 'object' && n.id !== undefined && n.id !== null)
        .map(n => {
            const data = (n.data && typeof n.data === 'object' ? n.data : {}) as Record<string, unknown>;
            const rawTransform = data.spark_transform ?? data.sparkTransform;
            const sparkTransform = typeof rawTransform === 'string' ? rawTransform : undefined;
            return {
                nodeId: String(n.id),
                label: typeof data.label === 'string' ? data.label : String(n.id),
                sparkTransform,
                skillId: resolveSkillForTransform(catalog, sparkTransform),
            };
        });
}

/**
 * Formats the node-to-skill table and the list of valid skill ids for the arbiter prompt.
 *
 * @param targets - Node skill targets.
 * @param skillIds - Every skill id in the catalog.
 * @returns Prompt section text.
 */
export function formatSkillTargetTable(targets: NodeSkillTarget[], skillIds: string[]): string {
    const rows = targets.length > 0
        ? targets.map(t => `- nodeId=${t.nodeId} | label=${t.label} | sparkTransform=${t.sparkTransform ?? '(none)'} | skill=${t.skillId ? skillFilePath(t.skillId) : '(no owning skill)'}`)
        : ['- (no pipeline nodes)'];
    return [
        'Node -> owning skill (resolved from each node sparkTransform):',
        ...rows,
        '',
        `Valid skill ids: ${[...skillIds].sort().join(', ')}`,
    ].join('\n');
}

/**
 * Extracts a skill id from a model value. Accepts a bare id or a path under `codegen-skills/`.
 *
 * @param value - Model value.
 * @returns Candidate skill id, or undefined.
 */
function extractSkillId(value: unknown): string | undefined {
    if (typeof value !== 'string' || !value.trim()) {
        return undefined;
    }
    const trimmed = value.trim();
    const fromPath = /codegen-skills\/([A-Za-z0-9_-]+)/.exec(trimmed);
    return fromPath ? fromPath[1] : trimmed;
}

/**
 * Validates and normalizes the model's skill remediation directives.
 *
 * Rules:
 * 1. If `nodeId` names a node with an owning skill, that skill wins over the model's `skillId`.
 * 2. Otherwise `skillId` (bare id or skill path) must be a known catalog skill.
 * 3. `finding` and `editInstruction` must be non-empty.
 * 4. `findingIds` keep only ids of known findings when a known-id set is given.
 * 5. Directives with the same skill and edit instruction merge into one.
 *
 * @param raw - Model output array.
 * @param targets - Node skill targets.
 * @param knownSkillIds - Every catalog skill id.
 * @param knownFindingIds - Ids of findings the directives may cite.
 * @returns Valid directives and the dropped entries with reasons.
 */
export function normalizeSkillRemediationDirectives(
    raw: unknown,
    targets: NodeSkillTarget[],
    knownSkillIds: ReadonlySet<string>,
    knownFindingIds: ReadonlySet<string> = new Set()
): { directives: SkillRemediationDirective[]; dropped: DroppedSkillDirective[] } {
    const directives: SkillRemediationDirective[] = [];
    const dropped: DroppedSkillDirective[] = [];
    if (!Array.isArray(raw)) {
        return { directives, dropped };
    }
    const byNode = new Map(targets.map(t => [t.nodeId, t]));
    const byKey = new Map<string, SkillRemediationDirective>();

    for (const item of raw) {
        if (!item || typeof item !== 'object') {
            dropped.push({ reason: 'Directive is not an object.', raw: item });
            continue;
        }
        const d = item as Record<string, unknown>;
        const nodeId = typeof d.nodeId === 'string' && d.nodeId.trim() ? d.nodeId.trim() : undefined;
        const target = nodeId ? byNode.get(nodeId) : undefined;
        // 1. and 2. Node sparkTransform resolution wins. Otherwise the named skill must exist.
        const skillId = target?.skillId ?? extractSkillId(d.skillId);
        if (!skillId || !knownSkillIds.has(skillId)) {
            dropped.push({ reason: `Unknown skill '${String(d.skillId ?? '')}'${nodeId ? ` for node '${nodeId}'` : ''}.`, raw: item });
            continue;
        }
        // 3. Required text.
        const finding = typeof d.finding === 'string' ? d.finding.trim() : '';
        const editInstruction = typeof d.editInstruction === 'string' ? d.editInstruction.trim() : '';
        if (!finding || !editInstruction) {
            dropped.push({ reason: 'Directive has no finding or no edit instruction.', raw: item });
            continue;
        }
        // 4. Cited finding ids.
        const rawIds = Array.isArray(d.findingIds) ? d.findingIds.filter((x): x is string => typeof x === 'string') : [];
        const findingIds = knownFindingIds.size > 0 ? rawIds.filter(id => knownFindingIds.has(id)) : rawIds;

        // 5. Merge duplicates.
        const key = `${skillId}::${editInstruction.toLowerCase()}`;
        const existing = byKey.get(key);
        if (existing) {
            existing.findingIds = [...new Set([...existing.findingIds, ...findingIds])];
            continue;
        }
        const directive: SkillRemediationDirective = {
            skillId,
            skillFile: skillFilePath(skillId),
            findingIds,
            finding,
            targetSection: typeof d.targetSection === 'string' ? d.targetSection.trim() : '',
            editInstruction,
            rationale: typeof d.rationale === 'string' ? d.rationale.trim() : '',
        };
        if (target && target.skillId === skillId) {
            directive.nodeId = target.nodeId;
            if (target.sparkTransform) {
                directive.sparkTransform = target.sparkTransform;
            }
        }
        byKey.set(key, directive);
        directives.push(directive);
    }
    return { directives, dropped };
}

/**
 * Converts skill directives to the legacy flat directive shape used by the Markdown reporter.
 *
 * @param directives - Skill remediation directives.
 * @returns Legacy prompt remediation directives.
 */
export function toPromptRemediationDirectives(directives: SkillRemediationDirective[]): PromptRemediationDirective[] {
    return directives.map(d => ({
        targetSkill: d.skillFile,
        targetSection: d.targetSection,
        recommendedRuleText: d.editInstruction,
        rationale: d.rationale,
    }));
}

/**
 * Calculates rule-based baseline scores and vetoes from findings.
 *
 * @param deterministic - Result of the deterministic pre-flight gate.
 * @param subAgentFindings - Consolidated findings from all sub-agents.
 * @returns Scorecard, veto status, and veto reasons.
 */
export function calculateBaselineScorecard(
    deterministic: DeterministicGateResult,
    subAgentFindings: EvaluatorFinding[]
): { scorecard: EvaluationScorecard; hardVetoTriggered: boolean; hardVetoReasons: string[] } {
    let architectureScore = 100;
    let specificationScore = 100;
    let securityScore = 100;
    let packagingScore = 100;
    let maintainabilityScore = 100;

    const hardVetoReasons: string[] = [];

    // Deterministic gate penalties & vetoes
    if (!deterministic.syntaxValid) {
        packagingScore = Math.max(0, packagingScore - 50);
        hardVetoReasons.push(...deterministic.syntaxErrors);
    }

    if (!deterministic.secretScanPassed) {
        // NOTE: The gate can report non-secret findings (banned patterns, D3 structure) in
        // `secretFindings`. Only SECURITY entries are secret leaks. Other categories score
        // through the per-category loop below, because callers pass them in subAgentFindings too.
        const leaks = deterministic.secretFindings.filter(f => f.category === 'SECURITY');
        if (leaks.length > 0 || deterministic.secretFindings.length === 0) {
            securityScore = 0;
        }
        leaks.forEach(f => {
            if (f.severity === 'CRITICAL') {
                hardVetoReasons.push(`Deterministic Secret Leak: ${f.description} (${f.file})`);
            }
        });
    }

    // Sub-agent findings deductions
    for (const f of subAgentFindings) {
        const deduction = f.severity === 'CRITICAL' ? 35 : f.severity === 'MAJOR' ? 15 : 5;

        switch (f.category) {
            case 'ARCHITECTURE':
            case 'IDIOMATIC_SPARK':
                architectureScore = Math.max(0, architectureScore - deduction);
                if (f.severity === 'CRITICAL') {
                    hardVetoReasons.push(`Critical Architecture Defect: ${f.description} (${f.file})`);
                }
                break;
            case 'SPECIFICATION':
                specificationScore = Math.max(0, specificationScore - deduction);
                if (f.severity === 'CRITICAL') {
                    hardVetoReasons.push(`Critical Specification Defect: ${f.description} (${f.file})`);
                }
                break;
            case 'SECURITY':
                securityScore = Math.max(0, securityScore - deduction);
                if (f.severity === 'CRITICAL') {
                    hardVetoReasons.push(`Critical Security Defect: ${f.description} (${f.file})`);
                }
                break;
            case 'PACKAGING':
            case 'SYNTAX':
                packagingScore = Math.max(0, packagingScore - deduction);
                if (f.severity === 'CRITICAL') {
                    hardVetoReasons.push(`Critical Packaging Defect: ${f.description} (${f.file})`);
                }
                break;
            default:
                maintainabilityScore = Math.max(0, maintainabilityScore - deduction);
        }
    }

    const overallScore = Math.round(
        architectureScore * 0.35 +
        specificationScore * 0.25 +
        securityScore * 0.20 +
        packagingScore * 0.15 +
        maintainabilityScore * 0.05
    );

    const hardVetoTriggered = hardVetoReasons.length > 0 || overallScore < 80;

    return {
        scorecard: {
            architectureScore,
            specificationScore,
            securityScore,
            packagingScore,
            maintainabilityScore,
            overallScore
        },
        hardVetoTriggered,
        hardVetoReasons
    };
}

/** Inputs of {@link buildArbiterPrompt}. */
export interface ArbiterPromptInput {
    language: string;
    yamlContent: string;
    files: GeneratedFileCandidate[];
    deterministic: DeterministicGateResult;
    specialistResults: Record<string, SpecialistAgentResult>;
    codeGenPromptTemplate: string;
    baseline: { scorecard: EvaluationScorecard; hardVetoTriggered: boolean; hardVetoReasons: string[] };
    skillTargetTable: string;
    sparkVersion?: string;
}

/**
 * Builds the Lead Arbiter prompt.
 *
 * @param input - Evaluation inputs, baseline scores, and the node-to-skill table.
 * @returns Prompt text.
 */
export function buildArbiterPrompt(input: ArbiterPromptInput): string {
    const sparkVersion = input.sparkVersion ?? DEFAULT_SPARK_VERSION;
    return `You are the Lead Arbiter & Quality Assurance Synthesizer Agent for the Apache Spark (PySpark 4.x) Code Generator Integration Test Suite.

Your tasks:
1. Synthesize the reports from the Deterministic Gate and Specialist Sub-Agents:
   - SparkArchitectureAgent (Catalyst optimization, read_sources / transform / write_sinks separation, cache pairing, foreachBatch idempotency, watermarks, Managed Spark classpath)
   - PipelineSpecificationAgent (YAML property completeness, custom instructions, runtime variables, jobConfig)
   - SecurityAndHardeningAgent (Secret Manager, container hardening, shell quoting)
   - PackagingAndDeployabilityAgent (dependencies, manifest syntax, runner scripts)
2. Ground-Truth Validation & False-Positive Reconciliation:
   - Carefully cross-reference each specialist finding against the generated line-numbered code below.
   - If a finding is a FALSE POSITIVE (e.g. the sub-agent claimed a property was omitted or insecure, but the line-numbered code actually implements it correctly), list its ID in \`rejectedFindingIds\` with a clear explanation in \`rejectedFindings\`.
   - **AUTHORITATIVE APACHE SPARK VERSION GROUNDING**: Target Apache Spark runtime version is \`${sparkVersion}\` (PySpark 4.x).
3. Actionable Skill Remediation Directives:
   - When the test fails or exhibits real defects, DO NOT patch or rewrite the generated code. Fix the codegen skill instead.
   - Emit one \`skillRemediationDirectives\` entry per root cause. Each entry targets exactly ONE skill file \`resources/codegen-skills/<skillId>/SKILL.md\`.
   - DEDUPLICATION & CONSOLIDATION: If several findings share one root cause, emit ONE directive and list every finding id in \`findingIds\`.
   - SKILL SELECTION:
     - If the defect belongs to one pipeline node, set \`nodeId\` to that node id from the table below. The harness resolves the skill from the node sparkTransform, so set \`skillId\` to the skill listed for that node.
     - If the defect is cross-cutting (project layout, Catalyst rules, streaming rules, runner scripts), leave \`nodeId\` empty and set \`skillId\` to the matching cross-cutting skill (for example \`core-pyspark\`, \`core-catalyst-optimization\`, \`core-streaming\`, or a \`runner-*\` skill).
     - Use only ids from the "Valid skill ids" list. The harness drops any other id.
   - CONTENT: \`finding\` states the defect in one sentence. \`targetSection\` names the existing SKILL.md heading to change (or the new heading to add). \`editInstruction\` gives the exact rule text to add or replace, written as a direct imperative rule. \`rationale\` explains why the rule prevents the defect.

=== NODE -> SKILL MAP ===
${input.skillTargetTable}
=========================

=== TARGET LANGUAGE ===
${input.language}
=======================

=== INPUT PIPELINE YAML ===
${input.yamlContent}

=== GENERATED SOURCE CODE & ARTIFACTS (LINE NUMBERED) ===
${formatFilesWithLineNumbers(input.files)}
==========================================================

=== DETERMINISTIC PRE-FLIGHT RESULTS ===
Syntax Valid: ${input.deterministic.syntaxValid}
Syntax Errors: ${JSON.stringify(input.deterministic.syntaxErrors)}
Secret Scan Passed: ${input.deterministic.secretScanPassed}
Secret Findings: ${JSON.stringify(input.deterministic.secretFindings)}

=== SPECIALIST SUB-AGENT REPORTS ===
${JSON.stringify(input.specialistResults, null, 2)}

=== CURRENT CODE GENERATION PROMPT (ASSEMBLED FROM SKILLS) ===
${input.codeGenPromptTemplate}
===============================================

=== INITIAL RAW BASELINE SCORES & VETOES ===
Hard Veto Triggered: ${input.baseline.hardVetoTriggered}
Hard Veto Reasons: ${JSON.stringify(input.baseline.hardVetoReasons)}
Scorecard: ${JSON.stringify(input.baseline.scorecard)}

Provide your synthesis in JSON.`;
}

/** JSON response schema of the Lead Arbiter. */
export const ARBITER_RESPONSE_SCHEMA = {
    type: 'object',
    properties: {
        isGood: {
            type: 'boolean',
            description: 'Final verdict: true only if code satisfies all requirements and has no unaddressed critical defects'
        },
        explanation: {
            type: 'string',
            description: 'Executive synthesis summarizing why the test passed or failed'
        },
        rejectedFindingIds: {
            type: 'array',
            description: 'IDs of any specialist findings determined to be false positives',
            items: { type: 'string' }
        },
        rejectedFindings: {
            type: 'array',
            description: 'Detailed justifications for each rejected false-positive finding',
            items: {
                type: 'object',
                properties: {
                    id: { type: 'string' },
                    reason: { type: 'string' }
                },
                required: ['id', 'reason']
            }
        },
        skillRemediationDirectives: {
            type: 'array',
            description: 'One directive per root cause. Each edits one resources/codegen-skills/<skillId>/SKILL.md file',
            items: {
                type: 'object',
                properties: {
                    skillId: { type: 'string', description: 'Skill id from the Valid skill ids list, e.g. io-kafka or core-streaming' },
                    nodeId: { type: 'string', description: 'Pipeline node id when the defect belongs to one node' },
                    findingIds: { type: 'array', items: { type: 'string' }, description: 'Confirmed finding ids that this directive fixes' },
                    finding: { type: 'string', description: 'One-sentence statement of the defect' },
                    targetSection: { type: 'string', description: 'SKILL.md heading to change or add' },
                    editInstruction: { type: 'string', description: 'Exact imperative rule text to add or replace in the skill file' },
                    rationale: { type: 'string', description: 'Why this rule stops the generator from repeating the defect' }
                },
                required: ['skillId', 'finding', 'targetSection', 'editInstruction', 'rationale']
            }
        }
    },
    required: ['isGood', 'explanation', 'skillRemediationDirectives']
};

/**
 * Runs the Lead Arbiter agent to synthesize the final evaluation and generate
 * Skill Remediation Directives targeting codegen skill files.
 *
 * @param ai - Initialized GoogleGenAI client.
 * @param language - Target language label.
 * @param yamlContent - Original pipeline YAML.
 * @param files - All generated files.
 * @param deterministic - Deterministic gate result.
 * @param specialistResults - Specialist results keyed by agent name.
 * @param codeGenPromptTemplate - Full generator prompt.
 * @param skillCatalog - Skill catalog. Defaults to the repository catalog.
 * @returns The final evaluation result.
 */
export async function synthesizeEvaluation(
    ai: GoogleGenAI,
    language: 'Java' | 'Python',
    yamlContent: string,
    files: GeneratedFileCandidate[],
    deterministic: DeterministicGateResult,
    specialistResults: Record<string, SpecialistAgentResult>,
    codeGenPromptTemplate: string,
    skillCatalog?: SkillCatalogLike
): Promise<MultiAgentEvaluationResult> {
    const allFindings: EvaluatorFinding[] = [
        ...deterministic.secretFindings,
        ...Object.values(specialistResults).flatMap(r => r.findings)
    ];

    const initialBaseline = calculateBaselineScorecard(deterministic, allFindings);
    const specialistsList = Object.values(specialistResults);
    const allSpecialistsPassed = specialistsList.length > 0 && specialistsList.every(r => r.passed);
    const hasMissingDeliverables = (deterministic.missingDeliverables?.length ?? 0) > 0;

    if (
        deterministic.syntaxValid &&
        deterministic.secretScanPassed &&
        !hasMissingDeliverables &&
        allFindings.length === 0 &&
        allSpecialistsPassed &&
        !initialBaseline.hardVetoTriggered
    ) {
        return {
            isGood: true,
            scorecard: initialBaseline.scorecard,
            hardVetoTriggered: false,
            hardVetoReasons: [],
            allFindings: [],
            confirmedFindings: [],
            rejectedFindings: [],
            specialistResults,
            skillRemediationDirectives: [],
            promptRemediationDirectives: [],
            explanation: 'All deterministic pre-flight checks and specialist sub-agent evaluations passed with zero findings.'
        };
    }

    // NOTE: Load the catalog only when the arbiter runs. A missing skills folder fails fast.
    const catalog = skillCatalog ?? getCodegenSkillCatalog(defaultSkillsRoot());
    const skillIds = catalog.all().map(skill => skill.name);
    const targets = resolveNodeSkillTargets(yamlContent, catalog);

    const prompt = buildArbiterPrompt({
        language,
        yamlContent,
        files,
        deterministic,
        specialistResults,
        codeGenPromptTemplate,
        baseline: initialBaseline,
        skillTargetTable: formatSkillTargetTable(targets, skillIds),
    });

    const response = await generateContentWithRetry(ai, {
        model: DEFAULT_GEMINI_MODEL,
        contents: prompt,
        config: {
            responseMimeType: 'application/json',
            responseSchema: ARBITER_RESPONSE_SCHEMA,
            temperature: 0.1
        }
    });

    if (!response.text) {
        throw new Error('Empty response from Lead Arbiter Agent');
    }

    const parsed = JSON.parse(response.text);

    // Filter out rejected false-positive findings
    const rejectedIds = new Set<string>(parsed.rejectedFindingIds || []);
    const confirmedFindings = allFindings.filter(f => !rejectedIds.has(f.id));

    // Recalculate scorecard and hard vetoes on confirmed findings only
    const finalBaseline = calculateBaselineScorecard(deterministic, confirmedFindings);
    const finalIsGood = !finalBaseline.hardVetoTriggered && parsed.isGood;

    const rejectedFindings: RejectedFinding[] = parsed.rejectedFindings || [];

    const { directives, dropped } = normalizeSkillRemediationDirectives(
        parsed.skillRemediationDirectives,
        targets,
        new Set(skillIds),
        new Set(confirmedFindings.map(f => f.id))
    );
    dropped.forEach(d => console.warn(`   [LeadArbiter] ⚠️ Dropped skill directive: ${d.reason}`));

    return {
        isGood: finalIsGood,
        scorecard: finalBaseline.scorecard,
        hardVetoTriggered: finalBaseline.hardVetoTriggered,
        hardVetoReasons: finalBaseline.hardVetoReasons,
        allFindings,
        confirmedFindings,
        rejectedFindings,
        specialistResults,
        skillRemediationDirectives: directives,
        promptRemediationDirectives: toPromptRemediationDirectives(directives),
        explanation: parsed.explanation
    };
}
