/**
 * Type definitions and contracts for the Multi-Agent Code Evaluation Harness.
 *
 * Purpose:
 * Defines domain models, sub-agent result structures, scoring rubrics, and
 * prompt remediation guidance types used during integration testing of the
 * Apache Spark (PySpark 4.x) code generation pipeline.
 *
 * Architectural decisions:
 * - Sub-agents output structured findings with severity levels: 'CRITICAL' (hard veto),
 *   'MAJOR' (potential bug or anti-pattern), and 'MINOR' (readability or stylistic improvement).
 * - Findings support 1-indexed line numbers and verbatim code evidence to eliminate hallucinated locations.
 * - Supports Requirement Traceability Matrix (RTM) verifications for 100% specification coverage audit.
 * - The Lead Arbiter calculates category-specific rubric scores (0-100) on confirmed findings
 *   (after filtering out false positives with recorded justification), and produces
 *   actionable Skill Remediation Directives. Each directive targets one
 *   resources/codegen-skills/<skill>/SKILL.md file, resolved from the node sparkTransform.
 */

export type FindingSeverity = 'CRITICAL' | 'MAJOR' | 'MINOR';

export interface EvaluatorFinding {
    id: string;
    category: 'ARCHITECTURE' | 'SECURITY' | 'PACKAGING' | 'SYNTAX' | 'IDIOMATIC_SPARK' | 'SPECIFICATION';
    severity: FindingSeverity;
    file: string;
    line?: number;
    evidence?: string;
    description: string;
    rationale: string;
}

export interface RequirementVerification {
    reqId: string;
    category: string;
    description: string;
    status: 'VERIFIED' | 'PARTIAL' | 'MISSING';
    evidence?: string;
}

export interface SpecialistAgentResult {
    agentName: string;
    passed: boolean;
    findings: EvaluatorFinding[];
    summary: string;
    requirementVerifications?: RequirementVerification[];
}

export interface DeterministicGateResult {
    syntaxValid: boolean;
    syntaxErrors: string[];
    secretScanPassed: boolean;
    secretFindings: EvaluatorFinding[];
    placeholderErrors?: string[];
    jsonErrors?: string[];
    shellErrors?: string[];
    missingDeliverables?: string[];
}

export interface PromptRemediationDirective {
    /**
     * Skill file to change, relative to the repository root, for example
     * `resources/codegen-skills/io-kafka/python.md` or `resources/codegen-skills/graph-wiring/SKILL.md`.
     */
    targetSkill: string;
    targetSection: string;
    recommendedRuleText: string;
    rationale: string;
}

/**
 * Actionable instruction to edit one codegen skill file so that the generator stops making an error.
 * The arbiter resolves the skill from the `sparkTransform` of the affected node when a node is known.
 */
export interface SkillRemediationDirective {
    /** Skill id (folder name under `resources/codegen-skills/`), for example `io-kafka`. */
    skillId: string;
    /** Skill file to edit, relative to the repository root: `resources/codegen-skills/<skillId>/SKILL.md`. */
    skillFile: string;
    /** Pipeline node id that exhibited the defect, when the defect belongs to one node. */
    nodeId?: string;
    /** Catalog `sparkTransform` of that node. */
    sparkTransform?: string;
    /** Ids of the confirmed findings that this directive fixes. */
    findingIds: string[];
    /** Short statement of the defect in the generated code. */
    finding: string;
    /** Section of the skill file to change, for example `## 2. Batch and Streaming Read Modes`. */
    targetSection: string;
    /** Concrete edit: the exact rule text to add or replace in the skill file. */
    editInstruction: string;
    /** Why the edit stops the generator from repeating the defect. */
    rationale: string;
}

export interface EvaluationScorecard {
    architectureScore: number;    // 0-100
    specificationScore?: number;  // 0-100 (YAML specification & property conformance)
    securityScore: number;        // 0-100
    packagingScore: number;       // 0-100
    maintainabilityScore: number; // 0-100
    overallScore: number;         // 0-100
}

export interface RejectedFinding {
    id: string;
    reason: string;
}

export interface MultiAgentEvaluationResult {
    isGood: boolean;
    scorecard: EvaluationScorecard;
    hardVetoTriggered: boolean;
    hardVetoReasons: string[];
    allFindings: EvaluatorFinding[];
    confirmedFindings?: EvaluatorFinding[];
    rejectedFindings?: RejectedFinding[];
    specialistResults: Record<string, SpecialistAgentResult>;
    /** Structured directives that target `resources/codegen-skills/<skill>/SKILL.md`. */
    skillRemediationDirectives: SkillRemediationDirective[];
    /** Legacy flat view of `skillRemediationDirectives` for the Markdown test reporter. */
    promptRemediationDirectives: PromptRemediationDirective[];
    explanation: string;
}

/**
 * Executes a Gemini model call with exponential backoff on HTTP 429 / RESOURCE_EXHAUSTED.
 */
export async function generateContentWithRetry(
    ai: any,
    params: any,
    maxRetries = 4,
    initialDelayMs = 4000
): Promise<any> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await ai.models.generateContent(params);
        } catch (err: any) {
            const errStr = String(err?.message || err);
            const isRateLimit = errStr.includes('429') || errStr.includes('RESOURCE_EXHAUSTED') || err?.status === 429;
            if (isRateLimit && attempt < maxRetries) {
                const delayMs = initialDelayMs * Math.pow(2, attempt - 1);
                console.warn(`   [RateLimit] ⚠️ 429 Resource Exhausted. Retrying in ${delayMs / 1000}s (attempt ${attempt}/${maxRetries})...`);
                await new Promise(resolve => setTimeout(resolve, delayMs));
            } else {
                throw err;
            }
        }
    }
}
