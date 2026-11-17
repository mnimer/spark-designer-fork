/**
 * Code Generation Prompt Builder
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Builds the ONE code generation prompt from the topic skills and the deterministic
 * pre-Gemini `ExecutionPlan` (`src/shared/executionPlanner.ts`). The extension
 * (`codeGeneration.ts`) and the live integration runner (`codegen-runner.ts`) both call
 * this builder, so the prompt that the tests send equals the prompt that users send.
 *
 * Prompt order:
 * ```
 * <codegen-common body + language specifics>   role, output contract, global rules
 * ## Pipeline YAML                              fenced block with the redacted YAML
 * ## Execution Plan                             deterministic markdown summary + fenced JSON
 * <redaction notice, only if values were redacted>
 * ## Skill: <name>                              every other selected skill, in kind order:
 *                                               language, runner, io, transform,
 *                                               crosscutting, review, gate (last)
 * ```
 *
 * Architectural decisions / notes for future sessions:
 * - Fix F7 is kept: `{{spark_version}}` (and legacy `{{beam_version}}`) is resolved in SKILL
 *   TEXT ONLY, before the YAML is appended. The user YAML is never substituted, so a property
 *   value that contains `{{spark_version}}` or `{{beam_version}}` reaches the model unchanged.
 * - All substitutions use replacer functions. `$&` or `$'` in any text stays literal.
 * - `{{pipeline_name}}`, `{{pipeline_class_name}}`, and `{{gcs_staging_path}}` stay in the
 *   prompt. They are instructions to the model (`MODEL_FACING_PLACEHOLDERS`).
 * - `buildExecutionPlan` runs on the parsed, redacted pipeline and injects a deterministic
 *   `## Execution Plan` section right after `## Pipeline YAML` so the model follows the exact
 *   topological node order, batch vs. streaming mode, `.cache()` / `.unpersist()` points,
 *   `foreachBatch` sink groupings, DLQ split mechanisms, and runtime artifacts.
 * - Runner templates are inlined as fenced blocks under `#### Template: <file>`.
 * - Prompt text lives only in `resources/codegen-skills/**`. This class only orders it.
 */

import { buildRedactionPromptNotice, redactPipelineDocument } from '../credentialRedaction';
import type { DagEdge, DagNode } from '../shared/dagValidator';
import { buildExecutionPlan, type ExecutionPlan } from '../shared/executionPlanner';
import { DEFAULT_SPARK_VERSION, type SupportedLanguage } from '../shared/types';
import { CodegenSkillCatalog, SelectedSkill, SkillTemplate, getCodegenSkillCatalog } from './CodegenSkillCatalog';
import {
    BEAM_VERSION_PLACEHOLDER,
    CODEGEN_COMMON_SKILL_NAME,
    PROMPT_SECTIONS,
    SKILL_CALLERS,
    SPARK_VERSION_PLACEHOLDER,
    SkillCaller,
} from './codegenConfig';
import { DetectedPipeline, detectPipelineFeatures } from './PipelineFeatureDetector';

/** Input of `CodegenPromptBuilder.build`. */
export interface PromptBuildInput {
    /** The parsed, redacted pipeline (used for skill selection and ExecutionPlan computation). */
    parsedPipeline: unknown;
    /** The redacted pipeline YAML text. Inserted verbatim. */
    redactedYaml: string;
    /** Property paths that the redaction step replaced. */
    redactedPaths: readonly string[];
    /** Resolved generation language. */
    language: SupportedLanguage;
    /** Resolved Apache Spark runtime version. Falls back to `DEFAULT_SPARK_VERSION`. */
    sparkVersion?: string;
    /** Legacy alias for `sparkVersion`. */
    beamVersion?: string;
    /** Which call loads the skills. Defaults to the generator. */
    caller?: SkillCaller;
}

/** Result of `CodegenPromptBuilder.build`. */
export interface BuiltPrompt {
    /** The final prompt string. */
    prompt: string;
    /** Names of the selected skills, in prompt order. Log these for debugging. */
    selectedSkills: string[];
    /** File names of every inlined runner template (the runner files the project needs). */
    templateFiles: string[];
    /** Detection facts that drove the selection. */
    detection: DetectedPipeline;
    /** Deterministic pre-computed execution plan injected into the prompt. */
    executionPlan: ExecutionPlan;
}

/** Placeholder pattern, global, for `{{spark_version}}` and legacy `{{beam_version}}`. */
const SPARK_VERSION_PATTERN = new RegExp(
    `(${SPARK_VERSION_PLACEHOLDER.replace(/[{}]/g, '\\$&')}|${BEAM_VERSION_PLACEHOLDER.replace(/[{}]/g, '\\$&')})`,
    'g'
);

/** Markdown code fence. */
const FENCE = '```';

/**
 * Narrows an unknown value to a plain object record.
 *
 * @param value Candidate value.
 * @returns Plain object record or undefined.
 */
function asRecord(value: unknown): Record<string, unknown> | undefined {
    return value !== null && typeof value === 'object' && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : undefined;
}

/**
 * Resolves `{{spark_version}}` (and legacy `{{beam_version}}`) in skill text.
 *
 * @param text Skill text.
 * @param sparkVersion Resolved Spark version.
 * @returns The text with every placeholder replaced.
 */
export function resolveSparkVersion(text: string, sparkVersion: string): string {
    // NOTE: replacer function, so a `$` sequence in the version is never expanded.
    return text.replace(SPARK_VERSION_PATTERN, () => sparkVersion);
}

/**
 * Backwards-compatible alias for {@link resolveSparkVersion}.
 *
 * @param text Skill text.
 * @param version Resolved runtime version.
 * @returns The text with every placeholder replaced.
 */
export const resolveBeamVersion = resolveSparkVersion;

/**
 * Wraps text in a fenced code block. Uses a longer fence when the text itself holds one.
 *
 * @param text Block content.
 * @param infoString Optional language tag (for example `yaml`).
 * @returns The fenced block.
 */
export function fence(text: string, infoString = ''): string {
    // Use a fence that is longer than any backtick run inside the text.
    const longestRun = Math.max(0, ...(text.match(/`+/g) ?? []).map((run) => run.length));
    const marker = longestRun >= FENCE.length ? '`'.repeat(longestRun + 1) : FENCE;
    const body = text.endsWith('\n') ? text : `${text}\n`;
    return `${marker}${infoString}\n${body}${marker}`;
}

/**
 * Returns the display name of a language for the `### <Language> specifics` heading.
 *
 * @param language Canonical language.
 * @returns The heading text.
 */
function languageHeading(language: SupportedLanguage): string {
    const displayLanguage = language === 'PySpark' ? 'Python' : language;
    return `${PROMPT_SECTIONS.languageHeadingPrefix}${displayLanguage}${PROMPT_SECTIONS.languageHeadingSuffix}`;
}

/**
 * Renders one template as a heading plus a fenced block.
 *
 * @param template Runner template.
 * @returns The rendered template section.
 */
function renderTemplate(template: SkillTemplate): string {
    return `${PROMPT_SECTIONS.templateHeadingPrefix}${template.fileName}\n${fence(template.content)}`;
}

/**
 * Renders one selected skill: optional heading, body, language specifics, templates.
 * The caller resolves `{{spark_version}}` afterwards.
 *
 * @param selected Selected skill.
 * @param language Canonical language (for the specifics heading).
 * @param withHeading True to emit `## Skill: <name>` (false for codegen-common).
 * @returns The rendered section.
 */
export function renderSkill(selected: SelectedSkill, language: SupportedLanguage, withHeading: boolean): string {
    const parts: string[] = [];
    if (withHeading) {
        parts.push(`${PROMPT_SECTIONS.skillHeadingPrefix}${selected.skill.name}`);
    }
    if (selected.skill.body) {
        parts.push(selected.skill.body);
    }
    if (selected.languageFile) {
        parts.push(`${languageHeading(language)}\n\n${selected.languageFile}`);
    }
    parts.push(...selected.templates.map(renderTemplate));
    return parts.join(PROMPT_SECTIONS.separator);
}

/**
 * Builds the pipeline YAML section. The YAML is inserted verbatim.
 *
 * @param redactedYaml Redacted pipeline YAML.
 * @returns The section text.
 */
function renderPipelineYaml(redactedYaml: string): string {
    return [PROMPT_SECTIONS.pipelineYamlHeading, PROMPT_SECTIONS.pipelineYamlIntro, fence(redactedYaml, 'yaml')].join(
        PROMPT_SECTIONS.separator
    );
}

/**
 * Computes the deterministic `ExecutionPlan` from the parsed pipeline object.
 *
 * @param parsedPipeline Parsed pipeline YAML document.
 * @returns Complete `ExecutionPlan`.
 */
export function computePipelineExecutionPlan(parsedPipeline: unknown): ExecutionPlan {
    const root = asRecord(parsedPipeline) ?? {};
    const rawNodes = Array.isArray(root.nodes) ? root.nodes : [];
    const rawEdges = Array.isArray(root.edges) ? root.edges : [];
    const nodes = rawNodes.filter((n): n is DagNode => asRecord(n) !== undefined);
    const edges = rawEdges.filter((e): e is DagEdge => asRecord(e) !== undefined);
    const jobConfig = asRecord(root.jobConfig);
    return buildExecutionPlan(nodes, edges, jobConfig);
}

/**
 * Formats the deterministic `ExecutionPlan` into a markdown summary plus a serialized JSON
 * block containing all planner fields (`sortedNodeIds`, `isStreaming`, `cacheNodes`,
 * `streamingSinkGroups`, `dlqRoutes`, and `runtimeJars` / `runtimePackages` / `sparkProperties`).
 *
 * @param plan Computed `ExecutionPlan`.
 * @returns Rendered `## Execution Plan` section text.
 */
export function renderExecutionPlan(plan: ExecutionPlan): string {
    const isStreaming = plan.pipelineMode === 'streaming';
    const orderSummary =
        plan.sortedNodeIds.length > 0 ? plan.sortedNodeIds.map((id) => `\`${id}\``).join(' -> ') : '(empty graph)';

    const cacheSummary =
        plan.cacheDirectives.length > 0
            ? plan.cacheDirectives
                  .map(
                      (directive) =>
                          `\`${directive.nodeId}\` (reason: \`${directive.reason}\`, outDegree: ${directive.outDegree}, unpersistAfter: [${directive.unpersistAfterNodeIds.map((id) => `\`${id}\``).join(', ')}])`
                  )
                  .join('; ')
            : 'none';

    const sinkGroupsSummary =
        plan.streamingSinkGroups.length > 0
            ? plan.streamingSinkGroups
                  .map(
                      (group) =>
                          `\`${group.id}\` (mode: \`${group.mode}\`, sourceSubqueryNodeId: \`${group.sourceSubqueryNodeId}\`, sinks: [${group.sinkIds.map((id) => `\`${id}\``).join(', ')}], requiresBatchCache: ${group.requiresBatchCache}, idempotentTxnOptions: ${group.idempotentTxnOptions}${group.txnAppId ? `, txnAppId: \`${group.txnAppId}\`` : ''})`
                  )
                  .join('; ')
            : 'none';

    const dlqSummary =
        plan.dlqSplitPoints.length > 0
            ? plan.dlqSplitPoints
                  .map(
                      (split) =>
                          `\`${split.nodeId}\` (\`${split.sparkTransform}\`, mechanism: \`${split.mechanism}\`, validTargets: [${split.validTargetIds.map((id) => `\`${id}\``).join(', ')}], errorTargets: [${split.errorTargetIds.map((id) => `\`${id}\``).join(', ')}])`
                  )
                  .join('; ')
            : 'none';

    const { profile, tier, gcsJars, mavenPackages, pipPackages, sparkProperties, pythonDataSources, preinstalledConnectors } =
        plan.runtimeArtifacts;
    const sparkPropEntries = Object.entries(sparkProperties);
    const propsText =
        sparkPropEntries.length > 0
            ? sparkPropEntries.map(([k, v]) => `\`${k}=${v}\``).join(', ')
            : 'none';

    const summaryBullets = [
        `- **Topological Execution Order (\`sortedNodeIds\`)**: ${orderSummary}`,
        `- **Execution Mode (\`pipelineMode\` / \`isStreaming\`)**: \`${plan.pipelineMode}\` (\`isStreaming: ${isStreaming}\`, streaming nodes: ${plan.streamingNodeIds.length > 0 ? plan.streamingNodeIds.map((id) => `\`${id}\``).join(', ') : 'none'})`,
        `- **Batch Fan-Out Caching (\`cacheNodes\` / \`unpersistPoints\`)**: ${cacheSummary}`,
        `- **Streaming Sink Groups (\`streamingSinkGroups\`)**: ${sinkGroupsSummary}`,
        `- **DLQ Split Routes (\`dlqRoutes\` / \`dlqSplitPoints\`)**: ${dlqSummary}`,
        `- **Runtime Profile**: \`${profile.id}\` (Runner: \`${profile.runner}\`, Spark \`${profile.sparkVersion}\`, Python \`${profile.pythonVersion}\`, Java \`${profile.javaVersion}\`, Scala \`${profile.scalaVersion}\`, Tier: \`${tier}\`)`,
        `- **Runtime JARs (\`runtimeJars\`)**: ${gcsJars.length > 0 ? gcsJars.map((j) => `\`${j}\``).join(', ') : 'none'}`,
        `- **Runtime Maven Packages (\`runtimePackages\`)**: ${mavenPackages.length > 0 ? mavenPackages.map((p) => `\`${p}\``).join(', ') : 'none'}`,
        `- **Pre-installed Managed Spark Connectors (NEVER pass via \`--packages\`)**: ${preinstalledConnectors.length > 0 ? preinstalledConnectors.map((c) => `\`${c}\``).join(', ') : 'none'}`,
        `- **Python PyPI Packages (\`pipPackages\`)**: ${pipPackages.length > 0 ? pipPackages.map((p) => `\`${p}\``).join(', ') : 'none'}`,
        `- **Spark 4.0 Python DataSources (\`pythonDataSources\`)**: ${pythonDataSources.length > 0 ? pythonDataSources.map((d) => `\`${d}\``).join(', ') : 'none'}`,
        `- **Required Spark / Dataproc Properties (\`sparkProperties\`)**: ${propsText}`,
    ].join('\n');

    const serializedPlan = {
        ...plan,
        isStreaming,
        cacheNodes: plan.cacheDirectives,
        dlqRoutes: plan.dlqSplitPoints,
        runtimeJars: gcsJars,
        runtimePackages: mavenPackages,
        sparkProperties,
    };

    return [
        PROMPT_SECTIONS.executionPlanHeading,
        PROMPT_SECTIONS.executionPlanIntro,
        summaryBullets,
        fence(JSON.stringify(serializedPlan, null, 2), 'json'),
    ].join(PROMPT_SECTIONS.separator);
}

/**
 * Builds code generation prompts from the skill catalog.
 */
export class CodegenPromptBuilder {
    /**
     * Creates a builder over a loaded catalog.
     *
     * @param catalog Skill catalog (load once with `getCodegenSkillCatalog`).
     */
    constructor(private readonly catalog: CodegenSkillCatalog) {}

    /**
     * Builds the prompt for one pipeline.
     *
     * @param input Parsed pipeline, redacted YAML, redacted paths, language, Spark version.
     * @returns The prompt, the selected skill names, the detection facts, and the ExecutionPlan.
     * @throws Error when the catalog has no `codegen-common` skill for the language.
     */
    public build(input: PromptBuildInput): BuiltPrompt {
        const sparkVersion = input.sparkVersion || input.beamVersion || DEFAULT_SPARK_VERSION;
        const caller = input.caller ?? SKILL_CALLERS.generator;

        // 1. Detect features, compute deterministic ExecutionPlan, and select skills.
        const detection = detectPipelineFeatures(input.parsedPipeline, input.language, (raw) =>
            this.catalog.resolveTransform(raw)
        );
        const executionPlan = computePipelineExecutionPlan(input.parsedPipeline);
        const selected = this.catalog.select({
            caller,
            language: detection.language,
            runners: detection.runners,
            transforms: detection.transforms,
            features: detection.features,
        });
        const common = selected.find((entry) => entry.skill.name === CODEGEN_COMMON_SKILL_NAME);
        if (!common) {
            throw new Error(`Skill '${CODEGEN_COMMON_SKILL_NAME}' is missing for ${input.language} (${caller}).`);
        }
        const rest = selected.filter((entry) => entry !== common);

        // 2. Render skill text and resolve {{spark_version}} BEFORE the YAML joins (fix F7).
        const head = resolveSparkVersion(renderSkill(common, input.language, false), sparkVersion);
        const skillSections = rest.map((entry) => resolveSparkVersion(renderSkill(entry, input.language, true), sparkVersion));

        // 3. Assemble: common, YAML (verbatim), ExecutionPlan, redaction notice, remaining skills.
        //    Empty sections (for example a skill without a body) are dropped.
        const notice = buildRedactionPromptNotice([...input.redactedPaths]).trim();
        const sections = [
            head,
            renderPipelineYaml(input.redactedYaml),
            renderExecutionPlan(executionPlan),
            notice,
            ...skillSections,
        ].filter((section) => section.length > 0);

        return {
            prompt: sections.join(PROMPT_SECTIONS.separator),
            selectedSkills: selected.map((entry) => entry.skill.name),
            templateFiles: selected.flatMap((entry) => entry.templates.map((template) => template.fileName)),
            detection,
            executionPlan,
        };
    }
}

/** Input of {@link buildPromptFromPipelineYaml}. */
export interface PipelinePromptRequest {
    /** Absolute path of `resources/codegen-skills`. */
    skillsRoot: string;
    /** Raw (unredacted) pipeline YAML from the `.spark` file or the webview. */
    yamlText: string;
    /** Resolved generation language. */
    language: SupportedLanguage;
    /** Resolved Apache Spark runtime version. */
    sparkVersion?: string;
    /** Legacy alias for `sparkVersion`. */
    beamVersion?: string;
    /** Which call loads the skills. Defaults to the generator. */
    caller?: SkillCaller;
}

/** Result of {@link buildPromptFromPipelineYaml}. */
export interface PipelinePrompt extends BuiltPrompt {
    /** Property paths that the redaction step replaced. */
    redactedPaths: string[];
}

/**
 * Redacts the pipeline, then builds the prompt. This is the ONE entry point that the
 * extension and the integration runner share, so both send the same prompt.
 *
 * NOTE: redaction runs BEFORE the builder. `gemini.log` stores the prompt, so a credential
 * must never reach the builder.
 *
 * @param request Skills root, raw YAML, language, Spark version.
 * @returns The prompt, the selected skills, the detection facts, the ExecutionPlan, and the redacted paths.
 * @throws Error when the YAML cannot be parsed (redaction refuses to guess).
 */
export function buildPromptFromPipelineYaml(request: PipelinePromptRequest): PipelinePrompt {
    const { value: redactedYaml, redactedPaths, document } = redactPipelineDocument(request.yamlText);
    const builder = new CodegenPromptBuilder(getCodegenSkillCatalog(request.skillsRoot));
    const built = builder.build({
        parsedPipeline: document,
        redactedYaml,
        redactedPaths,
        language: request.language,
        sparkVersion: request.sparkVersion ?? request.beamVersion,
        caller: request.caller,
    });
    return { ...built, redactedPaths };
}
