/**
 * Code Generation Prompt Builder
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Builds the ONE code generation prompt from the topic skills. The extension
 * (`codeGeneration.ts`) and the live integration runner (`codegen-runner.ts`) both call
 * this builder, so the prompt that the tests send equals the prompt that users send.
 *
 * Prompt order:
 * ```
 * <codegen-common body + language specifics>   role, output contract, global rules
 * ## Pipeline YAML                              fenced block with the redacted YAML
 * <redaction notice, only if values were redacted>
 * ## Skill: <name>                              every other selected skill, in kind order:
 *                                               language, runner, io, transform,
 *                                               crosscutting, review, gate (last)
 * ```
 *
 * Architectural decisions / notes for future sessions:
 * - Fix F7 is kept: `{{beam_version}}` is resolved in SKILL TEXT ONLY, before the YAML is
 *   appended. The user YAML is never substituted, so a property value that contains
 *   `{{beam_version}}` reaches the model unchanged.
 * - All substitutions use replacer functions. `$&` or `$'` in any text stays literal.
 * - `{{pipeline_name}}`, `{{pipeline_class_name}}`, and `{{gcs_staging_path}}` stay in the
 *   prompt. They are instructions to the model (`MODEL_FACING_PLACEHOLDERS`).
 * - Runner templates are inlined as fenced blocks under `#### Template: <file>`.
 * - Prompt text lives only in `resources/codegen-skills/**`. This class only orders it.
 */

import { buildRedactionPromptNotice, redactPipelineDocument } from '../credentialRedaction';
import { DEFAULT_SPARK_VERSION, type SupportedLanguage } from '../shared/types';
import { CodegenSkillCatalog, SelectedSkill, SkillTemplate, getCodegenSkillCatalog } from './CodegenSkillCatalog';
import {
    BEAM_VERSION_PLACEHOLDER,
    CODEGEN_COMMON_SKILL_NAME,
    PROMPT_SECTIONS,
    SKILL_CALLERS,
    SkillCaller,
} from './codegenConfig';
import { DetectedPipeline, detectPipelineFeatures } from './PipelineFeatureDetector';

/** Input of `CodegenPromptBuilder.build`. */
export interface PromptBuildInput {
    /** The parsed, redacted pipeline (used for skill selection only). */
    parsedPipeline: unknown;
    /** The redacted pipeline YAML text. Inserted verbatim. */
    redactedYaml: string;
    /** Property paths that the redaction step replaced. */
    redactedPaths: readonly string[];
    /** Resolved generation language. */
    language: SupportedLanguage;
    /** Resolved Apache Beam SDK version. Falls back to `FALLBACK_BEAM_VERSION`. */
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
}

/** Placeholder pattern, global, for `{{beam_version}}`. */
const BEAM_VERSION_PATTERN = new RegExp(BEAM_VERSION_PLACEHOLDER.replace(/[{}]/g, '\\$&'), 'g');

/** Markdown code fence. */
const FENCE = '```';

/**
 * Resolves `{{beam_version}}` in skill text.
 *
 * @param text Skill text.
 * @param beamVersion Resolved Beam version.
 * @returns The text with every placeholder replaced.
 */
export function resolveBeamVersion(text: string, beamVersion: string): string {
    // NOTE: replacer function, so a `$` sequence in the version is never expanded.
    return text.replace(BEAM_VERSION_PATTERN, () => beamVersion);
}

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
    return `${PROMPT_SECTIONS.languageHeadingPrefix}${language}${PROMPT_SECTIONS.languageHeadingSuffix}`;
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
 * The caller resolves `{{beam_version}}` afterwards.
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
     * @param input Parsed pipeline, redacted YAML, redacted paths, language, Beam version.
     * @returns The prompt, the selected skill names, and the detection facts.
     * @throws Error when the catalog has no `codegen-common` skill for the language.
     */
    public build(input: PromptBuildInput): BuiltPrompt {
        const beamVersion = input.beamVersion || DEFAULT_SPARK_VERSION;
        const caller = input.caller ?? SKILL_CALLERS.generator;

        // 1. Detect features and select skills.
        const detection = detectPipelineFeatures(input.parsedPipeline, input.language, (raw) =>
            this.catalog.resolveTransform(raw)
        );
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

        // 2. Render skill text and resolve {{beam_version}} BEFORE the YAML joins (fix F7).
        const head = resolveBeamVersion(renderSkill(common, input.language, false), beamVersion);
        const skillSections = rest.map((entry) => resolveBeamVersion(renderSkill(entry, input.language, true), beamVersion));

        // 3. Assemble: common, YAML (verbatim), redaction notice, remaining skills.
        //    Empty sections (for example a skill without a body) are dropped.
        const notice = buildRedactionPromptNotice([...input.redactedPaths]).trim();
        const sections = [head, renderPipelineYaml(input.redactedYaml), notice, ...skillSections].filter(
            (section) => section.length > 0
        );

        return {
            prompt: sections.join(PROMPT_SECTIONS.separator),
            selectedSkills: selected.map((entry) => entry.skill.name),
            templateFiles: selected.flatMap((entry) => entry.templates.map((template) => template.fileName)),
            detection,
        };
    }
}

/** Input of {@link buildPromptFromPipelineYaml}. */
export interface PipelinePromptRequest {
    /** Absolute path of `resources/codegen-skills`. */
    skillsRoot: string;
    /** Raw (unredacted) pipeline YAML from the `.beam` file or the webview. */
    yamlText: string;
    /** Resolved generation language. */
    language: SupportedLanguage;
    /** Resolved Apache Beam SDK version. */
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
 * @param request Skills root, raw YAML, language, Beam version.
 * @returns The prompt, the selected skills, the detection facts, and the redacted paths.
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
        beamVersion: request.beamVersion,
        caller: request.caller,
    });
    return { ...built, redactedPaths };
}
