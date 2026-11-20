/**
 * Reviewer Pass (extension wiring of the optional reviewer)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Connects the provider-agnostic `CodeReviewer` to the extension: reads the VS Code setting,
 * loads the reviewer and fix prompt templates, builds the reviewer context from the
 * reviewer-selected skills, runs the read-only checks, and makes the review and fix calls
 * with the Gemini client.
 *
 * Architectural decisions / notes for future sessions:
 * - OFF by default. `isReviewerEnabled` reads `sparkDesigner.codegen.enableReviewer` and
 *   falls back to `CODEGEN_REVIEWER_ENABLED_BY_DEFAULT`.
 * - No `vscode` import: the caller passes the configuration accessor. So the scratch A/B
 *   benchmark and unit tests can load this module in plain Node.
 * - The review and fix calls use the same model and output-token limit as the generator
 *   (`buildCodegenModelConfig`). The fix call uses the generator's JSON schema.
 * - The model client is a narrow interface (`ReviewerModelClient`), so tests inject a fake.
 */

import { DEFAULT_GEMINI_MODEL } from '../constants';
import type { SupportedLanguage } from '../shared/types';
import { buildPromptFromPipelineYaml } from './CodegenPromptBuilder';
import { ReviewOutcome, REVIEW_REPORT_SCHEMA, reviewAndFix } from './CodeReviewer';
import {
    buildCodegenModelConfig,
    CODEGEN_REVIEWER_ENABLED_BY_DEFAULT,
    CODEGEN_REVIEWER_SETTING,
    SKILL_CALLERS,
    toSkillLanguage,
} from './codegenConfig';
import { GeneratedFile, ReadOnlyCheckRunner } from './ReadOnlyCheckRunner';

/** The one SDK method that the reviewer needs (subset of `GoogleGenAI.models`). */
export interface ReviewerModelClient {
    models: {
        generateContent(request: {
            model: string;
            contents: string;
            config: Record<string, unknown>;
        }): Promise<{ text?: string }>;
    };
}

/** Input of {@link runReviewerPass}. */
export interface ReviewerPassOptions {
    /** Gemini client. */
    ai: ReviewerModelClient;
    /** Files from the generation call. */
    files: GeneratedFile[];
    /** The generation prompt (fix calls reuse it). */
    generationPrompt: string;
    /** Raw pipeline YAML (redacted again inside the builder). */
    yamlText: string;
    /** Resolved language. */
    language: SupportedLanguage;
    /** Pipeline base name. */
    baseName: string;
    /** Resolved Spark runtime version. */
    sparkVersion?: string;
    /** Legacy alias for `sparkVersion`. */
    beamVersion?: string;
    /** Absolute path of `resources/codegen-skills`. */
    skillsRoot: string;
    /** Reviewer prompt template text. */
    reviewerTemplate: string;
    /** Fix prompt template text. */
    fixTemplate: string;
    /** Generator response schema (`code-gen-schema.json`). */
    responseSchema: unknown;
    /** Progress sink (webview and output channel). */
    onProgress?(message: string): void;
    /** Cancellation signal. */
    abortSignal?: AbortSignal;
    /** Check runner (tests inject one with a fake executor). */
    checkRunner?: ReadOnlyCheckRunner;
}

/**
 * Reads the reviewer setting.
 *
 * @param getConfiguration Configuration accessor (`vscode.workspace.getConfiguration`).
 * @returns True when the user turned the reviewer on.
 */
export function isReviewerEnabled(
    getConfiguration: (section: string) => { get<T>(key: string, defaultValue: T): T } | undefined
): boolean {
    const value = getConfiguration(CODEGEN_REVIEWER_SETTING.section)?.get<boolean>(
        CODEGEN_REVIEWER_SETTING.key,
        CODEGEN_REVIEWER_ENABLED_BY_DEFAULT
    );
    return value === true;
}

/**
 * Runs the optional reviewer over generated files.
 *
 * @param options Client, files, prompts, pipeline, templates, and callbacks.
 * @returns The review outcome (files, reports, unresolved findings, error).
 */
export async function runReviewerPass(options: ReviewerPassOptions): Promise<ReviewOutcome> {
    // 1. Reviewer context: the reviewer-selected skills with the pipeline YAML.
    const { sparkVersion: explicitSparkVersion, beamVersion: legacyRuntimeVersion } = options;
    const reviewerPrompt = buildPromptFromPipelineYaml({
        skillsRoot: options.skillsRoot,
        yamlText: options.yamlText,
        language: options.language,
        sparkVersion: explicitSparkVersion ?? legacyRuntimeVersion,
        caller: SKILL_CALLERS.reviewer,
    });
    const checkRunner = options.checkRunner ?? new ReadOnlyCheckRunner();

    /**
     * Calls the model with a schema and returns the response text.
     *
     * @param prompt Prompt text.
     * @param schema Response schema.
     * @returns Response text.
     */
    const call = async (prompt: string, schema: unknown): Promise<string> => {
        const response = await options.ai.models.generateContent({
            model: DEFAULT_GEMINI_MODEL,
            contents: prompt,
            config: { ...buildCodegenModelConfig(schema), abortSignal: options.abortSignal },
        });
        if (!response.text) {
            throw new Error('Empty response from Gemini during review.');
        }
        return response.text;
    };

    // 2. Review -> fix loop.
    return reviewAndFix(
        {
            files: options.files,
            generationPrompt: options.generationPrompt,
            reviewerContext: reviewerPrompt.prompt,
            reviewerTemplate: options.reviewerTemplate,
            fixTemplate: options.fixTemplate,
        },
        {
            reviewFn: (prompt) => call(prompt, REVIEW_REPORT_SCHEMA),
            fixFn: (prompt) => call(prompt, options.responseSchema),
            runChecks: (files) =>
                checkRunner.run({
                    files,
                    language: toSkillLanguage(options.language),
                    baseName: options.baseName,
                    runnerFiles: reviewerPrompt.templateFiles,
                    paradigm: reviewerPrompt.detection.paradigm,
                }),
            onProgress: options.onProgress,
            abortSignal: options.abortSignal,
        }
    );
}
