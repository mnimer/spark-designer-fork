/**
 * Code Generation Configuration
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Single source of truth for skill-assembled PySpark 4.x code generation (`src/codegen/*`).
 * The skill loader, the skill catalog, the pipeline feature detector, the prompt builder,
 * and the optional reviewer read every shared literal from this file (project rule: strict
 * DRY, no magic constants).
 *
 * Architectural decisions / notes for future sessions:
 * - Code generation is ONE model call. The prompt is assembled from the topic skills in
 *   `resources/codegen-skills/<name>/SKILL.md`. A multi-agent workflow was measured 2.3-2.5x
 *   slower in the sister project (Visual Agent Designer) and was rejected.
 * - Skill language ids derive from `SupportedLanguage` in ONE place
 *   (`toSkillLanguage`).
 * - Parity rule: every project receives every runner script that its language supports
 *   (`LANGUAGE_RUNNERS`). The runner chosen in the Deployment tab does NOT filter skills.
 * - The extension cannot import the webview palette catalog (`tsconfig` rootDir is `src`).
 *   So `STREAMING_SOURCE_TRANSFORMS` and `PYTHON_DATASOURCE_TRANSFORMS` are kept here once,
 *   and the invariant unit test checks every entry against the catalog.
 * - The reviewer pass is opt-in (`CODEGEN_REVIEWER_ENABLED_BY_DEFAULT = false`).
 */

import type { SupportedLanguage } from '../shared/types';

/** Callers that load skills (skill frontmatter `metadata.usedBy`). */
export const SKILL_CALLERS = {
    generator: 'generator',
    reviewer: 'reviewer',
} as const;

/** A caller that loads skills. */
export type SkillCaller = (typeof SKILL_CALLERS)[keyof typeof SKILL_CALLERS];

/**
 * Skill kinds in prompt order (skill frontmatter `metadata.kind`).
 *
 * NOTE: the array order IS the prompt order. `gate` is last because the verification
 * checklist summarizes what to check before output.
 */
export const SKILL_KIND_ORDER = [
    'common',
    'language',
    'runner',
    'io',
    'transform',
    'crosscutting',
    'review',
    'gate',
] as const;

/** A skill kind. */
export type SkillKind = (typeof SKILL_KIND_ORDER)[number];

/**
 * Kinds that are selected for every pipeline (after the caller/language/runner filters).
 *
 * NOTE: `runner` is here because the runner filter (step 3 of the selection rule) already
 * decides which runner skills apply. Runner skills declare no transforms.
 */
export const ALWAYS_ON_KINDS: ReadonlySet<SkillKind> = new Set<SkillKind>(['common', 'language', 'runner', 'review', 'gate']);

/** Kind whose skills are always on when they declare no `transforms` and no `preloadWhen`. */
export const CROSSCUTTING_KIND: SkillKind = 'crosscutting';

/** Kinds that own catalog `sparkTransform` values (1:1 invariant). */
export const TRANSFORM_OWNER_KINDS: ReadonlySet<SkillKind> = new Set<SkillKind>(['io', 'transform']);

/** Wildcard value for `metadata.languages` and `metadata.runners`. */
export const SKILL_WILDCARD = 'all';

/** Skill language ids, one per `SupportedLanguage`. */
export const SKILL_LANGUAGE_IDS: Readonly<Record<SupportedLanguage, string>> = {
    PySpark: 'python',
} as const;

/** A skill language id (`python`). */
export type SkillLanguage = (typeof SKILL_LANGUAGE_IDS)[SupportedLanguage];

/** Every supported skill language id. */
export const ALL_SKILL_LANGUAGES: readonly SkillLanguage[] = Object.values(SKILL_LANGUAGE_IDS);

/** Runner ids used in skill `metadata.runners`. */
export const SKILL_RUNNERS = {
    local: 'local',
    dataprocCluster: 'dataproc-cluster',
    serverlessSpark: 'serverless-spark',
} as const;

/** A runner id. */
export type SkillRunner = (typeof SKILL_RUNNERS)[keyof typeof SKILL_RUNNERS];

/** Runners that each language supports. Every project gets every runner script (parity). */
export const LANGUAGE_RUNNERS: Readonly<Record<SkillLanguage, readonly SkillRunner[]>> = {
    python: [SKILL_RUNNERS.local, SKILL_RUNNERS.dataprocCluster, SKILL_RUNNERS.serverlessSpark],
};

/** Pipeline features that gate skills (skill frontmatter `metadata.preloadWhen`). */
export const PIPELINE_FEATURES = {
    sideInputs: 'side_inputs',
    multiOutput: 'multi_output',
    streaming: 'streaming',
    customInstructions: 'custom_instructions',
    pythonDatasource: 'python_datasource',
    xlang: 'xlang',
} as const;

/** A pipeline feature. */
export type PipelineFeature = (typeof PIPELINE_FEATURES)[keyof typeof PIPELINE_FEATURES];

/** Every known pipeline feature. */
export const KNOWN_PIPELINE_FEATURES: ReadonlySet<string> = new Set<string>(Object.values(PIPELINE_FEATURES));

/** Edge handle ids that the detector inspects. */
export const EDGE_HANDLES = {
    /** `targetHandle` of an edge that feeds a side input. */
    sideInput: 'side_input',
    /** `sourceHandle` values of a multi-output (valid / error) step. */
    multiOutput: ['valid', 'error'] as readonly string[],
} as const;

/** Node properties that carry custom code instructions for the model. */
export const CUSTOM_INSTRUCTION_KEYS = {
    /** `node.data.system_instructions`. */
    dataKey: 'system_instructions',
    /** `node.data.properties.<key>` values that also hold custom logic. */
    propertyKeys: ['codeGenerationPrompt', 'prompt'] as readonly string[],
} as const;

/**
 * SDK-style prefixes that a legacy `.spark` or `.beam` file can put in front of a connector name
 * (for example `ReadFromKafka`, `WriteToBigQuery`). The transform resolver strips them,
 * longest first, before it matches the name against the skill catalog.
 */
export const TRANSFORM_ALIAS_PREFIXES: readonly string[] = ['readfrom', 'writeto', 'read', 'write'];

/** `sparkTransform` of the explicit event-time watermark step (`df.withWatermark(...)`). */
export const WATERMARK_TRANSFORM = 'Watermark';

/** `sparkTransform` of the Spark 4.0 stateful streaming step (`transformWithState`). */
export const STATEFUL_STREAMING_TRANSFORM = 'StatefulTransform';

/** Legacy `sparkTransform` of the windowing step. Kept as an alias for backwards compatibility. */
export const WINDOW_INTO_TRANSFORM = 'WindowInto';

/** Step transforms that inherently mark a pipeline as streaming. */
export const STREAMING_STEP_TRANSFORMS: ReadonlySet<string> = new Set<string>([
    WATERMARK_TRANSFORM,
    STATEFUL_STREAMING_TRANSFORM,
    WINDOW_INTO_TRANSFORM,
]);

/**
 * Streaming source transforms in the Apache Spark 4.x catalog (`SRC1`–`SRC8`).
 * A SOURCE node with one of these values (unless explicitly configured as `readMode: 'batch'`)
 * or any SOURCE node with `readMode: 'streaming'` marks the pipeline as streaming.
 */
export const STREAMING_SOURCE_TRANSFORMS: ReadonlySet<string> = new Set<string>([
    'KafkaSource',
    'PubSubSource',
    'KinesisSource',
    'PulsarSource',
    'EventHubsSource',
    'MqttSource',
    'RateSource',
    'SocketSource',
]);

/**
 * Transforms backed by the Spark 4.0 Python `DataSource` API or Python `foreachBatch` SDK
 * connectors in `src/spark/versionMatrix.ts`. Selecting any of these sets the
 * `python_datasource` pipeline feature so `core-python-datasource` is preloaded.
 */
export const PYTHON_DATASOURCE_TRANSFORMS: ReadonlySet<string> = new Set<string>([
    'PubSubSource',
    'PubSubSink',
    'KinesisSource',
    'KinesisSink',
    'MqttSource',
    'MqttSink',
    'FirestoreSource',
    'FirestoreSink',
    'CassandraSource',
    'CassandraSink',
    'RedisSource',
    'RedisSink',
    'ForeachBatchSink',
]);

/**
 * Transforms that need a cross-language expansion service, per language.
 */
export const XLANG_TRANSFORMS: Readonly<Record<SkillLanguage, ReadonlySet<string>>> = {
    python: new Set<string>(),
};

/** The `codegen-common` skill: role and output contract. Always first in the prompt. */
export const CODEGEN_COMMON_SKILL_NAME = 'codegen-common';

/** Resource locations relative to the `resources/` folder. */
export const CODEGEN_RESOURCE_PATHS = {
    /** Folder that holds one sub-folder per skill. */
    skillsDir: 'codegen-skills',
    /** File name of a skill definition inside its folder. */
    skillFileName: 'SKILL.md',
    /** Folder (inside a runner skill) that holds `<language>/<file>` templates. */
    templatesDir: 'templates',
    /** Extension of a language reference file (`python.md`). */
    languageFileExtension: '.md',
    /** Folder that holds the reviewer and fix prompt templates. */
    promptsDir: 'codegen-prompts',
    /** Reviewer prompt template. */
    reviewerPrompt: 'reviewer.md',
    /** Fix prompt template. */
    fixPrompt: 'fix.md',
    /** Structured output schema of the generator and fix calls. */
    responseSchema: 'code-gen-schema.json',
} as const;

/** Headings and separators that the prompt builder emits. */
export const PROMPT_SECTIONS = {
    /** Heading of the pipeline YAML section. */
    pipelineYamlHeading: '## Pipeline YAML',
    /** Instruction line above the YAML fence. */
    pipelineYamlIntro: 'Convert this pipeline definition into the complete project deliverables:',
    /** Heading of the deterministic ExecutionPlan section. */
    executionPlanHeading: '## Execution Plan',
    /** Instruction line above the ExecutionPlan summary and JSON fence. */
    executionPlanIntro:
        'Follow this deterministic pre-computed execution plan for node topological order, batch vs. streaming execution, fan-out `.cache()` / `.unpersist()` insertion, `foreachBatch` sink grouping, dead-letter queue (`_corrupt_record` / `_error`) splits, and runtime artifacts:',
    /** Prefix of every skill heading (`## Skill: core-pyspark`). */
    skillHeadingPrefix: '## Skill: ',
    /** Suffix of the language reference heading (`### PySpark specifics`). */
    languageHeadingSuffix: ' specifics',
    /** Heading level of the language reference heading. */
    languageHeadingPrefix: '### ',
    /** Heading level of an inlined runner template (`#### Template: Dockerfile`). */
    templateHeadingPrefix: '#### Template: ',
    /** Separator between prompt sections. */
    separator: '\n\n',
} as const;

/** Placeholder that the builder resolves in skill text (never in the user YAML). */
export const SPARK_VERSION_PLACEHOLDER = '{{spark_version}}';

/** Legacy placeholder alias resolved in skill text for backwards compatibility. */
export const BEAM_VERSION_PLACEHOLDER = '{{beam_version}}';

/**
 * Placeholders that stay in the prompt. They are instructions TO THE MODEL, not to the
 * builder. The invariant test fails on any other `{{...}}` token in the skills.
 */
export const MODEL_FACING_PLACEHOLDERS: readonly string[] = [
    '{{pipeline_name}}',
    '{{pipeline_class_name}}',
    '{{gcs_staging_path}}',
];

/** Model settings shared by the generator, the reviewer, and the integration runner. */
export const CODEGEN_MODEL_SETTINGS = {
    /** Response MIME type for structured output. */
    responseMimeType: 'application/json',
    /** Maximum output tokens of one call. */
    maxOutputTokens: 65536,
} as const;

/** Optional sampling and thinking settings (`@google/genai` `GenerateContentConfig`). */
export interface CodegenModelTuning {
    /** Sampling temperature. */
    temperature?: number;
    /** Thinking depth for Gemini 3.x models (`thinkingConfig.thinkingLevel`). */
    thinkingLevel?: 'MINIMAL' | 'LOW' | 'MEDIUM' | 'HIGH';
}

/**
 * Sampling and thinking settings for every code generation call (generator, reviewer, fix,
 * integration runner). An empty object keeps model defaults.
 */
export const CODEGEN_MODEL_TUNING: CodegenModelTuning = {};

/**
 * Builds the shared `generateContent` config for code generation calls.
 *
 * @param responseSchema Structured output schema.
 * @param tuning Sampling and thinking settings (defaults to `CODEGEN_MODEL_TUNING`).
 * @returns The config object (without the abort signal).
 */
export function buildCodegenModelConfig(
    responseSchema: unknown,
    tuning: CodegenModelTuning = CODEGEN_MODEL_TUNING
): Record<string, unknown> {
    // Base settings, then only the tuning keys that are set.
    const config: Record<string, unknown> = {
        responseMimeType: CODEGEN_MODEL_SETTINGS.responseMimeType,
        responseSchema,
        maxOutputTokens: CODEGEN_MODEL_SETTINGS.maxOutputTokens,
    };
    if (tuning.temperature !== undefined) {
        config.temperature = tuning.temperature;
    }
    if (tuning.thinkingLevel !== undefined) {
        config.thinkingConfig = { thinkingLevel: tuning.thinkingLevel };
    }
    return config;
}

/** VS Code setting that turns on the optional reviewer pass. */
export const CODEGEN_REVIEWER_SETTING = {
    /** Configuration section. */
    section: 'sparkDesigner',
    /** Key inside the section. */
    key: 'codegen.enableReviewer',
} as const;

/** The reviewer pass is opt-in. */
export const CODEGEN_REVIEWER_ENABLED_BY_DEFAULT = false;

/** Maximum review -> fix rounds of the optional reviewer. */
export const CODEGEN_REVIEWER_MAX_FIX_ROUNDS = 2;

/** Build file that every generated project of a language must contain. */
export const LANGUAGE_BUILD_FILES: Readonly<Record<SkillLanguage, string>> = {
    python: 'requirements.txt',
};

/** Limits of the read-only checks that feed the optional reviewer. */
export const READ_ONLY_CHECK_LIMITS = {
    /** Timeout of one check subprocess (ms). */
    timeoutMs: 60_000,
    /** Maximum characters of check output forwarded to the reviewer per check. */
    maxOutputChars: 8_000,
    /** Python interpreter used for `py_compile` and `pytest`. */
    pythonCommand: 'python3',
} as const;

/** Severity values of reviewer findings. Only `blocking` findings trigger a fix call. */
export const REVIEW_SEVERITIES = {
    blocking: 'blocking',
    minor: 'minor',
} as const;

/**
 * Converts a `SupportedLanguage` into its skill language id.
 *
 * @param language Canonical language (`PySpark`).
 * @returns Skill language id (`python`).
 */
export function toSkillLanguage(language: SupportedLanguage): SkillLanguage {
    return SKILL_LANGUAGE_IDS[language] ?? 'python';
}
