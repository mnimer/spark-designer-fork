/**
 * Code Generation Skill Catalog
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Loads the bundled code generation skills (`resources/codegen-skills/<name>/`) once and
 * selects the skills that one pipeline needs. Each skill folder holds:
 * - `SKILL.md`: strict frontmatter plus the language-neutral rules (see `skillFile.ts`).
 * - `<language>.md` (optional): SDK-specific rules, one file per skill language.
 * - `templates/<language>/<file>` (optional, runner skills): file templates that the
 *   prompt builder inlines as fenced blocks.
 *
 * Selection rule (`select`): a skill is selected when ALL of these are true:
 * 1. `usedBy` contains the caller.
 * 2. `languages` contains `all` or the pipeline language.
 * 3. `runners` contains `all` or a runner that the project must support.
 * 4. One of: the kind is always on (`common`, `language`, `runner`, `review`, `gate`), the
 *    skill is an always-on `crosscutting` skill (no `transforms` and no `preloadWhen`),
 *    `transforms` intersects the graph's `spark_transform` set, or `preloadWhen` intersects
 *    the features.
 * The result is ordered by kind (`SKILL_KIND_ORDER`), then by name.
 *
 * Architectural decisions / notes for future sessions:
 * - Skills are the single source of prompt text. Code only selects and orders them.
 * - Invalid metadata fails fast at load time. A silently dropped skill is worse than an error.
 * - Unknown files next to `SKILL.md` throw, so a typo such as `pyhton.md` cannot hide rules.
 * - `metadata.sources` and `metadata.validated` are bookkeeping for humans and tests. They
 *   never reach the prompt.
 */

import * as fs from 'fs';
import * as path from 'path';
import { parseSkillFile, ParsedSkillFile } from './skillFile';
import {
    ALL_SKILL_LANGUAGES,
    ALWAYS_ON_KINDS,
    CODEGEN_RESOURCE_PATHS,
    CROSSCUTTING_KIND,
    KNOWN_PIPELINE_FEATURES,
    SKILL_CALLERS,
    SKILL_KIND_ORDER,
    SKILL_RUNNERS,
    SKILL_WILDCARD,
    SkillCaller,
    SkillKind,
    SkillLanguage,
    SkillRunner,
    TRANSFORM_ALIAS_PREFIXES,
} from './codegenConfig';

/** One template file of a runner skill. */
export interface SkillTemplate {
    /** File name that the model must produce (for example `Dockerfile`). */
    fileName: string;
    /** Raw template text. */
    content: string;
}

/** A loaded skill with validated metadata. */
export interface CodegenSkill {
    /** Skill id (equals the folder name). */
    name: string;
    /** One-sentence description. */
    description: string;
    /** Language-neutral markdown body of `SKILL.md`. */
    body: string;
    /** Prompt-order kind. */
    kind: SkillKind;
    /** Callers that load the skill. */
    usedBy: SkillCaller[];
    /** `all` or skill language ids. */
    languages: string[];
    /** `all` or runner ids. */
    runners: string[];
    /** Catalog `sparkTransform` values that select the skill. */
    transforms: string[];
    /** Pipeline features that select the skill. */
    preloadWhen: string[];
    /** Where the content came from (`prompt`, URLs). Never emitted into the prompt. */
    sources: string[];
    /** True only after a generated project that uses the skill compiled and ran. */
    validated: boolean;
    /** SDK-specific reference text per language. */
    languageFiles: Partial<Record<SkillLanguage, string>>;
    /** Runner templates per language, sorted by file name. */
    templates: Partial<Record<SkillLanguage, SkillTemplate[]>>;
}

/** Extra files loaded next to `SKILL.md`. */
export interface SkillAttachments {
    /** SDK-specific reference text per language. */
    languageFiles?: Partial<Record<SkillLanguage, string>>;
    /** Runner templates per language. */
    templates?: Partial<Record<SkillLanguage, SkillTemplate[]>>;
}

/** Input of `select`. Produced from the redacted, parsed `.beam` YAML. */
export interface SelectionRequest {
    /** Which call loads the skills. */
    caller: SkillCaller;
    /** Pipeline language. */
    language: SkillLanguage;
    /** Every runner that the project must support. */
    runners: ReadonlySet<SkillRunner>;
    /** Every node `data.spark_transform` in the graph. */
    transforms: ReadonlySet<string>;
    /** Detected pipeline features. */
    features: ReadonlySet<string>;
}

/** One selected skill plus the parts that belong to the pipeline language. */
export interface SelectedSkill {
    /** The skill. */
    skill: CodegenSkill;
    /** Reference text for the pipeline language, if the skill has one. */
    languageFile?: string;
    /** Templates for the pipeline language (empty when none). */
    templates: SkillTemplate[];
}

/** Result of resolving one `spark_transform` value against the catalog. */
export interface TransformResolution {
    /** Catalog `sparkTransform` value, or the trimmed input when nothing matched. */
    canonical: string;
    /** True when the input was not an exact catalog value but matched one. */
    aliased: boolean;
    /** True when a skill owns the canonical value. */
    known: boolean;
}

/**
 * Reduces a transform name to a comparison key: lowercase, letters and digits only, and
 * without a trailing `io` (so `KafkaIO`, `kafka`, and `Kafka_IO` share one key).
 *
 * @param value Transform name.
 * @returns The comparison key.
 */
export function transformKey(value: string): string {
    return value.toLowerCase().replace(/[^a-z0-9]/g, '').replace(/io$/, '');
}

/** Allowed `usedBy` values. */
const KNOWN_CALLERS: ReadonlySet<string> = new Set(Object.values(SKILL_CALLERS));
/** Allowed `kind` values. */
const KNOWN_KINDS: ReadonlySet<string> = new Set(SKILL_KIND_ORDER);
/** Allowed `languages` values. */
const KNOWN_LANGUAGES: ReadonlySet<string> = new Set([SKILL_WILDCARD, ...ALL_SKILL_LANGUAGES]);
/** Allowed `runners` values. */
const KNOWN_RUNNERS: ReadonlySet<string> = new Set([SKILL_WILDCARD, ...Object.values(SKILL_RUNNERS)]);
/** Allowed `validated` values. */
const BOOLEAN_VALUES: Readonly<Record<string, boolean>> = { true: true, false: false };

/**
 * Reads a list metadata field and checks every entry against an allow-list.
 *
 * @param skill Skill name (for errors).
 * @param metadata Parsed metadata.
 * @param field Field name.
 * @param allowed Allowed values, or undefined to accept any non-empty string.
 * @returns The list, or an empty list when the field is absent.
 * @throws Error when the field is not a list or holds an unknown value.
 */
function readList(
    skill: string,
    metadata: ParsedSkillFile['metadata'],
    field: string,
    allowed?: ReadonlySet<string>
): string[] {
    const raw = metadata[field];
    if (raw === undefined) {
        return [];
    }
    if (!Array.isArray(raw)) {
        throw new Error(`Code generation skill '${skill}': metadata.${field} must be a list, for example [a, b].`);
    }
    // Reject values outside the allow-list so that a typo cannot disable a skill.
    const unknown = allowed ? raw.filter((value) => !allowed.has(value)) : [];
    if (unknown.length > 0) {
        throw new Error(
            `Code generation skill '${skill}': metadata.${field} has unknown value(s) ${unknown.join(', ')}. Allowed: ${[...(allowed as ReadonlySet<string>)].join(', ')}.`
        );
    }
    return raw;
}

/**
 * Reads a required, non-empty list metadata field.
 *
 * @param skill Skill name (for errors).
 * @param metadata Parsed metadata.
 * @param field Field name.
 * @param allowed Allowed values.
 * @returns The non-empty list.
 * @throws Error when the field is missing or empty.
 */
function readRequiredList(
    skill: string,
    metadata: ParsedSkillFile['metadata'],
    field: string,
    allowed: ReadonlySet<string>
): string[] {
    const list = readList(skill, metadata, field, allowed);
    if (list.length === 0) {
        throw new Error(`Code generation skill '${skill}': metadata.${field} is required and must not be empty.`);
    }
    return list;
}

/**
 * Builds a validated skill from `SKILL.md` content and its attachments.
 *
 * @param content `SKILL.md` content.
 * @param sourceLabel Path or label for errors.
 * @param attachments Language files and templates loaded next to `SKILL.md`.
 * @param expectedName Folder name that `name` must equal, if known.
 * @returns The validated skill.
 * @throws Error when the frontmatter or metadata is invalid.
 */
export function buildSkill(
    content: string,
    sourceLabel: string,
    attachments: SkillAttachments = {},
    expectedName?: string
): CodegenSkill {
    const parsed = parseSkillFile(content, sourceLabel, expectedName);
    const { name, metadata } = parsed;

    // 1. Scalar fields: kind (required) and validated (optional boolean).
    const kind = metadata.kind;
    if (typeof kind !== 'string' || !KNOWN_KINDS.has(kind)) {
        throw new Error(`Code generation skill '${name}': metadata.kind must be one of ${SKILL_KIND_ORDER.join(', ')}.`);
    }
    const validatedRaw = metadata.validated ?? 'false';
    if (typeof validatedRaw !== 'string' || !(validatedRaw in BOOLEAN_VALUES)) {
        throw new Error(`Code generation skill '${name}': metadata.validated must be true or false.`);
    }

    // 2. List fields with allow-lists. `transforms` values are checked against the palette
    //    catalog by the invariant unit test, because the extension cannot import the catalog.
    return {
        name,
        description: parsed.description,
        body: parsed.body,
        kind: kind as SkillKind,
        usedBy: readRequiredList(name, metadata, 'usedBy', KNOWN_CALLERS) as SkillCaller[],
        languages: readRequiredList(name, metadata, 'languages', KNOWN_LANGUAGES),
        runners: readRequiredList(name, metadata, 'runners', KNOWN_RUNNERS),
        transforms: readList(name, metadata, 'transforms'),
        preloadWhen: readList(name, metadata, 'preloadWhen', KNOWN_PIPELINE_FEATURES),
        sources: readList(name, metadata, 'sources'),
        validated: BOOLEAN_VALUES[validatedRaw],
        languageFiles: attachments.languageFiles ?? {},
        templates: attachments.templates ?? {},
    };
}

/**
 * Reads the language files and runner templates of one skill folder.
 *
 * @param skillDir Absolute skill folder path.
 * @returns The attachments.
 * @throws Error when the folder holds an unknown file or folder.
 */
function readAttachments(skillDir: string): SkillAttachments {
    const languageFiles: Partial<Record<SkillLanguage, string>> = {};
    const templates: Partial<Record<SkillLanguage, SkillTemplate[]>> = {};
    const languageFileNames = new Map<string, SkillLanguage>(
        ALL_SKILL_LANGUAGES.map((language) => [`${language}${CODEGEN_RESOURCE_PATHS.languageFileExtension}`, language])
    );

    for (const entry of fs.readdirSync(skillDir, { withFileTypes: true })) {
        // SKILL.md is parsed by the caller.
        if (entry.name === CODEGEN_RESOURCE_PATHS.skillFileName) {
            continue;
        }
        // Language reference file (python.md, java.md).
        const language = languageFileNames.get(entry.name);
        if (entry.isFile() && language) {
            languageFiles[language] = fs.readFileSync(path.join(skillDir, entry.name), 'utf-8').trim();
            continue;
        }
        // Runner templates: templates/<language>/<file>.
        if (entry.isDirectory() && entry.name === CODEGEN_RESOURCE_PATHS.templatesDir) {
            Object.assign(templates, readTemplates(path.join(skillDir, entry.name)));
            continue;
        }
        throw new Error(`Code generation skill folder ${skillDir} has an unknown entry '${entry.name}'.`);
    }
    return { languageFiles, templates };
}

/**
 * Reads `templates/<language>/<file>` of a runner skill.
 *
 * @param templatesDir Absolute `templates` folder path.
 * @returns Templates per language, sorted by file name.
 * @throws Error when a sub-folder is not a known skill language.
 */
function readTemplates(templatesDir: string): Partial<Record<SkillLanguage, SkillTemplate[]>> {
    const result: Partial<Record<SkillLanguage, SkillTemplate[]>> = {};
    for (const entry of fs.readdirSync(templatesDir, { withFileTypes: true })) {
        const language = entry.name as SkillLanguage;
        if (!entry.isDirectory() || !ALL_SKILL_LANGUAGES.includes(language)) {
            throw new Error(`Template folder ${templatesDir} has an unknown entry '${entry.name}'.`);
        }
        const languageDir = path.join(templatesDir, entry.name);
        result[language] = fs
            .readdirSync(languageDir)
            .sort()
            .map((fileName) => ({ fileName, content: fs.readFileSync(path.join(languageDir, fileName), 'utf-8') }));
    }
    return result;
}

/**
 * Compares two skills by kind order, then by name. Gives a deterministic prompt order.
 *
 * @param a First skill.
 * @param b Second skill.
 * @returns Negative, zero, or positive.
 */
export function compareSkills(a: CodegenSkill, b: CodegenSkill): number {
    const byKind = SKILL_KIND_ORDER.indexOf(a.kind) - SKILL_KIND_ORDER.indexOf(b.kind);
    return byKind !== 0 ? byKind : a.name.localeCompare(b.name);
}

/**
 * Catalog of bundled code generation skills.
 */
export class CodegenSkillCatalog {
    /** Exact catalog transform values that a skill owns. */
    private readonly knownTransforms: ReadonlySet<string>;
    /** Comparison key -> catalog transform values with that key. */
    private readonly transformsByKey: ReadonlyMap<string, readonly string[]>;

    /**
     * Creates the catalog. Use `load` or `fromSkills`.
     *
     * @param skills Validated skills with unique names.
     */
    private constructor(private readonly skills: readonly CodegenSkill[]) {
        // Index every transform value once for `resolveTransform`.
        const known = new Set(skills.flatMap((skill) => skill.transforms));
        const byKey = new Map<string, string[]>();
        for (const transform of known) {
            const key = transformKey(transform);
            byKey.set(key, [...(byKey.get(key) ?? []), transform]);
        }
        this.knownTransforms = known;
        this.transformsByKey = byKey;
    }

    /**
     * Resolves a `spark_transform` value from a `.beam` file to the catalog value that a
     * skill owns. Legacy files can hold other spellings (`ReadFromKafka`, `kafka`,
     * `Kafka IO`). Resolution order:
     * 1. Exact catalog value.
     * 2. Same comparison key (case, punctuation, and a trailing `IO` ignored).
     * 3. Same key after removing one SDK prefix (`ReadFrom`, `WriteTo`, `Read`, `Write`).
     * A step matches only when exactly one catalog value fits, so the resolver never guesses.
     *
     * @param raw Raw `spark_transform` value.
     * @returns The canonical value and whether it was aliased or known.
     */
    public resolveTransform(raw: string): TransformResolution {
        const trimmed = raw.trim();
        // 1. Exact match.
        if (this.knownTransforms.has(trimmed)) {
            return { canonical: trimmed, aliased: false, known: true };
        }
        // 2. and 3. Key match, then key match without one SDK prefix.
        const key = transformKey(trimmed);
        const candidates = [key, ...TRANSFORM_ALIAS_PREFIXES.filter((p) => key.startsWith(p) && key.length > p.length).map((p) => key.slice(p.length))];
        for (const candidate of candidates) {
            const matches = this.transformsByKey.get(candidate) ?? [];
            if (matches.length === 1) {
                return { canonical: matches[0], aliased: true, known: true };
            }
        }
        return { canonical: trimmed, aliased: false, known: false };
    }

    /**
     * Loads every `<skillsRoot>/<folder>/SKILL.md` and its attachments.
     *
     * @param skillsRoot Absolute path of `resources/codegen-skills`.
     * @returns The catalog.
     * @throws Error when the folder is missing, empty, or holds an invalid skill.
     */
    public static load(skillsRoot: string): CodegenSkillCatalog {
        if (!fs.existsSync(skillsRoot)) {
            throw new Error(`Code generation skills directory not found: ${skillsRoot}`);
        }
        // Only folders are skills. Files at the root (README.md) are documentation.
        const skills = fs
            .readdirSync(skillsRoot, { withFileTypes: true })
            .filter((entry) => entry.isDirectory())
            .map((entry) => entry.name)
            .sort()
            .map((folder) => {
                const skillDir = path.join(skillsRoot, folder);
                const skillFile = path.join(skillDir, CODEGEN_RESOURCE_PATHS.skillFileName);
                if (!fs.existsSync(skillFile)) {
                    throw new Error(`Code generation skill folder ${skillDir} has no ${CODEGEN_RESOURCE_PATHS.skillFileName}.`);
                }
                return buildSkill(fs.readFileSync(skillFile, 'utf-8'), skillFile, readAttachments(skillDir), folder);
            });
        if (skills.length === 0) {
            throw new Error(`No code generation skills found in ${skillsRoot}`);
        }
        return CodegenSkillCatalog.fromSkills(skills);
    }

    /**
     * Builds a catalog from skills (tests and `load`).
     *
     * @param skills Skills.
     * @returns The catalog.
     * @throws Error when two skills share a name.
     */
    public static fromSkills(skills: readonly CodegenSkill[]): CodegenSkillCatalog {
        const seen = new Set<string>();
        for (const skill of skills) {
            if (seen.has(skill.name)) {
                throw new Error(`Duplicate code generation skill name '${skill.name}'.`);
            }
            seen.add(skill.name);
        }
        return new CodegenSkillCatalog([...skills].sort(compareSkills));
    }

    /**
     * Returns every skill in prompt order.
     *
     * @returns A copy of the skill list.
     */
    public all(): CodegenSkill[] {
        return [...this.skills];
    }

    /**
     * Selects the skills for one pipeline, in prompt order.
     *
     * @param request Caller, language, runners, transforms, and features.
     * @returns Selected skills with the language-specific parts attached.
     */
    public select(request: SelectionRequest): SelectedSkill[] {
        return this.skills
            // 1. The caller must load this skill.
            .filter((skill) => skill.usedBy.includes(request.caller))
            // 2. The skill must apply to the pipeline language.
            .filter((skill) => skill.languages.includes(SKILL_WILDCARD) || skill.languages.includes(request.language))
            // 3. The skill must apply to at least one runner that the project supports.
            .filter(
                (skill) =>
                    skill.runners.includes(SKILL_WILDCARD) ||
                    skill.runners.some((runner) => request.runners.has(runner as SkillRunner))
            )
            // 4. The skill must be always on, or match a transform or a feature.
            .filter(
                (skill) =>
                    ALWAYS_ON_KINDS.has(skill.kind) ||
                    (skill.kind === CROSSCUTTING_KIND && skill.transforms.length === 0 && skill.preloadWhen.length === 0) ||
                    skill.transforms.some((transform) => request.transforms.has(transform)) ||
                    skill.preloadWhen.some((feature) => request.features.has(feature))
            )
            // NOTE: `this.skills` is already sorted by `compareSkills`, so the order holds.
            .map((skill) => ({
                skill,
                languageFile: skill.languageFiles[request.language],
                templates: skill.templates[request.language] ?? [],
            }));
    }
}

/** Catalogs loaded in this process, keyed by skills root. Skills ship with the extension. */
const catalogCache = new Map<string, CodegenSkillCatalog>();

/**
 * Returns the catalog for a skills root. Loads it on first use and reuses it afterwards.
 *
 * @param skillsRoot Absolute path of `resources/codegen-skills`.
 * @returns The cached catalog.
 */
export function getCodegenSkillCatalog(skillsRoot: string): CodegenSkillCatalog {
    let catalog = catalogCache.get(skillsRoot);
    if (!catalog) {
        catalog = CodegenSkillCatalog.load(skillsRoot);
        catalogCache.set(skillsRoot, catalog);
    }
    return catalog;
}

/**
 * Clears the catalog cache (tests only).
 */
export function clearCodegenSkillCatalogCache(): void {
    catalogCache.clear();
}
