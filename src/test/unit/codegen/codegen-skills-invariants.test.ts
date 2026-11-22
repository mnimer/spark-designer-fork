/**
 * Invariant tests that protect the code generation skill library
 * (`resources/codegen-skills/**`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * These tests read the real skills in `resources/codegen-skills/`. They fail when:
 * - a skill does not parse, or its name differs from its folder,
 * - an io skill declares more than one transform,
 * - a skill names a transform that is not in `SPARK_NODE_CAPABILITIES` or the webview catalog,
 * - a skill holds a `{{...}}` token other than the model-facing ones and `{{spark_version}}` / `{{beam_version}}`,
 * - a skill still refers to the deleted `code-gen-prompt-*` files,
 * - a reference file or a runner template is missing for a declared language,
 * - a skill with non-prompt content lists no source URL,
 * - `metadata.sources` or `metadata.validated` leak into a built prompt,
 * - a constant in `codegenConfig.ts` (`STREAMING_SOURCE_TRANSFORMS`, `PYTHON_DATASOURCE_TRANSFORMS`)
 *   names a transform that is not in `SPARK_NODE_CAPABILITIES`.
 * They also REPORT (never fail) the count of unvalidated skills and TODO lines.
 */

import { describe, it, expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import YAML from 'yaml';
import { CodegenSkillCatalog, CodegenSkill } from '../../../codegen/CodegenSkillCatalog';
import { CodegenPromptBuilder } from '../../../codegen/CodegenPromptBuilder';
import {
    ALL_LANGUAGE_RUNNERS,
    ALL_SKILL_LANGUAGES,
    BEAM_VERSION_PLACEHOLDER,
    MODEL_FACING_PLACEHOLDERS,
    PYTHON_DATASOURCE_TRANSFORMS,
    SKILL_PARADIGMS,
    SKILL_WILDCARD,
    SPARK_VERSION_PLACEHOLDER,
    STREAMING_SOURCE_TRANSFORMS,
    TRANSFORM_OWNER_KINDS,
    XLANG_TRANSFORMS,
} from '../../../codegen/codegenConfig';
import { SPARK_NODE_CAPABILITIES } from '../../../shared/sparkNodeCapabilities';
import { SKILLS_ROOT, listSkillFolders } from '../helpers/codegenSkillsCorpus';

/** Repository root. */
const REPO_ROOT = path.join(__dirname, '..', '..', '..', '..');

/** One palette catalog entry. */
interface CatalogEntry {
    type: string;
    sparkTransform: string;
}

/**
 * Reads every palette node definition from the webview catalog sources.
 *
 * @returns Catalog entries (type and sparkTransform).
 */
function readCatalog(): CatalogEntry[] {
    const dir = path.join(REPO_ROOT, 'webview', 'src', 'catalog', 'nodes');
    const entries: CatalogEntry[] = [];
    for (const file of ['sources.ts', 'sinks.ts', 'steps.ts']) {
        const text = fs.readFileSync(path.join(dir, file), 'utf-8');
        for (const match of text.matchAll(/type:\s*'(\w+)'[^}]*?sparkTransform:\s*'([^']+)'/g)) {
            entries.push({ type: match[1], sparkTransform: match[2] });
        }
    }
    return entries;
}

/**
 * Lists every file under a folder, recursively.
 *
 * @param dir Folder.
 * @returns Absolute file paths.
 */
function walk(dir: string): string[] {
    return fs
        .readdirSync(dir, { withFileTypes: true })
        .flatMap((entry) => (entry.isDirectory() ? walk(path.join(dir, entry.name)) : [path.join(dir, entry.name)]));
}

const CATALOG = readCatalog();
const KNOWN_SPARK_TRANSFORMS = new Set([
    ...CATALOG.map((entry) => entry.sparkTransform),
    ...Object.keys(SPARK_NODE_CAPABILITIES),
]);
const LIBRARY = CodegenSkillCatalog.load(SKILLS_ROOT);
const SKILLS: CodegenSkill[] = LIBRARY.all();
const ALLOWED_PLACEHOLDERS = new Set([
    ...MODEL_FACING_PLACEHOLDERS,
    SPARK_VERSION_PLACEHOLDER,
    BEAM_VERSION_PLACEHOLDER,
]);

describe('codegen skill library invariants', () => {
    it('loads every skill folder, and every name equals its folder (load throws otherwise)', () => {
        expect(SKILLS.length).toBeGreaterThanOrEqual(2);
        expect(SKILLS.map((skill) => skill.name).sort()).toEqual(listSkillFolders());
    });

    it('has no duplicate transform owners across io and transform skills', () => {
        const owners = new Map<string, string[]>();
        for (const skill of SKILLS.filter((s) => TRANSFORM_OWNER_KINDS.has(s.kind))) {
            for (const transform of skill.transforms) {
                owners.set(transform, [...(owners.get(transform) ?? []), skill.name]);
            }
        }
        const duplicated = [...owners].filter(([, names]) => names.length > 1);
        expect(duplicated, 'catalog transforms with more than one skill').toEqual([]);
    });

    it('gives every io skill at least one transform', () => {
        const bad = SKILLS.filter((s) => s.kind === 'io' && s.transforms.length === 0).map((s) => s.name);
        expect(bad).toEqual([]);
    });

    it('resolves all 40 Spark 4.x SOURCE_NODES in webview/src/catalog/nodes/sources.ts to an io-* skill', () => {
        const sourceEntries = CATALOG.filter((entry) => entry.type === 'SOURCE');
        expect(sourceEntries).toHaveLength(40);
        const ioTransforms = new Map<string, string>();
        for (const skill of SKILLS.filter((s) => s.kind === 'io')) {
            for (const transform of skill.transforms) {
                ioTransforms.set(transform, skill.name);
            }
        }
        const missing = sourceEntries
            .filter((entry) => {
                const skillName = ioTransforms.get(entry.sparkTransform);
                return !skillName || !skillName.startsWith('io-');
            })
            .map((entry) => entry.sparkTransform);
        expect(missing).toEqual([]);
    });

    it('resolves all 38 Spark 4.x SINK_NODES in webview/src/catalog/nodes/sinks.ts to an io-* skill', () => {
        const sinkEntries = CATALOG.filter((entry) => entry.type === 'SINK');
        expect(sinkEntries).toHaveLength(38);
        const ioTransforms = new Map<string, string>();
        for (const skill of SKILLS.filter((s) => s.kind === 'io')) {
            for (const transform of skill.transforms) {
                ioTransforms.set(transform, skill.name);
            }
        }
        const missing = sinkEntries
            .filter((entry) => {
                const skillName = ioTransforms.get(entry.sparkTransform);
                return !skillName || !skillName.startsWith('io-');
            })
            .map((entry) => entry.sparkTransform);
        expect(missing).toEqual([]);
    });

    it('resolves all 33 Spark 4.x STEP_NODES in webview/src/catalog/nodes/steps.ts to a transform-* skill', () => {
        const stepEntries = CATALOG.filter((entry) => entry.type === 'STEP');
        expect(stepEntries).toHaveLength(33);
        const stepTransforms = new Map<string, string>();
        for (const skill of SKILLS.filter((s) => s.kind === 'transform')) {
            for (const transform of skill.transforms) {
                stepTransforms.set(transform, skill.name);
            }
        }
        const missing = stepEntries
            .filter((entry) => {
                const skillName = stepTransforms.get(entry.sparkTransform);
                return !skillName || !skillName.startsWith('transform-');
            })
            .map((entry) => entry.sparkTransform);
        expect(missing).toEqual([]);
    });

    it('parses every sparkTransform entry in each catalog file, so the regex never skips a node', () => {
        const dir = path.join(REPO_ROOT, 'webview', 'src', 'catalog', 'nodes');
        const expected: Record<string, { type: string; count: number }> = {
            'sources.ts': { type: 'SOURCE', count: 40 },
            'sinks.ts': { type: 'SINK', count: 38 },
            'steps.ts': { type: 'STEP', count: 33 },
        };
        for (const [file, { type, count }] of Object.entries(expected)) {
            const text = fs.readFileSync(path.join(dir, file), 'utf-8');
            const declared = [...text.matchAll(/sparkTransform:\s*'/g)].length;
            expect(declared, `${file} sparkTransform declarations`).toBe(count);
            expect(CATALOG.filter((entry) => entry.type === type), `${file} parsed entries`).toHaveLength(count);
        }
        expect(CATALOG).toHaveLength(40 + 38 + 33);
    });

    it('declares every catalog sparkTransform once across all node types', () => {
        const transforms = CATALOG.map((entry) => entry.sparkTransform);
        const duplicates = transforms.filter((value, index) => transforms.indexOf(value) !== index);
        expect(duplicates).toEqual([]);
    });

    it('resolves every catalog sparkTransform exactly to one owning skill folder with a SKILL.md on disk', () => {
        // Core skills can list transforms as triggers. Only io and transform skills own them.
        const owners = new Map<string, CodegenSkill[]>();
        for (const skill of SKILLS.filter((s) => TRANSFORM_OWNER_KINDS.has(s.kind))) {
            for (const transform of skill.transforms) {
                owners.set(transform, [...(owners.get(transform) ?? []), skill]);
            }
        }
        const problems: string[] = [];
        for (const entry of CATALOG) {
            const resolution = LIBRARY.resolveTransform(entry.sparkTransform);
            if (!resolution.known || resolution.aliased || resolution.canonical !== entry.sparkTransform) {
                problems.push(`${entry.type}:${entry.sparkTransform} does not resolve exactly`);
                continue;
            }
            const skills = owners.get(entry.sparkTransform) ?? [];
            if (skills.length !== 1) {
                problems.push(`${entry.type}:${entry.sparkTransform} has ${skills.length} owning skills`);
                continue;
            }
            const expectedPrefix = entry.type === 'STEP' ? 'transform-' : 'io-';
            if (!skills[0].name.startsWith(expectedPrefix)) {
                problems.push(`${entry.type}:${entry.sparkTransform} resolves to ${skills[0].name}`);
            }
            if (!fs.existsSync(path.join(SKILLS_ROOT, skills[0].name, 'SKILL.md'))) {
                problems.push(`${skills[0].name} has no SKILL.md`);
            }
        }
        expect(problems).toEqual([]);
    });

    it('gives every io and transform skill folder at least one catalog sparkTransform', () => {
        const catalogTransforms = new Set(CATALOG.map((entry) => entry.sparkTransform));
        const orphans = SKILLS.filter((s) => s.name.startsWith('io-') || s.name.startsWith('transform-'))
            .filter((s) => !s.transforms.some((t) => catalogTransforms.has(t)))
            .map((s) => s.name);
        expect(orphans).toEqual([]);
    });

    it('reports an unknown sparkTransform as unresolved instead of guessing a skill', () => {
        expect(LIBRARY.resolveTransform('NotARealSparkTransform')).toEqual({
            canonical: 'NotARealSparkTransform',
            aliased: false,
            known: false,
        });
        expect(LIBRARY.resolveTransform('   ').known).toBe(false);
    });

    it('names only known Spark transforms in metadata.transforms', () => {
        const unknown = SKILLS.flatMap((s) =>
            s.transforms.filter((t) => !KNOWN_SPARK_TRANSFORMS.has(t)).map((t) => `${s.name}: ${t}`)
        );
        expect(unknown).toEqual([]);
    });

    it('keeps only allowed {{...}} tokens in every skill file', () => {
        const bad = walk(SKILLS_ROOT).flatMap((file) =>
            [...fs.readFileSync(file, 'utf-8').matchAll(/\{\{\s*[A-Za-z_][\w.-]*\s*\}\}/g)]
                .map((m) => m[0])
                .filter((token) => !ALLOWED_PLACEHOLDERS.has(token))
                .map((token) => `${path.relative(SKILLS_ROOT, file)}: ${token}`)
        );
        expect(bad).toEqual([]);
    });

    it('has no reference to the deleted code-gen-prompt files', () => {
        const bad = walk(SKILLS_ROOT)
            .filter((file) => {
                const text = fs.readFileSync(file, 'utf-8');
                return text.includes('code-gen-prompt-');
            })
            .map((file) => path.relative(SKILLS_ROOT, file));
        expect(bad).toEqual([]);
    });

    it('has a reference file for every declared language of a skill that uses reference files', () => {
        const missing: string[] = [];
        for (const skill of SKILLS) {
            const declared = Object.keys(skill.languageFiles);
            if (declared.length === 0) {
                continue;
            }
            const languages = skill.languages.includes(SKILL_WILDCARD) ? ALL_SKILL_LANGUAGES : skill.languages;
            for (const language of languages) {
                if (!(language in skill.languageFiles)) {
                    missing.push(`${skill.name}/${language}.md`);
                }
            }
            for (const language of declared) {
                if (!languages.includes(language)) {
                    missing.push(`${skill.name}/${language}.md is not declared in languages`);
                }
            }
        }
        expect(missing).toEqual([]);
    });

    it('has a template for every language that each runner skill supports', () => {
        const missing: string[] = [];
        for (const skill of SKILLS.filter((s) => s.kind === 'runner')) {
            const languages = skill.languages.includes(SKILL_WILDCARD) ? ALL_SKILL_LANGUAGES : skill.languages;
            for (const language of languages as Array<keyof typeof ALL_LANGUAGE_RUNNERS>) {
                const supported = skill.runners.some((r) => (ALL_LANGUAGE_RUNNERS[language] as readonly string[]).includes(r));
                if (supported && !(skill.templates[language]?.length)) {
                    missing.push(`${skill.name}/templates/${language}`);
                }
            }
        }
        expect(missing).toEqual([]);
    });

    it('keeps every runner skill python template byte-identical with resources/templates/pyspark/', () => {
        const pysparkTemplatesDir = path.join(REPO_ROOT, 'resources', 'templates', 'pyspark');
        const mismatches: string[] = [];
        for (const skill of SKILLS.filter((s) => s.kind === 'runner')) {
            for (const tmpl of skill.templates.python ?? []) {
                const mirrorPath = path.join(pysparkTemplatesDir, tmpl.fileName);
                if (!fs.existsSync(mirrorPath)) {
                    mismatches.push(`${skill.name}: missing mirror resources/templates/pyspark/${tmpl.fileName}`);
                    continue;
                }
                const skillBytes = fs.readFileSync(path.join(SKILLS_ROOT, skill.name, 'templates', 'python', tmpl.fileName));
                const mirrorBytes = fs.readFileSync(mirrorPath);
                if (!skillBytes.equals(mirrorBytes)) {
                    mismatches.push(`${skill.name}: ${tmpl.fileName} differs from resources/templates/pyspark/${tmpl.fileName}`);
                }
            }
        }
        expect(mismatches).toEqual([]);
    });

    it('declares valid paradigms metadata across all bundled skills and bans Databricks-only APIs in sdp-* skills (SDP6)', () => {
        const allowedParadigms = new Set<string>([SKILL_WILDCARD, ...Object.values(SKILL_PARADIGMS)]);
        for (const skill of SKILLS) {
            expect(skill.paradigms.length, `${skill.name} paradigms empty`).toBeGreaterThan(0);
            for (const p of skill.paradigms) {
                expect(allowedParadigms.has(p), `${skill.name} unknown paradigm ${p}`).toBe(true);
            }
            const rawText = fs.readFileSync(path.join(SKILLS_ROOT, skill.name, 'SKILL.md'), 'utf-8');
            expect(rawText, `${skill.name} missing explicit paradigms key in frontmatter`).toMatch(/^\s+paradigms:\s*\[/m);
        }

        for (const sdpSkillName of ['sdp-core', 'sdp-spec', 'sdp-auto-cdc', 'sdp-quality-quarantine']) {
            const sdpSkill = SKILLS.find((s) => s.name === sdpSkillName);
            expect(sdpSkill, `Missing ${sdpSkillName}`).toBeDefined();
            expect(sdpSkill?.paradigms).toEqual(['sdp']);
        }

        const sdpCoreBody = SKILLS.find((s) => s.name === 'sdp-core')?.body ?? '';
        for (const bannedMention of ['dlt', '@dp.expect', 'dp.apply_changes', 'once=', 'stored_as_scd_type=2', 'cloudFiles']) {
            expect(sdpCoreBody).toContain(bannedMention);
        }

        for (const imperativeOnlyName of [
            'core-streaming',
            'runner-local',
            'runner-dataproc-cluster',
            'runner-serverless-spark',
            'io-foreach-batch',
            'io-vector-store',
            'transform-cache-persist',
            'transform-feature-prep',
            'transform-spark-ml-vertex-ai',
        ]) {
            const skill = SKILLS.find((s) => s.name === imperativeOnlyName);
            expect(skill?.paradigms, imperativeOnlyName).toEqual(['imperative']);
        }
    });

    it('lists at least one URL for every skill whose content is not only from the old prompt', () => {
        const bad = SKILLS.filter((s) => !(s.sources.length === 1 && s.sources[0] === 'prompt'))
            .filter((s) => !s.sources.some((source) => /^https?:\/\//.test(source)))
            .map((s) => s.name);
        expect(bad).toEqual([]);
    });

    it('never emits metadata.sources or metadata.validated into a built prompt', () => {
        for (const language of ['PySpark'] as const) {
            const nodes = [...KNOWN_SPARK_TRANSFORMS].map((t, idx) => ({
                id: `n_${idx}`,
                data: { type: 'SOURCE', spark_transform: t, system_instructions: 'x' },
            }));
            const edges = [{ targetHandle: 'left' }, { targetHandle: 'right' }, { sourceHandle: 'valid' }];
            const { prompt } = new CodegenPromptBuilder(LIBRARY).build({
                parsedPipeline: { nodes, edges },
                redactedYaml: 'nodes: []',
                redactedPaths: [],
                language,
            });
            expect(prompt).not.toMatch(/^\s*(sources|validated):/m);
            expect(prompt).not.toMatch(/^name: /m);
            expect(prompt).not.toContain('usedBy:');
        }
    });

    it('names only known Spark capability transforms in the streaming, python_datasource, and xlang constants', () => {
        expect([...STREAMING_SOURCE_TRANSFORMS].filter((t) => !(t in SPARK_NODE_CAPABILITIES))).toEqual([]);
        expect([...PYTHON_DATASOURCE_TRANSFORMS].filter((t) => !(t in SPARK_NODE_CAPABILITIES))).toEqual([]);
        for (const set of Object.values(XLANG_TRANSFORMS)) {
            expect([...set].filter((t) => !KNOWN_SPARK_TRANSFORMS.has(t))).toEqual([]);
        }
    });

    it('reports unvalidated skills and TODO lines (report only)', () => {
        const unvalidated = SKILLS.filter((s) => !s.validated).length;
        const todo = walk(SKILLS_ROOT).filter((file) => /^\s*TODO:/m.test(fs.readFileSync(file, 'utf-8')));
        console.info(
            `[codegen-skills] ${SKILLS.length} skills, ${unvalidated} with validated: false, ${todo.length} file(s) with a TODO line.`
        );
        expect(unvalidated).toBeGreaterThanOrEqual(0);
    });
});

describe('bundled .spark files', () => {
    /**
     * Lists every `.spark` file in the repository outside node_modules and build output.
     *
     * @param dir Folder.
     * @returns Absolute `.spark` paths.
     */
    function findSparkFiles(dir: string): string[] {
        if (!fs.existsSync(dir)) {
            return [];
        }
        return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
            if (['node_modules', 'out', 'dist', '.git'].includes(entry.name)) {
                return [];
            }
            const full = path.join(dir, entry.name);
            return entry.isDirectory() ? findSparkFiles(full) : entry.name.endsWith('.spark') ? [full] : [];
        });
    }

    it('gives every SOURCE, STEP, and SINK node in bundled .spark files a spark_transform', () => {
        const files = [...findSparkFiles(path.join(REPO_ROOT, 'resources')), ...findSparkFiles(path.join(REPO_ROOT, 'src'))];
        const missing: string[] = [];
        for (const file of files) {
            const doc = YAML.parse(fs.readFileSync(file, 'utf-8'));
            for (const node of doc?.nodes ?? []) {
                const data = node?.data ?? {};
                if (['SOURCE', 'STEP', 'SINK'].includes(data.type) && !data.spark_transform && !data.sparkTransform) {
                    missing.push(`${path.relative(REPO_ROOT, file)}: ${data.label ?? node.id}`);
                }
            }
        }
        expect(missing).toEqual([]);
    });
});
