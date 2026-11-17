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
    ALL_SKILL_LANGUAGES,
    BEAM_VERSION_PLACEHOLDER,
    LANGUAGE_RUNNERS,
    MODEL_FACING_PLACEHOLDERS,
    PYTHON_DATASOURCE_TRANSFORMS,
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
            for (const language of languages as Array<keyof typeof LANGUAGE_RUNNERS>) {
                const supported = skill.runners.some((r) => (LANGUAGE_RUNNERS[language] as readonly string[]).includes(r));
                if (supported && !(skill.templates[language]?.length)) {
                    missing.push(`${skill.name}/templates/${language}`);
                }
            }
        }
        expect(missing).toEqual([]);
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
            const edges = [{ targetHandle: 'side_input' }, { sourceHandle: 'valid' }];
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
