/**
 * Unit tests for the code generation skill catalog (`src/codegen/CodegenSkillCatalog.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Uses fixture skill folders in a temp directory. Covers every selection rule
 * (caller, language, runner, always-on kinds, always-on crosscutting, transforms,
 * preloadWhen), the deterministic order, attachments (language files, templates), the
 * cache, and every load/validation failure.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
    CodegenSkillCatalog,
    SelectionRequest,
    buildSkill,
    clearCodegenSkillCatalogCache,
    compareSkills,
    getCodegenSkillCatalog,
} from '../../../codegen/CodegenSkillCatalog';
import { SkillRunner } from '../../../codegen/codegenConfig';

/** Options for one fixture skill. */
interface FixtureSkill {
    name: string;
    kind: string;
    usedBy?: string;
    languages?: string;
    runners?: string;
    transforms?: string;
    preloadWhen?: string;
    extra?: string;
    body?: string;
}

/**
 * Renders SKILL.md content for a fixture skill.
 *
 * @param s Fixture options.
 * @returns SKILL.md text.
 */
function skillMd(s: FixtureSkill): string {
    return [
        '---',
        `name: ${s.name}`,
        `description: Fixture ${s.name}.`,
        'metadata:',
        `  kind: ${s.kind}`,
        `  usedBy: ${s.usedBy ?? '[generator, reviewer]'}`,
        `  languages: ${s.languages ?? '[all]'}`,
        `  runners: ${s.runners ?? '[all]'}`,
        `  transforms: ${s.transforms ?? '[]'}`,
        `  preloadWhen: ${s.preloadWhen ?? '[]'}`,
        ...(s.extra ? [s.extra] : []),
        '---',
        s.body ?? `Body of ${s.name}.`,
    ].join('\n');
}

/** The fixture library used by the selection tests. */
const FIXTURES: FixtureSkill[] = [
    { name: 'codegen-common', kind: 'common' },
    { name: 'lang-python', kind: 'language', languages: '[python]' },
    { name: 'lang-java', kind: 'language', languages: '[java]' },
    { name: 'runner-direct', kind: 'runner', runners: '[direct]' },
    { name: 'runner-spark', kind: 'runner', languages: '[java]', runners: '[spark]' },
    { name: 'io-kafka', kind: 'io', transforms: '[KafkaIO]' },
    { name: 'io-xml', kind: 'io', languages: '[java]', transforms: '[XmlIO]' },
    { name: 'transforms-pardo', kind: 'transform', transforms: '[ParDo]' },
    { name: 'graph-wiring', kind: 'crosscutting' },
    { name: 'side-inputs', kind: 'crosscutting', preloadWhen: '[side_inputs]' },
    { name: 'gated-by-transform', kind: 'crosscutting', transforms: '[WindowInto]' },
    { name: 'codegen-review', kind: 'review', usedBy: '[reviewer]' },
    { name: 'verification-gate', kind: 'gate' },
];

let tmpRoot: string;

/**
 * Writes fixture skills into a fresh temp skills root.
 *
 * @param skills Fixture skills.
 * @returns The skills root path.
 */
function writeFixtures(skills: FixtureSkill[]): string {
    const root = fs.mkdtempSync(path.join(tmpRoot, 'skills-'));
    for (const s of skills) {
        fs.mkdirSync(path.join(root, s.name));
        fs.writeFileSync(path.join(root, s.name, 'SKILL.md'), skillMd(s));
    }
    return root;
}

/**
 * Builds a selection request with defaults.
 *
 * @param overrides Fields to override.
 * @returns The request.
 */
function request(overrides: Partial<SelectionRequest> = {}): SelectionRequest {
    return {
        caller: 'generator',
        language: 'python',
        runners: new Set<SkillRunner>(['local', 'dataproc-cluster', 'serverless-spark']),
        transforms: new Set<string>(),
        features: new Set<string>(),
        ...overrides,
    };
}

beforeEach(() => {
    tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'codegen-catalog-'));
    clearCodegenSkillCatalogCache();
});

afterEach(() => {
    fs.rmSync(tmpRoot, { recursive: true, force: true });
});

describe('CodegenSkillCatalog.select', () => {
    it('selects only always-on skills for an empty Python graph (simple)', () => {
        const catalog = CodegenSkillCatalog.load(writeFixtures(FIXTURES));
        const names = catalog.select(request()).map((s) => s.skill.name);
        expect(names).toEqual(['codegen-common', 'lang-python', 'runner-direct', 'graph-wiring', 'verification-gate']);
    });

    it('selects language-only skills and the Spark runners for Python (simple)', () => {
        const catalog = CodegenSkillCatalog.load(writeFixtures(FIXTURES));
        const names = catalog
            .select(request({ language: 'python', runners: new Set<SkillRunner>(['local', 'dataproc-cluster', 'serverless-spark']), transforms: new Set(['XmlIO']) }))
            .map((s) => s.skill.name);
        expect(names).toEqual(['codegen-common', 'lang-python', 'io-xml', 'graph-wiring', 'verification-gate']);
    });

    it('selects io, transform, and crosscutting skills by transform (simple)', () => {
        const catalog = CodegenSkillCatalog.load(writeFixtures(FIXTURES));
        const names = catalog
            .select(request({ transforms: new Set(['KafkaIO', 'ParDo', 'WindowInto', 'XmlIO']) }))
            .map((s) => s.skill.name);
        // io-xml is Java-only, so it is not selected for Python.
        expect(names).toEqual([
            'codegen-common',
            'lang-python',
            'runner-direct',
            'io-kafka',
            'transforms-pardo',
            'gated-by-transform',
            'graph-wiring',
            'verification-gate',
        ]);
    });

    it('selects preloadWhen skills only when the feature is present (simple)', () => {
        const catalog = CodegenSkillCatalog.load(writeFixtures(FIXTURES));
        expect(catalog.select(request()).map((s) => s.skill.name)).not.toContain('side-inputs');
        expect(catalog.select(request({ features: new Set(['side_inputs']) })).map((s) => s.skill.name)).toContain('side-inputs');
    });

    it('selects reviewer-only skills only for the reviewer caller (simple)', () => {
        const catalog = CodegenSkillCatalog.load(writeFixtures(FIXTURES));
        expect(catalog.select(request()).map((s) => s.skill.name)).not.toContain('codegen-review');
        const reviewer = catalog.select(request({ caller: 'reviewer' })).map((s) => s.skill.name);
        expect(reviewer).toEqual(['codegen-common', 'lang-python', 'runner-direct', 'graph-wiring', 'codegen-review', 'verification-gate']);
    });

    it('drops a runner skill when the project supports none of its runners (extreme)', () => {
        const catalog = CodegenSkillCatalog.load(writeFixtures(FIXTURES));
        const names = catalog.select(request({ runners: new Set<SkillRunner>() })).map((s) => s.skill.name);
        expect(names).not.toContain('runner-direct');
    });

    it('orders by kind then name, independent of folder order (extreme)', () => {
        const reversed = [...FIXTURES].reverse().map((s) => buildSkill(skillMd(s), s.name));
        const catalog = CodegenSkillCatalog.fromSkills(reversed);
        const all = catalog.all().map((s) => s.name);
        expect(all[0]).toBe('codegen-common');
        expect(all[all.length - 1]).toBe('verification-gate');
        expect(all.indexOf('io-kafka')).toBeLessThan(all.indexOf('io-xml'));
        expect(all.indexOf('io-xml')).toBeLessThan(all.indexOf('transforms-pardo'));
        expect(compareSkills(reversed[0], reversed[0])).toBe(0);
    });

    it('attaches the language file and templates of the pipeline language only (simple)', () => {
        const root = writeFixtures([{ name: 'codegen-common', kind: 'common' }, { name: 'runner-dataflow', kind: 'runner', runners: '[dataflow]' }]);
        fs.writeFileSync(path.join(root, 'runner-dataflow', 'python.md'), '#### Imports\npy rule\n');
        fs.writeFileSync(path.join(root, 'runner-dataflow', 'java.md'), 'java rule');
        fs.mkdirSync(path.join(root, 'runner-dataflow', 'templates', 'python'), { recursive: true });
        fs.writeFileSync(path.join(root, 'runner-dataflow', 'templates', 'python', 'run_dataflowrunner.sh'), 'run');
        fs.writeFileSync(path.join(root, 'runner-dataflow', 'templates', 'python', 'Dockerfile'), 'FROM x');
        const catalog = CodegenSkillCatalog.load(root);

        const python = catalog.select(request()).find((s) => s.skill.name === 'runner-dataflow');
        expect(python?.languageFile).toBe('#### Imports\npy rule');
        expect(python?.templates.map((t) => t.fileName)).toEqual(['Dockerfile', 'run_dataflowrunner.sh']);

        const python2 = catalog
            .select(request({ language: 'python', runners: new Set<SkillRunner>(['local']) }))
            .find((s) => s.skill.name === 'runner-dataflow');
        expect(python2).toBeUndefined();
    });

    it('ignores files at the skills root such as README.md (extreme)', () => {
        const root = writeFixtures([{ name: 'codegen-common', kind: 'common' }]);
        fs.writeFileSync(path.join(root, 'README.md'), '# docs');
        expect(CodegenSkillCatalog.load(root).all()).toHaveLength(1);
    });
});

describe('buildSkill metadata', () => {
    it('reads sources and validated, and defaults optional lists (simple)', () => {
        const skill = buildSkill(
            skillMd({ name: 'io-kafka', kind: 'io', extra: '  sources:\n    - prompt\n  validated: true' }).replace(/ {2}transforms: \[\]\n {2}preloadWhen: \[\]\n/, ''),
            'io-kafka'
        );
        expect(skill.sources).toEqual(['prompt']);
        expect(skill.validated).toBe(true);
        expect(skill.transforms).toEqual([]);
        expect(skill.preloadWhen).toEqual([]);
        expect(skill.languageFiles).toEqual({});
        expect(skill.templates).toEqual({});
    });

    it('defaults validated to false (simple)', () => {
        expect(buildSkill(skillMd({ name: 'a', kind: 'io' }), 'a').validated).toBe(false);
    });

    it.each([
        ['an unknown kind', { name: 'a', kind: 'connector' }, /metadata.kind must be one of/],
        ['an unknown caller', { name: 'a', kind: 'io', usedBy: '[planner]' }, /usedBy has unknown value/],
        ['an empty usedBy', { name: 'a', kind: 'io', usedBy: '[]' }, /usedBy is required/],
        ['an unknown language', { name: 'a', kind: 'io', languages: '[go]' }, /languages has unknown value/],
        ['an unknown runner', { name: 'a', kind: 'io', runners: '[flink]' }, /runners has unknown value/],
        ['an unknown feature', { name: 'a', kind: 'io', preloadWhen: '[mcp]' }, /preloadWhen has unknown value/],
        ['a scalar list field', { name: 'a', kind: 'io', transforms: 'KafkaIO' }, /must be a list/],
        ['a bad validated value', { name: 'a', kind: 'io', extra: '  validated: yes' }, /validated must be true or false/],
        ['a list validated value', { name: 'a', kind: 'io', extra: '  validated: [true]' }, /validated must be true or false/],
    ])('throws on %s (failure)', (_label, fixture, message) => {
        expect(() => buildSkill(skillMd(fixture as FixtureSkill), 'label')).toThrow(message);
    });

    it('throws when kind is missing (failure)', () => {
        expect(() => buildSkill('---\nname: a\ndescription: d\nmetadata:\n  usedBy: [generator]\n---\n', 'a')).toThrow(/metadata.kind/);
    });
});

describe('CodegenSkillCatalog.load failures', () => {
    it('throws when the skills root does not exist (failure)', () => {
        expect(() => CodegenSkillCatalog.load(path.join(tmpRoot, 'missing'))).toThrow(/directory not found/);
    });

    it('throws when the skills root has no skill folders (failure)', () => {
        const root = fs.mkdtempSync(path.join(tmpRoot, 'empty-'));
        expect(() => CodegenSkillCatalog.load(root)).toThrow(/No code generation skills found/);
    });

    it('throws when a skill folder has no SKILL.md (failure)', () => {
        const root = writeFixtures([{ name: 'codegen-common', kind: 'common' }]);
        fs.mkdirSync(path.join(root, 'io-empty'));
        expect(() => CodegenSkillCatalog.load(root)).toThrow(/has no SKILL.md/);
    });

    it('throws when the name does not equal the folder (failure)', () => {
        const root = writeFixtures([{ name: 'codegen-common', kind: 'common' }]);
        fs.mkdirSync(path.join(root, 'io-kafka'));
        fs.writeFileSync(path.join(root, 'io-kafka', 'SKILL.md'), skillMd({ name: 'io-kafka2', kind: 'io' }));
        expect(() => CodegenSkillCatalog.load(root)).toThrow(/must equal its folder name/);
    });

    it('throws on an unknown file in a skill folder such as pyhton.md (failure)', () => {
        const root = writeFixtures([{ name: 'codegen-common', kind: 'common' }]);
        fs.writeFileSync(path.join(root, 'codegen-common', 'pyhton.md'), 'typo');
        expect(() => CodegenSkillCatalog.load(root)).toThrow(/unknown entry 'pyhton.md'/);
    });

    it('throws on an unknown language folder under templates (failure)', () => {
        const root = writeFixtures([{ name: 'runner-direct', kind: 'runner' }]);
        fs.mkdirSync(path.join(root, 'runner-direct', 'templates', 'go'), { recursive: true });
        expect(() => CodegenSkillCatalog.load(root)).toThrow(/unknown entry 'go'/);
    });

    it('throws on a file directly under templates (failure)', () => {
        const root = writeFixtures([{ name: 'runner-direct', kind: 'runner' }]);
        fs.mkdirSync(path.join(root, 'runner-direct', 'templates'));
        fs.writeFileSync(path.join(root, 'runner-direct', 'templates', 'Dockerfile'), 'x');
        expect(() => CodegenSkillCatalog.load(root)).toThrow(/unknown entry 'Dockerfile'/);
    });

    it('throws on duplicate skill names (failure)', () => {
        const a = buildSkill(skillMd({ name: 'a', kind: 'io' }), 'a');
        expect(() => CodegenSkillCatalog.fromSkills([a, a])).toThrow(/Duplicate code generation skill name 'a'/);
    });
});

describe('CodegenSkillCatalog.resolveTransform', () => {
    /** Catalog with the transform values the resolver must distinguish. */
    const catalog = () =>
        CodegenSkillCatalog.fromSkills([
            buildSkill(skillMd({ name: 'io-kafka', kind: 'io', transforms: '[KafkaIO]' }), 'a'),
            buildSkill(skillMd({ name: 'io-bigquery', kind: 'io', transforms: '[BigQueryIO]' }), 'b'),
            buildSkill(skillMd({ name: 'io-text', kind: 'io', transforms: '[TextIO]' }), 'c'),
            buildSkill(skillMd({ name: 'io-create', kind: 'io', transforms: '[Create]' }), 'd'),
            buildSkill(skillMd({ name: 'dup-a', kind: 'io', transforms: '[FooIO]' }), 'e'),
            buildSkill(skillMd({ name: 'dup-b', kind: 'transform', transforms: '[Foo]' }), 'f'),
        ]);

    it('keeps an exact catalog value (simple)', () => {
        expect(catalog().resolveTransform(' KafkaIO ')).toEqual({ canonical: 'KafkaIO', aliased: false, known: true });
    });

    it.each([
        ['ReadFromKafka', 'KafkaIO'],
        ['WriteToBigQuery', 'BigQueryIO'],
        ['kafka', 'KafkaIO'],
        ['Kafka_IO', 'KafkaIO'],
        ['ReadFromText', 'TextIO'],
        ['create', 'Create'],
    ])('resolves the legacy spelling %s to %s (simple)', (raw, canonical) => {
        expect(catalog().resolveTransform(raw)).toEqual({ canonical, aliased: true, known: true });
    });

    it('does not guess when two catalog values share a key (extreme)', () => {
        expect(catalog().resolveTransform('foo')).toEqual({ canonical: 'foo', aliased: false, known: false });
    });

    it('reports an unknown value unchanged (failure)', () => {
        expect(catalog().resolveTransform('MadeUpIO')).toEqual({ canonical: 'MadeUpIO', aliased: false, known: false });
        expect(catalog().resolveTransform('Read')).toEqual({ canonical: 'Read', aliased: false, known: false });
    });
});

describe('getCodegenSkillCatalog', () => {
    it('loads once and reuses the cached catalog (simple)', () => {
        const root = writeFixtures([{ name: 'codegen-common', kind: 'common' }]);
        const first = getCodegenSkillCatalog(root);
        fs.rmSync(path.join(root, 'codegen-common'), { recursive: true });
        expect(getCodegenSkillCatalog(root)).toBe(first);
        clearCodegenSkillCatalogCache();
        expect(() => getCodegenSkillCatalog(root)).toThrow(/No code generation skills found/);
    });
});
