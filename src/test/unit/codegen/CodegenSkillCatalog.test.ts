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
    transformKey,
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
    { name: 'core-pyspark', kind: 'language', languages: '[python]' },
    { name: 'runner-local', kind: 'runner', runners: '[local]' },
    { name: 'runner-dataproc-cluster', kind: 'runner', languages: '[python]', runners: '[dataproc-cluster]' },
    { name: 'runner-serverless-spark', kind: 'runner', languages: '[python]', runners: '[serverless-spark]' },
    { name: 'io-kafka', kind: 'io', transforms: '[KafkaSource]' },
    { name: 'io-bigquery', kind: 'io', languages: '[python]', transforms: '[BigQuerySource]' },
    { name: 'transforms-select', kind: 'transform', transforms: '[Select]' },
    { name: 'core-catalyst-optimization', kind: 'crosscutting' },
    { name: 'core-streaming', kind: 'crosscutting', transforms: '[Watermark, StatefulTransform]', preloadWhen: '[streaming]' },
    {
        name: 'core-python-datasource',
        kind: 'crosscutting',
        transforms: '[PubSubSource, PubSubSink, KinesisSource, KinesisSink, MqttSource, MqttSink, FirestoreSource, FirestoreSink, CassandraSource, RedisSource, RedisSink]',
        preloadWhen: '[python_datasource]',
    },
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
        expect(names).toEqual([
            'codegen-common',
            'core-pyspark',
            'runner-dataproc-cluster',
            'runner-local',
            'runner-serverless-spark',
            'core-catalyst-optimization',
            'verification-gate',
        ]);
    });

    it('selects language-only skills and the Spark runners for Python (simple)', () => {
        const catalog = CodegenSkillCatalog.load(writeFixtures(FIXTURES));
        const names = catalog
            .select(
                request({
                    language: 'python',
                    runners: new Set<SkillRunner>(['local']),
                    transforms: new Set(['BigQuerySource']),
                })
            )
            .map((s) => s.skill.name);
        expect(names).toEqual([
            'codegen-common',
            'core-pyspark',
            'runner-local',
            'io-bigquery',
            'core-catalyst-optimization',
            'verification-gate',
        ]);
    });

    it('selects io, transform, and crosscutting skills by transform (simple)', () => {
        const catalog = CodegenSkillCatalog.load(writeFixtures(FIXTURES));
        const names = catalog
            .select(request({ transforms: new Set(['KafkaSource', 'Select', 'Watermark', 'PubSubSource']) }))
            .map((s) => s.skill.name);
        expect(names).toEqual([
            'codegen-common',
            'core-pyspark',
            'runner-dataproc-cluster',
            'runner-local',
            'runner-serverless-spark',
            'io-kafka',
            'transforms-select',
            'core-catalyst-optimization',
            'core-python-datasource',
            'core-streaming',
            'verification-gate',
        ]);
    });

    it('selects preloadWhen skills when streaming or python_datasource feature is present (simple)', () => {
        const catalog = CodegenSkillCatalog.load(writeFixtures(FIXTURES));
        expect(catalog.select(request()).map((s) => s.skill.name)).not.toContain('core-streaming');
        expect(catalog.select(request()).map((s) => s.skill.name)).not.toContain('core-python-datasource');
        expect(catalog.select(request({ features: new Set(['streaming']) })).map((s) => s.skill.name)).toContain(
            'core-streaming'
        );
        expect(
            catalog.select(request({ features: new Set(['python_datasource']) })).map((s) => s.skill.name)
        ).toContain('core-python-datasource');
    });

    it('selects reviewer-only skills only for the reviewer caller (simple)', () => {
        const catalog = CodegenSkillCatalog.load(writeFixtures(FIXTURES));
        expect(catalog.select(request()).map((s) => s.skill.name)).not.toContain('codegen-review');
        const reviewer = catalog.select(request({ caller: 'reviewer' })).map((s) => s.skill.name);
        expect(reviewer).toEqual([
            'codegen-common',
            'core-pyspark',
            'runner-dataproc-cluster',
            'runner-local',
            'runner-serverless-spark',
            'core-catalyst-optimization',
            'codegen-review',
            'verification-gate',
        ]);
    });

    it('drops a runner skill when the project supports none of its runners (extreme)', () => {
        const catalog = CodegenSkillCatalog.load(writeFixtures(FIXTURES));
        const names = catalog.select(request({ runners: new Set<SkillRunner>() })).map((s) => s.skill.name);
        expect(names).not.toContain('runner-local');
        expect(names).not.toContain('runner-dataproc-cluster');
        expect(names).not.toContain('runner-serverless-spark');
    });

    it('orders by kind then name, independent of folder order (extreme)', () => {
        const reversed = [...FIXTURES].reverse().map((s) => buildSkill(skillMd(s), s.name));
        const catalog = CodegenSkillCatalog.fromSkills(reversed);
        const all = catalog.all().map((s) => s.name);
        expect(all[0]).toBe('codegen-common');
        expect(all[all.length - 1]).toBe('verification-gate');
        expect(all.indexOf('io-bigquery')).toBeLessThan(all.indexOf('io-kafka'));
        expect(all.indexOf('io-kafka')).toBeLessThan(all.indexOf('transforms-select'));
        expect(compareSkills(reversed[0], reversed[0])).toBe(0);
    });

    it('attaches the language file and templates of the pipeline language (simple)', () => {
        const root = writeFixtures([
            { name: 'codegen-common', kind: 'common' },
            { name: 'runner-serverless-spark', kind: 'runner', runners: '[serverless-spark]' },
        ]);
        fs.writeFileSync(path.join(root, 'runner-serverless-spark', 'python.md'), '#### Imports\npy rule\n');
        fs.mkdirSync(path.join(root, 'runner-serverless-spark', 'templates', 'python'), { recursive: true });
        fs.writeFileSync(
            path.join(root, 'runner-serverless-spark', 'templates', 'python', 'run_serverless_spark.sh'),
            'run'
        );
        fs.writeFileSync(path.join(root, 'runner-serverless-spark', 'templates', 'python', 'Dockerfile'), 'FROM x');
        const catalog = CodegenSkillCatalog.load(root);

        const python = catalog.select(request()).find((s) => s.skill.name === 'runner-serverless-spark');
        expect(python?.languageFile).toBe('#### Imports\npy rule');
        expect(python?.templates.map((t) => t.fileName)).toEqual(['Dockerfile', 'run_serverless_spark.sh']);

        const localOnly = catalog
            .select(request({ language: 'python', runners: new Set<SkillRunner>(['local']) }))
            .find((s) => s.skill.name === 'runner-serverless-spark');
        expect(localOnly).toBeUndefined();
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
            skillMd({ name: 'io-kafka', kind: 'io', extra: '  sources:\n    - prompt\n  validated: true' }).replace(
                / {2}transforms: \[\]\n {2}preloadWhen: \[\]\n/,
                ''
            ),
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
        ['a scalar list field', { name: 'a', kind: 'io', transforms: 'KafkaSource' }, /must be a list/],
        ['a bad validated value', { name: 'a', kind: 'io', extra: '  validated: yes' }, /validated must be true or false/],
        ['a list validated value', { name: 'a', kind: 'io', extra: '  validated: [true]' }, /validated must be true or false/],
    ])('throws on %s (failure)', (_label, fixture, message) => {
        expect(() => buildSkill(skillMd(fixture as FixtureSkill), 'label')).toThrow(message);
    });

    it('throws when kind is missing (failure)', () => {
        expect(() => buildSkill('---\nname: a\ndescription: d\nmetadata:\n  usedBy: [generator]\n---\n', 'a')).toThrow(
            /metadata.kind/
        );
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
        const root = writeFixtures([{ name: 'runner-local', kind: 'runner' }]);
        fs.mkdirSync(path.join(root, 'runner-local', 'templates', 'go'), { recursive: true });
        expect(() => CodegenSkillCatalog.load(root)).toThrow(/unknown entry 'go'/);
    });

    it('throws on a file directly under templates (failure)', () => {
        const root = writeFixtures([{ name: 'runner-local', kind: 'runner' }]);
        fs.mkdirSync(path.join(root, 'runner-local', 'templates'));
        fs.writeFileSync(path.join(root, 'runner-local', 'templates', 'Dockerfile'), 'x');
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
            buildSkill(skillMd({ name: 'io-kafka', kind: 'io', transforms: '[KafkaSource]' }), 'a'),
            buildSkill(skillMd({ name: 'io-bigquery', kind: 'io', transforms: '[BigQuerySink]' }), 'b'),
            buildSkill(skillMd({ name: 'io-text', kind: 'io', transforms: '[TextSource]' }), 'c'),
            buildSkill(skillMd({ name: 'io-create', kind: 'io', transforms: '[CreateDataFrameSource]' }), 'd'),
            buildSkill(skillMd({ name: 'dup-a', kind: 'io', transforms: '[FooSource]' }), 'e'),
            buildSkill(skillMd({ name: 'dup-b', kind: 'transform', transforms: '[FooSink]' }), 'f'),
        ]);

    it('normalizes comparison keys across IO, Source, and Sink suffixes', () => {
        expect(transformKey('KafkaSource')).toBe('kafka');
        expect(transformKey('KafkaSink')).toBe('kafka');
        expect(transformKey('KafkaIO')).toBe('kafka');
    });

    it('keeps an exact catalog value (simple)', () => {
        expect(catalog().resolveTransform(' KafkaSource ')).toEqual({
            canonical: 'KafkaSource',
            aliased: false,
            known: true,
        });
    });

    it.each([
        ['ReadFromKafka', 'KafkaSource'],
        ['KafkaIO', 'KafkaSource'],
        ['WriteToBigQuery', 'BigQuerySink'],
        ['kafka', 'KafkaSource'],
        ['Kafka_IO', 'KafkaSource'],
        ['ReadFromText', 'TextSource'],
        ['createdataframe', 'CreateDataFrameSource'],
    ])('resolves the legacy spelling %s to %s (simple)', (raw, canonical) => {
        expect(catalog().resolveTransform(raw)).toEqual({ canonical, aliased: true, known: true });
    });

    it('does not guess when two catalog values share a key (extreme)', () => {
        expect(catalog().resolveTransform('foo')).toEqual({ canonical: 'foo', aliased: false, known: false });
    });

    it('reports an unknown value unchanged (failure)', () => {
        expect(catalog().resolveTransform('MadeUpSource')).toEqual({
            canonical: 'MadeUpSource',
            aliased: false,
            known: false,
        });
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

    it('loads all bundled Phase 2 core/runner skills and Phase 5-6 io-* skills in resources/codegen-skills cleanly (G1-G10, SRC1-SRC38, SNK1-SNK37)', () => {
        const bundledRoot = path.join(__dirname, '..', '..', '..', '..', 'resources', 'codegen-skills');
        clearCodegenSkillCatalogCache();
        const catalog = getCodegenSkillCatalog(bundledRoot);
        const names = catalog.all().map((s) => s.name);
        expect(names).toEqual(
            expect.arrayContaining([
                'codegen-common',
                'core-pyspark',
                'runner-dataproc-cluster',
                'runner-local',
                'runner-serverless-spark',
                'core-catalyst-optimization',
                'core-python-datasource',
                'core-streaming',
                'verification-gate',
                'io-console',
                'io-memory',
                'io-foreach-batch',
            ])
        );
        const ioSkills = catalog.all().filter((s) => s.kind === 'io');
        expect(ioSkills.length).toBeGreaterThanOrEqual(41);
        const transformSkills = catalog.all().filter((s) => s.kind === 'transform');
        expect(transformSkills).toHaveLength(33);
        for (const stepTransform of [
            'Select',
            'Filter',
            'WithColumns',
            'RenameDropColumns',
            'ParsePayload',
            'Explode',
            'HandleNulls',
            'OrderBy',
            'LimitSample',
            'ConditionalSplit',
            'DataQualityAssert',
            'EnforceSchemaAndCast',
            'NestedStructArrayOps',
            'Join',
            'MergeCdcScd',
            'UnionByName',
            'SetOperation',
            'GroupByAgg',
            'TopN',
            'Deduplicate',
            'PivotUnpivot',
            'RollupCube',
            'WindowOver',
            'Watermark',
            'StatefulTransform',
            'SparkSqlQuery',
            'PandasUdfTransform',
            'CustomTransform',
            'RepartitionCoalesce',
            'CachePersist',
            'FeaturePrep',
            'SparkMlAndVertexAi',
            'SparkMlInference',
            'VertexAiInference',
            'TextChunkAndEmbed',
            'ParseMessage',
        ]) {
            expect(catalog.resolveTransform(stepTransform).known, `Unresolved step transform: ${stepTransform}`).toBe(true);
        }
        const common = catalog.all().find((s) => s.name === 'codegen-common')!;
        expect(common.kind).toBe('common');
        expect(common.body).toContain('def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:');
        expect(common.body).toContain('def transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]:');
        expect(common.body).toContain('def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:');
        expect(common.body).toContain('tests/test_pipeline.py');
    });
});
