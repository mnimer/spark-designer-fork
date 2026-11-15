/**
 * Unit tests for the code generation prompt builder (`src/codegen/CodegenPromptBuilder.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Uses an in-memory fixture catalog. Covers the section order, the YAML fence, the
 * redaction notice, `{{beam_version}}` handling in skill text only (fix F7 regression),
 * the guarantee that the user YAML is never changed (including `$&` sequences), language
 * specifics, template inlining, the fallback Beam version, and the missing-common failure.
 */

import { describe, it, expect } from 'vitest';
import YAML from 'yaml';
import { CodegenSkillCatalog, buildSkill } from '../../../codegen/CodegenSkillCatalog';
import { CodegenPromptBuilder, fence, resolveBeamVersion } from '../../../codegen/CodegenPromptBuilder';
import { DEFAULT_SPARK_VERSION } from '../../../shared/types';
import { REDACTION_PLACEHOLDER } from '../../../credentialRedaction';

/**
 * Builds a fixture skill.
 *
 * @param name Skill name.
 * @param kind Skill kind.
 * @param body Body text.
 * @param meta Extra metadata lines.
 * @param languageFiles Language reference files.
 * @param templates Runner templates.
 * @returns The skill.
 */
function skill(
    name: string,
    kind: string,
    body: string,
    meta = '',
    languageFiles: Record<string, string> = {},
    templates: Record<string, Array<{ fileName: string; content: string }>> = {}
) {
    const content = [
        '---',
        `name: ${name}`,
        `description: Fixture ${name}.`,
        'metadata:',
        `  kind: ${kind}`,
        '  usedBy: [generator, reviewer]',
        meta.includes('languages:') ? '' : '  languages: [all]',
        meta.includes('runners:') ? '' : '  runners: [all]',
        meta,
        '---',
        body,
    ]
        .filter((line) => line !== '')
        .join('\n');
    return buildSkill(content, name, { languageFiles, templates });
}

/** Fixture catalog that mirrors the real kinds. */
const CATALOG = CodegenSkillCatalog.fromSkills([
    skill('codegen-common', 'common', 'ROLE. Use Beam {{beam_version}}.', '', { python: 'common py', java: 'common java' }),
    skill('lang-python', 'language', 'LANG PY', '  languages: [python]'),
    skill('lang-java', 'language', 'LANG JAVA', '  languages: [java]'),
    skill('runner-dataflow', 'runner', 'Produce Dockerfile.', '  runners: [dataflow]', {}, {
        python: [{ fileName: 'Dockerfile', content: 'FROM apache/beam_python3.11_sdk:{{beam_version}}\n' }],
    }),
    skill('io-kafka', 'io', 'KAFKA RULES', '  transforms: [KafkaIO]', { python: 'kafka py', java: 'kafka java' }),
    skill('side-inputs', 'crosscutting', 'SIDE RULES', '  preloadWhen: [side_inputs]'),
    skill('verification-gate', 'gate', 'CHECKLIST'),
]);

/** Pipeline YAML with a version-like placeholder and `$` sequences inside a property. */
const YAML_TEXT = [
    'nodes:',
    '  - id: k',
    '    data:',
    '      type: SOURCE',
    '      spark_transform: KafkaIO',
    '      properties:',
    '        note: "keep {{beam_version}} and 2.76.0 and $& and $\' literal"',
    'edges: []',
    '',
].join('\n');

/**
 * Builds a prompt with defaults.
 *
 * @param overrides Input overrides.
 * @returns Built prompt.
 */
function build(overrides: Partial<Parameters<CodegenPromptBuilder['build']>[0]> = {}) {
    return new CodegenPromptBuilder(CATALOG).build({
        parsedPipeline: YAML.parse(YAML_TEXT),
        redactedYaml: YAML_TEXT,
        redactedPaths: [],
        language: 'PySpark',
        beamVersion: '2.99.1',
        ...overrides,
    });
}

describe('CodegenPromptBuilder.build', () => {
    it('emits common, YAML, then skills in kind order with the gate last (simple)', () => {
        const { prompt, selectedSkills } = build();
        expect(selectedSkills).toEqual(['codegen-common', 'lang-python', 'runner-dataflow', 'io-kafka', 'verification-gate']);
        const markers = ['ROLE.', '## Pipeline YAML', '## Skill: lang-python', '## Skill: runner-dataflow', '## Skill: io-kafka', '## Skill: verification-gate'];
        const positions = markers.map((m) => prompt.indexOf(m));
        expect(positions.every((p) => p >= 0)).toBe(true);
        expect([...positions].sort((a, b) => a - b)).toEqual(positions);
        expect(prompt.startsWith('ROLE.')).toBe(true);
        expect(prompt.trimEnd().endsWith('CHECKLIST')).toBe(true);
        // codegen-common has no "## Skill:" heading.
        expect(prompt).not.toContain('## Skill: codegen-common');
    });

    it('fences the YAML and inserts it byte-for-byte, never substituting inside it (F7 regression)', () => {
        const { prompt } = build();
        expect(prompt).toContain('```yaml\n' + YAML_TEXT + '```');
        expect(prompt).toContain('keep {{beam_version}} and 2.76.0 and $& and $\' literal');
    });

    it('resolves {{beam_version}} in skill text and templates (simple)', () => {
        const { prompt } = build();
        expect(prompt).toContain('Use Beam 2.99.1.');
        expect(prompt).toContain('#### Template: Dockerfile\n```\nFROM apache/beam_python3.11_sdk:2.99.1\n```');
        // The only remaining placeholder is the one inside the user YAML.
        expect(prompt.match(/\{\{beam_version\}\}/g)).toHaveLength(1);
    });

    it('falls back to DEFAULT_SPARK_VERSION when no version is given (extreme)', () => {
        const { prompt } = build({ beamVersion: undefined });
        expect(prompt).toContain(`Use Beam ${DEFAULT_SPARK_VERSION}.`);
    });

    it('emits only the pipeline language specifics (simple)', () => {
        const python = build().prompt;
        expect(python).toContain('### Python specifics\n\ncommon py');
        expect(python).toContain('### Python specifics\n\nkafka py');
        expect(python).not.toContain('kafka java');
    });

    it('adds the redaction notice right after the YAML only when values were redacted (simple)', () => {
        expect(build().prompt).not.toContain('## Redacted Credentials');
        const { prompt } = build({ redactedPaths: ['nodes[0].data.properties.password'] });
        const yamlEnd = prompt.indexOf('```', prompt.indexOf('```yaml') + 3);
        const notice = prompt.indexOf('## Redacted Credentials');
        expect(notice).toBeGreaterThan(yamlEnd);
        expect(notice).toBeLessThan(prompt.indexOf('## Skill: lang-python'));
        expect(prompt).toContain(REDACTION_PLACEHOLDER);
        expect(prompt).toContain('nodes[0].data.properties.password');
    });

    it('selects feature-gated skills from the parsed pipeline (simple)', () => {
        const parsed = { nodes: [], edges: [{ targetHandle: 'side_input' }] };
        const result = build({ parsedPipeline: parsed });
        expect(result.selectedSkills).toContain('side-inputs');
        expect(result.selectedSkills).not.toContain('io-kafka');
        expect(result.detection.features.has('side_inputs')).toBe(true);
    });

    it('throws when the catalog has no codegen-common skill (failure)', () => {
        const catalog = CodegenSkillCatalog.fromSkills([skill('verification-gate', 'gate', 'X')]);
        expect(() =>
            new CodegenPromptBuilder(catalog).build({ parsedPipeline: {}, redactedYaml: '', redactedPaths: [], language: 'PySpark' })
        ).toThrow(/Skill 'codegen-common' is missing for PySpark/);
    });

    it('renders a skill without a body (extreme)', () => {
        const catalog = CodegenSkillCatalog.fromSkills([skill('codegen-common', 'common', ''), skill('verification-gate', 'gate', '')]);
        const { prompt } = new CodegenPromptBuilder(catalog).build({ parsedPipeline: {}, redactedYaml: 'a: 1', redactedPaths: [], language: 'PySpark' });
        expect(prompt.startsWith('## Pipeline YAML')).toBe(true);
        expect(prompt.endsWith('## Skill: verification-gate')).toBe(true);
    });
});

describe('fence and resolveBeamVersion', () => {
    it('uses a longer fence when the text contains a fence (extreme)', () => {
        expect(fence('a\n```\nb', 'md')).toBe('````md\na\n```\nb\n````');
        expect(fence('plain\n')).toBe('```\nplain\n```');
    });

    it('treats $ sequences in the version literally (extreme)', () => {
        expect(resolveBeamVersion('v={{beam_version}} {{beam_version}}', "$&$'")).toBe("v=$&$' $&$'");
    });
});
