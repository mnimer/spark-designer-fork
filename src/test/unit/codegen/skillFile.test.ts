/**
 * Unit tests for the strict skill file parser (`src/codegen/skillFile.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Covers simple cases (valid flow and block lists), extreme cases (CRLF line endings,
 * `---` rules inside the body, quoted values), and failure cases (missing or unclosed
 * frontmatter, unknown keys, malformed lists, name/folder mismatch).
 */

import { describe, it, expect } from 'vitest';
import { parseSkillFile } from '../../../codegen/skillFile';

/** A valid skill file with flow lists and a block list. */
const VALID = [
    '---',
    'name: io-kafka',
    'description: "KafkaIO read mode and write mode."',
    'metadata:',
    '  kind: io',
    '  usedBy: [generator, reviewer]',
    "  languages: ['all']",
    '  runners: [all]',
    '  transforms: [KafkaIO]',
    '  preloadWhen: []',
    '  sources:',
    '    - prompt',
    '    - "https://beam.apache.org/documentation/io/connectors/"',
    '  validated: false',
    '---',
    '### Node properties',
    'Rule one.',
    '',
    '---',
    '',
    'After a markdown rule.',
    '',
].join('\n');

describe('parseSkillFile', () => {
    it('parses name, description, flow lists, block lists, and the body (simple)', () => {
        const parsed = parseSkillFile(VALID, 'io-kafka/SKILL.md', 'io-kafka');
        expect(parsed.name).toBe('io-kafka');
        expect(parsed.description).toBe('KafkaIO read mode and write mode.');
        expect(parsed.metadata).toEqual({
            kind: 'io',
            usedBy: ['generator', 'reviewer'],
            languages: ['all'],
            runners: ['all'],
            transforms: ['KafkaIO'],
            preloadWhen: [],
            sources: ['prompt', 'https://beam.apache.org/documentation/io/connectors/'],
            validated: 'false',
        });
        expect(parsed.body.startsWith('### Node properties')).toBe(true);
    });

    it('keeps --- markdown rules inside the body (extreme)', () => {
        const parsed = parseSkillFile(VALID, 'label');
        expect(parsed.body).toContain('---');
        expect(parsed.body.endsWith('After a markdown rule.')).toBe(true);
    });

    it('skips blank lines inside the frontmatter (extreme)', () => {
        const parsed = parseSkillFile('---\nname: x\n\ndescription: d\nmetadata:\n\n  kind: io\n---\nbody', 'label');
        expect(parsed.metadata).toEqual({ kind: 'io' });
    });

    it('accepts CRLF line endings (extreme)', () => {
        const parsed = parseSkillFile(VALID.replace(/\n/g, '\r\n'), 'label', 'io-kafka');
        expect(parsed.metadata.transforms).toEqual(['KafkaIO']);
        expect(parsed.body).not.toContain('\r');
    });

    it('accepts a file with no metadata block and an empty body (extreme)', () => {
        const parsed = parseSkillFile("---\nname: x\ndescription: 'd'\n---\n", 'label');
        expect(parsed).toEqual({ name: 'x', description: 'd', metadata: {}, body: '' });
    });

    it('unquotes single-quoted and escaped double-quoted values (extreme)', () => {
        const parsed = parseSkillFile('---\nname: x\ndescription: "say \\"hi\\""\n---\nbody', 'label');
        expect(parsed.description).toBe('say "hi"');
    });

    it('throws when the frontmatter is missing (failure)', () => {
        expect(() => parseSkillFile('# Title\nbody', 'f.md')).toThrow(/must start with a '---'/);
    });

    it('throws when the frontmatter is not closed (failure)', () => {
        expect(() => parseSkillFile('---\nname: x\ndescription: d\n', 'f.md')).toThrow(/not closed/);
    });

    it('throws on an unknown top-level key (failure)', () => {
        expect(() => parseSkillFile('---\nname: x\ndescription: d\nauthor: me\n---\n', 'f.md')).toThrow(
            /unknown frontmatter key 'author'/
        );
    });

    it('throws on an unknown metadata key (failure)', () => {
        expect(() => parseSkillFile('---\nname: x\ndescription: d\nmetadata:\n  preloadwhen: []\n---\n', 'f.md')).toThrow(
            /unknown metadata key 'preloadwhen'/
        );
    });

    it('throws on a malformed list (failure)', () => {
        expect(() =>
            parseSkillFile('---\nname: x\ndescription: d\nmetadata:\n  transforms: [KafkaIO\n---\n', 'f.md')
        ).toThrow(/malformed list/);
    });

    it('throws when metadata is a scalar instead of a map (failure)', () => {
        expect(() => parseSkillFile('---\nname: x\ndescription: d\nmetadata: oops\n---\n', 'f.md')).toThrow(
            /must be a nested map/
        );
    });

    it('throws on a block list item without a parent key (failure)', () => {
        expect(() =>
            parseSkillFile('---\nname: x\ndescription: d\nmetadata:\n  kind: io\n    - stray\n---\n', 'f.md')
        ).toThrow(/has no parent key/);
    });

    it('throws on an unsupported frontmatter line (failure)', () => {
        expect(() => parseSkillFile('---\nname: x\n- item\ndescription: d\n---\n', 'f.md')).toThrow(
            /unsupported frontmatter line/
        );
    });

    it('throws when name or description is missing (failure)', () => {
        expect(() => parseSkillFile('---\ndescription: d\n---\n', 'f.md')).toThrow(/'name' is required/);
        expect(() => parseSkillFile('---\nname: x\n---\n', 'f.md')).toThrow(/'description' is required/);
        expect(() => parseSkillFile('---\nname: [a]\ndescription: d\n---\n', 'f.md')).toThrow(/'name' is required/);
    });

    it('throws when the name does not equal the folder name (failure)', () => {
        expect(() => parseSkillFile(VALID, 'io-kafka/SKILL.md', 'io-kafka-2')).toThrow(/must equal its folder name/);
    });
});
