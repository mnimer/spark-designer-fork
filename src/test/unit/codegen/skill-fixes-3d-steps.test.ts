/**
 * Regression tests for Phase 3d skill fixes in the core step transform skills.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Each test pins a rule that was added after a generated project failed to compile or to pass
 * its unit tests (transforms-element, transforms-flatten-reshuffle, transforms-aggregation,
 * side-inputs).
 */

import { describe, it, expect } from 'vitest';
import { skillText } from '../helpers/codegenSkillsCorpus';

describe('Skill fixes from Phase 3d execution: core step transforms (regression)', () => {
    it('transforms-element (Java) uses KvSwap.create() and forbids KvSwap.of()', () => {
        // steps-java-key-value-manipulation: `KvSwap.of()` failed with "cannot find symbol".
        const text = skillText('transforms-element', 'java');
        expect(text).toContain('`KvSwap.create()`');
        expect(text).toContain('`KvSwap` has no `of()` factory method');
        expect(text).not.toContain('/ `KvSwap.of()` /');
    });

    it('transforms-flatten-reshuffle (Java) uses Reshuffle.viaRandomKey() for non-KV input', () => {
        // steps-java-batching-and-grouping: `Reshuffle.of()` on a PCollection<List<...>> failed
        // with "no suitable method found for apply".
        const text = skillText('transforms-flatten-reshuffle', 'java');
        expect(text).toContain('Reshuffle.viaRandomKey()');
        expect(text).toContain('`Reshuffle.of()` accepts only `PCollection<KV<K, V>>`');
    });

    it('transforms-aggregation (Java) imports CoGroupByKey from the join package', () => {
        // steps-java-relational-cogroupbykey-join: `import org.apache.beam.sdk.transforms.CoGroupByKey`
        // failed with "cannot find symbol".
        const text = skillText('transforms-aggregation', 'java');
        expect(text).toContain('Import `org.apache.beam.sdk.transforms.join.CoGroupByKey`');
        expect(text).toContain('The class `org.apache.beam.sdk.transforms.CoGroupByKey` does not exist');
    });

    it('transforms-aggregation (Python) forbids the apache_beam.combiners module', () => {
        // steps-python-distinct-sampling-approximate: `from apache_beam.combiners import Latest, Sample`
        // failed with "No module named 'apache_beam.combiners'".
        const text = skillText('transforms-aggregation', 'python');
        expect(text).toContain('The module `apache_beam.combiners` does not exist');
        expect(text).toContain('from apache_beam.transforms.combiners import Latest, Sample');
        expect(text).toContain('`beam.combiners.Sample.FixedSizeGlobally(n)`');
    });

    it('transforms-aggregation (Python) passes CombineFn side inputs to every method', () => {
        // steps-python-combine-and-context: `create_accumulator(self)` failed with
        // "got an unexpected keyword argument 'context'".
        const text = skillText('transforms-aggregation', 'python');
        expect(text).toContain('Beam passes the side inputs to EVERY `CombineFn` method');
        expect(text).toContain('def create_accumulator(self, context):');
    });

    it('side-inputs (Java) reuses the production key extractor in unit tests', () => {
        // steps-java-pardo-side-inputs: the test rewrote the key lambda as `"EUR".contains(json)`,
        // so the View.asMap() lookup missed and the main output was empty.
        expect(skillText('side-inputs', 'java')).toContain('Key the test side input with the SAME function as production code');
        expect(skillText('side-inputs', 'python')).toContain('Key the test side input with the SAME module-level function');
    });
});
