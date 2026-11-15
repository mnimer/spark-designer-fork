/**
 * Regression tests for Phase 3d skill fixes in the file connector skills.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Each test pins a rule that was added after a generated project failed to compile or to pass
 * its unit tests (io-parquet, io-avro, io-s3-file-system, io-hadoop-format).
 */

import { describe, it, expect } from 'vitest';
import { skillText } from '../helpers/codegenSkillsCorpus';

describe('Skill fixes from Phase 3d execution: file connectors (regression)', () => {
    it('io-parquet (Java) requires hadoop-client because ParquetIO declares Hadoop as provided', () => {
        // sources-java-parquet-source and sinks-java-parquet-sink: tests failed with
        // NoClassDefFoundError from WritableCoder$WritableCoderProviderRegistrar.
        const text = skillText('io-parquet', 'java');
        expect(text).toContain('<artifactId>hadoop-client</artifactId>');
        expect(text).toContain('Always add `org.apache.hadoop:hadoop-client:3.4.2`');
    });

    it('io-parquet (Java) sets AvroCoder on Create.of(GenericRecord) in tests', () => {
        // sources-java-parquet-source: "Unable to infer a coder" for Create.of(GenericRecord).
        const text = skillText('io-parquet', 'java');
        expect(text).toContain('.withCoder(AvroCoder.of(ParquetPipeline.SOURCE_SCHEMA))');
        expect(text).toContain('Beam cannot infer a coder for `GenericRecord`');
    });

    it('io-avro (Java) sets AvroCoder on Create.of(GenericRecord) in tests', () => {
        // sources-java-avro-source: "Unable to infer a coder" for Create.of(GenericRecord).
        const text = skillText('io-avro', 'java');
        expect(text).toContain('.withCoder(AvroCoder.of(SCHEMA))');
        expect(text).toContain('org.apache.beam.sdk.extensions.avro.coders.AvroCoder');
    });

    it('io-s3-file-system (Java) adds format dependencies and the GenericRecord coder rule', () => {
        // sources-java-s3filesystem-source (format ParquetIO): NoClassDefFoundError from WritableCoder.
        const text = skillText('io-s3-file-system', 'java');
        expect(text).toContain('If `format` is `ParquetIO`, never omit `org.apache.hadoop:hadoop-client:3.4.2`');
        expect(text).toContain('`org.apache.beam:beam-sdks-java-io-parquet`');
        expect(text).toContain('.withCoder(AvroCoder.of(SCHEMA))');
    });

    it('io-hadoop-format (Java) ends the write chain with withExternalSynchronization', () => {
        // sinks-java-hadoopformat-sink: `.withoutPartitioning()` returned ExternalSynchronizationBuilder
        // and `apply(...)` failed with "no suitable method found for apply".
        const text = skillText('io-hadoop-format', 'java');
        expect(text).toContain('.withoutPartitioning()\n        .withExternalSynchronization(new HDFSSynchronization(locksDir))');
        expect(text).toContain('return `ExternalSynchronizationBuilder`, not a `PTransform`');
        expect(text).toContain('<version>3.4.2</version>');
    });
});
