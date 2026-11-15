/**
 * Regression tests for Phase 3d skill fixes in the database connector skills (group A).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Each test pins a rule that was added after a generated Java project failed to compile
 * (io-datastore, io-spanner, io-firestore, io-influxdb, io-kudu, io-mongodb, io-mongodb-gridfs,
 * io-snowflake).
 */

import { describe, it, expect } from 'vitest';
import { skillText } from '../helpers/codegenSkillsCorpus';

describe('Skill fixes from Phase 3d execution: database connectors A (regression)', () => {
    it('io-datastore (Java) requires the full name com.google.datastore.v1.Key inside a DoFn', () => {
        // sinks/sources-java-datastore: `Key.Builder` inside a DoFn resolved to the DoFn.Key annotation.
        const text = skillText('io-datastore', 'java');
        expect(text).toContain('com.google.datastore.v1.Key.Builder key = makeKey(');
        expect(text).toContain('Do not import `com.google.datastore.v1.Key`');
        expect(text).toContain('`DoFn.Key`');
    });

    it('io-datastore (Java) names DatastoreV1.Write as the write type (attempt 2)', () => {
        // sinks-java-datastore-sink re-run: `DatastoreIO.v1.Write` is not a type.
        const text = skillText('io-datastore', 'java');
        expect(text).toContain('Never write `DatastoreIO.v1.Write`');
        expect(text).toContain('DatastoreV1.Write write = DatastoreIO.v1().write()');
    });

    it('io-spanner (Java) requires the full name com.google.cloud.Timestamp inside a DoFn', () => {
        // sinks/sources-java-spanner: `Timestamp` inside a DoFn resolved to the DoFn.Timestamp annotation.
        const text = skillText('io-spanner', 'java');
        expect(text).toContain('Do not import `com.google.cloud.Timestamp`');
        expect(text).toContain('`DoFn.Timestamp`');
        expect(text).toContain('com.google.cloud.Timestamp.parseTimestamp(');
    });

    it('io-spanner (Java) names com.google.cloud.spanner.Value (attempt 2)', () => {
        // sources-java-spanner-source re-run: the test imported the nonexistent `com.google.cloud.Value`.
        const text = skillText('io-spanner', 'java');
        expect(text).toContain('There is no `com.google.cloud.Value`');
        expect(text).toContain('Struct.newBuilder().set("id").to(1L)');
    });

    it('io-firestore (Java) uses TypeDescriptor.of and the real WriteFailure accessors', () => {
        // sources-java-firestore-source: `TypeDescriptors.valuesOf(Document.class)` does not exist.
        // sinks-java-firestore-sink: `WriteFailure.getThrowable()` does not exist.
        const text = skillText('io-firestore', 'java');
        expect(text).toContain('MapElements.into(TypeDescriptor.of(Document.class))');
        expect(text).toContain('`TypeDescriptors` has no `valuesOf(Class)` method');
        expect(text).toContain('It has no `getThrowable()`');
        expect(text).toContain('failure.getStatus().getMessage()');
    });

    it('io-influxdb (Java) always passes three arguments to DataSourceConfiguration.create', () => {
        // sources-java-influxdb-source: `create(StaticValueProvider.of(url))` did not compile.
        const text = skillText('io-influxdb', 'java');
        expect(text).toContain('There is no one-argument or two-argument overload');
        expect(text).toContain('StaticValueProvider.of("")');
    });

    it('io-kudu (Java) declares a typed FormatFunction before withFormatFn', () => {
        // sinks-java-kudu-sink: the raw `Write` return type made the inline lambda parameter `Object`.
        const text = skillText('io-kudu', 'java');
        expect(text).toContain('KuduIO.FormatFunction<MyRecord> formatFn = tableAndRecord ->');
        expect(text).toContain('.withFormatFn(formatFn)');
        expect(text).toContain('return the raw type `Write`');
    });

    it('io-mongodb (Java) warns that Create.of(Document) picks the Map overload', () => {
        // sources-java-mongodb-source: the test `Create.of(doc)` emitted KV<String, Object>.
        const text = skillText('io-mongodb', 'java');
        expect(text).toContain('picks the `Create.of(Map<K, V>)` overload');
        expect(text).toContain('.withCoder(SerializableCoder.of(Document.class))');
    });

    it('io-mongodb-gridfs (Java) pins the Parser and WriteFn signatures', () => {
        // sources-java-mongodbgridfs-source: `parse(GridFSDBFile)` does not implement Parser.
        // sinks-java-mongodbgridfs-sink: `write(...) throws Exception` does not override WriteFn.
        const text = skillText('io-mongodb-gridfs', 'java');
        expect(text).toContain(
            'void parse(GridFSFile gridFSFile, GridFSDownloadStream downloadStream, ParserCallback<T> callback) throws IOException',
        );
        expect(text).toContain('void write(T output, OutputStream outStream) throws IOException');
        expect(text).toContain('never `throws Exception`');
        expect(text).toContain('Never use the legacy driver class `com.mongodb.gridfs.GridFSDBFile`');
    });

    it('io-snowflake (Java) builds table schemas with SnowflakeColumn and type objects', () => {
        // sinks-java-snowflake-sink: `SnowflakeTableSchema.Column.of("ID", "NUMBER")` does not exist.
        const text = skillText('io-snowflake', 'java');
        expect(text).toContain('SnowflakeColumn.of("ID", SnowflakeNumber.of())');
        expect(text).toContain('Never write `SnowflakeTableSchema.Column.of(...)`');
    });
});
