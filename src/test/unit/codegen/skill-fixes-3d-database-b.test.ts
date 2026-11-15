/**
 * Regression tests for Phase 3d skill fixes in the database connector skills (group B).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Each test pins a rule that was added after a generated project failed to compile, install,
 * or pass its unit tests (io-neo4j, io-bigtable, io-mysql, io-database).
 */

import { describe, it, expect } from 'vitest';
import { skillText } from '../helpers/codegenSkillsCorpus';

describe('Skill fixes from Phase 3d execution: database connectors B (regression)', () => {
    it('io-neo4j (Java) requires Neo4jIO.RowMapper, not SerializableFunction', () => {
        // sources-java-neo4j-source: `class PersonCompanyRowMapper implements SerializableFunction<Record, Row>`
        // failed with "cannot be converted to Neo4jIO.RowMapper<Row>".
        const text = skillText('io-neo4j', 'java');
        expect(text).toContain('implements Neo4jIO.RowMapper<Row>');
        expect(text).toContain('public Row mapRow(Record record) throws Exception');
        expect(text).toContain('It is not a `SerializableFunction`');
    });

    it('io-bigtable (Python) forbids reading DirectRow private cells in tests', () => {
        // sinks-python-bigtable-sink: the test read `row._cells` and failed with AttributeError.
        const text = skillText('io-bigtable', 'python');
        expect(text).toContain('`row._cells` does not exist');
        expect(text).toContain('(row_key, [(family, qualifier, value), ...])');
    });

    it('io-mysql (Python) uses ReadFromJdbc/WriteToJdbc and never beam-mysql-connector (user decision)', () => {
        // beam-mysql-connector is unmaintained and installs only on Python < 3.12
        // (sinks-python-mysql-sink / sources-python-mysql-source failed to install).
        const text = skillText('io-mysql', 'python');
        expect(text).toContain('from apache_beam.io.jdbc import ReadFromJdbc, WriteToJdbc');
        expect(text).toContain("classpath=['com.mysql:mysql-connector-j:8.3.0']");
        expect(text).toContain('Never import `beam_mysql`');
        expect(text).not.toMatch(/from beam_mysql/);
    });

    it('io-database (Python) states the ReadFromJdbc TIMESTAMP type and exact test expectations', () => {
        // sources-python-database-source: the test expected '2023-05-20 14:00:00' but the DoFn
        // returned the ISO form '2023-05-20T14:00:00'.
        const text = skillText('io-database', 'python');
        expect(text).toContain('`apache_beam.utils.timestamp.Timestamp`');
        expect(text).toContain('.to_utc_datetime()');
        expect(text).toContain("returns `'2023-05-20T14:00:00'`");
        // Second run: the test expected '+00:00' but to_utc_datetime() is naive by default.
        expect(text).toContain('returns a naive `datetime` by default (`has_tz=False`)');
    });
});
