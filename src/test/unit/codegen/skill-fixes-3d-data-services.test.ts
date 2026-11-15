/**
 * Regression tests for Phase 3d skill fixes in the data-service and logging connector skills.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Each test pins a rule that was added after a generated project failed to compile, install,
 * or pass its unit tests (io-datadog, io-splunk, io-elasticsearch, io-google-ads, io-solr,
 * io-request-response, io-tinybird, io-pyodbc).
 */

import { describe, it, expect } from 'vitest';
import { skillText } from '../helpers/codegenSkillsCorpus';

describe('Skill fixes from Phase 3d execution: data services and logging (regression)', () => {
    it('io-datadog (Java) uses DatadogWriteError accessors without a get prefix', () => {
        // sinks-java-datadog-sink: cannot find symbol getStatusCode() / getStatusMessage().
        const text = skillText('io-datadog', 'java');
        expect(text).toContain('`statusCode()`, `statusMessage()`, and `payload()`');
        expect(text).toContain('Never call `getStatusCode()`');
        expect(text).toContain('error.statusCode(), error.statusMessage(), error.payload()');
    });

    it('io-datadog (Java) names the DatadogEvent getters ddsource() and ddtags()', () => {
        // sinks-java-datadog-sink rerun: the test called event.source() and event.tags().
        const text = skillText('io-datadog', 'java');
        expect(text).toContain('`ddsource()`, `ddtags()`, `hostname()`, `service()`, and `message()`');
        expect(text).toContain('`source()`, `tags()`, and every `get...()` form do not exist');
    });

    it('io-datadog (Java) sets DatadogEventCoder on PCollection<DatadogEvent>', () => {
        // sinks-java-datadog-sink rerun 2: PAssert failed with "Unable to return a default Coder".
        expect(skillText('io-datadog', 'java')).toContain('.setCoder(DatadogEventCoder.of())');
    });

    it('io-splunk (Java) uses SplunkWriteError accessors without a get prefix', () => {
        // sinks-java-splunk-sink: cannot find symbol getStatusCode().
        const text = skillText('io-splunk', 'java');
        expect(text).toContain('`statusCode()`, `statusMessage()`, and `payload()`');
        expect(text).toContain('Never call `getStatusCode()`');
    });

    it('io-elasticsearch (Java) says FieldValueExtractFn takes a JsonNode, not a String', () => {
        // sinks-java-elasticsearch-sink: DocumentIdExtractFn implemented apply(String).
        const text = skillText('io-elasticsearch', 'java');
        expect(text).toContain('extends `SerializableFunction<JsonNode, String>`');
        expect(text).toContain('Never write `apply(String input)`');
        expect(text).toContain('ElasticsearchIO.Write.FieldValueExtractFn idFn =');
    });

    it('io-google-ads (Java) extends GcpOptions so that getProject() compiles', () => {
        // sources-java-googleads-source: options extending only GoogleAdsOptions had no getProject().
        const text = skillText('io-google-ads', 'java');
        expect(text).toContain('`GoogleAdsOptions` extends only `PipelineOptions`');
        expect(text).toContain('extends GoogleAdsOptions, GcpOptions');
    });

    it('io-solr (Java) passes a List to Create.of because SolrDocument is a Map and an Iterable', () => {
        // sources-java-solr-source: "reference to of is ambiguous" for Create.of(solrDocument).
        const text = skillText('io-solr', 'java');
        expect(text).toContain('`Create.of(doc)` with one `SolrDocument` is ambiguous');
        expect(text).toContain('Create.of(Arrays.asList(doc1, doc2))');
    });

    it('io-request-response (Java) handles the checked NonDeterministicException of Cache.usingRedis', () => {
        // sources-java-webapis-source: unreported exception Coder.NonDeterministicException.
        const text = skillText('io-request-response', 'java');
        expect(text).toContain('declares `throws Coder.NonDeterministicException`');
    });

    it('io-tinybird (Python) forbids the tinybird-beam package and posts to the HTTP API', () => {
        // sinks-python-tinybird-sink: pip ResolutionImpossible (tinybird-beam pins apache-beam==2.27.0).
        const text = skillText('io-tinybird', 'python');
        expect(text).toContain('Never add `tinybird-beam`');
        expect(text).toContain('pins `apache-beam[gcp]==2.27.0`');
        expect(text).toContain('class WriteToTinybirdDoFn(beam.DoFn):');
        expect(text).not.toContain('from tinybird.beam import');
    });

    it('io-pyodbc (Python) imports pyodbc lazily and installs unixODBC in the Dockerfile', () => {
        // sinks-python-pyodbc-sink and sources-python-pyodbc-source: ImportError libodbc.so.2 at test collection.
        const text = skillText('io-pyodbc', 'python');
        expect(text).toContain('Never write `import pyodbc` at module level');
        expect(text).toContain('**Lazy import rule (CRITICAL)**');
        expect(text).toContain('import pyodbc  # Lazy import. See the lazy import rule.');
        expect(text).toContain('apt-get install -y --no-install-recommends unixodbc');
        expect(text).not.toContain('- `import pyodbc`\n');
    });
});
