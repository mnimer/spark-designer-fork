/**
 * Regression tests for Phase 3d skill fixes in the streaming and healthcare connector skills.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Each test pins a rule that was added after a generated project failed to compile or to pass
 * its unit tests (io-fhir, io-hl7v2, io-solace, io-jms).
 */

import { describe, it, expect } from 'vitest';
import { skillText } from '../helpers/codegenSkillsCorpus';

describe('Skill fixes from Phase 3d execution: streaming connectors (regression)', () => {
    it('io-fhir (Java) types the FhirIO write result as FhirIO.Write.AbstractResult', () => {
        // sinks-java-fhir-sink: `FhirIO.Write.Result r = ...apply(FhirIO.Write.executeBundles(...))`
        // failed with "inference variable OutputT has incompatible bounds".
        const text = skillText('io-fhir', 'java');
        expect(text).toContain('FhirIO.Write.AbstractResult writeResult = bundles');
        expect(text).toContain('Never declare it as `FhirIO.Write.Result`');
        expect(text).not.toContain('FhirIO.Write.Result writeResult');
    });

    it('io-fhir and io-hl7v2 (Java) name HealthcareIOError.getDataResource() and forbid getData()', () => {
        // The re-run of sinks-java-fhir-sink and sinks-java-hl7v2-sink called `error.getData()`.
        for (const skill of ['io-fhir', 'io-hl7v2']) {
            const text = skillText(skill, 'java');
            expect(text).toContain('There is no `getData()` method. Use `error.getDataResource()`');
        }
    });

    it('io-hl7v2 (Java) gives a zoned formatter for ER7 timestamps', () => {
        // The re-run of sinks-java-hl7v2-sink called the nonexistent Joda `Instant.toString(String)`.
        const text = skillText('io-hl7v2', 'java');
        expect(text).toContain('.withZone(java.time.ZoneOffset.UTC)');
        expect(text).toContain('Joda `org.joda.time.Instant` has no `toString(String pattern)` method');
    });

    it('io-hl7v2 (Java) forbids HL7v2Message setters and requires HL7v2MessageCoder', () => {
        // sinks-java-hl7v2-sink called `new HL7v2Message()` and `setData(...)`.
        // sources-java-hl7v2-source used `Create.of(hl7v2Message)` without a coder.
        const text = skillText('io-hl7v2', 'java');
        expect(text).toContain('`HL7v2Message` has no no-argument constructor');
        expect(text).toContain('Create.of(hl7v2Message).withCoder(HL7v2MessageCoder.of())');
        expect(text).toContain('import org.apache.beam.sdk.io.gcp.healthcare.HL7v2MessageCoder;');
    });

    it('io-solace (Java) requires setMessageId on Solace.Record.builder()', () => {
        // sinks-java-solace-sink threw "Missing required properties: messageId" at runtime.
        const text = skillText('io-solace', 'java');
        expect(text).toContain('`Solace.Record.builder()` requires `setMessageId(String)` and `setPayload(byte[])`');
        expect(text).toContain('.setMessageId("order-" + sequenceNumber)');
    });

    it('io-jms (Java) names JmsRecord.getJmsTimestamp() and forbids getTimestamp()', () => {
        // sources-java-jms-source called the nonexistent `record.getTimestamp()`.
        const text = skillText('io-jms', 'java');
        expect(text).toContain('`getJmsTimestamp()`');
        expect(text).toContain('`JmsRecord` has no `getTimestamp()` method');
    });

    it('io-jms (Java) documents the 12-argument JmsRecord constructor for tests', () => {
        // The re-run of sources-java-jms-source called the JmsRecord constructor with 11 arguments.
        const text = skillText('io-jms', 'java');
        expect(text).toContain('The `JmsRecord` constructor has exactly 12 arguments');
        expect(text).toContain('String, long, String, Destination, Destination, int, boolean, String, long, int, Map<String, Object>, String');
        // Attempt 2 then failed with an NPE in JmsRecord.equals() because jmsDestination was null.
        expect(text).toContain('Never pass `null` for `jmsDestination`');
        expect(text).not.toContain('Pass `null` for both `Destination` arguments');
    });
});
