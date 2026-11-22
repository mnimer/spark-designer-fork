/**
 * End-to-End Integration Test for the Stream-Static Join Enrichment Pipeline (I8).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Generates a PySpark 4.x repository joining a streaming Pub/Sub source with a BigQuery lookup.
 * Applies a broadcast left join and writes enriched events to a streaming BigQuery sink.
 * Audits the generated output with the multi-agent evaluator and runs pytest offline.
 */

import { describe, expect, it } from 'vitest';
import { loadSparkPipeline, runPipelineIntegrationTest } from '../../codegen-runner';

describe('E2E Pipeline: stream-static-join-enrichment (I8)', () => {
    it(
        'generates and validates stream-static broadcast join enrichment from Pub/Sub and BigQuery to BigQuery',
        async () => {
            const sparkContent = loadSparkPipeline(
                'e2e/stream-static-join-enrichment/stream-static-join-enrichment.spark'
            );
            const results = await runPipelineIntegrationTest({
                testName: 'stream-static-join-enrichment',
                sparkContent,
                languages: ['Python'],
            });

            expect(results.Python).toBe(true);
        },
        300_000
    );
});
