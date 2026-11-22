/**
 * End-to-End Integration Test for the Streaming Multi-Sink foreachBatch Pipeline (I7).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Generates a PySpark 4.x repository for a streaming Kafka pipeline with JSON payload parsing.
 * Fans out parsed records to Delta Lake and Cloud SQL sinks inside one foreachBatch writer.
 * Audits the generated repository with the multi-agent evaluator and runs pytest offline.
 */

import { describe, expect, it } from 'vitest';
import { loadSparkPipeline, runPipelineIntegrationTest } from '../../codegen-runner';

describe('E2E Pipeline: streaming-multi-sink-foreachbatch (I7)', () => {
    it(
        'generates and validates unified foreachBatch streaming writes to Delta Lake and Cloud SQL',
        async () => {
            const sparkContent = loadSparkPipeline(
                'e2e/streaming-multi-sink-foreachbatch/streaming-multi-sink-foreachbatch.spark'
            );
            const results = await runPipelineIntegrationTest({
                testName: 'streaming-multi-sink-foreachbatch',
                sparkContent,
                languages: ['Python'],
            });

            expect(results.Python).toBe(true);
        },
        300_000
    );
});
