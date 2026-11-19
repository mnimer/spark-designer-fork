/**
 * Live End-to-End Integration Test for Batch Fan-Out Auto-Cache Pipeline (I6).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Generates a complete PySpark 4.x repository for a BigQuerySource to WithColumns
 * batch pipeline fanning out to both IcebergSink and SpannerSink, evaluates deterministic
 * .cache() and .unpersist() compliance with the multi-agent harness, and runs offline pytest.
 */

import { describe, expect, it } from 'vitest';
import { loadSparkPipeline, runPipelineIntegrationTest } from '../../codegen-runner';

describe('E2E Pipeline: batch-fanout-auto-cache (I6)', () => {
    it('generates and validates a batch fan-out pipeline with automatic caching and unpersist', async () => {
        const sparkContent = loadSparkPipeline(
            'e2e/batch-fanout-auto-cache/batch-fanout-auto-cache.spark'
        );

        const results = await runPipelineIntegrationTest({
            testName: 'batch-fanout-auto-cache',
            sparkContent,
            languages: ['Python'],
        });

        expect(results.Python).toBe(true);
    });
});
