/**
 * Live End-to-End Integration Test for Zero-Transform CSV to Parquet Replication (I5).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Generates a complete PySpark 4.x repository from a direct CsvSource to ParquetSink
 * pipeline with zero intermediate steps, evaluates architectural compliance with the
 * multi-agent evaluation harness, and runs offline pytest verification.
 */

import { describe, expect, it } from 'vitest';
import { loadSparkPipeline, runPipelineIntegrationTest } from '../../codegen-runner';

describe('E2E Pipeline: zero-transform-csv-to-parquet (I5)', () => {
    it('generates and validates a direct CsvSource to ParquetSink PySpark pipeline', async () => {
        const sparkContent = loadSparkPipeline(
            'e2e/zero-transform-csv-to-parquet/zero-transform-csv-to-parquet.spark'
        );

        const results = await runPipelineIntegrationTest({
            testName: 'zero-transform-csv-to-parquet',
            sparkContent,
            languages: ['Python'],
        });

        expect(results.Python).toBe(true);
    });
});
