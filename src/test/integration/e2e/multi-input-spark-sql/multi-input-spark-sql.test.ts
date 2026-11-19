/**
 * @fileoverview Live E2E integration test for the multi-input Spark SQL query pipeline (I9).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Generates a complete PySpark 4.x repository from the three-source Catalyst SQL pipeline,
 * audits the output with the multi-agent evaluation harness, and runs offline pytest checks.
 */

import { describe, expect, it } from 'vitest';
import { loadSparkPipeline, runPipelineIntegrationTest } from '../../codegen-runner';

/**
 * Verifies end-to-end PySpark code generation and evaluation for multi-input SparkSqlQuery.
 */
describe('E2E Pipeline: multi-input-spark-sql (I9)', () => {
    it('generates, evaluates, and tests the multi-input Spark SQL query pipeline in Python', async () => {
        const sparkContent = loadSparkPipeline(
            'e2e/multi-input-spark-sql/multi-input-spark-sql.spark'
        );
        const results = await runPipelineIntegrationTest({
            testName: 'multi-input-spark-sql',
            sparkContent,
            languages: ['Python'],
        });
        expect(results.Python).toBe(true);
    });
});
