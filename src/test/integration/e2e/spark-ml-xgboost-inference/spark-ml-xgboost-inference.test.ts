/**
 * @fileoverview Live E2E integration test for the Spark ML XGBoost inference pipeline (I10).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Generates a complete PySpark 4.x repository for Iceberg feature engineering, distributed
 * SparkXGBClassifier scoring with vector_to_array conversion, ConditionalSplit risk routing,
 * and BigQuery sinks, then audits the output with the multi-agent evaluator and pytest.
 */

import { describe, expect, it } from 'vitest';
import { loadSparkPipeline, runPipelineIntegrationTest } from '../../codegen-runner';

/**
 * Verifies end-to-end PySpark code generation and evaluation for Spark ML XGBoost inference.
 */
describe('E2E Pipeline: spark-ml-xgboost-inference (I10)', () => {
    it('generates, evaluates, and tests the Spark ML XGBoost inference pipeline in Python', async () => {
        const sparkContent = loadSparkPipeline(
            'e2e/spark-ml-xgboost-inference/spark-ml-xgboost-inference.spark'
        );
        const results = await runPipelineIntegrationTest({
            testName: 'spark-ml-xgboost-inference',
            sparkContent,
            languages: ['Python'],
        });
        expect(results.Python).toBe(true);
    });
});
