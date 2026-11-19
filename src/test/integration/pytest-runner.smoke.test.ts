/**
 * Smoke test for the offline pytest runner against a real local PySpark session.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Writes a minimal repository that follows the generated layout (`src/pipeline.py`
 * with a pure `transform(dfs, config)` and `tests/test_pipeline.py`). It then runs
 * the runner end to end. If local tooling is missing, the test logs the reason
 * and passes. It never needs network access or cloud credentials.
 *
 * Set `SPARK_PYTEST_PYTHON` to point at an interpreter that has `pyspark>=4` and `pytest`.
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { afterAll, describe, expect, it } from 'vitest';
import { formatPytestReport, runGeneratedRepoTests } from './pytest-runner';

const PIPELINE_PY = `from pyspark.sql import DataFrame, functions as F


def transform(dfs, config):
    orders: DataFrame = dfs["orders"]
    out = orders.filter(F.col("amount") > config.get("min_amount", 0))
    return {"large_orders": out.withColumn("amount_x2", F.col("amount") * 2)}
`;

const TEST_PIPELINE_PY = `from pipeline import transform


def test_transform_filters_and_derives(spark):
    df = spark.createDataFrame([(1, 5), (2, 50)], ["id", "amount"])
    out = transform({"orders": df}, {"min_amount": 10})["large_orders"]
    rows = [r.asDict() for r in out.collect()]
    assert rows == [{"id": 2, "amount": 50, "amount_x2": 100}]


def test_transform_fails_on_purpose(spark):
    df = spark.createDataFrame([(1, 5)], ["id", "amount"])
    out = transform({"orders": df}, {"min_amount": 0})["large_orders"]
    assert out.count() == 2, "expected two rows"
`;

const tempDirs: string[] = [];

/** Creates a temporary repository with the generated layout. */
function makeRepo(): string {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'spark-gen-repo-'));
    tempDirs.push(dir);
    fs.mkdirSync(path.join(dir, 'src'));
    fs.mkdirSync(path.join(dir, 'tests'));
    fs.writeFileSync(path.join(dir, 'src', 'pipeline.py'), PIPELINE_PY);
    fs.writeFileSync(path.join(dir, 'tests', 'test_pipeline.py'), TEST_PIPELINE_PY);
    return dir;
}

afterAll(() => {
    for (const dir of tempDirs) {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

describe('offline pytest runner (local PySpark 4.x)', () => {
    it('runs generated tests and reports one pass and one failure, or skips without tooling', async () => {
        const repo = makeRepo();
        const result = await runGeneratedRepoTests(repo, {
            python: process.env.SPARK_PYTEST_PYTHON || 'python3',
            timeoutMs: 170_000,
        });
        console.log(formatPytestReport(result));
        if (result.status === 'skipped') {
            expect(result.reason).toBeTruthy();
            return;
        }
        expect(result.conftest).toBe('written');
        expect(result).toMatchObject({ status: 'failed', passed: 1, failed: 1, errors: 0 });
        expect(result.failures[0].test).toContain('test_transform_fails_on_purpose');
        expect(result.failures[0].message).toContain('expected two rows');
    });
});
