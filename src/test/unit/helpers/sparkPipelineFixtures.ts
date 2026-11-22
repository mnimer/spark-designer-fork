/**
 * Shared generated-repository fixtures for the Spark evaluator unit tests.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Provides a clean PySpark 4.x repository that follows decision D3
 * (`read_sources`, pure `transform(dfs, config)`, `write_sinks`) and passes every
 * deterministic SparkArchitectureAgent check. Tests derive defective variants from it.
 */

import { GeneratedFileCandidate } from '../../integration/evaluators/deterministic-gate';

/** Clean `src/pipeline.py` that passes every static Spark architecture rule. */
export const CLEAN_PIPELINE_PY = [
    '"""Generated PySpark pipeline."""',
    'from typing import Any',
    '',
    'from pyspark.sql import DataFrame, SparkSession',
    'from pyspark.sql import functions as F',
    '',
    '',
    'def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:',
    '    """Reads every source."""',
    '    return {"orders": spark.read.parquet(config["input_path"])}',
    '',
    '',
    'def transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]:',
    '    """Applies pure transformations."""',
    '    orders = dfs["orders"].filter(F.col("amount") > 0)',
    '    enriched = orders.withColumns({"amount_usd": F.col("amount") * 1.0}).cache()',
    '    return {"enriched": enriched}',
    '',
    '',
    'def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:',
    '    """Writes every sink."""',
    '    try:',
    '        dfs["enriched"].write.mode("overwrite").parquet(config["output_path"])',
    '    finally:',
    '        dfs["enriched"].unpersist()',
    '',
].join('\n');

/** Clean Dataproc runner script with a Scala 2.13 connector. */
export const CLEAN_DATAPROC_RUNNER = [
    '#!/usr/bin/env bash',
    'set -euo pipefail',
    'gcloud dataproc jobs submit pyspark src/pipeline.py \\',
    '  --cluster="${CLUSTER}" --region="${REGION}" --jars="${SPARK_JARS}" \\',
    '  --properties=spark.jars.packages=io.delta:delta-spark_2.13:4.0.0',
    '',
].join('\n');

/** Clean test file. Tests may collect small fixtures. */
export const CLEAN_TEST_PY = [
    'from src.pipeline import transform',
    '',
    'def test_transform(spark):',
    '    df = spark.createDataFrame([(1,)], ["amount"])',
    '    assert transform({"orders": df}, {})["enriched"].collect()',
    '',
].join('\n');

/** A clean generated repository. Returns a new array on every call. */
export function cleanSparkFiles(): GeneratedFileCandidate[] {
    return [
        { filename: 'src/pipeline.py', content: CLEAN_PIPELINE_PY },
        { filename: 'tests/test_pipeline.py', content: CLEAN_TEST_PY },
        { filename: 'run_dataproc_cluster.sh', content: CLEAN_DATAPROC_RUNNER, executable: true },
        { filename: 'run_local.sh', content: '#!/usr/bin/env bash\nset -euo pipefail\npython3 -m venv .venv\nexport JAVA_HOME="${JAVA_HOME}"\nspark-submit --jars "${SPARK_JARS}" --packages "${SPARK_PACKAGES}" --conf "spark.jars.ivySettings=.spark-ivy/ivysettings.xml" src/pipeline.py\n', executable: true },
        { filename: 'run_serverless_spark.sh', content: '#!/usr/bin/env bash\nset -euo pipefail\ngcloud dataproc batches submit pyspark src/pipeline.py --region="${REGION}" --version="${RUNTIME_VERSION:-4.0}" --jars="${SPARK_JARS}" --properties="${SPARK_PROPERTIES}"\n', executable: true },
        { filename: 'requirements.txt', content: 'pyspark==4.0.1\npytest>=8.0.0\n' },
        { filename: 'Dockerfile', content: 'FROM python:3.12-slim\nUSER 1000\n' },
        { filename: 'README.md', content: '# Generated pipeline\n' },
    ];
}
