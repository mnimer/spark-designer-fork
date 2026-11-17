---
name: verification-gate
description: Final pre-output verification checklist enforcing PySpark 4.x architecture, ExecutionPlan compliance, and security rules.
metadata:
  kind: gate
  usedBy: [generator, reviewer]
  languages: [all]
  runners: [all]
  transforms: []
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/
    - https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers
  validated: true
---
Before emitting the JSON output, verify that every generated file satisfies this checklist:

1. **Required Deliverable Files**:
   - `src/pipeline.py`, `tests/test_pipeline.py`, `requirements.txt`, `Dockerfile`, `run_local.sh`, `run_dataproc_cluster.sh`, `run_serverless_spark.sh`, and `README.md` are all present and complete.
2. **Testable 3-Phase Separation in `src/pipeline.py`**:
   - `src/pipeline.py` defines `read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]`, `transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]`, and `write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None`.
   - `transform(dfs, config)` is pure DataFrame/SQL logic with zero external cloud or network I/O.
   - `tests/test_pipeline.py` imports `transform` from `src.pipeline` and tests it offline with `spark.createDataFrame(...)`.
3. **Deterministic `ExecutionPlan` Compliance**:
   - Nodes are evaluated in `ExecutionPlan.sortedNodeIds` order.
   - Every node in `ExecutionPlan.cacheNodes` calls `.cache()` before branching and `.unpersist()` at the corresponding `ExecutionPlan.unpersistPoints` after downstream sinks complete.
   - Every group in `ExecutionPlan.streamingSinkGroups` with `mode: "foreachBatch"` uses `.writeStream.foreachBatch(write_micro_batch)`, wraps multi-sink micro-batches in `batch_df.cache()` and `try ... finally: batch_df.unpersist()`, and passes idempotent `txnVersion` / `txnAppId` where supported.
   - Every split in `ExecutionPlan.dlqRoutes` routes valid rows to `validTargetIds` and corrupt/error rows (`_corrupt_record` or `_error`) to `errorTargetIds`.
   - Runtime JARs (`runtimeJars`), Maven packages (`runtimePackages`), and Spark properties (`sparkProperties`) match `ExecutionPlan.runtimeArtifacts`, and pre-installed Managed Spark connectors (`spark-bigquery`, `iceberg-spark-runtime`, `gcs-connector`) are NEVER passed via `--packages` on Dataproc Cluster or Serverless Spark.
4. **Catalyst & Serverless Spark 4.x Hygiene**:
   - Zero calls to `spark._jsc`, `spark._jvm`, `spark.sparkContext`, or `.rdd` APIs.
   - Zero `.toPandas()` or `.collect()` calls on unbounded or production DataFrames in `src/pipeline.py`.
   - `SparkSession.builder` in `src/pipeline.py` never hardcodes `.master("local[*]")` unconditionally; local master is controlled by `run_local.sh` or test fixtures.
   - Entrypoint guard uses matching quotes (`if __name__ == "__main__":`) and `logging` instead of `print()`.
5. **Property Completeness & Zero Leftover Tokens**:
   - Walk the `properties` map of EVERY node in the pipeline YAML; silently dropping any configured property is a blocking defect.
   - No `{{...}}` placeholder tokens or `__REDACTED_BY_SPARK_DESIGNER__` / `__REDACTED_BY_BEAM_DESIGNER__` sentinels remain in any file; redacted secrets are resolved via `os.environ` or the Secret Manager helper.
