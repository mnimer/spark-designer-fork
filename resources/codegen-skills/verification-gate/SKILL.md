---
name: verification-gate
description: Final pre-output verification checklist enforcing PySpark 4.x architecture, ExecutionPlan compliance, and security rules.
metadata:
  kind: gate
  usedBy: [generator, reviewer]
  languages: [all]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: []
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/
    - https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers
  validated: true
---
Before emitting the JSON output, verify that every generated file satisfies the checklist for the active pipeline paradigm (`imperative` by default, or `sdp` when `## SDP Execution Plan` is present):

1. **Required Deliverable Files**:
   - **Imperative mode**: `src/pipeline.py`, `tests/test_pipeline.py`, `requirements.txt`, `Dockerfile`, `run_local.sh`, `run_dataproc_cluster.sh`, `run_serverless_spark.sh`, and `README.md` are all present and complete.
   - **SDP mode (`paradigm: sdp`)**: `spark-pipeline.yml`, `transformations/*.py`, `src/transforms.py`, `tests/test_transforms.py`, `requirements.txt`, `sdp_stage.pipeline.yaml`, `run_sdp_local.sh`, `run_sdp_dataproc.sh`, and `README.md` are all present and complete.
2. **Testable Separation & Pure Helpers**:
   - **Imperative mode**: `src/pipeline.py` defines `read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]`, `transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]`, and `write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None`. `transform(dfs, config)` is pure DataFrame/SQL logic with zero external cloud or network I/O, and `tests/test_pipeline.py` tests it offline with `spark.createDataFrame(...)`.
   - **SDP mode (`paradigm: sdp`)**: `transformations/*.py` dataset functions return lazy DataFrames only and delegate transformation and expectation filter logic to pure functions in `src/transforms.py`. `tests/test_transforms.py` imports `src.transforms` and tests every helper offline with `spark.createDataFrame(...)`.
3. **Deterministic Plan Compliance**:
   - **Imperative mode (`## Execution Plan`)**:
     - Nodes are evaluated in `ExecutionPlan.sortedNodeIds` order.
     - Every node in `ExecutionPlan.cacheNodes` calls `.cache()` before branching and `.unpersist()` at the corresponding `ExecutionPlan.unpersistPoints` after downstream sinks complete.
     - Every group in `ExecutionPlan.streamingSinkGroups` with `mode: "foreachBatch"` uses `.writeStream.foreachBatch(write_micro_batch)`, wraps multi-sink micro-batches in `batch_df.cache()` and `try ... finally: batch_df.unpersist()`, and passes idempotent `txnVersion` / `txnAppId` where supported.
     - Every split in `ExecutionPlan.dlqRoutes` routes valid rows to `validTargetIds` and corrupt/error rows (`_corrupt_record` or `_error`) to `errorTargetIds`.
   - **SDP mode (`## SDP Execution Plan`)**:
     - `spark-pipeline.yml` contains ONLY allowed top-level keys (`name`, `storage`, `catalog`, `database`, `schema`, `configuration`, `libraries`), uses `transformations/**` in `libraries` (never `transformations/*.py`), quotes all `configuration` values as strings, uses a plain identifier `catalog` alias, and places ZERO static Spark settings (`spark.jars*`, `spark.sql.extensions`, `spark.sql.catalogImplementation`, `spark.driver.*`, `spark.executor.*`, `spark.dynamicAllocation.*`, `spark.files`, `spark.archives`) inside `configuration:`.
     - Every planned dataset in `## SDP Execution Plan` uses the exact dataset name and OSS Spark 4.2 decorator (`@dp.table`, `@dp.materialized_view`, `@dp.temporary_view`, `dp.create_streaming_table` + `@dp.append_flow`, `dp.create_auto_cdc_flow` with `stored_as_scd_type=1`, or `dp.create_sink`).
     - `transformations/*.py` contains ZERO calls to `SparkSession.builder`, `spark.conf.set`, `createOrReplaceTempView`, `.write`, `.writeStream`, `.writeTo`, `saveAsTable`, `.collect()`, `.count()`, `.show()`, `.toPandas()`, `.schema`, or `.columns`, and ZERO Databricks-only APIs (`dlt`, `@dp.view`, `@dp.expect*`, `dp.apply_changes`, `once=`, `stored_as_scd_type=2`, `cloudFiles`).
   - Runtime JARs (`runtimeJars`), Maven packages (`runtimePackages`), and Spark properties (`sparkProperties`) match `runtimeArtifacts`, and pre-installed Managed Spark connectors (`spark-bigquery`, `iceberg-spark-runtime`, `gcs-connector`) are NEVER passed via `--packages` on Dataproc Cluster or Serverless Spark.
4. **Catalyst & Serverless Spark 4.x Hygiene**:
   - Zero calls to `spark._jsc`, `spark._jvm`, `spark.sparkContext`, or `.rdd` APIs.
   - Zero `.toPandas()` or `.collect()` calls on unbounded or production DataFrames.
   - In Imperative mode, `SparkSession.builder` in `src/pipeline.py` never hardcodes `.master("local[*]")` unconditionally, and the entrypoint guard uses matching quotes (`if __name__ == "__main__":`) and `logging` instead of `print()`.
5. **Property Completeness & Zero Leftover Tokens**:
   - Walk the `properties` map of EVERY node in the pipeline YAML. Silently dropping any configured property is a blocking defect.
   - No `{{...}}` placeholder tokens or `__REDACTED_BY_SPARK_DESIGNER__` / `__REDACTED_BY_BEAM_DESIGNER__` sentinels remain in any file. Redacted secrets are resolved via `os.environ` or the Secret Manager helper.
