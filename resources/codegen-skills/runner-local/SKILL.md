---
name: runner-local
description: Local PySpark 4.x execution, offline pytest verification, and optional Google Cloud Spark Connect (ManagedSparkSession) runner rules and run_local.sh template.
metadata:
  kind: runner
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [local]
  transforms: []
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/submitting-applications.html
    - https://cloud.google.com/dataproc-serverless/docs/guides/spark-connect
  validated: true
---

### Local PySpark & Spark Connect Runner (`run_local.sh`)

Every generated PySpark repository MUST include an executable `run_local.sh` script at the repository root based on the template below.

#### 1. Responsibilities of `run_local.sh`
1. **Offline Unit Test Mode (`--test`)**:
   - Running `./run_local.sh --test` (or `./run_local.sh --test <pytest-args>`) MUST execute `python3 -m pytest tests/ -v` with `PYTHONPATH` rooted at the project directory.
   - Unit tests in `tests/test_pipeline.py` test `transform(dfs, config)` offline using a local in-memory `SparkSession` without requiring cloud credentials or network access.
2. **Optional Spark Connect Mode (`ManagedSparkSession`)**:
   - Triggered when `USE_SPARK_CONNECT=true` or `SPARK_CONNECT_SESSION_TEMPLATE` is non-empty.
   - Exports `USE_SPARK_CONNECT=true` and `SPARK_CONNECT_SESSION_TEMPLATE` (plus `PROJECT_ID` and `REGION`) so `src/pipeline.py` initializes its session via `google.cloud.managed_spark_connect.ManagedSparkSession` (or `SparkSession.builder.remote(...)` when `SPARK_REMOTE` is set) instead of spawning a local JVM driver.
   - Executes `python3 -m src.pipeline "$@"`.
3. **Local PySpark Execution (`local[*]`)**:
   - When Spark Connect is not enabled, runs the pipeline locally against `SPARK_MASTER="${SPARK_MASTER:-local[*]}"`.
   - If `spark-submit` is on `PATH` (and `USE_PYTHON_DIRECT` is not `"true"`), invokes `spark-submit --master "${SPARK_MASTER}"` with:
     - `--jars "${SPARK_JARS}"` when `SPARK_JARS` is set (comma-separated local paths or `gs://` URIs).
     - `--packages "${SPARK_PACKAGES}"` when `SPARK_PACKAGES` is set (comma-separated Scala 2.13 Maven coordinates from the Execution Plan, such as `io.delta:delta-spark_2.13:4.0.0` or `com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.42.1` for local runs).
     - Any comma-separated `key=value` entries in `SPARK_PROPERTIES` expanded as `--conf key=value` flags.
   - If `spark-submit` is not available on `PATH`, falls back to `python3 -m src.pipeline "$@"` with `SPARK_MASTER="${SPARK_MASTER:-local[*]}"`.

#### 2. Session & Environment Contract with `src/pipeline.py`
- **Never Hardcode `.master("local[*]")` Unconditionally in `src/pipeline.py`**:
  - In `src/pipeline.py`, only set `.master(os.environ["SPARK_MASTER"])` when `SPARK_MASTER` is explicitly present in `os.environ`, or rely on `spark-submit --master`. Hardcoding `.master("local[*]")` in `src/pipeline.py` breaks Dataproc Cluster and Serverless Spark deployments.
- **Spark Connect Initialization in `src/pipeline.py`**:
  - When `os.environ.get("USE_SPARK_CONNECT", "").lower() == "true"` or `os.environ.get("SPARK_CONNECT_SESSION_TEMPLATE")` is set, initialize the session using `ManagedSparkSession`:
    ```python
    from google.cloud.managed_spark_connect import ManagedSparkSession

     builder = ManagedSparkSession.builder.appName(app_name)
    if os.environ.get("PROJECT_ID"):
        builder = builder.projectId(os.environ["PROJECT_ID"])
    if os.environ.get("REGION"):
        builder = builder.location(os.environ["REGION"])
    if os.environ.get("SPARK_CONNECT_SESSION_TEMPLATE"):
        builder = builder.sessionTemplate(os.environ["SPARK_CONNECT_SESSION_TEMPLATE"])
    spark = builder.getOrCreate()
    ```
- **Local Checkpoint Fallback**:
  - `run_local.sh` defaults `CHECKPOINT_ROOT` to `/tmp/spark-checkpoints` when unset so streaming pipelines can run locally without requiring a `gs://` bucket unless configured.

#### 3. Reviewer Checklist for `run_local.sh`
- Starts with `#!/usr/bin/env bash` and `set -euo pipefail`.
- Handles `--test` first and runs `python3 -m pytest tests/ -v`.
- Supports `USE_SPARK_CONNECT=true` and `SPARK_CONNECT_SESSION_TEMPLATE`.
- Passes `--jars "${SPARK_JARS}"` and `--packages "${SPARK_PACKAGES}"` to `spark-submit` only when those variables are non-empty.
- Forwards all remaining CLI arguments (`"$@"`) to `src/pipeline.py`.
