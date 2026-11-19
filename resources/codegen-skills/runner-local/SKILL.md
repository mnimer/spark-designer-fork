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
Copy the template below verbatim. Change only the `SPARK_PACKAGES`, `SPARK_JARS`, and `SPARK_PROPERTIES` defaults to the Execution Plan values for the Local runner.

1. **Automatic Bootstrap (runs before every mode)**:
   - **Python**: Creates `.venv` with `"${PYTHON_BIN:-python3}" -m venv` if it is missing. Installs `requirements.txt` only when its SHA-256 hash differs from the stamp file `.venv/.requirements.sha256`. Runs every mode with the venv Python and the venv `spark-submit` that the `pyspark` wheel installs.
   - **Java**: PySpark 4.0 needs JDK 17 or 21 (JDK 25 fails with `getSubject is not supported`). Uses `JAVA_HOME` if it is JDK 17 or 21. Otherwise scans `/usr/libexec/java_home`, `java` on `PATH`, `/usr/lib/jvm/*`, `~/.sdkman/candidates/java/*`, `~/.jdks/*`, macOS `JavaVirtualMachines`, and the JDK cache. If no JDK 17 or 21 exists, downloads Eclipse Temurin 21 from the Adoptium API into `${XDG_CACHE_HOME:-~/.cache}/spark-pipeline-builder/jdk-21`, verifies its SHA-256 checksum, and exports `JAVA_HOME`. Spark Connect mode skips the Java step.
   - **Maven (`--packages`)**: Writes a Maven Central-only Ivy settings file to `.spark-ivy/ivysettings.xml` and passes `--conf spark.jars.ivySettings=...` and `--conf spark.jars.ivy=.spark-ivy`. The default Ivy chain trusts `~/.m2` entries that have a POM but no JAR and fails with `download failed`.
   - **Overrides**: `PYTHON_BIN` selects the Python that creates the venv. `SKIP_VENV_BOOTSTRAP=true` uses `PYTHON_BIN` as is. `AUTO_INSTALL_JDK=false` fails fast instead of downloading a JDK. `JDK_CACHE_DIR` moves the JDK cache. `SPARK_IVY_SETTINGS` supplies a custom Ivy settings file. `spark.jars.ivySettings` or `spark.jars.ivy` in `SPARK_PROPERTIES` replace the defaults.
2. **Offline Unit Test Mode (`--test`)**:
   - Running `./run_local.sh --test` (or `./run_local.sh --test <pytest-args>`) MUST execute `pytest tests/ -v` with the venv Python and `PYTHONPATH` rooted at the project directory.
   - Unit tests in `tests/test_pipeline.py` test `transform(dfs, config)` offline using a local in-memory `SparkSession` without requiring cloud credentials or network access.
3. **Optional Spark Connect Mode (`ManagedSparkSession`)**:
   - Triggered when `USE_SPARK_CONNECT=true` or `SPARK_CONNECT_SESSION_TEMPLATE` is non-empty.
   - Exports `USE_SPARK_CONNECT=true` and `SPARK_CONNECT_SESSION_TEMPLATE` (plus `PROJECT_ID` and `REGION`) so `src/pipeline.py` initializes its session via `google.cloud.managed_spark_connect.ManagedSparkSession` (or `SparkSession.builder.remote(...)` when `SPARK_REMOTE` is set) instead of spawning a local JVM driver.
   - Executes `"${PYTHON}" -m src.pipeline "$@"` with the venv Python.
4. **Local PySpark Execution (`local[*]`)**:
   - When Spark Connect is not enabled, runs the pipeline locally against `SPARK_MASTER="${SPARK_MASTER:-local[*]}"`.
   - If `spark-submit` is on `PATH` (the venv provides it, and `USE_PYTHON_DIRECT` is not `"true"`), invokes `spark-submit --master "${SPARK_MASTER}"` with:
     - `--jars "${SPARK_JARS}"` when `SPARK_JARS` is set (comma-separated local paths or `gs://` URIs).
     - `--packages "${SPARK_PACKAGES}"` when `SPARK_PACKAGES` is set (comma-separated Scala 2.13 Maven coordinates from the Execution Plan, such as `io.delta:delta-spark_2.13:4.0.0` or `com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.42.1` for local runs), plus the Ivy settings above.
     - Any comma-separated `key=value` entries in `SPARK_PROPERTIES` expanded as `--conf key=value` flags.
   - If `spark-submit` is not available, runs `"${PYTHON}" -m src.pipeline "$@"` and passes the same arguments through `PYSPARK_SUBMIT_ARGS` (unless the user already set it).
5. **Version Alignment**:
   - Local Maven coordinates target Spark 4.0 (`_2.13`). `requirements.txt` MUST pin `pyspark=={{spark_version}}.*` so pip never installs a Spark minor without matching connector builds.

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
- Bootstraps `.venv` from `requirements.txt` with a hash stamp, selects or installs a JDK 17 or 21, and honors `PYTHON_BIN`, `SKIP_VENV_BOOTSTRAP`, and `AUTO_INSTALL_JDK`.
- Passes a Maven Central `spark.jars.ivySettings` file and a project-local `spark.jars.ivy` directory with `--packages`.
- Handles `--test` and runs `pytest tests/ -v` with the venv Python.
- Supports `USE_SPARK_CONNECT=true` and `SPARK_CONNECT_SESSION_TEMPLATE`.
- Passes `--jars "${SPARK_JARS}"` and `--packages "${SPARK_PACKAGES}"` to `spark-submit` only when those variables are non-empty.
- Forwards all remaining CLI arguments (`"$@"`) to `src/pipeline.py`.
