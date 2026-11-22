---
name: core-pyspark
description: Core PySpark 4.x session initialization, testable three-phase pipeline structure, production logging, Secret Manager access, and dead-letter queue splits.
metadata:
  kind: language
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: []
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/index.html
  validated: true
---

# Core PySpark 4.x Architecture & Runtime Standards

Follow these rules in every generated PySpark 4.x repository. When `paradigm` is `sdp` (`## SDP Execution Plan`), Sections 1, 2, and 5 (`src/pipeline.py`, `SparkSession.builder`, and imperative `write_sinks`) are superseded by `sdp-core` (`transformations/*.py` + `src/transforms.py`). When `paradigm` is `notebook`, Section 1 (`src/pipeline.py`) is superseded by `notebook-core` (`pipeline.ipynb` + `src/transforms.py`). Section 4 (`access_secret_version` and `## Credential Handling Rules`) applies to all paradigms.

## 1. Testable Three-Phase Pipeline Structure (Imperative Mode Only)

Structure `src/pipeline.py` into three pure, decoupled functions plus a CLI `main()` entrypoint so `tests/test_pipeline.py` can test all transformation logic offline with in-memory DataFrames:

1. `read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]`
   - Reads all configured batch and streaming sources (`SOURCE` nodes).
   - Returns a dictionary mapping each source `nodeId` to its input `DataFrame`.
2. `transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]`
   - Accepts the dictionary of upstream DataFrames and runtime configuration.
   - Executes all `STEP` transformations in topological order (`ExecutionPlan.sortedNodeIds`).
   - Performs zero external I/O and creates no `SparkSession` instances.
   - Returns a dictionary containing all intermediate and terminal DataFrames keyed by `nodeId` (and `<nodeId>__valid` / `<nodeId>__error` or branch handles where applicable) so sinks and unit tests can inspect any node output.
3. `write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None`
   - Writes terminal DataFrames to their configured `SINK` targets (`df.write` or `df.writeStream`).
   - Releases cached batch DataFrames (`df.unpersist()`) at their scheduled `unpersistPoints` after downstream sink actions complete.
   - If the pipeline runs in streaming mode (`ExecutionPlan.pipelineMode == "streaming"`), starts all streaming queries and calls `spark.streams.awaitAnyTermination()`.

```python
import argparse
import logging
import os
from typing import Any
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def create_spark_session(app_name: str, config: dict[str, Any]) -> SparkSession:
    """Creates a standard SparkSession or ManagedSparkSession when Spark Connect is configured."""
    session_template = config.get("spark_connect_session_template") or os.environ.get(
        "SPARK_CONNECT_SESSION_TEMPLATE", ""
    ).strip()
    if session_template:
        from google.cloud.managed_spark_connect import ManagedSparkSession

        logger.info("Initializing ManagedSparkSession with template: %s", session_template)
        return ManagedSparkSession(session_template=session_template)

    builder = SparkSession.builder.appName(app_name)
    for key, value in config.get("spark_properties", {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads all pipeline source nodes and returns DataFrames keyed by node ID."""
    dfs: dict[str, DataFrame] = {}
    # Populate source DataFrames per SOURCE node
    return dfs


def transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]:
    """Applies pure DataFrame transformations in topological order."""
    out: dict[str, DataFrame] = dict(dfs)
    # Apply STEP nodes in ExecutionPlan.sortedNodeIds order
    return out


def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes terminal DataFrames to sinks and unpersists cached batch DataFrames."""
    # Execute SINK writes and unpersist cached DataFrames
    pass


def parse_args(argv: list[str] | None = None) -> dict[str, Any]:
    """Parses command-line arguments and environment defaults."""
    parser = argparse.ArgumentParser(description="Apache Spark 4.x Pipeline")
    parser.add_argument("--app-name", default=os.environ.get("APP_NAME", "spark-pipeline"))
    parser.add_argument("--checkpoint-root", default=os.environ.get("CHECKPOINT_ROOT", ""))
    parser.add_argument(
        "--spark-connect-session-template",
        default=os.environ.get("SPARK_CONNECT_SESSION_TEMPLATE", ""),
    )
    args, _ = parser.parse_known_args(argv)
    return vars(args)


def main(argv: list[str] | None = None) -> None:
    """Entrypoint for batch and streaming pipeline execution."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    config = parse_args(argv)
    spark = create_spark_session(config["app_name"], config)
    try:
        source_dfs = read_sources(spark, config)
        transformed_dfs = transform(source_dfs, config)
        write_sinks(transformed_dfs, config)
    finally:
        if not config.get("spark_connect_session_template"):
            spark.stop()


if __name__ == "__main__":
    main()
```

## 2. Session Initialization Rules

- **Never hardcode `.master("local[*]")` in `src/pipeline.py`**: Hardcoding `.master("local[*]")` overrides Dataproc Cluster and GCP Serverless Spark resource managers. Pass `--master local[*]` only in `run_local.sh` or inside `tests/test_pipeline.py` fixtures.
- **Support Spark Connect when configured**: Check `SPARK_CONNECT_SESSION_TEMPLATE` (or `--spark-connect-session-template`). When set, instantiate `ManagedSparkSession` from `google.cloud.managed_spark_connect`. Otherwise, use `SparkSession.builder.appName(app_name).getOrCreate()`.
- **Unit test fixture**: In `tests/test_pipeline.py`, create a module-scoped or session-scoped PySpark fixture with `SparkSession.builder.master("local[2]").appName("unit-tests").config("spark.sql.shuffle.partitions", "2").getOrCreate()`.

## 3. Production Logging

- Use Python's standard `logging` module (`logger = logging.getLogger(__name__)`).
- Never use `print()` or unconditional `df.show()` in `src/pipeline.py`.
- Log structured milestone events (`logger.info("Reading source node %s", node_id)`, `logger.info("Writing sink node %s", node_id)`).

## 4. Zero-Dependency Secret Manager Helper

When any node references a Google Cloud Secret Manager secret resource (`projects/<project>/secrets/<secret>/versions/<version>`) or a `<REDACTED_SECRET:...>` placeholder, include this zero-dependency helper using `google.auth` and `urllib.request` so no extra `google-cloud-secret-manager` package is required:

## Credential Handling Rules

- **Secret Manager Pointer Properties (`*Secret` suffix)**: Any node property whose name ends in `Secret` (for example `passwordSecret`, `accessKeySecret`, `secretKeySecret`, `tokenSecret`, `apiKeySecret`, `credentialsSecret`, `clientSecretSecret`, `sessionTokenSecret`, `connectionStringSecret`, `uriSecret`, or `secretName`) holds a **Secret Manager secret ID or version resource name**, NOT the raw secret value.
- **Resolve Once at Initialization**: Resolve the secret ONCE on the driver during `read_sources()` / `write_sinks()` or once per partition inside `mapInPandas` / `foreachPartition` — never per element or inside a row-level `DoFn` / UDF loop.
- **Redacted Credential Placeholders (`__REDACTED_BY_BEAM_DESIGNER__`)**: When a property or runtime variable contains `__REDACTED_BY_BEAM_DESIGNER__` (redacted before sending the YAML to the model), never emit `__REDACTED_BY_BEAM_DESIGNER__` as a literal in generated code. Read the credential from an environment variable (`os.environ["<VAR_NAME>"]`) or resolve the corresponding `*Secret` property via `access_secret_version(...)`.

```python
import base64
import json
import urllib.request
import google.auth
import google.auth.transport.requests


def access_secret_version(secret_version_name: str) -> str:
    """Fetches a UTF-8 secret payload from Google Cloud Secret Manager via REST."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)

    url = f"https://secretmanager.googleapis.com/v1/{secret_version_name}:access"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    encoded_data = payload["payload"]["data"]
    return base64.b64decode(encoded_data).decode("utf-8")
```

## 5. Dead-Letter Queue (DLQ) Split Pattern

When a step's `error` output handle is connected (`ExecutionPlan.dlqSplitPoints` / `dlqRoutes`), split the intermediate DataFrame into valid rows and error rows without computing the upstream plan twice:

1. **Cache before splitting (batch mode)**: When the splitting node is in `ExecutionPlan.cachedNodeIds` (or ` executionMode == "batch"`), call `.cache()` on the intermediate DataFrame before filtering valid and error branches.
2. **Apply the deterministic DLQ mechanism** specified in `ExecutionPlan.dlqSplitPoints`:
   - **`_corrupt_record`** (`ParsePayload`, `CsvSource`, `JsonSource`, `XmlSource`):
     - Parse with `mode="PERMISSIVE"` and `columnNameOfCorruptRecord="_corrupt_record"`.
     - Valid branch: `df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")`.
     - Error branch: `df.filter(F.col("_corrupt_record").isNotNull())`.
   - **`negated_predicate`** (`Filter`):
     - Valid branch: `df.filter(predicate_expr)`.
     - Error branch: `df.filter(~(predicate_expr)).withColumn("_error", F.lit("Filter predicate failed"))`.
   - **`expectation_violation`** (`DataQualityAssert`):
     - Evaluate all data quality expectation expressions into a boolean `_is_valid` column and an `_error` message column.
     - Valid branch: `df.filter(F.col("_is_valid")).drop("_is_valid", "_error")`.
     - Error branch: `df.filter(~F.col("_is_valid")).drop("_is_valid")`.
   - **`try_except_error_col`** (`PandasUdfTransform`, `CustomTransform`, `SparkSqlQuery`, `SparkMlAndVertexAi`):
     - Capture row-level or batch-level exceptions into a nullable string column `_error`.
     - Valid branch: `df.filter(F.col("_error").isNull()).drop("_error")`.
     - Error branch: `df.filter(F.col("_error").isNotNull())`.
3. **Unpersist after downstream writes**: Call `.unpersist()` on the cached intermediate DataFrame in `write_sinks()` after all consumers in `unpersistAfterNodeIds` finish writing.
