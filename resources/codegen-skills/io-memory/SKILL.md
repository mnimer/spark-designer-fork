---
name: io-memory
description: Register batch temporary SQL views and write Structured Streaming micro-batches to an in-memory query table in PySpark 4.x.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [MemorySink]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  validated: true
---

# In-Memory Table & Temp View Sink (`MemorySink`)

Use these rules when generating PySpark {{spark_version}} code for a `MemorySink` interactive SQL and test sink node.

## 1. Driver Memory Warning (Streaming `.format("memory")`)

- In Structured Streaming, `.writeStream.format("memory").queryName(query_name)` collects and retains all emitted output rows inside the Spark driver JVM heap so callers can query `spark.sql(f"SELECT * FROM {query_name}")`.
- Use streaming `MemorySink` only for bounded debugging, `availableNow=True` test runs, or compact `complete` aggregations. Never route high-volume unbounded production streams into `MemorySink` because unbounded accumulation exhausts driver memory (`OutOfMemoryError`).
- Log a clear warning with `logger.warning(...)` when starting a streaming `.format("memory")` query.

## 2. Identifier Validation

- Resolve the target SQL view or query name from `queryName` or `viewName` (for example `"debug_metrics"`).
- Validate that the identifier matches `^[A-Za-z_][A-Za-z0-9_]*$` before registering a view or query name so arbitrary strings cannot inject SQL tokens.

## 3. Batch Execution (`createOrReplaceTempView` and `createOrReplaceGlobalTempView`)

When `MemorySink` executes in batch mode (or inside a multi-sink `foreachBatch` function):
- If `globalTempView` is true, call `df.createOrReplaceGlobalTempView(view_name)` so sessions can query `global_temp.<view_name>`.
- Otherwise call `df.createOrReplaceTempView(view_name)` to register a session-scoped temporary view.
- Log the registered view name via `logger.info(...)` without calling `.collect()` or `.toPandas()`.

## 4. Streaming Execution (`.writeStream.format("memory").queryName(...)`)

When `MemorySink` runs as a native single-sink streaming query (`ExecutionPlan.streamingSinkGroups` `mode == "native"`):
- Build `writer = df.writeStream.format("memory").queryName(query_name)`.
- Configure `outputMode` (`"append"` or `"complete"` — note that Spark's built-in `memory` sink supports `"append"` and `"complete"`), `checkpointLocation`, and `trigger` (`processingTime` or `availableNow=True`).
- Emit a driver-heap advisory via `logger.warning(...)` and start the query with `.start()`.

## 5. Reference Implementation (`src/pipeline.py`)

```python
import logging
import re
from typing import Any
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)
VALID_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def resolve_memory_sink_name(config: dict[str, Any]) -> str:
    """Validates and returns the SQL view or streaming memory query name."""
    raw_name = (
        config.get("memory_query_name")
        or config.get("memory_view_name")
        or "memory_sink_view"
    ).strip()
    if not VALID_SQL_IDENTIFIER.match(raw_name):
        raise ValueError(f"Invalid SQL identifier for MemorySink: {raw_name!r}")
    return raw_name


def write_memory_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Registers a batch DataFrame as a session or global temporary SQL view."""
    view_name = resolve_memory_sink_name(config)
    use_global = bool(config.get("memory_global_temp_view", False))
    if use_global:
        df.createOrReplaceGlobalTempView(view_name)
        logger.info("Registered global temporary view global_temp.%s", view_name)
    else:
        df.createOrReplaceTempView(view_name)
        logger.info("Registered session temporary view %s", view_name)


def start_memory_stream(df: DataFrame, config: dict[str, Any]):
    """Starts an in-memory Structured Streaming query for interactive SQL inspection."""
    query_name = resolve_memory_sink_name(config)
    output_mode = config.get("memory_output_mode", "append")
    if output_mode not in ("append", "complete"):
        output_mode = "append"
    checkpoint_dir = config["memory_checkpoint_location"]
    trigger_interval = config.get("memory_trigger_interval", "10 seconds")

    logger.warning(
        "MemorySink %r buffers streaming output in Spark driver memory. "
        "Use only for bounded debugging and testing.",
        query_name,
    )
    return (
        df.writeStream.format("memory")
        .queryName(query_name)
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .start()
    )
```

## 6. Offline Unit Testing (`tests/test_pipeline.py`)

Verify `write_memory_batch` and SQL queries against the registered temporary view offline:

```python
from pyspark.sql import SparkSession
from src.pipeline import transform, write_memory_batch


def test_memory_sink_registers_temp_view(spark: SparkSession) -> None:
    """Verifies that MemorySink registers a queryable temporary view in batch mode."""
    input_df = spark.createDataFrame(
        [("us-central1", 42), ("europe-west1", 19)],
        ["region", "active_jobs"],
    )
    transformed = transform({"src": input_df}, {})
    write_memory_batch(
        transformed["src"],
        {"memory_query_name": "job_summary_view", "memory_global_temp_view": False},
    )
    queried = spark.sql("SELECT SUM(active_jobs) AS total FROM job_summary_view").collect()
    assert queried[0]["total"] == 61
```
