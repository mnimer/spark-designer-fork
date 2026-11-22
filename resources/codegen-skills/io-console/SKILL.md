---
name: io-console
description: Log bounded DataFrame previews in batch mode and write Structured Streaming micro-batches to the console sink in PySpark 4.x.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [ConsoleSink]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  validated: true
---

# Console & Structured Log Sink (`ConsoleSink`)

Use these rules when generating PySpark {{spark_version}} code for a `ConsoleSink` (`LogMessage`) debug or dead-letter inspection node.

## 1. Safety Rules (No Unbounded Driver Collection or `print()`)

- Never call `print()`, unconditional `df.show()`, `df.toPandas()`, or unbounded `df.collect()` in `src/pipeline.py`.
- Always bound batch inspection with `df.limit(num_rows)` (clamped to a safe ceiling such as `1000` rows) before bringing sample rows to the driver via `.toLocalIterator()`.
- Emit batch previews through Python's standard `logging` module (`logger.info(...)` or `logger.log(level, ...)`) so output integrates with Cloud Logging on Serverless Spark and Dataproc Cluster.

## 2. Batch Execution (`df.limit(num_rows)` Structured `logging.info` Summary)

When `ConsoleSink` executes in batch mode (or inside a multi-sink `foreachBatch` function):
- Read `numRows` (default `20`), `truncate` (default `True`), and `logLevel` (`"INFO"`, `"DEBUG"`, `"WARNING"`, or `"ERROR"`, default `"INFO"`).
- Clamp `bounded_rows = max(1, min(int(num_rows), 1000))`.
- Iterate `df.limit(bounded_rows).toLocalIterator()` to format a bounded list of row dictionaries, truncating string values wider than `20` characters when `truncate` is true.
- Emit a structured summary message with `logger.info(...)` (or `logger.log(resolved_level, ...)` when a custom `logLevel` is configured) containing the schema field names, sampled row count, and serialized sample payload.

## 3. Streaming Execution (`.writeStream.format("console")`)

When `ConsoleSink` runs as a native single-sink streaming query (`ExecutionPlan.streamingSinkGroups` `mode == "native"`):
- Build `writer = df.writeStream.format("console")`.
- Pass `.option("numRows", str(num_rows))` and `.option("truncate", str(truncate).lower())`.
- Configure `outputMode` (`"append"`, `"update"`, or `"complete"`), `checkpointLocation`, and `trigger` (`processingTime` or `availableNow=True`).
- Log the streaming console sink startup at the configured `logLevel` and start the query with `.start()`.
- Note that `.format("console")` writes micro-batch tables to driver stdout for debugging and dead-letter inspection rather than durable storage.

## 4. Reference Implementation (`src/pipeline.py`)

```python
import json
import logging
from typing import Any
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_console_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Logs a bounded, structured preview of a batch DataFrame without unbounded collection."""
    raw_rows = int(config.get("console_num_rows", 20))
    bounded_rows = max(1, min(raw_rows, 1000))
    truncate = bool(config.get("console_truncate", True))
    level_name = str(config.get("console_log_level", "INFO")).upper()
    log_level = getattr(logging, level_name, logging.INFO)

    preview_rows: list[dict[str, Any]] = []
    for row in df.limit(bounded_rows).toLocalIterator():
        row_dict = row.asDict(recursive=True)
        if truncate:
            row_dict = {
                k: (f"{str(v)[:20]}..." if isinstance(v, str) and len(v) > 20 else v)
                for k, v in row_dict.items()
            }
        preview_rows.append(row_dict)

    if level_name == "INFO":
        logger.info(
            "ConsoleSink preview (limit=%d, sampled=%d, columns=%s): %s",
            bounded_rows,
            len(preview_rows),
            df.columns,
            json.dumps(preview_rows, default=str),
        )
    else:
        logger.log(
            log_level,
            "ConsoleSink preview (limit=%d, sampled=%d, columns=%s): %s",
            bounded_rows,
            len(preview_rows),
            df.columns,
            json.dumps(preview_rows, default=str),
        )


def start_console_stream(df: DataFrame, config: dict[str, Any]):
    """Starts a native Structured Streaming console sink query."""
    num_rows = int(config.get("console_num_rows", 20))
    truncate = str(config.get("console_truncate", True)).lower()
    output_mode = config.get("console_output_mode", "append")
    checkpoint_dir = config["console_checkpoint_location"]
    trigger_interval = config.get("console_trigger_interval", "10 seconds")

    logger.info(
        "Starting ConsoleSink stream (numRows=%d, truncate=%s, outputMode=%s)",
        num_rows,
        truncate,
        output_mode,
    )
    return (
        df.writeStream.format("console")
        .outputMode(output_mode)
        .option("numRows", str(num_rows))
        .option("truncate", truncate)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .start()
    )
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Verify `write_console_batch` and upstream `transform(dfs, config)` offline with `caplog` and in-memory DataFrames:

```python
import logging
from pyspark.sql import SparkSession
from src.pipeline import transform, write_console_batch


def test_console_sink_logs_bounded_preview(spark: SparkSession, caplog) -> None:
    """Verifies bounded structured logging for ConsoleSink."""
    input_df = spark.createDataFrame(
        [("evt-1", "long_diagnostic_message_payload"), ("evt-2", "ok")],
        ["event_id", "detail"],
    )
    transformed = transform({"src": input_df}, {})
    with caplog.at_level(logging.INFO):
        write_console_batch(
            transformed["src"],
            {"console_num_rows": 1, "console_truncate": True, "console_log_level": "INFO"},
        )
    assert "ConsoleSink preview" in caplog.text
```
