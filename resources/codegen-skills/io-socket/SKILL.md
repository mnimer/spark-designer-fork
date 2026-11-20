---
name: io-socket
description: Read UTF-8 text lines from a TCP socket in PySpark 4.x Structured Streaming for local development and debugging.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [SocketSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  validated: true
---

# TCP Socket Debug Streaming Source (`SocketSource`)

Use these rules when generating PySpark {{spark_version}} code for a TCP socket streaming source node (`SocketSource`).

## 1. Runtime Delivery Mechanism

The `.format("socket")` streaming source is built into Apache Spark 4.x. Do not attach any external JARs or Maven packages. Note in `README.md` that `.format("socket")` is intended for development and debugging because it does not provide end-to-end fault-tolerant offset replay.

## 2. Supported Node Properties

Map the `SocketSource` node properties to `spark.readStream.format("socket")` options in `read_sources(spark, config)`:
- `host` -> `.option("host", config.get("socket_host", "localhost"))`
- `port` -> `.option("port", str(config.get("socket_port", 9999)))`
- `includeTimestamp` -> `.option("includeTimestamp", str(config.get("socket_include_timestamp", False)).lower())`:
  - When `"false"` (default), the output schema is `value STRING`.
  - When `"true"`, the output schema is `value STRING, timestamp TIMESTAMP`.

## 3. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads streaming text lines from a TCP socket source."""
    dfs: dict[str, DataFrame] = {}

    include_ts = bool(config.get("socket_include_timestamp", False))
    dfs["src_socket"] = (
        spark.readStream.format("socket")
        .option("host", config.get("socket_host", "localhost"))
        .option("port", str(config.get("socket_port", 9999)))
        .option("includeTimestamp", str(include_ts).lower())
        .load()
    )
    return dfs
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame matching the socket output schema without opening a network socket:

```python
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from src.pipeline import transform


def test_socket_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory socket lines."""
    schema = StructType(
        [
            StructField("value", StringType(), False),
            StructField("timestamp", TimestampType(), False),
        ]
    )
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    input_df = spark.createDataFrame(
        [("hello spark", now), ("structured streaming", now)],
        schema=schema,
    )
    result_dfs = transform({"src_socket": input_df}, {})
    assert "src_socket" in result_dfs
    assert result_dfs["src_socket"].count() == 2
```
