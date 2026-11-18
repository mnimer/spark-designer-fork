---
name: io-rate
description: Generate synthetic streaming event rows at a controlled rate in PySpark 4.x using the built-in rate and rate-micro-batch sources.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [RateSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  validated: true
---

# Built-in Rate Streaming Source (`RateSource`)

Use these rules when generating PySpark {{spark_version}} code for a synthetic rate streaming source node (`RateSource` or `GenerateSequence` / `PeriodicImpulse`).

## 1. Runtime Delivery Mechanism

Both `.format("rate")` and `.format("rate-micro-batch")` are built into Apache Spark 4.x. Do not attach any external JARs or Maven packages.

## 2. Selecting `rate` vs `rate-micro-batch`

Inspect the node `rateFormat` property (default `"rate"`):
- **Standard wall-clock rate (`rateFormat == "rate"`)**: Uses `spark.readStream.format("rate")` to emit `(timestamp: TimestampType, value: LongType)` rows based on wall-clock time.
  - `rowsPerSecond` -> `.option("rowsPerSecond", str(config.get("rate_rows_per_second", 1)))`
  - `rampUpTime` -> `.option("rampUpTime", str(config["rate_ramp_up_time"]))` (for example `"5s"`)
  - `numPartitions` -> `.option("numPartitions", str(config["rate_num_partitions"]))`
- **Deterministic micro-batch rate (`rateFormat == "rate-micro-batch"`)**: Uses `spark.readStream.format("rate-micro-batch")` to emit a deterministic number of rows and advance logical event time by a fixed delta per micro-batch.
  - `rowsPerBatch` -> `.option("rowsPerBatch", str(config.get("rate_rows_per_batch", 100)))`
  - `advanceMillisPerBatch` -> `.option("advanceMillisPerBatch", str(config.get("rate_advance_millis_per_batch", 1000)))`
  - `numPartitions` -> `.option("numPartitions", str(config["rate_num_partitions"]))`

## 3. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads synthetic streaming rows from the built-in rate or rate-micro-batch source."""
    dfs: dict[str, DataFrame] = {}

    rate_format = config.get("rate_format", "rate")
    reader = spark.readStream.format(rate_format)

    if config.get("rate_num_partitions"):
        reader = reader.option("numPartitions", str(config["rate_num_partitions"]))

    if rate_format == "rate-micro-batch":
        reader = reader.option(
            "rowsPerBatch", str(config.get("rate_rows_per_batch", 100))
        ).option(
            "advanceMillisPerBatch",
            str(config.get("rate_advance_millis_per_batch", 1000)),
        )
    else:
        reader = reader.option(
            "rowsPerSecond", str(config.get("rate_rows_per_second", 1))
        )
        if config.get("rate_ramp_up_time"):
            reader = reader.option("rampUpTime", str(config["rate_ramp_up_time"]))

    dfs["src_rate"] = reader.load()
    return dfs
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing a static in-memory DataFrame with `(timestamp, value)` columns matching the built-in rate schema:

```python
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructField, StructType, TimestampType
from src.pipeline import transform


def test_rate_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory rate rows."""
    schema = StructType(
        [
            StructField("timestamp", TimestampType(), False),
            StructField("value", LongType(), False),
        ]
    )
    t0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    input_df = spark.createDataFrame([(t0, 0), (t0, 1), (t0, 2)], schema=schema)
    result_dfs = transform({"src_rate": input_df}, {})
    assert "src_rate" in result_dfs
    assert result_dfs["src_rate"].count() == 3
```
