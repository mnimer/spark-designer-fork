---
name: io-delta
description: Read batch, time-travel, streaming, and Change Data Feed records from Delta Lake tables in PySpark 4.x.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [DeltaSource]
  preloadWhen: []
  sources:
    - prompt
    - https://docs.delta.io/latest/delta-batch.html
    - https://docs.delta.io/latest/delta-streaming.html
  validated: true
---

# Delta Lake Source (`DeltaSource`)

Use these rules when generating PySpark {{spark_version}} code for a Delta Lake source node (`DeltaSource`).

## 1. Runtime Classpath and Session Extensions

- Attach the Scala 2.13 Delta Lake Spark 4.0 package (`io.delta:delta-spark_2.13:4.0.0`) through `--packages` or `spark.jars.packages` in the runner scripts (`run_local.sh`, `run_dataproc_cluster.sh`, and `run_serverless_spark.sh`).
- Configure the Delta Lake SQL extension and catalog properties on the `SparkSession`:
  - `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`
  - `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
- Include `delta-spark>=4.0.0` in `requirements.txt` and `Dockerfile`.

## 2. Batch, Time-Travel, Change Data Feed, and Streaming Options

Inspect `readMode` (default `"batch"`):
- **Batch mode (`readMode == "batch"`)**: Read with `spark.read.format("delta")`:
  - `versionAsOf` -> `.option("versionAsOf", str(config["delta_version_as_of"]))`
  - `timestampAsOf` -> `.option("timestampAsOf", config["delta_timestamp_as_of"])` (ISO-8601 timestamp string such as `"2026-01-01T00:00:00Z"`)
- **Streaming mode (`readMode == "streaming"`)**: Read with `spark.readStream.format("delta")`:
  - `maxFilesPerTrigger` -> `.option("maxFilesPerTrigger", str(config["delta_max_files_per_trigger"]))`
  - `maxBytesPerTrigger` -> `.option("maxBytesPerTrigger", str(config["delta_max_bytes_per_trigger"]))`
  - `ignoreChanges` -> `.option("ignoreChanges", str(config["delta_ignore_changes"]).lower())`
  - `ignoreDeletes` -> `.option("ignoreDeletes", str(config["delta_ignore_deletes"]).lower())`
- **Change Data Feed (`readChangeFeed == True`)**: Supported in both batch and streaming modes:
  - Set `.option("readChangeFeed", "true")`.
  - Pass `.option("startingVersion", str(config["delta_starting_version"]))` or `.option("startingTimestamp", config["delta_starting_timestamp"])`.
  - When `readChangeFeed=true`, Delta Lake appends `_change_type` (`"insert"`, `"update_preimage"`, `"update_postimage"`, `"delete"`), `_commit_version`, and `_commit_timestamp` columns to the DataFrame.
- **Path vs Table loading**: Load from a storage URI via `.load(config["delta_path"])` when `sourceMode == "path"` (default), or via `.table(config["delta_table_name"])` when `sourceMode == "table"`.

## 3. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads a Delta Lake table in batch, time-travel, or streaming mode."""
    dfs: dict[str, DataFrame] = {}

    read_mode = config.get("delta_read_mode", "batch")
    reader = (
        spark.readStream.format("delta")
        if read_mode == "streaming"
        else spark.read.format("delta")
    )

    if config.get("delta_read_change_feed"):
        reader = reader.option("readChangeFeed", "true")
        if config.get("delta_starting_version") is not None:
            reader = reader.option("startingVersion", str(config["delta_starting_version"]))
        elif config.get("delta_starting_timestamp"):
            reader = reader.option("startingTimestamp", config["delta_starting_timestamp"])

    if read_mode == "batch":
        if config.get("delta_version_as_of") is not None:
            reader = reader.option("versionAsOf", str(config["delta_version_as_of"]))
        elif config.get("delta_timestamp_as_of"):
            reader = reader.option("timestampAsOf", config["delta_timestamp_as_of"])
    else:
        if config.get("delta_max_files_per_trigger"):
            reader = reader.option(
                "maxFilesPerTrigger", str(config["delta_max_files_per_trigger"])
            )
        if config.get("delta_ignore_changes") is not None:
            reader = reader.option(
                "ignoreChanges", str(config["delta_ignore_changes"]).lower()
            )
        if config.get("delta_ignore_deletes") is not None:
            reader = reader.option(
                "ignoreDeletes", str(config["delta_ignore_deletes"]).lower()
            )

    if config.get("delta_source_mode") == "table" and config.get("delta_table_name"):
        dfs["src_delta"] = reader.table(config["delta_table_name"])
    else:
        dfs["src_delta"] = reader.load(config["delta_path"])

    return dfs
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame matching the Delta table schema (including `_change_type` when Change Data Feed is enabled) without requiring a Delta table on disk:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from src.pipeline import transform


def test_delta_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory Delta Lake rows."""
    schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("balance", DoubleType(), False),
            StructField("_change_type", StringType(), True),
            StructField("_commit_version", LongType(), True),
        ]
    )
    input_df = spark.createDataFrame(
        [("acc-1", 250.0, "insert", 1), ("acc-2", 500.0, "update_postimage", 2)],
        schema=schema,
    )
    result_dfs = transform({"src_delta": input_df}, {})
    assert "src_delta" in result_dfs
    assert result_dfs["src_delta"].count() == 2
```
