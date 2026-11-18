---
name: io-spanner
description: Read and write Google Cloud Spanner tables in PySpark 4.x using the Cloud Spanner Spark connector with Partitioned Read API, Data Boost, batch mutations, and streaming foreachBatch.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [SpannerSource, SpannerSink]
  preloadWhen: []
  sources:
    - prompt
    - https://cloud.google.com/spanner/docs/use-spark-connector
  validated: true
---

# Google Cloud Spanner Connector (`SpannerSource`, `SpannerSink`)

Use these rules when generating PySpark 4.x code for `SpannerSource` or `SpannerSink` nodes.

## 1. Runtime Classpath and Connector Delivery

- Supply the Cloud Spanner Spark 4.0 connector JAR with `--jars gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar` (or `spark.jars` in Spark properties) across Serverless Spark, Dataproc Cluster, and Local runners.
- Use `.format("cloud-spanner")` in `spark.read` and `df.write`.
- Never access `spark` internal JVM handles in application code.

## 2. Batch Read Patterns (`SpannerSource` in `read_sources`)

`SpannerSource` runs in `batch` mode and distributes parallel reads across executors using the Spanner Partitioned Read API.

### Required and Optional Read Options
- `projectId`: Google Cloud project ID that hosts the Spanner instance.
- `instanceId`: Cloud Spanner instance ID.
- `databaseId`: Cloud Spanner database ID.
- `table` or `query`: Specify either `.option("table", "<table_name>")` to read a table or `.option("query", "<sql_query>")` to execute a partitionable SQL query.
- `enableDataBoost`: When configured, set `.option("enableDataBoost", "true")` to run analytical queries on isolated Spanner Data Boost compute capacity with zero impact on transactional workloads.
- `timestampBound`: Optional staleness or snapshot bound (such as strong reads or an exact RFC 3339 timestamp) for consistent point-in-time reads.

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads a Cloud Spanner table or SQL query via the Cloud Spanner Spark connector."""
    dfs: dict[str, DataFrame] = {}

    reader = (
        spark.read.format("cloud-spanner")
        .option("projectId", config["spanner_project_id"])
        .option("instanceId", config["spanner_instance_id"])
        .option("databaseId", config["spanner_database_id"])
        .option("enableDataBoost", str(config.get("enable_data_boost", True)).lower())
    )

    if config.get("timestamp_bound"):
        reader = reader.option("timestampBound", config["timestamp_bound"])

    if config.get("spanner_query"):
        dfs["spanner_source"] = reader.option("query", config["spanner_query"]).load()
    else:
        dfs["spanner_source"] = reader.option("table", config["spanner_table"]).load()

    return dfs
```

## 3. Batch Write Patterns (`SpannerSink` in `write_sinks`)

`SpannerSink` writes DataFrames to an existing Cloud Spanner table with `df.write.format("cloud-spanner")`.

### Required and Optional Write Options
- `projectId`: Target Google Cloud project ID.
- `instanceId`: Target Cloud Spanner instance ID.
- `databaseId`: Target Cloud Spanner database ID.
- `table`: Destination Cloud Spanner table name. DataFrame column names must match Spanner table column names, including all primary key columns.
- Save mode: Apply `.mode(save_mode)` using `"append"`, `"overwrite"`, `"errorifexists"`, or `"ignore"`.
- Mutation mode (`mutationMode`): When configured, pass `.option("mutationType", mutation_mode)` (`"insert_or_update"` for idempotent upserts, `"insert"`, `"update"`, or `"replace"`).
- Batching and FGAC: When configured, pass `.option("mutationsPerTransaction", str(config["mutations_per_transaction"]))` and `.option("databaseRole", config["database_role"])`.

```python
import logging
from typing import Any
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_spanner_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a batch DataFrame to a Cloud Spanner table."""
    save_mode = config.get("spanner_save_mode", "append")
    mutation_mode = config.get("spanner_mutation_mode", "insert_or_update")

    writer = (
        df.write.format("cloud-spanner")
        .mode(save_mode)
        .option("projectId", config["spanner_project_id"])
        .option("instanceId", config["spanner_instance_id"])
        .option("databaseId", config["spanner_database_id"])
        .option("table", config["spanner_sink_table"])
        .option("mutationType", mutation_mode)
    )

    if config.get("mutations_per_transaction"):
        writer = writer.option(
            "mutationsPerTransaction", str(config["mutations_per_transaction"])
        )

    if config.get("database_role"):
        writer = writer.option("databaseRole", config["database_role"])

    if config.get("emulator_host"):
        writer = writer.option("emulatorHost", config["emulator_host"])

    logger.info("Writing DataFrame to Cloud Spanner table %s", config["spanner_sink_table"])
    writer.save()
```

## 4. Structured Streaming Micro-Batch Writes (`foreachBatch`)

Cloud Spanner connector writes operate in batch and micro-batch mode (`streamingSinkMode == "foreachBatch"`). Never call `.writeStream.format("cloud-spanner")` directly.

- When a streaming branch terminates in `SpannerSink`, wrap the batch writer inside `df.writeStream.foreachBatch(write_micro_batch)`.
- Configure `mutationType="insert_or_update"` so primary-key writes remain idempotent if Spark retries a failed micro-batch.
- When multiple sinks share the micro-batch group, call `batch_df.cache()` before writing and `batch_df.unpersist()` inside a `finally` block.
- Always configure `checkpointLocation`, `outputMode`, and `trigger`, then call `spark.streams.awaitAnyTermination()`.

```python
from typing import Any
from pyspark.sql import DataFrame


def start_spanner_stream(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a streaming DataFrame to Cloud Spanner using foreachBatch."""
    checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
    checkpoint_location = config.get(
        "spanner_checkpoint_location", f"{checkpoint_root.rstrip('/')}/spanner_sink"
    )

    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        try:
            write_spanner_batch(batch_df, config)
        finally:
            batch_df.unpersist()

    stream_writer = (
        df.writeStream.foreachBatch(write_micro_batch)
        .outputMode(config.get("spanner_output_mode", "append"))
        .option("checkpointLocation", checkpoint_location)
    )

    if config.get("spanner_trigger_type") == "availableNow":
        stream_writer.trigger(availableNow=True).start()
    else:
        interval = config.get("spanner_trigger_interval", "10 seconds")
        stream_writer.trigger(processingTime=interval).start()

    df.sparkSession.streams.awaitAnyTermination()
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by constructing an in-memory PySpark `DataFrame` with the expected Cloud Spanner schema instead of opening a network connection to Spanner:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType
from src.pipeline import transform


def test_spanner_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using an offline DataFrame."""
    schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("balance_cents", LongType(), False),
        ]
    )
    input_df = spark.createDataFrame(
        [("acc-100", "ACTIVE", 50000), ("acc-200", "CLOSED", 0)],
        schema=schema,
    )
    result_dfs = transform({"spanner_source": input_df}, {})
    assert result_dfs["spanner_source"].count() == 2
```
