---
name: io-bigquery
description: Read and write Google BigQuery tables in PySpark 4.x using the Storage Read and Storage Write APIs with the pre-installed Managed Spark connector.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [BigQuerySource, BigQuerySink]
  preloadWhen: []
  sources:
    - prompt
    - https://github.com/GoogleCloudDataproc/spark-bigquery-connector
  validated: true
---

# Google BigQuery Connector (`BigQuerySource`, `BigQuerySink`)

Use these rules when generating PySpark 4.x code for `BigQuerySource` or `BigQuerySink` nodes.

## 1. Runtime Classpath and Connector Delivery

- **Managed Spark (`ServerlessSpark` and `DataprocCluster`)**: The `spark-bigquery` connector comes pre-installed on Google Cloud Serverless for Apache Spark (`4.0` and `3.0`) and Dataproc Cluster (`3.0`). Never add `spark-bigquery` to `spark.jars.packages` or `--packages` on Managed Spark targets. Adding a duplicate JAR causes classpath conflicts.
- **Local Runner (`Local`)**: Pass `--packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.42.1` only in `run_local.sh`.
- Never access `spark` internal JVM handles in application code. Configure options directly on `DataFrameReader`, `DataFrameWriter`, or `DataStreamWriter`.

## 2. Batch Read Patterns (`BigQuerySource` in `read_sources`)

`BigQuerySource` supports `batch` mode through the high-throughput BigQuery Storage Read API (`.format("bigquery")`).

### Reading a Table (`table`)
- Set `.option("table", "<project>.<dataset>.<table>")` or pass the table identifier to `.load("<project>.<dataset>.<table>")`.
- When the billing project differs from the table project, set `.option("parentProject", config["parent_project"])`.
- To push row predicates down to the BigQuery Storage Read API before rows cross the network, set `.option("filter", "<sql_predicate>")`.
- When `maxParallelism` is configured, set `.option("maxParallelism", str(config["max_parallelism"]))`.

### Reading a Custom SQL Query or View (`query`)
- Set `.option("viewsEnabled", "true")`.
- Set `.option("materializationDataset", "<dataset>")` and optionally `.option("materializationProject", "<project>")` so BigQuery can materialize temporary query results.
- Pass the SQL statement with `.option("query", "<sql_query>")` and call `.load()`.

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads BigQuery source nodes using the BigQuery Storage Read API."""
    dfs: dict[str, DataFrame] = {}

    reader = (
        spark.read.format("bigquery")
        .option("parentProject", config.get("gcp_project", "my-gcp-project"))
    )
    if config.get("bq_filter"):
        reader = reader.option("filter", config["bq_filter"])

    if config.get("bq_query"):
        dfs["bq_source"] = (
            reader.option("viewsEnabled", "true")
            .option("materializationDataset", config["bq_materialization_dataset"])
            .option("query", config["bq_query"])
            .load()
        )
    else:
        dfs["bq_source"] = reader.option("table", config["bq_table"]).load()

    return dfs
```

### Column Pruning and Pushdown Rules
- Select only required columns or let Catalyst push column projection into the BigQuery Storage Read stream.
- Keep `read_sources(spark, config)` free of business logic. Place projections, joins, and aggregations inside `transform(dfs, config)`.

## 3. Batch Write Patterns (`BigQuerySink` in `write_sinks`)

`BigQuerySink` writes batch DataFrames with `df.write.format("bigquery")`.

### Write Method (`writeMethod`)
- **`direct` (Recommended Default)**: Uses the BigQuery Storage Write API with `.option("writeMethod", "direct")`. Writes stream directly to BigQuery without staging files in Cloud Storage.
- **`indirect`**: Stages intermediate Parquet or Avro files in Cloud Storage before triggering a BigQuery Load Job. When `writeMethod` is `"indirect"`, set `.option("writeMethod", "indirect")` and `.option("temporaryGcsBucket", bucket_name)` with any leading `gs://` prefix removed.

### Save Modes and Table Creation
- Apply `.mode(save_mode)` using `"append"`, `"overwrite"`, `"errorifexists"`, or `"ignore"`.
- Set `.option("createDisposition", create_disposition)` using `"CREATE_IF_NEEDED"` or `"CREATE_NEVER"`.

### Partitioning, Clustering, and Schema Evolution
- When `partitionField` is configured, set `.option("partitionField", config["partition_field"])` and `.option("partitionType", config.get("partition_type", "DAY"))` (`"HOUR"`, `"DAY"`, `"MONTH"`, or `"YEAR"`).
- When `clusteredFields` is configured, pass up to four comma-separated column names via `.option("clusteredFields", config["clustered_fields"])`.
- When `allowFieldAddition` is enabled, set `.option("allowFieldAddition", "true")`.
- When `allowFieldRelaxation` is enabled, set `.option("allowFieldRelaxation", "true")`.

```python
import logging
from typing import Any
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_bigquery_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a batch DataFrame to Google BigQuery."""
    write_method = config.get("bq_write_method", "direct")
    save_mode = config.get("bq_save_mode", "append")
    create_disposition = config.get("bq_create_disposition", "CREATE_IF_NEEDED")

    writer = (
        df.write.format("bigquery")
        .mode(save_mode)
        .option("table", config["bq_sink_table"])
        .option("writeMethod", write_method)
        .option("createDisposition", create_disposition)
    )

    if config.get("gcp_project"):
        writer = writer.option("parentProject", config["gcp_project"])

    if write_method == "indirect":
        raw_bucket = config["bq_temporary_gcs_bucket"]
        clean_bucket = raw_bucket.removeprefix("gs://").strip("/")
        writer = writer.option("temporaryGcsBucket", clean_bucket)

    if config.get("bq_partition_field"):
        writer = writer.option("partitionField", config["bq_partition_field"]).option(
            "partitionType", config.get("bq_partition_type", "DAY")
        )

    if config.get("bq_clustered_fields"):
        writer = writer.option("clusteredFields", config["bq_clustered_fields"])

    if config.get("bq_allow_field_addition"):
        writer = writer.option("allowFieldAddition", "true")

    if config.get("bq_allow_field_relaxation"):
        writer = writer.option("allowFieldRelaxation", "true")

    logger.info("Writing batch DataFrame to BigQuery table %s", config["bq_sink_table"])
    writer.save()
```

## 4. Structured Streaming Write Patterns (`BigQuerySink`)

`BigQuerySink` supports native Structured Streaming (`streamingSinkMode == "native"`):

- **Single Native Streaming Sink**: Use `df.writeStream.format("bigquery")` with `.option("table", ...)`, `.option("checkpointLocation", checkpoint_location)`, and `.option("writeMethod", write_method)`.
  - When `writeMethod` is `"direct"`, BigQuery commits micro-batches through the Storage Write API.
  - When `writeMethod` is `"indirect"`, also pass `.option("temporaryGcsBucket", clean_bucket)`.
- **Multi-Sink Fan-Out (`foreachBatch`)**: When `ExecutionPlan.streamingSinkGroups` groups `BigQuerySink` with other sinks in `mode == "foreachBatch"`, call the batch `df.write.format("bigquery")` pattern inside `write_micro_batch(batch_df, batch_id)` and unpersist `batch_df` in a `finally` block.
- Always configure the streaming trigger (`processingTime` or `availableNow=True`) and call `spark.streams.awaitAnyTermination()` at the end of `write_sinks()`.

```python
from typing import Any
from pyspark.sql import DataFrame


def start_bigquery_stream(df: DataFrame, config: dict[str, Any]) -> None:
    """Starts a native Structured Streaming write to Google BigQuery."""
    checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
    checkpoint_location = config.get(
        "bq_checkpoint_location", f"{checkpoint_root.rstrip('/')}/bq_sink"
    )
    write_method = config.get("bq_write_method", "direct")

    stream_writer = (
        df.writeStream.format("bigquery")
        .outputMode(config.get("bq_output_mode", "append"))
        .option("table", config["bq_sink_table"])
        .option("checkpointLocation", checkpoint_location)
        .option("writeMethod", write_method)
        .option("createDisposition", config.get("bq_create_disposition", "CREATE_IF_NEEDED"))
    )

    if write_method == "indirect":
        clean_bucket = config["bq_temporary_gcs_bucket"].removeprefix("gs://").strip("/")
        stream_writer = stream_writer.option("temporaryGcsBucket", clean_bucket)

    if config.get("bq_allow_field_addition"):
        stream_writer = stream_writer.option("allowFieldAddition", "true")

    if config.get("bq_trigger_type") == "availableNow":
        query = stream_writer.trigger(availableNow=True).start()
    else:
        interval = config.get("bq_trigger_interval", "10 seconds")
        query = stream_writer.trigger(processingTime=interval).start()

    df.sparkSession.streams.awaitAnyTermination()
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Never connect to Google BigQuery inside unit tests. Construct an in-memory `DataFrame` with an explicit `StructType` schema matching the BigQuery table columns and pass it directly into `transform(dfs, config)`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
from src.pipeline import transform


def test_bigquery_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without calling BigQuery APIs."""
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("event_ts", TimestampType(), True),
        ]
    )
    sample_df = spark.createDataFrame(
        [("ord-1", "cust-10", 125.50, None), ("ord-2", "cust-20", 42.00, None)],
        schema=schema,
    )
    result_dfs = transform({"bq_source": sample_df}, {"min_amount": 50.0})
    output_rows = result_dfs["bq_source"].collect()
    assert len(output_rows) == 2
```
