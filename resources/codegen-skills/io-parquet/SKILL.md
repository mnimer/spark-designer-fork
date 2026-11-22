---
name: io-parquet
description: Batch and Structured Streaming Apache Parquet file source and sink across GCS, S3A, ADLS Gen2, Azure Blob, HDFS, and local filesystems.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [ParquetSource, ParquetSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
  validated: true
---

# Apache Parquet Source & Sink (`ParquetSource`, `ParquetSink`)

Use these rules when generating PySpark 4.x code for `ParquetSource` (`SRC13`) or `ParquetSink` (`SNK11`) nodes.

## 1. Runtime & Connector Classpath Rules

- **Built-in Format**: Apache Parquet support is built into Spark 4.x. Never add a Parquet package to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.
- **Local Runner Storage Connectors**: Attach `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25`, `org.apache.hadoop:hadoop-aws:3.4.1`, or `org.apache.hadoop:hadoop-azure:3.4.1` via `--packages` only in `run_local.sh` when reading or writing cloud object storage locally.
- **Spark Connect Safe Configuration**: Always set cloud storage credentials through `spark.conf.set("spark.hadoop.fs.*", ...)` so pipelines run cleanly on both standard sessions and Spark Connect.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor the configured `path` (or legacy `uri` / `filePattern` / `to`) and `storageBackend` across all supported URI schemes:
- **Google Cloud Storage (`gs://bucket/path/`)**: Authenticate using the runtime service account or Application Default Credentials.
- **Amazon S3 (`s3a://bucket/path/`)**: Resolve AWS credentials once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key` (plus optional `spark.hadoop.fs.s3a.session.token` or `spark.hadoop.fs.s3a.endpoint`).
- **Azure ADLS Gen2 (`abfss://container@account.dfs.core.windows.net/path/`)**: Resolve the storage account key or OAuth client secret via `get_secret()` and set `spark.hadoop.fs.azure.account.key.<account>.dfs.core.windows.net`.
- **Azure Blob Storage (`wasbs://container@account.blob.core.windows.net/path/`)**: Resolve the storage account key via `get_secret()` and set `spark.hadoop.fs.azure.account.key.<account>.blob.core.windows.net`.
- **HDFS (`hdfs://namenode:port/path/`)** and **Local Filesystem (`file:///path/` or `/path/`)**: Read and write directly without cloud secret resolution.

```python
def configure_storage_auth(spark: SparkSession, config: dict[str, Any]) -> None:
    """Configures Hadoop cloud storage credentials from Secret Manager when secret IDs are provided."""
    if config.get("aws_access_key_secret") and config.get("aws_secret_key_secret"):
        access_key = get_secret(config["aws_access_key_secret"])
        secret_key = get_secret(config["aws_secret_key_secret"])
        spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)
    if config.get("azure_storage_account") and config.get("azure_account_key_secret"):
        account = config["azure_storage_account"]
        account_key = get_secret(config["azure_account_key_secret"])
        spark.conf.set(f"spark.hadoop.fs.azure.account.key.{account}.dfs.core.windows.net", account_key)
        spark.conf.set(f"spark.hadoop.fs.azure.account.key.{account}.blob.core.windows.net", account_key)
```

## 3. `ParquetSource` — Batch vs. Streaming Read Patterns (`read_sources`)

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read with `spark.read.options(**options).parquet(uri)`.
  - If an explicit schema is configured and `mergeSchema` is not enabled, apply `.schema(schema)`.
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit `StructType` Schema**: Structured Streaming file sources require an explicit `StructType` schema (unless `spark.sql.streaming.schemaInference` is explicitly set). Always pass `.schema(PARQUET_SCHEMA)` before `.parquet(uri)`: `spark.readStream.schema(PARQUET_SCHEMA).options(**options).parquet(uri)`.
  - If `maxFilesPerTrigger` is configured, pass it to bound micro-batch file ingestion size.

### Supported Reader Options (`ParquetSource`)

Map every configured node property to its corresponding Spark Parquet option:
- `mergeSchema`: `"true"` or `"false"` (enables schema evolution across Parquet part-files).
- `datetimeRebaseModeInRead` (or `datetimeRebaseMode`): `"EXCEPTION"`, `"CORRECTED"`, or `"LEGACY"` for legacy proleptic Gregorian calendar dates.
- `int96RebaseModeInRead` (or `int96RebaseMode`): `"EXCEPTION"`, `"CORRECTED"`, or `"LEGACY"` for legacy INT96 timestamps.
- `pathGlobFilter`: Glob pattern such as `"*.parquet"` to filter matching files.
- `recursiveFileLookup`: `"true"` or `"false"` to scan nested subdirectories (disables partition column discovery when `"true"`).
- `maxFilesPerTrigger`: Maximum number of new files read per streaming micro-batch (streaming mode only).

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("event_ts", TimestampType(), True),
    ]
)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads batch or streaming Parquet sources into DataFrames keyed by node ID."""
    dfs: dict[str, DataFrame] = {}
    parquet_uri = config.get("src_parquet_path", "gs://my-bucket/data/orders/")
    read_mode = config.get("src_parquet_read_mode", "batch")

    options: dict[str, str] = {
        "mergeSchema": str(config.get("src_parquet_merge_schema", False)).lower(),
        "datetimeRebaseModeInRead": config.get("src_parquet_datetime_rebase_mode", "CORRECTED"),
        "int96RebaseModeInRead": config.get("src_parquet_int96_rebase_mode", "CORRECTED"),
    }
    if config.get("src_parquet_path_glob_filter"):
        options["pathGlobFilter"] = str(config["src_parquet_path_glob_filter"])
    if config.get("src_parquet_recursive_file_lookup") is not None:
        options["recursiveFileLookup"] = str(config["src_parquet_recursive_file_lookup"]).lower()

    if read_mode == "streaming":
        if config.get("src_parquet_max_files_per_trigger"):
            options["maxFilesPerTrigger"] = str(config["src_parquet_max_files_per_trigger"])
        dfs["src_parquet"] = (
            spark.readStream.schema(ORDERS_SCHEMA)
            .options(**options)
            .parquet(parquet_uri)
        )
    else:
        dfs["src_parquet"] = (
            spark.read.schema(ORDERS_SCHEMA)
            .options(**options)
            .parquet(parquet_uri)
        )
    return dfs
```

## 4. `ParquetSink` — Batch & Structured Streaming Write Patterns (`write_sinks`)

`ParquetSink` (`SNK11`) has `streamingSinkMode: "native"`.

- **Batch Mode**:
  - Write using `df.write.mode(save_mode).options(**write_options).parquet(output_uri)`.
  - Supported `saveMode` values: `"append"`, `"overwrite"`, `"errorifexists"`, and `"ignore"`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` before `.parquet(output_uri)`.
- **Native Structured Streaming Mode (`mode == "native"`)**:
  - Write using `df.writeStream.format("parquet").outputMode(output_mode).option("checkpointLocation", checkpoint_location).options(**write_options)` and start with `.start(output_uri)`.
  - File sinks in Structured Streaming support `outputMode("append")`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` on the `DataStreamWriter`.
  - Apply `.trigger(availableNow=True)` or `.trigger(processingTime=trigger_interval)` per `ExecutionPlan.streamingSinkGroups`.
- **Multi-Sink `foreachBatch` Fan-Out**:
  - When `ExecutionPlan.streamingSinkGroups` groups `ParquetSink` into a `foreachBatch` function alongside other sinks, write `batch_df.write.mode("append").options(**write_options).parquet(output_uri)` inside `write_micro_batch(batch_df, batch_id)`.

### Supported Writer Options (`ParquetSink`)

- `compression`: `"snappy"` (default), `"zstd"`, `"gzip"`, or `"none"`.
- `partitionBy`: Comma-separated list or array of column names for Hive-style directory partitioning.
- `maxRecordsPerFile`: Positive integer limiting maximum rows written to each output Parquet part-file.
- `datetimeRebaseModeInWrite` (or `datetimeRebaseMode`): `"EXCEPTION"`, `"CORRECTED"`, or `"LEGACY"`.
- `int96RebaseModeInWrite` (or `int96RebaseMode`): `"EXCEPTION"`, `"CORRECTED"`, or `"LEGACY"`.

```python
def write_parquet_sink(
    df: DataFrame,
    config: dict[str, Any],
    is_streaming: bool = False,
) -> None:
    """Writes a batch or streaming DataFrame to Apache Parquet files."""
    output_uri = config.get("snk_parquet_path", "gs://my-bucket/output/orders_parquet/")
    save_mode = config.get("snk_parquet_save_mode", "append")
    compression = config.get("snk_parquet_compression", "snappy")
    raw_partitions = config.get("snk_parquet_partition_by", "")
    partition_cols = [c.strip() for c in raw_partitions.split(",") if c.strip()]

    write_options: dict[str, str] = {
        "compression": compression,
    }
    if config.get("snk_parquet_max_records_per_file"):
        write_options["maxRecordsPerFile"] = str(config["snk_parquet_max_records_per_file"])
    if config.get("snk_parquet_datetime_rebase_mode"):
        write_options["datetimeRebaseModeInWrite"] = str(config["snk_parquet_datetime_rebase_mode"])
    if config.get("snk_parquet_int96_rebase_mode"):
        write_options["int96RebaseModeInWrite"] = str(config["snk_parquet_int96_rebase_mode"])

    if is_streaming:
        checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
        checkpoint_uri = config.get(
            "snk_parquet_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/snk_parquet",
        )
        stream_writer = (
            df.writeStream.format("parquet")
            .outputMode(config.get("snk_parquet_output_mode", "append"))
            .option("checkpointLocation", checkpoint_uri)
            .options(**write_options)
        )
        if partition_cols:
            stream_writer = stream_writer.partitionBy(*partition_cols)
        trigger_type = config.get("snk_parquet_trigger_type", "processingTime")
        if trigger_type == "availableNow":
            stream_writer = stream_writer.trigger(availableNow=True)
        else:
            interval = config.get("snk_parquet_trigger_interval", "10 seconds")
            stream_writer = stream_writer.trigger(processingTime=interval)
        stream_writer.start(output_uri)
    else:
        batch_writer = df.write.mode(save_mode).options(**write_options)
        if partition_cols:
            batch_writer = batch_writer.partitionBy(*partition_cols)
        batch_writer.parquet(output_uri)
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame with `spark.createDataFrame(rows, schema=ORDERS_SCHEMA)`. When testing Parquet file writes locally, write to pytest's `tmp_path` fixture rather than `gs://`, `s3a://`, or `abfss://`.

```python
from datetime import datetime, timezone
from src.pipeline import ORDERS_SCHEMA, transform


def test_parquet_pipeline_transform_and_local_write(spark, tmp_path):
    """Verifies transformations and local Parquet write using tmp_path."""
    sample_rows = [
        ("ord_1", "cust_10", 125.50, datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)),
        ("ord_2", "cust_20", 42.00, datetime(2026, 1, 15, 12, 5, tzinfo=timezone.utc)),
    ]
    input_df = spark.createDataFrame(sample_rows, schema=ORDERS_SCHEMA)
    result_dfs = transform({"src_parquet": input_df}, {})

    out_dir = str(tmp_path / "parquet_out")
    result_dfs["src_parquet"].write.mode("overwrite").option("compression", "snappy").parquet(out_dir)
    reloaded_df = spark.read.parquet(out_dir)
    assert reloaded_df.count() == 2
```
