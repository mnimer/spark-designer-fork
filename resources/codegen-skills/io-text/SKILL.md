---
name: io-text
description: Batch and Structured Streaming plain-text file source and sink supporting line-by-line reads, single-column string projection, compression, and multi-cloud filesystems.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [TextSource, TextSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-text.html
  validated: true
---

# Text File Source & Sink (`TextSource`, `TextSink`)

Use these rules when generating PySpark 4.x code for `TextSource` (`SRC18`) or `TextSink` (`SNK16`) nodes.

## 1. Runtime & Connector Classpath Rules

- **Built-in Format**: Plain-text file reading and writing are built into Spark 4.x. Never add an external text package to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.
- **Local Runner Storage Connectors**: Attach `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25`, `org.apache.hadoop:hadoop-aws:3.4.1`, or `org.apache.hadoop:hadoop-azure:3.4.1` via `--packages` only in `run_local.sh` when reading or writing cloud URIs locally.
- **Spark Connect Safe Configuration**: Configure cloud storage credentials exclusively through `spark.conf.set("spark.hadoop.fs.*", ...)` so code runs cleanly on standard Spark and Spark Connect.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor `path` (or legacy `uri` / `filePattern` / `to`) and `storageBackend` across `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://`:
- When reading or writing `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.conf.set("spark.hadoop.fs.s3a.*", ...)` or `spark.conf.set("spark.hadoop.fs.azure.account.key.*", ...)`.

## 3. `TextSource` — Batch vs. Streaming Read Patterns (`read_sources`)

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read using `spark.read.options(**options).text(uri)`.
- **Streaming Mode (`readMode == "streaming"`)**:
  - Read using `spark.readStream.options(**options).text(uri)` (the text source has a fixed built-in schema of `StructType([StructField("value", StringType(), True)])`).
  - If `maxFilesPerTrigger` is configured, pass it to bound micro-batch ingestion.

### Supported Reader Options (`TextSource`)

Map every configured `TextSource` node property:
- `wholetext`: `"true"` or `"false"` (when `"true"`, reads each entire file into a single row's `value` column instead of splitting by line).
- `lineSep`: Custom line separator string when `wholetext` is `"false"` (defaults to `\r`, `\r\n`, and `\n`).
- `pathGlobFilter`: Glob pattern such as `"*.log"` or `"*.txt"`.
- `recursiveFileLookup`: `"true"` or `"false"` to scan subdirectories recursively.
- `maxFilesPerTrigger`: Maximum number of new text files processed per streaming trigger (streaming mode only).

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads batch or streaming text files into DataFrames keyed by node ID."""
    dfs: dict[str, DataFrame] = {}
    text_uri = config.get("src_text_path", "gs://my-bucket/logs/app/")
    read_mode = config.get("src_text_read_mode", "batch")

    options: dict[str, str] = {
        "wholetext": str(config.get("src_text_wholetext", False)).lower(),
    }
    if config.get("src_text_line_sep"):
        options["lineSep"] = str(config["src_text_line_sep"])
    if config.get("src_text_path_glob_filter"):
        options["pathGlobFilter"] = str(config["src_text_path_glob_filter"])
    if config.get("src_text_recursive_file_lookup") is not None:
        options["recursiveFileLookup"] = str(config["src_text_recursive_file_lookup"]).lower()

    if read_mode == "streaming":
        if config.get("src_text_max_files_per_trigger"):
            options["maxFilesPerTrigger"] = str(config["src_text_max_files_per_trigger"])
        dfs["src_text"] = spark.readStream.options(**options).text(text_uri)
    else:
        dfs["src_text"] = spark.read.options(**options).text(text_uri)
    return dfs
```

## 4. `TextSink` — Batch & Structured Streaming Write Patterns (`write_sinks`)

`TextSink` (`SNK16`) has `streamingSinkMode: "native"`.

- **Mandatory Single `StringType` Column Rule**:
  - Spark's `text` writer requires the DataFrame (excluding `partitionBy` columns) to contain **exactly one column of type `StringType`**. Passing multiple non-partition columns or a non-string column raises `AnalysisException: Text data source supports only a single column, and you have N columns`.
  - Before calling `.write.text(...)` or `.writeStream.format("text")`:
    - **Single column (`column` strategy)**: Project `df.select(*partition_cols, F.col(value_col).cast("string").alias("value"))`.
    - **Concatenated fields (`concat_ws` strategy)**: Project `df.select(*partition_cols, F.concat_ws(sep, *[F.col(c).cast("string") for c in data_cols]).alias("value"))`.
    - **JSON string (`to_json` strategy)**: Project `df.select(*partition_cols, F.to_json(F.struct(*data_cols)).alias("value"))`.
- **Batch Mode**:
  - Write with `text_df.write.mode(save_mode).options(**write_options).text(output_uri)`.
  - Supported `saveMode` values: `"append"`, `"overwrite"`, `"errorifexists"`, and `"ignore"`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` before `.text(output_uri)`.
- **Native Structured Streaming Mode (`mode == "native"`)**:
  - Write with `text_df.writeStream.format("text").outputMode(output_mode).option("checkpointLocation", checkpoint_location).options(**write_options)` and start with `.start(output_uri)`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` on the `DataStreamWriter`.
- **Multi-Sink `foreachBatch` Fan-Out**:
  - When `ExecutionPlan.streamingSinkGroups` groups `TextSink` into a multi-sink `foreachBatch` callback, project the single `value` string column and call `batch_text_df.write.mode("append").options(**write_options).text(output_uri)`.

### Supported Writer Options (`TextSink`)

- `lineSep`: Line separator appended after each row (defaults to `"\n"`).
- `compression`: `"none"` (default), `"gzip"`, `"zstd"`, `"bzip2"`, `"lz4"`, `"snappy"`, or `"deflate"`.
- `partitionBy`: Comma-separated list or array of column names for directory partitioning.
- `maxRecordsPerFile`: Positive integer capping lines written per output text part-file.

```python
from pyspark.sql import functions as F


def prepare_text_sink_df(
    df: DataFrame,
    partition_cols: list[str],
    strategy: str = "column",
    value_col: str = "value",
) -> DataFrame:
    """Projects a DataFrame into partition columns plus a single StringType value column."""
    data_cols = [c for c in df.columns if c not in partition_cols]
    if strategy == "to_json":
        payload_expr = F.to_json(F.struct(*[F.col(c) for c in data_cols])).alias("value")
    elif strategy == "concat_ws" or (strategy == "column" and value_col not in df.columns and len(data_cols) > 1):
        payload_expr = F.concat_ws(",", *[F.col(c).cast("string") for c in data_cols]).alias("value")
    else:
        target_col = value_col if value_col in df.columns else data_cols[0]
        payload_expr = F.col(target_col).cast("string").alias("value")
    return df.select(*[F.col(c) for c in partition_cols], payload_expr)


def write_text_sink(
    df: DataFrame,
    config: dict[str, Any],
    is_streaming: bool = False,
) -> None:
    """Writes a batch or streaming DataFrame to plain-text files."""
    output_uri = config.get("snk_text_path", "gs://my-bucket/logs/exported_text/")
    save_mode = config.get("snk_text_save_mode", "append")
    compression = config.get("snk_text_compression", "none")
    raw_partitions = config.get("snk_text_partition_by", "")
    partition_cols = [c.strip() for c in raw_partitions.split(",") if c.strip()]
    strategy = config.get("snk_text_serialization_strategy", "column")
    value_col = config.get("snk_text_value_column", "value")

    text_df = prepare_text_sink_df(df, partition_cols, strategy=strategy, value_col=value_col)

    write_options: dict[str, str] = {
        "compression": compression,
    }
    if config.get("snk_text_line_sep"):
        write_options["lineSep"] = str(config["snk_text_line_sep"])
    if config.get("snk_text_max_records_per_file"):
        write_options["maxRecordsPerFile"] = str(config["snk_text_max_records_per_file"])

    if is_streaming:
        checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
        checkpoint_uri = config.get(
            "snk_text_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/snk_text",
        )
        stream_writer = (
            text_df.writeStream.format("text")
            .outputMode(config.get("snk_text_output_mode", "append"))
            .option("checkpointLocation", checkpoint_uri)
            .options(**write_options)
        )
        if partition_cols:
            stream_writer = stream_writer.partitionBy(*partition_cols)
        trigger_type = config.get("snk_text_trigger_type", "processingTime")
        if trigger_type == "availableNow":
            stream_writer = stream_writer.trigger(availableNow=True)
        else:
            interval = config.get("snk_text_trigger_interval", "10 seconds")
            stream_writer = stream_writer.trigger(processingTime=interval)
        stream_writer.start(output_uri)
    else:
        batch_writer = text_df.write.mode(save_mode).options(**write_options)
        if partition_cols:
            batch_writer = batch_writer.partitionBy(*partition_cols)
        batch_writer.text(output_uri)
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by creating an in-memory DataFrame with `spark.createDataFrame(rows, schema=TEXT_SCHEMA)`. When verifying multi-column projection to `TextSink`, write to pytest's `tmp_path` directory and read back with `spark.read.text(out_dir)`.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from src.pipeline import transform

TEXT_SCHEMA = StructType([StructField("value", StringType(), True)])


def test_text_pipeline_transform_and_local_write(spark, tmp_path):
    """Verifies text pipeline transformations and single-column text file write."""
    rows = [
        ("2026-01-15 INFO Job started",),
        ("2026-01-15 ERROR Connection timeout",),
    ]
    input_df = spark.createDataFrame(rows, schema=TEXT_SCHEMA)
    result_dfs = transform({"src_text": input_df}, {})

    out_dir = str(tmp_path / "text_out")
    result_dfs["src_text"].select(F.col("value").cast("string")).write.mode("overwrite").text(out_dir)
    assert spark.read.text(out_dir).count() == 2
```
