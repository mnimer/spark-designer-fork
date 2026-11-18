---
name: io-csv
description: Batch and Structured Streaming CSV file source and sink with dialect options, corrupt record handling, compression, and multi-cloud storage support.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [CsvSource, CsvSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-csv.html
  validated: true
---

# CSV File Source & Sink (`CsvSource`, `CsvSink`)

Use these rules when generating PySpark 4.x code for `CsvSource` (`SRC16`) or `CsvSink` (`SNK14`) nodes.

## 1. Runtime & Connector Classpath Rules

- **Built-in Format**: CSV support is built into Spark 4.x. Never add an external CSV package to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.
- **Local Runner Storage Connectors**: Attach `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25`, `org.apache.hadoop:hadoop-aws:3.4.1`, or `org.apache.hadoop:hadoop-azure:3.4.1` via `--packages` only in `run_local.sh` when reading or writing cloud URIs locally.
- **Spark Connect Safe Configuration**: Configure cloud storage credentials exclusively through `spark.conf.set("spark.hadoop.fs.*", ...)` so code runs cleanly on standard Spark and Spark Connect.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor `path` (or legacy `uri` / `filePattern` / `to`) and `storageBackend` across `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://`:
- When reading or writing `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.conf.set("spark.hadoop.fs.s3a.*", ...)` or `spark.conf.set("spark.hadoop.fs.azure.account.key.*", ...)`.

## 3. `CsvSource` — Batch vs. Streaming Read Patterns & Corrupt Record Handling

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read with `spark.read.options(**options).csv(uri)`.
  - If an explicit schema is configured or corrupt record handling is enabled (`mode="PERMISSIVE"` with `columnNameOfCorruptRecord="_corrupt_record"`), use `.schema(CSV_SCHEMA)`. Otherwise pass `header=True` and `inferSchema=True` as configured.
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit `StructType` Schema**: Structured Streaming CSV sources require an explicit `StructType` schema (`inferSchema=True` is not supported on streaming CSV reads without explicit schema inference configuration). Always pass `spark.readStream.schema(CSV_SCHEMA).options(**options).csv(uri)`.
  - If `maxFilesPerTrigger` is configured, pass it to control micro-batch file count.
- **Corrupt Record & Dead-Letter Queue (DLQ) Handling**:
  - When `mode="PERMISSIVE"` (or `parseMode="PERMISSIVE"`) and corrupt record capture or a DLQ `error` branch is used, include `StructField("_corrupt_record", StringType(), True)` as the final field in the explicit `StructType` schema and set `.option("mode", "PERMISSIVE")` and `.option("columnNameOfCorruptRecord", "_corrupt_record")`.
  - When routing to a DLQ sink, split valid rows (`F.col("_corrupt_record").isNull()`) and malformed rows (`F.col("_corrupt_record").isNotNull()`).

### Supported Reader Options (`CsvSource`)

Map every configured `CsvSource` node property:
- `header`: `"true"` or `"false"` (default `"true"`).
- `inferSchema`: `"true"` or `"false"` (default `"true"` in batch mode when no explicit schema is provided).
- `sep` (or `delimiter`): Field delimiter character (default `","`).
- `quote`: Quote character for escaping delimiters inside fields (default `'"'`).
- `escape`: Escape character inside quoted values (default `"\\"`).
- `nullValue`: String representation interpreted as SQL `NULL` (for example `""` or `"NULL"`).
- `multiLine`: `"true"` or `"false"` to allow newlines within quoted CSV fields.
- `mode` (or `parseMode`): `"PERMISSIVE"` (default), `"DROPMALFORMED"`, or `"FAILFAST"`.
- `columnNameOfCorruptRecord`: Column storing unparseable raw lines in `PERMISSIVE` mode (default `"_corrupt_record"`).
- `pathGlobFilter`, `recursiveFileLookup`, and `maxFilesPerTrigger` (streaming mode).

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

CSV_TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("txn_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads batch or streaming CSV sources into DataFrames keyed by node ID."""
    dfs: dict[str, DataFrame] = {}
    csv_uri = config.get("src_csv_path", "gs://my-bucket/incoming/transactions.csv")
    read_mode = config.get("src_csv_read_mode", "batch")
    parse_mode = config.get("src_csv_mode", "PERMISSIVE")
    corrupt_col = config.get("src_csv_corrupt_record_col", "_corrupt_record")

    options: dict[str, str] = {
        "header": str(config.get("src_csv_header", True)).lower(),
        "sep": str(config.get("src_csv_delimiter", ",")),
        "quote": str(config.get("src_csv_quote", '"')),
        "escape": str(config.get("src_csv_escape", "\\")),
        "nullValue": str(config.get("src_csv_null_value", "")),
        "multiLine": str(config.get("src_csv_multiline", False)).lower(),
        "mode": parse_mode,
        "columnNameOfCorruptRecord": corrupt_col,
    }
    if config.get("src_csv_path_glob_filter"):
        options["pathGlobFilter"] = str(config["src_csv_path_glob_filter"])
    if config.get("src_csv_recursive_file_lookup") is not None:
        options["recursiveFileLookup"] = str(config["src_csv_recursive_file_lookup"]).lower()

    if read_mode == "streaming":
        if config.get("src_csv_max_files_per_trigger"):
            options["maxFilesPerTrigger"] = str(config["src_csv_max_files_per_trigger"])
        dfs["src_csv"] = (
            spark.readStream.schema(CSV_TRANSACTIONS_SCHEMA)
            .options(**options)
            .csv(csv_uri)
        )
    elif config.get("src_csv_use_explicit_schema", True):
        dfs["src_csv"] = (
            spark.read.schema(CSV_TRANSACTIONS_SCHEMA)
            .options(**options)
            .csv(csv_uri)
        )
    else:
        options["inferSchema"] = str(config.get("src_csv_infer_schema", True)).lower()
        dfs["src_csv"] = spark.read.options(**options).csv(csv_uri)
    return dfs
```

## 4. `CsvSink` — Batch & Structured Streaming Write Patterns (`write_sinks`)

`CsvSink` (`SNK14`) has `streamingSinkMode: "native"`.

- **Batch Mode**:
  - Write with `df.write.mode(save_mode).options(**write_options).csv(output_uri)`.
  - Supported `saveMode` values: `"append"`, `"overwrite"`, `"errorifexists"`, and `"ignore"`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` before `.csv(output_uri)`.
- **Native Structured Streaming Mode (`mode == "native"`)**:
  - Write with `df.writeStream.format("csv").outputMode(output_mode).option("checkpointLocation", checkpoint_location).options(**write_options)` and start with `.start(output_uri)`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` on the `DataStreamWriter`.
  - Apply `.trigger(availableNow=True)` or `.trigger(processingTime=trigger_interval)` per `ExecutionPlan.streamingSinkGroups`.
- **Multi-Sink `foreachBatch` Fan-Out**:
  - When `ExecutionPlan.streamingSinkGroups` groups `CsvSink` into a multi-sink `foreachBatch` callback, execute `batch_df.write.mode("append").options(**write_options).csv(output_uri)`.
- **Complex Column Serialization**:
  - Spark's CSV writer supports atomic scalar types only. If an upstream DataFrame contains `StructType`, `ArrayType`, or `MapType` columns, serialize them with `F.to_json(F.col(c))` before writing to `CsvSink`.

### Supported Writer Options (`CsvSink`)

- `header`: `"true"` or `"false"` (writes column header row in each part-file when `"true"`).
- `sep` (or `delimiter`): Field separator string (default `","`).
- `quote`: Quote character enclosing values containing `sep` or quotes (default `'"'`).
- `escape`: Escape character for quotes inside quoted strings (default `"\\"`).
- `nullValue`: String representation written for SQL `NULL` values (default `""`).
- `emptyValue`: String representation written for empty strings `""`.
- `quoteAll`: `"true"` or `"false"` (encloses every field in quotes when `"true"`).
- `compression`: `"none"` (default), `"gzip"`, `"zstd"`, `"bzip2"`, `"lz4"`, `"snappy"`, or `"deflate"`.
- `partitionBy`: Comma-separated list or array of column names for directory partitioning.
- `maxRecordsPerFile`: Positive integer capping rows written per output CSV part-file.

```python
def write_csv_sink(
    df: DataFrame,
    config: dict[str, Any],
    is_streaming: bool = False,
) -> None:
    """Writes a batch or streaming DataFrame to delimited CSV files."""
    output_uri = config.get("snk_csv_path", "gs://my-bucket/exports/transactions_csv/")
    save_mode = config.get("snk_csv_save_mode", "append")
    compression = config.get("snk_csv_compression", "none")
    raw_partitions = config.get("snk_csv_partition_by", "")
    partition_cols = [c.strip() for c in raw_partitions.split(",") if c.strip()]

    write_options: dict[str, str] = {
        "header": str(config.get("snk_csv_header", True)).lower(),
        "sep": str(config.get("snk_csv_sep", ",")),
        "quote": str(config.get("snk_csv_quote", '"')),
        "escape": str(config.get("snk_csv_escape", "\\")),
        "nullValue": str(config.get("snk_csv_null_value", "")),
        "quoteAll": str(config.get("snk_csv_quote_all", False)).lower(),
        "compression": compression,
    }
    if config.get("snk_csv_empty_value") is not None:
        write_options["emptyValue"] = str(config["snk_csv_empty_value"])
    if config.get("snk_csv_max_records_per_file"):
        write_options["maxRecordsPerFile"] = str(config["snk_csv_max_records_per_file"])

    if is_streaming:
        checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
        checkpoint_uri = config.get(
            "snk_csv_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/snk_csv",
        )
        stream_writer = (
            df.writeStream.format("csv")
            .outputMode(config.get("snk_csv_output_mode", "append"))
            .option("checkpointLocation", checkpoint_uri)
            .options(**write_options)
        )
        if partition_cols:
            stream_writer = stream_writer.partitionBy(*partition_cols)
        trigger_type = config.get("snk_csv_trigger_type", "processingTime")
        if trigger_type == "availableNow":
            stream_writer = stream_writer.trigger(availableNow=True)
        else:
            interval = config.get("snk_csv_trigger_interval", "10 seconds")
            stream_writer = stream_writer.trigger(processingTime=interval)
        stream_writer.start(output_uri)
    else:
        batch_writer = df.write.mode(save_mode).options(**write_options)
        if partition_cols:
            batch_writer = batch_writer.partitionBy(*partition_cols)
        batch_writer.csv(output_uri)
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame with `spark.createDataFrame(rows, schema=CSV_TRANSACTIONS_SCHEMA)`. When testing local CSV writes, write to pytest's `tmp_path` directory instead of cloud storage URIs.

```python
from src.pipeline import CSV_TRANSACTIONS_SCHEMA, transform


def test_csv_pipeline_transform_and_local_write(spark, tmp_path):
    """Verifies CSV pipeline transformation and local header-enabled CSV write."""
    rows = [
        ("tx_1", "acc_10", 50.25, None),
        ("tx_2", "acc_20", 19.99, None),
    ]
    input_df = spark.createDataFrame(rows, schema=CSV_TRANSACTIONS_SCHEMA)
    result_dfs = transform({"src_csv": input_df}, {})

    out_dir = str(tmp_path / "csv_out")
    result_dfs["src_csv"].drop("_corrupt_record").write.mode("overwrite").option("header", "true").csv(out_dir)
    assert spark.read.option("header", "true").csv(out_dir).count() == 2
```
