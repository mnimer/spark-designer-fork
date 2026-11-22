---
name: io-avro
description: Batch and Structured Streaming Apache Avro file source and sink using spark-avro across GCS, S3A, ADLS Gen2, Azure Blob, HDFS, and local filesystems.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [AvroSource, AvroSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-avro.html
  validated: true
---

# Apache Avro Source & Sink (`AvroSource`, `AvroSink`)

Use these rules when generating PySpark 4.x code for `AvroSource` (`SRC15`) or `AvroSink` (`SNK13`) nodes.

## 1. Runtime & Connector Classpath Rules

- **Required External Maven Package (`spark-avro`)**: Apache Avro is an external module in Spark 4.x. Include `org.apache.spark:spark-avro_2.13:{{spark_version}}` in `spark.jars.packages` / `--packages` across Local, Dataproc Cluster, and Serverless Spark runner scripts.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless Spark and Dataproc Clusters. Never add storage connectors to `spark.jars.packages` on Managed Spark targets.
- **Local Runner Storage Connectors**: Attach `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25`, `org.apache.hadoop:hadoop-aws:3.4.1`, or `org.apache.hadoop:hadoop-azure:3.4.1` via `--packages` only in `run_local.sh` when accessing cloud URIs locally.
- **Spark Connect Safe Configuration**: Set Hadoop storage credentials via `spark.conf.set("spark.hadoop.fs.*", ...)` so code executes cleanly on both standard sessions and Spark Connect.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Support all `StorageBackendFields` URI schemes (`gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, `file://`):
- When reading or writing `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and set the corresponding `spark.hadoop.fs.s3a.*` or `spark.hadoop.fs.azure.account.key.*` properties.

## 3. `AvroSource` — Batch vs. Streaming Read Patterns (`read_sources`)

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read using `spark.read.format("avro").options(**options).load(uri)`.
  - If a Spark SQL `StructType` is configured, apply `.schema(AVRO_STRUCT_SCHEMA)`.
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit Schema**: Provide either `.schema(AVRO_STRUCT_SCHEMA)` or `.option("avroSchema", avro_json_schema)` so Structured Streaming knows the schema prior to starting the stream:
    `spark.readStream.format("avro").schema(AVRO_STRUCT_SCHEMA).options(**options).load(uri)`.
  - If `maxFilesPerTrigger` is configured, pass it to bound each micro-batch.

### Supported Reader Options (`AvroSource`)

Map every configured `AvroSource` node property:
- `avroSchema`: Optional evolved or explicit Avro JSON schema string passed via `.option("avroSchema", avro_schema_json)`.
- `ignoreExtension`: `"true"` or `"false"` (when `"true"`, reads all files in the directory even if they do not end with `.avro`).
- `pathGlobFilter`: Glob filter such as `"*.avro"` to match input files.
- `recursiveFileLookup`: `"true"` or `"false"` to recursively scan subdirectories.
- `maxFilesPerTrigger`: Maximum number of new Avro files read per trigger in streaming mode.

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

AVRO_RECORDS_SCHEMA = StructType(
    [
        StructField("record_id", StringType(), False),
        StructField("account_id", StringType(), True),
        StructField("created_epoch_ms", LongType(), True),
    ]
)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads batch or streaming Avro files into DataFrames keyed by node ID."""
    dfs: dict[str, DataFrame] = {}
    avro_uri = config.get("src_avro_path", "gs://my-bucket/landing/avro_records/")
    read_mode = config.get("src_avro_read_mode", "batch")

    options: dict[str, str] = {}
    if config.get("src_avro_schema_json"):
        options["avroSchema"] = str(config["src_avro_schema_json"])
    if config.get("src_avro_ignore_extension") is not None:
        options["ignoreExtension"] = str(config["src_avro_ignore_extension"]).lower()
    if config.get("src_avro_path_glob_filter"):
        options["pathGlobFilter"] = str(config["src_avro_path_glob_filter"])
    if config.get("src_avro_recursive_file_lookup") is not None:
        options["recursiveFileLookup"] = str(config["src_avro_recursive_file_lookup"]).lower()

    if read_mode == "streaming":
        if config.get("src_avro_max_files_per_trigger"):
            options["maxFilesPerTrigger"] = str(config["src_avro_max_files_per_trigger"])
        dfs["src_avro"] = (
            spark.readStream.format("avro")
            .schema(AVRO_RECORDS_SCHEMA)
            .options(**options)
            .load(avro_uri)
        )
    else:
        dfs["src_avro"] = (
            spark.read.format("avro")
            .schema(AVRO_RECORDS_SCHEMA)
            .options(**options)
            .load(avro_uri)
        )
    return dfs
```

## 4. `AvroSink` — Batch & Structured Streaming Write Patterns (`write_sinks`)

`AvroSink` (`SNK13`) has `streamingSinkMode: "native"` and requires `org.apache.spark:spark-avro_2.13:{{spark_version}}`.

- **Batch Mode**:
  - Write with `df.write.format("avro").mode(save_mode).options(**write_options).save(output_uri)`.
  - Supported `saveMode` values: `"append"`, `"overwrite"`, `"errorifexists"`, and `"ignore"`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` before `.save(output_uri)`.
- **Native Structured Streaming Mode (`mode == "native"`)**:
  - Write with `df.writeStream.format("avro").outputMode(output_mode).option("checkpointLocation", checkpoint_location).options(**write_options)` and start with `.start(output_uri)`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` on the `DataStreamWriter`.
  - Apply `.trigger(availableNow=True)` or `.trigger(processingTime=trigger_interval)` per `ExecutionPlan.streamingSinkGroups`.
- **Multi-Sink `foreachBatch` Fan-Out**:
  - When `ExecutionPlan.streamingSinkGroups` groups `AvroSink` into a multi-sink `foreachBatch` callback, call `batch_df.write.format("avro").mode("append").options(**write_options).save(output_uri)`.

### Supported Writer Options (`AvroSink`)

- `compression`: `"snappy"` (default), `"deflate"`, `"bzip2"`, `"xz"`, `"zstandard"`, or `"uncompressed"`.
- `avroSchema`: Optional explicit Avro JSON writer schema passed via `.option("avroSchema", avro_writer_schema_json)`.
- `recordName`: Top-level Avro record name in generated Avro schema (default `"topLevelRecord"`).
- `recordNamespace`: Avro record namespace string (for example `"com.example.events"`).
- `partitionBy`: Comma-separated list or array of column names for directory partitioning.
- `maxRecordsPerFile`: Positive integer capping rows written per output Avro file.

```python
def write_avro_sink(
    df: DataFrame,
    config: dict[str, Any],
    is_streaming: bool = False,
) -> None:
    """Writes a batch or streaming DataFrame to Apache Avro files."""
    output_uri = config.get("snk_avro_path", "gs://my-bucket/curated/avro_records/")
    save_mode = config.get("snk_avro_save_mode", "append")
    compression = config.get("snk_avro_compression", "snappy")
    raw_partitions = config.get("snk_avro_partition_by", "")
    partition_cols = [c.strip() for c in raw_partitions.split(",") if c.strip()]

    write_options: dict[str, str] = {
        "compression": compression,
    }
    if config.get("snk_avro_schema"):
        write_options["avroSchema"] = str(config["snk_avro_schema"])
    if config.get("snk_avro_record_name"):
        write_options["recordName"] = str(config["snk_avro_record_name"])
    if config.get("snk_avro_record_namespace"):
        write_options["recordNamespace"] = str(config["snk_avro_record_namespace"])
    if config.get("snk_avro_max_records_per_file"):
        write_options["maxRecordsPerFile"] = str(config["snk_avro_max_records_per_file"])

    if is_streaming:
        checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
        checkpoint_uri = config.get(
            "snk_avro_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/snk_avro",
        )
        stream_writer = (
            df.writeStream.format("avro")
            .outputMode(config.get("snk_avro_output_mode", "append"))
            .option("checkpointLocation", checkpoint_uri)
            .options(**write_options)
        )
        if partition_cols:
            stream_writer = stream_writer.partitionBy(*partition_cols)
        trigger_type = config.get("snk_avro_trigger_type", "processingTime")
        if trigger_type == "availableNow":
            stream_writer = stream_writer.trigger(availableNow=True)
        else:
            interval = config.get("snk_avro_trigger_interval", "10 seconds")
            stream_writer = stream_writer.trigger(processingTime=interval)
        stream_writer.start(output_uri)
    else:
        batch_writer = df.write.format("avro").mode(save_mode).options(**write_options)
        if partition_cols:
            batch_writer = batch_writer.partitionBy(*partition_cols)
        batch_writer.save(output_uri)
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline using an in-memory DataFrame created with `spark.createDataFrame(rows, schema=AVRO_RECORDS_SCHEMA)`. Do not require `spark-avro` JVM JAR resolution or external cloud buckets inside offline unit tests unless `--packages` is explicitly attached to the test session.

```python
from src.pipeline import AVRO_RECORDS_SCHEMA, transform


def test_avro_pipeline_transform(spark):
    """Tests downstream transformations offline using an in-memory Avro-shaped DataFrame."""
    rows = [
        ("rec_1", "acct_100", 1700000000000),
        ("rec_2", "acct_200", 1700000060000),
    ]
    input_df = spark.createDataFrame(rows, schema=AVRO_RECORDS_SCHEMA)
    result_dfs = transform({"src_avro": input_df}, {})

    assert "src_avro" in result_dfs
    assert result_dfs["src_avro"].count() == 2
```
