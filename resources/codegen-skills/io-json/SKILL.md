---
name: io-json
description: Batch and Structured Streaming JSON Lines and multi-line JSON file source and sink with corrupt record handling, formatting options, compression, and multi-cloud storage support.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [JsonSource, JsonSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-json.html
  validated: true
---

# JSON File Source & Sink (`JsonSource`, `JsonSink`)

Use these rules when generating PySpark 4.x code for `JsonSource` (`SRC17`) or `JsonSink` (`SNK15`) nodes.

## 1. Runtime & Connector Classpath Rules

- **Built-in Format**: JSON support is built into Spark 4.x. Never add a JSON package to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.
- **Local Runner Storage Connectors**: Attach `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25`, `org.apache.hadoop:hadoop-aws:3.4.1`, or `org.apache.hadoop:hadoop-azure:3.4.1` via `--packages` only in `run_local.sh` when reading or writing cloud URIs locally.
- **Spark Connect Safe Configuration**: Configure cloud storage credentials exclusively through `spark.conf.set("spark.hadoop.fs.*", ...)` so code runs cleanly on standard Spark and Spark Connect.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor `path` (or legacy `uri` / `filePattern` / `to`) and `storageBackend` across `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://`:
- When reading or writing `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and configure the `spark.hadoop.fs.s3a.*` or `spark.hadoop.fs.azure.account.key.*` properties through `spark.conf.set(...)`.

## 3. `JsonSource` — Batch vs. Streaming Read Patterns & Corrupt Record Handling

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read with `spark.read.options(**options).json(uri)` (and apply `.schema(JSON_SCHEMA)` when an explicit schema or `_corrupt_record` column is configured).
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit `StructType` Schema**: Structured Streaming JSON file reads require an explicit `StructType` schema. Always call `spark.readStream.schema(JSON_SCHEMA).options(**options).json(uri)`.
  - If `maxFilesPerTrigger` is set on the node, configure it to bound micro-batch size.
- **Corrupt Record & Dead-Letter Queue (DLQ) Handling**:
  - When `mode="PERMISSIVE"` and corrupt record capture or a DLQ `error` branch is used, include `StructField("_corrupt_record", StringType(), True)` in `JSON_SCHEMA` and pass `.option("mode", "PERMISSIVE")` and `.option("columnNameOfCorruptRecord", "_corrupt_record")`.

### Supported Reader Options (`JsonSource`)

Map every configured `JsonSource` node property:
- `multiLine`: `"true"` or `"false"` (default `"false"` for JSON Lines, or `"true"` when JSON objects span multiple lines).
- `primitivesAsString`: `"true"` or `"false"` to infer all primitive JSON values as strings.
- `allowComments`: `"true"` or `"false"` to permit Java/C-style comments inside JSON files.
- `mode`: `"PERMISSIVE"` (default), `"DROPMALFORMED"`, or `"FAILFAST"`.
- `columnNameOfCorruptRecord`: Column name for malformed JSON records in `PERMISSIVE` mode (default `"_corrupt_record"`).
- `pathGlobFilter`, `recursiveFileLookup`, and `maxFilesPerTrigger` (streaming mode).

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

JSON_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("score", DoubleType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads batch or streaming JSON files into DataFrames keyed by node ID."""
    dfs: dict[str, DataFrame] = {}
    json_uri = config.get("src_json_path", "gs://my-bucket/events/json/")
    read_mode = config.get("src_json_read_mode", "batch")

    options: dict[str, str] = {
        "multiLine": str(config.get("src_json_multiline", False)).lower(),
        "primitivesAsString": str(config.get("src_json_primitives_as_string", False)).lower(),
        "allowComments": str(config.get("src_json_allow_comments", False)).lower(),
        "mode": config.get("src_json_mode", "PERMISSIVE"),
        "columnNameOfCorruptRecord": config.get("src_json_corrupt_record_col", "_corrupt_record"),
    }
    if config.get("src_json_path_glob_filter"):
        options["pathGlobFilter"] = str(config["src_json_path_glob_filter"])
    if config.get("src_json_recursive_file_lookup") is not None:
        options["recursiveFileLookup"] = str(config["src_json_recursive_file_lookup"]).lower()

    if read_mode == "streaming":
        if config.get("src_json_max_files_per_trigger"):
            options["maxFilesPerTrigger"] = str(config["src_json_max_files_per_trigger"])
        dfs["src_json"] = (
            spark.readStream.schema(JSON_EVENTS_SCHEMA)
            .options(**options)
            .json(json_uri)
        )
    else:
        dfs["src_json"] = (
            spark.read.schema(JSON_EVENTS_SCHEMA)
            .options(**options)
            .json(json_uri)
        )
    return dfs
```

## 4. `JsonSink` — Batch & Structured Streaming Write Patterns (`write_sinks`)

`JsonSink` (`SNK15`) has `streamingSinkMode: "native"` and writes newline-delimited JSON Lines (one JSON object per row).

- **Batch Mode**:
  - Write with `df.write.mode(save_mode).options(**write_options).json(output_uri)`.
  - Supported `saveMode` values: `"append"`, `"overwrite"`, `"errorifexists"`, and `"ignore"`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` before `.json(output_uri)`.
- **Native Structured Streaming Mode (`mode == "native"`)**:
  - Write with `df.writeStream.format("json").outputMode(output_mode).option("checkpointLocation", checkpoint_location).options(**write_options)` and start with `.start(output_uri)`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` on the `DataStreamWriter`.
  - Apply `.trigger(availableNow=True)` or `.trigger(processingTime=trigger_interval)` per `ExecutionPlan.streamingSinkGroups`.
- **Multi-Sink `foreachBatch` Fan-Out**:
  - When `ExecutionPlan.streamingSinkGroups` groups `JsonSink` into a multi-sink `foreachBatch` callback, call `batch_df.write.mode("append").options(**write_options).json(output_uri)`.

### Supported Writer Options (`JsonSink`)

- `ignoreNullFields`: `"true"` (Spark default, omits null keys from serialized JSON objects) or `"false"` (emits explicit `null` values).
- `timestampFormat`: Timestamp formatting pattern (for example `"yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"`).
- `dateFormat`: Date formatting pattern (for example `"yyyy-MM-dd"`).
- `lineSep`: Record line separator string (defaults to `"\n"`).
- `compression`: `"none"` (default), `"gzip"`, `"zstd"`, `"bzip2"`, `"lz4"`, `"snappy"`, or `"deflate"`.
- `partitionBy`: Comma-separated list or array of column names for directory partitioning.
- `maxRecordsPerFile`: Positive integer capping rows written per output JSON part-file.

```python
def write_json_sink(
    df: DataFrame,
    config: dict[str, Any],
    is_streaming: bool = False,
) -> None:
    """Writes a batch or streaming DataFrame to JSON Lines files."""
    output_uri = config.get("snk_json_path", "gs://my-bucket/events/json_out/")
    save_mode = config.get("snk_json_save_mode", "append")
    compression = config.get("snk_json_compression", "none")
    raw_partitions = config.get("snk_json_partition_by", "")
    partition_cols = [c.strip() for c in raw_partitions.split(",") if c.strip()]

    write_options: dict[str, str] = {
        "ignoreNullFields": str(config.get("snk_json_ignore_null_fields", True)).lower(),
        "compression": compression,
    }
    if config.get("snk_json_timestamp_format"):
        write_options["timestampFormat"] = str(config["snk_json_timestamp_format"])
    if config.get("snk_json_date_format"):
        write_options["dateFormat"] = str(config["snk_json_date_format"])
    if config.get("snk_json_line_sep"):
        write_options["lineSep"] = str(config["snk_json_line_sep"])
    if config.get("snk_json_max_records_per_file"):
        write_options["maxRecordsPerFile"] = str(config["snk_json_max_records_per_file"])

    if is_streaming:
        checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
        checkpoint_uri = config.get(
            "snk_json_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/snk_json",
        )
        stream_writer = (
            df.writeStream.format("json")
            .outputMode(config.get("snk_json_output_mode", "append"))
            .option("checkpointLocation", checkpoint_uri)
            .options(**write_options)
        )
        if partition_cols:
            stream_writer = stream_writer.partitionBy(*partition_cols)
        trigger_type = config.get("snk_json_trigger_type", "processingTime")
        if trigger_type == "availableNow":
            stream_writer = stream_writer.trigger(availableNow=True)
        else:
            interval = config.get("snk_json_trigger_interval", "10 seconds")
            stream_writer = stream_writer.trigger(processingTime=interval)
        stream_writer.start(output_uri)
    else:
        batch_writer = df.write.mode(save_mode).options(**write_options)
        if partition_cols:
            batch_writer = batch_writer.partitionBy(*partition_cols)
        batch_writer.json(output_uri)
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by constructing an in-memory DataFrame with `spark.createDataFrame(rows, schema=JSON_EVENTS_SCHEMA)`. When testing local JSON writes, target pytest's `tmp_path` directory.

```python
from src.pipeline import JSON_EVENTS_SCHEMA, transform


def test_json_pipeline_transform_and_local_write(spark, tmp_path):
    """Verifies JSON pipeline transformation logic and local JSON Lines write."""
    rows = [
        ("evt_1", "click", 0.95, None),
        ("evt_2", "purchase", 1.00, None),
    ]
    input_df = spark.createDataFrame(rows, schema=JSON_EVENTS_SCHEMA)
    result_dfs = transform({"src_json": input_df}, {})

    out_dir = str(tmp_path / "json_out")
    (
        result_dfs["src_json"]
        .drop("_corrupt_record")
        .write.mode("overwrite")
        .option("ignoreNullFields", "false")
        .json(out_dir)
    )
    assert spark.read.json(out_dir).count() == 2
```
