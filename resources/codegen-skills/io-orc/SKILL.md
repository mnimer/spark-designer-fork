---
name: io-orc
description: Batch and Structured Streaming Apache ORC file source and sink across GCS, S3A, ADLS Gen2, Azure Blob, HDFS, and local filesystems.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [OrcSource, OrcSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-orc.html
  validated: true
---

# Apache ORC Source & Sink (`OrcSource`, `OrcSink`)

Use these rules when generating PySpark 4.x code for `OrcSource` (`SRC14`) or `OrcSink` (`SNK12`) nodes.

## 1. Runtime & Connector Classpath Rules

- **Built-in Format**: Native vectorized Apache ORC support is built into Spark 4.x (`spark.sql.orc.impl=native`). Never add an ORC package to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.
- **Local Runner Storage Connectors**: Attach `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25`, `org.apache.hadoop:hadoop-aws:3.4.1`, or `org.apache.hadoop:hadoop-azure:3.4.1` via `--packages` only in `run_local.sh` when reading or writing cloud URIs locally.
- **Spark Connect Safe Configuration**: Configure cloud storage credentials exclusively via `spark.conf.set("spark.hadoop.fs.*", ...)` so code runs identically on standard Spark and Spark Connect.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor the configured `path` (or legacy `uri` / `filePattern` / `to`) and `storageBackend` across all supported URI schemes:
- **Google Cloud Storage (`gs://bucket/path/`)**: Authenticate via the runtime service account or Application Default Credentials.
- **Amazon S3 (`s3a://bucket/path/`)**: Fetch AWS credentials once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key`.
- **Azure ADLS Gen2 (`abfss://container@account.dfs.core.windows.net/path/`)** and **Azure Blob (`wasbs://container@account.blob.core.windows.net/path/`)**: Fetch the Azure storage key or secret via `get_secret()` and configure `spark.hadoop.fs.azure.account.key.<account>.dfs.core.windows.net` or `.blob.core.windows.net`.
- **HDFS (`hdfs://namenode:port/path/`)** and **Local Filesystem (`file:///path/` or `/path/`)**: Read and write directly from the path.

## 3. `OrcSource` — Batch vs. Streaming Read Patterns (`read_sources`)

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read with `spark.read.options(**options).orc(uri)` (with optional `.schema(ORC_SCHEMA)` when configured).
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit `StructType` Schema**: Structured Streaming file reads require an explicit `StructType` schema. Call `spark.readStream.schema(ORC_SCHEMA).options(**options).orc(uri)`.
  - If `maxFilesPerTrigger` is provided, configure it to rate-limit each micro-batch.

### Supported Reader Options (`OrcSource`)

Map every configured property on the `OrcSource` node:
- `mergeSchema`: `"true"` or `"false"` to merge schemas across ORC files.
- `pathGlobFilter`: Glob filter such as `"*.orc"` to select matching files.
- `recursiveFileLookup`: `"true"` or `"false"` to recursively read nested directories.
- `maxFilesPerTrigger`: Maximum number of new ORC files processed per streaming trigger (streaming mode only).

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

ORC_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", LongType(), True),
        StructField("metric_value", DoubleType(), True),
    ]
)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads batch or streaming ORC sources into DataFrames keyed by node ID."""
    dfs: dict[str, DataFrame] = {}
    orc_uri = config.get("src_orc_path", "gs://my-bucket/warehouse/events_orc/")
    read_mode = config.get("src_orc_read_mode", "batch")

    options: dict[str, str] = {
        "mergeSchema": str(config.get("src_orc_merge_schema", False)).lower(),
    }
    if config.get("src_orc_path_glob_filter"):
        options["pathGlobFilter"] = str(config["src_orc_path_glob_filter"])
    if config.get("src_orc_recursive_file_lookup") is not None:
        options["recursiveFileLookup"] = str(config["src_orc_recursive_file_lookup"]).lower()

    if read_mode == "streaming":
        if config.get("src_orc_max_files_per_trigger"):
            options["maxFilesPerTrigger"] = str(config["src_orc_max_files_per_trigger"])
        dfs["src_orc"] = (
            spark.readStream.schema(ORC_EVENTS_SCHEMA)
            .options(**options)
            .orc(orc_uri)
        )
    else:
        dfs["src_orc"] = (
            spark.read.schema(ORC_EVENTS_SCHEMA)
            .options(**options)
            .orc(orc_uri)
        )
    return dfs
```

## 4. `OrcSink` — Batch & Structured Streaming Write Patterns (`write_sinks`)

`OrcSink` (`SNK12`) has `streamingSinkMode: "native"`.

- **Batch Mode**:
  - Write with `df.write.mode(save_mode).options(**write_options).orc(output_uri)`.
  - Supported `saveMode` values: `"append"`, `"overwrite"`, `"errorifexists"`, and `"ignore"`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` before `.orc(output_uri)`.
- **Native Structured Streaming Mode (`mode == "native"`)**:
  - Write with `df.writeStream.format("orc").outputMode(output_mode).option("checkpointLocation", checkpoint_location).options(**write_options)` and start with `.start(output_uri)`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` on the `DataStreamWriter`.
  - Apply `.trigger(availableNow=True)` or `.trigger(processingTime=trigger_interval)` per `ExecutionPlan.streamingSinkGroups`.
- **Multi-Sink `foreachBatch` Fan-Out**:
  - When `ExecutionPlan.streamingSinkGroups` groups `OrcSink` into a multi-sink `foreachBatch` callback, execute `batch_df.write.mode("append").options(**write_options).orc(output_uri)`.

### Supported Writer Options (`OrcSink`)

- `compression`: `"snappy"` (default), `"zlib"`, `"zstd"`, `"lzo"`, or `"none"`.
- `partitionBy`: Comma-separated list or array of column names for directory partitioning.
- `maxRecordsPerFile`: Positive integer capping rows written per output ORC part-file.
- `orc.bloom.filter.columns` (`bloomFilterColumns`): Comma-separated column names for ORC bloom filter index creation (for example `"event_id,user_id"`).
- `orc.bloom.filter.fpp` (`bloomFilterFpp`): Optional bloom filter false positive probability between `0.0` and `1.0` (default `"0.05"`).

```python
def write_orc_sink(
    df: DataFrame,
    config: dict[str, Any],
    is_streaming: bool = False,
) -> None:
    """Writes a batch or streaming DataFrame to Apache ORC files."""
    output_uri = config.get("snk_orc_path", "gs://my-bucket/warehouse/events_orc_out/")
    save_mode = config.get("snk_orc_save_mode", "append")
    compression = config.get("snk_orc_compression", "snappy")
    raw_partitions = config.get("snk_orc_partition_by", "")
    partition_cols = [c.strip() for c in raw_partitions.split(",") if c.strip()]

    write_options: dict[str, str] = {
        "compression": compression,
    }
    if config.get("snk_orc_max_records_per_file"):
        write_options["maxRecordsPerFile"] = str(config["snk_orc_max_records_per_file"])
    if config.get("snk_orc_bloom_filter_columns"):
        bloom_cols = ",".join(
            c.strip() for c in str(config["snk_orc_bloom_filter_columns"]).split(",") if c.strip()
        )
        if bloom_cols:
            write_options["orc.bloom.filter.columns"] = bloom_cols
            if config.get("snk_orc_bloom_filter_fpp"):
                write_options["orc.bloom.filter.fpp"] = str(config["snk_orc_bloom_filter_fpp"])

    if is_streaming:
        checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
        checkpoint_uri = config.get(
            "snk_orc_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/snk_orc",
        )
        stream_writer = (
            df.writeStream.format("orc")
            .outputMode(config.get("snk_orc_output_mode", "append"))
            .option("checkpointLocation", checkpoint_uri)
            .options(**write_options)
        )
        if partition_cols:
            stream_writer = stream_writer.partitionBy(*partition_cols)
        trigger_type = config.get("snk_orc_trigger_type", "processingTime")
        if trigger_type == "availableNow":
            stream_writer = stream_writer.trigger(availableNow=True)
        else:
            interval = config.get("snk_orc_trigger_interval", "10 seconds")
            stream_writer = stream_writer.trigger(processingTime=interval)
        stream_writer.start(output_uri)
    else:
        batch_writer = df.write.mode(save_mode).options(**write_options)
        if partition_cols:
            batch_writer = batch_writer.partitionBy(*partition_cols)
        batch_writer.orc(output_uri)
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Verify `transform(dfs, config)` offline in `tests/test_pipeline.py` by building an in-memory DataFrame with `spark.createDataFrame(rows, schema=ORC_EVENTS_SCHEMA)`. When testing local ORC writes, use pytest's `tmp_path` directory rather than cloud storage URIs.

```python
from src.pipeline import ORC_EVENTS_SCHEMA, transform


def test_orc_pipeline_transform_and_local_write(spark, tmp_path):
    """Tests downstream pipeline logic and local ORC bloom-filtered write."""
    rows = [
        ("evt_100", 1001, 98.6),
        ("evt_101", 1002, 72.4),
    ]
    input_df = spark.createDataFrame(rows, schema=ORC_EVENTS_SCHEMA)
    result_dfs = transform({"src_orc": input_df}, {})

    out_dir = str(tmp_path / "orc_out")
    (
        result_dfs["src_orc"]
        .write.mode("overwrite")
        .option("compression", "snappy")
        .option("orc.bloom.filter.columns", "event_id")
        .orc(out_dir)
    )
    assert spark.read.orc(out_dir).count() == 2
```
