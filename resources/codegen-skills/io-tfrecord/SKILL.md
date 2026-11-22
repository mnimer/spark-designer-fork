---
name: io-tfrecord
description: TensorFlow TFRecord file source and sink using spark-tfrecord with Example and SequenceExample recordType, writeLocality, compression, and foreachBatch streaming support.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [TFRecordSource, TFRecordSink]
  preloadWhen: []
  sources:
    - prompt
    - https://github.com/linkedin/spark-tfrecord
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
# TensorFlow TFRecord Source & Sink (`TFRecordSource`, `TFRecordSink`)

Use these rules when generating PySpark 4.x code for `TFRecordSource` (`SRC21`) or `TFRecordSink` (`SNK18`) nodes.

## 1. Runtime & Connector Classpath Rules

- **Required External Maven Package (`spark-tfrecord`)**: Attach `com.linkedin.sparktfrecord:spark-tfrecord_2.13:0.7.0` via `spark.jars.packages` / `--packages` across Local, Dataproc Cluster, and Serverless Spark runner scripts.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add storage connectors to `spark.jars.packages` on Managed Spark targets.
- **Local Runner Storage Connectors**: Attach `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25`, `org.apache.hadoop:hadoop-aws:3.4.1`, or `org.apache.hadoop:hadoop-azure:3.4.1` via `--packages` only in `run_local.sh` when reading or writing cloud URIs locally.
- **Spark Connect Safe Configuration**: Configure cloud storage credentials exclusively through `spark.conf.set("spark.hadoop.fs.*", ...)` so code runs cleanly on standard Spark and Spark Connect.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor `path` (or legacy `uri` / `filePattern` / `to`) and `storageBackend` across `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://`:
- When reading or writing `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.conf.set("spark.hadoop.fs.s3a.*", ...)` or `spark.conf.set("spark.hadoop.fs.azure.account.key.*", ...)`.

## 3. `TFRecordSource` — Batch Read Pattern & Options (`read_sources`)

`TFRecordSource` is a **batch-only** (`readMode == "batch"`) source:
- Read with `spark.read.format("tfrecords").schema(TFRECORD_SCHEMA).option("recordType", record_type).load(uri)`.
- **Supported Reader Options**:
  - `recordType`: `"Example"` (default for `tf.train.Example` protocol buffers) or `"SequenceExample"` (for `tf.train.SequenceExample` sequential feature lists).
  - Explicit `StructType` schema: Supply `.schema(TFRECORD_SCHEMA)` when configured to avoid full-dataset schema inference scans over large TFRecord directories.
  - `pathGlobFilter` and `recursiveFileLookup` when configured on `StorageBackendFields`.

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, FloatType, LongType, StringType, StructField, StructType

TFRECORD_FEATURES_SCHEMA = StructType(
    [
        StructField("example_id", StringType(), False),
        StructField("label", LongType(), True),
        StructField("embedding", ArrayType(FloatType()), True),
    ]
)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads batch TFRecord files into a DataFrame keyed by node ID."""
    dfs: dict[str, DataFrame] = {}
    tfrecord_uri = config.get("src_tfrecord_path", "gs://my-bucket/ml/training_tfrecords/")
    record_type = config.get("src_tfrecord_record_type", "Example")

    options: dict[str, str] = {
        "recordType": record_type,
    }
    if config.get("src_tfrecord_path_glob_filter"):
        options["pathGlobFilter"] = str(config["src_tfrecord_path_glob_filter"])
    if config.get("src_tfrecord_recursive_file_lookup") is not None:
        options["recursiveFileLookup"] = str(config["src_tfrecord_recursive_file_lookup"]).lower()

    dfs["src_tfrecord"] = (
        spark.read.format("tfrecords")
        .schema(TFRECORD_FEATURES_SCHEMA)
        .options(**options)
        .load(tfrecord_uri)
    )
    return dfs
```

## 4. `TFRecordSink` — Batch & `foreachBatch` Streaming Write Patterns (`write_sinks`)

`TFRecordSink` (`SNK18`) uses `.write.format("tfrecords")` (`com.linkedin.sparktfrecord:spark-tfrecord_2.13:0.7.0`) and has `streamingSinkMode: "foreachBatch"`.

- **Batch Mode**:
  - Write with `df.write.format("tfrecords").mode(save_mode).options(**write_options).save(output_uri)`.
  - Supported `saveMode` values: `"append"`, `"overwrite"`, `"errorifexists"`, and `"ignore"`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` before `.save(output_uri)`.
- **Streaming Mode (`foreachBatch`)**:
  - Because `spark-tfrecord` implements batch `FileFormat` / `RelationProvider` rather than native `StreamingSink`, streaming branches use `df.writeStream.foreachBatch(write_micro_batch)`.
  - Write each micro-batch idempotently to `f"{output_uri.rstrip('/')}/batch_{batch_id}"` using `.mode("overwrite")`. When grouped with other sinks in `ExecutionPlan.streamingSinkGroups`, call `batch_df.cache()` before writing and `batch_df.unpersist()` inside a `finally` block.
- **Supported Feature Column Types**:
  - `Example` supports scalar numeric/string/binary columns and 1-D arrays (`Int64List`, `FloatList`, `BytesList`).
  - `SequenceExample` additionally supports 2-D arrays (`ArrayType(ArrayType(...))`) mapped to `FeatureLists`.

### Supported Writer Options (`TFRecordSink`)

- `recordType`: `"Example"` (default) or `"SequenceExample"`.
- `writeLocality`: `"distributed"` (default, writes through Hadoop FileSystem to GCS, S3A, ADLS, or HDFS) or `"local"` (writes directly to executor local disk paths).
- `codec` (`compression`): Omit when `"none"` (uncompressed TFRecord). Map `"gzip"` to `"org.apache.hadoop.io.compress.GzipCodec"` and `"deflate"` to `"org.apache.hadoop.io.compress.DeflateCodec"`.
- `partitionBy`: Comma-separated list or array of column names for directory partitioning.
- `maxRecordsPerFile`: Positive integer capping TFRecord examples written per output part-file.

```python
TFRECORD_CODEC_MAP: dict[str, str] = {
    "gzip": "org.apache.hadoop.io.compress.GzipCodec",
    "deflate": "org.apache.hadoop.io.compress.DeflateCodec",
}


def build_tfrecord_write_options(config: dict[str, Any]) -> dict[str, str]:
    """Builds spark-tfrecord writer options from pipeline configuration."""
    write_options: dict[str, str] = {
        "recordType": config.get("snk_tfrecord_record_type", "Example"),
        "writeLocality": config.get("snk_tfrecord_write_locality", "distributed"),
    }
    compression = str(config.get("snk_tfrecord_compression", "none")).lower()
    if compression in TFRECORD_CODEC_MAP:
        write_options["codec"] = TFRECORD_CODEC_MAP[compression]
    if config.get("snk_tfrecord_max_records_per_file"):
        write_options["maxRecordsPerFile"] = str(config["snk_tfrecord_max_records_per_file"])
    return write_options


def write_tfrecord_sink(
    df: DataFrame,
    config: dict[str, Any],
    is_streaming: bool = False,
) -> None:
    """Writes a batch or streaming DataFrame to TensorFlow TFRecord files."""
    output_uri = config.get("snk_tfrecord_path", "gs://my-bucket/ml/tfrecords_out/")
    save_mode = config.get("snk_tfrecord_save_mode", "append")
    raw_partitions = config.get("snk_tfrecord_partition_by", "")
    partition_cols = [c.strip() for c in raw_partitions.split(",") if c.strip()]
    write_options = build_tfrecord_write_options(config)

    if is_streaming:
        checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
        checkpoint_uri = config.get(
            "snk_tfrecord_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/snk_tfrecord",
        )

        def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
            batch_path = f"{output_uri.rstrip('/')}/batch_{batch_id}"
            writer = batch_df.write.format("tfrecords").mode("overwrite").options(**write_options)
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(batch_path)

        stream_writer = (
            df.writeStream.foreachBatch(write_micro_batch)
            .outputMode(config.get("snk_tfrecord_output_mode", "append"))
            .option("checkpointLocation", checkpoint_uri)
        )
        trigger_type = config.get("snk_tfrecord_trigger_type", "processingTime")
        if trigger_type == "availableNow":
            stream_writer = stream_writer.trigger(availableNow=True)
        else:
            interval = config.get("snk_tfrecord_trigger_interval", "10 seconds")
            stream_writer = stream_writer.trigger(processingTime=interval)
        stream_writer.start()
    else:
        batch_writer = df.write.format("tfrecords").mode(save_mode).options(**write_options)
        if partition_cols:
            batch_writer = batch_writer.partitionBy(*partition_cols)
        batch_writer.save(output_uri)
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by constructing an in-memory DataFrame with `spark.createDataFrame(rows, schema=TFRECORD_FEATURES_SCHEMA)` so unit tests run without needing the `spark-tfrecord` JVM JAR or external storage buckets.

```python
from src.pipeline import TFRECORD_FEATURES_SCHEMA, transform


def test_tfrecord_pipeline_transform(spark):
    """Tests TFRecord feature pipeline transformations offline using an in-memory DataFrame."""
    rows = [
        ("ex_1", 1, [0.12, 0.45, -0.31]),
        ("ex_2", 0, [-0.08, 0.22, 0.77]),
    ]
    input_df = spark.createDataFrame(rows, schema=TFRECORD_FEATURES_SCHEMA)
    result_dfs = transform({"src_tfrecord": input_df}, {})

    assert "src_tfrecord" in result_dfs
    assert result_dfs["src_tfrecord"].count() == 2
```
