---
name: io-tfrecord
description: Batch TensorFlow TFRecord file source using spark-tfrecord with Example and SequenceExample recordType support.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [TFRecordSource]
  preloadWhen: []
  sources:
    - prompt
    - https://github.com/linkedin/spark-tfrecord
  validated: true
---

# TensorFlow TFRecord Source (`TFRecordSource`)

Use these rules when generating PySpark 4.x code for a `TFRecordSource` node (`SRC21`).

## 1. Runtime & Connector Classpath Rules

- **Required External Maven Package (`spark-tfrecord`)**: Attach `com.linkedin.sparktfrecord:spark-tfrecord_2.13:0.7.0` via `spark.jars.packages` / `--packages` across Local, Dataproc Cluster, and Serverless Spark runner scripts.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add storage connectors to `spark.jars.packages` on Managed Spark targets.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor `path` (or legacy `uri` / `filePattern`) and `storageBackend` across `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://`:
- When reading from `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.conf.set("spark.hadoop.fs.s3a.*", ...)` or `spark.conf.set("spark.hadoop.fs.azure.account.key.*", ...)`.

## 3. Batch Read Pattern & Options (`read_sources`)

`TFRecordSource` is a **batch-only** (`readMode == "batch"`) source:
- Read with `spark.read.format("tfrecords").schema(TFRECORD_SCHEMA).option("recordType", record_type).load(uri)`.
- **Supported Options**:
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

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by constructing an in-memory DataFrame with `spark.createDataFrame(rows, schema=TFRECORD_FEATURES_SCHEMA)` so unit tests run without needing the `spark-tfrecord` JVM JAR or external files.

```python
from src.pipeline import TFRECORD_FEATURES_SCHEMA, transform


def test_tfrecord_source_pipeline_transform(spark):
    """Tests TFRecord pipeline transformations offline using an in-memory DataFrame."""
    rows = [
        ("ex_1", 1, [0.12, 0.45, -0.31]),
        ("ex_2", 0, [-0.08, 0.22, 0.77]),
    ]
    input_df = spark.createDataFrame(rows, schema=TFRECORD_FEATURES_SCHEMA)
    result_dfs = transform({"src_tfrecord": input_df}, {})

    assert "src_tfrecord" in result_dfs
    assert result_dfs["src_tfrecord"].count() == 2
```
