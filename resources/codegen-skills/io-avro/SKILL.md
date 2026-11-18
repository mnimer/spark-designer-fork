---
name: io-avro
description: Batch and Structured Streaming Apache Avro file source using spark-avro across GCS, S3A, ADLS Gen2, Azure Blob, HDFS, and local filesystems.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [AvroSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-avro.html
  validated: true
---

# Apache Avro Source (`AvroSource`)

Use these rules when generating PySpark 4.x code for an `AvroSource` node (`SRC15`).

## 1. Runtime & Connector Classpath Rules

- **Required External Maven Package (`spark-avro`)**: Apache Avro is an external module in Spark 4.x. Include `org.apache.spark:spark-avro_2.13:{{spark_version}}` in `spark.jars.packages` / `--packages` across Local, Dataproc Cluster, and Serverless Spark runner scripts.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless Spark and Dataproc Clusters. Never add storage connectors to `spark.jars.packages` on Managed Spark targets.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Support all `StorageBackendFields` URI schemes (`gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, `file://`):
- When reading from `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and set the corresponding `spark.hadoop.fs.s3a.*` or `spark.hadoop.fs.azure.account.key.*` properties.

## 3. Batch vs. Streaming Read Patterns (`read_sources`)

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read using `spark.read.format("avro").options(**options).load(uri)`.
  - Optionally apply `.schema(AVRO_STRUCT_SCHEMA)` when a Spark SQL `StructType` is configured.
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit Schema**: Provide either `.schema(AVRO_STRUCT_SCHEMA)` or `.option("avroSchema", avro_json_schema)` so Structured Streaming knows the schema prior to starting the stream:
    `spark.readStream.format("avro").schema(AVRO_STRUCT_SCHEMA).options(**options).load(uri)`.
  - Pass `maxFilesPerTrigger` when configured to bound each micro-batch.

### Supported Reader Options

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

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline using an in-memory DataFrame created with `spark.createDataFrame(rows, schema=AVRO_RECORDS_SCHEMA)`. Do not require `spark-avro` file I/O or cloud buckets in `tests/test_pipeline.py`.

```python
from src.pipeline import AVRO_RECORDS_SCHEMA, transform


def test_avro_source_pipeline_transform(spark):
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
