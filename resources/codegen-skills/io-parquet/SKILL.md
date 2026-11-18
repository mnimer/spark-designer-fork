---
name: io-parquet
description: Batch and Structured Streaming Apache Parquet file source across GCS, S3A, ADLS Gen2, Azure Blob, HDFS, and local filesystems.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [ParquetSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
  validated: true
---

# Apache Parquet Source (`ParquetSource`)

Use these rules when generating PySpark 4.x code for a `ParquetSource` node (`SRC13`).

## 1. Runtime & Connector Classpath Rules

- **Built-in Format**: Apache Parquet support is built into Spark 4.x. Never add a Parquet package to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.
- **Local Runner Storage Connectors**: Attach `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25`, `org.apache.hadoop:hadoop-aws:3.4.1`, or `org.apache.hadoop:hadoop-azure:3.4.1` via `--packages` only in `run_local.sh` when reading from cloud object storage locally.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor the configured `path` (or legacy `uri` / `filePattern`) and `storageBackend` across all supported URI schemes:
- **Google Cloud Storage (`gs://bucket/path/`)**: Authenticate using the runtime service account or Application Default Credentials.
- **Amazon S3 (`s3a://bucket/path/`)**: Resolve AWS credentials once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key` (plus optional `spark.hadoop.fs.s3a.session.token` or `spark.hadoop.fs.s3a.endpoint`).
- **Azure ADLS Gen2 (`abfss://container@account.dfs.core.windows.net/path/`)**: Resolve the storage account key or OAuth client secret via `get_secret()` and set `spark.hadoop.fs.azure.account.key.<account>.dfs.core.windows.net`.
- **Azure Blob Storage (`wasbs://container@account.blob.core.windows.net/path/`)**: Resolve the storage account key via `get_secret()` and set `spark.hadoop.fs.azure.account.key.<account>.blob.core.windows.net`.
- **HDFS (`hdfs://namenode:port/path/`)** and **Local Filesystem (`file:///path/` or `/path/`)**: Read directly without cloud secret resolution.

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

## 3. Batch vs. Streaming Read Patterns (`read_sources`)

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read with `spark.read.options(**options).parquet(uri)`.
  - Apply `.schema(schema)` when an explicit schema is configured and `mergeSchema` is not enabled.
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit `StructType` Schema**: Structured Streaming file sources require an explicit `StructType` schema (unless `spark.sql.streaming.schemaInference` is explicitly set). Always pass `.schema(PARQUET_SCHEMA)` before `.parquet(uri)`: `spark.readStream.schema(PARQUET_SCHEMA).options(**options).parquet(uri)`.
  - Pass `maxFilesPerTrigger` when configured to bound micro-batch file ingestion size.

### Supported Reader Options

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
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

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

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame with `spark.createDataFrame(rows, schema=ORDERS_SCHEMA)` and passing it under the `ParquetSource` node ID. Never connect to `gs://`, `s3a://`, or `abfss://` in unit tests.

```python
from datetime import datetime, timezone
from src.pipeline import ORDERS_SCHEMA, transform


def test_parquet_source_pipeline_transform(spark):
    """Verifies downstream transformations using an in-memory DataFrame matching ORDERS_SCHEMA."""
    sample_rows = [
        ("ord_1", "cust_10", 125.50, datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)),
        ("ord_2", "cust_20", 42.00, datetime(2026, 1, 15, 12, 5, tzinfo=timezone.utc)),
    ]
    input_df = spark.createDataFrame(sample_rows, schema=ORDERS_SCHEMA)
    result_dfs = transform({"src_parquet": input_df}, {})

    assert "src_parquet" in result_dfs
    assert result_dfs["src_parquet"].count() == 2
```
