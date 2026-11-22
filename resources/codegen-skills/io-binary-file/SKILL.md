---
name: io-binary-file
description: Batch and Structured Streaming binaryFile source for ingesting raw files, images, PDFs, and audio with fixed metadata columns.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [BinaryFileSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html
  validated: true
---

# Binary File Source (`BinaryFileSource`)

Use these rules when generating PySpark 4.x code for a `BinaryFileSource` node (`SRC20`).

## 1. Runtime & Connector Classpath Rules

- **Built-in Format**: The `binaryFile` data source is built into Spark 4.x. Never add a binaryFile package to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor `path` (or legacy `uri` / `filePattern`) and `storageBackend` across `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://`:
- When reading from `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.conf.set("spark.hadoop.fs.s3a.*", ...)` or `spark.conf.set("spark.hadoop.fs.azure.account.key.*", ...)`.

## 3. Fixed Output Schema & Batch vs. Streaming Read Patterns (`read_sources`)

Every `binaryFile` read produces a DataFrame with this fixed four-column output schema (plus any discovered partition columns when `recursiveFileLookup` is `"false"`):
- `path`: `StringType()` (the full file URI)
- `modificationTime`: `TimestampType()` (the file's last modification timestamp)
- `length`: `LongType()` (the file length in bytes)
- `content`: `BinaryType()` (the raw file bytes)

### Read Modes & Options

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read using `spark.read.format("binaryFile").options(**options).load(uri)`.
- **Streaming Mode (`readMode == "streaming"`)**:
  - Read using `spark.readStream.format("binaryFile").options(**options).load(uri)`.
  - Pass `maxFilesPerTrigger` when configured to bound micro-batch ingestion.
- **Supported Options**:
  - `pathGlobFilter`: Glob pattern such as `"*.png"`, `"*.jpg"`, or `"*.pdf"` to ingest only matching binary files without filtering after reading file bytes.
  - `recursiveFileLookup`: `"true"` or `"false"` to scan nested subdirectories recursively (disables partition discovery when `"true"`).
  - `maxFilesPerTrigger`: Maximum number of new files read per streaming trigger (streaming mode only).

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType, TimestampType

BINARY_FILE_SCHEMA = StructType(
    [
        StructField("path", StringType(), False),
        StructField("modificationTime", TimestampType(), False),
        StructField("length", LongType(), False),
        StructField("content", BinaryType(), True),
    ]
)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads batch or streaming raw binary files into DataFrames keyed by node ID."""
    dfs: dict[str, DataFrame] = {}
    binary_uri = config.get("src_binary_path", "gs://my-bucket/documents/incoming/")
    read_mode = config.get("src_binary_read_mode", "batch")

    options: dict[str, str] = {}
    if config.get("src_binary_path_glob_filter"):
        options["pathGlobFilter"] = str(config["src_binary_path_glob_filter"])
    if config.get("src_binary_recursive_file_lookup") is not None:
        options["recursiveFileLookup"] = str(config["src_binary_recursive_file_lookup"]).lower()

    if read_mode == "streaming":
        if config.get("src_binary_max_files_per_trigger"):
            options["maxFilesPerTrigger"] = str(config["src_binary_max_files_per_trigger"])
        dfs["src_binary"] = (
            spark.readStream.format("binaryFile")
            .options(**options)
            .load(binary_uri)
        )
    else:
        dfs["src_binary"] = (
            spark.read.format("binaryFile")
            .options(**options)
            .load(binary_uri)
        )
    return dfs
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by constructing an in-memory DataFrame matching `BINARY_FILE_SCHEMA` (`path`, `modificationTime`, `length`, `content`).

```python
from datetime import datetime, timezone
from src.pipeline import BINARY_FILE_SCHEMA, transform


def test_binary_file_source_pipeline_transform(spark):
    """Verifies binaryFile pipeline transformations offline with an in-memory DataFrame."""
    rows = [
        (
            "gs://my-bucket/documents/incoming/doc1.pdf",
            datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc),
            12,
            b"%PDF-1.7-raw",
        ),
        (
            "gs://my-bucket/documents/incoming/doc2.pdf",
            datetime(2026, 1, 15, 10, 5, tzinfo=timezone.utc),
            12,
            b"%PDF-1.7-two",
        ),
    ]
    input_df = spark.createDataFrame(rows, schema=BINARY_FILE_SCHEMA)
    result_dfs = transform({"src_binary": input_df}, {})

    assert "src_binary" in result_dfs
    assert result_dfs["src_binary"].count() == 2
```
