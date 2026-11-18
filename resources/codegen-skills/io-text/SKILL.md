---
name: io-text
description: Batch and Structured Streaming plain-text file source supporting line-by-line and whole-file reads across cloud and local filesystems.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [TextSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-text.html
  validated: true
---

# Text File Source (`TextSource`)

Use these rules when generating PySpark 4.x code for a `TextSource` node (`SRC18`).

## 1. Runtime & Connector Classpath Rules

- **Built-in Format**: Plain-text file reading is built into Spark 4.x and emits a single column `value: string` (plus any discovered partition columns). Never add an external text package to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor `path` (or legacy `uri` / `filePattern`) and `storageBackend` across `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://`:
- When reading from `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.conf.set("spark.hadoop.fs.s3a.*", ...)` or `spark.conf.set("spark.hadoop.fs.azure.account.key.*", ...)`.

## 3. Batch vs. Streaming Read Patterns (`read_sources`)

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read using `spark.read.options(**options).text(uri)`.
- **Streaming Mode (`readMode == "streaming"`)**:
  - Read using `spark.readStream.options(**options).text(uri)` (the text source has a fixed built-in schema of `StructType([StructField("value", StringType(), True)])`).
  - Pass `maxFilesPerTrigger` when configured to bound micro-batch ingestion.

### Supported Reader Options

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

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by creating an in-memory DataFrame with a `value` string column using `spark.createDataFrame(rows, schema=TEXT_SCHEMA)`.

```python
from pyspark.sql.types import StringType, StructField, StructType
from src.pipeline import transform

TEXT_SCHEMA = StructType([StructField("value", StringType(), True)])


def test_text_source_pipeline_transform(spark):
    """Verifies text pipeline transformations offline using an in-memory DataFrame."""
    rows = [
        ("2026-01-15 INFO Job started",),
        ("2026-01-15 ERROR Connection timeout",),
    ]
    input_df = spark.createDataFrame(rows, schema=TEXT_SCHEMA)
    result_dfs = transform({"src_text": input_df}, {})

    assert "src_text" in result_dfs
    assert result_dfs["src_text"].count() == 2
```
