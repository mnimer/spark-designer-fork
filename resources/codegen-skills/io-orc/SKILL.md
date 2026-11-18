---
name: io-orc
description: Batch and Structured Streaming Apache ORC file source across GCS, S3A, ADLS Gen2, Azure Blob, HDFS, and local filesystems.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [OrcSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-orc.html
  validated: true
---

# Apache ORC Source (`OrcSource`)

Use these rules when generating PySpark 4.x code for an `OrcSource` node (`SRC14`).

## 1. Runtime & Connector Classpath Rules

- **Built-in Format**: Native vectorized Apache ORC support is built into Spark 4.x (`spark.sql.orc.impl=native`). Never add an ORC package to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.
- **Local Runner Storage Connectors**: Attach `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25`, `org.apache.hadoop:hadoop-aws:3.4.1`, or `org.apache.hadoop:hadoop-azure:3.4.1` via `--packages` only in `run_local.sh` when reading cloud URIs locally.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor the configured `path` (or legacy `uri` / `filePattern`) and `storageBackend` across all supported URI schemes:
- **Google Cloud Storage (`gs://bucket/path/`)**: Authenticate via the runtime service account or Application Default Credentials.
- **Amazon S3 (`s3a://bucket/path/`)**: Fetch AWS credentials once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key`.
- **Azure ADLS Gen2 (`abfss://container@account.dfs.core.windows.net/path/`)** and **Azure Blob (`wasbs://container@account.blob.core.windows.net/path/`)**: Fetch the Azure storage key or secret via `get_secret()` and configure `spark.hadoop.fs.azure.account.key.<account>.dfs.core.windows.net` or `.blob.core.windows.net`.
- **HDFS (`hdfs://namenode:port/path/`)** and **Local Filesystem (`file:///path/` or `/path/`)**: Read directly from the path.

## 3. Batch vs. Streaming Read Patterns (`read_sources`)

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read with `spark.read.options(**options).orc(uri)` (with optional `.schema(ORC_SCHEMA)` when configured).
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit `StructType` Schema**: Structured Streaming file reads require an explicit `StructType` schema. Call `spark.readStream.schema(ORC_SCHEMA).options(**options).orc(uri)`.
  - Configure `maxFilesPerTrigger` when provided to rate-limit each micro-batch.

### Supported Reader Options

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

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Verify `transform(dfs, config)` offline in `tests/test_pipeline.py` by building an in-memory DataFrame with `spark.createDataFrame(rows, schema=ORC_EVENTS_SCHEMA)` for the `OrcSource` node ID.

```python
from src.pipeline import ORC_EVENTS_SCHEMA, transform


def test_orc_source_pipeline_transform(spark):
    """Tests downstream pipeline logic with an in-memory DataFrame matching ORC_EVENTS_SCHEMA."""
    rows = [
        ("evt_100", 1001, 98.6),
        ("evt_101", 1002, 72.4),
    ]
    input_df = spark.createDataFrame(rows, schema=ORC_EVENTS_SCHEMA)
    result_dfs = transform({"src_orc": input_df}, {})

    assert "src_orc" in result_dfs
    assert result_dfs["src_orc"].count() == 2
```
