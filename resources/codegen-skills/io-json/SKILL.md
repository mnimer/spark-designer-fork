---
name: io-json
description: Batch and Structured Streaming JSON Lines and multi-line JSON file source with corrupt record handling and multi-cloud storage support.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [JsonSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-json.html
  validated: true
---

# JSON File Source (`JsonSource`)

Use these rules when generating PySpark 4.x code for a `JsonSource` node (`SRC17`).

## 1. Runtime & Connector Classpath Rules

- **Built-in Format**: JSON support is built into Spark 4.x. Never add a JSON package to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor `path` (or legacy `uri` / `filePattern`) and `storageBackend` across `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://`:
- When reading from `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and configure the `spark.hadoop.fs.s3a.*` or `spark.hadoop.fs.azure.account.key.*` properties through `spark.conf.set(...)`.

## 3. Batch vs. Streaming Read Patterns & Corrupt Record Handling

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read with `spark.read.options(**options).json(uri)` (and apply `.schema(JSON_SCHEMA)` when an explicit schema or `_corrupt_record` column is configured).
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit `StructType` Schema**: Structured Streaming JSON file reads require an explicit `StructType` schema. Always call `spark.readStream.schema(JSON_SCHEMA).options(**options).json(uri)`.
  - Configure `maxFilesPerTrigger` when set on the node.
- **Corrupt Record & Dead-Letter Queue (DLQ) Handling**:
  - When `mode="PERMISSIVE"` and corrupt record capture or a DLQ `error` branch is used, include `StructField("_corrupt_record", StringType(), True)` in `JSON_SCHEMA` and pass `.option("mode", "PERMISSIVE")` and `.option("columnNameOfCorruptRecord", "_corrupt_record")`.

### Supported Reader Options

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

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by constructing an in-memory DataFrame with `spark.createDataFrame(rows, schema=JSON_EVENTS_SCHEMA)`.

```python
from src.pipeline import JSON_EVENTS_SCHEMA, transform


def test_json_source_pipeline_transform(spark):
    """Verifies JSON source transformation logic offline using an in-memory DataFrame."""
    rows = [
        ("evt_1", "click", 0.95, None),
        ("evt_2", "purchase", 1.00, None),
    ]
    input_df = spark.createDataFrame(rows, schema=JSON_EVENTS_SCHEMA)
    result_dfs = transform({"src_json": input_df}, {})

    assert "src_json" in result_dfs
    assert result_dfs["src_json"].count() == 2
```
