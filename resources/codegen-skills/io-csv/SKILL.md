---
name: io-csv
description: Batch and Structured Streaming CSV file source with schema inference, explicit StructType schemas, corrupt record handling, and multi-cloud storage support.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [CsvSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-csv.html
  validated: true
---

# CSV File Source (`CsvSource`)

Use these rules when generating PySpark 4.x code for a `CsvSource` node (`SRC16`).

## 1. Runtime & Connector Classpath Rules

- **Built-in Format**: CSV support is built into Spark 4.x. Never add an external CSV package to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor `path` (or legacy `uri` / `filePattern`) and `storageBackend` across `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://`:
- When reading from `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.conf.set("spark.hadoop.fs.s3a.*", ...)` or `spark.conf.set("spark.hadoop.fs.azure.account.key.*", ...)`.

## 3. Batch vs. Streaming Read Patterns & Corrupt Record Handling

- **Batch Mode (`readMode == "batch"`, default)**:
  - Read with `spark.read.options(**options).csv(uri)`.
  - Use `.schema(CSV_SCHEMA)` when an explicit schema is configured or when corrupt record handling is enabled (`mode="PERMISSIVE"` with `columnNameOfCorruptRecord="_corrupt_record"`). Otherwise pass `header=True` and `inferSchema=True` as configured.
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit `StructType` Schema**: Structured Streaming CSV sources require an explicit `StructType` schema (`inferSchema=True` is not supported on streaming CSV reads without explicit schema inference configuration). Always pass `spark.readStream.schema(CSV_SCHEMA).options(**options).csv(uri)`.
  - Pass `maxFilesPerTrigger` when configured to control micro-batch file count.
- **Corrupt Record & Dead-Letter Queue (DLQ) Handling**:
  - When `mode="PERMISSIVE"` (or `parseMode="PERMISSIVE"`) and corrupt record capture or a DLQ `error` branch is used, include `StructField("_corrupt_record", StringType(), True)` as the final field in the explicit `StructType` schema and set `.option("mode", "PERMISSIVE")` and `.option("columnNameOfCorruptRecord", "_corrupt_record")`.
  - Split valid rows (`F.col("_corrupt_record").isNull()`) and malformed rows (`F.col("_corrupt_record").isNotNull()`) when routing to a DLQ sink.

### Supported Reader Options

Map every configured `CsvSource` node property:
- `header`: `"true"` or `"false"` (default `"true"`).
- `inferSchema`: `"true"` or `"false"` (default `"true"` in batch mode when no explicit schema is provided).
- `sep` (or `delimiter`): Field delimiter character (default `","`).
- `quote`: Quote character for escaping delimiters inside fields (default `'"'`).
- `escape`: Escape character inside quoted values (default `"\\"`).
- `nullValue`: String representation interpreted as SQL `NULL` (for example `""` or `"NULL"`).
- `multiLine`: `"true"` or `"false"` to allow newlines within quoted CSV fields.
- `mode` (or `parseMode`): `"PERMISSIVE"` (default), `"DROPMALFORMED"`, or `"FAILFAST"`.
- `columnNameOfCorruptRecord`: Column storing unparseable raw lines in `PERMISSIVE` mode (default `"_corrupt_record"`).
- `pathGlobFilter`, `recursiveFileLookup`, and `maxFilesPerTrigger` (streaming mode).

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

CSV_TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("txn_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads batch or streaming CSV sources into DataFrames keyed by node ID."""
    dfs: dict[str, DataFrame] = {}
    csv_uri = config.get("src_csv_path", "gs://my-bucket/incoming/transactions.csv")
    read_mode = config.get("src_csv_read_mode", "batch")
    parse_mode = config.get("src_csv_mode", "PERMISSIVE")
    corrupt_col = config.get("src_csv_corrupt_record_col", "_corrupt_record")

    options: dict[str, str] = {
        "header": str(config.get("src_csv_header", True)).lower(),
        "sep": str(config.get("src_csv_delimiter", ",")),
        "quote": str(config.get("src_csv_quote", '"')),
        "escape": str(config.get("src_csv_escape", "\\")),
        "nullValue": str(config.get("src_csv_null_value", "")),
        "multiLine": str(config.get("src_csv_multiline", False)).lower(),
        "mode": parse_mode,
        "columnNameOfCorruptRecord": corrupt_col,
    }
    if config.get("src_csv_path_glob_filter"):
        options["pathGlobFilter"] = str(config["src_csv_path_glob_filter"])
    if config.get("src_csv_recursive_file_lookup") is not None:
        options["recursiveFileLookup"] = str(config["src_csv_recursive_file_lookup"]).lower()

    if read_mode == "streaming":
        if config.get("src_csv_max_files_per_trigger"):
            options["maxFilesPerTrigger"] = str(config["src_csv_max_files_per_trigger"])
        dfs["src_csv"] = (
            spark.readStream.schema(CSV_TRANSACTIONS_SCHEMA)
            .options(**options)
            .csv(csv_uri)
        )
    elif config.get("src_csv_use_explicit_schema", True):
        dfs["src_csv"] = (
            spark.read.schema(CSV_TRANSACTIONS_SCHEMA)
            .options(**options)
            .csv(csv_uri)
        )
    else:
        options["inferSchema"] = str(config.get("src_csv_infer_schema", True)).lower()
        dfs["src_csv"] = spark.read.options(**options).csv(csv_uri)
    return dfs
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame with `spark.createDataFrame(rows, schema=CSV_TRANSACTIONS_SCHEMA)` (including `None` or populated `_corrupt_record` values when testing DLQ handling).

```python
from src.pipeline import CSV_TRANSACTIONS_SCHEMA, transform


def test_csv_source_pipeline_transform(spark):
    """Verifies CSV pipeline transformation and corrupt record handling offline."""
    rows = [
        ("tx_1", "acc_10", 50.25, None),
        ("tx_2", "acc_20", 19.99, None),
    ]
    input_df = spark.createDataFrame(rows, schema=CSV_TRANSACTIONS_SCHEMA)
    result_dfs = transform({"src_csv": input_df}, {})

    assert "src_csv" in result_dfs
    assert result_dfs["src_csv"].count() == 2
```
