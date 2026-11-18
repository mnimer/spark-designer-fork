---
name: io-xml
description: Built-in Spark 4.0 batch and Structured Streaming XML file source with rowTag, attributePrefix, and corrupt record handling.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [XmlSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-xml.html
  validated: true
---

# Built-in Spark 4.0 XML File Source (`XmlSource`)

Use these rules when generating PySpark 4.x code for an `XmlSource` node (`SRC19`).

## 1. Runtime & Connector Classpath Rules

- **Native Built-in Spark 4.0 XML Format**: Apache Spark 4.0+ includes a native `.format("xml")` (`spark.read.xml` / `spark.readStream.xml`) data source out of the box. **Zero external Maven JAR is needed in Spark 4.x** — never add legacy `com.databricks:spark-xml` coordinates to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor `path` (or legacy `uri` / `filePattern`) and `storageBackend` across `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://`:
- When reading from `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.conf.set("spark.hadoop.fs.s3a.*", ...)` or `spark.conf.set("spark.hadoop.fs.azure.account.key.*", ...)`.

## 3. Batch vs. Streaming Read Patterns & Corrupt Record Handling

- **Required `rowTag` Option**: Every XML read must specify `.option("rowTag", row_tag)` (for example `"book"`, `"record"`, or `"item"`) to identify the repeating XML element that maps to each DataFrame row.
- **Batch Mode (`readMode == "batch"`, default)**:
  - Read with `spark.read.format("xml").options(**options).load(uri)` (and apply `.schema(XML_SCHEMA)` when configured).
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit `StructType` Schema**: Structured Streaming XML file reads require an explicit `StructType` schema:
    `spark.readStream.format("xml").schema(XML_SCHEMA).options(**options).load(uri)`.
  - Pass `maxFilesPerTrigger` when configured.
- **Corrupt Record & Dead-Letter Queue (DLQ) Handling**:
  - When `mode="PERMISSIVE"` and corrupt record capture or a DLQ `error` branch is used, include `StructField("_corrupt_record", StringType(), True)` in `XML_SCHEMA` and pass `.option("mode", "PERMISSIVE")` and `.option("columnNameOfCorruptRecord", "_corrupt_record")`.

### Supported Reader Options

Map every configured `XmlSource` node property:
- `rowTag` (**required**): XML element tag treated as a DataFrame row.
- `valueTag`: Field name used for character data inside elements that also carry attributes (default `"_VALUE"`).
- `attributePrefix`: Prefix prepended to XML attribute names in the inferred or explicit schema (default `"_"`).
- `mode`: `"PERMISSIVE"` (default), `"DROPMALFORMED"`, or `"FAILFAST"`.
- `columnNameOfCorruptRecord`: Column storing malformed XML elements in `PERMISSIVE` mode (default `"_corrupt_record"`).
- `pathGlobFilter`, `recursiveFileLookup`, and `maxFilesPerTrigger` (streaming mode).

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

XML_CATALOG_SCHEMA = StructType(
    [
        StructField("_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads batch or streaming XML files using Spark 4.0 built-in .format('xml')."""
    dfs: dict[str, DataFrame] = {}
    xml_uri = config.get("src_xml_path", "gs://my-bucket/feeds/catalog.xml")
    read_mode = config.get("src_xml_read_mode", "batch")

    options: dict[str, str] = {
        "rowTag": config.get("src_xml_row_tag", "book"),
        "valueTag": config.get("src_xml_value_tag", "_VALUE"),
        "attributePrefix": config.get("src_xml_attribute_prefix", "_"),
        "mode": config.get("src_xml_mode", "PERMISSIVE"),
        "columnNameOfCorruptRecord": config.get("src_xml_corrupt_record_col", "_corrupt_record"),
    }
    if config.get("src_xml_path_glob_filter"):
        options["pathGlobFilter"] = str(config["src_xml_path_glob_filter"])
    if config.get("src_xml_recursive_file_lookup") is not None:
        options["recursiveFileLookup"] = str(config["src_xml_recursive_file_lookup"]).lower()

    if read_mode == "streaming":
        if config.get("src_xml_max_files_per_trigger"):
            options["maxFilesPerTrigger"] = str(config["src_xml_max_files_per_trigger"])
        dfs["src_xml"] = (
            spark.readStream.format("xml")
            .schema(XML_CATALOG_SCHEMA)
            .options(**options)
            .load(xml_uri)
        )
    else:
        dfs["src_xml"] = (
            spark.read.format("xml")
            .schema(XML_CATALOG_SCHEMA)
            .options(**options)
            .load(xml_uri)
        )
    return dfs
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by constructing an in-memory DataFrame with `spark.createDataFrame(rows, schema=XML_CATALOG_SCHEMA)`.

```python
from src.pipeline import XML_CATALOG_SCHEMA, transform


def test_xml_source_pipeline_transform(spark):
    """Tests XML source pipeline transformations offline with an in-memory DataFrame."""
    rows = [
        ("bk_101", "Spark 4.0 Definitive Guide", 49.95, None),
        ("bk_102", "Streaming Systems", 39.50, None),
    ]
    input_df = spark.createDataFrame(rows, schema=XML_CATALOG_SCHEMA)
    result_dfs = transform({"src_xml": input_df}, {})

    assert "src_xml" in result_dfs
    assert result_dfs["src_xml"].count() == 2
```
