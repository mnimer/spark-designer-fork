---
name: io-xml
description: Built-in Spark 4.0 batch and Structured Streaming XML file source and sink with rootTag, rowTag, attributePrefix, valueTag, and foreachBatch streaming writes.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [XmlSource, XmlSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-xml.html
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
# Built-in Spark 4.0 XML File Source & Sink (`XmlSource`, `XmlSink`)

Use these rules when generating PySpark 4.x code for `XmlSource` (`SRC19`) or `XmlSink` (`SNK17`) nodes.

## 1. Runtime & Connector Classpath Rules

- **Native Built-in Spark 4.0 XML Format**: Apache Spark 4.0+ includes a native `.format("xml")` (`spark.read.xml`, `spark.readStream.xml`, and `df.write.format("xml")`) data source out of the box. **Zero external Maven JAR is needed in Spark 4.x** — never add legacy `com.databricks:spark-xml` coordinates to `spark.jars.packages`.
- **Managed Spark Pre-installed Storage Connectors**: `gcs-connector`, `hadoop-aws`, and `hadoop-azure` are pre-installed on Google Cloud Serverless for Apache Spark and Dataproc Clusters. Never add them to `spark.jars.packages` on Managed Spark targets.
- **Local Runner Storage Connectors**: Attach `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.25`, `org.apache.hadoop:hadoop-aws:3.4.1`, or `org.apache.hadoop:hadoop-azure:3.4.1` via `--packages` only in `run_local.sh` when reading or writing cloud URIs locally.
- **Spark Connect Safe Configuration**: Configure cloud storage credentials exclusively through `spark.conf.set("spark.hadoop.fs.*", ...)` so code runs cleanly on standard Spark and Spark Connect.

## 2. Supported `StorageBackendFields` URI Schemes & Cloud Authentication

Honor `path` (or legacy `uri` / `filePattern` / `to`) and `storageBackend` across `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://`:
- When reading or writing `s3a://`, `abfss://`, or `wasbs://` with Secret Manager credentials, resolve the secret once on the driver via `get_secret()` (or `access_secret_version()`) and configure `spark.conf.set("spark.hadoop.fs.s3a.*", ...)` or `spark.conf.set("spark.hadoop.fs.azure.account.key.*", ...)`.

## 3. `XmlSource` — Batch vs. Streaming Read Patterns & Corrupt Record Handling

- **Required `rowTag` Option**: Every XML read must specify `.option("rowTag", row_tag)` (for example `"book"`, `"record"`, or `"item"`) to identify the repeating XML element that maps to each DataFrame row.
- **Batch Mode (`readMode == "batch"`, default)**:
  - Read with `spark.read.format("xml").options(**options).load(uri)` (and apply `.schema(XML_SCHEMA)` when configured).
- **Streaming Mode (`readMode == "streaming"`)**:
  - **Mandatory Explicit `StructType` Schema**: Structured Streaming XML file reads require an explicit `StructType` schema:
    `spark.readStream.format("xml").schema(XML_SCHEMA).options(**options).load(uri)`.
  - If `maxFilesPerTrigger` is configured, pass it to bound micro-batch size.
- **Corrupt Record & Dead-Letter Queue (DLQ) Handling**:
  - When `mode="PERMISSIVE"` and corrupt record capture or a DLQ `error` branch is used, include `StructField("_corrupt_record", StringType(), True)` in `XML_SCHEMA` and pass `.option("mode", "PERMISSIVE")` and `.option("columnNameOfCorruptRecord", "_corrupt_record")`.

### Supported Reader Options (`XmlSource`)

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

## 4. `XmlSink` — Batch & `foreachBatch` Streaming Write Patterns (`write_sinks`)

`XmlSink` (`SNK17`) uses Spark 4.0's built-in `.write.format("xml")` writer (no `com.databricks:spark-xml` dependency) and has `streamingSinkMode: "foreachBatch"`.

- **Batch Mode**:
  - Write with `df.write.format("xml").mode(save_mode).options(**write_options).save(output_uri)`.
  - Supported `saveMode` values: `"append"`, `"overwrite"`, `"errorifexists"`, and `"ignore"`.
  - If `partitionBy` columns are configured, chain `.partitionBy(*partition_cols)` before `.save(output_uri)`.
- **Streaming Mode (`foreachBatch`)**:
  - When a streaming branch feeds `XmlSink` (`ExecutionPlan.streamingSinkGroups` `mode == "foreachBatch"`), define `def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:`.
  - Write each micro-batch idempotently to a deterministic batch subdirectory `f"{output_uri.rstrip('/')}/batch_{batch_id}"` with `.mode("overwrite")` so retried micro-batches do not duplicate XML part-files. If multiple sinks share the `foreachBatch` callback, call `batch_df.cache()` before writing and `batch_df.unpersist()` inside a `finally` block.

### Supported Writer Options (`XmlSink`)

- `rootTag`: Root XML element enclosing all rows in each output XML part-file (default `"ROWS"`).
- `rowTag`: XML element enclosing each individual DataFrame row (default `"ROW"`).
- `declaration`: XML declaration header string without `<?xml` and `?>` wrappers (for example `'version="1.0" encoding="UTF-8" standalone="yes"'`, or `""` to suppress the header).
- `nullValue`: String representation written for SQL `NULL` values.
- `attributePrefix`: Column prefix serialized as XML element attributes (default `"_"`).
- `valueTag`: Struct field name holding character content inside elements with attributes (default `"_VALUE"`).
- `compression`: `"none"` (default), `"gzip"`, `"zstd"`, `"bzip2"`, `"lz4"`, or `"snappy"`.
- `partitionBy`: Comma-separated list or array of column names for directory partitioning.
- `maxRecordsPerFile`: Positive integer capping rows written per output XML part-file.

```python
def build_xml_write_options(config: dict[str, Any]) -> dict[str, str]:
    """Builds Spark 4.0 built-in XML writer options from pipeline config."""
    write_options: dict[str, str] = {
        "rootTag": config.get("snk_xml_root_tag", "ROWS"),
        "rowTag": config.get("snk_xml_row_tag", "ROW"),
        "attributePrefix": config.get("snk_xml_attribute_prefix", "_"),
        "valueTag": config.get("snk_xml_value_tag", "_VALUE"),
        "compression": config.get("snk_xml_compression", "none"),
    }
    if config.get("snk_xml_declaration") is not None:
        write_options["declaration"] = str(config["snk_xml_declaration"])
    if config.get("snk_xml_null_value") is not None:
        write_options["nullValue"] = str(config["snk_xml_null_value"])
    if config.get("snk_xml_max_records_per_file"):
        write_options["maxRecordsPerFile"] = str(config["snk_xml_max_records_per_file"])
    return write_options


def write_xml_sink(
    df: DataFrame,
    config: dict[str, Any],
    is_streaming: bool = False,
) -> None:
    """Writes a batch or streaming DataFrame using Spark 4.0 built-in XML writer."""
    output_uri = config.get("snk_xml_path", "gs://my-bucket/feeds/catalog_out/")
    save_mode = config.get("snk_xml_save_mode", "append")
    raw_partitions = config.get("snk_xml_partition_by", "")
    partition_cols = [c.strip() for c in raw_partitions.split(",") if c.strip()]
    write_options = build_xml_write_options(config)

    if is_streaming:
        checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
        checkpoint_uri = config.get(
            "snk_xml_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/snk_xml",
        )

        def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
            batch_path = f"{output_uri.rstrip('/')}/batch_{batch_id}"
            writer = batch_df.write.format("xml").mode("overwrite").options(**write_options)
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(batch_path)

        stream_writer = (
            df.writeStream.foreachBatch(write_micro_batch)
            .outputMode(config.get("snk_xml_output_mode", "append"))
            .option("checkpointLocation", checkpoint_uri)
        )
        trigger_type = config.get("snk_xml_trigger_type", "processingTime")
        if trigger_type == "availableNow":
            stream_writer = stream_writer.trigger(availableNow=True)
        else:
            interval = config.get("snk_xml_trigger_interval", "10 seconds")
            stream_writer = stream_writer.trigger(processingTime=interval)
        stream_writer.start()
    else:
        batch_writer = df.write.format("xml").mode(save_mode).options(**write_options)
        if partition_cols:
            batch_writer = batch_writer.partitionBy(*partition_cols)
        batch_writer.save(output_uri)
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by constructing an in-memory DataFrame with `spark.createDataFrame(rows, schema=XML_CATALOG_SCHEMA)`. On Spark 4.x, built-in `.format("xml")` writes and reads directly against pytest's `tmp_path` directory with zero external JARs.

```python
from src.pipeline import XML_CATALOG_SCHEMA, transform


def test_xml_pipeline_transform_and_local_write(spark, tmp_path):
    """Tests XML pipeline transformations and built-in Spark 4.0 XML round-trip."""
    rows = [
        ("bk_101", "Spark 4.0 Definitive Guide", 49.95, None),
        ("bk_102", "Streaming Systems", 39.50, None),
    ]
    input_df = spark.createDataFrame(rows, schema=XML_CATALOG_SCHEMA)
    result_dfs = transform({"src_xml": input_df}, {})

    out_dir = str(tmp_path / "xml_out")
    (
        result_dfs["src_xml"]
        .drop("_corrupt_record")
        .write.format("xml")
        .mode("overwrite")
        .option("rootTag", "catalog")
        .option("rowTag", "book")
        .save(out_dir)
    )
    reloaded_df = spark.read.format("xml").option("rowTag", "book").load(out_dir)
    assert reloaded_df.count() == 2
```
