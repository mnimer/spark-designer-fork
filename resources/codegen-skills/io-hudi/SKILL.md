---
name: io-hudi
description: Read and write snapshot, incremental, streaming, Copy-On-Write, and Merge-On-Read Apache Hudi tables in PySpark 4.x.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [HudiSource, HudiSink]
  preloadWhen: []
  sources:
    - prompt
    - https://hudi.apache.org/docs/quick-start-guide
    - https://hudi.apache.org/docs/write_operations/
  validated: true
---

# Apache Hudi Lakehouse Connector (`HudiSource`, `HudiSink`)

Use these rules when generating PySpark {{spark_version}} code for Apache Hudi source (`HudiSource`) and sink (`HudiSink`) nodes.

## 1. Runtime Classpath and Kryo Serializer Configuration

- Attach the Scala 2.13 Apache Hudi Spark 4.0 bundle (`org.apache.hudi:hudi-spark4.0-bundle_2.13:1.0.1`) through `--packages` or `spark.jars.packages` in `run_local.sh`, `run_dataproc_cluster.sh`, and `run_serverless_spark.sh`.
- Configure the mandatory Kryo serializer and Hudi session extensions on the `SparkSession`:
  - `spark.serializer=org.apache.spark.serializer.KryoSerializer`
  - `spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar`
  - `spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension`
  - `spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog`

## 2. Reading Tables (`HudiSource`)

Inspect `readMode` (default `"batch"`) and `queryType` (default `"snapshot"`):
- **Batch mode (`readMode == "batch"`)**: Read with `spark.read.format("hudi")`:
  - Set `.option("hoodie.datasource.query.type", query_type)`:
    - `"snapshot"`: Latest compacted and log-merged table state.
    - `"read_optimized"`: Base columnar Parquet files only on Merge-On-Read tables.
    - `"incremental"`: Changes committed between `beginInstantTime` and optional `endInstantTime`.
  - When `queryType == "incremental"`, set `.option("hoodie.datasource.read.begin.instanttime", config["hudi_begin_instant_time"])` and optional `.option("hoodie.datasource.read.end.instanttime", config["hudi_end_instant_time"])`.
- **Streaming mode (`readMode == "streaming"`)**: Read Hudi commit streams with `spark.readStream.format("hudi")`:
  - If `beginInstantTime` is set, pass `.option("hoodie.datasource.read.begin.instanttime", config["hudi_begin_instant_time"])`.
- Load from the Hudi base path via `.load(config["hudi_path"])`.

## 3. Writing Tables (`HudiSink`)

Build a common dictionary of Hudi writer options for both batch and streaming writes:
- `hoodie.table.name`: `tableName` (required logical Hudi table name).
- `hoodie.datasource.write.table.type`: `tableType` (`"COPY_ON_WRITE"` or `"MERGE_ON_READ"`, default `"COPY_ON_WRITE"`).
- `hoodie.datasource.write.operation`: `operation` (`"upsert"`, `"insert"`, `"bulk_insert"`, or `"delete"`, default `"upsert"`).
- `hoodie.datasource.write.recordkey.field`: `recordKeyField` (comma-separated primary key column names).
- `hoodie.datasource.write.precombine.field`: `precombineField` (ordering column used to pick the latest record during deduplication and upserts).
- `hoodie.datasource.write.partitionpath.field`: `partitionPathField` (optional comma-separated partition columns, empty string `""` for non-partitioned tables).
- `hoodie.datasource.write.hive_style_partitioning`: `"true"` when `hiveStylePartitioning` is enabled.
- If `writeParallelism` is configured, set `hoodie.upsert.shuffle.parallelism` and `hoodie.insert.shuffle.parallelism`.

### Batch Writes (`df.write.format("hudi")`)
- Write with `df.write.format("hudi").options(**hudi_options).mode(save_mode).save(hudi_path)` where `save_mode` defaults to `"append"` (or `"overwrite"` when replacing the table).

### Streaming Writes (`df.writeStream.format("hudi")` and `foreachBatch`)
- **Native Single-Sink Streaming (`ExecutionPlan.streamingSinkGroups` `mode == "native"`)**:
  - Write with `df.writeStream.format("hudi").options(**hudi_options)`.
  - Configure `outputMode` (default `"append"`), `checkpointLocation`, and `trigger` (`processingTime` or `availableNow=True`).
  - Start the streaming query with `.start(hudi_path)`.
- **Multi-Sink `foreachBatch` Group (`mode == "foreachBatch"`)**:
  - Inside `write_micro_batch(batch_df, batch_id)`, call `batch_df.write.format("hudi").options(**hudi_options).mode("append").save(hudi_path)`. Because Hudi `upsert` deduplicates by `recordkey` and `precombine`, micro-batch retries remain idempotent.

## 4. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def build_hudi_write_options(config: dict[str, Any]) -> dict[str, str]:
    """Builds the Hudi writer option map from runtime configuration."""
    opts: dict[str, str] = {
        "hoodie.table.name": config["hudi_sink_table_name"],
        "hoodie.datasource.write.table.type": config.get(
            "hudi_table_type", "COPY_ON_WRITE"
        ),
        "hoodie.datasource.write.operation": config.get("hudi_operation", "upsert"),
        "hoodie.datasource.write.recordkey.field": config["hudi_record_key_field"],
        "hoodie.datasource.write.partitionpath.field": config.get(
            "hudi_partition_path_field", ""
        ),
    }
    if config.get("hudi_precombine_field"):
        opts["hoodie.datasource.write.precombine.field"] = config[
            "hudi_precombine_field"
        ]
    if config.get("hudi_hive_style_partitioning"):
        opts["hoodie.datasource.write.hive_style_partitioning"] = "true"
    if config.get("hudi_write_parallelism"):
        parallelism = str(config["hudi_write_parallelism"])
        opts["hoodie.upsert.shuffle.parallelism"] = parallelism
        opts["hoodie.insert.shuffle.parallelism"] = parallelism
    for key, value in config.get("hudi_extra_options", {}).items():
        opts[key] = str(value)
    return opts


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads an Apache Hudi table in snapshot, incremental, or streaming mode."""
    dfs: dict[str, DataFrame] = {}
    read_mode = config.get("hudi_read_mode", "batch")
    hudi_path = config["hudi_source_path"]

    if read_mode == "streaming":
        reader = spark.readStream.format("hudi")
        if config.get("hudi_begin_instant_time"):
            reader = reader.option(
                "hoodie.datasource.read.begin.instanttime",
                config["hudi_begin_instant_time"],
            )
        dfs["src_hudi"] = reader.load(hudi_path)
    else:
        query_type = config.get("hudi_query_type", "snapshot")
        reader = spark.read.format("hudi").option(
            "hoodie.datasource.query.type", query_type
        )
        if query_type == "incremental":
            reader = reader.option(
                "hoodie.datasource.read.begin.instanttime",
                config["hudi_begin_instant_time"],
            )
            if config.get("hudi_end_instant_time"):
                reader = reader.option(
                    "hoodie.datasource.read.end.instanttime",
                    config["hudi_end_instant_time"],
                )
        dfs["src_hudi"] = reader.load(hudi_path)

    return dfs


def write_hudi_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a batch DataFrame to an Apache Hudi table."""
    hudi_options = build_hudi_write_options(config)
    save_mode = config.get("hudi_save_mode", "append")
    (
        df.write.format("hudi")
        .options(**hudi_options)
        .mode(save_mode)
        .save(config["hudi_sink_path"])
    )


def start_hudi_stream(df: DataFrame, config: dict[str, Any]):
    """Starts a native Structured Streaming query writing to an Apache Hudi table."""
    hudi_options = build_hudi_write_options(config)
    return (
        df.writeStream.format("hudi")
        .options(**hudi_options)
        .outputMode(config.get("hudi_output_mode", "append"))
        .option("checkpointLocation", config["hudi_checkpoint_location"])
        .trigger(processingTime=config.get("hudi_trigger_interval", "30 seconds"))
        .start(config["hudi_sink_path"])
    )
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame matching the Hudi table schema without reading or writing storage:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.pipeline import transform


def test_hudi_pipeline_transform(spark: SparkSession) -> None:
    """Verifies transformation logic using in-memory Apache Hudi rows."""
    schema = StructType(
        [
            StructField("_hoodie_commit_time", StringType(), True),
            StructField("ride_id", StringType(), False),
            StructField("fare", DoubleType(), False),
        ]
    )
    input_df = spark.createDataFrame(
        [("20260101120000", "ride-1", 18.50), ("20260101120500", "ride-2", 34.00)],
        schema=schema,
    )
    result_dfs = transform({"src_hudi": input_df}, {})
    assert "src_hudi" in result_dfs
    assert result_dfs["src_hudi"].count() == 2
```
