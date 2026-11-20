---
name: io-hive
description: Read and write Apache Hive Metastore and Google Cloud Dataproc Metastore tables in PySpark 4.x.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [HiveSource, HiveSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
# Apache Hive & Dataproc Metastore Connector (`HiveSource`, `HiveSink`)

Use these rules when generating PySpark {{spark_version}} code for Apache Hive or Google Cloud Dataproc Metastore source (`HiveSource`, `HCatalogIO`) and sink (`HiveSink`, `SINK:HCatalogIO`) nodes.

## 1. Session Initialization and Metastore Configuration

- Call `.enableHiveSupport()` on `SparkSession.builder` inside `create_spark_session(app_name, config)` (when not using Spark Connect) so Catalyst binds to the Hive Metastore catalog.
- When `metastoreUri` (`spark.hadoop.hive.metastore.uris`, such as `"thrift://metastore-host:9083"`) is configured, pass `.config("spark.hadoop.hive.metastore.uris", config["hive_metastore_uri"])` during session initialization or via `--conf spark.hadoop.hive.metastore.uris=...` in runner scripts.
- On Dataproc Cluster and Serverless Spark, attach the Dataproc Metastore service via `--metastore-service` when configured in deployment settings.

## 2. Reading Tables (`HiveSource`)

In `read_sources(spark, config)`, read the Hive table using `spark.table(f"{database}.{table}")`:
- `database`: Defaults to `"default"` when omitted.
- `table`: Required destination or source table name in the metastore.
- `filterPredicate`: When a partition or row filter expression is configured (such as `"dt >= '2026-01-01'"`), chain `.filter(config["hive_filter_predicate"])` immediately after `spark.table(...)` so Catalyst pushes partition pruning down to the Hive Metastore.

## 3. Writing Tables (`HiveSink`)

Build the qualified table name `table_id = f"{database}.{table}"` (where `database` defaults to `"default"`).

### Batch Writes (`saveAsTable` and `insertInto`)
Inspect `writeMethod` (`"saveAsTable"` or `"insertInto"`, default `"saveAsTable"`) and `saveMode` (`"append"`, `"overwrite"`, `"errorifexists"`, or `"ignore"`, default `"append"`):
- **Dynamic Partition Overwrite**: If `saveMode == "overwrite"` and `dynamicPartitionOverwrite` is true, set `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")` (and `hive.exec.dynamic.partition.mode=nonstrict` for Hive format tables) so Spark replaces only partitions present in `df` rather than dropping the entire table.
- **`writeMethod == "saveAsTable"`**:
  - Start with `writer = df.write.mode(save_mode).format(file_format)` where `file_format` defaults to `"parquet"` (`"parquet"`, `"orc"`, `"hive"`, or `"avro"`).
  - If `externalPath` is set, chain `.option("path", config["hive_external_path"])` to register an external Hive table at that storage URI.
  - If `partitionCols` lists partition columns, chain `.partitionBy(*partition_cols)`.
  - If `numBuckets` (`> 0`) and `bucketCols` are configured, chain `.bucketBy(int(num_buckets), bucket_cols[0], *bucket_cols[1:])` and optional `.sortBy(sort_cols[0], *sort_cols[1:])`.
  - Execute `writer.saveAsTable(table_id)`.
- **`writeMethod == "insertInto"`**:
  - `DataFrameWriter.insertInto(table_id)` matches columns by position rather than column name. Read the target column order via `target_cols = spark.table(table_id).columns` and project `df.select(*target_cols)` before calling `.write.mode(save_mode).insertInto(table_id)`.

### Streaming Writes (`foreachBatch` Mode)
- Hive catalog tables have `streamingSinkMode: "foreachBatch"` in `ExecutionPlan.streamingSinkGroups`.
- Inside `def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:`, append each micro-batch to the Hive table using `write_hive_batch(batch_df, config)` with `mode("append")` (`saveAsTable` or `insertInto`), configured with a dedicated `checkpointLocation`.

## 4. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def create_spark_session(app_name: str, config: dict[str, Any]) -> SparkSession:
    """Creates a SparkSession with Hive Metastore support enabled."""
    builder = SparkSession.builder.appName(app_name)
    if config.get("hive_metastore_uri"):
        builder = builder.config(
            "spark.hadoop.hive.metastore.uris", config["hive_metastore_uri"]
        )
    for key, value in config.get("spark_properties", {}).items():
        builder = builder.config(key, value)
    return builder.enableHiveSupport().getOrCreate()


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads a Hive Metastore table with optional partition filter pushdown."""
    dfs: dict[str, DataFrame] = {}
    database = config.get("hive_source_database", "default")
    table = config["hive_source_table"]
    df = spark.table(f"{database}.{table}")
    if config.get("hive_filter_predicate"):
        df = df.filter(config["hive_filter_predicate"])
    dfs["src_hive"] = df
    return dfs


def write_hive_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a batch DataFrame to a Hive or Dataproc Metastore table."""
    spark = df.sparkSession
    database = config.get("hive_sink_database", "default")
    table_id = f"{database}.{config['hive_sink_table']}"
    write_method = config.get("hive_write_method", "saveAsTable")
    save_mode = config.get("hive_save_mode", "append")

    if save_mode == "overwrite" and config.get("hive_dynamic_partition_overwrite"):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if write_method == "insertInto":
        target_cols = spark.table(table_id).columns
        df.select(*target_cols).write.mode(save_mode).insertInto(table_id)
        return

    file_format = config.get("hive_file_format", "parquet")
    writer = df.write.mode(save_mode).format(file_format)
    if config.get("hive_external_path"):
        writer = writer.option("path", config["hive_external_path"])

    partition_cols = [
        c.strip()
        for c in config.get("hive_partition_cols", "").split(",")
        if c.strip()
    ]
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    num_buckets = int(config.get("hive_num_buckets") or 0)
    bucket_cols = [
        c.strip()
        for c in config.get("hive_bucket_cols", "").split(",")
        if c.strip()
    ]
    if num_buckets > 0 and bucket_cols:
        writer = writer.bucketBy(num_buckets, bucket_cols[0], *bucket_cols[1:])
        sort_cols = [
            c.strip()
            for c in config.get("hive_sort_cols", "").split(",")
            if c.strip()
        ]
        if sort_cols:
            writer = writer.sortBy(sort_cols[0], *sort_cols[1:])

    writer.saveAsTable(table_id)


def start_hive_stream(df: DataFrame, config: dict[str, Any]):
    """Appends streaming micro-batches to a Hive Metastore table via foreachBatch."""
    stream_cfg = dict(config, hive_save_mode="append")

    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        write_hive_batch(batch_df, stream_cfg)

    return (
        df.writeStream.foreachBatch(write_micro_batch)
        .outputMode(config.get("hive_output_mode", "append"))
        .option("checkpointLocation", config["hive_checkpoint_location"])
        .trigger(processingTime=config.get("hive_trigger_interval", "30 seconds"))
        .start()
    )
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame matching the Hive table schema without requiring a running Hive Metastore:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.pipeline import transform


def test_hive_pipeline_transform(spark: SparkSession) -> None:
    """Verifies transformation logic using in-memory Hive table rows."""
    schema = StructType(
        [
            StructField("item_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("dt", StringType(), False),
        ]
    )
    input_df = spark.createDataFrame(
        [("sku-10", 5, "2026-01-01"), ("sku-20", 12, "2026-01-01")],
        schema=schema,
    )
    result_dfs = transform({"src_hive": input_df}, {})
    assert "src_hive" in result_dfs
    assert result_dfs["src_hive"].count() == 2
```
