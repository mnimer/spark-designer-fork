---
name: io-delta
description: Read and write batch, time-travel, Change Data Feed, streaming, and DeltaTable merge records in PySpark 4.x.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [DeltaSource, DeltaSink]
  preloadWhen: []
  sources:
    - prompt
    - https://docs.delta.io/latest/delta-batch.html
    - https://docs.delta.io/latest/delta-update.html
    - https://docs.delta.io/latest/delta-streaming.html
  validated: true
---

# Delta Lake Connector (`DeltaSource`, `DeltaSink`)

Use these rules when generating PySpark {{spark_version}} code for Delta Lake source (`DeltaSource`) and sink (`DeltaSink`) nodes.

## 1. Runtime Classpath and Session Extensions

- Attach the Scala 2.13 Delta Lake Spark 4.0 package (`io.delta:delta-spark_2.13:4.0.0`) through `--packages` or `spark.jars.packages` in `run_local.sh`, `run_dataproc_cluster.sh`, and `run_serverless_spark.sh`.
- Configure the Delta Lake SQL extension and catalog properties on the `SparkSession`:
  - `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`
  - `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
- Include `delta-spark>=4.0.0` in `requirements.txt` and `Dockerfile`.

## 2. Reading Tables (`DeltaSource`)

Inspect `readMode` (default `"batch"`):
- **Batch mode (`readMode == "batch"`)**: Read with `spark.read.format("delta")`:
  - If `versionAsOf` is set, chain `.option("versionAsOf", str(config["delta_version_as_of"]))`.
  - If `timestampAsOf` is set, chain `.option("timestampAsOf", config["delta_timestamp_as_of"])`.
- **Streaming mode (`readMode == "streaming"`)**: Read with `spark.readStream.format("delta")`:
  - If `maxFilesPerTrigger` is set, chain `.option("maxFilesPerTrigger", str(config["delta_max_files_per_trigger"]))`.
  - If `maxBytesPerTrigger` is set, chain `.option("maxBytesPerTrigger", str(config["delta_max_bytes_per_trigger"]))`.
  - If `ignoreChanges` is set, chain `.option("ignoreChanges", str(config["delta_ignore_changes"]).lower())`.
  - If `ignoreDeletes` is set, chain `.option("ignoreDeletes", str(config["delta_ignore_deletes"]).lower())`.
- **Change Data Feed (`readChangeFeed == True`)**: Supported in both batch and streaming modes:
  - Set `.option("readChangeFeed", "true")`.
  - Pass `.option("startingVersion", str(config["delta_starting_version"]))` or `.option("startingTimestamp", config["delta_starting_timestamp"])`.
  - When `readChangeFeed=true`, Delta Lake appends `_change_type`, `_commit_version`, and `_commit_timestamp` columns to the DataFrame.
- **Path vs Table loading**: If `sourceMode == "table"`, load via `.table(config["delta_table_name"])`. Otherwise load from storage via `.load(config["delta_path"])`.

## 3. Writing Tables (`DeltaSink`)

Inspect `writeMode` (`"append"`, `"overwrite"`, or `"merge"`, default `"append"`) and `targetMode` (`"path"` or `"table"`, default `"path"`):

### Batch Writes (`append`, `overwrite`, and `DeltaTable.merge`)
- **`append` and `overwrite` modes**:
  - Build `writer = df.write.format("delta").mode(write_mode)`.
  - If `write_mode == "overwrite"` and `replaceWhere` is set, chain `.option("replaceWhere", config["delta_replace_where"])` for selective predicate overwrite.
  - If `mergeSchema` is true, chain `.option("mergeSchema", "true")`.
  - If `write_mode == "overwrite"` and `overwriteSchema` is true, chain `.option("overwriteSchema", "true")`.
  - If `optimizeWrite` is true, chain `.option("optimizeWrite", "true")`.
  - If `partitionBy` lists partition columns, chain `.partitionBy(*partition_cols)`.
  - When `targetMode == "table"`, call `writer.saveAsTable(config["delta_sink_table"])`. Otherwise call `writer.save(config["delta_sink_path"])`.
- **`merge` upsert mode (`from delta.tables import DeltaTable`)**:
  - Resolve the destination `DeltaTable` with `DeltaTable.forName(spark, table_name)` when `targetMode == "table"` or `DeltaTable.forPath(spark, path)` when `targetMode == "path"`.
  - Build the `mergeCondition` from the explicit SQL predicate or from `mergeKeys` (`target.<col> = source.<col>`).
  - If `mergeSchema` is true, set `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")` before executing the merge.
  - Execute `delta_table.alias("target").merge(df.alias("source"), merge_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`.

### CDC & SCD Type 1 / Type 2 Merges (`MergeCdcScd`)
When an upstream `MergeCdcScd` step feeds `DeltaSink` (`writeMode == "merge"`), compute `business_cols = [c for c in df.columns if c != action_col and not c.startswith("_merge_")]` so internal `_scd_action` and `_merge_*` staging columns never enter the target Delta table:
- **SCD Type 1 (`scdType == "scd1"`)**:
  - Join on `target.<k> = source.<k>` and chain conditional `DeltaMergeBuilder` clauses:
    ```python
    on_cond = " AND ".join(f"target.{k} = source.{k}" for k in keys)
    update_map = {c: f"source.{c}" for c in business_cols if c not in set(keys)}
    insert_map = {c: f"source.{c}" for c in business_cols}
    (
        delta_table.alias("target")
        .merge(df.alias("source"), on_cond)
        .whenMatchedDelete(condition=f"source.{action_col} = 'DELETE'")
        .whenMatchedUpdate(condition=f"source.{action_col} = 'UPDATE'", set=update_map)
        .whenNotMatchedInsert(condition=f"source.{action_col} = 'INSERT'", values=insert_map)
        .execute()
    )
    ```
- **SCD Type 2 (`scdType == "scd2"`)**:
  - Match active target rows against nullable `source._merge_<k>` columns so `'EXPIRE'` and `'DELETE'` rows close the active target version while `'INSERT'` rows (`_merge_<k> IS NULL`) insert new active versions in a single atomic pass:
    ```python
    on_cond = (
        " AND ".join(f"target.{k} = source._merge_{k}" for k in keys)
        + f" AND target.{is_current_col} = true"
    )
    insert_map = {c: f"source.{c}" for c in business_cols}
    (
        delta_table.alias("target")
        .merge(df.alias("source"), on_cond)
        .whenMatchedUpdate(
            condition=f"source.{action_col} IN ('EXPIRE', 'DELETE')",
            set={
                is_current_col: "false",
                eff_to_col: f"source.{eff_from_col}",
            },
        )
        .whenNotMatchedInsert(
            condition=f"source.{action_col} = 'INSERT'",
            values=insert_map,
        )
        .execute()
    )
    ```

### Streaming Writes (`native` and Idempotent `foreachBatch`)
- **Native Single-Sink Streaming (`ExecutionPlan.streamingSinkGroups` `mode == "native"` and `writeMode != "merge"`)**:
  - Build `writer = df.writeStream.format("delta").outputMode(output_mode).option("checkpointLocation", checkpoint_dir)`.
  - If `mergeSchema` is true, chain `.option("mergeSchema", "true")`.
  - If `partitionBy` lists partition columns, chain `.partitionBy(*partition_cols)`.
  - Configure the trigger (`processingTime` or `availableNow=True`).
  - When `targetMode == "table"`, start with `.toTable(table_name)`. Otherwise start with `.start(path)`.
- **Grouped `foreachBatch` or Streaming Upsert (`mode == "foreachBatch"` or `writeMode == "merge"`)**:
  - When writing `append` or `overwrite` inside `def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:`, always attach `.option("txnVersion", str(batch_id))` and `.option("txnAppId", txn_app_id)` so Delta Lake deduplicates retried micro-batches with exactly-once guarantees.
  - When `writeMode == "merge"`, invoke `DeltaTable.forPath(batch_df.sparkSession, path)` (or `DeltaTable.forName(...)`) inside `write_micro_batch` to upsert `batch_df`.

## 4. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads a Delta Lake table in batch, time-travel, or streaming mode."""
    dfs: dict[str, DataFrame] = {}
    read_mode = config.get("delta_read_mode", "batch")
    reader = (
        spark.readStream.format("delta")
        if read_mode == "streaming"
        else spark.read.format("delta")
    )

    if config.get("delta_read_change_feed"):
        reader = reader.option("readChangeFeed", "true")
        if config.get("delta_starting_version") is not None:
            reader = reader.option("startingVersion", str(config["delta_starting_version"]))
        elif config.get("delta_starting_timestamp"):
            reader = reader.option("startingTimestamp", config["delta_starting_timestamp"])

    if read_mode == "batch":
        if config.get("delta_version_as_of") is not None:
            reader = reader.option("versionAsOf", str(config["delta_version_as_of"]))
        elif config.get("delta_timestamp_as_of"):
            reader = reader.option("timestampAsOf", config["delta_timestamp_as_of"])
    else:
        if config.get("delta_max_files_per_trigger"):
            reader = reader.option(
                "maxFilesPerTrigger", str(config["delta_max_files_per_trigger"])
            )
        if config.get("delta_ignore_changes") is not None:
            reader = reader.option(
                "ignoreChanges", str(config["delta_ignore_changes"]).lower()
            )
        if config.get("delta_ignore_deletes") is not None:
            reader = reader.option(
                "ignoreDeletes", str(config["delta_ignore_deletes"]).lower()
            )

    if config.get("delta_source_mode") == "table" and config.get("delta_table_name"):
        dfs["src_delta"] = reader.table(config["delta_table_name"])
    else:
        dfs["src_delta"] = reader.load(config["delta_path"])

    return dfs


def write_delta_batch(
    df: DataFrame,
    config: dict[str, Any],
    batch_id: int | None = None,
) -> None:
    """Writes or merges a DataFrame into a Delta Lake path or catalog table."""
    write_mode = config.get("delta_write_mode", "append")
    target_mode = config.get("delta_target_mode", "path")
    spark = df.sparkSession

    if write_mode == "merge":
        if config.get("delta_merge_schema"):
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        delta_table = (
            DeltaTable.forName(spark, config["delta_sink_table"])
            if target_mode == "table"
            else DeltaTable.forPath(spark, config["delta_sink_path"])
        )
        merge_cond = config.get("delta_merge_condition", "").strip()
        if not merge_cond:
            keys = [
                k.strip()
                for k in config.get("delta_merge_keys", "").split(",")
                if k.strip()
            ]
            merge_cond = " AND ".join(f"target.{k} = source.{k}" for k in keys)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return

    writer = df.write.format("delta").mode(write_mode)
    if batch_id is not None:
        txn_app_id = config.get("delta_txn_app_id") or config.get("app_name", "spark_pipeline")
        writer = writer.option("txnVersion", str(batch_id)).option("txnAppId", txn_app_id)
    if write_mode == "overwrite" and config.get("delta_replace_where"):
        writer = writer.option("replaceWhere", config["delta_replace_where"])
    if config.get("delta_merge_schema"):
        writer = writer.option("mergeSchema", "true")
    if write_mode == "overwrite" and config.get("delta_overwrite_schema"):
        writer = writer.option("overwriteSchema", "true")
    if config.get("delta_optimize_write"):
        writer = writer.option("optimizeWrite", "true")

    partition_cols = [
        c.strip()
        for c in config.get("delta_partition_by", "").split(",")
        if c.strip()
    ]
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    if target_mode == "table":
        writer.saveAsTable(config["delta_sink_table"])
    else:
        writer.save(config["delta_sink_path"])


def start_delta_stream(df: DataFrame, config: dict[str, Any]):
    """Starts a native Structured Streaming query writing to Delta Lake."""
    target_mode = config.get("delta_target_mode", "path")
    writer = (
        df.writeStream.format("delta")
        .outputMode(config.get("delta_output_mode", "append"))
        .option("checkpointLocation", config["delta_checkpoint_location"])
    )
    if config.get("delta_merge_schema"):
        writer = writer.option("mergeSchema", "true")
    partition_cols = [
        c.strip()
        for c in config.get("delta_partition_by", "").split(",")
        if c.strip()
    ]
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer = writer.trigger(
        processingTime=config.get("delta_trigger_interval", "10 seconds")
    )
    if target_mode == "table":
        return writer.toTable(config["delta_sink_table"])
    return writer.start(config["delta_sink_path"])
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame matching the Delta table schema without requiring a Delta table on disk:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from src.pipeline import transform


def test_delta_pipeline_transform(spark: SparkSession) -> None:
    """Verifies transformation logic using in-memory Delta Lake rows."""
    schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("balance", DoubleType(), False),
            StructField("_change_type", StringType(), True),
            StructField("_commit_version", LongType(), True),
        ]
    )
    input_df = spark.createDataFrame(
        [("acc-1", 250.0, "insert", 1), ("acc-2", 500.0, "update_postimage", 2)],
        schema=schema,
    )
    result_dfs = transform({"src_delta": input_df}, {})
    assert "src_delta" in result_dfs
    assert result_dfs["src_delta"].count() == 2
```
