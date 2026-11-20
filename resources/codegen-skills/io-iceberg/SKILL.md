---
name: io-iceberg
description: Read and write batch or streaming Apache Iceberg tables in PySpark 4.x across BigLake REST, REST, Hive, Hadoop, AWS Glue, Project Nessie, and BigQuery Metastore catalogs.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [IcebergSource, IcebergSink]
  preloadWhen: []
  sources:
    - prompt
    - https://iceberg.apache.org/docs/latest/spark-queries/
    - https://iceberg.apache.org/docs/latest/spark-writes/
    - https://iceberg.apache.org/docs/latest/spark-structured-streaming/
    - https://iceberg.apache.org/docs/latest/aws/
    - https://projectnessie.org/iceberg/spark/
    - https://cloud.google.com/biglake/docs/apache-iceberg-in-biglake
  validated: true
---

# Apache Iceberg Lakehouse Connector (`IcebergSource`, `IcebergSink`)

Use these rules when generating PySpark {{spark_version}} code for Apache Iceberg source (`IcebergSource`, `IcebergIO`) and sink (`IcebergSink`, `SINK:IcebergIO`) nodes.

## 1. Runtime Classpath Rules (`versionMatrix.ts`)

- **Google Cloud Managed Spark (`run_serverless_spark.sh` and `run_dataproc_cluster.sh`)**: `iceberg-spark-runtime` (with `GCSFileIO` on Serverless Spark) is pre-installed on Serverless Spark 4.0 and Dataproc Cluster runtimes. Never pass `org.apache.iceberg:iceberg-spark-runtime*` to `--packages` or `spark.jars.packages` on Serverless Spark or Dataproc Cluster because duplicate JARs cause classpath conflicts.
- **Local Spark (`run_local.sh` only)**: If running locally, pass `--packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1` along with the required cloud storage connector coordinate.
- **Spark Session Extensions**: Configure `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` on the session so `MERGE INTO` and Iceberg DDL commands parse cleanly.

## 2. Catalog Configuration (`BigLake`, `REST`, `Hive`, `Hadoop`, `Glue`, `Nessie`, `BigQuery`)

Configure the target catalog `<catalog>` (`catalogName`, default `"lakehouse"` or `"biglake"`) on the `SparkSession` according to `catalogType`:
- **BigLake Metastore / BigLake REST Catalog (`catalogType == "biglake"`)**:
  - Set `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`.
  - When targeting the BigLake Iceberg REST endpoint, set `spark.sql.catalog.<catalog>.type=rest`, `spark.sql.catalog.<catalog>.uri=https://biglake.googleapis.com/iceberg/v1beta/restcatalog`, `spark.sql.catalog.<catalog>.warehouse=gs://<bucket>/<warehouse>`, and `spark.sql.catalog.<catalog>.rest.auth.type=google`.
  - When targeting BigLake Metastore directly, set `spark.sql.catalog.<catalog>.catalog-impl=org.apache.iceberg.gcp.biglake.BigLakeCatalog`, `spark.sql.catalog.<catalog>.gcp_project=<project_id>`, `spark.sql.catalog.<catalog>.gcp_location=<region>`, `spark.sql.catalog.<catalog>.blms_catalog=<catalog_name>`, and `spark.sql.catalog.<catalog>.warehouse=gs://<bucket>/<warehouse>`.
- **REST Catalog (`catalogType == "rest"`)**:
  - Set `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`, `spark.sql.catalog.<catalog>.type=rest`, and `spark.sql.catalog.<catalog>.uri=<rest_uri>`.
  - If `authMode == "secretManager"` and `secretName` is set, resolve the bearer token or credential once on the driver via `access_secret_version(...)` and pass `spark.sql.catalog.<catalog>.token`.
- **Hive Metastore Catalog (`catalogType == "hive"`)**:
  - Set `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`, `spark.sql.catalog.<catalog>.type=hive`, and `spark.sql.catalog.<catalog>.uri=thrift://<metastore-host>:9083`.
- **Hadoop File-System Catalog (`catalogType == "hadoop"`)**:
  - Set `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`, `spark.sql.catalog.<catalog>.type=hadoop`, and `spark.sql.catalog.<catalog>.warehouse=<warehouse_uri>`.
- **AWS Glue Data Catalog (`catalogType == "glue"`)**:
  - Set `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`, `spark.sql.catalog.<catalog>.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog`, `spark.sql.catalog.<catalog>.io-impl=org.apache.iceberg.aws.s3.S3FileIO`, and `spark.sql.catalog.<catalog>.warehouse=<s3_or_s3a_warehouse_uri>`.
- **Project Nessie Catalog (`catalogType == "nessie"`)**:
  - Set `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`, `spark.sql.catalog.<catalog>.catalog-impl=org.apache.iceberg.nessie.NessieCatalog`, `spark.sql.catalog.<catalog>.uri=<nessie_uri>` (for example `http://nessie:19120/api/v1`), `spark.sql.catalog.<catalog>.ref=main`, and `spark.sql.catalog.<catalog>.warehouse=<warehouse_uri>`.
  - If `authMode == "secretManager"` and `secretName` is set, set `spark.sql.catalog.<catalog>.authentication.type=BEARER` and resolve `spark.sql.catalog.<catalog>.authentication.token` once on the driver via Secret Manager.
- **BigQuery Metastore Catalog (`catalogType == "bigquery"`)**:
  - Set `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`, `spark.sql.catalog.<catalog>.catalog-impl=org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog`, `spark.sql.catalog.<catalog>.gcp_project=<project_id>`, `spark.sql.catalog.<catalog>.gcp_location=<region>`, `spark.sql.catalog.<catalog>.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO`, and `spark.sql.catalog.<catalog>.warehouse=gs://<bucket>/<warehouse>`.

## 3. Reading Tables (`IcebergSource`)

Inspect `readMode` (default `"batch"`):
- **Batch mode (`readMode == "batch"`)**: Read with `spark.read.format("iceberg")`:
  - If `snapshotId` is set, chain `.option("snapshot-id", str(config["iceberg_snapshot_id"]))`.
  - If `asOfTimestamp` is set, chain `.option("as-of-timestamp", str(config["iceberg_as_of_timestamp"]))`.
  - Load the qualified table identifier `f"{catalog}.{namespace}.{table}"` with `.load(table_id)`.
- **Streaming mode (`readMode == "streaming"`)**: Read incremental appends with `spark.readStream.format("iceberg")`:
  - If `streamingFromTimestamp` is set, chain `.option("streaming-from-timestamp", str(config["iceberg_streaming_from_timestamp"]))`.
  - If `streamingSkipDeleteSnapshots` is set, chain `.option("streaming-skip-delete-snapshots", str(skip_deletes).lower())`.
  - If `streamingSkipOverwriteSnapshots` is set, chain `.option("streaming-skip-overwrite-snapshots", str(skip_overwrites).lower())`.
  - Load the table with `.load(table_id)`.

## 4. Writing Tables (`IcebergSink`)

Build the three-part table identifier `<catalog>.<namespace>.<table>` (`table_id`):

### Batch Writes (`DataFrameWriterV2` and `MERGE INTO`)
Use Spark's `DataFrameWriterV2` API (`df.writeTo(table_id).using("iceberg")`) according to `writeOperation` (default `"append"`):
- **Fanout Option**: If `fanoutEnabled` is true, chain `.option("fanout-enabled", "true")` before the terminal action so writers handle unsorted partitions without sorting overhead.
- **Table Properties**: When `tableProperties` specifies `key=value` lines, apply `.tableProperty(key, value)` on `createOrReplace` writes and `.option(key, value)` on writer options.
- **`append`**: Call `writer.append()`.
- **`createOrReplace`**: If `partitionFields` specifies partition columns or transform expressions (such as `F.days("event_time")`, `F.hours("ts")`, `F.bucket(16, "user_id")`, or `F.col("region")`), chain `.partitionedBy(*partition_exprs)` and call `.createOrReplace()`.
- **`overwritePartitions`**: Call `writer.overwritePartitions()` to replace only the partitions present in the incoming DataFrame.
- **`merge`**: Register the incoming DataFrame as a temporary view (`df.createOrReplaceTempView(staging_view)`) and execute an atomic Iceberg SQL upsert:
  - Build the `ON` predicate from `mergeCondition` when provided, or join each column in `mergeKeys` (`target.<col> = source.<col>`).
  - Run `spark.sql(f"MERGE INTO {table_id} AS target USING {staging_view} AS source ON {on_clause} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")`.

### CDC & SCD Type 1 / Type 2 Merges (`MergeCdcScd`)
When an upstream `MergeCdcScd` step feeds `IcebergSink` (`writeOperation == "merge"`), the staged DataFrame includes `_scd_action` (`actionColumn`) and, for SCD Type 2, nullable `_merge_<key>` columns. Exclude internal staging columns (`_scd_action` and `_merge_*`) from target table column assignments:
- **SCD Type 1 (`scdType == "scd1"`)**:
  - Match target and staged source rows on `keyColumns` (`ON target.<k> = source.<k>`).
  - Execute a single atomic `MERGE INTO` statement handling deletes, updates, and inserts while omitting `_scd_action`:
    ```sql
    MERGE INTO <table_id> AS target
    USING <staging_view> AS source
    ON target.<k> = source.<k>
    WHEN MATCHED AND source._scd_action = 'DELETE' THEN DELETE
    WHEN MATCHED AND source._scd_action = 'UPDATE' THEN UPDATE SET target.<col> = source.<col>, ...
    WHEN NOT MATCHED AND source._scd_action = 'INSERT' THEN INSERT (<cols>) VALUES (source.<cols>)
    ```
- **SCD Type 2 (`scdType == "scd2"`)**:
  - Match only active target rows via the nullable `_merge_<k>` columns (`ON target.<k> = source._merge_<k> AND target.is_current = true`). Staged `'INSERT'` rows carry `NULL` in `_merge_<k>`, so they bypass `WHEN MATCHED` and insert cleanly in the same atomic pass.
  - Close expired or deleted active versions and insert new active versions while omitting `_scd_action` and `_merge_*` columns from `INSERT`:
    ```sql
    MERGE INTO <table_id> AS target
    USING <staging_view> AS source
    ON target.<k> = source._merge_<k> AND target.is_current = true
    WHEN MATCHED AND source._scd_action IN ('EXPIRE', 'DELETE')
      THEN UPDATE SET target.is_current = false, target.effective_to = source.effective_from
    WHEN NOT MATCHED AND source._scd_action = 'INSERT'
      THEN INSERT (<business_and_scd2_cols>) VALUES (source.<business_and_scd2_cols>)
    ```

### Streaming Writes (`DataStreamWriter`)
- **Native Append Streaming (`ExecutionPlan.streamingSinkGroups` `mode == "native"` and `writeOperation != "merge"`)**:
  - Write with `df.writeStream.format("iceberg")`.
  - Configure `outputMode` (`"append"` or `"complete"`), `checkpointLocation`, and `trigger` (`processingTime` or `availableNow=True`).
  - Pass `.option("fanout-enabled", str(fanout_enabled).lower())` (recommended `"true"` for streaming writers).
  - Start the stream with `.toTable(table_id)`.
- **Streaming Upsert or Multi-Sink Fan-Out (`writeOperation == "merge"` or `mode == "foreachBatch"`)**:
  - Inside `def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:`, register `batch_df.createOrReplaceGlobalTempView(...)` or `batch_df.createOrReplaceTempView(staging_view)` on `batch_df.sparkSession` and execute `batch_df.sparkSession.sql("MERGE INTO ...")` (or `batch_df.writeTo(table_id).append()`).

## 5. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads an Apache Iceberg table in batch or streaming mode."""
    dfs: dict[str, DataFrame] = {}
    read_mode = config.get("iceberg_read_mode", "batch")
    table_id = config["iceberg_source_table"]

    if read_mode == "streaming":
        reader = spark.readStream.format("iceberg")
        if config.get("iceberg_streaming_from_timestamp"):
            reader = reader.option(
                "streaming-from-timestamp",
                str(config["iceberg_streaming_from_timestamp"]),
            )
        if config.get("iceberg_skip_delete_snapshots") is not None:
            reader = reader.option(
                "streaming-skip-delete-snapshots",
                str(config["iceberg_skip_delete_snapshots"]).lower(),
            )
        if config.get("iceberg_skip_overwrite_snapshots") is not None:
            reader = reader.option(
                "streaming-skip-overwrite-snapshots",
                str(config["iceberg_skip_overwrite_snapshots"]).lower(),
            )
        dfs["src_iceberg"] = reader.load(table_id)
    else:
        reader = spark.read.format("iceberg")
        if config.get("iceberg_snapshot_id"):
            reader = reader.option("snapshot-id", str(config["iceberg_snapshot_id"]))
        elif config.get("iceberg_as_of_timestamp"):
            reader = reader.option(
                "as-of-timestamp", str(config["iceberg_as_of_timestamp"])
            )
        dfs["src_iceberg"] = reader.load(table_id)

    return dfs


def write_iceberg_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a batch DataFrame to an Apache Iceberg table using DataFrameWriterV2 or MERGE INTO."""
    table_id = config["iceberg_sink_table"]
    operation = config.get("iceberg_write_operation", "append")
    fanout = bool(config.get("iceberg_fanout_enabled", False))

    if operation == "merge":
        staging_view = "iceberg_merge_source"
        df.createOrReplaceTempView(staging_view)
        merge_cond = config.get("iceberg_merge_condition", "").strip()
        if not merge_cond:
            keys = [
                k.strip()
                for k in config.get("iceberg_merge_keys", "").split(",")
                if k.strip()
            ]
            merge_cond = " AND ".join(f"target.{k} = source.{k}" for k in keys)
        df.sparkSession.sql(
            f"MERGE INTO {table_id} AS target "
            f"USING {staging_view} AS source "
            f"ON {merge_cond} "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        )
        return

    writer = df.writeTo(table_id).using("iceberg")
    if fanout:
        writer = writer.option("fanout-enabled", "true")
    for key, value in config.get("iceberg_table_properties", {}).items():
        writer = writer.tableProperty(key, value)

    if operation == "createOrReplace":
        partition_cols = [
            F.col(c.strip())
            for c in config.get("iceberg_partition_fields", "").split(",")
            if c.strip()
        ]
        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)
        writer.createOrReplace()
    elif operation == "overwritePartitions":
        writer.overwritePartitions()
    else:
        writer.append()


def start_iceberg_stream(df: DataFrame, config: dict[str, Any]):
    """Starts a native Structured Streaming query writing to an Apache Iceberg table."""
    table_id = config["iceberg_sink_table"]
    checkpoint_dir = config["iceberg_checkpoint_location"]
    fanout = str(config.get("iceberg_fanout_enabled", True)).lower()
    trigger_interval = config.get("iceberg_trigger_interval", "30 seconds")

    return (
        df.writeStream.format("iceberg")
        .outputMode(config.get("iceberg_output_mode", "append"))
        .option("checkpointLocation", checkpoint_dir)
        .option("fanout-enabled", fanout)
        .trigger(processingTime=trigger_interval)
        .toTable(table_id)
    )
```

## 6. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame matching the Iceberg schema without connecting to a live Iceberg or BigLake catalog:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.pipeline import transform


def test_iceberg_pipeline_transform(spark: SparkSession) -> None:
    """Verifies transformation logic using in-memory Iceberg table rows."""
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("total_amount", DoubleType(), False),
        ]
    )
    input_df = spark.createDataFrame(
        [("ord-100", "cust-1", 149.95), ("ord-101", "cust-2", 89.50)],
        schema=schema,
    )
    result_dfs = transform({"src_iceberg": input_df}, {})
    assert "src_iceberg" in result_dfs
    assert result_dfs["src_iceberg"].count() == 2
```
