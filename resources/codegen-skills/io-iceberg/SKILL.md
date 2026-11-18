---
name: io-iceberg
description: Read batch and streaming Apache Iceberg tables in PySpark 4.x across BigLake REST, REST, Hive, and Hadoop catalogs.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [IcebergSource]
  preloadWhen: []
  sources:
    - prompt
    - https://iceberg.apache.org/docs/latest/spark-queries/
    - https://cloud.google.com/biglake/docs/apache-iceberg-in-biglake
  validated: true
---

# Apache Iceberg Lakehouse Source (`IcebergSource`)

Use these rules when generating PySpark {{spark_version}} code for an Apache Iceberg source node (`IcebergSource` or `IcebergIO`).

## 1. Runtime Classpath Rules (`versionMatrix.ts`)

- **Google Cloud Managed Spark (`run_serverless_spark.sh` and `run_dataproc_cluster.sh`)**: `iceberg-spark-runtime` is pre-installed on Serverless Spark and Dataproc Cluster runtimes. Never pass `org.apache.iceberg:iceberg-spark-runtime*` to `spark.jars.packages` or `--packages` on Serverless Spark or Dataproc Cluster because doing so causes classpath conflicts.
- **Local Spark (`run_local.sh` only)**: Pass `--packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.9.0` (plus the storage connector coordinate when reading from cloud buckets locally).
- **Spark Session Extensions**: Configure `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` on the session.

## 2. Catalog Configuration (`BigLake`, `REST`, `Hive`, `Hadoop`)

Configure the target catalog `<catalog>` (`catalogName`, default `"iceberg"` or `"biglake"`) on the `SparkSession` according to `catalogType`:
- **BigLake Metastore / BigLake REST Catalog (`catalogType == "biglake"`)**:
  - `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`
  - For BigLake REST catalog: `spark.sql.catalog.<catalog>.type=rest` (or `catalog-impl=org.apache.iceberg.rest.RESTCatalog`) and `spark.sql.catalog.<catalog>.uri=https://biglake.googleapis.com/iceberg/v1beta/restcatalog` with `spark.sql.catalog.<catalog>.warehouse=gs://<bucket>/<warehouse>`
  - For BigLake Metastore catalog implementation: `spark.sql.catalog.<catalog>.catalog-impl=org.apache.iceberg.gcp.biglake.BigLakeCatalog`, `spark.sql.catalog.<catalog>.gcp_project=<project_id>`, `spark.sql.catalog.<catalog>.gcp_location=<region>`, `spark.sql.catalog.<catalog>.blms_catalog=<catalog_name>`, and `spark.sql.catalog.<catalog>.warehouse=gs://<bucket>/<warehouse>`
- **REST Catalog (`catalogType == "rest"`)**:
  - `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`
  - `spark.sql.catalog.<catalog>.type=rest` (or `catalog-impl=org.apache.iceberg.rest.RESTCatalog`)
  - `spark.sql.catalog.<catalog>.uri=<rest_uri>`
- **Hive Metastore Catalog (`catalogType == "hive"`)**:
  - `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`
  - `spark.sql.catalog.<catalog>.type=hive`
  - `spark.sql.catalog.<catalog>.uri=thrift://<metastore-host>:9083`
- **Hadoop File-System Catalog (`catalogType == "hadoop"`)**:
  - `spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog`
  - `spark.sql.catalog.<catalog>.type=hadoop`
  - `spark.sql.catalog.<catalog>.warehouse=<gs:// or s3a:// or hdfs:// path>`

## 3. Batch, Time-Travel, and Streaming Read Options

Inspect `readMode` (default `"batch"`):
- **Batch mode (`readMode == "batch"`)**: Read with `spark.read.format("iceberg")`:
  - `snapshotId` -> `.option("snapshot-id", str(config["iceberg_snapshot_id"]))` for exact snapshot time-travel.
  - `asOfTimestamp` -> `.option("as-of-timestamp", str(config["iceberg_as_of_timestamp"]))` (epoch milliseconds) for timestamp time-travel.
  - Load the fully qualified table identifier `f"{catalog}.{namespace}.{table}"` via `.load(table_identifier)`.
- **Streaming mode (`readMode == "streaming"`)**: Read incremental Iceberg appends with `spark.readStream.format("iceberg")`:
  - `streamingFromTimestamp` -> `.option("streaming-from-timestamp", str(config["iceberg_streaming_from_timestamp"]))`
  - `streamingSkipDeleteSnapshots` -> `.option("streaming-skip-delete-snapshots", str(skip_deletes).lower())`
  - `streamingSkipOverwriteSnapshots` -> `.option("streaming-skip-overwrite-snapshots", str(skip_overwrites).lower())`
  - Load with `.load(table_identifier)`.

## 4. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads an Apache Iceberg table in batch or streaming mode."""
    dfs: dict[str, DataFrame] = {}

    read_mode = config.get("iceberg_read_mode", "batch")
    table_id = config["iceberg_table"]

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
        if config.get("iceberg_as_of_timestamp"):
            reader = reader.option(
                "as-of-timestamp", str(config["iceberg_as_of_timestamp"])
            )
        dfs["src_iceberg"] = reader.load(table_id)

    return dfs
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame matching the Iceberg table schema without querying a live Iceberg or BigLake catalog:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.pipeline import transform


def test_iceberg_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory Iceberg table rows."""
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
