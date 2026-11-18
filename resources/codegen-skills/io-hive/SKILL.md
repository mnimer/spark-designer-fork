---
name: io-hive
description: Read batch tables from Apache Hive Metastore or Google Cloud Dataproc Metastore in PySpark 4.x using spark.table().
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [HiveSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
  validated: true
---

# Apache Hive & Dataproc Metastore Source (`HiveSource`)

Use these rules when generating PySpark {{spark_version}} code for an Apache Hive or Dataproc Metastore source node (`HiveSource` or `HCatalogIO`).

## 1. Session Initialization and Metastore Configuration

- Call `.enableHiveSupport()` on `SparkSession.builder` inside `create_spark_session(app_name, config)` (when not using Spark Connect) so Catalyst binds to the Hive catalog.
- When `metastoreUri` (`spark.hadoop.hive.metastore.uris`, such as `"thrift://metastore-host:9083"`) is configured on the node, pass `.config("spark.hadoop.hive.metastore.uris", config["hive_metastore_uri"])` during session creation or via `--conf spark.hadoop.hive.metastore.uris=...` in the runner scripts.
- On Dataproc Cluster and Serverless Spark, attach the Dataproc Metastore service via `--metastore-service` when configured.

## 2. Reading Tables and Partition Filter Pushdown

In `read_sources(spark, config)`, read the Hive table using `spark.table(f"{database}.{table}")`:
- `database` -> defaults to `"default"` when omitted.
- `table` -> required table name in the metastore.
- `filterPredicate` -> when a partition or row filter expression is configured (such as `"dt >= '2026-01-01'"`), chain `.filter(config["hive_filter_predicate"])` immediately after `spark.table(...)` so Catalyst pushes partition pruning down to the Hive Metastore before scanning files.

## 3. Reference Implementation (`src/pipeline.py`)

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

    database = config.get("hive_database", "default")
    table = config["hive_table"]
    df = spark.table(f"{database}.{table}")

    if config.get("hive_filter_predicate"):
        df = df.filter(config["hive_filter_predicate"])

    dfs["src_hive"] = df
    return dfs
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame matching the Hive table schema without requiring a running Hive Metastore or warehouse directory:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.pipeline import transform


def test_hive_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory Hive table rows."""
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
