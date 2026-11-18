---
name: io-spanner
description: Read Google Cloud Spanner tables and SQL queries in PySpark 4.x using the Cloud Spanner Spark connector with Partitioned Read API and Data Boost.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [SpannerSource]
  preloadWhen: []
  sources:
    - prompt
    - https://cloud.google.com/spanner/docs/use-spark-connector
  validated: true
---

# Google Cloud Spanner Source (`SpannerSource`)

Use these rules when generating PySpark 4.x code for a `SpannerSource` node.

## 1. Runtime Classpath and Connector Delivery

- Supply the Cloud Spanner Spark 4.0 connector JAR via `--jars gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar` (or `spark.jars` in Spark properties) across Serverless Spark, Dataproc Cluster, and Local runners.
- Use `.format("cloud-spanner")` in `spark.read`.

## 2. Batch Read Patterns (`read_sources`)

`SpannerSource` runs in `batch` mode and distributes parallel reads across executors using the Spanner Partitioned Read API.

### Required and Optional Options
- `projectId`: Google Cloud project ID that hosts the Spanner instance.
- `instanceId`: Cloud Spanner instance ID.
- `databaseId`: Cloud Spanner database ID.
- `table` or `query`: Specify either `.option("table", "<table_name>")` to read a table or `.option("query", "<sql_query>")` to execute a partitionable SQL query.
- `enableDataBoost`: Set `.option("enableDataBoost", "true")` when configured to run analytical queries on independent Spanner Data Boost compute capacity with zero impact on transactional workloads.
- `timestampBound`: Optional staleness or snapshot bound (such as strong reads or an exact RFC 3339 timestamp via `readTimestamp`) for consistent point-in-time reads.

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads a Cloud Spanner table or SQL query via the Cloud Spanner Spark connector."""
    dfs: dict[str, DataFrame] = {}

    reader = (
        spark.read.format("cloud-spanner")
        .option("projectId", config["spanner_project_id"])
        .option("instanceId", config["spanner_instance_id"])
        .option("databaseId", config["spanner_database_id"])
        .option("enableDataBoost", str(config.get("enable_data_boost", True)).lower())
    )

    if config.get("timestamp_bound"):
        reader = reader.option("timestampBound", config["timestamp_bound"])

    if config.get("spanner_query"):
        dfs["spanner_source"] = reader.option("query", config["spanner_query"]).load()
    else:
        dfs["spanner_source"] = reader.option("table", config["spanner_table"]).load()

    return dfs
```

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by constructing an in-memory PySpark `DataFrame` with the expected Cloud Spanner schema instead of opening a network connection to Spanner:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType
from src.pipeline import transform


def test_spanner_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using an offline DataFrame."""
    schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("balance_cents", LongType(), False),
        ]
    )
    input_df = spark.createDataFrame(
        [("acc-100", "ACTIVE", 50000), ("acc-200", "CLOSED", 0)],
        schema=schema,
    )
    result_dfs = transform({"spanner_source": input_df}, {})
    assert result_dfs["spanner_source"].count() == 2
```
