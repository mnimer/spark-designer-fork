---
name: io-bigquery
description: Read Google BigQuery tables, views, and SQL queries in PySpark 4.x using the Storage Read API and the pre-installed Managed Spark connector.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [BigQuerySource]
  preloadWhen: []
  sources:
    - prompt
    - https://github.com/GoogleCloudDataproc/spark-bigquery-connector
  validated: true
---

# Google BigQuery Source (`BigQuerySource`)

Use these rules when generating PySpark 4.x code for a `BigQuerySource` node.

## 1. Runtime Classpath and Connector Delivery

- **Managed Spark (`ServerlessSpark` and `DataprocCluster`)**: The `spark-bigquery` connector is pre-installed on Google Cloud Serverless for Apache Spark (`4.0` and `3.0`) and Dataproc Cluster (`3.0`). Never add `spark-bigquery` to `spark.jars.packages` or `--packages` on Managed Spark targets. Adding a duplicate JAR causes classpath conflicts.
- **Local Runner (`Local`)**: Pass `--packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.42.1` only in `run_local.sh`.

## 2. Batch Read Patterns (`read_sources`)

`BigQuerySource` supports `batch` mode via the high-throughput BigQuery Storage Read API (`.format("bigquery")`).

### Reading a Table (`table`)
- Set `.option("table", "<project>.<dataset>.<table>")` or pass the table identifier to `.load("<project>.<dataset>.<table>")`.
- Set `.option("parentProject", config["parent_project"])` when the billing project differs from the table project.
- Apply `.option("filter", "<sql_predicate>")` to push row predicates directly down to the BigQuery Storage Read API before rows cross the network.

### Reading a Custom SQL Query or View (`query`)
- Set `.option("viewsEnabled", "true")`.
- Set `.option("materializationDataset", "<dataset>")` and optionally `.option("materializationProject", "<project>")` so BigQuery can materialize temporary query results.
- Pass the SQL statement via `.option("query", "<sql_query>")` and call `.load()`.

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads BigQuery source nodes using the BigQuery Storage Read API."""
    dfs: dict[str, DataFrame] = {}

    # Option A: Direct table read with Storage Read API filter pushdown
    reader = (
        spark.read.format("bigquery")
        .option("parentProject", config.get("gcp_project", "my-gcp-project"))
    )
    if config.get("bq_filter"):
        reader = reader.option("filter", config["bq_filter"])

    if config.get("bq_query"):
        # Option B: SQL query or logical view read
        dfs["bq_source"] = (
            reader.option("viewsEnabled", "true")
            .option("materializationDataset", config["bq_materialization_dataset"])
            .option("query", config["bq_query"])
            .load()
        )
    else:
        dfs["bq_source"] = reader.option("table", config["bq_table"]).load()

    return dfs
```

## 3. Column Pruning and Pushdown Rules

- Select only required columns immediately or let Catalyst push projection down into the BigQuery Storage Read API stream.
- Keep `read_sources(spark, config)` free of business transformations. Place all downstream projections, joins, and aggregations inside `transform(dfs, config)`.

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Never connect to Google BigQuery inside unit tests. Construct an in-memory `DataFrame` with an explicit `StructType` schema matching the BigQuery table columns and pass it directly into `transform(dfs, config)`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
from src.pipeline import transform


def test_bigquery_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without calling the BigQuery Storage Read API."""
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("event_ts", TimestampType(), True),
        ]
    )
    sample_df = spark.createDataFrame(
        [("ord-1", "cust-10", 125.50, None), ("ord-2", "cust-20", 42.00, None)],
        schema=schema,
    )
    result_dfs = transform({"bq_source": sample_df}, {"min_amount": 50.0})
    output_rows = result_dfs["bq_source"].collect()
    assert len(output_rows) == 2
```
