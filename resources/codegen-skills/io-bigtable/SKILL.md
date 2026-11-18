---
name: io-bigtable
description: Read Google Cloud Bigtable tables in PySpark 4.x using the Spark Bigtable connector with catalog JSON schema mapping and row key filter pushdown.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [BigtableSource]
  preloadWhen: []
  sources:
    - prompt
    - https://cloud.google.com/bigtable/docs/use-spark-connector
  validated: true
---

# Google Cloud Bigtable Source (`BigtableSource`)

Use these rules when generating PySpark 4.x code for a `BigtableSource` node.

## 1. Runtime Classpath and Connector Delivery

- Supply the Cloud Bigtable Spark connector JAR (Scala 2.13) via `--jars gs://spark-lib/bigtable/spark-bigtable_2.13-0.1.0.jar` (or `spark.jars`).
- Use `.format("bigtable")` in `spark.read`.

## 2. Batch Read Patterns (`read_sources`)

`BigtableSource` runs in `batch` mode and maps wide-column Bigtable cells to Catalyst columns through a JSON `catalog` definition.

### Required Options
- `spark.bigtable.project.id`: Google Cloud project ID containing the Bigtable instance.
- `spark.bigtable.instance.id`: Cloud Bigtable instance ID.
- `catalog`: A JSON string defining the `table` (`namespace` and `name`), `rowkey`, and `columns` mapping each Spark column name to `{ "cf": "<column_family>", "col": "<qualifier>", "type": "<type>" }` (where `"cf": "rowkey"` maps the row key).
- `spark.bigtable.push.down.row.key.filters`: Set to `"true"` (default) so Spark pushes predicate filters on the row key column directly down to Cloud Bigtable tablet scans.
- `spark.bigtable.app_profile.id`: Optional Bigtable application profile ID for workload routing.

```python
import json
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads a Cloud Bigtable table using a catalog JSON mapping and row key pushdown."""
    dfs: dict[str, DataFrame] = {}

    catalog_json = config.get("bigtable_catalog") or json.dumps(
        {
            "table": {"name": config["bigtable_table"]},
            "rowkey": "row_key",
            "columns": {
                "row_key": {"cf": "rowkey", "col": "rowkey", "type": "string"},
                "device_type": {"cf": "metadata", "col": "device_type", "type": "string"},
                "temperature": {"cf": "metrics", "col": "temperature", "type": "double"},
            },
        }
    )

    reader = (
        spark.read.format("bigtable")
        .option("spark.bigtable.project.id", config["bigtable_project_id"])
        .option("spark.bigtable.instance.id", config["bigtable_instance_id"])
        .option("spark.bigtable.push.down.row.key.filters", "true")
        .option("catalog", catalog_json)
    )
    if config.get("bigtable_app_profile_id"):
        reader = reader.option("spark.bigtable.app_profile.id", config["bigtable_app_profile_id"])

    dfs["bigtable_source"] = reader.load()
    return dfs
```

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by providing an in-memory `DataFrame` whose columns match the flattened `catalog` definition:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.pipeline import transform


def test_bigtable_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline using a DataFrame matching the Bigtable catalog."""
    schema = StructType(
        [
            StructField("row_key", StringType(), False),
            StructField("device_type", StringType(), True),
            StructField("temperature", DoubleType(), True),
        ]
    )
    input_df = spark.createDataFrame(
        [("dev#001", "thermostat", 21.5), ("dev#002", "sensor", 19.0)],
        schema=schema,
    )
    result_dfs = transform({"bigtable_source": input_df}, {})
    assert result_dfs["bigtable_source"].count() == 2
```
