---
name: io-hudi
description: Read snapshot, incremental, read-optimized, and streaming records from Apache Hudi tables in PySpark 4.x.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [HudiSource]
  preloadWhen: []
  sources:
    - prompt
    - https://hudi.apache.org/docs/quick-start-guide
  validated: true
---

# Apache Hudi Lakehouse Source (`HudiSource`)

Use these rules when generating PySpark {{spark_version}} code for an Apache Hudi source node (`HudiSource`).

## 1. Runtime Classpath and Kryo Serializer Configuration

- Attach the Scala 2.13 Apache Hudi Spark 4.0 bundle (`org.apache.hudi:hudi-spark4.0-bundle_2.13:1.0.1`) through `--packages` or `spark.jars.packages` in the runner scripts (`run_local.sh`, `run_dataproc_cluster.sh`, and `run_serverless_spark.sh`).
- Configure the mandatory Kryo serializer and Hudi session extensions on the `SparkSession`:
  - `spark.serializer=org.apache.spark.serializer.KryoSerializer`
  - `spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar`
  - `spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension`
  - `spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog`

## 2. Batch, Incremental, and Streaming Query Options

Inspect `readMode` (default `"batch"`) and `queryType` (default `"snapshot"`):
- **Batch mode (`readMode == "batch"`)**: Read with `spark.read.format("hudi")`:
  - `queryType` -> `.option("hoodie.datasource.query.type", query_type)`:
    - `"snapshot"`: Latest compacted and log-merged table state.
    - `"read_optimized"`: Base columnar Parquet files only on Merge-On-Read tables.
    - `"incremental"`: Changes committed between `beginInstantTime` and optional `endInstantTime`.
  - `beginInstantTime` (required when `queryType == "incremental"`) -> `.option("hoodie.datasource.read.begin.instanttime", config["hudi_begin_instant_time"])`
  - `endInstantTime` (optional when `queryType == "incremental"`) -> `.option("hoodie.datasource.read.end.instanttime", config["hudi_end_instant_time"])`
- **Streaming mode (`readMode == "streaming"`)**: Read Hudi commit streams with `spark.readStream.format("hudi")`:
  - Optionally set `.option("hoodie.datasource.read.begin.instanttime", config["hudi_begin_instant_time"])`.
- Load from the Hudi base path via `.load(config["hudi_path"])`.

## 3. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads an Apache Hudi table in snapshot, incremental, or streaming mode."""
    dfs: dict[str, DataFrame] = {}

    read_mode = config.get("hudi_read_mode", "batch")
    hudi_path = config["hudi_path"]

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
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame matching the Hudi table schema without reading a Hudi table from storage:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.pipeline import transform


def test_hudi_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory Apache Hudi rows."""
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
