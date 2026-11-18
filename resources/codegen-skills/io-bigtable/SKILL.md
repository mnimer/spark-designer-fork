---
name: io-bigtable
description: Read and write Google Cloud Bigtable tables in PySpark 4.x using the Spark Bigtable connector with catalog JSON schema mapping, row key pushdown, batch mutation tuning, and streaming foreachBatch.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [BigtableSource, BigtableSink]
  preloadWhen: []
  sources:
    - prompt
    - https://cloud.google.com/bigtable/docs/use-spark-connector
  validated: true
---

# Google Cloud Bigtable Connector (`BigtableSource`, `BigtableSink`)

Use these rules when generating PySpark 4.x code for `BigtableSource` or `BigtableSink` nodes.

## 1. Runtime Classpath and Connector Delivery

- Supply the Cloud Bigtable Spark connector JAR (Scala 2.13) with `--jars gs://spark-lib/bigtable/spark-bigtable_2.13-0.1.0.jar` (or `spark.jars`).
- Use `.format("bigtable")` in `spark.read` and `df.write`.
- Never access `spark` internal JVM handles in application code.

## 2. Batch Read Patterns (`BigtableSource` in `read_sources`)

`BigtableSource` runs in `batch` mode and maps wide-column Bigtable cells to Catalyst columns through a JSON `catalog` definition.

### Required Read Options
- `spark.bigtable.project.id`: Google Cloud project ID containing the Bigtable instance (`bigtable.project.id`).
- `spark.bigtable.instance.id`: Cloud Bigtable instance ID (`bigtable.instance.id`).
- `catalog`: A JSON string defining `table` (`namespace` and `name`), `rowkey`, and `columns` mapping each Spark column name to `{ "cf": "<column_family>", "col": "<qualifier>", "type": "<type>" }` (where `"cf": "rowkey"` maps the row key).
- `spark.bigtable.push.down.row.key.filters`: Set to `"true"` (default) so Spark pushes predicate filters on the row key column down to Cloud Bigtable tablet scans.
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

## 3. Batch Write Patterns (`BigtableSink` in `write_sinks`)

`BigtableSink` writes DataFrames to Cloud Bigtable cells with `df.write.format("bigtable")`.

### Catalog Mapping and Write Options
- `spark.bigtable.project.id` and `spark.bigtable.instance.id`: Target Google Cloud project and Bigtable instance identifiers (corresponding to `bigtable.project.id` and `bigtable.instance.id`).
- `catalog`: JSON catalog string mapping every output DataFrame column to `"rowkey"` or a target column family and column qualifier. Ensure the row key column contains no null values before writing.
- `createNewTable` (`spark.bigtable.create.new.table`): Set `.option("spark.bigtable.create.new.table", str(create_new_table).lower())`. Leave `"false"` in production when column families and garbage collection policies already exist.
- `writeRowTimestamp` (`spark.bigtable.write.timestamp.milliseconds`): When configured, pass an explicit epoch-millisecond timestamp string to stamp all mutated cells deterministically.
- Batch mutation tuning:
  - When `batchMutateSize` is configured, set `.option("spark.bigtable.batch_mutate.size", str(config["batch_mutate_size"]))` (up to `100000` rows per batch request).
  - When `enableFlowControl` is true, set `.option("spark.bigtable.enable.batch_mutate.flow_control", "true")` to throttle mutations dynamically against tablet server load.
  - When `appProfileId` is configured, set `.option("spark.bigtable.app_profile.id", config["bigtable_app_profile_id"])`.

```python
import json
import logging
from typing import Any
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_bigtable_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a batch DataFrame to Google Cloud Bigtable using a catalog JSON mapping."""
    catalog_json = config.get("bigtable_sink_catalog") or json.dumps(
        {
            "table": {"name": config["bigtable_sink_table"]},
            "rowkey": "row_key",
            "columns": {
                "row_key": {"cf": "rowkey", "col": "rowkey", "type": "string"},
                "device_type": {"cf": "metadata", "col": "device_type", "type": "string"},
                "temperature": {"cf": "metrics", "col": "temperature", "type": "double"},
            },
        }
    )

    create_new_table = str(config.get("create_new_table", False)).lower()
    writer = (
        df.write.format("bigtable")
        .option("spark.bigtable.project.id", config["bigtable_project_id"])
        .option("spark.bigtable.instance.id", config["bigtable_instance_id"])
        .option("spark.bigtable.create.new.table", create_new_table)
        .option("catalog", catalog_json)
    )

    if config.get("bigtable_app_profile_id"):
        writer = writer.option("spark.bigtable.app_profile.id", config["bigtable_app_profile_id"])

    if config.get("write_row_timestamp"):
        writer = writer.option(
            "spark.bigtable.write.timestamp.milliseconds",
            str(config["write_row_timestamp"]),
        )

    if config.get("batch_mutate_size"):
        writer = writer.option(
            "spark.bigtable.batch_mutate.size", str(config["batch_mutate_size"])
        )

    if config.get("enable_flow_control"):
        writer = writer.option("spark.bigtable.enable.batch_mutate.flow_control", "true")

    logger.info("Writing DataFrame to Cloud Bigtable table %s", config["bigtable_sink_table"])
    writer.save()
```

## 4. Structured Streaming Micro-Batch Writes (`foreachBatch`)

`BigtableSink` uses `streamingSinkMode == "foreachBatch"`. Never call `.writeStream.format("bigtable")` directly.

- Wrap `write_bigtable_batch(batch_df, config)` inside `df.writeStream.foreachBatch(write_micro_batch)`.
- Design deterministic row keys (and optional deterministic cell timestamps) so retried micro-batches overwrite identical cells idempotently without creating duplicate rows.
- Cache `batch_df` at the start of `write_micro_batch` and call `batch_df.unpersist()` inside a `finally` block.
- Configure `checkpointLocation`, `outputMode`, and `trigger`, then call `spark.streams.awaitAnyTermination()`.

```python
from typing import Any
from pyspark.sql import DataFrame


def start_bigtable_stream(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a streaming DataFrame to Cloud Bigtable via foreachBatch."""
    checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
    checkpoint_location = config.get(
        "bigtable_checkpoint_location", f"{checkpoint_root.rstrip('/')}/bigtable_sink"
    )

    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        try:
            write_bigtable_batch(batch_df, config)
        finally:
            batch_df.unpersist()

    stream_writer = (
        df.writeStream.foreachBatch(write_micro_batch)
        .outputMode(config.get("bigtable_output_mode", "append"))
        .option("checkpointLocation", checkpoint_location)
    )

    if config.get("bigtable_trigger_type") == "availableNow":
        stream_writer.trigger(availableNow=True).start()
    else:
        interval = config.get("bigtable_trigger_interval", "10 seconds")
        stream_writer.trigger(processingTime=interval).start()

    df.sparkSession.streams.awaitAnyTermination()
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

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
