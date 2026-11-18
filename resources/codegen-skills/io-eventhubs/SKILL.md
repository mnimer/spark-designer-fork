---
name: io-eventhubs
description: Read batch and streaming events from Azure Event Hubs in PySpark 4.x using the Kafka-compatible SASL_SSL endpoint.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [EventHubsSource]
  preloadWhen: []
  sources:
    - prompt
    - https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-developer-guide
    - https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
  validated: true
---

# Azure Event Hubs Source (`EventHubsSource`)

Use these rules when generating PySpark {{spark_version}} code for an Azure Event Hubs source node (`EventHubsSource`).

## 1. Runtime Classpath and Kafka Endpoint Architecture

Azure Event Hubs exposes a native Apache Kafka protocol endpoint on port `9093`. Use Spark's official `.format("kafka")` connector (`org.apache.spark:spark-sql-kafka-0-10_2.13:{{spark_version}}`) against `<namespace>.servicebus.windows.net:9093` so Event Hubs works across Spark 4.x local, Dataproc Cluster, and Serverless Spark runtimes without legacy Scala 2.12 JAR incompatibilities.

## 2. Batch and Streaming Read Modes

Inspect the node `readMode` property (default `"streaming"`):
- **Streaming mode (`readMode == "streaming"`)**: Read unbounded events with `spark.readStream.format("kafka")`.
- **Batch mode (`readMode == "batch"`)**: Read bounded event ranges with `spark.read.format("kafka")`.

## 3. Connection, SASL_SSL, and Offset Options

Configure `.format("kafka")` for Azure Event Hubs in `read_sources(spark, config)`:
- `eventHubNamespace` -> derive bootstrap server `f"{namespace}.servicebus.windows.net:9093"` and set `.option("kafka.bootstrap.servers", bootstrap_servers)`
- `eventHubName` / `subscribe` -> `.option("subscribe", config["eventhub_name"])`
- `consumerGroup` -> when set (default `"$Default"`), pass `.option("kafka.group.id", config["eventhub_consumer_group"])`
- `startingOffsets` -> `.option("startingOffsets", config.get("eventhub_starting_offsets", "latest"))` (`"latest"`, `"earliest"`, or JSON)
- `endingOffsets` (batch mode only) -> `.option("endingOffsets", config.get("eventhub_ending_offsets", "latest"))`
- `maxOffsetsPerTrigger` (streaming mode) -> `.option("maxOffsetsPerTrigger", str(config["eventhub_max_offsets_per_trigger"]))`
- `kafka.request.timeout.ms` and `kafka.session.timeout.ms` -> set `60000` and `30000` as recommended by Azure Event Hubs for Kafka.

## 4. Connection String Authentication via Secret Manager

Resolve the Azure Event Hubs connection string (`Endpoint=sb://<namespace>.servicebus.windows.net/...`) once on the driver using `get_secret()` (`access_secret_version()`):
- Set `.option("kafka.security.protocol", "SASL_SSL")`.
- Set `.option("kafka.sasl.mechanism", "PLAIN")`.
- Set `.option("kafka.sasl.jaas.config", jaas_config)` with `username="$ConnectionString"` and `password` set to the resolved connection string.

## 5. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_secret(secret_version_name: str) -> str:
    """Fetches a UTF-8 secret payload from Google Cloud Secret Manager."""
    return access_secret_version(secret_version_name)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads Azure Event Hubs records over the Kafka SASL_SSL endpoint."""
    dfs: dict[str, DataFrame] = {}

    namespace = config["eventhub_namespace"]
    eventhub_name = config["eventhub_name"]
    connection_string = get_secret(config["eventhub_connection_string_secret"])
    bootstrap_servers = f"{namespace}.servicebus.windows.net:9093"
    jaas_config = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{connection_string}";'
    )

    read_mode = config.get("eventhub_read_mode", "streaming")
    reader = (
        spark.read.format("kafka")
        if read_mode == "batch"
        else spark.readStream.format("kafka")
    )

    reader = (
        reader.option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", eventhub_name)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "30000")
        .option("startingOffsets", config.get("eventhub_starting_offsets", "latest"))
    )

    if config.get("eventhub_consumer_group"):
        reader = reader.option("kafka.group.id", config["eventhub_consumer_group"])

    if read_mode == "batch":
        reader = reader.option("endingOffsets", config.get("eventhub_ending_offsets", "latest"))
    elif config.get("eventhub_max_offsets_per_trigger"):
        reader = reader.option(
            "maxOffsetsPerTrigger", str(config["eventhub_max_offsets_per_trigger"])
        )

    raw_df = reader.load()
    dfs["src_eventhubs"] = raw_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp"),
    )
    return dfs
```

## 6. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline with an in-memory DataFrame matching the output columns of `read_sources()` without connecting to Azure Event Hubs:

```python
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from src.pipeline import transform


def test_eventhubs_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory Event Hubs records."""
    schema = StructType(
        [
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
            StructField("topic", StringType(), False),
            StructField("partition", IntegerType(), False),
            StructField("offset", LongType(), False),
            StructField("timestamp", TimestampType(), False),
        ]
    )
    input_df = spark.createDataFrame(
        [
            (
                "eh-1",
                '{"telemetry_id": "t-1", "cpu": 0.72}',
                "telemetry-hub",
                0,
                10,
                datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            )
        ],
        schema=schema,
    )
    result_dfs = transform({"src_eventhubs": input_df}, {})
    assert "src_eventhubs" in result_dfs
    assert result_dfs["src_eventhubs"].count() == 1
```
