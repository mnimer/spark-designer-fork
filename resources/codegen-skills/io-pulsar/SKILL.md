---
name: io-pulsar
description: Read batch and streaming messages from Apache Pulsar topics in PySpark 4.x using the StreamNative Pulsar Spark connector.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [PulsarSource]
  preloadWhen: []
  sources:
    - prompt
    - https://docs.streamnative.io/docs/pulsar-spark-connector
  validated: true
---

# Apache Pulsar Source (`PulsarSource`)

Use these rules when generating PySpark {{spark_version}} code for an Apache Pulsar source node (`PulsarSource` or `PulsarIO`).

## 1. Runtime Classpath and Package Delivery

Attach the Scala 2.13 StreamNative Pulsar Spark 4.0 connector (`io.streamnative.connectors:pulsar-spark-connector_2.13:4.0.0.1`) through `--packages` or `spark.jars.packages` in the runner scripts (`run_local.sh`, `run_dataproc_cluster.sh`, and `run_serverless_spark.sh`).

## 2. Batch and Streaming Read Modes

Inspect the node `readMode` property (default `"streaming"`):
- **Streaming mode (`readMode == "streaming"`)**: Read unbounded messages with `spark.readStream.format("pulsar")`.
- **Batch mode (`readMode == "batch"`)**: Read bounded message ranges with `spark.read.format("pulsar")`.

## 3. Connection, Subscription, and Offset Options

Map the `PulsarSource` node properties to `.format("pulsar")` options in `read_sources(spark, config)`:
- `serviceUrl` -> `.option("service.url", config["pulsar_service_url"])` (such as `"pulsar://localhost:6650"` or `"pulsar+ssl://pulsar.example.com:6651"`)
- `adminUrl` -> `.option("admin.url", config["pulsar_admin_url"])` (such as `"http://localhost:8080"` or `"https://pulsar.example.com:8443"`)
- Subscription mode (`subscriptionMode`):
  - `"topic"` (single topic): `.option("topic", config["pulsar_topic"])`
  - `"topics"` (comma-separated list): `.option("topics", config["pulsar_topics"])`
  - `"topicsPattern"` (regex pattern): `.option("topicsPattern", config["pulsar_topics_pattern"])`
- `startingOffsets` -> `.option("startingOffsets", config.get("pulsar_starting_offsets", "latest"))` (`"earliest"`, `"latest"`, or topic message ID JSON)
- `endingOffsets` (batch mode only) -> `.option("endingOffsets", config.get("pulsar_ending_offsets", "latest"))` (`"latest"` or topic message ID JSON)
- `allowDifferentTopicSchemas` -> `.option("allowDifferentTopicSchemas", str(allow_different).lower())` when subscribing across topics with distinct schemas

## 4. Token Authentication via Secret Manager

When `tokenSecret` (or Secret Manager authentication) is configured on the Pulsar node, fetch the JWT token once on the driver using `get_secret()` (`access_secret_version()`) and configure the Pulsar client authentication plugin:
- `.option("pulsar.client.authPluginClassName", "org.apache.pulsar.client.impl.auth.AuthenticationToken")`
- `.option("pulsar.client.authParams", f"token:{jwt_token}")`

## 5. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_secret(secret_version_name: str) -> str:
    """Fetches a UTF-8 secret payload from Google Cloud Secret Manager."""
    return access_secret_version(secret_version_name)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads Apache Pulsar messages in streaming or batch mode."""
    dfs: dict[str, DataFrame] = {}

    read_mode = config.get("pulsar_read_mode", "streaming")
    reader = (
        spark.read.format("pulsar")
        if read_mode == "batch"
        else spark.readStream.format("pulsar")
    )

    reader = (
        reader.option("service.url", config["pulsar_service_url"])
        .option("admin.url", config["pulsar_admin_url"])
        .option("startingOffsets", config.get("pulsar_starting_offsets", "latest"))
    )

    sub_mode = config.get("pulsar_subscription_mode", "topic")
    if sub_mode == "topics":
        reader = reader.option("topics", config["pulsar_topics"])
    elif sub_mode == "topicsPattern":
        reader = reader.option("topicsPattern", config["pulsar_topics_pattern"])
    else:
        reader = reader.option("topic", config["pulsar_topic"])

    if read_mode == "batch":
        reader = reader.option("endingOffsets", config.get("pulsar_ending_offsets", "latest"))

    if config.get("pulsar_token_secret"):
        token = get_secret(config["pulsar_token_secret"])
        reader = reader.option(
            "pulsar.client.authPluginClassName",
            "org.apache.pulsar.client.impl.auth.AuthenticationToken",
        ).option("pulsar.client.authParams", f"token:{token}")

    raw_df = reader.load()
    dfs["src_pulsar"] = raw_df.select(
        F.col("__key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("__topic").alias("topic"),
        F.col("__publishTime").alias("publish_time"),
        F.col("__eventTime").alias("event_time"),
    )
    return dfs
```

## 6. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame matching the output columns of `read_sources()` without connecting to a Pulsar cluster:

```python
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from src.pipeline import transform


def test_pulsar_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory Pulsar records."""
    schema = StructType(
        [
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
            StructField("topic", StringType(), False),
            StructField("publish_time", TimestampType(), False),
            StructField("event_time", TimestampType(), True),
        ]
    )
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    input_df = spark.createDataFrame(
        [("k-1", '{"payment_id": "p-1", "status": "SETTLED"}', "persistent://public/default/payments", now, now)],
        schema=schema,
    )
    result_dfs = transform({"src_pulsar": input_df}, {})
    assert "src_pulsar" in result_dfs
    assert result_dfs["src_pulsar"].count() == 1
```
