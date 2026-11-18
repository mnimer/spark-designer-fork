---
name: io-kafka
description: Read batch and streaming records from Apache Kafka topics in PySpark 4.x using spark-sql-kafka-0-10.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [KafkaSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
  validated: true
---

# Apache Kafka Source (`KafkaSource`)

Use these rules when generating PySpark {{spark_version}} code for an Apache Kafka source node (`KafkaSource` or `KafkaIO`).

## 1. Runtime Classpath and Package Delivery

Attach the Scala 2.13 Kafka connector matching the target Spark version (`org.apache.spark:spark-sql-kafka-0-10_2.13:{{spark_version}}`) through `--packages` or `spark.jars.packages` in the runner scripts (`run_local.sh`, `run_dataproc_cluster.sh`, and `run_serverless_spark.sh`). Do not hardcode Maven coordinates inside `src/pipeline.py`.

## 2. Batch and Streaming Read Modes

Inspect the node `readMode` property (default `"streaming"`):
- **Streaming mode (`readMode == "streaming"`)**: Read unbounded records with `spark.readStream.format("kafka")`.
- **Batch mode (`readMode == "batch"`)**: Read bounded offset ranges with `spark.read.format("kafka")`.

## 3. Subscription and Offset Options

Map the node properties to `.format("kafka")` options in `read_sources(spark, config)`:
- `bootstrapServers` -> `.option("kafka.bootstrap.servers", config["kafka_bootstrap_servers"])`
- Subscription mode (`subscriptionMode`):
  - `"subscribe"` (default): `.option("subscribe", config["kafka_topics"])` (comma-separated topic list).
  - `"subscribePattern"`: `.option("subscribePattern", config["kafka_topic_pattern"])` (Java regex pattern).
  - `"assign"`: `.option("assign", config["kafka_assigned_partitions"])` (JSON string mapping topic to partition list, such as `'{"topicA":[0,1]}'`).
- `startingOffsets` -> `.option("startingOffsets", starting_offsets)` (`"latest"`, `"earliest"`, or per-partition JSON). Default to `"latest"` for streaming and `"earliest"` for batch.
- `endingOffsets` (batch mode only) -> `.option("endingOffsets", ending_offsets)` (`"latest"` or per-partition JSON). Never set `endingOffsets` on `spark.readStream`.
- `maxOffsetsPerTrigger` (streaming mode) -> `.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))` to rate-limit micro-batches.
- `minPartitions` -> `.option("minPartitions", str(min_partitions))` to increase fan-out parallelism across Spark tasks.
- `failOnDataLoss` -> `.option("failOnDataLoss", str(fail_on_data_loss).lower())` (`"true"` or `"false"`).
- `includeHeaders` -> `.option("includeHeaders", str(include_headers).lower())` when message headers are required.

## 4. SASL and SSL Authentication via Secret Manager

When the node configures SASL authentication (`securityProtocol` such as `"SASL_SSL"` or `"SASL_PLAINTEXT"`, `saslMechanism` such as `"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`, or `"OAUTHBEARER"`), resolve credentials once on the driver using the zero-dependency `get_secret()` (`access_secret_version()`) Secret Manager helper:
- Never embed raw passwords, API keys, or redaction placeholders in generated code.
- Pass Kafka client security properties with the `kafka.` prefix:
  - `.option("kafka.security.protocol", security_protocol)`
  - `.option("kafka.sasl.mechanism", sasl_mechanism)`
  - `.option("kafka.sasl.jaas.config", jaas_config)`

## 5. Binary Key/Value Casting and `ParsePayload` Handoff

The Kafka connector returns a fixed envelope schema: `key` (` binary`), `value` (`binary`), `topic` (`string`), `partition` (`int`), `offset` (`long`), `timestamp` (`timestamp`), `timestampType` (`int`), and optional `headers`.
- If a downstream `ParsePayload` step parses the payload, keep `value` (or cast `F.col("value").cast("string")` when `ParsePayload` expects a UTF-8 JSON/CSV/XML string) and preserve `topic`, `partition`, `offset`, and `timestamp` metadata columns when needed.
- If no `ParsePayload` step follows, cast `key` and `value` to `STRING` (`F.col("key").cast("string")`, `F.col("value").cast("string")`) or apply `F.from_json(F.col("value").cast("string"), schema)` according to the node schema.

## 6. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_secret(secret_version_name: str) -> str:
    """Fetches a UTF-8 secret payload from Google Cloud Secret Manager."""
    return access_secret_version(secret_version_name)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads Kafka records in streaming or batch mode."""
    dfs: dict[str, DataFrame] = {}

    read_mode = config.get("kafka_read_mode", "streaming")
    reader = (
        spark.read.format("kafka")
        if read_mode == "batch"
        else spark.readStream.format("kafka")
    )

    reader = (
        reader.option("kafka.bootstrap.servers", config["kafka_bootstrap_servers"])
        .option("subscribe", config["kafka_topics"])
        .option("startingOffsets", config.get("kafka_starting_offsets", "latest"))
        .option("failOnDataLoss", str(config.get("kafka_fail_on_data_loss", True)).lower())
    )

    if config.get("kafka_min_partitions"):
        reader = reader.option("minPartitions", str(config["kafka_min_partitions"]))

    if read_mode == "batch":
        reader = reader.option("endingOffsets", config.get("kafka_ending_offsets", "latest"))
    elif config.get("kafka_max_offsets_per_trigger"):
        reader = reader.option(
            "maxOffsetsPerTrigger", str(config["kafka_max_offsets_per_trigger"])
        )

    if config.get("kafka_password_secret"):
        username = config["kafka_username"]
        password = get_secret(config["kafka_password_secret"])
        jaas = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{username}" password="{password}";'
        )
        reader = (
            reader.option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", config.get("kafka_sasl_mechanism", "PLAIN"))
            .option("kafka.sasl.jaas.config", jaas)
        )

    raw_df = reader.load()
    dfs["src_kafka"] = raw_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp"),
    )
    return dfs
```

## 7. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline by constructing an in-memory DataFrame matching the output schema of `read_sources()` without connecting to a live Kafka broker:

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


def test_kafka_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory Kafka records."""
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
                "ord-1",
                '{"order_id": "ord-1", "amount": 42.5}',
                "orders",
                0,
                100,
                datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            )
        ],
        schema=schema,
    )
    result_dfs = transform({"src_kafka": input_df}, {})
    assert "src_kafka" in result_dfs
    assert result_dfs["src_kafka"].count() == 1
```
