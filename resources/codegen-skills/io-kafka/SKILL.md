---
name: io-kafka
description: Read and write batch and streaming records with Apache Kafka topics in PySpark 4.x using spark-sql-kafka-0-10.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [KafkaSource, KafkaSink]
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

---

# Apache Kafka Sink (`KafkaSink`)

Use these rules when generating PySpark {{spark_version}} code for an Apache Kafka sink node (`KafkaSink`, `SINK:KafkaIO`, or `WriteToKafka`).

## 1. Batch and Native Streaming Write Modes

`KafkaSink` supports native Structured Streaming and bounded batch execution via `org.apache.spark:spark-sql-kafka-0-10_2.13:{{spark_version}}`:
- **Batch mode**: Write bounded DataFrames in `write_sinks(dfs, config)` using `kafka_df.write.format("kafka").options(...).save()`.
- **Native streaming mode (`mode == "native"` in `ExecutionPlan.streamingSinkGroups`)**: Start the streaming query with `kafka_df.writeStream.format("kafka").options(...)`, applying `outputMode`, `trigger`, and `checkpointLocation` via `configure_stream_writer(...)` before `.start()`.
- **Grouped `foreachBatch` mode (`mode == "foreachBatch"`)**: If `ExecutionPlan.streamingSinkGroups` groups `KafkaSink` with other sinks on a shared streaming branch, write each micro-batch inside `write_micro_batch(batch_df, batch_id)` using `batch_df.write.format("kafka").options(...).save()`.

## 2. Envelope Projection (`key`, `value`, `topic`, `partition`, `headers`)

Project the DataFrame to match the Spark Kafka writer contract before calling `.write.format("kafka")` or `.writeStream.format("kafka")`:
- **`value` (required, `STRING` or `BINARY`)**:
  - If `valueFormat == "json"` (default) and `valueColumn` is empty, serialize all payload columns into a JSON string with `F.to_json(F.struct(*payload_cols)).alias("value")`.
  - If `valueFormat == "binary"`, cast the configured `valueColumn` with `F.col(value_col).cast("binary").alias("value")`.
  - Otherwise cast `valueColumn` (default `"value"`) with `F.col(value_col).cast("string").alias("value")`.
- **`key` (optional, `STRING` or `BINARY`)**: If `keyColumn` is set, project `F.col(key_col).cast("string").alias("key")` for deterministic Kafka partition routing and log compaction.
- **`topic` (destination option or per-row `STRING` column)**:
  - If a static `topic` is configured, pass `.option("topic", config["kafka_sink_topic"])`.
  - If `topicColumn` is configured, project `F.col(topic_col).cast("string").alias("topic")` so each row routes dynamically to its target topic.
- **`partition` (optional, `INT` column)**: If `partitionColumn` is configured, project `F.col(partition_col).cast("int").alias("partition")`.
- **`headers` (optional, `ARRAY<STRUCT<key: STRING, value: BINARY>>`)**: If `includeHeaders` is true and `headersColumn` is present, project `F.col(headers_col).alias("headers")` and set `.option("includeHeaders", "true")`.

## 3. Producer Reliability, Compression, and SASL/SSL Security

Map `KafkaSink` producer and authentication settings to `kafka.*` writer options in `write_sinks(dfs, config)`:
- `bootstrapServers` -> `.option("kafka.bootstrap.servers", config["kafka_sink_bootstrap_servers"])`
- `acks` -> `.option("kafka.acks", config.get("kafka_sink_acks", "all"))`
- `compressionType` -> if not `"none"`, set `.option("kafka.compression.type", compression_type)` (`"snappy"`, `"lz4"`, `"zstd"`, or `"gzip"`)
- `enableIdempotence` -> `.option("kafka.enable.idempotence", str(enable_idempotence).lower())`
- **SASL/SSL via `get_secret()`**: If `secretName` (or `authMode == "secretManager"`) is configured, resolve the secret once on the driver in `write_sinks(dfs, config)` via `get_secret()`. Configure `kafka.security.protocol`, `kafka.sasl.mechanism` (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, or `OAUTHBEARER`), and `kafka.sasl.jaas.config`. Never hardcode credentials or emit redaction placeholders.

## 4. Reference Sink Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_kafka_sink_envelope(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    """Projects DataFrame columns into the Kafka key, value, topic, and headers envelope."""
    value_format = config.get("kafka_sink_value_format", "json")
    key_col = config.get("kafka_sink_key_column", "")
    value_col = config.get("kafka_sink_value_column", "")
    topic_col = config.get("kafka_sink_topic_column", "")
    partition_col = config.get("kafka_sink_partition_column", "")
    headers_col = config.get("kafka_sink_headers_column", "")

    select_exprs = []
    if key_col and key_col in df.columns:
        select_exprs.append(F.col(key_col).cast("string").alias("key"))

    if value_format == "binary" and value_col:
        select_exprs.append(F.col(value_col).cast("binary").alias("value"))
    elif value_format == "string" and value_col:
        select_exprs.append(F.col(value_col).cast("string").alias("value"))
    elif value_col and value_col in df.columns and value_format != "json":
        select_exprs.append(F.col(value_col).cast("string").alias("value"))
    else:
        excluded = {c for c in (topic_col, partition_col, headers_col) if c}
        struct_cols = [F.col(c) for c in df.columns if c not in excluded]
        select_exprs.append(F.to_json(F.struct(*struct_cols)).alias("value"))

    if topic_col and topic_col in df.columns:
        select_exprs.append(F.col(topic_col).cast("string").alias("topic"))
    if partition_col and partition_col in df.columns:
        select_exprs.append(F.col(partition_col).cast("int").alias("partition"))
    if config.get("kafka_sink_include_headers") and headers_col and headers_col in df.columns:
        select_exprs.append(F.col(headers_col).alias("headers"))

    return df.select(*select_exprs)


def build_kafka_writer_options(config: dict[str, Any]) -> dict[str, str]:
    """Builds Spark Kafka producer and SASL/SSL authentication options."""
    opts: dict[str, str] = {
        "kafka.bootstrap.servers": config["kafka_sink_bootstrap_servers"],
        "kafka.acks": str(config.get("kafka_sink_acks", "all")),
        "kafka.enable.idempotence": str(
            config.get("kafka_sink_enable_idempotence", True)
        ).lower(),
    }
    if config.get("kafka_sink_topic"):
        opts["topic"] = config["kafka_sink_topic"]
    compression = config.get("kafka_sink_compression_type", "none")
    if compression and compression != "none":
        opts["kafka.compression.type"] = compression
    if config.get("kafka_sink_include_headers"):
        opts["includeHeaders"] = "true"

    security_protocol = config.get("kafka_sink_security_protocol", "PLAINTEXT")
    opts["kafka.security.protocol"] = security_protocol
    if config.get("kafka_sink_secret_name") and security_protocol.startswith("SASL"):
        mechanism = config.get("kafka_sink_sasl_mechanism", "PLAIN")
        secret_val = get_secret(config["kafka_sink_secret_name"])
        username = config.get("kafka_sink_sasl_username", "")
        login_module = (
            "org.apache.kafka.common.security.scram.ScramLoginModule"
            if mechanism.startswith("SCRAM")
            else "org.apache.kafka.common.security.plain.PlainLoginModule"
        )
        jaas = (
            f'{login_module} required username="{username}" password="{secret_val}";'
            if username
            else secret_val
        )
        opts["kafka.sasl.mechanism"] = mechanism
        opts["kafka.sasl.jaas.config"] = jaas
    return opts


def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes output DataFrame records to Apache Kafka in batch or streaming mode."""
    payload_df = prepare_kafka_sink_envelope(dfs["step_out"], config)
    kafka_opts = build_kafka_writer_options(config)

    if payload_df.isStreaming:
        checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
        checkpoint_loc = config.get("kafka_sink_checkpoint") or (
            f"{checkpoint_root.rstrip('/')}/snk_kafka"
        )
        writer = (
            payload_df.writeStream.format("kafka")
            .options(**kafka_opts)
            .outputMode(config.get("kafka_sink_output_mode", "append"))
            .option("checkpointLocation", checkpoint_loc)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
        )
        writer.start()
        payload_df.sparkSession.streams.awaitAnyTermination()
    else:
        payload_df.write.format("kafka").options(**kafka_opts).save()
```
