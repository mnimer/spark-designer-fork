---
name: io-pulsar
description: Read and write batch and streaming messages with Apache Pulsar topics in PySpark 4.x using the StreamNative Pulsar Spark connector.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [PulsarSource, PulsarSink]
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

---

# Apache Pulsar Sink (`PulsarSink`)

Use these rules when generating PySpark {{spark_version}} code for an Apache Pulsar sink node (`PulsarSink` or `SINK:PulsarIO`).

## 1. Batch and Native Streaming Write Modes

`PulsarSink` supports native Structured Streaming and bounded batch execution using `io.streamnative.connectors:pulsar-spark-connector_2.13:4.0.0.1`:
- **Batch mode**: Write bounded DataFrames in `write_sinks(dfs, config)` with `pulsar_df.write.format("pulsar").options(...).save()`.
- **Native streaming mode (`mode == "native"` in `ExecutionPlan.streamingSinkGroups`)**: Start the streaming query with `pulsar_df.writeStream.format("pulsar").options(...)` along with `outputMode`, `trigger`, and `checkpointLocation`.
- **Grouped `foreachBatch` mode (`mode == "foreachBatch"`)**: If `ExecutionPlan.streamingSinkGroups` groups `PulsarSink` with other sinks, write inside `write_micro_batch(batch_df, batch_id)` using `batch_df.write.format("pulsar").options(...).save()`.

## 2. Metadata Columns (`__key`, `__eventTime`, `__topic`) and Payload Modes

Project DataFrame columns to match the StreamNative Pulsar Spark sink conventions before writing:
- **`__key` (optional, `STRING` or `BINARY`)**: If `keyColumn` is set, project `F.col(key_col).cast("string").alias("__key")` for Pulsar partition routing and topic compaction.
- **`__eventTime` (optional, `TIMESTAMP`)**: If `eventTimeColumn` is set, project `F.col(event_time_col).cast("timestamp").alias("__eventTime")`.
- **`topic` vs `__topic`**: Pass `.option("topic", config["pulsar_sink_topic"])` for a fixed destination topic, or project `F.col(topic_col).cast("string").alias("__topic")` when routing rows dynamically across Pulsar topics.
- **Payload Serialization (`payloadMode`)**:
  - `"json"` (default): Serialize non-metadata columns into a single UTF-8 JSON `value` column with `F.to_json(F.struct(*payload_cols)).alias("value")`.
  - `"schema"`: Keep typed DataFrame columns alongside `__key` and `__eventTime` so the Pulsar Spark connector registers and writes a structured Pulsar schema.
  - `"string"` or `"binary"`: Cast `valueColumn` to `"string"` or `"binary"` aliased as `"value"`.

## 3. JWT Token and OAuth2 Authentication via `get_secret()`

Resolve Pulsar credentials once on the driver in `write_sinks(dfs, config)` via `get_secret()`:
- **JWT Token (`authMode == "token"`)**:
  - `.option("pulsar.client.authPluginClassName", "org.apache.pulsar.client.impl.auth.AuthenticationToken")`
  - `.option("pulsar.client.authParams", f"token:{jwt_token}")`
- **OAuth2 (`authMode == "oauth2"`)**:
  - Fetch the service account key JSON via `get_secret()`, base64-encode it into an inline JSON data URI so no cleartext key file touches disk, and pass:
  - `.option("pulsar.client.authPluginClassName", "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2")`
  - `.option("pulsar.client.authParams", json.dumps({"type": "client_credentials", "issuerUrl": issuer_url, "audience": audience, "privateKey": data_uri}))`

## 4. Reference Sink Implementation (`src/pipeline.py`)

```python
import base64
import json
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_pulsar_sink_df(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    """Projects __key, __eventTime, __topic, and payload columns for PulsarSink."""
    payload_mode = config.get("pulsar_sink_payload_mode", "json")
    key_col = config.get("pulsar_sink_key_column", "")
    value_col = config.get("pulsar_sink_value_column", "")
    event_time_col = config.get("pulsar_sink_event_time_column", "")
    topic_col = config.get("pulsar_sink_topic_column", "")

    meta_exprs = []
    if key_col and key_col in df.columns:
        meta_exprs.append(F.col(key_col).cast("string").alias("__key"))
    if event_time_col and event_time_col in df.columns:
        meta_exprs.append(F.col(event_time_col).cast("timestamp").alias("__eventTime"))
    if topic_col and topic_col in df.columns:
        meta_exprs.append(F.col(topic_col).cast("string").alias("__topic"))

    if payload_mode == "schema":
        excluded = {c for c in (key_col, event_time_col, topic_col) if c}
        data_exprs = [F.col(c) for c in df.columns if c not in excluded]
        return df.select(*meta_exprs, *data_exprs)

    if payload_mode == "binary" and value_col:
        val_expr = F.col(value_col).cast("binary").alias("value")
    elif payload_mode == "string" and value_col:
        val_expr = F.col(value_col).cast("string").alias("value")
    else:
        excluded = {c for c in (topic_col,) if c}
        struct_cols = [F.col(c) for c in df.columns if c not in excluded]
        val_expr = F.to_json(F.struct(*struct_cols)).alias("value")

    return df.select(*meta_exprs, val_expr)


def build_pulsar_sink_options(config: dict[str, Any]) -> dict[str, str]:
    """Builds Pulsar service.url, topic, producer tuning, and authentication options."""
    opts: dict[str, str] = {
        "service.url": config["pulsar_sink_service_url"],
    }
    if config.get("pulsar_sink_admin_url"):
        opts["admin.url"] = config["pulsar_sink_admin_url"]
    if config.get("pulsar_sink_topic"):
        opts["topic"] = config["pulsar_sink_topic"]

    compression = config.get("pulsar_sink_compression_type", "NONE")
    if compression and compression != "NONE":
        opts["pulsar.producer.compressionType"] = compression
    opts["pulsar.producer.batchingEnabled"] = str(
        config.get("pulsar_sink_batching_enabled", True)
    ).lower()

    auth_mode = config.get("pulsar_sink_auth_mode", "none")
    secret_name = config.get("pulsar_sink_secret_name", "")
    if auth_mode == "token" and secret_name:
        token = get_secret(secret_name)
        opts["pulsar.client.authPluginClassName"] = (
            "org.apache.pulsar.client.impl.auth.AuthenticationToken"
        )
        opts["pulsar.client.authParams"] = f"token:{token}"
    elif auth_mode == "oauth2" and secret_name:
        key_json = get_secret(secret_name)
        encoded_key = base64.b64encode(key_json.encode("utf-8")).decode("ascii")
        auth_params = json.dumps(
            {
                "type": "client_credentials",
                "issuerUrl": config["pulsar_sink_oauth_issuer_url"],
                "audience": config["pulsar_sink_oauth_audience"],
                "privateKey": f"data:application/json;base64,{encoded_key}",
            }
        )
        opts["pulsar.client.authPluginClassName"] = (
            "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2"
        )
        opts["pulsar.client.authParams"] = auth_params
    return opts


def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes output DataFrame records to Apache Pulsar in batch or streaming mode."""
    pulsar_df = prepare_pulsar_sink_df(dfs["step_out"], config)
    pulsar_opts = build_pulsar_sink_options(config)

    if pulsar_df.isStreaming:
        checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
        checkpoint_loc = config.get("pulsar_sink_checkpoint") or (
            f"{checkpoint_root.rstrip('/')}/snk_pulsar"
        )
        (
            pulsar_df.writeStream.format("pulsar")
            .options(**pulsar_opts)
            .outputMode(config.get("pulsar_sink_output_mode", "append"))
            .option("checkpointLocation", checkpoint_loc)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
            .start()
        )
        pulsar_df.sparkSession.streams.awaitAnyTermination()
    else:
        pulsar_df.write.format("pulsar").options(**pulsar_opts).save()
```
