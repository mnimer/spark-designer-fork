---
name: io-eventhubs
description: Read and write batch and streaming events with Azure Event Hubs in PySpark 4.x using the Kafka-compatible SASL_SSL endpoint.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [EventHubsSource, EventHubsSink]
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

---

# Azure Event Hubs Sink (`EventHubsSink`)

Use these rules when generating PySpark {{spark_version}} code for an Azure Event Hubs sink node (`EventHubsSink`).

## 1. Batch and Native Streaming Write Modes over Port 9093

`EventHubsSink` publishes events through Spark's `.format("kafka")` writer (`org.apache.spark:spark-sql-kafka-0-10_2.13:{{spark_version}}`) targeting the Azure Event Hubs Kafka endpoint `<namespace>.servicebus.windows.net:9093`:
- If `eventHubNamespace` already contains `.servicebus.windows.net`, strip any `:9093` suffix and append `:9093` once so the host never duplicates the domain.
- **Batch mode**: Write bounded DataFrames in `write_sinks(dfs, config)` using `eh_df.write.format("kafka").options(...).save()`.
- **Native streaming mode (`mode == "native"` in `ExecutionPlan.streamingSinkGroups`)**: Start the streaming query with `eh_df.writeStream.format("kafka").options(...)` along with `outputMode`, `trigger`, and `checkpointLocation`.
- **Grouped `foreachBatch` mode (`mode == "foreachBatch"`)**: If `ExecutionPlan.streamingSinkGroups` groups `EventHubsSink` with other sinks, write inside `write_micro_batch(batch_df, batch_id)` using `batch_df.write.format("kafka").options(...).save()`.

## 2. SASL_SSL `$ConnectionString` Authentication via `get_secret()`

Resolve the Event Hubs Shared Access Signature connection string once on the driver in `write_sinks(dfs, config)` via `get_secret()`:
- Configure `.option("kafka.bootstrap.servers", bootstrap_servers)` and `.option("topic", eventhub_name)`.
- Set `.option("kafka.security.protocol", "SASL_SSL")` and `.option("kafka.sasl.mechanism", "PLAIN")`.
- Build `jaas_config` with `username="$ConnectionString"` and `password` equal to the secret payload resolved from `get_secret()`.
- Set `.option("kafka.request.timeout.ms", str(request_timeout_ms))` (default `"60000"`) and `.option("kafka.acks", acks)` (default `"all"`). Avoid unsupported Kafka compression codecs such as `snappy` or `lz4` against Event Hubs endpoints.

## 3. Envelope Projection (`key`, `value`, `headers`)

Project DataFrame columns into the Kafka envelope expected by Azure Event Hubs:
- **`key` (optional partition key)**: If `partitionKeyColumn` is configured, project `F.col(pk_col).cast("string").alias("key")` so Azure Event Hubs hashes the key to a consistent partition.
- **`value` (required payload)**: If `valueFormat == "json"` (default) and `valueColumn` is empty, serialize row columns with `F.to_json(F.struct(*payload_cols)).alias("value")`. Otherwise cast `valueColumn` to `"string"` or `"binary"` aliased as `"value"`.
- **`headers` (optional custom properties)**: If `includeHeaders` is true and `headersColumn` is present, project `F.col(headers_col).alias("headers")` and set `.option("includeHeaders", "true")`.

## 4. Reference Sink Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _resolve_eventhubs_bootstrap(namespace_or_fqdn: str) -> str:
    """Normalizes an Azure Event Hubs namespace or FQDN to <host>:9093."""
    cleaned = namespace_or_fqdn.strip().split(":")[0]
    if cleaned.endswith(".servicebus.windows.net"):
        return f"{cleaned}:9093"
    return f"{cleaned}.servicebus.windows.net:9093"


def prepare_eventhubs_sink_df(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    """Projects key, value, and optional headers columns for EventHubsSink."""
    value_format = config.get("eventhub_sink_value_format", "json")
    pk_col = config.get("eventhub_sink_partition_key_column", "")
    value_col = config.get("eventhub_sink_value_column", "")
    headers_col = config.get("eventhub_sink_headers_column", "")

    exprs = []
    if pk_col and pk_col in df.columns:
        exprs.append(F.col(pk_col).cast("string").alias("key"))

    if value_format == "binary" and value_col:
        exprs.append(F.col(value_col).cast("binary").alias("value"))
    elif value_format == "string" and value_col:
        exprs.append(F.col(value_col).cast("string").alias("value"))
    else:
        excluded = {c for c in (headers_col,) if c}
        struct_cols = [F.col(c) for c in df.columns if c not in excluded]
        exprs.append(F.to_json(F.struct(*struct_cols)).alias("value"))

    if config.get("eventhub_sink_include_headers") and headers_col and headers_col in df.columns:
        exprs.append(F.col(headers_col).alias("headers"))

    return df.select(*exprs)


def build_eventhubs_sink_options(config: dict[str, Any]) -> dict[str, str]:
    """Builds SASL_SSL Kafka producer options targeting Azure Event Hubs on port 9093."""
    bootstrap_servers = _resolve_eventhubs_bootstrap(config["eventhub_sink_namespace"])
    connection_string = get_secret(config["eventhub_sink_secret_name"])
    jaas_config = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{connection_string}";'
    )
    opts: dict[str, str] = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "topic": config["eventhub_sink_name"],
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config,
        "kafka.acks": str(config.get("eventhub_sink_acks", "all")),
        "kafka.request.timeout.ms": str(
            config.get("eventhub_sink_request_timeout_ms", 60000)
        ),
    }
    if config.get("eventhub_sink_include_headers"):
        opts["includeHeaders"] = "true"
    return opts


def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes output DataFrame records to Azure Event Hubs in batch or streaming mode."""
    eh_df = prepare_eventhubs_sink_df(dfs["step_out"], config)
    eh_opts = build_eventhubs_sink_options(config)

    if eh_df.isStreaming:
        checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
        checkpoint_loc = config.get("eventhub_sink_checkpoint") or (
            f"{checkpoint_root.rstrip('/')}/snk_eventhubs"
        )
        (
            eh_df.writeStream.format("kafka")
            .options(**eh_opts)
            .outputMode(config.get("eventhub_sink_output_mode", "append"))
            .option("checkpointLocation", checkpoint_loc)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
            .start()
        )
        eh_df.sparkSession.streams.awaitAnyTermination()
    else:
        eh_df.write.format("kafka").options(**eh_opts).save()
```
