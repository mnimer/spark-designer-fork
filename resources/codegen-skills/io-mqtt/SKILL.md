---
name: io-mqtt
description: Read and write IoT messages with MQTT brokers in PySpark 4.x using the Python DataSource API, foreachBatch, and paho-mqtt.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [MqttSource, MqttSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
    - https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
# MQTT Streaming Source (`MqttSource`)

Use these rules when generating PySpark {{spark_version}} code for an MQTT broker source node (`MqttSource` or `MqttIO`).

## 1. Runtime Delivery Mechanism

MQTT does not ship a built-in Spark 4.x Scala 2.13 JVM connector. Always implement and register a Spark 4.0 Python `DataSource` (`MqttDataSource` with `DataSourceStreamReader` from `pyspark.sql.datasource`) using `paho-mqtt`:
- Include `paho-mqtt>=2.1.0` in `requirements.txt` and `Dockerfile`.
- Register `MqttDataSource` on the active session with `spark.dataSource.register(MqttDataSource)` inside `read_sources(spark, config)` and read with `spark.readStream.format("mqtt")`.

## 2. Supported Node Properties

Map the `MqttSource` node properties to `.format("mqtt")` options:
- `brokerUrl` -> `.option("brokerUrl", config["mqtt_broker_url"])` (such as `"tcp://broker.hivemq.com:1883"` or `"ssl://mqtt.example.com:8883"`)
- `topic` -> `.option("topic", config["mqtt_topic"])` (such as `"sensors/#"`)
- `clientId` -> `.option("clientId", config.get("mqtt_client_id", "spark-mqtt-reader"))`
- `qos` -> `.option("qos", str(config.get("mqtt_qos", 1)))` (`0`, `1`, or `2`)
- `cleanSession` -> `.option("cleanSession", str(config.get("mqtt_clean_session", True)).lower())`
- `keepAliveSeconds` -> `.option("keepAliveSeconds", str(config.get("mqtt_keep_alive_seconds", 60)))`

## 3. Broker Authentication via Secret Manager

When `username` and `passwordSecret` are configured on the MQTT node, resolve the password once on the driver in `read_sources(spark, config)` using the zero-dependency `get_secret()` (`access_secret_version()`) Secret Manager helper:
- Pass `username` and `password` through DataSource options and configure `client.username_pw_set(username, password)` on the `paho.mqtt.client.Client` instance inside `read(self, partition)`.

## 4. Reference Implementation (`src/pipeline.py`)

```python
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Any, Iterator, Sequence
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.datasource import (
    DataSource,
    DataSourceStreamReader,
    InputPartition,
)
from pyspark.sql.types import StructType


def get_secret(secret_version_name: str) -> str:
    """Fetches a UTF-8 secret payload from Google Cloud Secret Manager."""
    return access_secret_version(secret_version_name)


@dataclass(frozen=True)
class MqttMicroBatchPartition(InputPartition):
    """Serializable micro-batch partition for MQTT topic polling."""

    batch_id: int
    max_messages: int


class MqttStreamReader(DataSourceStreamReader):
    """Spark 4.0 DataSourceStreamReader for MQTT topics via paho-mqtt."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)
        self._batch_seq = 0

    def initialOffset(self) -> dict[str, int]:
        return {"batch": 0}

    def latestOffset(self) -> dict[str, int]:
        self._batch_seq += 1
        return {"batch": self._batch_seq}

    def partitions(self, start: dict[str, int], end: dict[str, int]) -> Sequence[InputPartition]:
        start_b = int(start.get("batch", 0))
        end_b = int(end.get("batch", 0))
        if end_b <= start_b:
            return []
        max_msgs = int(self.options.get("maxMessagesPerBatch", "200"))
        return [MqttMicroBatchPartition(batch_id=end_b, max_messages=max_msgs)]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        import paho.mqtt.client as mqtt

        max_msgs = partition.max_messages if isinstance(partition, MqttMicroBatchPartition) else 200
        broker_url = self.options["brokerUrl"]
        parsed = urlparse(broker_url if "://" in broker_url else f"tcp://{broker_url}")
        host = parsed.hostname or "localhost"
        port = parsed.port or (8883 if parsed.scheme in ("ssl", "tls", "mqtts") else 1883)
        topic = self.options["topic"]
        qos = int(self.options.get("qos", "1"))
        client_id = self.options.get("clientId", "spark-mqtt-reader")
        keep_alive = int(self.options.get("keepAliveSeconds", "60"))

        buffer: list[tuple[int, str, bytes, str]] = []

        def on_message(_client: Any, _userdata: Any, msg: Any) -> None:
            ts = datetime.now(tz=timezone.utc).isoformat()
            buffer.append((int( getattr(msg, "mid", 0)), str(msg.topic), bytes(msg.payload), ts))

        client = mqtt.Client(client_id=client_id)
        if self.options.get("username"):
            client.username_pw_set(self.options["username"], self.options.get("password"))
        if parsed.scheme in ("ssl", "tls", "mqtts"):
            client.tls_set()

        client.on_message = on_message
        client.connect(host, port, keepalive=keep_alive)
        client.subscribe(topic, qos=qos)
        client.loop_start()
        deadline = time.monotonic() + float(self.options.get("pollDurationSeconds", "2.0"))
        try:
            while time.monotonic() < deadline and len(buffer) < max_msgs:
                time.sleep(0.05)
        finally:
            client.loop_stop()
            client.disconnect()

        for msg_id, msg_topic, payload_bytes, ts in buffer:
            yield (
                msg_id,
                msg_topic,
                payload_bytes.decode("utf-8", errors="replace"),
                ts,
            )

    def commit(self, end: dict[str, int]) -> None:
        pass


class MqttDataSource(DataSource):
    """Registered Spark 4.0 Python DataSource for MQTT."""

    @classmethod
    def name(cls) -> str:
        return "mqtt"

    def schema(self) -> str:
        return "id INT, topic STRING, payload STRING, timestamp STRING"

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        return MqttStreamReader(schema, self.options)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Registers MqttDataSource and reads a streaming MQTT DataFrame."""
    spark.dataSource.register(MqttDataSource)
    dfs: dict[str, DataFrame] = {}

    reader = (
        spark.readStream.format("mqtt")
        .option("brokerUrl", config["mqtt_broker_url"])
        .option("topic", config["mqtt_topic"])
        .option("clientId", config.get("mqtt_client_id", "spark-mqtt-reader"))
        .option("qos", str(config.get("mqtt_qos", 1)))
        .option("cleanSession", str(config.get("mqtt_clean_session", True)).lower())
        .option("keepAliveSeconds", str(config.get("mqtt_keep_alive_seconds", 60)))
    )
    if config.get("mqtt_username"):
        reader = reader.option("username", config["mqtt_username"])
    if config.get("mqtt_password_secret"):
        reader = reader.option("password", get_secret(config["mqtt_password_secret"]))

    dfs["src_mqtt"] = reader.load()
    return dfs
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline with an in-memory DataFrame matching the `MqttDataSource` schema without connecting to an MQTT broker:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.pipeline import transform


def test_mqtt_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory MQTT messages."""
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("topic", StringType(), False),
            StructField("payload", StringType(), True),
            StructField("timestamp", StringType(), False),
        ]
    )
    input_df = spark.createDataFrame(
        [(1, "sensors/temperature", '{"sensor_id": "s-1", "temp_c": 23.8}', "2026-01-01T00:00:00Z")],
        schema=schema,
    )
    result_dfs = transform({"src_mqtt": input_df}, {})
    assert "src_mqtt" in result_dfs
    assert result_dfs["src_mqtt"].count() == 1
```

---

# MQTT Sink (`MqttSink`)

Use these rules when generating PySpark {{spark_version}} code for an MQTT broker sink node (`MqttSink`, `SINK:MqttIO`, or `WriteToMqtt`).

## 1. Batch (`foreachPartition` or `mapInPandas`) and Streaming (`foreachBatch`) Execution

`MqttSink` has `streamingSinkMode: "foreachBatch"` and publishes messages per executor partition using `paho-mqtt`:
- Include `paho-mqtt>=2.1.0` in `requirements.txt` and `Dockerfile`.
- **Batch mode**: Execute `df.foreachPartition(publish_mqtt_partition)` (or `df.mapInPandas(publish_mqtt_batches, schema="published LONG").count()`) so each Spark partition opens a single broker connection rather than reconnecting on every row.
- **Streaming mode**: Wrap writes inside `.writeStream.foreachBatch(write_micro_batch)` per `ExecutionPlan.streamingSinkGroups`. Cache `batch_df` when `requiresBatchCache` is true, publish each micro-batch partition via `paho-mqtt`, and call `batch_df.unpersist()` inside a `finally` block.

## 2. Per-Partition `paho-mqtt` Publisher Rules (QoS, Retain, TLS, and Auth)

Enforce these connection and delivery rules inside the partition publisher:
- **Unique Client ID per Partition**: Combine `clientId` prefix (default `"spark-mqtt-publisher"`) with a short UUID suffix (`uuid.uuid4().hex[:8]`) so parallel executor tasks never kick one another off the broker.
- **Concrete Publish Topic**: Use `topicColumn` per row when configured, falling back to `topic`. Validate that the target topic never contains `+` or `#` MQTT subscription wildcards.
- **QoS and Retain**: Pass `qos=int(sink_opts.get("qos", "1"))` (`0`, `1`, or `2`) and `retain=(sink_opts.get("retain", "false") == "true")` to `client.publish(...)`. If `qos > 0`, call `msg_info.wait_for_publish(timeout=30.0)` before disconnecting so broker PUBACK or PUBCOMP handshakes complete.
- **TLS and Secret Manager Authentication**: Resolve broker credentials once on the driver in `write_sinks(dfs, config)` via `get_secret()`. If `username` or `password` is present, call `client.username_pw_set(username, password)`. If the broker scheme is `ssl`, `tls`, or `mqtts` (or `tlsEnabled` is `"true"`), call `client.tls_set()` before `client.connect(...)`.

## 3. Reference Sink Implementation (`src/pipeline.py`)

```python
import json
import uuid
from typing import Any, Iterable, Iterator
from urllib.parse import urlparse
import pandas as pd
from pyspark.sql import DataFrame


def _resolve_mqtt_sink_options(config: dict[str, Any]) -> dict[str, str]:
    """Resolves MQTT broker credentials once on the driver for executor tasks."""
    opts: dict[str, str] = {
        "brokerUrl": config["mqtt_sink_broker_url"],
        "topic": config.get("mqtt_sink_topic", ""),
        "topicColumn": config.get("mqtt_sink_topic_column", ""),
        "payloadFormat": config.get("mqtt_sink_payload_format", "json"),
        "payloadColumn": config.get("mqtt_sink_payload_column", ""),
        "clientId": config.get("mqtt_sink_client_id", "spark-mqtt-publisher"),
        "qos": str(config.get("mqtt_sink_qos", 1)),
        "retain": str(config.get("mqtt_sink_retain", False)).lower(),
        "keepAliveSeconds": str(config.get("mqtt_sink_keep_alive_seconds", 60)),
        "tlsEnabled": str(config.get("mqtt_sink_tls_enabled", False)).lower(),
    }
    if config.get("mqtt_sink_username"):
        opts["username"] = config["mqtt_sink_username"]
    if config.get("mqtt_sink_secret_name"):
        opts["password"] = get_secret(config["mqtt_sink_secret_name"])
    return opts


def _publish_mqtt_records(records: Iterable[dict[str, Any]], sink_opts: dict[str, str]) -> int:
    """Opens one paho-mqtt connection for a partition and publishes all records."""
    import paho.mqtt.client as mqtt

    broker_url = sink_opts["brokerUrl"]
    parsed = urlparse(broker_url if "://" in broker_url else f"tcp://{broker_url}")
    host = parsed.hostname or "localhost"
    use_tls = parsed.scheme in ("ssl", "tls", "mqtts") or sink_opts.get("tlsEnabled") == "true"
    port = parsed.port or (8883 if use_tls else 1883)
    default_topic = sink_opts.get("topic", "")
    topic_col = sink_opts.get("topicColumn", "")
    payload_fmt = sink_opts.get("payloadFormat", "json")
    payload_col = sink_opts.get("payloadColumn", "")
    qos = int(sink_opts.get("qos", "1"))
    retain = sink_opts.get("retain", "false") == "true"
    keep_alive = int(sink_opts.get("keepAliveSeconds", "60"))
    client_prefix = sink_opts.get("clientId") or "spark-mqtt-publisher"

    client = mqtt.Client(client_id=f"{client_prefix}-{uuid.uuid4().hex[:8]}")
    if sink_opts.get("username") or sink_opts.get("password"):
        client.username_pw_set(sink_opts.get("username", ""), sink_opts.get("password"))
    if use_tls:
        client.tls_set()

    published = 0
    connected = False
    try:
        for row_dict in records:
            if not connected:
                client.connect(host, port, keepalive=keep_alive)
                client.loop_start()
                connected = True

            target_topic = (
                str(row_dict[topic_col])
                if topic_col and row_dict.get(topic_col) is not None
                else default_topic
            )
            if "+" in target_topic or "#" in target_topic or not target_topic:
                raise ValueError(f"Invalid concrete MQTT publish topic: {target_topic}")

            if payload_fmt == "binary" and payload_col and row_dict.get(payload_col) is not None:
                raw = row_dict[payload_col]
                payload_bytes = bytes(raw) if isinstance(raw, (bytes, bytearray)) else str(raw).encode("utf-8")
            elif payload_fmt == "string" and payload_col:
                payload_bytes = str(row_dict.get(payload_col, "")).encode("utf-8")
            else:
                payload_bytes = json.dumps(row_dict, default=str).encode("utf-8")

            msg_info = client.publish(target_topic, payload=payload_bytes, qos=qos, retain=retain)
            if qos > 0:
                msg_info.wait_for_publish(timeout=30.0)
            published += 1
    finally:
        if connected:
            client.loop_stop()
            client.disconnect()
    return published


def _write_mqtt_df_pandas(df: DataFrame, sink_opts: dict[str, str]) -> None:
    """Publishes DataFrame partitions to MQTT using mapInPandas."""

    def publish_batches(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        def iter_rows() -> Iterator[dict[str, Any]]:
            for pdf in pdf_iter:
                for rec in pdf.to_dict(orient="records"):
                    yield rec

        count = _publish_mqtt_records(iter_rows(), sink_opts)
        yield pd.DataFrame({"published": [count]})

    df.mapInPandas(publish_batches, schema="published LONG").count()


def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes output DataFrame records to an MQTT broker in batch or streaming mode."""
    target_df = dfs["step_out"]
    sink_opts = _resolve_mqtt_sink_options(config)

    if target_df.isStreaming:
        checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
        checkpoint_loc = config.get("mqtt_sink_checkpoint") or (
            f"{checkpoint_root.rstrip('/')}/snk_mqtt"
        )

        def write_micro_batch(batch_df: DataFrame, _batch_id: int) -> None:
            batch_df.cache()
            try:
                batch_df.foreachPartition(
                    lambda rows: _publish_mqtt_records(
                        (r.asDict(recursive=True) for r in rows), sink_opts
                    )
                )
            finally:
                batch_df.unpersist()

        (
            target_df.writeStream.foreachBatch(write_micro_batch)
            .outputMode(config.get("mqtt_sink_output_mode", "append"))
            .option("checkpointLocation", checkpoint_loc)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
            .start()
        )
        target_df.sparkSession.streams.awaitAnyTermination()
    else:
        _write_mqtt_df_pandas(target_df, sink_opts)
```
