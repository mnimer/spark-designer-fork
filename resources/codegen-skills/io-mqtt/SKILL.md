---
name: io-mqtt
description: Read streaming IoT messages from MQTT brokers in PySpark 4.x using the Python DataSourceStreamReader API and paho-mqtt.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [MqttSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
    - https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html
  validated: true
---

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
