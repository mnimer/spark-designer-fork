---
name: io-pubsub
description: Read batch and streaming messages from Google Cloud Pub/Sub subscriptions in PySpark 4.x using the Python DataSource API.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [PubSubSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
    - https://cloud.google.com/python/docs/reference/pubsub/latest
  validated: true
---

# Google Cloud Pub/Sub Source (`PubSubSource`)

Use these rules when generating PySpark {{spark_version}} code for a Google Cloud Pub/Sub source node (`PubSubSource` or `PubSubIO`).

## 1. Runtime Delivery Mechanism

Google Cloud Pub/Sub does not publish a Spark 4.x Scala 2.13 JVM connector. Always implement and register a Spark 4.0 Python `DataSource` (`PubSubDataSource` with `DataSourceStreamReader` and `DataSourceReader` from `pyspark.sql.datasource`) backed by `google-cloud-pubsub`:
- Include `google-cloud-pubsub>=2.21.0` in `requirements.txt` and `Dockerfile`.
- Register `PubSubDataSource` on the active session with `spark.dataSource.register(PubSubDataSource)` inside `read_sources(spark, config)`.

## 2. Batch and Streaming Read Modes (`SimplePartition`)

Implement both `reader()` and `streamReader()` on `PubSubDataSource` using synchronous micro-batch pulls (`SubscriberClient().pull()`):
- Instantiate `pubsub_v1.SubscriberClient()` lazily inside `read(self, partition)` on the executor worker so driver-to-worker serialization remains picklable.
- Use `SimplePartition` (or a frozen `InputPartition` dataclass) to partition synchronous pull calls across tasks.
- Acknowledge pulled messages on the executor after yielding all records in the batch when `acknowledgeMessages` is enabled (default `"true"`).
- Support both `readMode == "streaming"` (`spark.readStream.format("pubsub")`) and `readMode == "batch"` (`spark.read.format("pubsub")`).

## 3. Supported Node Properties

Map the `PubSubSource` node properties to `.format("pubsub")` options:
- `projectId` -> `.option("projectId", config["pubsub_project_id"])`
- `subscription` -> `.option("subscription", config["pubsub_subscription"])` (accepts either a short subscription ID or a full `projects/<project>/subscriptions/<sub>` path)
- `maxMessagesPerBatch` -> `.option("maxMessagesPerBatch", str(config.get("pubsub_max_messages", 500)))`
- `includeAttributes` -> `.option("includeAttributes", str(config.get("pubsub_include_attributes", True)).lower())`

## 4. Reference Implementation (`src/pipeline.py`)

```python
import json
from dataclasses import dataclass
from typing import Any, Iterator, Sequence
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    InputPartition,
)
from pyspark.sql.types import StructType


@dataclass(frozen=True)
class SimplePartition(InputPartition):
    """Serializable micro-batch pull partition for Cloud Pub/Sub."""

    partition_id: int
    max_messages: int


def _resolve_subscription_path(subscriber: Any, project_id: str, subscription: str) -> str:
    """Returns a fully qualified Pub/Sub subscription resource path."""
    if subscription.startswith("projects/"):
        return subscription
    return subscriber.subscription_path(project_id, subscription)


def _pull_pubsub_batch(options: dict[str, str], max_messages: int) -> Iterator[tuple[Any, ...]]:
    """Pulls and acknowledges a batch of messages from Cloud Pub/Sub on the executor."""
    from google.cloud import pubsub_v1

    project_id = options["projectId"]
    subscription = options["subscription"]
    include_attributes = options.get("includeAttributes", "true").lower() == "true"

    subscriber = pubsub_v1.SubscriberClient()
    sub_path = _resolve_subscription_path(subscriber, project_id, subscription)
    response = subscriber.pull(
        request={"subscription": sub_path, "max_messages": max_messages},
        timeout=15.0,
    )
    ack_ids: list[str] = []
    for received in response.received_messages:
        msg = received.message
        ack_ids.append(received.ack_id)
        attrs_json = json.dumps(dict(msg.attributes)) if include_attributes and msg.attributes else "{}"
        yield (
            msg.message_id,
            bytes(msg.data).decode("utf-8", errors="replace"),
            msg.publish_time.isoformat() if msg.publish_time else None,
            attrs_json,
        )
    if ack_ids:
        subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})
    subscriber.close()


class PubSubBatchReader(DataSourceReader):
    """Spark 4.0 batch DataSourceReader for Cloud Pub/Sub."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)

    def partitions(self) -> Sequence[InputPartition]:
        max_messages = int(self.options.get("maxMessagesPerBatch", "500"))
        return [SimplePartition(partition_id=0, max_messages=max_messages)]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        max_msgs = partition.max_messages if isinstance(partition, SimplePartition) else 500
        yield from _pull_pubsub_batch(self.options, max_msgs)


class PubSubStreamReader(DataSourceStreamReader):
    """Spark 4.0 streaming DataSourceStreamReader for Cloud Pub/Sub."""

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
        if int(end.get("batch", 0)) <= int(start.get("batch", 0)):
            return []
        max_messages = int(self.options.get("maxMessagesPerBatch", "500"))
        return [SimplePartition(partition_id=0, max_messages=max_messages)]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        max_msgs = partition.max_messages if isinstance(partition, SimplePartition) else 500
        yield from _pull_pubsub_batch(self.options, max_msgs)

    def commit(self, end: dict[str, int]) -> None:
        pass


class PubSubDataSource(DataSource):
    """Registered Spark 4.0 Python DataSource for Google Cloud Pub/Sub."""

    @classmethod
    def name(cls) -> str:
        return "pubsub"

    def schema(self) -> str:
        return "message_id STRING, data STRING, publish_time STRING, attributes STRING"

    def reader(self, schema: StructType) -> DataSourceReader:
        return PubSubBatchReader(schema, self.options)

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        return PubSubStreamReader(schema, self.options)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Registers PubSubDataSource and reads Cloud Pub/Sub messages."""
    spark.dataSource.register(PubSubDataSource)
    dfs: dict[str, DataFrame] = {}

    read_mode = config.get("pubsub_read_mode", "streaming")
    reader = (
        spark.read.format("pubsub")
        if read_mode == "batch"
        else spark.readStream.format("pubsub")
    )
    dfs["src_pubsub"] = (
        reader.option("projectId", config["pubsub_project_id"])
        .option("subscription", config["pubsub_subscription"])
        .option("maxMessagesPerBatch", str(config.get("pubsub_max_messages", 500)))
        .option(
            "includeAttributes",
            str(config.get("pubsub_include_attributes", True)).lower(),
        )
        .load()
    )
    return dfs
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline with an in-memory DataFrame matching the `PubSubDataSource` schema without calling Google Cloud Pub/Sub:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from src.pipeline import transform


def test_pubsub_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory Pub/Sub records."""
    schema = StructType(
        [
            StructField("message_id", StringType(), False),
            StructField("data", StringType(), True),
            StructField("publish_time", StringType(), True),
            StructField("attributes", StringType(), True),
        ]
    )
    input_df = spark.createDataFrame(
        [
            (
                "msg-101",
                '{"event_type": "click", "user_id": "u-1"}',
                "2026-01-01T00:00:00Z",
                '{"source": "web"}',
            )
        ],
        schema=schema,
    )
    result_dfs = transform({"src_pubsub": input_df}, {})
    assert "src_pubsub" in result_dfs
    assert result_dfs["src_pubsub"].count() == 1
```
