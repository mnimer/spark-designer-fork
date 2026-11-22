---
name: io-pubsub
description: Read and write batch and streaming messages with Google Cloud Pub/Sub in PySpark 4.x using the Python DataSource API and PublisherClient.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [PubSubSource, PubSubSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
    - https://cloud.google.com/python/docs/reference/pubsub/latest
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
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

---

# Google Cloud Pub/Sub Sink (`PubSubSink`)

Use these rules when generating PySpark {{spark_version}} code for a Google Cloud Pub/Sub sink node (`PubSubSink` or `SINK:PubSubIO`).

## 1. Runtime Delivery Mechanism (`DataSourceWriter` and `foreachBatch`)

`PubSubSink` has `streamingSinkMode: "foreachBatch"` and publishes messages through `google-cloud-pubsub` (`pubsub_v1.PublisherClient`):
- Include `google-cloud-pubsub>=2.21.0` in `requirements.txt` and `Dockerfile`.
- **Batch mode**: Implement and register a Spark 4.0 `DataSourceWriter` (`PubSubBatchWriter` on `PubSubDataSource` via `df.write.format("pubsub").mode("append").options(...).save()`).
- **Streaming mode**: Wrap micro-batch writes inside `.writeStream.foreachBatch(write_micro_batch)` following `ExecutionPlan.streamingSinkGroups`. Inside `write_micro_batch(batch_df, batch_id)`, write `batch_df` via `PubSubDataSource` (`batch_df.write.format("pubsub").mode("append").option("batchId", str(batch_id)).options(...).save()`) or `batch_df.foreachPartition(...)`.

## 2. `PublisherClient` Batching, Ordering Keys, Attributes, and Idempotency

Instantiate `pubsub_v1.PublisherClient` lazily once per executor partition inside `write(self, iterator)`:
- **Publisher Batch Settings (`BatchSettings`)**: Configure `pubsub_v1.types.BatchSettings(max_messages=max_messages, max_bytes=max_bytes, max_latency=max_latency_ms / 1000.0)` from `batchMaxMessages` (default `100`), `batchMaxBytes` (default `1048576`), and `batchMaxLatencyMs` (default `10`).
- **Ordering Keys (`PublisherOptions`)**: If `enableMessageOrdering` is true or `orderingKeyColumn` is configured, pass `pubsub_v1.types.PublisherOptions(enable_message_ordering=True)` to `PublisherClient` and supply `ordering_key=str(row_dict[ordering_key_col])` on each `publisher.publish(...)` call.
- **Message Attributes (`attributeColumns`)**: If `attributeColumns` lists comma-separated column names, pass each non-null column value as a `str` keyword argument to `publisher.publish(topic_path, payload_bytes, **attributes)` because Pub/Sub attributes require UTF-8 string values.
- **Micro-Batch Idempotency (`idempotencyKeyColumn`)**: Attach deterministic deduplication metadata (`event_id` from `idempotencyKeyColumn` and `spark_batch_id` from `batch_id`) as Pub/Sub message attributes so downstream subscribers or Pub/Sub snapshot consumers can deduplicate retried micro-batches.
- **Await All Futures**: Collect publish futures per partition and call `future.result(timeout=60.0)` for every published message before returning `WriterCommitMessage` so transient publish failures trigger Spark task retries instead of silent message drops.

## 3. Reference Sink Implementation (`src/pipeline.py`)

```python
import json
from dataclasses import dataclass
from typing import Any, Iterator
from pyspark.sql import DataFrame
from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType


@dataclass
class PubSubCommitMessage(WriterCommitMessage):
    """Commit summary returned from each executor partition after Pub/Sub publish."""

    published_count: int


def _resolve_topic_path(publisher: Any, project_id: str, topic: str) -> str:
    """Returns a fully qualified Cloud Pub/Sub topic resource path."""
    if topic.startswith("projects/"):
        return topic
    return publisher.topic_path(project_id, topic)


class PubSubBatchWriter(DataSourceWriter):
    """Spark 4.0 DataSourceWriter publishing partition batches to Cloud Pub/Sub."""

    def __init__(self, options: dict[str, str]) -> None:
        self.options = dict(options)

    def write(self, iterator: Iterator[Any]) -> WriterCommitMessage:
        from google.cloud import pubsub_v1

        max_messages = int(self.options.get("batchMaxMessages", "100"))
        max_bytes = int(self.options.get("batchMaxBytes", "1048576"))
        max_latency_s = float(self.options.get("batchMaxLatencyMs", "10")) / 1000.0
        ordering_col = self.options.get("orderingKeyColumn", "").strip()
        enable_ordering = (
            self.options.get("enableMessageOrdering", "false").lower() == "true"
            or bool(ordering_col)
        )
        payload_format = self.options.get("payloadFormat", "json")
        payload_col = self.options.get("payloadColumn", "").strip()
        idempotency_col = self.options.get("idempotencyKeyColumn", "").strip()
        batch_id = self.options.get("batchId", "")
        attr_cols = [
            c.strip()
            for c in self.options.get("attributeColumns", "").split(",")
            if c.strip()
        ]

        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages=max_messages,
            max_bytes=max_bytes,
            max_latency=max_latency_s,
        )
        publisher_options = pubsub_v1.types.PublisherOptions(
            enable_message_ordering=enable_ordering
        )
        publisher = pubsub_v1.PublisherClient(
            batch_settings=batch_settings,
            publisher_options=publisher_options,
        )
        topic_path = _resolve_topic_path(
            publisher,
            self.options.get("projectId", ""),
            self.options["topic"],
        )

        futures = []
        for row in iterator:
            row_dict = row.asDict(recursive=True)
            if payload_format == "binary" and payload_col and row_dict.get(payload_col) is not None:
                raw_val = row_dict[payload_col]
                payload_bytes = bytes(raw_val) if isinstance(raw_val, (bytes, bytearray)) else str(raw_val).encode("utf-8")
            elif payload_format == "string" and payload_col:
                payload_bytes = str(row_dict.get(payload_col, "")).encode("utf-8")
            elif payload_col and payload_col in row_dict and payload_format != "json":
                payload_bytes = str(row_dict[payload_col]).encode("utf-8")
            else:
                payload_bytes = json.dumps(row_dict, default=str).encode("utf-8")

            attrs: dict[str, str] = {}
            for col_name in attr_cols:
                if row_dict.get(col_name) is not None:
                    attrs[col_name] = str(row_dict[col_name])
            if idempotency_col and row_dict.get(idempotency_col) is not None:
                attrs["event_id"] = str(row_dict[idempotency_col])
            if batch_id:
                attrs["spark_batch_id"] = batch_id

            ordering_key = (
                str(row_dict[ordering_col])
                if enable_ordering and ordering_col and row_dict.get(ordering_col) is not None
                else ""
            )
            futures.append(
                publisher.publish(
                    topic_path,
                    payload_bytes,
                    ordering_key=ordering_key,
                    **attrs,
                )
            )

        for fut in futures:
            fut.result(timeout=60.0)
        return PubSubCommitMessage(published_count=len(futures))

    def commit(self, messages: list[WriterCommitMessage | None]) -> None:
        pass

    def abort(self, messages: list[WriterCommitMessage | None]) -> None:
        pass


class PubSubSinkDataSource(DataSource):
    """Spark 4.0 DataSource registering PubSubBatchWriter under format 'pubsub_sink'."""

    @classmethod
    def name(cls) -> str:
        return "pubsub_sink"

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return PubSubBatchWriter(self.options)


def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Publishes DataFrame records to Cloud Pub/Sub in batch or streaming foreachBatch mode."""
    target_df = dfs["step_out"]
    spark = target_df.sparkSession
    spark.dataSource.register(PubSubSinkDataSource)

    writer_opts = {
        "topic": config["pubsub_sink_topic"],
        "projectId": config.get("pubsub_sink_project_id", ""),
        "payloadFormat": config.get("pubsub_sink_payload_format", "json"),
        "payloadColumn": config.get("pubsub_sink_payload_column", ""),
        "attributeColumns": config.get("pubsub_sink_attribute_columns", ""),
        "orderingKeyColumn": config.get("pubsub_sink_ordering_key_column", ""),
        "idempotencyKeyColumn": config.get("pubsub_sink_idempotency_key_column", ""),
        "enableMessageOrdering": str(
            config.get("pubsub_sink_enable_ordering", False)
        ).lower(),
        "batchMaxMessages": str(config.get("pubsub_sink_batch_max_messages", 100)),
        "batchMaxBytes": str(config.get("pubsub_sink_batch_max_bytes", 1048576)),
        "batchMaxLatencyMs": str(config.get("pubsub_sink_batch_max_latency_ms", 10)),
    }

    if target_df.isStreaming:
        checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
        checkpoint_loc = config.get("pubsub_sink_checkpoint") or (
            f"{checkpoint_root.rstrip('/')}/snk_pubsub"
        )

        def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
            batch_df.cache()
            try:
                (
                    batch_df.write.format("pubsub_sink")
                    .mode("append")
                    .options(**writer_opts)
                    .option("batchId", str(batch_id))
                    .save()
                )
            finally:
                batch_df.unpersist()

        query = (
            target_df.writeStream.foreachBatch(write_micro_batch)
            .outputMode(config.get("pubsub_sink_output_mode", "append"))
            .option("checkpointLocation", checkpoint_loc)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
            .start()
        )
        spark.streams.awaitAnyTermination()
    else:
        target_df.write.format("pubsub_sink").mode("append").options(**writer_opts).save()
```
