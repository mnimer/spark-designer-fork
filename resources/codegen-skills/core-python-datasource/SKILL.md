---
name: core-python-datasource
description: Spark 4.0 Python DataSource API templates (DataSource, DataSourceReader, DataSourceStreamReader, DataSourceWriter, DataSourceStreamWriter) and foreachBatch patterns for Python-native connectors.
metadata:
  kind: crosscutting
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [PubSubSource, PubSubSink, KinesisSource, KinesisSink, MqttSource, MqttSink, FirestoreSource, FirestoreSink, CassandraSource, RedisSource, RedisSink]
  preloadWhen: [python_datasource]
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
  validated: true
---

# Spark 4.0 Python DataSource API & Native SDK Connector Patterns

Apache Spark 4.x (Scala 2.13) introduces the first-class Python `DataSource` API (`pyspark.sql.datasource`). Use this API or `foreachBatch` / `mapInPandas` for connectors that do not publish a Spark 4.x Scala 2.13 JVM JAR (`PubSub`, `Kinesis`, `MQTT`, `Firestore`, `Cassandra`, `Redis`).

## 1. Connector Mechanism Matrix (`versionMatrix.ts`)

| Connector Transform | Delivery Mechanism | Python Class / Pattern | Required PyPI Package (`requirements.txt`) |
| :--- | :--- | :--- | :--- |
| `PubSubSource` | `python_datasource` | `PubSubDataSource` (`DataSourceReader` / `DataSourceStreamReader`) | `google-cloud-pubsub` |
| `PubSubSink` | `foreach_batch_sdk` / `DataSourceWriter` | `DataSourceWriter` (batch) or `foreachBatch` publisher (streaming) | `google-cloud-pubsub` |
| `KinesisSource` | `python_datasource` | `KinesisDataSource` (`DataSourceStreamReader`) | `boto3` |
| `KinesisSink` | `foreach_batch_sdk` | `foreachBatch` / `foreachPartition` (`boto3` `put_records`) | `boto3` |
| `MqttSource` | `python_datasource` | `MqttDataSource` (`DataSourceStreamReader`) | `paho-mqtt` |
| `MqttSink` | `foreach_batch_sdk` | `foreachBatch` / `foreachPartition` (`paho-mqtt` publisher) | `paho-mqtt` |
| `FirestoreSource` | `python_datasource` | `FirestoreDataSource` (`DataSourceReader`) | `google-cloud-firestore` |
| `FirestoreSink` | `foreach_batch_sdk` / `DataSourceWriter` | `DataSourceWriter` or `foreachBatch` (`WriteBatch` commits) | `google-cloud-firestore` |
| `CassandraSource` | `python_datasource` | `CassandraDataSource` (`DataSourceReader`) | `cassandra-driver` |
| `RedisSource` | `python_datasource` | `RedisDataSource` (`DataSourceReader` / `DataSourceStreamReader`) | `redis` |
| `RedisSink` | `foreach_batch_sdk` / `DataSourceWriter` | `DataSourceWriter` or `foreachBatch` (`redis` pipeline) | `redis` |

### When to Use Python `DataSource` vs `foreachBatch`
- **Sources (`SOURCE` nodes)**: Always implement and register a Spark 4.0 `DataSource` (`DataSourceReader` for batch mode, `DataSourceStreamReader` for streaming mode) so `read_sources(spark, config)` returns a standard Catalyst `DataFrame` via `spark.read.format("<name>")` or `spark.readStream.format("<name>")`.
- **Sinks (`SINK` nodes)**:
  - In **Streaming mode**, prefer `.writeStream.foreachBatch(write_micro_batch)` (or `DataSourceStreamWriter`) so multi-sink groups and idempotent micro-batch retries follow `ExecutionPlan.streamingSinkGroups`.
  - In **Batch mode**, use either a registered `DataSourceWriter` (`df.write.format("<name>").mode("append").save()`) or `df.foreachPartition(write_partition)` with lazy per-partition client initialization.

## 2. Core Imports & Registration

Import all Python DataSource base classes from `pyspark.sql.datasource` and register the custom `DataSource` class on the active `SparkSession` inside `read_sources()` (or before `write_sinks()`):

```python
from dataclasses import dataclass
from typing import Any, Iterator, Sequence
from pyspark.sql import SparkSession
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    DataSourceStreamWriter,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import StructType

# Register once on the SparkSession before reading or writing:
# spark.dataSource.register(CustomDataSource)
```

## 3. Batch `DataSourceReader` Template

Use this pattern for batch sources (`FirestoreSource`, `CassandraSource`, batch `PubSubSource`, batch `RedisSource`). Always instantiate network SDK clients **inside** `read(self, partition)` on the executor, never on the driver in `__init__` (objects serialized from driver to worker must be picklable):

```python
@dataclass(frozen=True)
class ShardInputPartition(InputPartition):
    """Serializable partition descriptor passed from driver to executors."""

    shard_id: int
    num_shards: int


class CustomBatchReader(DataSourceReader):
    """Spark 4.0 batch DataSourceReader implementation."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)

    def partitions(self) -> Sequence[InputPartition]:
        num_partitions = int(self.options.get("numPartitions", "4"))
        return [ShardInputPartition(shard_id=i, num_shards=num_partitions) for i in range(num_partitions)]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        # Instantiate the SDK client lazily inside read() on the executor worker
        shard = partition.shard_id if isinstance(partition, ShardInputPartition) else 0
        # Yield tuples matching self.schema field order
        yield (f"record-{shard}", "{}")
```

## 4. Streaming `DataSourceStreamReader` Template

Use this pattern for streaming sources (`PubSubSource`, `KinesisSource`, `MqttSource`, streaming `RedisSource`). Implement `initialOffset()`, `latestOffset()`, `partitions(start, end)`, `read(partition)`, and `commit(end)`:

```python
@dataclass(frozen=True)
class StreamRangePartition(InputPartition):
    """Serializable micro-batch offset range for one shard or subscription."""

    shard_id: str
    start_seq: int
    end_seq: int


class CustomStreamReader(DataSourceStreamReader):
    """Spark 4.0 Structured Streaming DataSourceStreamReader implementation."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)
        self._current_seq = 0

    def initialOffset(self) -> dict[str, int]:
        return {"seq": 0}

    def latestOffset(self) -> dict[str, int]:
        max_records = int(self.options.get("maxRecordsPerMicroBatch", "500"))
        self._current_seq += max_records
        return {"seq": self._current_seq}

    def partitions(self, start: dict[str, int], end: dict[str, int]) -> Sequence[InputPartition]:
        start_seq = int(start.get("seq", 0))
        end_seq = int(end.get("seq", start_seq))
        if end_seq <= start_seq:
            return []
        return [StreamRangePartition(shard_id="shard-0", start_seq=start_seq, end_seq=end_seq)]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        # Pull messages on the executor using the SDK client (e.g., SubscriberClient, boto3, redis)
        if not isinstance(partition, StreamRangePartition):
            return
        for seq in range(partition.start_seq, partition.end_seq):
            yield (f"msg-{seq}", b"{}", None)

    def commit(self, end: dict[str, int]) -> None:
        # Acknowledge or clean up committed offsets if required
        pass
```

## 5. `DataSourceWriter` & `DataSourceStreamWriter` Template

When implementing a custom writer via the Spark 4.0 `DataSource` API, batch records per partition inside `write(self, iterator)` and return a `WriterCommitMessage`:

```python
@dataclass
class BatchCommitMessage(WriterCommitMessage):
    """Commit metadata returned from each executor task to the driver."""

    records_written: int


class CustomBatchWriter(DataSourceWriter):
    """Spark 4.0 batch DataSourceWriter with batched SDK writes."""

    def __init__(self, options: dict[str, str]) -> None:
        self.options = dict(options)

    def write(self, iterator: Iterator[Any]) -> WriterCommitMessage:
        # Initialize the SDK client once per executor partition
        batch_size = int(self.options.get("batchSize", "500"))
        buffer: list[Any] = []
        total_written = 0

        for row in iterator:
            buffer.append(row.asDict(recursive=True))
            if len(buffer) >= batch_size:
                # Flush buffer using SDK batch API (e.g., Firestore WriteBatch, Redis pipeline, Pub/Sub publish)
                total_written += len(buffer)
                buffer.clear()

        if buffer:
            total_written += len(buffer)
            buffer.clear()

        return BatchCommitMessage(records_written=total_written)

    def commit(self, messages: list[WriterCommitMessage | None]) -> None:
        pass

    def abort(self, messages: list[WriterCommitMessage | None]) -> None:
        pass


class CustomStreamWriter(DataSourceStreamWriter):
    """Spark 4.0 streaming DataSourceStreamWriter delegating micro-batches."""

    def __init__(self, options: dict[str, str]) -> None:
        self.options = dict(options)
        self._batch_writer = CustomBatchWriter(options)

    def write(self, iterator: Iterator[Any]) -> WriterCommitMessage:
        return self._batch_writer.write(iterator)

    def commit(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        self._batch_writer.commit(messages)

    def abort(self, messages: list[WriterCommitMessage | None], batchId: int) -> None:
        self._batch_writer.abort(messages)
```

## 6. Complete `DataSource` Class Registration & Usage

Bind the reader, stream reader, writer, and stream writer in a single `DataSource` subclass and register it on `spark.dataSource`:

```python
class CustomConnectorDataSource(DataSource):
    """Registered Spark 4.0 Python DataSource."""

    @classmethod
    def name(cls) -> str:
        return "custom_connector"

    def schema(self) -> StructType | str:
        return self.options.get(
            "schema",
            "message_id STRING, data BINARY, publish_time TIMESTAMP",
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        return CustomBatchReader(schema, self.options)

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        return CustomStreamReader(schema, self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return CustomBatchWriter(self.options)

    def streamWriter(self, schema: StructType, overwrite: bool) -> DataSourceStreamWriter:
        return CustomStreamWriter(self.options)


# Registration and usage inside read_sources(spark, config):
# spark.dataSource.register(CustomConnectorDataSource)
# df = spark.readStream.format("custom_connector").options(**connector_opts).load()
```
