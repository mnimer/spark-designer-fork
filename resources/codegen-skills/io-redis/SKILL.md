---
name: io-redis
description: Read and write Redis hashes, strings, JSON keys, and Streams in PySpark 4.x using Spark 4.0 Python DataSource and redis-py pipelines.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [RedisSource, RedisSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
    - https://redis.readthedocs.io/en/stable/commands.html
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
# Redis Source and Sink (`RedisSource`, `RedisSink`)

Use these rules when generating PySpark 4.x code for `RedisSource` and `RedisSink` nodes.

## 1. Runtime Delivery Mechanism (`python_datasource` and `foreach_batch_sdk`)

- Implement a Spark 4.0 Python `DataSource` (`RedisDataSource` with `RedisBatchReader`, `RedisStreamReader`, and `RedisBatchWriter`) using the `redis` (`redis-py`) package (`src/spark/versionMatrix.ts`).
- Add `redis` to `requirements.txt`.
- Register `RedisDataSource` via `spark.dataSource.register(RedisDataSource)` before reading or writing.
- Resolve the Redis AUTH password once on the driver via `get_secret()`.

## 2. Batch Key Scan and Streaming Redis Streams (`RedisSource` in `read_sources`)

`RedisSource` supports both `batch` mode (`scan_iter` over a `keyPattern`) and `streaming` mode (consuming Redis Streams via `xread` or `xreadgroup`).

### Key Read Options
- `host` and `port`: Redis endpoint hostname and port (defaults to `"6379"`).
- `password`: Resolve the Redis password once on the driver via `get_secret()`.
- `keyPattern` and `dataType`: For `batch` reads, scan matching keys with `client.scan_iter(match=key_pattern, count=1000)` and read values according to `dataType` (`"hash"` via `hgetall` or `"string"` via `get`).
- `streamKey` and `consumerGroup`: For `streaming` reads, read micro-batches from the Redis Stream using `client.xreadgroup(...)` or `client.xread(...)`.

```python
import base64
from dataclasses import dataclass
import json
from typing import Any, Iterator, Sequence
import urllib.request
import google.auth
import google.auth.transport.requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import StringType, StructField, StructType


def get_secret(secret_version_name: str) -> str:
    """Fetches a UTF-8 secret payload from Google Cloud Secret Manager once on the driver."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    url = f"https://secretmanager.googleapis.com/v1/{secret_version_name}:access"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {credentials.token}"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return base64.b64decode(payload["payload"]["data"]).decode("utf-8")


@dataclass(frozen=True)
class RedisInputPartition(InputPartition):
    """Serializable partition descriptor for Redis batch and stream reads."""

    batch_seq: int
    count: int


@dataclass
class RedisCommitMessage(WriterCommitMessage):
    """Commit summary returned from each Redis partition writer."""

    commands_executed: int


class RedisBatchReader(DataSourceReader):
    """Spark 4.0 DataSourceReader scanning Redis keys via scan_iter."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)

    def partitions(self) -> Sequence[InputPartition]:
        return [RedisInputPartition(batch_seq=0, count=1000)]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        import redis

        client = redis.Redis(
            host=self.options["host"],
            port=int(self.options.get("port", "6379")),
            db=int(self.options.get("database", "0")),
            password=self.options.get("password") or None,
            ssl=self.options.get("ssl", "false").lower() == "true",
            decode_responses=True,
        )
        key_pattern = self.options.get("keyPattern", "user:*")
        data_type = self.options.get("dataType", "hash")

        for key in client.scan_iter(match=key_pattern, count=500):
            if data_type == "hash":
                payload = json.dumps(client.hgetall(key))
            else:
                payload = str(client.get(key) or "")
            yield (str(key), payload)


class RedisStreamReader(DataSourceStreamReader):
    """Spark 4.0 DataSourceStreamReader consuming a Redis Stream via xread/xreadgroup."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)
        self._seq = 0

    def initialOffset(self) -> dict[str, int]:
        return {"seq": 0}

    def latestOffset(self) -> dict[str, int]:
        self._seq += 1
        return {"seq": self._seq}

    def partitions(self, start: dict[str, int], end: dict[str, int]) -> Sequence[InputPartition]:
        if int(end.get("seq", 0)) <= int(start.get("seq", 0)):
            return []
        return [
            RedisInputPartition(
                batch_seq=int(end["seq"]),
                count=int(self.options.get("maxEntriesPerBatch", "500")),
            )
        ]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        import redis

        if not isinstance(partition, RedisInputPartition):
            return
        client = redis.Redis(
            host=self.options["host"],
            port=int(self.options.get("port", "6379")),
            db=int(self.options.get("database", "0")),
            password=self.options.get("password") or None,
            ssl=self.options.get("ssl", "false").lower() == "true",
            decode_responses=True,
        )
        stream_key = self.options["streamKey"]
        group = self.options.get("consumerGroup", "").strip()
        if group:
            consumer = self.options.get("consumerName", "spark-worker-0")
            entries = client.xreadgroup(group, consumer, {stream_key: ">"}, count=partition.count)
        else:
            entries = client.xread({stream_key: "0-0"}, count=partition.count)

        for _, messages in entries:
            for msg_id, fields in messages:
                yield (str(msg_id), json.dumps(fields))

    def commit(self, end: dict[str, int]) -> None:
        pass
```

## 3. Pipelined Batch `DataSourceWriter` and Streaming `foreachBatch` (`RedisSink` in `write_sinks`)

`RedisSink` has `streamingSinkMode: "foreachBatch"`. Use `RedisBatchWriter` (`DataSourceWriter`) for batch writes and `.writeStream.foreachBatch(...)` for Structured Streaming micro-batches.

### Key Write Options and Rules
- **Pipelined Execution**: Initialize `redis.Redis(..., ssl=use_ssl)` once per executor partition inside `write(self, iterator)` and buffer commands with `client.pipeline(transaction=False)`. Flush `pipe.execute()` every `batchSize` rows (default `1000`) to avoid per-row round-trip latency.
- **Supported `dataType` Modes**:
  - `"hash"`: Constructs `redis_key = f"{key_prefix}{row_dict[key_col]}"` and calls `pipe.hset(redis_key, mapping=fields_dict)`.
  - `"string"` / `"json"`: Constructs `redis_key = f"{key_prefix}{row_dict[key_col]}"`. Writes `row_dict[value_col]` when `dataType == "string"` and `valueColumn` is set, or `json.dumps(payload_dict)` when `dataType == "json"`. Uses `pipe.set(redis_key, value_str, ex=ttl_seconds)`.
  - `"stream"`: Appends entries to `streamKey` using `pipe.xadd(stream_key, fields_dict, maxlen=max_stream_len, approximate=True)`.
- **Optional Key Expiration (`ttlSeconds`)**: When `ttlSeconds` > 0 on `"hash"` keys, queue `pipe.expire(redis_key, ttl_seconds)` in the same pipeline batch.
- **TLS and Secret Manager Auth**: Set `ssl=True` when connecting to TLS-enabled Memorystore or Redis Enterprise endpoints and resolve `password` once on the driver via `get_secret()`.

```python
class RedisBatchWriter(DataSourceWriter):
    """Spark 4.0 DataSourceWriter executing non-transactional redis-py pipelines per partition."""

    def __init__(self, options: dict[str, str]) -> None:
        self.options = dict(options)

    def write(self, iterator: Iterator[Any]) -> WriterCommitMessage:
        import redis

        client = redis.Redis(
            host=self.options["host"],
            port=int(self.options.get("port", "6379")),
            db=int(self.options.get("database", "0")),
            password=self.options.get("password") or None,
            ssl=self.options.get("ssl", "false").lower() == "true",
            decode_responses=True,
        )
        data_type = self.options.get("dataType", "hash")
        key_prefix = self.options.get("keyPrefix", "")
        key_col = self.options.get("keyColumn", "key")
        value_col = self.options.get("valueColumn", "").strip()
        stream_key = self.options.get("streamKey", "events:stream")
        ttl_raw = self.options.get("ttlSeconds", "").strip()
        ttl_seconds = int(ttl_raw) if ttl_raw.isdigit() and int(ttl_raw) > 0 else None
        maxlen_raw = self.options.get("maxStreamLen", "").strip()
        max_stream_len = int(maxlen_raw) if maxlen_raw.isdigit() and int(maxlen_raw) > 0 else None
        batch_size = max(1, int(self.options.get("batchSize", "1000")))

        pipe = client.pipeline(transaction=False)
        buffered = 0
        total_executed = 0

        try:
            for row in iterator:
                row_dict = row.asDict(recursive=True)
                if data_type == "stream":
                    stream_fields = {
                        str(k): json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                        for k, v in row_dict.items()
                        if v is not None
                    }
                    if stream_fields:
                        pipe.xadd(
                            stream_key,
                            stream_fields,
                            maxlen=max_stream_len,
                            approximate=True,
                        )
                        buffered += 1
                else:
                    raw_key = row_dict.get(key_col)
                    if raw_key is None:
                        continue
                    redis_key = f"{key_prefix}{raw_key}"
                    payload = {k: v for k, v in row_dict.items() if k != key_col and v is not None}

                    if data_type == "hash":
                        hash_fields = {
                            str(k): json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                            for k, v in payload.items()
                        }
                        if hash_fields:
                            pipe.hset(redis_key, mapping=hash_fields)
                            buffered += 1
                            if ttl_seconds is not None:
                                pipe.expire(redis_key, ttl_seconds)
                    elif data_type == "string" and value_col:
                        val_str = str(row_dict.get(value_col, ""))
                        pipe.set(redis_key, val_str, ex=ttl_seconds)
                        buffered += 1
                    else:
                        pipe.set(redis_key, json.dumps(payload), ex=ttl_seconds)
                        buffered += 1

                if buffered >= batch_size:
                    pipe.execute()
                    total_executed += buffered
                    buffered = 0

            if buffered > 0:
                pipe.execute()
                total_executed += buffered
        finally:
            pipe.close()
            client.close()

        return RedisCommitMessage(commands_executed=total_executed)

    def commit(self, messages: list[WriterCommitMessage | None]) -> None:
        pass

    def abort(self, messages: list[WriterCommitMessage | None]) -> None:
        pass


class RedisDataSource(DataSource):
    """Spark 4.0 Python DataSource for Redis reads, Streams, and pipelined writes."""

    @classmethod
    def name(cls) -> str:
        return "redis_py"

    def schema(self) -> StructType | str:
        return StructType(
            [
                StructField("key", StringType(), False),
                StructField("payload", StringType(), True),
            ]
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        return RedisBatchReader(schema, self.options)

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        return RedisStreamReader(schema, self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return RedisBatchWriter(self.options)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Registers RedisDataSource and reads keys in batch mode or Redis Streams in streaming mode."""
    spark.dataSource.register(RedisDataSource)
    dfs: dict[str, DataFrame] = {}

    password = get_secret(config["password_secret"]) if config.get("password_secret") else ""
    opts: dict[str, str] = {
        "host": config["redis_host"],
        "port": str(config.get("redis_port", 6379)),
        "password": password,
    }

    if config.get("read_mode", "batch") == "streaming":
        opts["streamKey"] = config["redis_stream_key"]
        if config.get("consumer_group"):
            opts["consumerGroup"] = config["consumer_group"]
        dfs["redis_source"] = spark.readStream.format("redis_py").options(**opts).load()
    else:
        opts["keyPattern"] = config.get("key_pattern", "user:*")
        opts["dataType"] = config.get("data_type", "hash")
        dfs["redis_source"] = spark.read.format("redis_py").options(**opts).load()

    return dfs


def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes DataFrames to Redis via pipelined DataSourceWriter or streaming foreachBatch."""
    df = dfs["redis_sink_input"]
    spark = df.sparkSession
    spark.dataSource.register(RedisDataSource)

    password = get_secret(config["password_secret"]) if config.get("password_secret") else ""
    write_opts: dict[str, str] = {
        "host": config["redis_host"],
        "port": str(config.get("redis_port", 6379)),
        "database": str(config.get("redis_db", 0)),
        "ssl": str(config.get("redis_ssl", False)).lower(),
        "password": password,
        "dataType": config.get("redis_sink_data_type", "hash"),
        "keyPrefix": config.get("redis_key_prefix", "user:"),
        "keyColumn": config.get("redis_key_column", "key"),
        "batchSize": str(config.get("redis_batch_size", 1000)),
    }
    if config.get("redis_value_column"):
        write_opts["valueColumn"] = config["redis_value_column"]
    if config.get("redis_stream_key"):
        write_opts["streamKey"] = config["redis_stream_key"]
    if config.get("redis_ttl_seconds"):
        write_opts["ttlSeconds"] = str(config["redis_ttl_seconds"])
    if config.get("redis_max_stream_len"):
        write_opts["maxStreamLen"] = str(config["redis_max_stream_len"])

    if df.isStreaming:
        checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
        checkpoint_path = config.get(
            "redis_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/redis_sink",
        )

        def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
            batch_df.write.format("redis_py").mode("append").options(**write_opts).save()

        (
            df.writeStream.foreachBatch(write_micro_batch)
            .outputMode(config.get("output_mode", "update"))
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
            .start()
        )
        spark.streams.awaitAnyTermination()
    else:
        df.write.format("redis_py").mode("append").options(**write_opts).save()
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` with an in-memory `DataFrame`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from src.pipeline import transform


def test_redis_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without connecting to a Redis server."""
    schema = StructType(
        [
            StructField("key", StringType(), False),
            StructField("payload", StringType(), True),
        ]
    )
    input_df = spark.createDataFrame(
        [("user:1", '{"tier":"gold"}'), ("user:2", '{"tier":"silver"}')],
        schema=schema,
    )
    result_dfs = transform({"redis_source": input_df}, {})
    assert result_dfs["redis_source"].count() == 2
```
