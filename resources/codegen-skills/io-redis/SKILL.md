---
name: io-redis
description: Read Redis keys, hashes, and Redis Streams in PySpark 4.x using a custom Spark 4.0 Python DataSource backed by redis-py in batch and streaming modes.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [RedisSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
  validated: true
---

# Redis Source (`RedisSource`)

Use these rules when generating PySpark 4.x code for a `RedisSource` node.

## 1. Runtime Delivery Mechanism (`python_datasource`)

- Implement a Spark 4.0 Python `DataSource` (`RedisDataSource`, `RedisBatchReader`, and `RedisStreamReader`) using the `redis` (`redis-py`) package (`src/spark/versionMatrix.ts`).
- Add `redis` to `requirements.txt`.
- Register `RedisDataSource` via `spark.dataSource.register(RedisDataSource)` inside `read_sources(spark, config)`.

## 2. Batch Key Scan and Streaming Redis Streams (`read_sources`)

`RedisSource` supports both `batch` mode (`scan_iter` over a `keyPattern`) and `streaming` mode (consuming Redis Streams via `xread` or `xreadgroup`).

### Key Options
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
    InputPartition,
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
            password=self.options.get("password") or None,
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
            password=self.options.get("password") or None,
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


class RedisDataSource(DataSource):
    """Spark 4.0 Python DataSource for Redis batch key scans and Redis Streams."""

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
```

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

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
