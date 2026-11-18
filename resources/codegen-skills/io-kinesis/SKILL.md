---
name: io-kinesis
description: Read streaming records from Amazon Kinesis Data Streams in PySpark 4.x using the Python DataSourceStreamReader API and boto3.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [KinesisSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
    - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html
  validated: true
---

# Amazon Kinesis Data Streams Source (`KinesisSource`)

Use these rules when generating PySpark {{spark_version}} code for an Amazon Kinesis source node (`KinesisSource` or `KinesisIO`).

## 1. Runtime Delivery Mechanism

Implement and register a Spark 4.0 Python `DataSource` (`KinesisDataSource` with `DataSourceStreamReader` from `pyspark.sql.datasource`) backed by `boto3` (`describe_stream_summary`, `list_shards`, `get_shard_iterator`, and `get_records`) and registered under `.format("kinesis")`:
- Include `boto3>=1.34.0` in `requirements.txt` and `Dockerfile`.
- Register `KinesisDataSource` on the active session with `spark.dataSource.register(KinesisDataSource)` inside `read_sources(spark, config)` and read with `spark.readStream.format("kinesis")`.

## 2. Supported Node Properties

Map the `KinesisSource` node properties to `.format("kinesis")` options:
- `streamName` -> `.option("streamName", config["kinesis_stream_name"])`
- `region` -> `.option("region", config["kinesis_region"])`
- `initialPosition` -> `.option("initialPosition", config.get("kinesis_initial_position", "latest"))`:
  - `"latest"` -> `ShardIteratorType="LATEST"`
  - `"trim_horizon"` -> `ShardIteratorType="TRIM_HORIZON"`
  - `"at_timestamp"` -> `ShardIteratorType="AT_TIMESTAMP"` with `Timestamp` from `initialTimestamp`
- `endpointUrl` -> `.option("endpointUrl", config["kinesis_endpoint_url"])` when custom or LocalStack endpoints are configured
- `maxRecordsPerFetch` -> `.option("maxRecordsPerFetch", str(config.get("kinesis_max_records_per_fetch", 500)))`

## 3. AWS Credentials via Secret Manager

Resolve AWS credentials once on the driver in `read_sources(spark, config)` using the zero-dependency `get_secret()` (`access_secret_version()`) Secret Manager helper when `accessKeySecret` and `secretKeySecret` (and optional `sessionTokenSecret`) are configured:
- Pass resolved credentials via options (`awsAccessKeyId`, `awsSecretAccessKey`, `awsSessionToken`) or rely on default IAM role credentials when no secret is specified.
- Instantiate `boto3.client("kinesis", ...)` inside executor methods rather than storing a live client on `self`.

## 4. Reference Implementation (`src/pipeline.py`)

```python
from dataclasses import dataclass
from datetime import datetime
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
class KinesisShardPartition(InputPartition):
    """Serializable shard descriptor for a Kinesis micro-batch."""

    shard_id: str
    limit: int


def _create_kinesis_client(options: dict[str, str]) -> Any:
    """Creates a boto3 Kinesis client from DataSource options."""
    import boto3

    kwargs: dict[str, Any] = {"region_name": options.get("region", "us-east-1")}
    if options.get("endpointUrl"):
        kwargs["endpoint_url"] = options["endpointUrl"]
    if options.get("awsAccessKeyId") and options.get("awsSecretAccessKey"):
        kwargs["aws_access_key_id"] = options["awsAccessKeyId"]
        kwargs["aws_secret_access_key"] = options["awsSecretAccessKey"]
    if options.get("awsSessionToken"):
        kwargs["aws_session_token"] = options["awsSessionToken"]
    return boto3.client("kinesis", **kwargs)


class KinesisStreamReader(DataSourceStreamReader):
    """Spark 4.0 DataSourceStreamReader for Amazon Kinesis Data Streams."""

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
        client = _create_kinesis_client(self.options)
        stream_name = self.options["streamName"]
        summary = client.describe_stream_summary(StreamName=stream_name)
        open_shards = int(summary["StreamDescriptionSummary"].get("OpenShardCount", 1))
        shards_resp = client.list_shards(StreamName=stream_name, MaxResults=max(open_shards, 1))
        limit = int(self.options.get("maxRecordsPerFetch", "500"))
        return [
            KinesisShardPartition(shard_id=s["ShardId"], limit=limit)
            for s in shards_resp.get("Shards", [])
        ]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        if not isinstance(partition, KinesisShardPartition):
            return
        client = _create_kinesis_client(self.options)
        stream_name = self.options["streamName"]
        pos = self.options.get("initialPosition", "latest").lower()
        iterator_type = "TRIM_HORIZON" if pos == "trim_horizon" else "LATEST"
        iter_kwargs: dict[str, Any] = {
            "StreamName": stream_name,
            "ShardId": partition.shard_id,
            "ShardIteratorType": iterator_type,
        }
        if pos == "at_timestamp" and self.options.get("initialTimestamp"):
            iter_kwargs["ShardIteratorType"] = "AT_TIMESTAMP"
            iter_kwargs["Timestamp"] = datetime.fromisoformat(self.options["initialTimestamp"])

        shard_iter = client.get_shard_iterator(**iter_kwargs)["ShardIterator"]
        records_resp = client.get_records(ShardIterator=shard_iter, Limit=partition.limit)
        for rec in records_resp.get("Records", []):
            arrival = rec.get("ApproximateArrivalTimestamp")
            yield (
                rec.get("PartitionKey", ""),
                bytes(rec.get("Data", b"")).decode("utf-8", errors="replace"),
                rec.get("SequenceNumber", ""),
                partition.shard_id,
                arrival.isoformat() if arrival else None,
            )

    def commit(self, end: dict[str, int]) -> None:
        pass


class KinesisDataSource(DataSource):
    """Registered Spark 4.0 Python DataSource for Amazon Kinesis."""

    @classmethod
    def name(cls) -> str:
        return "kinesis"

    def schema(self) -> str:
        return (
            "partition_key STRING, data STRING, sequence_number STRING, "
            "shard_id STRING, approximate_arrival_timestamp STRING"
        )

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        return KinesisStreamReader(schema, self.options)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Registers KinesisDataSource and reads a streaming Kinesis DataFrame."""
    spark.dataSource.register(KinesisDataSource)
    dfs: dict[str, DataFrame] = {}

    reader = (
        spark.readStream.format("kinesis")
        .option("streamName", config["kinesis_stream_name"])
        .option("region", config.get("kinesis_region", "us-east-1"))
        .option("initialPosition", config.get("kinesis_initial_position", "latest"))
        .option("maxRecordsPerFetch", str(config.get("kinesis_max_records_per_fetch", 500)))
    )
    if config.get("kinesis_endpoint_url"):
        reader = reader.option("endpointUrl", config["kinesis_endpoint_url"])
    if config.get("kinesis_access_key_secret") and config.get("kinesis_secret_key_secret"):
        reader = reader.option(
            "awsAccessKeyId", get_secret(config["kinesis_access_key_secret"])
        ).option(
            "awsSecretAccessKey", get_secret(config["kinesis_secret_key_secret"])
        )

    dfs["src_kinesis"] = reader.load()
    return dfs
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline with an in-memory DataFrame matching the `KinesisDataSource` schema without calling AWS Kinesis:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from src.pipeline import transform


def test_kinesis_pipeline_transform(spark: SparkSession) -> None:
    """Verifies downstream transformation logic using in-memory Kinesis records."""
    schema = StructType(
        [
            StructField("partition_key", StringType(), False),
            StructField("data", StringType(), True),
            StructField("sequence_number", StringType(), False),
            StructField("shard_id", StringType(), False),
            StructField("approximate_arrival_timestamp", StringType(), True),
        ]
    )
    input_df = spark.createDataFrame(
        [
            (
                "pk-1",
                '{"device_id": "dev-9", "temperature": 21.4}',
                "496000000000000000000001",
                "shardId-000000000000",
                "2026-01-01T00:00:00Z",
            )
        ],
        schema=schema,
    )
    result_dfs = transform({"src_kinesis": input_df}, {})
    assert "src_kinesis" in result_dfs
    assert result_dfs["src_kinesis"].count() == 1
```
