---
name: io-kinesis
description: Read and write Amazon Kinesis Data Streams records in PySpark 4.x using the Python DataSource API, foreachBatch, and boto3.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [KinesisSource, KinesisSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
    - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
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

---

# Amazon Kinesis Data Streams Sink (`KinesisSink`)

Use these rules when generating PySpark {{spark_version}} code for an Amazon Kinesis sink node (`KinesisSink`, `SINK:KinesisIO`, or `WriteToKinesis`).

## 1. Batch and Streaming `foreachBatch` Execution

`KinesisSink` has `streamingSinkMode: "foreachBatch"` and publishes records using `boto3` (`put_records`):
- Include `boto3>=1.34.0` in `requirements.txt` and `Dockerfile`.
- **Batch mode**: Call `df.foreachPartition(publish_kinesis_partition)` so each executor task opens one `boto3` client and flushes partition batches.
- **Streaming mode**: Wrap writes inside `.writeStream.foreachBatch(write_micro_batch)` per `ExecutionPlan.streamingSinkGroups`. Cache `batch_df` when `requiresBatchCache` is true, invoke `batch_df.foreachPartition(publish_kinesis_partition)`, and call `batch_df.unpersist()` inside a `finally` block.

## 2. `put_records` Batching (Up to 500 Records) and `FailedRecordCount` Retries

Enforce AWS Kinesis `PutRecords` API limits and partial failure handling in every executor partition:
- **Cap batch size at 500 records**: Flush buffered records whenever `len(batch) >= min(max_batch_size, 500)` or when accumulated payload size approaches 4.5 MB.
- **Retry failed records on `FailedRecordCount > 0`**: Inspect `response.get("FailedRecordCount", 0)`. If greater than zero, extract only the subset of `batch[idx]` entries whose `response["Records"][idx]` contains `"ErrorCode"`. Retry those failed entries with exponential backoff (`time.sleep(0.2 * (2 ** attempt))`) up to `maxRetries` (default `5`). If any records still fail after `maxRetries`, raise a `RuntimeError` so Spark retries the partition rather than silently losing data.
- **Partition Key Routing (`partitionKeyMode`)**:
  - `"column"`: Read `str(row_dict[partition_key_col])` (falling back to SHA-256 payload digest if null).
  - `"static"`: Use `staticPartitionKey` (truncated to 256 characters per Kinesis limits).
  - `"hash"`: Compute `hashlib.sha256(payload_bytes).hexdigest()[:32]` for uniform shard distribution.

## 3. AWS Authentication via Google Cloud Secret Manager

Resolve AWS credentials once on the driver inside `write_sinks(dfs, config)` via `get_secret()`:
- Support JSON credential payloads (`{"aws_access_key_id": "...", "aws_secret_access_key": "..."}`) from `secretName` or separate `accessKeySecretName` and `secretKeySecretName` properties.
- If `roleArn` is configured, call `boto3.client("sts").assume_role(RoleArn=role_arn, RoleSessionName="spark-kinesis-sink")` inside the partition helper to obtain short-lived session credentials.
- Never serialize a live `boto3` client from the driver to executors.

## 4. Reference Sink Implementation (`src/pipeline.py`)

```python
import hashlib
import json
import time
from typing import Any, Iterator
from pyspark.sql import DataFrame


def _flush_kinesis_batch(
    client: Any,
    stream_name: str,
    records: list[dict[str, Any]],
    max_retries: int,
) -> None:
    """Sends up to 500 records via put_records and retries entries when FailedRecordCount > 0."""
    pending = list(records)
    for attempt in range(max_retries + 1):
        response = client.put_records(StreamName=stream_name, Records=pending)
        failed_count = int(response.get("FailedRecordCount", 0))
        if failed_count == 0:
            return
        if attempt >= max_retries:
            raise RuntimeError(
                f"Kinesis put_records failed for {failed_count} records after {max_retries} retries"
            )
        retry_subset: list[dict[str, Any]] = []
        for idx, res_item in enumerate(response.get("Records", [])):
            if res_item.get("ErrorCode"):
                retry_subset.append(pending[idx])
        pending = retry_subset
        time.sleep(min(0.2 * (2**attempt), 5.0))


def _resolve_kinesis_sink_options(config: dict[str, Any]) -> dict[str, str]:
    """Resolves AWS credentials on the driver and returns serializable sink options."""
    opts: dict[str, str] = {
        "streamName": config["kinesis_sink_stream_name"],
        "region": config.get("kinesis_sink_region", "us-east-1"),
        "partitionKeyMode": config.get("kinesis_sink_partition_key_mode", "column"),
        "partitionKeyColumn": config.get("kinesis_sink_partition_key_column", ""),
        "staticPartitionKey": config.get("kinesis_sink_static_partition_key", "default"),
        "payloadFormat": config.get("kinesis_sink_payload_format", "json"),
        "payloadColumn": config.get("kinesis_sink_payload_column", ""),
        "maxBatchSize": str(min(int(config.get("kinesis_sink_max_batch_size", 500)), 500)),
        "maxRetries": str(config.get("kinesis_sink_max_retries", 5)),
    }
    if config.get("kinesis_sink_endpoint_url"):
        opts["endpointUrl"] = config["kinesis_sink_endpoint_url"]
    if config.get("kinesis_sink_role_arn"):
        opts["roleArn"] = config["kinesis_sink_role_arn"]

    if config.get("kinesis_sink_secret_name"):
        secret_payload = json.loads(get_secret(config["kinesis_sink_secret_name"]))
        opts["awsAccessKeyId"] = secret_payload["aws_access_key_id"]
        opts["awsSecretAccessKey"] = secret_payload["aws_secret_access_key"]
        if secret_payload.get("aws_session_token"):
            opts["awsSessionToken"] = secret_payload["aws_session_token"]
    elif config.get("kinesis_sink_access_key_secret") and config.get(
        "kinesis_sink_secret_key_secret"
    ):
        opts["awsAccessKeyId"] = get_secret(config["kinesis_sink_access_key_secret"])
        opts["awsSecretAccessKey"] = get_secret(config["kinesis_sink_secret_key_secret"])
    return opts


def _write_kinesis_df(df: DataFrame, sink_opts: dict[str, str]) -> None:
    """Publishes a batch or micro-batch DataFrame to Amazon Kinesis via put_records."""

    def publish_partition(iterator: Iterator[Any]) -> None:
        client = _create_kinesis_client(sink_opts)
        stream_name = sink_opts["streamName"]
        max_batch = min(int(sink_opts.get("maxBatchSize", "500")), 500)
        max_retries = int(sink_opts.get("maxRetries", "5"))
        pk_mode = sink_opts.get("partitionKeyMode", "column")
        pk_col = sink_opts.get("partitionKeyColumn", "")
        static_pk = sink_opts.get("staticPartitionKey", "default")[:256]
        payload_fmt = sink_opts.get("payloadFormat", "json")
        payload_col = sink_opts.get("payloadColumn", "")

        buffer: list[dict[str, Any]] = []
        for row in iterator:
            row_dict = row.asDict(recursive=True)
            if payload_fmt == "binary" and payload_col and row_dict.get(payload_col) is not None:
                raw = row_dict[payload_col]
                data_bytes = bytes(raw) if isinstance(raw, (bytes, bytearray)) else str(raw).encode("utf-8")
            elif payload_fmt == "string" and payload_col:
                data_bytes = str(row_dict.get(payload_col, "")).encode("utf-8")
            else:
                data_bytes = json.dumps(row_dict, default=str).encode("utf-8")

            if pk_mode == "static" and static_pk:
                partition_key = static_pk
            elif pk_mode == "column" and pk_col and row_dict.get(pk_col) is not None:
                partition_key = str(row_dict[pk_col])[:256]
            else:
                partition_key = hashlib.sha256(data_bytes).hexdigest()[:32]

            buffer.append({"Data": data_bytes, "PartitionKey": partition_key})
            if len(buffer) >= max_batch:
                _flush_kinesis_batch(client, stream_name, buffer, max_retries)
                buffer.clear()

        if buffer:
            _flush_kinesis_batch(client, stream_name, buffer, max_retries)
            buffer.clear()

    df.foreachPartition(publish_partition)


def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes output DataFrame records to Amazon Kinesis in batch or streaming mode."""
    target_df = dfs["step_out"]
    sink_opts = _resolve_kinesis_sink_options(config)

    if target_df.isStreaming:
        checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
        checkpoint_loc = config.get("kinesis_sink_checkpoint") or (
            f"{checkpoint_root.rstrip('/')}/snk_kinesis"
        )

        def write_micro_batch(batch_df: DataFrame, _batch_id: int) -> None:
            batch_df.cache()
            try:
                _write_kinesis_df(batch_df, sink_opts)
            finally:
                batch_df.unpersist()

        (
            target_df.writeStream.foreachBatch(write_micro_batch)
            .outputMode(config.get("kinesis_sink_output_mode", "append"))
            .option("checkpointLocation", checkpoint_loc)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
            .start()
        )
        target_df.sparkSession.streams.awaitAnyTermination()
    else:
        _write_kinesis_df(target_df, sink_opts)
```
