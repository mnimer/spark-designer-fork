---
name: io-firestore
description: Read and write Google Cloud Firestore collections in PySpark 4.x using Spark 4.0 Python DataSource APIs and google-cloud-firestore WriteBatch.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [FirestoreSource, FirestoreSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
    - https://cloud.google.com/python/docs/reference/firestore/latest
  validated: true
---

# Google Cloud Firestore Source and Sink (`FirestoreSource`, `FirestoreSink`)

Use these rules when generating PySpark 4.x code for `FirestoreSource` and `FirestoreSink` nodes.

## 1. Runtime Delivery Mechanism (`python_datasource` and `foreach_batch_sdk`)

- Google Cloud Firestore does not publish a Spark 4.x Scala 2.13 JVM connector JAR. Implement a Spark 4.0 Python `DataSource` (`FirestoreDataSource` with `FirestoreBatchReader` and `FirestoreBatchWriter` from `pyspark.sql.datasource`) backed by `google-cloud-firestore`.
- Include `google-cloud-firestore` in `requirements.txt`.
- Register `FirestoreDataSource` on the active `SparkSession` via `spark.dataSource.register(FirestoreDataSource)`.
- Authenticate with Application Default Credentials on Managed Spark, or resolve a service account JSON payload once on the driver via `get_secret()` when `secretName` is configured.

## 2. `FirestoreSource` Batch Read Pattern (`read_sources`)

`FirestoreSource` runs in `batch` mode.

### Key Read Options and Rules
- `projectId`: Google Cloud project ID hosting Firestore.
- `databaseId`: Firestore database ID (defaults to `"(default)"`).
- `collectionPath`: Collection path or collection group name to query.
- Optional field filters: Support `filterField`, `filterOp` (such as `"=="`, `">="`, `"<="`), and `filterValue` via `google.cloud.firestore_v1.base_query.FieldFilter`.
- **Lazy Executor Client Initialization**: Always instantiate `firestore.Client(project=..., database=...)` inside `read(self, partition)` on the executor worker. Never instantiate the gRPC client in `__init__` on the driver because `DataSourceReader` instances are pickled and sent to executors.
- **Explicit Schema**: Define an explicit `StructType` schema and project each document dictionary (`doc.to_dict()`) into a tuple matching `self.schema.fieldNames()`.

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
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


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
class FirestoreInputPartition(InputPartition):
    """Serializable partition descriptor for Firestore reads."""

    partition_id: int


@dataclass
class FirestoreCommitMessage(WriterCommitMessage):
    """Commit summary returned from each Firestore partition writer."""

    mutations_committed: int


class FirestoreBatchReader(DataSourceReader):
    """Spark 4.0 DataSourceReader that streams documents from Google Cloud Firestore."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)

    def partitions(self) -> Sequence[InputPartition]:
        return [FirestoreInputPartition(partition_id=0)]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        from google.cloud import firestore
        from google.cloud.firestore_v1.base_query import FieldFilter

        project_id = self.options["projectId"]
        database_id = self.options.get("databaseId", "(default)")
        collection_path = self.options["collectionPath"]

        client = firestore.Client(project=project_id, database=database_id)
        query = client.collection(collection_path)

        filter_field = self.options.get("filterField")
        filter_op = self.options.get("filterOp")
        filter_val = self.options.get("filterValue")
        if filter_field and filter_op and filter_val is not None:
            query = query.where(filter=FieldFilter(filter_field, filter_op, filter_val))

        field_names = self.schema.fieldNames()
        for doc in query.stream():
            payload = doc.to_dict() or {}
            payload.setdefault("document_id", doc.id)
            yield tuple(payload.get(col) for col in field_names)
```

## 3. `FirestoreSink` Batch `DataSourceWriter` and Streaming `foreachBatch` (`write_sinks`)

`FirestoreSink` has `streamingSinkMode: "foreachBatch"`. Use `FirestoreBatchWriter` (`DataSourceWriter`) for batch writes and `.writeStream.foreachBatch(...)` for Structured Streaming micro-batches.

### Key Write Options and Rules
- `projectId`, `databaseId` (defaults to `"(default)"`), and `collectionPath`: Target Firestore collection coordinates.
- `documentIdColumn`: Column providing the deterministic Firestore document ID (`collection_ref.document(str(doc_id))`). Always supply `documentIdColumn` for `"update"`, `"delete"`, and streaming `foreachBatch` upserts so micro-batch retries remain idempotent.
- `operation`: Mutation mode per row:
  - `"set_merge"` (default): Calls `batch.set(doc_ref, payload, merge=True)` (or `merge=merge_fields` when `mergeFields` lists specific field paths).
  - `"set"`: Calls `batch.set(doc_ref, payload)` to overwrite the full document.
  - `"update"`: Calls `batch.update(doc_ref, payload)` on existing documents.
  - `"delete"`: Calls `batch.delete(doc_ref)` using `documentIdColumn`.
- **500-Mutation `WriteBatch` Cap**: Firestore limits each `WriteBatch.commit()` call to at most 500 mutations. Clamp `batch_size = max(1, min(int(options.get("maxBatchSize", "500")), 500))` and flush `batch.commit()` whenever the pending mutation count reaches `batch_size`.

```python
class FirestoreBatchWriter(DataSourceWriter):
    """Spark 4.0 DataSourceWriter committing <=500 Firestore mutations per WriteBatch."""

    def __init__(self, options: dict[str, str]) -> None:
        self.options = dict(options)

    def write(self, iterator: Iterator[Any]) -> WriterCommitMessage:
        from google.cloud import firestore
        from google.oauth2 import service_account

        project_id = self.options["projectId"]
        database_id = self.options.get("databaseId", "(default)")
        collection_path = self.options["collectionPath"]
        operation = self.options.get("operation", "set_merge")
        doc_id_col = self.options.get("documentIdColumn", "").strip()
        merge_fields_raw = self.options.get("mergeFields", "").strip()
        merge_fields = [f.strip() for f in merge_fields_raw.split(",") if f.strip()]
        max_batch_size = max(1, min(int(self.options.get("maxBatchSize", "500")), 500))

        creds = None
        if self.options.get("serviceAccountJson"):
            creds_info = json.loads(self.options["serviceAccountJson"])
            creds = service_account.Credentials.from_service_account_info(creds_info)

        client = firestore.Client(project=project_id, database=database_id, credentials=creds)
        collection_ref = client.collection(collection_path)

        batch = client.batch()
        pending = 0
        total_committed = 0

        for row in iterator:
            record = row.asDict(recursive=True)
            doc_id = record.pop(doc_id_col, None) if doc_id_col else None
            doc_ref = (
                collection_ref.document(str(doc_id))
                if doc_id is not None and str(doc_id) != ""
                else collection_ref.document()
            )

            if operation == "delete":
                batch.delete(doc_ref)
            elif operation == "update":
                batch.update(doc_ref, record)
            elif operation == "set":
                batch.set(doc_ref, record)
            else:
                merge_arg: bool | list[str] = merge_fields if merge_fields else True
                batch.set(doc_ref, record, merge=merge_arg)

            pending += 1
            if pending >= max_batch_size:
                batch.commit()
                total_committed += pending
                batch = client.batch()
                pending = 0

        if pending > 0:
            batch.commit()
            total_committed += pending

        return FirestoreCommitMessage(mutations_committed=total_committed)

    def commit(self, messages: list[WriterCommitMessage | None]) -> None:
        pass

    def abort(self, messages: list[WriterCommitMessage | None]) -> None:
        pass


class FirestoreDataSource(DataSource):
    """Spark 4.0 Python DataSource for Google Cloud Firestore reads and batched writes."""

    @classmethod
    def name(cls) -> str:
        return "firestore"

    def schema(self) -> StructType | str:
        return StructType(
            [
                StructField("document_id", StringType(), False),
                StructField("user_id", StringType(), True),
                StructField("score", DoubleType(), True),
            ]
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        return FirestoreBatchReader(schema, self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return FirestoreBatchWriter(self.options)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Registers FirestoreDataSource and reads the configured Firestore collection."""
    spark.dataSource.register(FirestoreDataSource)
    dfs: dict[str, DataFrame] = {}

    reader = (
        spark.read.format("firestore")
        .option("projectId", config["firestore_project_id"])
        .option("databaseId", config.get("firestore_database_id", "(default)"))
        .option("collectionPath", config["firestore_collection_path"])
    )
    if config.get("filter_field"):
        reader = (
            reader.option("filterField", config["filter_field"])
            .option("filterOp", config.get("filter_op", "=="))
            .option("filterValue", str(config["filter_value"]))
        )

    dfs["firestore_source"] = reader.load()
    return dfs


def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes DataFrames to Firestore via DataSourceWriter in batch mode or foreachBatch in streaming mode."""
    df = dfs["firestore_sink_input"]
    spark = df.sparkSession
    spark.dataSource.register(FirestoreDataSource)

    write_opts: dict[str, str] = {
        "projectId": config["firestore_project_id"],
        "databaseId": config.get("firestore_database_id", "(default)"),
        "collectionPath": config["firestore_sink_collection"],
        "operation": config.get("firestore_operation", "set_merge"),
        "documentIdColumn": config.get("firestore_doc_id_col", "document_id"),
        "maxBatchSize": str(min(int(config.get("firestore_max_batch_size", 500)), 500)),
    }
    if config.get("firestore_merge_fields"):
        write_opts["mergeFields"] = config["firestore_merge_fields"]
    if config.get("firestore_sa_secret"):
        write_opts["serviceAccountJson"] = get_secret(config["firestore_sa_secret"])

    if df.isStreaming:
        checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
        checkpoint_path = config.get(
            "firestore_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/firestore_sink",
        )

        def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
            batch_df.write.format("firestore").mode("append").options(**write_opts).save()

        (
            df.writeStream.foreachBatch(write_micro_batch)
            .outputMode(config.get("output_mode", "update"))
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
            .start()
        )
        spark.streams.awaitAnyTermination()
    else:
        df.write.format("firestore").mode("append").options(**write_opts).save()
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by constructing an in-memory `DataFrame` with the same `StructType` schema:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.pipeline import transform


def test_firestore_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without calling the Firestore API."""
    schema = StructType(
        [
            StructField("document_id", StringType(), False),
            StructField("user_id", StringType(), True),
            StructField("score", DoubleType(), True),
        ]
    )
    input_df = spark.createDataFrame(
        [("doc-1", "u-10", 95.0), ("doc-2", "u-20", 88.5)],
        schema=schema,
    )
    result_dfs = transform({"firestore_source": input_df}, {})
    assert result_dfs["firestore_source"].count() == 2
```
