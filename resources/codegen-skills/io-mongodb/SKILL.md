---
name: io-mongodb
description: Read and write MongoDB collections and Change Streams in PySpark 4.x using MongoDB Spark Connector v10.4+ with Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [MongoDbSource, MongoDbSink]
  preloadWhen: []
  sources:
    - prompt
    - https://www.mongodb.com/docs/spark-connector/current/
  validated: true
---

# MongoDB Source and Sink (`MongoDbSource`, `MongoDbSink`)

Use these rules when generating PySpark 4.x code for `MongoDbSource` and `MongoDbSink` nodes.

## 1. Runtime Classpath and Connector Delivery

- Supply the MongoDB Spark Connector v10.4+ (Scala 2.13) via `--packages org.mongodb.spark:mongo-spark-connector_2.13:10.4.1` (`spark.jars.packages`).
- Use `.format("mongodb")` in `spark.read`, `spark.readStream`, `df.write`, and `df.writeStream`.

## 2. Batch and Streaming Read Patterns (`MongoDbSource` in `read_sources`)

`MongoDbSource` supports both `batch` collection reads and `streaming` Change Stream reads.

### Required and Optional Read Options
- `spark.mongodb.read.connection.uri` (or `.option("connection.uri", ...)`): Resolve the MongoDB connection URI (`mongodb+srv://...`) once on the driver via `get_secret()`.
- `database`: Target MongoDB database name.
- `collection`: Target MongoDB collection name.
- `aggregation.pipeline`: Optional JSON array string defining a server-side MongoDB `$match` or `$project` aggregation pipeline pushed down to MongoDB.
- `change.stream.publish.full.document.only`: In `streaming` mode, set `.option("change.stream.publish.full.document.only", "true")` (and optionally `.option("change.stream.lookup.full.document", "updateLookup")`) to emit the full document schema directly from MongoDB Change Streams.
- Always provide an explicit `StructType` `.schema(...)` in `streaming` mode (and prefer it in `batch` mode) to avoid driver sampling overhead.

```python
import base64
import json
from typing import Any
import urllib.request
import google.auth
import google.auth.transport.requests
from pyspark.sql import DataFrame, SparkSession
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


MONGO_DOCUMENT_SCHEMA = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
    ]
)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads a MongoDB collection in batch mode or via MongoDB Change Streams in streaming mode."""
    dfs: dict[str, DataFrame] = {}

    mongo_uri = get_secret(config["mongo_uri_secret"])
    read_mode = config.get("read_mode", "batch")

    base_opts: dict[str, str] = {
        "spark.mongodb.read.connection.uri": mongo_uri,
        "connection.uri": mongo_uri,
        "database": config["mongo_database"],
        "collection": config["mongo_collection"],
    }
    if config.get("aggregation_pipeline"):
        base_opts["aggregation.pipeline"] = config["aggregation_pipeline"]

    if read_mode == "streaming":
        base_opts["change.stream.publish.full.document.only"] = "true"
        dfs["mongo_source"] = (
            spark.readStream.format("mongodb")
            .schema(MONGO_DOCUMENT_SCHEMA)
            .options(**base_opts)
            .load()
        )
    else:
        dfs["mongo_source"] = (
            spark.read.format("mongodb")
            .schema(MONGO_DOCUMENT_SCHEMA)
            .options(**base_opts)
            .load()
        )

    return dfs
```

## 3. Batch and Streaming Write Patterns (`MongoDbSink` in `write_sinks`)

`MongoDbSink` has `streamingSinkMode: "native"` and supports both batch `df.write.format("mongodb")` and Structured Streaming `df.writeStream.format("mongodb")`.

### Required and Optional Write Options
- `spark.mongodb.write.connection.uri` and `connection.uri`: Resolve the MongoDB connection URI once on the driver via `get_secret()`.
- `database`: Target MongoDB database name.
- `collection`: Target MongoDB collection name.
- `operationType`: Bulk write operation mode:
  - `"insert"`: Inserts every row as a new document.
  - `"replace"`: Replaces matching documents by key (idempotent upsert when `upsertDocument` is `"true"`).
  - `"update"`: Updates matching fields on existing documents using `$set` semantics.
- `idFieldList`: Comma-separated document key columns (defaults to `"_id"`). When `operationType` is `"replace"` or `"update"`, set `idFieldList` to the business key columns so retries remain idempotent.
- `upsertDocument`: Set to `"true"` (default) or `"false"` when `operationType` is `"replace"` or `"update"`.
- `maxBatchSize`: Maximum number of operations grouped into each MongoDB bulk write request (defaults to `"512"`).
- `ordered`: Set to `"true"` (default) to stop a bulk batch on the first write failure, or `"false"` to continue writing remaining documents in the batch.
- If `ExecutionPlan.streamingSinkGroups` places `MongoDbSink` in a multi-sink `foreachBatch` group, write each `batch_df` with `batch_df.write.format("mongodb").mode("append").options(**write_opts).save()`.

```python
def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes a terminal DataFrame to MongoDB in batch or native Structured Streaming mode."""
    df = dfs["mongo_sink_input"]
    mongo_uri = get_secret(config["mongo_uri_secret"])
    operation_type = config.get("mongo_operation_type", "replace")

    write_opts: dict[str, str] = {
        "spark.mongodb.write.connection.uri": mongo_uri,
        "connection.uri": mongo_uri,
        "database": config["mongo_database"],
        "collection": config["mongo_collection"],
        "operationType": operation_type,
        "maxBatchSize": str(config.get("mongo_max_batch_size", 512)),
        "ordered": str(config.get("mongo_ordered", True)).lower(),
    }
    if operation_type in ("replace", "update"):
        write_opts["idFieldList"] = config.get("mongo_id_field_list", "_id")
        write_opts["upsertDocument"] = str(config.get("mongo_upsert_document", True)).lower()

    if df.isStreaming:
        checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
        checkpoint_path = config.get(
            "mongo_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/mongo_sink",
        )
        query = (
            df.writeStream.format("mongodb")
            .outputMode(config.get("output_mode", "append"))
            .options(**write_opts)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
            .start()
        )
        df.sparkSession.streams.awaitAnyTermination()
    else:
        save_mode = config.get("mongo_save_mode", "append")
        df.write.format("mongodb").mode(save_mode).options(**write_opts).save()
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` using an in-memory `DataFrame` matching `MONGO_DOCUMENT_SCHEMA`:

```python
from pyspark.sql import SparkSession
from src.pipeline import MONGO_DOCUMENT_SCHEMA, transform


def test_mongodb_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without connecting to MongoDB."""
    input_df = spark.createDataFrame(
        [("doc-1", "cust-1", 75.0), ("doc-2", "cust-2", 150.0)],
        schema=MONGO_DOCUMENT_SCHEMA,
    )
    result_dfs = transform({"mongo_source": input_df}, {})
    assert result_dfs["mongo_source"].count() == 2
```
