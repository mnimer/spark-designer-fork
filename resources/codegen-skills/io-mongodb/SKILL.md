---
name: io-mongodb
description: Read MongoDB collections and Change Streams in PySpark 4.x using the MongoDB Spark Connector v10.4+ in batch and streaming modes with Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [MongoDbSource]
  preloadWhen: []
  sources:
    - prompt
    - https://www.mongodb.com/docs/spark-connector/current/
  validated: true
---

# MongoDB Source (`MongoDbSource`)

Use these rules when generating PySpark 4.x code for a `MongoDbSource` node.

## 1. Runtime Classpath and Connector Delivery

- Supply the MongoDB Spark Connector v10.4+ (Scala 2.13) via `--packages org.mongodb.spark:mongo-spark-connector_2.13:10.4.1` (`spark.jars.packages`).
- Use `.format("mongodb")` in both `spark.read` (`batch` mode) and `spark.readStream` (`streaming` mode).

## 2. Batch and Streaming Read Patterns (`read_sources`)

`MongoDbSource` supports both `batch` collection reads and `streaming` Change Stream reads.

### Required and Optional Options
- `spark.mongodb.read.connection.uri` (or `.option("connection.uri", ...)`): Resolve the MongoDB connection URI (`mongodb+srv://...`) once on the driver via `get_secret()`.
- `database`: Target MongoDB database name.
- `collection`: Target MongoDB collection name.
- `aggregation.pipeline`: Optional JSON array string defining a server-side MongoDB `$match` / `$project` aggregation pipeline pushed down to MongoDB.
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

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

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
