---
name: io-neo4j
description: Read and write Neo4j nodes, relationships, and Cypher queries in PySpark 4.x using the Neo4j Spark 4.0 Connector with Secret Manager auth.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [Neo4jSource, Neo4jSink]
  preloadWhen: []
  sources:
    - prompt
    - https://neo4j.com/docs/spark/current/
  validated: true
---

# Neo4j Graph Source and Sink (`Neo4jSource`, `Neo4jSink`)

Use these rules when generating PySpark 4.x code for `Neo4jSource` and `Neo4jSink` nodes.

## 1. Runtime Classpath and Connector Delivery

- Supply the Neo4j Connector for Apache Spark 4.0 (Scala 2.13) via `--packages org.neo4j:neo4j-connector-apache-spark_2.13:5.3.4_for_spark_4` (`spark.jars.packages`).
- Use `.format("org.neo4j.spark.DataSource")` in `spark.read`, `spark.readStream`, `df.write`, and `df.writeStream`.

## 2. Batch and Streaming Read Patterns (`Neo4jSource` in `read_sources`)

`Neo4jSource` supports both `batch` graph reads and `streaming` (CDC or timestamp property polling) reads.

### Required and Optional Read Options
- `url`: Neo4j Bolt or Aura URI (`neo4j+s://<host>:7687` or `bolt://<host>:7687`).
- `database`: Neo4j database name (defaults to `"neo4j"`).
- `authentication.type`: Set to `"basic"`.
- `authentication.basic.username` and `authentication.basic.password`: Resolve the password once on the driver via `get_secret()`.
- Read target (choose one):
  - `labels`: Node label(s), such as `:Person` or `Person`.
  - `relationship`: Relationship type (combined with `relationship.source.labels` and `relationship.target.labels`).
  - `query`: Custom Cypher query (`MATCH (n:Person) RETURN n.id AS id, n.name AS name`).
- `streaming.from`: In `streaming` mode, set `.option("streaming.from", "NOW")` or `.option("streaming.from", "ALL")` along with `.option("streaming.property.name", "<timestamp_property>")` (or Neo4j CDC mode).

```python
import base64
import json
from typing import Any
import urllib.request
import google.auth
import google.auth.transport.requests
from pyspark.sql import DataFrame, SparkSession


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


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads Neo4j nodes, relationships, or Cypher queries in batch or streaming mode."""
    dfs: dict[str, DataFrame] = {}

    neo4j_password = get_secret(config["password_secret"])
    opts: dict[str, str] = {
        "url": config["neo4j_url"],
        "database": config.get("neo4j_database", "neo4j"),
        "authentication.type": "basic",
        "authentication.basic.username": config.get("neo4j_user", "neo4j"),
        "authentication.basic.password": neo4j_password,
    }

    if config.get("neo4j_query"):
        opts["query"] = config["neo4j_query"]
    elif config.get("neo4j_relationship"):
        opts["relationship"] = config["neo4j_relationship"]
        opts["relationship.source.labels"] = config.get("source_labels", ":Node")
        opts["relationship.target.labels"] = config.get("target_labels", ":Node")
    else:
        opts["labels"] = config.get("neo4j_labels", ":Entity")

    if config.get("read_mode", "batch") == "streaming":
        opts["streaming.property.name"] = config.get("streaming_property_name", "updatedAt")
        opts["streaming.from"] = config.get("streaming_from", "NOW")
        dfs["neo4j_source"] = (
            spark.readStream.format("org.neo4j.spark.DataSource")
            .options(**opts)
            .load()
        )
    else:
        dfs["neo4j_source"] = (
            spark.read.format("org.neo4j.spark.DataSource")
            .options(**opts)
            .load()
        )

    return dfs
```

## 3. Batch and Streaming Write Patterns (`Neo4jSink` in `write_sinks`)

`Neo4jSink` has `streamingSinkMode: "native"` and supports both batch `df.write.format("org.neo4j.spark.DataSource")` and Structured Streaming `df.writeStream.format("org.neo4j.spark.DataSource")`.

### Save Modes and Write Targets
- **Spark Save Mode Mapping**:
  - `.mode("Overwrite")` (or `.option("save.mode", "Overwrite")` on `writeStream`): Executes idempotent Cypher `MERGE` operations matched on `node.keys` or relationship keys. Always prefer `Overwrite` for idempotent retries.
  - `.mode("Append")`: Executes Cypher `CREATE` operations without key deduplication.
- **Target Mode 1 — Node Labels (`labels` + `node.keys`)**:
  - Set `labels` (for example `":Customer"`) and `node.keys` (comma-separated key columns such as `"customer_id"`).
- **Target Mode 2 — Relationships (`relationship` + source/target keys)**:
  - Set `relationship` (for example `"PURCHASED"`), `relationship.save.strategy="keys"`, `relationship.source.labels`, `relationship.source.save.mode` (`"Match"` or `"Overwrite"`), `relationship.source.node.keys` (such as `"customer_id:id"`), `relationship.target.labels`, `relationship.target.save.mode` (`"Match"` or `"Overwrite"`), and `relationship.target.node.keys` (such as `"product_id:id"`).
  - When writing dense relationships concurrently, repartition by the source node key or reduce write parallelism to prevent Neo4j lock contention.
- **Target Mode 3 — Custom Cypher `MERGE` (`query`)**:
  - Set `query` to a Cypher statement referencing each DataFrame row through `event.<col>` (for example `"MERGE (c:Customer {id: event.customer_id}) SET c.tier = event.tier"`). The connector automatically wraps `query` in a batched `UNWIND $events AS event` transaction.
- **Batching and Auth Options**:
  - `batch.size`: Rows committed per Neo4j transaction (defaults to `"5000"`).
  - `transaction.retries`: Retry count on transient Neo4j lock errors (defaults to `"3"`).
  - `url`, `database`, `authentication.type="basic"`, `authentication.basic.username`, and `authentication.basic.password` resolved once on the driver via `get_secret()`.

```python
def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes DataFrames to Neo4j nodes, relationships, or custom Cypher in batch or streaming mode."""
    df = dfs["neo4j_sink_input"]
    neo4j_password = get_secret(config["password_secret"])
    save_mode = config.get("neo4j_save_mode", "Overwrite")
    write_target = config.get("neo4j_write_target", "labels")

    write_opts: dict[str, str] = {
        "url": config["neo4j_url"],
        "database": config.get("neo4j_database", "neo4j"),
        "authentication.type": "basic",
        "authentication.basic.username": config.get("neo4j_user", "neo4j"),
        "authentication.basic.password": neo4j_password,
        "batch.size": str(config.get("neo4j_batch_size", 5000)),
        "transaction.retries": str(config.get("neo4j_tx_retries", 3)),
    }

    if write_target == "query":
        write_opts["query"] = config["neo4j_sink_query"]
    elif write_target == "relationship":
        write_opts["relationship"] = config["neo4j_relationship"]
        write_opts["relationship.save.strategy"] = "keys"
        write_opts["relationship.source.labels"] = config.get("source_labels", ":Customer")
        write_opts["relationship.source.save.mode"] = config.get("source_save_mode", "Overwrite")
        write_opts["relationship.source.node.keys"] = config["source_node_keys"]
        write_opts["relationship.target.labels"] = config.get("target_labels", ":Product")
        write_opts["relationship.target.save.mode"] = config.get("target_save_mode", "Overwrite")
        write_opts["relationship.target.node.keys"] = config["target_node_keys"]
    else:
        write_opts["labels"] = config.get("neo4j_labels", ":Entity")
        if config.get("neo4j_node_keys"):
            write_opts["node.keys"] = config["neo4j_node_keys"]

    if df.isStreaming:
        checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
        checkpoint_path = config.get(
            "neo4j_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/neo4j_sink",
        )
        write_opts["save.mode"] = save_mode
        (
            df.writeStream.format("org.neo4j.spark.DataSource")
            .outputMode(config.get("output_mode", "append"))
            .options(**write_opts)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
            .start()
        )
        df.sparkSession.streams.awaitAnyTermination()
    else:
        df.write.format("org.neo4j.spark.DataSource").mode(save_mode).options(**write_opts).save()
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` with an in-memory `DataFrame`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType
from src.pipeline import transform


def test_neo4j_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without connecting to Neo4j."""
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("updatedAt", LongType(), True),
        ]
    )
    input_df = spark.createDataFrame(
        [("n-1", "Alice", 1700000000), ("n-2", "Bob", 1700000100)],
        schema=schema,
    )
    result_dfs = transform({"neo4j_source": input_df}, {})
    assert result_dfs["neo4j_source"].count() == 2
```
