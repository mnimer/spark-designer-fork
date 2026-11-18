---
name: io-neo4j
description: Read Neo4j nodes, relationships, Cypher queries, and CDC streams in PySpark 4.x using the Neo4j Connector for Apache Spark 4.0 and Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [Neo4jSource]
  preloadWhen: []
  sources:
    - prompt
    - https://neo4j.com/docs/spark/current/
  validated: true
---

# Neo4j Graph Source (`Neo4jSource`)

Use these rules when generating PySpark 4.x code for a `Neo4jSource` node.

## 1. Runtime Classpath and Connector Delivery

- Supply the Neo4j Connector for Apache Spark 4.0 (Scala 2.13) via `--packages org.neo4j:neo4j-connector-apache-spark_2.13:5.3.4_for_spark_4` (`spark.jars.packages`).
- Use `.format("org.neo4j.spark.DataSource")` in both `spark.read` (`batch` mode) and `spark.readStream` (`streaming` mode).

## 2. Batch and Streaming Read Patterns (`read_sources`)

`Neo4jSource` supports both `batch` graph reads and `streaming` (CDC or timestamp property polling) reads.

### Required and Optional Options
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

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

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
