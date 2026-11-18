---
name: io-elasticsearch
description: Read Elasticsearch indices and DSL queries in PySpark 4.x using the official Elasticsearch Apache Spark 4.0 connector and Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [ElasticsearchSource]
  preloadWhen: []
  sources:
    - prompt
    - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
  validated: true
---

# Elasticsearch Source (`ElasticsearchSource`)

Use these rules when generating PySpark 4.x code for an `ElasticsearchSource` node.

## 1. Runtime Classpath and Connector Delivery

- Supply the Elasticsearch Spark 4.0 connector (Scala 2.13) via `--packages org.elasticsearch:elasticsearch-spark-40_2.13:8.17.0` (`spark.jars.packages`).
- Use `.format("org.elasticsearch.spark.sql")` in `spark.read`.

## 2. Batch Read Patterns (`read_sources`)

`ElasticsearchSource` runs in `batch` mode and pushes Query DSL filters directly down to Elasticsearch shards.

### Required and Optional Options
- `es.nodes`: Elasticsearch endpoint host or comma-separated hosts.
- `es.port`: HTTP/HTTPS port (typically `"9200"` or `"443"`).
- `es.resource`: Target index or index pattern (`<index>`).
- `es.query`: Optional Elasticsearch Query DSL JSON string or URI query string (`?q=...`) pushed down to Elasticsearch.
- `es.nodes.wan.only`: Always set `.option("es.nodes.wan.only", "true")` when reading from Elastic Cloud, VPC load balancers, or Serverless Spark so Spark connects through the configured endpoint rather than attempting direct private shard discovery.
- `es.net.http.auth.user` and `es.net.http.auth.pass`: Resolve the password once on the driver via `get_secret()`.

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
    """Reads an Elasticsearch index using the Elasticsearch Spark 4.0 connector."""
    dfs: dict[str, DataFrame] = {}

    es_password = get_secret(config["password_secret"])
    reader = (
        spark.read.format("org.elasticsearch.spark.sql")
        .option("es.nodes", config["es_nodes"])
        .option("es.port", str(config.get("es_port", 9200)))
        .option("es.resource", config["es_resource"])
        .option("es.nodes.wan.only", str(config.get("es_nodes_wan_only", True)).lower())
        .option("es.net.http.auth.user", config["es_user"])
        .option("es.net.http.auth.pass", es_password)
    )
    if config.get("es_query"):
        reader = reader.option("es.query", config["es_query"])

    dfs["es_source"] = reader.load()
    return dfs
```

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` with an in-memory `DataFrame`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from src.pipeline import transform


def test_elasticsearch_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without connecting to Elasticsearch."""
    schema = StructType(
        [
            StructField("doc_id", StringType(), False),
            StructField("message", StringType(), True),
        ]
    )
    input_df = spark.createDataFrame(
        [("es-1", "service started"), ("es-2", "request completed")],
        schema=schema,
    )
    result_dfs = transform({"es_source": input_df}, {})
    assert result_dfs["es_source"].count() == 2
```
