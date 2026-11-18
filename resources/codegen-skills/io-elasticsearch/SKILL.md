---
name: io-elasticsearch
description: Read and write Elasticsearch indices in PySpark 4.x using the official Elasticsearch Spark 4.0 connector with Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [ElasticsearchSource, ElasticsearchSink]
  preloadWhen: []
  sources:
    - prompt
    - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
  validated: true
---

# Elasticsearch Source and Sink (`ElasticsearchSource`, `ElasticsearchSink`)

Use these rules when generating PySpark 4.x code for `ElasticsearchSource` and `ElasticsearchSink` nodes.

## 1. Runtime Classpath and Connector Delivery

- Supply the Elasticsearch Spark 4.0 connector (Scala 2.13) via `--packages org.elasticsearch:elasticsearch-spark-40_2.13:8.17.0` (`spark.jars.packages`).
- Use `.format("org.elasticsearch.spark.sql")` in `spark.read`, `df.write`, and `df.writeStream`.

## 2. Batch Read Patterns (`ElasticsearchSource` in `read_sources`)

`ElasticsearchSource` runs in `batch` mode and pushes Query DSL filters directly down to Elasticsearch shards.

### Required and Optional Read Options
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

## 3. Batch and Streaming Write Patterns (`ElasticsearchSink` in `write_sinks`)

`ElasticsearchSink` has `streamingSinkMode: "native"` and supports both batch `df.write.format("org.elasticsearch.spark.sql")` and Structured Streaming `df.writeStream.format("org.elasticsearch.spark.sql")`.

### Required and Optional Write Options
- `es.nodes` and `es.port`: Target Elasticsearch cluster endpoint and port (defaults to `"9200"` or `"443"`).
- `es.nodes.wan.only`: Default to `"true"` on Elastic Cloud, VPC load balancers, and Serverless Spark.
- `es.net.ssl`: Set to `"true"` when connecting to HTTPS endpoints.
- `es.resource`: Target index or data stream name.
- `es.write.operation`: Bulk indexing mode:
  - `"index"` (default): Creates or replaces documents in full.
  - `"upsert"`: Updates matched documents or inserts new documents by `es.mapping.id`.
  - `"create"`: Indexes a document only if its `_id` does not already exist.
  - `"update"`: Updates existing documents matched by `es.mapping.id`.
- `es.mapping.id`: Input DataFrame column mapped to Elasticsearch `_id`. Always set `es.mapping.id` when `es.write.operation` is `"upsert"`, `"update"`, or `"create"`, and prefer setting it in Structured Streaming so micro-batch retries are idempotent.
- `es.batch.size.entries` and `es.batch.size.bytes`: Bulk request batching thresholds (for example `"1000"` entries and `"5mb"`).
- `es.net.http.auth.user` and `es.net.http.auth.pass`: Resolve the password once on the driver via `get_secret()`.
- If `ExecutionPlan.streamingSinkGroups` groups `ElasticsearchSink` inside a multi-sink `foreachBatch`, write each micro-batch via `batch_df.write.format("org.elasticsearch.spark.sql").mode("append").options(**write_opts).save()`.

```python
def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes a DataFrame to Elasticsearch in batch or native Structured Streaming mode."""
    df = dfs["es_sink_input"]
    es_password = get_secret(config["password_secret"]) if config.get("password_secret") else ""
    write_op = config.get("es_write_operation", "index")

    write_opts: dict[str, str] = {
        "es.nodes": config["es_nodes"],
        "es.port": str(config.get("es_port", 9200)),
        "es.nodes.wan.only": str(config.get("es_nodes_wan_only", True)).lower(),
        "es.net.ssl": str(config.get("es_ssl", False)).lower(),
        "es.resource": config["es_sink_resource"],
        "es.write.operation": write_op,
        "es.batch.size.entries": str(config.get("es_batch_size_entries", 1000)),
    }
    if config.get("es_batch_size_bytes"):
        write_opts["es.batch.size.bytes"] = str(config["es_batch_size_bytes"])
    if config.get("es_mapping_id"):
        write_opts["es.mapping.id"] = config["es_mapping_id"]
    if config.get("es_user"):
        write_opts["es.net.http.auth.user"] = config["es_user"]
        write_opts["es.net.http.auth.pass"] = es_password

    if df.isStreaming:
        checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
        checkpoint_path = config.get(
            "es_checkpoint_location",
            f"{checkpoint_root.rstrip('/')}/es_sink",
        )
        (
            df.writeStream.format("org.elasticsearch.spark.sql")
            .outputMode(config.get("output_mode", "append"))
            .options(**write_opts)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
            .start()
        )
        df.sparkSession.streams.awaitAnyTermination()
    else:
        save_mode = config.get("es_save_mode", "append")
        df.write.format("org.elasticsearch.spark.sql").mode(save_mode).options(**write_opts).save()
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

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
