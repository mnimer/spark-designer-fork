---
name: io-singlestore
description: Read and write SingleStoreDB tables in PySpark 4.x using the SingleStore Spark 4.0 connector with parallel loads, upserts, and Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [SingleStoreSource, SingleStoreSink]
  preloadWhen: []
  sources:
    - prompt
    - https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-spark/
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
# SingleStoreDB Connector (`SingleStoreSource` and `SingleStoreSink`)

Use these rules when generating PySpark 4.x code for `SingleStoreSource` and `SingleStoreSink` nodes.

## 1. Runtime Classpath and Connector Delivery

- Supply the SingleStore Spark 4.0 connector (Scala 2.13) via `--packages com.singlestore:singlestore-spark-connector_2.13:4.1.9-spark-4.0.0` (`spark.jars.packages`).
- Prefer `.format("singlestore")` for direct aggregator parallel reads and compressed `LOAD DATA` writes. Use `.format("jdbc")` with `com.singlestore.jdbc.Driver` or `org.mariadb.jdbc.Driver` only as an explicit JDBC fallback.

## 2. Batch Read Patterns (`SingleStoreSource` in `read_sources`)

`SingleStoreSource` runs in `batch` mode and supports parallel partition reads across SingleStore leaf nodes.

### Required and Optional Read Options
- `ddlEndpoint`: SingleStore master aggregator host and port (for example `<host>:3306`).
- `database`: SingleStore database name.
- `user` and `password`: Resolve the database password once on the driver via `get_secret()`.
- `enableParallelRead`: Set `.option("enableParallelRead", "Automatic")` (or `"Forced"`) so Spark reads partitions directly in parallel.
- `dbtable` or `query`: Pass `.option("dbtable", "<database>.<table>")` or `.option("query", "<sql_query>")` (or pass the table name to `.load("<database>.<table>")`).

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
    """Reads a SingleStoreDB table or SQL query using the SingleStore Spark connector."""
    dfs: dict[str, DataFrame] = {}

    s2_password = get_secret(config["password_secret"])
    database = config["singlestore_database"]

    reader = (
        spark.read.format("singlestore")
        .option("ddlEndpoint", config["singlestore_ddl_endpoint"])
        .option("database", database)
        .option("user", config["singlestore_user"])
        .option("password", s2_password)
        .option("enableParallelRead", config.get("enable_parallel_read", "Automatic"))
    )

    if config.get("singlestore_query"):
        dfs["singlestore_source"] = reader.option("query", config["singlestore_query"]).load()
    else:
        dfs["singlestore_source"] = reader.option(
            "dbtable", f"{database}.{config['singlestore_table']}"
        ).load()

    return dfs
```

## 3. Batch and Streaming Write Patterns (`SingleStoreSink` in `write_sinks`)

`SingleStoreSink` writes DataFrames via `.write.format("singlestore")` (or `.write.format("jdbc")` fallback) in batch mode and wraps that batch write inside `df.writeStream.foreachBatch(...)` in streaming mode (`streamingSinkMode: "foreachBatch"`).

### Required and Optional Write Options
- **Endpoint and Target Table (`ddlEndpoint`, `database`, `.save(...)`)**: Pass `.option("ddlEndpoint", "<host>:3306")`, `.option("database", database)`, `.option("user", user)`, `.option("password", s2_password)`, and `.save(f"{database}.{table}")`.
- **Streamed Payload Compression (`loadDataCompression`)**: Pass `.option("loadDataCompression", compression)` (`"LZ4"`, `"GZip"`, or `"Skip"`) to compress `LOAD DATA` streams between Spark executors and SingleStore aggregators.
- **Overwrite Behavior (`overwriteBehavior`)**: When `.mode("overwrite")` is used on `.format("singlestore")`, set `.option("overwriteBehavior", behavior)` to `"truncate"` (preserves shard keys, sort keys, and indexes), `"dropAndCreate"` (drops and recreates the table), or `"merge"` (replaces matching primary-key rows without dropping the table).
- **Idempotent Upsert (`onDuplicateKeySQL`)**: When upserting into tables with a primary or unique key in `"append"` mode or inside `foreachBatch`, pass `.option("onDuplicateKeySQL", clause)` (for example `"metric_val = VALUES(metric_val), updated_at = VALUES(updated_at)"`).
- **JDBC Fallback (`.write.format("jdbc")`)**: If `.format("jdbc")` is explicitly selected instead of `.format("singlestore")`, connect to `jdbc:singlestore://<ddlEndpoint>/<database>?rewriteBatchedStatements=true` (with `com.singlestore.jdbc.Driver` or `org.mariadb.jdbc.Driver`), set `.option("batchsize", "5000")`, and pass `.option("truncate", "true")` on overwrite.
- **Credentials**: Resolve `passwordSecret` once on the driver via `get_secret()`.

### Batch Write Example (`.format("singlestore")` and `.format("jdbc")` Fallback)

```python
def write_singlestore_df(df: DataFrame, config: dict[str, Any], s2_password: str) -> None:
    """Writes a batch or micro-batch DataFrame to SingleStoreDB."""
    ddl_endpoint = config["singlestore_sink_ddl_endpoint"]
    database = config["singlestore_sink_database"]
    table = config["singlestore_sink_table"]
    save_mode = config.get("singlestore_save_mode", "append")
    write_format = config.get("singlestore_write_format", "singlestore")

    if write_format == "jdbc":
        jdbc_url = f"jdbc:singlestore://{ddl_endpoint}/{database}?rewriteBatchedStatements=true"
        jdbc_writer = (
            df.write.format("jdbc")
            .mode(save_mode)
            .option("driver", config.get("singlestore_jdbc_driver", "com.singlestore.jdbc.Driver"))
            .option("url", jdbc_url)
            .option("dbtable", f"{database}.{table}")
            .option("user", config["singlestore_sink_user"])
            .option("password", s2_password)
            .option("batchsize", str(config.get("singlestore_batchsize", 5000)))
        )
        if save_mode == "overwrite":
            jdbc_writer = jdbc_writer.option("truncate", "true")
        jdbc_writer.save()
        return

    writer = (
        df.write.format("singlestore")
        .mode(save_mode)
        .option("ddlEndpoint", ddl_endpoint)
        .option("database", database)
        .option("user", config["singlestore_sink_user"])
        .option("password", s2_password)
        .option("loadDataCompression", config.get("singlestore_compression", "LZ4"))
    )

    if save_mode == "overwrite":
        writer = writer.option(
            "overwriteBehavior", config.get("singlestore_overwrite_behavior", "truncate")
        )
    if config.get("singlestore_on_duplicate_key_sql"):
        writer = writer.option("onDuplicateKeySQL", config["singlestore_on_duplicate_key_sql"])

    writer.save(f"{database}.{table}")
```

### Streaming `foreachBatch` Write Example

```python
def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes streaming micro-batches to SingleStoreDB via foreachBatch."""
    checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
    s2_password = get_secret(config["singlestore_sink_password_secret"])

    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        try:
            write_singlestore_df(batch_df, config, s2_password)
        finally:
            batch_df.unpersist()

    query = (
        dfs["upstream_stream"]
        .writeStream.foreachBatch(write_micro_batch)
        .outputMode(config.get("output_mode", "append"))
        .option("checkpointLocation", f"{checkpoint_root.rstrip('/')}/singlestore_sink")
        .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
        .start()
    )
    query.awaitTermination()
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` using an in-memory `DataFrame`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.pipeline import transform


def test_singlestore_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without connecting to SingleStoreDB."""
    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("price", DoubleType(), False),
        ]
    )
    input_df = spark.createDataFrame([("GOOG", 185.25), ("MSFT", 420.10)], schema=schema)
    result_dfs = transform({"singlestore_source": input_df}, {})
    assert result_dfs["singlestore_source"].count() == 2
```
