---
name: io-clickhouse
description: Read and write ClickHouse tables in PySpark 4.x using the ClickHouse Spark V2 connector or JDBC driver with Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [ClickHouseSource, ClickHouseSink]
  preloadWhen: []
  sources:
    - prompt
    - https://clickhouse.com/docs/integrations/apache-spark
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
# ClickHouse Connector (`ClickHouseSource` and `ClickHouseSink`)

Use these rules when generating PySpark 4.x code for `ClickHouseSource` and `ClickHouseSink` nodes.

## 1. Runtime Classpath and Connector Delivery

- Supply the ClickHouse Spark 4.0 runtime connector (Scala 2.13) via `--packages com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.8.1` (`spark.jars.packages`), alongside `com.clickhouse:clickhouse-jdbc` when using `.format("jdbc")`.
- Support either `.format("jdbc")` with the official ClickHouse JDBC driver (`com.clickhouse.jdbc.ClickHouseDriver`) or the ClickHouse Spark V2 catalog (`com.clickhouse.spark.ClickHouseCatalog`).

## 2. Batch Read Patterns (`ClickHouseSource` in `read_sources`)

`ClickHouseSource` runs in `batch` mode.

### Key Read Configuration Properties
- `url` (or `host`, `port`, and `database` forming `jdbc:clickhouse://<host>:<port>/<database>`): ClickHouse HTTP/HTTPS endpoint (typically port `8123` or `8443` for TLS).
- `dbtable` or `query`: Target table (`<database>.<table>`) or custom SQL query. When combining `query` with `partitionColumn` and `numPartitions`, wrap the query as `(SELECT ...) AS subq` in `dbtable`.
- `numPartitions`: Parallel read degree across ClickHouse shards or numeric partition bounds.
- `user` and `password`: Resolve the password once on the driver via `get_secret()`.

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
    """Reads a ClickHouse table or SQL query in batch mode."""
    dfs: dict[str, DataFrame] = {}

    host = config.get("clickhouse_host", "localhost")
    port = config.get("clickhouse_port", 8443)
    database = config.get("clickhouse_database", "default")
    jdbc_url = config.get("clickhouse_url") or f"jdbc:clickhouse://{host}:{port}/{database}?ssl=true"
    ch_password = get_secret(config["password_secret"])

    reader = (
        spark.read.format("jdbc")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("url", jdbc_url)
        .option("user", config.get("clickhouse_user", "default"))
        .option("password", ch_password)
        .option("fetchsize", str(config.get("fetchsize", 10000)))
    )

    if config.get("clickhouse_query"):
        reader = reader.option("query", config["clickhouse_query"])
    else:
        reader = reader.option("dbtable", f"{database}.{config['clickhouse_table']}")
        if config.get("num_partitions"):
            reader = reader.option("numPartitions", str(config["num_partitions"]))

    dfs["clickhouse_source"] = reader.load()
    return dfs
```

## 3. Batch and Streaming Write Patterns (`ClickHouseSink` in `write_sinks`)

`ClickHouseSink` writes DataFrames via `.write.format("jdbc")` with `com.clickhouse.jdbc.ClickHouseDriver` (or via the ClickHouse Spark V2 catalog `df.writeTo("clickhouse.<db>.<table>").append()`). In streaming mode (`streamingSinkMode: "foreachBatch"`), wrap the batch write inside `df.writeStream.foreachBatch(...)`.

### Critical MergeTree Bulk-Insert Rules
- **Never Use Tiny Batch Sizes**: Every ClickHouse `INSERT` creates an immutable MergeTree data part on disk. Tiny batches (such as `batchsize=100` or row-by-row inserts) overwhelm background merges and trigger `DB::Exception: Too many parts (300)`. Always set `.option("batchsize", "50000")` (or between `10000` and `100000`).
- **Disable JDBC Transaction Isolation (`isolationLevel="NONE"`)**: ClickHouse HTTP and native protocols do not use traditional RDBMS transaction isolation levels. Always set `.option("isolationLevel", "NONE")`.
- **Batched Statements and Async Inserts (`rewriteBatchedStatements`, `async_insert`)**: Include `rewriteBatchedStatements=true` in the JDBC URL or connection options so the driver packs rows into bulk `INSERT` blocks. When streaming micro-batches are small, enable `async_insert=1&wait_for_async_insert=1` on the ClickHouse JDBC URL so the server buffers and merges small inserts safely.
- **Safe Overwrite (`truncate=true`)**: When `saveMode == "overwrite"`, pass `.option("truncate", "true")` so Spark runs `TRUNCATE TABLE` instead of dropping the MergeTree table definition, `ORDER BY` key, and partitioning expression.
- **Credentials**: Resolve `passwordSecret` once on the driver via `get_secret()`.

### Batch Write Example

```python
def build_clickhouse_jdbc_url(config: dict[str, Any]) -> str:
    """Builds a ClickHouse JDBC URL with bulk-insert and optional async_insert parameters."""
    base_url = config.get("clickhouse_sink_url")
    if not base_url:
        host = config.get("clickhouse_sink_host", "localhost")
        port = config.get("clickhouse_sink_port", 8443)
        database = config.get("clickhouse_sink_database", "default")
        base_url = f"jdbc:clickhouse://{host}:{port}/{database}?ssl=true"

    params: list[str] = []
    if config.get("clickhouse_rewrite_batched", True) and "rewriteBatchedStatements=" not in base_url:
        params.append("rewriteBatchedStatements=true")
    if config.get("clickhouse_async_insert", False) and "async_insert=" not in base_url:
        params.append("async_insert=1")
        params.append("wait_for_async_insert=1")

    if not params:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}{'&'.join(params)}"


def write_clickhouse_df(df: DataFrame, config: dict[str, Any], ch_password: str) -> None:
    """Writes a batch or micro-batch DataFrame to ClickHouse with MergeTree-safe bulk batches."""
    save_mode = config.get("clickhouse_save_mode", "append")
    database = config.get("clickhouse_sink_database", "default")
    table = config["clickhouse_sink_table"]
    dbtable = table if "." in table else f"{database}.{table}"
    batch_size = max(int(config.get("clickhouse_batchsize", 50000)), 10000)

    writer = (
        df.write.format("jdbc")
        .mode(save_mode)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("url", build_clickhouse_jdbc_url(config))
        .option("dbtable", dbtable)
        .option("user", config.get("clickhouse_sink_user", "default"))
        .option("password", ch_password)
        .option("batchsize", str(batch_size))
        .option("isolationLevel", "NONE")
    )

    if save_mode == "overwrite" and config.get("clickhouse_truncate", True):
        writer = writer.option("truncate", "true")
    if config.get("clickhouse_num_partitions"):
        writer = writer.option("numPartitions", str(config["clickhouse_num_partitions"]))

    writer.save()
```

### Streaming `foreachBatch` Write Example

```python
def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes streaming micro-batches to ClickHouse via foreachBatch."""
    checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
    ch_password = get_secret(config["clickhouse_sink_password_secret"])

    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        try:
            write_clickhouse_df(batch_df, config, ch_password)
        finally:
            batch_df.unpersist()

    query = (
        dfs["upstream_stream"]
        .writeStream.foreachBatch(write_micro_batch)
        .outputMode(config.get("output_mode", "append"))
        .option("checkpointLocation", f"{checkpoint_root.rstrip('/')}/clickhouse_sink")
        .trigger(processingTime=config.get("trigger_interval", "15 seconds"))
        .start()
    )
    query.awaitTermination()
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` using an in-memory `DataFrame`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType
from src.pipeline import transform


def test_clickhouse_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without connecting to ClickHouse."""
    schema = StructType(
        [
            StructField("event_type", StringType(), False),
            StructField("latency_ms", LongType(), False),
        ]
    )
    input_df = spark.createDataFrame([("click", 12), ("view", 4)], schema=schema)
    result_dfs = transform({"clickhouse_source": input_df}, {})
    assert result_dfs["clickhouse_source"].count() == 2
```
