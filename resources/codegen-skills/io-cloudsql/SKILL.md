---
name: io-cloudsql
description: Read and write Google Cloud SQL for PostgreSQL, MySQL, and SQL Server in PySpark 4.x using the Cloud SQL JDBC Socket Factory, IAM or Secret Manager authentication, parallel partitioning, safe truncate writes, and streaming foreachBatch.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [CloudSqlSource, CloudSqlSink]
  preloadWhen: []
  sources:
    - prompt
    - https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
# Google Cloud SQL Connector (`CloudSqlSource`, `CloudSqlSink`)

Use these rules when generating PySpark 4.x code for `CloudSqlSource` or `CloudSqlSink` nodes.

## 1. Runtime Classpath and Socket Factory Configuration

- Attach the database JDBC driver JAR (`postgresql`, `mysql-connector-j`, or `mssql-jdbc`) and the corresponding Cloud SQL JDBC Socket Factory Uber JAR with `--jars` or `spark.jars`.
- **Mandatory Conscrypt Removal for SQL Server**: When `dbEngine` is `"sqlserver"`, always pass `--properties="dataproc.artifacts.remove=conscrypt"` in `run_serverless_spark.sh` and `run_dataproc_cluster.sh` (per `src/spark/versionMatrix.ts`) so TLS handshakes succeed on Managed Spark.
- Construct the JDBC connection and driver per database engine:
  - **PostgreSQL (`postgres` or `postgresql`)**:
    - Driver: `org.postgresql.Driver`
    - URL: `jdbc:postgresql:///<database>?cloudSqlInstance=<project>:<region>:<instance>&socketFactory=com.google.cloud.sql.postgres.SocketFactory`
  - **MySQL (`mysql`)**:
    - Driver: `com.mysql.cj.jdbc.Driver`
    - URL: `jdbc:mysql:///<database>?cloudSqlInstance=<project>:<region>:<instance>&socketFactory=com.google.cloud.sql.mysql.SocketFactory`
  - **SQL Server (`sqlserver`)**:
    - Driver: `com.microsoft.sqlserver.jdbc.SQLServerDriver`
    - URL: `jdbc:sqlserver://localhost` paired with `.option("databaseName", database)`, `.option("socketFactoryClass", "com.google.cloud.sql.sqlserver.SocketFactory")`, and `.option("socketFactoryConstructorArg", instance_conn)`.
- **IAM Database Authentication (`authMode == "iam"`)**:
  - For PostgreSQL and MySQL, append `&enableIamAuth=true&sslmode=disable` to the Socket Factory JDBC URL because the Cloud SQL Socket Factory encrypts the tunnel directly.
  - Pass the IAM principal username with `.option("user", config["db_user"])` and `.option("password", "empty")`.
- **Password Authentication via Secret Manager (`authMode == "secret_manager"`)**:
  - Resolve the password once on the driver using `get_secret(config["password_secret"])` and pass `.option("password", db_password)`. Never hardcode passwords in `src/pipeline.py`.
- Never access `spark` internal JVM handles in application code.

## 2. Dynamic Parallel Partitioning (`CloudSqlSource` in `read_sources`)

Never read a large Cloud SQL table over a single JDBC connection. Use Spark parallel JDBC partitioning with `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`, and `fetchsize=5000`:

1. **Explicit bounds**: When `lowerBound` and `upperBound` are provided in node configuration, pass them directly alongside `partitionColumn` and `numPartitions`.
2. **Dynamic `MIN`/`MAX` bounds query**: When `partitionColumn` is configured without static bounds, execute a fast metadata query `(SELECT MIN(<col>) AS min_v, MAX(<col>) AS max_v FROM <table>) AS bounds_q` first, collect the single row on the driver, and pass the discovered values as `lowerBound` and `upperBound`.
3. **Custom SQL Queries (`query`)**: When reading a custom SQL query with `partitionColumn`, wrap the query as a subquery in `dbtable` (`(SELECT ...) AS subq`) because Spark JDBC forbids combining `.option("query", ...)` with `.option("partitionColumn", ...)`.

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


def build_cloudsql_jdbc_options(config: dict[str, Any]) -> dict[str, str]:
    """Builds Cloud SQL Socket Factory JDBC connection options for PostgreSQL, MySQL, or SQL Server."""
    instance_conn = config["cloud_sql_instance"]
    database = config["db_name"]
    engine = config.get("db_engine", "postgresql").lower()
    auth_mode = config.get("auth_mode", "iam")

    opts: dict[str, str] = {"user": config["db_user"]}

    if engine == "sqlserver":
        opts["driver"] = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        opts["url"] = "jdbc:sqlserver://localhost"
        opts["databaseName"] = database
        opts["socketFactoryClass"] = "com.google.cloud.sql.sqlserver.SocketFactory"
        opts["socketFactoryConstructorArg"] = instance_conn
        opts["password"] = get_secret(config["password_secret"])
        return opts

    if engine == "mysql":
        opts["driver"] = "com.mysql.cj.jdbc.Driver"
        socket_factory = "com.google.cloud.sql.mysql.SocketFactory"
        base_url = f"jdbc:mysql:///{database}?cloudSqlInstance={instance_conn}&socketFactory={socket_factory}"
    else:
        opts["driver"] = "org.postgresql.Driver"
        socket_factory = "com.google.cloud.sql.postgres.SocketFactory"
        base_url = f"jdbc:postgresql:///{database}?cloudSqlInstance={instance_conn}&socketFactory={socket_factory}"

    if auth_mode == "iam":
        opts["url"] = f"{base_url}&enableIamAuth=true&sslmode=disable"
        opts["password"] = "empty"
    else:
        opts["url"] = base_url
        opts["password"] = get_secret(config["password_secret"])
    return opts


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads a Cloud SQL table in parallel using the Cloud SQL Socket Factory."""
    dfs: dict[str, DataFrame] = {}
    jdbc_opts = build_cloudsql_jdbc_options(config)

    target_table = config["dbtable"]
    part_col = config.get("partition_column", "id")
    num_parts = int(config.get("num_partitions", 8))

    bounds_row = (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option(
            "dbtable",
            f"(SELECT MIN({part_col}) AS min_v, MAX({part_col}) AS max_v FROM {target_table}) AS bounds_q",
        )
        .load()
        .first()
    )
    lower_bound = str(bounds_row["min_v"] if bounds_row and bounds_row["min_v"] is not None else 0)
    upper_bound = str(bounds_row["max_v"] if bounds_row and bounds_row["max_v"] is not None else 1000)

    dfs["cloudsql_source"] = (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("dbtable", target_table)
        .option("partitionColumn", part_col)
        .option("lowerBound", lower_bound)
        .option("upperBound", upper_bound)
        .option("numPartitions", str(num_parts))
        .option("fetchsize", "5000")
        .load()
    )
    return dfs
```

## 3. Batch Write Patterns (`CloudSqlSink` in `write_sinks`)

`CloudSqlSink` writes DataFrames to Cloud SQL with `df.write.format("jdbc")`:

- **Batch Size and Transaction Isolation**:
  - Always set `.option("batchsize", str(config.get("batchsize", 5000)))` so JDBC drivers execute batched multi-row inserts instead of row-by-row round trips.
  - Set `.option("isolationLevel", config.get("isolation_level", "READ_COMMITTED"))` (`"NONE"`, `"READ_UNCOMMITTED"`, `"READ_COMMITTED"`, `"REPEATABLE_READ"`, or `"SERIALIZABLE"`).
- **Safe `truncate=true` on `overwrite`**:
  - When `saveMode` is `"overwrite"`, set `.mode("overwrite").option("truncate", "true")` (default `True`). Without `truncate=true`, Spark drops and recreates the destination table, which destroys primary keys, secondary indexes, foreign keys, and grants.
- **Connection Pool Protection (`numPartitions`)**:
  - When `numPartitions` is configured, set `.option("numPartitions", str(config["sink_num_partitions"]))` (or call `df.coalesce(n)`) so parallel executors do not exhaust Cloud SQL `max_connections`.

```python
import logging
from typing import Any
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_cloudsql_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a batch DataFrame to Cloud SQL using JDBC batching and safe truncate."""
    jdbc_opts = build_cloudsql_jdbc_options(config)
    save_mode = config.get("cloudsql_save_mode", "append")
    batch_size = str(config.get("cloudsql_batchsize", 5000))
    isolation_level = config.get("cloudsql_isolation_level", "READ_COMMITTED")

    writer = (
        df.write.format("jdbc")
        .mode(save_mode)
        .options(**jdbc_opts)
        .option("dbtable", config["cloudsql_sink_table"])
        .option("batchsize", batch_size)
        .option("isolationLevel", isolation_level)
    )

    if save_mode == "overwrite" and config.get("cloudsql_truncate", True):
        writer = writer.option("truncate", "true")

    if config.get("cloudsql_sink_num_partitions"):
        writer = writer.option("numPartitions", str(config["cloudsql_sink_num_partitions"]))

    logger.info("Writing DataFrame to Cloud SQL table %s", config["cloudsql_sink_table"])
    writer.save()
```

## 4. Structured Streaming Micro-Batch Writes (`foreachBatch`)

`CloudSqlSink` uses `streamingSinkMode == "foreachBatch"` because Spark JDBC does not expose a native `DataStreamWriter` sink.

- Wrap `write_cloudsql_batch(batch_df, config)` inside `df.writeStream.foreachBatch(write_micro_batch)`.
- Resolve Secret Manager credentials once on the driver before starting the stream or cache the resolved options dictionary outside row loops.
- Call `batch_df.cache()` before writing and `batch_df.unpersist()` inside a `finally` block.
- Configure `checkpointLocation`, `outputMode`, and `trigger`, then call `spark.streams.awaitAnyTermination()`.

```python
from typing import Any
from pyspark.sql import DataFrame


def start_cloudsql_stream(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a streaming DataFrame to Cloud SQL via foreachBatch."""
    checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
    checkpoint_location = config.get(
        "cloudsql_checkpoint_location", f"{checkpoint_root.rstrip('/')}/cloudsql_sink"
    )

    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        try:
            write_cloudsql_batch(batch_df, config)
        finally:
            batch_df.unpersist()

    stream_writer = (
        df.writeStream.foreachBatch(write_micro_batch)
        .outputMode(config.get("cloudsql_output_mode", "append"))
        .option("checkpointLocation", checkpoint_location)
    )

    if config.get("cloudsql_trigger_type") == "availableNow":
        stream_writer.trigger(availableNow=True).start()
    else:
        interval = config.get("cloudsql_trigger_interval", "10 seconds")
        stream_writer.trigger(processingTime=interval).start()

    df.sparkSession.streams.awaitAnyTermination()
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` with an in-memory `DataFrame` so unit tests never require a live Cloud SQL instance or Secret Manager call:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType
from src.pipeline import transform


def test_cloudsql_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without connecting to Cloud SQL."""
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField("customer_email", StringType(), False),
        ]
    )
    input_df = spark.createDataFrame(
        [(1, "alice@example.com"), (2, "bob@example.com")], schema=schema
    )
    result_dfs = transform({"cloudsql_source": input_df}, {})
    assert result_dfs["cloudsql_source"].count() == 2
```
