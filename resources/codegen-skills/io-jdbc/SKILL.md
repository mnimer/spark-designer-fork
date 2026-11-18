---
name: io-jdbc
description: Read and write relational databases (PostgreSQL, MySQL, Oracle, SQL Server, and DB2) in PySpark 4.x using JDBC and Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [JdbcSource, JdbcSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
  validated: true
---

# Generic Relational JDBC Connector (`JdbcSource` and `JdbcSink`)

Use these rules when generating PySpark 4.x code for `JdbcSource` and `JdbcSink` nodes across PostgreSQL, MySQL, Oracle, SQL Server, and IBM DB2.

## 1. Dialect Driver Class Presets and Runtime Rules

| Database Preset | JDBC URL Prefix | Driver Class (`driver`) | Special Runtime Property |
| :--- | :--- | :--- | :--- |
| PostgreSQL | `jdbc:postgresql://host:5432/db` | `org.postgresql.Driver` | None |
| MySQL | `jdbc:mysql://host:3306/db` | `com.mysql.cj.jdbc.Driver` | None |
| Oracle | `jdbc:oracle:thin:@//host:1521/service` | `oracle.jdbc.OracleDriver` | None |
| SQL Server | `jdbc:sqlserver://host:1433` | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | `--properties="dataproc.artifacts.remove=conscrypt"` on Managed Spark |
| IBM DB2 | `jdbc:db2://host:50000/db` | `com.ibm.db2.jcc.DB2Driver` | None |

- Attach the corresponding JDBC driver JAR via `--jars` or `spark.jars`.
- If the engine is SQL Server (`sqlserver` or `mssql`), include `--properties="dataproc.artifacts.remove=conscrypt"` on Serverless Spark and Dataproc Cluster so TLS handshakes succeed.

## 2. Parallel Partitioning and Subquery Wrapping (`JdbcSource` in `read_sources`)

- **Important Spark JDBC Rule (`query` vs `dbtable` with `partitionColumn`)**: Spark JDBC throws an `AnalysisException` if you set both `.option("query", ...)` and `.option("partitionColumn", ...)`. If a user provides both a custom SQL `query` and parallel partitioning (`partitionColumn`), wrap the SQL statement as an aliased subquery in `dbtable`:
  `.option("dbtable", f"({sql_query}) AS subq")`
- **Dynamic `MIN`/`MAX` Bounds or Explicit Bounds**: Use explicit `lowerBound` and `upperBound` when configured, or query `SELECT MIN(<col>) AS min_v, MAX(<col>) AS max_v` first. Always set `.option("fetchsize", "5000")` (or the configured `fetchsize`) to avoid small row-by-row round-trips.
- **Credentials**: Resolve `passwordSecret` once on the driver via `get_secret()`.

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
    """Reads a relational database table or subquery in parallel over JDBC."""
    dfs: dict[str, DataFrame] = {}

    jdbc_url = config["jdbc_url"]
    driver = config.get("jdbc_driver", "org.postgresql.Driver")
    db_user = config["jdbc_user"]
    db_password = get_secret(config["password_secret"])
    fetch_size = str(config.get("fetchsize", 5000))

    # Wrap custom query as a subquery in dbtable when partitionColumn is enabled
    raw_query = config.get("query")
    dbtable = f"({raw_query}) AS subq" if raw_query else config["dbtable"]

    reader = (
        spark.read.format("jdbc")
        .option("driver", driver)
        .option("url", jdbc_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("dbtable", dbtable)
        .option("fetchsize", fetch_size)
    )

    part_col = config.get("partition_column")
    if part_col:
        lower_bound = config.get("lower_bound")
        upper_bound = config.get("upper_bound")
        if lower_bound is None or upper_bound is None:
            bounds_row = (
                spark.read.format("jdbc")
                .option("driver", driver)
                .option("url", jdbc_url)
                .option("user", db_user)
                .option("password", db_password)
                .option("dbtable", f"(SELECT MIN({part_col}) AS min_v, MAX({part_col}) AS max_v FROM {dbtable}) AS bq")
                .load()
                .first()
            )
            lower_bound = bounds_row["min_v"] if bounds_row and bounds_row["min_v"] is not None else 0
            upper_bound = bounds_row["max_v"] if bounds_row and bounds_row["max_v"] is not None else 1000

        reader = (
            reader.option("partitionColumn", part_col)
            .option("lowerBound", str(lower_bound))
            .option("upperBound", str(upper_bound))
            .option("numPartitions", str(config.get("num_partitions", 8)))
        )

    dfs["jdbc_source"] = reader.load()
    return dfs
```

## 3. Batch and Streaming Write Patterns (`JdbcSink` in `write_sinks`)

`JdbcSink` writes terminal DataFrames via `.write.format("jdbc")` in batch mode and wraps `.write.format("jdbc")` inside `df.writeStream.foreachBatch(...)` in streaming mode (`streamingSinkMode: "foreachBatch"`).

### Required and Performance Options
- **Explicit `driver`**: Always pass `.option("driver", driver)` matching the target database preset (`org.postgresql.Driver`, `com.mysql.cj.jdbc.Driver`, `oracle.jdbc.OracleDriver`, `com.microsoft.sqlserver.jdbc.SQLServerDriver`, or `com.ibm.db2.jcc.DB2Driver`).
- **Batched Statements (`batchsize`)**: Default `.option("batchsize", "5000")` so JDBC `executeBatch()` sends multi-row batches per round-trip. For MySQL, include `rewriteBatchedStatements=true` in the JDBC URL or connection options when high-throughput bulk inserts are needed.
- **Safe Overwrite (`truncate=true` and `cascadeTruncate`)**: When `saveMode` is `"overwrite"`, set `.option("truncate", "true")` by default so Spark runs `TRUNCATE TABLE` instead of dropping and recreating the table. This preserves indexes, foreign keys, and grants. If `cascadeTruncate` is enabled on PostgreSQL or Oracle, also pass `.option("cascadeTruncate", "true")`.
- **Connection Cap (`numPartitions`)**: Pass `.option("numPartitions", str(max_connections))` (for example `8` or `16`) to cap concurrent executor JDBC connections and protect the target database connection pool.
- **Transaction Isolation and DDL (`isolationLevel`, `createTableOptions`)**: Configure `.option("isolationLevel", isolation_level)` (`READ_COMMITTED`, `READ_UNCOMMITTED`, `REPEATABLE_READ`, `SERIALIZABLE`, or `NONE`) and `.option("createTableOptions", create_table_options)` when creating new tables.
- **Credentials**: Resolve the database password once on the driver via `get_secret()` before starting batch writes or streaming queries. Never fetch secrets per row or inside a task loop.

### Batch Write Example

```python
def write_jdbc_df(df: DataFrame, config: dict[str, Any], db_password: str) -> None:
    """Writes a batch or micro-batch DataFrame to a relational table via JDBC."""
    save_mode = config.get("jdbc_save_mode", "append")
    writer = (
        df.write.format("jdbc")
        .mode(save_mode)
        .option("driver", config.get("jdbc_driver", "org.postgresql.Driver"))
        .option("url", config["jdbc_sink_url"])
        .option("dbtable", config["jdbc_sink_table"])
        .option("user", config["jdbc_sink_user"])
        .option("password", db_password)
        .option("batchsize", str(config.get("jdbc_batchsize", 5000)))
        .option("isolationLevel", config.get("jdbc_isolation_level", "READ_COMMITTED"))
    )

    if save_mode == "overwrite" and config.get("jdbc_truncate", True):
        writer = writer.option("truncate", "true")
        if config.get("jdbc_cascade_truncate", False):
            writer = writer.option("cascadeTruncate", "true")

    if config.get("jdbc_num_partitions"):
        writer = writer.option("numPartitions", str(config["jdbc_num_partitions"]))

    if config.get("jdbc_create_table_options"):
        writer = writer.option("createTableOptions", config["jdbc_create_table_options"])

    writer.save()
```

### Streaming `foreachBatch` Write Example

Spark JDBC does not provide a native `DataStreamWriter` sink. Always use `.writeStream.foreachBatch(write_micro_batch)` for streaming branches targeting `JdbcSink`:

```python
def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes streaming micro-batches to a relational database via foreachBatch."""
    checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
    db_password = get_secret(config["jdbc_sink_password_secret"])

    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        try:
            write_jdbc_df(batch_df, config, db_password)
        finally:
            batch_df.unpersist()

    query = (
        dfs["upstream_stream"]
        .writeStream.foreachBatch(write_micro_batch)
        .outputMode(config.get("output_mode", "append"))
        .option("checkpointLocation", f"{checkpoint_root.rstrip('/')}/jdbc_sink")
        .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
        .start()
    )
    query.awaitTermination()
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` with an in-memory `DataFrame`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType
from src.pipeline import transform


def test_jdbc_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without a JDBC database connection."""
    schema = StructType(
        [
            StructField("tx_id", LongType(), False),
            StructField("merchant", StringType(), False),
        ]
    )
    input_df = spark.createDataFrame([(1, "Store-A"), (2, "Store-B")], schema=schema)
    result_dfs = transform({"jdbc_source": input_df}, {})
    assert result_dfs["jdbc_source"].count() == 2
```
