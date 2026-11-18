---
name: io-cloudsql
description: Read Google Cloud SQL for PostgreSQL, MySQL, and SQL Server in PySpark 4.x using the Cloud SQL JDBC Socket Factory, IAM or Secret Manager authentication, and parallel JDBC partitioning.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [CloudSqlSource]
  preloadWhen: []
  sources:
    - prompt
    - https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory
  validated: true
---

# Google Cloud SQL Source (`CloudSqlSource`)

Use these rules when generating PySpark 4.x code for a `CloudSqlSource` node.

## 1. Runtime Classpath and Socket Factory Configuration

- Attach the database JDBC driver JAR (`postgresql`, `mysql-connector-j`, or `mssql-jdbc`) and the Cloud SQL JDBC Socket Factory Uber JAR via `--jars` / `spark.jars`.
- Construct the JDBC URL with the Cloud SQL Socket Factory parameters:
  - **PostgreSQL**:
    `jdbc:postgresql:///<database>?cloudSqlInstance=<project>:<region>:<instance>&socketFactory=com.google.cloud.sql.postgres.SocketFactory`
  - **MySQL**:
    `jdbc:mysql:///<database>?cloudSqlInstance=<project>:<region>:<instance>&socketFactory=com.google.cloud.sql.mysql.SocketFactory`
- **IAM Database Authentication (`authMode == "iam"`)**:
  - Append `&enableIamAuth=true&sslmode=disable` to the PostgreSQL or MySQL Socket Factory JDBC URL (the Cloud SQL Socket Factory encrypts the tunnel directly).
  - Pass the IAM principal username via `.option("user", config["db_user"])` and `.option("password", "empty")`.
- **Password Authentication via Secret Manager (`authMode == "secret_manager"`)**:
  - Resolve the password once on the driver using `get_secret(config["password_secret"])` (Google Cloud Secret Manager REST helper) and pass `.option("password", db_password)`. Never hardcode passwords in `src/pipeline.py`.

## 2. Dynamic Parallel Partitioning (`read_sources`)

Never read a large Cloud SQL table on a single JDBC connection. Use Spark's parallel JDBC partitioning with `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`, and `fetchsize=5000`:

1. **Explicit bounds**: When `lowerBound` and `upperBound` are provided in the node configuration, pass them directly alongside `partitionColumn` and `numPartitions`.
2. **Dynamic `MIN`/`MAX` bounds query**: When `partitionColumn` is configured without static bounds, execute a fast metadata query `(SELECT MIN(<col>) AS min_id, MAX(<col>) AS max_id FROM <table>) AS bounds_q` first, collect the single row on the driver, and pass the discovered `min_id` and `max_id` as `lowerBound` and `upperBound`.
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


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads a Cloud SQL table in parallel using the Cloud SQL Socket Factory."""
    dfs: dict[str, DataFrame] = {}

    instance_conn = config["cloud_sql_instance"]  # <project>:<region>:<instance>
    database = config["db_name"]
    auth_mode = config.get("auth_mode", "iam")
    engine = config.get("db_engine", "postgres")

    if engine == "mysql":
        driver = "com.mysql.cj.jdbc.Driver"
        socket_factory = "com.google.cloud.sql.mysql.SocketFactory"
        base_url = f"jdbc:mysql:///{database}?cloudSqlInstance={instance_conn}&socketFactory={socket_factory}"
    else:
        driver = "org.postgresql.Driver"
        socket_factory = "com.google.cloud.sql.postgres.SocketFactory"
        base_url = f"jdbc:postgresql:///{database}?cloudSqlInstance={instance_conn}&socketFactory={socket_factory}"

    if auth_mode == "iam":
        jdbc_url = f"{base_url}&enableIamAuth=true&sslmode=disable"
        db_password = "empty"
    else:
        jdbc_url = base_url
        db_password = get_secret(config["password_secret"])

    target_table = config["dbtable"]
    part_col = config.get("partition_column", "id")
    num_parts = int(config.get("num_partitions", 8))

    bounds_row = (
        spark.read.format("jdbc")
        .option("driver", driver)
        .option("url", jdbc_url)
        .option("user", config["db_user"])
        .option("password", db_password)
        .option("dbtable", f"(SELECT MIN({part_col}) AS min_v, MAX({part_col}) AS max_v FROM {target_table}) AS bounds_q")
        .load()
        .first()
    )
    lower_bound = str(bounds_row["min_v"] if bounds_row and bounds_row["min_v"] is not None else 0)
    upper_bound = str(bounds_row["max_v"] if bounds_row and bounds_row["max_v"] is not None else 1000)

    dfs["cloudsql_source"] = (
        spark.read.format("jdbc")
        .option("driver", driver)
        .option("url", jdbc_url)
        .option("user", config["db_user"])
        .option("password", db_password)
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

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

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
    input_df = spark.createDataFrame([(1, "alice@example.com"), (2, "bob@example.com")], schema=schema)
    result_dfs = transform({"cloudsql_source": input_df}, {})
    assert result_dfs["cloudsql_source"].count() == 2
```
