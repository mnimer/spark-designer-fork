---
name: io-singlestore
description: Read SingleStoreDB tables and SQL queries in PySpark 4.x using the SingleStore Spark 4.0 connector with parallel partition reads and Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [SingleStoreSource]
  preloadWhen: []
  sources:
    - prompt
    - https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-spark/
  validated: true
---

# SingleStoreDB Source (`SingleStoreSource`)

Use these rules when generating PySpark 4.x code for a `SingleStoreSource` node.

## 1. Runtime Classpath and Connector Delivery

- Supply the SingleStore Spark 4.0 connector (Scala 2.13) via `--packages com.singlestore:singlestore-spark-connector_2.13:4.1.9-spark-4.0.0` (`spark.jars.packages`).
- Use `.format("singlestore")` (recommended for direct aggregator and leaf partition parallel reads) or `.format("jdbc")` with `org.mariadb.jdbc.Driver`.

## 2. Batch Read Patterns (`read_sources`)

`SingleStoreSource` runs in `batch` mode and supports parallel partition reads across SingleStore leaf nodes.

### Required and Optional Options
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

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

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
