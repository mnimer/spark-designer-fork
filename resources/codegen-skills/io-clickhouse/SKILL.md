---
name: io-clickhouse
description: Read ClickHouse tables and SQL queries in PySpark 4.x using the ClickHouse Spark V2 connector or JDBC driver with Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [ClickHouseSource]
  preloadWhen: []
  sources:
    - prompt
    - https://clickhouse.com/docs/integrations/apache-spark
  validated: true
---

# ClickHouse Source (`ClickHouseSource`)

Use these rules when generating PySpark 4.x code for a `ClickHouseSource` node.

## 1. Runtime Classpath and Connector Delivery

- Supply the ClickHouse Spark 4.0 runtime connector (Scala 2.13) via `--packages com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.8.1` (`spark.jars.packages`), alongside `com.clickhouse:clickhouse-jdbc` when using `.format("jdbc")`.
- Support either `.format("jdbc")` with the official ClickHouse JDBC driver (`com.clickhouse.jdbc.ClickHouseDriver`) or the ClickHouse Spark V2 catalog (`com.clickhouse.spark.ClickHouseCatalog`).

## 2. Batch Read Patterns (`read_sources`)

`ClickHouseSource` runs in `batch` mode.

### Key Configuration Properties
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

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

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
