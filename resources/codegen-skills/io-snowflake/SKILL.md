---
name: io-snowflake
description: Read Snowflake tables and SQL queries in PySpark 4.x using the Spark Snowflake connector and Secret Manager password or PEM private key credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [SnowflakeSource]
  preloadWhen: []
  sources:
    - prompt
    - https://docs.snowflake.com/en/user-guide/spark-connector-use
  validated: true
---

# Snowflake Source (`SnowflakeSource`)

Use these rules when generating PySpark 4.x code for a `SnowflakeSource` node.

## 1. Runtime Classpath and Connector Delivery

- Supply the Snowflake Spark connector (Scala 2.13) via `--packages net.snowflake:spark-snowflake_2.13:3.1.0` (`spark.jars.packages`).
- Use `.format("net.snowflake.spark.snowflake")` in `spark.read`.

## 2. Batch Read Patterns and Authentication (`read_sources`)

`SnowflakeSource` runs in `batch` mode and supports both password authentication (`sfPassword`) and key-pair authentication (`pem_private_key`).

### Required and Optional Options
- `sfURL`: Snowflake account endpoint (for example `<account>.snowflakecomputing.com`).
- `sfWarehouse`: Snowflake virtual warehouse name.
- `sfDatabase`: Database name.
- `sfSchema`: Schema name (for example `PUBLIC`).
- `sfRole`: Optional Snowflake role.
- `sfUser`: Snowflake login username.
- `sfPassword` or `pem_private_key`: Resolve the secret payload once on the driver via `get_secret()` from Google Cloud Secret Manager. When using a PEM private key, strip the `-----BEGIN PRIVATE KEY-----` and `-----END PRIVATE KEY-----` headers and newlines before passing `pem_private_key`.
- `dbtable` or `query`: Pass `.option("dbtable", "<table_name>")` for a full table or `.option("query", "<sql_query>")` for a custom SQL projection or filter.

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
    """Reads a Snowflake table or query using the Spark Snowflake connector."""
    dfs: dict[str, DataFrame] = {}

    sf_options: dict[str, str] = {
        "sfURL": config["sf_url"],
        "sfWarehouse": config["sf_warehouse"],
        "sfDatabase": config["sf_database"],
        "sfSchema": config.get("sf_schema", "PUBLIC"),
        "sfUser": config["sf_user"],
    }
    if config.get("sf_role"):
        sf_options["sfRole"] = config["sf_role"]

    if config.get("private_key_secret"):
        raw_pem = get_secret(config["private_key_secret"])
        cleaned_pem = (
            raw_pem.replace("-----BEGIN PRIVATE KEY-----", "")
            .replace("-----END PRIVATE KEY-----", "")
            .replace("\n", "")
            .strip()
        )
        sf_options["pem_private_key"] = cleaned_pem
    else:
        sf_options["sfPassword"] = get_secret(config["password_secret"])

    if config.get("sf_query"):
        sf_options["query"] = config["sf_query"]
    else:
        sf_options["dbtable"] = config["sf_table"]

    dfs["snowflake_source"] = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .load()
    )
    return dfs
```

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` using an in-memory `DataFrame` without connecting to Snowflake:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.pipeline import transform


def test_snowflake_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without connecting to Snowflake."""
    schema = StructType(
        [
            StructField("REGION", StringType(), False),
            StructField("REVENUE", DoubleType(), False),
        ]
    )
    input_df = spark.createDataFrame([("NA", 1200.0), ("EU", 950.0)], schema=schema)
    result_dfs = transform({"snowflake_source": input_df}, {})
    assert result_dfs["snowflake_source"].count() == 2
```
