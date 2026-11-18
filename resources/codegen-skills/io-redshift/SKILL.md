---
name: io-redshift
description: Read Amazon Redshift tables and SQL queries in PySpark 4.x using the Spark Redshift Community connector with S3 staging and Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [RedshiftSource]
  preloadWhen: []
  sources:
    - prompt
    - https://github.com/spark-redshift-community/spark-redshift
  validated: true
---

# Amazon Redshift Source (`RedshiftSource`)

Use these rules when generating PySpark 4.x code for a `RedshiftSource` node.

## 1. Runtime Classpath and Connector Delivery

- Supply the Spark 4.0 Redshift Community connector via `--packages io.github.spark-redshift-community:spark-redshift_2.13:6.3.0-spark_4.0` (`spark.jars.packages`), plus the Redshift JDBC driver JAR if not already on the classpath.
- Use `.format("io.github.spark_redshift_community.spark.redshift")` in `spark.read`.

## 2. Batch Read Patterns and S3 Unload Staging (`read_sources`)

`RedshiftSource` executes a parallel `UNLOAD` to an Amazon S3 staging path (`s3a://...`) and reads the unloaded Parquet or text splits in `batch` mode.

### Required Options
- `url`: Redshift JDBC connection URL (`jdbc:redshift://<endpoint>:5439/<database>`). Authenticate with `.option("user", ...)` and `.option("password", get_secret(...))` or embed credentials obtained from `get_secret()`.
- `dbtable` or `query`: Specify either `.option("dbtable", "<schema>.<table>")` or `.option("query", "<sql_query>")`.
- `tempdir`: S3 staging URI (`s3a://<bucket>/redshift-temp/`).
- `aws_iam_role` or `forward_spark_s3_credentials`: Prefer `.option("aws_iam_role", "arn:aws:iam::<account>:role/<role>")` so Redshift assumes the IAM role directly for `UNLOAD`, or set `.option("forward_spark_s3_credentials", "true")` when authenticating S3 via `spark.hadoop.fs.s3a.*` credentials fetched with `get_secret()`.

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
    """Reads an Amazon Redshift table or query via S3 staging."""
    dfs: dict[str, DataFrame] = {}

    rs_password = get_secret(config["redshift_password_secret"])
    reader = (
        spark.read.format("io.github.spark_redshift_community.spark.redshift")
        .option("url", config["redshift_jdbc_url"])
        .option("user", config["redshift_user"])
        .option("password", rs_password)
        .option("tempdir", config["redshift_tempdir"])
    )

    if config.get("aws_iam_role"):
        reader = reader.option("aws_iam_role", config["aws_iam_role"])
    else:
        reader = reader.option("forward_spark_s3_credentials", "true")

    if config.get("redshift_query"):
        dfs["redshift_source"] = reader.option("query", config["redshift_query"]).load()
    else:
        dfs["redshift_source"] = reader.option("dbtable", config["redshift_table"]).load()

    return dfs
```

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` using an in-memory `DataFrame`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.pipeline import transform


def test_redshift_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without connecting to Amazon Redshift or S3."""
    schema = StructType(
        [
            StructField("sale_id", StringType(), False),
            StructField("total", DoubleType(), False),
        ]
    )
    input_df = spark.createDataFrame([("s-1", 49.99), ("s-2", 149.99)], schema=schema)
    result_dfs = transform({"redshift_source": input_df}, {})
    assert result_dfs["redshift_source"].count() == 2
```
