---
name: io-redshift
description: Read and write Amazon Redshift tables in PySpark 4.x using the Spark Redshift Community connector with S3 staging and Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [RedshiftSource, RedshiftSink]
  preloadWhen: []
  sources:
    - prompt
    - https://github.com/spark-redshift-community/spark-redshift
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
# Amazon Redshift Connector (`RedshiftSource` and `RedshiftSink`)

Use these rules when generating PySpark 4.x code for `RedshiftSource` and `RedshiftSink` nodes.

## 1. Runtime Classpath and Connector Delivery

- Supply the Spark 4.0 Redshift Community connector via `--packages io.github.spark-redshift-community:spark-redshift_2.13:6.3.0-spark_4.0` (`spark.jars.packages`), plus the Redshift JDBC driver JAR if not already on the classpath.
- On Managed Spark (`ServerlessSpark` and `DataprocCluster`), `hadoop-aws` and `gcs-connector` are pre-installed. Never pass `hadoop-aws` or `gcs-connector` to `spark.jars.packages` on Managed Spark.
- Use `.format("io.github.spark_redshift_community.spark.redshift")` for both `spark.read` and `df.write`.

## 2. Batch Read Patterns and S3 Unload Staging (`RedshiftSource` in `read_sources`)

`RedshiftSource` executes a parallel `UNLOAD` to an Amazon S3 staging path (`s3a://...`) and reads the unloaded Parquet or text splits in `batch` mode.

### Required Read Options
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

## 3. Batch and Streaming Write Patterns (`RedshiftSink` in `write_sinks`)

`RedshiftSink` stages Spark partitions to `tempdir` (`s3a://...`) and issues a parallel Redshift `COPY` command. In streaming pipelines (`streamingSinkMode: "foreachBatch"`), wrap the batch write inside `df.writeStream.foreachBatch(...)`.

### Required and Optional Write Options
- **Connection and Staging (`url`, `dbtable`, `tempdir`, `tempformat`)**: Pass `.option("url", jdbc_url)`, `.option("dbtable", "<schema>.<table>")`, `.option("tempdir", "s3a://<bucket>/redshift-temp/")`, and `.option("tempformat", temp_format)` (`"AVRO"` default or `"CSV"` / `"CSV GZIP"`).
- **IAM Role or Temporary S3 Credentials**: Pass `.option("aws_iam_role", iam_role_arn)` when Redshift assumes an IAM role for `COPY`. Otherwise set `.option("forward_spark_s3_credentials", "true")` and configure temporary or static S3A credentials resolved once via `get_secret()`.
- **SQL Hooks (`preactions` and `postactions`)**: Pass `.option("preactions", sql_text)` and `.option("postactions", sql_text)` for idempotent staging merges (`DELETE` + `INSERT` in a transaction) or post-load `GRANT` statements.
- **Physical Table Distribution and Sort Keys (`diststyle`, `distkey`, `sortkeyspec`)**: When creating or overwriting Redshift tables, pass `.option("diststyle", dist_style)` (`"AUTO"`, `"EVEN"`, `"KEY"`, or `"ALL"`), `.option("distkey", dist_key)` when `diststyle == "KEY"`, and `.option("sortkeyspec", sort_key_spec)` (for example `"SORTKEY(sale_date, region)"`).

### Batch Write Example

```python
def write_redshift_df(df: DataFrame, config: dict[str, Any], rs_password: str) -> None:
    """Writes a batch or micro-batch DataFrame to Amazon Redshift via S3 COPY."""
    save_mode = config.get("redshift_save_mode", "append")
    dist_style = config.get("redshift_diststyle", "AUTO")

    writer = (
        df.write.format("io.github.spark_redshift_community.spark.redshift")
        .mode(save_mode)
        .option("url", config["redshift_sink_url"])
        .option("user", config["redshift_sink_user"])
        .option("password", rs_password)
        .option("dbtable", config["redshift_sink_table"])
        .option("tempdir", config["redshift_sink_tempdir"])
        .option("tempformat", config.get("redshift_tempformat", "AVRO"))
    )

    if config.get("redshift_aws_iam_role"):
        writer = writer.option("aws_iam_role", config["redshift_aws_iam_role"])
    else:
        writer = writer.option("forward_spark_s3_credentials", "true")

    if dist_style and dist_style != "AUTO":
        writer = writer.option("diststyle", dist_style)
        if dist_style == "KEY" and config.get("redshift_distkey"):
            writer = writer.option("distkey", config["redshift_distkey"])

    if config.get("redshift_sortkeyspec"):
        writer = writer.option("sortkeyspec", config["redshift_sortkeyspec"])
    if config.get("redshift_preactions"):
        writer = writer.option("preactions", config["redshift_preactions"])
    if config.get("redshift_postactions"):
        writer = writer.option("postactions", config["redshift_postactions"])

    writer.save()
```

### Streaming `foreachBatch` Write Example

```python
def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes streaming micro-batches to Amazon Redshift via foreachBatch."""
    checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
    rs_password = get_secret(config["redshift_sink_password_secret"])

    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        try:
            write_redshift_df(batch_df, config, rs_password)
        finally:
            batch_df.unpersist()

    query = (
        dfs["upstream_stream"]
        .writeStream.foreachBatch(write_micro_batch)
        .outputMode(config.get("output_mode", "append"))
        .option("checkpointLocation", f"{checkpoint_root.rstrip('/')}/redshift_sink")
        .trigger(processingTime=config.get("trigger_interval", "30 seconds"))
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
