---
name: io-alloydb
description: Read and write Google Cloud AlloyDB for PostgreSQL in PySpark 4.x using the AlloyDB JDBC Socket Factory, Conscrypt removal on Managed Spark, parallel partitioning, safe truncate writes, and streaming foreachBatch.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [AlloyDbSource, AlloyDbSink]
  preloadWhen: []
  sources:
    - prompt
    - https://github.com/GoogleCloudPlatform/alloydb-java-connector
  validated: true
---

> **SDP Mode Note (`paradigm: sdp`)**: In Spark Declarative Pipelines mode, only the source reader from this skill applies inside `@dp.table` or `@dp.materialized_view` dataset functions. Imperative `write_sinks` and `foreachBatch` sink patterns are unsupported in SDP.
# Google Cloud AlloyDB Connector (`AlloyDbSource`, `AlloyDbSink`)

Use these rules when generating PySpark 4.x code for `AlloyDbSource` or `AlloyDbSink` nodes.

## 1. Runtime Classpath and Mandatory Conscrypt Removal

- Attach the PostgreSQL JDBC driver JAR (`org.postgresql.Driver`) and the AlloyDB JDBC Connector Uber JAR (`alloydb-jdbc-connector`) with `--jars` or `spark.jars`.
- **Mandatory on Managed Spark (`ServerlessSpark` and `DataprocCluster`)**: Always pass `--properties="dataproc.artifacts.remove=conscrypt"` in `run_serverless_spark.sh` and `run_dataproc_cluster.sh` (per `src/spark/versionMatrix.ts`). Removing the bundled Conscrypt TLS provider prevents TLS handshake failures with `com.google.cloud.alloydb.SocketFactory`.
- Never access `spark` internal JVM handles in application code.

## 2. AlloyDB Socket Factory URL and Authentication

- Build the JDBC URL using `com.google.cloud.alloydb.SocketFactory`:
  `jdbc:postgresql:///<database>?socketFactory=com.google.cloud.alloydb.SocketFactory&alloydbInstanceName=projects/<project>/locations/<region>/clusters/<cluster>/instances/<instance>&alloydbIpType=<PRIVATE|PUBLIC|PSC>`
- **IAM Authentication (`alloydbEnableIAMAuth=true`)**:
  - When `authMode` is `"iam"` (or `enable_iam_auth` is true), append `&alloydbEnableIAMAuth=true` to the JDBC URL.
  - Set `.option("user", config["db_user"])` and `.option("password", "empty")`.
- **Password Authentication via Secret Manager (`authMode == "secret_manager"`)**:
  - Resolve the password once on the driver using `get_secret(config["password_secret"])` and pass `.option("password", db_password)`. Never hardcode credentials in `src/pipeline.py`.

## 3. Dynamic `MIN`/`MAX` Parallel Partitioning (`AlloyDbSource` in `read_sources`)

Use parallel JDBC reads with `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`, and `fetchsize=5000`. Query `SELECT MIN(<col>), MAX(<col>)` first when static bounds are not configured:

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


def build_alloydb_jdbc_options(config: dict[str, Any]) -> dict[str, str]:
    """Builds AlloyDB Socket Factory JDBC connection options for Spark readers and writers."""
    instance_uri = config["alloydb_instance_name"]
    ip_type = config.get("alloydb_ip_type", "PRIVATE")
    database = config["db_name"]
    auth_mode = config.get("auth_mode", "iam")

    jdbc_url = (
        f"jdbc:postgresql:///{database}"
        f"?socketFactory=com.google.cloud.alloydb.SocketFactory"
        f"&alloydbInstanceName={instance_uri}"
        f"&alloydbIpType={ip_type}"
    )
    if auth_mode == "iam" or config.get("enable_iam_auth", False):
        jdbc_url = f"{jdbc_url}&alloydbEnableIAMAuth=true"
        db_password = "empty"
    else:
        db_password = get_secret(config["password_secret"])

    return {
        "driver": "org.postgresql.Driver",
        "url": jdbc_url,
        "user": config["db_user"],
        "password": db_password,
    }


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads an AlloyDB for PostgreSQL table in parallel using the AlloyDB Socket Factory."""
    dfs: dict[str, DataFrame] = {}
    jdbc_opts = build_alloydb_jdbc_options(config)

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

    dfs["alloydb_source"] = (
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

## 4. Batch Write Patterns (`AlloyDbSink` in `write_sinks`)

`AlloyDbSink` writes DataFrames to AlloyDB for PostgreSQL with `df.write.format("jdbc")`:

- **Batched Multi-Row Inserts**: Set `.option("batchsize", str(config.get("batchsize", 5000)))` and `.option("isolationLevel", config.get("isolation_level", "READ_COMMITTED"))`.
- **Safe `truncate=true` on `overwrite`**: When `saveMode` is `"overwrite"`, set `.mode("overwrite").option("truncate", "true")` (default `True`) so Spark runs `TRUNCATE TABLE` instead of dropping and recreating the table. Preserving the existing DDL protects PostgreSQL primary keys, columnar engine settings, indexes, and foreign keys.
- **Connection Limiting (`numPartitions`)**: When `numPartitions` is configured, pass `.option("numPartitions", str(config["sink_num_partitions"]))` so concurrent Spark tasks stay within AlloyDB connection limits.

```python
import logging
from typing import Any
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_alloydb_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a batch DataFrame to AlloyDB for PostgreSQL via the AlloyDB Socket Factory."""
    jdbc_opts = build_alloydb_jdbc_options(config)
    save_mode = config.get("alloydb_save_mode", "append")
    batch_size = str(config.get("alloydb_batchsize", 5000))
    isolation_level = config.get("alloydb_isolation_level", "READ_COMMITTED")

    writer = (
        df.write.format("jdbc")
        .mode(save_mode)
        .options(**jdbc_opts)
        .option("dbtable", config["alloydb_sink_table"])
        .option("batchsize", batch_size)
        .option("isolationLevel", isolation_level)
    )

    if save_mode == "overwrite" and config.get("alloydb_truncate", True):
        writer = writer.option("truncate", "true")

    if config.get("alloydb_sink_num_partitions"):
        writer = writer.option("numPartitions", str(config["alloydb_sink_num_partitions"]))

    logger.info("Writing DataFrame to AlloyDB table %s", config["alloydb_sink_table"])
    writer.save()
```

## 5. Structured Streaming Micro-Batch Writes (`foreachBatch`)

`AlloyDbSink` uses `streamingSinkMode == "foreachBatch"`:

- Wrap `write_alloydb_batch(batch_df, config)` inside `df.writeStream.foreachBatch(write_micro_batch)`.
- Cache `batch_df` at the start of `write_micro_batch` and call `batch_df.unpersist()` inside a `finally` block.
- Configure `checkpointLocation`, `outputMode`, and `trigger`, then call `spark.streams.awaitAnyTermination()`.

```python
from typing import Any
from pyspark.sql import DataFrame


def start_alloydb_stream(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes a streaming DataFrame to AlloyDB for PostgreSQL via foreachBatch."""
    checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
    checkpoint_location = config.get(
        "alloydb_checkpoint_location", f"{checkpoint_root.rstrip('/')}/alloydb_sink"
    )

    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        try:
            write_alloydb_batch(batch_df, config)
        finally:
            batch_df.unpersist()

    stream_writer = (
        df.writeStream.foreachBatch(write_micro_batch)
        .outputMode(config.get("alloydb_output_mode", "append"))
        .option("checkpointLocation", checkpoint_location)
    )

    if config.get("alloydb_trigger_type") == "availableNow":
        stream_writer.trigger(availableNow=True).start()
    else:
        interval = config.get("alloydb_trigger_interval", "10 seconds")
        stream_writer.trigger(processingTime=interval).start()

    df.sparkSession.streams.awaitAnyTermination()
```

## 6. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` using an in-memory `DataFrame`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StructField, StructType
from src.pipeline import transform


def test_alloydb_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without connecting to AlloyDB."""
    schema = StructType(
        [
            StructField("id", LongType(), False),
            StructField("score", DoubleType(), False),
        ]
    )
    input_df = spark.createDataFrame([(101, 98.5), (102, 87.0)], schema=schema)
    result_dfs = transform({"alloydb_source": input_df}, {})
    assert result_dfs["alloydb_source"].count() == 2
```
