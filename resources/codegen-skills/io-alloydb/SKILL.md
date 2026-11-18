---
name: io-alloydb
description: Read Google Cloud AlloyDB for PostgreSQL in PySpark 4.x using the AlloyDB JDBC Socket Factory, Conscrypt removal on Managed Spark, and parallel JDBC partitioning.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [AlloyDbSource]
  preloadWhen: []
  sources:
    - prompt
    - https://github.com/GoogleCloudPlatform/alloydb-java-connector
  validated: true
---

# Google Cloud AlloyDB Source (`AlloyDbSource`)

Use these rules when generating PySpark 4.x code for an `AlloyDbSource` node.

## 1. Runtime Classpath and Mandatory Conscrypt Removal

- Attach the PostgreSQL JDBC driver JAR and the AlloyDB JDBC Connector Uber JAR (`alloydb-jdbc-connector`) via `--jars` / `spark.jars`.
- **Mandatory on Managed Spark (`ServerlessSpark` and `DataprocCluster`)**: Always pass `--properties="dataproc.artifacts.remove=conscrypt"` in `run_serverless_spark.sh` and `run_dataproc_cluster.sh` (per `src/spark/versionMatrix.ts`). Removing the bundled Conscrypt TLS provider prevents TLS handshake failures with the AlloyDB Java Socket Factory.

## 2. AlloyDB Socket Factory URL and Authentication

- Build the JDBC URL using `com.google.cloud.alloydb.SocketFactory`:
  `jdbc:postgresql:///<database>?socketFactory=com.google.cloud.alloydb.SocketFactory&alloydbInstanceName=projects/<project>/locations/<region>/clusters/<cluster>/instances/<instance>&alloydbIpType=<PRIVATE|PUBLIC|PSC>`
- **IAM Authentication (`alloydbEnableIAMAuth=true`)**:
  - Append `&alloydbEnableIAMAuth=true` to the JDBC URL.
  - Set `.option("user", config["alloydb_user"])` and `.option("password", "empty")`.
- **Password Authentication via Secret Manager**:
  - Resolve the password once on the driver using `get_secret(config["password_secret"])` and pass `.option("password", db_password)`.

## 3. Dynamic `MIN`/`MAX` Parallel Partitioning (`read_sources`)

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


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Reads an AlloyDB for PostgreSQL table in parallel using the AlloyDB Socket Factory."""
    dfs: dict[str, DataFrame] = {}

    instance_uri = config["alloydb_instance_name"]  # projects/<p>/locations/<r>/clusters/<c>/instances/<i>
    ip_type = config.get("alloydb_ip_type", "PRIVATE")
    database = config["db_name"]

    jdbc_url = (
        f"jdbc:postgresql:///{database}"
        f"?socketFactory=com.google.cloud.alloydb.SocketFactory"
        f"&alloydbInstanceName={instance_uri}"
        f"&alloydbIpType={ip_type}"
    )
    if config.get("enable_iam_auth", True):
        jdbc_url = f"{jdbc_url}&alloydbEnableIAMAuth=true"
        db_password = "empty"
    else:
        db_password = get_secret(config["password_secret"])

    target_table = config["dbtable"]
    part_col = config.get("partition_column", "id")
    num_parts = int(config.get("num_partitions", 8))

    bounds_row = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", jdbc_url)
        .option("user", config["db_user"])
        .option("password", db_password)
        .option("dbtable", f"(SELECT MIN({part_col}) AS min_v, MAX({part_col}) AS max_v FROM {target_table}) AS bounds_q")
        .load()
        .first()
    )
    lower_bound = str(bounds_row["min_v"] if bounds_row and bounds_row["min_v"] is not None else 0)
    upper_bound = str(bounds_row["max_v"] if bounds_row and bounds_row["max_v"] is not None else 1000)

    dfs["alloydb_source"] = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
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

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

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
