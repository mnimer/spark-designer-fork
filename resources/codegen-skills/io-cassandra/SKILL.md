---
name: io-cassandra
description: Read Apache Cassandra tables in PySpark 4.x using a custom Spark 4.0 Python DataSource backed by cassandra-driver and Secret Manager credentials.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [CassandraSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
  validated: true
---

# Apache Cassandra Source (`CassandraSource`)

Use these rules when generating PySpark 4.x code for a `CassandraSource` node.

## 1. Runtime Delivery Mechanism (`python_datasource`)

- The JVM `spark-cassandra-connector` targets Spark 3.5 and does not publish a Spark 4.0 Scala 2.13 artifact (`src/spark/versionMatrix.ts`). On Spark 4.x runtimes, implement and register a Spark 4.0 Python `DataSource` (`CassandraDataSource` and `CassandraBatchReader`) using the `cassandra-driver` PyPI package.
- Add `cassandra-driver` to `requirements.txt`.
- Fallback note: If running on a custom cluster that provides a compatible `spark-cassandra-connector` assembly JAR, `.format("org.apache.spark.sql.cassandra")` with `.options(table=..., keyspace=...)` can be used as an alternative, but default to `CassandraDataSource` on Spark 4.x.

## 2. `CassandraDataSource` Implementation (`read_sources`)

`CassandraSource` runs in `batch` mode.

### Required and Optional Options
- `contactPoints`: Comma-separated list of Cassandra node hostnames or IP addresses.
- `port`: Native CQL transport port (defaults to `"9042"`).
- `localDc`: Datacenter name for `DCAwareRoundRobinPolicy`.
- `keyspace`: Cassandra keyspace name.
- `table`: Cassandra table name.
- `cqlFilter`: Optional `WHERE` predicate appended to the CQL `SELECT` statement.
- `username` and `password`: Resolve the password once on the driver via `get_secret()` and pass it to the DataSource options.
- Always open the `Cluster` and `Session` lazily inside `read(self, partition)` on the executor and shut down the cluster in a `finally` block.

```python
import base64
from dataclasses import dataclass
import json
from typing import Any, Iterator, Sequence
import urllib.request
import google.auth
import google.auth.transport.requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


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


@dataclass(frozen=True)
class CassandraInputPartition(InputPartition):
    """Serializable partition descriptor for Cassandra CQL reads."""

    partition_id: int


class CassandraBatchReader(DataSourceReader):
    """Spark 4.0 DataSourceReader executing CQL queries with cassandra-driver."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)

    def partitions(self) -> Sequence[InputPartition]:
        return [CassandraInputPartition(partition_id=0)]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster
        from cassandra.policies import DCAwareRoundRobinPolicy

        hosts = [h.strip() for h in self.options["contactPoints"].split(",") if h.strip()]
        port = int(self.options.get("port", "9042"))
        local_dc = self.options.get("localDc", "datacenter1")
        keyspace = self.options["keyspace"]
        table = self.options["table"]
        cql_filter = self.options.get("cqlFilter", "").strip()

        auth_provider = None
        if self.options.get("username") and self.options.get("password"):
            auth_provider = PlainTextAuthProvider(
                username=self.options["username"],
                password=self.options["password"],
            )

        cluster = Cluster(
            contact_points=hosts,
            port=port,
            auth_provider=auth_provider,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=local_dc),
        )
        try:
            session = cluster.connect(keyspace)
            cols = ", ".join(self.schema.fieldNames())
            cql = f"SELECT {cols} FROM {keyspace}.{table}"
            if cql_filter:
                cql = f"{cql} WHERE {cql_filter}"
            for row in session.execute(cql):
                yield tuple(getattr(row, col, None) for col in self.schema.fieldNames())
        finally:
            cluster.shutdown()


class CassandraDataSource(DataSource):
    """Spark 4.0 Python DataSource for Apache Cassandra."""

    @classmethod
    def name(cls) -> str:
        return "cassandra_py"

    def schema(self) -> StructType | str:
        return StructType(
            [
                StructField("sensor_id", StringType(), False),
                StructField("reading", DoubleType(), True),
            ]
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        return CassandraBatchReader(schema, self.options)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Registers CassandraDataSource and reads the target Cassandra table."""
    spark.dataSource.register(CassandraDataSource)
    dfs: dict[str, DataFrame] = {}

    password = get_secret(config["password_secret"]) if config.get("password_secret") else ""
    reader = (
        spark.read.format("cassandra_py")
        .option("contactPoints", config["cassandra_contact_points"])
        .option("port", str(config.get("cassandra_port", 9042)))
        .option("localDc", config.get("cassandra_local_dc", "datacenter1"))
        .option("keyspace", config["cassandra_keyspace"])
        .option("table", config["cassandra_table"])
    )
    if config.get("cassandra_user"):
        reader = reader.option("username", config["cassandra_user"]).option("password", password)
    if config.get("cql_filter"):
        reader = reader.option("cqlFilter", config["cql_filter"])

    dfs["cassandra_source"] = reader.load()
    return dfs
```

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` with an in-memory `DataFrame`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.pipeline import transform


def test_cassandra_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without connecting to Apache Cassandra."""
    schema = StructType(
        [
            StructField("sensor_id", StringType(), False),
            StructField("reading", DoubleType(), True),
        ]
    )
    input_df = spark.createDataFrame([("s-1", 23.4), ("s-2", 18.9)], schema=schema)
    result_dfs = transform({"cassandra_source": input_df}, {})
    assert result_dfs["cassandra_source"].count() == 2
```
