---
name: io-cassandra
description: Read and write Apache Cassandra tables in PySpark 4.x using Spark 4.0 Python DataSource cassandra-driver or spark-cassandra-connector with Secret Manager auth.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [CassandraSource, CassandraSink]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
    - https://github.com/datastax/spark-cassandra-connector
  validated: true
---

# Apache Cassandra Source and Sink (`CassandraSource`, `CassandraSink`)

Use these rules when generating PySpark 4.x code for `CassandraSource` and `CassandraSink` nodes.

## 1. Runtime Delivery Mechanism (`python_datasource` and `foreach_batch_sdk`)

- The OSS JVM `spark-cassandra-connector` targets Spark 3.5 and lags Spark 4.x Scala 2.13 runtimes (`src/spark/versionMatrix.ts`). Default to a Spark 4.0 Python `DataSource` (`CassandraDataSource` with `CassandraBatchReader` and `CassandraBatchWriter`) backed by the `cassandra-driver` PyPI package.
- Add `cassandra-driver` to `requirements.txt`.
- When a custom cluster explicitly provides a Spark 4.x compatible `spark-cassandra-connector` assembly JAR (`connectorBackend == "jvm_connector"`), use `.format("org.apache.spark.sql.cassandra")` for batch and native Structured Streaming writes.

## 2. `CassandraSource` Implementation (`read_sources`)

`CassandraSource` runs in `batch` mode.

### Required and Optional Read Options
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
import ssl
from typing import Any, Iterator, Sequence
import urllib.request
import google.auth
import google.auth.transport.requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
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


@dataclass
class CassandraCommitMessage(WriterCommitMessage):
    """Commit summary returned from each Cassandra partition writer."""

    rows_written: int


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
```

## 3. `CassandraSink` Write Patterns (`write_sinks`)

`CassandraSink` supports two runtime patterns:
1. **Spark 4.0 Python `DataSourceWriter` + `foreachBatch` (`cassandra-driver`, Default)**: Works on every Spark 4.x runtime without JVM connector version mismatches.
2. **Native JVM Connector (`.format("org.apache.spark.sql.cassandra")`)**: Available when a compatible Spark Cassandra Connector JAR is attached (`df.write.format("org.apache.spark.sql.cassandra")` and `df.writeStream.format("org.apache.spark.sql.cassandra")`).

### Key Write Options
- `keyspace` and `table`: Destination Cassandra keyspace and table names.
- `consistencyLevel`: CQL write consistency level such as `"LOCAL_QUORUM"` (default), `"QUORUM"`, `"LOCAL_ONE"`, `"ONE"`, or `"ALL"`.
- `ttlColumn`: Optional column name (or positive integer literal in seconds) mapped to CQL `USING TTL`.
- `writetimeColumn`: Optional microsecond epoch column mapped to CQL `USING TIMESTAMP` for deterministic idempotent replays.
- `concurrency`: Concurrent in-flight prepared statements per executor partition via `execute_concurrent_with_args` (defaults to `"100"`).
- Resolve `password` once on the driver via `get_secret()`.

### Pattern A: Default Spark 4.0 Python `DataSourceWriter` and `foreachBatch` (`cassandra-driver`)

```python
class CassandraBatchWriter(DataSourceWriter):
    """Spark 4.0 DataSourceWriter executing concurrent prepared CQL upserts."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)

    def write(self, iterator: Iterator[Any]) -> WriterCommitMessage:
        from cassandra import ConsistencyLevel
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster
        from cassandra.concurrent import execute_concurrent_with_args
        from cassandra.policies import DCAwareRoundRobinPolicy

        hosts = [h.strip() for h in self.options["contactPoints"].split(",") if h.strip()]
        port = int(self.options.get("port", "9042"))
        local_dc = self.options.get("localDc", "datacenter1")
        keyspace = self.options["keyspace"]
        table = self.options["table"]
        ttl_col = self.options.get("ttlColumn", "").strip()
        writetime_col = self.options.get("writetimeColumn", "").strip()
        concurrency = int(self.options.get("concurrency", "100"))
        use_ssl = self.options.get("ssl", "false").lower() == "true"

        consistency_name = self.options.get("consistencyLevel", "LOCAL_QUORUM").upper()
        consistency = getattr(
            ConsistencyLevel, consistency_name, ConsistencyLevel.LOCAL_QUORUM
        )

        auth_provider = None
        if self.options.get("username") and self.options.get("password"):
            auth_provider = PlainTextAuthProvider(
                username=self.options["username"],
                password=self.options["password"],
            )

        ssl_context = ssl.create_default_context() if use_ssl else None
        cluster = Cluster(
            contact_points=hosts,
            port=port,
            auth_provider=auth_provider,
            ssl_context=ssl_context,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=local_dc),
        )
        try:
            session = cluster.connect(keyspace)
            all_fields = list(self.schema.fieldNames())
            data_cols = [
                c for c in all_fields if c not in {ttl_col, writetime_col}
            ]
            placeholders = ", ".join("?" for _ in data_cols)
            col_list = ", ".join(data_cols)
            cql = f"INSERT INTO {keyspace}.{table} ({col_list}) VALUES ({placeholders})"

            using_clauses: list[str] = []
            extra_cols: list[str] = []
            if ttl_col:
                if ttl_col.isdigit():
                    using_clauses.append(f"TTL {int(ttl_col)}")
                elif ttl_col in all_fields:
                    using_clauses.append("TTL ?")
                    extra_cols.append(ttl_col)
            if writetime_col and writetime_col in all_fields:
                using_clauses.append("TIMESTAMP ?")
                extra_cols.append(writetime_col)
            if using_clauses:
                cql = f"{cql} USING {' AND '.join(using_clauses)}"

            prepared = session.prepare(cql)
            prepared.consistency_level = consistency
            param_cols = data_cols + extra_cols

            buffer: list[tuple[Any, ...]] = []
            total_written = 0
            for row in iterator:
                row_dict = row.asDict(recursive=True)
                buffer.append(tuple(row_dict.get(col) for col in param_cols))
                if len(buffer) >= 500:
                    execute_concurrent_with_args(
                        session, prepared, buffer, concurrency=concurrency
                    )
                    total_written += len(buffer)
                    buffer.clear()

            if buffer:
                execute_concurrent_with_args(
                    session, prepared, buffer, concurrency=concurrency
                )
                total_written += len(buffer)
                buffer.clear()

            return CassandraCommitMessage(rows_written=total_written)
        finally:
            cluster.shutdown()

    def commit(self, messages: list[WriterCommitMessage | None]) -> None:
        pass

    def abort(self, messages: list[WriterCommitMessage | None]) -> None:
        pass


class CassandraDataSource(DataSource):
    """Spark 4.0 Python DataSource for Apache Cassandra reads and writes."""

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

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return CassandraBatchWriter(schema, self.options)
```

### Pattern B: Unified `write_sinks` Supporting Python Fallback and Native JVM Connector

```python
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


def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes DataFrames to Apache Cassandra via cassandra-driver or spark-cassandra-connector."""
    df = dfs["cassandra_sink_input"]
    spark = df.sparkSession
    password = get_secret(config["password_secret"]) if config.get("password_secret") else ""
    backend = config.get("cassandra_connector_backend", "python_driver")
    checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
    checkpoint_path = config.get(
        "cassandra_checkpoint_location",
        f"{checkpoint_root.rstrip('/')}/cassandra_sink",
    )

    if backend == "jvm_connector":
        jvm_opts: dict[str, str] = {
            "keyspace": config["cassandra_keyspace"],
            "table": config["cassandra_table"],
            "spark.cassandra.connection.host": config["cassandra_contact_points"],
            "spark.cassandra.connection.port": str(config.get("cassandra_port", 9042)),
            "spark.cassandra.connection.localDC": config.get("cassandra_local_dc", "datacenter1"),
            "spark.cassandra.output.consistency.level": config.get(
                "cassandra_consistency_level", "LOCAL_QUORUM"
            ),
        }
        if config.get("cassandra_user"):
            jvm_opts["spark.cassandra.auth.username"] = config["cassandra_user"]
            jvm_opts["spark.cassandra.auth.password"] = password
        if config.get("cassandra_ttl_column"):
            jvm_opts["spark.cassandra.output.ttl"] = str(config["cassandra_ttl_column"])
        if config.get("cassandra_writetime_column"):
            jvm_opts["spark.cassandra.output.timestamp"] = config["cassandra_writetime_column"]

        if df.isStreaming:
            (
                df.writeStream.format("org.apache.spark.sql.cassandra")
                .outputMode(config.get("output_mode", "append"))
                .options(**jvm_opts)
                .option("checkpointLocation", checkpoint_path)
                .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
                .start()
            )
            spark.streams.awaitAnyTermination()
        else:
            df.write.format("org.apache.spark.sql.cassandra").mode("append").options(**jvm_opts).save()
        return

    spark.dataSource.register(CassandraDataSource)
    py_opts: dict[str, str] = {
        "contactPoints": config["cassandra_contact_points"],
        "port": str(config.get("cassandra_port", 9042)),
        "localDc": config.get("cassandra_local_dc", "datacenter1"),
        "keyspace": config["cassandra_keyspace"],
        "table": config["cassandra_table"],
        "consistencyLevel": config.get("cassandra_consistency_level", "LOCAL_QUORUM"),
        "concurrency": str(config.get("cassandra_concurrency", 100)),
        "ssl": str(config.get("cassandra_ssl", False)).lower(),
    }
    if config.get("cassandra_user"):
        py_opts["username"] = config["cassandra_user"]
        py_opts["password"] = password
    if config.get("cassandra_ttl_column"):
        py_opts["ttlColumn"] = str(config["cassandra_ttl_column"])
    if config.get("cassandra_writetime_column"):
        py_opts["writetimeColumn"] = config["cassandra_writetime_column"]

    if df.isStreaming:
        def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
            batch_df.write.format("cassandra_py").mode("append").options(**py_opts).save()

        (
            df.writeStream.foreachBatch(write_micro_batch)
            .outputMode(config.get("output_mode", "append"))
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=config.get("trigger_interval", "10 seconds"))
            .start()
        )
        spark.streams.awaitAnyTermination()
    else:
        df.write.format("cassandra_py").mode("append").options(**py_opts).save()
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

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
