---
name: io-firestore
description: Read Google Cloud Firestore collections in PySpark 4.x using a custom Spark 4.0 Python DataSourceReader and the google-cloud-firestore SDK.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [FirestoreSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
  validated: true
---

# Google Cloud Firestore Source (`FirestoreSource`)

Use these rules when generating PySpark 4.x code for a `FirestoreSource` node.

## 1. Runtime Delivery Mechanism (`python_datasource`)

- Google Cloud Firestore does not publish a Spark 4.x Scala 2.13 JVM connector JAR. Always implement a Spark 4.0 Python `DataSource` (`FirestoreDataSource` and `FirestoreBatchReader` subclassing `DataSource` and `DataSourceReader` from `pyspark.sql.datasource`) backed by `google-cloud-firestore`.
- Include `google-cloud-firestore` in `requirements.txt`.
- Register `FirestoreDataSource` on the active `SparkSession` via `spark.dataSource.register(FirestoreDataSource)` inside `read_sources(spark, config)`.

## 2. `FirestoreDataSource` Implementation and Batch Read (`read_sources`)

`FirestoreSource` runs in `batch` mode.

### Key Options and Rules
- `projectId`: Google Cloud project ID hosting Firestore.
- `databaseId`: Firestore database ID (defaults to `"(default)"`).
- `collectionPath`: Collection path or collection group name to query.
- Optional field filters: Support `filterField`, `filterOp` (such as `"=="`, `">="`, `"<="`), and `filterValue` via `google.cloud.firestore_v1.base_query.FieldFilter`.
- **Lazy Executor Client Initialization**: Always instantiate `firestore.Client(project=..., database=...)` inside `read(self, partition)` on the executor worker. Never instantiate the gRPC client in `__init__` on the driver because `DataSourceReader` instances are pickled and sent to executors.
- **Explicit Schema**: Define an explicit `StructType` schema and project each document dictionary (`doc.to_dict()`) into a tuple matching `self.schema.fieldNames()`.

```python
from dataclasses import dataclass
from typing import Any, Iterator, Sequence
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


@dataclass(frozen=True)
class FirestoreInputPartition(InputPartition):
    """Serializable partition descriptor for Firestore reads."""

    partition_id: int


class FirestoreBatchReader(DataSourceReader):
    """Spark 4.0 DataSourceReader that streams documents from Google Cloud Firestore."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)

    def partitions(self) -> Sequence[InputPartition]:
        return [FirestoreInputPartition(partition_id=0)]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        from google.cloud import firestore
        from google.cloud.firestore_v1.base_query import FieldFilter

        project_id = self.options["projectId"]
        database_id = self.options.get("databaseId", "(default)")
        collection_path = self.options["collectionPath"]

        client = firestore.Client(project=project_id, database=database_id)
        query = client.collection(collection_path)

        filter_field = self.options.get("filterField")
        filter_op = self.options.get("filterOp")
        filter_val = self.options.get("filterValue")
        if filter_field and filter_op and filter_val is not None:
            query = query.where(filter=FieldFilter(filter_field, filter_op, filter_val))

        field_names = self.schema.fieldNames()
        for doc in query.stream():
            payload = doc.to_dict() or {}
            payload.setdefault("document_id", doc.id)
            yield tuple(payload.get(col) for col in field_names)


class FirestoreDataSource(DataSource):
    """Spark 4.0 Python DataSource for Google Cloud Firestore."""

    @classmethod
    def name(cls) -> str:
        return "firestore"

    def schema(self) -> StructType | str:
        return StructType(
            [
                StructField("document_id", StringType(), False),
                StructField("user_id", StringType(), True),
                StructField("score", DoubleType(), True),
            ]
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        return FirestoreBatchReader(schema, self.options)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Registers FirestoreDataSource and reads the configured Firestore collection."""
    spark.dataSource.register(FirestoreDataSource)
    dfs: dict[str, DataFrame] = {}

    reader = (
        spark.read.format("firestore")
        .option("projectId", config["firestore_project_id"])
        .option("databaseId", config.get("firestore_database_id", "(default)"))
        .option("collectionPath", config["firestore_collection_path"])
    )
    if config.get("filter_field"):
        reader = (
            reader.option("filterField", config["filter_field"])
            .option("filterOp", config.get("filter_op", "=="))
            .option("filterValue", str(config["filter_value"]))
        )

    dfs["firestore_source"] = reader.load()
    return dfs
```

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by constructing an in-memory `DataFrame` with the same `StructType` schema:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from src.pipeline import transform


def test_firestore_pipeline_transform(spark: SparkSession) -> None:
    """Tests transformation logic offline without calling the Firestore API."""
    schema = StructType(
        [
            StructField("document_id", StringType(), False),
            StructField("user_id", StringType(), True),
            StructField("score", DoubleType(), True),
        ]
    )
    input_df = spark.createDataFrame(
        [("doc-1", "u-10", 95.0), ("doc-2", "u-20", 88.5)],
        schema=schema,
    )
    result_dfs = transform({"firestore_source": input_df}, {})
    assert result_dfs["firestore_source"].count() == 2
```
