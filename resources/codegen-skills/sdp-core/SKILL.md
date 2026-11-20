---
name: sdp-core
description: Core Apache Spark 4.2 Declarative Pipelines Python API rules, dataset decorators, pure helper separation, and strict bans on imperative actions and Databricks-only APIs.
metadata:
  kind: language
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [sdp]
  transforms: []
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html
    - https://cloud.google.com/dataproc-serverless/docs/concepts/versions/dataproc-serverless-versions
  validated: true
---

# Spark Declarative Pipelines Core Python Architecture (`pyspark.pipelines`, Spark 4.2+)

Follow these rules in every generated Spark Declarative Pipelines (`paradigm: sdp`) project targeting Apache Spark 4.2 or later.

## 1. Project Structure & Testable Pure Helper Separation

Organize every SDP repository so declarative graph registration in `transformations/` stays decoupled from pure Catalyst DataFrame logic in `src/transforms.py`:

```text
<project>/
├── spark-pipeline.yml                  # SDP pipeline specification (sdp-spec)
├── transformations/
│   └── pipeline_datasets.py            # Python @dp.* dataset and flow declarations only
├── src/
│   └── transforms.py                   # Pure stateless DataFrame helpers (no dp decorators, no I/O)
├── tests/
│   └── test_transforms.py              # Offline pytest suite testing src.transforms in memory
├── sdp_stage.pipeline.yaml             # Managed Data Pipelines sdp stage manifest (runner-sdp)
├── run_sdp_local.sh                    # Local Spark 4.2 Spark Connect runner + --test mode
├── run_sdp_dataproc.sh                 # Standalone Dataproc 4.0 dev launcher
├── requirements.txt                    # pyspark[connect]==4.2.*, pyyaml>=6.0, pytest>=8.0.0, zstandard>=0.25.0
└── README.md                           # Architecture, catalog, refresh, and deployment guide
```

- Keep **only** `.py` dataset declaration modules inside `transformations/` so the `transformations/**` glob in `spark-pipeline.yml` never matches non-pipeline files (`PIPELINE_UNSUPPORTED_DEFINITIONS_FILE_EXTENSION`).
- Pin `pyspark[connect]==4.2.*`, `pyyaml>=6.0`, `pytest>=8.0.0`, and `zstandard>=0.25.0` in `requirements.txt` so `spark-pipelines` and local Spark Connect (`--remote "local[*]"`) install cleanly.
- Implement every column projection, filter, join, watermark, window aggregation, payload parse, and quality predicate as a pure function `(df: DataFrame, ...) -> DataFrame` in `src/transforms.py`.
- Import those pure helpers from `src.transforms` inside `transformations/pipeline_datasets.py` and inside `tests/test_transforms.py`.

## 2. Session Binding & `pyspark.pipelines` Imports

- Import the OSS Spark 4.2 declarative API with `from pyspark import pipelines as dp`.
- Bind the active session at module load with `spark = SparkSession.active()` (or use the `spark` global injected by the SDP CLI). **Never** call `SparkSession.builder.getOrCreate()` or `ManagedSparkSession` inside `transformations/*.py` or `src/transforms.py`.

## 3. OSS Spark 4.2 Dataset & Flow APIs (`branch-4.2`)

Match every node in `## SDP Execution Plan` to its exact planned dataset name and registration API:

1. **Streaming Table (`@dp.table`)**:
   - Signature: `@dp.table(*, name=None, comment=None, spark_conf=None, table_properties=None, partition_cols=None, cluster_by=None, schema=None, format=None)`
   - Returns a **streaming** DataFrame (`spark.readStream...`). Use for file stream ingestion (`readMode: "streaming"`), streaming message brokers, row-level streaming transformations, watermarked streaming aggregations, and streaming Lakehouse catalog tables (`format="iceberg"` or `format="delta"`).
2. **Materialized View (`@dp.materialized_view`)**:
   - Signature: `@dp.materialized_view(*, name=None, comment=None, spark_conf=None, table_properties=None, partition_cols=None, cluster_by=None, schema=None, format=None)`
   - Returns a **batch** DataFrame (`spark.read...`). Use for batch sources, recomputed aggregations (`GroupByAgg`, `RollupCube`, `TopN`), joins over batch or changing dimensions, and gold Lakehouse catalog tables.
   - Never read an upstream `@dp.materialized_view` with `spark.readStream.table(...)` (`INVALID_FLOW_QUERY_TYPE`). Materialized views are truncated and recomputed on every run.
3. **Temporary View (`@dp.temporary_view`)**:
   - Signature: `@dp.temporary_view(*, name=None, comment=None, spark_conf=None)`
   - Declares a non-persisted intermediate DataFrame referenced by downstream datasets within the same pipeline run. Match downstream `spark.read.table(...)` vs. `spark.readStream.table(...)` to the view's query mode (`INCOMPATIBLE_BATCH_VIEW_READ` / `INCOMPATIBLE_STREAMING_VIEW_READ`).
4. **Multi-Flow Streaming Table (`dp.create_streaming_table` + `@dp.append_flow`)**:
   - Declare the target table once: `dp.create_streaming_table(name="<target>", format="iceberg", comment=..., table_properties=..., partition_cols=..., cluster_by=...)`.
   - Register one `@dp.append_flow(target="<target>", name="<flow_name>")` function per incoming streaming branch (for example `UnionByName` or multi-source fan-in).
   - **Never** pass `once=True` or `once=False` to `@dp.append_flow` (`TypeError: append_flow() got an unexpected keyword argument 'once'`).
5. **External Streaming Sink (`dp.create_sink` + `@dp.append_flow`)**:
   - Signature: `dp.create_sink(name="<sink_name>", format="<spark_format>", options={...})`
   - Register an `@dp.append_flow(target="<sink_name>", name="<flow_name>")` returning a streaming DataFrame (`spark.readStream.table(...)` or transformed stream) that writes into `<sink_name>`.
   - Note in generated comments and `README.md` that `pipelines.reset.allowed` cannot protect `dp.create_sink` targets, so `--full-refresh-all` resets sink flow checkpoints and re-sends available source records.
6. **Auto CDC SCD Type 1 (`dp.create_streaming_table` + `dp.create_auto_cdc_flow`)**:
   - Follow `sdp-auto-cdc` when `MergeCdcScd` is present in the graph.

## 4. Upstream Dataset Reads, File Schemas & Table Layout Rules

- **Read upstream datasets by plain dataset name**: Call `spark.readStream.table("<upstream_dataset>")` for streaming flows and `spark.read.table("<upstream_dataset>")` for materialized views. Do not qualify internal pipeline dataset reads with hard-coded catalog or database prefixes unless reading an external pre-existing catalog table.
- **Explicit schema on file streams**: Every `spark.readStream.format("csv" | "json" | "parquet" | "avro" | "orc" | "xml")` source MUST specify an explicit `.schema(STRUCT_SCHEMA)` (`StructType` or DDL string). Include `.option("header", "true")` for CSV headers and `.option("pathGlobFilter", "<pattern>")` when configured.
- **Mutually exclusive layout**: Never pass both `partition_cols` and `cluster_by` on the same `@dp.table`, `@dp.materialized_view`, or `dp.create_streaming_table` (`SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED`).
- **Reset protection propagation**: When a streaming table sets `table_properties={"pipelines.reset.allowed": "false"}`, every upstream pipeline streaming table that it reads via `readStream` must also set `"pipelines.reset.allowed": "false"` (`INVALID_RESETTABLE_DEPENDENCY`).

## 5. Reference Pattern (`src/transforms.py`, `transformations/pipeline_datasets.py`, `tests/test_transforms.py`)

```python
# src/transforms.py
"""Pure DataFrame transformation helpers for Spark Declarative Pipelines.

Author: Mike Nimer <mikenimer@google.com>
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_orders(df: DataFrame) -> DataFrame:
    """Normalizes order timestamps and filters positive order amounts."""
    return (
        df.withColumns(
            {
                "created_at": F.to_timestamp(F.col("created_at")),
                "amount": F.col("amount").cast("double"),
            }
        )
        .filter(F.col("amount") > 0)
    )


def aggregate_daily_revenue(df: DataFrame) -> DataFrame:
    """Computes daily order count and total revenue per store."""
    return (
        df.withColumns({"order_day": F.to_date(F.col("created_at"))})
        .groupBy("store_id", "order_day")
        .agg(
            F.count(F.lit(1)).alias("order_count"),
            F.sum("amount").alias("total_revenue"),
        )
    )
```

```python
# transformations/pipeline_datasets.py
"""Declarative dataset definitions registered with pyspark.pipelines.

Author: Mike Nimer <mikenimer@google.com>
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.transforms import aggregate_daily_revenue, clean_orders

spark = SparkSession.active()

RAW_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("amount", StringType(), True),
    ]
)


@dp.table(name="bronze_orders", format="iceberg", comment="Incremental raw order files.")
def bronze_orders() -> DataFrame:
    return (
        spark.readStream.format("csv")
        .option("header", "true")
        .schema(RAW_ORDERS_SCHEMA)
        .load("gs://my-raw-bucket/orders/")
    )


@dp.table(name="silver_orders", format="iceberg", comment="Validated order stream.")
def silver_orders() -> DataFrame:
    return clean_orders(spark.readStream.table("bronze_orders"))


@dp.materialized_view(name="gold_store_daily", format="iceberg", comment="Daily store revenue.")
def gold_store_daily() -> DataFrame:
    return aggregate_daily_revenue(spark.read.table("silver_orders"))
```

## 6. Strict Bans in SDP Mode (Blocking Defects)

1. **Never mutate or create sessions**: Zero `SparkSession.builder`, `spark.conf.set(...)`, `spark.catalog.setCurrentCatalog(...)`, `spark.catalog.setCurrentDatabase(...)`, `df.createOrReplaceTempView(...)`, or `spark.udf.register(...)` calls (`SESSION_MUTATION_IN_DECLARATIVE_PIPELINE`). Use `@dp.temporary_view` for views, `spark_conf={...}` on decorators or `configuration:` in `spark-pipeline.yml` for dynamic SQL settings, and inline `F.udf` / `@pandas_udf` expressions.
2. **Never execute writers, actions, or eager schema inspection inside dataset functions**: Zero `.write`, `.writeStream`, `.writeTo`, `.save(`, `.saveAsTable(`, `.toTable(`, `.start(`, `.collect()`, `.count()`, `.show()`, `.toPandas()`, `.cache()`, `.persist()`, `.schema`, or `.columns` accesses (`ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION`). When pivoting, always pass explicit values `.pivot(col, [v1, v2, ...])`.
3. **Never emit Databricks-only DLT APIs**: Zero `import dlt`, `from pyspark.pipelines import dlt`, `@dp.view` (use `@dp.temporary_view`), `@dp.expect*` (use `sdp-quality-quarantine` filter helpers), `dp.apply_changes` (use `dp.create_auto_cdc_flow`), `once=` on `@dp.append_flow`, `stored_as_scd_type=2`, or `.format("cloudFiles")`.
