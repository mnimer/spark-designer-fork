---
name: io-create-dataframe
description: Batch synthetic and in-memory DataFrame source supporting inline JSON rows, spark.range sequences, and deterministic synthetic dataset generation.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [CreateDataFrameSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html
  validated: true
---

# Synthetic & In-Memory DataFrame Source (`CreateDataFrameSource`)

Use these rules when generating PySpark 4.x code for a `CreateDataFrameSource` (or legacy `Create`) node (`SRC38`).

## 1. Runtime & Connector Classpath Rules

- **Built-in Spark Session APIs**: `spark.createDataFrame(...)` and `spark.range(...)` are core PySpark APIs. Never add external Maven JARs for `CreateDataFrameSource`.
- **Batch-Only Execution Mode**: `CreateDataFrameSource` is a batch source (`readMode == "batch"`). Use `RateSource` (`io-rate`) when an unbounded synthetic streaming source is required.

## 2. Three Supported Generation Modes (`generationMode`)

Inspect `generationMode` (default `"rows"`) on the `CreateDataFrameSource` node and implement the matching generation strategy in `read_sources(spark, config)`:

### Mode A: Inline JSON / Dict Rows (`generationMode == "rows"`)
- Parse inline JSON records (`rows` or `elements`) or define the configured list of Python dictionaries/tuples directly in `src/pipeline.py`.
- Always pass an explicit `StructType` schema (`spark.createDataFrame(rows, schema=INLINE_SCHEMA)`) so column types and nullability are deterministic even when `rows` is empty or contains `None` values.

### Mode B: Numeric Sequence (`generationMode == "range"`)
- Generate a distributed sequence of `LongType` `id` rows using `spark.range(start, end, step, numPartitions)`:
  - `rangeStart` (or `start`, default `0`)
  - `rangeEnd` (or `end`, default `1000`)
  - `rangeStep` (or `step`, default `1`)
  - `numPartitions` (optional integer partition count)

### Mode C: Synthetic Dataset Generation (`generationMode == "faker"`)
- Generate `rowCount` (default `100`) deterministic synthetic rows matching the configured `StructType` schema and `fakerSpec` (or field rules).
- Seed Python's `random.Random(seed)` (or `Faker` when listed in `requirements.txt`) with a fixed seed (default `42`) so runs are reproducible across Local, Dataproc Cluster, and Serverless Spark environments.
- Construct the resulting DataFrame with `spark.createDataFrame(synthetic_rows, schema=SYNTHETIC_SCHEMA)`.

```python
import json
import random
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

SYNTHETIC_USERS_SCHEMA = StructType(
    [
        StructField("user_id", LongType(), False),
        StructField("tier", StringType(), False),
        StructField("spend", DoubleType(), False),
    ]
)


def build_synthetic_rows(row_count: int, seed: int = 42) -> list[tuple[int, str, float]]:
    """Generates deterministic synthetic rows matching SYNTHETIC_USERS_SCHEMA."""
    rng = random.Random(seed)
    tiers = ["free", "pro", "enterprise"]
    return [
        (idx + 1, rng.choice(tiers), round(rng.uniform(10.0, 500.0), 2))
        for idx in range(row_count)
    ]


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Creates in-memory or synthetic batch DataFrames keyed by node ID."""
    dfs: dict[str, DataFrame] = {}
    mode = config.get("src_create_generation_mode", "rows")

    if mode == "range":
        start = int(config.get("src_create_range_start", 0))
        end = int(config.get("src_create_range_end", 1000))
        step = int(config.get("src_create_range_step", 1))
        num_partitions = int(config.get("src_create_num_partitions", 4))
        dfs["src_create"] = spark.range(start, end, step, numPartitions=num_partitions)
    elif mode == "faker":
        row_count = int(config.get("src_create_row_count", 100))
        seed = int(config.get("src_create_seed", 42))
        rows = build_synthetic_rows(row_count=row_count, seed=seed)
        dfs["src_create"] = spark.createDataFrame(rows, schema=SYNTHETIC_USERS_SCHEMA)
    else:
        raw_rows = config.get("src_create_rows_json")
        if raw_rows:
            parsed_rows = json.loads(raw_rows)
        else:
            parsed_rows = [
                {"user_id": 1, "tier": "pro", "spend": 120.50},
                {"user_id": 2, "tier": "enterprise", "spend": 450.00},
            ]
        dfs["src_create"] = spark.createDataFrame(parsed_rows, schema=SYNTHETIC_USERS_SCHEMA)

    return dfs
```

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

Because `CreateDataFrameSource` produces pure in-memory DataFrames without external I/O, unit tests in `tests/test_pipeline.py` can test both `read_sources(spark, config)` and `transform(dfs, config)` directly.

```python
from src.pipeline import SYNTHETIC_USERS_SCHEMA, read_sources, transform


def test_create_dataframe_source_and_transform(spark):
    """Verifies CreateDataFrameSource across inline rows, range, and synthetic modes."""
    source_dfs = read_sources(spark, {"src_create_generation_mode": "rows"})
    result_dfs = transform(source_dfs, {})

    assert "src_create" in result_dfs
    assert result_dfs["src_create"].schema == SYNTHETIC_USERS_SCHEMA
    assert result_dfs["src_create"].count() == 2
```
