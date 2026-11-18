---
name: transform-order-by
description: Sort DataFrame rows globally with orderBy or locally per partition with sortWithinPartitions using explicit null ordering in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [OrderBy]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sortWithinPartitions.html
  validated: true
---

# Sort & Order Rows (`OrderBy`)

Use these rules when generating PySpark {{spark_version}} code for an `OrderBy` step inside `transform(dfs, config)`.

## 1. Global `df.orderBy` vs Local `df.sortWithinPartitions`

Read `sortColumns` (`list[dict]`) and `withinPartitionsOnly` (`bool`, default `False`) from node properties:

- **Global Total Order (`withinPartitionsOnly == False`)**:
  - Call `df.orderBy(*sort_exprs)` to produce a globally sorted DataFrame across all Spark partitions.
  - Note that `df.orderBy` triggers a full `Exchange rangepartitioning(...)` shuffle across the cluster. Avoid placing unnecessary global `orderBy` steps before unordered downstream joins or aggregations.
- **Partition-Local Order (`withinPartitionsOnly == True`)**:
  - Call `df.sortWithinPartitions(*sort_exprs)` to sort records inside each existing partition without triggering a global range-partitioning shuffle.
  - Prefer `sortWithinPartitions` before columnar file or table sinks (`ParquetSink`, `OrcSink`, `IcebergSink`, `DeltaSink`) to improve dictionary encoding, Z-order locality, and min/max row-group pruning.

## 2. Direction & Explicit Null Ordering Expressions

Map each `{column, direction, nullsOrder}` entry in `sortColumns` to a `pyspark.sql.Column` sort expression:

- `direction == "asc"` and `nullsOrder == "default"` -> `F.col(column).asc()`
- `direction == "asc"` and `nullsOrder == "nulls_first"` -> `F.col(column).asc_nulls_first()`
- `direction == "asc"` and `nullsOrder == "nulls_last"` -> `F.col(column).asc_nulls_last()`
- `direction == "desc"` and `nullsOrder == "default"` -> `F.col(column).desc()`
- `direction == "desc"` and `nullsOrder == "nulls_first"` -> `F.col(column).desc_nulls_first()`
- `direction == "desc"` and `nullsOrder == "nulls_last"` -> `F.col(column).desc_nulls_last()`

Preserve the exact sequence of `sortColumns` because Spark evaluates tie-breakers left-to-right.

## 3. Structured Streaming Restrictions

- Spark Structured Streaming rejects global `df.orderBy(...)` and `df.sort(...)` on unaggregated streaming DataFrames with an `AnalysisException`.
- On a streaming branch (`ExecutionPlan.nodes[nodeId].executionMode == "streaming"`), global `df.orderBy(*sort_exprs)` is valid **only** after an upstream streaming aggregation (`GroupByAgg`, `RollupCube`, or `PivotUnpivot`) when the downstream streaming sink runs with `outputMode == "complete"`.
- If a streaming pipeline requires per-micro-batch sorting without `complete` aggregation mode, apply `batch_df.orderBy(*sort_exprs)` or `batch_df.sortWithinPartitions(*sort_exprs)` inside the sink's `foreachBatch` callback rather than on the unbounded streaming DataFrame.

## 4. Reference Implementation

```python
from typing import Any
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def _build_sort_expr(spec: dict[str, Any]) -> Column:
    """Builds a PySpark Column sort expression with explicit direction and null placement."""
    col_expr = F.col(str(spec["column"]).strip())
    direction = str(spec.get("direction", "asc")).lower()
    nulls_order = str(spec.get("nullsOrder", "default")).lower()

    if direction == "desc":
        if nulls_order == "nulls_first":
            return col_expr.desc_nulls_first()
        if nulls_order == "nulls_last":
            return col_expr.desc_nulls_last()
        return col_expr.desc()

    if nulls_order == "nulls_first":
        return col_expr.asc_nulls_first()
    if nulls_order == "nulls_last":
        return col_expr.asc_nulls_last()
    return col_expr.asc()


def apply_order_by(df: DataFrame, step_cfg: dict[str, Any]) -> DataFrame:
    """Applies global orderBy or partition-local sortWithinPartitions to a DataFrame."""
    raw_specs = [
        item
        for item in step_cfg.get("sortColumns", [])
        if str(item.get("column", "")).strip()
    ]
    if not raw_specs:
        return df

    sort_exprs = [_build_sort_expr(spec) for spec in raw_specs]
    if bool(step_cfg.get("withinPartitionsOnly", False)):
        return df.sortWithinPartitions(*sort_exprs)
    return df.orderBy(*sort_exprs)
```

## 5. Offline Unit Testing

Verify multi-column sort precedence, explicit null placement, and `sortWithinPartitions` in `tests/test_pipeline.py` with in-memory DataFrames:

```python
from pyspark.sql import SparkSession


def test_order_by_direction_and_nulls_placement(spark: SparkSession) -> None:
    """Verifies multi-column ordering and explicit nulls_last placement."""
    input_df = spark.createDataFrame(
        [
            ("us", None),
            ("us", 50),
            ("eu", 90),
            ("us", 120),
        ],
        ["region", "score"],
    )

    sorted_df = apply_order_by(
        input_df,
        {
            "withinPartitionsOnly": False,
            "sortColumns": [
                {"column": "region", "direction": "asc", "nullsOrder": "default"},
                {"column": "score", "direction": "desc", "nullsOrder": "nulls_last"},
            ],
        },
    )
    actual = [(r["region"], r["score"]) for r in sorted_df.collect()]
    assert actual == [("eu", 90), ("us", 120), ("us", 50), ("us", None)]
```
