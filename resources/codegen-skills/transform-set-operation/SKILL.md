---
name: transform-set-operation
description: Computes SQL set intersections and differences across two or more upstream DataFrames using functools.reduce.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [SetOperation]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.intersect.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.intersectAll.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.exceptAll.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.subtract.html
  validated: true
---

# Multi-Input DataFrame Set Operations (`SetOperation`)

Use these rules when generating PySpark {{spark_version}} code for a `SetOperation` (`OP14`) step inside `transform(dfs, config)`.

## 1. Ordered Multi-Input Resolution ($N \ge 2$ Inputs)

`SetOperation` has `inputArity: "n"` and requires at least two incoming edges in the pipeline YAML:
- Collect upstream DataFrames in exact incoming edge order: `input_dfs = [dfs[src_id] for src_id in ordered_upstream_ids]`.
- Preserve edge order strictly. Difference operations (`exceptAll` and `subtract`) are non-commutative: `input_dfs[0]` is the base DataFrame from which `input_dfs[1:]` are subtracted left-to-right.

## 2. Schema Alignment by Name (`alignColumnsByName`)

PySpark set methods (`intersect`, `intersectAll`, `exceptAll`, and `subtract`) compare columns **by position** rather than by column name:
- When `properties.alignColumnsByName` is `True` (default), align every subsequent DataFrame to the first DataFrame's column order before reducing:
  `aligned_dfs = [input_dfs[0], *[df.select(*base_cols) for df in input_dfs[1:]]]`
  where `base_cols = input_dfs[0].columns`.
- When `properties.alignColumnsByName` is `False`, pass `input_dfs` directly and rely on positional column compatibility.

## 3. Supported Set Operations (`operation`)

Reduce `aligned_dfs` in a single flat `functools.reduce` call using the configured `operation` (defaulting to `"intersect"`):
- `"intersect"` — `a.intersect(b)` (SQL `INTERSECT DISTINCT`, returning distinct rows present in every upstream DataFrame).
- `"intersectAll"` — `a.intersectAll(b)` (SQL `INTERSECT ALL`, preserving duplicate row multiplicity across inputs).
- `"exceptAll"` — `a.exceptAll(b)` (SQL `EXCEPT ALL`, subtracting matching row multiplicity in `b` from `a` while preserving remaining duplicates).
- `"subtract"` — `a.subtract(b)` (SQL `EXCEPT DISTINCT`, returning distinct rows in `a` that do not appear in `b`).

> **Streaming Note**: Catalyst set operations (`intersect`, `intersectAll`, `exceptAll`, `subtract`) target batch DataFrames. For streaming inclusion or exclusion filters, use a `Join` step with `joinType="left_semi"` or `joinType="left_anti"`.

```python
import functools
from pyspark.sql import DataFrame


def apply_set_operation_step(
    input_dfs: list[DataFrame],
    operation: str = "intersect",
    align_columns_by_name: bool = True,
) -> DataFrame:
    """Applies intersect, intersectAll, exceptAll, or subtract across 2+ DataFrames."""
    if len(input_dfs) < 2:
        raise ValueError("SetOperation requires at least 2 upstream DataFrames.")

    base_cols = list(input_dfs[0].columns)
    aligned_dfs = (
        [input_dfs[0], *[df.select(*base_cols) for df in input_dfs[1:]]]
        if align_columns_by_name
        else input_dfs
    )

    if operation == "intersectAll":
        return functools.reduce(lambda a, b: a.intersectAll(b), aligned_dfs)
    if operation == "exceptAll":
        return functools.reduce(lambda a, b: a.exceptAll(b), aligned_dfs)
    if operation == "subtract":
        return functools.reduce(lambda a, b: a.subtract(b), aligned_dfs)
    return functools.reduce(lambda a, b: a.intersect(b), aligned_dfs)
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Verify `SetOperation` offline in `tests/test_pipeline.py` using in-memory DataFrames with reordered columns to confirm both `alignColumnsByName` alignment and set semantics:

```python
def test_set_operation_except_all_aligns_columns(spark):
    """Verifies exceptAll aligns column names and subtracts matching multiplicity."""
    all_orders = spark.createDataFrame(
        [("o1", "US"), ("o1", "US"), ("o2", "EU")],
        ["order_id", "region"],
    )
    cancelled_orders = spark.createDataFrame(
        [("US", "o1")],
        ["region", "order_id"],
    )
    out_df = apply_set_operation_step(
        [all_orders, cancelled_orders],
        operation="exceptAll",
        align_columns_by_name=True,
    )
    rows = [(r["order_id"], r["region"]) for r in out_df.orderBy("order_id").collect()]
    assert rows == [("o1", "US"), ("o2", "EU")]
```
