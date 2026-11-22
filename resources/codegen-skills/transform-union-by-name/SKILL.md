---
name: transform-union-by-name
description: Combines two or more upstream DataFrames by column name or position using flat functools.reduce reduction.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [UnionByName]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unionByName.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.union.html
  validated: true
---

# Multi-Input DataFrame Union (`UnionByName`)

Use these rules when generating PySpark {{spark_version}} code for a `UnionByName` (`OP13`) step inside `transform(dfs, config)`.

## 1. Ordered Multi-Input Resolution ($N \ge 2$ Inputs)

`UnionByName` has `inputArity: "n"` and requires at least two incoming edges in the pipeline YAML:
- Collect upstream DataFrames in exact incoming edge order: `input_dfs = [dfs[src_id] for src_id in ordered_upstream_ids]`.
- Raise a clear `ValueError` if fewer than two upstream DataFrames are provided.

## 2. Flat `functools.reduce` Union Modes (`unionMode`)

Spark `unionByName` and `union` follow SQL `UNION ALL` semantics (preserving duplicate rows without a shuffle):

- **By Column Name (`unionMode == "by_name"`, default)**:
  - Combine all upstream DataFrames in a single flat pass using `functools.reduce`:
    `functools.reduce(lambda a, b: a.unionByName(b, allowMissingColumns=allow_missing), input_dfs)`
  - Default `allowMissingColumns` to `True` unless `properties.allowMissingColumns` is explicitly `False`. When `allowMissingColumns=True`, Spark aligns columns by name across schemas with different column orders and fills missing columns with `NULL`.
- **By Column Position (`unionMode == "by_position"`)**:
  - Combine upstream DataFrames positionally using `functools.reduce(lambda a, b: a.union(b), input_dfs)`.
  - Positional union requires every upstream DataFrame to have the same number of columns in compatible positional order, adopting column names from `input_dfs[0]`.

## 3. Optional Row Deduplication (`deduplicate`)

- When `properties.deduplicate` is `False` (default), return the unioned DataFrame directly without an extra shuffle.
- When `properties.deduplicate` is `True`:
  - For batch DataFrames (`not unioned_df.isStreaming`), call `unioned_df = unioned_df.distinct()`.
  - For streaming DataFrames (`unioned_df.isStreaming`), call `unioned_df = unioned_df.dropDuplicates()` (or `dropDuplicatesWithinWatermark()` when an upstream watermark is active).

```python
import functools
from pyspark.sql import DataFrame


def apply_union_by_name_step(
    input_dfs: list[DataFrame],
    union_mode: str = "by_name",
    allow_missing_columns: bool = True,
    deduplicate: bool = False,
) -> DataFrame:
    """Unions two or more ordered DataFrames by column name or column position."""
    if len(input_dfs) < 2:
        raise ValueError("UnionByName requires at least 2 upstream DataFrames.")

    if union_mode == "by_position":
        unioned_df = functools.reduce(lambda a, b: a.union(b), input_dfs)
    else:
        unioned_df = functools.reduce(
            lambda a, b: a.unionByName(b, allowMissingColumns=allow_missing_columns),
            input_dfs,
        )

    if deduplicate:
        if unioned_df.isStreaming:
            return unioned_df.dropDuplicates()
        return unioned_df.distinct()

    return unioned_df
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `UnionByName` offline in `tests/test_pipeline.py` with two or three in-memory DataFrames that have reordered or missing columns, asserting null-filled schema alignment and row counts:

```python
def test_union_by_name_three_branches_with_missing_columns(spark):
    """Verifies 3-way unionByName aligns columns and fills missing fields with None."""
    web_df = spark.createDataFrame([("e1", "web", "chrome")], ["id", "channel", "browser"])
    mobile_df = spark.createDataFrame([("mobile", "e2", "ios")], ["channel", "id", "os"])
    pos_df = spark.createDataFrame([("e3", "pos")], ["id", "channel"])

    result_dfs = transform(
        {"src_web": web_df, "src_mobile": mobile_df, "src_pos": pos_df},
        {},
    )
    out_df = result_dfs["step_union"]
    assert out_df.count() == 3
    assert set(out_df.columns) == {"id", "channel", "browser", "os"}
```
