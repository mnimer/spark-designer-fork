---
name: transform-rollup-cube
description: PySpark 4.x hierarchical rollup and multidimensional cube aggregation rules with grouping and grouping_id null disambiguation.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [RollupCube]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.rollup.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cube.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.grouping_id.html
  validated: true
---

# PySpark 4.x Hierarchical Rollup & Multidimensional Cube (`RollupCube`)

Implement `RollupCube` steps inside `transform(dfs, config)` using native Catalyst `df.rollup(*cols).agg(*exprs)` or `df.cube(*cols).agg(*exprs)`.

## 1. Choosing Between `rollup` and `cube`

- **Hierarchical Subtotals (`mode == "rollup"`)**:
  - Call `df.rollup(*dimension_cols).agg(*agg_exprs)`.
  - Preserves left-to-right column hierarchy across $N$ dimensions and emits $N + 1$ grouping levels. For `["year", "quarter", "region"]`, Spark computes `(year, quarter, region)`, `(year, quarter)`, `(year)`, and the grand total `()`.
- **Cross-Tabular Combinations (`mode == "cube"`)**:
  - Call `df.cube(*dimension_cols).agg(*agg_exprs)`.
  - Computes subtotals across every subset of the $N$ dimension columns ($2^N$ grouping sets). Keep dimension lists concise because high-cardinality cubes expand shuffle volume exponentially.

## 2. Disambiguating Subtotal `NULL` Rows from Real `NULL` Keys (`F.grouping` & `F.grouping_id`)

Spark populates aggregated dimension columns with `NULL` in subtotal and grand-total rows. Never rely on `F.col(dim).isNull()` alone to identify subtotals when source data may contain genuine `NULL` dimension values:

- **`F.grouping(col)`**: Returns `1` when `col` is collapsed into a subtotal or grand-total row, and `0` when `col` represents an actual grouping key (even if that key's data value is `NULL`).
- **`F.grouping_id(*dimension_cols)`**: Computes an integer bitmask across all dimension columns in the exact order passed to `.rollup(*dimension_cols)` or `.cube(*dimension_cols)`.
  - `grouping_id == 0`: Base detail row where every dimension column is active.
  - `grouping_id == (1 << len(dimension_cols)) - 1`: Grand-total row where every dimension column is collapsed.
- When `properties.includeGroupingId` is `True` (the default), append `F.grouping_id(*dimension_cols).alias(grouping_id_alias)` directly inside `.agg(...)`.

## 3. Aggregate Expression Construction

Map each entry in `properties.aggregations` (`{column, fn, alias}`) to `pyspark.sql.functions`:
- `count`: `F.count(F.lit(1)).alias(alias)` when `column == "*"` else `F.count(F.col(column)).alias(alias)`
- `countDistinct`: `F.count_distinct(F.col(column)).alias(alias)`
- `sum`, `avg`, `min`, `max`: `getattr(F, fn)(F.col(column)).alias(alias)`
- `first`, `last`: `getattr(F, fn)(F.col(column), ignorenulls=True).alias(alias)`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_rollup_cube(df: DataFrame) -> DataFrame:
    """Computes hierarchical revenue subtotals with grouping_id null disambiguation."""
    dims = ["region", "country", "channel"]
    return df.rollup(*dims).agg(
        F.sum(F.col("revenue")).alias("total_revenue"),
        F.count(F.lit(1)).alias("order_count"),
        F.grouping_id(*dims).alias("grouping_id"),
    )
```

If downstream logic labels grand totals or subtotals explicitly, combine `F.grouping(col)` with `F.when(...)` in a single `df.withColumns({...})` pass after `.agg(...)`:

```python
labeled_df = agg_df.withColumns(
    {
        "region_label": F.when(F.col("grouping_id") == 7, F.lit("ALL_REGIONS")).otherwise(
            F.coalesce(F.col("region"), F.lit("UNKNOWN_REGION"))
        )
    }
)
```

## 4. Streaming Compatibility Note

Structured Streaming does not support `.rollup()` or `.cube()` directly on unbounded streaming DataFrames. In streaming pipelines, execute `RollupCube` inside a `foreachBatch` micro-batch callback on the static `batch_df` DataFrame.
