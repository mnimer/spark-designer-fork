---
name: transform-pivot-unpivot
description: PySpark 4.x DataFrame pivot and unpivot reshaping patterns with explicit pivot domain lists and native unpivot column semantics.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [PivotUnpivot]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.pivot.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unpivot.html
  validated: true
---

# PySpark 4.x Pivot & Unpivot Reshaping (`PivotUnpivot`)

Implement `PivotUnpivot` steps inside `transform(dfs, config)` as pure DataFrame reshaping operations with zero driver-side collection.

## 1. Wide Reshaping with `GroupedData.pivot` (`operation == "pivot"`)

Rotate distinct values from one column into multiple output columns using `df.groupBy(*group_by_cols).pivot(pivot_col, values).agg(agg_expr)`.

### Critical Catalyst Performance Rule: Pass Explicit `values=[...]`
- **Always pass explicit `values=[...]`** to `.pivot(pivot_col, values)` whenever `properties.pivotValues` is non-empty.
- When `values` is omitted, Spark Catalyst triggers an immediate eager job scan (`SELECT DISTINCT pivot_col`) on the driver to discover column names before building the logical plan. Providing explicit values eliminates that extra full-dataset scan and makes output schemas deterministic for downstream steps and unit tests.
- Omit `values` only when `properties.pivotValues` is empty and dynamic column discovery is explicitly required on a batch DataFrame.

### Aggregation Function Mapping
Map `properties.aggFunction` (`sum`, `avg`, `count`, `min`, `max`, `first`) and `properties.aggColumn` to native `pyspark.sql.functions`:
- `sum`: `F.sum(F.col(agg_col))`
- `avg`: `F.avg(F.col(agg_col))`
- `count`: `F.count(F.lit(1))` when `agg_col == "*"` else `F.count(F.col(agg_col))`
- `min`: `F.min(F.col(agg_col))`
- `max`: `F.max(F.col(agg_col))`
- `first`: `F.first(F.col(agg_col), ignorenulls=True)`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_pivot(df: DataFrame) -> DataFrame:
    """Pivots quarterly revenue into wide columns without an extra distinct-values scan."""
    grouped = df.groupBy("region", "product_line")
    pivoted = grouped.pivot("quarter", values=["Q1", "Q2", "Q3", "Q4"])
    return pivoted.agg(F.sum(F.col("revenue")))
```

### Streaming Compatibility Note
- Structured Streaming does not support `.pivot()` directly on unbounded streaming DataFrames. If a pipeline requires pivoting micro-batch output, execute the pivot inside a `foreachBatch` callback on the static `batch_df` DataFrame.

## 2. Tall Reshaping with Spark 4.x `DataFrame.unpivot` (`operation == "unpivot"`)

Convert wide measure columns into tall key-value rows using the native Spark 4.x `df.unpivot(...)` API rather than legacy `F.expr("stack(...)")` SQL strings.

### Signature & Column Rules
- Call `df.unpivot(ids=id_cols, values=value_cols_or_none, variableColumnName=variable_col, valueColumnName=value_col)`.
- **`ids`** (`properties.idColumns`): Identifier columns preserved on every emitted row. Pass `[]` if no identifier columns exist.
- **`values`** (`properties.valueColumns`): Columns rotated into rows. When `properties.valueColumns` is non-empty, pass the list of column names. When `properties.valueColumns` is empty, pass `values=None` so Spark unpivots all columns not listed in `ids`.
- **`variableColumnName`** (`properties.variableColumnName`, default `"variable"`): Output column holding each original value column name.
- **`valueColumnName`** (`properties.valueColumnName`, default `"value"`): Output column holding each cell value.
- **Type Compatibility**: `df.unpivot` requires all unpivoted `values` columns to share a compatible data type (for example all `DoubleType` or all `StringType`). If input columns have heterogeneous types, cast them in a single `df.withColumns({...})` projection before calling `df.unpivot(...)`.

```python
from pyspark.sql import DataFrame


def apply_unpivot(df: DataFrame) -> DataFrame:
    """Unpivots quarterly metric columns into tall quarter and revenue rows."""
    return df.unpivot(
        ids=["region", "product_line"],
        values=["Q1", "Q2", "Q3", "Q4"],
        variableColumnName="quarter",
        valueColumnName="revenue",
    )
```
