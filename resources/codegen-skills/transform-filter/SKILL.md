---
name: transform-filter
description: Pure PySpark 4.x row filtering using df.filter with Catalyst Column or SQL predicates and null-safe negated-predicate dead-letter queue splits.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [Filter]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html
  validated: true
---

# PySpark 4.x Filter Transformation (`Filter`)

Use these rules when generating PySpark 4.x code for a `Filter` (`OP2`) step node inside `transform(dfs, config)`.

## 1. Pure `transform(dfs, config)` & Pushdown Rules

- Implement `Filter` inside `transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]` with zero external I/O so `tests/test_pipeline.py` runs offline with in-memory DataFrames.
- Place `Filter` predicates as early as possible so Catalyst `PushDownPredicates` pushes row filters into Parquet, ORC, Iceberg, Delta, BigQuery, and JDBC readers.
- Always express predicates using built-in `pyspark.sql.functions` (`F.col(...)`, `F.expr(...)`) or SQL predicate strings. Never use Python `@udf` functions or `.rdd.filter(...)`, which block predicate pushdown and fail on Spark Connect.

## 2. Predicate Construction (`mode: "builder" | "sql"`)

### Mode A: Structured Column Predicate (`mode == "builder"`)

Read `column` (or legacy `property`), `operator`, and `value` from the node properties and build a Catalyst `Column` expression:
- Binary comparisons (`==`, `!=`, `>`, `>=`, `<`, `<=`): compare `F.col(col_name)` against a typed literal (`F.lit(parsed_val)`).
- String pattern operators:
  - `contains`: `F.col(col_name).contains(val)`
  - `startswith`: `F.col(col_name).startswith(val)`
  - `endswith`: `F.col(col_name).endswith(val)`
  - `rlike`: `F.col(col_name).rlike(val)`
- Set membership (`isin`): parse comma-separated items into `F.col(col_name).isin([item.strip() for item in val.split(",") if item.strip()])`.
- Unary null and emptiness checks:
  - `is not null`: `F.col(col_name).isNotNull()`
  - `is null`: `F.col(col_name).isNull()`
  - `is not empty`: `F.col(col_name).isNotNull() & (F.trim(F.col(col_name).cast("string")) != "")`

```python
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def apply_status_filter(df: DataFrame) -> DataFrame:
    """Keeps active orders exceeding the minimum threshold."""
    cond: Column = (F.col("status") == F.lit("ACTIVE")) & (F.col("amount") >= F.lit(100.0))
    return df.filter(cond)
```

### Mode B: SQL Predicate Expression (`mode == "sql"`)

Read `condition` from the node properties and wrap it with `F.expr(condition)` (or pass `condition` directly to `df.filter(condition)` when no DLQ branch is connected):

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_sql_filter(df: DataFrame) -> DataFrame:
    """Applies a Catalyst SQL boolean predicate expression."""
    cond = F.expr("status = 'ACTIVE' AND country IN ('US', 'CA') AND amount >= 100.0")
    return df.filter(cond)
```

## 3. Negated-Predicate Dead-Letter Queue Split (`mechanism: "negated_predicate"`)

When `ExecutionPlan.dlqSplitPoints` contains an entry for this `Filter` node (`error` output handle connected):

1. **Wrap the predicate as a `Column`**: Ensure `cond` is a Catalyst `Column` (use `cond = F.expr(sql_str)` when configured as SQL text).
2. **Cache before splitting (batch mode)**: When the `Filter` node (or its upstream input) is listed in `ExecutionPlan.cachedNodeIds` (batch execution), call `.cache()` once before deriving the two branches, and `.unpersist()` in `write_sinks()` after all consumers in `unpersistAfterNodeIds` finish writing.
3. **Null-safe negated predicate**: Under SQL three-valued logic, `~(cond)` evaluates to `NULL` when a referenced column is `NULL`, which `df.filter(~(cond))` would silently drop from both branches. Route both `False` and `NULL` rows to the `error` DLQ branch using `(~(cond)) | (cond).isNull()` (or `~(cond)` only when `cond` is guaranteed non-nullable such as `.isNotNull()`), and attach an `_error` reason column:

```python
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]:
    """Splits valid and rejected rows for a Filter node with a connected error handle."""
    out: dict[str, DataFrame] = dict(dfs)
    base_df = out["src_orders"].cache()

    cond = (F.col("amount") > F.lit(0)) & F.col("customer_id").isNotNull()
    valid_df = base_df.filter(cond)
    error_df = base_df.filter((~cond) | cond.isNull()).withColumn(
        "_error",
        F.lit("Filter predicate failed: amount > 0 AND customer_id IS NOT NULL"),
    )

    out["step_filter_cached_input"] = base_df
    out["step_filter"] = valid_df
    out["step_filter__valid"] = valid_df
    out["step_filter__error"] = error_df
    return out
```
