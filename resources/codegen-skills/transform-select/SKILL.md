---
name: transform-select
description: Pure PySpark 4.x column projection and SQL expression selection using df.select and df.selectExpr with aliases, wildcard expansion, and regex matching.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [Select]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.selectExpr.html
  validated: true
---

# PySpark 4.x Select Transformation (`Select`)

Use these rules when generating PySpark 4.x code for a `Select` (`OP1`) step node inside `transform(dfs, config)`.

## 1. Pure `transform(dfs, config)` Contract

- Implement `Select` inside `transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]` with zero external I/O and zero `SparkSession` creation so `tests/test_pipeline.py` runs offline with in-memory DataFrames.
- Place `Select` projections as early as possible after wide upstream sources so Catalyst `ColumnPruning` pushes required field lists into Parquet, ORC, Iceberg, Delta, and BigQuery scans.
- Keep all column selection Spark Connect-compatible. Never touch `df.rdd` or Python `@udf` functions for column projection or renaming.

## 2. Selection Modes (`mode: "select" | "selectExpr"`)

Read `mode` (default `"select"`) and the ordered `columns` list (`[{expression, alias}, ...]`) from the node properties:

### Mode A: `mode == "select"` (`df.select(*cols)`)

Build Catalyst `Column` objects with `pyspark.sql.functions as F`:
- **Named or nested struct columns**: `F.col("order_id")`, `F.col("customer.email")`. When `alias` is non-empty, append `.alias(alias)`.
- **Wildcard star expansion**: Pass `"*"` to keep all top-level columns or `"payload.*"` (`F.col("payload.*")`) to expand all fields of a nested struct column. Do not attach `.alias(...)` to wildcard `*` expressions.
- **Regex column selection**: When an expression uses Spark backtick regex syntax (such as `` "`^metric_.*`" ``), select matching columns via `df.colRegex(expr)` inside `df.select(...)`.

```python
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_select_orders(df: DataFrame) -> DataFrame:
    """Projects and aliases selected columns in a single Catalyst Project pass."""
    return df.select(
        F.col("order_id"),
        F.col("customer.id").alias("customer_id"),
        F.col("payload.*"),
        df.colRegex("`^metric_.*`"),
    )
```

### Mode B: `mode == "selectExpr"` (`df.selectExpr(*exprs)`)

Pass Catalyst SQL projection strings directly to `df.selectExpr(*sql_exprs)`:
- When an item specifies both `expression` and a non-empty `alias` (and `expression` is not `"*"` and does not already end with an `AS <alias>` clause), emit `f"{expression} AS {alias}"`.
- Otherwise, pass `expression` directly to `df.selectExpr(...)`.

```python
from pyspark.sql import DataFrame


def apply_select_expr_orders(df: DataFrame) -> DataFrame:
    """Evaluates SQL projection expressions in a single Catalyst selectExpr pass."""
    return df.selectExpr(
        "order_id",
        "UPPER(TRIM(currency)) AS currency_code",
        "CAST(amount_cents / 100.0 AS DECIMAL(12, 2)) AS amount_usd",
        "COALESCE(region, 'GLOBAL') AS region",
    )
```

## 3. Wiring Inside `transform(dfs, config)` & Fan-Out Caching

- Register the output DataFrame under `out[node_id]` in `transform(dfs, config)`.
- If `node_id` appears in `ExecutionPlan.cachedNodeIds` (batch fan-out `outDegree > 1`), call `.cache()` once on the projected DataFrame and unpersist it in `write_sinks()` after downstream consumers complete:

```python
def transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]:
    """Executes topological step transformations without external I/O."""
    out: dict[str, DataFrame] = dict(dfs)
    projected_df = apply_select_orders(out["src_orders"])
    # Apply .cache() only when ExecutionPlan.cachedNodeIds includes 'step_select'
    out["step_select"] = projected_df
    return out
```
