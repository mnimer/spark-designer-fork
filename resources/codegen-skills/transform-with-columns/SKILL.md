---
name: transform-with-columns
description: Single-pass PySpark 4.x derived and cast column projection using df.withColumns without iterative withColumn loops.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [WithColumns]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumns.html
  validated: true
---

# PySpark 4.x WithColumns Transformation (`WithColumns`)

Use these rules when generating PySpark 4.x code for a `WithColumns` (`OP3`) step node inside `transform(dfs, config)`.

## 1. Single-Pass Catalyst Projection Rule (`df.withColumns`)

- **Always call `df.withColumns({...})` in a single pass**: Pass a Python `dict[str, Column]` mapping all target column names to Catalyst `Column` expressions (`F.expr(...)` or `F.*` functions).
- **Strict ban on `.withColumn()` loops**: Never call `df = df.withColumn(name, expr)` inside a Python `for` loop. Iterative `.withColumn()` calls create deeply nested Catalyst `Project` plan nodes that degrade query analysis and optimizer performance.
- **Zero row-level Python `@udf`s for standard expressions**: Express string formatting, math, null coalescing, conditional `CASE WHEN` (`F.when(...).otherwise(...)`), JSON extraction, and date/timestamp math with built-in Catalyst functions or `F.expr(...)` so execution stays inside Tungsten on standard Spark and Spark Connect.

## 2. Mapping `columns` Specifications & Optional `castType`

Read `columns` (`[{name, expression, castType}, ...]`) from the node properties:
- Build each column expression using `F.expr(spec["expression"])` or native `pyspark.sql.functions` (`F.*`) equivalents.
- When `castType` is non-empty (for example `"STRING"`, `"BIGINT"`, `"DOUBLE"`, `"DECIMAL(18, 2)"`, `"BOOLEAN"`, `"DATE"`, or `"TIMESTAMP"`), chain `.cast(cast_type)` on the expression inside the `df.withColumns({...})` dictionary.
- Because `df.withColumns({...})` resolves every dictionary value against the input DataFrame schema simultaneously, if one derived column depends on another column defined in the same step, inline the upstream expression or emit a second `df.withColumns({...})` stage only for the dependent tier rather than looping row-by-row or column-by-column.

```python
from typing import Any
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def apply_with_columns_enrichment(df: DataFrame) -> DataFrame:
    """Computes derived and cast columns in one Catalyst Project pass."""
    projections: dict[str, Column] = {
        "net_amount": (F.col("gross_amount") - F.coalesce(F.col("discount_amount"), F.lit(0.0))).cast(
            "DECIMAL(18, 2)"
        ),
        "currency_upper": F.upper(F.trim(F.col("currency"))).cast("STRING"),
        "event_date": F.to_date(F.col("event_timestamp")),
        "is_high_value": (F.col("gross_amount") >= F.lit(1000.0)).cast("BOOLEAN"),
    }
    return df.withColumns(projections)
```

## 3. Dynamic Configuration Helper & `transform(dfs, config)` Wiring

When building projections from a list of `{name, expression, castType}` dictionaries in `config`, construct the full `cols_map` dictionary first and invoke `df.withColumns(cols_map)` once:

```python
def apply_configured_with_columns(
    df: DataFrame,
    column_specs: list[dict[str, str]],
) -> DataFrame:
    """Applies configured SQL expressions and optional casts via a single df.withColumns call."""
    cols_map: dict[str, Column] = {}
    for spec in column_specs:
        col_name = spec["name"].strip()
        col_expr: Column = F.expr(spec["expression"])
        cast_type = spec.get("castType", "").strip()
        if cast_type:
            col_expr = col_expr.cast(cast_type)
        cols_map[col_name] = col_expr
    return df.withColumns(cols_map) if cols_map else df


def transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]:
    """Executes pure step transformations in topological order."""
    out: dict[str, DataFrame] = dict(dfs)
    enriched_df = apply_with_columns_enrichment(out["src_orders"])
    # Apply .cache() when ExecutionPlan.cachedNodeIds includes 'step_with_columns'
    out["step_with_columns"] = enriched_df
    return out
```
