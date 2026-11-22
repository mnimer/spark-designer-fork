---
name: transform-explode
description: PySpark 4.x collection unnesting using F.explode, F.explode_outer, F.posexplode, F.posexplode_outer, F.inline, and F.inline_outer inside df.select.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [Explode]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode_outer.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.posexplode.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.inline.html
  validated: true
---

# PySpark 4.x Explode Transformation (`Explode`)

Use these rules when generating PySpark 4.x code for an `Explode` (`OP6`) step node inside `transform(dfs, config)`.

## 1. Catalyst Generator Projection Rules (`df.select`, Not `.withColumn`)

- **Always project generator functions inside `df.select(...)`**: Multi-column Catalyst generators (`F.posexplode`, `F.posexplode_outer`, `F.inline`, `F.inline_outer`, and `F.explode` on `MapType` columns) emit two or more columns and raise `AnalysisException` if passed to `.withColumn()`. Always build `[F.col(c) for c in kept_cols]` and pass the generator expression as the final argument of `df.select(...)`.
- **One generator per `select` pass**: Spark SQL permits one `Generator` expression per `Project` node. If custom instructions request exploding multiple collection columns, chain separate `.select(...)` calls sequentially.
- **Outer vs. inner null/empty semantics**:
  - `explode`, `posexplode`, and `inline` **drop** parent rows whose collection column is `NULL` or empty (`[]` or `{}`).
  - `explode_outer`, `posexplode_outer`, and `inline_outer` **preserve** parent rows when the collection is `NULL` or empty by emitting `NULL` output columns.

## 2. Generator Patterns by `explodeFunction`

Read `explodeFunction` (default `"explode"`), `sourceColumn`, `outputAlias` (default `"item"`), `positionAlias` (default `"pos"`), and `dropSourceColumn` (default `False`) from the node properties:

1. **Determine retained parent columns**:
   - When `dropSourceColumn` is `True`: `kept_cols = [F.col(c) for c in df.columns if c != source_col]`
   - When `dropSourceColumn` is `False`: `kept_cols = [F.col(c) for c in df.columns]`
2. **Construct the generator expression**:
   - **`explode` / `explode_outer`**:
     - On `ArrayType`: `(F.explode_outer if is_outer else F.explode)(F.col(source_col)).alias(output_alias)`.
     - On `MapType`: `.alias(key_alias, value_alias)` (for example `.alias("map_key", output_alias)`).
   - **`posexplode` / `posexplode_outer`**:
     - On `ArrayType`: `(F.posexplode_outer if is_outer else F.posexplode)(F.col(source_col)).alias(position_alias, output_alias)`.
     - On `MapType`: `.alias(position_alias, "map_key", output_alias)`.
   - **`inline` / `inline_outer` (`ArrayType(StructType)`)**:
     - `(F.inline_outer if is_outer else F.inline)(F.col(source_col))` — expands each struct element directly into top-level columns using the struct's field names (no single-column `.alias(...)` call).

```python
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_posexplode_line_items(df: DataFrame) -> DataFrame:
    """Unnests order line items with 0-based position while preserving empty orders."""
    kept_cols = [F.col(c) for c in df.columns if c != "line_items"]
    return df.select(
        *kept_cols,
        F.posexplode_outer(F.col("line_items")).alias("item_index", "line_item"),
    )


def apply_inline_events(df: DataFrame) -> DataFrame:
    """Flattens an array of structs directly into struct subfield columns."""
    kept_cols = [F.col(c) for c in df.columns if c != "events"]
    return df.select(
        *kept_cols,
        F.inline_outer(F.col("events")),
    )
```

## 3. Wiring Inside `transform(dfs, config)`

- Execute collection unnesting inside `transform(dfs, config)` with zero external I/O so `tests/test_pipeline.py` can verify empty/null collection behavior offline.
- Honor `ExecutionPlan.cachedNodeIds` when the exploded DataFrame fans out to multiple batch consumers:

```python
def transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]:
    """Applies pure collection unnesting transformations."""
    out: dict[str, DataFrame] = dict(dfs)
    exploded_df = apply_posexplode_line_items(out["src_orders"])
    # Apply .cache() when ExecutionPlan.cachedNodeIds includes 'step_explode'
    out["step_explode"] = exploded_df
    return out
```
