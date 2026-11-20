---
name: transform-rename-drop-columns
description: Bulk PySpark 4.x column renaming and pruning using df.withColumnsRenamed and df.drop in a single Catalyst pass.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [RenameDropColumns]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumnsRenamed.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop.html
  validated: true
---

# PySpark 4.x Rename & Drop Columns Transformation (`RenameDropColumns`)

Use these rules when generating PySpark 4.x code for a `RenameDropColumns` (`OP4`) step node inside `transform(dfs, config)`.

## 1. Bulk Catalyst Rename & Drop Rules

- **Single-pass bulk renames (`df.withColumnsRenamed`)**: Pass a Python `dict[str, str]` (`{"old_col": "new_col", ...}`) to `df.withColumnsRenamed(...)` in one call. Never chain `.withColumnRenamed()` inside a Python `for` loop.
- **Single-pass bulk drops (`df.drop`)**: Unpack all dropped column names into one `df.drop(*drop_cols)` call. PySpark `df.drop(*cols)` ignores column names that do not exist in the schema without raising an error.
- **Combined execution order**: When both `renameMappings` and `dropColumns` are configured, chain `df.withColumnsRenamed(rename_map).drop(*drop_cols)` inside pure `transform(dfs, config)`.
- **Spark Connect compatibility**: Keep all operations on native `DataFrame` methods with zero `.rdd` or JVM internal calls.

## 2. Mapping `renameMappings` and `dropColumns`

Read `renameMappings` (`[{from, to}, ...]`) and `dropColumns` (`["col_a", "col_b", ...]`) from the node properties:
- Filter out any incomplete mapping where `from` or `to` is blank before calling `df.withColumnsRenamed(rename_map)`.
- Trim non-empty column names in `dropColumns` before calling `.drop(*drop_cols)`.

```python
from typing import Any
from pyspark.sql import DataFrame


def apply_rename_and_drop_orders(df: DataFrame) -> DataFrame:
    """Renames legacy source columns and drops internal staging fields in one pass."""
    rename_map: dict[str, str] = {
        "cust_id": "customer_id",
        "ord_ts": "order_timestamp",
        "amt_usd": "amount_usd",
    }
    drop_cols: list[str] = ["_ingestion_batch_id", "raw_debug_payload"]

    result_df = df
    if rename_map:
        result_df = result_df.withColumnsRenamed(rename_map)
    if drop_cols:
        result_df = result_df.drop(*drop_cols)
    return result_df
```

## 3. Wiring Inside `transform(dfs, config)`

- Execute `apply_rename_and_drop_*` inside `transform(dfs, config)` with zero external I/O so `tests/test_pipeline.py` can validate output schemas (`assert result_df.columns == [...]`) using in-memory DataFrames.
- Honor `ExecutionPlan.cachedNodeIds` when the `RenameDropColumns` node fans out to multiple batch branches:

```python
def transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]:
    """Applies pure schema rename and column drop transformations."""
    out: dict[str, DataFrame] = dict(dfs)
    cleaned_df = apply_rename_and_drop_orders(out["src_orders"])
    # Apply .cache() when ExecutionPlan.cachedNodeIds includes 'step_rename_drop'
    out["step_rename_drop"] = cleaned_df
    return out
```
