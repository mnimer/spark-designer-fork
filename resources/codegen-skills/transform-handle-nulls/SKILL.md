---
name: transform-handle-nulls
description: Fill, drop, or replace null, NaN, and sentinel values across DataFrame columns using PySpark 4.x DataFrameNaFunctions.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [HandleNulls]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html
  validated: true
---

# Handle Nulls & Sentinel Values (`HandleNulls`)

Use these rules when generating PySpark {{spark_version}} code for a `HandleNulls` step inside `transform(dfs, config)`.

## 1. Execution Model & Streaming Compatibility

- `df.na.fill(...)`, `df.na.drop(...)`, and `df.na.replace(...)` compile into stateless Catalyst row projections and filters.
- All three strategies run identically on batch DataFrames and Structured Streaming DataFrames without watermarks or state stores.
- Keep all null-handling logic inside `transform(dfs, config)` with zero external I/O so unit tests run offline with in-memory DataFrames.

## 2. Null-Handling Strategies (`strategy`)

Read `strategy` (`"fill"`, `"drop"`, or `"replace"`) and optional `subsetColumns` (`list[str]`) from node properties:

### A. Fill Nulls & NaNs (`strategy == "fill"`)
- **Per-column mapping (`fillMappings`)**: Build a dictionary `{col_name: typed_value}` from `fillMappings` (`[{column, value}]`) and call `df = df.na.fill(per_col_map)`. Coerce each string representation to match the target schema field type (`int`, `float`, `bool`, or `str`) or cast explicitly because PySpark `df.na.fill` ignores entries whose Python scalar type does not match the DataFrame column type.
- **Global fallback (`defaultFillValue`)**: If `defaultFillValue` is configured, apply `df = df.na.fill(default_val, subset=subset_cols)` after or alongside per-column mappings, scoping `subset` to columns not already overridden by `fillMappings` (or to `subsetColumns` when specified).

### B. Drop Rows with Nulls (`strategy == "drop"`)
- Read `dropHow` (`"any"` or `"all"`, default `"any"`) and optional positive integer `dropThresh`.
- Resolve `subset_arg = subset_cols if subset_cols else None`.
- In PySpark `DataFrameNaFunctions.drop`, passing a non-null `thresh` overrides `how`. Call `df.na.drop(thresh=int(drop_thresh), subset=subset_arg)` only when `dropThresh` is configured as a positive integer. Otherwise call `df.na.drop(how=drop_how, subset=subset_arg)`.

### C. Replace Sentinel Values (`strategy == "replace"`)
- Build replacement pairs from `replaceMappings` (`[{fromValue, toValue}]`).
- PySpark `df.na.replace(to_replace, value=None, subset=...)` requires dictionary keys and values in a single call to share compatible types (`str`, `int`, `float`, or `bool`). Group mappings by scalar type (for example string sentinels `{"N/A": None, "": "UNKNOWN"}` vs numeric sentinels `{-1: 0}`) or coerce according to target column types in `df.schema`.
- When replacing a sentinel with SQL `NULL`, pass `None` as the mapped value in the replacement dictionary or use `F.when(F.col(c) == sentinel, F.lit(None)).otherwise(F.col(c))` inside `df.withColumns({...})`.

## 3. Reference Implementation

```python
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import types as T


def _coerce_scalar_for_dtype(raw_value: str, data_type: T.DataType | None) -> Any:
    """Coerces a UI string value to match a Spark atomic column type."""
    if isinstance(data_type, T.IntegralType):
        return int(raw_value)
    if isinstance(data_type, T.FractionalType):
        return float(raw_value)
    if isinstance(data_type, T.BooleanType):
        return raw_value.strip().lower() in ("true", "1", "yes")
    return raw_value


def apply_handle_nulls(df: DataFrame, step_cfg: dict[str, Any]) -> DataFrame:
    """Applies fill, drop, or replace null handling using PySpark DataFrameNaFunctions."""
    strategy = step_cfg.get("strategy", "fill")
    subset_cols = [c for c in step_cfg.get("subsetColumns", []) if c]
    subset_arg = subset_cols if subset_cols else None
    fields_by_name = {field.name: field.dataType for field in df.schema.fields}

    if strategy == "drop":
        raw_thresh = step_cfg.get("dropThresh")
        if raw_thresh is not None and str(raw_thresh).strip() != "":
            return df.na.drop(thresh=int(raw_thresh), subset=subset_arg)
        drop_how = step_cfg.get("dropHow", "any")
        return df.na.drop(how=drop_how, subset=subset_arg)

    if strategy == "replace":
        str_replacements: dict[str, str | None] = {}
        for item in step_cfg.get("replaceMappings", []):
            from_val = item.get("fromValue", "")
            if from_val == "":
                continue
            to_val = item.get("toValue", "")
            str_replacements[str(from_val)] = None if to_val == "NULL" else str(to_val)
        if str_replacements:
            return df.na.replace(str_replacements, subset=subset_arg)
        return df

    result_df = df
    per_col_fill: dict[str, Any] = {}
    for item in step_cfg.get("fillMappings", []):
        col_name = str(item.get("column", "")).strip()
        if col_name and col_name in fields_by_name and "value" in item:
            per_col_fill[col_name] = _coerce_scalar_for_dtype(
                str(item["value"]),
                fields_by_name.get(col_name),
            )
    if per_col_fill:
        result_df = result_df.na.fill(per_col_fill)

    default_fill = step_cfg.get("defaultFillValue")
    if default_fill is not None and str(default_fill) != "":
        target_cols = subset_cols if subset_cols else list(fields_by_name.keys())
        remaining_cols = [c for c in target_cols if c not in per_col_fill and c in fields_by_name]
        fallback_map = {}
        for col_name in remaining_cols:
            try:
                fallback_map[col_name] = _coerce_scalar_for_dtype(
                    str(default_fill),
                    fields_by_name[col_name],
                )
            except (TypeError, ValueError):
                continue
        if fallback_map:
            result_df = result_df.na.fill(fallback_map)

    return result_df
```

## 4. Offline Unit Testing

Verify `HandleNulls` inside `transform(dfs, config)` using in-memory DataFrames containing `None`, `float("nan")`, and sentinel values:

```python
from pyspark.sql import SparkSession


def test_handle_nulls_fill_drop_and_replace(spark: SparkSession) -> None:
    """Verifies fill, drop, and sentinel replacement behavior offline."""
    raw_df = spark.createDataFrame(
        [
            ("u1", None, "N/A"),
            ("u2", 25.0, "US"),
            (None, None, None),
        ],
        ["user_id", "amount", "country"],
    )

    dropped_df = apply_handle_nulls(
        raw_df,
        {"strategy": "drop", "dropHow": "all", "subsetColumns": ["user_id", "amount", "country"]},
    )
    assert dropped_df.count() == 2

    filled_df = apply_handle_nulls(
        dropped_df,
        {
            "strategy": "fill",
            "fillMappings": [{"column": "amount", "value": "0.0"}],
            "defaultFillValue": "UNKNOWN",
        },
    )
    replaced_df = apply_handle_nulls(
        filled_df,
        {
            "strategy": "replace",
            "subsetColumns": ["country"],
            "replaceMappings": [{"fromValue": "N/A", "toValue": "UNKNOWN"}],
        },
    )
    rows = {r["user_id"]: (r["amount"], r["country"]) for r in replaced_df.collect()}
    assert rows["u1"] == (0.0, "UNKNOWN")
    assert rows["u2"] == (25.0, "US")
```
