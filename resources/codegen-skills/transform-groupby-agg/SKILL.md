---
name: transform-groupby-agg
description: Grouped, global, tumbling, sliding, and session-windowed DataFrame aggregations with flat window unpacking and streaming watermark rules.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [GroupByAgg]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time
  validated: true
---

# GroupBy & Time-Window Aggregation (`GroupByAgg`)

Use these rules when generating PySpark {{spark_version}} code for a `GroupByAgg` (`OP15`) step inside `transform(dfs, config)`.

## 1. Grouping Keys & Built-in Event-Time Windows (`df.groupBy`)

Build the list of grouping expressions passed to `df.groupBy(*group_exprs)` from `windowType` and `groupByColumns`:

1. **Event-Time Window Expression (`windowType`)**:
   - **`"None"` (default)**: Do not prepend a window expression.
   - **`"Tumbling"`**: Prepend `F.window(F.col(time_column), window_duration)` using `timeColumn` and `windowDuration` (for example `"5 minutes"`).
   - **`"Sliding"`**: Prepend `F.window(F.col(time_column), window_duration, slide_duration)` using `timeColumn`, `windowDuration`, and `slideDuration` (for example `"10 minutes"` and `"1 minute"`).
   - **`"Session"`**: Prepend `F.session_window(F.col(time_column), session_gap_duration)` using `timeColumn` and `sessionGapDuration` (for example `"10 minutes"`).
2. **Grouping Columns (`groupByColumns`)**:
   - Append `[F.col(c) for c in group_by_columns]` after any active window expression.
   - If `windowType == "None"` and `groupByColumns` is empty, call `df.agg(*agg_exprs)` (or `df.groupBy().agg(*agg_exprs)`) for a global single-row aggregation.

## 2. Supported Aggregate Functions (`aggregations`)

Map every `{ column, fn, alias }` entry in `properties.aggregations` to a Catalyst `pyspark.sql.functions` expression with `.alias(output_alias)`:
- `"count"` — `F.count(F.lit(1)).alias(alias)` when `column == "*"` or empty, otherwise `F.count(F.col(column)).alias(alias)`.
- `"sum"` — `F.sum(F.col(column)).alias(alias)`.
- `"avg"` — `F.avg(F.col(column)).alias(alias)`.
- `"min"` — `F.min(F.col(column)).alias(alias)`.
- `"max"` — `F.max(F.col(column)).alias(alias)`.
- `"countDistinct"` — `F.count_distinct(F.col(column)).alias(alias)` (exact `count_distinct` applies to batch DataFrames only, so use `approx_count_distinct` on streaming DataFrames).
- `"approx_count_distinct"` — `F.approx_count_distinct(F.col(column)).alias(alias)` (HyperLogLog++ sketch supported in both batch and Structured Streaming).
- `"stddev"` — `F.stddev(F.col(column)).alias(alias)`.
- `"variance"` — `F.variance(F.col(column)).alias(alias)`.
- `"first"` — `F.first(F.col(column), ignorenulls=True).alias(alias)`.
- `"last"` — `F.last(F.col(column), ignorenulls=True).alias(alias)`.
- `"collect_list"` — `F.collect_list(F.col(column)).alias(alias)`.
- `"collect_set"` — `F.collect_set(F.col(column)).alias(alias)`.

## 3. Unpacking `window.start` and `window.end` (`flattenWindowStruct`)

`F.window(...)` and `F.session_window(...)` emit a nested struct column (`window: struct<start: timestamp, end: timestamp>` or `session_window: struct<start: timestamp, end: timestamp>`):
- When `properties.flattenWindowStruct` is `True` OR when a downstream sink requires flat scalar columns (such as CSV, JDBC, Cloud SQL, AlloyDB, Spanner, or flat relational tables), unpack the window struct in a single `.withColumns(...)` call and drop the struct column:
  ```python
  win_col = "session_window" if window_type == "Session" else "window"
  agg_df = agg_df.withColumns(
      {
          "window_start": F.col(f"{win_col}.start"),
          "window_end": F.col(f"{win_col}.end"),
      }
  ).drop(win_col)
  ```

## 4. Structured Streaming Watermark & Output Mode Rules

When `GroupByAgg` executes on a streaming DataFrame (`df.isStreaming == True` or node listed in `ExecutionPlan.streamingNodeIds`):
- **Upstream Watermark**: Ensure an upstream `Watermark` (`df.withWatermark(time_column, delay_threshold)`) precedes windowed streaming aggregations so Spark bounds state store size and finalizes expired windows.
- **Output Mode Compatibility**:
  - `outputMode("append")` on a streaming aggregation **requires** both an event-time window (`F.window` or `F.session_window`) and an upstream watermark on `timeColumn`.
  - Unwindowed streaming aggregations cannot use `outputMode("append")` and must use `outputMode("update")` or `outputMode("complete")` (configured via `ExecutionPlan.streamingSinkGroups`).
- **Streaming Function Support**: Prefer `F.approx_count_distinct(...)` over `F.count_distinct(...)` in streaming aggregations because Structured Streaming forbids exact distinct counts inside stateful streaming `agg()`.

```python
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_agg_expr(column: str, fn: str, alias: str):
    """Builds a single aliased PySpark aggregate Column expression."""
    col_name = column.strip()
    out_alias = alias.strip() or f"{fn}_{col_name or 'all'}"
    if fn == "count":
        target = F.lit(1) if col_name in {"", "*"} else F.col(col_name)
        return F.count(target).alias(out_alias)
    if fn == "sum":
        return F.sum(F.col(col_name)).alias(out_alias)
    if fn == "avg":
        return F.avg(F.col(col_name)).alias(out_alias)
    if fn == "min":
        return F.min(F.col(col_name)).alias(out_alias)
    if fn == "max":
        return F.max(F.col(col_name)).alias(out_alias)
    if fn == "countDistinct":
        return F.count_distinct(F.col(col_name)).alias(out_alias)
    if fn == "approx_count_distinct":
        return F.approx_count_distinct(F.col(col_name)).alias(out_alias)
    if fn == "stddev":
        return F.stddev(F.col(col_name)).alias(out_alias)
    if fn == "variance":
        return F.variance(F.col(col_name)).alias(out_alias)
    if fn == "first":
        return F.first(F.col(col_name), ignorenulls=True).alias(out_alias)
    if fn == "last":
        return F.last(F.col(col_name), ignorenulls=True).alias(out_alias)
    if fn == "collect_list":
        return F.collect_list(F.col(col_name)).alias(out_alias)
    if fn == "collect_set":
        return F.collect_set(F.col(col_name)).alias(out_alias)
    raise ValueError(f"Unsupported Spark aggregate function: {fn}")


def apply_groupby_agg_step(
    df: DataFrame,
    group_by_columns: list[str] | None = None,
    window_type: str = "None",
    time_column: str = "",
    window_duration: str = "5 minutes",
    slide_duration: str = "1 minute",
    session_gap_duration: str = "10 minutes",
    flatten_window_struct: bool = False,
    aggregations: list[dict[str, str]] | None = None,
) -> DataFrame:
    """Applies grouped or time-windowed Catalyst aggregations to a DataFrame."""
    group_exprs = []
    if window_type == "Tumbling":
        group_exprs.append(F.window(F.col(time_column), window_duration))
    elif window_type == "Sliding":
        group_exprs.append(F.window(F.col(time_column), window_duration, slide_duration))
    elif window_type == "Session":
        group_exprs.append(F.session_window(F.col(time_column), session_gap_duration))

    for col_name in group_by_columns or []:
        if col_name.strip():
            group_exprs.append(F.col(col_name.strip()))

    specs = aggregations or [{"column": "*", "fn": "count", "alias": "record_count"}]
    agg_exprs = [
        build_agg_expr(spec.get("column", "*"), spec.get("fn", "count"), spec.get("alias", ""))
        for spec in specs
    ]

    agg_df = df.groupBy(*group_exprs).agg(*agg_exprs) if group_exprs else df.agg(*agg_exprs)

    if window_type != "None" and flatten_window_struct:
        win_col = "session_window" if window_type == "Session" else "window"
        agg_df = agg_df.withColumns(
            {
                "window_start": F.col(f"{win_col}.start"),
                "window_end": F.col(f"{win_col}.end"),
            }
        ).drop(win_col)

    return agg_df
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `GroupByAgg` offline in `tests/test_pipeline.py` with in-memory `TimestampType` and numeric columns, verifying grouped metrics and unpacked `window_start` / `window_end` boundaries:

```python
from datetime import datetime, timezone


def test_groupby_agg_tumbling_window_flattened(spark):
    """Verifies 5-minute tumbling window aggregation and flat window column unpacking."""
    rows = [
        ("US", 10.0, datetime(2026, 1, 1, 12, 1, tzinfo=timezone.utc)),
        ("US", 25.0, datetime(2026, 1, 1, 12, 3, tzinfo=timezone.utc)),
        ("EU", 40.0, datetime(2026, 1, 1, 12, 2, tzinfo=timezone.utc)),
    ]
    df = spark.createDataFrame(rows, ["region", "amount", "event_ts"])
    out_df = apply_groupby_agg_step(
        df,
        group_by_columns=["region"],
        window_type="Tumbling",
        time_column="event_ts",
        window_duration="5 minutes",
        flatten_window_struct=True,
        aggregations=[{"column": "amount", "fn": "sum", "alias": "total_amount"}],
    )
    assert "window_start" in out_df.columns
    assert "window_end" in out_df.columns
    totals = {r["region"]: r["total_amount"] for r in out_df.collect()}
    assert totals == {"US": 35.0, "EU": 40.0}
```
