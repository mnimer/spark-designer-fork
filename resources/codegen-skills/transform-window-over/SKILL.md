---
name: transform-window-over
description: PySpark 4.x Window analytic ranking, offset, and frame-bounded cumulative aggregation rules with single-pass projections.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [WindowOver]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html
  validated: true
---

# PySpark 4.x Analytic Window Functions (`WindowOver`)

Implement `WindowOver` steps inside `transform(dfs, config)` using `pyspark.sql.window.Window` specifications and a single-pass `df.withColumns({...})` projection.

## 1. Building the `WindowSpec` (`partitionBy` & `orderBy`)

Import `Window` from `pyspark.sql.window` and `functions as F` from `pyspark.sql`:

- **Partition Columns (`properties.partitionByColumns`)**:
  - Pass partition keys to `Window.partitionBy(*[F.col(c) for c in partition_cols])`.
  - **Single-Partition Shuffle Warning**: Never omit `partitionBy` on large production datasets unless global ranking is explicitly requested. An unpartitioned `Window.orderBy(...)` forces Catalyst (`SinglePartition` distribution) to shuffle every row onto one executor task, risking out-of-memory failures.
- **Order Columns (`properties.orderByColumns`)**:
  - Map `{column, direction}` entries to `F.col(column).desc()` when `direction == "desc"` and `F.col(column).asc()` otherwise, then chain `.orderBy(*sort_exprs)`.

## 2. Frame Specifications (`rowsBetween` & `rangeBetween`)

Resolve frame boundary tokens (`unboundedPreceding`, `currentRow`, `unboundedFollowing`, or integer offsets):
- `"unboundedPreceding"` -> `Window.unboundedPreceding`
- `"currentRow"` -> `Window.currentRow`
- `"unboundedFollowing"` -> `Window.unboundedFollowing`
- Signed integer string (for example `"-6"` or `"3"`) -> `int(boundary)`

Apply the frame according to `properties.frameType`:
- **`"default"`**: Keep `base_win = Window.partitionBy(...).orderBy(...)` without explicit frame bounds.
- **`"rowsBetween"`**: `framed_win = base_win.rowsBetween(start_bound, end_bound)` for physical row-count windows (such as 7-row moving averages).
- **`"rangeBetween"`**: `framed_win = base_win.rangeBetween(start_bound, end_bound)` for logical value-offset windows relative to the `orderBy` expression.

### Frame Compatibility Rule for Ranking & Offset Functions
Catalyst rejects custom `rowsBetween` or `rangeBetween` frames on ranking and offset functions (`row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`). When a `WindowOver` node mixes ranking/offset metrics with cumulative aggregate metrics, evaluate ranking/offset functions against `base_win` (unframed) and cumulative aggregates against `framed_win`.

## 3. Analytic & Cumulative Metric Mapping (`windowMetrics`)

Construct all window expressions and apply them in **one** `df.withColumns({...})` call (never call `.withColumn()` inside a loop):

- **Ranking functions** (use `base_win`):
  - `row_number`: `F.row_number().over(base_win)`
  - `rank`: `F.rank().over(base_win)`
  - `dense_rank`: `F.dense_rank().over(base_win)`
  - `percent_rank`: `F.percent_rank().over(base_win)`
  - `cume_dist`: `F.cume_dist().over(base_win)`
  - `ntile`: `F.ntile(int(metric.get("offset") or 4)).over(base_win)`
- **Offset functions** (use `base_win`):
  - `lag`: `F.lag(F.col(source_col), offset=int(offset or 1), default=default_val).over(base_win)`
  - `lead`: `F.lead(F.col(source_col), offset=int(offset or 1), default=default_val).over(base_win)`
- **Value & cumulative aggregate functions** (use `framed_win`):
  - `first`: `F.first(F.col(source_col), ignorenulls=True).over(framed_win)`
  - `last`: `F.last(F.col(source_col), ignorenulls=True).over(framed_win)`
  - `sum`: `F.sum(F.col(source_col)).over(framed_win)`
  - `avg`: `F.avg(F.col(source_col)).over(framed_win)`
  - `min`: `F.min(F.col(source_col)).over(framed_win)`
  - `max`: `F.max(F.col(source_col)).over(framed_win)`
  - `count`: `F.count(F.lit(1)).over(framed_win)` when `source_col == "*"` else `F.count(F.col(source_col)).over(framed_win)`

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def apply_window_over(df: DataFrame) -> DataFrame:
    """Computes per-account event ranks, prior amounts, and running totals in one pass."""
    base_win = Window.partitionBy(F.col("account_id")).orderBy(F.col("event_ts").asc())
    running_win = base_win.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    return df.withColumns(
        {
            "tx_seq": F.row_number().over(base_win),
            "prev_amount": F.lag(F.col("amount"), offset=1, default=0.0).over(base_win),
            "running_total": F.sum(F.col("amount")).over(running_win),
        }
    )
```

## 4. Streaming Compatibility Note

Non-time-windowed `Window.partitionBy(...).orderBy(...)` analytic specifications are batch Catalyst operations. For unbounded streaming DataFrames, execute `WindowOver` inside a `foreachBatch` micro-batch callback, or use `GroupByAgg` time windows / `StatefulTransform` for continuous event-time state.
