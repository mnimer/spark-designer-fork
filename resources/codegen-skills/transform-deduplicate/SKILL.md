---
name: transform-deduplicate
description: Removes duplicate rows across all columns, key subsets, streaming watermark bounds, or deterministic latest-row windows.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [Deduplicate]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicatesWithinWatermark.html
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#streaming-deduplication
  validated: true
---

# DataFrame Deduplication (`Deduplicate`)

Use these rules when generating PySpark {{spark_version}} code for a `Deduplicate` (`OP17`) step inside `transform(dfs, config)`.

## 1. Deduplication Strategies (`strategy`)

Select the Catalyst deduplication pattern matching `properties.strategy`:

1. **`"distinct_all"` (`df.distinct()`)**:
   - Deduplicates across every column in the schema.
   - Emit `df.distinct()` on batch DataFrames, or `df.dropDuplicates()` on streaming DataFrames.
2. **`"drop_duplicates"` (`df.dropDuplicates(subset)`)**:
   - Deduplicates on `subsetColumns` (`df.dropDuplicates(subset_cols)` when non-empty, or `df.dropDuplicates()` when empty).
   - Retains an arbitrary matching row per key across batch or streaming executions. On streaming DataFrames without a watermark, state grows indefinitely unless the event-time watermark column is part of `subsetColumns` or `within_watermark` is used.
3. **`"within_watermark"` (`df.dropDuplicatesWithinWatermark(subset)`)**:
   - Bounded-state Structured Streaming deduplication in Spark 4.x.
   - Requires an upstream `Watermark` step (`df.withWatermark(event_time_col, delay_threshold)`). Spark evicts deduplication state keys once the event-time watermark advances past the configured delay threshold.
   - When `df.isStreaming` is `True`, call `df.dropDuplicatesWithinWatermark(subset_cols)` (or `df.dropDuplicatesWithinWatermark()` if `subset_cols` is empty).
   - When `df.isStreaming` is `False` (such as offline `pytest` unit tests using static `spark.createDataFrame(...)` inputs), fall back cleanly to `df.dropDuplicates(subset_cols or None)` so `transform(dfs, config)` remains 100% testable in memory.
4. **`"keep_latest"` (Deterministic Batch Window Deduplication)**:
   - Deterministically selects the newest (or oldest) record per key subset in batch pipelines (such as CDC upserts ordered by `updated_at` or `sequence_num`), avoiding the non-deterministic row choice of `dropDuplicates(subset)`.
   - Construct `win_spec = Window.partitionBy(*[F.col(c) for c in subset_cols]).orderBy(sort_expr)` where `sort_expr` uses `F.col(order_by_column).asc()` when `orderDirection == "asc"` or `F.col(order_by_column).desc()` when `orderDirection == "desc"` (default).
   - Attach `_dedup_rn` with `df.withColumns({"_dedup_rn": F.row_number().over(win_spec)})`, filter `F.col("_dedup_rn") == 1`, and `.drop("_dedup_rn")`.

```python
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def apply_deduplicate_step(
    df: DataFrame,
    strategy: str = "drop_duplicates",
    subset_columns: list[str] | None = None,
    order_by_column: str = "",
    order_direction: str = "desc",
) -> DataFrame:
    """Removes duplicate rows using distinct, dropDuplicates, watermark state, or Window."""
    subset = [c.strip() for c in (subset_columns or []) if c.strip()]

    if strategy == "distinct_all":
        return df.dropDuplicates() if df.isStreaming else df.distinct()

    if strategy == "within_watermark":
        if df.isStreaming:
            return (
                df.dropDuplicatesWithinWatermark(subset)
                if subset
                else df.dropDuplicatesWithinWatermark()
            )
        return df.dropDuplicates(subset) if subset else df.dropDuplicates()

    if strategy == "keep_latest":
        if not subset or not order_by_column.strip():
            raise ValueError("keep_latest requires both subsetColumns and orderByColumn.")
        sort_col = F.col(order_by_column.strip())
        sort_expr = sort_col.asc() if order_direction == "asc" else sort_col.desc()
        win_spec = Window.partitionBy(*[F.col(c) for c in subset]).orderBy(sort_expr)
        return (
            df.withColumns({"_dedup_rn": F.row_number().over(win_spec)})
            .filter(F.col("_dedup_rn") == 1)
            .drop("_dedup_rn")
        )

    return df.dropDuplicates(subset) if subset else df.dropDuplicates()
```

## 2. Offline Unit Testing (`tests/test_pipeline.py`)

Test `Deduplicate` offline in `tests/test_pipeline.py` using static DataFrames, verifying key uniqueness and deterministic `keep_latest` ordering:

```python
def test_deduplicate_keep_latest_retains_newest_version(spark):
    """Verifies keep_latest retains the highest version row per event_id."""
    df = spark.createDataFrame(
        [
            ("evt_1", 1, "PENDING"),
            ("evt_1", 3, "SETTLED"),
            ("evt_1", 2, "AUTHORIZED"),
            ("evt_2", 1, "PENDING"),
        ],
        ["event_id", "version", "status"],
    )
    out_df = apply_deduplicate_step(
        df,
        strategy="keep_latest",
        subset_columns=["event_id"],
        order_by_column="version",
        order_direction="desc",
    )
    assert "_dedup_rn" not in out_df.columns
    statuses = {r["event_id"]: r["status"] for r in out_df.collect()}
    assert statuses == {"evt_1": "SETTLED", "evt_2": "PENDING"}
```
