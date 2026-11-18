---
name: transform-top-n
description: Selects global top N rows via orderBy and limit or per-group top N rows using Catalyst Window ranking functions.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [TopN]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html
  validated: true
---

# Top-N & Per-Group Ranking Transformation (`TopN`)

Use these rules when generating PySpark {{spark_version}} code for a `TopN` (`OP16`) step inside `transform(dfs, config)`.

## 1. Sort Expression Construction (`orderByColumns`)

Convert every `{ column, direction }` item in `properties.orderByColumns` into a Catalyst sort expression:
- `"desc"` (default) — `F.col(column).desc()`
- `"asc"` — `F.col(column).asc()`

Never call `.take(n)` or `.head(n)` inside `transform(dfs, config)` because those actions collect rows onto the driver instead of returning a lazy `DataFrame`.

## 2. Global Top-N vs. Per-Group Window Top-N (`partitionByColumns`)

Choose the execution strategy based on `partitionByColumns`, `rankingFunction`, and `keepRankColumn`:

1. **Global Top-N (`partitionByColumns` is empty, `rankingFunction == "row_number"`, and `keepRankColumn == False`)**:
   - Emit `df.orderBy(*sort_exprs).limit(n)`.
   - Catalyst compiles `orderBy(...).limit(n)` into a `TakeOrderedAndProject` top-K heap on each executor partition without shuffling the entire dataset into a single global `Window` partition.
2. **Per-Group Top-N (`partitionByColumns` is non-empty, or `keepRankColumn == True`, or `rankingFunction` is `"rank"` / `"dense_rank"`)**:
   - Build a `WindowSpec` partitioned by `partitionByColumns` (when non-empty) and ordered by `sort_exprs`:
     - When `partitionByColumns` is non-empty: `win_spec = Window.partitionBy(*[F.col(c) for c in partition_cols]).orderBy(*sort_exprs)`
     - When `partitionByColumns` is empty: `win_spec = Window.orderBy(*sort_exprs)`
   - Select the ranking expression from `properties.rankingFunction`:
     - `"row_number"` (default) — `F.row_number().over(win_spec)` (emits at most `n` rows per partition, breaking ties deterministically by `orderByColumns`).
     - `"rank"` — `F.rank().over(win_spec)` (retains tied rows at boundary rank `<= n` with standard rank gaps).
     - `"dense_rank"` — `F.dense_rank().over(win_spec)` (retains all rows in the top `n` distinct sort values without rank gaps).
   - Attach the rank column using `df.withColumns({rank_col: rank_expr})`, filter `F.col(rank_col) <= n`, and drop `rank_col` via `.drop(rank_col)` unless `properties.keepRankColumn` is `True`.

```python
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def apply_top_n_step(
    df: DataFrame,
    n: int = 10,
    ranking_function: str = "row_number",
    partition_by_columns: list[str] | None = None,
    order_by_columns: list[dict[str, str]] | None = None,
    keep_rank_column: bool = False,
    rank_column_alias: str = "rank_num",
) -> DataFrame:
    """Selects global or per-partition top N rows using Catalyst sort or Window ranking."""
    limit_n = max(1, int(n))
    part_cols = [c.strip() for c in (partition_by_columns or []) if c.strip()]
    sort_specs = [
        item for item in (order_by_columns or []) if item.get("column", "").strip()
    ]
    sort_exprs = [
        F.col(item["column"].strip()).asc()
        if item.get("direction") == "asc"
        else F.col(item["column"].strip()).desc()
        for item in sort_specs
    ]
    if not sort_exprs:
        raise ValueError("TopN requires at least one orderByColumns entry.")

    if not part_cols and ranking_function == "row_number" and not keep_rank_column:
        return df.orderBy(*sort_exprs).limit(limit_n)

    win_spec = (
        Window.partitionBy(*[F.col(c) for c in part_cols]).orderBy(*sort_exprs)
        if part_cols
        else Window.orderBy(*sort_exprs)
    )

    if ranking_function == "rank":
        rank_expr = F.rank().over(win_spec)
    elif ranking_function == "dense_rank":
        rank_expr = F.dense_rank().over(win_spec)
    else:
        rank_expr = F.row_number().over(win_spec)

    alias_name = rank_column_alias.strip() or "rank_num"
    ranked_df = df.withColumns({alias_name: rank_expr}).filter(F.col(alias_name) <= limit_n)
    if not keep_rank_column:
        return ranked_df.drop(alias_name)
    return ranked_df
```

## 3. Offline Unit Testing (`tests/test_pipeline.py`)

Verify both global `TopN` and per-group `Window.partitionBy(...)` `TopN` in `tests/test_pipeline.py` using in-memory DataFrames:

```python
def test_top_n_per_partition_drops_helper_rank_column(spark):
    """Verifies per-region top 2 revenue rows and drops helper rank column by default."""
    df = spark.createDataFrame(
        [
            ("US", "p1", 300.0),
            ("US", "p2", 200.0),
            ("US", "p3", 50.0),
            ("EU", "p4", 400.0),
        ],
        ["region", "product_id", "revenue"],
    )
    out_df = apply_top_n_step(
        df,
        n=2,
        ranking_function="row_number",
        partition_by_columns=["region"],
        order_by_columns=[{"column": "revenue", "direction": "desc"}],
        keep_rank_column=False,
    )
    assert "rank_num" not in out_df.columns
    products = sorted(r["product_id"] for r in out_df.collect())
    assert products == ["p1", "p2", "p4"]
```
