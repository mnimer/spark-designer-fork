---
name: transform-merge-cdc-scd
description: Deduplicate incoming CDC streams per primary key and stage pure SCD Type 1 or SCD Type 2 merge rows in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [MergeCdcScd]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html
    - https://iceberg.apache.org/docs/latest/spark-writes/
  validated: true
---

# CDC Deduplication & SCD Type 1 / Type 2 Staging (`MergeCdcScd`)

Use these rules when generating PySpark {{spark_version}} code for a `MergeCdcScd` (`EXT4`) step inside `transform(dfs, config)`.

## 1. Pure Two-Input Contract (`left` and `right`)

A `MergeCdcScd` node receives two incoming edges in the pipeline YAML and preserves strict `transform(dfs, config)` purity (`D3`) with zero external catalog or storage I/O:
- Resolve `left_df = dfs[left_edge_source_id]` from the incoming edge with `targetHandle: "left"`. `left_df` carries the incoming CDC stream or batch rows.
- Resolve `right_df = dfs.get(right_edge_source_id)` from the incoming edge with `targetHandle: "right"`. `right_df` carries the current target dimension or table snapshot read inside `read_sources(spark, config)`.
- Never call `spark.read`, `spark.table`, `DeltaTable`, or `spark.sql("MERGE INTO ...")` inside `transform(dfs, config)`. Emit a staged DataFrame from `transform(dfs, config)` and execute the physical `MERGE` inside `write_sinks(dfs, config)` (or inside `foreachBatch` for streaming pipelines, refreshing the static target snapshot per micro-batch before calling `transform`).

## 2. Four-Stage Pure CDC / SCD Algorithm

Implement `apply_merge_cdc_scd(...)` with four deterministic Catalyst stages:

1. **Key Deduplication (`keyColumns`, `sequenceColumn`)**:
   - If `cdcFormat == "delta_cdf"` and `"_change_type"` exists on `left_df`, filter out `"update_preimage"` rows before windowing so tied `_commit_version` updates always select `"update_postimage"`.
   - Partition `left_df` by `keyColumns` and order by `F.col(sequenceColumn).desc_nulls_last()` using `Window.partitionBy(*key_cols).orderBy(F.col(sequence_col).desc_nulls_last())`.
   - Keep `F.row_number() == 1` and drop the helper row-number column so duplicate keys in one batch or micro-batch never trigger SQL `MERGE` cardinality errors.
2. **CDC Operation Normalization (`cdcFormat`, `operationColumn`, `deleteValues`)**:
   - Compute a boolean `_is_delete` column according to `cdcFormat`:
     - `"debezium"`: `F.col(op_col) == F.lit("d")` (default `operationColumn` is `"op"`).
     - `"delta_cdf"`: `F.col("_change_type") == F.lit("delete")`.
     - `"custom_op"`: `F.col(op_col).isin(delete_markers)` using trimmed `deleteValues` entries.
     - `"upsert_only"`: `F.lit(False)`.
   - Exclude CDC envelope operation columns (`op`, `_change_type`, `_commit_version`, `_commit_timestamp`) from business payload columns unless they belong to `keyColumns`, match `sequenceColumn`, or exist on `right_df`.
3. **SCD Type 1 Staging (`scdType == "scd1"`)**:
   - Compute null-safe 64-bit hashes with `F.xxhash64(*[F.col(c) for c in tracked_cols])` over `trackedColumns` (defaulting when empty to shared non-key payload columns excluding `sequenceColumn`).
   - When `right_df` is provided, project only renamed keys (`_rk_<key>`) and `_right_hash` from `right_df`, then left-outer join against deduplicated `left_df` on `keyColumns` to avoid column ambiguity.
   - Skip unchanged rows where `F.col("_left_hash").eqNullSafe(F.col("_right_hash"))` is true, skip delete tombstones for keys absent from `right_df`, and emit actionable rows tagged with `actionColumn` (default `"_scd_action"`):
     - Deleted existing key -> `"DELETE"`.
     - Changed existing key -> `"UPDATE"`.
     - Brand-new non-delete key -> `"INSERT"`.
4. **SCD Type 2 Historical Versioning (`scdType == "scd2"`)**:
   - Filter `right_df` to active rows (`F.col(isCurrentColumn) == F.lit(True)`), compute `F.xxhash64(*tracked_cols)`, and left-outer join deduplicated `left_df` against active `right_df` on `keyColumns`.
   - Resolve `open_end_expr` for `openEndValue`: if `"9999-12-31"`, cast `F.lit("9999-12-31")` to the `sequenceColumn` data type. Otherwise cast `F.lit(None)` to the `sequenceColumn` data type.
   - Emit staged rows with nullable `_merge_<key>` columns (one per key in `keyColumns`), `actionColumn` (default `"_scd_action"`), `isCurrentColumn` (default `"is_current"`), `effectiveFromColumn` (default `"effective_from"`), and `effectiveToColumn` (default `"effective_to"`):
     - **Changed active row**: emit one expiration row (`_merge_<key> = F.col(key)`, `_scd_action = "EXPIRE"`, `is_current = False`, `effective_from = F.col(sequence_col)`, `effective_to = F.col(sequence_col)`) unioned with one new version row (`_merge_<key> = NULL`, `_scd_action = "INSERT"`, `is_current = True`, `effective_from = F.col(sequence_col)`, `effective_to = open_end_expr`).
     - **Deleted active row**: emit one row with `_merge_<key> = F.col(key)`, `_scd_action = "DELETE"`, `is_current = False`, `effective_from = F.col(sequence_col)`, `effective_to = F.col(sequence_col)`.
     - **Brand-new key (non-delete)**: emit one row with `_merge_<key> = NULL`, `_scd_action = "INSERT"`, `is_current = True`, `effective_from = F.col(sequence_col)`, `effective_to = open_end_expr`.
     - Unchanged active rows and delete tombstones for missing keys are filtered out as no-ops.

## 3. Reference Implementation (`src/pipeline.py`)

```python
from functools import reduce
from operator import and_
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def apply_merge_cdc_scd(
    left_df: DataFrame,
    right_df: DataFrame | None,
    key_columns: list[str],
    sequence_column: str,
    scd_type: str = "scd1",
    cdc_format: str = "debezium",
    operation_column: str = "op",
    delete_values: list[str] | None = None,
    tracked_columns: list[str] | None = None,
    effective_from_column: str = "effective_from",
    effective_to_column: str = "effective_to",
    is_current_column: str = "is_current",
    open_end_value: str = "null",
    action_column: str = "_scd_action",
) -> DataFrame:
    """Deduplicates left CDC events and emits pure SCD Type 1 or Type 2 staged rows."""
    keys = [c.strip() for c in key_columns if c and c.strip()]
    seq_col = sequence_column.strip()
    if not keys or not seq_col:
        raise ValueError("MergeCdcScd requires keyColumns and sequenceColumn.")

    staged_left = left_df
    if cdc_format == "delta_cdf" and "_change_type" in staged_left.columns:
        staged_left = staged_left.filter(
            F.col("_change_type") != F.lit("update_preimage")
        )

    dedup_win = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(
        F.col(seq_col).desc_nulls_last()
    )
    dedup_df = (
        staged_left.withColumns({"_cdc_rn": F.row_number().over(dedup_win)})
        .filter(F.col("_cdc_rn") == 1)
        .drop("_cdc_rn")
    )

    op_col = operation_column.strip() or "op"
    if cdc_format == "debezium":
        is_delete_expr = F.col(op_col) == F.lit("d")
        cdc_meta = {op_col}
    elif cdc_format == "delta_cdf":
        is_delete_expr = F.col("_change_type") == F.lit("delete")
        cdc_meta = {"_change_type", "_commit_version", "_commit_timestamp"}
    elif cdc_format == "custom_op":
        raw_markers = (
            delete_values.split(",")
            if isinstance(delete_values, str)
            else (delete_values or ["D", "DELETE"])
        )
        markers = [v.strip() for v in raw_markers if v and v.strip()]
        is_delete_expr = F.col(op_col).isin(markers)
        cdc_meta = {op_col}
    else:
        is_delete_expr = F.lit(False)
        cdc_meta = set()

    right_col_set = set(right_df.columns) if right_df is not None else set()
    scd_meta = (
        {
            action_column,
            effective_from_column,
            effective_to_column,
            is_current_column,
        }
        if scd_type == "scd2"
        else {action_column}
    )
    key_set = set(keys)
    payload_cols = [
        c
        for c in dedup_df.columns
        if c not in scd_meta
        and not (
            c in cdc_meta
            and c not in key_set
            and c != seq_col
            and c not in right_col_set
        )
    ]

    explicit_tracked = [c.strip() for c in (tracked_columns or []) if c and c.strip()]
    if explicit_tracked:
        tracked = explicit_tracked
    elif right_df is not None:
        tracked = [
            c
            for c in payload_cols
            if c not in key_set and c != seq_col and c in right_col_set
        ]
        if not tracked:
            tracked = [c for c in payload_cols if c not in key_set and c in right_col_set]
    else:
        tracked = []

    left_hash_expr = (
        F.xxhash64(*[F.col(c) for c in tracked]) if tracked else F.lit(0)
    )
    left_hashed = dedup_df.withColumns(
        {"_is_delete": is_delete_expr, "_left_hash": left_hash_expr}
    )

    seq_dtype = dedup_df.schema[seq_col].dataType
    open_end_expr = (
        F.lit("9999-12-31").cast(seq_dtype)
        if open_end_value == "9999-12-31"
        else F.lit(None).cast(seq_dtype)
    )

    if right_df is None:
        inserts = left_hashed.filter(~F.col("_is_delete"))
        if scd_type == "scd2":
            return inserts.select(
                *[F.col(c) for c in payload_cols],
                *[
                    F.lit(None).cast(dedup_df.schema[k].dataType).alias(f"_merge_{k}")
                    for k in keys
                ],
                F.lit("INSERT").alias(action_column),
                F.lit(True).alias(is_current_column),
                F.col(seq_col).alias(effective_from_column),
                open_end_expr.alias(effective_to_column),
            )
        return inserts.select(
            *[F.col(c) for c in payload_cols],
            F.lit("INSERT").alias(action_column),
        )

    right_source = (
        right_df.filter(F.col(is_current_column) == F.lit(True))
        if scd_type == "scd2"
        else right_df
    )
    right_hash_expr = (
        F.xxhash64(*[F.col(c) for c in tracked]) if tracked else F.lit(0)
    )
    right_lookup = right_source.select(
        *[F.col(k).alias(f"_rk_{k}") for k in keys],
        right_hash_expr.alias("_right_hash"),
    )

    join_cond = reduce(and_, [F.col(k) == F.col(f"_rk_{k}") for k in keys])
    joined = left_hashed.join(right_lookup, on=join_cond, how="left_outer")
    right_exists = F.col(f"_rk_{keys[0]}").isNotNull()
    has_changed = ~F.col("_left_hash").eqNullSafe(F.col("_right_hash"))

    if scd_type == "scd1":
        action_expr = (
            F.when(F.col("_is_delete") & right_exists, F.lit("DELETE"))
            .when(~F.col("_is_delete") & ~right_exists, F.lit("INSERT"))
            .when(~F.col("_is_delete") & right_exists & has_changed, F.lit("UPDATE"))
        )
        return (
            joined.withColumns({action_column: action_expr})
            .filter(F.col(action_column).isNotNull())
            .select(*[F.col(c) for c in payload_cols], F.col(action_column))
        )

    expire_or_delete_df = joined.filter(
        (F.col("_is_delete") & right_exists)
        | (~F.col("_is_delete") & right_exists & has_changed)
    ).select(
        *[F.col(c) for c in payload_cols],
        *[F.col(k).alias(f"_merge_{k}") for k in keys],
        F.when(F.col("_is_delete"), F.lit("DELETE"))
        .otherwise(F.lit("EXPIRE"))
        .alias(action_column),
        F.lit(False).alias(is_current_column),
        F.col(seq_col).alias(effective_from_column),
        F.col(seq_col).alias(effective_to_column),
    )

    insert_df = joined.filter(
        (~F.col("_is_delete") & ~right_exists)
        | (~F.col("_is_delete") & right_exists & has_changed)
    ).select(
        *[F.col(c) for c in payload_cols],
        *[
            F.lit(None).cast(dedup_df.schema[k].dataType).alias(f"_merge_{k}")
            for k in keys
        ],
        F.lit("INSERT").alias(action_column),
        F.lit(True).alias(is_current_column),
        F.col(seq_col).alias(effective_from_column),
        open_end_expr.alias(effective_to_column),
    )

    return expire_or_delete_df.unionByName(insert_df)
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test `MergeCdcScd` completely offline with in-memory `spark.createDataFrame(...)` inputs covering duplicate-key deduplication, inserts, updates, no-op skips, and deletes for both SCD Type 1 and SCD Type 2:

```python
from pyspark.sql import SparkSession
from src.pipeline import apply_merge_cdc_scd


def test_merge_cdc_scd1_dedup_skip_noop_and_delete(spark: SparkSession) -> None:
    """Verifies SCD1 deduplicates keys, skips unchanged rows, updates changes, and emits deletes."""
    cdc_df = spark.createDataFrame(
        [
            ("c1", "Silver", "u", "2026-03-01T09:00:00Z"),
            ("c1", "Platinum", "u", "2026-03-01T10:00:00Z"),
            ("c2", "Gold", "u", "2026-03-01T10:00:00Z"),
            ("c3", "Silver", "d", "2026-03-01T10:00:00Z"),
            ("c4", "Bronze", "c", "2026-03-01T10:00:00Z"),
        ],
        ["customer_id", "tier", "op", "updated_at"],
    )
    target_df = spark.createDataFrame(
        [
            ("c1", "Starter", "2026-02-01T00:00:00Z"),
            ("c2", "Gold", "2026-02-01T00:00:00Z"),
            ("c3", "Silver", "2026-02-01T00:00:00Z"),
        ],
        ["customer_id", "tier", "updated_at"],
    )

    staged = apply_merge_cdc_scd(
        left_df=cdc_df,
        right_df=target_df,
        key_columns=["customer_id"],
        sequence_column="updated_at",
        scd_type="scd1",
        cdc_format="debezium",
    )
    rows = {r["customer_id"]: (r["tier"], r["_scd_action"]) for r in staged.collect()}
    assert rows == {
        "c1": ("Platinum", "UPDATE"),
        "c3": ("Silver", "DELETE"),
        "c4": ("Bronze", "INSERT"),
    }


def test_merge_cdc_scd2_expires_changed_deletes_active_and_inserts_new(
    spark: SparkSession,
) -> None:
    """Verifies SCD2 emits EXPIRE plus INSERT for changes, DELETE for tombstones, and skips no-ops."""
    cdc_df = spark.createDataFrame(
        [
            ("c1", "Enterprise", "u", "2026-03-02"),
            ("c2", "Pro", "u", "2026-03-02"),
            ("c3", "Starter", "d", "2026-03-02"),
            ("c4", "Free", "c", "2026-03-02"),
        ],
        ["customer_id", "tier", "op", "updated_at"],
    )
    target_df = spark.createDataFrame(
        [
            ("c1", "Starter", "2026-01-01", True, "2026-01-01", None),
            ("c2", "Pro", "2026-01-01", True, "2026-01-01", None),
            ("c3", "Starter", "2026-01-01", True, "2026-01-01", None),
        ],
        [
            "customer_id",
            "tier",
            "updated_at",
            "is_current",
            "effective_from",
            "effective_to",
        ],
    )

    staged = apply_merge_cdc_scd(
        left_df=cdc_df,
        right_df=target_df,
        key_columns=["customer_id"],
        sequence_column="updated_at",
        scd_type="scd2",
        cdc_format="debezium",
        open_end_value="null",
    )
    by_key_action = {
        (r["customer_id"], r["_scd_action"]): (
            r["_merge_customer_id"],
            r["is_current"],
            r["effective_from"],
            r["effective_to"],
        )
        for r in staged.collect()
    }
    assert by_key_action == {
        ("c1", "EXPIRE"): ("c1", False, "2026-03-02", "2026-03-02"),
        ("c1", "INSERT"): (None, True, "2026-03-02", None),
        ("c3", "DELETE"): ("c3", False, "2026-03-02", "2026-03-02"),
        ("c4", "INSERT"): (None, True, "2026-03-02", None),
    }
```
