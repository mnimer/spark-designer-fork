---
name: transform-join
description: Batch, stream-static, and stream-stream DataFrame joins with broadcast hints, column disambiguation, and event-time range constraints.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [Join]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#join-operations
  validated: true
---

# DataFrame Join Transformation (`Join`)

Use these rules when generating PySpark {{spark_version}} code for a `Join` (`OP12`) step inside `transform(dfs, config)`.

## 1. Upstream Handle Resolution (`left` and `right`)

A `Join` node receives exactly two incoming edges in the pipeline YAML:
- Resolve `left_df = dfs[left_edge_source_id]` from the incoming edge with `targetHandle: "left"`.
- Resolve `right_df = dfs[right_edge_source_id]` from the incoming edge with `targetHandle: "right"`.
- Never swap `left` and `right` inputs because outer, semi, anti, and stream-static joins depend on side order.

## 2. Supported Join Types (`joinType` / `how`)

Normalize `properties.joinType` (or fallback `properties.how`, defaulting to `"inner"`) to one of PySpark's canonical `how` strings:
- `"inner"` — Emits matching rows from both `left_df` and `right_df`.
- `"left_outer"` — Emits all rows from `left_df` and matching rows from `right_df` (null-padded when unmatched).
- `"right_outer"` — Emits all rows from `right_df` and matching rows from `left_df` (null-padded when unmatched).
- `"full_outer"` — Emits matched and unmatched rows from both sides.
- `"left_semi"` — Emits rows from `left_df` that have a match in `right_df`, projecting only `left_df` columns.
- `"left_anti"` — Emits rows from `left_df` that have no match in `right_df`, projecting only `left_df` columns.
- `"cross"` — Cartesian product via `left_df.crossJoin(right_target)` when no join condition is provided, or `how="cross"`.

## 3. Broadcast Lookup Joins (`broadcastRight`)

- When `properties.broadcastRight` is `True` and `right_df` is a static batch DataFrame (`not right_df.isStreaming`), wrap the right side in `F.broadcast(right_df)` so Catalyst plans a `BroadcastHashJoin` instead of a cluster-wide `SortMergeJoin` shuffle.
- Never call `F.broadcast(right_df)` on a streaming DataFrame (`right_df.isStreaming == True`) or reference `spark.sparkContext.broadcast(...)`.

## 4. Column Disambiguation, Duplicate Keys, and Prefixing

Prevent ambiguous column reference errors (`AMBIGUOUS_REFERENCE`) downstream when both inputs share column names:

1. **Right Non-Key Column Prefixing (`rightColumnPrefix`)**:
   - If `rightColumnPrefix` is non-empty (for example `"right_"`) and `joinType` is not `"left_semi"` or `"left_anti"`, rename colliding non-key columns on `right_df` in a single pass with `right_df.withColumnsRenamed(rename_map)` before joining.
2. **Same-Name Equi-Join Keys (`conditionMode == "keys"`)**:
   - If every configured `{ leftColumn, rightColumn }` pair has `leftColumn == rightColumn`, `dropDuplicateRightKeys` is `True` (default), and `streamTimeConstraint` is empty, pass `on=[pair["leftColumn"] for pair in join_keys]`. PySpark automatically deduplicates those equi-join key columns.
3. **Aliased Column Conditions (`left_df.alias("l")`, `right_df.alias("r")`)**:
   - When left and right key names differ, when `conditionMode == "expression"` (`joinExpression`), or when `streamTimeConstraint` is present, alias both sides (`left_aliased = left_df.alias("l")`, `right_aliased = right_df.alias("r")`) and construct the join predicate against `l.<col>` and `r.<col>`.
   - If `dropDuplicateRightKeys` is `True` and `joinType` is not `"left_semi"` or `"left_anti"`, drop the redundant `r.<rightColumn>` key columns after `.join(...)` (for example `joined_df = joined_df.drop(*[F.col(f"r.{k['rightColumn']}") for k in join_keys])` when `joinType` is `"inner"` or `"left_outer"`). For `"full_outer"` joins on differing key names, coalesce or retain keys explicitly as needed so unmatched right rows do not lose their key values.

## 5. Execution Modes (`ExecutionPlan.nodes[nodeId].joinMode`)

Honor `ExecutionPlan.nodes[nodeId].joinMode` when generating join logic:

- **`batch_batch`**:
  - Standard Catalyst batch join supporting all seven `joinType` values and `F.broadcast(right_df)`.
- **`stream_static`**:
  - Stateless streaming enrichment join between a streaming stream and a static dimension DataFrame.
  - Supported combinations: `left` streaming + `right` static supports `"inner"`, `"left_outer"`, `"left_semi"`, and `"left_anti"` (plus `F.broadcast(right_df)`). `left` static + `right` streaming supports `"inner"` and `"right_outer"`. `"full_outer"` is not supported in Structured Streaming stream-static joins.
- **`stream_stream`**:
  - Stateful join between two streaming DataFrames.
  - **Stream-Stream Outer Joins (`left_outer`, `right_outer`, `full_outer`)**: Both upstream branches MUST apply `.withWatermark(...)` before the join AND the join condition MUST include a bounded event-time range predicate (`streamTimeConstraint`, such as `r.event_ts >= l.event_ts AND r.event_ts <= l.event_ts + INTERVAL 10 MINUTES`) combined via `& F.expr(stream_time_constraint)`. Without both watermarks and an event-time range constraint, Spark cannot evict state or emit null-padded unmatched rows.
  - **Stream-Stream Inner Joins (`inner`)**: Include `& F.expr(stream_time_constraint)` whenever `streamTimeConstraint` is configured so Spark bounds state store memory.

```python
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_join_step(
    left_df: DataFrame,
    right_df: DataFrame,
    join_type: str = "inner",
    broadcast_right: bool = False,
    condition_mode: str = "keys",
    join_keys: list[dict[str, str]] | None = None,
    join_expression: str = "",
    stream_time_constraint: str = "",
    drop_duplicate_right_keys: bool = True,
    right_column_prefix: str = "",
) -> DataFrame:
    """Joins left_df and right_df with broadcast, disambiguation, and stream time constraints."""
    keys = [
        k for k in (join_keys or [])
        if k.get("leftColumn", "").strip() and k.get("rightColumn", "").strip()
    ]
    is_semi_or_anti = join_type in {"left_semi", "left_anti"}

    if right_column_prefix and not is_semi_or_anti:
        right_key_set = {k["rightColumn"].strip() for k in keys}
        left_col_set = set(left_df.columns)
        rename_map = {
            col_name: f"{right_column_prefix}{col_name}"
            for col_name in right_df.columns
            if col_name in left_col_set and col_name not in right_key_set
        }
        if rename_map:
            right_df = right_df.withColumnsRenamed(rename_map)

    use_broadcast = bool(broadcast_right) and not right_df.isStreaming

    if join_type == "cross":
        target_right = F.broadcast(right_df) if use_broadcast else right_df
        return left_df.crossJoin(target_right)

    same_name_keys = (
        condition_mode == "keys"
        and len(keys) > 0
        and all(k["leftColumn"].strip() == k["rightColumn"].strip() for k in keys)
        and drop_duplicate_right_keys
        and not stream_time_constraint.strip()
    )
    if same_name_keys:
        target_right = F.broadcast(right_df) if use_broadcast else right_df
        key_names = [k["leftColumn"].strip() for k in keys]
        return left_df.join(target_right, on=key_names, how=join_type)

    left_aliased = left_df.alias("l")
    right_aliased = right_df.alias("r")
    target_right = F.broadcast(right_aliased) if use_broadcast else right_aliased

    if condition_mode == "expression" and join_expression.strip():
        cond = F.expr(join_expression.strip())
    else:
        cond = None
        for pair in keys:
            eq_expr = F.col(f"l.{pair['leftColumn'].strip()}") == F.col(
                f"r.{pair['rightColumn'].strip()}"
            )
            cond = eq_expr if cond is None else (cond & eq_expr)

    if stream_time_constraint.strip():
        time_expr = F.expr(stream_time_constraint.strip())
        cond = time_expr if cond is None else (cond & time_expr)

    joined_df = left_aliased.join(target_right, on=cond, how=join_type)

    if (
        drop_duplicate_right_keys
        and condition_mode == "keys"
        and not is_semi_or_anti
        and join_type in {"inner", "left_outer"}
    ):
        for pair in keys:
            right_col_ref = F.col(f"r.{pair['rightColumn'].strip()}")
            joined_df = joined_df.drop(right_col_ref)

    return joined_df
```

## 6. Offline Unit Testing (`tests/test_pipeline.py`)

Verify `Join` inside `tests/test_pipeline.py` using in-memory `spark.createDataFrame(...)` fixtures for both `left` and `right` upstream node IDs, asserting output row counts, null-padded outer columns, and absence of duplicate key columns:

```python
def test_join_step_broadcast_lookup(spark):
    """Verifies left_outer broadcast join deduplicates matching join keys."""
    orders_df = spark.createDataFrame(
        [("o1", "c1", 100.0), ("o2", "c99", 50.0)],
        ["order_id", "customer_id", "amount"],
    )
    customers_df = spark.createDataFrame(
        [("c1", "Enterprise")],
        ["customer_id", "tier"],
    )
    result_dfs = transform(
        {"src_orders": orders_df, "src_customers": customers_df},
        {},
    )
    out_df = result_dfs["step_join"]
    assert out_df.columns == ["customer_id", "order_id", "amount", "tier"]
    rows = {r["order_id"]: r["tier"] for r in out_df.collect()}
    assert rows == {"o1": "Enterprise", "o2": None}
```
