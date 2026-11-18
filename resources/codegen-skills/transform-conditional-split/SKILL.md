---
name: transform-conditional-split
description: Route DataFrame rows across named boolean rule branches and a null-safe else fallback handle in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [ConditionalSplit]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.coalesce.html
  validated: true
---

# Multi-Output Conditional Routing (`ConditionalSplit`)

Use these rules when generating PySpark {{spark_version}} code for a `ConditionalSplit` step inside `transform(dfs, config)`.

## 1. Branch Topology (`ExecutionPlan.conditionalSplits`)

- Consult `ExecutionPlan.conditionalSplits` (and `ExecutionPlan.nodes[nodeId].conditionalBranches`) to resolve the ordered list of output handles (`handleId`, `label`, `condition`, `isFallback`, and `targetNodeIds`).
- `ConditionalSplit` exposes one output handle per configured rule (`rules: [{name, condition}]`) plus a terminal fallback handle (`handleId == "else"`, `isFallback == True`).
- Store each active branch DataFrame in `dfs` under a deterministic branch key (such as `f"{node_id}:{handle_id}"`) and pass the matching branch DataFrame to every downstream node listed in `branch.targetNodeIds`.

## 2. Evaluation Modes (`evaluationMode`) & Null-Safe Three-Valued Logic

SQL boolean expressions evaluate to `NULL` when referenced columns contain `NULL`. Wrapping a raw predicate in `~(...)` without `F.coalesce(..., F.lit(False))` causes `~(NULL | False)` to evaluate to `NULL`, silently dropping rows with null columns from both rule branches and the `else` branch.

Always wrap every rule condition in `F.coalesce(F.expr(condition), F.lit(False))`:

### A. Waterfall / First-Match Mode (`evaluationMode == "first_match"`, default)
- Each rule branch receives rows that match its predicate **and** did not match any earlier rule in `rules`:
  1. Initialize `matched_any = F.lit(False)`.
  2. For each non-fallback rule `i` with predicate `pred_i = F.coalesce(F.expr(cond_i), F.lit(False))`:
     - `branch_df = source_df.filter(pred_i & (~matched_any))`
     - `matched_any = matched_any | pred_i`
  3. Every input row lands in **at most one** rule branch, and all remaining unmatched or null-predicate rows flow to `else`.

### B. Independent Fan-Out Mode (`evaluationMode == "independent"`)
- Each rule branch evaluates its predicate independently against `source_df`:
  1. Initialize `matched_any = F.lit(False)`.
  2. For each non-fallback rule `i` with predicate `pred_i = F.coalesce(F.expr(cond_i), F.lit(False))`:
     - `branch_df = source_df.filter(pred_i)`
     - `matched_any = matched_any | pred_i`
  3. Rows matching multiple predicates are emitted to every matching rule branch.

### C. Terminal `else` Fallback Handle
- In both evaluation modes, compute the `else` branch as:
  ```python
  else_df = source_df.filter(~matched_any)
  ```
- Because every `pred_i` is wrapped in `F.coalesce(..., F.lit(False))`, `matched_any` is guaranteed non-null (`True` or `False`), so `else_df` captures every unmatched row including rows with `NULL` column values.

## 3. Caching & Unpersist Coordination (`ExecutionPlan.cacheDirectives`)

- When a **batch** `ConditionalSplit` node routes to more than one connected downstream branch (`outDegree > 1`), `ExecutionPlan.cacheDirectives` (`ExecutionPlan.cachedNodeIds`) marks the node with `reason: "batch_fanout"`.
- Call `source_df = source_df.cache()` **once** on the input DataFrame before filtering the individual rule and `else` branches so Spark does not re-read and recompute upstream stages per output branch.
- **Never** call `.cache()` or `.persist()` directly on an unbounded Structured Streaming DataFrame (`executionMode == "streaming"`). In streaming mode, `ExecutionPlan.streamingSinkGroups` handles micro-batch caching inside `foreachBatch` when multiple sinks share a query.
- In `write_sinks(dfs, config)`, honor `ExecutionPlan.unpersistPoints` (or `unpersistAfterNodeIds` on the `CacheDirective`) and call `.unpersist()` inside a `finally` block after all downstream branch sinks complete.

## 4. Reference Implementation

```python
from functools import reduce
import operator
from typing import Any
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def split_conditional_branches(
    df: DataFrame,
    step_cfg: dict[str, Any],
    cache_input: bool = False,
) -> dict[str, DataFrame]:
    """Splits a DataFrame into named boolean rule branches and a null-safe else branch."""
    source_df = df.cache() if cache_input and not df.isStreaming else df
    eval_mode = str(step_cfg.get("evaluationMode", "first_match")).lower()
    raw_rules = step_cfg.get("rules") or [{"name": "rule_1", "condition": ""}]

    branches: dict[str, DataFrame] = {}
    predicates: list[Column] = []
    matched_so_far: Column = F.lit(False)

    for idx, rule in enumerate(raw_rules):
        raw_name = str(rule.get("name", "")).strip() or f"rule_{idx + 1}"
        handle_id = f"{raw_name}_{idx + 1}" if raw_name == "else" or raw_name in branches else raw_name
        cond_sql = str(rule.get("condition", "")).strip()

        pred = F.coalesce(F.expr(cond_sql), F.lit(False)) if cond_sql else F.lit(False)
        predicates.append(pred)

        if eval_mode == "independent":
            branches[handle_id] = source_df.filter(pred)
        else:
            branches[handle_id] = source_df.filter(pred & (~matched_so_far))

        matched_so_far = matched_so_far | pred

    any_matched = reduce(operator.or_, predicates, F.lit(False))
    branches["else"] = source_df.filter(~any_matched)
    if cache_input and not df.isStreaming:
        branches["__cached_source__"] = source_df
    return branches
```

## 5. Offline Unit Testing

Verify waterfall exclusivity, independent fan-out, and null-safe `else` routing in `tests/test_pipeline.py`:

```python
from pyspark.sql import SparkSession


def test_conditional_split_first_match_and_null_safe_else(spark: SparkSession) -> None:
    """Verifies first_match waterfall precedence and null-safe else routing."""
    input_df = spark.createDataFrame(
        [
            ("o1", 1500.0),
            ("o2", 250.0),
            ("o3", 20.0),
            ("o4", None),
        ],
        ["order_id", "amount"],
    )
    rules = [
        {"name": "high_value", "condition": "amount >= 1000"},
        {"name": "standard", "condition": "amount >= 100"},
    ]

    waterfall = split_conditional_branches(
        input_df,
        {"evaluationMode": "first_match", "rules": rules},
    )
    assert [r["order_id"] for r in waterfall["high_value"].collect()] == ["o1"]
    assert [r["order_id"] for r in waterfall["standard"].collect()] == ["o2"]
    assert sorted(r["order_id"] for r in waterfall["else"].collect()) == ["o3", "o4"]

    independent = split_conditional_branches(
        input_df,
        {"evaluationMode": "independent", "rules": rules},
    )
    assert sorted(r["order_id"] for r in independent["standard"].collect()) == ["o1", "o2"]
```
