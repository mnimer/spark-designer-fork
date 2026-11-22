---
name: transform-data-quality-assert
description: Evaluate vectorized DataFrame expectations in a single pass and route violations to dead-letter error branches or fail jobs in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [DataQualityAssert]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_compact.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.assert_true.html
  validated: true
---

# Vectorized Data Quality Assertions & DLQ Routing (`DataQualityAssert`)

Use these rules when generating PySpark {{spark_version}} code for a `DataQualityAssert` step inside `transform(dfs, config)` in Imperative mode (`paradigm: imperative`). When `paradigm` is `sdp` (`## SDP Execution Plan`), follow `sdp-quality-quarantine` to emit filter-based clean and `<dataset>_quarantine` datasets via pure Catalyst predicate helpers in `src/transforms.py` without `.cache()` or `.schema` inspection.

## 1. Single-Pass Vectorized Expectation Evaluation

Evaluate all configured expectations (`expectations: list[dict]`) in **one** Catalyst projection pass without Python row loops or `@udf` functions:

Map each expectation (`name`, `column`, `checkType`) to a boolean `passed_expr` (`pyspark.sql.Column`) wrapped in `F.coalesce(..., F.lit(False))`:

- **`not_null`**: `F.col(col).isNotNull()`
- **`range`**: Combine `F.col(col).isNotNull()` with `(F.col(col) >= float_or_expr(minValue))` when `minValue` is set and `(F.col(col) <= float_or_expr(maxValue))` when `maxValue` is set.
- **`regex`**: `F.col(col).isNotNull() & F.col(col).cast("string").rlike(regexPattern)`
- **`in_set`**: Parse comma-separated `allowedValues` into trimmed tokens `allowed_list` and evaluate `F.col(col).isNotNull() & F.col(col).cast("string").isin(allowed_list)` (or typed literals matching `df.schema[col].dataType`).
- **`unique`**: On batch DataFrames (or inside a `foreachBatch` micro-batch), evaluate `F.col(col).isNotNull() & (F.count(F.lit(1)).over(Window.partitionBy(F.col(col))) == 1)`. Note that global uniqueness across unbounded Structured Streaming queries is unsupported via `Window.partitionBy` without micro-batch scoping or upstream watermark deduplication.
- **`sql_expr`**: `F.coalesce(F.expr(sqlExpression), F.lit(False))`

Construct a single `_failed_rules` array column collecting the names of violated expectations per row:

```python
violation_tags = [
    F.when(~F.coalesce(passed_expr, F.lit(False)), F.lit(rule_name))
    for rule_name, passed_expr in compiled_rules
]
checked_df = df.withColumn("_failed_rules", F.array_compact(F.array(*violation_tags)))
```

## 2. Failure Policies (`failurePolicy`) & DLQ Split (`ExecutionPlan.dlqSplitPoints`)

Inspect `failurePolicy` (`"route_to_dlq"`, `"drop_invalid"`, or `"fail_job"`) alongside `ExecutionPlan.dlqSplitPoints` (`mechanism == "expectation_violation"`):

### A. Dead-Letter Queue Split (`error` Handle Connected in `ExecutionPlan.dlqSplitPoints`)
- When the node appears in `ExecutionPlan.dlqSplitPoints`:
  1. If the node is a batch DataFrame listed in `ExecutionPlan.cachedNodeIds` (`reason: "dlq_split"`), call `checked_df = checked_df.cache()` once before splitting and unpersist after downstream sinks finish in `write_sinks()`.
  2. Emit the clean primary branch (`validTargetIds`) with helper columns removed:
     ```python
     valid_df = checked_df.filter(F.size(F.col("_failed_rules")) == 0).drop("_failed_rules")
     ```
  3. Emit the dead-letter branch (`errorTargetIds`) enriched with both `_failed_rules` (`array<string>`) and a human-readable `_error` summary (`string`):
     ```python
     error_df = checked_df.filter(F.size(F.col("_failed_rules")) > 0).withColumn(
         "_error",
         F.concat_ws(", ", F.col("_failed_rules")),
     )
     ```

### B. Drop Invalid Rows (`failurePolicy == "drop_invalid"` or Unconnected DLQ)
- When no `error` handle is connected and `failurePolicy` is `"drop_invalid"` (or `"route_to_dlq"` without a downstream error sink), filter to valid rows and drop `_failed_rules`:
  ```python
  valid_df = checked_df.filter(F.size(F.col("_failed_rules")) == 0).drop("_failed_rules")
  ```

### C. Fail Job on Violation (`failurePolicy == "fail_job"`)
- In batch mode (or lazy Catalyst evaluation), enforce zero violations without pulling unbounded rows onto the driver by projecting Spark SQL `F.assert_true(...)`:
  ```python
  asserted_df = checked_df.withColumn(
      "_dq_assert",
      F.assert_true(
          F.size(F.col("_failed_rules")) == 0,
          F.concat(
              F.lit("DataQualityAssert failed expectations: "),
              F.concat_ws(", ", F.col("_failed_rules")),
          ),
      ),
  ).drop("_dq_assert", "_failed_rules")
  ```
- When Spark executes a downstream write action on `asserted_df`, any row with `size(_failed_rules) > 0` immediately aborts the task with the violated expectation names.

## 3. Reference Implementation

```python
from typing import Any
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def _compile_expectation(exp: dict[str, Any], is_streaming: bool) -> tuple[str, Column]:
    """Compiles one expectation dictionary into (rule_name, passed_boolean_column)."""
    name = str(exp.get("name", "")).strip() or "unnamed_expectation"
    col_name = str(exp.get("column", "")).strip()
    check_type = str(exp.get("checkType", "not_null")).lower()

    if check_type == "sql_expr":
        sql_expr = str(exp.get("sqlExpression", "true")).strip() or "true"
        return name, F.coalesce(F.expr(sql_expr), F.lit(False))

    col_ref = F.col(col_name)
    if check_type == "not_null":
        return name, col_ref.isNotNull()

    if check_type == "range":
        cond = col_ref.isNotNull()
        min_val = str(exp.get("minValue", "")).strip()
        max_val = str(exp.get("maxValue", "")).strip()
        if min_val != "":
            cond = cond & (col_ref >= F.expr(min_val))
        if max_val != "":
            cond = cond & (col_ref <= F.expr(max_val))
        return name, cond

    if check_type == "regex":
        pattern = str(exp.get("regexPattern", "")).strip()
        return name, col_ref.isNotNull() & col_ref.cast("string").rlike(pattern)

    if check_type == "in_set":
        raw_allowed = str(exp.get("allowedValues", ""))
        allowed = [token.strip() for token in raw_allowed.split(",") if token.strip()]
        return name, col_ref.isNotNull() & col_ref.cast("string").isin(allowed)

    if check_type == "unique" and not is_streaming:
        win = Window.partitionBy(col_ref)
        return name, col_ref.isNotNull() & (F.count(F.lit(1)).over(win) == 1)

    return name, col_ref.isNotNull()


def evaluate_data_quality(
    df: DataFrame,
    step_cfg: dict[str, Any],
    emit_dlq_split: bool = False,
    cache_checked: bool = False,
) -> dict[str, DataFrame]:
    """Evaluates DataQualityAssert expectations in one vectorized pass."""
    expectations = step_cfg.get("expectations") or []
    failure_policy = str(step_cfg.get("failurePolicy", "route_to_dlq")).lower()

    if not expectations:
        return {"valid": df, "error": df.limit(0)}

    compiled = [_compile_expectation(exp, df.isStreaming) for exp in expectations]
    violation_tags = [
        F.when(~F.coalesce(passed_col, F.lit(False)), F.lit(rule_name))
        for rule_name, passed_col in compiled
    ]
    checked_df = df.withColumn("_failed_rules", F.array_compact(F.array(*violation_tags)))
    if cache_checked and not df.isStreaming:
        checked_df = checked_df.cache()

    if failure_policy == "fail_job" and not emit_dlq_split:
        asserted_df = checked_df.withColumn(
            "_dq_assert",
            F.assert_true(
                F.size(F.col("_failed_rules")) == 0,
                F.concat(
                    F.lit("DataQualityAssert failed: "),
                    F.concat_ws(", ", F.col("_failed_rules")),
                ),
            ),
        ).drop("_dq_assert", "_failed_rules")
        return {"valid": asserted_df}

    valid_df = checked_df.filter(F.size(F.col("_failed_rules")) == 0).drop("_failed_rules")
    if emit_dlq_split:
        error_df = checked_df.filter(F.size(F.col("_failed_rules")) > 0).withColumn(
            "_error",
            F.concat_ws(", ", F.col("_failed_rules")),
        )
        result: dict[str, DataFrame] = {"valid": valid_df, "error": error_df}
        if cache_checked and not df.isStreaming:
            result["__cached_checked__"] = checked_df
        return result

    return {"valid": valid_df}
```

## 4. Offline Unit Testing

Verify vectorized multi-rule DLQ routing and `fail_job` enforcement in `tests/test_pipeline.py`:

```python
import pytest
from pyspark.sql import SparkSession


def test_data_quality_assert_dlq_split_and_fail_job(spark: SparkSession) -> None:
    """Verifies vectorized expectation splitting and fail_job assertion behavior."""
    raw_df = spark.createDataFrame(
        [
            ("o1", 50.0, "USD", "a@example.com"),
            ("o2", -10.0, "INVALID", "bad-email"),
            ("o1", 25.0, "EUR", "b@example.com"),
        ],
        ["order_id", "amount", "currency", "email"],
    )
    cfg = {
        "failurePolicy": "route_to_dlq",
        "expectations": [
            {"name": "unique_order", "column": "order_id", "checkType": "unique"},
            {"name": "positive_amount", "column": "amount", "checkType": "range", "minValue": "0"},
            {"name": "valid_currency", "column": "currency", "checkType": "in_set", "allowedValues": "USD, EUR"},
            {"name": "valid_email", "column": "email", "checkType": "regex", "regexPattern": "^[^@]+@[^@]+$"},
        ],
    }

    split_out = evaluate_data_quality(raw_df, cfg, emit_dlq_split=True)
    assert split_out["valid"].count() == 0
    errors = {r["email"]: r["_failed_rules"] for r in split_out["error"].collect()}
    assert set(errors["bad-email"]) == {"positive_amount", "valid_currency", "valid_email"}
    assert errors["a@example.com"] == ["unique_order"]

    fail_out = evaluate_data_quality(
        raw_df,
        {**cfg, "failurePolicy": "fail_job"},
        emit_dlq_split=False,
    )
    with pytest.raises(Exception):
        fail_out["valid"].collect()
```
