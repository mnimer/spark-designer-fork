---
name: sdp-quality-quarantine
description: Filter-based clean and quarantine dataset patterns for DataQualityAssert and corrupt-record routing in Spark Declarative Pipelines without Databricks expectations decorators.
metadata:
  kind: crosscutting
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [sdp]
  transforms: [DataQualityAssert]
  preloadWhen: [sdp_quarantine]
  sources:
    - https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
  validated: true
---

# Spark Declarative Pipelines Data Quality & Quarantine Datasets (`sdp-quality-quarantine`)

Follow these rules whenever a Spark Declarative Pipelines (`paradigm: sdp`) graph includes `DataQualityAssert` (or schema/payload validation with quarantine routing, `sdp_quarantine`).

## 1. Zero `@dp.expect*` Decorators in OSS Apache Spark 4.2

OSS Apache Spark 4.2 (`pyspark.pipelines`) does **not** include `@dp.expect`, `@dp.expect_or_drop`, `@dp.expect_or_fail`, `@dp.expect_all*`, or SQL `CONSTRAINT ... EXPECT` clauses. Never invent expectation decorators on `dp` or import `dlt`.

Instead, implement row-level validation via pure vectorized Catalyst predicate helpers in `src/transforms.py` and expose both a clean dataset and a `<dataset_name>_quarantine` dataset in `transformations/*.py`.

## 2. Pure Stateless Validation Helpers in `src/transforms.py`

Remember that SDP dataset functions forbid `.schema`, `.columns`, and `.cache()` (`ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION`). Build validation helpers using pure `pyspark.sql.functions` expressions only:

```python
# Inside src/transforms.py
from typing import Any
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def compile_quality_rule(rule: dict[str, Any]) -> tuple[str, Column]:
    """Compiles one DataQualityAssert expectation into (rule_name, passed_boolean_expr)."""
    name = str(rule.get("name", "")).strip() or "unnamed_rule"
    col_name = str(rule.get("column", "")).strip()
    check_type = str(rule.get("checkType", "not_null")).lower()

    if check_type == "sql_expr":
        expr_sql = str(rule.get("sqlExpression", "true")).strip() or "true"
        return name, F.coalesce(F.expr(expr_sql), F.lit(False))

    col_ref = F.col(col_name)
    if check_type == "range":
        cond = col_ref.isNotNull()
        min_val = str(rule.get("minValue", "")).strip()
        max_val = str(rule.get("maxValue", "")).strip()
        if min_val != "":
            cond = cond & (col_ref >= F.expr(min_val))
        if max_val != "":
            cond = cond & (col_ref <= F.expr(max_val))
        return name, cond

    if check_type == "regex":
        pattern = str(rule.get("regexPattern", "")).strip()
        return name, col_ref.isNotNull() & col_ref.cast("string").rlike(pattern)

    if check_type == "in_set":
        allowed = [
            token.strip()
            for token in str(rule.get("allowedValues", "")).split(",")
            if token.strip()
        ]
        return name, col_ref.isNotNull() & col_ref.cast("string").isin(allowed)

    return name, col_ref.isNotNull()


def annotate_quality_failures(df: DataFrame, rules: list[dict[str, Any]]) -> DataFrame:
    """Adds a _failed_rules array<string> column listing violated expectation names."""
    if not rules:
        return df.withColumns({"_failed_rules": F.array().cast("array<string>")})
    tags = [
        F.when(~F.coalesce(passed_col, F.lit(False)), F.lit(rule_name))
        for rule_name, passed_col in (compile_quality_rule(r) for r in rules)
    ]
    return df.withColumns({"_failed_rules": F.array_compact(F.array(*tags))})


def filter_clean_records(df: DataFrame, rules: list[dict[str, Any]]) -> DataFrame:
    """Returns only rows passing all quality rules with helper columns removed."""
    return (
        annotate_quality_failures(df, rules)
        .filter(F.size(F.col("_failed_rules")) == 0)
        .drop("_failed_rules")
    )


def filter_quarantine_records(df: DataFrame, rules: list[dict[str, Any]]) -> DataFrame:
    """Returns rows violating one or more quality rules with _failed_rules and _error details."""
    return (
        annotate_quality_failures(df, rules)
        .filter(F.size(F.col("_failed_rules")) > 0)
        .withColumns({"_error": F.concat_ws(", ", F.col("_failed_rules"))})
    )
```

## 3. Declaring Clean & `<dataset>_quarantine` Datasets in `transformations/*.py`

1. **Shared Upstream Source or Temporary View**:
   - Read the upstream dataset (`spark.readStream.table("<upstream>")` for streaming flows or `spark.read.table("<upstream>")` for batch materialized views).
2. **Clean Dataset (`<dataset_name>`)**:
   - Register `@dp.table(name="<dataset_name>", format="iceberg")` (when `isStreaming: true`) or `@dp.materialized_view(name="<dataset_name>", format="iceberg")` (when `isStreaming: false`) returning `filter_clean_records(upstream_df, QUALITY_RULES)` (or `F.assert_true(...)` when `failurePolicy == "fail_job"` and no quarantine dataset is planned).
3. **Quarantine Dataset (`<quarantineDatasetName>`, default `<dataset_name>_quarantine`)**:
   - When `failurePolicy` is `"route_to_dlq"` (or `quarantineDatasetName` is specified in `## SDP Execution Plan`), register `@dp.table(name="<quarantineDatasetName>", format="iceberg")` (streaming) or `@dp.materialized_view(name="<quarantineDatasetName>", format="iceberg")` (batch) returning `filter_quarantine_records(upstream_df, QUALITY_RULES)`.
   - If the `error` handle on the canvas connects to an explicit downstream quarantine table or `dp.create_sink` node, feed that downstream dataset from `<quarantineDatasetName>`.

## 4. Offline Unit Testing (`tests/test_transforms.py`)

Test `filter_clean_records` and `filter_quarantine_records` in `tests/test_transforms.py` with in-memory `spark.createDataFrame(...)` inputs asserting:
- Clean rows exclude invalid records and drop `_failed_rules`.
- Quarantine rows capture every violated rule name in `_failed_rules` and format `_error`.
