---
name: sdp-expectations
description: Optional declarative data quality expectations for Spark Declarative Pipelines using sdp_extensions.expectations (@dq.expect, @dq.expect_or_drop, @dq.expect_or_fail, @dq.expect_all, @dq.expect_all_or_drop, @dq.expect_all_or_fail).
metadata:
  kind: crosscutting
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [sdp]
  transforms: []
  preloadWhen: [sdp_expectations]
  sources:
    - https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html
  validated: true
---

# Optional SDP Expectations (`sdp_extensions.expectations`, `V2.8`)

Follow these rules whenever `## SDP Execution Plan` sets `qualityMode: "expectations"` (`usesSdpExpectations: true`).

## 1. Imports & Dependencies

Import dataset decorators as `dp` from `pyspark` and expectation decorators as `dq` from `sdp_extensions`:

```python
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from sdp_extensions import expectations as dq
```

- Add `sdp-extensions` to `requirements.txt`.
- Use only public exports from `sdp_extensions.expectations`: `dq.expect`, `dq.expect_or_drop`, `dq.expect_or_fail`, `dq.expect_all`, `dq.expect_all_or_drop`, `dq.expect_all_or_fail`, `dq.ViolationAction`.
- Never use `@dp.expect*` or import `dlt`. Those Databricks APIs do not exist in OSS Apache Spark 4.2.

## 2. Mandatory Decorator Ordering & Placement Rules

1. **Always place `@dq.expect*` BELOW `@dp.table`, `@dp.materialized_view`, or `@dp.temporary_view`**:
   - SDP dataset decorators return `None`. Placing `@dq.expect*` above `@dp.*` raises `PySparkTypeError: [DECORATOR_ARGUMENT_NOT_CALLABLE]`.
2. **Never decorate `@dp.append_flow` or standalone helper functions**:
   - Apply `@dq.expect*` only to `@dp.table`, `@dp.materialized_view`, or `@dp.temporary_view` functions.
3. **`FAIL` evaluates before `DROP`**:
   - When combining `drop` and `fail` on the same dataset for a nullable column, allow `NULL` in the `fail` constraint (for example `"age IS NULL OR age > 0"`) or drop nulls in an upstream `@dp.temporary_view`.
4. **Strict boolean expressions and `NULL` semantics**:
   - Every constraint must evaluate to a strict boolean (`"amount > 0"`, `"id IS NOT NULL"`). `NULL` evaluates to `False` (a violation). For optional columns, explicitly allow `NULL` (`"discount IS NULL OR discount >= 0"`).

## 3. Mapping `DataQualityAssert` Failure Policies

- `warn` / `log`: `@dq.expect_all(RULES)` (default `action="warn"`, observes violation counts without dropping rows).
- `drop` / `drop_rows`: `@dq.expect_all_or_drop(RULES)` (drops failing rows and records `<name>_violations` via `DataFrame.observe()`).
- `fail_job` / `fail`: `@dq.expect_all_or_fail(RULES)` (aborts query immediately on violation with `[SDP_EXPECTATION_VIOLATED]`).
- `route_to_dlq` (or when `quarantineDatasetName` is planned): Declare an upstream `@dp.temporary_view` with `@dq.expect_all(RULES)` computing `_is_quarantined`, then split into clean and `<dataset>_quarantine` tables:

```python
RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "positive_amount": "amount > 0",
}
QUARANTINE_EXPR = (
    f"NOT coalesce({' AND '.join(f'({r})' for r in RULES.values())}, false)"
)


@dp.temporary_view(name="staged_orders")
@dq.expect_all(RULES)
def staged_orders() -> DataFrame:
    return spark.readStream.table("bronze_orders").withColumn(
        "_is_quarantined", F.expr(QUARANTINE_EXPR)
    )


@dp.table(name="clean_orders", format="iceberg")
def clean_orders() -> DataFrame:
    return (
        spark.readStream.table("staged_orders")
        .filter(~F.col("_is_quarantined"))
        .drop("_is_quarantined")
    )


@dp.table(name="clean_orders_quarantine", format="iceberg")
def clean_orders_quarantine() -> DataFrame:
    return (
        spark.readStream.table("staged_orders")
        .filter(F.col("_is_quarantined"))
        .drop("_is_quarantined")
    )
```

## 4. Direct Single-Dataset Example (`drop` or `fail_job`)

```python
@dp.table(name="silver_orders", format="iceberg")
@dq.expect_all_or_drop({
    "valid_order_id": "order_id IS NOT NULL",
    "positive_amount": "amount > 0",
})
def silver_orders() -> DataFrame:
    return spark.readStream.table("bronze_orders")
```
