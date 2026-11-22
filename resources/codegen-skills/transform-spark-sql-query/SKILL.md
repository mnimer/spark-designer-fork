---
name: transform-spark-sql-query
description: Register upstream DataFrames as temporary views and execute parameterized Catalyst SQL queries in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [SparkSqlQuery]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html
  validated: true
---

# Spark SQL Query Step (`SparkSqlQuery`)

Use these rules when generating PySpark {{spark_version}} code for a `SparkSqlQuery` step node inside `transform(dfs, config)`.

## 1. Temporary View Registration & Identifier Safety

`SparkSqlQuery` accepts `n` upstream DataFrame inputs (`inputArity: 'n'`) and maps each upstream input to a session-scoped temporary view before calling `spark.sql(...)`:

- Obtain the active `SparkSession` from the first upstream DataFrame via `spark = first_df.sparkSession` so `transform(dfs, config)` preserves its pure `(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]` signature.
- Sanitize and validate every view name from `inputViewAliases` against `^[A-Za-z_][A-Za-z0-9_]*$` before calling `df.createOrReplaceTempView(view_name)`. If `inputViewAliases` omits an upstream input at index `i`, register it as `f"input_{i}"`.
- `createOrReplaceTempView` works identically on local PySpark, Dataproc Clusters, GCP Serverless Spark, and Spark Connect sessions without touching JVM internals.

## 2. Spark 4.x Parameterized SQL (`args={...}`)

- When `sqlParameters` (`Array<{ key: string, value: string }>`) or runtime `config` values supply query parameters, bind them through Spark 4.x named parameter markers (`:param_name`) using `spark.sql(sql_query, args=sql_args)`.
- Never interpolate untrusted runtime strings into SQL text with Python f-strings or `%` formatting when `spark.sql(query, args={...})` applies.
- Strip any leading `:` from configured `sqlParameters[].key` entries when building the `args` dictionary so `:region` in SQL binds cleanly to `args={"region": value}`.

## 3. Dead-Letter Queue (`error` Handle) Split

When `ExecutionPlan.dlqSplitPoints` includes this `SparkSqlQuery` node (`mechanism: "try_except_error_col"`):

1. Evaluate fallible type conversions or arithmetic inside SQL using ANSI-safe `try_cast(...)`, `try_to_timestamp(...)`, `try_to_number(...)`, or `try_divide(...)` and project a diagnostic `_error` string column when an expected output is `NULL` while the raw source value was non-null (or wrap execution error capture into `_error`).
2. If `ExecutionPlan.cachedNodeIds` includes this node, call `.cache()` once on the combined result DataFrame before splitting so Spark does not execute the SQL query twice.
3. Route rows where `F.col("_error").isNull()` (dropping `_error`) to the primary valid output key, and route rows where `F.col("_error").isNotNull()` to the error DLQ branch key.

## 4. Reference Implementation (`src/pipeline.py`)

```python
import re
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_VIEW_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sanitize_view_name(raw_name: str, fallback_index: int) -> str:
    """Validates or normalizes a temporary view identifier for createOrReplaceTempView."""
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", raw_name.strip())
    if not cleaned or not VALID_VIEW_IDENT.match(cleaned):
        return f"input_{fallback_index}"
    return cleaned


def run_spark_sql_step(
    upstream_dfs: list[DataFrame],
    view_aliases: list[dict[str, Any]],
    sql_query: str,
    sql_parameters: dict[str, Any] | None = None,
) -> DataFrame:
    """Registers upstream DataFrames as temp views and runs parameterized spark.sql."""
    if not upstream_dfs:
        raise ValueError("SparkSqlQuery requires at least one upstream DataFrame.")

    spark = upstream_dfs[0].sparkSession
    alias_by_index = {
        int(entry["inputIndex"]): str(entry["viewName"])
        for entry in view_aliases
        if "inputIndex" in entry and str(entry.get("viewName", "")).strip()
    }

    for idx, df in enumerate(upstream_dfs):
        raw_alias = alias_by_index.get(idx, f"input_{idx}")
        view_name = sanitize_view_name(raw_alias, idx)
        df.createOrReplaceTempView(view_name)

    normalized_args = {
        k.lstrip(":"): v for k, v in (sql_parameters or {}).items() if k.lstrip(":")
    }
    if normalized_args:
        return spark.sql(sql_query, args=normalized_args)
    return spark.sql(sql_query)


def split_sql_dlq(result_df: DataFrame, cache_before_split: bool = True) -> tuple[DataFrame, DataFrame]:
    """Splits a SQL result containing an _error column into valid and DLQ DataFrames."""
    working_df = result_df.cache() if cache_before_split else result_df
    if "_error" not in working_df.columns:
        working_df = working_df.withColumn("_error", F.lit(None).cast("string"))
    valid_df = working_df.filter(F.col("_error").isNull()).drop("_error")
    error_df = working_df.filter(F.col("_error").isNotNull())
    return valid_df, error_df
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Verify multi-input view registration, `:param` SQL binding, and DLQ routing using in-memory DataFrames:

```python
from pyspark.sql import SparkSession
from src.pipeline import run_spark_sql_step, split_sql_dlq


def test_spark_sql_query_parameterized_join_and_dlq(spark: SparkSession) -> None:
    """Verifies temp view registration, named SQL parameters, and DLQ split offline."""
    orders_df = spark.createDataFrame(
        [("o1", "u1", "42.50", "US"), ("o2", "u1", "bad_num", "US"), ("o3", "u2", "10.00", "EU")],
        ["order_id", "user_id", "raw_amount", "region"],
    )
    users_df = spark.createDataFrame([("u1", "Ada"), ("u2", "Grace")], ["user_id", "user_name"])

    query = """
        SELECT
            o.order_id,
            u.user_name,
            TRY_CAST(o.raw_amount AS DOUBLE) AS amount,
            CASE
                WHEN o.raw_amount IS NOT NULL AND TRY_CAST(o.raw_amount AS DOUBLE) IS NULL
                THEN CONCAT('Invalid DOUBLE in raw_amount: ', o.raw_amount)
                ELSE NULL
            END AS _error
        FROM orders_view o
        INNER JOIN users_view u ON o.user_id = u.user_id
        WHERE o.region = :target_region
    """
    result_df = run_spark_sql_step(
        upstream_dfs=[orders_df, users_df],
        view_aliases=[
            {"inputIndex": 0, "viewName": "orders_view"},
            {"inputIndex": 1, "viewName": "users_view"},
        ],
        sql_query=query,
        sql_parameters={"target_region": "US"},
    )
    valid_df, error_df = split_sql_dlq(result_df, cache_before_split=False)

    assert valid_df.count() == 1
    assert valid_df.first()["amount"] == 42.5
    assert error_df.count() == 1
```
