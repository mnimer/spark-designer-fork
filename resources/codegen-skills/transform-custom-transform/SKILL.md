---
name: transform-custom-transform
description: Apply modular prompt-driven DataFrame.transform functions using native Catalyst expressions and Spark Connect safe APIs in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [CustomTransform]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html
  validated: true
---

# Custom DataFrame Transformation Step (`CustomTransform`)

Use these rules when generating PySpark {{spark_version}} code for a `CustomTransform` step node inside `transform(dfs, config)`.

## 1. Modular `df.transform(...)` Composition & Catalyst First

- Encapsulate the user's `system_instructions` logic in a dedicated helper function (`def <functionName>(df: DataFrame) -> DataFrame:`) and invoke it via `df.transform(<functionName>)`.
- Prefer native Catalyst `pyspark.sql.functions` (`F.*`), SQL expressions (`F.expr(...)`), and single-pass `df.withColumns({...})` over Python UDFs so Catalyst predicate pushdown, column pruning, and Tungsten codegen remain active.
- When `preserveExistingColumns` is `True` (default), enrich the upstream DataFrame via `df.withColumns({...})`. When `preserveExistingColumns` is `False`, return only the target projection via `df.select(...)` matching `outputSchemaHint` when provided.

## 2. Strict Spark Connect & Serverless Spark 4.x Purity

- Never call `df.rdd`, `spark.sparkContext`, `spark._jsc`, `spark._jvm`, or `sc.broadcast()` inside `CustomTransform` helpers. Every `CustomTransform` function must run identically on local PySpark, Dataproc Clusters, GCP Serverless Spark, and remote Spark Connect sessions.
- Never call `.collect()`, `.toPandas()`, `.take()`, or eager `.count()` inside `CustomTransform` logic. Keep all operations lazy DataFrame transformations.
- If the prompt logic cannot be expressed with Catalyst `pyspark.sql.functions`, use vectorized `@pandas_udf` or `df.mapInPandas(...)` rather than row-by-row pickled `@udf`.

## 3. Dead-Letter Queue (`error` Handle) Split (`try_except_error_col`)

When `ExecutionPlan.dlqSplitPoints` includes this `CustomTransform` node (`mechanism: "try_except_error_col"`):

1. Express fallible conversions with Spark 4.x safe expressions (`F.expr("try_cast(col AS DOUBLE)")`, `F.try_divide(...)`, `F.expr("try_to_timestamp(ts_str)")`) or vectorized `try/except` batch error capture and populate a nullable `_error: string` column describing any row failure.
2. Cache the intermediate DataFrame when listed in `ExecutionPlan.cachedNodeIds` before filtering both branches.
3. Emit `valid_df = result_df.filter(F.col("_error").isNull()).drop("_error")` to the valid output handle and `error_df = result_df.filter(F.col("_error").isNotNull())` to the error DLQ branch.

## 4. Reference Implementation (`src/pipeline.py`)

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def custom_transform(df: DataFrame) -> DataFrame:
    """Applies modular Catalyst enrichment with Spark 4.x single-pass withColumns."""
    return df.withColumns(
        {
            "parsed_total": F.expr("try_cast(raw_total AS DOUBLE)"),
            "normalized_email": F.lower(F.trim(F.col("email"))),
            "_error": F.when(
                F.col("raw_total").isNotNull()
                & F.expr("try_cast(raw_total AS DOUBLE)").isNull(),
                F.concat(F.lit("Invalid numeric raw_total: "), F.col("raw_total")),
            ).otherwise(F.lit(None).cast("string")),
        }
    )


def run_custom_transform_step(
    df: DataFrame,
    enable_dlq: bool = False,
    cache_before_split: bool = True,
) -> DataFrame | tuple[DataFrame, DataFrame]:
    """Executes custom_transform via df.transform() and splits DLQ rows when wired."""
    transformed_df = df.transform(custom_transform)
    if not enable_dlq:
        return transformed_df.drop("_error")

    working_df = transformed_df.cache() if cache_before_split else transformed_df
    valid_df = working_df.filter(F.col("_error").isNull()).drop("_error")
    error_df = working_df.filter(F.col("_error").isNotNull())
    return valid_df, error_df
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

```python
from pyspark.sql import SparkSession
from src.pipeline import run_custom_transform_step


def test_custom_transform_catalyst_and_dlq_offline(spark: SparkSession) -> None:
    """Verifies df.transform() execution and _error DLQ routing with in-memory data."""
    df = spark.createDataFrame(
        [("1", "  Ada@Example.COM ", "99.5"), ("2", "grace@example.com", "N/A")],
        ["id", "email", "raw_total"],
    )
    valid_df, error_df = run_custom_transform_step(
        df,
        enable_dlq=True,
        cache_before_split=False,
    )
    row = valid_df.first()
    assert valid_df.count() == 1
    assert row["normalized_email"] == "ada@example.com"
    assert row["parsed_total"] == 99.5
    assert error_df.count() == 1
```
