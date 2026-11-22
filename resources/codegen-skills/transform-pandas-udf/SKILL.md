---
name: transform-pandas-udf
description: Execute vectorized Apache Arrow transformations with pandas_udf, mapInPandas, and mapInArrow in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [PandasUdfTransform]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html
  validated: true
---

# Vectorized Pandas & Apache Arrow Step (`PandasUdfTransform`)

Use these rules when generating PySpark {{spark_version}} code for a `PandasUdfTransform` step node inside `transform(dfs, config)`.

## 1. Vectorized Execution Modes (`udfMode`)

Always use Apache Arrow vectorized APIs and never fall back to row-by-row pickled `@udf`:

- **`pandas_udf` (Series-to-Series or Iterator-of-Series)**:
  - Decorate with `@pandas_udf(return_schema_ddl)` from `pyspark.sql.functions`.
  - Use `pd.Series -> pd.Series` for stateless column operations, or `Iterator[pd.Series] -> Iterator[pd.Series]` when amortizing per-partition state setup.
  - Attach the output via `df.withColumn(output_column, fn(*[F.col(c) for c in input_columns]))`.
- **`mapInPandas` (`Iterator[pd.DataFrame] -> Iterator[pd.DataFrame]`)**:
  - Call `df.mapInPandas(batch_fn, schema=return_schema_ddl)` when reshaping columns, changing row cardinality, or sharing batch resources across multiple columns.
- **`mapInArrow` (`Iterator[pa.RecordBatch] -> Iterator[pa.RecordBatch]`)**:
  - Call `df.mapInArrow(arrow_batch_fn, schema=return_schema_ddl)` for zero-copy `pyarrow.compute` batch processing without Pandas conversion overhead.

## 2. Batch Sizing & Python Dependencies (`requirements.txt`)

- Include `pandas>=2.0.0` and `pyarrow>=15.0.0` in `requirements.txt` whenever `PandasUdfTransform` appears in the pipeline.
- If `maxRecordsPerBatch` is configured as a positive integer, apply `df.sparkSession.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(max_records_per_batch))` so Arrow batches stay within executor memory bounds.
- Keep `returnSchemaDDL` explicit (`"double"`, `"struct<score: double, label: string>"`, or full table DDL `"id string, normalized double"`) so Spark validates output types without driver inference.

## 3. Dead-Letter Queue (`error` Handle) Split (`try_except_error_col`)

When `ExecutionPlan.dlqSplitPoints` includes this `PandasUdfTransform` node (`mechanism: "try_except_error_col"`):

1. Catch batch or element exceptions inside the vectorized Arrow/Pandas function and populate a nullable `_error: string` column alongside output columns instead of crashing the executor task.
2. When `udfMode == "pandas_udf"` with DLQ active, return `struct<value: <returnType>, _error: string>`, promote `result.value` to `outputColumn` and `result._error` to `_error`, and drop the temporary struct.
3. Cache the combined DataFrame when listed in `ExecutionPlan.cachedNodeIds`, then emit `valid_df = combined_df.filter(F.col("_error").isNull()).drop("_error")` and `error_df = combined_df.filter(F.col("_error").isNotNull())`.

## 4. Reference Implementation (`src/pipeline.py`)

```python
from typing import Iterator
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf


@pandas_udf("double")
def zscore_series(values: pd.Series) -> pd.Series:
    """Computes a vectorized z-score over each Arrow batch Series."""
    std = values.std(ddof=0)
    if pd.isna(std) or std == 0.0:
        return pd.Series([0.0] * len(values), index=values.index, dtype="float64")
    return (values - values.mean()) / std


def apply_map_in_pandas_with_dlq(
    df: DataFrame,
    return_schema_ddl: str,
    max_records_per_batch: int | None = None,
    cache_before_split: bool = True,
) -> tuple[DataFrame, DataFrame]:
    """Runs mapInPandas with per-row error capture into _error for DLQ routing."""
    if max_records_per_batch and max_records_per_batch > 0:
        df.sparkSession.conf.set(
            "spark.sql.execution.arrow.maxRecordsPerBatch",
            str(max_records_per_batch),
        )

    dlq_schema = f"{return_schema_ddl}, _error string"

    def transform_batches(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for pdf in pdf_iter:
            parsed_vals: list[float | None] = []
            errors: list[str | None] = []
            for raw in pdf["raw_metric"]:
                try:
                    parsed_vals.append(float(raw) * 100.0)
                    errors.append(None)
                except (TypeError, ValueError) as exc:
                    parsed_vals.append(None)
                    errors.append(f"PandasUdfTransform error: {exc}")
            out_pdf = pdf[["id"]].copy()
            out_pdf["scaled_metric"] = parsed_vals
            out_pdf["_error"] = errors
            yield out_pdf

    combined_df = df.mapInPandas(transform_batches, schema=dlq_schema)
    if cache_before_split:
        combined_df = combined_df.cache()
    valid_df = combined_df.filter(F.col("_error").isNull()).drop("_error")
    error_df = combined_df.filter(F.col("_error").isNotNull())
    return valid_df, error_df


def apply_map_in_arrow(df: DataFrame, return_schema_ddl: str) -> DataFrame:
    """Runs zero-copy pyarrow RecordBatch processing via mapInArrow."""
    def arrow_batches(batch_iter: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
        for batch in batch_iter:
            upper_col = pc.utf8_upper(batch.column("code"))
            yield pa.RecordBatch.from_arrays(
                [batch.column("id"), upper_col],
                names=["id", "code_upper"],
            )

    return df.mapInArrow(arrow_batches, schema=return_schema_ddl)
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.pipeline import apply_map_in_pandas_with_dlq, zscore_series


def test_pandas_udf_and_dlq_split_offline(spark: SparkSession) -> None:
    """Verifies @pandas_udf Series execution and mapInPandas DLQ capture offline."""
    df = spark.createDataFrame(
        [("r1", "1.25", 10.0), ("r2", "invalid", 20.0)],
        ["id", "raw_metric", "score"],
    )
    scored_df = df.withColumn("z_score", zscore_series(F.col("score")))
    assert "z_score" in scored_df.columns

    valid_df, error_df = apply_map_in_pandas_with_dlq(
        df.select("id", "raw_metric"),
        return_schema_ddl="id string, scaled_metric double",
        max_records_per_batch=1024,
        cache_before_split=False,
    )
    assert valid_df.count() == 1
    assert valid_df.first()["scaled_metric"] == 125.0
    assert error_df.count() == 1
```
