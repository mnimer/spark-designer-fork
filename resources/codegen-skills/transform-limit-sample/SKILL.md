---
name: transform-limit-sample
description: Bound DataFrame row counts with lazy limit or sample rows deterministically with sample in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [LimitSample]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.limit.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sample.html
  validated: true
---

# Row Limit & Sampling (`LimitSample`)

Use these rules when generating PySpark {{spark_version}} code for a `LimitSample` step inside `transform(dfs, config)`.

## 1. Lazy Catalyst Transformations Only

- Always keep row bounding and sampling lazy inside `transform(dfs, config)` using `df.limit(n)` or `df.sample(...)`.
- Never call eager driver collection methods such as `df.take(n)`, `df.head(n)`, or `df.collect()` to implement a `LimitSample` step.

## 2. Modes (`mode`)

Read `mode` (`"limit"` or `"sample"`, default `"limit"`) from node properties:

### A. Row Limit (`mode == "limit"`)
- Read `limitCount` (positive integer, default `1000`).
- Validate `n = int(limit_count)` with `n >= 1` and return `df.limit(n)`.
- Catalyst executes `LocalLimit(n)` on each upstream partition before applying `GlobalLimit(n)`, minimizing shuffle data transfer.
- Note that `df.limit(n)` without an upstream `OrderBy` step returns an arbitrary subset of `n` rows. Pair an upstream `OrderBy` step when top-N ranking is required, or use the dedicated `TopN` step.

### B. Statistical Row Sampling (`mode == "sample"`)
- Read `fraction` (`float` in `[0.0, 1.0]`, default `0.1`), `withReplacement` (`bool`, default `False`), and optional integer `seed`.
- When `seed` is provided (`seed is not None` and `str(seed).strip() != ""`), pass `seed=int(seed)` so unit tests and development runs are deterministic:
  ```python
  sampled_df = df.sample(
      withReplacement=with_replacement,
      fraction=fraction,
      seed=int(seed),
  )
  ```
- When `seed` is omitted or blank, pass `df.sample(withReplacement=with_replacement, fraction=fraction)` so Spark selects a fresh random seed at runtime.
- `df.sample(...)` evaluates row-by-row inside each partition without triggering a shuffle and works on both batch and Structured Streaming DataFrames.

## 3. Reference Implementation

```python
from typing import Any
from pyspark.sql import DataFrame


def apply_limit_sample(df: DataFrame, step_cfg: dict[str, Any]) -> DataFrame:
    """Applies lazy row limiting or fractional sampling to a Spark DataFrame."""
    mode = str(step_cfg.get("mode", "limit")).lower()

    if mode == "sample":
        fraction = float(step_cfg.get("fraction", 0.1))
        if fraction < 0.0 or fraction > 1.0:
            raise ValueError(f"Sample fraction must be between 0.0 and 1.0, got {fraction}")
        with_replacement = bool(step_cfg.get("withReplacement", False))
        raw_seed = step_cfg.get("seed")
        if raw_seed is not None and str(raw_seed).strip() != "":
            return df.sample(
                withReplacement=with_replacement,
                fraction=fraction,
                seed=int(raw_seed),
            )
        return df.sample(withReplacement=with_replacement, fraction=fraction)

    limit_count = int(step_cfg.get("limitCount", 1000))
    if limit_count < 1:
        raise ValueError(f"Limit count must be a positive integer, got {limit_count}")
    return df.limit(limit_count)
```

## 4. Offline Unit Testing

Verify `df.limit(n)` and seeded `df.sample(...)` determinism offline in `tests/test_pipeline.py`:

```python
from pyspark.sql import SparkSession


def test_limit_and_seeded_sample(spark: SparkSession) -> None:
    """Verifies exact row bounding in limit mode and reproducible output in sample mode."""
    input_df = spark.range(0, 100).toDF("id")

    limited_df = apply_limit_sample(input_df, {"mode": "limit", "limitCount": 5})
    assert limited_df.count() == 5

    sample_cfg = {
        "mode": "sample",
        "fraction": 0.25,
        "withReplacement": False,
        "seed": 42,
    }
    first_run = [r["id"] for r in apply_limit_sample(input_df, sample_cfg).collect()]
    second_run = [r["id"] for r in apply_limit_sample(input_df, sample_cfg).collect()]
    assert first_run == second_run
```
