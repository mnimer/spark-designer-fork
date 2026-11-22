---
name: transform-repartition-coalesce
description: Control DataFrame partition count and data distribution with coalesce, repartition, and repartitionByRange in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [RepartitionCoalesce]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html
  validated: true
---

# Partition Management Step (`RepartitionCoalesce`)

Use these rules when generating PySpark {{spark_version}} code for a `RepartitionCoalesce` step node inside `transform(dfs, config)`.

## 1. Choosing `coalesce`, `repartition`, or `repartitionByRange`

Select the exact DataFrame partitioning call from `operation` (`"coalesce"`, `"repartition"`, or `"repartitionByRange"`):

- **`coalesce` (`df.coalesce(num_partitions)`) — Narrow Partition Reduction (Zero Shuffle)**:
  - Merges existing executor partitions locally without a network shuffle.
  - Use `coalesce` immediately before file or object-store sinks when compacting small output files.
  - Do not place a small `coalesce(n)` upstream of expensive CPU or ML steps because Catalyst fuses narrow upstream stages into `n` tasks and throttles cluster parallelism.
  - `coalesce` cannot increase partition count and does not accept `partitionColumns`.
- **`repartition` (`df.repartition(num_partitions, *cols)`) — Full Shuffle (`RoundRobinPartitioning` or `HashPartitioning`)**:
  - Executes a full cluster shuffle to increase or rebalance parallelism across executors.
  - If `partitionColumns` is empty, call `df.repartition(num_partitions)` for uniform round-robin rebalancing across skewed partitions.
  - If `partitionColumns` is non-empty, call `df.repartition(num_partitions, *[F.col(c) for c in partition_columns])` (or `df.repartition(*[F.col(c) for c in partition_columns])` when `numPartitions` is omitted) to hash-partition rows by key before joins or partitioned sink writes.
- **`repartitionByRange` (`df.repartitionByRange(num_partitions, *cols)`) — Full Range Shuffle (`RangePartitioning`)**:
  - Requires at least one column in `partitionColumns`.
  - Samples keys and shuffles rows into ordered key ranges via `df.repartitionByRange(num_partitions, *[F.col(c) for c in partition_columns])`, optimizing Iceberg, Delta, and Parquet min/max file pruning.

## 2. Spark Connect Compatibility

- Never inspect or manipulate partitions through `df.rdd` (`df.rdd.getNumPartitions()`, `df.rdd.repartition()`).
- When verifying partition assignment in Spark Connect or tests, use Catalyst `F.spark_partition_id()`.

## 3. Reference Implementation (`src/pipeline.py`)

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_repartition_coalesce(
    df: DataFrame,
    operation: str,
    num_partitions: int | None = None,
    partition_columns: list[str] | None = None,
) -> DataFrame:
    """Applies coalesce, repartition, or repartitionByRange using DataFrame APIs."""
    cols = [F.col(c) for c in (partition_columns or []) if c.strip()]

    if operation == "coalesce":
        if not num_partitions or num_partitions < 1:
            raise ValueError("coalesce requires a positive numPartitions value.")
        return df.coalesce(num_partitions)

    if operation == "repartitionByRange":
        if not cols:
            raise ValueError("repartitionByRange requires at least one partition column.")
        if num_partitions and num_partitions > 0:
            return df.repartitionByRange(num_partitions, *cols)
        return df.repartitionByRange(*cols)

    # Default: hash or round-robin repartition
    if cols and num_partitions and num_partitions > 0:
        return df.repartition(num_partitions, *cols)
    if cols:
        return df.repartition(*cols)
    if num_partitions and num_partitions > 0:
        return df.repartition(num_partitions)
    raise ValueError("repartition requires numPartitions or partitionColumns.")
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Verify partition operations using `F.spark_partition_id()` without touching `df.rdd`:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.pipeline import apply_repartition_coalesce


def test_repartition_and_coalesce_offline(spark: SparkSession) -> None:
    """Verifies coalesce, hash repartition, and range repartition offline."""
    df = spark.range(0, 100).withColumn(
        "region",
        F.when((F.col("id") % 2) == 0, F.lit("US")).otherwise(F.lit("EU")),
    )

    rehashed_df = apply_repartition_coalesce(
        df,
        operation="repartition",
        num_partitions=4,
        partition_columns=["region"],
    )
    assert rehashed_df.count() == 100

    ranged_df = apply_repartition_coalesce(
        rehashed_df,
        operation="repartitionByRange",
        num_partitions=3,
        partition_columns=["id"],
    )
    coalesced_df = apply_repartition_coalesce(
        ranged_df,
        operation="coalesce",
        num_partitions=1,
    )
    distinct_pids = coalesced_df.select(F.spark_partition_id()).distinct().count()
    assert distinct_pids == 1
```
