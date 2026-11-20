---
name: core-catalyst-optimization
description: PySpark 4.x Catalyst optimizer rules, broadcast joins, single-pass projections, vectorized Arrow UDFs, Spark Connect compatibility, and deterministic fan-out caching.
metadata:
  kind: crosscutting
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: []
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/sql-performance-tuning.html
  validated: true
---

# PySpark 4.x Catalyst Optimization & Execution Rules

Follow these Catalyst optimization and Spark 4.x execution rules in all generated pipeline code.

## 1. Broadcast Joins (`F.broadcast`)

When a `Join` node sets `broadcastRight: true` (or `ExecutionPlan.nodes[nodeId].joinMode` / node properties specify a broadcast lookup join), wrap the right-hand DataFrame in `F.broadcast(right_df)` so Spark executes a `BroadcastHashJoin` instead of a full cluster `SortMergeJoin` shuffle:

```python
from pyspark.sql import functions as F

joined_df = left_df.join(
    F.broadcast(right_df),
    on=join_condition,
    how=join_type,
)
```

## 2. No Unbounded Driver Collection

Never pull unbounded or large production DataFrames onto the driver node:

- **Banned in production pipeline paths**: `.toPandas()`, `.collect()`, `.take()`, `.head()`, `.tail()`, and eager `.count()` for empty-check branching.
- Keep all data transformations inside lazy Catalyst `DataFrame` operations until `write_sinks()` executes.
- Driver collection (`.collect()`) is permitted only inside `tests/test_pipeline.py` on small synthetic test fixtures.

## 3. No Terminal Actions in Loops & Flat Multi-Input Unions

- Never trigger terminal actions (`.write`, `.count()`, `.collect()`, `.first()`) inside `for` or `while` loops over data values.
- When combining multiple upstream DataFrames (`UnionByName`), use `functools.reduce` with `DataFrame.unionByName` in a single flat pass rather than iterative re-binding loops:

```python
import functools
from pyspark.sql import DataFrame

unioned_df = functools.reduce(
    lambda left, right: left.unionByName(right, allowMissingColumns=True),
    upstream_dfs,
)
```

## 4. Single-Pass Column Projections (`df.withColumns`)

Never chain `.withColumn()` inside a Python `for` loop. Repeated `.withColumn()` calls build deeply nested `Project` plans in Catalyst and degrade analyzer performance. Always pass a dictionary of column expressions to `df.withColumns({...})` in a single pass:

```python
from pyspark.sql import functions as F

# Correct: single-pass Catalyst projection
enriched_df = df.withColumns(
    {
        "event_date": F.to_date(F.col("event_timestamp")),
        "net_amount": F.col("gross_amount") - F.col("discount_amount"),
        "currency_upper": F.upper(F.col("currency")),
    }
)
```

For bulk column renames, use `df.withColumnsRenamed({"old_a": "new_a", "old_b": "new_b"})` in a single call.

## 5. Native Catalyst Functions & Vectorized Arrow UDFs Over Row UDFs

- **First choice**: Built-in `pyspark.sql.functions` (`F.*`) and SQL expressions (`F.expr(...)`), which compile directly into Catalyst tungsten bytecode and run inside the JVM without Python serialization overhead.
- **Second choice (custom Python / ML logic)**: Vectorized Apache Arrow APIs:
  - `@pandas_udf` (`from pyspark.sql.functions import pandas_udf`) for vectorized scalar or grouped series transformations.
  - `df.mapInPandas(fn, schema=...)` or `df.mapInArrow(fn, schema=...)` for batched partition processing (such as SDK connection pooling or ML inference).
- **Avoid**: Row-at-a-time `@udf` (pickled Python UDFs) and legacy `.rdd` APIs (`df.rdd.map`, `df.rdd.flatMap`), which break Catalyst pushdown and fail on Spark Connect.

## 6. Partition Management (`.coalesce` vs `.repartition`)

- **Reducing partitions without a shuffle**: Use `df.coalesce(num_partitions)` before file sinks when reducing file count without redistributing keys.
- **Increasing partitions or partitioning by key**: Use `df.repartition(num_partitions, *partition_cols)` or `df.repartitionByRange(num_partitions, *range_cols)` when increasing parallelism or co-locating rows by key before writes.

## 7. Zero JVM / SparkContext Internals (Spark Connect & Serverless Spark 4.x Compatibility)

Generated code must be 100% compatible with Spark Connect (`ManagedSparkSession`) and GCP Serverless Spark 4.x:

- **Strict ban**: Never reference `spark.sparkContext`, `spark._jsc`, `spark._jvm`, `df.rdd`, or `sc.broadcast()`.
- Use DataFrame-native APIs (`F.broadcast(df)`, `spark.sql(...)`, `spark.read`, `spark.readStream`, `spark.createDataFrame`, `spark.range`).

## 8. Deterministic Fan-Out Caching (`ExecutionPlan.cacheDirectives` / `cacheNodes`)

Honor the deterministic caching plan in `ExecutionPlan`:

1. **Cache before branching**: For every node listed in `ExecutionPlan.cachedNodeIds` (`cacheDirectives` / `cacheNodes`), call `df = df.cache()` once immediately after computing that node's DataFrame in `transform()`.
2. **Unpersist after downstream sinks finish**: In `write_sinks()`, consult `ExecutionPlan.unpersistPoints` (or `unpersistAfterNodeIds` on each `CacheDirective`). Immediately after the final downstream consumer sink completes its write action, call `cached_df.unpersist()` to free executor memory:

```python
# Inside transform(dfs, config):
# Node 'step_enrich' has outDegree > 1 in ExecutionPlan.cachedNodeIds
step_enrich_df = compute_step_enrich(dfs["source_orders"]).cache()
out["step_enrich"] = step_enrich_df

# Inside write_sinks(dfs, config):
try:
    write_iceberg_sink(dfs["sink_iceberg_input"], config)
    write_spanner_sink(dfs["sink_spanner_input"], config)
finally:
    if "step_enrich" in dfs:
        dfs["step_enrich"].unpersist()
```
