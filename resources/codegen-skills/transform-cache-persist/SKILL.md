---
name: transform-cache-persist
description: Materialize intermediate batch DataFrames in executor memory or disk with cache, persist StorageLevel constants, and deterministic unpersist in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, notebook]
  transforms: [CachePersist]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html
  validated: true
---

# Cache & Persist Step (`CachePersist`)

Use these rules when generating PySpark {{spark_version}} code for a `CachePersist` step node inside `transform(dfs, config)` and `write_sinks(dfs, config)`.

## 1. `df.cache()` vs `df.persist(StorageLevel)`

Import `from pyspark import StorageLevel` and apply persistence according to `mode` (`"cache"` or `"persist"`):

- **`mode == "cache"`**: Call `df.cache()`, which delegates to Spark's default `StorageLevel.MEMORY_AND_DISK` persistence tier.
- **`mode == "persist"`**: Resolve `storageLevel` against PySpark {{spark_version}} and Spark Connect valid `StorageLevel` constants using `getattr(StorageLevel, storage_level)`:
  - `MEMORY_AND_DISK`: Stores partitions in executor RAM and spills excess blocks to local disk.
  - `MEMORY_ONLY`: Keeps partitions strictly in executor memory and recomputes evicted blocks on demand.
  - `DISK_ONLY`: Writes all persisted blocks directly to executor scratch disk.
  - `MEMORY_AND_DISK_DESER`: Stores deserialized objects in memory and spills overflow blocks to disk.
  - `OFF_HEAP`: Stores serialized blocks in configured off-heap memory (`spark.memory.offHeap.enabled=true`).

## 2. Structured Streaming Guard

- Never call `.cache()` or `.persist()` directly on an unbounded streaming DataFrame (`df.isStreaming == True`) inside `transform(dfs, config)`. Structured Streaming rejects unbounded stream caching outside a micro-batch boundary.
- Guard persistence with `if not df.isStreaming:` in `transform(dfs, config)`. Inside streaming pipelines, cache finite micro-batch DataFrames (`batch_df.cache()` paired with `batch_df.unpersist()` in `try / finally`) only inside `foreachBatch(batch_df, batch_id)` when `ExecutionPlan.streamingSinkGroups` sets `requiresBatchCache: true`.

## 3. Deterministic Cleanup via `ExecutionPlan.unpersistPoints`

- `ExecutionPlan.cacheDirectives` records `reason: "cache_persist_step"` and populates `ExecutionPlan.unpersistPoints` mapping the final transitive downstream consumer node ID to cached upstream node IDs.
- When `unpersistAfterDownstream` is `True` (default), release cached executor memory and disk blocks in `write_sinks(dfs, config)` inside a `try / finally` block (`cached_df.unpersist()`) as soon as all downstream consumers in `ExecutionPlan.unpersistPoints` finish writing.

## 4. Reference Implementation (`src/pipeline.py`)

```python
from typing import Any
from pyspark import StorageLevel
from pyspark.sql import DataFrame

ALLOWED_STORAGE_LEVELS = {
    "MEMORY_AND_DISK",
    "MEMORY_ONLY",
    "DISK_ONLY",
    "MEMORY_AND_DISK_DESER",
    "OFF_HEAP",
}


def apply_cache_persist(
    df: DataFrame,
    mode: str = "cache",
    storage_level: str = "MEMORY_AND_DISK",
) -> DataFrame:
    """Persists a batch DataFrame using df.cache() or df.persist(StorageLevel)."""
    if df.isStreaming:
        return df

    if mode == "cache":
        return df.cache()

    normalized_level = storage_level.strip().upper()
    if normalized_level not in ALLOWED_STORAGE_LEVELS:
        raise ValueError(f"Unsupported Spark StorageLevel: {storage_level}")

    level_obj = getattr(StorageLevel, normalized_level)
    return df.persist(level_obj)


def write_sinks_with_unpersist(
    dfs: dict[str, DataFrame],
    config: dict[str, Any],
    unpersist_node_ids: list[str],
) -> None:
    """Executes terminal sink writes and deterministically unpersists cached DataFrames."""
    try:
        # Execute downstream sink writes consuming persisted DataFrames
        _ = config
    finally:
        for node_id in unpersist_node_ids:
            if node_id in dfs and not dfs[node_id].isStreaming:
                dfs[node_id].unpersist()
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

```python
from pyspark.sql import SparkSession
from src.pipeline import apply_cache_persist, write_sinks_with_unpersist


def test_cache_persist_and_unpersist_offline(spark: SparkSession) -> None:
    """Verifies batch DataFrame persistence levels and unpersist cleanup offline."""
    df = spark.createDataFrame([("a", 1), ("b", 2)], ["key", "val"])

    persisted_df = apply_cache_persist(df, mode="persist", storage_level="DISK_ONLY")
    assert persisted_df.storageLevel.useDisk is True
    assert persisted_df.count() == 2

    dfs = {"step_cache": persisted_df}
    write_sinks_with_unpersist(dfs, {}, unpersist_node_ids=["step_cache"])
    assert persisted_df.storageLevel.useDisk is False
```
