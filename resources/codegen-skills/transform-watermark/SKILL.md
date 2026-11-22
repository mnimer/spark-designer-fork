---
name: transform-watermark
description: PySpark 4.x Structured Streaming event-time watermark placement, pre-watermark timestamp casting, and state eviction rules.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [Watermark]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withWatermark.html
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  validated: true
---

# PySpark 4.x Structured Streaming Watermark (`Watermark`)

Implement `Watermark` steps inside `transform(dfs, config)` using `df.withWatermark(eventTimeColumn, delayThreshold)` so Structured Streaming tracks event-time progress and bounds operator state.

## 1. Pre-Watermark `TimestampType` Casting (`sourceTimestampType`)

`df.withWatermark(eventTimeColumn, delayThreshold)` requires `eventTimeColumn` to have Spark SQL `TimestampType` (`"timestamp"`). Never overwrite or recast `eventTimeColumn` **after** calling `.withWatermark(...)`, because redefining the column strips Catalyst's internal `spark.watermarkDelayMs` metadata. Always cast raw strings or numeric epochs **before** calling `.withWatermark(...)`:

- **`"timestamp"` (default)**: Column already has `TimestampType` (for example produced by `ParsePayload` with `TIMESTAMP` DDL). Apply `df.withWatermark(col, threshold)` directly.
- **`"iso8601"`**: Cast ISO-8601 strings via `df = df.withColumn(col, F.to_timestamp(F.col(col)))`.
- **`"epoch_seconds"`**: Convert integer or floating-point Unix seconds via `df = df.withColumn(col, F.timestamp_seconds(F.col(col)))`.
- **`"epoch_millis"`**: Convert integer Unix milliseconds via `df = df.withColumn(col, F.timestamp_millis(F.col(col).cast("bigint")))`.
- **`"custom_format"`**: Parse formatted datetime strings using `properties.timestampFormat` (for example `"yyyy-MM-dd HH:mm:ss.SSS"`) via `df = df.withColumn(col, F.to_timestamp(F.col(col), timestamp_format))`.

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_watermark(
    df: DataFrame,
    event_time_col: str = "event_timestamp",
    delay_threshold: str = "10 minutes",
    source_ts_type: str = "timestamp",
    timestamp_format: str | None = None,
) -> DataFrame:
    """Normalizes the event-time column to TimestampType and applies df.withWatermark."""
    if source_ts_type == "iso8601":
        df = df.withColumn(event_time_col, F.to_timestamp(F.col(event_time_col)))
    elif source_ts_type == "epoch_seconds":
        df = df.withColumn(event_time_col, F.timestamp_seconds(F.col(event_time_col)))
    elif source_ts_type == "epoch_millis":
        df = df.withColumn(
            event_time_col,
            F.timestamp_millis(F.col(event_time_col).cast("bigint")),
        )
    elif source_ts_type == "custom_format" and timestamp_format:
        df = df.withColumn(
            event_time_col,
            F.to_timestamp(F.col(event_time_col), timestamp_format),
        )
    return df.withWatermark(event_time_col, delay_threshold)
```

## 2. Topological Placement Rules in Streaming Pipelines

Place `Watermark` at the exact boundary between stateless payload extraction and stateful event-time operators:

1. **After `ParsePayload` / Schema Extraction**:
   - Raw streaming sources (`KafkaSource`, `PubSubSource`, `KinesisSource`) emit binary `value` or `data` payloads. Parse the payload into structured columns first so `eventTimeColumn` exists as a top-level field.
2. **Before Stateful Operators**:
   - **Windowed Aggregations (`GroupByAgg`)**: Enables `Append` output mode on `F.window(...)` and `F.session_window(...)` and evicts finalized window state once `max_event_time - delay_threshold` advances past window end.
   - **Streaming Deduplication (`Deduplicate`)**: Required before `df.dropDuplicatesWithinWatermark(subset_cols)` so Spark expires seen keys after the watermark delay instead of retaining keys forever.
   - **Arbitrary Stateful Processing (`StatefulTransform`)**: Required upstream whenever `transformWithState` / `transformWithStateInPandas` uses `timeMode="EventTime"` or event-time TTLs.
   - **Stream-Stream Joins (`Join`)**: Required on **both** streaming sides (coupled with an event-time range predicate in the join condition) for stream-stream outer joins (`left`, `right`, `full`) and state cleanup on inner joins.

## 3. Offline Unit Testing Compatibility

`df.withWatermark(col, threshold)` is a valid no-op metadata annotation on static batch DataFrames in PySpark 4.x. Keep `apply_watermark` inside `transform(dfs, config)` without branching on `df.isStreaming` unless downstream operators specifically require `isStreaming` guards (such as `dropDuplicatesWithinWatermark`).
