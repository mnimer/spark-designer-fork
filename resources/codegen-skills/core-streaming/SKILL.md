---
name: core-streaming
description: Structured Streaming 4.x watermarks, triggers, checkpoints, idempotent multi-sink foreachBatch grouping, and StatefulProcessor transformWithState patterns.
metadata:
  kind: crosscutting
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative]
  transforms: [Watermark, StatefulTransform]
  preloadWhen: [streaming]
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
  validated: true
---

# Structured Streaming 4.x Architecture & Execution Rules

Follow these Structured Streaming rules whenever `ExecutionPlan.pipelineMode == "streaming"` or when generating `Watermark` and `StatefulTransform` steps.

## 1. Streaming Sources & Watermark Placement (`Watermark`)

- Read unbounded sources in `read_sources()` using `spark.readStream.format(...)`.
- Apply `.withWatermark(event_time_col, delay_threshold)` immediately after parsing the event-time timestamp column (such as after `ParsePayload` or timestamp casting) and **before** any stateful operation:
  - Windowed aggregations (`F.window(F.col(event_time_col), window_duration, slide_duration)` or `F.session_window(F.col(event_time_col), gap_duration)`)
  - Streaming deduplication (`df.dropDuplicatesWithinWatermark(subset_cols)`)
  - Stream–stream joins (`joinMode == "stream_stream"`, where outer joins require watermarks on both sides and an event-time range condition)
  - Arbitrary stateful processing (`transformWithState` / `applyInPandasWithState`)

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_watermark(df: DataFrame, event_time_col: str, delay_threshold: str) -> DataFrame:
    """Applies a Structured Streaming event-time watermark."""
    return df.withWatermark(event_time_col, delay_threshold)
```

## 2. Triggers, Output Modes, and Mandatory Checkpoints

Every `DataStreamWriter` (`df.writeStream`) query must configure `outputMode`, `trigger`, and `checkpointLocation` from `ExecutionPlan.streamingSinkGroups`:

### Trigger Mapping
- **`processingTime`**: `.trigger(processingTime=trigger_interval)` (for example `"10 seconds"` or `"1 minute"`).
- **`availableNow`**: `.trigger(availableNow=True)` — always prefer `availableNow=True` over deprecated `once=True` for incremental batch-style catch-up processing in Spark 4.x.
- **`continuous`**: `.trigger(continuous=trigger_interval)` (for example `"1 second"`).
- **`once`** (legacy fallback only when explicitly requested): `.trigger(availableNow=True)`.

### Output Modes
- **`append`**: Default for stateless streams, watermarks with finalized windows, and file/lakehouse sinks.
- **`update`**: Emits only updated aggregation or stateful rows per micro-batch.
- **`complete`**: Emits the full aggregation table every micro-batch (requires bounded state).

### Checkpoint Location Resolution
Every streaming query requires a dedicated, fault-tolerant checkpoint directory (`gs://`, `s3a://`, `abfss://`, `hdfs://`, or local path):
- Use the explicit per-sink `checkpointLocation` from `ExecutionPlan.streamingSinkGroups[i].settings.checkpointLocation` when configured.
- Otherwise derive `f"{checkpoint_root.rstrip('/')}/{sink_or_group_id}"` from `config["checkpoint_root"]`.

```python
def configure_stream_writer(
    writer,
    output_mode: str,
    trigger_type: str,
    trigger_interval: str | None,
    checkpoint_location: str,
):
    """Applies outputMode, checkpointLocation, and trigger to a DataStreamWriter."""
    writer = writer.outputMode(output_mode).option("checkpointLocation", checkpoint_location)
    if trigger_type == "availableNow" or trigger_type == "once":
        return writer.trigger(availableNow=True)
    if trigger_type == "continuous":
        return writer.trigger(continuous=trigger_interval or "1 second")
    return writer.trigger(processingTime=trigger_interval or "10 seconds")
```

## 3. Multi-Sink & Batch-Only Sink `foreachBatch` Grouping

Consult `ExecutionPlan.streamingSinkGroups` when writing streaming sinks in `write_sinks()`:

1. **Native Single-Sink Group (`mode == "native"`)**:
   - Start the query directly with `df.writeStream.format(...).options(...).start()`.
2. **Grouped or Batch-Only Sink Group (`mode == "foreachBatch"`)**:
   - When a streaming branch feeds a batch-only connector (such as `SpannerSink`, `CloudSqlSink`, `AlloyDbSink`, `JdbcSink`, `SnowflakeSink`, `RedshiftSink`, `PubSubSink`, `FirestoreSink`, `RedisSink`) OR fans out to $\ge 2$ sinks in the same `StreamingSinkGroup`:
   - Define `def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:`.
   - Call `batch_df.cache()` at the start of `write_micro_batch` (especially when `requiresBatchCache` is true or multiple sinks are written).
   - Wrap sink writes in a `try ... finally` block that always calls `batch_df.unpersist()`.
   - Pass idempotent transaction options (`txnVersion=str(batch_id)` and `txnAppId=txn_app_id`) for connectors that support exactly-once micro-batch deduplication (such as Delta Lake), or use deterministic upsert keys.

```python
def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Starts all Structured Streaming sink queries and awaits termination."""
    checkpoint_root = config.get("checkpoint_root") or "/tmp/spark-checkpoints"
    txn_app_id = config.get("app_name", "spark_pipeline")

    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        try:
            # Sink 1: Idempotent Delta / Lakehouse write with txnVersion and txnAppId
            (
                batch_df.write.format("delta")
                .mode("append")
                .option("txnVersion", str(batch_id))
                .option("txnAppId", txn_app_id)
                .save(config["delta_output_path"])
            )
            # Sink 2: Batch-only relational / NoSQL sink inside the same micro-batch
            (
                batch_df.write.format("jdbc")
                .mode("append")
                .option("url", config["jdbc_url"])
                .option("dbtable", config["jdbc_table"])
                .option("batchsize", "5000")
                .save()
            )
        finally:
            batch_df.unpersist()

    stream_writer = configure_stream_writer(
        dfs["stream_subquery"].writeStream.foreachBatch(write_micro_batch),
        output_mode="append",
        trigger_type="processingTime",
        trigger_interval="10 seconds",
        checkpoint_location=f"{checkpoint_root.rstrip('/')}/foreach_batch_1",
    )
    stream_writer.start()

    # Wait for all active streaming queries
    spark = dfs["stream_subquery"].sparkSession
    spark.streams.awaitAnyTermination()
```

## 4. Arbitrary Stateful Streaming (`StatefulTransform` in Spark 4.0)

For `StatefulTransform` nodes, use Spark 4.0's `transformWithStateInPandas` / `transformWithState` (`StatefulProcessor` and `StatefulProcessorHandle`) or `applyInPandasWithState`:

- **Serverless Spark / Spark 4.x `timeMode` Rule**: Set `timeMode="EventTime"` (when upstream has `.withWatermark(...)`) or `timeMode="None"`.
- Initialize state primitives (`handle.getValueState`, `handle.getListState`, `handle.getMapState`) inside `init(self, handle: StatefulProcessorHandle) -> None`.
- Process each key's rows inside `handleInputRows(self, key, rows, timer_values)` and yield output DataFrames matching `outputStructType`.
- Register and clean up expired timers in `handleExpiredTimer(self, key, timer_values, expired_timer_info)` when using event-time timers.

```python
from typing import Iterator
import pandas as pd
from pyspark.sql.streaming.state import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import LongType, StringType, StructField, StructType

STATE_SCHEMA = StructType([StructField("count", LongType(), True)])
OUTPUT_SCHEMA = StructType(
    [
        StructField("key", StringType(), False),
        StructField("total_count", LongType(), False),
    ]
)


class RunningCountProcessor(StatefulProcessor):
    """Spark 4.0 StatefulProcessor tracking per-key event counts."""

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.count_state = handle.getValueState("count_state", STATE_SCHEMA)

    def handleInputRows(
        self,
        key: tuple,
        rows: Iterator[pd.DataFrame],
        timer_values,
    ) -> Iterator[pd.DataFrame]:
        current = self.count_state.get()
        running_total = int(current[0]) if current is not None else 0
        for pdf in rows:
            running_total += len(pdf)
        self.count_state.update((running_total,))
        yield pd.DataFrame({"key": [str(key[0])], "total_count": [running_total]})

    def close(self) -> None:
        pass


def apply_stateful_transform(df: DataFrame, group_col: str) -> DataFrame:
    """Applies Spark 4.0 transformWithStateInPandas on a grouped streaming DataFrame."""
    return df.groupBy(group_col).transformWithStateInPandas(
        statefulProcessor=RunningCountProcessor(),
        outputStructType=OUTPUT_SCHEMA,
        outputMode="Update",
        timeMode="None",
    )
```

## 5. Awaiting Streaming Queries

Always call `spark.streams.awaitAnyTermination()` at the end of `write_sinks()` when one or more streaming queries are started so long-running streaming jobs do not exit prematurely.
