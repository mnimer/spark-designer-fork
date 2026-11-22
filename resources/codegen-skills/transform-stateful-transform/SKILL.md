---
name: transform-stateful-transform
description: PySpark 4.0 StatefulProcessor transformWithState, RocksDB state store configuration, applyInPandasWithState, and DLQ error routing rules.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [StatefulTransform]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.transformWithState.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.streaming.StatefulProcessor.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandasWithState.html
  validated: true
---

# PySpark 4.0 Arbitrary Stateful Streaming (`StatefulTransform`)

Implement `StatefulTransform` steps inside `transform(dfs, config)` using Spark 4.0's `StatefulProcessor` (`df.groupBy(*key_cols).transformWithState(...)` / `transformWithStateInPandas(...)`) or `df.groupBy(*key_cols).applyInPandasWithState(...)`.

## 1. RocksDB State Store Requirement (`transformWithState`)

Spark 4.0's `transformWithState` and `transformWithStateInPandas` require the RocksDB state store provider at runtime:

- Configure `spark.sql.streaming.stateStore.providerClass` to `org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider` in `create_spark_session()` (and inside `tests/test_pipeline.py` SparkSession fixtures whenever testing `transformWithState`).

```python
builder = SparkSession.builder.appName(app_name).config(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
)
```

## 2. Spark 4.0 `StatefulProcessor` & `transformWithState` (`apiMode == "transformWithState"`)

Import `StatefulProcessor` and `StatefulProcessorHandle` from `pyspark.sql.streaming` (and `TTLConfig` from `pyspark.sql.streaming.state` with `datetime.timedelta` when `properties.ttlDuration` is set):

- **State Primitives (`properties.statePrimitive`)**: Initialize state handles inside `init(self, handle: StatefulProcessorHandle) -> None`:
  - **`ValueState`**: `self.state = handle.getValueState("value_state", state_schema)` (single struct per key).
  - **`ListState`**: `self.state = handle.getListState("list_state", state_schema)` (appendable sequence of structs per key).
  - **`MapState`**: `self.state = handle.getMapState("map_state", user_key_schema, state_schema)` (dictionary keyed by sub-key per group).
  - When `properties.ttlDuration` is configured, pass `TTLConfig(ttlDuration=timedelta(...))` as the final argument to `getValueState`, `getListState`, or `getMapState` (requires `timeMode="ProcessingTime"`).
- **Time Modes (`properties.timeMode`)**:
  - Prefer `timeMode="EventTime"` (requires an upstream `Watermark` step on the input DataFrame) or `timeMode="None"` on GCP Serverless Spark 4.x.
  - Use `timeMode="ProcessingTime"` when processing-time timers or state `TTLConfig` expiration are active.
- **Invocation**:
  - Row-based Python iterator: `df.groupBy(*key_cols).transformWithState(statefulProcessor=..., outputStructType=output_schema_ddl, outputMode=output_mode, timeMode=time_mode)`
  - Vectorized Pandas iterator: `df.groupBy(*key_cols).transformWithStateInPandas(statefulProcessor=..., outputStructType=output_schema_ddl, outputMode=output_mode, timeMode=time_mode)`

```python
from typing import Iterator
from pyspark.sql import DataFrame, Row
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType


class SessionAlertProcessor(StatefulProcessor):
    """Tracks per-key state using Spark 4.0 ValueState."""

    def init(self, handle: StatefulProcessorHandle) -> None:
        state_schema = StructType.fromDDL("event_count BIGINT, total_amount DOUBLE")
        self.summary_state = handle.getValueState("summary_state", state_schema)

    def handleInputRows(
        self,
        key: tuple,
        rows: Iterator[Row],
        timer_values,
    ) -> Iterator[Row]:
        existing = self.summary_state.get()
        count = int(existing[0]) if existing is not None else 0
        total = float(existing[1]) if existing is not None else 0.0
        for row in rows:
            count += 1
            total += float(row["amount"] or 0.0)
        self.summary_state.update((count, total))
        yield Row(user_id=str(key[0]), event_count=count, total_amount=total)

    def close(self) -> None:
        pass


def apply_stateful_transform(df: DataFrame) -> DataFrame:
    """Applies Spark 4.0 transformWithState grouped by user_id."""
    output_schema = StructType.fromDDL(
        "user_id STRING, event_count BIGINT, total_amount DOUBLE"
    )
    return df.groupBy("user_id").transformWithState(
        statefulProcessor=SessionAlertProcessor(),
        outputStructType=output_schema,
        outputMode="Update",
        timeMode="None",
    )
```

## 3. `applyInPandasWithState` Alternative (`apiMode == "applyInPandasWithState"`)

When `properties.apiMode == "applyInPandasWithState"`, use `df.groupBy(*key_cols).applyInPandasWithState(...)` with `GroupState` and `GroupStateTimeout` from `pyspark.sql.streaming.state`:

- Map `timeMode` to `timeoutConf`:
  - `"EventTime"` -> `GroupStateTimeout.EventTimeTimeout`
  - `"ProcessingTime"` -> `GroupStateTimeout.ProcessingTimeTimeout`
  - `"None"` -> `GroupStateTimeout.NoTimeout`

```python
from typing import Iterator
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout


def update_pandas_state(
    key: tuple,
    pdfs: Iterator[pd.DataFrame],
    state: GroupState,
) -> Iterator[pd.DataFrame]:
    """Updates per-key count state across micro-batches using Pandas DataFrames."""
    total = int(state.get[0]) if state.exists else 0
    for pdf in pdfs:
        total += len(pdf)
    state.update((total,))
    yield pd.DataFrame({"user_id": [str(key[0])], "event_count": [total]})


def apply_pandas_stateful(df: DataFrame) -> DataFrame:
    """Applies applyInPandasWithState with explicit state and output DDL schemas."""
    return df.groupBy("user_id").applyInPandasWithState(
        func=update_pandas_state,
        outputStructType="user_id STRING, event_count BIGINT",
        stateStructType="event_count BIGINT",
        outputMode="Update",
        timeoutConf=GroupStateTimeout.NoTimeout,
    )
```

## 4. Dead-Letter Queue Split (`try_except_error_col`)

When `ExecutionPlan.dlqSplitPoints` lists a `StatefulTransform` node (`mechanism == "try_except_error_col"` because its `error` handle is connected):

1. Append `_error STRING` to `outputStructType` so both valid and failed records flow out of the stateful operator in a single pass.
2. Inside `handleInputRows` (or the `applyInPandasWithState` callback), wrap record processing in `try ... except Exception as exc:` and emit `_error=None` for valid output rows and `_error=str(exc)` for failed records.
3. Route downstream DataFrames in `transform(dfs, config)`:
   - Valid branch: `valid_df = stateful_df.filter(F.col("_error").isNull()).drop("_error")`
   - Error DLQ branch: `error_df = stateful_df.filter(F.col("_error").isNotNull())`
