# Local Streaming Rate to Delta Lake Pipeline

## Overview

This sample pipeline generates synthetic streaming events, derives metadata columns, deduplicates keys within an event-time watermark, and writes to Delta Lake.

The pipeline runs locally with zero cloud dependencies, credentials, or external message brokers using built-in PySpark and local filesystem paths. If duplicate event keys arrive within the one-minute watermark threshold, Spark drops the duplicates and evicts expired state.

## Pipeline Architecture & Data Flow

`Synthetic Rate Stream ➔ Derive Event Columns ➔ Event-Time Watermark ➔ Watermark Key Deduplicate ➔ Local Delta Lake Sink`

1. **Synthetic Rate Stream (`RateSource` / `SOURCE`)**: Emits ten synthetic `(timestamp, value)` streaming rows per second across two local Spark partitions.
2. **Derive Event Columns (`WithColumns` / `STEP`)**: Computes `event_key`, `device_bucket`, and `event_date` in a single Catalyst `withColumns` projection pass.
3. **Event-Time Watermark (`Watermark` / `STEP`)**: Applies a `1 minute` event-time watermark on `timestamp` to bound streaming deduplication state memory.
4. **Watermark Key Deduplicate (`Deduplicate` / `STEP`)**: Deduplicates streaming records on `event_key` using `dropDuplicatesWithinWatermark` with automatic state cleanup.
5. **Local Delta Lake Sink (`DeltaSink` / `SINK`)**: Appends deduplicated micro-batches into a partitioned local Delta Lake table with schema evolution enabled.

## Runtime Parameters

| Parameter | Default Value | Description |
| :--- | :--- | :--- |
| `JOB_ID` | `rate-stream-to-delta` | Local Spark application identifier |
| `RATE_ROWS_PER_SECOND` | `10` | Synthetic streaming event generation throughput per second |
| `DELTA_TABLE_PATH` | `file:///tmp/spark-pipeline-builder/rate-stream-to-delta/delta-events` | Local filesystem URI for the output Delta Lake table |
| `CHECKPOINT_ROOT` | `file:///tmp/spark-pipeline-builder/rate-stream-to-delta/checkpoints` | Local filesystem directory for Structured Streaming query checkpoints |

## Code Generation & Execution

1. Save a copy of `rate-stream-to-delta.spark` into your workspace from the **Samples** palette.
2. Open the **Source Code** panel and click **Generate Code** to synthesize the standalone PySpark project.
3. If you want to validate Catalyst column derivation and deduplication offline, run the generated `pytest` suite.
4. Run the streaming Delta Lake pipeline locally without cloud accounts:

```bash
./run_local.sh
```
