# Generate Mock Data Pipeline

## Overview

This sample pipeline uses Python `Faker` inside a vectorized `mapInPandas` step to generate realistic mock `Customer` records on every streaming tick and log them to the console.

The pipeline runs locally with zero cloud credentials, zero external message brokers, and zero Maven JAR dependencies using built-in PySpark Structured Streaming. By default, Spark generates one mock customer per second and prints untruncated customer rows every 1 second with JVM logs set to `WARN`.

## Pipeline Architecture & Data Flow

`Synthetic Rate Stream ➔ Generate Faker Customers ➔ Console Log Sink`

1. **Synthetic Rate Stream (`RateSource` / `SOURCE`)**: Emits one synthetic `(timestamp, value)` tick per second to pace mock customer generation.
2. **Generate Faker Customers (`PandasUdfTransform` / `STEP`)**: Runs `mapInPandas` with `faker.Faker()` initialized once per partition to generate `customer_id`, `full_name`, `email`, `phone`, `city`, `country`, and `created_at` columns.
3. **Console Log Sink (`ConsoleSink` / `SINK`)**: Prints up to 20 untruncated customer rows per micro-batch every `1 second` via `.writeStream.format("console")` while silencing noisy JVM `INFO` progress logs.

## Runtime Parameters

| Parameter | Default Value | Description |
| :--- | :--- | :--- |
| `JOB_ID` | `generate-mock-data` | Local Spark application identifier |
| `RATE_ROWS_PER_SECOND` | `1` | Mock customer records generated per second |
| `TRIGGER_INTERVAL` | `1 second` | Processing-time interval between consecutive console log batches |
| `CHECKPOINT_ROOT` | `file:///tmp/spark-pipeline-builder/generate-mock-data/checkpoints` | Local filesystem directory for Structured Streaming query checkpoints |

## Code Generation & Execution

1. Save a copy of `generate-mock-data.spark` into your workspace from the **Samples** palette.
2. If you want to log every `X` seconds instead of `1 second`, update `jobConfig.defaultTriggerInterval`, `sink_console` `triggerInterval`, and `TRIGGER_INTERVAL` (for example, `5 seconds` or `1 minute`).
3. Keep `paradigm: imperative` for continuous micro-batch logging. OSS Spark Declarative Pipelines (`paradigm: sdp`) runs triggered `Trigger.AvailableNow` batches and exits after one pass, whereas Imperative PySpark Structured Streaming runs continuous `trigger(processingTime=...)` loops.
4. Open the **Source Code** panel and click **Generate Code** to synthesize the standalone PySpark project with `faker`, `pandas`, and `pyarrow` in `requirements.txt`.
5. Run the streaming console pipeline locally without cloud accounts or extra JARs:

```bash
./run_local.sh
```
