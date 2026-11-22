# Streaming Kafka to BigLake Iceberg Pipeline

## Overview

This sample pipeline consumes streaming JSON order events from Apache Kafka, isolates malformed payloads, computes tumbling window metrics, and writes to BigLake Iceberg.

If a Kafka message contains invalid JSON, the parser routes the corrupt payload to a dead-letter console stream. Upstream event-time watermarks bound Structured Streaming state and finalize five-minute regional revenue aggregations for Serverless for Apache Spark.

## Pipeline Architecture & Data Flow

`Kafka Event Stream ➔ Parse Order JSON ➔ [Event-Time Watermark, Corrupt Payload DLQ] ➔ Tumbling Window Metrics ➔ BigLake Iceberg Sink`

1. **Kafka Event Stream (`KafkaSource` / `SOURCE`)**: Reads unbounded records from `commerce.order_events` over `SASL_SSL` using credentials stored in Google Cloud Secret Manager.
2. **Parse Order JSON (`ParsePayload` / `STEP`)**: Deserializes UTF-8 JSON payloads with `PERMISSIVE` mode and routes `_corrupt_record` rows through the `error` handle.
3. **Corrupt Payload DLQ (`ConsoleSink` / `SINK`)**: Writes malformed records from the `error` handle to a streaming console query with an isolated checkpoint directory.
4. **Event-Time Watermark (`Watermark` / `STEP`)**: Applies a `10 minutes` event-time watermark on `event_timestamp` to bound operator state and evict expired windows.
5. **Tumbling Window Metrics (`GroupByAgg` / `STEP`)**: Aggregates `order_count` and `gross_revenue` by `region` across `5 minutes` tumbling windows and unpacks flat window timestamps.
6. **BigLake Iceberg Sink (`IcebergSink` / `SINK`)**: Appends finalized window metrics into a partitioned BigLake Iceberg REST catalog table with fanout writing enabled.

## Runtime Parameters

| Parameter | Default Value | Description |
| :--- | :--- | :--- |
| `JOB_ID` | `kafka-streaming-to-iceberg` | Unique batch job identifier for Serverless for Apache Spark |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-broker.internal:9092` | Comma-separated Kafka broker host and port endpoints |
| `KAFKA_TOPIC` | `commerce.order_events` | Source Kafka topic containing raw JSON order events |
| `ICEBERG_WAREHOUSE` | `gs://my-lakehouse-bucket/iceberg/warehouse` | Cloud Storage warehouse URI for BigLake Iceberg metadata and Parquet files |
| `CHECKPOINT_ROOT` | `gs://my-lakehouse-bucket/checkpoints/kafka-to-iceberg` | Base Cloud Storage URI for Structured Streaming query checkpoints |

## Code Generation & Execution

1. Save a copy of `kafka-streaming-to-iceberg.spark` into your workspace from the **Samples** palette.
2. Update the Kafka broker address, Secret Manager secret path, and Cloud Storage URIs in the **Deployment** panel.
3. Open the **Source Code** panel and click **Generate Code** to synthesize the PySpark repository.
4. If you want to verify transformation and dead-letter split logic offline, run the generated `pytest` suite.
5. Submit the streaming workload to Google Cloud Serverless for Apache Spark:

```bash
PROJECT_ID=my-gcp-project REGION=us-central1 DEPS_BUCKET=gs://my-spark-deps-bucket ./run_serverless_spark.sh
```
