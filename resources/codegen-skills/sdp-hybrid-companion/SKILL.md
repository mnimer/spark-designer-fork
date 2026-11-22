---
name: sdp-hybrid-companion
description: Hybrid Spark Declarative Pipelines plus companion imperative PySpark job pattern for foreachBatch-only sinks (Cloud SQL, Spanner, Bigtable, Firestore, Redis, Vector Store, custom foreachBatch).
metadata:
  kind: crosscutting
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [sdp]
  transforms: []
  preloadWhen: [sdp_hybrid]
  sources:
    - https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch
  validated: true
---

# Hybrid SDP + Imperative Companion Sink Job (`sdp-hybrid-companion`)

Follow these rules whenever `## SDP Execution Plan` reports `hasHybridCompanionSinks: true` (`V2.6`).

## 1. Two-Stage Hybrid Architecture

Apache Spark 4.2 SDP (`pyspark.pipelines`) does not support imperative `foreachBatch` micro-batch writers inside `@dp.*` dataset functions. Split the generated repository into two coordinated stages:

1. **Stage 1 — Declarative SDP Project (`spark-pipeline.yml` + `transformations/`)**:
   - Materialize all upstream sources, transforms, and catalog tables (`@dp.table`, `@dp.materialized_view`, `dp.create_auto_cdc_flow`).
   - Never import `foreachBatch` writers or call `.write` / `.writeStream` inside `transformations/`.
   - Ensure every dataset feeding a `hybrid_companion` sink is a persisted catalog table or materialized view (never a `@dp.temporary_view`).
2. **Stage 2 — Companion Imperative Sink Job (`companion/sink_job.py` + `run_companion_sinks.sh`)**:
   - Place the companion imperative PySpark job in `companion/sink_job.py` (outside `transformations/` so `spark-pipeline.yml` `transformations/**` never loads it).
   - Read each persisted upstream SDP catalog table (`<catalog>.<database>.<upstream_dataset>`) with `spark.readStream.table(...)` (with `Trigger.AvailableNow` and a dedicated checkpoint under `<storage>/_companion_checkpoints/<sink_name>`) or `spark.read.table(...)` for batch materialized views.
   - Structure `companion/sink_job.py` cleanly into `read_catalog_inputs(spark, config)` and `write_companion_sinks(dfs, config)` so `tests/test_companion_sinks.py` can unit test micro-batch formatting and idempotent upsert logic offline.

## 2. Companion Script & Orchestration Deliverables

- Generate `companion/sink_job.py` with explicit Secret Manager credential lookup (zero hardcoded passwords or keys) and idempotent upsert semantics (`txnAppId` / `txnVersion=batch_id` or `ON CONFLICT` / `INSERT OR UPDATE`).
- Generate `tests/test_companion_sinks.py` testing the companion payload transformation and sink batch handler with mocks/in-memory DataFrames.
- Generate executable `run_companion_sinks.sh` that runs Stage 2 after Stage 1 (`run_sdp_local.sh` or `run_sdp_dataproc.sh`) finishes.
- In `sdp_stage.pipeline.yaml`, document or declare the companion `pyspark` stage after the `sdp` stage so Managed Data Pipelines executes Stage 2 after Stage 1 completes.
- Document the two-stage run order and checkpoint isolation in `README.md`.
