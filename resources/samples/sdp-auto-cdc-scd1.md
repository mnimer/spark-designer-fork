# Auto CDC SCD Type 1 Declarative Pipeline

## Overview

This Spark Declarative Pipelines (`paradigm: sdp`) sample applies Change Data Capture upserts and delete tombstones with out-of-order sequence numbers into a BigLake Iceberg customer table.

Apache Spark 4.2 executes `dp.create_auto_cdc_flow` with `stored_as_scd_type=1`, orders events by `updated_at`, and drops the `op` envelope column from `silver_customers`.

## Pipeline Architecture & Data Flow

`Customer CDC JSON Stream ➔ Normalize Sequence Timestamp ➔ Filter Valid Keys & Sequence ➔ Auto CDC SCD Type 1 Flow ➔ Silver Customers Iceberg Table`

1. **Customer CDC JSON Stream (`JsonSource` / `SOURCE` / `TABLE`)**: Reads incremental JSONL change files matching `cdc-*.json` with an explicit `StructType` schema.
2. **Normalize Sequence Timestamp (`WithColumns` / `STEP` / `TABLE`)**: Casts `updated_at` to `TIMESTAMP` for monotonic event ordering and normalizes customer email addresses.
3. **Filter Valid Keys & Sequence (`Filter` / `STEP` / `TABLE`)**: Filters out null `customer_id` or `updated_at` values to prevent micro-batch Auto CDC validation errors.
4. **Auto CDC SCD Type 1 Flow (`MergeCdcScd` / `STEP` / `CDC`)**: Connects on the `left` handle and registers `dp.create_auto_cdc_flow` with `apply_as_deletes=expr("op = 'DELETE'")`.
5. **Silver Customers Iceberg Table (`IcebergSink` / `SINK` / `CDC`)**: Declares `dp.create_streaming_table(name="silver_customers", format="iceberg")` inside the BigLake REST catalog.

## Runtime Parameters

| Parameter | Default Value | Description |
| :--- | :--- | :--- |
| `CDC_SOURCE_URI` | `gs://my-crm-landing/customer-cdc/` | Cloud Storage folder receiving raw customer CDC JSONL files |
| `SDP_STORAGE` | `gs://my-sdp-checkpoints/customer-cdc` | Cloud Storage URI storing SDP flow checkpoints outside the source folder |
| `SDP_CATALOG` | `lake` | Plain Spark catalog alias mapped to the BigLake Iceberg REST catalog |
| `SDP_DATABASE` | `crm_silver` | Target BigLake namespace containing `silver_customers` and its Auto CDC state table |
| `BIGLAKE_WAREHOUSE` | `bl://projects/my-gcp-project/catalogs/crm_lakehouse` | BigLake Lakehouse catalog warehouse URI configured in `spark-pipeline.yml` |

## Code Generation & Execution

1. Save a copy of `sdp-auto-cdc-scd1.spark` into your workspace from the **Samples** palette.
2. Configure your Google Cloud project, BigLake catalog ID, and Cloud Storage URIs in the **Deployment** panel.
3. Open the **Source Code** panel and click **Generate Code** to synthesize `spark-pipeline.yml`, `transformations/*.py`, and `src/transforms.py`.
4. If you query `silver_customers` downstream in BigQuery or `dbt`, exclude `__spark_autocdc_metadata` and `__spark_autocdc_aux_state_silver_customers`.
5. Validate the Auto CDC graph locally or deploy `sdp_stage.pipeline.yaml` to Managed Spark 4.0:

```bash
SDP_COMMAND=dry-run ./run_sdp_local.sh
```
