# BigLake Iceberg Medallion Declarative Pipeline

## Overview

This Spark Declarative Pipelines (`paradigm: sdp`) sample ingests streaming CSV order files into Bronze and Silver streaming tables and computes a Gold BigLake Iceberg materialized view.

Apache Spark 4.2 resolves dataset dependencies automatically, maintains incremental file checkpoints under `sdpStorage`, and recomputes Gold store revenue metrics cleanly on each triggered run.

## Pipeline Architecture & Data Flow

`Raw Orders CSV Stream ➔ Bronze Order Projections ➔ Silver Event Watermark ➔ Silver Order Validation ➔ Gold Daily Store Revenue ➔ Gold BigLake Iceberg Table`

1. **Raw Orders CSV Stream (`CsvSource` / `SOURCE` / `TABLE`)**: Ingests new CSV files matching `orders-*.csv` once per file using an explicit `StructType` schema.
2. **Bronze Order Projections (`WithColumns` / `STEP` / `TABLE`)**: Parses `event_timestamp`, computes `net_amount` after discounts, and derives `order_day` in one Catalyst pass.
3. **Silver Event Watermark (`Watermark` / `STEP` / `TABLE`)**: Applies a `10 minutes` event-time watermark on `event_timestamp` for downstream streaming state management.
4. **Silver Order Validation (`Filter` / `STEP` / `TABLE`)**: Retains clean order records with non-null identifiers and positive `net_amount` values in `silver_orders`.
5. **Gold Daily Store Revenue (`GroupByAgg` / `STEP` / `MV`)**: Aggregates `order_count` and `net_revenue` by `store_id` and `order_day` as a batch materialized view.
6. **Gold BigLake Iceberg Table (`IcebergSink` / `SINK` / `MV`)**: Publishes `gold_store_revenue` partitioned by `order_day` in the `lake.retail_medallion` BigLake Iceberg REST catalog.

## Runtime Parameters

| Parameter | Default Value | Description |
| :--- | :--- | :--- |
| `ORDERS_SOURCE_URI` | `gs://my-retail-landing/orders/incoming/` | Landing Cloud Storage folder holding incoming order CSV files |
| `SDP_STORAGE` | `gs://my-sdp-checkpoints/medallion` | Dedicated Cloud Storage URI outside the source folder for SDP checkpoints |
| `SDP_CATALOG` | `lake` | Plain Spark catalog alias mapped to the BigLake Iceberg REST catalog |
| `SDP_DATABASE` | `retail_medallion` | Target BigLake namespace holding Bronze, Silver, and Gold Iceberg datasets |
| `BIGLAKE_WAREHOUSE` | `bl://projects/my-gcp-project/catalogs/retail_lakehouse` | BigLake Lakehouse catalog warehouse URI configured in `spark-pipeline.yml` |

## Code Generation & Execution

1. Save a copy of `sdp-biglake-iceberg-medallion.spark` into your workspace from the **Samples** palette.
2. Update your Google Cloud project, BigLake catalog ID, and Cloud Storage URIs in the **Deployment** panel.
3. Open the **Source Code** panel and click **Generate Code** to synthesize `spark-pipeline.yml`, `transformations/*.py`, and `src/transforms.py`.
4. If you want to verify pure transformation helpers offline, run `pytest` on `tests/test_transforms.py`.
5. Validate the declarative graph with `dry-run` or deploy the generated `sdp_stage.pipeline.yaml` stage to Managed Spark 4.0:

```bash
SDP_COMMAND=dry-run ./run_sdp_local.sh
```
