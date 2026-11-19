# Batch GCS Parquet ETL Pipeline

## Overview

This Apache Spark 4.x batch pipeline ingests raw order Parquet files from Google Cloud Storage and writes partitioned revenue summaries back to Cloud Storage.

Catalyst pushes status and amount predicates into the Parquet reader, computes derived columns in one projection pass, aggregates daily metrics, and compacts output files.

## Pipeline Architecture & Data Flow

`Raw Orders Parquet (ParquetSource)` ➔ `Filter Completed Orders (Filter)` ➔ `Derive Revenue Columns (WithColumns)` ➔ `Aggregate Daily Revenue (GroupByAgg)` ➔ `Coalesce Partitions (RepartitionCoalesce)` ➔ `Curated Revenue Parquet (ParquetSink)`

1. **Raw Orders Parquet (`ParquetSource` / `SOURCE`)**: Reads columnar Parquet files in batch mode from `gs://example-analytics-lake/raw/orders/` using the pre-installed Cloud Storage connector.
2. **Filter Completed Orders (`Filter` / `STEP`)**: Keeps rows matching `status = 'COMPLETED' AND gross_amount > 0` through Catalyst predicate pushdown.
3. **Derive Revenue Columns (`WithColumns` / `STEP`)**: Computes `net_amount`, `event_date`, and `normalized_region` in a single `df.withColumns` projection pass.
4. **Aggregate Daily Revenue (`GroupByAgg` / `STEP`)**: Groups records by `event_date` and `normalized_region` with `windowType` set to `None` to calculate order counts and revenue totals.
5. **Coalesce Partitions (`RepartitionCoalesce` / `STEP`)**: Reduces executor partitions to four via `coalesce` without a network shuffle before writing files.
6. **Curated Revenue Parquet (`ParquetSink` / `SINK`)**: Writes Zstandard-compressed Parquet files to `gs://example-analytics-lake/curated/daily_regional_revenue/` partitioned by `event_date` and `normalized_region`.

## Runtime Parameters

| Parameter | Default Value | Description |
| :--- | :--- | :--- |
| `JOB_ID` | `gcs-parquet-etl` | Unique job identifier for batch tracking. |
| `GCP_PROJECT` | `example-analytics-prod` | Target Google Cloud project identifier. |
| `INPUT_PARQUET_URI` | `gs://example-analytics-lake/raw/orders/` | Source Cloud Storage URI containing raw order Parquet files. |
| `OUTPUT_PARQUET_URI` | `gs://example-analytics-lake/curated/daily_regional_revenue/` | Destination Cloud Storage URI for curated Parquet partitions. |
| `TARGET_PARTITIONS` | `4` | Target partition count applied by the `coalesce` step. |

## Code Generation & Execution

1. Click **New File** on the sample card to save `gcs-parquet-etl.spark` into your workspace.
2. Open the **Source Code** tab and click **Generate Code** to synthesize the PySpark 4.x project.
3. If you want to verify transformations offline, run the generated `pytest` suite locally:
```bash
python3 -m pytest tests/test_pipeline.py
```
4. If you deploy to Google Cloud Serverless for Apache Spark, update the bucket URIs and run the batch script:
```bash
./run_serverless_spark.sh
```
