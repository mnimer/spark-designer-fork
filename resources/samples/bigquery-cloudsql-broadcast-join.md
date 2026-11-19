# BigQuery and Cloud SQL Broadcast Join Pipeline

## Overview

This Apache Spark 4.x batch pipeline enriches BigQuery payment transactions with Cloud SQL PostgreSQL merchant accounts and writes ranked records to BigQuery.

Spark reads Cloud SQL in parallel using IAM Socket Factory authentication, broadcasts the lookup DataFrame, and ranks per-merchant transactions.

## Pipeline Architecture & Data Flow

`[BigQuery Transactions Fact (BigQuerySource), Cloud SQL Merchant Accounts (CloudSqlSource)]` ➔ `Enrich Merchant Tier (Join)` ➔ `Rank Merchant Transactions (WindowOver)` ➔ `BigQuery Enriched Ledger (BigQuerySink)`

1. **BigQuery Transactions Fact (`BigQuerySource` / `SOURCE`)**: Reads captured payment transactions from BigQuery via the Storage Read API with filter pushdown.
2. **Cloud SQL Merchant Accounts (`CloudSqlSource` / `SOURCE`)**: Reads `public.merchant_accounts` from PostgreSQL over the Cloud SQL Socket Factory using IAM authentication and dynamic bounds.
3. **Enrich Merchant Tier (`Join` / `STEP`)**: Executes a `left_outer` join on `merchant_id` with `broadcastRight` enabled to avoid shuffling the transaction fact DataFrame.
4. **Rank Merchant Transactions (`WindowOver` / `STEP`)**: Computes `row_number` partitioned by `merchant_id` and ordered by `event_timestamp` descending to rank recent transactions.
5. **BigQuery Enriched Ledger (`BigQuerySink` / `SINK`)**: Writes enriched rows to BigQuery using the direct Storage Write API with daily partitioning and merchant clustering.

## Runtime Parameters

| Parameter | Default Value | Description |
| :--- | :--- | :--- |
| `JOB_ID` | `bigquery-cloudsql-broadcast-join` | Unique job identifier for batch tracking. |
| `GCP_PROJECT` | `example-analytics-prod` | Target Google Cloud project identifier. |
| `BQ_SOURCE_TABLE` | `example-analytics-prod.commerce_raw.payment_transactions` | Source BigQuery fact table containing payment transactions. |
| `CLOUDSQL_INSTANCE` | `example-analytics-prod:us-central1:merchant-directory-pg` | Cloud SQL PostgreSQL instance connection name. |
| `CLOUDSQL_IAM_USER` | `spark-etl-runner@example-analytics-prod.iam` | Cloud SQL IAM database principal username. |
| `BQ_SINK_TABLE` | `example-analytics-prod.commerce_curated.enriched_merchant_transactions` | Destination BigQuery table receiving enriched transaction rows. |

## Code Generation & Execution

1. Click **New File** on the sample card to save `bigquery-cloudsql-broadcast-join.spark` into your workspace.
2. Open the **Source Code** tab and click **Generate Code** to synthesize the PySpark 4.x project.
3. If you want to test the broadcast join and window logic offline, run `pytest` locally:
```bash
python3 -m pytest tests/test_pipeline.py
```
4. If you deploy on Serverless Spark, stage the PostgreSQL and Socket Factory JARs in Cloud Storage and run:
```bash
./run_serverless_spark.sh
```
