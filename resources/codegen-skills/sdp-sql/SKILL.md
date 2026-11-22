---
name: sdp-sql
description: Declarative SQL transformation authoring rules (transformations/*.sql) for Apache Spark 4.2 Spark Declarative Pipelines (CREATE STREAMING TABLE, CREATE MATERIALIZED VIEW, CREATE TEMPORARY VIEW, CREATE FLOW ... INSERT INTO ... BY NAME).
metadata:
  kind: crosscutting
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [sdp]
  transforms: []
  preloadWhen: [sdp_sql]
  sources:
    - https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html
  validated: true
---

# Spark Declarative Pipelines SQL Output Mode (`sdp-sql`, Apache Spark 4.2+)

Follow these rules whenever `## SDP Execution Plan` sets `outputFormat` to `"sql"` or `"mixed"` (`V2.7`).

## 1. File Placement & Spec Globs

- Place SQL dataset definitions in `transformations/pipeline_datasets.sql` (or topic-grouped `transformations/*.sql` files).
- Keep `libraries: [{ glob: { include: "transformations/**" } }]` in `spark-pipeline.yml`. Spark 4.2 automatically loads both `.sql` and `.py` files matched by `transformations/**`.
- If `pythonDatasetNames` is non-empty (for example `dp.create_sink`, `dp.create_auto_cdc_flow`, `cluster_by`, Python UDFs, or `sdp_extensions.expectations`), emit those Python-only datasets in `transformations/pipeline_python.py` alongside `transformations/pipeline_datasets.sql`.

## 2. Allowed Spark 4.2 SDP SQL Statements (`SqlGraphRegistrationContext`)

Use only these valid Apache Spark 4.2 SQL DDL statements inside `transformations/*.sql`:

```sql
-- Streaming table reading an upstream streaming dataset or file source
CREATE STREAMING TABLE bronze_orders
USING iceberg
COMMENT 'Incremental raw order ingestion.'
AS SELECT *
FROM STREAM(raw_orders);

-- Row-level streaming cleaning with optional identity partitioning and reset protection
CREATE STREAMING TABLE silver_orders
USING iceberg
PARTITIONED BY (order_date)
TBLPROPERTIES ('pipelines.reset.allowed' = 'false')
AS SELECT
  order_id,
  store_id,
  CAST(created_at AS TIMESTAMP) AS created_at,
  TO_DATE(created_at) AS order_date,
  CAST(amount AS DOUBLE) AS amount
FROM STREAM(bronze_orders)
WHERE CAST(amount AS DOUBLE) > 0;

-- Batch materialized view for aggregations and joins
CREATE MATERIALIZED VIEW gold_store_daily
USING iceberg
COMMENT 'Daily store revenue summary.'
AS SELECT
  store_id,
  order_date,
  COUNT(*) AS order_count,
  SUM(amount) AS total_revenue
FROM silver_orders
GROUP BY store_id, order_date;

-- Non-persisted intermediate view
CREATE TEMPORARY VIEW recent_orders
AS SELECT *
FROM silver_orders
WHERE order_date >= date_sub(current_date(), 7);

-- Multi-flow append into one streaming table (MUST include BY NAME)
CREATE STREAMING TABLE unified_customers USING iceberg;

CREATE FLOW append_us_west
AS INSERT INTO unified_customers BY NAME
SELECT * FROM STREAM(customers_us_west);
```

## 3. Strict SDP SQL Bans (Blocking Defects)

1. **Disallowed statements**: Never emit `CREATE SCHEMA`, `CREATE NAMESPACE`, standalone `INSERT`, standalone `SELECT`, `MERGE`, `DROP`, or `ALTER` in `transformations/*.sql`. They fail registration with `PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR`. Keep `CREATE NAMESPACE IF NOT EXISTS` in runner setup scripts only.
2. **Disallowed DDL clauses**: Never emit `CLUSTER BY` (use Python `cluster_by=`), `LOCATION`, `OPTIONS`, `STORED AS`, bucketing, or Databricks `CONSTRAINT ... EXPECT` clauses in `.sql` files.
3. **`CREATE FLOW` rules**: Always include `BY NAME` (`INSERT INTO <table> BY NAME SELECT ...`). Never use `OVERWRITE`, `IF NOT EXISTS`, a partition spec, or an explicit column list in `CREATE FLOW`.
4. **Unsupported SQL operations**: Never emit `PIVOT` or Python UDF calls inside `.sql` files. Place those datasets in `transformations/*.py`.
5. **Streaming vs. Batch reads**: Use `FROM STREAM(<dataset>)` inside `CREATE STREAMING TABLE` and `CREATE FLOW`. Use plain `FROM <dataset>` inside `CREATE MATERIALIZED VIEW`.
