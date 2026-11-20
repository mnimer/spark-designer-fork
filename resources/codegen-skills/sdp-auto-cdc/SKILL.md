---
name: sdp-auto-cdc
description: Managed Change Data Capture SCD Type 1 flows in Spark Declarative Pipelines using dp.create_streaming_table and dp.create_auto_cdc_flow on Apache Spark 4.2+.
metadata:
  kind: crosscutting
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [sdp]
  transforms: [MergeCdcScd]
  preloadWhen: [sdp_auto_cdc]
  sources:
    - https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html
    - https://iceberg.apache.org/docs/latest/spark-writes/
  validated: true
---

# Spark Declarative Pipelines Auto CDC SCD Type 1 (`dp.create_auto_cdc_flow`)

Follow these rules whenever a Spark Declarative Pipelines (`paradigm: sdp`) graph contains a `MergeCdcScd` node (`sdp_auto_cdc`).

## 1. OSS Spark 4.2 Auto CDC Registration Contract

In OSS Apache Spark 4.2 (`branch-4.2`), managed Change Data Capture upserts and deletes are declared in `transformations/*.py` by pairing `dp.create_streaming_table(...)` with `dp.create_auto_cdc_flow(...)`:

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

dp.create_streaming_table(
    name="silver_customers",
    format="iceberg",
    comment="SCD Type 1 customer dimension maintained via Auto CDC.",
)

dp.create_auto_cdc_flow(
    target="silver_customers",
    source="bronze_customer_cdc",
    keys=["customer_id"],
    sequence_by="updated_at",
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op"],
    stored_as_scd_type=1,
    name="silver_customers_cdc_flow",
)
```

### Exact Parameter Rules (`python/pyspark/pipelines/api.py`)

- **Allowed parameters only**: `dp.create_auto_cdc_flow(target, source, keys, sequence_by, apply_as_deletes=None, column_list=None, except_column_list=None, stored_as_scd_type=None, name=None)`.
- **Never emit Databricks-only DLT APIs or kwargs**: Do **not** call `dp.apply_changes` or `dlt.apply_changes`, and never pass `where`, `ignore_null_updates`, `apply_as_truncates`, `track_history_column_list`, `track_history_except_column_list`, `once`, or `spark_conf` to `dp.create_auto_cdc_flow`.
- **SCD Type 1 only (`stored_as_scd_type=1`)**: OSS Spark 4.2 supports `stored_as_scd_type=1` (or `"1"`) only. Passing `stored_as_scd_type=2` fails with `AUTOCDC_SCD2_NOT_SUPPORTED` / `NOT_EXPECTED_TYPE`.
- **MERGE-capable target format**: Always set `format="iceberg"` (or `format="delta"`) on `dp.create_streaming_table(name=target, ...)`. Formats without `MERGE` support (such as Parquet) fail with `AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE`.
- **Single flow per target**: Only one flow may target an Auto CDC streaming table (`AUTOCDC_MULTIPLE_FLOWS_TO_TARGET`). In SDP mode, `MergeCdcScd` reads a single upstream streaming CDC dataset (`source`) and needs no `right` target-snapshot input.
- **Unqualified column identifiers**: Pass plain, unqualified column names in `keys`, `sequence_by`, `column_list`, and `except_column_list` (`AUTOCDC_NON_COLUMN_IDENTIFIER`, `AUTOCDC_MULTIPART_COLUMN_IDENTIFIER`). Specify **at most one** of `column_list` or `except_column_list`, never both.

## 2. Mapping `MergeCdcScd` Node Properties to `dp.create_auto_cdc_flow`

1. **`keyColumns` & `sequenceColumn`**:
   - Pass `keys=key_columns` and `sequence_by=sequence_column`.
   - In the upstream staging helper inside `src/transforms.py` (for example `prepare_cdc_events(df)`), filter out or quarantine rows where any key column or `sequence_by` column is `NULL`, because null keys or null sequence numbers fail the Auto CDC micro-batch (`AUTOCDC_MICROBATCH_VALIDATION.NULL_KEY` / `NULL_SEQUENCE`).
2. **`cdcFormat`, `operationColumn`, and `deleteValues`**:
   - **`"debezium"`**: `apply_as_deletes=F.expr(f"{op_col} = 'd'")`, `except_column_list=[op_col]`.
   - **`"delta_cdf"`**: Filter `_change_type != 'update_preimage'` in `prepare_cdc_events(df)` inside `src/transforms.py`, then pass `apply_as_deletes=F.expr("_change_type = 'delete'")` and `except_column_list=["_change_type", "_commit_version", "_commit_timestamp"]` (excluding any column that is itself `sequence_by`).
   - **`"custom_op"`**: Build SQL `IN` literals from comma-separated `deleteValues` (for example `F.expr("op IN ('DELETE', 'D')")`) and set `except_column_list=[op_col]`.
   - **`"upsert_only"`**: Set `apply_as_deletes=None` and omit `except_column_list` unless explicit envelope columns are excluded.

## 3. State Table, Reserved Prefix & Offline Testing Rules

- **Reserved `__spark_autocdc_` prefix**: SDP adds a hidden/extra `__spark_autocdc_metadata` column to `<target>` and creates a companion state table `__spark_autocdc_aux_state_<target>` in `<catalog>.<database>`. Never define user columns or dataset names starting with `__spark_autocdc_` (`AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT`), and document in `README.md` that downstream BigQuery / `dbt` `sources.yml` models should exclude `__spark_autocdc_metadata` and `__spark_autocdc_aux_state_<target>`.
- **Key schema drift**: Changing `keys` on an existing Auto CDC target fails incremental runs with `AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT` and requires an explicit `--full-refresh <target>` confirmation.
- **Offline `pytest` coverage (`tests/test_transforms.py`)**: Keep upstream CDC normalization, null-key filtering, and delete-marker projection in pure helpers inside `src/transforms.py` and test out-of-order sequence events, delete tombstones, and null-key guards offline with `spark.createDataFrame(...)`.
