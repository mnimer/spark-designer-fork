---
name: transform-enforce-schema-cast
description: Enforce explicit Spark SQL DDL data contracts with schema drift policies, Spark 4.0 ANSI-safe F.try_cast conversions, and dead-letter error routing in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [EnforceSchemaAndCast]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_cast.html
    - https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html
  validated: true
---

# Data Contract Schema Enforcement & Safe Cast (`EnforceSchemaAndCast`)

Use these rules when generating PySpark {{spark_version}} code for an `EnforceSchemaAndCast` (`EXT5`) step inside `transform(dfs, config)`.

## 1. Parse Target DDL Contract & Non-Nullable Constraints

- Parse `schemaDDL` into a target `StructType` at plan time using `StructType.fromDDL(schema_ddl)` from `pyspark.sql.types`.
- Never infer contracts from sample rows or RDD inspection. If `schemaDDL` is empty, raise a `ValueError` immediately.
- Build the effective non-nullable column set by combining every `StructField` where `not field.nullable` (`NOT NULL` in DDL) with all column names listed in `nonNullableColumns`.

## 2. Enforce Schema Drift Policy (`driftPolicy`)

Compare `df.schema` against the target `StructType` at plan construction time before projecting rows:

- **`fail`**: Inspect `df.columns` against the target `StructType` fields. Raise `ValueError` if upstream `df` contains unexpected extra columns outside the contract. Raise `ValueError` if any non-nullable contract column is missing from `df`, or if any nullable contract column is missing when `allowMissingNullable` is `False`. When `allowMissingNullable` is `True`, synthesize missing nullable contract columns as `F.lit(None).cast(field.dataType).alias(field.name)`.
- **`drop_extra`**: Project only the contract columns in exact target `StructType` field order. Synthesize missing nullable contract columns as `F.lit(None).cast(field.dataType).alias(field.name)`. Raise `ValueError` if a missing contract column is non-nullable and has no upstream source column.
- **`add_nulls`**: Synthesize missing nullable contract columns as `F.lit(None).cast(field.dataType).alias(field.name)` and raise `ValueError` if a missing contract column is non-nullable. Cast all matching contract columns in target order and retain upstream extra columns after the contract columns.

## 3. Spark 4.0 ANSI-Safe Casting (`F.try_cast` & Custom Date/Timestamp Formats)

Spark 4.0 enables `spark.sql.ansi.enabled=true` by default. Under ANSI mode, `F.col(c).cast(t)` throws `CAST_INVALID_INPUT` at runtime on malformed strings instead of returning `NULL`.

Honor `castMode` (`"try_cast_dlq"`, `"try_cast_null"`, or `"ansi_strict"`):

- **`"try_cast_dlq"` (default) and `"try_cast_null"`**:
  - Convert columns using `F.try_cast(F.col(escaped_col), field.dataType.simpleString())` so malformed inputs evaluate to `NULL` without aborting the Spark task.
  - When `dateFormat` is non-empty, `isinstance(field.dataType, DateType)`, and the upstream column is `StringType`, parse strings with `F.try_to_timestamp(F.col(escaped_col), F.lit(date_format)).cast("date")`.
  - When `timestampFormat` is non-empty, `isinstance(field.dataType, TimestampType)`, and the upstream column is `StringType`, parse strings with `F.try_to_timestamp(F.col(escaped_col), F.lit(timestamp_format))`.
- **`"ansi_strict"`**:
  - Use strict ANSI `F.col(escaped_col).cast(field.dataType)` (or `F.to_date(F.col(escaped_col), date_format)` and `F.to_timestamp(F.col(escaped_col), timestamp_format)` on string inputs when custom formats are set) so invalid values fail the Spark job immediately.
  - Enforce non-nullable columns in strict mode with `F.assert_true(F.col(escaped_col).isNotNull(), F.lit(f"NOT NULL contract violation on {field.name}"))`.

## 4. Dead-Letter Queue (`error` Handle) Split

Evaluate all column casts and contract checks in a single vectorized Catalyst pass without Python `@udf` loops:

1. Preserve raw upstream columns under temporary shadow aliases (`__raw_<col>`) before projecting casted contract columns.
2. Detect row-level contract violations:
   - **Cast failure** (`castMode == "try_cast_dlq"`): `F.col(raw_alias).isNotNull() & casted_col.isNull()`.
   - **Non-nullable violation**: `casted_col.isNull()` on any column in the effective non-nullable set (guarded with `F.col(raw_alias).isNull()` when `castMode == "try_cast_dlq"` so a malformed non-null string reports one cast failure tag instead of two duplicate tags).
3. Assemble a nullable `_error` diagnostic column via `F.concat_ws("; ", F.array_compact(F.array(*violation_tags)))` when at least one violation occurred, and `F.lit(None).cast("string")` when the row satisfies the contract.
4. Route output branches according to `ExecutionPlan.dlqSplitPoints`:
   - If the node is in `ExecutionPlan.cachedNodeIds` (batch mode fan-out), call `.cache()` once on the staged DataFrame before filtering and `.unpersist()` after downstream sinks complete in `write_sinks()`.
   - **`valid` branch**: Filter `F.col("_error").isNull()` and project only the clean casted contract columns (plus extra columns when `driftPolicy == "add_nulls"`), dropping `_error` and all `__raw_*` shadow columns.
   - **`error` DLQ branch**: Filter `F.col("_error").isNotNull()` and restore the original uncasted upstream columns from `__raw_*` alongside `_error`.

## 5. Reference Implementation

```python
from typing import Any
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType


def _quote_ident(name: str) -> str:
    """Quotes a column identifier with backticks for safe Catalyst resolution."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def _build_cast_expr(
    field: StructField,
    upstream_field: StructField,
    cast_mode: str,
    date_format: str,
    timestamp_format: str,
) -> Column:
    """Builds an ANSI-safe or strict Catalyst cast expression for one contract field."""
    src_col = F.col(_quote_ident(field.name))
    is_string_input = isinstance(upstream_field.dataType, StringType)

    if cast_mode == "ansi_strict":
        if date_format and isinstance(field.dataType, DateType) and is_string_input:
            return F.to_date(src_col, date_format)
        if timestamp_format and isinstance(field.dataType, TimestampType) and is_string_input:
            return F.to_timestamp(src_col, timestamp_format)
        return src_col.cast(field.dataType)

    if date_format and isinstance(field.dataType, DateType) and is_string_input:
        return F.try_to_timestamp(src_col, F.lit(date_format)).cast("date")
    if timestamp_format and isinstance(field.dataType, TimestampType) and is_string_input:
        return F.try_to_timestamp(src_col, F.lit(timestamp_format))
    return F.try_cast(src_col, field.dataType.simpleString())


def enforce_schema_and_cast(
    df: DataFrame,
    step_cfg: dict[str, Any],
    emit_dlq_split: bool = False,
    cache_staged: bool = False,
) -> dict[str, DataFrame]:
    """Enforces a target DDL contract with drift checks, F.try_cast, and DLQ routing."""
    schema_ddl = str(step_cfg.get("schemaDDL", "")).strip()
    if not schema_ddl:
        raise ValueError("EnforceSchemaAndCast requires a non-empty schemaDDL string.")

    target_schema = StructType.fromDDL(schema_ddl)
    drift_policy = str(step_cfg.get("driftPolicy", "drop_extra")).strip().lower()
    cast_mode = str(step_cfg.get("castMode", "try_cast_dlq")).strip().lower()
    allow_missing_nullable = bool(step_cfg.get("allowMissingNullable", False))
    date_format = str(step_cfg.get("dateFormat", "")).strip()
    timestamp_format = str(step_cfg.get("timestampFormat", "")).strip()

    configured_non_null = {
        str(col_name).strip()
        for col_name in (step_cfg.get("nonNullableColumns") or [])
        if str(col_name).strip()
    }
    non_nullable_cols = configured_non_null | {
        field.name for field in target_schema.fields if not field.nullable
    }

    upstream_by_name: dict[str, StructField] = {f.name: f for f in df.schema.fields}
    contract_names = [f.name for f in target_schema.fields]
    contract_set = set(contract_names)

    extra_cols = [c for c in df.columns if c not in contract_set]
    missing_fields = [f for f in target_schema.fields if f.name not in upstream_by_name]
    missing_non_nullable = [f.name for f in missing_fields if f.name in non_nullable_cols]

    if drift_policy == "fail":
        if extra_cols:
            raise ValueError(
                f"Schema drift policy 'fail' rejected unexpected extra columns: {extra_cols}"
            )
        if missing_non_nullable:
            raise ValueError(
                f"Schema drift policy 'fail' rejected missing non-nullable columns: {missing_non_nullable}"
            )
        if missing_fields and not allow_missing_nullable:
            raise ValueError(
                f"Schema drift policy 'fail' rejected missing contract columns: {[f.name for f in missing_fields]}"
            )
    elif drift_policy in ("drop_extra", "add_nulls"):
        if missing_non_nullable:
            raise ValueError(
                f"Missing non-nullable contract columns have no upstream source: {missing_non_nullable}"
            )
    else:
        raise ValueError(f"Unsupported driftPolicy: {drift_policy}")

    raw_shadow_exprs: list[Column] = [
        F.col(_quote_ident(c)).alias(f"__raw_{c}") for c in df.columns
    ]

    casted_exprs: list[Column] = []
    for field in target_schema.fields:
        if field.name in upstream_by_name:
            expr = _build_cast_expr(
                field=field,
                upstream_field=upstream_by_name[field.name],
                cast_mode=cast_mode,
                date_format=date_format,
                timestamp_format=timestamp_format,
            )
            casted_exprs.append(expr.alias(field.name))
        else:
            casted_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))

    retained_extra_exprs: list[Column] = (
        [F.col(_quote_ident(c)).alias(c) for c in extra_cols]
        if drift_policy == "add_nulls"
        else []
    )
    output_col_names = contract_names + (extra_cols if drift_policy == "add_nulls" else [])

    if cast_mode == "ansi_strict":
        strict_df = df.select(*(casted_exprs + retained_extra_exprs))
        for req_col in contract_names:
            if req_col in non_nullable_cols:
                strict_df = strict_df.withColumn(
                    f"__nn_assert_{req_col}",
                    F.assert_true(
                        F.col(_quote_ident(req_col)).isNotNull(),
                        F.lit(f"NOT NULL contract violation on column '{req_col}'"),
                    ),
                ).drop(f"__nn_assert_{req_col}")
        return {"valid": strict_df}

    violation_tags: list[Column] = []
    for field in target_schema.fields:
        casted_ref = F.col(_quote_ident(field.name))
        if field.name in upstream_by_name and cast_mode == "try_cast_dlq":
            raw_ref = F.col(_quote_ident(f"__raw_{field.name}"))
            violation_tags.append(
                F.when(
                    raw_ref.isNotNull() & casted_ref.isNull(),
                    F.lit(f"cast_failed:{field.name}->{field.dataType.simpleString()}"),
                )
            )
            if field.name in non_nullable_cols:
                violation_tags.append(
                    F.when(
                        raw_ref.isNull() & casted_ref.isNull(),
                        F.lit(f"null_violation:{field.name}"),
                    )
                )
        elif field.name in non_nullable_cols:
            violation_tags.append(
                F.when(
                    casted_ref.isNull(),
                    F.lit(f"null_violation:{field.name}"),
                )
            )

    staged_df = df.select(*(raw_shadow_exprs + casted_exprs + retained_extra_exprs))
    if violation_tags:
        failed_arr = F.array_compact(F.array(*violation_tags))
        error_col = F.when(
            F.size(failed_arr) > 0,
            F.concat_ws("; ", failed_arr),
        ).otherwise(F.lit(None).cast("string"))
    else:
        error_col = F.lit(None).cast("string")

    staged_df = staged_df.withColumn("_error", error_col)
    if cache_staged and not df.isStreaming:
        staged_df = staged_df.cache()

    valid_df = staged_df.filter(F.col("_error").isNull()).select(
        *[F.col(_quote_ident(c)) for c in output_col_names]
    )

    if emit_dlq_split:
        error_df = staged_df.filter(F.col("_error").isNotNull()).select(
            *[F.col(_quote_ident(f"__raw_{c}")).alias(c) for c in df.columns],
            F.col("_error"),
        )
        result: dict[str, DataFrame] = {"valid": valid_df, "error": error_df}
        if cache_staged and not df.isStreaming:
            result["__cached_staged__"] = staged_df
        return result

    return {"valid": valid_df}
```

## 6. Offline Unit Testing

Verify valid casts, ANSI-safe `F.try_cast` DLQ routing on malformed numbers and timestamps, `NOT NULL` contract violations, and `fail`, `drop_extra`, and `add_nulls` drift policies in `tests/test_pipeline.py`:

```python
import pytest
from pyspark.sql import SparkSession


def test_enforce_schema_try_cast_dlq_and_not_null(spark: SparkSession) -> None:
    """Routes malformed numeric or timestamp strings and NOT NULL violations to error DLQ."""
    raw_df = spark.createDataFrame(
        [
            ("101", "cust_1", "49.95", "2026-04-01 10:30:00", "debug_ok"),
            ("102", "cust_2", "not_a_number", "bad_ts", "debug_bad_cast"),
            ("103", None, "15.00", "2026-04-01 12:00:00", "debug_null_cust"),
        ],
        ["order_id", "customer_id", "amount", "event_ts", "extra_trace"],
    )
    cfg = {
        "schemaDDL": (
            "order_id BIGINT NOT NULL, customer_id STRING NOT NULL, "
            "amount DECIMAL(10, 2), event_ts TIMESTAMP"
        ),
        "driftPolicy": "drop_extra",
        "castMode": "try_cast_dlq",
        "timestampFormat": "yyyy-MM-dd HH:mm:ss",
    }

    out = enforce_schema_and_cast(raw_df, cfg, emit_dlq_split=True)
    valid_rows = out["valid"].collect()
    assert out["valid"].columns == ["order_id", "customer_id", "amount", "event_ts"]
    assert len(valid_rows) == 1
    assert valid_rows[0]["order_id"] == 101

    err_rows = {r["order_id"]: r for r in out["error"].collect()}
    assert set(err_rows.keys()) == {"102", "103"}
    assert "cast_failed:amount" in err_rows["102"]["_error"]
    assert "cast_failed:event_ts" in err_rows["102"]["_error"]
    assert err_rows["102"]["amount"] == "not_a_number"
    assert err_rows["103"]["_error"] == "null_violation:customer_id"


def test_enforce_schema_drift_policies(spark: SparkSession) -> None:
    """Verifies fail, drop_extra, and add_nulls schema drift enforcement."""
    raw_df = spark.createDataFrame(
        [("201", "20.50", "keep_me")],
        ["order_id", "amount", "upstream_extra"],
    )
    ddl = "order_id BIGINT NOT NULL, amount DOUBLE, discount DOUBLE"

    with pytest.raises(ValueError, match="unexpected extra columns"):
        enforce_schema_and_cast(raw_df, {"schemaDDL": ddl, "driftPolicy": "fail"})

    dropped = enforce_schema_and_cast(
        raw_df,
        {"schemaDDL": ddl, "driftPolicy": "drop_extra"},
    )["valid"]
    assert dropped.columns == ["order_id", "amount", "discount"]
    assert dropped.collect()[0]["discount"] is None

    added = enforce_schema_and_cast(
        raw_df,
        {"schemaDDL": ddl, "driftPolicy": "add_nulls"},
    )["valid"]
    assert added.columns == ["order_id", "amount", "discount", "upstream_extra"]
    row = added.collect()[0]
    assert row["discount"] is None
    assert row["upstream_extra"] == "keep_me"
```
