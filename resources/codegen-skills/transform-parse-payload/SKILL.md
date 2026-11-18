---
name: transform-parse-payload
description: PySpark 4.x structured payload parsing using F.from_json, from_avro, F.from_csv, and native F.from_xml with explicit schemas and _corrupt_record dead-letter routing.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [ParsePayload, ParseMessage]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.avro.functions.from_avro.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_csv.html
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_xml.html
  validated: true
---

# PySpark 4.x ParsePayload Transformation (`ParsePayload`, `ParseMessage`)

Use these rules when generating PySpark 4.x code for `ParsePayload` (`OP5`) or legacy `ParseMessage` step nodes inside `transform(dfs, config)`.

## 1. Explicit Schema & Runtime Package Rules

- **Mandatory explicit schema**: Streaming and batch payload deserialization (`F.from_json`, `from_avro`, `F.from_csv`, `F.from_xml`) must always declare an explicit `StructType`, Spark SQL DDL string, or Avro JSON schema. Never call `spark.read.json(df.rdd.map(...))` to infer schemas at runtime.
- **Avro runtime package (`format == "avro"`)**: `pyspark.sql.avro.functions.from_avro` lives in the external `spark-avro` module. Attach `org.apache.spark:spark-avro_2.13:{{spark_version}}` via `--packages` / `spark.jars.packages` across Local, Dataproc Cluster, and GCP Serverless Spark runner scripts whenever `ParsePayload` sets `format: "avro"`.
- **Native Spark 4.0 XML (`format == "xml"`)**: Use built-in `F.from_xml` from `pyspark.sql.functions`. Never add `com.databricks:spark-xml` to Maven packages in Spark 4.x.
- **Legacy `ParseMessage` compatibility**: When a legacy `ParseMessage` node sets `messageType == "Base64"`, decode via `F.unbase64(F.col(source_col))`. When `messageType == "String"`, decode via `F.col(source_col).cast("string")`. Otherwise treat `ParseMessage` identically to JSON `ParsePayload`.

## 2. Deserializer Patterns by `format`

Read `format` (`"json" | "avro" | "csv" | "xml"`, default `"json"`), `sourceColumn` (default `"value"`), `schemaDDL`, `flattenFields` (default `True`), `targetColumn` (default `"parsed_payload"`), `rowTag` (for XML), `corruptRecordColumn` (default `"_corrupt_record"`), and `keepSourceColumn` (default `False`):

1. **JSON (`format == "json"`)**:
   - Include `f"{corrupt_col} STRING"` in the DDL or `StructField(corrupt_col, StringType(), True)` in `StructType`.
   - Parse UTF-8 text (`F.col(source_col).cast("string")`) with:
     `F.from_json(F.col(source_col).cast("string"), schema, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": corrupt_col})`.
2. **Avro (`format == "avro"`)**:
   - Import `from pyspark.sql.avro.functions import from_avro`.
   - Parse binary payload bytes (`F.col(source_col)`) with:
     `from_avro(F.col(source_col), avro_json_schema, {"mode": "PERMISSIVE"})`.
   - Derive `corrupt_col` for malformed binary payloads where `F.col(source_col).isNotNull() & F.col("_parsed").isNull()` by storing `F.base64(F.col(source_col))` in `corrupt_col`.
3. **CSV (`format == "csv"`)**:
   - Pass a Spark SQL DDL schema string (including `f"{corrupt_col} STRING"`) to:
     `F.from_csv(F.col(source_col).cast("string"), ddl_schema, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": corrupt_col})`.
4. **XML (`format == "xml"`)**:
   - Include `StructField(corrupt_col, StringType(), True)` in the `StructType` or DDL schema.
   - Build options `{"mode": "PERMISSIVE", "columnNameOfCorruptRecord": corrupt_col}` (plus `"rowTag": row_tag` when `rowTag` is configured) and call:
     `F.from_xml(F.col(source_col).cast("string"), schema, xml_options)`.

## 3. Flattening, Column Retention, and `_corrupt_record` DLQ Split

### Without Connected DLQ `error` Handle

- Parse into a temporary struct column `_parsed` (or directly into `targetColumn` when `flattenFields` is `False`).
- When `flattenFields` is `True`, project upstream metadata columns (keeping `sourceColumn` only when `keepSourceColumn` is `True`) plus `F.col("_parsed.*")`, and drop `corrupt_col` if not needed downstream.

### With Connected DLQ `error` Handle (`mechanism: "_corrupt_record"`)

When `ExecutionPlan.dlqSplitPoints` lists this `ParsePayload` node:
1. **Cache in batch mode**: If `node_id` is in `ExecutionPlan.cachedNodeIds` (batch execution), call `.cache()` on the parsed intermediate DataFrame before splitting valid and error branches, and `.unpersist()` in `write_sinks()`.
2. **Valid branch**: Keep rows where `F.col(corrupt_col).isNull()`, drop `corrupt_col`, and drop `sourceColumn` when `keepSourceColumn` is `False`.
3. **Error DLQ branch**: Keep rows where `F.col(corrupt_col).isNotNull()` so malformed payloads flow to the dead-letter sink with `corrupt_col` preserved:

```python
from typing import Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ORDER_EVENT_DDL = (
    "order_id STRING, customer_id STRING, amount DOUBLE, "
    "event_timestamp TIMESTAMP, _corrupt_record STRING"
)


def transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]:
    """Parses raw JSON payloads and splits malformed records to the error DLQ branch."""
    out: dict[str, DataFrame] = dict(dfs)
    raw_df = out["src_kafka"]
    corrupt_col = "_corrupt_record"

    parsed_df = raw_df.withColumns(
        {
            "_parsed": F.from_json(
                F.col("value").cast("string"),
                ORDER_EVENT_DDL,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": corrupt_col},
            )
        }
    ).select(
        *[F.col(c) for c in raw_df.columns if c != "value"],
        F.col("value").alias("_raw_value"),
        F.col("_parsed.*"),
    )

    # Apply .cache() when ExecutionPlan.cachedNodeIds includes 'step_parse' (batch mode)
    valid_df = parsed_df.filter(F.col(corrupt_col).isNull()).drop(corrupt_col, "_raw_value")
    error_df = parsed_df.filter(F.col(corrupt_col).isNotNull())

    out["step_parse"] = valid_df
    out["step_parse__valid"] = valid_df
    out["step_parse__error"] = error_df
    return out
```
