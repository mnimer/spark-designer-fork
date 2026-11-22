---
name: transform-nested-struct-array-ops
description: Flatten or pack nested StructType columns and apply zero-shuffle Catalyst higher-order array functions in a single projection pass in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [NestedStructArrayOps]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
    - https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#array-functions
  validated: true
---

# Zero-Shuffle Nested Struct & Higher-Order Array Operations (`NestedStructArrayOps`)

Use these rules when generating PySpark {{spark_version}} code for a `NestedStructArrayOps` (`EXT6`) step inside `transform(dfs, config)`.

## 1. Zero-Shuffle Catalyst Invariant (Strict Anti-Pattern Ban)

- **Never unnest and re-aggregate arrays**: Strictly forbid `df.select(F.explode(...)).groupBy(...).agg(F.collect_list(...))` when transforming, filtering, testing, or reducing elements inside an `ArrayType` column. Exploding and re-grouping multiplies row counts, forces a full cluster shuffle, drops parent rows with empty or `NULL` arrays unless outer-joined, and breaks element order.
- **Zero Python `@udf` loops on nested data**: Express struct flattening, struct packing, and array element transformations exclusively with Catalyst struct projections and built-in higher-order functions so execution remains inside Tungsten on Local Spark, Spark Connect, Dataproc Clusters, and Serverless Spark.
- **Single-pass projections**: Emit `df.select(*projections)` once for struct flattening and `df.withColumns({...})` once for struct packing or higher-order array rules. Never chain `.withColumn(...)` inside a Python loop.

## 2. Mode 1: Recursive Struct Flattening (`operationMode == "flatten_structs"`)

Walk `df.schema` (`StructType`) recursively up to `maxDepth` (`int >= 1`, default `3`) and build flat projections in a single `df.select(*projections)` pass:

- **Optional `structColumns` filter**: When `structColumns` is non-empty, flatten only top-level `StructType` columns listed in `structColumns` and pass unlisted columns through unchanged. When `structColumns` is empty, flatten every top-level `StructType` column.
- **Backtick path quoting & `separator` naming**: Reference nested fields safely with backtick-quoted path segments (`F.col("`parent`.`child`")`) and alias flattened columns using `separator` (default `'_'`, producing `"parent_child"`).
- **Depth ceiling (`maxDepth`)**: Stop recursion when `current_depth == maxDepth` so deeper nested structs remain intact as `StructType` columns at that level.
- **Retaining original structs (`dropOriginalStructs`)**: When `dropOriginalStructs` is `True` (default), omit the top-level `StructType` column from `projections`. When `False`, retain `F.col("`parent`")` immediately before its flattened child columns.

## 3. Mode 2: Struct Packing (`operationMode == "pack_struct"`)

Pack `sourceColumns` (`list[str]`) into a single nested `StructType` column named `outputStructColumn`:

- Build the struct expression with `F.struct(*[F.col(_quote_ident(src)).alias(src) for src in source_cols])` (or `F.named_struct(...)` when explicit alternating literal field names and computed expressions are required).
- Project the struct column via `df.withColumns({output_struct_col: struct_expr})`.
- When `dropPackedSourceColumns` is `True`, call `.drop(*[c for c in source_cols if c != output_struct_col])` so packed source columns are removed without accidentally dropping `outputStructColumn` when overwriting an existing column name.

## 4. Mode 3: Higher-Order Array Functions (`operationMode == "array_higher_order"`)

Evaluate all configured higher-order array rules (`arrayRules`) together in one `df.withColumns({...})` pass:

### A. Native PySpark 4.x `Column` Lambda Functions
When authoring direct PySpark transformations from concrete business logic, prefer typed `pyspark.sql.functions` higher-order functions:

- **`F.transform(col, lambda x: ...)`** (or positional `lambda x, i: ...`): Map each element (and optional 0-based index `i`) to a new value or struct.
- **`F.filter(col, lambda x: ...)`** (or `lambda x, i: ...`): Keep only array elements where the boolean predicate evaluates to `True`.
- **`F.exists(col, lambda x: ...)`**: Return `True` if any element satisfies the predicate.
- **`F.forall(col, lambda x: ...)`**: Return `True` if every element satisfies the predicate.
- **`F.aggregate(col, initial_value, merge_fn, finish_fn=None)`**: Fold array elements into a scalar or struct accumulator starting at `initial_value` without exploding rows.

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def enrich_order_items_hof(df: DataFrame) -> DataFrame:
    """Applies all five Catalyst higher-order array functions in one zero-shuffle pass."""
    return df.withColumns(
        {
            "normalized_tags": F.transform("tags", lambda x: F.lower(F.trim(x))),
            "high_value_items": F.filter("items", lambda x: x["qty"] * x["unit_price"] >= F.lit(100.0)),
            "has_backorder": F.exists("items", lambda x: x["backordered"] == F.lit(True)),
            "all_positive_qty": F.forall("items", lambda x: x["qty"] > F.lit(0)),
            "items_subtotal": F.aggregate(
                "items",
                F.lit(0.0),
                lambda acc, x: acc + (x["qty"] * x["unit_price"]),
                lambda acc: F.round(acc, 2),
            ),
        }
    )
```

### B. SQL Lambda Expressions (`F.expr`) from Step Configuration
When `arrayRules` supplies SQL lambda strings (`x -> upper(trim(x))`, `(acc, x) -> acc + x.amount`), compile each rule with `F.expr(...)` and pass the full dictionary to `df.withColumns(hof_projections)` once:

- `transform`: `F.expr(f"transform({q_src}, {lambda_expr})")`
- `filter`: `F.expr(f"filter({q_src}, {lambda_expr})")`
- `exists`: `F.expr(f"exists({q_src}, {lambda_expr})")`
- `forall`: `F.expr(f"forall({q_src}, {lambda_expr})")`
- `aggregate`: `F.expr(f"aggregate({q_src}, {init_expr}, {lambda_expr}, {finish_expr})")` when `finishLambdaExpr` is set, or `F.expr(f"aggregate({q_src}, {init_expr}, {lambda_expr})")` otherwise.

## 5. Reference Implementation

```python
from typing import Any
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def _quote_ident(name: str) -> str:
    """Quotes a column identifier with backticks for safe Catalyst path resolution."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def _walk_struct_fields(
    struct_type: StructType,
    path_parts: list[str],
    alias_parts: list[str],
    current_depth: int,
    max_depth: int,
    separator: str,
) -> list[Column]:
    """Recursively builds flat Column projections from nested StructType fields."""
    cols: list[Column] = []
    for sub_field in struct_type.fields:
        next_path = path_parts + [_quote_ident(sub_field.name)]
        next_alias = alias_parts + [sub_field.name]
        if isinstance(sub_field.dataType, StructType) and current_depth < max_depth:
            cols.extend(
                _walk_struct_fields(
                    struct_type=sub_field.dataType,
                    path_parts=next_path,
                    alias_parts=next_alias,
                    current_depth=current_depth + 1,
                    max_depth=max_depth,
                    separator=separator,
                )
            )
        else:
            cols.append(F.col(".".join(next_path)).alias(separator.join(next_alias)))
    return cols


def apply_nested_struct_array_ops(df: DataFrame, step_cfg: dict[str, Any]) -> DataFrame:
    """Executes flatten_structs, pack_struct, or array_higher_order in a single pass."""
    mode = str(step_cfg.get("operationMode", "flatten_structs")).strip().lower()

    if mode == "flatten_structs":
        separator = str(step_cfg.get("separator", "_"))
        if not separator:
            separator = "_"
        max_depth = int(step_cfg.get("maxDepth", 3))
        if max_depth < 1:
            raise ValueError("maxDepth must be at least 1.")
        drop_original = bool(step_cfg.get("dropOriginalStructs", True))
        struct_filter = {
            str(c).strip() for c in (step_cfg.get("structColumns") or []) if str(c).strip()
        }

        projections: list[Column] = []
        for field in df.schema.fields:
            should_flatten = isinstance(field.dataType, StructType) and (
                not struct_filter or field.name in struct_filter
            )
            if should_flatten:
                if not drop_original:
                    projections.append(F.col(_quote_ident(field.name)).alias(field.name))
                projections.extend(
                    _walk_struct_fields(
                        struct_type=field.dataType,
                        path_parts=[_quote_ident(field.name)],
                        alias_parts=[field.name],
                        current_depth=1,
                        max_depth=max_depth,
                        separator=separator,
                    )
                )
            else:
                projections.append(F.col(_quote_ident(field.name)).alias(field.name))
        return df.select(*projections)

    if mode == "pack_struct":
        output_col = str(step_cfg.get("outputStructColumn", "")).strip()
        source_cols = [
            str(c).strip() for c in (step_cfg.get("sourceColumns") or []) if str(c).strip()
        ]
        if not output_col or not source_cols:
            raise ValueError("pack_struct requires outputStructColumn and at least one source column.")
        drop_sources = bool(step_cfg.get("dropPackedSourceColumns", False))

        packed_df = df.withColumns(
            {
                output_col: F.struct(
                    *[F.col(_quote_ident(src)).alias(src) for src in source_cols]
                )
            }
        )
        if drop_sources:
            cols_to_drop = [c for c in source_cols if c != output_col]
            if cols_to_drop:
                packed_df = packed_df.drop(*cols_to_drop)
        return packed_df

    if mode == "array_higher_order":
        rules = step_cfg.get("arrayRules") or []
        hof_map: dict[str, Column] = {}
        for rule in rules:
            src_col = str(rule.get("sourceArrayColumn", "")).strip()
            target_col = str(rule.get("targetColumn", "")).strip()
            fn_type = str(rule.get("functionType", "transform")).strip().lower()
            lambda_expr = str(rule.get("lambdaExpression", "")).strip()
            if not src_col or not target_col or not lambda_expr:
                raise ValueError("Each array higher-order rule requires source, target, and lambda.")

            q_src = _quote_ident(src_col)
            if fn_type in ("transform", "filter", "exists", "forall"):
                hof_map[target_col] = F.expr(f"{fn_type}({q_src}, {lambda_expr})")
            elif fn_type == "aggregate":
                init_expr = str(rule.get("initialValueExpr", "")).strip()
                if not init_expr:
                    raise ValueError("aggregate rule requires initialValueExpr.")
                finish_expr = str(rule.get("finishLambdaExpr", "")).strip()
                if finish_expr:
                    hof_map[target_col] = F.expr(
                        f"aggregate({q_src}, {init_expr}, {lambda_expr}, {finish_expr})"
                    )
                else:
                    hof_map[target_col] = F.expr(
                        f"aggregate({q_src}, {init_expr}, {lambda_expr})"
                    )
            else:
                raise ValueError(f"Unsupported higher-order functionType: {fn_type}")
        return df.withColumns(hof_map) if hof_map else df

    raise ValueError(f"Unsupported NestedStructArrayOps operationMode: {mode}")
```

## 6. Offline Unit Testing

Verify multi-level struct flattening, struct packing, and all five higher-order array functions (`transform`, `filter`, `exists`, `forall`, `aggregate`) offline in `tests/test_pipeline.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def test_flatten_and_pack_structs_zero_shuffle(spark: SparkSession) -> None:
    """Verifies recursive struct flattening with maxDepth and struct packing."""
    schema = StructType.fromDDL(
        "order_id STRING, customer STRUCT<id: STRING, address: STRUCT<city: STRING, zip: STRING>>"
    )
    nested_df = spark.createDataFrame(
        [("o1", ("c1", ("Seattle", "98101")))],
        schema=schema,
    )

    flat_all = apply_nested_struct_array_ops(
        nested_df,
        {
            "operationMode": "flatten_structs",
            "separator": "_",
            "maxDepth": 2,
            "dropOriginalStructs": True,
        },
    )
    assert flat_all.columns == ["order_id", "customer_id", "customer_address_city", "customer_address_zip"]
    row = flat_all.collect()[0]
    assert row["customer_address_city"] == "Seattle"

    repacked = apply_nested_struct_array_ops(
        flat_all,
        {
            "operationMode": "pack_struct",
            "outputStructColumn": "shipping_loc",
            "sourceColumns": ["customer_address_city", "customer_address_zip"],
            "dropPackedSourceColumns": True,
        },
    )
    assert repacked.columns == ["order_id", "customer_id", "shipping_loc"]
    assert repacked.collect()[0]["shipping_loc"]["customer_address_zip"] == "98101"


def test_all_five_higher_order_array_functions(spark: SparkSession) -> None:
    """Verifies transform, filter, exists, forall, and aggregate without exploding rows."""
    schema = StructType.fromDDL(
        "order_id STRING, tags ARRAY<STRING>, prices ARRAY<DOUBLE>"
    )
    df = spark.createDataFrame(
        [("o1", ["  Promo ", "VIP"], [10.0, 25.5, 5.0])],
        schema=schema,
    )
    result = apply_nested_struct_array_ops(
        df,
        {
            "operationMode": "array_higher_order",
            "arrayRules": [
                {
                    "sourceArrayColumn": "tags",
                    "targetColumn": "clean_tags",
                    "functionType": "transform",
                    "lambdaExpression": "x -> lower(trim(x))",
                },
                {
                    "sourceArrayColumn": "prices",
                    "targetColumn": "premium_prices",
                    "functionType": "filter",
                    "lambdaExpression": "x -> x >= 10.0",
                },
                {
                    "sourceArrayColumn": "prices",
                    "targetColumn": "has_expensive",
                    "functionType": "exists",
                    "lambdaExpression": "x -> x > 20.0",
                },
                {
                    "sourceArrayColumn": "prices",
                    "targetColumn": "all_positive",
                    "functionType": "forall",
                    "lambdaExpression": "x -> x > 0.0",
                },
                {
                    "sourceArrayColumn": "prices",
                    "targetColumn": "total_price",
                    "functionType": "aggregate",
                    "initialValueExpr": "CAST(0.0 AS DOUBLE)",
                    "lambdaExpression": "(acc, x) -> acc + x",
                    "finishLambdaExpr": "acc -> round(acc, 1)",
                },
            ],
        },
    )
    out = result.collect()[0]
    assert out["clean_tags"] == ["promo", "vip"]
    assert out["premium_prices"] == [10.0, 25.5]
    assert out["has_expensive"] is True
    assert out["all_positive"] is True
    assert out["total_price"] == 40.5
```
