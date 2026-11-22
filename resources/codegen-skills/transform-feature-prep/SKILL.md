---
name: transform-feature-prep
description: Build and execute PySpark MLlib feature engineering pipelines with Imputer, StringIndexer, OneHotEncoder, VectorAssembler, and feature scalers in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, notebook]
  transforms: [FeaturePrep]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/ml-features.html
  validated: true
---

# Spark ML Feature Engineering Step (`FeaturePrep`)

Use these rules when generating PySpark {{spark_version}} code for a `FeaturePrep` step node inside `transform(dfs, config)`.

## 1. Pre-Trained `PipelineModel` vs On-the-Fly `Pipeline` Fit

- **Pre-fitted URI (`pretrainedPipelineUri` non-empty, such as `gs://my-bucket/models/feature_prep`)**:
  - Load the persisted feature pipeline via `PipelineModel.load(pretrained_pipeline_uri)` and call `.transform(df)` directly without refitting on inference batches.
- **Dynamic Stage Assembly (`pretrainedPipelineUri` empty)**:
  - Construct a `pyspark.ml.Pipeline(stages=stages)` in deterministic order and execute `Pipeline(stages=stages).fit(df).transform(df)`.

## 2. Stage Ordering & `handleInvalid` Safety

Assemble stages from `pyspark.ml.feature` in this exact sequence:

1. **Numeric Imputation (`imputeStrategy` in `{"mean", "median", "mode"}`)**:
   - Cast `numericColumns` to `double` if needed and add `Imputer(inputCols=numeric_cols, outputCols=imputed_cols, strategy=impute_strategy)`.
   - When `imputeStrategy == "none"`, pass `numericColumns` directly to `VectorAssembler`.
2. **Categorical Indexing (`categoricalColumns` non-empty)**:
   - Add `StringIndexer(inputCols=categorical_cols, outputCols=indexed_cols, handleInvalid=handle_invalid)` with default `handleInvalid="keep"` so unseen categories at scoring time map to an extra index bucket instead of aborting the job.
3. **One-Hot Encoding (`enableOneHotEncoding == True` and `categoricalColumns` non-empty)**:
   - Add `OneHotEncoder(inputCols=indexed_cols, outputCols=encoded_cols, handleInvalid=ohe_invalid)` where `ohe_invalid = "keep" if handle_invalid == "keep" else "error"` (`OneHotEncoder` supports `"keep"` and `"error"`).
4. **Vector Assembly (`VectorAssembler`)**:
   - Combine encoded categorical columns (or indexed columns when `enableOneHotEncoding` is `False`) and numeric/imputed columns via `VectorAssembler(inputCols=assembler_inputs, outputCol=assembled_col, handleInvalid=handle_invalid)`.
5. **Feature Scaling (`scalerType` in `{"standard", "minmax"}`)**:
   - When `scalerType == "standard"`, add `StandardScaler(inputCol=assembled_col, outputCol=output_features_col, withStd=True, withMean=False)` (`withMean=False` preserves sparse one-hot vectors without dense memory blowup).
   - When `scalerType == "minmax"`, add `MinMaxScaler(inputCol=assembled_col, outputCol=output_features_col)`.
   - When `scalerType == "none"`, set `VectorAssembler(outputCol=output_features_col)` directly.

## 3. Dependencies & Downstream Sink Compatibility

- Include `numpy>=1.26.0` in `requirements.txt` because `pyspark.ml.linalg` vectors depend on `numpy`.
- If a `FeaturePrep` output feeds directly into a tabular sink that rejects MLlib `VectorUDT` (such as BigQuery or Iceberg) without an intervening `SparkMlAndVertexAi` step, convert the vector column via `from pyspark.ml.functions import vector_to_array`.

## 4. Reference Implementation (`src/pipeline.py`)

```python
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    Imputer,
    MinMaxScaler,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_feature_prep(
    df: DataFrame,
    categorical_columns: list[str] | None = None,
    numeric_columns: list[str] | None = None,
    enable_one_hot_encoding: bool = True,
    impute_strategy: str = "none",
    scaler_type: str = "none",
    handle_invalid: str = "keep",
    output_features_column: str = "features",
    pretrained_pipeline_uri: str = "",
) -> DataFrame:
    """Fits or loads a pyspark.ml feature engineering pipeline and transforms df."""
    if pretrained_pipeline_uri.strip():
        return PipelineModel.load(pretrained_pipeline_uri.strip()).transform(df)

    cat_cols = [c.strip() for c in (categorical_columns or []) if c.strip()]
    num_cols = [c.strip() for c in (numeric_columns or []) if c.strip()]
    if not cat_cols and not num_cols:
        raise ValueError("FeaturePrep requires at least one categorical or numeric column.")

    working_df = df
    if num_cols:
        working_df = working_df.withColumns(
            {col_name: F.col(col_name).cast("double") for col_name in num_cols}
        )

    stages = []
    assembler_inputs: list[str] = []

    if cat_cols:
        indexed_cols = [f"__idx_{c}" for c in cat_cols]
        stages.append(
            StringIndexer(
                inputCols=cat_cols,
                outputCols=indexed_cols,
                handleInvalid=handle_invalid,
            )
        )
        if enable_one_hot_encoding:
            ohe_cols = [f"__ohe_{c}" for c in cat_cols]
            ohe_invalid = "keep" if handle_invalid == "keep" else "error"
            stages.append(
                OneHotEncoder(
                    inputCols=indexed_cols,
                    outputCols=ohe_cols,
                    handleInvalid=ohe_invalid,
                )
            )
            assembler_inputs.extend(ohe_cols)
        else:
            assembler_inputs.extend(indexed_cols)

    if num_cols:
        if impute_strategy in {"mean", "median", "mode"}:
            imputed_cols = [f"__imp_{c}" for c in num_cols]
            stages.append(
                Imputer(
                    inputCols=num_cols,
                    outputCols=imputed_cols,
                    strategy=impute_strategy,
                )
            )
            assembler_inputs.extend(imputed_cols)
        else:
            assembler_inputs.extend(num_cols)

    needs_scaler = scaler_type in {"standard", "minmax"}
    raw_vec_col = f"__raw_{output_features_column}" if needs_scaler else output_features_column
    stages.append(
        VectorAssembler(
            inputCols=assembler_inputs,
            outputCol=raw_vec_col,
            handleInvalid=handle_invalid,
        )
    )

    if scaler_type == "standard":
        stages.append(
            StandardScaler(
                inputCol=raw_vec_col,
                outputCol=output_features_column,
                withStd=True,
                withMean=False,
            )
        )
    elif scaler_type == "minmax":
        stages.append(
            MinMaxScaler(
                inputCol=raw_vec_col,
                outputCol=output_features_column,
            )
        )

    model = Pipeline(stages=stages).fit(working_df)
    return model.transform(working_df)
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

```python
from pyspark.sql import SparkSession
from src.pipeline import apply_feature_prep


def test_feature_prep_pipeline_with_nulls_and_categories(spark: SparkSession) -> None:
    """Verifies Imputer, StringIndexer, OneHotEncoder, VectorAssembler, and StandardScaler."""
    df = spark.createDataFrame(
        [
            ("US", "mobile", 10.0, 100.0),
            ("EU", "desktop", None, 250.0),
            ("APAC", "mobile", 30.0, 400.0),
        ],
        ["region", "device", "age", "spend"],
    )
    featurized_df = apply_feature_prep(
        df,
        categorical_columns=["region", "device"],
        numeric_columns=["age", "spend"],
        enable_one_hot_encoding=True,
        impute_strategy="mean",
        scaler_type="standard",
        handle_invalid="keep",
        output_features_column="features",
    )
    assert "features" in featurized_df.columns
    assert featurized_df.count() == 3
```
