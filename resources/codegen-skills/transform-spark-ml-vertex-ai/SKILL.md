---
name: transform-spark-ml-vertex-ai
description: Run distributed Spark ML PipelineModel scoring, XGBoost Spark inference, or batched Vertex AI Endpoint and Gemini 4 Hydrogen predictions in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative]
  transforms: [SparkMlAndVertexAi, SparkMlInference, VertexAiInference]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/ml-pipeline.html
    - https://docs.cloud.google.com/gemini-enterprise-agent-platform/overview
  validated: true
---

# Spark ML & Vertex AI Inference Step (`SparkMlAndVertexAi`)

Use these rules when generating PySpark {{spark_version}} code for `SparkMlAndVertexAi` (and aliases `SparkMlInference` and `VertexAiInference`) inside `transform(dfs, config)`.

## 1. Mode 1 — Spark MLlib `PipelineModel` (`engine == "spark_ml"`)

- Load the saved model from Cloud Storage or local storage via `PipelineModel.load(model_uri)` and score the DataFrame with `model.transform(df)`.
- **Tabular Sink Vector Conversion (`convertVectorsToArray == True`)**:
  - BigQuery, Apache Iceberg, CSV, and JSON sinks reject MLlib `VectorUDT` columns (`features`, `rawPrediction`, `probability`).
  - Import `from pyspark.ml.functions import vector_to_array` and convert MLlib vector columns to standard `ARRAY<DOUBLE>` columns in a single `df.withColumns({...})` pass before writing downstream.

## 2. Mode 2 — Distributed XGBoost on Spark (`engine == "xgboost"`)

- Import `SparkXGBClassifierModel` (when `xgboostTask == "classification"`) or `SparkXGBRegressorModel` (when `xgboostTask == "regression"`) from `xgboost.spark`.
- Load the model via `.load(model_uri)` and execute `model.transform(df)`.
- When `convertVectorsToArray` is `True` and `"probability"` or `"rawPrediction"` exists on the scored DataFrame, convert vector columns with `vector_to_array("probability")`.
- Add `xgboost>=2.1.0`, `scikit-learn>=1.4.0`, and `numpy>=1.26.0` to `requirements.txt`.

## 3. Mode 3 — Vertex AI Endpoint & Gemini Batched Inference (`vertex_ai_endpoint` / `vertex_ai_gemini`)

Execute remote model calls inside executor partitions via `df.mapInPandas(predict_batches, schema=...)` — never collect rows onto the driver and never use a row-by-row `@udf`:

- **Once-Per-Partition Client Initialization**:
  - Initialize the SDK client **once at the top of `predict_batches(pdf_iter)` outside the `for pdf in pdf_iter:` loop** so executors reuse HTTP/gRPC connection pools across all Arrow batches in the partition. Never instantiate a client per row or per batch.
- **Vertex AI Gemini (`engine == "vertex_ai_gemini"`)**:
  - Use the unified Google Gen AI SDK (`from google import genai`) and include `google-genai`, `pandas`, and `pyarrow` in `requirements.txt`.
  - Initialize `client = genai.Client(vertexai=True, project=project_id, location="global")` and default `geminiModel` to `"gemini-4-hydrogen"`.
  - **Strict Model & Endpoint Rule**: Always use `location="global"` for Gemini 3.x/4.x models such as `"gemini-4-hydrogen"` and `"gemini-3.8-flash"`. Never revert model names to `gemini-2.5*` or Gemini 1.x/2.x, and never point Gemini 3.x/4.x models at regional endpoints such as `us-central1` (`us-central1` returns HTTP 404).
  - Format prompts from `inputColumns` using single-brace Python templates (`prompt_template.format(**row_dict)`) and chunk requests according to `batchSize` with exponential backoff on HTTP `429` or `5xx` errors.
- **Vertex AI Online Prediction Endpoint (`engine == "vertex_ai_endpoint"`)**:
  - Import `from google.cloud import aiplatform` and include `google-cloud-aiplatform`, `pandas`, and `pyarrow` in `requirements.txt`.
  - Initialize `aiplatform.init(project=project_id, location=location)` (defaulting `location` to `"us-central1"` for custom tabular endpoints) and create `endpoint = aiplatform.Endpoint(endpoint_id)` once per partition before iterating `pdf_iter`.
  - Slice each Pandas DataFrame into chunks of `batchSize` (default `32`) and call `endpoint.predict(instances=chunk_records)` in a single batched RPC per chunk with bounded exponential backoff.

## 4. Dead-Letter Queue (`error` Handle) Split (`try_except_error_col`)

When `ExecutionPlan.dlqSplitPoints` includes this node (`mechanism: "try_except_error_col"`):

1. Catch transient or payload-level inference failures inside `predict_batches` and record the exception message in a nullable `_error: string` column while emitting `None` for `outputColumn` on failed rows.
2. Cache the scored DataFrame when listed in `ExecutionPlan.cachedNodeIds` so Spark does not invoke Vertex AI or ML scoring twice across valid and error branches.
3. Route `scored_df.filter(F.col("_error").isNull()).drop("_error")` to the primary output handle and `scored_df.filter(F.col("_error").isNotNull())` to the `error` DLQ branch.

## 5. Reference Implementation (`src/pipeline.py`)

```python
import json
import time
from typing import Any, Iterator
import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def score_spark_ml_model(
    df: DataFrame,
    model_uri: str,
    convert_vectors: bool = True,
) -> DataFrame:
    """Scores a DataFrame with a saved pyspark.ml PipelineModel."""
    scored_df = PipelineModel.load(model_uri).transform(df)
    if not convert_vectors:
        return scored_df

    vector_patches = {}
    for col_name in ("features", "rawPrediction", "probability"):
        if col_name in scored_df.columns:
            vector_patches[col_name] = vector_to_array(F.col(col_name))
    return scored_df.withColumns(vector_patches) if vector_patches else scored_df


def score_xgboost_spark_model(
    df: DataFrame,
    model_uri: str,
    xgboost_task: str = "classification",
    convert_vectors: bool = True,
) -> DataFrame:
    """Scores a DataFrame with SparkXGBClassifierModel or SparkXGBRegressorModel."""
    from xgboost.spark import SparkXGBClassifierModel, SparkXGBRegressorModel

    model_cls = (
        SparkXGBRegressorModel if xgboost_task == "regression" else SparkXGBClassifierModel
    )
    scored_df = model_cls.load(model_uri).transform(df)
    if convert_vectors and "probability" in scored_df.columns:
        scored_df = scored_df.withColumns({"probability": vector_to_array(F.col("probability"))})
    return scored_df


def score_vertex_ai_gemini(
    df: DataFrame,
    project_id: str,
    prompt_template: str,
    input_columns: list[str],
    output_column: str = "ai_response",
    gemini_model: str = "gemini-4-hydrogen",
    location: str = "global",
    batch_size: int = 32,
    enable_dlq: bool = False,
    cache_before_split: bool = True,
) -> DataFrame | tuple[DataFrame, DataFrame]:
    """Runs batched Gemini 4 Hydrogen inference via google-genai initialized once per partition."""
    existing_ddl = ", ".join(f"{field.name} {field.dataType.simpleString()}" for field in df.schema.fields)
    out_ddl = f"{existing_ddl}, {output_column} string, _error string"

    def predict_batches(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        from google import genai

        # Initialize Vertex AI Gen AI client ONCE per partition on the global endpoint
        client = genai.Client(vertexai=True, project=project_id, location=location)

        for pdf in pdf_iter:
            responses: list[str | None] = []
            errors: list[str | None] = []
            records = pdf[input_columns].to_dict(orient="records") if input_columns else [{}] * len(pdf)

            for start in range(0, len(records), max(1, batch_size)):
                chunk = records[start : start + max(1, batch_size)]
                for row_dict in chunk:
                    prompt_text = prompt_template.format(**row_dict)
                    attempt = 0
                    while True:
                        try:
                            resp = client.models.generate_content(
                                model=gemini_model,
                                contents=prompt_text,
                            )
                            responses.append(resp.text)
                            errors.append(None)
                            break
                        except Exception as exc:
                            if attempt >= 2:
                                if not enable_dlq:
                                    raise
                                responses.append(None)
                                errors.append(f"Gemini inference error: {exc}")
                                break
                            time.sleep(2**attempt)
                            attempt += 1

            out_pdf = pdf.copy()
            out_pdf[output_column] = responses
            out_pdf["_error"] = errors
            yield out_pdf

    scored_df = df.mapInPandas(predict_batches, schema=out_ddl)
    if not enable_dlq:
        return scored_df.drop("_error")

    working_df = scored_df.cache() if cache_before_split else scored_df
    valid_df = working_df.filter(F.col("_error").isNull()).drop("_error")
    error_df = working_df.filter(F.col("_error").isNotNull())
    return valid_df, error_df


def score_vertex_ai_endpoint(
    df: DataFrame,
    project_id: str,
    location: str,
    endpoint_id: str,
    input_columns: list[str],
    output_column: str = "ai_response",
    batch_size: int = 32,
    enable_dlq: bool = False,
    cache_before_split: bool = True,
) -> DataFrame | tuple[DataFrame, DataFrame]:
    """Runs batched Vertex AI Endpoint predictions initialized once per partition."""
    existing_ddl = ", ".join(f"{field.name} {field.dataType.simpleString()}" for field in df.schema.fields)
    out_ddl = f"{existing_ddl}, {output_column} string, _error string"

    def predict_batches(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        from google.cloud import aiplatform

        # Initialize Vertex AI Endpoint client ONCE per partition outside the batch loop
        aiplatform.init(project=project_id, location=location)
        endpoint = aiplatform.Endpoint(endpoint_id)

        for pdf in pdf_iter:
            preds_out: list[str | None] = []
            errors_out: list[str | None] = []
            records = pdf[input_columns].to_dict(orient="records")
            step_size = max(1, batch_size)

            for start in range(0, len(records), step_size):
                chunk = records[start : start + step_size]
                try:
                    prediction_resp = endpoint.predict(instances=chunk)
                    for item in prediction_resp.predictions:
                        preds_out.append(json.dumps(item))
                        errors_out.append(None)
                except Exception as exc:
                    if not enable_dlq:
                        raise
                    for _ in chunk:
                        preds_out.append(None)
                        errors_out.append(f"Vertex Endpoint error: {exc}")

            out_pdf = pdf.copy()
            out_pdf[output_column] = preds_out
            out_pdf["_error"] = errors_out
            yield out_pdf

    scored_df = df.mapInPandas(predict_batches, schema=out_ddl)
    if not enable_dlq:
        return scored_df.drop("_error")

    working_df = scored_df.cache() if cache_before_split else scored_df
    valid_df = working_df.filter(F.col("_error").isNull()).drop("_error")
    error_df = working_df.filter(F.col("_error").isNotNull())
    return valid_df, error_df
```

## 6. Offline Unit Testing (`tests/test_pipeline.py`)

Test `PipelineModel` scoring with local fixtures or mock `google.genai.Client` so `tests/test_pipeline.py` runs 100% offline without network calls:

```python
from unittest.mock import MagicMock, patch
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from src.pipeline import score_spark_ml_model, score_vertex_ai_gemini


def test_spark_ml_vector_to_array_offline(spark: SparkSession, tmp_path) -> None:
    """Verifies PipelineModel scoring and vector_to_array conversion offline."""
    train_df = spark.createDataFrame([(0.0, 1.0, 0.0), (1.0, 5.0, 1.0)], ["x1", "x2", "label"])
    pipeline = Pipeline(
        stages=[
            VectorAssembler(inputCols=["x1", "x2"], outputCol="features"),
            LogisticRegression(featuresCol="features", labelCol="label", maxIter=5),
        ]
    )
    model_dir = str(tmp_path / "lr_model")
    pipeline.fit(train_df).save(model_dir)

    scored_df = score_spark_ml_model(train_df.drop("label"), model_uri=model_dir, convert_vectors=True)
    row = scored_df.first()
    assert isinstance(row["probability"], list)


def test_vertex_ai_gemini_mocked_offline(spark: SparkSession) -> None:
    """Verifies batched Gemini inference with mocked google.genai.Client offline."""
    df = spark.createDataFrame([("1", "Great service")], ["id", "review_text"])
    fake_client = MagicMock()
    fake_client.models.generate_content.return_value.text = "POSITIVE"

    with patch("google.genai.Client", return_value=fake_client):
        valid_df, error_df = score_vertex_ai_gemini(
            df,
            project_id="demo-project",
            prompt_template="Classify review: {review_text}",
            input_columns=["review_text"],
            output_column="sentiment",
            gemini_model="gemini-4-hydrogen",
            location="global",
            enable_dlq=True,
            cache_before_split=False,
        )
        assert valid_df.first()["sentiment"] == "POSITIVE"
        assert error_df.count() == 0
```
