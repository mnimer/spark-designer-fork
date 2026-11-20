---
name: transform-text-chunk-embed
description: Split RAG document text into overlapping chunks and generate batched Vertex AI embeddings via mapInPandas in PySpark 4.x.
metadata:
  kind: transform
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [TextChunkAndEmbed]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.mapInPandas.html
    - https://docs.cloud.google.com/gemini-enterprise-agent-platform/overview
  validated: true
---

# Text Chunk & Vertex AI Embed Step (`TextChunkAndEmbed`)

Use these rules when generating PySpark {{spark_version}} code for `TextChunkAndEmbed` inside `transform(dfs, config)`.

## 1. Execution Modes and Packaging (`requirements.txt`)

`TextChunkAndEmbed` supports three modes configured via `mode`:

1. **`mode == "chunk_only"`**:
   - Splits `sourceTextColumn` into overlapping text segments on executors via `df.mapInPandas(...)` using pure Python string and regex logic.
   - Emits `chunkIdColumn` (`string`), `chunkIndexColumn` (`int`), and `chunkTextColumn` (`string`) while preserving all upstream metadata columns.
   - Requires `pandas` and `pyarrow` in `requirements.txt` with zero external LangChain dependencies and zero network calls.
2. **`mode == "embed_only"`**:
   - Preserves one output row per input row and embeds `sourceTextColumn` in batches of `batchSize` (`1..250`, default `64`) via the unified Google Gen AI SDK (`google-genai`), appending `embeddingColumn` (`array<float>`).
   - Add `google-genai`, `pandas`, and `pyarrow` to `requirements.txt`.
3. **`mode == "chunk_and_embed"` (default)**:
   - Splits each document row into overlapping chunks and generates batched Vertex AI embeddings for `chunkTextColumn` inside a single `df.mapInPandas(...)` pass so rows cross the JVM-to-Python Arrow boundary only once.
   - Add `google-genai`, `pandas`, and `pyarrow` to `requirements.txt`.

## 2. Pure Python Chunking Rules (`chunk_only` and `chunk_and_embed`)

- **Validate Overlap Bounds Strictly**:
  - Enforce `chunkSize > 0` and `0 <= chunkOverlap < chunkSize` before processing partitions. If `chunkOverlap < 0` or `chunkOverlap >= chunkSize`, raise a `ValueError` immediately so sliding windows always advance forward.
- **Supported `chunkStrategy` Algorithms**:
  - `"recursive_character"` (default): Split hierarchically across paragraph, newline, sentence, word, and character separators (`"\n\n"`, `"\n"`, `". "`, `" "`, `""`) to keep semantic units together up to `chunkSize` while carrying `chunkOverlap` trailing characters into the next chunk.
  - `"fixed_chars"`: Slice text with fixed window length `chunkSize` and stride `chunkSize - chunkOverlap`.
  - `"sentence"`: Split text on sentence boundaries (`(?<=[.!?])\s+`) and pack consecutive sentences up to `chunkSize` while retaining `chunkOverlap` trailing context characters across adjacent chunks.
- **Deterministic Chunk Identifiers**:
  - Read `doc_id = row[document_id_column]` (default `"doc_id"`) and assign deterministic IDs `f"{doc_id}_{chunk_idx}"` in `chunkIdColumn` (default `"chunk_id"`) alongside `0`-based `chunkIndexColumn` (`int`, default `"chunk_index"`).

## 3. Batched Vertex AI / Gemini Embeddings (`embed_only` and `chunk_and_embed`)

- **Unified `google-genai` SDK Only**:
  - Import `from google import genai` and `from google.genai import types`.
  - Never import deprecated `vertexai.language_models`, `TextEmbeddingModel`, `textembedding-gecko*`, `gemini-1.*`, `gemini-2.0*`, or `gemini-2.5*`.
- **Initialize Client Once Per Spark Partition**:
  - Instantiate `client = genai.Client(vertexai=True, project=project_id, location=location)` **once at the top of the `mapInPandas` generator function outside `for pdf in pdf_iter:`**.
  - Default `location` to `"global"` and `embeddingModel` to `"gemini-embedding-001"` (also supporting `"text-embedding-005"`).
- **Batched `embed_content` Calls with Retry Backoff**:
  - Clamp `batchSize` to `1..250` (default `64`).
  - Call `client.models.embed_content(model=embedding_model, contents=batch_texts, config=types.EmbedContentConfig(task_type=task_type, output_dimensionality=output_dimensionality))`.
  - Extract `[float(v) for v in emb.values]` from `response.embeddings` into `embeddingColumn` (`array<float>`) with bounded exponential backoff (`2**attempt`) on HTTP `429` or `5xx` errors.

## 4. Dead-Letter Queue (`error` Handle) Split (`try_except_error_col`)

When `ExecutionPlan.dlqSplitPoints` includes this step (`mechanism: "try_except_error_col"`):

1. Catch chunking or embedding exceptions inside `mapInPandas`, setting `embeddingColumn` to `None` on failed rows and recording the exception text in `_error: string`.
2. Cache the output DataFrame when listed in `ExecutionPlan.cachedNodeIds` so Spark does not invoke Vertex AI embedding RPCs twice across valid and dead-letter branches.
3. Route `out_df.filter(F.col("_error").isNull()).drop("_error")` to the main output branch and `out_df.filter(F.col("_error").isNotNull())` to the `error` DLQ branch.

## 5. Reference Implementation (`src/pipeline.py`)

```python
import re
import time
from typing import Any, Iterator
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def split_text_chunks(
    text: str,
    strategy: str = "recursive_character",
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
) -> list[str]:
    """Splits a document string into overlapping chunks without external RAG libraries."""
    if chunk_size <= 0:
        raise ValueError("chunkSize must be greater than 0")
    if chunk_overlap < 0 or chunk_overlap >= chunk_size:
        raise ValueError("chunkOverlap must be >= 0 and strictly less than chunkSize")
    if not text or not text.strip():
        return []

    stride = chunk_size - chunk_overlap
    if strategy == "fixed_chars":
        chunks: list[str] = []
        start = 0
        while start < len(text):
            piece = text[start : start + chunk_size]
            if piece:
                chunks.append(piece)
            if start + chunk_size >= len(text):
                break
            start += stride
        return chunks

    if strategy == "sentence":
        units = [s for s in SENTENCE_SPLIT_RE.split(text.strip()) if s]
        joiner = " "
    else:
        # recursive_character: split paragraphs and lines into semantic units first
        raw_parts = re.split(r"(\n\n|\n|(?<=[.!?])\s+)", text)
        units = [p for p in raw_parts if p]
        joiner = ""

    packed: list[str] = []
    current = ""
    for unit in units:
        candidate = f"{current}{joiner}{unit}" if current and joiner else f"{current}{unit}"
        if len(candidate) <= chunk_size:
            current = candidate
            continue

        if current:
            packed.append(current)
            tail = current[-chunk_overlap:] if chunk_overlap > 0 else ""
        else:
            tail = ""

        combined = f"{tail}{unit}"
        while len(combined) > chunk_size:
            packed.append(combined[:chunk_size])
            combined = combined[stride:]
        current = combined

    if current.strip():
        packed.append(current)
    return packed


def run_text_chunk_and_embed(
    df: DataFrame,
    mode: str = "chunk_and_embed",
    source_text_column: str = "text",
    document_id_column: str = "doc_id",
    chunk_strategy: str = "recursive_character",
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    chunk_text_column: str = "chunk_text",
    chunk_id_column: str = "chunk_id",
    chunk_index_column: str = "chunk_index",
    project_id: str = "",
    location: str = "global",
    embedding_model: str = "gemini-embedding-001",
    task_type: str = "RETRIEVAL_DOCUMENT",
    output_dimensionality: int = 768,
    batch_size: int = 64,
    embedding_column: str = "embedding",
    enable_dlq: bool = False,
    cache_before_split: bool = True,
) -> DataFrame | tuple[DataFrame, DataFrame]:
    """Executes chunk_only, embed_only, or chunk_and_embed via Spark mapInPandas."""
    do_chunk = mode in ("chunk_only", "chunk_and_embed")
    do_embed = mode in ("embed_only", "chunk_and_embed")

    if do_chunk:
        if chunk_size <= 0:
            raise ValueError("chunkSize must be greater than 0")
        if chunk_overlap < 0 or chunk_overlap >= chunk_size:
            raise ValueError("chunkOverlap must satisfy 0 <= chunkOverlap < chunkSize")

    clamped_batch = min(250, max(1, int(batch_size)))
    passthrough_fields = [
        field
        for field in df.schema.fields
        if not (
            do_chunk
            and field.name in {chunk_id_column, chunk_index_column, chunk_text_column}
        )
        and not (do_embed and field.name == embedding_column)
        and field.name != "_error"
    ]
    ddl_parts = [f"{f.name} {f.dataType.simpleString()}" for f in passthrough_fields]
    if do_chunk:
        ddl_parts.extend(
            [
                f"{chunk_id_column} string",
                f"{chunk_index_column} int",
                f"{chunk_text_column} string",
            ]
        )
    if do_embed:
        ddl_parts.append(f"{embedding_column} array<float>")
    ddl_parts.append("_error string")
    out_ddl = ", ".join(ddl_parts)
    keep_col_names = [f.name for f in passthrough_fields]

    def process_partitions(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        client = None
        embed_cfg = None
        if do_embed:
            from google import genai
            from google.genai import types

            # Initialize Vertex AI Gen AI client ONCE per Spark partition
            client = genai.Client(
                vertexai=True,
                project=project_id,
                location=location,
            )
            embed_cfg = types.EmbedContentConfig(
                task_type=task_type,
                output_dimensionality=output_dimensionality,
            )

        def embed_batch_with_retry(texts: list[str]) -> tuple[list[list[float] | None], str | None]:
            attempt = 0
            while True:
                try:
                    resp = client.models.embed_content(
                        model=embedding_model,
                        contents=texts,
                        config=embed_cfg,
                    )
                    vecs = [[float(v) for v in item.values] for item in resp.embeddings]
                    return vecs, None
                except Exception as exc:
                    if attempt >= 2:
                        if not enable_dlq:
                            raise
                        return [None] * len(texts), f"Embedding error: {exc}"
                    time.sleep(2**attempt)
                    attempt += 1

        for pdf in pdf_iter:
            expanded_rows: list[dict[str, Any]] = []
            if do_chunk:
                for _, src_row in pdf.iterrows():
                    base = {col: src_row[col] for col in keep_col_names}
                    raw_text = src_row[source_text_column]
                    doc_id_val = src_row[document_id_column]
                    try:
                        pieces = split_text_chunks(
                            "" if pd.isna(raw_text) else str(raw_text),
                            strategy=chunk_strategy,
                            chunk_size=chunk_size,
                            chunk_overlap=chunk_overlap,
                        )
                        for idx, piece in enumerate(pieces):
                            rec = dict(base)
                            rec[chunk_id_column] = f"{doc_id_val}_{idx}"
                            rec[chunk_index_column] = idx
                            rec[chunk_text_column] = piece
                            rec["_error"] = None
                            expanded_rows.append(rec)
                    except Exception as exc:
                        if not enable_dlq:
                            raise
                        rec = dict(base)
                        rec[chunk_id_column] = f"{doc_id_val}_0"
                        rec[chunk_index_column] = 0
                        rec[chunk_text_column] = None
                        rec["_error"] = f"Chunking error: {exc}"
                        expanded_rows.append(rec)
            else:
                for _, src_row in pdf.iterrows():
                    rec = {col: src_row[col] for col in keep_col_names}
                    rec["_error"] = None
                    expanded_rows.append(rec)

            if not expanded_rows:
                empty_cols = list(keep_col_names)
                if do_chunk:
                    empty_cols.extend([chunk_id_column, chunk_index_column, chunk_text_column])
                if do_embed:
                    empty_cols.append(embedding_column)
                empty_cols.append("_error")
                yield pd.DataFrame(columns=empty_cols)
                continue

            out_pdf = pd.DataFrame(expanded_rows)
            if do_embed:
                target_text_col = chunk_text_column if do_chunk else source_text_column
                embeddings_out: list[list[float] | None] = [None] * len(out_pdf)
                errors_out: list[str | None] = list(out_pdf["_error"])

                valid_indices = [
                    i
                    for i, err in enumerate(errors_out)
                    if err is None and pd.notna(out_pdf.iloc[i][target_text_col])
                ]
                for start in range(0, len(valid_indices), clamped_batch):
                    batch_idx = valid_indices[start : start + clamped_batch]
                    batch_texts = [str(out_pdf.iloc[i][target_text_col]) for i in batch_idx]
                    vecs, batch_err = embed_batch_with_retry(batch_texts)
                    for pos, row_i in enumerate(batch_idx):
                        embeddings_out[row_i] = vecs[pos]
                        if batch_err is not None:
                            errors_out[row_i] = batch_err

                out_pdf[embedding_column] = embeddings_out
                out_pdf["_error"] = errors_out

            yield out_pdf

    result_df = df.mapInPandas(process_partitions, schema=out_ddl)
    if not enable_dlq:
        return result_df.drop("_error")

    working_df = result_df.cache() if cache_before_split else result_df
    valid_df = working_df.filter(F.col("_error").isNull()).drop("_error")
    error_df = working_df.filter(F.col("_error").isNotNull())
    return valid_df, error_df
```

## 6. Offline Unit Testing (`tests/test_pipeline.py`)

Test `chunk_only` directly and mock `google.genai.Client` for `chunk_and_embed` plus DLQ routing so `tests/test_pipeline.py` runs 100% offline:

```python
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import pytest
from pyspark.sql import SparkSession
from src.pipeline import run_text_chunk_and_embed


def test_chunk_only_splits_with_overlap_offline(spark: SparkSession) -> None:
    """Verifies chunk_only produces deterministic chunk IDs and rejects invalid overlap."""
    df = spark.createDataFrame(
        [("docA", "First sentence here. Second sentence follows. Third sentence ends.")],
        ["doc_id", "text"],
    )
    chunked_df = run_text_chunk_and_embed(
        df,
        mode="chunk_only",
        chunk_strategy="sentence",
        chunk_size=40,
        chunk_overlap=10,
    )
    rows = chunked_df.orderBy("chunk_index").collect()
    assert len(rows) >= 2
    assert rows[0]["chunk_id"] == "docA_0"
    assert rows[1]["chunk_id"] == "docA_1"

    with pytest.raises(ValueError, match="chunkOverlap"):
        run_text_chunk_and_embed(df, mode="chunk_only", chunk_size=100, chunk_overlap=100)


def test_chunk_and_embed_with_dlq_mocked_offline(spark: SparkSession) -> None:
    """Verifies batched google-genai embed_content and DLQ error routing offline."""
    df = spark.createDataFrame([("docB", "Spark RAG embedding test.")], ["doc_id", "text"])

    fake_client = MagicMock()
    fake_client.models.embed_content.return_value = SimpleNamespace(
        embeddings=[SimpleNamespace(values=[0.1, 0.2, 0.3])]
    )

    with patch("google.genai.Client", return_value=fake_client):
        valid_df, error_df = run_text_chunk_and_embed(
            df,
            mode="chunk_and_embed",
            project_id="rag-demo",
            location="global",
            embedding_model="gemini-embedding-001",
            output_dimensionality=3,
            batch_size=64,
            enable_dlq=True,
            cache_before_split=False,
        )
        row = valid_df.first()
        assert row["chunk_id"] == "docB_0"
        assert len(row["embedding"]) == 3
        assert error_df.count() == 0
```
