---
name: io-vector-store
description: Write embedding DataFrames in PySpark 4.x to AlloyDB pgvector or Vertex AI Vector Search in batch and Structured Streaming pipelines.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, notebook]
  transforms: [VectorStoreSink]
  preloadWhen: []
  sources:
    - prompt
    - https://cloud.google.com/alloydb/docs/ai/store-embeddings
    - https://cloud.google.com/vertex-ai/docs/vector-search/upsert-datapoints
  validated: true
---

# Vector Store Sink (`VectorStoreSink`)

Use these rules when generating PySpark {{spark_version}} code for `VectorStoreSink` nodes inside `write_sinks(dfs, config)`.

## 1. Backend Selection, Dependencies, and Runtime Properties

`VectorStoreSink` supports two vector indexing backends selected by `backend`:

1. **AlloyDB for PostgreSQL `pgvector` (`backend == "alloydb_pgvector"`, default)**:
   - Add `google-cloud-alloydb-connector` and `psycopg[binary]` to `requirements.txt`.
   - **Mandatory on Managed Spark (`ServerlessSpark` and `DataprocCluster`)**: Always pass `--properties="dataproc.artifacts.remove=conscrypt"` in `run_serverless_spark.sh` and `run_dataproc_cluster.sh`. Removing the bundled Conscrypt provider prevents TLS handshake failures when connecting to AlloyDB instances.
   - If `driverJarUri` or `socketFactoryJarUri` is configured for companion JVM JDBC operations, attach those Cloud Storage JARs via `--jars` or `spark.jars`.
2. **Vertex AI Vector Search (`backend == "vertex_vector_search"`)**:
   - When `writeMode == "stream_upsert"`, add `google-cloud-aiplatform` to `requirements.txt`.
   - When `writeMode == "gcs_batch_json"`, use Spark's built-in JSON writer to export Vertex AI Vector Search JSONL files to Cloud Storage (`gcsOutputUri`) with zero extra Python packages.

## 2. Backend 1 — AlloyDB `pgvector` Upsert (`backend == "alloydb_pgvector"`)

- **Resolve Credentials Once on the Driver**:
  - When `authMode == "secret_manager"`, fetch the database password once on the Spark driver via `get_secret(config["secret_name"])` before launching `df.rdd.foreachPartition(...)`. Never call Secret Manager inside partition or row loops.
  - When `authMode == "iam"`, set `enable_iam_auth=True` on `Connector.connect(...)` and omit password secrets.
- **Validate SQL Identifiers Strictly**:
  - Validate `schemaName`, `table`, `idColumn`, `embeddingColumn`, `contentColumn`, and every column in `metadataColumns` against `^[A-Za-z_][A-Za-z0-9_]*$` before interpolating identifiers into SQL strings. Validate `vectorDimensions` and `batchSize` as positive integers (`> 0`).
- **Initialize One Connection Per Partition (`foreachPartition`)**:
  - Instantiate `google.cloud.alloydb.connector.Connector` and open one `psycopg` connection at the top of `upsert_partition(rows)` inside a `try ... finally` block so all rows in the partition share one TLS connection.
- **Format `pgvector` Literals and Batch `ON CONFLICT` Upserts**:
  - Serialize each `ARRAY<FLOAT>` embedding list as a compact vector string `"[0.125,-0.45,0.88]"` and cast the parameter placeholder with `%s::vector(<vectorDimensions>)`.
  - Group partition rows into chunks of `batchSize` (default `500`).
  - Execute `INSERT INTO <schema>.<table> (...) VALUES (...) ON CONFLICT (<idColumn>) DO UPDATE SET ...` when `upsertConflictAction == "do_update"`, or `ON CONFLICT (<idColumn>) DO NOTHING` when `upsertConflictAction == "do_nothing"`. Commit after each chunk.

```python
import base64
import json
import re
from typing import Any, Iterable, Iterator
import urllib.request
import google.auth
import google.auth.transport.requests
from pyspark.sql import DataFrame, Row

SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_sql_identifier(identifier: str, field_label: str) -> str:
    """Validates a PostgreSQL identifier to prevent SQL injection."""
    cleaned = identifier.strip()
    if not SQL_IDENT_RE.match(cleaned):
        raise ValueError(f"Invalid SQL identifier for {field_label}: {identifier!r}")
    return f'"{cleaned}"'


def get_secret(secret_version_name: str) -> str:
    """Fetches a UTF-8 secret payload from Google Cloud Secret Manager once on the driver."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    url = f"https://secretmanager.googleapis.com/v1/{secret_version_name}:access"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {credentials.token}"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return base64.b64decode(payload["payload"]["data"]).decode("utf-8")


def format_pgvector_literal(values: Iterable[float]) -> str:
    """Formats a numeric sequence as a pgvector literal string."""
    return "[" + ",".join(f"{float(v):.8g}" for v in values) + "]"


def write_alloydb_pgvector_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Upserts an embedding DataFrame into AlloyDB pgvector once per partition."""
    instance_uri = config["alloydb_instance_uri"]
    ip_type_raw = config.get("ip_type", "PRIVATE").upper()
    database = config["database"]
    db_user = config["db_user"]
    auth_mode = config.get("auth_mode", "iam")
    use_iam = auth_mode == "iam"
    # Resolve Secret Manager password ONCE on the driver before foreachPartition
    db_password = None if use_iam else get_secret(config["secret_name"])

    schema_q = validate_sql_identifier(config.get("schema_name", "public"), "schemaName")
    table_q = validate_sql_identifier(config["table"], "table")
    id_col_raw = config.get("id_column", "chunk_id").strip()
    emb_col_raw = config.get("embedding_column", "embedding").strip()
    content_col_raw = config.get("content_column", "chunk_text").strip()
    meta_cols_raw = [
        c.strip()
        for c in config.get("metadata_columns", [])
        if isinstance(c, str) and c.strip()
    ]

    id_col_q = validate_sql_identifier(id_col_raw, "idColumn")
    emb_col_q = validate_sql_identifier(emb_col_raw, "embeddingColumn")
    include_content = bool(content_col_raw) and content_col_raw in df.columns
    content_col_q = (
        validate_sql_identifier(content_col_raw, "contentColumn") if include_content else None
    )
    meta_cols = [c for c in meta_cols_raw if c in df.columns]
    meta_cols_q = [validate_sql_identifier(c, "metadataColumns") for c in meta_cols]

    vector_dims = int(config.get("vector_dimensions", 768))
    if vector_dims <= 0:
        raise ValueError("vectorDimensions must be a positive integer")
    batch_size = max(1, int(config.get("batch_size", 500)))
    conflict_action = config.get("upsert_conflict_action", "do_update")

    insert_cols_q = [id_col_q, emb_col_q]
    row_placeholder_parts = ["%s", f"%s::vector({vector_dims})"]
    if content_col_q:
        insert_cols_q.append(content_col_q)
        row_placeholder_parts.append("%s")
    for meta_q in meta_cols_q:
        insert_cols_q.append(meta_q)
        row_placeholder_parts.append("%s")

    row_tuple_sql = "(" + ", ".join(row_placeholder_parts) + ")"
    cols_joined = ", ".join(insert_cols_q)
    if conflict_action == "do_nothing":
        conflict_tail = f"ON CONFLICT ({id_col_q}) DO NOTHING"
    else:
        updatable_cols = [c for c in insert_cols_q if c != id_col_q]
        update_assignments = ", ".join(f"{col} = EXCLUDED.{col}" for col in updatable_cols)
        conflict_tail = f"ON CONFLICT ({id_col_q}) DO UPDATE SET {update_assignments}"

    projected_cols = [id_col_raw, emb_col_raw]
    if include_content:
        projected_cols.append(content_col_raw)
    projected_cols.extend(meta_cols)

    def upsert_partition(rows: Iterator[Row]) -> None:
        from google.cloud.alloydb.connector import Connector, IPTypes

        ip_map = {
            "PRIVATE": IPTypes.PRIVATE,
            "PUBLIC": IPTypes.PUBLIC,
            "PSC": IPTypes.PSC,
        }
        connector = Connector(ip_type=ip_map.get(ip_type_raw, IPTypes.PRIVATE))
        conn = None
        try:
            # Initialize AlloyDB connection ONCE per Spark partition
            connect_kwargs: dict[str, Any] = {
                "user": db_user,
                "db": database,
                "enable_iam_auth": use_iam,
            }
            if not use_iam and db_password is not None:
                connect_kwargs["password"] = db_password
            conn = connector.connect(instance_uri, "psycopg", **connect_kwargs)

            buffer: list[list[Any]] = []
            with conn.cursor() as cur:
                for row in rows:
                    vec_values = row[emb_col_raw]
                    if vec_values is None:
                        continue
                    params: list[Any] = [
                        row[id_col_raw],
                        format_pgvector_literal(vec_values),
                    ]
                    if include_content:
                        params.append(row[content_col_raw])
                    for meta_col in meta_cols:
                        val = row[meta_col]
                        params.append(
                            json.dumps(val) if isinstance(val, (dict, list)) else val
                        )
                    buffer.append(params)

                    if len(buffer) >= batch_size:
                        values_sql = ", ".join([row_tuple_sql] * len(buffer))
                        stmt = (
                            f"INSERT INTO {schema_q}.{table_q} ({cols_joined}) "
                            f"VALUES {values_sql} {conflict_tail}"
                        )
                        flat_params = [item for grp in buffer for item in grp]
                        cur.execute(stmt, flat_params)
                        conn.commit()
                        buffer.clear()

                if buffer:
                    values_sql = ", ".join([row_tuple_sql] * len(buffer))
                    stmt = (
                        f"INSERT INTO {schema_q}.{table_q} ({cols_joined}) "
                        f"VALUES {values_sql} {conflict_tail}"
                    )
                    flat_params = [item for grp in buffer for item in grp]
                    cur.execute(stmt, flat_params)
                    conn.commit()
        finally:
            if conn is not None:
                conn.close()
            connector.close()

    df.select(*projected_cols).rdd.foreachPartition(upsert_partition)
```

## 3. Backend 2 — Vertex AI Vector Search (`backend == "vertex_vector_search"`)

Support both streaming index datapoint upserts and Cloud Storage batch JSONL exports:

- **Stream Upsert (`writeMode == "stream_upsert"`)**:
  - Inside `df.rdd.foreachPartition(...)`, call `aiplatform.init(project=project_id, location=location)` and instantiate `aiplatform.MatchingEngineIndex(index_name=index_name)` **once per Spark partition**.
  - Build datapoint payloads with `datapoint_id` (`str(row[id_col])`), `feature_vector` (`list(row[emb_col])`), and optional `restricts`, `numeric_restricts`, and `crowding_tag` fields when `restrictsColumn` or `crowdingTagColumn` is present on the DataFrame.
  - Upsert chunks of `batchSize` (default `200`) via `index.upsert_datapoints(datapoints=chunk)` with bounded exponential backoff on transient `429` or `5xx` errors.
- **GCS Batch JSONL Export (`writeMode == "gcs_batch_json"`)**:
  - Select `F.col(id_col).cast("string").alias("id")`, `F.col(emb_col).alias("embedding")`, plus optional `F.col(restricts_col).alias("restricts")` and `F.struct(F.col(crowding_col).cast("string").alias("crowding_attribute")).alias("crowding_tag")` when configured.
  - Write JSON Lines files directly to `gcsOutputUri` via `export_df.write.mode("overwrite").json(gcs_output_uri)`.

```python
import time
from typing import Any, Iterator
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F


def write_vertex_vector_search_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Writes embeddings to Vertex AI Vector Search via stream_upsert or gcs_batch_json."""
    write_mode = config.get("write_mode", "stream_upsert")
    id_col = config.get("id_column", "chunk_id")
    emb_col = config.get("embedding_column", "embedding")
    restricts_col = (config.get("restricts_column") or "").strip()
    crowding_col = (config.get("crowding_tag_column") or "").strip()

    if write_mode == "gcs_batch_json":
        gcs_output_uri = config["gcs_output_uri"]
        projections = [
            F.col(id_col).cast("string").alias("id"),
            F.col(emb_col).alias("embedding"),
        ]
        if restricts_col and restricts_col in df.columns:
            projections.append(F.col(restricts_col).alias("restricts"))
        if crowding_col and crowding_col in df.columns:
            projections.append(
                F.col(crowding_col).cast("string").alias("crowding_tag")
            )
        df.select(*projections).write.mode("overwrite").json(gcs_output_uri)
        return

    project_id = config["project_id"]
    location = config.get("location", "us-central1")
    index_name = config["index_name"]
    batch_size = max(1, int(config.get("batch_size", 200)))
    has_restricts = bool(restricts_col) and restricts_col in df.columns
    has_crowding = bool(crowding_col) and crowding_col in df.columns

    select_cols = [id_col, emb_col]
    if has_restricts:
        select_cols.append(restricts_col)
    if has_crowding:
        select_cols.append(crowding_col)

    def send_with_backoff(index_client: Any, datapoints: list[dict[str, Any]]) -> None:
        attempt = 0
        while True:
            try:
                index_client.upsert_datapoints(datapoints=datapoints)
                return
            except Exception:
                if attempt >= 3:
                    raise
                time.sleep(2**attempt)
                attempt += 1

    def upsert_datapoints_partition(rows: Iterator[Row]) -> None:
        from google.cloud import aiplatform

        # Initialize Vertex AI Vector Search index client ONCE per partition
        aiplatform.init(project=project_id, location=location)
        index_client = aiplatform.MatchingEngineIndex(index_name=index_name)

        chunk: list[dict[str, Any]] = []
        for row in rows:
            vec = row[emb_col]
            if vec is None:
                continue
            dp: dict[str, Any] = {
                "datapoint_id": str(row[id_col]),
                "feature_vector": [float(v) for v in vec],
            }
            if has_restricts and row[restricts_col] is not None:
                raw_restricts = row[restricts_col]
                if isinstance(raw_restricts, dict) and "numeric_restricts" in raw_restricts:
                    dp["numeric_restricts"] = raw_restricts["numeric_restricts"]
                else:
                    dp["restricts"] = raw_restricts
            if has_crowding and row[crowding_col] is not None:
                dp["crowding_tag"] = {"crowding_attribute": str(row[crowding_col])}
            chunk.append(dp)

            if len(chunk) >= batch_size:
                send_with_backoff(index_client, chunk)
                chunk.clear()

        if chunk:
            send_with_backoff(index_client, chunk)

    df.select(*select_cols).rdd.foreachPartition(upsert_datapoints_partition)
```

## 4. Unified `write_sinks(dfs, config)` for Batch and Structured Streaming (`foreachBatch`)

`VectorStoreSink` uses `streamingSinkMode == "foreachBatch"` when consuming a streaming DataFrame:

- Route `write_vector_store_batch(df, config)` to `write_vertex_vector_search_batch` when `backend == "vertex_vector_search"` and to `write_alloydb_pgvector_batch` otherwise.
- When `df.isStreaming` is `True`, wrap `write_vector_store_batch(batch_df, config)` inside `df.writeStream.foreachBatch(write_micro_batch)`.
- Cache `batch_df` before writing and always call `batch_df.unpersist()` inside a `finally` block.

```python
from typing import Any
from pyspark.sql import DataFrame


def write_vector_store_batch(df: DataFrame, config: dict[str, Any]) -> None:
    """Dispatches a batch DataFrame write to the configured vector store backend."""
    backend = config.get("backend", "alloydb_pgvector")
    if backend == "vertex_vector_search":
        write_vertex_vector_search_batch(df, config)
    else:
        write_alloydb_pgvector_batch(df, config)


def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:
    """Writes batch or streaming embedding DataFrames to VectorStoreSink."""
    df = dfs["vector_ready"]
    if not df.isStreaming:
        write_vector_store_batch(df, config)
        return

    checkpoint_root = config.get("checkpoint_root", "/tmp/spark-checkpoints")
    checkpoint_location = config.get(
        "vector_sink_checkpoint_location",
        f"{checkpoint_root.rstrip('/')}/vector_store_sink",
    )

    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.cache()
        try:
            write_vector_store_batch(batch_df, config)
        finally:
            batch_df.unpersist()

    stream_writer = (
        df.writeStream.foreachBatch(write_micro_batch)
        .outputMode(config.get("vector_sink_output_mode", "append"))
        .option("checkpointLocation", checkpoint_location)
    )
    if config.get("vector_sink_trigger_type") == "availableNow":
        stream_writer.trigger(availableNow=True).start()
    else:
        interval = config.get("vector_sink_trigger_interval", "10 seconds")
        stream_writer.trigger(processingTime=interval).start()

    df.sparkSession.streams.awaitAnyTermination()
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Keep all vector preparation inside `transform(dfs, config)` so `tests/test_pipeline.py` runs 100% offline with in-memory vector DataFrames:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType
from src.pipeline import transform


def test_vector_store_pipeline_transform_offline(spark: SparkSession) -> None:
    """Tests RAG vector preparation logic offline with an in-memory embedding DataFrame."""
    schema = StructType(
        [
            StructField("chunk_id", StringType(), False),
            StructField("chunk_text", StringType(), False),
            StructField("embedding", ArrayType(FloatType()), False),
        ]
    )
    input_df = spark.createDataFrame(
        [
            ("doc1_0", "Apache Spark 4.x runs vector indexing at scale.", [0.12, -0.34, 0.56]),
            ("doc1_1", "AlloyDB pgvector stores dense embeddings.", [0.22, 0.41, -0.19]),
        ],
        schema=schema,
    )
    result_dfs = transform({"embedded_chunks": input_df}, {})
    out_df = result_dfs["vector_ready"]
    assert out_df.count() == 2
    first_row = out_df.filter("chunk_id = 'doc1_0'").first()
    assert len(first_row["embedding"]) == 3
```
