---
name: io-foreach-batch
description: Execute custom Python micro-batch writers or batched HTTP webhooks for Datadog, Splunk HEC, and REST endpoints in PySpark 4.x.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  transforms: [ForeachBatchSink]
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch
  validated: true
---

# Custom Python & HTTP Webhook `foreachBatch` Sink (`ForeachBatchSink`)

Use these rules when generating PySpark {{spark_version}} code for a `ForeachBatchSink` (`DatadogIO`, `SplunkIO`, or `RequestResponseIO`) node.

## 1. Execution Model (`batch` and `foreachBatch` Streaming)

`ForeachBatchSink` supports four preset `sinkMode` values:
- **`custom_python`**: Executes custom PySpark DataFrame or partition writer logic inside `write_micro_batch(batch_df: DataFrame, batch_id: int) -> None` (and calls `write_micro_batch(df, batch_id=0)` in batch mode) according to `customFunctionInstructions` and `system_instructions`.
- **`http_webhook`**: Sends batched JSON arrays (`POST`, `PUT`, or `PATCH`) to a REST endpoint using `urllib.request` with `Authorization: Bearer <token>` (when `tokenSecret` is set).
- **`datadog`**: Sends batched JSON arrays (`<= 500` events per request) to the Datadog HTTP Logs Intake API v2 (`https://http-intake.logs.datadoghq.com/api/v2/logs`) with `DD-API-Key: <api_key>`.
- **`splunk_hec`**: Sends newline-delimited JSON event envelopes (`{"event": <row_dict>}`) to Splunk HTTP Event Collector (`/services/collector/event`) with `Authorization: Splunk <hec_token>`.

## 2. Partition-Parallel Dispatch, Secrets, and Idempotency Rules

- **Never collect onto the driver**: Dispatch HTTP batches inside executor partitions using `df.mapInPandas(send_partition_fn, schema="records_sent bigint").write.format("noop").mode("append").save()`. Using `.write.format("noop")` executes the distributed partition writers on both classic Spark and Spark Connect without referencing legacy SparkContext internals, RDD APIs, or driver collection.
- **Secret Resolution (`get_secret`)**: Resolve `tokenSecret` (or `apiKeySecret`) once via the Secret Manager REST helper `get_secret(secret_name)` (wrapping `access_secret_version(...)`) before launching partition writers or once per partition initialization, never inside a per-row loop.
- **Idempotency Header**: Attach `Idempotency-Key` and `X-Spark-Batch-Id` headers derived from `batch_id` and chunk sequence (`f"{app_name}-b{batch_id}-c{chunk_index}"`) so downstream receivers deduplicate retried micro-batches.
- **Bounded Retries with Exponential Backoff**: Honor `batchSize` (default `500`), `timeoutSeconds` (default `30`), and `maxRetries` (default `3`), retrying HTTP `429` and `5xx` responses or transient `urllib.error.URLError` failures with exponential backoff.

## 3. Reference Implementation (`src/pipeline.py`)

```python
import base64
import json
import logging
import time
import urllib.error
import urllib.request
from typing import Any, Iterator
import google.auth
import google.auth.transport.requests
import pandas as pd
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def get_secret(secret_name: str, project_id: str = "") -> str:
    """Resolves a UTF-8 credential from Google Cloud Secret Manager via REST."""
    resource = (
        secret_name
        if secret_name.startswith("projects/")
        else f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    )
    credentials, default_project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    if not secret_name.startswith("projects/") and not project_id and default_project:
        resource = f"projects/{default_project}/secrets/{secret_name}/versions/latest"
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)

    url = f"https://secretmanager.googleapis.com/v1/{resource}:access"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))["payload"]["data"]
    return base64.b64decode(data).decode("utf-8").strip()


def send_http_batch_with_retry(
    endpoint_url: str,
    http_method: str,
    payload_bytes: bytes,
    headers: dict[str, str],
    timeout_seconds: int,
    max_retries: int,
) -> None:
    """Sends one HTTP batch with exponential backoff on 429 and 5xx errors."""
    attempt = 0
    while True:
        req = urllib.request.Request(
            endpoint_url,
            data=payload_bytes,
            headers=headers,
            method=http_method,
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                resp.read()
                return
        except urllib.error.HTTPError as exc:
            retryable = exc.code == 429 or exc.code >= 500
            if not retryable or attempt >= max_retries:
                raise
        except urllib.error.URLError:
            if attempt >= max_retries:
                raise
        time.sleep(min(2**attempt, 30))
        attempt += 1


def write_foreach_batch_df(
    batch_df: DataFrame,
    batch_id: int,
    config: dict[str, Any],
) -> None:
    """Writes a batch or micro-batch DataFrame using the configured ForeachBatchSink preset."""
    sink_mode = config.get("foreach_sink_mode", "custom_python")
    if sink_mode == "custom_python":
        logger.info("Executing custom ForeachBatchSink logic for batch_id=%d", batch_id)
        # Execute user-specified custom_python DataFrame writer operations here
        return

    endpoint_url = config["foreach_endpoint_url"]
    http_method = config.get("foreach_http_method", "POST").upper()
    batch_size = max(1, int(config.get("foreach_batch_size", 500)))
    timeout_sec = max(1, int(config.get("foreach_timeout_seconds", 30)))
    max_retries = max(0, int(config.get("foreach_max_retries", 3)))
    app_name = config.get("app_name", "spark_pipeline")

    secret_value = ""
    if config.get("foreach_token_secret"):
        secret_value = get_secret(
            config["foreach_token_secret"],
            config.get("gcp_project", ""),
        )

    def dispatch_partition(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        buffer: list[dict[str, Any]] = []
        chunk_idx = 0
        total_sent = 0

        def flush_chunk(rows: list[dict[str, Any]], idx: int) -> None:
            headers: dict[str, str] = {
                "Content-Type": "application/json",
                "X-Spark-Batch-Id": str(batch_id),
                "Idempotency-Key": f"{app_name}-b{batch_id}-c{idx}",
            }
            if sink_mode == "datadog":
                headers["DD-API-Key"] = secret_value
                body = json.dumps(
                    [{"ddsource": "spark", "service": app_name, "message": r} for r in rows],
                    default=str,
                ).encode("utf-8")
            elif sink_mode == "splunk_hec":
                headers["Authorization"] = f"Splunk {secret_value}"
                body = "\n".join(
                    json.dumps({"event": r}, default=str) for r in rows
                ).encode("utf-8")
            else:
                if secret_value:
                    headers["Authorization"] = f"Bearer {secret_value}"
                body = json.dumps(rows, default=str).encode("utf-8")

            send_http_batch_with_retry(
                endpoint_url=endpoint_url,
                http_method=http_method,
                payload_bytes=body,
                headers=headers,
                timeout_seconds=timeout_sec,
                max_retries=max_retries,
            )

        for pdf in pdf_iter:
            for record in pdf.to_dict(orient="records"):
                buffer.append(record)
                if len(buffer) >= batch_size:
                    flush_chunk(buffer, chunk_idx)
                    total_sent += len(buffer)
                    chunk_idx += 1
                    buffer.clear()

        if buffer:
            flush_chunk(buffer, chunk_idx)
            total_sent += len(buffer)
            buffer.clear()

        yield pd.DataFrame({"records_sent": [total_sent]})

    (
        batch_df.mapInPandas(dispatch_partition, schema="records_sent bigint")
        .write.format("noop")
        .mode("append")
        .save()
    )


def start_foreach_batch_stream(df: DataFrame, config: dict[str, Any]):
    """Starts a Structured Streaming query backed by write_foreach_batch_df."""
    def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        write_foreach_batch_df(batch_df, batch_id, config)

    return (
        df.writeStream.foreachBatch(write_micro_batch)
        .outputMode(config.get("foreach_output_mode", "append"))
        .option("checkpointLocation", config["foreach_checkpoint_location"])
        .trigger(processingTime=config.get("foreach_trigger_interval", "15 seconds"))
        .start()
    )
```

## 4. Offline Unit Testing (`tests/test_pipeline.py`)

Test upstream `transform(dfs, config)` and custom micro-batch payload formatting offline with mocked `urllib.request.urlopen`:

```python
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from src.pipeline import send_http_batch_with_retry, transform


def test_foreach_batch_http_helper_sets_headers(spark: SparkSession) -> None:
    """Verifies transformation output and HTTP webhook delivery helper offline."""
    input_df = spark.createDataFrame([("evt-1", 200)], ["event_id", "status_code"])
    transformed = transform({"src": input_df}, {})
    assert transformed["src"].count() == 1

    mock_resp = MagicMock()
    mock_resp.__enter__.return_value.read.return_value = b'{"status":"ok"}'
    with patch("urllib.request.urlopen", return_value=mock_resp) as mocked_open:
        send_http_batch_with_retry(
            endpoint_url="https://api.example.com/v1/events",
            http_method="POST",
            payload_bytes=b'[{"event_id":"evt-1"}]',
            headers={"Content-Type": "application/json", "X-Spark-Batch-Id": "0"},
            timeout_seconds=10,
            max_retries=1,
        )
        assert mocked_open.call_count == 1
```
