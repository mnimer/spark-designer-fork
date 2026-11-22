---
name: io-rest-api
description: Read batch HTTP and REST API endpoints in PySpark 4.x with Spark 4.0 Python DataSourceReader pagination, parallel range partitions, and Secret Manager authentication.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp, notebook]
  transforms: [RestApiSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
  validated: true
---

# Batch HTTP and REST API Source (`RestApiSource`)

Use these rules when generating PySpark 4.x code for `RestApiSource` nodes.

## 1. Runtime Delivery Mechanism (`python_datasource`)

- Apache Spark 4.x does not ship a built-in JVM REST connector JAR. Implement a Spark 4.0 Python `DataSource` (`RestApiDataSource`) and `DataSourceReader` (`RestApiBatchReader`) from `pyspark.sql.datasource` with `@dataclass(frozen=True) class RestApiPartition(InputPartition)`.
- Include `requests` in `requirements.txt`.
- Register `RestApiDataSource` on the active `SparkSession` via `spark.dataSource.register(RestApiDataSource)` inside `read_sources(spark, config)`.
- Parse `schemaDDL` with `StructType.fromDDL(schema_ddl)` so Catalyst plans a deterministic schema without driver-side HTTP inference calls.

## 2. Authentication and Driver-Side Secret Resolution (`get_secret`)

- Resolve credentials from Google Cloud Secret Manager **once on the driver** via `get_secret(secret_name)` inside `read_sources(spark, config)` before calling `.load()`. Never call Secret Manager inside `read(self, partition)` per executor task or per record.
- Support `authMode` (`"default"` or `"secretManager"`), `secretName`, `authScheme` (`"none"`, `"bearer"`, `"api_key"`, or `"basic"`), and `authHeaderName`:
  - When `authScheme == "none"`, send no authentication header.
  - When `authScheme == "bearer"`, attach `Authorization: Bearer <secret>` (or use `authHeaderName` when specified).
  - When `authScheme == "api_key"`, attach `<authHeaderName>: <secret>` (default `authHeaderName` to `"X-API-Key"`).
  - When `authScheme == "basic"`, base64-encode `<secret>` (`username:password`) and attach `Authorization: Basic <encoded>`.

## 3. Pagination Modes, Parallel Partitions, and Executor Retry Rules

### Supported Read Options
- `endpointUrl` and `httpMethod` (`"GET"` or `"POST"`): Target HTTP or HTTPS URL and request verb.
- `schemaDDL`: Catalyst DDL schema string parsed via `StructType.fromDDL(schema_ddl)`.
- `recordsJsonPath`: Dotted path to the record array inside the JSON payload (for example `"data.items"`). If `recordsJsonPath` is empty, read records directly from a top-level JSON list or wrap a top-level JSON object in a one-element list.
- `paginationType`:
  - `"none"`: Execute one HTTP request per partition.
  - `"offset_limit"`: Request pages up to `maxPages` using `offsetParam` (default `"offset"`), `limitParam` (default `"limit"`), and `pageSize` (default `100`). Stop when a page returns fewer than `pageSize` records or `maxPages` is reached.
  - `"cursor"`: Pass `limitParam` set to `pageSize` and `cursorParam` (default `"cursor"`) after the first page. Extract the next token via `nextCursorJsonPath` (for example `"meta.next_cursor"`) and stop when the token is missing, empty, or `maxPages` is reached.
  - `"link_header"`: Follow RFC 5988 `Link` headers via `response.links.get("next", {}).get("url")` up to `maxPages` until no `next` URL exists.
- **Parallel Range Partitioning (`partitions()`)**:
  - When `numPartitions > 1` and `partitionParam`, `lowerBound`, and `upperBound` are provided, split `[lowerBound, upperBound]` evenly into `numPartitions` contiguous integer ranges and return one `RestApiPartition` per slice.
  - When parallel partitioning is not configured, emit a single `RestApiPartition(partition_id=0)`.
- **Lazy Executor `requests.Session()` and Exponential Backoff**:
  - Instantiate `requests.Session()` lazily inside `read(self, partition)` on the executor worker using a `with requests.Session() as session:` context manager. Never create a `Session` in `__init__` on the driver because `DataSourceReader` instances must remain picklable.
  - Retry HTTP `429` and `5xx` (`500`, `502`, `503`, `504`) responses plus transient `requests.RequestException` errors up to `maxRetries` using exponential backoff (`min(60.0, 1.0 * (2 ** attempt))`), honoring a numeric `Retry-After` response header when present.

## 4. Reference Implementation (`src/pipeline.py`)

```python
import base64
from dataclasses import dataclass
import json
import time
from typing import Any, Iterator, Sequence
import urllib.request
import google.auth
import google.auth.transport.requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType


def get_secret(secret_version_name: str) -> str:
    """Fetches a UTF-8 secret payload from Google Cloud Secret Manager once on the driver."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    resource = (
        secret_version_name
        if "/versions/" in secret_version_name
        else f"{secret_version_name}/versions/latest"
    )
    url = f"https://secretmanager.googleapis.com/v1/{resource}:access"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {credentials.token}"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return base64.b64decode(payload["payload"]["data"]).decode("utf-8")


def _extract_json_path(payload: Any, dotted_path: str) -> Any:
    """Walks a dotted JSON path and returns the nested value or None when absent."""
    cleaned = dotted_path.strip()
    if not cleaned:
        return payload
    current = payload
    for segment in cleaned.split("."):
        if isinstance(current, dict) and segment in current:
            current = current[segment]
        else:
            return None
    return current


@dataclass(frozen=True)
class RestApiPartition(InputPartition):
    """Serializable partition descriptor for parallel or single-slice REST reads."""

    partition_id: int
    partition_param: str = ""
    lower_bound: int | None = None
    upper_bound: int | None = None


class RestApiBatchReader(DataSourceReader):
    """Spark 4.0 DataSourceReader fetching JSON records from HTTP and REST endpoints."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)

    def partitions(self) -> Sequence[InputPartition]:
        num_partitions = max(1, int(self.options.get("numPartitions", "1")))
        partition_param = self.options.get("partitionParam", "").strip()
        lower_raw = self.options.get("lowerBound", "").strip()
        upper_raw = self.options.get("upperBound", "").strip()

        if num_partitions > 1 and partition_param and lower_raw and upper_raw:
            lower = int(lower_raw)
            upper = int(upper_raw)
            total_span = max(1, upper - lower + 1)
            slice_count = min(num_partitions, total_span)
            step, remainder = divmod(total_span, slice_count)
            slices: list[InputPartition] = []
            cursor = lower
            for idx in range(slice_count):
                width = step + (1 if idx < remainder else 0)
                end_bound = cursor + width - 1
                slices.append(
                    RestApiPartition(
                        partition_id=idx,
                        partition_param=partition_param,
                        lower_bound=cursor,
                        upper_bound=end_bound,
                    )
                )
                cursor = end_bound + 1
            return slices

        return [RestApiPartition(partition_id=0)]

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json"}
        raw_headers = self.options.get("requestHeaders", "").strip()
        if raw_headers:
            parsed_headers = json.loads(raw_headers)
            if isinstance(parsed_headers, dict):
                for key, val in parsed_headers.items():
                    headers[str(key)] = str(val)

        auth_scheme = self.options.get("authScheme", "none").strip().lower()
        auth_secret = self.options.get("resolvedAuthSecret", "")
        header_name = self.options.get("authHeaderName", "").strip()

        if auth_scheme == "bearer" and auth_secret:
            target_header = header_name or "Authorization"
            headers[target_header] = f"Bearer {auth_secret}"
        elif auth_scheme == "api_key" and auth_secret:
            target_header = header_name or "X-API-Key"
            headers[target_header] = auth_secret
        elif auth_scheme == "basic" and auth_secret:
            target_header = header_name or "Authorization"
            encoded = base64.b64encode(auth_secret.encode("utf-8")).decode("ascii")
            headers[target_header] = f"Basic {encoded}"

        return headers

    def _request_with_backoff(
        self,
        session: Any,
        method: str,
        url: str,
        params: dict[str, Any],
        body_payload: Any,
        timeout_sec: float,
        max_retries: int,
    ) -> Any:
        import requests

        retryable_statuses = {429, 500, 502, 503, 504}
        for attempt in range(max_retries + 1):
            try:
                response = session.request(
                    method=method,
                    url=url,
                    params=params if params else None,
                    json=body_payload if method == "POST" and isinstance(body_payload, (dict, list)) else None,
                    data=body_payload if method == "POST" and isinstance(body_payload, str) else None,
                    timeout=timeout_sec,
                )
                if response.status_code in retryable_statuses and attempt < max_retries:
                    retry_after = response.headers.get("Retry-After", "").strip()
                    delay = float(retry_after) if retry_after.isdigit() else min(60.0, float(2**attempt))
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                return response
            except requests.RequestException:
                if attempt >= max_retries:
                    raise
                time.sleep(min(60.0, float(2**attempt)))
        raise RuntimeError(f"Exhausted HTTP retries for {url}")

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        import requests

        endpoint_url = self.options["endpointUrl"]
        http_method = self.options.get("httpMethod", "GET").strip().upper()
        pagination_type = self.options.get("paginationType", "none").strip().lower()
        page_size = max(1, int(self.options.get("pageSize", "100")))
        max_pages = max(1, int(self.options.get("maxPages", "100")))
        offset_param = self.options.get("offsetParam", "offset").strip() or "offset"
        limit_param = self.options.get("limitParam", "limit").strip() or "limit"
        cursor_param = self.options.get("cursorParam", "cursor").strip() or "cursor"
        next_cursor_path = self.options.get("nextCursorJsonPath", "next_cursor").strip()
        records_path = self.options.get("recordsJsonPath", "").strip()
        timeout_sec = max(1.0, float(self.options.get("timeoutSeconds", "30")))
        max_retries = max(0, int(self.options.get("maxRetries", "3")))

        raw_body = self.options.get("requestBody", "").strip()
        body_payload: Any = None
        if raw_body:
            try:
                body_payload = json.loads(raw_body)
            except ValueError:
                body_payload = raw_body

        base_params: dict[str, Any] = {}
        if isinstance(partition, RestApiPartition) and partition.partition_param:
            if partition.lower_bound is not None and partition.upper_bound is not None:
                base_params[f"{partition.partition_param}_gte"] = partition.lower_bound
                base_params[f"{partition.partition_param}_lte"] = partition.upper_bound

        field_names = self.schema.fieldNames()
        next_url: str | None = endpoint_url
        current_offset = 0
        current_cursor: str | None = None
        pages_fetched = 0

        with requests.Session() as session:
            session.headers.update(self._build_headers())

            while next_url and pages_fetched < max_pages:
                params = dict(base_params)
                target_url = next_url

                if pagination_type == "offset_limit":
                    params[offset_param] = current_offset
                    params[limit_param] = page_size
                elif pagination_type == "cursor":
                    params[limit_param] = page_size
                    if current_cursor:
                        params[cursor_param] = current_cursor
                elif pagination_type == "link_header" and pages_fetched == 0:
                    params[limit_param] = page_size
                elif pagination_type == "link_header" and pages_fetched > 0:
                    params = {}

                response = self._request_with_backoff(
                    session=session,
                    method=http_method,
                    url=target_url,
                    params=params,
                    body_payload=body_payload,
                    timeout_sec=timeout_sec,
                    max_retries=max_retries,
                )
                payload = response.json()
                extracted = _extract_json_path(payload, records_path)
                if extracted is None:
                    records: list[dict[str, Any]] = []
                elif isinstance(extracted, list):
                    records = [item for item in extracted if isinstance(item, dict)]
                elif isinstance(extracted, dict):
                    records = [extracted]
                else:
                    records = []

                for record in records:
                    yield tuple(record.get(col) for col in field_names)

                pages_fetched += 1

                if pagination_type == "none":
                    break
                if pagination_type == "offset_limit":
                    if len(records) < page_size:
                        break
                    current_offset += page_size
                elif pagination_type == "cursor":
                    token = _extract_json_path(payload, next_cursor_path)
                    if token is None or str(token).strip() == "" or not records:
                        break
                    current_cursor = str(token)
                elif pagination_type == "link_header":
                    link_next = response.links.get("next", {}).get("url")
                    next_url = str(link_next) if link_next else None
                else:
                    break


class RestApiDataSource(DataSource):
    """Spark 4.0 Python DataSource for batch HTTP and REST API ingestion."""

    @classmethod
    def name(cls) -> str:
        return "rest_api"

    def schema(self) -> StructType:
        schema_ddl = self.options.get("schemaDDL", "").strip()
        if not schema_ddl:
            raise ValueError("RestApiDataSource requires a non-empty schemaDDL option.")
        return StructType.fromDDL(schema_ddl)

    def reader(self, schema: StructType) -> DataSourceReader:
        return RestApiBatchReader(schema, self.options)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Registers RestApiDataSource, resolves Secret Manager auth once on the driver, and loads records."""
    spark.dataSource.register(RestApiDataSource)
    dfs: dict[str, DataFrame] = {}

    auth_mode = str(config.get("rest_auth_mode", "default"))
    secret_name = str(config.get("rest_secret_name", "")).strip()
    resolved_secret = (
        get_secret(secret_name)
        if auth_mode == "secretManager" and secret_name
        else ""
    )

    read_opts: dict[str, str] = {
        "endpointUrl": str(config["rest_endpoint_url"]),
        "httpMethod": str(config.get("rest_http_method", "GET")),
        "schemaDDL": str(config["rest_schema_ddl"]),
        "recordsJsonPath": str(config.get("rest_records_json_path", "")),
        "paginationType": str(config.get("rest_pagination_type", "none")),
        "pageSize": str(config.get("rest_page_size", 100)),
        "maxPages": str(config.get("rest_max_pages", 100)),
        "offsetParam": str(config.get("rest_offset_param", "offset")),
        "limitParam": str(config.get("rest_limit_param", "limit")),
        "cursorParam": str(config.get("rest_cursor_param", "cursor")),
        "nextCursorJsonPath": str(config.get("rest_next_cursor_json_path", "next_cursor")),
        "numPartitions": str(config.get("rest_num_partitions", 1)),
        "partitionParam": str(config.get("rest_partition_param", "")),
        "lowerBound": str(config.get("rest_lower_bound", "")),
        "upperBound": str(config.get("rest_upper_bound", "")),
        "authScheme": str(config.get("rest_auth_scheme", "none")),
        "authHeaderName": str(config.get("rest_auth_header_name", "")),
        "resolvedAuthSecret": resolved_secret,
        "requestHeaders": str(config.get("rest_request_headers", "")),
        "requestBody": str(config.get("rest_request_body", "")),
        "timeoutSeconds": str(config.get("rest_timeout_seconds", 30)),
        "maxRetries": str(config.get("rest_max_retries", 3)),
    }

    schema_struct = StructType.fromDDL(read_opts["schemaDDL"])
    dfs["rest_api_source"] = (
        spark.read.format("rest_api")
        .schema(schema_struct)
        .options(**read_opts)
        .load()
    )
    return dfs
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` by building an in-memory `DataFrame` with `StructType.fromDDL(schema_ddl)` so unit tests never issue live HTTP requests:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from src.pipeline import transform


def test_rest_api_pipeline_transform(spark: SparkSession) -> None:
    """Tests pipeline transformations offline using an in-memory DataFrame matching schemaDDL."""
    schema = StructType.fromDDL("id STRING, status STRING, amount DOUBLE")
    input_df = spark.createDataFrame(
        [("ord-1", "PAID", 125.50), ("ord-2", "PENDING", 40.00)],
        schema=schema,
    )
    result_dfs = transform({"rest_api_source": input_df}, {})
    assert result_dfs["rest_api_source"].count() == 2
```
