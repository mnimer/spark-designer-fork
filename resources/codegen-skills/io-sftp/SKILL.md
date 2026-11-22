---
name: io-sftp
description: Read remote files over SFTP in PySpark 4.x using Spark 4.0 Python DataSourceReader, Paramiko SSH transport, Secret Manager authentication, and multi-format record parsing.
metadata:
  kind: io
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [imperative, sdp]
  transforms: [SftpSource]
  preloadWhen: []
  sources:
    - prompt
    - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/datasource.html
    - https://docs.paramiko.org/en/latest/api/sftp.html
  validated: true
---

# Standalone Batch SFTP Source (`SftpSource`)

Use these rules when generating PySpark 4.x code for `SftpSource` nodes.

## 1. Runtime Delivery Mechanism (`python_datasource`)

- Apache Spark 4.x does not include a built-in Scala 2.13 SFTP JVM connector JAR. Implement a standalone Spark 4.0 Python `DataSource` (`SftpDataSource`) and `DataSourceReader` (`SftpBatchReader`) from `pyspark.sql.datasource` backed by `paramiko`.
- Include `paramiko` in `requirements.txt`. When `fileFormat == "parquet"`, also include `pyarrow` in `requirements.txt` so executors decode remote Parquet files in memory.
- Register `SftpDataSource` on the active `SparkSession` via `spark.dataSource.register(SftpDataSource)` inside `read_sources(spark, config)`.
- Parse `schemaDDL` with `StructType.fromDDL(schema_ddl)` so Catalyst enforces an explicit schema.

## 2. Driver-Side Secret Resolution and Host Key Verification

- Resolve credentials from Google Cloud Secret Manager **once on the driver** via `get_secret(secret_name)` inside `read_sources(spark, config)` before calling `.load()`. Never call Secret Manager inside `read(self, partition)` on executors.
- Support `host`, `port` (default `22`), `username`, and `authType` (`"ssh_key"` or `"password"`):
  - When `authType == "ssh_key"`, parse the PEM private key payload from memory (`io.StringIO`) with `paramiko` key loaders (`RSAKey`, `Ed25519Key`, or `ECDSAKey`) and connect via `transport.connect(username=username, pkey=pkey)`.
  - When `authType == "password"`, connect via `transport.connect(username=username, password=secret_payload)`.
- Enforce SSH host key verification when `strictHostKeyChecking == "true"` (default):
  - Load `paramiko.HostKeys` from `knownHostsPath` (or `~/.ssh/known_hosts` when `knownHostsPath` is blank), look up the expected server key for `host` (or `[host]:port` on non-22 ports), and pass `hostkey=expected_key` to `transport.connect(...)`. Raise `paramiko.SSHException` if no trusted host key entry exists.
  - Skip host key verification only when `strictHostKeyChecking` is explicitly `"false"`.

## 3. Parallel File Sharding, Transport Cleanup, and Multi-Format Parsing

- **Remote Glob Discovery and Sharding (`partitions()`)**:
  - Connect briefly inside `partitions()` inside a `try / finally` block that closes both `SFTPClient` and `Transport` before returning. Never store live `Transport` or `SFTPClient` objects on `self` because `DataSourceReader` instances must remain picklable.
  - If `remotePath` is a directory, list entries via `sftp.listdir_attr(remote_path)`, filter regular files matching `filePattern` with `fnmatch.fnmatch(entry.filename, file_pattern)` (default `"*"`), and sort matched full paths deterministically. If `remotePath` targets a single file, emit `[remotePath]`.
  - Shard matched file paths evenly across `numPartitions` (`matched_files[idx::slice_count]`) into `@dataclass(frozen=True) class SftpFilePartition(InputPartition)` instances.
- **Lazy Executor Transport Lifecycle (`read(self, partition)`)**:
  - Open a fresh `paramiko.Transport((host, port))` and `paramiko.SFTPClient.from_transport(transport)` inside `read(self, partition)` and always close both in a `try / finally` block.
- **Supported `fileFormat` Parsers**:
  - `"csv"`: Stream text with `io.TextIOWrapper`, parse rows using `csv.DictReader` when `csvHeader == "true"` or positional `csv.reader` when `csvHeader == "false"`, and honor `csvDelimiter` (default `","`).
  - `"json"`: Parse a full JSON array or object when `jsonMultiLine == "true"`, or parse newline-delimited JSON objects line by line when `jsonMultiLine == "false"`.
  - `"parquet"`: Read remote file bytes into `io.BytesIO`, decode with `pyarrow.parquet.read_table(buffer)`, and project rows by `self.schema.fieldNames()`.
  - `"text"`: Yield each text line mapped to the first column in `self.schema.fieldNames()` (and populate a second column with the remote file path when two columns are declared).
  - `"xml"`: Parse XML bytes with `xml.etree.ElementTree.fromstring` and iterate `.findall(f".//{xml_row_tag}")` using `xmlRowTag` (default `"row"`), reading child element text or XML attributes for each schema field.

## 4. Reference Implementation (`src/pipeline.py`)

```python
import base64
import csv
from dataclasses import dataclass
import fnmatch
import io
import json
import os
import posixpath
import stat
from typing import Any, Iterator, Sequence
import urllib.request
import xml.etree.ElementTree as ET
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


def _load_private_key(pem_text: str) -> Any:
    """Parses an SSH private key in RSA, Ed25519, or ECDSA format from a PEM string."""
    import paramiko

    key_classes = [paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey]
    for key_cls in key_classes:
        try:
            return key_cls.from_private_key(io.StringIO(pem_text))
        except paramiko.SSHException:
            continue
    raise ValueError("Unsupported or invalid SSH private key payload.")


def _open_sftp_session(options: dict[str, str]) -> tuple[Any, Any]:
    """Opens a verified Paramiko Transport and SFTPClient pair from reader options."""
    import paramiko

    host = options["host"].strip()
    port = int(options.get("port", "22"))
    username = options["username"].strip()
    auth_type = options.get("authType", "ssh_key").strip().lower()
    secret_payload = options.get("resolvedAuthSecret", "")
    strict_host_key = options.get("strictHostKeyChecking", "true").strip().lower() == "true"
    known_hosts_raw = options.get("knownHostsPath", "").strip()

    expected_host_key = None
    if strict_host_key:
        hosts_file = known_hosts_raw or os.path.expanduser("~/.ssh/known_hosts")
        host_keys = paramiko.HostKeys(hosts_file)
        lookup_key = host if port == 22 else f"[{host}]:{port}"
        entry = host_keys.lookup(lookup_key) or host_keys.lookup(host)
        if not entry:
            raise paramiko.SSHException(f"No trusted host key found for {lookup_key}")
        expected_host_key = next(iter(entry.values()))

    transport = paramiko.Transport((host, port))
    try:
        if auth_type == "ssh_key":
            pkey = _load_private_key(secret_payload)
            transport.connect(hostkey=expected_host_key, username=username, pkey=pkey)
        else:
            transport.connect(
                hostkey=expected_host_key,
                username=username,
                password=secret_payload,
            )
        sftp = paramiko.SFTPClient.from_transport(transport)
        if sftp is None:
            raise paramiko.SSHException("Failed to initialize SFTP subsystem.")
        return transport, sftp
    except Exception:
        transport.close()
        raise


@dataclass(frozen=True)
class SftpFilePartition(InputPartition):
    """Serializable partition holding a shard of remote SFTP file paths."""

    partition_id: int
    remote_files: tuple[str, ...]


class SftpBatchReader(DataSourceReader):
    """Spark 4.0 DataSourceReader reading remote files over SFTP via Paramiko."""

    def __init__(self, schema: StructType, options: dict[str, str]) -> None:
        self.schema = schema
        self.options = dict(options)

    def partitions(self) -> Sequence[InputPartition]:
        remote_path = self.options["remotePath"].strip()
        file_pattern = self.options.get("filePattern", "*").strip() or "*"
        num_partitions = max(1, int(self.options.get("numPartitions", "4")))

        transport, sftp = _open_sftp_session(self.options)
        try:
            path_stat = sftp.stat(remote_path)
            if stat.S_ISDIR(path_stat.st_mode or 0):
                matched = [
                    posixpath.join(remote_path, attr.filename)
                    for attr in sftp.listdir_attr(remote_path)
                    if not stat.S_ISDIR(attr.st_mode or 0)
                    and fnmatch.fnmatch(attr.filename, file_pattern)
                ]
                matched.sort()
            else:
                matched = [remote_path]
        finally:
            sftp.close()
            transport.close()

        if not matched:
            return []

        slice_count = min(num_partitions, len(matched))
        return [
            SftpFilePartition(
                partition_id=idx,
                remote_files=tuple(matched[idx::slice_count]),
            )
            for idx in range(slice_count)
        ]

    def _parse_remote_file(
        self,
        sftp: Any,
        remote_file: str,
        field_names: list[str],
    ) -> Iterator[tuple[Any, ...]]:
        file_format = self.options.get("fileFormat", "csv").strip().lower()
        csv_header = self.options.get("csvHeader", "true").strip().lower() == "true"
        csv_delimiter = self.options.get("csvDelimiter", ",") or ","
        json_multi_line = self.options.get("jsonMultiLine", "false").strip().lower() == "true"
        xml_row_tag = self.options.get("xmlRowTag", "row").strip() or "row"

        with sftp.open(remote_file, "rb") as raw_handle:
            if file_format == "parquet":
                import pyarrow.parquet as pq

                buffer = io.BytesIO(raw_handle.read())
                arrow_table = pq.read_table(buffer)
                for record in arrow_table.to_pylist():
                    yield tuple(record.get(col) for col in field_names)
                return

            text_body = raw_handle.read().decode("utf-8")

        if file_format == "csv":
            stream = io.StringIO(text_body)
            if csv_header:
                dict_reader = csv.DictReader(stream, delimiter=csv_delimiter)
                for row in dict_reader:
                    yield tuple(row.get(col) for col in field_names)
            else:
                row_reader = csv.reader(stream, delimiter=csv_delimiter)
                for values in row_reader:
                    padded = list(values) + [None] * max(0, len(field_names) - len(values))
                    yield tuple(padded[: len(field_names)])
        elif file_format == "json":
            if json_multi_line:
                parsed = json.loads(text_body)
                items = parsed if isinstance(parsed, list) else [parsed]
                for item in items:
                    if isinstance(item, dict):
                        yield tuple(item.get(col) for col in field_names)
            else:
                for line in text_body.splitlines():
                    stripped = line.strip()
                    if not stripped:
                        continue
                    obj = json.loads(stripped)
                    if isinstance(obj, dict):
                        yield tuple(obj.get(col) for col in field_names)
        elif file_format == "xml":
            root = ET.fromstring(text_body)
            for elem in root.findall(f".//{xml_row_tag}"):
                row_map: dict[str, Any] = dict(elem.attrib)
                for child in elem:
                    row_map[child.tag] = child.text
                yield tuple(row_map.get(col) for col in field_names)
        else:
            for line in text_body.splitlines():
                if len(field_names) >= 2:
                    yield (line, remote_file) + (None,) * (len(field_names) - 2)
                elif len(field_names) == 1:
                    yield (line,)

    def read(self, partition: InputPartition) -> Iterator[tuple[Any, ...]]:
        if not isinstance(partition, SftpFilePartition) or not partition.remote_files:
            return

        field_names = self.schema.fieldNames()
        transport, sftp = _open_sftp_session(self.options)
        try:
            for remote_file in partition.remote_files:
                yield from self._parse_remote_file(sftp, remote_file, field_names)
        finally:
            sftp.close()
            transport.close()


class SftpDataSource(DataSource):
    """Spark 4.0 Python DataSource for standalone batch SFTP ingestion."""

    @classmethod
    def name(cls) -> str:
        return "sftp_source"

    def schema(self) -> StructType:
        schema_ddl = self.options.get("schemaDDL", "").strip()
        if not schema_ddl:
            raise ValueError("SftpDataSource requires a non-empty schemaDDL option.")
        return StructType.fromDDL(schema_ddl)

    def reader(self, schema: StructType) -> DataSourceReader:
        return SftpBatchReader(schema, self.options)


def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Registers SftpDataSource, resolves Secret Manager SSH credentials once on the driver, and loads files."""
    spark.dataSource.register(SftpDataSource)
    dfs: dict[str, DataFrame] = {}

    auth_mode = str(config.get("sftp_auth_mode", "secretManager"))
    secret_name = str(config.get("sftp_secret_name", "")).strip()
    resolved_secret = (
        get_secret(secret_name)
        if auth_mode == "secretManager" and secret_name
        else str(os.environ.get("SFTP_AUTH_SECRET", ""))
    )

    read_opts: dict[str, str] = {
        "host": str(config["sftp_host"]),
        "port": str(config.get("sftp_port", 22)),
        "username": str(config["sftp_username"]),
        "authType": str(config.get("sftp_auth_type", "ssh_key")),
        "resolvedAuthSecret": resolved_secret,
        "remotePath": str(config["sftp_remote_path"]),
        "filePattern": str(config.get("sftp_file_pattern", "*")),
        "fileFormat": str(config.get("sftp_file_format", "csv")),
        "csvHeader": str(config.get("sftp_csv_header", True)).lower(),
        "csvDelimiter": str(config.get("sftp_csv_delimiter", ",")),
        "jsonMultiLine": str(config.get("sftp_json_multi_line", False)).lower(),
        "xmlRowTag": str(config.get("sftp_xml_row_tag", "row")),
        "schemaDDL": str(config["sftp_schema_ddl"]),
        "strictHostKeyChecking": str(config.get("sftp_strict_host_key_checking", True)).lower(),
        "knownHostsPath": str(config.get("sftp_known_hosts_path", "")),
        "numPartitions": str(config.get("sftp_num_partitions", 4)),
    }

    schema_struct = StructType.fromDDL(read_opts["schemaDDL"])
    dfs["sftp_source"] = (
        spark.read.format("sftp_source")
        .schema(schema_struct)
        .options(**read_opts)
        .load()
    )
    return dfs
```

## 5. Offline Unit Testing (`tests/test_pipeline.py`)

Test `transform(dfs, config)` offline in `tests/test_pipeline.py` using an in-memory `DataFrame` with `StructType.fromDDL(schema_ddl)` so tests run without an SSH or SFTP server:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from src.pipeline import transform


def test_sftp_pipeline_transform(spark: SparkSession) -> None:
    """Tests pipeline transformations offline without opening an SFTP connection."""
    schema = StructType.fromDDL("invoice_id STRING, customer_id STRING, total STRING")
    input_df = spark.createDataFrame(
        [("inv-100", "cust-1", "250.00"), ("inv-101", "cust-2", "99.95")],
        schema=schema,
    )
    result_dfs = transform({"sftp_source": input_df}, {})
    assert result_dfs["sftp_source"].count() == 2
```
