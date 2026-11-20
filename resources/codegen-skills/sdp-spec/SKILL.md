---
name: sdp-spec
description: Root spark-pipeline.yml specification allowlist, BigLake Iceberg REST catalog configuration, downstream BigQuery four-part contract, and static Spark property routing rules.
metadata:
  kind: crosscutting
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [sdp]
  transforms: []
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html
    - https://cloud.google.com/biglake/docs/iceberg-rest-catalog
  validated: true
---

# Spark Declarative Pipelines Specification & BigLake Catalog Rules (`spark-pipeline.yml`)

Follow these rules when generating `spark-pipeline.yml` and catalog configurations for every Spark Declarative Pipelines (`paradigm: sdp`) project.

## 1. Strict `spark-pipeline.yml` Field Allowlist

Emit **one** specification file named `spark-pipeline.yml` at the repository root. Never generate both `spark-pipeline.yml` and `spark-pipeline.yaml` (`MULTIPLE_PIPELINE_SPEC_FILES_FOUND`).

Only these top-level keys are valid in OSS Apache Spark 4.2:
- `name`: Pipeline identifier string (required for standalone `spark-pipelines` CLI runs).
- `storage`: Required absolute URI with an explicit scheme (for example `gs://<checkpoint-bucket>/pipelines/<pipeline_name>`). Never emit a bare path without a URI scheme (`PIPELINE_STORAGE_ROOT_INVALID`), and always keep `storage` outside every source data folder so Spark never ingests its own `_checkpoints` metadata.
- `catalog`: Plain Spark catalog identifier (`^[A-Za-z_][A-Za-z0-9_]*$`, default `lake`, or a Managed Data Pipelines double-brace Jinja template expression when orchestrated by Managed Data Pipelines). Never use a hyphenated catalog name such as `my-bucket` because SDP does not quote catalog identifiers on streaming tables (`INVALID_IDENTIFIER`).
- `database` (or alias `schema`): Target namespace identifier (for example `retail_medallion`).
- `configuration`: Optional map of **runtime and catalog** Spark SQL settings where **every value is a quoted string** (`PIPELINE_SPEC_DICT_VALUE_NOT_STRING` on unquoted booleans or numbers).
- `libraries`: List of `glob: { include: <pattern> }` entries locating dataset definitions. Always use `transformations/**` (folder ending in `/**`) or explicit file paths. Never write `transformations/*.py` because Spark rejects wildcard filename globs (`PIPELINE_SPEC_INVALID_GLOB_PATTERN`).

Never emit Databricks DLT or invented keys such as `continuous`, `trigger`, `development`, `clusters`, `photon`, `edition`, `channel`, or `target` (`PIPELINE_SPEC_UNEXPECTED_FIELD`).

## 2. Canonical `spark-pipeline.yml` Catalog Configuration (`usesIceberg` vs. Non-Iceberg)

Check `usesIceberg` and `specConfig.bigLakeMode` in `## SDP Execution Plan` before configuring catalog classes:

- **If `usesIceberg` is `true` (Iceberg or BigLake pipelines)**: Set `catalog: lake` (or the configured alias), set `spark.sql.catalog.<alias>: "org.apache.iceberg.spark.SparkCatalog"` in `spark-pipeline.yml` `configuration:`, and route `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` to static runner properties. Omitting an external catalog on Serverless Spark falls back to ephemeral `spark_catalog` metadata while checkpoints under `storage` persist.
- **If `usesIceberg` is `false` (non-Iceberg file, CSV, JSON, Parquet, Delta, or Kafka SDP pipelines)**: Omit `org.apache.iceberg.spark.SparkCatalog` and omit `IcebergSparkSessionExtensions`. Set `catalog: spark_catalog` (unless a non-Iceberg catalog is configured) so pure non-Iceberg SDP pipelines run locally out of the box without Iceberg JARs.

```yaml
name: retail_medallion_sdp
storage: gs://my-sdp-state-bucket/pipelines/retail_medallion_sdp
catalog: lake
database: retail_medallion
configuration:
  spark.sql.shuffle.partitions: "200"
  spark.sql.catalog.lake: "org.apache.iceberg.spark.SparkCatalog"
  spark.sql.catalog.lake.type: "rest"
  spark.sql.catalog.lake.uri: "https://biglake.googleapis.com/iceberg/v1/restcatalog"
  spark.sql.catalog.lake.warehouse: "bl://projects/my-gcp-project/catalogs/my_lakehouse_catalog"
  spark.sql.catalog.lake.header.x-goog-user-project: "my-gcp-project"
  spark.sql.catalog.lake.rest.auth.type: "org.apache.iceberg.gcp.auth.GoogleAuthManager"
  spark.sql.catalog.lake.io-impl: "org.apache.iceberg.gcp.gcs.GCSFileIO"
libraries:
  - glob:
      include: transformations/**
```

### BigLake Warehouse URI Variants (`sdpBigLakeMode`)

1. **BigLake Lakehouse Catalog (`sdpBigLakeMode: "lakehouse"`)**:
   - Set `spark.sql.catalog.<alias>.warehouse` to `"bl://projects/<project>/catalogs/<catalog_id>"`.
2. **GCS-Bucket BigLake REST Catalog (`sdpBigLakeMode: "gcs_bucket"`)**:
   - Set `spark.sql.catalog.<alias>.warehouse` to bucket-root `"gs://<warehouse-bucket>"` **only**.
   - **Never** append `/warehouse` or any path suffix to a GCS-bucket BigLake REST `warehouse` URI (`BadRequestException: Bucket name is invalid`).

## 3. Strict Ban on Static Spark Settings in `configuration:`

Never place static session, JVM, or cluster startup keys inside `spark-pipeline.yml` `configuration:`:
- `spark.jars`, `spark.jars.packages`, `spark.jars.ivy`, `spark.jars.ivySettings`
- `spark.sql.extensions`
- `spark.sql.catalogImplementation`
- `spark.driver.*` (such as `spark.driver.memory`, `spark.driver.extraJavaOptions`)
- `spark.executor.*` (such as `spark.executor.memory`, `spark.executor.instances`, `spark.executor.cores`)
- `spark.dynamicAllocation.*`
- `spark.files`, `spark.archives`, `dataproc.*`

Static keys in `spark-pipeline.yml` fail at runtime with `CANNOT_MODIFY_STATIC_CONFIG` (and fail `SdpPackager` validation in Managed Data Pipelines). Route all static properties and extra JARs to:
- `sdp.execution_config.properties` and `sdp.execution_config.jar_uris` in `sdp_stage.pipeline.yaml` (Managed Data Pipelines production path), and
- `--conf` / `--jars` CLI flags before the subcommand in `run_sdp_local.sh` and `run_sdp_dataproc.sh`.

## 4. Target Namespace Setup & Downstream 4-Part BigQuery / `dbt` Contract

- **Namespace creation (`SCHEMA_NOT_FOUND`)**: OSS SDP does not auto-create `<catalog>.<database>` and never permits `CREATE NAMESPACE` or `CREATE SCHEMA` inside pipeline transformation files (`UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND`). Ensure standalone runner scripts (`run_sdp_local.sh` and `run_sdp_dataproc.sh`) run `CREATE NAMESPACE IF NOT EXISTS <catalog>.<database>` before `spark-pipelines run` when `SDP_CREATE_NAMESPACE=true`.
- **Cross-stage 4-part contract**: Inside Spark, SDP addresses tables with its 3-part Spark alias (`lake.<database>.<table>`), whereas downstream BigQuery queries and `dbt` `sources.yml` definitions must query the **4-part `<project>.<catalog_id>.<database>.<table>`** identifier using the BigLake `<catalog_id>`. Document this mapping in the generated `README.md`.
