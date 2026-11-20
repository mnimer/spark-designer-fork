---
name: runner-sdp
description: Spark Declarative Pipelines runner templates for local Spark 4.2 Spark Connect, Managed Data Pipelines sdp stages on Serverless Spark 4.0, and standalone Dataproc 4.0 clusters.
metadata:
  kind: runner
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [sdp]
  paradigms: [sdp]
  transforms: []
  preloadWhen: []
  sources:
    - https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html
    - https://cloud.google.com/dataproc-serverless/docs/concepts/versions/dataproc-serverless-versions
    - https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-version-clusters
  validated: true
---

### Spark Declarative Pipelines Runners & Deployment Manifests (`runner-sdp`)

Every generated Spark Declarative Pipelines (`paradigm: sdp`) project targets **Apache Spark 4.2 or later** over Spark Connect (`spark.api.mode=connect`) and MUST include all three deployment deliverables below based on their templates:

1. **`sdp_stage.pipeline.yaml` (Primary Production Path — Google Cloud Managed Data Pipelines)**:
   - Configures an `sdp` `data_model` stage (`sdp: { spec_path: spark-pipeline.yml, execution_config: ... }`) packaged onto Google Cloud Serverless Spark runtime `4.0` (Apache Spark 4.2+).
   - Places all static Spark and cluster settings (`spark.sql.extensions`, `spark.driver.*`, `spark.executor.*`, `spark.dynamicAllocation.*`) in `sdp.execution_config.properties` and extra connector JARs (`gs://...`) in `sdp.execution_config.jar_uris` so `spark-pipeline.yml` remains 100% free of static settings (`CANNOT_MODIFY_STATIC_CONFIG`).
   - Never pass pre-installed Managed Spark connectors (`spark-bigquery`, `iceberg-spark-runtime`, `gcs-connector`, `hadoop-aws`, `hadoop-azure`) in `jar_uris` or `spark.jars.packages`.
2. **`run_sdp_local.sh` (Local Spark 4.2 Spark Connect Validation, Incremental Runs & Offline `pytest`)**:
   - `./run_sdp_local.sh --test`: Bootstraps `.venv` and JDK 21 (or 17) and runs `pytest tests/ -v` offline against `src/transforms.py` without cloud credentials.
   - `SDP_COMMAND=dry-run ./run_sdp_local.sh` (or `./run_sdp_local.sh --dry-run`): Runs `spark-pipelines --remote "local[*]" ... dry-run --spec spark-pipeline.yml` to validate the declarative dataset graph without writing tables.
   - `SDP_COMMAND=run ./run_sdp_local.sh`: Executes `CREATE NAMESPACE IF NOT EXISTS <catalog>.<database>` when `SDP_CREATE_NAMESPACE=true`, then runs `spark-pipelines --remote "local[*]" ... run --spec spark-pipeline.yml` with non-conflicting `--refresh`, `--full-refresh`, or `--full-refresh-all` flags.
   - Note in `README.md` that Maven Central lacks `iceberg-spark-runtime-4.2_2.13`, and local `dry-run` also loads `org.apache.iceberg.spark.SparkCatalog`. Advise `./run_sdp_local.sh --test` (offline `pytest`), supplying `SPARK_JARS`, or running on Serverless Spark `4.0` or Dataproc `4.0`.
3. **`run_sdp_dataproc.sh` (Standalone Self-Managed Dataproc Image `4.0` Development Launcher)**:
   - Targets a Dataproc Compute Engine cluster on image `4.0` (`4.0-debian13`, Apache Spark 4.2) created with `--single-node`, `--optional-components=ICEBERG`, and driver Python package `zstandard>=0.25.0`.
   - Stages the SDP project folder to GCS (`DEPS_BUCKET` / `GCS_STAGING_PATH`) and submits a driver wrapper job that:
     1. Copies `/etc/spark/conf/spark-defaults.conf` without `spark.master` or `spark.submit.deployMode` and unsets `MASTER` so `--remote "local[*]"` never fails with `Remote cannot be specified with master and/or deploy mode`.
     2. Runs `CREATE NAMESPACE IF NOT EXISTS <catalog>.<database>` prior to `run` when `SDP_CREATE_NAMESPACE=true` (`SCHEMA_NOT_FOUND` prevention).
     3. Invokes `org.apache.spark.deploy.SparkPipelines` with `/usr/lib/spark/python/pyspark/pipelines/cli.py` (never inside `pyspark.zip`), forwarding static `--conf` flags before `${SDP_COMMAND}` (`run` or `dry-run`) and refresh arguments after `--spec spark-pipeline.yml`.

#### Triggered Execution & Refresh Safety Contract
- OSS SDP runs in **triggered incremental mode** (`Trigger.AvailableNow`) and exits after processing available records. Never overlap two concurrent runs of the same pipeline because they share `<storage>/_checkpoints/`.
- `--full-refresh-all` cannot be combined with `--refresh` or `--full-refresh` (`CONFLICTING_PIPELINE_REFRESH_OPTIONS`), and `--refresh` / `--full-refresh` dataset lists must remain disjoint.
- Materialized views are truncated (`TRUNCATE TABLE`) at the start of every run, so readers see empty materialized views mid-run and after a failed run until the next successful run.
- External `dp.create_sink` targets cannot be protected by `pipelines.reset.allowed=false`. Running `--full-refresh-all` resets sink flow checkpoints and re-sends available source records as duplicates.
