---
name: runner-serverless-spark
description: Google Cloud Serverless for Apache Spark (gcloud dataproc batches submit pyspark, runtime 4.0 and 3.0) batch/streaming execution rules, run_serverless_spark.sh, and custom container Dockerfile templates.
metadata:
  kind: runner
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [serverless-spark]
  transforms: []
  preloadWhen: []
  sources:
    - prompt
    - https://cloud.google.com/dataproc-serverless/docs/overview
    - https://cloud.google.com/dataproc-serverless/docs/concepts/versions/dataproc-serverless-versions
    - https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers
  validated: true
---

### Google Cloud Serverless for Apache Spark Runner (`run_serverless_spark.sh` & `Dockerfile`)

Every generated PySpark repository MUST include an executable `run_serverless_spark.sh` script and a `Dockerfile` at the repository root for running workloads on Google Cloud Serverless for Apache Spark (`gcloud dataproc batches submit pyspark`).

#### 1. Serverless Spark 4.x Runtime Baseline (`serverless_4.0` and `serverless_3.0`)
- **Default Runtime (`RUNTIME_VERSION=4.0`)**:
  - Apache Spark `4.2.0`, Python `3.13`, Java `25`, Scala `2.13`
- **Alternative Runtime (`RUNTIME_VERSION=3.0`)**:
  - Apache Spark `4.0.1`, Python `3.12`, Java `21`, Scala `2.13`
- **Compute Tier (`SERVERLESS_TIER`)**:
  - MUST default to `standard` (`dataproc.tier=standard`).
  - **NEVER silently default to `premium`** unless the user explicitly sets `SERVERLESS_TIER=premium`.
- **Pre-installed Managed Spark Connectors (NEVER pass to `spark.jars.packages`)**:
  - `spark-bigquery`, `gcs-connector`, `iceberg-spark-runtime` (with `GCSFileIO`), `hadoop-aws`, and `hadoop-azure` are pre-installed on the Serverless Spark runtime classpath. Do NOT attach duplicate Maven coordinates for these connectors.

#### 2. Critical Serverless Spark Operational Rules
1. **Normalize `DEPS_BUCKET` to a Root `gs://<bucket>` URI**:
   - `gcloud dataproc batches submit pyspark --deps-bucket` requires a bucket root (`gs://<bucket>`) or bucket name and rejects object subpaths if malformed.
   - `run_serverless_spark.sh` accepts `DEPS_BUCKET` (falling back to `GCS_STAGING_PATH` if `DEPS_BUCKET` is unset) and normalizes it to `gs://<bucket-name>` by stripping any trailing `/path/to/prefix`.
2. **Streaming Batch TTL (`BATCH_TTL` / `TTL`)**:
   - Serverless Spark enforces a **default batch TTL of 4 hours** (`14400s`) and a **maximum TTL of 14 days** (`1209600s` / `14d`).
   - For long-running Structured Streaming workloads (`spark.readStream` -> `writeStream`), pass `--ttl="${BATCH_TTL}"` (or `TTL`) and always configure a persistent `gs://` `checkpointLocation` (`CHECKPOINT_ROOT`) so restarted batches resume cleanly from the last committed micro-batch offset.
   - Prefer `trigger(availableNow=True)` for scheduled incremental backfill batches on Serverless Spark when low-latency continuous streaming is not required.
3. **External Connector JARs & Conscrypt Removal**:
   - **Cloud Spanner**: Pass `gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar` via `SPARK_JARS` (`--jars`).
   - **Cloud Bigtable**: Pass `gs://spark-lib/bigtable/spark-bigtable_2.13-0.1.0.jar` via `SPARK_JARS` (`--jars`).
   - **AlloyDB & SQL Server TLS**: Include `dataproc.artifacts.remove=conscrypt` in `SPARK_PROPERTIES` (`--properties`) alongside the JDBC / Socket Factory JARs in `SPARK_JARS`.
4. **Custom Container (`Dockerfile` & `--container-image`)**:
   - When the pipeline requires third-party PyPI packages (such as `google-cloud-pubsub`, `google-cloud-firestore`, `google-cloud-aiplatform`, `xgboost`, `boto3`, `paho-mqtt`, `cassandra-driver`, `redis`, or `requests` listed in `pipPackages` of the Execution Plan), build and push the repository `Dockerfile` to Artifact Registry and pass `CONTAINER_IMAGE` (`--container-image="${CONTAINER_IMAGE}"`).
   - The `Dockerfile` MUST install `requirements.txt` into the system/conda Python environment and run as the hardened non-root `spark` user (`UID 1099`, `GID 1099`).

#### 3. Reviewer Checklist for `run_serverless_spark.sh` and `Dockerfile`
- `run_serverless_spark.sh` starts with `#!/usr/bin/env bash` and `set -euo pipefail`.
- Validates `PROJECT_ID`, `REGION`, and `DEPS_BUCKET` (and normalizes `DEPS_BUCKET` to `gs://<bucket>`).
- Defaults `RUNTIME_VERSION` to `4.0` and `SERVERLESS_TIER` to `standard` (`dataproc.tier=standard`).
- Supports optional `SUBNET` (or `SUBNETWORK`), `SERVICE_ACCOUNT`, `BATCH_TTL` (or `TTL`), `CONTAINER_IMAGE`, `SPARK_JARS`, `SPARK_PACKAGES`, and `SPARK_PROPERTIES`.
- `Dockerfile` installs `requirements.txt`, copies `src/`, and switches to non-root `USER 1099:1099`.
