---
name: runner-dataproc-cluster
description: Google Cloud Dataproc Cluster (GCE/GKE image 3.0, Spark 4.1.2) job submission rules, pre-installed connector classpath matrix, and run_dataproc_cluster.sh template.
metadata:
  kind: runner
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [dataproc-cluster]
  paradigms: [imperative]
  transforms: []
  preloadWhen: []
  sources:
    - prompt
    - https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-3.0
    - https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/pyspark
  validated: true
---

### Google Cloud Dataproc Cluster Runner (`run_dataproc_cluster.sh`)

Every generated PySpark repository MUST include an executable `run_dataproc_cluster.sh` script at the repository root that submits `src/pipeline.py` to a running Google Cloud Dataproc Cluster via `gcloud dataproc jobs submit pyspark`.

#### 1. Dataproc Image `3.0` Runtime Baseline (`cluster_3.0`)
- **Platform Image**: Dataproc `3.0` (`3.0-debian12` / `3.0-ubuntu22` / `3.0-rocky9`)
- **Bundled Versions**: Apache Spark `4.1.2`, Python `3.12`, Scala `2.13`, Java `21`
- **Pre-installed Managed Spark Connectors (NEVER pass to `spark.jars.packages`)**:
  - **BigQuery**: `spark-bigquery` (`.format("bigquery")` works out of the box)
  - **Cloud Storage**: `gcs-connector` (`gs://` URIs work out of the box)
  - **Apache Iceberg**: `iceberg-spark-runtime` (`.format("iceberg")` / BigLake Iceberg catalog)
  - **Multi-Cloud Object Storage**: `hadoop-aws` (`s3a://`) and `hadoop-azure` (`abfss://`, `wasbs://`)
  - Passing duplicate Maven coordinates for pre-installed connectors causes JVM `LinkageError` / shading conflicts on Dataproc clusters.

#### 2. External Connector JARs & Properties (`--jars` and `--properties`)
- **Cloud Spanner**: Attach `gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar` via `SPARK_JARS` (`--jars`).
- **Cloud Bigtable**: Attach `gs://spark-lib/bigtable/spark-bigtable_2.13-0.1.0.jar` via `SPARK_JARS` (`--jars`).
- **Cloud SQL / AlloyDB / Generic JDBC**:
  - Attach the JDBC driver JAR (and Cloud SQL / AlloyDB Socket Factory Uber JAR when Socket Factory auth is configured) from Cloud Storage (`gs://<bucket>/jars/<driver>.jar`) via `SPARK_JARS` (`--jars`).
  - **Conscrypt TLS Workaround**: When the pipeline includes **AlloyDB** (`AlloyDbSource` / `AlloyDbSink`) or **Microsoft SQL Server** (`jdbc:sqlserver://`), include `dataproc.artifacts.remove=conscrypt` in `SPARK_PROPERTIES` (`--properties`) so TLS handshakes succeed without Conscrypt ALPN errors.
- **Scala 2.13 Maven Packages**:
  - When the Execution Plan specifies `mavenPackages` (for example `io.delta:delta-spark_2.13:4.0.0`, `org.apache.hudi:hudi-spark4.0-bundle_2.13:1.0.1`, `org.mongodb.spark:mongo-spark-connector_2.13:10.4.1`, `net.snowflake:spark-snowflake_2.13:3.1.0`, `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2`), pass them via `SPARK_PACKAGES` (merged into `--properties="spark.jars.packages=..."`) or pre-staged `--jars`.

#### 3. Required Environment Variables & Validation in `run_dataproc_cluster.sh`
1. `PROJECT_ID`: Required. Must be a real GCP Project ID (reject empty or `YOUR_PROJECT_ID`).
2. `REGION`: Required (defaults to `us-central1` if unset, or validated before submission).
3. `CLUSTER_NAME`: Required. Name of the running Dataproc cluster in `${REGION}`.
4. Optional environment variables:
   - `SPARK_JARS`: Comma-separated `gs://` JAR URIs passed to `--jars="${SPARK_JARS}"`.
   - `SPARK_PROPERTIES`: Comma-separated `key=value` Spark/Dataproc properties passed to `--properties="${SPARK_PROPERTIES}"` (automatically merges `spark.jars.packages=${SPARK_PACKAGES}` if `SPARK_PACKAGES` is set, choosing an alternate gcloud delimiter `#^#` when values contain commas).
   - `PY_FILES`: Optional comma-separated `.py` or `.zip` archives passed to `--py-files="${PY_FILES}"`.
   - `CHECKPOINT_ROOT`: Forwarded into driver/executor environment or pipeline arguments when set.
5. All trailing CLI arguments (`"$@"`) MUST be forwarded after `--` so `src/pipeline.py` receives them.

#### 4. Reviewer Checklist for `run_dataproc_cluster.sh`
- Starts with `#!/usr/bin/env bash` and `set -euo pipefail`.
- Validates `PROJECT_ID`, `REGION`, and `CLUSTER_NAME` before calling `gcloud`.
- Invokes `gcloud dataproc jobs submit pyspark src/pipeline.py --project="${PROJECT_ID}" --region="${REGION}" --cluster="${CLUSTER_NAME}"`.
- Conditionally appends `--jars="${SPARK_JARS}"` and `--properties="${SPARK_PROPERTIES}"` when non-empty.
- Appends `-- "$@"` when pipeline CLI arguments are present.
