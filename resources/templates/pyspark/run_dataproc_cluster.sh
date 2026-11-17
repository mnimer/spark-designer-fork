#!/usr/bin/env bash
# ==============================================================================
# Google Cloud Dataproc Cluster PySpark Job Submission Script
#
# Target Runtime: Dataproc Image 3.0 (Apache Spark 4.1.2, Scala 2.13, Java 21)
#
# Required Environment Variables:
#   PROJECT_ID    - Google Cloud Project ID
#   REGION        - Dataproc Cluster region (default: us-central1)
#   CLUSTER_NAME  - Target Dataproc Cluster name
#
# Optional Environment Variables:
#   SPARK_JARS       - Comma-separated gs:// JAR URIs (e.g. Spanner / JDBC JARs)
#   SPARK_PACKAGES   - Comma-separated Scala 2.13 Maven coordinates
#   SPARK_PROPERTIES - Comma-separated Spark/Dataproc properties
#                      (e.g. dataproc.artifacts.remove=conscrypt)
#   PY_FILES         - Comma-separated additional .py / .zip files
#   CHECKPOINT_ROOT  - Structured Streaming checkpoint root URI (gs://...)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-us-central1}"
CLUSTER_NAME="${CLUSTER_NAME:-}"
SPARK_JARS="${SPARK_JARS:-}"
SPARK_PACKAGES="${SPARK_PACKAGES:-}"
SPARK_PROPERTIES="${SPARK_PROPERTIES:-}"
PY_FILES="${PY_FILES:-}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-}"

# ------------------------------------------------------------------------------
# 1. Validate Required Environment Variables
# ------------------------------------------------------------------------------
if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "YOUR_PROJECT_ID" ]]; then
  echo "Error: PROJECT_ID environment variable is required and must not be YOUR_PROJECT_ID." >&2
  exit 1
fi

if [[ -z "${REGION}" ]]; then
  echo "Error: REGION environment variable is required." >&2
  exit 1
fi

if [[ -z "${CLUSTER_NAME}" ]]; then
  echo "Error: CLUSTER_NAME environment variable is required for Dataproc Cluster submission." >&2
  exit 1
fi

# ------------------------------------------------------------------------------
# 2. Assemble gcloud dataproc jobs submit pyspark Arguments
# ------------------------------------------------------------------------------
GCLOUD_ARGS=(
  dataproc jobs submit pyspark src/pipeline.py
  "--project=${PROJECT_ID}"
  "--region=${REGION}"
  "--cluster=${CLUSTER_NAME}"
)

if [[ -n "${SPARK_JARS}" ]]; then
  GCLOUD_ARGS+=("--jars=${SPARK_JARS}")
fi

if [[ -n "${PY_FILES}" ]]; then
  GCLOUD_ARGS+=("--py-files=${PY_FILES}")
fi

# Merge SPARK_PACKAGES into SPARK_PROPERTIES when provided.
EFFECTIVE_PROPERTIES="${SPARK_PROPERTIES}"
if [[ -n "${SPARK_PACKAGES}" ]]; then
  if [[ -n "${EFFECTIVE_PROPERTIES}" ]]; then
    EFFECTIVE_PROPERTIES="${EFFECTIVE_PROPERTIES}#spark.jars.packages=${SPARK_PACKAGES}"
    GCLOUD_ARGS+=("--properties=^#^${EFFECTIVE_PROPERTIES}")
  else
    GCLOUD_ARGS+=("--properties=^#^spark.jars.packages=${SPARK_PACKAGES}")
  fi
elif [[ -n "${EFFECTIVE_PROPERTIES}" ]]; then
  GCLOUD_ARGS+=("--properties=${EFFECTIVE_PROPERTIES}")
fi

if [[ $# -gt 0 ]]; then
  GCLOUD_ARGS+=(-- "$@")
fi

echo "[run_dataproc_cluster] Submitting PySpark job to cluster '${CLUSTER_NAME}' in ${PROJECT_ID}/${REGION}..."
exec gcloud "${GCLOUD_ARGS[@]}"
