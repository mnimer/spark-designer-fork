#!/usr/bin/env bash
# ==============================================================================
# Google Cloud Serverless for Apache Spark Submission Script
#
# Target Runtime: Serverless Spark 4.0 (Spark 4.2.0, Python 3.13, Java 25)
#                 or 3.0 (Spark 4.0.1, Python 3.12, Java 21)
#
# Required Environment Variables:
#   PROJECT_ID       - Google Cloud Project ID
#   REGION           - GCP region (default: us-central1)
#   DEPS_BUCKET      - Staging GCS bucket URI (normalized to gs://<bucket>)
#
# Optional Environment Variables:
#   RUNTIME_VERSION  - Serverless Spark runtime version (default: 4.0; supports 3.0)
#   SERVERLESS_TIER  - Compute tier: standard (default) or premium
#   SUBNET           - VPC subnetwork name or URI (falls back to SUBNETWORK)
#   SERVICE_ACCOUNT  - Custom workload service account email
#   BATCH_TTL        - Batch time-to-live (e.g. 14400s, 24h; Serverless default 4h, max 14d)
#   CONTAINER_IMAGE  - Custom Artifact Registry container image URI
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
RUNTIME_VERSION="${RUNTIME_VERSION:-4.0}"
SERVERLESS_TIER="${SERVERLESS_TIER:-standard}"
RAW_DEPS_BUCKET="${DEPS_BUCKET:-${GCS_STAGING_PATH:-}}"
SUBNET="${SUBNET:-${SUBNETWORK:-}}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-}"
BATCH_TTL="${BATCH_TTL:-${TTL:-}}"
BATCH_ID="${BATCH_ID:-}"
CONTAINER_IMAGE="${CONTAINER_IMAGE:-}"
SPARK_JARS="${SPARK_JARS:-}"
SPARK_PACKAGES="${SPARK_PACKAGES:-}"
SPARK_PROPERTIES="${SPARK_PROPERTIES:-}"
PY_FILES="${PY_FILES:-}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-}"

# ------------------------------------------------------------------------------
# 1. Validate Required Environment Variables & Normalize DEPS_BUCKET
# ------------------------------------------------------------------------------
if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "YOUR_PROJECT_ID" ]]; then
  echo "Error: PROJECT_ID environment variable is required and must not be YOUR_PROJECT_ID." >&2
  exit 1
fi

if [[ -z "${REGION}" ]]; then
  echo "Error: REGION environment variable is required." >&2
  exit 1
fi

if [[ -z "${RAW_DEPS_BUCKET}" ]]; then
  echo "Error: DEPS_BUCKET environment variable (gs://<bucket>) is required for Serverless Spark." >&2
  exit 1
fi

# Strip gs:// prefix if present, extract only the root bucket segment, and re-prefix gs://
BUCKET_WITHOUT_SCHEME="${RAW_DEPS_BUCKET#gs://}"
BUCKET_ROOT_NAME="${BUCKET_WITHOUT_SCHEME%%/*}"
if [[ -z "${BUCKET_ROOT_NAME}" ]]; then
  echo "Error: DEPS_BUCKET '${RAW_DEPS_BUCKET}' is not a valid GCS bucket name or gs://<bucket> URI." >&2
  exit 1
fi
DEPS_BUCKET="gs://${BUCKET_ROOT_NAME}"

# Normalize SERVERLESS_TIER so it never silently defaults to premium
if [[ "${SERVERLESS_TIER}" != "premium" ]]; then
  SERVERLESS_TIER="standard"
fi

# ------------------------------------------------------------------------------
# 2. Assemble gcloud dataproc batches submit pyspark Arguments
# ------------------------------------------------------------------------------
GCLOUD_ARGS=(
  dataproc batches submit pyspark src/pipeline.py
  "--project=${PROJECT_ID}"
  "--region=${REGION}"
  "--version=${RUNTIME_VERSION}"
  "--deps-bucket=${DEPS_BUCKET}"
)

if [[ -n "${BATCH_ID}" ]]; then
  GCLOUD_ARGS+=("--batch=${BATCH_ID}")
fi

if [[ -n "${SUBNET}" ]]; then
  GCLOUD_ARGS+=("--subnet=${SUBNET}")
fi

if [[ -n "${SERVICE_ACCOUNT}" ]]; then
  GCLOUD_ARGS+=("--service-account=${SERVICE_ACCOUNT}")
fi

if [[ -n "${BATCH_TTL}" ]]; then
  GCLOUD_ARGS+=("--ttl=${BATCH_TTL}")
fi

if [[ -n "${CONTAINER_IMAGE}" ]]; then
  GCLOUD_ARGS+=("--container-image=${CONTAINER_IMAGE}")
fi

if [[ -n "${SPARK_JARS}" ]]; then
  GCLOUD_ARGS+=("--jars=${SPARK_JARS}")
fi

if [[ -n "${PY_FILES}" ]]; then
  GCLOUD_ARGS+=("--py-files=${PY_FILES}")
fi

# Build properties list: always enforce explicit dataproc.tier (default: standard)
PROP_ITEMS=("dataproc.tier=${SERVERLESS_TIER}")
if [[ -n "${SPARK_PROPERTIES}" ]]; then
  IFS=',' read -r -a USER_PROPS <<< "${SPARK_PROPERTIES}"
  for prop in "${USER_PROPS[@]}"; do
    trimmed_prop="${prop#"${prop%%[![:space:]]*}"}"
    trimmed_prop="${trimmed_prop%"${trimmed_prop##*[![:space:]]}"}"
    if [[ -n "${trimmed_prop}" && "${trimmed_prop}" != dataproc.tier=* ]]; then
      PROP_ITEMS+=("${trimmed_prop}")
    fi
  done
fi

if [[ -n "${SPARK_PACKAGES}" ]]; then
  PROP_ITEMS+=("spark.jars.packages=${SPARK_PACKAGES}")
  JOINED_PROPS="$(IFS='#'; echo "${PROP_ITEMS[*]}")"
  GCLOUD_ARGS+=("--properties=^#^${JOINED_PROPS}")
else
  JOINED_PROPS="$(IFS=','; echo "${PROP_ITEMS[*]}")"
  GCLOUD_ARGS+=("--properties=${JOINED_PROPS}")
fi

if [[ $# -gt 0 ]]; then
  GCLOUD_ARGS+=(-- "$@")
fi

echo "[run_serverless_spark] Submitting Serverless Spark batch (version=${RUNTIME_VERSION}, tier=${SERVERLESS_TIER}, deps=${DEPS_BUCKET})..."
exec gcloud "${GCLOUD_ARGS[@]}"
