#!/usr/bin/env bash
# ==============================================================================
# Local PySpark 4.x & Spark Connect Execution Script
#
# Modes:
#   1. Unit test mode:
#        ./run_local.sh --test [pytest-args...]
#   2. Spark Connect mode (ManagedSparkSession):
#        USE_SPARK_CONNECT=true PROJECT_ID=my-proj REGION=us-central1 ./run_local.sh [args...]
#   3. Local JVM mode (spark-submit / python3 fallback):
#        SPARK_MASTER="local[*]" ./run_local.sh [args...]
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
export PYTHONUNBUFFERED="1"

# ------------------------------------------------------------------------------
# 1. Unit Test Mode (--test)
# ------------------------------------------------------------------------------
if [[ "${1:-}" == "--test" ]]; then
  shift
  echo "[run_local] Running offline PySpark unit tests via pytest..."
  exec python3 -m pytest tests/ -v "$@"
fi

# ------------------------------------------------------------------------------
# 2. Default Local Environment Variables
# ------------------------------------------------------------------------------
export PROJECT_ID="${PROJECT_ID:-}"
export REGION="${REGION:-us-central1}"
export CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/tmp/spark-checkpoints}"
export SPARK_MASTER="${SPARK_MASTER:-local[*]}"
export SPARK_JARS="${SPARK_JARS:-}"
export SPARK_PACKAGES="${SPARK_PACKAGES:-}"
export SPARK_PROPERTIES="${SPARK_PROPERTIES:-}"
export USE_SPARK_CONNECT="${USE_SPARK_CONNECT:-false}"
export SPARK_CONNECT_SESSION_TEMPLATE="${SPARK_CONNECT_SESSION_TEMPLATE:-}"

# ------------------------------------------------------------------------------
# 3. Optional Google Cloud Spark Connect Mode (ManagedSparkSession)
# ------------------------------------------------------------------------------
if [[ "${USE_SPARK_CONNECT}" == "true" || -n "${SPARK_CONNECT_SESSION_TEMPLATE}" ]]; then
  export USE_SPARK_CONNECT="true"
  echo "[run_local] Starting pipeline in Spark Connect mode (ManagedSparkSession)..."
  if [[ -n "${SPARK_CONNECT_SESSION_TEMPLATE}" ]]; then
    echo "[run_local] Session template: ${SPARK_CONNECT_SESSION_TEMPLATE}"
  fi
  exec python3 -m src.pipeline "$@"
fi

# ------------------------------------------------------------------------------
# 4. Local PySpark Execution (spark-submit or python3 -m src.pipeline)
# ------------------------------------------------------------------------------
if command -v spark-submit >/dev/null 2>&1 && [[ "${USE_PYTHON_DIRECT:-false}" != "true" ]]; then
  echo "[run_local] Submitting pipeline via spark-submit (master=${SPARK_MASTER})..."
  SUBMIT_ARGS=(
    --master "${SPARK_MASTER}"
  )

  if [[ -n "${SPARK_JARS}" ]]; then
    SUBMIT_ARGS+=(--jars "${SPARK_JARS}")
  fi

  if [[ -n "${SPARK_PACKAGES}" ]]; then
    SUBMIT_ARGS+=(--packages "${SPARK_PACKAGES}")
  fi

  if [[ -n "${SPARK_PROPERTIES}" ]]; then
    IFS=',' read -r -a PROP_ARRAY <<< "${SPARK_PROPERTIES}"
    for prop in "${PROP_ARRAY[@]}"; do
      trimmed_prop="${prop#"${prop%%[![:space:]]*}"}"
      trimmed_prop="${trimmed_prop%"${trimmed_prop##*[![:space:]]}"}"
      if [[ -n "${trimmed_prop}" ]]; then
        SUBMIT_ARGS+=(--conf "${trimmed_prop}")
      fi
    done
  fi

  exec spark-submit "${SUBMIT_ARGS[@]}" src/pipeline.py "$@"
else
  echo "[run_local] Running pipeline directly via python3 -m src.pipeline (master=${SPARK_MASTER})..."
  exec python3 -m src.pipeline "$@"
fi
