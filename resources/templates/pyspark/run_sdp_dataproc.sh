#!/usr/bin/env bash
# ==============================================================================
# Standalone Google Cloud Dataproc 4.0 (Spark 4.2) SDP Development Launcher
#
# Target Runtime:
#   Dataproc on Compute Engine image 4.0 (e.g. 4.0-debian13, Apache Spark 4.2)
#   created with --single-node, --optional-components=ICEBERG, and driver Python
#   package zstandard>=0.25.0:
#
#     gcloud dataproc clusters create "${CLUSTER_NAME}" \
#       --project="${PROJECT_ID}" --region="${REGION}" \
#       --image-version=4.0-debian13 \
#       --single-node \
#       --optional-components=ICEBERG \
#       --metadata="PIP_PACKAGES=zstandard>=0.25.0" \
#       --scopes=cloud-platform --enable-component-gateway \
#       --max-idle=2h
#
# Note: Production GCP deployments should prefer Managed Data Pipelines
# (sdp_stage.pipeline.yaml on Serverless Spark 4.0). This script runs SDP in
# Spark Connect driver-local mode on a standalone Dataproc 4.0 cluster.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

log() {
  echo "[run_sdp_dataproc] $*"
}

die() {
  echo "[run_sdp_dataproc] ERROR: $*" >&2
  exit 1
}

if [[ "${1:-}" == "--dry-run" ]]; then
  SDP_COMMAND="dry-run"
  shift
fi

PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-us-central1}"
CLUSTER_NAME="${CLUSTER_NAME:-}"
DEPS_BUCKET="${DEPS_BUCKET:-${GCS_STAGING_PATH:-}}"
SDP_SPEC="${SDP_SPEC:-spark-pipeline.yml}"
SDP_COMMAND="${SDP_COMMAND:-run}"
SDP_CATALOG="${SDP_CATALOG:-lake}"
SDP_DATABASE="${SDP_DATABASE:-}"
SDP_CREATE_NAMESPACE="${SDP_CREATE_NAMESPACE:-true}"
SDP_REFRESH="${SDP_REFRESH:-}"
SDP_FULL_REFRESH="${SDP_FULL_REFRESH:-}"
SDP_FULL_REFRESH_ALL="${SDP_FULL_REFRESH_ALL:-false}"
SPARK_JARS="${SPARK_JARS:-}"
SPARK_PROPERTIES="${SPARK_PROPERTIES:-spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions}"

# ------------------------------------------------------------------------------
# 1. Validate Required Environment Variables & Refresh Flag Compatibility
# ------------------------------------------------------------------------------
if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "YOUR_PROJECT_ID" ]]; then
  die "PROJECT_ID environment variable is required and must not be YOUR_PROJECT_ID."
fi

if [[ -z "${REGION}" ]]; then
  die "REGION environment variable is required."
fi

if [[ -z "${CLUSTER_NAME}" ]]; then
  die "CLUSTER_NAME environment variable is required for standalone Dataproc 4.0 SDP execution."
fi

if [[ -z "${DEPS_BUCKET}" ]]; then
  die "DEPS_BUCKET (or GCS_STAGING_PATH) gs:// URI is required to stage the SDP project."
fi

if [[ "${DEPS_BUCKET}" != gs://* ]]; then
  die "DEPS_BUCKET '${DEPS_BUCKET}' must start with gs://."
fi

[[ -f "${SDP_SPEC}" ]] || die "SDP spec file '${SDP_SPEC}' not found in ${SCRIPT_DIR}."

if [[ "${SDP_COMMAND}" != "run" && "${SDP_COMMAND}" != "dry-run" ]]; then
  die "Unsupported SDP_COMMAND='${SDP_COMMAND}'. Expected 'run' or 'dry-run'."
fi

if [[ "${SDP_FULL_REFRESH_ALL}" == "true" && ( -n "${SDP_REFRESH}" || -n "${SDP_FULL_REFRESH}" ) ]]; then
  die "CONFLICTING_PIPELINE_REFRESH_OPTIONS: --full-refresh-all cannot be combined with --refresh or --full-refresh."
fi

if [[ -n "${SDP_REFRESH}" && -n "${SDP_FULL_REFRESH}" ]]; then
  IFS=',' read -r -a refresh_arr <<< "${SDP_REFRESH}"
  IFS=',' read -r -a full_arr <<< "${SDP_FULL_REFRESH}"
  for refresh_item in "${refresh_arr[@]}"; do
    refresh_item="${refresh_item//[[:space:]]/}"
    [[ -z "${refresh_item}" ]] && continue
    for full_item in "${full_arr[@]}"; do
      full_item="${full_item//[[:space:]]/}"
      if [[ "${refresh_item}" == "${full_item}" ]]; then
        die "CONFLICTING_PIPELINE_REFRESH_OPTIONS: dataset '${refresh_item}' appears in both SDP_REFRESH and SDP_FULL_REFRESH."
      fi
    done
  done
fi

# ------------------------------------------------------------------------------
# 2. Stage SDP Project & Driver Wrapper to GCS
# ------------------------------------------------------------------------------
STAGE_ROOT="${DEPS_BUCKET%/}/sdp-staging/$(basename "${SCRIPT_DIR}")-$$"
PROJECT_URI="${STAGE_ROOT}/project"

WORK_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

mkdir -p "${WORK_DIR}/project"
cp "${SDP_SPEC}" "${WORK_DIR}/project/"
if [[ -d transformations ]]; then
  cp -r transformations "${WORK_DIR}/project/"
fi
if [[ -d src ]]; then
  cp -r src "${WORK_DIR}/project/"
fi

DRIVER_WRAPPER="${WORK_DIR}/run_sdp_on_dataproc_driver.py"
cat >"${DRIVER_WRAPPER}" <<'PYWRAPPER'
"""Standalone Dataproc 4.0 driver wrapper for Apache Spark 4.2 Declarative Pipelines.

Author: Mike Nimer <mikenimer@google.com>
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile


def parse_args() -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description="Run OSS Spark 4.2 SDP on Dataproc 4.0 driver.")
    parser.add_argument("--project_uri", required=True)
    parser.add_argument("--spec", default="spark-pipeline.yml")
    parser.add_argument("--command", choices=["run", "dry-run"], default="run")
    parser.add_argument("--create_namespace", default="")
    parser.add_argument("--conf", action="append", default=[])
    return parser.parse_known_args()


def ensure_zstandard() -> None:
    try:
        import zstandard  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "PACKAGE_NOT_INSTALLED: zstandard>=0.25.0 is required on the Dataproc 4.0 driver."
        ) from exc


def build_clean_spark_conf_dir(work_dir: str) -> str:
    clean_conf = os.path.join(work_dir, "spark-conf")
    os.makedirs(clean_conf, exist_ok=True)
    src_conf = "/etc/spark/conf"
    if os.path.isdir(src_conf):
        for entry in os.listdir(src_conf):
            full_src = os.path.join(src_conf, entry)
            full_dst = os.path.join(clean_conf, entry)
            if entry == "spark-defaults.conf" and os.path.isfile(full_src):
                with open(full_src, encoding="utf-8") as fin, open(full_dst, "w", encoding="utf-8") as fout:
                    for line in fin:
                        stripped = line.strip()
                        if stripped.startswith("spark.master") or stripped.startswith("spark.submit.deployMode"):
                            continue
                        fout.write(line)
            elif os.path.isfile(full_src):
                shutil.copy2(full_src, full_dst)
    return clean_conf


def create_namespace_if_needed(namespace_ident: str, spec_file: str, conf_items: list[str]) -> None:
    if not namespace_ident:
        return
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$", namespace_ident):
        raise ValueError(f"INVALID_IDENTIFIER: '{namespace_ident}' must be <catalog>.<database>.")
    import yaml  # type: ignore
    from pyspark.sql import SparkSession

    spec_doc = yaml.safe_load(open(spec_file, encoding="utf-8")) or {}
    builder = SparkSession.builder.appName("sdp-dataproc-namespace").master("local[1]")
    for k, v in (spec_doc.get("configuration") or {}).items():
        builder = builder.config(str(k), str(v))
    for item in conf_items:
        if "=" in item:
            k, v = item.split("=", 1)
            builder = builder.config(k.strip(), v.strip())
    spark = builder.getOrCreate()
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace_ident}")
    finally:
        spark.stop()


def main() -> int:
    args, extra_pipeline_args = parse_args()
    ensure_zstandard()

    work_dir = tempfile.mkdtemp(prefix="sdp_dataproc_")
    project_dir = os.path.join(work_dir, "project")
    os.makedirs(project_dir, exist_ok=True)
    subprocess.run(["gcloud", "storage", "cp", "-r", f"{args.project_uri}/*", project_dir], check=True)

    clean_conf_dir = build_clean_spark_conf_dir(work_dir)
    env = dict(os.environ)
    env.pop("MASTER", None)
    env["SPARK_CONF_DIR"] = clean_conf_dir
    env["PYTHONPATH"] = f"{project_dir}:{env.get('PYTHONPATH', '')}"

    spec_path = os.path.join(project_dir, args.spec)
    if args.command == "run" and args.create_namespace:
        create_namespace_if_needed(args.create_namespace, spec_path, args.conf)

    cli_py = "/usr/lib/spark/python/pyspark/pipelines/cli.py"
    cmd = [
        "/usr/lib/spark/bin/spark-submit",
        "--class",
        "org.apache.spark.deploy.SparkPipelines",
        "--remote",
        "local[*]",
    ]
    for c in args.conf:
        if c.strip():
            cmd.extend(["--conf", c.strip()])
    cmd.extend([cli_py, args.command, "--spec", spec_path, *extra_pipeline_args])
    completed = subprocess.run(cmd, cwd=project_dir, env=env, check=False)
    return completed.returncode


if __name__ == "__main__":
    sys.exit(main())
PYWRAPPER

log "Staging SDP project to ${PROJECT_URI}..."
gcloud storage cp -r "${WORK_DIR}/project" "${STAGE_ROOT}/"

# ------------------------------------------------------------------------------
# 3. Assemble gcloud dataproc jobs submit pyspark Command
# ------------------------------------------------------------------------------
GCLOUD_ARGS=(
  dataproc jobs submit pyspark "${DRIVER_WRAPPER}"
  "--project=${PROJECT_ID}"
  "--region=${REGION}"
  "--cluster=${CLUSTER_NAME}"
)

if [[ -n "${SPARK_JARS}" ]]; then
  GCLOUD_ARGS+=("--jars=${SPARK_JARS}")
fi

WRAPPER_ARGS=(
  "--project_uri=${PROJECT_URI}"
  "--spec=${SDP_SPEC}"
  "--command=${SDP_COMMAND}"
)

if [[ "${SDP_COMMAND}" == "run" && "${SDP_CREATE_NAMESPACE}" == "true" && -n "${SDP_CATALOG}" && -n "${SDP_DATABASE}" ]]; then
  WRAPPER_ARGS+=("--create_namespace=${SDP_CATALOG}.${SDP_DATABASE}")
fi

if [[ -n "${SPARK_PROPERTIES}" ]]; then
  IFS=',' read -r -a PROP_ARRAY <<< "${SPARK_PROPERTIES}"
  for prop in "${PROP_ARRAY[@]}"; do
    trimmed_prop="${prop#"${prop%%[![:space:]]*}"}"
    trimmed_prop="${trimmed_prop%"${trimmed_prop##*[![:space:]]}"}"
    if [[ -n "${trimmed_prop}" ]]; then
      WRAPPER_ARGS+=("--conf=${trimmed_prop}")
    fi
  done
fi

if [[ "${SDP_COMMAND}" == "run" ]]; then
  if [[ "${SDP_FULL_REFRESH_ALL}" == "true" ]]; then
    WRAPPER_ARGS+=(--full-refresh-all)
  else
    if [[ -n "${SDP_REFRESH}" ]]; then
      WRAPPER_ARGS+=(--refresh "${SDP_REFRESH}")
    fi
    if [[ -n "${SDP_FULL_REFRESH}" ]]; then
      WRAPPER_ARGS+=(--full-refresh "${SDP_FULL_REFRESH}")
    fi
  fi
fi

log "Submitting SDP '${SDP_COMMAND}' job to Dataproc 4.0 cluster '${CLUSTER_NAME}' (${PROJECT_ID}/${REGION})..."
gcloud "${GCLOUD_ARGS[@]}" -- "${WRAPPER_ARGS[@]}" "$@"

