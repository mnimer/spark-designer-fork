#!/usr/bin/env bash
# ==============================================================================
# Local Apache Spark 4.2+ Declarative Pipelines (SDP) & Spark Connect Runner
#
# Modes:
#   1. Offline unit test mode (pure src/transforms.py helpers):
#        ./run_sdp_local.sh --test [pytest-args...]
#   2. Graph validation dry-run (writes nothing):
#        SDP_COMMAND=dry-run ./run_sdp_local.sh
#        ./run_sdp_local.sh --dry-run
#   3. Triggered incremental or refresh run over local Spark 4.2 Connect:
#        SDP_COMMAND=run ./run_sdp_local.sh
#
# Environment Variables:
#   SDP_SPEC              Path to SDP spec file (default: spark-pipeline.yml).
#   SDP_COMMAND           Subcommand: 'run' or 'dry-run' (default: run).
#   SDP_REMOTE            Spark Connect remote string (default: local[*]).
#   SDP_STORAGE           Optional checkpoint storage URI override.
#   SDP_CATALOG           Target catalog identifier for namespace setup (default: lake).
#   SDP_DATABASE          Target database/namespace identifier for setup.
#   SDP_CREATE_NAMESPACE  Run CREATE NAMESPACE IF NOT EXISTS before 'run' (default: true).
#   SDP_REFRESH           Comma-separated dataset names for --refresh.
#   SDP_FULL_REFRESH      Comma-separated dataset names for --full-refresh.
#   SDP_FULL_REFRESH_ALL  Set 'true' for --full-refresh-all (default: false).
#   SPARK_JARS            Comma-separated extra JAR paths or URIs.
#   SPARK_PACKAGES        Comma-separated Scala 2.13 Maven coordinates.
#   SPARK_PROPERTIES      Comma-separated static Spark key=value settings passed via --conf.
#   SPARK_IVY_SETTINGS    Optional custom Ivy settings XML file for --packages resolution.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
export PYTHONUNBUFFERED="1"

log() {
  echo "[run_sdp_local] $*"
}

die() {
  echo "[run_sdp_local] ERROR: $*" >&2
  exit 1
}

RUN_MODE="sdp"
if [[ "${1:-}" == "--test" ]]; then
  RUN_MODE="test"
  shift
elif [[ "${1:-}" == "--dry-run" ]]; then
  SDP_COMMAND="dry-run"
  shift
fi

# ------------------------------------------------------------------------------
# 1. Environment Defaults & Refresh Option Validation
# ------------------------------------------------------------------------------
SDP_SPEC="${SDP_SPEC:-spark-pipeline.yml}"
SDP_COMMAND="${SDP_COMMAND:-run}"
SDP_REMOTE="${SDP_REMOTE:-local[*]}"
SDP_STORAGE="${SDP_STORAGE:-}"
SDP_CATALOG="${SDP_CATALOG:-lake}"
SDP_DATABASE="${SDP_DATABASE:-}"
SDP_CREATE_NAMESPACE="${SDP_CREATE_NAMESPACE:-true}"
SDP_REFRESH="${SDP_REFRESH:-}"
SDP_FULL_REFRESH="${SDP_FULL_REFRESH:-}"
SDP_FULL_REFRESH_ALL="${SDP_FULL_REFRESH_ALL:-false}"
SPARK_JARS="${SPARK_JARS:-}"
SPARK_PACKAGES="${SPARK_PACKAGES:-}"
SPARK_PROPERTIES="${SPARK_PROPERTIES:-}"

PYTHON_BIN="${PYTHON_BIN:-python3}"
SKIP_VENV_BOOTSTRAP="${SKIP_VENV_BOOTSTRAP:-false}"
VENV_DIR="${SCRIPT_DIR}/.venv"
AUTO_INSTALL_JDK="${AUTO_INSTALL_JDK:-true}"
JDK_CACHE_DIR="${JDK_CACHE_DIR:-${XDG_CACHE_HOME:-${HOME}/.cache}/spark-pipeline-builder/jdk-21}"
SPARK_IVY_DIR="${SCRIPT_DIR}/.spark-ivy"
RESOLVED_IVY_SETTINGS="${SPARK_IVY_SETTINGS:-}"
PYTHON=""

validate_sdp_flags() {
  if [[ "${SDP_COMMAND}" != "run" && "${SDP_COMMAND}" != "dry-run" ]]; then
    die "Unsupported SDP_COMMAND='${SDP_COMMAND}'. Expected 'run' or 'dry-run'."
  fi

  if [[ "${SDP_FULL_REFRESH_ALL}" == "true" ]]; then
    if [[ -n "${SDP_REFRESH}" || -n "${SDP_FULL_REFRESH}" ]]; then
      die "CONFLICTING_PIPELINE_REFRESH_OPTIONS: --full-refresh-all cannot be combined with --refresh or --full-refresh."
    fi
  fi

  if [[ -n "${SDP_REFRESH}" && -n "${SDP_FULL_REFRESH}" ]]; then
    local refresh_item full_item
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
}

check_local_iceberg_runtime() {
  local uses_iceberg="false"
  if printf '%s\n' "${SPARK_PROPERTIES}" | grep -Eiq 'org\.apache\.iceberg'; then
    uses_iceberg="true"
  elif awk '{ sub(/^[[:space:]]*#.*/, ""); line = tolower($0); if (line ~ /org\.apache\.iceberg/ || line ~ /format[[:space:]]*:[[:space:]]*[\042\047]?iceberg[\042\047]?([[:space:]]|$)/) { found = 1; exit 0 } } END { exit(found ? 0 : 1) }' "${SDP_SPEC}"; then
    uses_iceberg="true"
  elif [[ -d "${SCRIPT_DIR}/transformations" ]]; then
    local py_file
    for py_file in "${SCRIPT_DIR}"/transformations/*.py "${SCRIPT_DIR}"/transformations/*/*.py; do
      [[ -f "${py_file}" ]] || continue
      if awk '{ sub(/#.*/, ""); line = tolower($0); if (line ~ /org\.apache\.iceberg/ || line ~ /format[[:space:]]*=[[:space:]]*[\042\047]iceberg[\042\047]/) { found = 1; exit 0 } } END { exit(found ? 0 : 1) }' "${py_file}"; then
        uses_iceberg="true"
        break
      fi
    done
  fi

  if [[ "${uses_iceberg}" == "true" ]]; then
    local lower_jars_pkgs
    lower_jars_pkgs="$(printf '%s,%s' "${SPARK_JARS}" "${SPARK_PACKAGES}" | tr '[:upper:]' '[:lower:]')"
    if [[ "${lower_jars_pkgs}" != *"iceberg"* ]]; then
      die "MISSING_LOCAL_ICEBERG_RUNTIME: Pipeline uses Apache Iceberg (org.apache.iceberg), but Apache Iceberg has not published iceberg-spark-runtime-4.2_2.13 to Maven Central for local Spark 4.2 execution. Run this SDP pipeline on GCP Serverless Spark 4.0 or Dataproc Cluster 4.0 (which pre-install the Spark 4.2 Iceberg runtime), supply a custom Spark 4.2 Iceberg JAR via SPARK_JARS=/path/to/iceberg-spark-runtime-4.2_2.13.jar, or run offline transformation unit tests locally with './run_sdp_local.sh --test'."
    fi
  fi
}

if [[ "${RUN_MODE}" == "sdp" ]]; then
  validate_sdp_flags
  [[ -f "${SDP_SPEC}" ]] || die "SDP spec file '${SDP_SPEC}' not found in ${SCRIPT_DIR}."
  check_local_iceberg_runtime
fi

# ------------------------------------------------------------------------------
# 2. Python Virtual Environment & JDK 17/21 Bootstrap
# ------------------------------------------------------------------------------
sha256_of() {
  "${PYTHON}" -c 'import hashlib, sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$1"
}

bootstrap_python() {
  if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    die "Python interpreter '${PYTHON_BIN}' not found. Install Python 3.11+ or set PYTHON_BIN."
  fi

  if [[ "${SKIP_VENV_BOOTSTRAP}" == "true" ]]; then
    PYTHON="$(command -v "${PYTHON_BIN}")"
    log "SKIP_VENV_BOOTSTRAP=true. Using ${PYTHON} without installing requirements."
    return 0
  fi

  if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
    log "Creating Python virtual environment at ${VENV_DIR} with ${PYTHON_BIN}..."
    if ! "${PYTHON_BIN}" -m venv "${VENV_DIR}"; then
      rm -rf "${VENV_DIR}"
      die "Cannot create a virtual environment. Install python3-venv or set PYTHON_BIN."
    fi
  fi
  PYTHON="${VENV_DIR}/bin/python"

  [[ -f requirements.txt ]] || die "requirements.txt not found in ${SCRIPT_DIR}."
  local stamp_file="${VENV_DIR}/.requirements.sha256"
  local current_hash
  current_hash="$(sha256_of requirements.txt)"
  if [[ -f "${stamp_file}" && "$(cat "${stamp_file}")" == "${current_hash}" ]]; then
    log "requirements.txt is unchanged. Reusing ${VENV_DIR}."
  else
    log "Installing requirements.txt into ${VENV_DIR}..."
    "${PYTHON}" -m pip install --disable-pip-version-check -r requirements.txt
    printf '%s\n' "${current_hash}" >"${stamp_file}"
  fi

  export VIRTUAL_ENV="${VENV_DIR}"
  export PATH="${VENV_DIR}/bin:${PATH}"
}

jdk_major() {
  local home="$1"
  local version=""
  [[ -x "${home}/bin/java" ]] || return 1
  if [[ -f "${home}/release" ]]; then
    version="$(sed -n 's/^JAVA_VERSION="\(.*\)"$/\1/p' "${home}/release")"
    version="${version%%$'\n'*}"
  fi
  if [[ -z "${version}" ]]; then
    version="$("${home}/bin/java" -version 2>&1 | awk -F'"' '/version/ { print $2; exit }')"
  fi
  [[ -n "${version}" ]] || return 1
  version="${version#1.}"
  printf '%s\n' "${version%%[.+-]*}"
}

is_supported_jdk() {
  local major
  major="$(jdk_major "$1" 2>/dev/null)" || return 1
  [[ "${major}" == "21" || "${major}" == "17" ]]
}

find_supported_jdk() {
  local candidates=()
  local dir
  local version
  local java_path
  if [[ -x /usr/libexec/java_home ]]; then
    for version in 21 17; do
      if dir="$(/usr/libexec/java_home -v "${version}" 2>/dev/null)"; then
        candidates+=("${dir}")
      fi
    done
  fi
  if java_path="$(command -v java 2>/dev/null)"; then
    java_path="$(readlink -f "${java_path}" 2>/dev/null || printf '%s' "${java_path}")"
    dir="$(dirname "$(dirname "${java_path}")")"
    if [[ -f "${dir}/release" ]]; then
      candidates+=("${dir}")
    fi
  fi
  for dir in \
    "${JDK_CACHE_DIR}"/*/ \
    "${JDK_CACHE_DIR}"/*/Contents/Home/ \
    /usr/lib/jvm/*/ \
    "${HOME}"/.sdkman/candidates/java/*/ \
    "${HOME}"/.jdks/*/ \
    /Library/Java/JavaVirtualMachines/*/Contents/Home/ \
    "${HOME}"/Library/Java/JavaVirtualMachines/*/Contents/Home/; do
    candidates+=("${dir%/}")
  done

  for version in 21 17; do
    for dir in "${candidates[@]}"; do
      if [[ "$(jdk_major "${dir}" 2>/dev/null || true)" == "${version}" ]]; then
        printf '%s\n' "${dir}"
        return 0
      fi
    done
  done
  return 1
}

install_temurin_jdk() {
  local os arch
  case "$(uname -s)" in
    Linux) os="linux" ;;
    Darwin) os="mac" ;;
    *) die "Automatic JDK download supports Linux and macOS only. Install JDK 21 and set JAVA_HOME." ;;
  esac
  case "$(uname -m)" in
    x86_64 | amd64) arch="x64" ;;
    aarch64 | arm64) arch="aarch64" ;;
    *) die "Automatic JDK download does not support CPU '$(uname -m)'. Install JDK 21 and set JAVA_HOME." ;;
  esac
  command -v curl >/dev/null 2>&1 || die "curl is required to download a JDK. Install JDK 21 and set JAVA_HOME."
  command -v tar >/dev/null 2>&1 || die "tar is required to unpack a JDK. Install JDK 21 and set JAVA_HOME."

  local api_url="https://api.adoptium.net/v3/assets/latest/21/hotspot?architecture=${arch}&image_type=jdk&os=${os}&vendor=eclipse"
  local metadata link checksum
  log "Querying Adoptium API for Eclipse Temurin JDK 21 (${os}/${arch})..."
  metadata="$(curl -fsSL --retry 3 "${api_url}")" || die "Cannot reach Adoptium API. Install JDK 21 and set JAVA_HOME."
  link="$(printf '%s' "${metadata}" | "${PYTHON}" -c 'import json, sys; print(json.load(sys.stdin)[0]["binary"]["package"]["link"])')" \
    || die "Adoptium API returned no JDK 21 package."
  checksum="$(printf '%s' "${metadata}" | "${PYTHON}" -c 'import json, sys; print(json.load(sys.stdin)[0]["binary"]["package"]["checksum"])')" \
    || die "Adoptium API returned no checksum."

  mkdir -p "${JDK_CACHE_DIR}"
  local work_dir
  work_dir="$(mktemp -d "${JDK_CACHE_DIR}/.download.XXXXXX")"
  if ! curl -fSL --retry 3 --progress-bar -o "${work_dir}/jdk.tar.gz" "${link}"; then
    rm -rf "${work_dir}"
    die "JDK download failed."
  fi
  local actual
  actual="$(sha256_of "${work_dir}/jdk.tar.gz")"
  if [[ "${actual}" != "${checksum}" ]]; then
    rm -rf "${work_dir}"
    die "JDK checksum mismatch (expected ${checksum}, got ${actual})."
  fi
  mkdir -p "${work_dir}/extract"
  tar -xzf "${work_dir}/jdk.tar.gz" -C "${work_dir}/extract"
  local extracted target
  for extracted in "${work_dir}"/extract/*/; do
    extracted="${extracted%/}"
    target="${JDK_CACHE_DIR}/$(basename "${extracted}")"
    rm -rf "${target}"
    mv "${extracted}" "${target}"
  done
  rm -rf "${work_dir}"
}

bootstrap_java() {
  if [[ -n "${JAVA_HOME:-}" ]]; then
    if is_supported_jdk "${JAVA_HOME}"; then
      log "Using JAVA_HOME=${JAVA_HOME} (JDK $(jdk_major "${JAVA_HOME}"))."
      export PATH="${JAVA_HOME}/bin:${PATH}"
      return 0
    fi
    log "Ignoring JAVA_HOME=${JAVA_HOME}. Spark 4.2 local execution requires JDK 17 or 21."
  fi

  local found=""
  if found="$(find_supported_jdk)"; then
    :
  elif [[ "${AUTO_INSTALL_JDK}" == "true" ]]; then
    log "No JDK 17 or 21 found. Installing Eclipse Temurin 21 into ${JDK_CACHE_DIR}..."
    install_temurin_jdk
    found="$(find_supported_jdk)" || die "Downloaded JDK in ${JDK_CACHE_DIR} is not a valid JDK 21."
  else
    die "No JDK 17 or 21 found and AUTO_INSTALL_JDK=false. Install JDK 21 and set JAVA_HOME."
  fi

  export JAVA_HOME="${found}"
  export PATH="${JAVA_HOME}/bin:${PATH}"
  log "Using JAVA_HOME=${JAVA_HOME} (JDK $(jdk_major "${JAVA_HOME}"))."
}

bootstrap_python
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-${PYTHON}}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-${PYTHON}}"
bootstrap_java

# ------------------------------------------------------------------------------
# 3. Offline Unit Test Mode (--test)
# ------------------------------------------------------------------------------
if [[ "${RUN_MODE}" == "test" ]]; then
  log "Running offline SDP unit tests via pytest..."
  exec "${PYTHON}" -m pytest tests/ -v "$@"
fi

# ------------------------------------------------------------------------------
# 4. Prevent MASTER / Deploy-Mode Conflicts with Spark Connect (--remote)
# ------------------------------------------------------------------------------
unset MASTER || true

# ------------------------------------------------------------------------------
# 5. Assemble Static Spark Connect Launcher Options (Before Subcommand)
# ------------------------------------------------------------------------------
# NOTE: Spark's default Ivy chain trusts ~/.m2 entries that have a POM but no JAR.
# A Maven Central-only settings file avoids "download failed" errors.
LAUNCHER_ARGS=(
  --remote "${SDP_REMOTE}"
)

if [[ -n "${SPARK_JARS}" ]]; then
  LAUNCHER_ARGS+=(--jars "${SPARK_JARS}")
fi

if [[ -n "${SPARK_PACKAGES}" ]]; then
  LAUNCHER_ARGS+=(--packages "${SPARK_PACKAGES}")
  if [[ "${SPARK_PROPERTIES}" != *"spark.jars.ivySettings="* ]]; then
    if [[ -z "${RESOLVED_IVY_SETTINGS}" ]]; then
      RESOLVED_IVY_SETTINGS="${SPARK_IVY_DIR}/ivysettings.xml"
      mkdir -p "${SPARK_IVY_DIR}"
      cat >"${RESOLVED_IVY_SETTINGS}" <<'IVYSETTINGS'
<ivysettings>
  <settings defaultResolver="central"/>
  <resolvers>
    <ibiblio name="central" m2compatible="true" root="https://repo1.maven.org/maven2/"/>
  </resolvers>
</ivysettings>
IVYSETTINGS
    fi
    log "Resolving --packages with Ivy settings ${RESOLVED_IVY_SETTINGS}."
    LAUNCHER_ARGS+=(--conf "spark.jars.ivySettings=${RESOLVED_IVY_SETTINGS}")
  fi
  if [[ "${SPARK_PROPERTIES}" != *"spark.jars.ivy="* ]]; then
    LAUNCHER_ARGS+=(--conf "spark.jars.ivy=${SPARK_IVY_DIR}")
  fi
fi

if [[ -n "${SPARK_PROPERTIES}" ]]; then
  IFS=',' read -r -a PROP_ARRAY <<< "${SPARK_PROPERTIES}"
  for prop in "${PROP_ARRAY[@]}"; do
    trimmed_prop="${prop#"${prop%%[![:space:]]*}"}"
    trimmed_prop="${trimmed_prop%"${trimmed_prop##*[![:space:]]}"}"
    if [[ -n "${trimmed_prop}" ]]; then
      if [[ "${trimmed_prop}" == "spark.api.mode=classic" ]]; then
        die "SDP requires Spark Connect (spark.api.mode=connect). Remove spark.api.mode=classic."
      fi
      LAUNCHER_ARGS+=(--conf "${trimmed_prop}")
    fi
  done
fi

# ------------------------------------------------------------------------------
# 6. Optional Target Namespace Setup Prior to 'run' (SCHEMA_NOT_FOUND Guard)
# ------------------------------------------------------------------------------
if [[ "${SDP_COMMAND}" == "run" && "${SDP_CREATE_NAMESPACE}" == "true" && -n "${SDP_CATALOG}" && -n "${SDP_DATABASE}" ]]; then
  if [[ ! "${SDP_CATALOG}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    die "INVALID_IDENTIFIER: SDP_CATALOG '${SDP_CATALOG}' must be a plain Spark identifier."
  fi
  if [[ ! "${SDP_DATABASE}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    die "INVALID_IDENTIFIER: SDP_DATABASE '${SDP_DATABASE}' must be a plain Spark identifier."
  fi
  log "Ensuring target namespace exists: CREATE NAMESPACE IF NOT EXISTS ${SDP_CATALOG}.${SDP_DATABASE}"
  "${PYTHON}" - "${SDP_SPEC}" "${SDP_CATALOG}" "${SDP_DATABASE}" "${SPARK_PROPERTIES}" "${SPARK_JARS}" "${SPARK_PACKAGES}" "${RESOLVED_IVY_SETTINGS}" "${SPARK_IVY_DIR}" <<'PYNAMESPACE'
import sys

spec_path = sys.argv[1]
catalog_name = sys.argv[2]
db_name = sys.argv[3]
raw_props = sys.argv[4] if len(sys.argv) > 4 else ""
raw_jars = sys.argv[5] if len(sys.argv) > 5 else ""
raw_packages = sys.argv[6] if len(sys.argv) > 6 else ""
ivy_settings = sys.argv[7] if len(sys.argv) > 7 else ""
ivy_dir = sys.argv[8] if len(sys.argv) > 8 else ""
try:
    import yaml  # type: ignore
    spec_doc = yaml.safe_load(open(spec_path, encoding="utf-8")) or {}
except Exception:
    spec_doc = {}

from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("sdp-create-namespace").master("local[1]")
if raw_jars.strip():
    builder = builder.config("spark.jars", raw_jars.strip())
if raw_packages.strip():
    builder = builder.config("spark.jars.packages", raw_packages.strip())
    if ivy_settings.strip():
        builder = builder.config("spark.jars.ivySettings", ivy_settings.strip())
    if ivy_dir.strip():
        builder = builder.config("spark.jars.ivy", ivy_dir.strip())
for k, v in (spec_doc.get("configuration") or {}).items():
    builder = builder.config(str(k), str(v))
for entry in (raw_props or "").split(","):
    entry = entry.strip()
    if "=" in entry:
        k, v = entry.split("=", 1)
        builder = builder.config(k.strip(), v.strip())

spark = builder.getOrCreate()
try:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{db_name}")
finally:
    spark.stop()
PYNAMESPACE
fi

# ------------------------------------------------------------------------------
# 7. Assemble Subcommand & Refresh Flags and Execute spark-pipelines
# ------------------------------------------------------------------------------
SUBCOMMAND_ARGS=(
  "${SDP_COMMAND}"
  --spec "${SDP_SPEC}"
)

if [[ "${SDP_COMMAND}" == "run" ]]; then
  if [[ "${SDP_FULL_REFRESH_ALL}" == "true" ]]; then
    SUBCOMMAND_ARGS+=(--full-refresh-all)
  else
    if [[ -n "${SDP_REFRESH}" ]]; then
      SUBCOMMAND_ARGS+=(--refresh "${SDP_REFRESH}")
    fi
    if [[ -n "${SDP_FULL_REFRESH}" ]]; then
      SUBCOMMAND_ARGS+=(--full-refresh "${SDP_FULL_REFRESH}")
    fi
  fi
fi

if ! command -v spark-pipelines >/dev/null 2>&1; then
  die "spark-pipelines CLI not found on PATH. Install Apache Spark 4.2+ (pyspark>=4.2.0 with zstandard>=0.25.0)."
fi

log "Executing: spark-pipelines ${LAUNCHER_ARGS[*]} ${SUBCOMMAND_ARGS[*]} $*"
exec spark-pipelines "${LAUNCHER_ARGS[@]}" "${SUBCOMMAND_ARGS[@]}" "$@"
