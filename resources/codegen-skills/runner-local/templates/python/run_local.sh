#!/usr/bin/env bash
# ==============================================================================
# Local PySpark 4.x & Spark Connect Execution Script
#
# Modes:
#   1. Unit test mode:
#        ./run_local.sh --test [pytest-args...]
#   2. Spark Connect mode (ManagedSparkSession):
#        USE_SPARK_CONNECT=true PROJECT_ID=my-proj REGION=us-central1 ./run_local.sh [args...]
#   3. Local JVM mode (spark-submit / python fallback):
#        SPARK_MASTER="local[*]" ./run_local.sh [args...]
#
# Automatic bootstrap (no manual setup steps):
#   - Python: creates ./.venv and installs requirements.txt when the file changes.
#   - Java: selects a JDK 17 or 21. If none exists, downloads Eclipse Temurin 21
#     into a user cache. Spark Connect mode skips this step.
#   - Maven: resolves --packages from Maven Central into ./.spark-ivy.
#
# Bootstrap overrides:
#   PYTHON_BIN=/path/to/python3    Python that creates the venv (default: python3).
#   SKIP_VENV_BOOTSTRAP=true       Use PYTHON_BIN as is. Do not create or update .venv.
#   AUTO_INSTALL_JDK=false         Do not download a JDK. Fail if no JDK 17 or 21 exists.
#   JDK_CACHE_DIR=/path            Cache for the downloaded JDK
#                                  (default: ~/.cache/spark-pipeline-builder/jdk-21).
#   SPARK_IVY_SETTINGS=/path.xml   Custom Ivy settings file for --packages resolution.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
export PYTHONUNBUFFERED="1"

log() {
  echo "[run_local] $*"
}

die() {
  echo "[run_local] ERROR: $*" >&2
  exit 1
}

RUN_MODE="pipeline"
if [[ "${1:-}" == "--test" ]]; then
  RUN_MODE="test"
  shift
fi

# ------------------------------------------------------------------------------
# 1. Default Local Environment Variables
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

if [[ "${RUN_MODE}" == "pipeline" ]] && [[ "${USE_SPARK_CONNECT}" == "true" || -n "${SPARK_CONNECT_SESSION_TEMPLATE}" ]]; then
  export USE_SPARK_CONNECT="true"
  RUN_MODE="connect"
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
SKIP_VENV_BOOTSTRAP="${SKIP_VENV_BOOTSTRAP:-false}"
VENV_DIR="${SCRIPT_DIR}/.venv"
AUTO_INSTALL_JDK="${AUTO_INSTALL_JDK:-true}"
JDK_CACHE_DIR="${JDK_CACHE_DIR:-${XDG_CACHE_HOME:-${HOME}/.cache}/spark-pipeline-builder/jdk-21}"
SPARK_IVY_DIR="${SCRIPT_DIR}/.spark-ivy"
PYTHON=""

# ------------------------------------------------------------------------------
# 2. Python Virtual Environment Bootstrap
# ------------------------------------------------------------------------------
sha256_of() {
  "${PYTHON}" -c 'import hashlib, sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$1"
}

bootstrap_python() {
  if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    die "Python interpreter '${PYTHON_BIN}' not found. Install Python 3.10+ or set PYTHON_BIN."
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
      die "Cannot create a virtual environment. Install the python3-venv package or set PYTHON_BIN."
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

# ------------------------------------------------------------------------------
# 3. Java (JDK 17 or 21) Bootstrap
# ------------------------------------------------------------------------------
# Prints the major version of the JDK at the given home directory.
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

  # NOTE: Prefer JDK 21. Use JDK 17 only if no JDK 21 exists.
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
  local os
  local arch
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
  local metadata
  log "Querying the Adoptium API for the latest Eclipse Temurin JDK 21 (${os}/${arch})..."
  metadata="$(curl -fsSL --retry 3 "${api_url}")" || die "Cannot reach the Adoptium API. Install JDK 21 and set JAVA_HOME."

  local link
  local checksum
  link="$(printf '%s' "${metadata}" | "${PYTHON}" -c 'import json, sys; print(json.load(sys.stdin)[0]["binary"]["package"]["link"])')" \
    || die "The Adoptium API returned no JDK 21 package for ${os}/${arch}."
  checksum="$(printf '%s' "${metadata}" | "${PYTHON}" -c 'import json, sys; print(json.load(sys.stdin)[0]["binary"]["package"]["checksum"])')" \
    || die "The Adoptium API returned no checksum for the JDK 21 package."

  mkdir -p "${JDK_CACHE_DIR}"
  local work_dir
  work_dir="$(mktemp -d "${JDK_CACHE_DIR}/.download.XXXXXX")"
  log "Downloading ${link}..."
  if ! curl -fSL --retry 3 --progress-bar -o "${work_dir}/jdk.tar.gz" "${link}"; then
    rm -rf "${work_dir}"
    die "JDK download failed. Install JDK 21 and set JAVA_HOME."
  fi

  local actual
  actual="$(sha256_of "${work_dir}/jdk.tar.gz")"
  if [[ "${actual}" != "${checksum}" ]]; then
    rm -rf "${work_dir}"
    die "JDK checksum mismatch (expected ${checksum}, got ${actual})."
  fi
  log "Verified SHA-256 checksum ${checksum}."

  mkdir -p "${work_dir}/extract"
  tar -xzf "${work_dir}/jdk.tar.gz" -C "${work_dir}/extract"
  local extracted
  local target
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
    log "Ignoring JAVA_HOME=${JAVA_HOME} (JDK $(jdk_major "${JAVA_HOME}" 2>/dev/null || echo unknown)). PySpark 4.0 needs JDK 17 or 21."
  fi

  local found=""
  if found="$(find_supported_jdk)"; then
    :
  elif [[ "${AUTO_INSTALL_JDK}" == "true" ]]; then
    log "No JDK 17 or 21 found. Installing Eclipse Temurin 21 into ${JDK_CACHE_DIR}..."
    install_temurin_jdk
    found="$(find_supported_jdk)" || die "The downloaded JDK in ${JDK_CACHE_DIR} is not a working JDK 21."
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
if [[ "${RUN_MODE}" != "connect" ]]; then
  bootstrap_java
fi

# ------------------------------------------------------------------------------
# 4. Unit Test Mode (--test)
# ------------------------------------------------------------------------------
if [[ "${RUN_MODE}" == "test" ]]; then
  log "Running offline PySpark unit tests via pytest..."
  exec "${PYTHON}" -m pytest tests/ -v "$@"
fi

# ------------------------------------------------------------------------------
# 5. Optional Google Cloud Spark Connect Mode (ManagedSparkSession)
# ------------------------------------------------------------------------------
if [[ "${RUN_MODE}" == "connect" ]]; then
  log "Starting pipeline in Spark Connect mode (ManagedSparkSession)..."
  if [[ -n "${SPARK_CONNECT_SESSION_TEMPLATE}" ]]; then
    log "Session template: ${SPARK_CONNECT_SESSION_TEMPLATE}"
  fi
  exec "${PYTHON}" -m src.pipeline "$@"
fi

# ------------------------------------------------------------------------------
# 6. Maven Package Resolution (Ivy settings for --packages)
# ------------------------------------------------------------------------------
# NOTE: Spark's default Ivy chain trusts ~/.m2 entries that have a POM but no JAR.
# A Maven Central-only settings file avoids "download failed" errors.
SUBMIT_ARGS=(
  --master "${SPARK_MASTER}"
)

if [[ -n "${SPARK_JARS}" ]]; then
  SUBMIT_ARGS+=(--jars "${SPARK_JARS}")
fi

if [[ -n "${SPARK_PACKAGES}" ]]; then
  SUBMIT_ARGS+=(--packages "${SPARK_PACKAGES}")
  if [[ "${SPARK_PROPERTIES}" != *"spark.jars.ivySettings="* ]]; then
    if [[ -z "${SPARK_IVY_SETTINGS:-}" ]]; then
      SPARK_IVY_SETTINGS="${SPARK_IVY_DIR}/ivysettings.xml"
      mkdir -p "${SPARK_IVY_DIR}"
      cat >"${SPARK_IVY_SETTINGS}" <<'IVYSETTINGS'
<ivysettings>
  <settings defaultResolver="central"/>
  <resolvers>
    <ibiblio name="central" m2compatible="true" root="https://repo1.maven.org/maven2/"/>
  </resolvers>
</ivysettings>
IVYSETTINGS
    fi
    log "Resolving --packages with Ivy settings ${SPARK_IVY_SETTINGS}."
    SUBMIT_ARGS+=(--conf "spark.jars.ivySettings=${SPARK_IVY_SETTINGS}")
  fi
  if [[ "${SPARK_PROPERTIES}" != *"spark.jars.ivy="* ]]; then
    SUBMIT_ARGS+=(--conf "spark.jars.ivy=${SPARK_IVY_DIR}")
  fi
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

# ------------------------------------------------------------------------------
# 7. Local PySpark Execution (spark-submit or python -m src.pipeline)
# ------------------------------------------------------------------------------
if command -v spark-submit >/dev/null 2>&1 && [[ "${USE_PYTHON_DIRECT:-false}" != "true" ]]; then
  log "Submitting pipeline via $(command -v spark-submit) (master=${SPARK_MASTER})..."
  exec spark-submit "${SUBMIT_ARGS[@]}" src/pipeline.py "$@"
else
  if [[ -z "${PYSPARK_SUBMIT_ARGS:-}" ]]; then
    PYSPARK_SUBMIT_ARGS="$(printf '%q ' "${SUBMIT_ARGS[@]}")pyspark-shell"
    export PYSPARK_SUBMIT_ARGS
  fi
  log "Running pipeline directly via ${PYTHON} -m src.pipeline (master=${SPARK_MASTER})..."
  exec "${PYTHON}" -m src.pipeline "$@"
fi
