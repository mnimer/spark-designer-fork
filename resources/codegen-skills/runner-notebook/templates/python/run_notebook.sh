#!/usr/bin/env bash
# ==============================================================================
# Jupyter Notebook (.ipynb) PySpark 4.x Execution Script
#
# Modes:
#   1. Headless execution mode (default):
#        ./run_notebook.sh [--execute] [nbconvert-args...]
#   2. Interactive JupyterLab mode:
#        ./run_notebook.sh --lab [jupyter-lab-args...]
#   3. Offline unit test mode:
#        ./run_notebook.sh --test [pytest-args...]
#
# Automatic bootstrap (no manual setup steps):
#   - Python: creates ./.venv and installs requirements.txt when the file changes.
#   - Java: selects a JDK 17 or 21. If none exists, downloads Eclipse Temurin 21
#     into a user cache.
#   - Maven: resolves --packages from Maven Central into ./.spark-ivy.
#
# Bootstrap overrides:
#   PYTHON_BIN=/path/to/python3    Python that creates the venv (default: python3).
#   SKIP_VENV_BOOTSTRAP=true       Use PYTHON_BIN as is. Do not create or update .venv.
#   AUTO_INSTALL_JDK=false         Do not download a JDK. Fail if no JDK 17 or 21 exists.
#   JDK_CACHE_DIR=/path            Cache for the downloaded JDK
#                                  (default: ~/.cache/spark-pipeline-builder/jdk-21).
#   SPARK_IVY_SETTINGS=/path.xml   Custom Ivy settings file for --packages resolution.
#   NOTEBOOK_FILE=pipeline.ipynb   Target notebook filename (default: pipeline.ipynb).
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"
export PYTHONUNBUFFERED="1"

log() {
  echo "[run_notebook] $*"
}

die() {
  echo "[run_notebook] ERROR: $*" >&2
  exit 1
}

RUN_MODE="execute"
if [[ "${1:-}" == "--test" ]]; then
  RUN_MODE="test"
  shift
elif [[ "${1:-}" == "--lab" || "${1:-}" == "--serve" ]]; then
  RUN_MODE="lab"
  shift
elif [[ "${1:-}" == "--execute" ]]; then
  RUN_MODE="execute"
  shift
fi

# ------------------------------------------------------------------------------
# 1. Default Environment Variables
# ------------------------------------------------------------------------------
export PROJECT_ID="${PROJECT_ID:-}"
export REGION="${REGION:-us-central1}"
export CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/tmp/spark-checkpoints}"
export SPARK_MASTER="${SPARK_MASTER:-local[*]}"
export SPARK_JARS="${SPARK_JARS:-}"
export SPARK_PACKAGES="${SPARK_PACKAGES:-}"
export SPARK_PROPERTIES="${SPARK_PROPERTIES:-}"
NOTEBOOK_FILE="${NOTEBOOK_FILE:-pipeline.ipynb}"
EXECUTED_NOTEBOOK_FILE="${EXECUTED_NOTEBOOK_FILE:-executed_${NOTEBOOK_FILE}}"

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
java_major_version() {
  local java_bin="$1"
  [[ -x "${java_bin}" ]] || return 1
  local raw
  raw="$("${java_bin}" -version 2>&1 | head -n 1 || true)"
  if [[ "${raw}" =~ \"([0-9]+)(\.[0-9]+)? ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  return 1
}

is_supported_java_home() {
  local candidate="$1"
  [[ -n "${candidate}" && -x "${candidate}/bin/java" ]] || return 1
  local major
  major="$(java_major_version "${candidate}/bin/java" || echo 0)"
  [[ "${major}" == "17" || "${major}" == "21" ]]
}

find_existing_jdk() {
  if [[ -n "${JAVA_HOME:-}" ]] && is_supported_java_home "${JAVA_HOME}"; then
    echo "${JAVA_HOME}"
    return 0
  fi

  if [[ -x /usr/libexec/java_home ]]; then
    for ver in 21 17; do
      local mac_home
      mac_home="$(/usr/libexec/java_home -v "${ver}" 2>/dev/null || true)"
      if is_supported_java_home "${mac_home}"; then
        echo "${mac_home}"
        return 0
      fi
    done
  fi

  if command -v java >/dev/null 2>&1; then
    local path_java path_home
    path_java="$(readlink -f "$(command -v java)" 2>/dev/null || command -v java)"
    path_home="$(dirname "$(dirname "${path_java}")")"
    if is_supported_java_home "${path_home}"; then
      echo "${path_home}"
      return 0
    fi
  fi

  local candidate
  for candidate in \
    "${JDK_CACHE_DIR}" "${JDK_CACHE_DIR}"/* \
    /usr/lib/jvm/java-21-* /usr/lib/jvm/temurin-21-* /usr/lib/jvm/jdk-21* \
    /usr/lib/jvm/java-17-* /usr/lib/jvm/temurin-17-* /usr/lib/jvm/jdk-17* \
    "${HOME}/.sdkman/candidates/java/"21* "${HOME}/.sdkman/candidates/java/"17* \
    "${HOME}/.jdks/"*21* "${HOME}/.jdks/"*17* \
    /Library/Java/JavaVirtualMachines/*/Contents/Home; do
    if is_supported_java_home "${candidate}"; then
      echo "${candidate}"
      return 0
    fi
  done

  return 1
}

install_cached_jdk21() {
  local os_name arch_name api_os api_arch
  os_name="$(uname -s)"
  arch_name="$(uname -m)"
  case "${os_name}" in
    Linux) api_os="linux" ;;
    Darwin) api_os="mac" ;;
    *) die "Automatic JDK download is unsupported on ${os_name}. Install JDK 17 or 21 and set JAVA_HOME." ;;
  esac
  case "${arch_name}" in
    x86_64|amd64) api_arch="x64" ;;
    arm64|aarch64) api_arch="aarch64" ;;
    *) die "Automatic JDK download is unsupported on architecture ${arch_name}. Install JDK 17 or 21 and set JAVA_HOME." ;;
  esac

  log "No JDK 17 or 21 found. Downloading Eclipse Temurin 21 (${api_os}/${api_arch}) into ${JDK_CACHE_DIR}..."
  mkdir -p "$(dirname "${JDK_CACHE_DIR}")"
  local tmp_dir
  tmp_dir="$(mktemp -d "${JDK_CACHE_DIR}.tmp.XXXXXX")"
  trap 'rm -rf "${tmp_dir:-}"' RETURN

  local meta_url="https://api.adoptium.net/v3/assets/latest/21/hotspot?architecture=${api_arch}&image_type=jdk&os=${api_os}&vendor=eclipse"
  local meta_json="${tmp_dir}/meta.json"
  local tarball="${tmp_dir}/jdk21.tar.gz"
  curl -fsSL "${meta_url}" -o "${meta_json}" || die "Failed to query Adoptium API for JDK 21."

  local dl_url expected_sha
  dl_url="$("${PYTHON}" -c 'import json, sys; data = json.load(open(sys.argv[1])); print(data[0]["binary"]["package"]["link"])' "${meta_json}")"
  expected_sha="$("${PYTHON}" -c 'import json, sys; data = json.load(open(sys.argv[1])); print(data[0]["binary"]["package"]["checksum"])' "${meta_json}")"

  curl -fsSL "${dl_url}" -o "${tarball}" || die "Failed to download JDK 21 archive."
  local actual_sha
  actual_sha="$(sha256_of "${tarball}")"
  if [[ "${actual_sha}" != "${expected_sha}" ]]; then
    die "JDK 21 SHA-256 mismatch (expected ${expected_sha}, got ${actual_sha})."
  fi

  tar -xzf "${tarball}" -C "${tmp_dir}"
  local extracted
  extracted="$(find "${tmp_dir}" -maxdepth 3 -name java -path '*/bin/java' | head -n 1)"
  [[ -n "${extracted}" ]] || die "Downloaded archive did not contain bin/java."
  local jdk_root
  jdk_root="$(dirname "$(dirname "${extracted}")")"

  rm -rf "${JDK_CACHE_DIR}"
  mv "${jdk_root}" "${JDK_CACHE_DIR}"
  rm -rf "${tmp_dir}"
  trap - RETURN
}

bootstrap_java() {
  local found_home
  if found_home="$(find_existing_jdk)"; then
    export JAVA_HOME="${found_home}"
    export PATH="${JAVA_HOME}/bin:${PATH}"
    log "Using JDK at ${JAVA_HOME}."
    return 0
  fi

  if [[ "${AUTO_INSTALL_JDK}" != "true" ]]; then
    die "No JDK 17 or 21 found and AUTO_INSTALL_JDK=${AUTO_INSTALL_JDK}. Install JDK 17/21 and set JAVA_HOME."
  fi

  install_cached_jdk21
  export JAVA_HOME="${JDK_CACHE_DIR}"
  export PATH="${JAVA_HOME}/bin:${PATH}"
  log "Using cached JDK 21 at ${JAVA_HOME}."
}

# ------------------------------------------------------------------------------
# 4. Maven Central Ivy Settings & PYSPARK_SUBMIT_ARGS
# ------------------------------------------------------------------------------
ensure_ivy_settings() {
  if [[ -n "${SPARK_IVY_SETTINGS:-}" ]]; then
    echo "${SPARK_IVY_SETTINGS}"
    return 0
  fi
  mkdir -p "${SPARK_IVY_DIR}"
  local settings_file="${SPARK_IVY_DIR}/ivysettings.xml"
  cat >"${settings_file}" <<'EOF'
<ivysettings>
  <settings defaultResolver="central"/>
  <resolvers>
    <ibiblio name="central" m2compatible="true" root="https://repo1.maven.org/maven2/"/>
  </resolvers>
</ivysettings>
EOF
  echo "${settings_file}"
}

configure_pyspark_submit_args() {
  if [[ -n "${PYSPARK_SUBMIT_ARGS:-}" ]]; then
    return 0
  fi
  local args=()
  args+=(--master "${SPARK_MASTER}")
  if [[ -n "${SPARK_JARS}" ]]; then
    args+=(--jars "${SPARK_JARS}")
  fi
  if [[ -n "${SPARK_PACKAGES}" ]]; then
    local ivy_settings
    ivy_settings="$(ensure_ivy_settings)"
    args+=(--packages "${SPARK_PACKAGES}")
    if [[ "${SPARK_PROPERTIES}" != *"spark.jars.ivySettings="* ]]; then
      args+=(--conf "spark.jars.ivySettings=${ivy_settings}")
    fi
    if [[ "${SPARK_PROPERTIES}" != *"spark.jars.ivy="* ]]; then
      args+=(--conf "spark.jars.ivy=${SPARK_IVY_DIR}")
    fi
  fi
  if [[ -n "${SPARK_PROPERTIES}" ]]; then
    local IFS=','
    for prop in ${SPARK_PROPERTIES}; do
      if [[ -n "${prop}" ]]; then
        args+=(--conf "${prop}")
      fi
    done
  fi
  args+=(pyspark-shell)
  export PYSPARK_SUBMIT_ARGS="${args[*]}"
}

# ------------------------------------------------------------------------------
# 5. Execute Selected Mode
# ------------------------------------------------------------------------------
bootstrap_python
bootstrap_java

if [[ "${RUN_MODE}" == "test" ]]; then
  log "Running offline pytest suite..."
  exec "${PYTHON}" -m pytest tests/ -v "$@"
fi

[[ -f "${NOTEBOOK_FILE}" ]] || die "Notebook '${NOTEBOOK_FILE}' not found in ${SCRIPT_DIR}."
configure_pyspark_submit_args

if [[ "${RUN_MODE}" == "lab" ]]; then
  log "Starting JupyterLab for ${NOTEBOOK_FILE}..."
  exec "${PYTHON}" -m jupyter lab "${NOTEBOOK_FILE}" "$@"
fi

log "Executing ${NOTEBOOK_FILE} headlessly via jupyter nbconvert -> ${EXECUTED_NOTEBOOK_FILE}..."
exec "${PYTHON}" -m jupyter nbconvert \
  --to notebook \
  --execute \
  --output "${EXECUTED_NOTEBOOK_FILE}" \
  "${NOTEBOOK_FILE}" \
  "$@"
