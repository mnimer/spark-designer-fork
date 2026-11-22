---
name: runner-notebook
description: Local JupyterLab, headless nbconvert notebook execution, and offline pytest runner rules and run_notebook.sh template.
metadata:
  kind: runner
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [notebook]
  paradigms: [notebook]
  transforms: []
  preloadWhen: []
  sources:
    - prompt
    - https://nbconvert.readthedocs.io/en/latest/usage.html
    - https://jupyterlab.readthedocs.io/en/stable/
  validated: true
---

### Jupyter Notebook Runner (`run_notebook.sh`)

Every generated Jupyter Notebook repository (`paradigm: notebook`) MUST include an executable `run_notebook.sh` script at the repository root based on the template below.

#### 1. Responsibilities of `run_notebook.sh`
Copy the template below verbatim. Change only the `SPARK_PACKAGES`, `SPARK_JARS`, and `SPARK_PROPERTIES` defaults to the Execution Plan values for the pipeline.

1. **Automatic Bootstrap (runs before every mode)**:
   - **Python**: Creates `.venv` with `"${PYTHON_BIN:-python3}" -m venv` if missing and installs `requirements.txt` when its SHA-256 hash changes.
   - **Java**: Selects a JDK 17 or 21 (or downloads Eclipse Temurin 21 into `${XDG_CACHE_HOME:-~/.cache}/spark-pipeline-builder/jdk-21` unless `AUTO_INSTALL_JDK=false`).
   - **Maven (`--packages`)**: Writes a Maven Central-only Ivy settings file to `.spark-ivy/ivysettings.xml` and configures `PYSPARK_SUBMIT_ARGS` so Spark inside the Jupyter kernel resolves connector packages cleanly.
2. **Headless Execution Mode (default or `--execute`)**:
   - Runs `jupyter nbconvert --to notebook --execute --output executed_pipeline.ipynb pipeline.ipynb` so the notebook executes end-to-end from the CLI or VS Code Run button without overwriting the clean source notebook.
3. **Interactive JupyterLab Mode (`--lab`)**:
   - Launches `jupyter lab pipeline.ipynb` for interactive cell-by-cell development.
4. **Offline Unit Test Mode (`--test`)**:
   - Runs `pytest tests/ -v` with the venv Python against `tests/test_transforms.py`.

#### 2. Reviewer Checklist for `run_notebook.sh`
- Starts with `#!/usr/bin/env bash` and `set -euo pipefail`.
- Bootstraps `.venv` from `requirements.txt` with a hash stamp and selects/installs JDK 17 or 21.
- Handles `--test`, `--lab`, and default headless `--execute` modes.
- Configures `PYSPARK_SUBMIT_ARGS` with `--packages`, `--jars`, `--conf`, and Maven Central Ivy settings when connector artifacts are required.
