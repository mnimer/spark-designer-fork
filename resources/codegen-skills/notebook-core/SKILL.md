---
name: notebook-core
description: Interactive Jupyter Notebook (.ipynb) nbformat 4.5 architecture for Vertex AI Workbench, Colab Enterprise, and local PySpark 4.x kernels.
metadata:
  kind: language
  usedBy: [generator, reviewer]
  languages: [python]
  runners: [all]
  paradigms: [notebook]
  transforms: []
  preloadWhen: []
  sources:
    - prompt
    - https://nbformat.readthedocs.io/en/latest/format_description.html
    - https://cloud.google.com/colab/docs/spark-serverless-overview
    - https://cloud.google.com/vertex-ai/docs/workbench/introduction
  validated: true
---

# Jupyter Notebook (`.ipynb`) Output Mode Standards

Follow these rules whenever `paradigm` is `notebook`. Generate an interactive, runnable Jupyter Notebook (`pipeline.ipynb`) designed for Vertex AI Workbench, Colab Enterprise (Dataproc Serverless Interactive Sessions), and local JupyterLab kernels, backed by pure, unit-testable PySpark transformations in `src/transforms.py`.

## 1. Valid nbformat 4.5 JSON Contract for `pipeline.ipynb`

Emit `pipeline.ipynb` as strict, valid JSON conforming to Jupyter Notebook Format v4.5 (`"nbformat": 4, "nbformat_minor": 5`):

```json
{
  "nbformat": 4,
  "nbformat_minor": 5,
  "metadata": {
    "kernelspec": {
      "display_name": "PySpark (Spark {{spark_version}})",
      "language": "python",
      "name": "python3"
    },
    "language_info": {
      "name": "python",
      "version": "3.11"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "id": "overview",
      "metadata": {},
      "source": [
        "# Pipeline Title\n",
        "..."
      ]
    },
    {
      "cell_type": "code",
      "id": "parameters",
      "metadata": {
        "tags": ["parameters"]
      },
      "execution_count": null,
      "outputs": [],
      "source": [
        "PROJECT_ID = \"...\"\n"
      ]
    }
  ]
}
```

Rules for every cell in `pipeline.ipynb`:
- Every cell MUST have a unique string `id`, a valid `cell_type` (`"markdown"` or `"code"`), a `metadata` object, and a `source` list of strings (each line except the last ending with `\n`).
- Every `"code"` cell MUST include `"execution_count": null` and `"outputs": []`.
- Never include raw credentials, tokens, or redaction sentinels inside notebook cells. Use environment variables or `access_secret_version(...)`.
- Because `pipeline.ipynb` is JSON embedded inside the outer JSON response `content` string, escape all inner double quotes and newlines accurately so `JSON.parse(content)` succeeds with zero syntax errors.

## 2. Mandatory Cell Structure (Execution Order)

Organize `pipeline.ipynb` into clear Markdown headers paired with focused Python code cells following `ExecutionPlan.sortedNodeIds`:

1. **Overview (Markdown cell)**:
   - Pipeline title, execution mode (`batch` or `streaming`), node topology summary, and target runtime instructions for Vertex AI Workbench, Colab Enterprise, and local JupyterLab (`./run_notebook.sh --lab`).
2. **Parameters (Code cell with `"metadata": {"tags": ["parameters"]}`)**:
   - Define every deployment-varying configuration variable (project ID, region, input/output URIs, table names, checkpoint root, Secret Manager IDs) with `os.environ.get(...)` defaults from the YAML so Papermill and Vertex AI Workbench scheduled runs can override them cleanly.
   - Build a `CONFIG: dict[str, Any]` dictionary consumed by downstream cells.
3. **Environment, Kernel Bootstrap & Imports (Code cell)**:
   - Add project root to `sys.path` if needed (`if str(Path.cwd()) not in sys.path: sys.path.insert(0, str(Path.cwd()))`) so `from src.transforms import ...` works in both local JupyterLab / VS Code Jupyter and Workbench / Colab Enterprise.
   - **ALWAYS auto-bootstrap missing Python packages BEFORE `from pyspark.sql import ...`** so opening `pipeline.ipynb` directly in VS Code Jupyter or a fresh Workbench kernel never fails with `ModuleNotFoundError: No module named 'pyspark'`. On managed Spark kernels where `pyspark` is already installed, this check is an instant no-op:
     ```python
     import importlib.util
     import subprocess
     import sys
     from pathlib import Path

     if str(Path.cwd()) not in sys.path:
         sys.path.insert(0, str(Path.cwd()))

     if importlib.util.find_spec("pyspark") is None:
         req_file = Path.cwd() / "requirements.txt"
         if req_file.exists():
             subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-r", str(req_file)])
         else:
             subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pyspark=={{spark_version}}.*"])
     ```
   - If the pipeline also needs extra PyPI packages from `ExecutionPlan.runtimeArtifacts.pipPackages` (for example `faker` or `google-cloud-secret-manager`), verify their module specs or install them via the same `subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", ...])` guard before importing them.
4. **SparkSession Initialization (Code cell)**:
   - Reuse an already-active managed Spark session when running inside Colab Enterprise or a Dataproc Serverless Interactive Session kernel.
   - When `SparkSession.getActiveSession() is None` (local kernel), ensure `JAVA_HOME` points to a JDK 17 or 21 before calling `SparkSession.builder...getOrCreate()`, because PySpark 4.0 fails on JDK 25+ with `java.lang.UnsupportedOperationException: getSubject is not supported`. Search both `~/.cache/spark-pipeline-builder/jdk-21` and its subdirectories (for example `jdk-21/jdk-21.0.*`), plus standard system JDK 21/17 paths:
     ```python
     import re

     def _java_major(java_bin: Path) -> int:
         if not java_bin.exists():
             return 0
         out = subprocess.run([str(java_bin), "-version"], capture_output=True, text=True)
         match = re.search(r'"(\d+)', (out.stderr or out.stdout or ""))
         return int(match.group(1)) if match else 0

     def _ensure_compatible_jdk() -> None:
         current = os.environ.get("JAVA_HOME")
         if current and _java_major(Path(current) / "bin" / "java") in (17, 21):
             return
         cache_root = Path.home() / ".cache" / "spark-pipeline-builder" / "jdk-21"
         candidates = [cache_root]
         if cache_root.exists():
             candidates.extend(sorted(cache_root.glob("*")))
         jvm_dir = Path("/usr/lib/jvm")
         if jvm_dir.exists():
             candidates.extend(sorted(jvm_dir.glob("java-21-*")))
             candidates.extend(sorted(jvm_dir.glob("java-17-*")))
         for candidate in candidates:
             if _java_major(candidate / "bin" / "java") in (17, 21):
                 os.environ["JAVA_HOME"] = str(candidate)
                 os.environ["PATH"] = f"{candidate / 'bin'}:{os.environ.get('PATH', '')}"
                 return

     spark = SparkSession.getActiveSession()
     if spark is None:
         _ensure_compatible_jdk()
         builder = SparkSession.builder.appName(CONFIG["app_name"])
         if os.environ.get("SPARK_MASTER"):
             builder = builder.master(os.environ["SPARK_MASTER"])
         # Apply ExecutionPlan.runtimeArtifacts.sparkProperties here.
         spark = builder.getOrCreate()
     ```
   - Never hardcode `.master("local[*]")` unconditionally.
5. **Source Ingestion Cells (Markdown + Code cells)**:
   - Read every `SOURCE` node in `ExecutionPlan.sortedNodeIds` order into `dfs["<node_id>"]`.
   - For batch sources, include a concise schema and sample preview (`dfs["<node_id>"].printSchema()` and `dfs["<node_id>"].show(5, truncate=False)`).
   - For streaming sources, call `.printSchema()` only (never call `.show()` or `.count()` on a streaming DataFrame).
6. **Transformation Cells (Markdown + Code cells)**:
   - Delegate pure DataFrame transformation logic to functions in `src/transforms.py` (for example `dfs = apply_transforms(dfs, CONFIG)` or one cell per logical step calling its helper in `src/transforms.py`).
   - Honor every configured property, `cacheNodes` (`.cache()`), `conditionalSplits`, and `dlqRoutes` (`dfs["<node_id>__error"]`).
7. **Sink Write & Cleanup Cells (Markdown + Code cells)**:
   - Write every `SINK` node from its upstream DataFrame in `dfs`.
   - Honor `ExecutionPlan.streamingSinkGroups` (`writeStream` vs `foreachBatch` with `txnVersion=batch_id`).
   - Call `.unpersist()` on cached batch DataFrames at `ExecutionPlan.unpersistPoints` after sink actions complete.
   - Do NOT call `spark.stop()` unconditionally in the final cell: stopping the session terminates managed Colab Enterprise / Workbench kernels and triggers a noisy `pyspark/daemon.py BrokenPipeError` on local PySpark 4.0 + Python 3.13. Leave `# spark.stop()` commented out as an optional manual teardown line.
   - For streaming pipelines, NEVER call `query.stop()` unconditionally in the next cell after `.start()`: clicking **Run All** executes that cell immediately and aborts the first micro-batch mid-flight with `TaskKilledException` (`SPARK_JOB_CANCELLED`). Instead, wait briefly or inspect status (`query.awaitTermination(timeout=10)` or `[q.status for q in spark.streams.active]`), and put `# for q in spark.streams.active: q.stop()` commented out in a separate manual-stop cell.

## 3. Pure Testable Transformations in `src/transforms.py` and `tests/test_transforms.py`

- Keep all pure PySpark DataFrame transformation logic inside `src/transforms.py` (`transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]` plus per-step helper functions).
- `src/transforms.py` MUST perform zero external network I/O and MUST NOT instantiate `SparkSession.builder`.
- `tests/test_transforms.py` MUST import `src.transforms` and verify all transformation steps offline using an in-memory local `SparkSession` fixture and `spark.createDataFrame(...)`.

## 4. Reviewer Checklist for Notebook Mode

- `pipeline.ipynb` parses as valid JSON with `nbformat == 4` and a non-empty `cells` array.
- Every code cell has valid Python syntax (ignoring `%` / `!` notebook magics) and includes `"execution_count": null` and `"outputs": []`.
- A `"parameters"`-tagged cell exposes all runtime variables for Papermill / Vertex AI Workbench scheduled execution.
- `SparkSession.getActiveSession()` is checked before `SparkSession.builder` so Colab Enterprise and Workbench kernels reuse their managed Spark session.
- `importlib.util.find_spec("pyspark")` guards and auto-installs `requirements.txt` / `pyspark` before `from pyspark.sql import ...` so fresh VS Code Jupyter kernels never fail with `ModuleNotFoundError`.
- `src/transforms.py` and `tests/test_transforms.py` exist and test all transformations offline without external network I/O.
