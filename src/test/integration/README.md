# Apache Spark Integration Test Suite & Multi-Agent Evaluation Harness

> **Author**: Mike Nimer <mikenimer@google.com>  
> **Location**: `src/test/integration/`

This directory holds live end-to-end integration tests for the extension AI code generator. Each suite loads a `.spark` pipeline graph and invokes `gemini-4-hydrogen` on the Vertex AI `global` endpoint. The harness generates an Apache Spark 4.x repository, audits every file with specialist agents, and runs generated `pytest` tests offline.

---

## 1. Directory Architecture & End-to-End Suites

```text
src/test/integration/
├── e2e/
│   ├── zero-transform-csv-to-parquet/      # Direct CsvSource to ParquetSink replication
│   ├── batch-fanout-auto-cache/            # BigQuerySource fan-out with .cache() and .unpersist()
│   ├── streaming-multi-sink-foreachbatch/  # KafkaSource JSON parse into unified foreachBatch sinks
│   ├── stream-static-join-enrichment/      # PubSubSource and BigQuerySource broadcast join
│   ├── multi-input-spark-sql/              # Three-source Catalyst SparkSqlQuery pipeline
│   └── spark-ml-xgboost-inference/         # IcebergSource SparkXGBClassifier and ConditionalSplit
├── evaluators/
│   ├── deterministic-gate.ts               # Non-LLM syntax, deliverable, and secret pre-flight gate
│   ├── spark-architecture-evaluator.ts     # SparkArchitectureAgent (static + semantic Spark audit)
│   ├── pipeline-spec-evaluator.ts          # PipelineSpecificationAgent (RTM and plan compliance)
│   ├── security-evaluator.ts               # SecurityAndHardeningAgent (CWE and container audit)
│   ├── packaging-evaluator.ts              # PackagingAndDeployabilityAgent (deps and runner flags)
│   ├── arbiter-evaluator.ts                # Lead Arbiter (0-100 rubric, vetoes, skill directives)
│   ├── multi-agent-evaluator.ts            # Parallel evaluator orchestrator and console formatter
│   ├── format-utils.ts                     # 1-indexed line-number formatter for code citations
│   └── types.ts                            # Shared scorecard, finding, and retry contracts
├── codegen-runner.ts                       # End-to-end coordinator: generate, evaluate, pytest, report
├── pytest-runner.ts                        # Offline local PySpark 4.x pytest execution harness
├── pytest-runner.smoke.test.ts             # Hermetic local PySpark smoke test for pytest-runner.ts
├── test-reporter.ts                        # Incremental Markdown scorecard and directive reporter
├── global-setup.ts                         # Clears cached reports and resolves GOOGLE_CLOUD_PROJECT
├── env.d.ts                                # Raw YAML and .spark module type declarations
└── README.md                               # This guide
```

### Live End-to-End Pipeline Suites (`src/test/integration/e2e/`)

Each subfolder in `src/test/integration/e2e/` pairs one `.spark` pipeline specification with a Vitest `.test.ts` runner.

| Suite Folder | Pipeline Pattern Verified |
| :--- | :--- |
| `zero-transform-csv-to-parquet` | Replicates `CsvSource` records directly into `ParquetSink` with zero intermediate steps. |
| `batch-fanout-auto-cache` | Fans out `BigQuerySource` and `WithColumns` to `IcebergSink` and `SpannerSink` with `.cache()` and `.unpersist()`. |
| `streaming-multi-sink-foreachbatch` | Parses `KafkaSource` JSON payloads and writes Delta Lake plus Cloud SQL inside one `foreachBatch` writer. |
| `stream-static-join-enrichment` | Enriches a streaming `PubSubSource` with a `BigQuerySource` lookup via broadcast left join into `BigQuerySink`. |
| `multi-input-spark-sql` | Combines three upstream sources inside one Catalyst `SparkSqlQuery` transformation step. |
| `spark-ml-xgboost-inference` | Scores `IcebergSource` features with `SparkXGBClassifier` and `vector_to_array`. Routes `ConditionalSplit` branches to `BigQuerySink`. |

### Topology & Graph Validation (`integration-suites-topology.test.ts`)

The offline unit suite `src/test/unit/integration-suites-topology.test.ts` validates every `.spark` pipeline file before live runs start. It scans `src/test/integration/e2e/`, optional connector subfolders (`sources/`, `sinks/`, `steps/`), and `resources/samples/`. For each pipeline, `validateDagTopology` runs Kahn's algorithm and enforces strict graph invariants:

- Every graph contains zero cycles, zero orphan nodes, zero validation errors, and zero validation warnings.
- Every pipeline starts at a `SOURCE` node with zero inbound edges and ends at a `SINK` node with zero outbound edges.
- Every node declares a non-empty `spark_transform` identifier.
- Only `.spark`, `.ts`, and `.md` files may exist inside the integration and sample suite folders.

---

## 2. Dual-Paradigm Generated Repository Contracts

The deterministic gate (`deterministic-gate.ts`) auto-detects the pipeline paradigm and verifies mandatory deliverables.

### Imperative PySpark (`paradigm: imperative`, Decision D3)

Imperative pipelines target Apache Spark 4.0+ and produce eight required deliverables (`REQUIRED_SPARK_DELIVERABLES`).

| File | Contract |
| :--- | :--- |
| `src/pipeline.py` | Declares `read_sources(spark, config)`, pure `transform(dfs, config)`, and `write_sinks(dfs, config)`. |
| `tests/test_pipeline.py` | Runs offline `pytest` assertions against `transform(dfs, config)` with in-memory DataFrames. |
| `requirements.txt` | Pins `pyspark==4.0.*` alongside `pytest` and required connector packages. |
| `Dockerfile` | Builds a hardened container image that runs as non-root UID `1099:1099`. |
| `run_local.sh` | Bootstraps `.venv`, JDK 17 or 21, and Ivy settings. Launches `spark-submit` locally or via Spark Connect. |
| `run_dataproc_cluster.sh` | Submits `src/pipeline.py` via `gcloud dataproc jobs submit pyspark` with `--cluster`, `--region`, `--jars`, and `--properties`. |
| `run_serverless_spark.sh` | Submits `src/pipeline.py` via `gcloud dataproc batches submit pyspark` with Spark 4.x `--version`, `--region`, `--jars`, and `--properties`. |
| `README.md` | Documents generated repository configuration and launch commands. |

### Spark Declarative Pipelines (`paradigm: sdp`, Spark 4.2+)

SDP pipelines target Apache Spark 4.2+ (`pyspark.pipelines`) and produce `transformations/*.py` plus eight root deliverables (`REQUIRED_SDP_DELIVERABLES`).

| File | Contract |
| :--- | :--- |
| `spark-pipeline.yml` | Declares allowlisted SDP keys (`name`, `storage`, `catalog`, `database`, `schema`, `configuration`, `libraries`) with string `configuration` values. |
| `transformations/*.py` | Registers `@dp.*` datasets (`@dp.table`, `@dp.materialized_view`, `@dp.temporary_view`, `@dp.append_flow`) and `dp.create_*` flows or sinks. |
| `src/transforms.py` | Isolates pure DataFrame helper functions without `@dp.*` decorators or external storage I/O. |
| `tests/test_transforms.py` | Verifies pure DataFrame helpers in `src/transforms.py` offline with `pytest`. |
| `requirements.txt` | Pins `pyspark` to Apache Spark 4.2+ (such as `pyspark==4.2.*`) alongside `pytest`. |
| `run_sdp_local.sh` | Runs `spark-pipelines dry-run` and `spark-pipelines run` with `--spec spark-pipeline.yml` on local Spark 4.2 Connect. |
| `run_sdp_dataproc.sh` | Executes `CREATE NAMESPACE IF NOT EXISTS` setup and submits standalone Dataproc `4.0` SDP jobs. |
| `sdp_stage.pipeline.yaml` | Configures the Managed Data Pipelines `sdp` stage and places static Spark settings under `execution_config`. |
| `README.md` | Documents SDP dataset topology, catalog prerequisites, and launch scripts. |

---

## 3. Running the Multi-Agent Evaluation Suite

### Cloud Prerequisites

Live integration suites generate and audit repositories through Vertex AI. Complete these steps before running live suites:

1. Authenticate Application Default Credentials with `gcloud auth application-default login`.
2. Export `GOOGLE_CLOUD_PROJECT` or run `gcloud config set project <project-id>`.
3. Enable the Vertex AI API for `gemini-4-hydrogen` at `location: 'global'`.

> [!IMPORTANT]
> At startup, `global-setup.ts` clears cached reports and resolves `GOOGLE_CLOUD_PROJECT` from environment variables, local gcloud files, or `gcloud config get-value project`. Keep Vertex AI requests on `location: 'global'` because regional endpoints do not host `gemini-4-hydrogen`.

### Typechecking & Execution Commands

The root `tsconfig.json` excludes `src/test/integration/` so `npm run compile` builds only extension host code. Always run `npm run typecheck:integration` (`tsc -p tsconfig.integration.json`) to typecheck the integration harness.

```bash
# Typecheck all integration runners, evaluators, and E2E test files
npm run typecheck:integration

# Run every integration test file (requires gcloud ADC and Vertex AI access)
npm run test:integration

# Run all 6 end-to-end pipeline suites under src/test/integration/e2e/
npm run test:integration:e2e

# Run a single E2E pipeline suite
npx vitest run --config vitest.integration.config.mts \
  src/test/integration/e2e/<suite>/<suite>.test.ts

# Example: run only zero-transform-csv-to-parquet
npx vitest run --config vitest.integration.config.mts \
  src/test/integration/e2e/zero-transform-csv-to-parquet/zero-transform-csv-to-parquet.test.ts

# Run Vitest in interactive watch mode
npm run test:integration:watch
```

`vitest.integration.config.mts` sets a 3-minute (`180000` ms) default timeout, limits concurrency to `2`, and registers `yaml-raw-loader` for `.spark` files. Streaming E2E suites extend their test timeout to 5 minutes (`300000` ms).

### Evaluation Pipeline Lifecycle (`codegen-runner.ts` & `multi-agent-evaluator.ts`)

```text
[.spark Pipeline YAML]
         │
         ▼
1. Prompt Assembly & Generation (codegen-runner.ts)
   buildPromptFromPipelineYaml + resources/codegen-skills/ -> gemini-4-hydrogen (location: 'global')
         │
         ▼
2. Parallel Gate + 4 Specialist Sub-Agents (multi-agent-evaluator.ts, Promise.all)
   ├── Deterministic Pre-Flight Gate (deterministic-gate.ts)
   ├── SparkArchitectureAgent (spark-architecture-evaluator.ts)
   ├── PipelineSpecificationAgent (pipeline-spec-evaluator.ts)
   ├── SecurityAndHardeningAgent (security-evaluator.ts)
   └── PackagingAndDeployabilityAgent (packaging-evaluator.ts)
         │
         ▼
3. Lead Arbiter & Synthesizer (arbiter-evaluator.ts)
   False-positive reconciliation, 0-100 weighted scorecard, hard vetoes, 1-to-1 skill directives
         │
         ▼
4. Offline pytest Harness (pytest-runner.ts) & Markdown Reporter (test-reporter.ts)
```

### `SparkArchitectureAgent` Deep Dive (`spark-architecture-evaluator.ts`)

`SparkArchitectureAgent` (`SPARK_ARCHITECTURE_AGENT_NAME = 'SparkArchitectureAgent'`) uses a deterministic-first, two-layer architecture:

- **Layer 1 (Deterministic Static Checks)**: `runSparkArchitectureStaticChecks` (and `runSdpArchitectureStaticChecks` in SDP mode) extracts `SparkArchitectureSignals` and emits high-precision `SPARK-ARCH-*` findings offline.
- **Layer 2 (Semantic Rubric Audit)**: `evaluateSparkArchitecture` passes 1-indexed line-numbered files, static signals, and recorded static findings to `gemini-4-hydrogen`. It then deduplicates combined findings via `mergeArchitectureFindings`.
- **Resilient Failure Handling**: If the `gemini-4-hydrogen` call fails, `evaluateSparkArchitecture` preserves `staticFindings`, sets `passed: false`, and never invents synthetic findings.

#### Imperative Mode Checks (`paradigm: imperative`)

- **Decision D3 Separation & Purity**: Verifies `src/pipeline.py` defines `read_sources(spark, config)`, pure `transform(dfs, config)`, and `write_sinks(dfs, config)`. Flags any `spark.read`, `spark.readStream`, `spark.table`, `.write`, `.writeStream`, `.writeTo`, `saveAsTable`, or `insertInto` call inside `transform`.
- **Catalyst Optimization Bans**: Scans non-test Python modules (`tests/`, `test_*.py`, and `*_test.py` are exempt) for `.collect()`, `.toPandas()`, `.rdd`, and `.sparkContext`. Flags row-at-a-time Python UDFs (`@udf`, `F.udf`) and terminal actions inside loops. In parallel, `deterministic-gate.ts` enforces the repo-wide ban on private `_jsc` and `_jvm` gateway access.
- **Cache & Unpersist Pairing**: Flags any repository that calls `.cache()` or `.persist()` without a matching `.unpersist()` release after downstream writes complete.
- **`foreachBatch` Idempotency**: Requires Delta Lake writes inside `foreachBatch` to set `.option("txnVersion", str(batch_id))` and `.option("txnAppId", ...)`. Rejects constant `txnVersion` literals that would skip subsequent micro-batches.
- **Watermark Placement (Decision D8)**: Requires `.withWatermark(event_time_col, delay)` on streaming sources before `window`, `session_window`, `dropDuplicatesWithinWatermark`, stream-stream joins, or `transformWithState`.
- **Managed Spark Classpath Rules**: Rejects Scala 2.11/2.12 (`_2.11`, `_2.12`) and Spark 3.x coordinates. Bans `spark.jars` or `spark.jars.packages` in Python code. Also bans passing pre-installed connectors (`spark-bigquery`, `iceberg-spark-runtime`, `gcs-connector`, `hadoop-aws`, `hadoop-azure`) in `run_dataproc_cluster.sh`, `run_serverless_spark.sh`, or `run_sdp_dataproc.sh`.

#### Spark Declarative Pipelines Checks (`paradigm: sdp`)

- **`spark-pipeline.yml` Hygiene**: Enforces one root spec file and allowlisted top-level keys (`name`, `storage`, `catalog`, `database`, `schema`, `configuration`, `libraries`). Requires string-only `configuration` values and bans static Spark keys (`spark.jars*`, `spark.sql.extensions`, `spark.driver.*`, `spark.executor.*`).
- **`transformations/*.py` Dataset Purity**: Bans `SparkSession.builder`, `spark.conf.set`, `createOrReplaceTempView`, and `spark.udf.register`. Also bans writers (`.write`, `.writeStream`, `.writeTo`, `saveAsTable`), actions (`.collect()`, `.count()`, `.show()`, `.toPandas()`), and `.schema` or `.columns` access.
- **Pure Helper Isolation (`src/transforms.py`)**: Verifies `src/transforms.py` contains pure DataFrame helpers with zero table or storage I/O so `tests/test_transforms.py` executes offline.
- **Query-Type Alignment & Plan Ceilings**: Rejects `readStream` inside `@dp.materialized_view`, rejects batch `spark.read` without `readStream` inside `@dp.table`, and enforces `SdpExecutionPlan` compatibility rules.
- **Databricks API Bans**: Rejects legacy DLT symbols including `dlt`, `@dp.view`, `@dp.expect*`, `dp.apply_changes`, `append_flow(once=...)`, `stored_as_scd_type=2`, and `format("cloudFiles")`.

### Companion Gate, Specialist Agents, Arbiter & Report

- **Deterministic Gate (`deterministic-gate.ts`)**: Compiles Python files with `python3 -m py_compile` and checks `.sh` scripts with `bash -n`, shebang, and executable rules. Parses JSON and YAML files, checks deliverables and `transform(dfs, config)` or SDP layout, and scans banned patterns, `{{...}}` placeholders, and secrets. When Spark 4.2 is installed, SDP runs execute `spark-pipelines dry-run --spec spark-pipeline.yml` (`runOptionalSdpGateDryRun`). If `spark-pipelines` is absent, the gate skips that dry-run cleanly.
- **`PipelineSpecificationAgent` (`pipeline-spec-evaluator.ts`)**: Builds a Requirement Traceability Matrix (`REQ-NODE-*`, `REQ-INSTR-*`, `REQ-VAR-*`, `REQ-CONFIG-*`, `REQ-EDGE-*`, `REQ-PLAN-*`, `REQ-SDP-*`) and verifies `spark_transform` names against `SPARK_NODE_CAPABILITIES`. Checks `ExecutionPlan` compliance (`dfs` keys, auto-cache, `foreachBatch`, `checkpointLocation`, DLQ `<node_id>__error` and `_corrupt_record` splits) and `SdpExecutionPlan` compliance (`@dp.*` datasets, `dp.create_auto_cdc_flow`, `dp.create_sink`, `_quarantine` tables).
- **`SecurityAndHardeningAgent` (`security-evaluator.ts`)**: Audits Secret Manager lookup (CWE-798), non-root `Dockerfile` execution (CWE-250), and shell quoting with `set -euo pipefail` (CWE-78). Also checks parameterized SQL/JDBC queries (CWE-89) and private VPC flags.
- **`PackagingAndDeployabilityAgent` (`packaging-evaluator.ts`)**: Audits `requirements.txt` (`pyspark==4.0.*` for Imperative or Spark 4.2+ for SDP, `pytest`, and `ExecutionPlan` `pipPackages`). Verifies runner flags across Imperative and SDP launch scripts, `sdp_stage.pipeline.yaml` spec references, and `versionMatrix.ts` connector artifacts.
- **Lead Arbiter (`arbiter-evaluator.ts`)**: Verifies sub-agent findings against line-numbered code and discards false positives (`rejectedFindingIds`). Computes a 0–100 weighted rubric scorecard (`Architecture` 35%, `Specification` 25%, `Security` 20%, `Packaging` 15%, `Maintainability` 5%). Triggers a hard veto on syntax errors, `CRITICAL` defects, secret leaks, or an overall score under `80`. Emits 1-to-1 `SkillRemediationDirective` items targeting `resources/codegen-skills/<skill>/SKILL.md` via `CodegenSkillCatalog` (`sparkTransform` ownership) rather than patching generated code.
- **Markdown Test Report (`test-reporter.ts`)**: Caches run entries in `.test-report.json` and writes `src/test/integration/test-report.md` with executive summaries, severity-tagged findings, and consolidated skill directives.

> [!WARNING]
> Live runs write `src/test/integration/test-report.md` and `src/test/integration/.test-report.json` locally. Delete both generated report files before committing so `git status` remains clean.

---

## 4. Running the Offline `pytest` Harness (`pytest-runner.ts`)

The offline `pytest` harness executes generated `tests/test_pipeline.py` suites against a local PySpark 4.x session. It requires zero network access and zero Google Cloud credentials.

### JVM & Python Requirements

PySpark 4.0 cannot start a local session on JDK 25 or JDK 27 (`UnsupportedOperationException: getSubject is not supported`). Follow these steps to run the offline smoke test:

1. Point `JAVA_HOME` at **JDK 17 or JDK 21** (such as `~/.cache/spark-pipeline-builder/jdk-21`).
2. Point `SPARK_PYTEST_PYTHON` at a Python interpreter with `pyspark>=4` and `pytest`.
3. Execute `npm run test:integration:pytest` with both variables set.

```bash
JAVA_HOME=/path/to/jdk-21 SPARK_PYTEST_PYTHON=/path/to/python npm run test:integration:pytest
```

### `runGeneratedRepoTests` Mechanics

- **Host Tooling & JVM Probes**: `probeTooling` runs `TOOLING_PROBE_SCRIPT` to check `pyspark>=4`, `pytest`, and Java. Next, `probeSparkSession` runs `SPARK_SESSION_PROBE_SCRIPT` to start and stop a `local[2]` `SparkSession`.
- **`skipped` vs Blocking Outcomes**: If Python, `pyspark>=4`, `pytest`, or JDK 17/21 is unavailable, `runGeneratedRepoTests` returns `status: 'skipped'` with a diagnostic reason. `isBlockingPytestStatus` treats `skipped` as non-blocking, whereas `failed`, `error`, and `timeout` statuses fail the integration test.
- **Root `conftest.py` Fixture Injection**: If `tests/conftest.py` and root `conftest.py` lack `spark`, `ensureConftest` writes a `SparkSession.builder.master("local[2]")` fixture to root `conftest.py`. Note that `deterministic-gate.ts` exempts `conftest.py`, `tests/`, and `run_local.sh` from banned local-master scans.
- **Process Isolation & Sanitization**: `execFileSafe` spawns Python and `pytest` with argument arrays and never invokes a shell. `buildChildEnv` filters caller variables through `envSanitizer` and pins `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON`, and `SPARK_LOCAL_IP=127.0.0.1`.
- **JUnit XML Report Parsing**: `parseJUnitXml` reads structured pass, fail, skip, and error counts plus failure tracebacks from `--junitxml` (`junit_family=xunit2`).
- **Shared Cache & Disk Hygiene (`sharedTestCache.ts`)**: Reuses `~/.cache/spark-pipeline-builder/` for virtualenvs, pip, Ivy, and JDK 21. Removes per-run scratch caches after execution.

### Offline Vitest Unit Suites (`src/test/unit/`)

All deterministic gates, static evaluator checks, SDP rules, report formatters, and `pytest-runner.ts` helpers have fast, offline unit suites under `src/test/unit/`:

- `src/test/unit/deterministic-gate.test.ts`
- `src/test/unit/spark-architecture-evaluator.test.ts`
- `src/test/unit/pipeline-spec-evaluator.test.ts`
- `src/test/unit/packaging-evaluator.test.ts`
- `src/test/unit/arbiter-evaluator.test.ts`
- `src/test/unit/multi-agent-evaluator.test.ts`
- `src/test/unit/pytest-runner.test.ts`
- `src/test/unit/codegen-runner-pytest-hook.test.ts`
- `src/test/unit/sdp/sdp-evaluators-and-gate.test.ts`
- `src/test/unit/test-reporter.test.ts`
- `src/test/unit/integration-suites-topology.test.ts`

Run all host unit suites offline with `npm run test:unit`.
