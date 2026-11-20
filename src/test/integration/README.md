# Apache Spark Integration Test Suite & Multi-Agent Evaluation Harness

> **Author:** Mike Nimer <mikenimer@google.com>  
> **Location:** `src/test/integration/`

This directory holds **live end-to-end integration tests** for the extension's AI code generation. The tests call Gemini (`gemini-4-hydrogen`) on Vertex AI with `location: 'global'`. Each test generates a complete PySpark 4.x repository from a `.spark` pipeline file. The multi-agent evaluation harness then audits the repository, and the offline pytest runner executes its generated tests.

---

## Directory Architecture

```
src/test/integration/
├── sources/                  # Source connector suites (.spark + .test.ts), added in Phase 9
├── sinks/                    # Sink connector suites, added in Phase 9
├── steps/                    # Step (transformation) suites, added in Phase 9
├── evaluators/
│   ├── deterministic-gate.ts           # Non-LLM pre-flight gate
│   ├── spark-architecture-evaluator.ts # SparkArchitectureAgent
│   ├── pipeline-spec-evaluator.ts      # PipelineSpecificationAgent
│   ├── security-evaluator.ts           # SecurityAndHardeningAgent
│   ├── packaging-evaluator.ts          # PackagingAndDeployabilityAgent
│   ├── arbiter-evaluator.ts            # Lead Arbiter & Synthesizer
│   ├── multi-agent-evaluator.ts        # Orchestrator
│   └── types.ts, format-utils.ts       # Shared types and report formatting
├── codegen-runner.ts         # E2E coordinator: generate, evaluate, run pytest, report
├── pytest-runner.ts          # Offline pytest runner for generated repositories
├── pytest-runner.smoke.test.ts  # Real local PySpark smoke test for the runner
├── test-reporter.ts          # Markdown scorecard and remediation directive report
├── global-setup.ts           # Clears the report cache and resolves the gcloud project
└── README.md                 # This document
```

> [!NOTE]
> Phase 9 (`I1`–`I4`) adds the `.spark` suites under `sources/`, `sinks/`, and `steps/`. The unit test `integration-suites-topology` validates every suite file: zero cycles, zero orphans, a SOURCE start, a SINK end, and a `spark_transform` on every node. It rejects unexpected non-Spark suite files.

---

## Generated Repository Contract (Decision D3)

Every generated repository must have this layout. The deterministic gate checks it.

| File | Purpose |
| :--- | :--- |
| `src/pipeline.py` | `read_sources(spark, config)`, pure `transform(dfs, config)`, `write_sinks(dfs, config)` |
| `tests/test_pipeline.py` | pytest suite for `transform` with a local `spark` fixture |
| `requirements.txt` | `pyspark` pinned on the 4.x line, plus `pytest` |
| `Dockerfile` | Non-root container image |
| `run_local.sh` | `.venv`, JDK, and Ivy bootstrap, then `spark-submit` on Local PySpark / Spark Connect |
| `run_dataproc_cluster.sh` | `gcloud dataproc jobs submit pyspark` |
| `run_serverless_spark.sh` | `gcloud dataproc batches submit pyspark` with a Spark 4.x runtime version |
| `README.md` | Usage notes |

---

## Running Integration Tests

All integration tests run with Vitest and `vitest.integration.config.mts`. That config sets a 3-minute test timeout for generation, evaluation, and the pytest run.

```bash
# Run every integration suite (needs gcloud credentials and Vertex AI access)
npm run test:integration

# Run one domain
npm run test:integration:sources
npm run test:integration:sinks
npm run test:integration:steps

# Run one file
npx vitest run --config vitest.integration.config.mts src/test/integration/<domain>/<name>.test.ts

# Watch mode
npm run test:integration:watch
```

### Offline pytest runner smoke test

The smoke test needs no network and no cloud credentials. It builds a small repository and runs its tests on a real local SparkSession.

```bash
SPARK_PYTEST_PYTHON=<python with pyspark>=4 and pytest> JAVA_HOME=<JDK 17 or 21> npm run test:integration:pytest
```

If PySpark 4.x, pytest, or a compatible JVM is missing, the test passes with a `skipped` result and prints the reason.

---

## How the Evaluation Works

```
[.spark Pipeline YAML]
         │
         ▼
1. Code Generation (Gemini 4 Hydrogen, global endpoint)
   Same prompt builder as the extension (buildPromptFromPipelineYaml + codegen skills)
         │
         ▼
2. Multi-Agent Evaluation Harness (evaluators/)
   ├── Step A: Deterministic Pre-Flight Gate
   │   ├── python3 -m py_compile on every module, bash -n on every script
   │   ├── D3 deliverables and transform(dfs, config) / read_sources / write_sinks
   │   ├── Banned patterns: hardcoded local master in deployed code, _jsc / _jvm access
   │   └── Plaintext secret and unreplaced placeholder scan
   │
   ├── Step B: Parallel Specialist Sub-Agents (Promise.all)
   │   ├── SparkArchitectureAgent: D3 separation, Catalyst rules (no collect/toPandas/RDD),
   │   │   cache/unpersist, foreachBatch idempotency, watermarks, Managed Spark classpath
   │   ├── PipelineSpecificationAgent: property completeness, spark_transform audit,
   │   │   ExecutionPlan compliance (cache, foreachBatch, checkpoints, DLQ splits)
   │   ├── SecurityAndHardeningAgent: Secret Manager usage, non-root Dockerfile, shell quoting
   │   └── PackagingAndDeployabilityAgent: pyspark 4.x pin, pip packages, runner script flags,
   │       connector JARs against the target runtime profile
   │
   └── Step C: Lead Arbiter & Synthesizer
       ├── Multi-dimensional rubric scorecard (0-100) and hard vetoes
       └── Skill Remediation Directives (one codegen skill per directive)
         │
         ▼
3. Offline pytest run (pytest-runner.ts)
   Writes the repository to a temporary folder, adds a local[2] conftest fixture if needed,
   and runs tests/test_pipeline.py. Status failed, error, or timeout fails the test.
   Status skipped (tooling missing) never fails the test.
```

Deterministic checks run before every LLM call. If an agent's model call fails, the agent keeps its deterministic findings and does not invent findings.

### Skill Remediation Directives

The Lead Arbiter never patches generated code. Each directive targets exactly one codegen skill folder under `resources/codegen-skills/`. The arbiter resolves the skill from the node's `sparkTransform` through the codegen skill catalog. An `io-*` or `transform-*` skill wins over a cross-cutting skill. Directives with an unknown skill are dropped.

### Automated Test Report (`test-report.md`)

After a run, the harness updates `test-report.md` in this directory with:

- **Executive Summary Table**: Pass/fail status, overall score, hard veto flags, and defect counts.
- **Failed Jobs & Defect Analysis**: Findings by severity (🔴 CRITICAL, 🟠 MAJOR, 🟡 MINOR). A blocking pytest result is added to the explanation.
- **Remediation Directives**: Rule changes grouped by the skill they target. Fix the skill, never the generated code.
- **Detailed Scorecards & Generated File Lists**: Category rubric scores (Architecture, Specification, Security, Packaging, Maintainability).

---

## Offline Unit Coverage

The deterministic logic of the harness has offline Vitest suites under `src/test/unit/`. They mock the model client and child processes:

- `deterministic-gate.test.ts`, `spark-architecture-evaluator.test.ts`, `pipeline-spec-evaluator.test.ts`, `packaging-evaluator.test.ts`, `arbiter-evaluator.test.ts`, `multi-agent-evaluator.test.ts`
- `pytest-runner.test.ts` and `codegen-runner-pytest-hook.test.ts`

Run them with `npm test`.
