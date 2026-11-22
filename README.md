<!-- Author: Mike Nimer <mikenimer@google.com> -->

# Apache Spark / Managed Spark Pipeline Builder

[![VS Code](https://img.shields.io/badge/VS%20Code-v1.74%2B-007ACC?logo=visualstudiocode)](https://code.visualstudio.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.x%20%7C%20SDP%204.2%2B-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Google Cloud Managed Spark](https://img.shields.io/badge/Google%20Cloud-Managed%20Spark-4285F4?logo=googlecloud)](https://cloud.google.com/dataproc-serverless)
[![Powered by Gemini](https://img.shields.io/badge/Vertex%20AI-Gemini%204%20Hydrogen-8E75B2?logo=googlegemini)](https://cloud.google.com/vertex-ai)

**Apache Spark / Managed Spark Pipeline Builder** visually designs, validates, generates, and runs **Apache Spark 4.x** pipelines inside VS Code. Target **Local PySpark / Spark Connect**, **Google Cloud Dataproc Clusters**, and **Serverless for Apache Spark** from one `.spark` YAML canvas.

Build DAGs on an interactive React Flow canvas using **111 catalog nodes** (**40 Sources**, **33 Steps**, and **38 Sinks**). Generate **Imperative PySpark 4.x** repositories or **Spark Declarative Pipelines (SDP, Apache Spark 4.2+)** projects with offline `pytest` suites.

---

## Dual Pipeline Paradigms

Select a paradigm during `.spark` file creation, or switch paradigms directly from the canvas toolbar.

| Capability | Imperative PySpark (`imperative`, Default) | Spark Declarative Pipelines (`sdp`, Spark 4.2+) |
| :--- | :--- | :--- |
| **Target Runtimes** | Apache Spark 4.0+ across Serverless 4.0 and 3.0, Dataproc Cluster 4.0 and 3.0, and Local 4.0. | Apache Spark 4.2+ over Spark Connect on Serverless 4.0, Dataproc Cluster 4.0, and Local 4.2 Connect. |
| **Generated Deliverables** | `src/pipeline.py`, `tests/test_pipeline.py`, `requirements.txt`, `Dockerfile`, `run_local.sh`, `run_dataproc_cluster.sh`, `run_serverless_spark.sh`, and `README.md`. | `spark-pipeline.yml`, `transformations/*.py`, `src/transforms.py`, `tests/test_transforms.py`, `requirements.txt`, `run_sdp_local.sh`, `run_sdp_dataproc.sh`, `sdp_stage.pipeline.yaml`, and `README.md`. |
| **Execution & Triggers** | Batch runs plus `availableNow`, `processingTime` micro-batch, and `continuous` Structured Streaming triggers. | Triggered `AvailableNow` runs over `@dp.table`, `@dp.materialized_view`, and `@dp.temporary_view` datasets. |
| **CDC & SCD Support** | SCD Type 1 and SCD Type 2 merges via the two-input `MergeCdcScd` step. | Managed SCD Type 1 Auto CDC flows via `dp.create_auto_cdc_flow` with `left` input routing. |
| **Sinks & AI** | All native and `foreachBatch` sinks plus Spark MLlib, XGBoost, Vertex AI, and RAG embedding steps. | Lakehouse catalog tables (BigLake Iceberg, Delta, Hudi) and streaming `dp.create_sink` connectors (Kafka, BigQuery, Pulsar, files). |

---

## Key Features

- **Visual `.spark` DAG Editor & Canvas Semantics**
  - Connect **40 Sources**, **33 Steps**, and **38 Sinks** (**111 catalog nodes**) with live cycle, orphan, watermark, and checkpoint validation.
  - Wire direct `SOURCE -> SINK` connections for zero-transform format conversion and table replication pipelines.
  - Inspect Imperative micro-badges (`BATCH`, `STREAM`, `CACHE`, `foreachBatch`, `BROADCAST`, and `N-IN`) directly on canvas nodes.
  - When authoring SDP pipelines, inspect dataset badges (`VIEW`, `MV`, `TABLE`, `CDC`, and `SINK`) directly on canvas nodes.
  - Route multi-branch flows through `Join` (`left` and `right` handles), `ConditionalSplit` (dynamic rule handles plus `else`), and red `error` DLQ handles.
  - If an Imperative graph contains unsupported SDP nodes, the compatibility gate explains each blocker before switching paradigms.

- **111-Node Apache Spark 4.x Catalog**
  - **40 Sources**: Stream from Kafka, Pub/Sub, Kinesis, Pulsar, Event Hubs, MQTT, Rate, and Socket sources. Read Iceberg, Delta Lake, Hudi, Hive, Parquet, ORC, Avro, CSV, JSON, Text, XML, BinaryFile, and TFRecord datasets. Query BigQuery, Spanner, Bigtable, Cloud SQL, AlloyDB, JDBC, Snowflake, Redshift, ClickHouse, and SingleStore. Ingest MongoDB, Firestore, Cassandra, Redis, Elasticsearch, Neo4j, REST API, SFTP, and in-memory DataFrames.
  - **33 Steps**: Project, filter, enforce DDL schema contracts, manipulate nested structs and arrays, parse payloads, split branches, and assert data quality. Join (`left`/`right` and broadcast), merge CDC/SCD, union or intersect sets, window, aggregate (`Tumbling`, `Sliding`, `Session`), deduplicate, and pivot. Apply watermarks, `transformWithState`, Catalyst SQL, Arrow/Pandas UDFs, repartition/cache controls, Spark MLlib, Vertex AI (`gemini-4-hydrogen`), and RAG embeddings (`gemini-embedding-001`).
  - **38 Sinks**: Write to Kafka, Pub/Sub, Kinesis, Pulsar, Event Hubs, MQTT, BigLake Iceberg, Delta Lake, Hudi, and Hive. Export Parquet, ORC, Avro, CSV, JSON, Text, XML, and TFRecord files across GCS, S3, ADLS, HDFS, or local storage. Load BigQuery, Spanner, Bigtable, Cloud SQL, AlloyDB, JDBC, Snowflake, Redshift, ClickHouse, SingleStore, NoSQL stores, Vector Store, Console, Memory, and `ForeachBatch`.

- **Dynamic Gemini Model Discovery & Skill-Driven Code Generation**
  - Discover supported Gemini 3.x+ and 4.x text-generation models dynamically via `@google/genai` `ai.models.list` (`src/codegen/modelDiscovery.ts`).
  - If your environment defines a Google Cloud project, query Vertex AI (`location: 'global'`) first, then check the Gemini Developer API.
  - Filter out deprecated legacy models, embeddings, media generators, tuned models, and live-only endpoints automatically.
  - Rank eligible models by descending version and select the highest-ranked Flash-class model (`gemini-4-hydrogen`) by default.
  - If credentials or network access are unavailable, fall back offline to `gemini-4-hydrogen` (`DEFAULT_GEMINI_MODEL`).
  - Pair modular skills (`resources/codegen-skills/`) with deterministic planners (`executionPlanner.ts`, `sdpPlanner.ts`, and `versionMatrix.ts`) during prompt construction.
  - Isolate pure DataFrame logic in `transform(dfs, config)` (Imperative) or `src/transforms.py` (SDP) so generated `pytest` suites run offline.
  - If you enable `sparkDesigner.codegen.enableReviewer`, run `ReadOnlyCheckRunner` (`py_compile`, purity checks, `pytest --collect-only`, and `spark-pipelines dry-run`) with Gemini fix passes.

- **Live GCP CLI Asset Discovery, Redaction & Manifest Protection**
  - Execute local `gcloud` and `bq` commands using safe argument arrays without shell interpolation.
  - Autocomplete Dataproc clusters, session templates, BigLake catalogs, BigQuery datasets and tables, GCS buckets, subnetworks, regions, and Secret Manager secrets.
  - Redact credentials and secret values automatically via `redactPipelineDocument` before building Gemini prompts or writing `gemini.log`.
  - Record generated file ownership in `.sparkgen.json` so subsequent generations never overwrite untracked user files.

- **Integrated Runners, SDP Refresh Controls & `SparkExplorer` Sidebar**
  - **Imperative Runners**: Run `Local` (`run_local.sh` with automatic `.venv`, Eclipse Temurin JDK 21, Central Ivy bootstrap, and optional `ManagedSparkSession` Spark Connect). Submit cloud workloads to `DataprocCluster` (`run_dataproc_cluster.sh`) or `ServerlessSpark` (`run_serverless_spark.sh`, defaulting to the `standard` billing tier).
  - **SDP Runners & Refresh Controls**: Generate Local Spark 4.2 Connect (`run_sdp_local.sh`), Managed Data Pipelines (`sdp_stage.pipeline.yaml`), and Standalone Dataproc 4.0 (`run_sdp_dataproc.sh`) runners. Trigger `dry-run`, incremental `run`, targeted `--refresh`, and confirmed `--full-refresh` or `--full-refresh-all` resets with overlapping-run protection.
  - **`SparkExplorer` Sidebar**: Browse workspace `.spark` files, live `Serverless Batches`, and `Dataproc Cluster Jobs` with Cloud Console links and active job cancellation.

---

## Bundled Sample Pipelines (`resources/samples/`)

The extension includes **six** bundled `.spark` sample pipelines and companion `.md` guides in `resources/samples/`.

| Sample | Paradigm | Highlights |
| :--- | :--- | :--- |
| `gcs-parquet-etl.spark` | `imperative` | Ingests Cloud Storage Parquet orders, pushes down filters, computes daily revenue metrics, and coalesces output files. |
| `kafka-streaming-to-iceberg.spark` | `imperative` | Consumes Kafka SASL_SSL events, routes corrupt JSON to Console DLQ, applies a watermark, and writes windows to BigLake Iceberg. |
| `bigquery-cloudsql-broadcast-join.spark` | `imperative` | Enriches a BigQuery fact table with Cloud SQL PostgreSQL via IAM Socket Factory broadcast join and window ranking. |
| `generate-mock-data.spark` | `imperative` | Generates synthetic `Customer` records locally every second using Python `Faker` inside `mapInPandas` and logs micro-batches to Console. |
| `sdp-biglake-iceberg-medallion.spark` | `sdp` | Streams CSV orders into Bronze and Silver `@dp.table` datasets and aggregates a Gold BigLake Iceberg `@dp.materialized_view`. |
| `sdp-auto-cdc-scd1.spark` | `sdp` | Merges streaming customer CDC JSON events, delete tombstones, and out-of-order timestamps via `dp.create_auto_cdc_flow` (SCD Type 1). |

---

## Prerequisites

- **VS Code**: Install Visual Studio Code version `1.74.0` or newer.
- **Google Cloud SDK**: Install `gcloud` and `bq`, then authenticate your active Google Cloud project.
  ```bash
  gcloud auth login
  gcloud auth application-default login
  gcloud config set project <YOUR_GCP_PROJECT_ID>
  ```
- **Gemini Access**: Enable `aiplatform.googleapis.com` in your Google Cloud project, or set `GEMINI_API_KEY` for the Gemini Developer API.
- **Imperative Local Runtime (Optional)**: Install Python 3.10+. When needed, `run_local.sh` bootstraps `.venv` and Eclipse Temurin JDK 21 automatically.
- **SDP Local Runtime (Optional)**: Install Python 3.10+, JDK 17 or 21, and `pyspark>=4.2.0` (`spark-pipelines` CLI over Spark Connect).

---

## Getting Started

Open one of the six bundled templates from the **Samples** tab, or create a new pipeline from scratch.

1. Click the **Spark Designer** icon in the VS Code Activity Bar.
2. Click **Spark: New Pipeline** (`+`) in the **SparkExplorer** title bar.
3. When prompted, select **Imperative PySpark (Default)** or **Spark Declarative Pipelines (SDP)**.
4. Save the new `.spark` pipeline file in your workspace folder.
5. Drag Sources, Steps, and Sinks from the palette onto the canvas.
6. Connect node handles on the canvas to build the pipeline DAG.
7. Select each node to configure connector, schema, or transformation properties in the inspector.
8. Select an output folder and Gemini model in the **Source Code** tab.
9. Click **Generate Code** to synthesize your project files and offline `pytest` suite.
10. Configure your target runner or SDP refresh mode in the **Deployment** tab.
11. Click **Run Now** to execute the pipeline locally or on Google Cloud.

---

## Directory Layout

```text
apache-spark-designer/
├── resources/
│   ├── code-gen-schema.json        # Structured JSON output schema for Gemini
│   ├── codegen-prompts/            # Optional AI reviewer and fix prompt templates
│   ├── codegen-skills/             # Modular PySpark 4.x and SDP 4.2+ codegen skills
│   │   └── README.md               # Skill authoring contract and ExecutionPlan placeholders
│   ├── samples/                    # Six bundled .spark samples and companion .md guides
│   └── templates/pyspark/          # Byte-identical Imperative and SDP runner templates
├── src/
│   ├── cli/                        # Safe gcloud and bq asset discovery clients
│   ├── codegen/                    # Dynamic modelDiscovery, prompt builder, and check runner
│   ├── editor/                     # SparkEditorProvider custom .spark editor and tab upgrade
│   ├── execution/                  # PipelineRunner and cloud job ID detection
│   ├── samples/                    # Bundled sample discovery and metadata service
│   ├── security/                   # Path, URI, env, and executable script guards
│   ├── shared/                     # Pure cross-boundary DAG validator and Imperative/SDP planners
│   ├── spark/                      # Spark 4.x versionMatrix and connector artifact resolver
│   ├── tree/                       # SparkExplorer sidebar tree view provider
│   └── test/
│       ├── integration/            # Multi-agent SparkArchitectureAgent and pytest harness
│       │   └── README.md           # Evaluator suite and offline pytest runner guide
│       └── unit/                   # Vitest unit suites for host, planners, and docs
├── walkthroughs/                   # Interactive VS Code onboarding walkthrough steps
├── webview/                        # React 19 + React Flow + Tailwind CSS Vite frontend
├── ARCHITECTURE.md                 # Component topology, planners, versionMatrix, and IPC registry
├── DESIGN.md                       # Design tokens, node badges, handles, and UI contracts
├── LICENSE.txt                     # Apache License 2.0
└── package.json                    # VS Code extension manifest, settings, and npm scripts
```

### Documentation Guides

- [ARCHITECTURE.md](ARCHITECTURE.md): Spark 4.x component topology, `executionPlanner.ts` and `sdpPlanner.ts` flows, `versionMatrix.ts`, and IPC message registry.
- [DESIGN.md](DESIGN.md): Visual design system, `BATCH`/`STREAM` and SDP dataset badges, `Join` and `ConditionalSplit` handles, and component contracts.
- [resources/codegen-skills/README.md](resources/codegen-skills/README.md): Codegen skill authoring contract, YAML frontmatter rules, and `ExecutionPlan` prompt placeholders.
- [src/test/integration/README.md](src/test/integration/README.md): Guide for running the `SparkArchitectureAgent` multi-agent evaluation suite and offline PySpark `pytest` harness.

---

## Development, Verification & Integration Testing

```bash
# Install dependencies (outside offline sandboxes)
npm install
npm --prefix webview install

# Compile, typecheck, lint, bundle, and run unit and webview test suites
npm run compile
npm run typecheck:webview
npm run typecheck:integration
npm run lint
npm --prefix webview run lint
npm run build:webview
npm run test:all

# Run end-to-end multi-agent Spark evaluator suites (requires Gemini credentials)
npm run test:integration:e2e

# Run offline PySpark 4.x pytest verification on generated pipelines
JAVA_HOME=/path/to/jdk-21 SPARK_PYTEST_PYTHON=/path/to/venv/bin/python npm run test:integration:pytest
```

- Before reporting test counts, run `npm run build:webview` so the `webview-csp` unit suite inspects the compiled webview bundle.
- When running `npm run test:integration:pytest`, set `JAVA_HOME` to JDK 17 or 21 and `SPARK_PYTEST_PYTHON` to a Python environment with `pyspark>=4` and `pytest`.

---

## Extension Settings

| Setting | Default | Description |
| :--- | :--- | :--- |
| `sparkDesigner.gcloudPath` | `""` | Custom absolute path to the `gcloud` executable. If empty, the extension searches `PATH` and standard install locations. |
| `sparkDesigner.codegen.enableReviewer` | `false` | If `true`, runs read-only checks and up to two Gemini review-and-fix passes after code generation. |

---

## License

See [Apache 2.0 License](LICENSE.txt) for license terms.
