# Apache Spark / Managed Spark Pipeline Builder

[![VS Code](https://img.shields.io/badge/VS%20Code-v1.74%2B-007ACC?logo=visualstudiocode)](https://code.visualstudio.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.x%20%7C%20SDP%204.2%2B-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Google Cloud Managed Spark](https://img.shields.io/badge/Google%20Cloud-Managed%20Spark-4285F4?logo=googlecloud)](https://cloud.google.com/dataproc-serverless)
[![Powered by Gemini](https://img.shields.io/badge/Vertex%20AI-Gemini%203.8%20Flash-8E75B2?logo=googlegemini)](https://cloud.google.com/vertex-ai)

**Apache Spark / Managed Spark Pipeline Builder** is a visual VS Code extension for designing, validating, generating, and deploying **Apache Spark 4.x** pipelines across **Local PySpark / Spark Connect**, **Google Cloud Dataproc Clusters**, and **Serverless for Apache Spark**.

Design pipelines once in `.spark` YAML on an interactive React Flow canvas and synthesize either **Imperative PySpark 4.x** repositories or **Spark Declarative Pipelines (SDP, Apache Spark 4.2+)** projects with offline `pytest` suites.

---

## Dual Pipeline Paradigms: Imperative PySpark vs. Spark Declarative Pipelines

Pick a paradigm when creating a new `.spark` pipeline or toggle paradigms from the canvas toolbar:

| Capability | Imperative PySpark (`imperative`, Default) | Spark Declarative Pipelines (`sdp`, Spark 4.2+) |
| :--- | :--- | :--- |
| **Target Spark Version** | Apache Spark 4.0+ (Serverless 3.0 & 4.0, Dataproc 3.0 & 4.0, Local 4.0) | Apache Spark 4.2+ over Spark Connect (Serverless 4.0, Dataproc 4.0, Local 4.2 Connect) |
| **Generated Deliverables** | `src/pipeline.py`, `tests/test_pipeline.py`, `requirements.txt`, `Dockerfile`, `run_local.sh`, `run_dataproc_cluster.sh`, `run_serverless_spark.sh`, `README.md` | `spark-pipeline.yml`, `transformations/*.py` (`@dp.*`), `src/transforms.py`, `tests/test_transforms.py`, `requirements.txt`, `run_sdp_local.sh`, `run_sdp_dataproc.sh`, `sdp_stage.pipeline.yaml`, `README.md` |
| **Streaming & Batch Execution** | Batch, `availableNow`, `processingTime` micro-batches, and `continuous` triggers | Triggered `AvailableNow` runs (`@dp.table`, `@dp.materialized_view`, `@dp.temporary_view`) |
| **CDC & SCD Support** | SCD Type 1 and SCD Type 2 merges (`MergeCdcScd`) | Managed SCD Type 1 Auto CDC (`dp.create_auto_cdc_flow`) |
| **Sink & ML Coverage** | All native and `foreachBatch` sinks (Spanner, Cloud SQL, AlloyDB, Bigtable, JDBC, Pub/Sub) plus Spark ML and Vertex AI | Lakehouse catalog tables (BigLake Iceberg, Delta, Hudi) and streaming `dp.create_sink` connectors (Kafka, BigQuery, Pulsar, files) |

---

## Key Features

- **Interactive Visual DAG Canvas (`.spark`)**
  - Drag-and-drop Sources, Steps, and Sinks with real-time topological validation, cycle detection, watermark checks, and Undo/Redo history.
  - Direct `SOURCE -> SINK` connections for zero-transform ingestion and replication pipelines.
  - Paradigm-aware micro-badges: `BATCH`, `STREAM`, `CACHE`, and `foreachBatch` in Imperative mode, or `VIEW`, `MV`, `TABLE`, `CDC`, and `SINK` in SDP mode.
  - Smart compatibility gate when switching from Imperative to SDP, plus greyed-out palette tooltips on unsupported SDP nodes.

- **Comprehensive Spark 4.x Catalog**
  - **Sources**: Kafka, Pub/Sub, Kinesis, Pulsar, Event Hubs, MQTT, BigQuery, BigLake Iceberg, Delta Lake, Hudi, Hive, Parquet, ORC, Avro, CSV, JSON, Text, XML, BinaryFile, TFRecord, Spanner, Cloud SQL, AlloyDB, Generic JDBC, Snowflake, Redshift, ClickHouse, SingleStore, Bigtable, MongoDB, Firestore, Cassandra, Redis, Elasticsearch, Neo4j, REST API, SFTP, Rate, Socket, and in-memory DataFrames.
  - **Steps**: Filter, Select, WithColumns, Rename/Drop, Enforce Schema & Cast, Handle Nulls, Parse Payload, Explode, Nested Struct/Array Ops, Conditional Split, Data Quality Assert, Watermark, GroupBy & Aggregate (with Tumbling, Sliding, and Session windows), Rollup/Cube, Window Over, Top-N, OrderBy, Deduplicate, Join (including Broadcast and Stream-Static), Merge CDC / SCD, UnionByName, Set Operations, Pivot/Unpivot, Repartition/Coalesce, Cache/Persist, Pandas UDF, Stateful Transform (`transformWithState`), Spark SQL, Feature Prep, Spark ML & Vertex AI (`gemini-3.8-flash`), and Text Chunk & Embed.
  - **Sinks**: Lakehouse tables (Iceberg, Delta, Hudi), warehouses and databases, streaming brokers, cloud object storage formats, Vector Store, Console, Memory, and custom `ForeachBatch`.

- **Live Cloud Asset Discovery & Credential Safety**
  - Queries local `gcloud` and `bq` CLIs using safe argument arrays to autocomplete BigQuery datasets and tables, Cloud Storage buckets, BigLake catalogs, Cloud SQL instances, Bigtable tables, VPC subnetworks, and Secret Manager secrets.
  - Redacts credentials automatically (`redactPipelineDocument`) before building prompts or writing `gemini.log`.

- **Skill-Driven Gemini Code Generation (`gemini-3.8-flash`)**
  - Uses Vertex AI (`location: 'global'`) and modular skills under `resources/codegen-skills/` paired with deterministic execution plans (`executionPlanner.ts` and `sdpPlanner.ts`).
  - Separates pure DataFrame logic (`transform(dfs, config)` in Imperative mode and `src/transforms.py` in SDP mode) so generated `pytest` suites run offline with in-memory DataFrames.
  - Includes an optional AI Code Reviewer Pass (`sparkDesigner.codegen.enableReviewer`) backed by `ReadOnlyCheckRunner` (`py_compile`, contract and SDP purity checks, `pytest --collect-only`, and optional `spark-pipelines dry-run`).

- **Integrated Runners, SDP Refresh Controls & Job Explorer**
  - **Imperative Runners**: Local PySpark (`run_local.sh` with automatic `.venv`, JDK 17/21, and Maven Central Ivy bootstrap), Dataproc Clusters (`run_dataproc_cluster.sh`), and Serverless for Apache Spark (`run_serverless_spark.sh`, `standard` tier default).
  - **SDP Runners & Refresh Controls**: Local Spark 4.2 Spark Connect (`run_sdp_local.sh`), Managed Data Pipelines `sdp` stage (`sdp_stage.pipeline.yaml`), and Standalone Dataproc 4.0 (`run_sdp_dataproc.sh`), with UI controls for `dry-run`, incremental `run`, `--refresh`, `--full-refresh`, and `--full-refresh-all` (with dataset-named reset confirmation and overlapping-run protection).
  - **Spark Explorer Sidebar**: Lists workspace `*.spark` pipelines alongside live `Serverless Batches` and `Dataproc Cluster Jobs` with one-click Cloud Console links.

---

## Bundled Sample Pipelines (`resources/samples/`)

| Sample | Paradigm | Highlights |
| :--- | :--- | :--- |
| `gcs-parquet-etl.spark` | `PySpark` | Batch Cloud Storage Parquet ingestion, filter pushdown, revenue projections, daily aggregation, and partition coalescing |
| `kafka-streaming-to-iceberg.spark` | `PySpark` | Streaming Kafka SASL_SSL consumer, corrupt JSON DLQ split, event-time watermark, tumbling window metrics, and BigLake Iceberg sink |
| `bigquery-cloudsql-broadcast-join.spark` | `PySpark` | BigQuery fact table enriched with Cloud SQL PostgreSQL via IAM Socket Factory broadcast join and window ranking |
| `generate-mock-data.spark` | `PySpark` | Zero-cloud, zero-JAR local streaming pipeline using Python `Faker` via `mapInPandas` to generate mock `Customer` records and log them to console every 1 second |
| `sdp-biglake-iceberg-medallion.spark` | `SDP (4.2+)` | Streaming CSV ingestion into Bronze and Silver `@dp.table` datasets and a Gold BigLake Iceberg `@dp.materialized_view` |
| `sdp-auto-cdc-scd1.spark` | `SDP (4.2+)` | Streaming customer CDC JSON events with delete tombstones and out-of-order timestamps merged via `dp.create_auto_cdc_flow` (SCD Type 1) |

---

## Prerequisites

1. **Visual Studio Code**: Version `1.74.0` or newer.
2. **Google Cloud SDK (`gcloud` and `bq`)**: Authenticated with an active project for Vertex AI code generation and cloud deployment:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   gcloud config set project <YOUR_GCP_PROJECT_ID>
   ```
3. **Vertex AI API**: Enable `aiplatform.googleapis.com` in your active Google Cloud project.
4. **Local Python & Java (Optional for Local Runs & Offline Tests)**:
   - Imperative Local mode: Python 3.10+ and JDK 17 or 21 (`run_local.sh` bootstraps `.venv` and Temurin JDK 21 automatically).
   - SDP Local mode: Apache Spark 4.2+ (`pyspark>=4.2.0`) with Spark Connect (`spark-pipelines` CLI) and JDK 17 or 21.

---

## Getting Started

1. Open the **Spark Designer** icon in the VS Code Activity Bar.
2. Click **`+`** (`Spark: New Pipeline`) and choose **Imperative PySpark (Default)** or **Spark Declarative Pipelines (SDP)**.
3. Save the pipeline as `<name>.spark` in your workspace (or open one of the seven bundled templates from the **Samples** tab).
4. Drag Sources, Steps, and Sinks onto the canvas, connect their handles, and configure properties in the right-hand inspector.
5. Open the **Source Code** tab in the bottom panel, pick an output folder, and click **Generate Code**.
6. Open the **Deployment** tab to configure your target runner (or SDP storage, BigLake catalog, and refresh mode) and click **Run Now**.

---

## Directory Layout

```text
apache-spark-designer/
├── resources/
│   ├── code-gen-schema.json        # Structured JSON output schema for Gemini
│   ├── codegen-prompts/            # Optional AI reviewer and fix prompt templates
│   ├── codegen-skills/             # Modular PySpark 4.x and SDP 4.2+ codegen skills
│   ├── samples/                    # Seven bundled .spark samples and companion .md guides
│   └── templates/pyspark/          # Imperative and SDP runner templates
├── src/
│   ├── cli/                        # Safe gcloud and bq asset discovery clients
│   ├── codegen/                    # Feature detector, skill catalog, prompt builder, check runner
│   ├── editor/                     # SparkEditorProvider custom .spark editor & tab upgrade
│   ├── execution/                  # PipelineRunner & job ID detection
│   ├── samples/                    # Sample discovery and metadata service
│   ├── security/                   # Path, URI, env, and executable script guards
│   ├── shared/                     # Pure cross-boundary DAG validator & Imperative/SDP planners
│   ├── spark/                      # Spark 4.x runtime version matrix & connector artifacts
│   ├── tree/                       # SparkExplorer sidebar tree view provider
│   └── test/                       # Vitest unit suites and multi-agent integration evaluators
├── walkthroughs/                   # Interactive VS Code onboarding walkthrough steps
├── webview/                        # React 19 + React Flow + Tailwind CSS Vite frontend
├── ARCHITECTURE.md                 # System architecture, planners, and SDP decision guide
├── DESIGN.md                       # UI/UX design tokens, badges, and component contracts
└── package.json                    # VS Code extension manifest and npm scripts
```

---

## Development & Verification Scripts

```bash
# Install dependencies (outside offline sandboxes)
npm install
npm --prefix webview install

# Compile, typecheck, lint, bundle, and test
npm run compile
npm run typecheck:webview
npm run typecheck:integration
npm run lint
npm --prefix webview run lint
npm run build:webview
npm run test:all
```

### Extension Settings

| Setting | Default | Description |
| :--- | :--- | :--- |
| `sparkDesigner.gcloudPath` | `""` | Custom absolute path to the `gcloud` executable. Searches `PATH` when empty. |
| `sparkDesigner.codegen.enableReviewer` | `false` | Runs read-only checks and up to two Gemini review-and-fix passes after code generation. |

---

## License

This project is licensed under the [Apache 2.0 License](LICENSE).
