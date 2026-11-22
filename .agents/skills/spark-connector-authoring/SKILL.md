---
name: spark-connector-authoring
description: >-
  Authors and wires Apache Spark 4.x catalog nodes (Sources, Sinks, and Steps),
  React property forms, documentation entries, bundled .spark samples, E2E
  integration pipelines, and resources/codegen-skills/ SKILL.md files while
  satisfying all repository invariant tests. Use when implementing SRC*, SNK*,
  OP*, EXT*, or sample and E2E .spark pipeline tasks. Don't use for pure swarm
  branch orchestration without node or pipeline authoring (use
  phase-swarm-execution).
---

# Spark 4.x Connector & Transform Authoring Guide

Before editing files, apply the canonical workspace rules in `.agents/rules/codegen-skill-library.md`, `.agents/rules/webview-ui-design-system.md`, `.agents/rules/shared-module-purity.md`, `.agents/rules/model-selection.md`, and `.agents/rules/documentation-style.md`.

## 1. Codegen Skill Schema & Classpath Details (`resources/codegen-skills/`)

Every catalog `sparkTransform` must resolve to a skill in `resources/codegen-skills/<folder>/SKILL.md` (`src/test/unit/codegen/codegen-skills-invariants.test.ts`) while obeying `.agents/rules/codegen-skill-library.md`.

1. **Strict `skillFile.ts` Frontmatter**:
   ```yaml
   ---
   name: io-<slug>
   description: <one direct sentence with zero semicolons>
   metadata:
     kind: io
     usedBy: [generator, reviewer]
     languages: [python]
     runners: [all]
     paradigms: [imperative, sdp]
     transforms: [<SourceTransform>, <SinkTransform>]
     preloadWhen: []
     sources:
       - prompt
       - https://<official-spark-or-connector-docs-url>
     validated: true
   ---
   ```
   - `name` must equal the folder name. Unknown frontmatter keys or extra files next to `SKILL.md` throw at load time.
   - Every `io` and `transform` skill must include at least one `https://` URL in `metadata.sources`.
   - Restrict `preloadWhen` to `PIPELINE_FEATURES` in `src/codegen/codegenConfig.ts` (`streaming`, `python_datasource`, `multi_output`, `custom_instructions`, `xlang`, `sdp_auto_cdc`, and `sdp_quarantine`).
   - Use only `{{spark_version}}`, `{{pipeline_name}}`, `{{pipeline_class_name}}`, and `{{gcs_staging_path}}` placeholders (`.agents/rules/codegen-skill-library.md`). Use shell variables (`${PROJECT_ID}`, `${DEPS_BUCKET}`) inside bash templates, and never cite deleted `code-gen-prompt-*` files.
2. **Skill Ownership & Transform Resolution**:
   - Create new `io-*` folders only when no counterpart Source/Sink skill exists (such as sink-only `io-console`, `io-memory`, `io-foreach-batch`), and group Step nodes under `kind: transform` skills (`.agents/rules/codegen-skill-library.md`). Only `kind: crosscutting` skills (`core-streaming`, `core-python-datasource`) may co-list owned transforms for preloading.
   - **Legacy Alias Disambiguation**: `CodegenSkillCatalog.transformKey()` strips trailing `IO`, `Source`, and `Sink`. Once both `KafkaSource` and `KafkaSink` exist, bare `'KafkaIO'` matches two transforms and `resolveTransform('KafkaIO')` throws rather than guessing. Map legacy `IO` names by node `type` (`SOURCE` vs `SINK`) in `webview/src/utils/legacyNodeMigration.ts`.
   - **Prompt Builder Fixture Sync**: If you add an `io-*` or `transform-*` skill for a transform referenced in `src/test/unit/codegen/CodegenPromptBuilder.test.ts` (such as `Watermark` or `PubSubSource`), update the expected selected skill list in that test.
3. **Managed Spark 4.x Classpath & Testable I/O Rules (`src/spark/versionMatrix.ts`)**:
   - Structure imperative code examples into `read_sources(spark, config)`, pure `transform(dfs, config)` (zero external I/O so `tests/test_pipeline.py` runs offline with in-memory DataFrames), and `write_sinks(dfs, config)`.
   - Never pass pre-installed Managed Spark connectors (`spark-bigquery`, `iceberg-spark-runtime`, `gcs-connector`, `hadoop-aws`, `hadoop-azure`) to `spark.jars.packages` on Serverless Spark or Dataproc Clusters (`local` runner only).
   - Attach Spanner (`gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar`), Bigtable (`gs://spark-lib/bigtable/spark-bigtable_2.13-0.1.0.jar`), and Cloud SQL / AlloyDB Socket Factory JARs via `--jars` / `spark.jars`. Emit `dataproc.artifacts.remove=conscrypt` for AlloyDB (including `VectorStoreSink` when `backend === 'alloydb_pgvector'`) and SQL Server, and validate dynamic SQL identifiers against `^[A-Za-z_][A-Za-z0-9_]*$`.
   - Use Spark 4.0 Python `DataSource` / `foreachBatch` SDK patterns (`core-python-datasource`) for connectors without a Spark 4.x Scala 2.13 JVM JAR (`PubSub*`, `Kinesis*`, `Mqtt*`, `Firestore*`, `Cassandra*`, `Redis*`, `RestApiSource`, `SftpSource`, `VectorStoreSink`, `ForeachBatchSink`). Instantiate SDK clients lazily inside executor partition callbacks (`DataSourceReader.read`, `foreachPartition`, or `mapInPandas`), never in driver `__init__` or per row.

## 2. Property Forms & Cross-Boundary Node Contracts

Follow `.agents/rules/webview-ui-design-system.md` for layout tokens, `PropertyForm.tsx` container cards, `<SystemInstructionsField />`, `StorageBackendFields` reuse, RTL `{ selector }` queries, and 1-to-1 `PROPERTY_FORM_REGISTRY` / `NODE_DOCUMENTATION_REGISTRY` `lazyForm` parity.

1. **Live GCP Asset Discovery (`useAssetLoader`)**:
   - Preserve `useAssetLoader` hooks in `BigQueryIO` (`datasets`, `tables`, `buckets`), `IcebergIO` (`bigLakeCatalogs`), `BigTableIO` (`bigtableInstances`, `bigtableTables`), `CloudSQL` (`sqlInstances`, `sqlDatabases`), `StorageBackendFields` (`buckets`), and `SecretManagerAuthFields` / `SecretManagerSecretField` (`secrets`). `SpannerIO` and `AlloyDb` use manual text fields rather than `useAssetLoader`. If adding a discovery command, follow `.agents/rules/asset-discovery-ipc.md`.
2. **Catalog & Invariant Count Lockstep**:
   - Remove unreferenced legacy panel `.tsx` and `.test.tsx` files under `webview/src/properties/{source,sink,step}/` when replacing palette nodes so no dead chunks remain.
   - Match `NodeDefinition` capability flags (`supportedReadModes`, `defaultReadMode`, `streamingSinkMode`, `errorOutput`, `inputArity`) in `webview/src/catalog/nodes/{sources,sinks,steps}.ts` against `SPARK_NODE_CAPABILITIES` in `src/shared/sparkNodeCapabilities.ts`.
   - Update `defaultGroupOpenState` in `webview/src/catalog/paletteGroupState.ts` whenever palette `category` names change.
   - When adding or removing palette nodes (currently `40` Sources, `38` Sinks, `33` Steps = `111` total), update the pinned counts across all 4 invariant suites in lockstep: `webview/src/catalog/propertyFormRegistry.test.ts`, `src/test/unit/codegen/codegen-skills-invariants.test.ts`, `src/test/unit/codegen/CodegenSkillCatalog.test.ts`, and `src/test/unit/version-matrix.test.ts`.
3. **Cross-Boundary Node & Property Contracts (`src/shared/*` & `src/spark/versionMatrix.ts`)**:
   - **Dual-Input (`inputArity: 2`) Steps (`Join` & `MergeCdcScd`)**: Wire all 6 touchpoints in lockstep: `src/shared/sparkNodeCapabilities.ts` (`inputArity: 2`), `src/shared/sparkDagRules.ts` (`isDualInputNode`, `isJoinNode`, `isMergeCdcScdNode`), `src/shared/dagValidator.ts` (1 `'left'` + 1 `'right'` `targetHandle`), `webview/src/components/PipelineNode.tsx` (`L`/`R` target handles, keeping `BROADCAST` badge scoped to `Join` when `properties.broadcastRight === true`), `webview/src/hooks/usePipelineGraph.ts` (`isValidConnection`), and `webview/src/graph/nodeFactory.ts` (auto-wire `targetHandle: 'left'`). For `MergeCdcScd`, `'left'` is incoming CDC and `'right'` must be a batch target snapshot, emitting pure `_scd_action` / `_merge_*` staging columns inside `transform(dfs, config)` for `io-iceberg` and `io-delta` `MERGE` rules.
   - **`ConditionalSplit`**: `sparkDagRules.ts` (`getConditionalSplitHandles`) reads `properties.rules` (`Array` of `{ name, condition }` objects where `name` is never `'else'` or duplicated) and appends the terminal `'else'` handle.
   - **`Watermark` & `GroupByAgg`**: `Watermark` uses `properties.eventTimeColumn` and `properties.delayThreshold`. `GroupByAgg` uses `properties.groupByColumns`, `properties.windowType` (`'None' | 'Tumbling' | 'Sliding' | 'Session'`), `properties.timeColumn`, `properties.windowDuration`, `properties.slideDuration`, `properties.sessionGapDuration`, and `properties.aggregations`.
   - **`EnforceSchemaAndCast`**: Uses Spark 4.0 ANSI-safe `F.try_cast` / `F.try_to_timestamp` (never raw `.cast()`, which throws `CAST_INVALID_INPUT` under Spark 4.0 `spark.sql.ansi.enabled=true`), `properties.driftPolicy` (`'fail' | 'drop_extra' | 'add_nulls'`), and `_error` DLQ routing.
   - **`SparkMlAndVertexAi`, `TextChunkAndEmbed` & `VectorStoreSink`**: Follow `.agents/rules/model-selection.md` for default Gemini and embedding models on Vertex AI `location: 'global'`. `versionMatrix.ts` inspects `SparkMlAndVertexAi` (`engine`, `useXgboost`, `useVertexAi`, `endpointId`, `geminiModel`), `TextChunkAndEmbed` (`mode`: `'chunk_only'` -> `builtin`, `'embed_only'` / `'chunk_and_embed'` -> `google-genai` with `0 <= chunkOverlap < chunkSize`), and `VectorStoreSink` (`backend`: `'alloydb_pgvector'` -> `psycopg[binary]` + `google-cloud-alloydb-connector` + `dataproc.artifacts.remove=conscrypt`, `'vertex_vector_search'` -> `google-cloud-aiplatform`).

## 3. Bundled `.spark` Sample & E2E Pipeline YAML Contract

1. **React Flow Canvas & Handle Fidelity**:
   - Set `type: 'pipeline'` on every serialized node and use valid `data.iconName` keys from `webview/src/components/common/nodeIcons.ts`.
   - Wire explicit `sourceHandle: 'valid'` and `sourceHandle: 'error'` on error-capable steps (`errorOutput: true`), rule `name` and `'else'` handles on `ConditionalSplit`, and `targetHandle: 'left'` and `'right'` on `Join` and `MergeCdcScd`.
2. **Zero-Warning DAG & Streaming Validation**:
   - Include `pipelineConfig: { language: 'PySpark' }` and align every `data.properties` key and enum value with both its React form (`webview/src/properties/`) and its owning skill (`resources/codegen-skills/`).
   - For streaming pipelines, configure `jobConfig.checkpointRoot`, per-sink `checkpointLocation`, and an upstream `Watermark` step before streaming `GroupByAgg`, `Deduplicate` (`within_watermark`), or stream-stream outer `Join` so `validateDagTopology(nodes, edges, { jobConfig, requireCheckpoints: true })` returns `isValid: true`, `errors: []`, and `warnings: []`.
   - Use Secret Manager secret IDs or IAM authentication so `redactPipelineDocument` reports `redactedPaths: []`.
   - Verify `(cd webview && npx vitest run src/components/PropertyForm.test.tsx)` renders every node in `resources/samples/*.spark` and `src/test/integration/e2e/**/*.spark` with `> 0` form controls and zero unregistered warnings.
3. **Companion Sample Markdown (`resources/samples/<slug>.md`)**:
   - Follow `.agents/rules/documentation-style.md` (ASD-STE100 active voice with zero semicolons in prose). Include `# <Title>`, `## Overview`, `## Pipeline Architecture & Data Flow`, `## Runtime Parameters`, and `## Code Generation & Execution`, and omit `## Pipeline Nodes & Transforms`.

## 4. Verification Checklist

Run the full verification gate in `.agents/rules/toolchain-and-verification.md` plus these targeted catalog suites before committing:

```bash
npx vitest run src/test/unit/codegen/codegen-skills-invariants.test.ts src/test/unit/codegen/CodegenSkillCatalog.test.ts src/test/unit/credential-redaction.test.ts src/test/unit/shared-purity.test.ts
(cd webview && npx vitest run src/catalog/propertyFormRegistry.test.ts src/components/PropertyForm.test.tsx)
```
