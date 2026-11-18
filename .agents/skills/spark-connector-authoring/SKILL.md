---
name: spark-connector-authoring
description: >-
  Authors and wires Apache Spark 4.x catalog nodes (Sources, Sinks, and Steps),
  React property forms, documentation entries, and resources/codegen-skills/
  SKILL.md files while satisfying all repository invariant tests. Use when
  implementing SRC*, SNK*, or OP* tasks or adding a new Spark connector or
  transformation node.
---

# Spark 4.x Connector & Transform Authoring Guide

## 1. Codegen Skill Invariants (`resources/codegen-skills/`)

Every catalog `sparkTransform` must resolve to an `io-*` or `transform-*` skill in `resources/codegen-skills/<folder>/SKILL.md` (`src/test/unit/codegen/codegen-skills-invariants.test.ts`).

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
   - Restrict `preloadWhen` to `streaming`, `python_datasource`, `multi_output`, `custom_instructions`, `side_inputs`, and `xlang`.
   - Only `{{spark_version}}`, `{{pipeline_name}}`, `{{pipeline_class_name}}`, and `{{gcs_staging_path}}` are allowed as `{{...}}` placeholders. Any other `{{...}}` token fails the invariant suite. Use shell variables (`${PROJECT_ID}`, `${DEPS_BUCKET}`) inside bash templates, and never reference deleted `code-gen-prompt-*` files.
2. **One `io-<connector>` Skill Per Connector (Source + Sink Sharing)**:
   - Each `sparkTransform` can be owned by at most **one** `kind: io` or `kind: transform` skill (`TRANSFORM_OWNER_KINDS`). Only `kind: crosscutting` skills (`core-streaming`, `core-python-datasource`) may co-list those transforms in `metadata.transforms` for preloading.
   - When adding a Sink (`<Name>Sink`) whose counterpart Source (`<Name>Source`) already exists in `resources/codegen-skills/io-<connector>/SKILL.md`, extend that existing skill (`transforms: [<Name>Source, <Name>Sink]`) and add the `write_sinks(dfs, config)` batch and streaming write rules to it.
   - Create new `io-*` folders only for sink-only connectors (`io-console`, `io-memory`, `io-foreach-batch`), and group Step nodes under `kind: transform` skills.
   - **Legacy Alias Disambiguation**: `CodegenSkillCatalog.transformKey()` strips trailing `IO`, `Source`, and `Sink`. Once both `KafkaSource` and `KafkaSink` are registered, bare `'KafkaIO'` matches two transforms (`matches.length === 2`) and `resolveTransform('KafkaIO')` will not guess. Always map legacy `BeamIO` names by node `type` (`SOURCE` vs `SINK`) in `webview/src/utils/legacyNodeMigration.ts`.
   - **Feature Detector Config Sync**: When adding or renaming catalog transforms, keep `STREAMING_SOURCE_TRANSFORMS`, `STREAMING_STEP_TRANSFORMS`, and `PYTHON_DATASOURCE_TRANSFORMS` in `src/codegen/codegenConfig.ts` aligned with `src/shared/sparkNodeCapabilities.ts` and `src/spark/versionMatrix.ts`.
   - **Prompt Builder Fixture Sync**: If you add a new `io-*` or `transform-*` skill for a transform referenced in `src/test/unit/codegen/CodegenPromptBuilder.test.ts` (such as `Watermark` or `PubSubSource`), update the expected selected skill list in that test.
3. **Managed Spark 4.x Classpath & Testable I/O Rules (`src/spark/versionMatrix.ts`)**:
   - Structure code examples around `read_sources(spark, config)`, pure `transform(dfs, config)` (zero external I/O so `tests/test_pipeline.py` runs offline with in-memory DataFrames), and `write_sinks(dfs, config)`.
   - Never pass pre-installed Managed Spark connectors (`spark-bigquery`, `iceberg-spark-runtime`, `gcs-connector`, `hadoop-aws`, `hadoop-azure`) to `spark.jars.packages` on Serverless Spark or Dataproc Clusters (Maven coordinates apply to `local` runner only).
   - Attach Spanner (`gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar`), Bigtable (`gs://spark-lib/bigtable/spark-bigtable_2.13-0.1.0.jar`), and Cloud SQL / AlloyDB Socket Factory JARs via `--jars` / `spark.jars`. Emit `dataproc.artifacts.remove=conscrypt` for AlloyDB and SQL Server.
   - Use Spark 4.0 Python `DataSource` / `foreachBatch` SDK patterns (`core-python-datasource`) for connectors without a Spark 4.x Scala 2.13 JVM JAR (`PubSub*`, `Kinesis*`, `Mqtt*`, `Firestore*`, `Cassandra*`, `Redis*`, `ForeachBatchSink`).
4. **Runner Template Mirroring & Pinned Credential Corpus Guards**:
   - Keep runner templates (`run_local.sh`, `run_dataproc_cluster.sh`, `run_serverless_spark.sh`, `Dockerfile`) byte-identical across `resources/codegen-skills/runner-<id>/templates/python/<file>` and `resources/templates/pyspark/<file>`.
   - Preserve the test-pinned credential phrases in `resources/codegen-skills/core-pyspark/SKILL.md` (`src/test/unit/credential-redaction.test.ts` checks `skillsCorpus('python')` for `## Credential Handling Rules`, `passwordSecret`, `accessKeySecret`, `secretName`, `name ends in \`Secret\``, `Resolve the secret ONCE`, `never ... per element` / `DoFn`, `REDACTED_CREDENTIAL_USE_ENV_VAR`, and `environment variable`).

## 2. Property Forms & Shared Components (`webview/src/`)

1. **Shared Form Building Blocks & `PropertyForm.tsx` Container Contract**:
   - **Container-Managed Cards (Never Duplicate)**: `webview/src/components/PropertyForm.tsx` automatically renders `<NodeDocsCard />`, `<StepExecutionSettingsField />` (on `STEP` nodes), and `<StreamingSinkSettingsField />` (on streaming `SINK` nodes) above the lazy panel component. Never render those three components inside individual Source, Sink, or Step panels.
   - **System Instructions Footer**: Render `<SystemInstructionsField />` (`resolveSystemInstructions(node)`) at the bottom of every Source, Sink, and Step property panel.
   - **File Format Nodes (`Parquet`, `ORC`, `Avro`, `CSV`, `JSON`, `Text`, `XML`, `BinaryFile`, `TFRecord`)**: Embed `StorageBackendFields` (`webview/src/components/common/StorageBackendFields.tsx`) for `gs://`, `s3a://`, `abfss://`, `wasbs://`, `hdfs://`, and `file://` URIs, live GCS bucket quick-pick (`useAssetLoader`), `pathGlobFilter`, `recursiveFileLookup`, and `SecretManagerAuthFields`.
   - **Live GCP Asset Discovery (`useAssetLoader`)**: Preserve `useAssetLoader` hooks in `BigQueryIO` (`datasets`, `tables`, `buckets`), `IcebergIO` (`bigLakeCatalogs`), `BigTableIO` (`bigtableInstances`, `bigtableTables`), `CloudSQL` (`sqlInstances`, `sqlDatabases`), `StorageBackendFields` (`buckets`), and `SecretManagerAuthFields` / `SecretManagerSecretField` (`secrets`). Note that `SpannerIO` and `AlloyDb` use manual text fields rather than `useAssetLoader`.
2. **Registry & Documentation 1-to-1 Parity (`webview/src/catalog/propertyFormRegistry.test.ts`)**:
   - Every key added to `PROPERTY_FORM_REGISTRY` (`webview/src/catalog/propertyFormRegistry.ts`) must have a matching key in `NODE_DOCUMENTATION_REGISTRY` (`webview/src/catalog/documentation/{sources,sinks,transforms}.ts`).
   - Declare **one** `const XProperties = lazyForm(() => import(...))` per concrete React component and reuse that variable for all alias keys (`SINK:KafkaSink`, `KafkaSink`, `SINK:KafkaIO`, `WriteToKafka`). Two `lazyForm()` calls pointing to the same component fail the unique-chunk assertion.
   - Delete unreferenced legacy Beam panel `.tsx` and `.test.tsx` files under `webview/src/properties/{source,sink,step}/` when replacing legacy palette nodes so no dead chunks remain.
   - Match `NodeDefinition` capability flags (`supportedReadModes`, `defaultReadMode`, `streamingSinkMode`, `errorOutput`, `inputArity`) in `webview/src/catalog/nodes/{sources,sinks,steps}.ts` against `SPARK_NODE_CAPABILITIES` in `src/shared/sparkNodeCapabilities.ts`.
   - Update `defaultGroupOpenState` in `webview/src/catalog/paletteGroupState.ts` whenever palette `category` names change.
3. **Cross-Boundary `STEP` Property Contracts (`src/shared/*` & `src/spark/versionMatrix.ts`)**:
   - **`Join`**: Connect incoming edges to `targetHandle: 'left'` and `targetHandle: 'right'`. `PipelineNode.tsx` reads `properties.broadcastRight === true` (`BROADCAST` badge), and `dagValidator.ts` reads `properties.joinType` (`'inner' | 'left_outer' | 'right_outer' | 'full_outer' | 'left_semi' | 'left_anti' | 'cross'`).
   - **`ConditionalSplit`**: `sparkDagRules.ts` (`getConditionalSplitHandles`) reads `properties.rules` (`Array` of `{ name, condition }` objects where `name` is never `'else'` or duplicated) and automatically appends the terminal `'else'` handle.
   - **`Watermark` & `GroupByAgg`**: `Watermark` uses `properties.eventTimeColumn` and `properties.delayThreshold`. `GroupByAgg` uses `properties.groupByColumns`, `properties.windowType` (`'None' | 'Tumbling' | 'Sliding' | 'Session'`), `properties.timeColumn`, `properties.windowDuration`, `properties.slideDuration`, `properties.sessionGapDuration`, and `properties.aggregations`.
   - **`SparkMlAndVertexAi`**: `versionMatrix.ts` (`resolveMlAndVertexConnector`) inspects `properties.engine` (`'spark_ml' | 'xgboost' | 'vertex_ai_endpoint' | 'vertex_ai_gemini'`), `properties.useXgboost`, `properties.useVertexAi`, `properties.endpointId`, and `properties.geminiModel` (default `'gemini-3.8-flash'` with `location: 'global'` enforced for `/^gemini-3\./i`).

## 3. Verification Checklist

Run these checks before committing:

```bash
npm run typecheck
npm --prefix webview run typecheck
npx vitest run src/test/unit/codegen/codegen-skills-invariants.test.ts src/test/unit/codegen/CodegenSkillCatalog.test.ts src/test/unit/credential-redaction.test.ts src/test/unit/shared-purity.test.ts
npm --prefix webview test -- --run src/catalog/propertyFormRegistry.test.ts
```
