# Graph Report - apache-spark-designer  (2026-11-20)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 3655 nodes · 10674 edges · 189 communities (176 shown, 13 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 164 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `8ad277f5`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- ref_vitest
- extension.ts
- shared/dagValidator.ts
- PipelineRunner.ts
- PropertyField
- codeGeneration.ts
- pytest-runner.ts
- App.tsx
- core/constants.ts
- SparkEditorProvider.ts
- CodegenSkillCatalog.ts
- env
- DeploymentTab.tsx
- source/AvroIO.tsx
- ref_path
- codegenConfig.ts
- SourceCodeTab.tsx
- deterministic-gate.ts
- shared/types.ts
- CodegenPromptBuilder.ts
- spark/versionMatrix.ts
- StorageBackendFields.tsx
- arbiter-evaluator.ts
- documentation/sources.ts
- useAssetLoader.ts
- ref_os
- documentation/sinks.ts
- shared/sdpPlanner.ts
- spark-architecture-evaluator.ts
- credentialRedaction.ts
- shared/executionPlanner.ts
- shared/sparkDagRules.ts
- webview/package.json
- scripts
- CodeReviewer.ts
- codegen-runner.ts
- package.json
- usePipelineGraph.ts
- AdvancedConfig.tsx
- SecretManagerAuthFields.tsx
- propertyFormRegistry.ts
- contributes
- packaging-evaluator.ts
- python/run_local.sh
- pyspark/run_local.sh
- codegen-skills-invariants.test.ts
- ReadOnlyCheckRunner.ts
- vscodeMock.ts
- react
- SamplePipeline
- sink/MqttIO.tsx
- pipeline-spec-evaluator.ts
- WindowOver.tsx
- devDependencies
- multi-agent-evaluator.test.ts
- ReadOnlyCheckRunner
- compilerOptions
- SystemInstructionsField.tsx
- ThemeContext.tsx
- compilerOptions
- PipelineParadigm
- test-reporter.ts
- MergeCdcScd.tsx
- NestedStructArrayOps.tsx
- panelBuildingBlocks.test.tsx
- StorageBackendFields
- compilerOptions
- eslint.config.js
- python/run_sdp_local.sh
- pyspark/run_sdp_local.sh
- propertyFormRegistry.test.ts
- HandleNulls.tsx
- sink/JdbcIO.tsx
- reviewerPass.ts
- systemInstructions.ts
- DataQualityAssert.tsx
- GroupByAgg.tsx
- Join.tsx
- StatefulTransform.tsx
- TextChunkAndEmbed.tsx
- TopN.tsx
- VectorStoreSink.tsx
- PropertyPane.tsx
- nodeDocumentation.ts
- ConditionalSplit.tsx
- EnforceSchemaAndCast.tsx
- Filter.tsx
- PropertyField.tsx
- sink/KafkaIO.tsx
- SelectField
- RestApiSource.tsx
- gcloudClient.ts
- PropertyForm.test.tsx
- Deduplicate.tsx
- sink/KinesisIO.tsx
- OrderBy.tsx
- PivotUnpivot.tsx
- RollupCube.tsx
- SdpSinkSettingsField.tsx
- legacyNodeMigration.ts
- EventHubsSink.tsx
- SelectField.tsx
- sink/TFRecordIO.tsx
- sink/AvroIO.tsx
- sink/BigQueryIO.tsx
- sink/CsvIO.tsx
- FeaturePrep.tsx
- sink/ParquetIO.tsx
- SparkMlAndVertexAi.tsx
- SparkSqlQuery.tsx
- sink/TextIO.tsx
- jobIdDetector.ts
- NodesCard.tsx
- SftpSource.tsx
- Watermark.tsx
- WithColumns.tsx
- sink/XmlIO.tsx
- StepExecutionSettingsField.tsx
- dependencies
- run-local-template.test.ts
- CachePersist.tsx
- RenameDropColumns.tsx
- Select.tsx
- lucide-react
- vite.config.ts
- documentation/index.ts
- source/CsvIO.tsx
- Explode.tsx
- ForeachBatchSink.tsx
- HudiSink.tsx
- source/ParquetIO.tsx
- SetOperation.tsx
- scripts-and-config.test.ts
- scripts
- AlloyDbSink.tsx
- sink/CloudSQL.tsx
- DeltaSink.tsx
- sink/HCatalogIO.tsx
- PandasUdfTransform.tsx
- sink/PulsarIO.tsx
- source/PulsarIO.tsx
- RepartitionCoalesce.tsx
- UnionByName.tsx
- sink/IcebergIO.test.tsx
- tsconfig.integration.json
- LimitSample.tsx
- sink/MongoDbIO.tsx
- sink/Neo4jIO.tsx
- sink/SingleStoreDBIO.tsx
- sink/SpannerIO.tsx
- consoleUrl.ts
- ExecutionTerminal.tsx
- SecretManagerSecretField.tsx
- env.d.ts
- App.test.tsx
- AlloyDbSource.tsx
- sink/CassandraIO.tsx
- CreateIO.tsx
- DeltaSource.tsx
- HudiSource.tsx
- source/Neo4jIO.tsx
- source/JdbcIO.test.tsx
- python/run_sdp_dataproc.sh
- pyspark/run_sdp_dataproc.sh
- LanguageStrategy
- security-wiring.test.ts
- sink/ClickHouseIO.tsx
- sink/ElasticsearchIO.tsx
- sink/RedisIO.tsx
- source/RedisIO.tsx
- StoragePathField.tsx
- LimitSampleProperties
- source/SpannerIO.tsx
- CanvasToolbar.test.tsx
- PulsarAuthFields.tsx
- RabbitMqConnectionFields.tsx
- RabbitMqDestinationFields.tsx
- cdfOptions.ts
- cdfValidators.test.ts
- SelectField.test.tsx
- source/SpannerIO.test.tsx
- setup.ts
- webview/tsconfig.json
- python/run_dataproc_cluster.sh
- python/run_serverless_spark.sh
- pyspark/run_dataproc_cluster.sh
- pyspark/run_serverless_spark.sh
- vitest.integration.config.mts

## God Nodes (most connected - your core abstractions)
1. `PropertyField()` - 233 edges
2. `SystemInstructionsField()` - 229 edges
3. `resolveSystemInstructions()` - 228 edges
4. `InfoTooltip()` - 226 edges
5. `react` - 189 edges
6. `@testing-library/react` - 166 edges
7. `SelectField()` - 153 edges
8. `@testing-library/jest-dom` - 150 edges
9. `lucide-react` - 99 edges
10. `SecretManagerAuthFields()` - 94 edges

## Surprising Connections (you probably didn't know these)
- `StepExecutionSettingsFieldProps` --references--> `PipelineParadigm`  [EXTRACTED]
  webview/src/components/common/StepExecutionSettingsField.tsx → src/shared/types.ts
- `SdpSinkInspectorNode` --references--> `PipelineParadigm`  [EXTRACTED]
  webview/src/components/common/SdpSinkSettingsField.tsx → src/shared/types.ts
- `SdpSinkSettingsFieldProps` --references--> `PipelineParadigm`  [EXTRACTED]
  webview/src/components/common/SdpSinkSettingsField.tsx → src/shared/types.ts
- `PropertyPaneProps` --references--> `PipelineParadigm`  [EXTRACTED]
  webview/src/components/PropertyPane.tsx → src/shared/types.ts
- `LogEntry` --references--> `LogStream`  [EXTRACTED]
  webview/src/components/BottomPanel/deployment/ExecutionTerminal.tsx → src/shared/types.ts

## Import Cycles
- None detected.

## Communities (189 total, 13 thin omitted)

### Community 0 - "ref_vitest"
Cohesion: 0.04
Nodes (36): @testing-library/jest-dom, @testing-library/react, CassandraSinkProperties, ClickHouseSinkProperties, ElasticsearchSinkProperties, EventHubsIOSinkProperties, FirestoreSinkProperties, HiveSinkProperties (+28 more)

### Community 1 - "extension.ts"
Cohesion: 0.08
Nodes (63): listAssets(), BigLakeCatalogItem, DataprocClusterItem, lastPathSegment(), mapBigLakeCatalogs(), mapBigtableInstances(), mapBigtableTables(), mapDataprocClusters() (+55 more)

### Community 2 - "shared/dagValidator.ts"
Cohesion: 0.06
Nodes (54): CODEGEN_RESOURCE_PATHS, clearCodegenSkillCatalogCache(), ALLOWED_CLOUD_CHECKPOINT_SCHEMES, ALLOWED_LOCAL_CHECKPOINT_SCHEMES, DagEdge, DagNode, DagValidationOptions, DagValidationResult (+46 more)

### Community 3 - "PipelineRunner.ts"
Cohesion: 0.06
Nodes (53): getDesignerOutputChannel(), FORCE_KILL_DELAY_MS, buildRunnerEnvironment(), cancelRemoteSparkJob(), CancelRemoteSparkJobOptions, CancelRemoteSparkJobResult, createJobIdTracker(), DEFAULT_RUN_REGION (+45 more)

### Community 4 - "PropertyField"
Cohesion: 0.12
Nodes (69): PropertyField(), PulsarAuthFields(), RabbitMqConnectionFields(), RabbitMqDestinationFields(), SecretManagerAuthFields(), SecretPicker(), SystemInstructionsField(), InfoTooltip() (+61 more)

### Community 5 - "codeGeneration.ts"
Cohesion: 0.06
Nodes (43): buildCodegenModelConfig(), PipelinePrompt, resolveBeamVersion, findLastCompleteFileEndOffset(), parseOrRepairJson(), runReviewerPass(), buildExtensionPrompt(), deduplicateGeneratedFiles() (+35 more)

### Community 6 - "pytest-runner.ts"
Cohesion: 0.06
Nodes (54): RunnerEnvironmentOptions, applyRuntimeVariables(), DENIED_ENV_KEYS, DENIED_ENV_PREFIXES, isAllowedEnvKey(), RuntimeVariable, buildChildEnv(), buildConftestSource() (+46 more)

### Community 7 - "App.tsx"
Cohesion: 0.07
Nodes (40): evaluateSdpCompatibility(), App(), AppWithProvider(), CanvasPipelineNode, LabelChangeContext, nodeTypes, PipelineSnapshot, getVsCodeApi() (+32 more)

### Community 8 - "core/constants.ts"
Cohesion: 0.07
Nodes (30): findGcloudPath(), getGcloudProject(), GENERATED_FILE_LIST_EXCLUDE_DIRS, getInitialPipelineContent(), PIPELINE_SEARCH_EXCLUDE_DIRS, PIPELINE_SEARCH_EXCLUDE_PATTERN, SPARK_EDITOR_VIEW_TYPE, isPlainTextSparkTab() (+22 more)

### Community 9 - "SparkEditorProvider.ts"
Cohesion: 0.06
Nodes (25): ExecuteCodeGenerationOptions, HandleRegenerateCodeOptions, createSafePostMessage(), DocumentSyncState, shouldPushDocumentUpdate(), EditorSession, signalProcessTree(), SparkEditorProvider (+17 more)

### Community 10 - "CodegenSkillCatalog.ts"
Cohesion: 0.06
Nodes (38): ALWAYS_ON_KINDS, CROSSCUTTING_KIND, KNOWN_PIPELINE_FEATURES, SKILL_KIND_ORDER, SKILL_RUNNERS, SkillKind, TRANSFORM_ALIAS_PREFIXES, BOOLEAN_VALUES (+30 more)

### Community 11 - "env"
Cohesion: 0.04
Nodes (48): name, permissions, runs-on, steps, concurrency, cancel-in-progress, group, default (+40 more)

### Community 12 - "DeploymentTab.tsx"
Cohesion: 0.09
Nodes (36): BOTTOM_PANEL_EXPANDED_HEIGHT, BottomPanel(), BottomPanelProps, BottomPanelContext, BottomPanelContextValue, BottomPanelExecutionSlice, BottomPanelJobConfigSlice, BottomPanelPipelineConfigSlice (+28 more)

### Community 13 - "source/AvroIO.tsx"
Cohesion: 0.09
Nodes (38): DEFAULT_OPTION_LABELS, DEFAULT_OPTIONS, MappingStrategy, MappingStrategyField(), MappingStrategyFieldProps, BEAM_SCHEMA_TYPES, BIGQUERY_TYPES, BigQueryField (+30 more)

### Community 14 - "ref_path"
Cohesion: 0.10
Nodes (22): PIPELINE_FILE_EXT, nextAvailableSparkFilename(), resolveOutputFolder(), savePipelineFiles(), computePipelineLocation(), PipelineLocation, promptForSparkSaveLocation(), handleOpenExternal() (+14 more)

### Community 15 - "codegenConfig.ts"
Cohesion: 0.09
Nodes (37): CODEGEN_COMMON_SKILL_NAME, CODEGEN_MODEL_SETTINGS, CODEGEN_MODEL_TUNING, CodegenModelTuning, CUSTOM_INSTRUCTION_KEYS, EDGE_HANDLES, LANGUAGE_RUNNERS, PIPELINE_FEATURES (+29 more)

### Community 16 - "SourceCodeTab.tsx"
Cohesion: 0.09
Nodes (32): CodeViewer(), CodeViewerProps, escapeRegExp(), FileTabBar(), FileTabBarProps, FileColorInfo, getFileColorInfo(), getFileDescription() (+24 more)

### Community 17 - "deterministic-gate.ts"
Cohesion: 0.10
Nodes (33): isSdpDryRunEnvironmentSkip(), scanSdpPythonViolations(), stripPythonCommentsAndDocstrings(), BANNED_PATTERN_RULES, checkBashSyntax(), checkDeliverableCompleteness(), checkJsonSyntax(), checkSdpStructureAndSpec() (+25 more)

### Community 18 - "shared/types.ts"
Cohesion: 0.07
Nodes (38): AssetListResponse, CodeGenerationProgressMessage, CodeGenerationStoppedMessage, CodeRegeneratedMessage, FileContentErrorMessage, FileContentLoadedMessage, FolderFilesReadMessage, FolderSelectedMessage (+30 more)

### Community 19 - "CodegenPromptBuilder.ts"
Cohesion: 0.09
Nodes (35): asRecord(), buildPromptFromPipelineYaml(), BuiltPrompt, CodegenPromptBuilder, computePipelineExecutionPlan(), computePipelineSdpPlan(), describeSdpDatasetDecorator(), fence() (+27 more)

### Community 20 - "spark/versionMatrix.ts"
Cohesion: 0.10
Nodes (36): BUILTIN_FILE_FORMAT_TRANSFORMS, cloneRuntimeProfile(), CloudStorageBackend, ConnectorDeliveryMechanism, ConnectorResolution, DEFAULT_SERVERLESS_TIER, detectCloudStorageBackend(), extractJobConfigJars() (+28 more)

### Community 21 - "StorageBackendFields.tsx"
Cohesion: 0.06
Nodes (34): BinaryFileSourceProperties, JsonIOSinkProperties, OrcSinkProperties, TextIOSourceProperties, AZURE_AUTH_OPTIONS, DEFAULT_PATH_PLACEHOLDERS, inferStorageBackend(), resolveStoragePathValue() (+26 more)

### Community 22 - "arbiter-evaluator.ts"
Cohesion: 0.10
Nodes (35): getCodegenSkillCatalog(), ARBITER_RESPONSE_SCHEMA, ArbiterPromptInput, buildArbiterPrompt(), calculateBaselineScorecard(), CODEGEN_SKILLS_REPO_DIR, defaultSkillsRoot(), DroppedSkillDirective (+27 more)

### Community 23 - "documentation/sources.ts"
Cohesion: 0.05
Nodes (40): ALLOYDB_SOURCE_DOC, AVRO_SOURCE_DOC, BIGQUERY_SOURCE_DOC, BIGTABLE_SOURCE_DOC, BINARY_FILE_SOURCE_DOC, CASSANDRA_SOURCE_DOC, CLICKHOUSE_SOURCE_DOC, CLOUDSQL_SOURCE_DOC (+32 more)

### Community 24 - "useAssetLoader.ts"
Cohesion: 0.08
Nodes (24): AssetResponseCommand, HostToWebviewCommand, WebviewToHostCommand, WebviewToHostMessage, postToHost(), resetVsCodeApiForTesting(), VsCodeApi, Window (+16 more)

### Community 25 - "ref_os"
Cohesion: 0.11
Nodes (25): disposeDesignerOutputChannel(), tryGetDesignerOutputChannel(), executeScopedCleanup(), gateOutputFolder(), pruneEmptyDirectories(), readGenerationManifest(), resolvePipelineId(), writeGenerationManifest() (+17 more)

### Community 26 - "documentation/sinks.ts"
Cohesion: 0.05
Nodes (38): ALLOYDB_SINK_DOC, AVRO_SINK_DOC, BIGQUERY_SINK_DOC, BIGTABLE_SINK_DOC, CASSANDRA_SINK_DOC, CLICKHOUSE_SINK_DOC, CLOUDSQL_SINK_DOC, CONSOLE_SINK_DOC (+30 more)

### Community 27 - "shared/sdpPlanner.ts"
Cohesion: 0.11
Nodes (32): allocateUniqueDatasetName(), asRecord(), badgeForDatasetKind(), BATCH_MATERIALIZED_VIEW_STEPS, buildSdpPlan(), checkNodeSdpCompatibility(), EXTERNAL_SINK_FORMAT_BY_TRANSFORM, extractTableLeaf() (+24 more)

### Community 28 - "spark-architecture-evaluator.ts"
Cohesion: 0.13
Nodes (31): buildSparkArchitecturePrompt(), collectSparkArchitectureSignals(), countMatches(), evaluateSparkArchitecture(), extractPythonFunctionBody(), findLines(), getPipelineSourceFiles(), hasBlockingFinding() (+23 more)

### Community 29 - "credentialRedaction.ts"
Cohesion: 0.09
Nodes (22): classifyKey(), CREDENTIAL_NAME_TOKENS, hasCredentialToken(), KeyClassification, normalizeKey(), redactCredentialValues(), RedactedPipelineDocument, REDACTION_PLACEHOLDER (+14 more)

### Community 30 - "shared/executionPlanner.ts"
Cohesion: 0.11
Nodes (31): alignStageTopologicalOrder(), asRecord(), buildBatchCachePlan(), buildConditionalSplits(), buildDlqSplitPoints(), buildExecutionPlan(), buildStreamingSinkGroups(), CacheReason (+23 more)

### Community 31 - "shared/sparkDagRules.ts"
Cohesion: 0.16
Nodes (31): NodeExecutionPlan, computeStreamingNodeSet(), ConditionalSplitHandle, getConditionalSplitHandles(), getEffectiveSourceReadMode(), getNodeCategory(), getNodeInputArity(), getNodeSparkTransform() (+23 more)

### Community 32 - "webview/package.json"
Cohesion: 0.08
Nodes (32): eslint@^8.28.0, eslint-plugin-jsdoc@^48.11.0, eslint, eslint-plugin-jsdoc, eslint-plugin-react-hooks, eslint-plugin-react-refresh, jsdom, @types/react (+24 more)

### Community 33 - "scripts"
Cohesion: 0.06
Nodes (33): scripts, build, build:webview, compile, lint, package, pretest, test (+25 more)

### Community 34 - "CodeReviewer.ts"
Cohesion: 0.12
Nodes (25): CODEGEN_REVIEWER_MAX_FIX_ROUNDS, REVIEW_SEVERITIES, abortError(), buildFixPrompt(), buildReviewPrompt(), CodeReviewerDeps, isAbort(), mergeFiles() (+17 more)

### Community 35 - "codegen-runner.ts"
Cohesion: 0.11
Nodes (24): buildRunnerPrompt(), createGeminiClient(), deduplicateGeneratedFiles(), evaluateGeneratedCode(), EvaluationResult, GeneratedFile, getGcloudProject(), getResourcesRoot() (+16 more)

### Community 36 - "package.json"
Cohesion: 0.07
Nodes (31): activationEvents, bugs, url, capabilities, untrustedWorkspaces, categories, dependencies, @google/genai@^2.2.0 (+23 more)

### Community 37 - "usePipelineGraph.ts"
Cohesion: 0.15
Nodes (24): reactflow, COL_WIDTH, computeLayoutedNodes(), estimateNodeWidth(), ROW_HEIGHT, START_X, START_Y, CreatedPipelineElements (+16 more)

### Community 38 - "AdvancedConfig.tsx"
Cohesion: 0.14
Nodes (27): AdvancedConfig(), AdvancedConfigProps, COMMON_GCP_REGIONS, STREAMING_TRIGGER_OPTIONS, SubnetworkOption, triggerUsesInterval(), validateDepsBucketUri(), SdpSettingsPanel() (+19 more)

### Community 39 - "SecretManagerAuthFields.tsx"
Cohesion: 0.09
Nodes (24): CassandraIOSourceProperties, FirestoreIOSinkProperties, MongoDbIOSourceProperties, SnowflakeIOSinkProperties, SnowflakeIOSourceProperties, AuthModeOption, DEFAULT_AUTH_OPTIONS, SecretItem (+16 more)

### Community 40 - "propertyFormRegistry.ts"
Cohesion: 0.08
Nodes (21): AvroIOSourceProperties, BigQuerySourceProperties, ClickHouseIOSourceProperties, FirestoreIOSourceProperties, FORM_LOADERS, JsonIOSourceProperties, MemorySinkProperties, OrcSourceProperties (+13 more)

### Community 41 - "contributes"
Cohesion: 0.07
Nodes (28): properties, title, workbench.editorAssociations, contributes, commands, configuration, configurationDefaults, customEditors (+20 more)

### Community 42 - "packaging-evaluator.ts"
Cohesion: 0.18
Nodes (22): isSdpGeneratedProject(), auditConnectorArtifacts(), auditRequirementsTxt(), auditRunnerScripts(), auditSdpRunnerScriptsAndStageYaml(), classifyPysparkSpecifier(), extractServerlessRuntimeVersions(), normalizePipName() (+14 more)

### Community 43 - "python/run_local.sh"
Cohesion: 0.14
Nodes (23): bootstrap_java(), bootstrap_python(), CHECKPOINT_ROOT, die(), find_supported_jdk(), install_temurin_jdk(), is_supported_jdk(), jdk_major() (+15 more)

### Community 44 - "pyspark/run_local.sh"
Cohesion: 0.14
Nodes (23): bootstrap_java(), bootstrap_python(), CHECKPOINT_ROOT, die(), find_supported_jdk(), install_temurin_jdk(), is_supported_jdk(), jdk_major() (+15 more)

### Community 45 - "codegen-skills-invariants.test.ts"
Cohesion: 0.10
Nodes (21): ALL_LANGUAGE_RUNNERS, ALL_SKILL_LANGUAGES, BEAM_VERSION_PLACEHOLDER, SKILL_WILDCARD, SPARK_VERSION_PLACEHOLDER, TRANSFORM_OWNER_KINDS, ALLOWED_PLACEHOLDERS, CATALOG (+13 more)

### Community 46 - "ReadOnlyCheckRunner.ts"
Cohesion: 0.11
Nodes (21): LANGUAGE_BUILD_FILES, MODEL_FACING_PLACEHOLDERS, READ_ONLY_CHECK_LIMITS, ALLOWED_SDP_SPEC_KEYS, CHECK_IDS, CheckId, CheckStatus, CommandExecutor (+13 more)

### Community 47 - "vscodeMock.ts"
Cohesion: 0.09
Nodes (16): CONTEXT, REPO_ROOT, YAML_TEXT, createVscodeMock(), deepMerge(), MockCancellationTokenSource, MockEventEmitter, MockMarkdownString (+8 more)

### Community 48 - "react"
Cohesion: 0.11
Nodes (16): react, BigTableIOSourceProperties, ConsoleSinkProperties, ElasticsearchIOSourceProperties, ParseMessageProperties, RedshiftSourceProperties, InfoTooltipProps, CONSOLE_LOG_LEVEL_OPTIONS (+8 more)

### Community 49 - "SamplePipeline"
Cohesion: 0.19
Nodes (15): SamplesLoadedMessage, SamplePipeline, DEFAULT_SAMPLES, MarkdownViewer(), MarkdownViewerProps, renderInlineMarkdown(), NodesCardProps, PaletteTabBarProps (+7 more)

### Community 50 - "sink/MqttIO.tsx"
Cohesion: 0.11
Nodes (18): MqttIOSinkProperties, MqttIOSourceProperties, ParsePayloadProperties, MQTT_PAYLOAD_FORMAT_OPTIONS, MqttIOSinkNode, MqttIOSinkPropertiesProps, MQTT_QOS_OPTIONS, MqttIOSourceNode (+10 more)

### Community 51 - "pipeline-spec-evaluator.ts"
Cohesion: 0.20
Nodes (16): auditExecutionPlanCompliance(), auditSdpPlanCompliance(), BEAM_PRIMITIVE_TRANSFORMS, buildSpecExecutionPlan(), buildSpecSdpExecutionPlan(), containsQuoted(), evaluatePipelineSpecification(), formatSpecChecklist() (+8 more)

### Community 52 - "WindowOver.tsx"
Cohesion: 0.11
Nodes (20): WindowOverProperties, DEFAULT_METRICS, FNS_REQUIRING_SOURCE_COLUMN, FNS_WITH_DEFAULT_VALUE, FNS_WITH_OFFSET, FRAME_BOUNDARY_PRESETS, FRAME_TYPE_OPTIONS, normalizeOrderByColumns() (+12 more)

### Community 53 - "devDependencies"
Cohesion: 0.11
Nodes (19): devDependencies, @eslint/js@^8.57.1, globals@^15.15.0, @types/node@^20.17.0, @types/vscode@^1.74.0, typescript@^5.3.3, vitest@^4.1.11, @vscode/vsce@^3.2.1 (+11 more)

### Community 54 - "multi-agent-evaluator.test.ts"
Cohesion: 0.23
Nodes (11): @google/genai, DEFAULT_GEMINI_MODEL, GeneratedFileCandidate, formatFilesWithLineNumbers(), formattedFilesCache, runMultiAgentEvaluation(), evaluatePackagingAndDeployability(), evaluateSecurityAndHardening() (+3 more)

### Community 55 - "ReadOnlyCheckRunner"
Cohesion: 0.27
Nodes (6): findMainPipelineFile(), findSdpTestTransformsFile(), findTestPipelineFile(), normalizeRelPath(), ReadOnlyCheckRunner, truncateCheckOutput()

### Community 56 - "compilerOptions"
Cohesion: 0.11
Nodes (18): compilerOptions, esModuleInterop, lib, module, noFallthroughCasesInSwitch, noImplicitReturns, noUnusedLocals, noUnusedParameters (+10 more)

### Community 57 - "SystemInstructionsField.tsx"
Cohesion: 0.11
Nodes (15): CloudSQLSourceProperties, CustomTransformProperties, RedshiftSinkProperties, SystemInstructionsFieldProps, RedshiftDistStyle, RedshiftSaveMode, RedshiftSinkPropertiesProps, RedshiftTempFormat (+7 more)

### Community 58 - "ThemeContext.tsx"
Cohesion: 0.13
Nodes (11): AppTheme, STORAGE_THEME_KEY, LocalStorageMock, mockStorage, TestComponent(), THEME_OPTIONS, ThemeContext, ThemeContextType (+3 more)

### Community 59 - "compilerOptions"
Cohesion: 0.11
Nodes (18): compilerOptions, allowImportingTsExtensions, erasableSyntaxOnly, jsx, lib, module, moduleDetection, moduleResolution (+10 more)

### Community 60 - "PipelineParadigm"
Cohesion: 0.16
Nodes (14): RunnerJobConfig, PipelineParadigm, DeterministicGateOptions, ParsedPipelineSpec, SparkArchitectureCheckOptions, SparkArchitectureSignals, CanvasToolbarProps, StreamingSinkSettingsFieldProps (+6 more)

### Community 61 - "test-reporter.ts"
Cohesion: 0.29
Nodes (15): EvaluationScorecard, PromptRemediationDirective, setup(), clearReportCache(), ConsolidatedDirective, consolidateDirectives(), detectDirectiveTopic(), generateReportMarkdown() (+7 more)

### Community 62 - "MergeCdcScd.tsx"
Cohesion: 0.17
Nodes (16): MergeCdcScdProperties, CDC_FORMAT_OPTIONS, CdcFormat, MergeCdcScdNode, MergeCdcScdProperties(), MergeCdcScdPropertiesProps, MergeCdcScdStepProperties, normalizeCdcFormat() (+8 more)

### Community 63 - "NestedStructArrayOps.tsx"
Cohesion: 0.14
Nodes (17): NestedStructArrayOpsProperties, DEFAULT_ARRAY_RULES, HigherOrderArrayFunction, HigherOrderArrayRule, HOF_FUNCTION_OPTIONS, NestedOperationMode, NestedStructArrayOpsNode, NestedStructArrayOpsProperties() (+9 more)

### Community 64 - "panelBuildingBlocks.test.tsx"
Cohesion: 0.14
Nodes (14): EXTERNAL_DOCS_LINK_CLASS, NormalizedSecret, SECRET_PICKER_HELP_DISCOVERED, SECRET_PICKER_HELP_EMPTY, SecretPickerProps, mockPostMessage, PickerHarness(), FieldChangeEvent (+6 more)

### Community 65 - "StorageBackendFields"
Cohesion: 0.15
Nodes (14): StorageBackendFields(), BinaryFileSourceProperties(), JSON_PARSE_MODES, JsonIOSourceNode, JsonIOSourceProperties(), JsonIOSourcePropertiesProps, JsonSourceProperties, READ_MODE_OPTIONS (+6 more)

### Community 66 - "compilerOptions"
Cohesion: 0.11
Nodes (17): compilerOptions, allowImportingTsExtensions, erasableSyntaxOnly, lib, module, moduleDetection, moduleResolution, noEmit (+9 more)

### Community 67 - "eslint.config.js"
Cohesion: 0.13
Nodes (9): globals, js, jsdocPlugin, tsParser, tsPlugin, @typescript-eslint/eslint-plugin@^5.45.0, @typescript-eslint/parser@^5.45.0, @typescript-eslint/eslint-plugin (+1 more)

### Community 68 - "python/run_sdp_local.sh"
Cohesion: 0.26
Nodes (16): bootstrap_java(), bootstrap_python(), check_local_iceberg_runtime(), die(), find_supported_jdk(), install_temurin_jdk(), is_supported_jdk(), jdk_major() (+8 more)

### Community 69 - "pyspark/run_sdp_local.sh"
Cohesion: 0.26
Nodes (16): bootstrap_java(), bootstrap_python(), check_local_iceberg_runtime(), die(), find_supported_jdk(), install_temurin_jdk(), is_supported_jdk(), jdk_major() (+8 more)

### Community 70 - "propertyFormRegistry.test.ts"
Cohesion: 0.26
Nodes (10): StreamingSinkGroup, InputArity, StreamingSinkMode, SINK_NODES, SOURCE_NODES, STEP_NODES, NodeDefinition, CATALOG_BY_TYPE (+2 more)

### Community 71 - "HandleNulls.tsx"
Cohesion: 0.18
Nodes (15): HandleNullsProperties, DROP_HOW_OPTIONS, HandleNullsDropHow, HandleNullsFillMapping, HandleNullsNode, HandleNullsProperties(), HandleNullsPropertiesProps, HandleNullsReplaceMapping (+7 more)

### Community 72 - "sink/JdbcIO.tsx"
Cohesion: 0.15
Nodes (14): JdbcIOSinkProperties, JdbcIOSourceProperties, inferEngineFromDriver(), JDBC_SINK_AUTH_OPTIONS, JdbcIOSinkPropertiesProps, JdbcIsolationLevel, JdbcSaveMode, getJdbcUrlExample() (+6 more)

### Community 73 - "reviewerPass.ts"
Cohesion: 0.17
Nodes (11): CODEGEN_REVIEWER_ENABLED_BY_DEFAULT, CODEGEN_REVIEWER_SETTING, SKILL_CALLERS, SkillCaller, PipelinePromptRequest, PromptBuildInput, REVIEW_REPORT_SCHEMA, isReviewerEnabled() (+3 more)

### Community 74 - "systemInstructions.ts"
Cohesion: 0.12
Nodes (11): AggregationProperties, BigTableIOSinkProperties, PubSubProperties, BigTableIOSinkPropertiesProps, normalizeAuthMode(), PUBSUB_READ_MODE_OPTIONS, PubSubPropertiesProps, AggregationNode (+3 more)

### Community 75 - "DataQualityAssert.tsx"
Cohesion: 0.17
Nodes (14): DataQualityAssertProperties, CHECK_TYPE_OPTIONS, DataQualityAssertNode, DataQualityAssertProperties(), DataQualityAssertPropertiesProps, DataQualityAssertStepProperties, DataQualityCheckType, DataQualityExpectation (+6 more)

### Community 76 - "GroupByAgg.tsx"
Cohesion: 0.17
Nodes (14): GroupByAggProperties, AGG_FUNCTION_OPTIONS, DEFAULT_AGGREGATIONS, GroupByAggNode, GroupByAggProperties(), GroupByAggPropertiesProps, GroupByAggregationSpec, GroupByAggStepProperties (+6 more)

### Community 77 - "Join.tsx"
Cohesion: 0.17
Nodes (14): JoinProperties, CONDITION_MODE_OPTIONS, DEFAULT_JOIN_KEYS, JOIN_TYPE_OPTIONS, JoinConditionMode, JoinKeyPair, JoinNode, JoinProperties() (+6 more)

### Community 78 - "StatefulTransform.tsx"
Cohesion: 0.12
Nodes (15): StatefulTransformProperties, API_MODE_OPTIONS, normalizeKeyColumns(), OUTPUT_MODE_OPTIONS, STATE_PRIMITIVE_OPTIONS, StatefulApiMode, StatefulOutputMode, StatefulPrimitiveType (+7 more)

### Community 79 - "TextChunkAndEmbed.tsx"
Cohesion: 0.15
Nodes (15): TextChunkAndEmbedProperties, CHUNK_STRATEGY_OPTIONS, EMBEDDING_MODEL_SUGGESTIONS, EmbeddingTaskType, MODE_OPTIONS, resolveChunkStrategy(), resolveMode(), resolveTaskType() (+7 more)

### Community 80 - "TopN.tsx"
Cohesion: 0.17
Nodes (14): TopNProperties, DEFAULT_ORDER_BY, DIRECTION_OPTIONS, normalizeOrderByColumns(), normalizePartitionColumns(), RANKING_FUNCTION_OPTIONS, TopNNode, TopNOrderBySpec (+6 more)

### Community 81 - "VectorStoreSink.tsx"
Cohesion: 0.12
Nodes (15): VectorStoreSinkProperties, ALLOYDB_AUTH_OPTIONS, AlloyDbVectorIpType, BACKEND_OPTIONS, CONFLICT_ACTION_OPTIONS, formatMetadataColumnsText(), IP_TYPE_OPTIONS, UpsertConflictAction (+7 more)

### Community 82 - "PropertyPane.tsx"
Cohesion: 0.17
Nodes (9): ErrorBoundary, ErrorBoundaryProps, ErrorBoundaryState, getNodeIcon(), NODE_ICON_COMPONENTS, PropertyForm(), PropertyFormLoading(), PropertyPane() (+1 more)

### Community 83 - "nodeDocumentation.ts"
Cohesion: 0.23
Nodes (11): DEFAULT_BEAM_IO_DOCS_URL, DEFAULT_BEAM_TRANSFORMS_DOCS_URL, ExternalDocsLink(), ExternalDocsLinkProps, NodeDocsCard(), NodeDocsCardProps, getNodeDocumentation(), hasInlineNodeDocsCard() (+3 more)

### Community 84 - "ConditionalSplit.tsx"
Cohesion: 0.20
Nodes (13): ConditionalSplitProperties, buildNextUniqueRuleName(), ConditionalSplitEvaluationMode, ConditionalSplitNode, ConditionalSplitProperties(), ConditionalSplitPropertiesProps, ConditionalSplitRule, ConditionalSplitRuleValidation (+5 more)

### Community 85 - "EnforceSchemaAndCast.tsx"
Cohesion: 0.16
Nodes (14): EnforceSchemaAndCastProperties, CAST_MODE_OPTIONS, DRIFT_POLICY_OPTIONS, EnforceSchemaAndCastNode, EnforceSchemaAndCastProperties(), EnforceSchemaAndCastPropertiesProps, EnforceSchemaAndCastStepProperties, normalizeCastMode() (+6 more)

### Community 86 - "Filter.tsx"
Cohesion: 0.18
Nodes (13): FilterProperties, buildSqlFilterSystemInstruction(), FILTER_MODE_OPTIONS, FILTER_OPERATOR_OPTIONS, FilterMode, FilterNode, FilterOperator, FilterProperties() (+5 more)

### Community 87 - "PropertyField.tsx"
Cohesion: 0.17
Nodes (11): IcebergIOSinkProperties, IcebergIOSourceProperties, PropertyFieldOption, PropertyFieldProps, IcebergIOSinkNode, IcebergIOSinkPropertiesProps, IcebergWriteOperation, BigLakeCatalogOption (+3 more)

### Community 88 - "sink/KafkaIO.tsx"
Cohesion: 0.15
Nodes (13): KafkaIOSinkProperties, KafkaIOSourceProperties, KAFKA_ACKS_OPTIONS, KAFKA_COMPRESSION_OPTIONS, KAFKA_VALUE_FORMAT_OPTIONS, KafkaIOSinkNode, KafkaIOSinkPropertiesProps, KAFKA_READ_MODE_OPTIONS (+5 more)

### Community 89 - "SelectField"
Cohesion: 0.30
Nodes (15): CollapsibleSection(), SelectField(), StoragePathField(), useAssetLoader(), DeltaSinkProperties(), HCatalogIOSinkProperties(), HudiSinkProperties(), IcebergIOSinkProperties() (+7 more)

### Community 90 - "RestApiSource.tsx"
Cohesion: 0.14
Nodes (13): RFC-5988, RestApiSourceProperties, AUTH_SCHEME_OPTIONS, HTTP_METHOD_OPTIONS, PAGINATION_TYPE_OPTIONS, REST_AUTH_MODE_OPTIONS, RestApiAuthScheme, RestApiHttpMethod (+5 more)

### Community 91 - "gcloudClient.ts"
Cohesion: 0.21
Nodes (8): findExecutableInPath(), resolveCli(), GCLOUD_PROJECT_CACHE_TTL_MS, getGcloudProjectCached(), resetGcloudProjectCache(), handleReady(), ExecCallback, execCalls

### Community 92 - "PropertyForm.test.tsx"
Cohesion: 0.22
Nodes (7): loadPropertyForm(), PROPERTY_FORM_REGISTRY, PropertyFormComponent, PropertyFormProps, getPropertyFormComponent(), PropertyFormContainerProps, resolvePropertyForm()

### Community 93 - "Deduplicate.tsx"
Cohesion: 0.20
Nodes (12): DeduplicateProperties, DEDUPLICATE_STRATEGY_OPTIONS, DeduplicateNode, DeduplicateOrderDirection, DeduplicateProperties(), DeduplicatePropertiesProps, DeduplicateStepProperties, DeduplicateStrategy (+4 more)

### Community 94 - "sink/KinesisIO.tsx"
Cohesion: 0.15
Nodes (12): KinesisIOSinkProperties, KinesisIOSourceProperties, KINESIS_PARTITION_KEY_MODE_OPTIONS, KINESIS_PAYLOAD_FORMAT_OPTIONS, KinesisIOSinkNode, KinesisIOSinkPropertiesProps, AWS_REGION_OPTIONS, AWS_REGION_PRESETS (+4 more)

### Community 95 - "OrderBy.tsx"
Cohesion: 0.18
Nodes (12): OrderByProperties, DEFAULT_SORT_COLUMNS, DIRECTION_OPTIONS, normalizeSortColumns(), NULLS_ORDER_OPTIONS, OrderByNode, OrderByProperties(), OrderByPropertiesProps (+4 more)

### Community 96 - "PivotUnpivot.tsx"
Cohesion: 0.18
Nodes (13): PivotUnpivotProperties, normalizeStringList(), OPERATION_OPTIONS, PIVOT_AGG_OPTIONS, PivotAggFunction, PivotUnpivotNode, PivotUnpivotOperation, PivotUnpivotProperties() (+5 more)

### Community 97 - "RollupCube.tsx"
Cohesion: 0.16
Nodes (13): RollupCubeProperties, AGG_FUNCTION_OPTIONS, DEFAULT_AGGREGATIONS, MODE_OPTIONS, normalizeAggregations(), normalizeDimensionColumns(), RollupCubeAggregationSpec, RollupCubeMode (+5 more)

### Community 98 - "SdpSinkSettingsField.tsx"
Cohesion: 0.20
Nodes (13): CATALOG_FORMAT_OPTIONS, formatColumnList(), getDefaultSdpFormat(), getDefaultSinkTargetKind(), LAKEHOUSE_CATALOG_SINKS, LAYOUT_MODE_OPTIONS, resolveLayoutMode(), SdpSinkInspectorNode (+5 more)

### Community 99 - "legacyNodeMigration.ts"
Cohesion: 0.22
Nodes (10): inferBeamTransform(), KNOWN_AGGREGATIONS, LEGACY_SINK_TRANSFORM_MAP, LEGACY_SOURCE_TRANSFORM_MAP, LEGACY_STEP_TRANSFORM_MAP, LegacyNodeData, migrateLegacyNodes(), resolveCanonicalSinkTransform() (+2 more)

### Community 100 - "EventHubsSink.tsx"
Cohesion: 0.17
Nodes (11): EventHubsSinkProperties, EventHubsSourceProperties, EVENTHUBS_ACKS_OPTIONS, EVENTHUBS_VALUE_FORMAT_OPTIONS, EventHubsSinkNode, EventHubsSinkPropertiesProps, EVENTHUBS_CONNECTOR_MODE_OPTIONS, EVENTHUBS_READ_MODE_OPTIONS (+3 more)

### Community 101 - "SelectField.tsx"
Cohesion: 0.18
Nodes (10): PubSubSinkProperties, RateSourceProperties, SelectFieldOption, SelectFieldProps, PUBSUB_PAYLOAD_FORMAT_OPTIONS, PubSubSinkNode, PubSubSinkPropertiesProps, RATE_FORMAT_OPTIONS (+2 more)

### Community 102 - "sink/TFRecordIO.tsx"
Cohesion: 0.21
Nodes (11): TFRecordIOSinkProperties, normalizeTFRecordCompression(), resolvePartitionByValue(), TFRECORD_COMPRESSION_CODECS, TFRECORD_SAVE_MODES, TFRECORD_WRITE_LOCALITY_MODES, TFRECORD_WRITE_RECORD_TYPES, TFRecordIOSinkNode (+3 more)

### Community 103 - "sink/AvroIO.tsx"
Cohesion: 0.24
Nodes (10): AvroIOSinkProperties, AVRO_COMPRESSION_CODECS, AVRO_SAVE_MODES, AvroIOSinkNode, AvroIOSinkProperties(), AvroIOSinkPropertiesProps, AvroSinkProperties, normalizeAvroCompression() (+2 more)

### Community 104 - "sink/BigQueryIO.tsx"
Cohesion: 0.23
Nodes (11): BigQuerySinkProperties, BigQueryCreateDisposition, BigQueryPartitionType, BigQuerySaveMode, BigQuerySinkProperties(), BigQuerySinkPropertiesProps, BigQueryWriteMethod, normalizeCreateDisposition() (+3 more)

### Community 105 - "sink/CsvIO.tsx"
Cohesion: 0.24
Nodes (10): CsvIOSinkProperties, CSV_COMPRESSION_CODECS, CSV_SAVE_MODES, CsvIOSinkNode, CsvIOSinkProperties(), CsvIOSinkPropertiesProps, CsvSinkProperties, normalizeCsvCompression() (+2 more)

### Community 106 - "FeaturePrep.tsx"
Cohesion: 0.17
Nodes (11): FeaturePrepProperties, FeatureHandleInvalid, FeatureImputeStrategy, FeaturePrepNode, FeaturePrepPropertiesProps, FeaturePrepStepProperties, FeatureScalerType, HANDLE_INVALID_OPTIONS (+3 more)

### Community 107 - "sink/ParquetIO.tsx"
Cohesion: 0.23
Nodes (10): ParquetIOSinkProperties, normalizeParquetCompression(), PARQUET_COMPRESSION_CODECS, PARQUET_WRITE_REBASE_MODES, ParquetIOSinkNode, ParquetIOSinkProperties(), ParquetIOSinkPropertiesProps, ParquetSinkProperties (+2 more)

### Community 108 - "SparkMlAndVertexAi.tsx"
Cohesion: 0.20
Nodes (11): SparkMlAndVertexAiProperties, ENGINE_OPTIONS, MlInferenceEngine, normalizeMlInputColumns(), resolveEngine(), SparkMlAndVertexAiNode, SparkMlAndVertexAiProperties(), SparkMlAndVertexAiPropertiesProps (+3 more)

### Community 109 - "SparkSqlQuery.tsx"
Cohesion: 0.21
Nodes (11): SparkSqlQueryProperties, DEFAULT_VIEW_ALIASES, InputViewAliasMapping, isValidIdentifier(), normalizeInputViewAliases(), normalizeSqlParameters(), SparkSqlQueryNode, SparkSqlQueryProperties() (+3 more)

### Community 110 - "sink/TextIO.tsx"
Cohesion: 0.23
Nodes (10): TextIOSinkProperties, normalizeTextCompression(), resolvePartitionByValue(), TEXT_COMPRESSION_CODECS, TEXT_SAVE_MODES, TEXT_SERIALIZATION_STRATEGIES, TextIOSinkNode, TextIOSinkProperties() (+2 more)

### Community 111 - "jobIdDetector.ts"
Cohesion: 0.33
Nodes (9): CONSOLE_SUBPAGE_SEGMENTS, detectClusterJobId(), detectDataflowJob(), DetectedDataflowJob, DetectedSparkJob, detectServerlessBatchId(), detectSparkJob(), parseConsoleQueryParams() (+1 more)

### Community 112 - "NodesCard.tsx"
Cohesion: 0.31
Nodes (8): defaultGroupOpenState, getPersistedGroupState(), GROUPS_STORAGE_KEY, persistGroupState(), VsCodeStateApi, NodePaletteTab, NodesCard(), PaletteTabBar()

### Community 113 - "SftpSource.tsx"
Cohesion: 0.18
Nodes (10): SftpSourceProperties, AUTH_TYPE_OPTIONS, FILE_FORMAT_OPTIONS, SFTP_AUTH_MODE_OPTIONS, SftpAuthType, SftpFileFormat, SftpSourceNode, SftpSourcePropertiesProps (+2 more)

### Community 114 - "Watermark.tsx"
Cohesion: 0.22
Nodes (9): WatermarkProperties, buildWatermarkSystemInstruction(), SOURCE_TIMESTAMP_TYPE_OPTIONS, VALID_SOURCE_TS_TYPES, WATERMARK_DELAY_PRESETS, WatermarkNode, WatermarkPropertiesProps, WatermarkSourceTimestampType (+1 more)

### Community 115 - "WithColumns.tsx"
Cohesion: 0.24
Nodes (9): WithColumnsProperties, DEFAULT_WITH_COLUMNS, normalizeWithColumns(), SPARK_CAST_TYPE_OPTIONS, WithColumnsNode, WithColumnSpec, WithColumnsProperties(), WithColumnsPropertiesProps (+1 more)

### Community 116 - "sink/XmlIO.tsx"
Cohesion: 0.25
Nodes (9): XmlIOSinkProperties, normalizeXmlCompression(), resolvePartitionByValue(), XML_COMPRESSION_CODECS, XML_SAVE_MODES, XmlIOSinkNode, XmlIOSinkProperties(), XmlIOSinkPropertiesProps (+1 more)

### Community 117 - "StepExecutionSettingsField.tsx"
Cohesion: 0.33
Nodes (9): CACHE_OUTPUT_OPTIONS, getSupportedReadModes(), normalizeCacheOutputSelection(), READ_MODE_OPTIONS, SDP_STEP_DATASET_OPTIONS, shouldShowSourceReadModeToggle(), shouldShowStepCacheToggle(), StepExecutionSettingsField() (+1 more)

### Community 118 - "dependencies"
Cohesion: 0.20
Nodes (10): react-dom, tailwindcss, dependencies, lucide-react@^1.15.0, react@^19.2.5, react-dom@^19.2.5, reactflow@^11.11.4, tailwindcss@^4.2.4 (+2 more)

### Community 119 - "run-local-template.test.ts"
Cohesion: 0.22
Nodes (6): fakeJdk(), PYSPARK_TEMPLATES, RESOURCES, SKILLS_ROOT, TEMPLATE, writeStub()

### Community 120 - "CachePersist.tsx"
Cohesion: 0.20
Nodes (9): CachePersistProperties, CachePersistMode, CachePersistNode, CachePersistPropertiesProps, CachePersistStepProperties, MODE_OPTIONS, SparkStorageLevel, STORAGE_LEVEL_OPTIONS (+1 more)

### Community 121 - "RenameDropColumns.tsx"
Cohesion: 0.31
Nodes (8): RenameDropColumnsProperties, normalizeDropColumns(), normalizeRenameMappings(), RenameColumnMapping, RenameDropColumnsNode, RenameDropColumnsProperties(), RenameDropColumnsPropertiesProps, RenameDropColumnsStepProperties

### Community 122 - "Select.tsx"
Cohesion: 0.20
Nodes (9): SelectProperties, DEFAULT_SELECT_COLUMNS, normalizeSelectColumns(), SELECT_MODE_OPTIONS, SelectColumnSpec, SelectMode, SelectNode, SelectPropertiesProps (+1 more)

### Community 123 - "lucide-react"
Cohesion: 0.25
Nodes (6): lucide-react, HCatalogIOSourceProperties, CollapsibleSectionProps, PulsarConnectionFields(), PulsarConnectionFieldsProps, HCatalogIOSourcePropertiesProps

### Community 124 - "vite.config.ts"
Cohesion: 0.22
Nodes (5): @tailwindcss/vite, @vitejs/plugin-react, @tailwindcss/vite@^4.2.4, @vitejs/plugin-react@^6.0.1, HOST_REWRITTEN_ASSETS

### Community 125 - "documentation/index.ts"
Cohesion: 0.44
Nodes (5): NODE_DOCUMENTATION_REGISTRY, SINK_DOCS, SOURCE_DOCS, TRANSFORM_DOCS, NodeDocMetadata

### Community 126 - "source/CsvIO.tsx"
Cohesion: 0.28
Nodes (7): CsvIOSourceProperties, CSV_PARSE_MODES, CsvIOSourceNode, CsvIOSourceProperties(), CsvIOSourcePropertiesProps, CsvSourceProperties, READ_MODE_OPTIONS

### Community 127 - "Explode.tsx"
Cohesion: 0.22
Nodes (8): ExplodeProperties, EXPLODE_FUNCTION_OPTIONS, ExplodeFunction, ExplodeNode, ExplodePropertiesProps, ExplodeStepProperties, normalizeExplodeFunction(), VALID_EXPLODE_FUNCTIONS

### Community 128 - "ForeachBatchSink.tsx"
Cohesion: 0.22
Nodes (8): ForeachBatchSinkProperties, FOREACH_BATCH_HTTP_METHOD_OPTIONS, FOREACH_BATCH_SINK_MODE_OPTIONS, ForeachBatchHttpMethod, ForeachBatchSinkMode, ForeachBatchSinkNode, ForeachBatchSinkPropertiesProps, getEndpointPlaceholder()

### Community 129 - "HudiSink.tsx"
Cohesion: 0.22
Nodes (8): HudiSinkProperties, HUDI_SAVE_MODE_OPTIONS, HUDI_TABLE_TYPE_OPTIONS, HUDI_WRITE_OPERATION_OPTIONS, HudiSinkNode, HudiSinkPropertiesProps, HudiTableType, HudiWriteOperation

### Community 130 - "source/ParquetIO.tsx"
Cohesion: 0.28
Nodes (7): ParquetIOSourceProperties, PARQUET_REBASE_MODES, ParquetIOSourceNode, ParquetIOSourceProperties(), ParquetIOSourcePropertiesProps, ParquetSourceProperties, READ_MODE_OPTIONS

### Community 131 - "SetOperation.tsx"
Cohesion: 0.22
Nodes (8): SetOperationProperties, normalizeSetOperation(), SET_OPERATION_OPTIONS, SetOperationNode, SetOperationPropertiesProps, SetOperationStepProperties, SetOperationType, VALID_SET_OPERATIONS

### Community 132 - "scripts-and-config.test.ts"
Cohesion: 0.25
Nodes (5): CompoundConfigEntry, FlatConfigEntry, LaunchConfigEntry, req, TaskConfigEntry

### Community 133 - "scripts"
Cohesion: 0.25
Nodes (8): scripts, build, dev, lint, preview, test, test:changed, typecheck

### Community 134 - "AlloyDbSink.tsx"
Cohesion: 0.25
Nodes (7): AlloyDbSinkProperties, ALLOYDB_AUTH_OPTIONS, AlloyDbAuthMode, AlloyDbIpType, AlloyDbIsolationLevel, AlloyDbSaveMode, AlloyDbSinkPropertiesProps

### Community 135 - "sink/CloudSQL.tsx"
Cohesion: 0.25
Nodes (7): CloudSQLSinkProperties, CLOUD_SQL_AUTH_OPTIONS, CloudSqlAuthMode, CloudSqlDbEngine, CloudSqlIsolationLevel, CloudSqlSaveMode, CloudSQLSinkPropertiesProps

### Community 136 - "DeltaSink.tsx"
Cohesion: 0.25
Nodes (7): DeltaSinkProperties, DELTA_TARGET_MODE_OPTIONS, DELTA_WRITE_MODE_OPTIONS, DeltaSinkNode, DeltaSinkPropertiesProps, DeltaTargetMode, DeltaWriteMode

### Community 137 - "sink/HCatalogIO.tsx"
Cohesion: 0.25
Nodes (7): HCatalogIOSinkProperties, HCatalogIOSinkPropertiesProps, HIVE_FILE_FORMAT_OPTIONS, HIVE_SAVE_MODE_OPTIONS, HIVE_WRITE_METHOD_OPTIONS, HiveSaveMode, HiveWriteMethod

### Community 138 - "PandasUdfTransform.tsx"
Cohesion: 0.25
Nodes (7): PandasUdfTransformProperties, normalizeColumns(), PandasUdfExecutionMode, PandasUdfTransformNode, PandasUdfTransformPropertiesProps, PandasUdfTransformStepProperties, UDF_MODE_OPTIONS

### Community 139 - "sink/PulsarIO.tsx"
Cohesion: 0.25
Nodes (7): PulsarIOSinkProperties, PULSAR_AUTH_MODE_OPTIONS, PULSAR_COMPRESSION_OPTIONS, PULSAR_PAYLOAD_MODE_OPTIONS, PULSAR_TOPIC_REGEX, PulsarIOSinkNode, PulsarIOSinkPropertiesProps

### Community 140 - "source/PulsarIO.tsx"
Cohesion: 0.25
Nodes (7): PulsarIOSourceProperties, PULSAR_ENDING_OFFSETS_OPTIONS, PULSAR_READ_MODE_OPTIONS, PULSAR_STARTING_OFFSETS_OPTIONS, PULSAR_SUBSCRIPTION_MODE_OPTIONS, PulsarIOSourceNode, PulsarIOSourcePropertiesProps

### Community 141 - "RepartitionCoalesce.tsx"
Cohesion: 0.25
Nodes (7): RepartitionCoalesceProperties, normalizePartitionColumns(), OPERATION_OPTIONS, PartitionOperation, RepartitionCoalesceNode, RepartitionCoalescePropertiesProps, RepartitionCoalesceStepProperties

### Community 142 - "UnionByName.tsx"
Cohesion: 0.25
Nodes (7): UnionByNameProperties, normalizeUnionMode(), UNION_MODE_OPTIONS, UnionByNameNode, UnionByNamePropertiesProps, UnionByNameStepProperties, UnionMode

### Community 143 - "sink/IcebergIO.test.tsx"
Cohesion: 0.29
Nodes (6): ICEBERG_CATALOG_TYPES, ICEBERG_WRITE_OPERATION_OPTIONS, IcebergSinkProperties, ICEBERG_CATALOG_TYPES, IcebergIOSourceNode, IcebergSourceProperties

### Community 144 - "tsconfig.integration.json"
Cohesion: 0.29
Nodes (6): ./tsconfig.json, compilerOptions, noEmit, exclude, extends, include

### Community 145 - "LimitSample.tsx"
Cohesion: 0.29
Nodes (6): LimitSampleProperties, LimitSampleMode, LimitSampleNode, LimitSamplePropertiesProps, LimitSampleStepProperties, MODE_OPTIONS

### Community 146 - "sink/MongoDbIO.tsx"
Cohesion: 0.29
Nodes (6): MongoDbIOSinkProperties, MONGO_SINK_AUTH_OPTIONS, MongoDbIOSinkPropertiesProps, MongoOperationType, MongoSaveMode, resolveOperationType()

### Community 147 - "sink/Neo4jIO.tsx"
Cohesion: 0.29
Nodes (6): Neo4jIOSinkProperties, Neo4jIOSinkPropertiesProps, Neo4jSaveMode, Neo4jWriteTarget, resolveWriteTarget(), VALID_NEO4J_SCHEMES

### Community 148 - "sink/SingleStoreDBIO.tsx"
Cohesion: 0.29
Nodes (6): SingleStoreDBIOSinkProperties, SingleStoreDBIOSinkPropertiesProps, SingleStoreLoadDataCompression, SingleStoreOverwriteBehavior, SingleStoreSaveMode, SingleStoreWriteFormat

### Community 149 - "sink/SpannerIO.tsx"
Cohesion: 0.29
Nodes (6): SpannerIOSinkProperties, normalizeAuthMode(), normalizeMutationMode(), SpannerIOSinkPropertiesProps, SpannerMutationMode, SpannerSaveMode

### Community 150 - "consoleUrl.ts"
Cohesion: 0.71
Nodes (5): buildDataflowConsoleUrl(), buildDataprocConsoleUrl(), buildDataprocJobConsoleUrl(), buildGcpConsoleUrl(), buildServerlessBatchConsoleUrl()

### Community 151 - "ExecutionTerminal.tsx"
Cohesion: 0.43
Nodes (4): ExecutionTerminal(), ExecutionTerminalProps, classifyLogLine, getLogLineColorClass()

### Community 152 - "SecretManagerSecretField.tsx"
Cohesion: 0.48
Nodes (5): normalizeSecretItem(), DEFAULT_SECRET_FIELD_PLACEHOLDER, DEFAULT_SECRET_FIELD_TOOLTIP, SecretManagerSecretField(), SecretManagerSecretFieldProps

### Community 153 - "env.d.ts"
Cohesion: 0.33
Nodes (5): *.spark, *.spark?raw, *.yaml, *.yaml?raw, *.yml

### Community 154 - "App.test.tsx"
Cohesion: 0.33
Nodes (5): capturedEdges, capturedNodes, mockFitView, mockZoomIn, mockZoomOut

### Community 155 - "AlloyDbSource.tsx"
Cohesion: 0.33
Nodes (5): AlloyDbSourceProperties, ALLOYDB_AUTH_OPTIONS, AlloyDbAuthMode, AlloyDbIpType, AlloyDbSourcePropertiesProps

### Community 156 - "sink/CassandraIO.tsx"
Cohesion: 0.33
Nodes (5): CassandraIOSinkProperties, CassandraConnectorBackend, CassandraConsistencyLevel, CassandraIOSinkPropertiesProps, CONSISTENCY_LEVELS

### Community 157 - "CreateIO.tsx"
Cohesion: 0.33
Nodes (5): CreateProperties, CreateGenerationMode, CreatePropertiesProps, GENERATION_MODE_OPTIONS, normalizeElements()

### Community 158 - "DeltaSource.tsx"
Cohesion: 0.33
Nodes (5): DeltaSourceProperties, DELTA_READ_MODE_OPTIONS, DELTA_SOURCE_MODE_OPTIONS, DeltaSourceNode, DeltaSourcePropertiesProps

### Community 159 - "HudiSource.tsx"
Cohesion: 0.33
Nodes (5): HudiSourceProperties, HUDI_QUERY_TYPE_OPTIONS, HUDI_READ_MODE_OPTIONS, HudiSourceNode, HudiSourcePropertiesProps

### Community 160 - "source/Neo4jIO.tsx"
Cohesion: 0.33
Nodes (5): Neo4jIOSourceProperties, Neo4jIOSourcePropertiesProps, Neo4jQueryMode, Neo4jReadMode, Neo4jStreamingFrom

### Community 161 - "source/JdbcIO.test.tsx"
Cohesion: 0.20
Nodes (3): Neo4jSinkProperties, JdbcSourceProperties, PubSubSourceProperties

### Community 162 - "python/run_sdp_dataproc.sh"
Cohesion: 0.60
Nodes (3): die(), log(), run_sdp_dataproc.sh script

### Community 163 - "pyspark/run_sdp_dataproc.sh"
Cohesion: 0.60
Nodes (3): die(), log(), run_sdp_dataproc.sh script

### Community 166 - "sink/ClickHouseIO.tsx"
Cohesion: 0.40
Nodes (4): ClickHouseIOSinkProperties, ClickHouseIOSinkPropertiesProps, ClickHouseIsolationLevel, ClickHouseSaveMode

### Community 167 - "sink/ElasticsearchIO.tsx"
Cohesion: 0.40
Nodes (4): ElasticsearchIOSinkProperties, ElasticsearchIOSinkPropertiesProps, ElasticsearchWriteOperation, resolveWriteOperation()

### Community 168 - "sink/RedisIO.tsx"
Cohesion: 0.40
Nodes (4): RedisIOSinkProperties, RedisIOSinkPropertiesProps, RedisSinkDataType, resolveRedisDataType()

### Community 169 - "source/RedisIO.tsx"
Cohesion: 0.40
Nodes (4): RedisIOSourceProperties, RedisDataType, RedisIOSourcePropertiesProps, RedisReadMode

### Community 170 - "StoragePathField.tsx"
Cohesion: 0.40
Nodes (3): DEFAULT_BUCKET_ERROR_PREFIX, StoragePathFieldProps, toBucketSuggestions()

### Community 171 - "LimitSampleProperties"
Cohesion: 0.70
Nodes (4): LimitSampleProperties(), validateLimitCount(), validateSampleFraction(), validateSampleSeed()

### Community 172 - "source/SpannerIO.tsx"
Cohesion: 0.50
Nodes (3): SpannerIOSourceProperties, SpannerIOSourcePropertiesProps, SpannerReadMode

### Community 173 - "CanvasToolbar.test.tsx"
Cohesion: 0.50
Nodes (3): mockFitView, mockZoomIn, mockZoomOut

### Community 174 - "PulsarAuthFields.tsx"
Cohesion: 0.50
Nodes (3): PulsarAuthFieldsProps, PulsarAuthMode, SecretItem

### Community 175 - "RabbitMqConnectionFields.tsx"
Cohesion: 0.50
Nodes (3): ComputeInstanceItem, RabbitMqConnectionFieldsProps, SecretItem

### Community 176 - "RabbitMqDestinationFields.tsx"
Cohesion: 0.50
Nodes (3): RabbitMqDestinationFieldsProps, RabbitMqDestinationMode, RabbitMqExchangeType

### Community 177 - "cdfOptions.ts"
Cohesion: 0.50
Nodes (3): CDF_AUTH_TYPE_OPTIONS, CDF_DOCS_URL, CdfAuthTypeOption

## Knowledge Gaps
- **36 isolated node(s):** `./tsconfig.json`, `CHECKPOINT_ROOT`, `PROJECT_ID`, `PYSPARK_DRIVER_PYTHON`, `PYSPARK_PYTHON` (+31 more)
  These have ≤1 connection - possible missing edges. (Counts symbols only; 1459 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **13 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `react` connect `react` to `ref_vitest`, `App.tsx`, `DeploymentTab.tsx`, `source/AvroIO.tsx`, `SourceCodeTab.tsx`, `shared/types.ts`, `StorageBackendFields.tsx`, `useAssetLoader.ts`, `shared/sparkDagRules.ts`, `webview/package.json`, `usePipelineGraph.ts`, `AdvancedConfig.tsx`, `SecretManagerAuthFields.tsx`, `propertyFormRegistry.ts`, `SamplePipeline`, `sink/MqttIO.tsx`, `WindowOver.tsx`, `SystemInstructionsField.tsx`, `ThemeContext.tsx`, `PipelineParadigm`, `MergeCdcScd.tsx`, `NestedStructArrayOps.tsx`, `panelBuildingBlocks.test.tsx`, `StorageBackendFields`, `HandleNulls.tsx`, `sink/JdbcIO.tsx`, `systemInstructions.ts`, `DataQualityAssert.tsx`, `GroupByAgg.tsx`, `Join.tsx`, `StatefulTransform.tsx`, `TextChunkAndEmbed.tsx`, `TopN.tsx`, `VectorStoreSink.tsx`, `PropertyPane.tsx`, `nodeDocumentation.ts`, `ConditionalSplit.tsx`, `EnforceSchemaAndCast.tsx`, `Filter.tsx`, `PropertyField.tsx`, `sink/KafkaIO.tsx`, `RestApiSource.tsx`, `PropertyForm.test.tsx`, `Deduplicate.tsx`, `sink/KinesisIO.tsx`, `OrderBy.tsx`, `PivotUnpivot.tsx`, `RollupCube.tsx`, `SdpSinkSettingsField.tsx`, `EventHubsSink.tsx`, `SelectField.tsx`, `sink/TFRecordIO.tsx`, `sink/AvroIO.tsx`, `sink/BigQueryIO.tsx`, `sink/CsvIO.tsx`, `FeaturePrep.tsx`, `sink/ParquetIO.tsx`, `SparkMlAndVertexAi.tsx`, `SparkSqlQuery.tsx`, `sink/TextIO.tsx`, `NodesCard.tsx`, `SftpSource.tsx`, `Watermark.tsx`, `WithColumns.tsx`, `sink/XmlIO.tsx`, `StepExecutionSettingsField.tsx`, `dependencies`, `CachePersist.tsx`, `RenameDropColumns.tsx`, `Select.tsx`, `lucide-react`, `source/CsvIO.tsx`, `Explode.tsx`, `ForeachBatchSink.tsx`, `HudiSink.tsx`, `source/ParquetIO.tsx`, `SetOperation.tsx`, `AlloyDbSink.tsx`, `sink/CloudSQL.tsx`, `DeltaSink.tsx`, `sink/HCatalogIO.tsx`, `PandasUdfTransform.tsx`, `sink/PulsarIO.tsx`, `source/PulsarIO.tsx`, `RepartitionCoalesce.tsx`, `UnionByName.tsx`, `LimitSample.tsx`, `sink/MongoDbIO.tsx`, `sink/Neo4jIO.tsx`, `sink/SingleStoreDBIO.tsx`, `sink/SpannerIO.tsx`, `ExecutionTerminal.tsx`, `SecretManagerSecretField.tsx`, `App.test.tsx`, `AlloyDbSource.tsx`, `sink/CassandraIO.tsx`, `CreateIO.tsx`, `DeltaSource.tsx`, `HudiSource.tsx`, `source/Neo4jIO.tsx`, `sink/ClickHouseIO.tsx`, `sink/ElasticsearchIO.tsx`, `sink/RedisIO.tsx`, `source/RedisIO.tsx`, `StoragePathField.tsx`, `source/SpannerIO.tsx`, `PulsarAuthFields.tsx`, `RabbitMqConnectionFields.tsx`, `RabbitMqDestinationFields.tsx`, `SelectField.test.tsx`, `StoragePathField.test.tsx`?**
  _High betweenness centrality (0.116) - this node is a cross-community bridge._
- **What connects `./tsconfig.json`, `CHECKPOINT_ROOT`, `PROJECT_ID` to the rest of the system?**
  _36 weakly-connected nodes found - possible missing edges._
- **Should `ref_vitest` be split into smaller, more focused modules?**
  _Cohesion score 0.037806451612903226 - nodes in this community are weakly interconnected._
- **Why does `@google/genai` connect `multi-agent-evaluator.test.ts` to `codegen-runner.ts`, `package.json`, `codeGeneration.ts`, `packaging-evaluator.ts`, `pipeline-spec-evaluator.ts`, `arbiter-evaluator.ts`, `spark-architecture-evaluator.ts`?**
  _High betweenness centrality (0.035) - this node is a cross-community bridge._
- **Should `extension.ts` be split into smaller, more focused modules?**
  _Cohesion score 0.08302469135802469 - nodes in this community are weakly interconnected._
- **Why does `PipelineParadigm` connect `PipelineParadigm` to `extension.ts`, `SdpSinkSettingsField.tsx`, `PipelineRunner.ts`, `usePipelineGraph.ts`, `App.tsx`, `core/constants.ts`, `packaging-evaluator.ts`, `PropertyForm.test.tsx`, `NodesCard.tsx`, `deterministic-gate.ts`, `shared/types.ts`, `pipeline-spec-evaluator.ts`, `SamplePipeline`, `StepExecutionSettingsField.tsx`, `PropertyPane.tsx`, `shared/sdpPlanner.ts`, `spark-architecture-evaluator.ts`, `shared/sparkDagRules.ts`?**
  _High betweenness centrality (0.032) - this node is a cross-community bridge._
- **Should `shared/dagValidator.ts` be split into smaller, more focused modules?**
  _Cohesion score 0.060833902939166094 - nodes in this community are weakly interconnected._