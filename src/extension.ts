import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';
import { ensureSystemCaBundle } from './codeGeneration';
import { getGcloudProject } from './cli/gcloudClient';
import { SparkTreeProvider, type SparkJob } from './tree/sparkTree';
import { SparkEditorProvider } from './editor/SparkEditorProvider';
import { promptForSparkSaveLocation } from './editor/sparkSaveDialog';
import { upgradeRestoredSparkTextTabs } from './editor/restoredTabUpgrade';
import type { PipelineParadigm } from './shared/types';

export type { WebviewHtmlOptions } from './webviewHtml';
export {
    buildWebviewHtml,
    getNonce,
} from './webviewHtml';

/**
 * Apache Spark Designer Extension Main Entry Point
 * 
 * Author: Mike Nimer <mikenimer@google.com>
 * 
 * Purpose:
 * Activates the VS Code extension for Apache Spark pipeline visual authoring.
 * Registers tree view providers, custom editor providers, commands, and IPC handlers
 * between the VS Code extension host and the webview designer canvas.
 * 
 * Architectural Decisions:
 * - Uses CustomTextEditorProvider (SparkEditorProvider) for two-way synchronization of .spark files.
 * - Bridges gcloud and bq CLI commands for real-time asset discovery (BigQuery datasets/tables,
 *   Dataproc clusters, session templates, VPC networks/subnets, Cloud Storage buckets).
 * - Executes AI code generation via Google GenAI SDK (@google/genai).
 * - Provides non-blocking asynchronous IPC message dispatch to webview.
 * 
 * @param context The extension context provided by VS Code
 */
import { getDesignerOutputChannel } from './core/outputChannel';

export {
    PIPELINE_FILE_EXT,
    SPARK_EDITOR_VIEW_TYPE,
    getInitialPipelineContent,
    PIPELINE_SEARCH_EXCLUDE_DIRS,
    PIPELINE_SEARCH_EXCLUDE_PATTERN,
    GENERATED_FILE_LIST_EXCLUDE_DIRS,
} from './core/constants';
import {
    PIPELINE_FILE_EXT,
    SPARK_EDITOR_VIEW_TYPE,
    getInitialPipelineContent,
    PIPELINE_SEARCH_EXCLUDE_PATTERN,
} from './core/constants';

export {
    DEFAULT_IGNORE_PATTERNS,
    getDefaultIgnoreFileContent,
    readResourceFile
} from './resources/resourceLoader';

/**
 * Activates the Apache Spark Designer extension.
 *
 * @param context The extension context used to register providers and subscriptions.
 */
export function activate(context: vscode.ExtensionContext) {
    // Ensure both https.globalAgent and native fetch (tls.createSecureContext) trust the system CA bundle
    ensureSystemCaBundle();

    console.log('Apache Spark Pipeline Builder is now active!');
    const outputChannel = getDesignerOutputChannel();
    context.subscriptions.push(outputChannel);
    outputChannel.appendLine(`[${new Date().toLocaleTimeString()}] Apache Spark Designer initialized.`);

    const treeDataProvider = new SparkTreeProvider();
    vscode.window.registerTreeDataProvider('sparkExplorer', treeDataProvider);

    context.subscriptions.push(
        vscode.window.registerCustomEditorProvider(
            SPARK_EDITOR_VIEW_TYPE,
            new SparkEditorProvider(context),
            {
                webviewOptions: {
                    retainContextWhenHidden: true
                },
                supportsMultipleEditorsPerDocument: false
            }
        )
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('spark-pipeline-builder.openWithDesigner', async (uri?: vscode.Uri) => {
            const targetUri = uri || vscode.window.activeTextEditor?.document.uri;
            if (targetUri) {
                await vscode.commands.executeCommand('vscode.openWith', targetUri, SPARK_EDITOR_VIEW_TYPE);
            }
        })
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('spark-pipeline-builder.createNewPipeline', async () => {
            interface ParadigmPickItem extends vscode.QuickPickItem {
                readonly paradigm: PipelineParadigm;
            }

            const paradigmItems: ParadigmPickItem[] = [
                {
                    label: 'Imperative PySpark (Default)',
                    description: 'PySpark 4.x',
                    detail: 'PySpark 4.x DataFrame & Structured Streaming (read_sources / transform / write_sinks)',
                    paradigm: 'imperative'
                },
                {
                    label: 'Spark Declarative Pipelines (SDP)',
                    description: 'Apache Spark 4.2+',
                    detail: 'Apache Spark 4.2+ pyspark.pipelines (spark-pipeline.yml, Streaming Tables & Materialized Views)',
                    paradigm: 'sdp'
                }
            ];

            const selectedParadigm = await vscode.window.showQuickPick(paradigmItems, {
                title: 'Select Pipeline Paradigm',
                placeHolder: 'Choose Imperative PySpark or Spark Declarative Pipelines (SDP)'
            });

            if (!selectedParadigm) {
                return;
            }

            const fileUri = await promptForSparkSaveLocation('Create New Apache Spark Pipeline', 'pipeline');

            if (!fileUri) {
                return;
            }

            const initialContent = getInitialPipelineContent(selectedParadigm.paradigm);

            fs.writeFileSync(fileUri.fsPath, initialContent, 'utf-8');
            treeDataProvider.refresh();
            await vscode.commands.executeCommand('vscode.openWith', fileUri, SPARK_EDITOR_VIEW_TYPE);
        })
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('spark-pipeline-builder.refreshPipelines', () => {
            treeDataProvider.refresh();
        })
    );

    /*
     * `Spark: Show Editor` opens a pipeline in the custom editor.
     */
    context.subscriptions.push(
        vscode.commands.registerCommand('spark-pipeline-builder.showEditor', async () => {
            // Prefer whatever the user is already looking at.
            const activeUri = vscode.window.activeTextEditor?.document.uri;
            if (activeUri?.fsPath.endsWith(PIPELINE_FILE_EXT)) {
                await vscode.commands.executeCommand('vscode.openWith', activeUri, SPARK_EDITOR_VIEW_TYPE);
                return;
            }

            // Otherwise look for pipelines in the workspace, honouring the same exclude
            // pattern the tree view uses so we do not surface files from node_modules.
            const files = await vscode.workspace.findFiles(`**/*${PIPELINE_FILE_EXT}`, PIPELINE_SEARCH_EXCLUDE_PATTERN);
            if (files.length === 0) {
                vscode.window.showInformationMessage(
                    'No .spark pipelines found in this workspace. Use "Spark: New Pipeline" to create one.'
                );
                return;
            }

            if (files.length === 1) {
                await vscode.commands.executeCommand('vscode.openWith', files[0], SPARK_EDITOR_VIEW_TYPE);
                return;
            }

            // More than one candidate: let the user choose rather than guessing.
            const picked = await vscode.window.showQuickPick(
                files.map(f => ({
                    label: path.basename(f.fsPath),
                    description: vscode.workspace.asRelativePath(f),
                    uri: f,
                })),
                { title: 'Open Spark Pipeline', placeHolder: 'Select a pipeline to open' }
            );
            if (picked) {
                await vscode.commands.executeCommand('vscode.openWith', picked.uri, SPARK_EDITOR_VIEW_TYPE);
            }
        })
    );

    // Refresh tree view on file rename or move
    context.subscriptions.push(
        vscode.workspace.onDidRenameFiles(_e => {
            treeDataProvider.refresh();
        })
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('spark-pipeline-builder.openJobInConsole', async (job?: Partial<SparkJob>, project?: string) => {
            if (!job || !job.id) return;
            const region = job.region || job.location || 'us-central1';
            let proj = project;
            if (!proj || proj === 'YOUR_PROJECT_ID') {
                proj = await getGcloudProject();
            }
            const isClusterJob = job.jobType === 'cluster-job' || job.category === 'cluster-job';
            const url = isClusterJob
                ? `https://console.cloud.google.com/dataproc/jobs/${job.id}?region=${region}${proj ? `&project=${proj}` : ''}`
                : `https://console.cloud.google.com/dataproc/batches/${region}/${job.id}/monitoring${proj ? `?project=${proj}` : ''}`;
            vscode.env.openExternal(vscode.Uri.parse(url));
        })
    );

    // VS Code restores tabs with the editor type they were persisted with and never
    // re-resolves them, so a .spark file once opened while this extension was not loaded
    // keeps coming back as plain YAML. Reopen such tabs in the designer.
    void upgradeRestoredSparkTextTabs().catch((err: unknown) => {
        outputChannel.appendLine(
            `[${new Date().toLocaleTimeString()}] Failed to reopen restored .spark tabs in the designer: ${err instanceof Error ? err.message : String(err)}`
        );
    });
}

export type {
    SparkJob,
    RawServerlessBatch,
    RawClusterJob,
} from './tree/sparkTree';
export {
    categorizeJobStatus,
    getJobStartTime,
    normalizeServerlessBatch,
    normalizeClusterJob,
    SparkTreeProvider,
    DataItem,
    SparkTreeItem,
} from './tree/sparkTree';

/**
 * Deactivates the Apache Spark Designer extension and disposes resources.
 */
export function deactivate() { }

export { findLastCompleteFileEndOffset, parseOrRepairJson } from './codegen/jsonRepair';
export type {
    CodegenModelOption,
    CodegenModelTier,
} from './shared/types';
export type {
    DiscoveredCodegenModel,
    DiscoverCodegenModelsOptions,
    DiscoverCodegenModelsResult,
} from './codegen/modelDiscovery';
export {
    discoverCodegenModels,
    filterAndRankCodegenModels,
    resolveDefaultCodegenModel,
    resolveSelectedCodegenModel,
    normalizeGeminiModelId,
    parseGeminiModelVersion,
    classifyGeminiModelTier,
    isSupportedCodegenModel,
    createDefaultCodegenModelOption,
} from './codegen/modelDiscovery';

export { findExecutableInPath, resolveCli, findGcloudPath } from './cli/cliResolver';
export type {
    SecretManagerItem,
    DataprocClusterItem,
    SessionTemplateItem,
    BigLakeCatalogItem,
} from './cli/resourceMappers';
export {
    toShortSecretId,
    mapSecretManagerItems,
    parseSecretManagerOutput,
    mapBigtableInstances,
    mapBigtableTables,
    mapSqlInstances,
    mapSqlDatabases,
    mapProjects,
    parseBigtableInstanceParam,
    parseSqlInstanceParam,
    mapHealthcareDatasetItems,
    mapDataprocClusters,
    mapSessionTemplates,
    mapBigLakeCatalogs,
    mapRegions,
} from './cli/resourceMappers';
export { listAssets, getGcloudProject } from './cli/gcloudClient';

export {
    SamplePipeline,
    formatSampleTitle,
    computePipelineFlow,
    getSamplePipelines
} from './samples/samplePipelineService';



/**
 * Directory names, system paths, and dependency folders to ignore during file tree scans and cleanup.
 */
export const IGNORED_DIRECTORY_NAMES = new Set([
    'venv',
    '.venv',
    'env',
    '.env',
    'virtualenv',
    '.virtualenv',
    'node_modules',
    '__pycache__',
    'target',
    'build',
    'dist',
    'out',
    'bin',
    'site-packages',
    'dist-packages',
    '.pytest_cache',
    '.mypy_cache',
    '.ruff_cache',
    '.tox',
    '.nox',
    '.eggs',
    '.git',
    '.github',
    '.idea',
    '.vscode'
]);

/**
 * Checks if a given relative or absolute file path belongs to an ignored directory,
 * virtual environment, or hidden cache structure.
 */
export function isIgnoredPath(fileNameOrPath: string): boolean {
    if (!fileNameOrPath) return true;
    const parts = fileNameOrPath.toLowerCase().replace(/\\/g, '/').split('/');
    return parts.some(part => {
        if (!part) return false;
        if (part === '.dockerignore' || part === '.gcloudignore') return false;
        if (IGNORED_DIRECTORY_NAMES.has(part)) return true;
        if (part.startsWith('.') || part.endsWith('.egg-info')) return true;
        return false;
    });
}

export {
    resolvePipelineId,
    gateOutputFolder,
    executeScopedCleanup,
    writeGenerationManifest,
} from './generation/outputFolderGate';

export {
    ALLOWED_EXECUTABLE_SCRIPTS,
    isAllowedExecutableScript,
    resolveSafeOutputPath,
    SHELL_METASYMBOLS_REGEX,
    isSafeFolderPath,
    terminalCwdMatches,
} from './security/scriptGuard';




export type { DocumentSyncState } from './editor/documentSync';
export { createSafePostMessage, shouldPushDocumentUpdate } from './editor/documentSync';
export { savePipelineFiles } from './editor/pipelineFileWriter';
export { SparkEditorProvider } from './editor/SparkEditorProvider';
export { isPlainTextSparkTab, upgradeRestoredSparkTextTabs } from './editor/restoredTabUpgrade';

export { getDesignerOutputChannel, disposeDesignerOutputChannel } from './core/outputChannel';

export type { MessageContext, MessageHandler } from './ipc/messageContext';
export {
    handleGetDatasets,
    handleGetTables,
    handleGetTopics,
    handleGetSubscriptions,
    handleGetNetworks,
    handleGetSubnetworks,
    handleGetStorageBuckets,
    handleGetSecrets,
    handleGetHealthcareDatasets,
    handleGetAwsQueues,
    handleGetComputeInstances,
    handleGetBigtableInstances,
    handleGetBigtableTables,
    handleGetSqlInstances,
    handleGetSqlDatabases,
    handleGetProjects,
    handleGetDataprocClusters,
    handleGetSessionTemplates,
    handleGetBigLakeCatalogs,
    handleGetRegions,
    handleGetCodegenModels,
    handleGetRunningJobs,
    fetchDataprocClusters,
    fetchSessionTemplates,
    fetchBigLakeCatalogs,
    fetchDatasets,
    fetchTables,
    fetchBuckets,
    fetchStorageBuckets,
    fetchSubnetworks,
    fetchRegions,
    fetchSecrets,
    fetchCodegenModels,
} from './ipc/handlers/discoveryHandlers';
export {
    handleReady,
    handleGetSamples,
    handleOpenSample,
    handleSavePipeline,
    handlePickFolder,
    handleReadFolderFiles,
    handleReadFileContent,
    handleShowOutputChannel,
    handleOpenExternal,
} from './ipc/handlers/workspaceHandlers';
export {
    handleRunPipeline,
    handleStopPipeline,
    handleStopCodeGeneration,
    handleRegenerateCodeCommand,
} from './ipc/handlers/executionHandlers';
export {
    MESSAGE_HANDLERS,
    handleMessages,
} from './ipc/messageRouter';


