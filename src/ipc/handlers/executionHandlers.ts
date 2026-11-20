/** @fileoverview Execution and code generation IPC message handlers.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import * as vscode from 'vscode';
import * as fs from 'fs';
import * as path from 'path';
import { handleRegenerateCode } from '../../codeGeneration';
import { getDesignerOutputChannel } from '../../core/outputChannel';
import { EditorSession } from '../../editor/EditorSession';
import { resolveOutputFolder } from '../../editor/outputFolder';
import { savePipelineFiles } from '../../editor/pipelineFileWriter';
import { isSafeFolderPath, isAllowedExecutableScript } from '../../security/scriptGuard';
import {
    PLACEHOLDER_PROJECT_ID,
    buildRunnerEnvironment,
    cancelRemoteSparkJob,
    createJobIdTracker,
    describeRunner,
    findRunnerScript,
    resolveRunTarget,
    runnerScriptCandidates,
    spawnRunnerScript,
    type DetectedRunnerJob,
} from '../../execution/PipelineRunner';
import type { MessageContext } from '../messageContext';

/**
 * Session used when a MessageContext carries no per-panel session (legacy
 * callers and direct handler invocations in tests).
 */
const fallbackSession = new EditorSession();

/**
 * Tracks the most recently detected remote job per editor session so Stop/Cancel
 * can cancel an active Serverless batch or Dataproc cluster job even when the
 * webview only sends `{ cancelRemoteJob: true }`.
 */
const activeRemoteJobBySession = new WeakMap<EditorSession, DetectedRunnerJob>();

/**
 * Returns the session that owns long-running work for the sending panel.
 *
 * @param ctx Message context.
 * @returns The panel's session, or the shared fallback session.
 */
function sessionOf(ctx: MessageContext): EditorSession {
    return ctx.session ?? fallbackSession;
}

/**
 * Whether VS Code reports the workspace as explicitly untrusted (finding S2).
 * Running runner scripts or writing generated code from an untrusted folder
 * would let a malicious `.spark` file or checked-in script execute locally.
 *
 * @returns True only when `workspace.isTrusted` is exactly false.
 */
function isWorkspaceUntrusted(): boolean {
    return vscode.workspace.isTrusted === false;
}

/** User-facing explanation shown when an action is blocked by Workspace Trust. */
const UNTRUSTED_WORKSPACE_MESSAGE =
    'This workspace is not trusted. Trust the workspace to run pipelines or generate code.';

/** Error message shown when an overlapping Spark Declarative Pipeline run is rejected on shared checkpoint storage. */
export const SDP_OVERLAPPING_RUN_MESSAGE =
    'Cannot start an overlapping Spark Declarative Pipeline run while another run is active on shared checkpoint storage. Stop or wait for the current run first.';

/**
 * Executes a pipeline script as a child process and streams real-time stdout/stderr
 * logs and detected job IDs to the webview and output channel.
 *
 * @param ctx Message context
 * @param message Incoming IPC message with pipeline and runner configuration
 */
export async function handleRunPipeline(ctx: MessageContext, message: any): Promise<void> {
    if (isWorkspaceUntrusted()) {
        vscode.window.showErrorMessage(UNTRUSTED_WORKSPACE_MESSAGE);
        ctx.postToWebview({ command: 'pipelineLog', text: `[Error]: ${UNTRUSTED_WORKSPACE_MESSAGE}\n`, stream: 'stderr' });
        ctx.postToWebview({ command: 'pipelineFinished', exitCode: 1, status: 'failed' });
        return;
    }
    const session = sessionOf(ctx);
    const requestedParadigm: 'imperative' | 'sdp' =
        message?.pipelineConfig?.paradigm === 'sdp' || message?.jobConfig?.paradigm === 'sdp'
            ? 'sdp'
            : 'imperative';

    if (requestedParadigm === 'sdp' && session.activeProcess !== null) {
        vscode.window.showErrorMessage(SDP_OVERLAPPING_RUN_MESSAGE);
        const channel = ctx.outputChannel || getDesignerOutputChannel();
        channel.appendLine(`[${new Date().toLocaleTimeString()}] ERROR: ${SDP_OVERLAPPING_RUN_MESSAGE}`);
        ctx.postToWebview({
            command: 'pipelineLog',
            text: `[Error]: ${SDP_OVERLAPPING_RUN_MESSAGE}\n`,
            stream: 'stderr',
        });
        return;
    }

    const runUri = ctx.document ? ctx.document.uri : ctx.uri;
    if (runUri) {
        const outputFolder = message.pipelineConfig?.outputFolder;
        if (outputFolder && outputFolder.trim() && !isSafeFolderPath(outputFolder)) {
            vscode.window.showErrorMessage(`Invalid output folder "${outputFolder}": contains unsafe shell characters.`);
            return;
        }
        const folderPath = resolveOutputFolder(
            runUri.fsPath,
            outputFolder,
            vscode.workspace.workspaceFolders?.[0]?.uri.fsPath
        );

        if (!isSafeFolderPath(folderPath)) {
            vscode.window.showErrorMessage(`Invalid pipeline folder path "${folderPath}": contains unsafe shell characters.`);
            return;
        }

        const runner = message.jobConfig?.runner || 'Local';
        const resolveCandidates = runnerScriptCandidates as (
            runnerName: string,
            paradigm?: string
        ) => string[];
        const possibleNames = resolveCandidates(runner, requestedParadigm);
        const scriptName = findRunnerScript(folderPath, possibleNames);

        if (scriptName) {
            if (!isAllowedExecutableScript(scriptName)) {
                vscode.window.showErrorMessage(`Unauthorized runner script: ${scriptName}`);
                return;
            }

            try {
                // If this panel already has an imperative run in progress, terminate it before starting a new one.
                // Runs owned by other editor panels are left alone.
                session.stopProcess(false);
                activeRemoteJobBySession.delete(session);

                const scriptFullPath = path.join(folderPath, scriptName);
                try {
                    fs.chmodSync(scriptFullPath, '755');
                } catch {
                    // Best effort file permission update, continue execution
                }

                const commandLine = `${describeRunner(runner, requestedParadigm)}\ncd "${folderPath}" && bash ./${scriptName}`;

                ctx.postToWebview({
                    command: 'pipelineStarted',
                    runner: runner,
                    commandLine: commandLine,
                    folderPath: folderPath
                });

                const channel = ctx.outputChannel || getDesignerOutputChannel();
                channel.appendLine(`\n[${new Date().toLocaleTimeString()}] Starting pipeline execution with ${runner}:`);
                channel.appendLine(`Command: cd "${folderPath}" && bash ./${scriptName}`);

                // F6: resolve the project before spawning (explicit projectId wins).
                const target = await resolveRunTarget(message.jobConfig);
                const explicitParadigm =
                    message?.pipelineConfig?.paradigm ?? message?.jobConfig?.paradigm;
                const effectiveJobConfig = message.jobConfig || explicitParadigm
                    ? {
                        ...(message.jobConfig || {}),
                        ...(explicitParadigm ? { paradigm: explicitParadigm } : {}),
                        ...(target.project !== PLACEHOLDER_PROJECT_ID ? { projectId: target.project } : {}),
                        region: message.jobConfig?.region || target.region,
                    }
                    : undefined;

                const { env, rejectedKeys } = buildRunnerEnvironment({
                    baseEnv: process.env,
                    runtimeVariables: message.runtimeVariables,
                    jobConfig: effectiveJobConfig,
                });
                if (rejectedKeys.length > 0) {
                    const warning = `Ignored unsafe runtime variable name(s): ${rejectedKeys.join(', ')}`;
                    channel.appendLine(`[${new Date().toLocaleTimeString()}] WARNING: ${warning}`);
                    ctx.postToWebview({ command: 'pipelineLog', text: `[Warning]: ${warning}\n`, stream: 'stderr' });
                }

                const trackJobId = createJobIdTracker(target);

                const child = spawnRunnerScript(folderPath, scriptFullPath, env, {
                    onOutput: (text, stream) => {
                        channel.append(text);
                        ctx.postToWebview({ command: 'pipelineLog', text, stream });
                        const job = trackJobId(text);
                        if (job) {
                            activeRemoteJobBySession.set(session, job);
                            const label = job.runner === 'Dataflow'
                                ? 'Dataflow Job ID'
                                : job.runner === 'DataprocCluster'
                                    ? 'Dataproc Cluster Job ID'
                                    : 'Serverless Spark Batch ID';
                            channel.appendLine(`[${new Date().toLocaleTimeString()}] Detected ${label}: ${job.jobId}`);
                            ctx.postToWebview({ command: 'pipelineJobIdDetected', ...job });
                        }
                    },
                    onError: (err) => {
                        const errText = `\n[Process execution error: ${err.message}]\n`;
                        channel.appendLine(errText);
                        ctx.postToWebview({ command: 'pipelineLog', text: errText, stream: 'stderr' });
                        ctx.postToWebview({ command: 'pipelineFinished', exitCode: 1, status: 'failed' });
                        session.clearProcessIf(child);
                    },
                    onClose: (code, signal) => {
                        const status = (code === 0) ? 'completed' : (signal ? 'stopped' : 'failed');
                        const finishText = `\n[Execution ${status} (exit code: ${code ?? 0}${signal ? `, signal: ${signal}` : ''})]\n`;
                        channel.appendLine(finishText);
                        ctx.postToWebview({ command: 'pipelineLog', text: finishText, stream: 'stdout' });
                        ctx.postToWebview({ command: 'pipelineFinished', exitCode: code ?? 0, status: status });
                        session.clearProcessIf(child);
                    },
                });
                session.setProcess(child);

            } catch (e: any) {
                console.error('Failed to spawn runner process', e);
                vscode.window.showErrorMessage(`Failed to execute runner process: ${e.message}`);
                ctx.postToWebview({
                    command: 'pipelineLog',
                    text: `\nFailed to start runner process: ${e.message}\n`,
                    stream: 'stderr'
                });
                ctx.postToWebview({
                    command: 'pipelineFinished',
                    exitCode: 1,
                    status: 'failed'
                });
            }
        } else {
            const namesStr = possibleNames.join(' or ');
            const notFoundMsg = `Runner script not found (${namesStr}) in ${folderPath}. Please save and/or generate code first.`;
            vscode.window.showErrorMessage(notFoundMsg);
            ctx.postToWebview({
                command: 'pipelineLog',
                text: `[Error]: ${notFoundMsg}\n`,
                stream: 'stderr'
            });
            ctx.postToWebview({
                command: 'pipelineFinished',
                exitCode: 1,
                status: 'failed'
            });
        }
    } else {
        vscode.window.showErrorMessage("Please save the pipeline first to run it.");
    }
}

/**
 * Terminates an active background pipeline execution process and optionally
 * cancels an active remote Serverless Spark batch or Dataproc Cluster job.
 *
 * @param ctx Message context
 * @param message Incoming IPC message with optional remote cancellation target
 */
export async function handleStopPipeline(ctx: MessageContext, message: any): Promise<void> {
    const session = sessionOf(ctx);
    // SIGINT the process group now, SIGKILL after a grace period if still alive.
    const stopped = session.stopProcess(true);

    const trackedJob = activeRemoteJobBySession.get(session);
    const rawCancelFlag = message?.cancelRemoteJob ?? message?.jobConfig?.cancelRemoteJob;
    const explicitJobId = (message?.jobId || message?.jobConfig?.jobId) as string | undefined;
    const effectiveJobId = (explicitJobId || (rawCancelFlag === true ? trackedJob?.jobId : undefined))?.trim();
    const effectiveRunner = (message?.runner || message?.jobConfig?.runner || trackedJob?.runner || 'ServerlessSpark') as string;
    const effectiveProjectId = (message?.projectId || message?.jobConfig?.projectId || trackedJob?.projectId) as string | undefined;
    const effectiveRegion = (message?.region || message?.jobConfig?.region || trackedJob?.region) as string | undefined;

    const isRemoteSparkRunner =
        effectiveRunner === 'ServerlessSpark' ||
        effectiveRunner === 'Spark' ||
        effectiveRunner === 'DataprocCluster';
    const shouldCancelRemote = Boolean(
        effectiveJobId &&
        isRemoteSparkRunner &&
        (rawCancelFlag === true || (rawCancelFlag !== false && Boolean(explicitJobId)))
    );

    const channel = ctx.outputChannel || getDesignerOutputChannel();
    if (stopped) {
        channel.appendLine(`[${new Date().toLocaleTimeString()}] Pipeline execution stopped by user.`);
    }

    if (shouldCancelRemote && effectiveJobId) {
        activeRemoteJobBySession.delete(session);
        const result = await cancelRemoteSparkJob({
            runner: effectiveRunner,
            jobId: effectiveJobId,
            projectId: effectiveProjectId,
            region: effectiveRegion,
        });
        if (result.cancelled) {
            const msg = `[${new Date().toLocaleTimeString()}] Cancelled remote ${effectiveRunner} job '${effectiveJobId}' (${result.commandLine}).`;
            channel.appendLine(msg);
            ctx.postToWebview({ command: 'pipelineLog', text: `${msg}\n`, stream: 'stdout' });
        } else {
            const errMsg = `[${new Date().toLocaleTimeString()}] Failed to cancel remote ${effectiveRunner} job '${effectiveJobId}': ${result.error}`;
            channel.appendLine(errMsg);
            ctx.postToWebview({ command: 'pipelineLog', text: `${errMsg}\n`, stream: 'stderr' });
        }
    }

    if (stopped || shouldCancelRemote) {
        activeRemoteJobBySession.delete(session);
        ctx.postToWebview({ command: 'pipelineStopped' });
    }
}

/**
 * Cancels any active Gemini code generation task.
 *
 * @param ctx Message context
 * @param _message Incoming IPC message
 */
export async function handleStopCodeGeneration(ctx: MessageContext, _message: any): Promise<void> {
    // Aborting the controller cancels the in-flight Gemini request and makes
    // executeCodeGeneration bail out before touching the output folder.
    sessionOf(ctx).cancelGeneration();
    const channel = ctx.outputChannel || getDesignerOutputChannel();
    channel.appendLine(`[${new Date().toLocaleTimeString()}] Code generation canceled by user.`);
    vscode.window.showInformationMessage("Gemini code generation stopped.");
    ctx.postToWebview({ command: 'codeGenerationStopped' });
}

/**
 * Handler for 'regenerateCode' command.
 * Automatically saves the `.spark` pipeline file to disk and invokes the AI code generation pipeline.
 *
 * @param ctx Message context
 * @param message Incoming IPC message with code generation parameters
 */
export async function handleRegenerateCodeCommand(ctx: MessageContext, message: any): Promise<void> {
    if (isWorkspaceUntrusted()) {
        vscode.window.showErrorMessage(UNTRUSTED_WORKSPACE_MESSAGE);
        ctx.postToWebview({ command: 'codeRegenerated', success: false, error: UNTRUSTED_WORKSPACE_MESSAGE });
        return;
    }

    // Automatically persist the latest .spark YAML and flush the open TextDocument to disk
    const targetUri = ctx.document ? ctx.document.uri : ctx.uri;
    if (targetUri && targetUri.scheme !== 'untitled' && typeof message?.data === 'string') {
        await savePipelineFiles(
            targetUri,
            message.data,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            ctx.syncState
        );
        const openDoc = ctx.document ?? vscode.workspace.textDocuments?.find(
            (doc) => doc.uri.toString() === targetUri.toString()
        );
        if (openDoc && typeof (openDoc as any).save === 'function') {
            await (openDoc as any).save();
        }
    }

    const session = sessionOf(ctx);
    // Starting a new generation aborts any generation still running for this panel.
    const controller = session.beginGeneration();
    try {
        await handleRegenerateCode({
            message,
            context: ctx.context,
            document: ctx.document,
            uri: ctx.uri,
            postToWebview: ctx.postToWebview,
            outputChannel: ctx.outputChannel || getDesignerOutputChannel(),
            abortSignal: controller.signal,
        });
    } finally {
        session.endGeneration(controller);
    }
}
