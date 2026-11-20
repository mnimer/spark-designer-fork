/**
 * @fileoverview Unit tests for Spark Declarative Pipelines overlapping-run guard and paradigm forwarding in executionHandlers (SDP9).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const vscodeState = vi.hoisted(() => ({ isTrusted: true as boolean | undefined }));

vi.mock('vscode', () => ({
    TreeItem: class {},
    TreeItemCollapsibleState: { None: 0, Collapsed: 1, Expanded: 2 },
    EventEmitter: class {
        public fire = vi.fn();
        public event = vi.fn();
    },
    ThemeIcon: class {},
    ThemeColor: class {},
    MarkdownString: class {},
    CancellationTokenSource: class {
        public token = { isCancellationRequested: false };
        public cancel = vi.fn();
        public dispose = vi.fn();
    },
    workspace: {
        get isTrusted() {
            return vscodeState.isTrusted;
        },
        workspaceFolders: [],
        findFiles: vi.fn().mockResolvedValue([]),
        getConfiguration: vi.fn(() => ({ get: vi.fn() })),
    },
    Uri: {
        file: (p: string) => ({ scheme: 'file', fsPath: p, toString: () => p }),
        parse: (u: string) => ({ scheme: 'file', fsPath: u, toString: () => u }),
    },
    commands: { registerCommand: vi.fn(), executeCommand: vi.fn() },
    window: {
        showErrorMessage: vi.fn(),
        showWarningMessage: vi.fn(),
        showInformationMessage: vi.fn(),
        createOutputChannel: vi.fn(() => ({
            show: vi.fn(),
            append: vi.fn(),
            appendLine: vi.fn(),
            dispose: vi.fn(),
        })),
    },
    env: { openExternal: vi.fn() },
}));

const runnerMocks = vi.hoisted(() => ({
    spawnCalls: [] as Array<{ folderPath: string; scriptFullPath: string; env: Record<string, string>; child: any }>,
    envInputs: [] as Array<any>,
    candidateCalls: [] as Array<{ runner: string; paradigm?: string }>,
}));

vi.mock('../../../execution/PipelineRunner', async (importOriginal) => {
    const { EventEmitter: EE } = await import('events');
    const actual = await importOriginal<typeof import('../../../execution/PipelineRunner')>();
    return {
        ...actual,
        runnerScriptCandidates: vi.fn((runner: string, paradigm?: string) => {
            runnerMocks.candidateCalls.push({ runner, paradigm });
            return actual.runnerScriptCandidates(runner);
        }),
        buildRunnerEnvironment: vi.fn((options: any) => {
            runnerMocks.envInputs.push(options);
            return actual.buildRunnerEnvironment(options);
        }),
        resolveRunTarget: vi.fn(async (jobConfig: any) => ({
            project: jobConfig?.projectId || 'resolved-project',
            region: jobConfig?.region || 'us-central1',
        })),
        spawnRunnerScript: vi.fn((folderPath: string, scriptFullPath: string, env: Record<string, string>, callbacks: any) => {
            const child: any = new EE();
            child.stdout = new EE();
            child.stderr = new EE();
            child.pid = undefined;
            child.kill = vi.fn();
            child._callbacks = callbacks;
            runnerMocks.spawnCalls.push({ folderPath, scriptFullPath, env, child });
            return child;
        }),
    };
});

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';
import { EditorSession } from '../../../editor/EditorSession';
import {
    handleRunPipeline,
    SDP_OVERLAPPING_RUN_MESSAGE,
} from '../../../ipc/handlers/executionHandlers';
import type { MessageContext } from '../../../ipc/messageContext';

/**
 * Builds a minimal MessageContext bound to an isolated EditorSession and temporary .spark URI.
 *
 * @param session Active panel session.
 * @param sparkPath Temporary .spark file path.
 * @returns Message context and postToWebview spy.
 */
function makeRunContext(session: EditorSession, sparkPath: string) {
    const postToWebview = vi.fn(() => true);
    const ctx = {
        context: {} as any,
        webview: {} as any,
        postToWebview,
        session,
        uri: vscode.Uri.file(sparkPath) as any,
        outputChannel: { show: vi.fn(), append: vi.fn(), appendLine: vi.fn() } as any,
    } as MessageContext;
    return { ctx, postToWebview };
}

describe('SDP overlapping-run guard and paradigm forwarding (SDP9)', () => {
    let tmpDir: string;
    let sparkPath: string;

    beforeEach(() => {
        runnerMocks.spawnCalls.length = 0;
        runnerMocks.envInputs.length = 0;
        runnerMocks.candidateCalls.length = 0;
        vscodeState.isTrusted = true;
        vi.mocked(vscode.window.showErrorMessage).mockClear();

        tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sdp-exec-guard-'));
        fs.mkdirSync(path.join(tmpDir, 'medallion'));
        fs.writeFileSync(path.join(tmpDir, 'medallion', 'run_local.sh'), '#!/bin/bash\necho ok\n');
        sparkPath = path.join(tmpDir, 'medallion.spark');
    });

    it('rejects an overlapping SDP run without killing the active child process when pipelineConfig.paradigm is sdp', async () => {
        const session = new EditorSession();
        const { ctx, postToWebview } = makeRunContext(session, sparkPath);

        // Start first SDP run
        await handleRunPipeline(ctx, {
            command: 'runPipeline',
            jobConfig: {
                runner: 'Local',
                sdpStorage: 'gs://sdp-checkpoints/medallion',
                sdpCommand: 'run',
                sdpRefreshMode: 'incremental',
            },
            pipelineConfig: {
                language: 'Python',
                outputFolder: '',
                needsRebuild: false,
                paradigm: 'sdp',
            },
        });

        expect(runnerMocks.spawnCalls).toHaveLength(1);
        const firstChild = runnerMocks.spawnCalls[0].child;
        expect(session.activeProcess).toBe(firstChild);

        // Forwarded paradigm to describeRunner, runnerScriptCandidates, and buildRunnerEnvironment
        expect(runnerMocks.candidateCalls[0]).toEqual({ runner: 'Local', paradigm: 'sdp' });
        expect(runnerMocks.envInputs[0].jobConfig.paradigm).toBe('sdp');
        expect(postToWebview).toHaveBeenCalledWith(
            expect.objectContaining({
                command: 'pipelineStarted',
                commandLine: expect.stringContaining('# Running Spark Declarative Pipeline via Local Spark 4.2 Connect'),
            })
        );

        postToWebview.mockClear();

        // Attempt overlapping second SDP run while firstChild is still active
        await handleRunPipeline(ctx, {
            command: 'runPipeline',
            jobConfig: {
                runner: 'Local',
                sdpStorage: 'gs://sdp-checkpoints/medallion',
            },
            pipelineConfig: {
                language: 'Python',
                outputFolder: '',
                needsRebuild: false,
                paradigm: 'sdp',
            },
        });

        // Must NOT kill firstChild or spawn a second process
        expect(firstChild.kill).not.toHaveBeenCalled();
        expect(session.activeProcess).toBe(firstChild);
        expect(runnerMocks.spawnCalls).toHaveLength(1);
        expect(vscode.window.showErrorMessage).toHaveBeenCalledWith(SDP_OVERLAPPING_RUN_MESSAGE);
        expect(postToWebview).toHaveBeenCalledWith(
            expect.objectContaining({
                command: 'pipelineLog',
                stream: 'stderr',
                text: expect.stringContaining(SDP_OVERLAPPING_RUN_MESSAGE),
            })
        );
        // Must NOT mark the still-running first session as finished
        expect(postToWebview).not.toHaveBeenCalledWith(
            expect.objectContaining({ command: 'pipelineFinished' })
        );

        // Once the first run finishes cleanly, a subsequent SDP run is allowed
        firstChild._callbacks.onClose(0, null);
        expect(session.activeProcess).toBeNull();

        await handleRunPipeline(ctx, {
            command: 'runPipeline',
            jobConfig: { runner: 'Local' },
            pipelineConfig: { language: 'Python', outputFolder: '', needsRebuild: false, paradigm: 'sdp' },
        });
        expect(runnerMocks.spawnCalls).toHaveLength(2);
    });

    it('also blocks overlapping SDP runs when paradigm is supplied on jobConfig.paradigm', async () => {
        const session = new EditorSession();
        const { ctx } = makeRunContext(session, sparkPath);

        await handleRunPipeline(ctx, {
            command: 'runPipeline',
            jobConfig: { runner: 'Local', paradigm: 'sdp' },
        });
        expect(runnerMocks.spawnCalls).toHaveLength(1);

        await handleRunPipeline(ctx, {
            command: 'runPipeline',
            jobConfig: { runner: 'Local', paradigm: 'sdp' },
        });
        expect(runnerMocks.spawnCalls).toHaveLength(1);
        expect(vscode.window.showErrorMessage).toHaveBeenCalledWith(SDP_OVERLAPPING_RUN_MESSAGE);
    });

    it('preserves imperative restart behavior when paradigm is imperative', async () => {
        const session = new EditorSession();
        const { ctx } = makeRunContext(session, sparkPath);

        await handleRunPipeline(ctx, {
            command: 'runPipeline',
            jobConfig: { runner: 'Local' },
            pipelineConfig: { language: 'Python', outputFolder: '', needsRebuild: false, paradigm: 'imperative' },
        });
        const firstChild = runnerMocks.spawnCalls[0].child;

        await handleRunPipeline(ctx, {
            command: 'runPipeline',
            jobConfig: { runner: 'Local' },
            pipelineConfig: { language: 'Python', outputFolder: '', needsRebuild: false, paradigm: 'imperative' },
        });
        expect(firstChild.kill).toHaveBeenCalledWith('SIGINT');
        expect(runnerMocks.spawnCalls).toHaveLength(2);
    });
});
