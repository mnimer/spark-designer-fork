/**
 * Unit tests for workspace sample save and default output-folder handlers.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies that `handleOpenSample`, `handleReadFolderFiles`, and `handleReadFileContent`
 * normalize `.spark` filenames cleanly without duplicating extensions and derive default
 * sibling folders from `.spark` document paths.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

const workspaceState: { workspaceFolders: Array<{ uri: { fsPath: string; toString: () => string }; name: string }> } = {
    workspaceFolders: [],
};

vi.mock('vscode', async () => {
    const { createVscodeMock } = await import('./helpers/vscodeMock');
    return createVscodeMock({
        workspace: {
            get workspaceFolders() {
                return workspaceState.workspaceFolders;
            },
        },
    });
});

import * as vscode from 'vscode';
import {
    handleOpenSample,
    handleReadFolderFiles,
    handleReadFileContent,
} from '../../ipc/handlers/workspaceHandlers';
import { SPARK_EDITOR_VIEW_TYPE } from '../../core/constants';
import type { MessageContext } from '../../ipc/messageContext';
import type { HostToWebviewMessage } from '../../shared/protocol';

const REPO_ROOT = path.resolve(__dirname, '../../..');

/**
 * Creates a typed MessageContext fixture with a recording webview post sink.
 *
 * @param documentPath Optional active `.spark` document path.
 * @param extensionPath Root path used when discovering bundled sample pipelines.
 * @returns Message context and recorded outbound messages.
 */
function createContext(
    documentPath?: string,
    extensionPath = REPO_ROOT
): { ctx: MessageContext; posted: HostToWebviewMessage[] } {
    const posted: HostToWebviewMessage[] = [];
    const ctx: MessageContext = {
        context: { extensionPath } as unknown as vscode.ExtensionContext,
        webview: {} as unknown as vscode.Webview,
        document: documentPath
            ? ({
                uri: { fsPath: documentPath, toString: () => `file://${documentPath}` },
                getText: () => 'nodes: []\nedges: []\n',
            } as unknown as vscode.TextDocument)
            : undefined,
        postToWebview: vi.fn((msg: HostToWebviewMessage) => {
            posted.push(msg);
            return true;
        }),
    };
    return { ctx, posted };
}

describe('handleOpenSample (.spark filename normalization)', () => {
    let tempWorkspace: string;

    beforeEach(() => {
        vi.clearAllMocks();
        tempWorkspace = fs.mkdtempSync(path.join(os.tmpdir(), 'spark-open-sample-'));
        workspaceState.workspaceFolders = [
            {
                uri: { fsPath: tempWorkspace, toString: () => tempWorkspace },
                name: 'workspace',
            },
        ];
    });

    afterEach(() => {
        fs.rmSync(tempWorkspace, { recursive: true, force: true });
    });

    it('saves gcs-parquet-etl.spark as gcs-parquet-etl.spark without duplicating extensions', async () => {
        const { ctx } = createContext();
        const chosenPath = path.join(tempWorkspace, 'gcs-parquet-etl.spark');
        vi.mocked(vscode.window.showSaveDialog).mockResolvedValue({
            fsPath: chosenPath,
            toString: () => chosenPath,
        } as unknown as vscode.Uri);

        await handleOpenSample(ctx, {
            command: 'openSample',
            filename: 'gcs-parquet-etl.spark',
            content: 'nodes: []\nedges: []\n',
        });

        expect(vscode.window.showSaveDialog).toHaveBeenCalledTimes(1);
        const dialogOpts = vi.mocked(vscode.window.showSaveDialog).mock.calls[0][0];
        expect(dialogOpts?.defaultUri?.fsPath).toBe(chosenPath);
        expect(dialogOpts?.filters).toEqual({ 'Spark Pipeline': ['spark'] });
        expect(fs.readFileSync(chosenPath, 'utf-8')).toBe('nodes: []\nedges: []\n');
        expect(vscode.commands.executeCommand).toHaveBeenCalledWith(
            'spark-pipeline-builder.refreshPipelines'
        );
        expect(vscode.commands.executeCommand).toHaveBeenCalledWith(
            'vscode.openWith',
            expect.objectContaining({ fsPath: chosenPath }),
            SPARK_EDITOR_VIEW_TYPE
        );
    });

    it('appends .spark when given a bare sample slug without an extension', async () => {
        const { ctx } = createContext();
        const expectedPath = path.join(tempWorkspace, 'gcs-parquet-etl.spark');
        vi.mocked(vscode.window.showSaveDialog).mockResolvedValue({
            fsPath: expectedPath,
            toString: () => expectedPath,
        } as unknown as vscode.Uri);

        await handleOpenSample(ctx, {
            command: 'openSample',
            filename: 'gcs-parquet-etl',
            content: 'nodes: []\nedges: []\n',
        });

        const dialogOpts = vi.mocked(vscode.window.showSaveDialog).mock.calls[0][0];
        expect(dialogOpts?.defaultUri?.fsPath).toBe(expectedPath);
        expect(fs.existsSync(expectedPath)).toBe(true);
    });

    it('normalizes uppercase .SPARK suffix without appending a second extension', async () => {
        const { ctx } = createContext();
        const expectedPath = path.join(tempWorkspace, 'GCS-PARQUET-ETL.spark');
        vi.mocked(vscode.window.showSaveDialog).mockResolvedValue({
            fsPath: expectedPath,
            toString: () => expectedPath,
        } as unknown as vscode.Uri);

        await handleOpenSample(ctx, {
            command: 'openSample',
            filename: 'GCS-PARQUET-ETL.SPARK',
            content: 'nodes: []\n',
        });

        const dialogOpts = vi.mocked(vscode.window.showSaveDialog).mock.calls[0][0];
        expect(dialogOpts?.defaultUri?.fsPath).toBe(expectedPath);
    });

    it('falls back to sample-pipeline.spark when filename is omitted or blank', async () => {
        const { ctx } = createContext();
        const expectedPath = path.join(tempWorkspace, 'sample-pipeline.spark');
        vi.mocked(vscode.window.showSaveDialog).mockResolvedValue({
            fsPath: expectedPath,
            toString: () => expectedPath,
        } as unknown as vscode.Uri);

        await handleOpenSample(ctx, {
            command: 'openSample',
            content: 'nodes: []\nedges: []\n',
        });

        const dialogOpts = vi.mocked(vscode.window.showSaveDialog).mock.calls[0][0];
        expect(dialogOpts?.defaultUri?.fsPath).toBe(expectedPath);
        expect(fs.existsSync(expectedPath)).toBe(true);
    });

    it('loads content from bundled samples when content is omitted and appends collision counter if taken', async () => {
        const { ctx } = createContext();
        fs.writeFileSync(path.join(tempWorkspace, 'gcs-parquet-etl.spark'), 'existing\n', 'utf-8');
        const collisionFreePath = path.join(tempWorkspace, 'gcs-parquet-etl-1.spark');
        vi.mocked(vscode.window.showSaveDialog).mockResolvedValue({
            fsPath: collisionFreePath,
            toString: () => collisionFreePath,
        } as unknown as vscode.Uri);

        await handleOpenSample(ctx, {
            command: 'openSample',
            filename: 'gcs-parquet-etl',
        });

        const dialogOpts = vi.mocked(vscode.window.showSaveDialog).mock.calls[0][0];
        expect(dialogOpts?.defaultUri?.fsPath).toBe(collisionFreePath);
        expect(fs.readFileSync(collisionFreePath, 'utf-8')).toContain('ParquetSource');
    });

    it('writes nothing and does not open an editor when user cancels the save dialog', async () => {
        const { ctx } = createContext();
        vi.mocked(vscode.window.showSaveDialog).mockResolvedValue(undefined);

        await handleOpenSample(ctx, {
            command: 'openSample',
            filename: 'gcs-parquet-etl.spark',
            content: 'nodes: []\n',
        });

        expect(fs.readdirSync(tempWorkspace)).toEqual([]);
        expect(vscode.commands.executeCommand).not.toHaveBeenCalled();
    });
});

describe('handleReadFolderFiles and handleReadFileContent default .spark folder derivation', () => {
    let tempDir: string;

    beforeEach(() => {
        vi.clearAllMocks();
        tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'spark-default-folder-'));
        workspaceState.workspaceFolders = [];
    });

    afterEach(() => {
        fs.rmSync(tempDir, { recursive: true, force: true });
    });

    it('handleReadFolderFiles resolves <dir>/<name> when folderPath is omitted for <name>.spark', async () => {
        const sparkDocPath = path.join(tempDir, 'gcs-parquet-etl.spark');
        const siblingOutputDir = path.join(tempDir, 'gcs-parquet-etl');
        fs.mkdirSync(path.join(siblingOutputDir, 'src'), { recursive: true });
        fs.writeFileSync(path.join(siblingOutputDir, 'src', 'pipeline.py'), 'print("ok")\n', 'utf-8');
        fs.writeFileSync(path.join(siblingOutputDir, 'requirements.txt'), 'pyspark>=4.0\n', 'utf-8');

        const { ctx, posted } = createContext(sparkDocPath);
        await handleReadFolderFiles(ctx, { command: 'readFolderFiles' });

        expect(posted).toHaveLength(1);
        expect(posted[0]).toEqual({
            command: 'folderFilesRead',
            folderPath: siblingOutputDir,
            files: expect.arrayContaining(['requirements.txt', 'src/pipeline.py']),
            tokenUsage: undefined,
        });
    });

    it('handleReadFolderFiles posts empty file list with <dir>/<name> when sibling folder does not exist', async () => {
        const sparkDocPath = path.join(tempDir, 'gcs-parquet-etl.spark');
        const expectedFolder = path.join(tempDir, 'gcs-parquet-etl');

        const { ctx, posted } = createContext(sparkDocPath);
        await handleReadFolderFiles(ctx, { command: 'readFolderFiles' });

        expect(posted).toEqual([
            {
                command: 'folderFilesRead',
                folderPath: expectedFolder,
                files: [],
            },
        ]);
    });

    it('handleReadFileContent reads files inside <dir>/<name> when folderPath is omitted for <name>.spark', async () => {
        const sparkDocPath = path.join(tempDir, 'gcs-parquet-etl.spark');
        const siblingOutputDir = path.join(tempDir, 'gcs-parquet-etl');
        fs.mkdirSync(path.join(siblingOutputDir, 'src'), { recursive: true });
        fs.writeFileSync(path.join(siblingOutputDir, 'src', 'pipeline.py'), '# spark etl\n', 'utf-8');

        const { ctx, posted } = createContext(sparkDocPath);
        await handleReadFileContent(ctx, {
            command: 'readFileContent',
            filePath: 'src/pipeline.py',
        });

        expect(posted).toEqual([
            {
                command: 'fileContentLoaded',
                filePath: 'src/pipeline.py',
                content: '# spark etl\n',
            },
        ]);
    });

    it('handleReadFileContent blocks traversal outside <dir>/<name> when folderPath is omitted', async () => {
        const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined);
        try {
            const sparkDocPath = path.join(tempDir, 'gcs-parquet-etl.spark');
            const siblingOutputDir = path.join(tempDir, 'gcs-parquet-etl');
            fs.mkdirSync(siblingOutputDir, { recursive: true });
            fs.writeFileSync(path.join(tempDir, 'outside.txt'), 'secret', 'utf-8');

            const { ctx, posted } = createContext(sparkDocPath);
            await handleReadFileContent(ctx, {
                command: 'readFileContent',
                filePath: '../outside.txt',
            });

            expect(posted).toEqual([
                {
                    command: 'fileContentError',
                    filePath: '../outside.txt',
                    error: 'Blocked attempt to read file outside folder',
                },
            ]);
        } finally {
            consoleErrorSpy.mockRestore();
        }
    });
});
