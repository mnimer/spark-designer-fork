/**
 * Unit tests for selected Gemini model routing in `executeCodeGeneration` and `handleRegenerateCode`.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Verifies that custom Gemini 3.x and 4.x model selections flow through `handleRegenerateCode`,
 * `executeCodeGeneration`, `gemini.log` (both success and failure paths), and `runReviewerPass`
 * (both review and fix calls), while omitted models default to `DEFAULT_GEMINI_MODEL` and
 * deprecated `gemini-2.5-*` models are rejected cleanly.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

const generateContentMock = vi.fn();

vi.mock('@google/genai', () => ({
    GoogleGenAI: class {
        public models = { generateContent: generateContentMock };
    },
}));

const showErrorMessage = vi.fn();
const withProgressMock = vi.fn(
    async (
        _options: unknown,
        task: (
            progress: { report: () => void },
            token: { isCancellationRequested: boolean; onCancellationRequested: () => { dispose: () => void } }
        ) => Promise<unknown>
    ) =>
        task(
            { report: () => undefined },
            {
                isCancellationRequested: false,
                onCancellationRequested: () => ({ dispose: () => undefined }),
            }
        )
);

vi.mock('vscode', async () =>
    (await import('../helpers/vscodeMock')).createVscodeMock({
        workspace: {
            workspaceFolders: [],
        },
        window: {
            showErrorMessage: (...args: unknown[]) => showErrorMessage(...args),
            withProgress: (...args: Parameters<typeof withProgressMock>) => withProgressMock(...args),
        },
    })
);

import { executeCodeGeneration, GEMINI_LOG_FILENAME, handleRegenerateCode } from '../../../codeGeneration';
import { DEFAULT_GEMINI_MODEL } from '../../../constants';
import type { HostToWebviewMessage } from '../../../shared/protocol';

const REPO_ROOT = path.resolve(__dirname, '../../../..');
const FAKE_CONTEXT = { asAbsolutePath: (rel: string) => path.join(REPO_ROOT, rel) } as never;

const VALID_GENERATION_RESPONSE = {
    text: JSON.stringify({
        files: [{ filename: 'src/orders.py', content: 'print("ok")\n' }],
    }),
    candidates: [{ finishReason: 'STOP' }],
    usageMetadata: { promptTokenCount: 12, candidatesTokenCount: 24 },
};

describe('codeGeneration model routing (MOD1)', () => {
    let tempDir: string;

    beforeEach(() => {
        tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'spark-codegen-model-'));
        generateContentMock.mockReset();
        showErrorMessage.mockReset();
        withProgressMock.mockClear();
    });

    afterEach(() => {
        fs.rmSync(tempDir, { recursive: true, force: true });
    });

    it('defaults to DEFAULT_GEMINI_MODEL when model is omitted', async () => {
        generateContentMock.mockResolvedValue(VALID_GENERATION_RESPONSE);
        const posted: HostToWebviewMessage[] = [];

        await executeCodeGeneration({
            targetFolderPath: tempDir,
            yamlData: 'nodes: []\nedges: []\n',
            languageVal: 'PySpark',
            baseName: 'orders',
            generationPipelineId: 'orders-id',
            folderGate: { ownership: 'fresh', manifest: null, mayClean: false },
            context: FAKE_CONTEXT,
            postToWebview: (msg) => {
                posted.push(msg);
                return true;
            },
            getGcloudProjectFn: async () => 'test-gcp-project',
        });

        expect(DEFAULT_GEMINI_MODEL).toBe('gemini-4-hydrogen');
        expect(generateContentMock).toHaveBeenCalledTimes(1);
        expect(generateContentMock.mock.calls[0][0].model).toBe(DEFAULT_GEMINI_MODEL);

        const logJson = JSON.parse(fs.readFileSync(path.join(tempDir, GEMINI_LOG_FILENAME), 'utf-8'));
        expect(logJson.model).toBe(DEFAULT_GEMINI_MODEL);
        expect(posted).toContainEqual(expect.objectContaining({ command: 'codeRegenerated', success: true }));
    });

    it('routes custom gemini-4-carbon model into generator, reviewer pass, outputChannel, and gemini.log', async () => {
        generateContentMock
            .mockResolvedValueOnce(VALID_GENERATION_RESPONSE)
            .mockResolvedValueOnce({
                text: JSON.stringify({
                    findings: [
                        {
                            file: 'src/orders.py',
                            severity: 'blocking',
                            problem: 'missing entrypoint',
                            fix: 'add main',
                            skillRule: 'core-pyspark',
                        },
                    ],
                    summary: 'needs fix',
                }),
            })
            .mockResolvedValueOnce({
                text: JSON.stringify({
                    files: [{ filename: 'src/orders.py', content: 'print("reviewed")\n' }],
                }),
            })
            .mockResolvedValueOnce({
                text: JSON.stringify({ findings: [], summary: 'all clear' }),
            });

        const channelLines: string[] = [];
        const outputChannel = { appendLine: (line: string) => channelLines.push(line) } as never;

        await executeCodeGeneration({
            targetFolderPath: tempDir,
            yamlData: 'nodes: []\nedges: []\n',
            languageVal: 'PySpark',
            baseName: 'orders',
            generationPipelineId: 'orders-id',
            folderGate: { ownership: 'fresh', manifest: null, mayClean: false },
            context: FAKE_CONTEXT,
            postToWebview: () => true,
            outputChannel,
            getGcloudProjectFn: async () => 'test-gcp-project',
            enableReviewer: true,
            model: 'gemini-4-carbon',
        });

        expect(generateContentMock).toHaveBeenCalledTimes(4);
        const invokedModels = generateContentMock.mock.calls.map((c) => c[0].model);
        expect(invokedModels).toEqual([
            'gemini-4-carbon',
            'gemini-4-carbon',
            'gemini-4-carbon',
            'gemini-4-carbon',
        ]);
        expect(channelLines).toContain('[codegen] model: gemini-4-carbon');

        const logJson = JSON.parse(fs.readFileSync(path.join(tempDir, GEMINI_LOG_FILENAME), 'utf-8'));
        expect(logJson.model).toBe('gemini-4-carbon');
        expect(fs.readFileSync(path.join(tempDir, 'src', 'orders.py'), 'utf-8')).toBe(
            'print("reviewed")\n'
        );
    });

    it('records the resolved custom model in error gemini.log when SDK call fails', async () => {
        generateContentMock.mockRejectedValueOnce(new Error('Quota exceeded for gemini-3.8-flash'));
        const posted: HostToWebviewMessage[] = [];

        await executeCodeGeneration({
            targetFolderPath: tempDir,
            yamlData: 'nodes: []\nedges: []\n',
            languageVal: 'PySpark',
            baseName: 'orders',
            generationPipelineId: 'orders-id',
            folderGate: { ownership: 'fresh', manifest: null, mayClean: false },
            context: FAKE_CONTEXT,
            postToWebview: (msg) => {
                posted.push(msg);
                return true;
            },
            getGcloudProjectFn: async () => 'test-gcp-project',
            model: 'gemini-3.8-flash',
        });

        const logJson = JSON.parse(fs.readFileSync(path.join(tempDir, GEMINI_LOG_FILENAME), 'utf-8'));
        expect(logJson.model).toBe('gemini-3.8-flash');
        expect(logJson.error).toContain('Quota exceeded for gemini-3.8-flash');
        expect(posted).toContainEqual(
            expect.objectContaining({
                command: 'codeRegenerated',
                success: false,
                error: expect.stringContaining('Quota exceeded'),
            })
        );
    });

    it('rejects deprecated gemini-2.5-flash without invoking the Gemini SDK', async () => {
        const posted: HostToWebviewMessage[] = [];

        await executeCodeGeneration({
            targetFolderPath: tempDir,
            yamlData: 'nodes: []\nedges: []\n',
            languageVal: 'PySpark',
            baseName: 'orders',
            generationPipelineId: 'orders-id',
            folderGate: { ownership: 'fresh', manifest: null, mayClean: false },
            context: FAKE_CONTEXT,
            postToWebview: (msg) => {
                posted.push(msg);
                return true;
            },
            getGcloudProjectFn: async () => 'test-gcp-project',
            model: 'gemini-2.5-flash',
        });

        expect(generateContentMock).not.toHaveBeenCalled();
        expect(showErrorMessage).toHaveBeenCalledWith(
            expect.stringContaining("Unsupported or deprecated Gemini code generation model: 'gemini-2.5-flash'")
        );
        expect(posted).toContainEqual(
            expect.objectContaining({
                command: 'codeRegenerated',
                success: false,
                error: expect.stringContaining('gemini-2.5-flash'),
            })
        );
    });

    it('forwards message.model through handleRegenerateCode into executeCodeGeneration and progress title', async () => {
        generateContentMock.mockResolvedValueOnce(VALID_GENERATION_RESPONSE);
        const posted: HostToWebviewMessage[] = [];
        const channelLines: string[] = [];
        const outputChannel = { appendLine: (line: string) => channelLines.push(line) } as never;

        await handleRegenerateCode({
            message: {
                command: 'regenerateCode',
                data: 'nodes: []\nedges: []\n',
                filename: 'orders',
                language: 'PySpark',
                outputFolder: tempDir,
                model: '  gemini-4.1-flash  ',
            },
            context: FAKE_CONTEXT,
            uri: { fsPath: path.join(tempDir, 'orders.spark') } as never,
            postToWebview: (msg) => {
                posted.push(msg);
                return true;
            },
            outputChannel,
            getGcloudProjectFn: async () => 'test-gcp-project',
        });

        expect(withProgressMock).toHaveBeenCalledTimes(1);
        expect((withProgressMock.mock.calls[0][0] as { title: string }).title).toContain(
            'gemini-4.1-flash'
        );
        expect(channelLines.some((line) => line.includes('model: gemini-4.1-flash'))).toBe(true);
        expect(generateContentMock).toHaveBeenCalledTimes(1);
        expect(generateContentMock.mock.calls[0][0].model).toBe('gemini-4.1-flash');
        expect(posted).toContainEqual(expect.objectContaining({ command: 'codeRegenerated', success: true }));
    });
});
