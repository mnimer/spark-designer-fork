/**
 * Restored `.spark` Tab Upgrade Unit Tests
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * VS Code restores persisted tabs with their original editor type, so a `.spark` file once
 * opened while the extension was not loaded comes back as plain text forever. Verifies that
 * `activate()` reopens such tabs with SPARK_EDITOR_VIEW_TYPE and closes the text tab, while
 * leaving non-`.spark` tabs, custom editor tabs, diff tabs and dirty tabs untouched.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import * as vscode from 'vscode';

const { tabGroupsState, mockCloseTab } = vi.hoisted(() => ({
    tabGroupsState: { all: [] as unknown[] },
    mockCloseTab: vi.fn().mockResolvedValue(true),
}));

vi.mock('vscode', async () => (await import('./helpers/vscodeMock')).createVscodeMock({
    window: {
        tabGroups: {
            get all() {
                return tabGroupsState.all;
            },
            close: mockCloseTab,
        },
    },
    workspace: {
        onDidRenameFiles: vi.fn().mockReturnValue({ dispose: vi.fn() }),
    },
}));

import {
    activate,
    isPlainTextSparkTab,
    upgradeRestoredSparkTextTabs,
    SPARK_EDITOR_VIEW_TYPE,
} from '../../extension';

const uri = (p: string) => ({ scheme: 'file', fsPath: p, path: p, toString: () => `file://${p}` });

const textTab = (p: string, extra: Record<string, unknown> = {}) =>
    ({ input: { uri: uri(p) }, isDirty: false, isActive: false, ...extra }) as unknown as vscode.Tab;

const customTab = (p: string, viewType: string) =>
    ({ input: { uri: uri(p), viewType }, isDirty: false, isActive: false }) as unknown as vscode.Tab;

const diffTab = (p: string) =>
    ({ input: { original: uri(p), modified: uri(p) }, isDirty: false, isActive: false }) as unknown as vscode.Tab;

const flushPromises = () => new Promise(resolve => setTimeout(resolve, 0));

/** Returns the URIs passed to `vscode.openWith`. */
const openWithCalls = () =>
    vi.mocked(vscode.commands.executeCommand).mock.calls.filter(c => c[0] === 'vscode.openWith');

describe('isPlainTextSparkTab', () => {
    it('matches a plain-text .spark tab (case-insensitive extension)', () => {
        expect(isPlainTextSparkTab(textTab('/ws/a.spark'))).toBe(true);
        expect(isPlainTextSparkTab(textTab('/ws/A.SPARK'))).toBe(true);
    });

    it('rejects non-.spark, custom editor, notebook, diff and input-less tabs', () => {
        expect(isPlainTextSparkTab(textTab('/ws/a.yaml'))).toBe(false);
        expect(isPlainTextSparkTab(textTab('/ws/a.spark.bak'))).toBe(false);
        expect(isPlainTextSparkTab(customTab('/ws/a.spark', SPARK_EDITOR_VIEW_TYPE))).toBe(false);
        expect(isPlainTextSparkTab(
            { input: { uri: uri('/ws/a.spark'), notebookType: 'jupyter' } } as unknown as vscode.Tab
        )).toBe(false);
        expect(isPlainTextSparkTab(diffTab('/ws/a.spark'))).toBe(false);
        expect(isPlainTextSparkTab({ input: undefined } as unknown as vscode.Tab)).toBe(false);
    });
});

describe('activate() upgrades restored plain-text .spark tabs', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.mocked(vscode.commands.registerCommand).mockReturnValue({ dispose: vi.fn() });
        vi.mocked(vscode.commands.executeCommand).mockResolvedValue(undefined);
        tabGroupsState.all = [];
    });

    it('reopens a restored text .spark tab with SPARK_EDITOR_VIEW_TYPE in the same column and closes the text tab', async () => {
        const sparkText = textTab('/ws/pipelines/test1.spark', { isActive: true });
        tabGroupsState.all = [{ viewColumn: 2, tabs: [sparkText] }];

        activate({ subscriptions: [] } as unknown as vscode.ExtensionContext);
        await flushPromises();

        const calls = openWithCalls();
        expect(calls).toHaveLength(1);
        expect(calls[0][1]).toBe((sparkText.input as { uri: unknown }).uri);
        expect(calls[0][2]).toBe(SPARK_EDITOR_VIEW_TYPE);
        expect(calls[0][3]).toEqual({ viewColumn: 2, preserveFocus: false, preview: false });
        expect(mockCloseTab).toHaveBeenCalledTimes(1);
        expect(mockCloseTab).toHaveBeenCalledWith(sparkText);
    });

    it('leaves non-.spark text tabs, custom editor tabs, diff tabs and dirty .spark tabs untouched', async () => {
        const yamlText = textTab('/ws/config.yaml');
        const sparkCustom = customTab('/ws/b.spark', SPARK_EDITOR_VIEW_TYPE);
        const otherCustom = customTab('/ws/c.custom', 'other.customEditor');
        const sparkDiff = diffTab('/ws/d.spark');
        const dirtySpark = textTab('/ws/e.spark', { isDirty: true });
        const cleanSpark = textTab('/ws/f.spark');
        tabGroupsState.all = [
            { viewColumn: 1, tabs: [yamlText, sparkCustom, sparkDiff] },
            { viewColumn: 2, tabs: [otherCustom, dirtySpark, cleanSpark] },
        ];

        const upgraded = await upgradeRestoredSparkTextTabs();

        expect(upgraded).toBe(1);
        const calls = openWithCalls();
        expect(calls).toHaveLength(1);
        expect(calls[0][1]).toBe((cleanSpark.input as { uri: unknown }).uri);
        expect(calls[0][3]).toEqual({ viewColumn: 2, preserveFocus: true, preview: false });
        expect(mockCloseTab).toHaveBeenCalledTimes(1);
        expect(mockCloseTab).toHaveBeenCalledWith(cleanSpark);
    });

    it('is a no-op when tabGroups is unavailable', async () => {
        const original = Object.getOwnPropertyDescriptor(vscode.window, 'tabGroups');
        Object.defineProperty(vscode.window, 'tabGroups', { value: undefined, configurable: true });
        try {
            await expect(upgradeRestoredSparkTextTabs()).resolves.toBe(0);
            expect(openWithCalls()).toHaveLength(0);
        } finally {
            if (original) {
                Object.defineProperty(vscode.window, 'tabGroups', original);
            }
        }
    });
});
