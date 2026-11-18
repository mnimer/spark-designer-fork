/** @fileoverview Upgrades restored plain-text `.spark` tabs to the Spark pipeline designer.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Why this exists:
 * VS Code persists every open tab together with the editor type it was opened in, and on
 * window reload it restores the tab with that same editor type -- the editor resolver (and
 * therefore `customEditors` / `workbench.editorAssociations`) is never consulted. If a
 * `.spark` file was ever opened while this extension was not loaded (for example in an
 * Extension Development Host for a different extension that shares the same workspace
 * folder), the tab is persisted as a plain text editor and comes back as a text editor on
 * every subsequent launch. The extension then activates via `onLanguage:spark` instead of
 * `onCustomEditor:`, and the user only ever sees YAML. This helper runs once at activation
 * and reopens such tabs in the designer.
 */

import * as vscode from 'vscode';
import { PIPELINE_FILE_EXT, SPARK_EDITOR_VIEW_TYPE } from '../core/constants';

/**
 * Returns true when the tab is a plain text editor tab showing a `.spark` file.
 *
 * Structural check (rather than `instanceof vscode.TabInputText`) so it works against the
 * unit-test vscode mock. A `TabInputText` only carries `uri`; custom editor, notebook and
 * diff tab inputs additionally carry `viewType`, `notebookType` or `original`/`modified`.
 *
 * @param tab The tab to inspect.
 * @returns Whether the tab is a plain-text `.spark` tab that should be upgraded.
 */
export function isPlainTextSparkTab(tab: vscode.Tab): boolean {
    const input = tab.input as Record<string, unknown> | undefined | null;
    if (!input || typeof input !== 'object') {
        return false;
    }
    if ('viewType' in input || 'notebookType' in input || 'original' in input || 'modified' in input) {
        return false;
    }
    const uri = input.uri as vscode.Uri | undefined;
    const fsPath = uri?.fsPath ?? uri?.path;
    return typeof fsPath === 'string' && fsPath.toLowerCase().endsWith(PIPELINE_FILE_EXT);
}

/**
 * Reopens every restored plain-text `.spark` tab in the Spark pipeline designer and closes
 * the plain-text tab. Dirty text tabs are left alone so unsaved edits never trigger a save
 * prompt or get lost.
 *
 * @returns The number of tabs that were upgraded.
 */
export async function upgradeRestoredSparkTextTabs(): Promise<number> {
    const tabGroups = vscode.window.tabGroups;
    if (!tabGroups?.all) {
        return 0;
    }

    let upgraded = 0;
    for (const group of tabGroups.all) {
        // Copy: closing tabs mutates group.tabs.
        for (const tab of [...group.tabs]) {
            if (!isPlainTextSparkTab(tab) || tab.isDirty) {
                continue;
            }
            const uri = (tab.input as { uri: vscode.Uri }).uri;
            await vscode.commands.executeCommand('vscode.openWith', uri, SPARK_EDITOR_VIEW_TYPE, {
                viewColumn: group.viewColumn,
                preserveFocus: !tab.isActive,
                preview: false,
            });
            await tabGroups.close(tab);
            upgraded++;
        }
    }
    return upgraded;
}
