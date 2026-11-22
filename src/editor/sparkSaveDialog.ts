/**
 * @fileoverview Helper for prompting the user with a Save dialog for a .spark pipeline file.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import * as vscode from 'vscode';
import * as path from 'path';
import { PIPELINE_FILE_EXT } from '../core/constants';
import { nextAvailableSparkFilename } from './outputFolder';

/**
 * Prompts the user to choose a save location for a .spark pipeline file.
 *
 * @param title Dialog title displayed to the user.
 * @param baseName Preferred base filename or candidate filename (e.g. 'pipeline' or sample filename).
 * @returns The chosen Uri, or undefined if the user cancelled the dialog.
 */
export async function promptForSparkSaveLocation(
    title: string,
    baseName: string
): Promise<vscode.Uri | undefined> {
    const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
    const defaultUri = workspaceFolder
        ? vscode.Uri.file(path.join(workspaceFolder, nextAvailableSparkFilename(workspaceFolder, baseName)))
        : undefined;
    return vscode.window.showSaveDialog({
        defaultUri,
        title,
        filters: { 'Spark Pipeline': [PIPELINE_FILE_EXT.slice(1)] }
    });
}
