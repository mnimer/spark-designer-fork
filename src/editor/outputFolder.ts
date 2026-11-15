/**
 * @fileoverview Helpers for resolving pipeline output folders and generating collision-free .beam filenames.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import * as path from 'path';
import * as fs from 'fs';
import { PIPELINE_FILE_EXT } from '../core/constants';

/**
 * Resolves the output folder path for a pipeline, falling back to a default folder next to the .spark file.
 *
 * @param pipelineFsPath Filesystem path to the active .spark file, if any.
 * @param outputFolder Explicit output folder override configured on the pipeline, if any.
 * @param workspaceFolder Root workspace folder path, if any.
 * @returns The resolved output folder path, or empty string if it cannot be determined.
 */
export function resolveOutputFolder(
    pipelineFsPath: string | undefined,
    outputFolder: string | undefined,
    workspaceFolder: string | undefined
): string {
    const defaultFolder = pipelineFsPath
        ? path.join(path.dirname(pipelineFsPath), path.basename(pipelineFsPath, PIPELINE_FILE_EXT))
        : '';

    if (outputFolder && outputFolder.trim()) {
        if (path.isAbsolute(outputFolder)) {
            return outputFolder;
        } else if (workspaceFolder) {
            return path.join(workspaceFolder, outputFolder);
        } else {
            return defaultFolder;
        }
    }

    return defaultFolder;
}

/**
 * Generates the next available collision-free filename with a .spark extension in a directory.
 *
 * @param dir Directory where the candidate filename will reside.
 * @param baseName Preferred base filename (with or without .spark suffix).
 * @param exists Optional filesystem existence checker, defaulting to fs.existsSync.
 * @returns The candidate filename with a .spark extension that does not currently exist.
 */
export function nextAvailableSparkFilename(
    dir: string,
    baseName: string,
    exists: (p: string) => boolean = fs.existsSync
): string {
    const base = baseName.replace(/\.spark$/, '');
    let candidate = `${base}${PIPELINE_FILE_EXT}`;
    let counter = 1;
    while (exists(path.join(dir, candidate))) {
        candidate = `${base}-${counter}${PIPELINE_FILE_EXT}`;
        counter++;
    }
    return candidate;
}
