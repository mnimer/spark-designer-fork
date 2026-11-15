/**
 * @fileoverview Utility for computing pipeline relative and default output folder locations.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import * as path from 'path';
import { PIPELINE_FILE_EXT } from '../core/constants';
import { isSubPath } from '../security/pathGuard';

/**
 * Result of computing relative and default output folders for a pipeline file.
 */
export interface PipelineLocation {
    /** Relative directory of the pipeline file from workspaceFolder, or empty string. */
    relativeFolder: string;
    /** Default output folder path (relativeFolder + filenameWithoutExt, or just filenameWithoutExt). */
    defaultOutputFolder: string;
}

/**
 * Computes relativeFolder and defaultOutputFolder for a .spark file given an optional workspace folder.
 *
 * @param sparkFsPath The filesystem path of the .spark file.
 * @param workspaceFolder The filesystem path of the root workspace folder, if defined.
 * @returns An object containing relativeFolder and defaultOutputFolder.
 */
export function computePipelineLocation(
    sparkFsPath: string,
    workspaceFolder: string | undefined
): PipelineLocation {
    let relativeFolder = '';
    const sparkFileDir = path.dirname(sparkFsPath);
    if (workspaceFolder && isSubPath(workspaceFolder, sparkFileDir)) {
        relativeFolder = path.relative(workspaceFolder, sparkFileDir);
    }
    const filenameWithoutExt = path.basename(sparkFsPath, PIPELINE_FILE_EXT);
    const defaultOutputFolder = relativeFolder
        ? path.join(relativeFolder, filenameWithoutExt)
        : filenameWithoutExt;

    return { relativeFolder, defaultOutputFolder };
}
