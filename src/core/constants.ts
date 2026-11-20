/** @fileoverview Central configuration constants, pipeline defaults, and search exclude patterns.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import YAML from 'yaml';
import { DEFAULT_IGNORE_PATTERNS } from '../resources/resourceLoader';
import { DEFAULT_PIPELINE_PARADIGM, type PipelineParadigm } from '../shared/types';

/**
 * File extension for Apache Spark pipeline design files.
 */
export const PIPELINE_FILE_EXT = '.spark';

/**
 * View type of the custom Spark pipeline editor.
 *
 * Keep this value synchronized with `contributes.customEditors[0].viewType` and
 * `onCustomEditor:` in package.json so VS Code opens pipeline files in the visual designer.
 */
export const SPARK_EDITOR_VIEW_TYPE = 'spark-pipeline-builder.sparkEditor';

/**
 * Generates initial starter pipeline document content serialized in YAML format.
 * Keeps newly created .spark pipeline files byte-identical to pipelines saved from the webview.
 *
 * @param paradigm Target pipeline authoring paradigm ('imperative' default or 'sdp').
 * @returns YAML-serialized starter pipeline content.
 */
export function getInitialPipelineContent(paradigm: PipelineParadigm = DEFAULT_PIPELINE_PARADIGM): string {
    const resolvedParadigm: PipelineParadigm = paradigm === 'sdp' ? 'sdp' : 'imperative';
    // NOTE: Key order and key set match the webview PipelineState serializer in usePipelinePersistence.ts.
    // Matching both keeps a newly created pipeline structurally identical to a saved document.
    return YAML.stringify({
        paradigm: resolvedParadigm,
        nodes: [],
        edges: [],
        runtimeVariables: [],
        jobConfig:
            resolvedParadigm === 'sdp'
                ? {
                    runner: 'ServerlessSpark',
                    runtimeVersion: '4.0',
                    machineType: 'n1-standard-1',
                    minInstances: 1,
                    maxInstances: 10,
                    gcsStagingPath: '',
                    network: '',
                    subnetwork: ''
                }
                : {
                    runner: 'Local',
                    machineType: 'n1-standard-1',
                    minInstances: 1,
                    maxInstances: 10,
                    gcsStagingPath: '',
                    network: '',
                    subnetwork: ''
                },
        pipelineConfig: {
            language: 'PySpark',
            outputFolder: '',
            needsRebuild: false
        }
    });
}

/**
 * Directory names to exclude when scanning workspace files for pipeline definitions.
 * Combines directory patterns from DEFAULT_IGNORE_PATTERNS with standard build output directories.
 */
export const PIPELINE_SEARCH_EXCLUDE_DIRS: readonly string[] = [
    ...DEFAULT_IGNORE_PATTERNS
        .filter(p => !p.startsWith('*') && !p.endsWith('.spark') && !p.endsWith('.tf') && !p.endsWith('.log'))
        .map(p => p.replace(/\/$/, '')),
    'dist',
    'build',
    'out'
];

/**
 * Glob pattern for excluding dependency, build, and VCS directories when scanning
 * workspace files for .spark pipelines.
 */
export const PIPELINE_SEARCH_EXCLUDE_PATTERN: string = `{${PIPELINE_SEARCH_EXCLUDE_DIRS.map(dir => `**/${dir}/**`).join(',')}}`;

/**
 * Directory names to skip when listing the files of a generated pipeline project
 * for display in the Source Code tab.
 *
 * The Source Code tab is meant to show the ~10 files the generator produced. Running the
 * pipeline once creates a virtualenv (`venv`, created by the generated run_directrunner.sh)
 * or a Maven `target/` directory, either of which adds thousands of entries and makes the
 * tab unusable.
 *
 * NOTE: the walker independently skips every dot-prefixed entry, which already covers
 * `.venv`, `.git` and friends. This list therefore exists for the directories that do NOT
 * start with a dot -- `venv` being the one that actually caused the reported bug. The
 * dot-prefixed names are still included so the list stays correct on its own terms if the
 * walker's dot rule is ever relaxed.
 */
export const GENERATED_FILE_LIST_EXCLUDE_DIRS: readonly string[] = PIPELINE_SEARCH_EXCLUDE_DIRS;
