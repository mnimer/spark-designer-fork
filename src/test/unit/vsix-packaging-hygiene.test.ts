/**
 * VSIX Packaging Hygiene and Integration Script Unit Tests
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates package.json integration scripts, custom editor registration invariants,
 * and .vscodeignore exclusion rules so packaged VSIX archives contain only runtime files.
 */

import { afterAll, describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { createRequire } from 'module';
import { PIPELINE_FILE_EXT, SPARK_EDITOR_VIEW_TYPE } from '../../core/constants';

const req = createRequire(__filename);

interface VsceApi {
    PackageManager: {
        Npm: number;
        Yarn: number;
        None: number;
    };
    listFiles(options?: {
        cwd?: string;
        packageManager?: number;
        ignoreFile?: string;
    }): Promise<string[]>;
}

type MinimatchFn = (target: string, pattern: string, options?: { dot?: boolean }) => boolean;

interface PackageJsonShape {
    name: string;
    version: string;
    main: string;
    activationEvents?: string[];
    scripts?: Record<string, string>;
    contributes?: {
        languages?: Array<{ id?: string; extensions?: string[] }>;
        configurationDefaults?: {
            'workbench.editorAssociations'?: Record<string, string>;
        };
        customEditors?: Array<{
            viewType?: string;
            selector?: Array<{ filenamePattern?: string }>;
        }>;
    };
}

const vsce = req('@vscode/vsce') as VsceApi;
const minimatchModule = req('minimatch') as { minimatch: MinimatchFn } | MinimatchFn;
const minimatch: MinimatchFn =
    typeof minimatchModule === 'function' ? minimatchModule : minimatchModule.minimatch;

const ROOT_DIR = path.resolve(__dirname, '../../../');
const PACKAGE_JSON_PATH = path.join(ROOT_DIR, 'package.json');
const VSCODEIGNORE_PATH = path.join(ROOT_DIR, '.vscodeignore');

/**
 * Parses raw .vscodeignore content using the vsce ignore and negate algorithm.
 *
 * @param rawIgnore Raw text from a .vscodeignore file.
 * @returns Parsed ignore and negate pattern arrays.
 */
function parseVscodeIgnoreRules(rawIgnore: string): { ignore: string[]; negate: string[] } {
    const lines = rawIgnore
        .split(/[\n\r]/)
        .map((line) => line.trim())
        .filter((line) => line.length > 0 && !line.startsWith('#'));

    const expanded = [
        ...lines,
        ...lines
            .filter((entry) => !/(^|\/)[^/]*\*[^/]*$/.test(entry))
            .map((entry) => (entry.endsWith('/') ? `${entry}**` : `${entry}/**`)),
    ];

    const ignore: string[] = [];
    const negate: string[] = [];
    for (const pattern of expanded) {
        if (pattern.startsWith('!')) {
            negate.push(pattern.slice(1));
        } else {
            ignore.push(pattern);
        }
    }
    return { ignore, negate };
}

/**
 * Evaluates whether a relative POSIX path survives .vscodeignore filtering.
 *
 * @param relativePath Forward-slash path relative to the extension root.
 * @param rawIgnore Raw text from a .vscodeignore file.
 * @returns True when the file remains included in the packaged archive.
 */
function isIncludedByVscodeIgnore(relativePath: string, rawIgnore: string): boolean {
    const { ignore, negate } = parseVscodeIgnoreRules(rawIgnore);
    const opts = { dot: true };
    const matchedIgnore = ignore.some((pattern) => minimatch(relativePath, pattern, opts));
    const matchedNegate = negate.some((pattern) => minimatch(relativePath, pattern, opts));
    return !matchedIgnore || matchedNegate;
}

/**
 * Writes a set of relative files into a directory for packaging tests.
 *
 * @param baseDir Root temporary directory.
 * @param files Relative POSIX file paths mapped to text content.
 */
function writeFixtureTree(baseDir: string, files: Record<string, string>): void {
    for (const [relPath, content] of Object.entries(files)) {
        const fullPath = path.join(baseDir, ...relPath.split('/'));
        fs.mkdirSync(path.dirname(fullPath), { recursive: true });
        fs.writeFileSync(fullPath, content, 'utf-8');
    }
}

describe('package.json integration scripts and extension contributions (CLN5)', () => {
    const pkg = JSON.parse(fs.readFileSync(PACKAGE_JSON_PATH, 'utf-8')) as PackageJsonShape;
    const scripts = pkg.scripts ?? {};

    it('keeps the four Spark integration scripts and validates their target paths (normal condition)', () => {
        expect(scripts['test:integration']).toBe('vitest run --config vitest.integration.config.mts');
        expect(scripts['test:integration:watch']).toBe('vitest --config vitest.integration.config.mts');
        expect(scripts['test:integration:e2e']).toBe(
            'vitest run --config vitest.integration.config.mts src/test/integration/e2e'
        );
        expect(scripts['test:integration:pytest']).toBe(
            'vitest run --config vitest.integration.config.mts src/test/integration/pytest-runner.smoke.test.ts'
        );

        expect(fs.existsSync(path.join(ROOT_DIR, 'vitest.integration.config.mts'))).toBe(true);
        expect(fs.existsSync(path.join(ROOT_DIR, 'src/test/integration/e2e'))).toBe(true);
        expect(fs.existsSync(path.join(ROOT_DIR, 'src/test/integration/pytest-runner.smoke.test.ts'))).toBe(true);
    });

    it('preserves SPARK_EDITOR_VIEW_TYPE and custom editor contributions untouched (normal condition)', () => {
        expect(pkg.main).toBe('./out/extension.js');
        expect(pkg.activationEvents).toContain(`onCustomEditor:${SPARK_EDITOR_VIEW_TYPE}`);

        const associations = pkg.contributes?.configurationDefaults?.['workbench.editorAssociations'] ?? {};
        expect(associations[`*${PIPELINE_FILE_EXT}`]).toBe(SPARK_EDITOR_VIEW_TYPE);
        expect(associations[`{**/*${PIPELINE_FILE_EXT}}`]).toBe(SPARK_EDITOR_VIEW_TYPE);

        const customEditor = pkg.contributes?.customEditors?.find(
            (editor) => editor.viewType === SPARK_EDITOR_VIEW_TYPE
        );
        expect(customEditor).toBeDefined();
        const patterns = (customEditor?.selector ?? []).map((entry) => entry.filenamePattern);
        expect(patterns).toContain(`*${PIPELINE_FILE_EXT}`);
        expect(patterns).toContain(`**/*${PIPELINE_FILE_EXT}`);
    });

    it('restricts integration script keys to the supported set and verifies referenced paths exist (boundary condition)', () => {
        const integrationKeys = Object.keys(scripts)
            .filter((key) => key.startsWith('test:integration'))
            .sort();

        expect(integrationKeys).toEqual([
            'test:integration',
            'test:integration:e2e',
            'test:integration:pytest',
            'test:integration:watch',
        ]);

        for (const [scriptName, command] of Object.entries(scripts)) {
            const tokens = command.split(/\s+/);
            for (const token of tokens) {
                if (token.startsWith('src/')) {
                    expect(
                        fs.existsSync(path.join(ROOT_DIR, token)),
                        `Script "${scriptName}" references missing path "${token}"`
                    ).toBe(true);
                }
            }
        }
    });

    it('removes all legacy language and category integration scripts targeting nonexistent folders (failure condition)', () => {
        const removedLegacyScripts = [
            'test:integration:java',
            'test:integration:python',
            'test:integration:sources',
            'test:integration:sources:java',
            'test:integration:sources:python',
            'test:integration:sinks',
            'test:integration:sinks:java',
            'test:integration:sinks:python',
            'test:integration:steps',
            'test:integration:steps:java',
            'test:integration:steps:python',
        ];

        for (const legacyKey of removedLegacyScripts) {
            expect(scripts[legacyKey], `Legacy script "${legacyKey}" must not exist`).toBeUndefined();
        }

        expect(fs.existsSync(path.join(ROOT_DIR, 'src/test/integration/sources'))).toBe(false);
        expect(fs.existsSync(path.join(ROOT_DIR, 'src/test/integration/sinks'))).toBe(false);
        expect(fs.existsSync(path.join(ROOT_DIR, 'src/test/integration/steps'))).toBe(false);
    });
});

describe('.vscodeignore VSIX runtime-only packaging rules (CLN5)', () => {
    const rawVscodeIgnore = fs.readFileSync(VSCODEIGNORE_PATH, 'utf-8');
    const ignoreLines = rawVscodeIgnore
        .split(/[\n\r]/)
        .map((line) => line.trim())
        .filter((line) => line.length > 0 && !line.startsWith('#'));

    const tempDirs: string[] = [];
    afterAll(() => {
        for (const dir of tempDirs) {
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    it('replaces TASK.md with TASKS.md, drops .gemini, and excludes non-runtime workspace docs (normal condition)', () => {
        expect(ignoreLines).toContain('TASKS.md');
        expect(ignoreLines).not.toContain('TASK.md');
        expect(rawVscodeIgnore).not.toContain('.gemini');

        expect(ignoreLines).toContain('AGENTS.md');
        expect(ignoreLines).toContain('ARCHITECTURE.md');
        expect(ignoreLines).toContain('DESIGN.md');
        expect(ignoreLines).toContain('.agents/**');
        expect(ignoreLines).toContain('graphify-out/**');
        expect(ignoreLines).toContain('src/**');
        expect(ignoreLines).toContain('out/test/**');
        expect(ignoreLines).toContain('**/*.map');
        expect(ignoreLines).toContain('webview/src/**');
        expect(ignoreLines).toContain('webview/node_modules/**');
    });

    it('packs only runtime assets when evaluated by vsce listFiles against the repository (normal condition)', async () => {
        const files = await vsce.listFiles({
            cwd: ROOT_DIR,
            packageManager: vsce.PackageManager.None,
        });

        expect(files).toContain('package.json');
        expect(files).toContain('README.md');
        expect(files).toContain('LICENSE.txt');
        expect(files).toContain('resources/icon.svg');
        expect(files).toContain('resources/code-gen-schema.json');
        expect(files).toContain('resources/codegen-skills/core-pyspark/SKILL.md');
        expect(files).toContain('resources/samples/gcs-parquet-etl.spark');
        expect(files).toContain('resources/templates/pyspark/run_local.sh');
        expect(files).toContain('walkthroughs/create-pipeline.md');

        const forbiddenPatterns = [
            /^TASKS?\.md$/,
            /^AGENTS\.md$/,
            /^ARCHITECTURE\.md$/,
            /^DESIGN\.md$/,
            /^\.agents\//,
            /^\.gemini\//,
            /^graphify-out\//,
            /^src\//,
            /^out\/test\//,
            /\.map$/,
            /^webview\/src\//,
            /^webview\/node_modules\//,
            /^webview\/public\//,
            /^webview\/package/,
            /^webview\/index\.html$/,
            /(^|\/)vitest/,
            /(^|\/)tsconfig/,
            /\.tsbuildinfo$/,
            /test-report\./,
            /^skills-lock\.json$/,
            /eslint\.config\./,
        ];

        for (const file of files) {
            for (const pattern of forbiddenPatterns) {
                expect(
                    pattern.test(file),
                    `Packaged file "${file}" matched forbidden pattern ${pattern}`
                ).toBe(false);
            }
        }

        const allowedTopLevelEntries = new Set([
            'LICENSE.txt',
            'README.md',
            'out',
            'package.json',
            'resources',
            'walkthroughs',
            'webview',
        ]);
        for (const file of files) {
            const topSegment = file.split('/')[0];
            expect(
                allowedTopLevelEntries.has(topSegment),
                `Unexpected top-level entry "${topSegment}" in packaged file "${file}"`
            ).toBe(true);
            if (topSegment === 'webview') {
                expect(
                    file.startsWith('webview/dist/'),
                    `Unexpected non-dist webview file "${file}"`
                ).toBe(true);
            }
        }
    });

    it('filters boundary build artifacts, caches, credentials, and non-runtime node_modules files (boundary condition)', async () => {
        const fixtureDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cln5-vsix-boundary-'));
        tempDirs.push(fixtureDir);

        writeFixtureTree(fixtureDir, {
            '.vscodeignore': rawVscodeIgnore,
            'package.json': JSON.stringify(
                {
                    name: 'fixture-spark-ext',
                    publisher: 'antigravity',
                    version: '1.0.0',
                    engines: { vscode: '^1.74.0' },
                    activationEvents: ['onLanguage:spark'],
                    main: './out/extension.js',
                },
                null,
                2
            ),
            'README.md': '# Fixture Extension\n',
            'LICENSE.txt': 'Apache-2.0\n',
            'TASKS.md': '# Tasks\n',
            'AGENTS.md': '# Agents\n',
            'ARCHITECTURE.md': '# Architecture\n',
            'DESIGN.md': '# Design\n',
            '.agents/rules/git-workflow.md': '# Rule\n',
            'graphify-out/graph.json': '{}\n',
            'src/extension.ts': 'export {};\n',
            'out/extension.js': 'exports.activate = function () {};\n',
            'out/extension.js.map': '{}\n',
            'out/core/constants.js': 'exports.SPARK = true;\n',
            'out/core/constants.js.map': '{}\n',
            'out/test/unit/sample.test.js': 'test();\n',
            'out/test/integration/evaluators/types.js': 'export {};\n',
            'webview/src/App.tsx': 'export default null;\n',
            'webview/index.html': '<html/>\n',
            'webview/package.json': '{}\n',
            'webview/dist/index.html': '<!doctype html>\n',
            'webview/dist/assets/index.js': 'console.log(1);\n',
            'webview/dist/assets/index.js.map': '{}\n',
            'resources/icon.svg': '<svg/>\n',
            'resources/codegen-skills/core-pyspark/SKILL.md': '# Skill\n',
            'walkthroughs/design-dag.md': '# Walkthrough\n',
            'vitest.config.mts': 'export default {};\n',
            'vitest.integration.config.mts': 'export default {};\n',
            'tsconfig.json': '{}\n',
            'tsconfig.integration.json': '{}\n',
            'webview/tsconfig.tsbuildinfo': '{}\n',
            'test-report.md': '# Report\n',
            'src/test/integration/.test-report.json': '{}\n',
            'coverage/lcov.info': 'TN:\n',
            '.venv/bin/python': '',
            '.pytest_cache/README.md': '',
            '.env': 'SECRET=1\n',
            '.env.local': 'SECRET=2\n',
            'service-account-test.json': '{}\n',
            'dev.pem': 'KEY\n',
        });

        const included = (
            await vsce.listFiles({
                cwd: fixtureDir,
                packageManager: vsce.PackageManager.None,
            })
        ).sort();

        expect(included).toEqual([
            'LICENSE.txt',
            'README.md',
            'out/core/constants.js',
            'out/extension.js',
            'package.json',
            'resources/codegen-skills/core-pyspark/SKILL.md',
            'resources/icon.svg',
            'walkthroughs/design-dag.md',
            'webview/dist/assets/index.js',
            'webview/dist/index.html',
        ]);

        // NOTE: Keep runtime JS in node_modules and strip declarations, docs, maps, and tests.
        expect(isIncludedByVscodeIgnore('node_modules/yaml/dist/index.js', rawVscodeIgnore)).toBe(true);
        expect(isIncludedByVscodeIgnore('node_modules/@google/genai/dist/index.cjs', rawVscodeIgnore)).toBe(true);
        expect(isIncludedByVscodeIgnore('node_modules/yaml/dist/index.d.ts', rawVscodeIgnore)).toBe(false);
        expect(
            isIncludedByVscodeIgnore(
                'node_modules/@modelcontextprotocol/sdk/dist/esm/index.d.mts',
                rawVscodeIgnore
            )
        ).toBe(false);
        expect(isIncludedByVscodeIgnore('node_modules/zod/index.d.cts', rawVscodeIgnore)).toBe(false);
        expect(isIncludedByVscodeIgnore('node_modules/yaml/dist/index.js.map', rawVscodeIgnore)).toBe(false);
        expect(isIncludedByVscodeIgnore('node_modules/yaml/README.md', rawVscodeIgnore)).toBe(false);
        expect(isIncludedByVscodeIgnore('node_modules/yaml/LICENSE', rawVscodeIgnore)).toBe(false);
        expect(isIncludedByVscodeIgnore('node_modules/ws/test/ws.test.js', rawVscodeIgnore)).toBe(false);
    });

    it('forbids blanket directory negations that bypass out/test and sourcemap exclusions in vsce (failure condition)', async () => {
        const { negate } = parseVscodeIgnoreRules(rawVscodeIgnore);
        expect(negate).toEqual([]);

        // NOTE: If .vscodeignore contains !out/**, vsce includes out/test/** and **/*.map.
        const brokenDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cln5-vsix-negate-bug-'));
        tempDirs.push(brokenDir);

        writeFixtureTree(brokenDir, {
            '.vscodeignore': ['!out/**', 'out/test/**', '**/*.map'].join('\n'),
            'package.json': JSON.stringify(
                {
                    name: 'broken-negate-fixture',
                    publisher: 'antigravity',
                    version: '1.0.0',
                    engines: { vscode: '^1.74.0' },
                    activationEvents: ['onLanguage:spark'],
                    main: './out/extension.js',
                },
                null,
                2
            ),
            'README.md': '# Broken Fixture\n',
            'out/extension.js': 'exports.activate = function () {};\n',
            'out/extension.js.map': '{}\n',
            'out/test/unit/leaked.test.js': 'test();\n',
        });

        const leakedFiles = await vsce.listFiles({
            cwd: brokenDir,
            packageManager: vsce.PackageManager.None,
        });
        expect(leakedFiles).toContain('out/extension.js.map');
        expect(leakedFiles).toContain('out/test/unit/leaked.test.js');
    });
});
