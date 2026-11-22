/**
 * @fileoverview Unit tests verifying consolidation of root compatibility shims (CLN7).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Ensures `src/constants.ts` and `src/dagValidator.ts` remain deleted, confirms canonical
 * exports on `src/core/constants.ts` and `src/shared/dagValidator.ts`, and scans `src/` to
 * reject any static, dynamic, CommonJS, or mock specifier targeting deleted root shims.
 */

import { afterEach, describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
    DEFAULT_GEMINI_MODEL,
    PIPELINE_FILE_EXT,
    SPARK_EDITOR_VIEW_TYPE,
} from '../../core/constants';
import {
    validateDagTopology,
    type DagEdge,
    type DagNode,
} from '../../shared/dagValidator';

/**
 * One detected import or mock specifier referencing a deleted root compatibility shim.
 */
interface ShimImportViolation {
    /** File path relative to the scanned `src/` root. */
    readonly relativeFile: string;
    /** One-based line number of the offending specifier. */
    readonly line: number;
    /** Raw module specifier written in source code. */
    readonly specifier: string;
    /** Deleted root module basename (`constants` or `dagValidator`). */
    readonly targetShim: 'constants' | 'dagValidator';
}

const TRANSIENT_DIRS: ReadonlySet<string> = new Set([
    'node_modules',
    '.git',
    '.venv',
    'venv',
    '__pycache__',
    '.pytest_cache',
    '.spark-ivy',
]);

const RELATIVE_SPECIFIER_PATTERNS: readonly RegExp[] = [
    /\bfrom\s+['"](\.[^'"]+)['"]/g,
    /\bimport\s*\(\s*['"](\.[^'"]+)['"]\s*\)/g,
    /\brequire\s*\(\s*['"](\.[^'"]+)['"]\s*\)/g,
    /\bvi\.(?:mock|doMock)\s*\(\s*['"](\.[^'"]+)['"]/g,
];

/**
 * Recursively collects all TypeScript source files under a directory.
 *
 * @param dirPath Directory to walk.
 * @returns Sorted list of absolute `.ts` file paths.
 */
function collectTypeScriptFiles(dirPath: string): string[] {
    const resolvedDir = path.resolve(dirPath);
    if (!fs.existsSync(resolvedDir)) {
        return [];
    }
    const collected: string[] = [];
    for (const entry of fs.readdirSync(resolvedDir, { withFileTypes: true })) {
        if (entry.isSymbolicLink()) {
            continue;
        }
        const fullPath = path.join(resolvedDir, entry.name);
        if (entry.isDirectory()) {
            if (!TRANSIENT_DIRS.has(entry.name)) {
                collected.push(...collectTypeScriptFiles(fullPath));
            }
        } else if (entry.isFile() && entry.name.endsWith('.ts')) {
            collected.push(fullPath);
        }
    }
    return collected.sort();
}

/**
 * Normalizes a resolved path by stripping `.ts` or `.js` extensions for target comparison.
 *
 * @param resolvedPath Absolute path resolved from a relative module specifier.
 * @returns Extensionless absolute path.
 */
function stripModuleExtension(resolvedPath: string): string {
    return resolvedPath.replace(/\.(?:ts|js)$/i, '');
}

/**
 * Scans a `src/` tree and reports any relative specifier resolving to `<srcRoot>/constants`
 * or `<srcRoot>/dagValidator`.
 *
 * @param srcRoot Absolute path to the `src/` directory under audit.
 * @returns Detected violations and total scanned TypeScript file count.
 */
function scanForDeletedShimImports(srcRoot: string): {
    violations: ShimImportViolation[];
    scannedCount: number;
} {
    const normalizedRoot = path.resolve(srcRoot);
    const forbiddenConstantsTarget = path.join(normalizedRoot, 'constants');
    const forbiddenDagValidatorTarget = path.join(normalizedRoot, 'dagValidator');

    const files = collectTypeScriptFiles(normalizedRoot);
    const violations: ShimImportViolation[] = [];

    for (const absFile of files) {
        const relFile = path.relative(normalizedRoot, absFile).replace(/\\/g, '/');
        const fileDir = path.dirname(absFile);
        const lines = fs.readFileSync(absFile, 'utf-8').split(/\r?\n/);

        for (let idx = 0; idx < lines.length; idx++) {
            const lineText = lines[idx];
            for (const pattern of RELATIVE_SPECIFIER_PATTERNS) {
                const regex = new RegExp(pattern.source, pattern.flags);
                let match: RegExpExecArray | null = regex.exec(lineText);
                while (match !== null) {
                    const specifier = match[1];
                    const resolvedTarget = stripModuleExtension(path.resolve(fileDir, specifier));
                    if (resolvedTarget === forbiddenConstantsTarget) {
                        violations.push({
                            relativeFile: relFile,
                            line: idx + 1,
                            specifier,
                            targetShim: 'constants',
                        });
                    } else if (resolvedTarget === forbiddenDagValidatorTarget) {
                        violations.push({
                            relativeFile: relFile,
                            line: idx + 1,
                            specifier,
                            targetShim: 'dagValidator',
                        });
                    }
                    match = regex.exec(lineText);
                }
            }
        }
    }

    return { violations, scannedCount: files.length };
}

describe('Root compatibility shim consolidation invariants (CLN7)', () => {
    const repoRoot = path.resolve(__dirname, '..', '..', '..');
    const srcRoot = path.join(repoRoot, 'src');
    const tempDirs: string[] = [];

    afterEach(() => {
        while (tempDirs.length > 0) {
            const dir = tempDirs.pop();
            if (dir) {
                fs.rmSync(dir, { recursive: true, force: true });
            }
        }
    });

    it('removes root src/constants.ts and src/dagValidator.ts shims and preserves canonical exports (normal condition)', () => {
        expect(fs.existsSync(path.join(srcRoot, 'constants.ts'))).toBe(false);
        expect(fs.existsSync(path.join(srcRoot, 'dagValidator.ts'))).toBe(false);

        expect(DEFAULT_GEMINI_MODEL).toBe('gemini-4-hydrogen');
        expect(PIPELINE_FILE_EXT).toBe('.spark');
        expect(SPARK_EDITOR_VIEW_TYPE).toBe('spark-pipeline-builder.sparkEditor');

        const nodes: DagNode[] = [
            { id: 'src-1', data: { label: 'CSV Input', type: 'SOURCE', spark_transform: 'CsvSource' } },
            { id: 'snk-1', data: { label: 'Parquet Output', type: 'SINK', spark_transform: 'ParquetSink' } },
        ];
        const edges: DagEdge[] = [{ id: 'e-1', source: 'src-1', target: 'snk-1' }];
        const validation = validateDagTopology(nodes, edges);
        expect(validation.isValid).toBe(true);
        expect(validation.errors).toEqual([]);
    });

    it('finds zero imports of deleted root shims across all TypeScript files under src/ (normal condition)', () => {
        const { violations, scannedCount } = scanForDeletedShimImports(srcRoot);
        expect(scannedCount).toBeGreaterThan(100);
        const formatted = violations.map(
            (v) => `${v.relativeFile}:${v.line} -> ${v.specifier} (${v.targetShim})`
        );
        expect(formatted).toEqual([]);
    });

    it('allows canonical imports of src/core/constants and src/shared/dagValidator from any depth (boundary condition)', () => {
        const tmpSrc = fs.mkdtempSync(path.join(os.tmpdir(), 'shim-consolidation-boundary-'));
        tempDirs.push(tmpSrc);

        fs.mkdirSync(path.join(tmpSrc, 'core'), { recursive: true });
        fs.mkdirSync(path.join(tmpSrc, 'shared'), { recursive: true });
        fs.mkdirSync(path.join(tmpSrc, 'nested', 'deep'), { recursive: true });

        fs.writeFileSync(
            path.join(tmpSrc, 'rootCaller.ts'),
            [
                "import { DEFAULT_GEMINI_MODEL } from './core/constants';",
                "import { validateDagTopology } from './shared/dagValidator';",
                'export const ok = Boolean(DEFAULT_GEMINI_MODEL && validateDagTopology);',
            ].join('\n'),
            'utf-8'
        );
        fs.writeFileSync(
            path.join(tmpSrc, 'shared', 'peerCaller.ts'),
            "import type { DagNode } from './dagValidator';\nexport type LocalNode = DagNode;\n",
            'utf-8'
        );
        fs.writeFileSync(
            path.join(tmpSrc, 'core', 'siblingCaller.ts'),
            "import { DEFAULT_GEMINI_MODEL } from './constants';\nexport const m = DEFAULT_GEMINI_MODEL;\n",
            'utf-8'
        );
        fs.writeFileSync(
            path.join(tmpSrc, 'nested', 'deep', 'unitCaller.ts'),
            [
                "import { DEFAULT_GEMINI_MODEL } from '../../core/constants';",
                "import type { DagEdge } from '../../shared/dagValidator';",
                'export type Pair = [typeof DEFAULT_GEMINI_MODEL, DagEdge];',
            ].join('\n'),
            'utf-8'
        );

        const { violations, scannedCount } = scanForDeletedShimImports(tmpSrc);
        expect(scannedCount).toBe(4);
        expect(violations).toEqual([]);
    });

    it('detects static, type-only, dynamic, require, and mock imports targeting deleted root shims (failure condition)', () => {
        const tmpSrc = fs.mkdtempSync(path.join(os.tmpdir(), 'shim-consolidation-fail-'));
        tempDirs.push(tmpSrc);

        fs.mkdirSync(path.join(tmpSrc, 'codegen'), { recursive: true });
        fs.mkdirSync(path.join(tmpSrc, 'test', 'unit'), { recursive: true });

        fs.writeFileSync(
            path.join(tmpSrc, 'rootBad.ts'),
            [
                "import { DEFAULT_GEMINI_MODEL } from './constants';",
                "import { validateDagTopology } from './dagValidator';",
            ].join('\n'),
            'utf-8'
        );
        fs.writeFileSync(
            path.join(tmpSrc, 'codegen', 'nestedBad.ts'),
            [
                "const mod = await import('../constants');",
                "const legacy = require('../dagValidator.js');",
            ].join('\n'),
            'utf-8'
        );
        const parentDepth = ['..', '..'].join('/');
        fs.writeFileSync(
            path.join(tmpSrc, 'test', 'unit', 'mockBad.ts'),
            [
                `import type { DagNode } from '${parentDepth}/dagValidator';`,
                `vi.mock('${parentDepth}/constants');`,
            ].join('\n'),
            'utf-8'
        );

        const { violations } = scanForDeletedShimImports(tmpSrc);
        expect(violations).toHaveLength(6);
        expect(violations.filter((v) => v.targetShim === 'constants')).toHaveLength(3);
        expect(violations.filter((v) => v.targetShim === 'dagValidator')).toHaveLength(3);
    });
});
