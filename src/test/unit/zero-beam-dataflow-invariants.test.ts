/**
 * @fileoverview Repository invariant tests forbidding Beam and Dataflow era tokens (CLN4).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Scans `src/`, `webview/src/`, `resources/`, and `package.json` (exempting only this
 * invariant test file itself) and fails on any case-insensitive match of `apache-beam`,
 * `.beam` (`\.beam\b`), `DirectRunner`, `Dataflow`, or `run_directrunner`.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

/**
 * One forbidden legacy token rule evaluated case-insensitively across repository files.
 */
interface ForbiddenTokenRule {
    /** Human-readable token label reported on violation. */
    readonly label: string;
    /** Case-insensitive regular expression detecting the forbidden token. */
    readonly regex: RegExp;
}

/**
 * One detected forbidden token occurrence in a scanned file.
 */
interface InvariantViolation {
    /** Path relative to the scan root. */
    readonly relativePath: string;
    /** One-based line number (0 when matched on the file path itself). */
    readonly line: number;
    /** Label of the triggered rule. */
    readonly ruleLabel: string;
    /** Trimmed excerpt showing the offending match. */
    readonly snippet: string;
}

const FORBIDDEN_TOKEN_RULES: readonly ForbiddenTokenRule[] = [
    { label: 'apache-beam', regex: /apache-beam/i },
    { label: '.beam', regex: /\.beam\b/i },
    { label: 'DirectRunner', regex: /directrunner/i },
    { label: 'Dataflow', regex: /dataflow/i },
    { label: 'run_directrunner', regex: /run_directrunner/i },
];

const BINARY_ASSET_EXTENSIONS: ReadonlySet<string> = new Set([
    '.png',
    '.jpg',
    '.jpeg',
    '.gif',
    '.ico',
    '.webp',
    '.woff',
    '.woff2',
    '.ttf',
]);

const TRANSIENT_DIR_NAMES: ReadonlySet<string> = new Set([
    'node_modules',
    '.git',
    '.venv',
    'venv',
    '__pycache__',
    '.pytest_cache',
    '.spark-ivy',
]);

/**
 * Collects all regular files under a target file or directory, excluding transient caches.
 *
 * @param entryPath Absolute file or directory path to walk.
 * @param exemptedFile Real or resolved path of the single exempted invariant test file.
 * @returns Sorted list of absolute file paths to inspect.
 */
function collectTargetFiles(entryPath: string, exemptedFile: string): string[] {
    const resolvedEntry = path.resolve(entryPath);
    if (!fs.existsSync(resolvedEntry)) {
        return [];
    }
    const stat = fs.lstatSync(resolvedEntry);
    if (stat.isSymbolicLink()) {
        return [];
    }
    if (stat.isFile()) {
        return resolvedEntry === exemptedFile ? [] : [resolvedEntry];
    }
    if (!stat.isDirectory()) {
        return [];
    }

    const collected: string[] = [];
    for (const dirent of fs.readdirSync(resolvedEntry, { withFileTypes: true })) {
        if (dirent.isSymbolicLink()) {
            continue;
        }
        if (dirent.isDirectory()) {
            if (TRANSIENT_DIR_NAMES.has(dirent.name)) {
                continue;
            }
            collected.push(
                ...collectTargetFiles(path.join(resolvedEntry, dirent.name), exemptedFile)
            );
        } else if (dirent.isFile()) {
            const childPath = path.resolve(resolvedEntry, dirent.name);
            if (childPath !== exemptedFile) {
                collected.push(childPath);
            }
        }
    }
    return collected.sort();
}

/**
 * Scans a set of target paths for forbidden legacy tokens in file names and text contents.
 *
 * @param rootDir Base directory used to format relative violation paths.
 * @param targetPaths Files or directories to scan recursively.
 * @param exemptedFile Single absolute file path exempted from scanning.
 * @returns Detected violations and total number of scanned files.
 */
function scanForLegacyTokens(
    rootDir: string,
    targetPaths: readonly string[],
    exemptedFile: string
): { violations: InvariantViolation[]; scannedFileCount: number } {
    const normalizedExempt = path.resolve(exemptedFile);
    const files: string[] = [];
    for (const target of targetPaths) {
        files.push(...collectTargetFiles(target, normalizedExempt));
    }

    const violations: InvariantViolation[] = [];
    for (const absFile of files) {
        const relPath = path.relative(rootDir, absFile).replace(/\\/g, '/');

        for (const rule of FORBIDDEN_TOKEN_RULES) {
            if (rule.regex.test(relPath)) {
                violations.push({
                    relativePath: relPath,
                    line: 0,
                    ruleLabel: rule.label,
                    snippet: relPath,
                });
            }
        }

        const ext = path.extname(absFile).toLowerCase();
        if (BINARY_ASSET_EXTENSIONS.has(ext)) {
            continue;
        }

        const content = fs.readFileSync(absFile, 'utf-8');
        const lines = content.split(/\r?\n/);
        for (let idx = 0; idx < lines.length; idx++) {
            const lineText = lines[idx];
            for (const rule of FORBIDDEN_TOKEN_RULES) {
                if (rule.regex.test(lineText)) {
                    violations.push({
                        relativePath: relPath,
                        line: idx + 1,
                        ruleLabel: rule.label,
                        snippet: lineText.trim().slice(0, 140),
                    });
                }
            }
        }
    }

    return { violations, scannedFileCount: files.length };
}

describe('Zero Beam and Dataflow repository invariants (CLN4)', () => {
    const repoRoot = path.resolve(__dirname, '..', '..', '..');
    const selfFilePath = path.resolve(__filename);
    const tempDirs: string[] = [];

    afterEach(() => {
        while (tempDirs.length > 0) {
            const dir = tempDirs.pop();
            if (dir) {
                fs.rmSync(dir, { recursive: true, force: true });
            }
        }
    });

    it('finds zero forbidden legacy tokens across src/, webview/src/, resources/, and package.json (normal condition)', () => {
        const targets = [
            path.join(repoRoot, 'src'),
            path.join(repoRoot, 'webview', 'src'),
            path.join(repoRoot, 'resources'),
            path.join(repoRoot, 'package.json'),
        ];

        const { violations, scannedFileCount } = scanForLegacyTokens(
            repoRoot,
            targets,
            selfFilePath
        );

        expect(scannedFileCount).toBeGreaterThan(200);
        const formatted = violations.map(
            (v) => `${v.relativePath}:${v.line} [${v.ruleLabel}] ${v.snippet}`
        );
        expect(formatted).toEqual([]);
    });

    it('exempts only the designated self file and enforces word boundaries on .beam (boundary condition)', () => {
        const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'spark-invariants-boundary-'));
        tempDirs.push(tmpRoot);

        const exemptedPath = path.join(tmpRoot, 'exempted-self.test.ts');
        fs.writeFileSync(exemptedPath, 'const forbidden = "Dataflow DirectRunner apache-beam";\n', 'utf-8');

        const cleanSparkFile = path.join(tmpRoot, 'orders.spark');
        fs.writeFileSync(
            cleanSparkFile,
            'paradigm: imperative\n# Word boundary check: sun.beaming and run_local.sh are allowed\njobConfig:\n  runner: Local\n',
            'utf-8'
        );

        const { violations, scannedFileCount } = scanForLegacyTokens(
            tmpRoot,
            [tmpRoot],
            exemptedPath
        );

        expect(scannedFileCount).toBe(1);
        expect(violations).toEqual([]);
    });

    it('detects case-insensitive apache-beam, .beam, DirectRunner, Dataflow, and run_directrunner matches (failure condition)', () => {
        const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'spark-invariants-fail-'));
        tempDirs.push(tmpRoot);

        fs.writeFileSync(path.join(tmpRoot, 'req.txt'), 'APACHE-BEAM>=2.60.0\n', 'utf-8');
        fs.writeFileSync(path.join(tmpRoot, 'legacy.BEAM'), 'nodes: []\n', 'utf-8');
        fs.writeFileSync(
            path.join(tmpRoot, 'sample.ts'),
            [
                'const a = "directRunner";',
                'const b = "Google Cloud DATAFLOW";',
                'const c = "./RUN_DIRECTRUNNER.sh";',
            ].join('\n'),
            'utf-8'
        );

        const { violations } = scanForLegacyTokens(
            tmpRoot,
            [tmpRoot],
            path.join(tmpRoot, 'nonexistent-exempt.ts')
        );

        const matchedRules = new Set(violations.map((v) => v.ruleLabel));
        expect(matchedRules.has('apache-beam')).toBe(true);
        expect(matchedRules.has('.beam')).toBe(true);
        expect(matchedRules.has('DirectRunner')).toBe(true);
        expect(matchedRules.has('Dataflow')).toBe(true);
        expect(matchedRules.has('run_directrunner')).toBe(true);
    });
});
