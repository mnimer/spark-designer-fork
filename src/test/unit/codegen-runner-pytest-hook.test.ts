/**
 * Unit tests for the offline pytest hook in the integration codegen runner.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies that generated files are written safely to a temporary repository,
 * that only failed, error, and timeout pytest results block a test, and that
 * the temporary repository is always removed. The pytest runner is mocked, so
 * no Python, Spark, or network access is needed.
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { runGeneratedRepoTestsMock } = vi.hoisted(() => ({ runGeneratedRepoTestsMock: vi.fn() }));

vi.mock('vscode', async () => (await import('./helpers/vscodeMock')).createVscodeMock());
vi.mock('../integration/pytest-runner', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../integration/pytest-runner')>();
    return { ...actual, runGeneratedRepoTests: runGeneratedRepoTestsMock };
});

import {
    OFFLINE_PYTEST_TIMEOUT_MS,
    isBlockingPytestStatus,
    runOfflinePytest,
    writeGeneratedRepo,
} from '../integration/codegen-runner';
import type { PytestRunResult } from '../integration/pytest-runner';

/** Builds a minimal pytest run result with the given status. */
function result(status: PytestRunResult['status']): PytestRunResult {
    return {
        status,
        passed: 0,
        failed: 0,
        skipped: 0,
        errors: 0,
        total: 0,
        failures: [],
        cases: [],
        exitCode: null,
        durationMs: 1,
        stdout: '',
        stderr: '',
    };
}

describe('writeGeneratedRepo', () => {
    let root: string;

    beforeEach(() => {
        root = fs.mkdtempSync(path.join(os.tmpdir(), 'hook-test-'));
    });

    afterEach(() => {
        fs.rmSync(root, { recursive: true, force: true });
    });

    it('writes nested files and returns their relative paths', () => {
        const written = writeGeneratedRepo(
            [
                { filename: 'src/pipeline.py', content: 'def transform(dfs, config):\n    return dfs\n' },
                { filename: 'tests/test_pipeline.py', content: 'def test_ok():\n    pass\n' },
            ],
            root
        );
        expect(written).toEqual([path.join('src', 'pipeline.py'), path.join('tests', 'test_pipeline.py')]);
        expect(fs.readFileSync(path.join(root, 'src', 'pipeline.py'), 'utf8')).toContain('def transform');
    });

    it('marks executable files with mode 755', () => {
        writeGeneratedRepo([{ filename: 'run_local.sh', content: '#!/bin/bash\n', executable: true }], root);
        expect(fs.statSync(path.join(root, 'run_local.sh')).mode & 0o777).toBe(0o755);
    });

    it('writes nothing for an empty file list', () => {
        expect(writeGeneratedRepo([], root)).toEqual([]);
        expect(fs.readdirSync(root)).toEqual([]);
    });

    it.each(['../escape.py', 'src/../../escape.py', '/etc/passwd', '', '   ', '.'])(
        'rejects the unsafe filename %j',
        (filename) => {
            expect(() => writeGeneratedRepo([{ filename, content: 'x' }], root)).toThrow(/Refusing to write/);
            expect(fs.existsSync(path.join(path.dirname(root), 'escape.py'))).toBe(false);
        }
    );
});

describe('isBlockingPytestStatus', () => {
    it.each([
        ['failed', true],
        ['error', true],
        ['timeout', true],
        ['passed', false],
        ['skipped', false],
    ] as const)('maps %s to %s', (status, expected) => {
        expect(isBlockingPytestStatus(status)).toBe(expected);
    });
});

describe('runOfflinePytest', () => {
    beforeEach(() => {
        runGeneratedRepoTestsMock.mockReset();
    });

    it('runs the runner on a temporary repo with the files and removes it afterwards', async () => {
        let seenDir = '';
        runGeneratedRepoTestsMock.mockImplementation(async (dir: string) => {
            seenDir = dir;
            expect(fs.readFileSync(path.join(dir, 'tests', 'test_pipeline.py'), 'utf8')).toBe('def test_ok(): pass\n');
            return result('passed');
        });
        const res = await runOfflinePytest([{ filename: 'tests/test_pipeline.py', content: 'def test_ok(): pass\n' }], '/py');
        expect(res.status).toBe('passed');
        expect(runGeneratedRepoTestsMock).toHaveBeenCalledWith(seenDir, { python: '/py', timeoutMs: OFFLINE_PYTEST_TIMEOUT_MS });
        expect(fs.existsSync(seenDir)).toBe(false);
    });

    it('returns a skipped result unchanged', async () => {
        runGeneratedRepoTestsMock.mockResolvedValue({ ...result('skipped'), reason: 'pyspark missing' });
        const res = await runOfflinePytest([], '/py');
        expect(res).toMatchObject({ status: 'skipped', reason: 'pyspark missing' });
    });

    it('removes the temporary repo if the runner rejects', async () => {
        let seenDir = '';
        runGeneratedRepoTestsMock.mockImplementation(async (dir: string) => {
            seenDir = dir;
            throw new Error('boom');
        });
        await expect(runOfflinePytest([], '/py')).rejects.toThrow('boom');
        expect(seenDir).not.toBe('');
        expect(fs.existsSync(seenDir)).toBe(false);
    });

    it('rejects an unsafe filename before the runner starts', async () => {
        await expect(runOfflinePytest([{ filename: '../x.py', content: '' }], '/py')).rejects.toThrow(/Refusing to write/);
        expect(runGeneratedRepoTestsMock).not.toHaveBeenCalled();
    });
});
