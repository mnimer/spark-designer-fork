/**
 * Unit tests for the read-only check runner (`src/codegen/ReadOnlyCheckRunner.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Uses a fake command executor, so no process is spawned except in the tests that exercise
 * `defaultCommandExecutor`. Covers every PySpark check (`required_files`, `leftover_tokens`,
 * `python_syntax`, `pipeline_contract`, `test_imports`, `pytest_collect`) in pass, fail, and
 * skip states, path containment, and output truncation.
 */

import { describe, it, expect, vi } from 'vitest';
import {
    CHECK_IDS,
    CommandExecutor,
    GeneratedFile,
    ReadOnlyCheckRunner,
    defaultCommandExecutor,
    expectedSourceFiles,
    truncateCheckOutput,
} from '../../../codegen/ReadOnlyCheckRunner';
import { READ_ONLY_CHECK_LIMITS } from '../../../codegen/codegenConfig';

/** A complete, valid PySpark 4.x project. */
const PYSPARK_FILES: GeneratedFile[] = [
    { filename: 'requirements.txt', content: 'pyspark>=4.0.0\npytest>=8.0.0\n' },
    {
        filename: 'src/pipeline.py',
        content: [
            'def read_sources(spark, config):',
            '    return {}',
            '',
            'def transform(dfs, config):',
            '    return dfs',
            '',
            'def write_sinks(dfs, config):',
            '    pass',
            '',
        ].join('\n'),
    },
    {
        filename: 'tests/test_pipeline.py',
        content: 'from src.pipeline import transform\n\ndef test_transform():\n    assert transform({}, {}) == {}\n',
    },
    { filename: 'run_local.sh', content: '#!/usr/bin/env bash\n' },
];

/**
 * Creates a fake executor that returns one outcome (or a sequence of outcomes).
 *
 * @param outcomes Outcome(s) to return.
 * @returns The executor mock.
 */
function fakeExecutor(
    ...outcomes: Array<{ exitCode: number; stdout?: string; stderr?: string; notFound?: boolean }>
) {
    const queue = [...outcomes];
    const fallback = outcomes[outcomes.length - 1] ?? { exitCode: 0 };
    return vi.fn<CommandExecutor>(async () => {
        const next = queue.shift() ?? fallback;
        return { stdout: '', stderr: '', ...next };
    });
}

describe('ReadOnlyCheckRunner.run (PySpark)', () => {
    it('passes every check for a complete PySpark project (simple)', async () => {
        const executor = fakeExecutor({ exitCode: 0 }, { exitCode: 0, stdout: '1 test collected' });
        const results = await new ReadOnlyCheckRunner(executor).run({
            files: PYSPARK_FILES,
            language: 'python',
            baseName: 'orders',
            runnerFiles: ['run_local.sh'],
        });
        expect(results.map((r) => [r.check, r.status])).toEqual([
            [CHECK_IDS.requiredFiles, 'passed'],
            [CHECK_IDS.leftoverTokens, 'passed'],
            [CHECK_IDS.pythonSyntax, 'passed'],
            [CHECK_IDS.pipelineContract, 'passed'],
            [CHECK_IDS.testImports, 'passed'],
            [CHECK_IDS.pytestCollect, 'passed'],
        ]);
        // First call is py_compile, second call is pytest --collect-only -q.
        expect(executor).toHaveBeenCalledTimes(2);
        expect(executor.mock.calls[0][1]).toEqual(['-m', 'py_compile', 'src/pipeline.py', 'tests/test_pipeline.py']);
        expect(executor.mock.calls[1][1]).toEqual(['-m', 'pytest', '--collect-only', '-q']);
    });

    it('reports missing files, leftover tokens, syntax errors, missing pipeline functions, wrong test imports, and pytest collection failure (failure)', async () => {
        const files: GeneratedFile[] = [
            {
                filename: 'src/pipeline.py',
                content: 'def read_sources(spark, config):\n    x = "{{pipeline_name}}" + "__REDACTED_BY_SPARK_DESIGNER__"\n',
            },
            { filename: 'tests/test_pipeline.py', content: 'import unittest\n' },
            { filename: '../escape.py', content: 'evil' },
        ];
        const executor = fakeExecutor(
            { exitCode: 1, stderr: 'SyntaxError: bad' },
            { exitCode: 2, stderr: 'ERROR collecting tests/test_pipeline.py' }
        );
        const results = await new ReadOnlyCheckRunner(executor).run({
            files,
            language: 'python',
            baseName: 'orders',
            runnerFiles: ['run_local.sh'],
        });
        const byId = Object.fromEntries(results.map((r) => [r.check, r]));
        expect(byId.required_files.output).toBe('Missing file(s): requirements.txt, run_local.sh');
        expect(byId.leftover_tokens.output).toBe(
            'src/pipeline.py: {{pipeline_name}}, __REDACTED_BY_SPARK_DESIGNER__'
        );
        expect(byId.python_syntax).toEqual({
            check: 'python_syntax',
            status: 'failed',
            output: 'SyntaxError: bad',
        });
        expect(byId.pipeline_contract).toEqual({
            check: 'pipeline_contract',
            status: 'failed',
            output: 'src/pipeline.py is missing required testable entrypoint function(s): transform, write_sinks.',
        });
        expect(byId.test_imports.status).toBe('failed');
        expect(byId.pytest_collect).toEqual({
            check: 'pytest_collect',
            status: 'failed',
            output: 'ERROR collecting tests/test_pipeline.py',
        });
        // The out-of-bounds file is never written or compiled.
        expect(executor.mock.calls[0][1]).toEqual(['-m', 'py_compile', 'src/pipeline.py', 'tests/test_pipeline.py']);
    });

    it('gracefully skips py_compile and pytest when python3 or pytest is absent (extreme)', async () => {
        const missingPython = await new ReadOnlyCheckRunner(
            fakeExecutor({ exitCode: -1, notFound: true }),
            'nopython'
        ).run({
            files: PYSPARK_FILES,
            language: 'python',
            baseName: 'orders',
            runnerFiles: [],
        });
        const byIdMissingPy = Object.fromEntries(missingPython.map((r) => [r.check, r]));
        expect(byIdMissingPy.python_syntax).toEqual({
            check: 'python_syntax',
            status: 'skipped',
            output: "Python interpreter 'nopython' not found.",
        });
        expect(byIdMissingPy.pytest_collect).toEqual({
            check: 'pytest_collect',
            status: 'skipped',
            output: "Python interpreter 'nopython' not found.",
        });

        // Python exists, but pytest module is not installed.
        const missingPytest = await new ReadOnlyCheckRunner(
            fakeExecutor(
                { exitCode: 0 },
                { exitCode: 1, stderr: '/usr/bin/python3: No module named pytest' }
            )
        ).run({
            files: PYSPARK_FILES,
            language: 'python',
            baseName: 'orders',
            runnerFiles: [],
        });
        const byIdMissingPytest = Object.fromEntries(missingPytest.map((r) => [r.check, r]));
        expect(byIdMissingPytest.python_syntax.status).toBe('passed');
        expect(byIdMissingPytest.pytest_collect.status).toBe('skipped');

        // Empty file list skips syntax, contract, imports, and pytest.
        const none = await new ReadOnlyCheckRunner(fakeExecutor({ exitCode: 0 })).run({
            files: [],
            language: 'python',
            baseName: 'orders',
            runnerFiles: [],
        });
        const byIdNone = Object.fromEntries(none.map((r) => [r.check, r]));
        expect(byIdNone.python_syntax.status).toBe('skipped');
        expect(byIdNone.pipeline_contract.status).toBe('skipped');
        expect(byIdNone.test_imports.status).toBe('skipped');
        expect(byIdNone.pytest_collect.status).toBe('skipped');
    });

    it('accepts every supported test import form for src.pipeline and src.<baseName> (extreme)', async () => {
        for (const line of [
            'from src.pipeline import transform',
            'import src.pipeline',
            'from pipeline import transform',
            'import pipeline',
            'from src.orders import transform',
            '  from src.pipeline import (read_sources, transform, write_sinks)',
        ]) {
            const files = [{ filename: 'tests/test_pipeline.py', content: `# test\n${line}\n` }];
            const results = await new ReadOnlyCheckRunner(fakeExecutor({ exitCode: 0 })).run({
                files,
                language: 'python',
                baseName: 'orders',
                runnerFiles: [],
            });
            const byId = Object.fromEntries(results.map((r) => [r.check, r]));
            expect(byId.test_imports.status, line).toBe('passed');
        }
    });
});

describe('helpers', () => {
    it('truncates long output with a marker (extreme)', () => {
        const long = 'x'.repeat(READ_ONLY_CHECK_LIMITS.maxOutputChars + 5);
        expect(truncateCheckOutput(long).endsWith('...[truncated]')).toBe(true);
        expect(truncateCheckOutput('short')).toBe('short');
    });

    it('returns the canonical PySpark source paths (simple)', () => {
        expect(expectedSourceFiles('python', 'orders')).toEqual(['src/pipeline.py', 'tests/test_pipeline.py']);
    });

    it('reports notFound from the default executor for a missing binary (failure)', async () => {
        const outcome = await defaultCommandExecutor('definitely-not-a-real-binary-xyz', [], {
            cwd: process.cwd(),
            timeoutMs: 5000,
        });
        expect(outcome.notFound).toBe(true);
        expect(outcome.exitCode).toBe(-1);
    });

    it('maps a killed process (timeout, no numeric exit code) to exit code 1 (failure)', async () => {
        const killed = await defaultCommandExecutor(process.execPath, ['-e', 'setTimeout(() => {}, 5000)'], {
            cwd: process.cwd(),
            timeoutMs: 100,
        });
        expect(killed.exitCode).toBe(1);
        expect(killed.notFound).toBeUndefined();
    });

    it('returns the exit code and output from the default executor (simple)', async () => {
        const ok = await defaultCommandExecutor(process.execPath, ['-e', 'process.stdout.write("hi")'], {
            cwd: process.cwd(),
            timeoutMs: 5000,
        });
        expect(ok).toEqual({ exitCode: 0, stdout: 'hi', stderr: '' });
        const bad = await defaultCommandExecutor(
            process.execPath,
            ['-e', 'process.stderr.write("no");process.exit(3)'],
            { cwd: process.cwd(), timeoutMs: 5000 }
        );
        expect(bad.exitCode).toBe(3);
        expect(bad.stderr).toBe('no');
    });
});
