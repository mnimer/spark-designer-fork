/**
 * Unit tests for the read-only check runner (`src/codegen/ReadOnlyCheckRunner.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Uses a fake command executor, so no process is spawned except in the one test that runs
 * the default executor against a missing binary. Covers every check in pass, fail, and skip
 * states, path containment, output truncation, and the Java bracket scanner edge cases.
 */

import { describe, it, expect, vi } from 'vitest';
import {
    CHECK_IDS,
    CommandExecutor,
    GeneratedFile,
    ReadOnlyCheckRunner,
    defaultCommandExecutor,
    expectedSourceFiles,
    findJavaBracketProblem,
    truncateCheckOutput,
} from '../../../codegen/ReadOnlyCheckRunner';
import { READ_ONLY_CHECK_LIMITS } from '../../../codegen/codegenConfig';

/** A complete, valid Python project. */
const PY_FILES: GeneratedFile[] = [
    { filename: 'requirements.txt', content: 'apache-beam[gcp]>=2.76.0' },
    { filename: 'src/orders.py', content: 'def run():\n    pass\n' },
    { filename: 'src/orders_test.py', content: 'from src.orders import run\n' },
    { filename: 'run_directrunner.sh', content: '#!/bin/bash' },
];

/** A complete, valid Java project. */
const JAVA_FILES: GeneratedFile[] = [
    { filename: 'pom.xml', content: '<project/>' },
    { filename: 'src/main/java/com/example/Orders.java', content: 'class Orders { void run() { int[] a = {1}; } }' },
    { filename: 'src/test/java/com/example/OrdersTest.java', content: 'class OrdersTest {}' },
];

/**
 * Creates a fake executor that returns one outcome.
 *
 * @param outcome Outcome to return.
 * @returns The executor mock.
 */
function fakeExecutor(outcome: { exitCode: number; stdout?: string; stderr?: string; notFound?: boolean }) {
    return vi.fn<CommandExecutor>(async () => ({ stdout: '', stderr: '', ...outcome }));
}

describe('ReadOnlyCheckRunner.run (Python)', () => {
    it('passes every check for a complete project (simple)', async () => {
        const executor = fakeExecutor({ exitCode: 0 });
        const results = await new ReadOnlyCheckRunner(executor).run({ files: PY_FILES, language: 'python', baseName: 'orders', runnerFiles: ['run_directrunner.sh'] });
        expect(results.map((r) => [r.check, r.status])).toEqual([
            [CHECK_IDS.requiredFiles, 'passed'],
            [CHECK_IDS.leftoverTokens, 'passed'],
            [CHECK_IDS.pythonSyntax, 'passed'],
            [CHECK_IDS.javaBraces, 'skipped'],
            [CHECK_IDS.testImports, 'passed'],
        ]);
        // py_compile receives every .py file relative to the temp folder.
        const [, args] = executor.mock.calls[0];
        expect(args).toEqual(['-m', 'py_compile', 'src/orders.py', 'src/orders_test.py']);
    });

    it('reports missing files, leftover tokens, syntax errors, and wrong test imports (failure)', async () => {
        const files: GeneratedFile[] = [
            { filename: 'src/orders.py', content: 'x = "{{pipeline_name}}" + "__REDACTED_BY_BEAM_DESIGNER__"' },
            { filename: 'src/orders_test.py', content: 'from src.pipeline import run' },
            { filename: '../escape.py', content: 'evil' },
        ];
        const executor = fakeExecutor({ exitCode: 1, stderr: 'SyntaxError: bad' });
        const results = await new ReadOnlyCheckRunner(executor).run({ files, language: 'python', baseName: 'orders', runnerFiles: ['run_directrunner.sh'] });
        const byId = Object.fromEntries(results.map((r) => [r.check, r]));
        expect(byId.required_files.output).toBe('Missing file(s): requirements.txt, run_directrunner.sh');
        expect(byId.leftover_tokens.output).toBe('src/orders.py: {{pipeline_name}}, __REDACTED_BY_BEAM_DESIGNER__');
        expect(byId.python_syntax).toEqual({ check: 'python_syntax', status: 'failed', output: 'SyntaxError: bad' });
        expect(byId.test_imports.status).toBe('failed');
        // The out-of-bounds file is never written or compiled.
        expect(executor.mock.calls[0][1]).toEqual(['-m', 'py_compile', 'src/orders.py', 'src/orders_test.py']);
    });

    it('skips py_compile when the interpreter is missing or no .py file exists (extreme)', async () => {
        const missing = await new ReadOnlyCheckRunner(fakeExecutor({ exitCode: -1, notFound: true }), 'nopython').run({
            files: PY_FILES, language: 'python', baseName: 'orders', runnerFiles: [],
        });
        expect(missing[2]).toEqual({ check: 'python_syntax', status: 'skipped', output: "Python interpreter 'nopython' not found." });

        const none = await new ReadOnlyCheckRunner(fakeExecutor({ exitCode: 0 })).run({ files: [], language: 'python', baseName: 'orders', runnerFiles: [] });
        expect(none[2].status).toBe('skipped');
        expect(none[4]).toEqual({ check: 'test_imports', status: 'skipped', output: 'No src/orders_test.py generated.' });
    });

    it('accepts every supported test import form (extreme)', async () => {
        for (const line of ['import src.orders', 'from orders import run', 'import orders', '  from src.orders import (run)']) {
            const files = [{ filename: 'src/orders_test.py', content: `# test\n${line}\n` }];
            const results = await new ReadOnlyCheckRunner(fakeExecutor({ exitCode: 0 })).run({ files, language: 'python', baseName: 'orders', runnerFiles: [] });
            expect(results[4].status, line).toBe('passed');
        }
    });
});

describe('ReadOnlyCheckRunner.run (Java)', () => {
    it('passes for a complete project and skips Python checks (simple)', async () => {
        const executor = fakeExecutor({ exitCode: 0 });
        const results = await new ReadOnlyCheckRunner(executor).run({ files: JAVA_FILES, language: 'java', baseName: 'orders', runnerFiles: [] });
        expect(results.map((r) => r.status)).toEqual(['passed', 'passed', 'skipped', 'passed', 'skipped']);
        expect(executor).not.toHaveBeenCalled();
    });

    it('reports unbalanced braces and skips when no Java file exists (failure)', async () => {
        const runner = new ReadOnlyCheckRunner(fakeExecutor({ exitCode: 0 }));
        const broken = await runner.run({
            files: [{ filename: 'A.java', content: 'class A {\n void f() {\n' }],
            language: 'java', baseName: 'a', runnerFiles: [],
        });
        expect(broken[3]).toEqual({ check: 'java_braces', status: 'failed', output: "A.java: Unclosed '{' opened at line 2." });
        const none = await runner.run({ files: [], language: 'java', baseName: 'a', runnerFiles: [] });
        expect(none[3].status).toBe('skipped');
    });
});

describe('findJavaBracketProblem', () => {
    it.each([
        ['balanced code', 'class A { void f(int[] a) { g(a[0]); } }', null],
        ['braces in strings and chars', 'class A { String s = "}{)("; char c = \'}\'; String e = "\\"}"; }', null],
        ['braces in comments', 'class A { // }\n /* { ( */ }', null],
        ['a text block', 'class A { String t = """\n } { \n"""; }', null],
        ['an unclosed block comment', 'class A { } /* never closed {', null],
        ['an unclosed text block', 'class A { } String t = """ {', null],
        ['a line comment at the end', 'class A { } // done', null],
        ['an extra closing brace', 'class A { }\n}', "Unexpected '}' at line 2."],
        ['a mismatched pair', 'class A { f(]; }', "Unexpected ']' at line 1."],
        ['a missing close after a multi-line comment', '/*\n\n*/ class A {', "Unclosed '{' opened at line 3."],
    ])('handles %s', (_label, source, expected) => {
        expect(findJavaBracketProblem(source)).toBe(expected);
    });
});

describe('helpers', () => {
    it('truncates long output with a marker (extreme)', () => {
        const long = 'x'.repeat(READ_ONLY_CHECK_LIMITS.maxOutputChars + 5);
        expect(truncateCheckOutput(long).endsWith('...[truncated]')).toBe(true);
        expect(truncateCheckOutput('short')).toBe('short');
    });

    it('returns the routed source paths per language (simple)', () => {
        expect(expectedSourceFiles('python', 'orders')).toEqual(['src/orders.py', 'src/orders_test.py']);
        expect(expectedSourceFiles('java', 'orders')).toEqual(['src/main/java/com/example/Orders.java', 'src/test/java/com/example/OrdersTest.java']);
    });

    it('reports notFound from the default executor for a missing binary (failure)', async () => {
        const outcome = await defaultCommandExecutor('definitely-not-a-real-binary-xyz', [], { cwd: process.cwd(), timeoutMs: 5000 });
        expect(outcome.notFound).toBe(true);
        expect(outcome.exitCode).toBe(-1);
    });

    it('maps a killed process (timeout, no numeric exit code) to exit code 1 (failure)', async () => {
        const killed = await defaultCommandExecutor(process.execPath, ['-e', 'setTimeout(() => {}, 5000)'], { cwd: process.cwd(), timeoutMs: 100 });
        expect(killed.exitCode).toBe(1);
        expect(killed.notFound).toBeUndefined();
    });

    it('returns the exit code and output from the default executor (simple)', async () => {
        const ok = await defaultCommandExecutor(process.execPath, ['-e', 'process.stdout.write("hi")'], { cwd: process.cwd(), timeoutMs: 5000 });
        expect(ok).toEqual({ exitCode: 0, stdout: 'hi', stderr: '' });
        const bad = await defaultCommandExecutor(process.execPath, ['-e', 'process.stderr.write("no");process.exit(3)'], { cwd: process.cwd(), timeoutMs: 5000 });
        expect(bad.exitCode).toBe(3);
        expect(bad.stderr).toBe('no');
    });
});
