/**
 * Security Wiring Guard Tests
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * The fixes for findings C1 (chmod allowlist), C2 (credential redaction) and C3 (scoped
 * cleanup) all live in well-tested modules. Those module tests prove the helpers behave
 * correctly IN ISOLATION. None of them prove the code-generation path still CALLS them.
 *
 * That is a real hole. If a refactor deletes the `redactPipelineYaml` call, every one of
 * the existing tests still passes while plaintext database passwords resume flowing to
 * the Gemini API and into gemini.log. The same is true of the chmod allowlist: drop the
 * guard and `chmod 755` is handed back to model-controlled filenames.
 *
 * These tests therefore assert WIRING, not behaviour. They are deliberately written
 * against the source text of whichever file performs the work, rather than against a
 * fixed line range or a specific function, so that they keep working when the pending
 * A1/A3 refactors move this logic into a new module.
 *
 * NOTE: a source-text assertion is a blunt instrument. It is used here because the real
 * call path requires a live GoogleGenAI client and a writable output tree, which is not
 * worth mocking for a guard whose only job is to fail loudly when a call disappears.
 */

import { describe, it, expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';

const SRC_DIR = path.resolve(__dirname, '../..');

/** Reads every non-test TypeScript file under src/ as { file, text }. */
const productionSources = (): Array<{ file: string; text: string }> => {
    const collect = (dir: string): Array<{ file: string; text: string }> => {
        const results: Array<{ file: string; text: string }> = [];
        for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
            if (entry.isDirectory()) {
                if (entry.name !== 'test') {
                    results.push(...collect(path.join(dir, entry.name)));
                }
            } else if (entry.name.endsWith('.ts')) {
                const fullPath = path.join(dir, entry.name);
                results.push({
                    file: path.relative(SRC_DIR, fullPath),
                    text: fs.readFileSync(fullPath, 'utf-8')
                });
            }
        }
        return results;
    };
    return collect(SRC_DIR);
};

describe('security wiring — C2 credential redaction reaches the Gemini call', () => {
    // Locate every file that performs a model call, wherever it now lives.
    // NOTE: since the skill-based prompt builder, two production files call the model: the
    // generator (codeGeneration.ts) and the optional reviewer (codegen/reviewerPass.ts).
    // Both must build their prompts through the shared, redacting entry point.
    const sources = productionSources();
    const callers = sources.filter((s) => s.text.includes('generateContent('));
    const builder = sources.find((s) => s.file === path.join('codegen', 'CodegenPromptBuilder.ts'));
    const PROMPT_ENTRY_POINTS = ['buildPromptFromPipelineYaml(', 'buildExtensionPrompt('];

    it('only the generator and the optional reviewer perform model calls', () => {
        // If a third caller appears, it needs its own redaction guard here.
        expect(callers.map((c) => c.file).sort()).toEqual(['codeGeneration.ts', path.join('codegen', 'reviewerPass.ts')]);
    });

    it('every model caller builds its prompt through the shared redacting builder BEFORE the call', () => {
        for (const { text, file } of callers) {
            const buildIdx = Math.min(...PROMPT_ENTRY_POINTS.map((e) => text.indexOf(e)).filter((i) => i >= 0));
            // `.generateContent(` is the call site (the reviewer also declares the method in an interface).
            const callIdx = text.indexOf('.generateContent(');
            expect(callIdx, `${file} has no generateContent call site`).toBeGreaterThan(-1);
            expect(Number.isFinite(buildIdx), `${file} never builds its prompt with the shared builder`).toBe(true);
            expect(buildIdx, `${file} calls generateContent before building the redacted prompt`).toBeLessThan(callIdx);
        }
    });

    it('the shared builder redacts the pipeline BEFORE assembling the prompt', () => {
        expect(builder, 'codegen/CodegenPromptBuilder.ts not found').toBeDefined();
        const text = builder!.text;
        const entry = text.indexOf('export function buildPromptFromPipelineYaml(');
        const redactIdx = text.indexOf('redactPipelineDocument(', entry);
        const buildIdx = text.indexOf('.build(', entry);
        expect(redactIdx, 'buildPromptFromPipelineYaml never calls redactPipelineDocument').toBeGreaterThan(-1);
        expect(redactIdx, 'the builder assembles the prompt before redaction').toBeLessThan(buildIdx);
    });

    it('appends the redaction notice so the model does not embed the sentinel literally', () => {
        expect(builder!.text, 'CodegenPromptBuilder never calls buildRedactionPromptNotice').toContain(
            'buildRedactionPromptNotice('
        );
    });

    it('writes gemini.log only from the redacted prompt text', () => {
        // Both log sites log `promptText`, which is built from the redacted YAML. If a
        // future change logs the raw pipeline instead, this catches it.
        const generator = callers.find((c) => c.file === 'codeGeneration.ts')!;
        const rawLogPatterns = [/prompt:\s*yamlData/, /prompt:\s*rawYaml/, /prompt:\s*pipelineYaml/];
        for (const pattern of rawLogPatterns) {
            expect(
                pattern.test(generator.text),
                `${generator.file} appears to log the unredacted pipeline (${pattern})`
            ).toBe(false);
        }
        expect(generator.text).toMatch(/prompt:\s*promptText/);
    });
});

describe('security wiring — C1 chmod stays behind the allowlist', () => {
    it('every chmod on a model-influenced path is guarded by ALLOWED_EXECUTABLE_SCRIPTS', () => {
        for (const { file, text } of productionSources()) {
            const lines = text.split('\n');
            lines.forEach((line, i) => {
                const call = /\bchmod\(\s*([A-Za-z0-9_.]+)/.exec(line);
                if (!call) return;
                const target = call[1];

                // Look back a short window for either form of safety.
                const windowStart = Math.max(0, i - 15);
                const preceding = lines.slice(windowStart, i).join('\n');

                // Form 1: the allowlist is consulted, either directly or through the
                // sanctioned `isAllowedExecutableScript()` wrapper. Required whenever the
                // filename could have come from the model. A separate test below asserts
                // that wrapper actually queries the set, so it cannot be hollowed out.
                if (
                    preceding.includes('ALLOWED_EXECUTABLE_SCRIPTS') ||
                    preceding.includes('isAllowedExecutableScript(')
                ) {
                    return;
                }

                // Form 2: the target is built from a hardcoded literal in this source file,
                // e.g. `const runScriptPath = path.join(folderPath, 'run_directrunner.sh')`.
                // The model cannot influence that name, so C1 does not apply. This escape
                // hatch is intentionally narrow: it requires a quoted .sh literal on the
                // very assignment of the variable being chmod'ed.
                const literalAssign = new RegExp(
                    `${target}\\s*=\\s*path\\.join\\([^)]*['"][^'"]+\\.sh['"]\\s*\\)`
                );
                if (literalAssign.test(preceding)) return;

                throw new Error(
                    `${file}:${i + 1} calls chmod on '${target}' with neither an ` +
                    `ALLOWED_EXECUTABLE_SCRIPTS check nor a hardcoded .sh filename in the ` +
                    `preceding 15 lines. If this path can be influenced by model output, ` +
                    `it must go through the allowlist (finding C1).`
                );
            });
        }
    });

    it('the allowlist is still consulted with .has() rather than being bypassed', () => {
        const users = productionSources().filter((s) =>
            s.text.includes('ALLOWED_EXECUTABLE_SCRIPTS')
        );
        expect(users.length, 'nothing references the executable allowlist any more').toBeGreaterThan(0);
        const consulted = users.some((s) => /ALLOWED_EXECUTABLE_SCRIPTS\.has\(/.test(s.text));
        expect(consulted, 'the allowlist is referenced but never queried with .has()').toBe(true);
    });
});

describe('security wiring — C3 cleanup stays scoped to the manifest', () => {
    it('cleanup is invoked through executeScopedCleanup, not a bare directory wipe', () => {
        const sources = productionSources();
        const invokes = sources.some((s) => s.text.includes('executeScopedCleanup('));
        expect(invokes, 'nothing calls executeScopedCleanup; cleanup may have been inlined').toBe(true);
    });

    it('no production file recursively removes the whole output folder', () => {
        // The pre-fix behaviour was to wipe targetFolderPath before the model call, which
        // destroyed user files. Guard against it being reintroduced in any form.
        for (const { file, text } of productionSources()) {
            const dangerous = [
                /rm\(\s*targetFolderPath/,
                /rmSync\(\s*targetFolderPath/,
                /rmdir\(\s*targetFolderPath/,
            ];
            for (const pattern of dangerous) {
                expect(
                    pattern.test(text),
                    `${file} recursively deletes the output folder (${pattern}) -- this is the C3 bug`
                ).toBe(false);
            }
        }
    });
});
