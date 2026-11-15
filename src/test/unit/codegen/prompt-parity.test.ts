/**
 * Regression test: the extension and the integration runner build the SAME prompt.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Bug history: `codegen-runner.ts` used to keep its own copy of the prompt assembly
 * (`buildCodeGenPrompt`). The copy drifted. It replaced `{{sparkrunner_template_section}}`,
 * which no prompt contained, so the integration tests never sent the Spark template. It
 * also resolved `{{beam_version}}` after inserting the YAML, which re-opened fix F7. Both
 * call sites now use `buildPromptFromPipelineYaml`. This test pins that.
 */

import { describe, it, expect, vi } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';

vi.mock('vscode', async () => (await import('../helpers/vscodeMock')).createVscodeMock());

import { buildExtensionPrompt } from '../../../codeGeneration';
import { buildRunnerPrompt } from '../../integration/codegen-runner';

/** Repository root. */
const REPO_ROOT = path.join(__dirname, '..', '..', '..', '..');

/** Minimal extension context that resolves resources from the repository. */
const CONTEXT = { asAbsolutePath: (relative: string) => path.join(REPO_ROOT, relative) } as any;

/** A pipeline with a credential, a `{{beam_version}}` literal, and `$` sequences. */
const YAML_TEXT = fs.readFileSync(path.join(REPO_ROOT, 'resources', 'samples', 'kafka-to-iceberg.beam'), 'utf-8') +
    '\nnotes:\n  password: hunter2\n  text: "keep {{beam_version}} and $& literal"\n';

describe('prompt parity: extension vs integration runner', () => {
    it.each(['Python', 'Java'] as const)('builds an identical %s prompt for the same input', (language) => {
        const extension = buildExtensionPrompt(CONTEXT, YAML_TEXT, language, '2.99.0');
        const runner = buildRunnerPrompt(language, YAML_TEXT, '2.99.0');
        expect(runner.prompt).toBe(extension.prompt);
        expect(runner.selectedSkills).toEqual(extension.selectedSkills);
        // Both redact before building (the runner used to skip redaction).
        expect(extension.redactedPaths.length).toBeGreaterThan(0);
        expect(extension.prompt).not.toContain('hunter2');
    });

    it('sends the Spark runner template for Java (regression: runner never sent it)', () => {
        const { prompt, selectedSkills } = buildRunnerPrompt('Java', YAML_TEXT, '2.99.0');
        expect(selectedSkills).toContain('runner-spark');
        expect(prompt).toContain('#### Template: run_sparkrunner.sh');
        expect(prompt).not.toContain('sparkrunner_template_section');
    });

    it('keeps {{beam_version}} in the user YAML literal while resolving it in skills (F7 regression)', () => {
        const { prompt } = buildRunnerPrompt('Python', YAML_TEXT, '2.99.0');
        expect(prompt).toContain('keep {{beam_version}} and $& literal');
        expect(prompt).toContain('2.99.0');
        expect(prompt.match(/\{\{beam_version\}\}/g)).toHaveLength(1);
    });
});
