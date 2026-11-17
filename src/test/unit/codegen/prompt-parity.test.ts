/**
 * Regression test: the extension and the integration runner build the SAME prompt.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Bug history: `codegen-runner.ts` used to keep its own copy of the prompt assembly
 * (`buildCodeGenPrompt`). The copy drifted. It also resolved `{{spark_version}}` after
 * inserting the YAML, which re-opened fix F7. Both call sites now use
 * `buildPromptFromPipelineYaml`. This test pins that.
 */

import { describe, it, expect, vi } from 'vitest';
import * as path from 'path';

vi.mock('vscode', async () => (await import('../helpers/vscodeMock')).createVscodeMock());

import { buildExtensionPrompt } from '../../../codeGeneration';
import { buildRunnerPrompt } from '../../integration/codegen-runner';

/** Repository root. */
const REPO_ROOT = path.join(__dirname, '..', '..', '..', '..');

/** Minimal extension context that resolves resources from the repository. */
const CONTEXT = { asAbsolutePath: (relative: string) => path.join(REPO_ROOT, relative) } as any;

/** A pipeline with a credential, a `{{spark_version}}` literal, and `$` sequences. */
const YAML_TEXT = [
    'nodes:',
    '  - id: src_kafka',
    '    data:',
    '      type: SOURCE',
    '      label: Kafka Source',
    '      spark_transform: KafkaSource',
    '      properties:',
    '        bootstrapServers: localhost:9092',
    '        subscribe: orders',
    '        password: hunter2',
    '        note: "keep {{spark_version}} and {{beam_version}} and $& literal"',
    '  - id: sink_iceberg',
    '    data:',
    '      type: SINK',
    '      label: Iceberg Sink',
    '      spark_transform: IcebergSink',
    '      properties:',
    '        table: catalog.db.orders',
    'edges:',
    '  - id: e1',
    '    source: src_kafka',
    '    target: sink_iceberg',
    '',
].join('\n');

describe('prompt parity: extension vs integration runner', () => {
    it('builds an identical PySpark prompt for the same input', () => {
        const extension = buildExtensionPrompt(CONTEXT, YAML_TEXT, 'PySpark', '4.2.0');
        const runner = buildRunnerPrompt('PySpark', YAML_TEXT, '4.2.0');
        expect(runner.prompt).toBe(extension.prompt);
        expect(runner.selectedSkills).toEqual(extension.selectedSkills);
        expect(runner.executionPlan).toEqual(extension.executionPlan);
        // Both redact before building (the runner used to skip redaction).
        expect(extension.redactedPaths.length).toBeGreaterThan(0);
        expect(extension.prompt).not.toContain('hunter2');
        expect(extension.prompt).toContain('## Execution Plan');
    });

    it('keeps {{spark_version}} in the user YAML literal while resolving it in skills (F7 regression)', () => {
        const { prompt } = buildRunnerPrompt('PySpark', YAML_TEXT, '4.2.0');
        expect(prompt).toContain('keep {{spark_version}} and {{beam_version}} and $& literal');
        expect(prompt).toContain('4.2.0');
        expect(prompt.match(/\{\{spark_version\}\}/g)).toHaveLength(1);
    });
});
