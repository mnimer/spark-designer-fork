/**
 * Unit tests for the shared code generation model config (`buildCodegenModelConfig` in
 * `src/codegen/codegenConfig.ts`), used by the generator, the reviewer, and the runner.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect } from 'vitest';
import { CODEGEN_MODEL_SETTINGS, CODEGEN_MODEL_TUNING, buildCodegenModelConfig } from '../../../codegen/codegenConfig';

describe('buildCodegenModelConfig', () => {
    it('sets the structured-output base settings (simple)', () => {
        const config = buildCodegenModelConfig({ s: 1 }, {});
        expect(config).toEqual({
            responseMimeType: CODEGEN_MODEL_SETTINGS.responseMimeType,
            responseSchema: { s: 1 },
            maxOutputTokens: CODEGEN_MODEL_SETTINGS.maxOutputTokens,
        });
    });

    it('adds temperature and thinkingConfig only when tuning sets them (extreme)', () => {
        expect(buildCodegenModelConfig({}, { temperature: 0 })).toMatchObject({ temperature: 0 });
        expect(buildCodegenModelConfig({}, { thinkingLevel: 'LOW' })).toMatchObject({ thinkingConfig: { thinkingLevel: 'LOW' } });
        expect(buildCodegenModelConfig({}, { temperature: 0.2 })).not.toHaveProperty('thinkingConfig');
    });

    it('uses CODEGEN_MODEL_TUNING by default (simple)', () => {
        const config = buildCodegenModelConfig({});
        expect('temperature' in config).toBe(CODEGEN_MODEL_TUNING.temperature !== undefined);
        expect('thinkingConfig' in config).toBe(CODEGEN_MODEL_TUNING.thinkingLevel !== undefined);
    });
});
