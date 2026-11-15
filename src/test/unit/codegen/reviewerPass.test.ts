/**
 * Unit tests for the reviewer pass wiring (`src/codegen/reviewerPass.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Uses a fake Gemini client, a fixture skills root, and a fake command executor. Covers the
 * setting (default off, on, missing configuration), the review and fix calls (model, schema,
 * token limit), the runner files passed to the checks, and an empty model response.
 */

import { describe, it, expect, vi, beforeAll, afterAll } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

vi.mock('vscode', async () => (await import('../helpers/vscodeMock')).createVscodeMock());

import * as vscode from 'vscode';
import { isReviewerEnabled, runReviewerPass, ReviewerModelClient } from '../../../codegen/reviewerPass';
import { ReadOnlyCheckRunner } from '../../../codegen/ReadOnlyCheckRunner';
import { REVIEW_REPORT_SCHEMA } from '../../../codegen/CodeReviewer';
import { CODEGEN_MODEL_SETTINGS, CODEGEN_REVIEWER_SETTING } from '../../../codegen/codegenConfig';
import { DEFAULT_GEMINI_MODEL } from '../../../constants';
import { clearCodegenSkillCatalogCache } from '../../../codegen/CodegenSkillCatalog';

let skillsRoot: string;

/**
 * Writes one fixture skill.
 *
 * @param name Skill name.
 * @param kind Kind.
 * @param usedBy usedBy list.
 * @param runners runners list.
 */
function writeSkill(name: string, kind: string, usedBy: string, runners = '[all]'): void {
    fs.mkdirSync(path.join(skillsRoot, name), { recursive: true });
    fs.writeFileSync(
        path.join(skillsRoot, name, 'SKILL.md'),
        `---\nname: ${name}\ndescription: d.\nmetadata:\n  kind: ${kind}\n  usedBy: ${usedBy}\n  languages: [all]\n  runners: ${runners}\n---\n${name.toUpperCase()} BODY`
    );
}

beforeAll(() => {
    skillsRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'reviewer-pass-'));
    writeSkill('codegen-common', 'common', '[generator, reviewer]');
    writeSkill('codegen-review', 'review', '[reviewer]');
    writeSkill('runner-direct', 'runner', '[generator, reviewer]', '[direct]');
    fs.mkdirSync(path.join(skillsRoot, 'runner-direct', 'templates', 'python'), { recursive: true });
    fs.writeFileSync(path.join(skillsRoot, 'runner-direct', 'templates', 'python', 'run_directrunner.sh'), '#!/bin/bash');
    clearCodegenSkillCatalogCache();
});

afterAll(() => {
    fs.rmSync(skillsRoot, { recursive: true, force: true });
    clearCodegenSkillCatalogCache();
});

describe('isReviewerEnabled', () => {
    it('is off by default (simple)', () => {
        expect(isReviewerEnabled((_section) => ({ get: <T>(_key: string, defaultValue: T) => defaultValue }))).toBe(false);
    });

    it('is on when the setting is true and reads the right section and key (simple)', () => {
        const get = vi.fn(() => true);
        const getConfiguration = vi.fn(() => ({ get: get as any }));
        expect(isReviewerEnabled(getConfiguration)).toBe(true);
        expect(getConfiguration).toHaveBeenCalledWith(CODEGEN_REVIEWER_SETTING.section);
        expect(get).toHaveBeenCalledWith(CODEGEN_REVIEWER_SETTING.key, false);
    });

    it('is off when the configuration is missing or holds a non-boolean (failure)', () => {
        expect(isReviewerEnabled(() => undefined)).toBe(false);
        expect(isReviewerEnabled(() => ({ get: (() => 'yes') as any }))).toBe(false);
    });

    it('works with the VS Code configuration accessor (simple)', () => {
        // The shared vscode mock returns undefined for every key.
        expect(isReviewerEnabled((section) => vscode.workspace.getConfiguration(section) as any)).toBe(false);
        expect(vscode.workspace.getConfiguration).toHaveBeenCalledWith(CODEGEN_REVIEWER_SETTING.section);
    });
});

describe('runReviewerPass', () => {
    it('reviews with the reviewer schema, fixes with the generator schema, and checks runner files (simple)', async () => {
        const responses = [
            JSON.stringify({ findings: [{ file: 'src/orders.py', severity: 'blocking', problem: 'p', fix: 'f', skillRule: 'lang-python' }], summary: 's' }),
            JSON.stringify({ files: [{ filename: 'src/orders.py', content: 'fixed' }] }),
            JSON.stringify({ findings: [], summary: 'clean' }),
        ];
        const generateContent = vi.fn(async () => ({ text: responses.shift() }));
        const ai: ReviewerModelClient = { models: { generateContent } };
        const executor = vi.fn(async () => ({ exitCode: 0, stdout: '', stderr: '' }));
        const progress: string[] = [];

        const outcome = await runReviewerPass({
            ai,
            files: [{ filename: 'src/orders.py', content: 'bad' }],
            generationPrompt: 'GEN',
            yamlText: 'nodes: []\n',
            language: 'PySpark',
            baseName: 'orders',
            skillsRoot,
            reviewerTemplate: 'REVIEW TEMPLATE',
            fixTemplate: 'FIX TEMPLATE',
            responseSchema: { generator: true },
            onProgress: (m) => progress.push(m),
            checkRunner: new ReadOnlyCheckRunner(executor),
        });

        expect(outcome.fixRounds).toBe(1);
        expect(outcome.files[0].content).toBe('fixed');
        const calls = generateContent.mock.calls as unknown as Array<[{ model: string; contents: string; config: any }]>;
        expect(calls).toHaveLength(3);
        // Review call: reviewer template + reviewer-only skill + check results naming the missing runner file.
        expect(calls[0][0].model).toBe(DEFAULT_GEMINI_MODEL);
        expect(calls[0][0].config.responseSchema).toBe(REVIEW_REPORT_SCHEMA);
        expect(calls[0][0].config.maxOutputTokens).toBe(CODEGEN_MODEL_SETTINGS.maxOutputTokens);
        expect(calls[0][0].contents).toContain('REVIEW TEMPLATE');
        expect(calls[0][0].contents).toContain('CODEGEN-REVIEW BODY');
        expect(calls[0][0].contents).toContain('run_directrunner.sh');
        // Fix call: generator schema and generation prompt.
        expect(calls[1][0].config.responseSchema).toEqual({ generator: true });
        expect(calls[1][0].contents.startsWith('GEN')).toBe(true);
        expect(progress.length).toBeGreaterThan(0);
    });

    it('records an error when the model returns an empty response (failure)', async () => {
        const ai: ReviewerModelClient = { models: { generateContent: vi.fn(async () => ({ text: '' })) } };
        const outcome = await runReviewerPass({
            ai,
            files: [],
            generationPrompt: 'GEN',
            yamlText: 'nodes: []\n',
            language: 'PySpark',
            baseName: 'orders',
            skillsRoot,
            reviewerTemplate: 'R',
            fixTemplate: 'F',
            responseSchema: {},
            // No checkRunner: the default runner is used. No file exists, so no process starts.
        });
        expect(outcome.error).toBe('Empty response from Gemini during review.');
    });
});
