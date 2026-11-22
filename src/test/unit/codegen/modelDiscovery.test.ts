/**
 * Unit tests for dynamic Gemini code-generation model discovery (`src/codegen/modelDiscovery.ts`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Covers resource name normalization, non-Google publisher rejection, Gemini 3.x+ version
 * parsing, deprecated Gemini 1.x and 2.x exclusion, media and live model filtering, tier
 * classification, Flash-default ranking, Vertex AI global discovery, Gemini Developer API
 * fallback, offline fallback, and selected model validation.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { DEFAULT_GEMINI_MODEL } from '../../../core/constants';
import {
    classifyGeminiModelTier,
    createDefaultCodegenModelOption,
    discoverCodegenModels,
    filterAndRankCodegenModels,
    isSupportedCodegenModel,
    normalizeGeminiModelId,
    parseGeminiModelVersion,
    resolveDefaultCodegenModel,
    resolveSelectedCodegenModel,
} from '../../../codegen/modelDiscovery';

/**
 * Creates an async iterable pager from a static array of items.
 *
 * @param items Items yielded by the simulated SDK pager.
 * @returns Async iterable yielding each item in order.
 */
function createAsyncPager<T>(items: readonly T[]): AsyncIterable<T> {
    return {
        async *[Symbol.asyncIterator]() {
            for (const item of items) {
                yield item;
            }
        },
    };
}

describe('modelDiscovery — createDefaultCodegenModelOption', () => {
    it('returns the canonical offline Gemini 4 Hydrogen option derived from DEFAULT_GEMINI_MODEL', () => {
        expect(DEFAULT_GEMINI_MODEL).toBe('gemini-4-hydrogen');
        expect(createDefaultCodegenModelOption()).toEqual({
            id: DEFAULT_GEMINI_MODEL,
            displayName: 'Gemini 4 Hydrogen',
            tier: 'flash',
            version: '4.0',
            isDefault: true,
        });
    });
});

describe('modelDiscovery — normalizeGeminiModelId', () => {
    it('normalizes Vertex AI publisher, project-scoped, and Developer API model names', () => {
        expect(normalizeGeminiModelId('publishers/google/models/gemini-4-hydrogen')).toBe(
            'gemini-4-hydrogen'
        );
        expect(
            normalizeGeminiModelId(
                'projects/my-gcp-project/locations/global/publishers/google/models/gemini-3.8-flash@default'
            )
        ).toBe('gemini-3.8-flash');
        expect(normalizeGeminiModelId('models/gemini-3.8-flash-001')).toBe('gemini-3.8-flash-001');
        expect(normalizeGeminiModelId('  GEMINI-4-ARGON@002  ')).toBe('gemini-4-argon');
    });

    it('rejects non-Google publisher paths and non-Gemini identifiers', () => {
        expect(normalizeGeminiModelId('publishers/anthropic/models/claude-3-7-sonnet')).toBe('');
        expect(normalizeGeminiModelId('publishers/meta/models/llama-3.3-70b')).toBe('');
        expect(
            normalizeGeminiModelId(
                'projects/my-proj/locations/global/publishers/anthropic/models/gemini-4-spoof'
            )
        ).toBe('');
        expect(normalizeGeminiModelId('models/text-bison-001')).toBe('');
        expect(normalizeGeminiModelId('tunedModels/gemini-3.8-flash-custom')).toBe('');
    });

    it('rejects empty, nullish, and unsafe path inputs', () => {
        expect(normalizeGeminiModelId('')).toBe('');
        expect(normalizeGeminiModelId('   ')).toBe('');
        expect(normalizeGeminiModelId(null)).toBe('');
        expect(normalizeGeminiModelId(undefined)).toBe('');
        expect(normalizeGeminiModelId('gemini-3.8/../evil')).toBe('');
        expect(normalizeGeminiModelId('gemini-3.8..flash')).toBe('');
        expect(normalizeGeminiModelId('gemini-3.8$flash')).toBe('');
    });
});

describe('modelDiscovery — parseGeminiModelVersion', () => {
    it('parses major, minor, and patch tuples from Gemini 3.x and 4.x model IDs', () => {
        expect(parseGeminiModelVersion('gemini-4-hydrogen')).toEqual([4, 0, 0]);
        expect(parseGeminiModelVersion('gemini-4.1-flash')).toEqual([4, 1, 0]);
        expect(parseGeminiModelVersion('gemini-3.8-flash')).toEqual([3, 8, 0]);
        expect(parseGeminiModelVersion('gemini-3.8.2-pro')).toEqual([3, 8, 2]);
        expect(parseGeminiModelVersion('gemini-3-flash-preview')).toEqual([3, 0, 0]);
    });

    it('returns null for unversioned or non-Gemini model names', () => {
        expect(parseGeminiModelVersion('gemini-pro')).toBeNull();
        expect(parseGeminiModelVersion('gemini-flash-latest')).toBeNull();
        expect(parseGeminiModelVersion('claude-3-7-sonnet')).toBeNull();
    });
});

describe('modelDiscovery — classifyGeminiModelTier', () => {
    it('classifies flash-lite before flash, then flash (including hydrogen), pro, and other', () => {
        expect(classifyGeminiModelTier('gemini-3.8-flash-lite')).toBe('flash-lite');
        expect(classifyGeminiModelTier('gemini-4-lite')).toBe('flash-lite');
        expect(classifyGeminiModelTier('gemini-4-gluon')).toBe('flash-lite');
        expect(classifyGeminiModelTier('gemini-4-hydrogen')).toBe('flash');
        expect(classifyGeminiModelTier('gemini-3.8-flash')).toBe('flash');
        expect(classifyGeminiModelTier('gemini-4-argon')).toBe('pro');
        expect(classifyGeminiModelTier('gemini-4-carbon')).toBe('pro');
        expect(classifyGeminiModelTier('gemini-4-neon')).toBe('pro');
        expect(classifyGeminiModelTier('gemini-4-cobalt')).toBe('pro');
        expect(classifyGeminiModelTier('gemini-4-krypton')).toBe('pro');
        expect(classifyGeminiModelTier('gemini-3.8-pro')).toBe('pro');
        expect(classifyGeminiModelTier('gemini-4-ultra')).toBe('other');
    });
});

describe('modelDiscovery — isSupportedCodegenModel', () => {
    it('accepts Gemini 3.x and 4.x text-generation models including empty SDK base metadata', () => {
        expect(
            isSupportedCodegenModel({
                name: 'publishers/google/models/gemini-4-hydrogen',
                supportedActions: ['generateContent', 'countTokens'],
            })
        ).toBe(true);
        expect(
            isSupportedCodegenModel({
                name: 'models/gemini-3.8-flash',
                tunedModelInfo: {},
                supportedActions: ['generateContent'],
            })
        ).toBe(true);
        expect(
            isSupportedCodegenModel({
                name: 'publishers/google/models/gemini-3.8-pro',
                tunedModelInfo: { createTime: '2026-01-01T00:00:00Z' },
            })
        ).toBe(true);
    });

    it('rejects deprecated Gemini 1.x, 2.x, 2.5-*, and unversioned models', () => {
        for (const legacy of [
            'models/gemini-1.0-pro',
            'models/gemini-1.5-pro',
            'models/gemini-1.5-flash',
            'models/gemini-2.0-flash',
            'models/gemini-2.5-flash',
            'models/gemini-2.5-pro',
            'models/gemini-pro',
            'models/gemini-flash-latest',
        ]) {
            expect(isSupportedCodegenModel({ name: legacy }), legacy).toBe(false);
        }
    });

    it('rejects tuned models, non-generateContent actions, and media/embedding/live/specialized models', () => {
        expect(
            isSupportedCodegenModel({
                name: 'models/gemini-3.8-flash',
                tunedModelInfo: { baseModel: 'models/gemini-3.8-flash' },
            })
        ).toBe(false);
        expect(
            isSupportedCodegenModel({
                name: 'models/gemini-3.8-flash',
                tunedModelInfo: true,
            })
        ).toBe(false);
        expect(
            isSupportedCodegenModel({
                name: 'models/gemini-3.8-flash',
                supportedActions: ['bidiGenerateContent'],
            })
        ).toBe(false);
        expect(
            isSupportedCodegenModel({
                name: 'models/gemini-3.8-flash',
                supportedActions: ['embedContent'],
            })
        ).toBe(false);

        for (const nonCodegen of [
            'models/gemini-3.8-embed-001',
            'models/gemini-embedding-001',
            'models/gemini-3.8-flash-image',
            'models/gemini-3-imagen',
            'models/gemini-3.8-vision',
            'models/gemini-3.8-flash-tts',
            'models/gemini-3.8-flash-audio',
            'models/gemini-3.8-speech',
            'models/gemini-3.8-voice',
            'models/gemini-3-lyria-music',
            'models/gemini-3-veo-video',
            'models/gemini-3.8-flash-live',
            'models/gemini-3.8-realtime',
            'models/gemini-3-robotics',
            'models/gemini-3-computer-use',
            'models/gemini-3-cyber',
            'models/gemini-3-medlm',
            'models/gemini-3-learnlm',
            'models/gemini-3-aqa',
            'models/gemini-3-transcribe',
        ]) {
            expect(isSupportedCodegenModel({ name: nonCodegen }), nonCodegen).toBe(false);
        }
    });
});

describe('modelDiscovery — filterAndRankCodegenModels & resolveDefaultCodegenModel', () => {
    it('ranks Gemini 4 Hydrogen above 3.8 Flash, stable GA above preview, and marks Flash as default', () => {
        const ranked = filterAndRankCodegenModels([
            { name: 'publishers/google/models/gemini-2.5-flash', displayName: 'Gemini 2.5 Flash' },
            { name: 'publishers/google/models/gemini-3.8-flash-preview', displayName: 'Gemini 3.8 Flash Preview' },
            { name: 'publishers/google/models/gemini-3.8-flash', displayName: 'publishers/google/models/gemini-3.8-flash' },
            { name: 'publishers/google/models/gemini-4-argon', displayName: 'Gemini 4 Argon' },
            { name: 'publishers/google/models/gemini-4-hydrogen' },
            { name: 'publishers/google/models/gemini-4-gluon', displayName: 'Gemini 4 Gluon' },
            { name: 'publishers/anthropic/models/claude-3-7-sonnet' },
        ]);

        expect(ranked.map((m) => m.id)).toEqual([
            'gemini-4-hydrogen',
            'gemini-4-argon',
            'gemini-4-gluon',
            'gemini-3.8-flash',
            'gemini-3.8-flash-preview',
        ]);
        expect(ranked[0]).toEqual({
            id: 'gemini-4-hydrogen',
            displayName: 'Gemini 4 Hydrogen',
            tier: 'flash',
            version: '4.0',
            isDefault: true,
        });
        expect(ranked[3].displayName).toBe('Gemini 3.8 Flash');
        expect(ranked.filter((m) => m.isDefault).map((m) => m.id)).toEqual(['gemini-4-hydrogen']);
    });

    it('selects Gemini 3.8 Flash as default even when higher-version Pro Gemini 4 Argon is ranked first', () => {
        const ranked = filterAndRankCodegenModels([
            { name: 'models/gemini-4-argon', displayName: 'Gemini 4 Argon' },
            { name: 'models/gemini-3.8-flash', displayName: 'Gemini 3.8 Flash' },
            { name: 'models/gemini-3.8-pro', displayName: 'Gemini 3.8 Pro' },
        ]);

        expect(ranked.map((m) => m.id)).toEqual([
            'gemini-4-argon',
            'gemini-3.8-flash',
            'gemini-3.8-pro',
        ]);
        expect(resolveDefaultCodegenModel(ranked)).toBe('gemini-3.8-flash');
        expect(ranked.find((m) => m.id === 'gemini-4-argon')?.isDefault).toBe(false);
        expect(ranked.find((m) => m.id === 'gemini-3.8-flash')?.isDefault).toBe(true);
    });

    it('deduplicates repeated normalized model IDs while preserving richer metadata', () => {
        const ranked = filterAndRankCodegenModels([
            { name: 'publishers/google/models/gemini-3.8-flash' },
            {
                name: 'projects/p/locations/global/publishers/google/models/gemini-3.8-flash@default',
                displayName: 'Gemini 3.8 Flash Official',
                description: 'Fast multimodal and code model.',
            },
        ]);

        expect(ranked).toEqual([
            {
                id: 'gemini-3.8-flash',
                displayName: 'Gemini 3.8 Flash Official',
                description: 'Fast multimodal and code model.',
                tier: 'flash',
                version: '3.8',
                isDefault: true,
            },
        ]);
    });

    it('falls back to DEFAULT_GEMINI_MODEL when all raw entries are filtered out', () => {
        const ranked = filterAndRankCodegenModels([
            { name: 'models/gemini-2.5-flash' },
            { name: 'models/gemini-embedding-001' },
        ]);
        expect(ranked).toEqual([createDefaultCodegenModelOption()]);
    });
});

describe('modelDiscovery — resolveSelectedCodegenModel', () => {
    it('returns DEFAULT_GEMINI_MODEL when omitted, null, or blank', () => {
        expect(resolveSelectedCodegenModel()).toBe(DEFAULT_GEMINI_MODEL);
        expect(resolveSelectedCodegenModel(undefined)).toBe(DEFAULT_GEMINI_MODEL);
        expect(resolveSelectedCodegenModel(null)).toBe(DEFAULT_GEMINI_MODEL);
        expect(resolveSelectedCodegenModel('   ')).toBe(DEFAULT_GEMINI_MODEL);
    });

    it('normalizes and returns supported Gemini 3.x and 4.x model identifiers', () => {
        expect(resolveSelectedCodegenModel('gemini-4-hydrogen')).toBe('gemini-4-hydrogen');
        expect(resolveSelectedCodegenModel('gemini-3.8-flash')).toBe('gemini-3.8-flash');
        expect(resolveSelectedCodegenModel('publishers/google/models/gemini-4-carbon@001')).toBe(
            'gemini-4-carbon'
        );
    });

    it('throws a clear validation error for deprecated, embedding, or non-string models', () => {
        expect(() => resolveSelectedCodegenModel('gemini-2.5-flash')).toThrow(
            /Unsupported or deprecated Gemini code generation model: 'gemini-2\.5-flash'/
        );
        expect(() => resolveSelectedCodegenModel('gemini-1.5-pro')).toThrow(
            /Unsupported or deprecated Gemini code generation model/
        );
        expect(() => resolveSelectedCodegenModel('gemini-embedding-001')).toThrow(
            /Unsupported or deprecated Gemini code generation model/
        );
        expect(() => resolveSelectedCodegenModel(42)).toThrow(
            /Unsupported or deprecated Gemini code generation model/
        );
    });
});

describe('modelDiscovery — discoverCodegenModels', () => {
    const savedEnv: Record<string, string | undefined> = {};
    const envKeys = [
        'GOOGLE_CLOUD_PROJECT',
        'GCLOUD_PROJECT',
        'GEMINI_API_KEY',
        'GOOGLE_API_KEY',
        'GOOGLE_GENAI_USE_VERTEXAI',
    ];

    beforeEach(() => {
        for (const key of envKeys) {
            savedEnv[key] = process.env[key];
            delete process.env[key];
        }
    });

    afterEach(() => {
        for (const key of envKeys) {
            if (savedEnv[key] === undefined) {
                delete process.env[key];
            } else {
                process.env[key] = savedEnv[key];
            }
        }
    });

    it('queries Vertex AI at location global with queryBase true and consumes async-iterable Pager', async () => {
        const ensureCaBundleFn = vi.fn();
        const listMock = vi.fn(async () =>
            createAsyncPager([
                { name: 'publishers/google/models/gemini-3.8-flash', displayName: 'Gemini 3.8 Flash' },
                { name: 'publishers/google/models/gemini-4-hydrogen', displayName: 'Gemini 4 Hydrogen' },
                { name: 'publishers/google/models/gemini-2.5-flash', displayName: 'Gemini 2.5 Flash' },
            ])
        );
        const createClient = vi.fn(() => ({ models: { list: listMock } }));
        const controller = new AbortController();

        const result = await discoverCodegenModels({
            projectId: 'my-vertex-project',
            ensureCaBundleFn,
            createClient,
            abortSignal: controller.signal,
        });

        expect(ensureCaBundleFn).toHaveBeenCalledTimes(1);
        expect(createClient).toHaveBeenCalledWith({
            vertexai: true,
            project: 'my-vertex-project',
            location: 'global',
        });
        expect(listMock).toHaveBeenCalledWith({
            config: {
                queryBase: true,
                abortSignal: controller.signal,
            },
        });
        expect(result.source).toBe('vertexai');
        expect(result.defaultModel).toBe('gemini-4-hydrogen');
        expect(result.models.map((m) => m.id)).toEqual(['gemini-4-hydrogen', 'gemini-3.8-flash']);
    });

    it('ignores placeholder YOUR_PROJECT_ID and falls back to Gemini Developer API when Vertex AI fails', async () => {
        const clientConfigs: Array<Record<string, unknown>> = [];
        const createClient = vi.fn((clientOptions: Record<string, unknown>) => {
            clientConfigs.push(clientOptions);
            if (clientOptions.vertexai) {
                return {
                    models: {
                        list: vi.fn(async () => {
                            throw new Error('ADC token expired');
                        }),
                    },
                };
            }
            return {
                models: {
                    list: vi.fn(async () => [
                        { name: 'models/gemini-4-argon', displayName: 'Gemini 4 Argon' },
                        { name: 'models/gemini-3.8-flash', displayName: 'Gemini 3.8 Flash' },
                    ]),
                },
            };
        });

        const result = await discoverCodegenModels({
            projectId: 'YOUR_PROJECT_ID',
            getGcloudProjectFn: async () => 'resolved-gcloud-proj',
            apiKey: 'test-dev-api-key',
            ensureCaBundleFn: () => undefined,
            createClient,
        });

        expect(clientConfigs).toEqual([
            { vertexai: true, project: 'resolved-gcloud-proj', location: 'global' },
            { apiKey: 'test-dev-api-key' },
        ]);
        expect(result.source).toBe('gemini-api');
        expect(result.defaultModel).toBe('gemini-3.8-flash');
        expect(result.models.map((m) => m.id)).toEqual(['gemini-4-argon', 'gemini-3.8-flash']);
    });

    it('uses Gemini Developer API directly when no GCP project is configured', async () => {
        process.env.GEMINI_API_KEY = 'env-gemini-key';
        const createClient = vi.fn(() => ({
            models: {
                list: vi.fn(async () => [{ name: 'models/gemini-4-hydrogen' }]),
            },
        }));

        const result = await discoverCodegenModels({
            getGcloudProjectFn: async () => undefined,
            ensureCaBundleFn: () => undefined,
            createClient,
        });

        expect(createClient).toHaveBeenCalledTimes(1);
        expect(createClient).toHaveBeenCalledWith({ apiKey: 'env-gemini-key' });
        expect(result.source).toBe('gemini-api');
        expect(result.defaultModel).toBe('gemini-4-hydrogen');
    });

    it('returns offline DEFAULT_GEMINI_MODEL fallback without throwing when no credentials exist or all providers fail', async () => {
        const noCreds = await discoverCodegenModels({
            getGcloudProjectFn: async () => undefined,
            ensureCaBundleFn: () => undefined,
        });
        expect(noCreds).toEqual({
            models: [createDefaultCodegenModelOption()],
            defaultModel: DEFAULT_GEMINI_MODEL,
            source: 'fallback',
            warning: expect.stringContaining('No Google Cloud project or Gemini API key configured'),
        });

        const sdkFailure = await discoverCodegenModels({
            projectId: 'offline-proj',
            ensureCaBundleFn: () => undefined,
            createClient: () => ({
                models: {
                    list: async () => {
                        throw new Error('ENOTFOUND aiplatform.googleapis.com');
                    },
                },
            }),
        });
        expect(sdkFailure).toEqual({
            models: [createDefaultCodegenModelOption()],
            defaultModel: DEFAULT_GEMINI_MODEL,
            source: 'fallback',
            warning: 'ENOTFOUND aiplatform.googleapis.com',
        });
    });
});
