/**
 * Dynamic Gemini code-generation model discovery, filtering, and ranking.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Queries the live Gemini model catalog via the unified Google Gen AI SDK across
 * Vertex AI (global endpoint) or the Gemini Developer API. Filters out deprecated
 * Gemini 1.x and 2.x releases, embeddings, media models, live-only endpoints, and
 * non-Google publishers. Ranks Gemini 3.x+ text-generation models by version and
 * selects the highest-ranked Flash-class model as the default.
 */

import { GoogleGenAI } from '@google/genai';
import { DEFAULT_GEMINI_MODEL } from '../constants';

/**
 * Tier classification for dynamically discovered Gemini code-generation models.
 */
export type CodegenModelTier = 'flash' | 'pro' | 'flash-lite' | 'other';

/**
 * Metadata for a discovered Gemini code-generation model.
 */
export interface DiscoveredCodegenModel {
    /** Short canonical model identifier passed to Gemini. */
    id: string;
    /** Human-readable display label. */
    displayName: string;
    /** Optional model description from the provider catalog. */
    description?: string;
    /** Model tier classification used for Flash-default ranking. */
    tier: CodegenModelTier;
    /** Optional parsed major.minor version label. */
    version?: string;
    /** True when this model is the highest-ranked Flash-class default. */
    isDefault?: boolean;
}

/**
 * Configuration and dependency injection hooks for {@link discoverCodegenModels}.
 */
export interface DiscoverCodegenModelsOptions {
    /** Optional explicit Google Cloud project identifier for Vertex AI. */
    projectId?: string;
    /** Optional explicit Gemini Developer API key. */
    apiKey?: string;
    /** Optional override that resolves the default Google Cloud project. */
    getGcloudProjectFn?: () => Promise<string | undefined>;
    /** Optional SDK client factory used by unit tests. */
    createClient?: (options: Record<string, unknown>) => {
        models: {
            list: (params?: {
                config?: { queryBase?: boolean; abortSignal?: AbortSignal };
            }) => Promise<AsyncIterable<unknown> | unknown[]>;
        };
    };
    /** Optional system CA bundle loader override. */
    ensureCaBundleFn?: () => void;
    /** Optional cancellation signal forwarded to the list request. */
    abortSignal?: AbortSignal;
}

/**
 * Outcome returned by {@link discoverCodegenModels}.
 */
export interface DiscoverCodegenModelsResult {
    /** Ranked list of supported code-generation models (never empty). */
    models: DiscoveredCodegenModel[];
    /** Recommended default model identifier. */
    defaultModel: string;
    /** Provider source that supplied the catalog, or offline fallback. */
    source: 'vertexai' | 'gemini-api' | 'fallback';
    /** Optional diagnostic message when falling back to the default model. */
    warning?: string;
}

/** Raw model shape accepted by {@link isSupportedCodegenModel}. */
interface RawModelCandidate {
    name?: string;
    id?: string;
    displayName?: string;
    description?: string;
    supportedActions?: string[];
    tunedModelInfo?: unknown;
}

const VALID_PROJECT_ID_PATTERN = /^[a-zA-Z0-9.:-]+$/;
const NON_GOOGLE_PUBLISHER_PATTERN = /(?:^|\/)publishers\/(?!google(?:\/|$))/i;
const VERTEX_PROJECT_MODEL_PREFIX = /^projects\/[^/]+\/locations\/[^/]+\/publishers\/google\/models\//i;
const VERTEX_PUBLISHER_MODEL_PREFIX = /^publishers\/google\/models\//i;
const DEVELOPER_MODEL_PREFIX = /^models\//i;
const CANONICAL_GEMINI_ID_PATTERN = /^gemini-[a-z0-9][a-z0-9.-]{0,120}$/;
const GEMINI_VERSION_PATTERN = /^gemini-(\d+)(?:\.(\d+))?(?:\.(\d+))?(?=-|$)/i;
const FLASH_LITE_TIER_PATTERN = /(?:^|-)flash-lite(?:-|$)|(?:^|-)lite(?:-|$)|(?:^|-)gluon(?:-|$)/i;
const FLASH_TIER_PATTERN = /(?:^|-)flash(?:-|$)|(?:^|-)hydrogen(?:-|$)/i;
const PRO_TIER_PATTERN = /(?:^|-)(?:pro|argon|carbon|neon|cobalt|krypton)(?:-|$)/i;
const NON_CODEGEN_MODEL_TOKEN_PATTERN =
    /(?:embed|image|imagen|vision|tts|audio|speech|voice|music|lyria|video|veo|live|realtime|robotics|computer-use|cyber|medlm|learnlm|aqa|transcribe)/i;
const PRERELEASE_OR_SNAPSHOT_PATTERN =
    /(?:^|-)(?:preview|exp|experimental|rc|eap|alpha|beta|\d{3,})(?:-|$)/i;
const RAW_RESOURCE_DISPLAY_PREFIX = /^(?:projects\/|publishers\/|models\/)/i;

const TIER_RANK_ORDER: Record<CodegenModelTier, number> = {
    flash: 0,
    pro: 1,
    'flash-lite': 2,
    other: 3,
};

/**
 * Builds the offline default model entry derived from {@link DEFAULT_GEMINI_MODEL}.
 *
 * @returns Default Gemini model option for offline fallback.
 */
export function createDefaultCodegenModelOption(): DiscoveredCodegenModel {
    const versionTuple = parseGeminiModelVersion(DEFAULT_GEMINI_MODEL);
    return {
        id: DEFAULT_GEMINI_MODEL,
        displayName: titleCaseModelId(DEFAULT_GEMINI_MODEL),
        tier: classifyGeminiModelTier(DEFAULT_GEMINI_MODEL),
        ...(versionTuple ? { version: `${versionTuple[0]}.${versionTuple[1]}` } : {}),
        isDefault: true,
    };
}

/**
 * Normalizes a Vertex AI or Gemini Developer API resource name to a canonical Gemini model ID.
 *
 * @param rawName Raw model identifier or resource path.
 * @returns Canonical lowercase Gemini model ID, or an empty string if invalid.
 */
export function normalizeGeminiModelId(rawName: string | undefined | null): string {
    if (typeof rawName !== 'string') {
        return '';
    }
    const trimmed = rawName.trim();
    if (!trimmed || NON_GOOGLE_PUBLISHER_PATTERN.test(trimmed)) {
        return '';
    }
    const stripped = trimmed
        .replace(VERTEX_PROJECT_MODEL_PREFIX, '')
        .replace(VERTEX_PUBLISHER_MODEL_PREFIX, '')
        .replace(DEVELOPER_MODEL_PREFIX, '')
        .replace(/@.*$/, '')
        .toLowerCase();

    if (!stripped.startsWith('gemini-') || stripped.includes('..') || !CANONICAL_GEMINI_ID_PATTERN.test(stripped)) {
        return '';
    }
    return stripped;
}

/**
 * Extracts the semantic `[major, minor, patch]` tuple from a Gemini model ID.
 *
 * @param modelId Canonical or raw Gemini model identifier.
 * @returns Parsed `[major, minor, patch]` version numbers, or null when unversioned.
 */
export function parseGeminiModelVersion(modelId: string): [number, number, number] | null {
    const normalized = normalizeGeminiModelId(modelId);
    if (!normalized) {
        return null;
    }
    const match = GEMINI_VERSION_PATTERN.exec(normalized);
    if (!match) {
        return null;
    }
    const major = Number.parseInt(match[1], 10);
    const minor = match[2] !== undefined ? Number.parseInt(match[2], 10) : 0;
    const patch = match[3] !== undefined ? Number.parseInt(match[3], 10) : 0;
    return [major, minor, patch];
}

/**
 * Classifies a Gemini model identifier into its performance and latency tier.
 *
 * @param modelId Canonical or raw Gemini model identifier.
 * @param _displayName Optional human-readable display name.
 * @returns Classified model tier.
 */
export function classifyGeminiModelTier(modelId: string, _displayName?: string): CodegenModelTier {
    const normalized = normalizeGeminiModelId(modelId) || modelId.trim().toLowerCase();
    if (FLASH_LITE_TIER_PATTERN.test(normalized)) {
        return 'flash-lite';
    }
    if (FLASH_TIER_PATTERN.test(normalized)) {
        return 'flash';
    }
    if (PRO_TIER_PATTERN.test(normalized)) {
        return 'pro';
    }
    return 'other';
}

/**
 * Checks whether a SDK `tunedModelInfo` payload indicates an actual tuned model.
 *
 * The `@google/genai` list converter attaches an empty `{}` (or base timestamps without
 * `baseModel`) on publisher base models. Only treat `tunedModelInfo` as tuned when a
 * primitive truthy flag or a non-timestamp metadata property such as `baseModel` is present.
 *
 * @param tunedModelInfo Raw `tunedModelInfo` field from a model record.
 * @returns True when the record represents a tuned model.
 */
function hasTunedModelMetadata(tunedModelInfo: unknown): boolean {
    if (!tunedModelInfo) {
        return false;
    }
    if (typeof tunedModelInfo !== 'object') {
        return true;
    }
    const record = tunedModelInfo as Record<string, unknown>;
    return Object.entries(record).some(
        ([key, value]) => key !== 'createTime' && key !== 'updateTime' && Boolean(value)
    );
}

/**
 * Determines whether a raw provider model supports Gemini 3.x+ text code generation.
 *
 * @param rawModel Raw model metadata returned by Vertex AI or the Gemini Developer API.
 * @returns True when the model is eligible for PySpark code generation and review.
 */
export function isSupportedCodegenModel(rawModel: RawModelCandidate): boolean {
    if (!rawModel || typeof rawModel !== 'object') {
        return false;
    }
    const id = normalizeGeminiModelId(rawModel.name ?? rawModel.id);
    if (!id) {
        return false;
    }
    const version = parseGeminiModelVersion(id);
    if (!version || version[0] < 3) {
        return false;
    }
    if (hasTunedModelMetadata(rawModel.tunedModelInfo)) {
        return false;
    }
    if (Array.isArray(rawModel.supportedActions) && rawModel.supportedActions.length > 0) {
        if (!rawModel.supportedActions.includes('generateContent')) {
            return false;
        }
    }
    if (NON_CODEGEN_MODEL_TOKEN_PATTERN.test(id)) {
        return false;
    }
    return true;
}

/**
 * Selects the recommended default model ID from a ranked model list, preferring Flash-class models.
 *
 * @param models Ranked list of discovered models.
 * @returns Default model ID, falling back to {@link DEFAULT_GEMINI_MODEL}.
 */
export function resolveDefaultCodegenModel(
    models: readonly Pick<DiscoveredCodegenModel, 'id' | 'tier'>[]
): string {
    return models.find((m) => m.tier === 'flash')?.id ?? models[0]?.id ?? DEFAULT_GEMINI_MODEL;
}

/**
 * Formats a human-readable display label from a canonical Gemini model ID.
 *
 * @param id Canonical Gemini model identifier.
 * @returns Title-cased display label such as `Gemini 4 Hydrogen`.
 */
function titleCaseModelId(id: string): string {
    return id
        .split('-')
        .filter((part) => part.length > 0)
        .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
        .join(' ');
}

/**
 * Chooses a clean display label from provider metadata or synthesizes one from the model ID.
 *
 * @param id Canonical Gemini model identifier.
 * @param rawDisplayName Optional display name from the provider catalog.
 * @returns Clean human-readable display name and whether it came from explicit metadata.
 */
function resolveDisplayName(
    id: string,
    rawDisplayName?: string
): { displayName: string; hasCustomDisplayName: boolean } {
    const trimmed = typeof rawDisplayName === 'string' ? rawDisplayName.trim() : '';
    if (trimmed && !RAW_RESOURCE_DISPLAY_PREFIX.test(trimmed) && trimmed.toLowerCase() !== id) {
        return { displayName: trimmed, hasCustomDisplayName: true };
    }
    return { displayName: titleCaseModelId(id), hasCustomDisplayName: false };
}

/**
 * Filters, deduplicates, ranks, and marks the default entry for raw catalog models.
 *
 * @param rawModels Iterable of raw model records from the SDK pager or an array.
 * @returns Ranked list of supported code-generation models (falls back to default when empty).
 */
export function filterAndRankCodegenModels(rawModels: Iterable<unknown>): DiscoveredCodegenModel[] {
    interface RankedCandidate {
        model: DiscoveredCodegenModel;
        versionTuple: [number, number, number];
        isPreRelease: boolean;
        hasCustomDisplayName: boolean;
    }

    const byId = new Map<string, RankedCandidate>();

    if (rawModels && typeof (rawModels as Iterable<unknown>)[Symbol.iterator] === 'function') {
        for (const item of rawModels) {
            if (!item || typeof item !== 'object') {
                continue;
            }
            const raw = item as RawModelCandidate;
            if (!isSupportedCodegenModel(raw)) {
                continue;
            }
            const id = normalizeGeminiModelId(raw.name ?? raw.id);
            const versionTuple = parseGeminiModelVersion(id);
            if (!id || !versionTuple) {
                continue;
            }

            const { displayName, hasCustomDisplayName } = resolveDisplayName(id, raw.displayName);
            const description =
                typeof raw.description === 'string' && raw.description.trim().length > 0
                    ? raw.description.trim()
                    : undefined;
            const tier = classifyGeminiModelTier(id, displayName);
            const versionLabel = `${versionTuple[0]}.${versionTuple[1]}`;
            const isPreRelease = PRERELEASE_OR_SNAPSHOT_PATTERN.test(id);

            const existing = byId.get(id);
            if (!existing) {
                byId.set(id, {
                    model: {
                        id,
                        displayName,
                        ...(description ? { description } : {}),
                        tier,
                        version: versionLabel,
                    },
                    versionTuple,
                    isPreRelease,
                    hasCustomDisplayName,
                });
            } else {
                if (!existing.hasCustomDisplayName && hasCustomDisplayName) {
                    existing.model.displayName = displayName;
                    existing.hasCustomDisplayName = true;
                }
                if (
                    description &&
                    (!existing.model.description || description.length > existing.model.description.length)
                ) {
                    existing.model.description = description;
                }
            }
        }
    }

    if (byId.size === 0) {
        return [createDefaultCodegenModelOption()];
    }

    const ranked = Array.from(byId.values()).sort((a, b) => {
        if (b.versionTuple[0] !== a.versionTuple[0]) {
            return b.versionTuple[0] - a.versionTuple[0];
        }
        if (b.versionTuple[1] !== a.versionTuple[1]) {
            return b.versionTuple[1] - a.versionTuple[1];
        }
        if (b.versionTuple[2] !== a.versionTuple[2]) {
            return b.versionTuple[2] - a.versionTuple[2];
        }
        if (a.isPreRelease !== b.isPreRelease) {
            return a.isPreRelease ? 1 : -1;
        }
        const tierDiff = TIER_RANK_ORDER[a.model.tier] - TIER_RANK_ORDER[b.model.tier];
        if (tierDiff !== 0) {
            return tierDiff;
        }
        return a.model.id.localeCompare(b.model.id);
    });

    const models = ranked.map((entry) => entry.model);
    const defaultId = resolveDefaultCodegenModel(models);
    return models.map((entry) => ({
        ...entry,
        isDefault: entry.id === defaultId,
    }));
}

/**
 * Validates and normalizes an explicit model selection for code generation or review.
 *
 * @param requestedModel Optional model identifier supplied by the caller or webview.
 * @returns Validated canonical Gemini 3.x+ model ID.
 */
export function resolveSelectedCodegenModel(requestedModel?: unknown): string {
    if (requestedModel === undefined || requestedModel === null) {
        return DEFAULT_GEMINI_MODEL;
    }
    if (typeof requestedModel !== 'string') {
        throw new Error(
            `Unsupported or deprecated Gemini code generation model: '${String(requestedModel)}'. Select a Gemini 3.x+ text generation model such as ${DEFAULT_GEMINI_MODEL}.`
        );
    }
    if (requestedModel.trim() === '') {
        return DEFAULT_GEMINI_MODEL;
    }
    const normalized = normalizeGeminiModelId(requestedModel);
    if (!normalized || !isSupportedCodegenModel({ name: normalized })) {
        throw new Error(
            `Unsupported or deprecated Gemini code generation model: '${requestedModel}'. Select a Gemini 3.x+ text generation model such as ${DEFAULT_GEMINI_MODEL}.`
        );
    }
    return normalized;
}

/**
 * Validates a candidate Google Cloud project identifier.
 *
 * @param raw Raw project identifier string.
 * @returns Sanitized project ID, or undefined when missing or invalid.
 */
function sanitizeProjectId(raw: string | undefined): string | undefined {
    const trimmed = typeof raw === 'string' ? raw.trim() : '';
    if (!trimmed || trimmed === 'YOUR_PROJECT_ID' || !VALID_PROJECT_ID_PATTERN.test(trimmed)) {
        return undefined;
    }
    return trimmed;
}

/**
 * Invokes the configured or default CA bundle initializer before outbound HTTPS requests.
 *
 * @param ensureCaBundleFn Optional injected CA bundle initializer.
 */
async function runCaBundleSetup(ensureCaBundleFn?: () => void): Promise<void> {
    if (ensureCaBundleFn) {
        ensureCaBundleFn();
        return;
    }
    try {
        const { ensureSystemCaBundle } = await import('../codeGeneration');
        ensureSystemCaBundle();
    } catch {
        // Plain Node test runners without the vscode host mock may skip host CA setup.
    }
}

/**
 * Resolves a validated Google Cloud project ID from options, gcloud, or environment variables.
 *
 * @param options Optional discovery options.
 * @returns Validated Google Cloud project ID, or undefined when unavailable.
 */
async function resolveDiscoveryProjectId(
    options?: DiscoverCodegenModelsOptions
): Promise<string | undefined> {
    const explicitProject = sanitizeProjectId(options?.projectId);
    if (explicitProject) {
        return explicitProject;
    }

    try {
        if (options?.getGcloudProjectFn) {
            const fromInjected = sanitizeProjectId(await options.getGcloudProjectFn());
            if (fromInjected) {
                return fromInjected;
            }
        } else {
            const { getGcloudProjectCached } = await import('../cli/gcloudClient');
            const fromGcloud = sanitizeProjectId(await getGcloudProjectCached());
            if (fromGcloud) {
                return fromGcloud;
            }
        }
    } catch {
        // Fall through to environment variables when gcloud lookup fails.
    }

    return (
        sanitizeProjectId(process.env.GOOGLE_CLOUD_PROJECT) ??
        sanitizeProjectId(process.env.GCLOUD_PROJECT)
    );
}

/**
 * Discovers, filters, and ranks available Gemini 3.x+ code-generation models at runtime.
 *
 * Attempts Vertex AI (`location: 'global'`) when a valid Google Cloud project is available,
 * then falls back to the Gemini Developer API when an API key is configured, and finally
 * returns the offline {@link DEFAULT_GEMINI_MODEL} fallback without throwing.
 *
 * @param options Optional project, API key, cancellation signal, and test overrides.
 * @returns Ranked model options, recommended Flash-class default ID, and catalog source.
 */
export async function discoverCodegenModels(
    options?: DiscoverCodegenModelsOptions
): Promise<DiscoverCodegenModelsResult> {
    await runCaBundleSetup(options?.ensureCaBundleFn);

    const projectId = await resolveDiscoveryProjectId(options);
    const apiKey =
        options?.apiKey?.trim() ||
        process.env.GEMINI_API_KEY?.trim() ||
        process.env.GOOGLE_API_KEY?.trim() ||
        '';

    const candidates: Array<{
        source: 'vertexai' | 'gemini-api';
        clientOptions: Record<string, unknown>;
    }> = [];

    if (projectId && process.env.GOOGLE_GENAI_USE_VERTEXAI?.trim().toLowerCase() !== 'false') {
        candidates.push({
            source: 'vertexai',
            clientOptions: {
                vertexai: true,
                project: projectId,
                location: 'global',
            },
        });
    }

    if (apiKey) {
        candidates.push({
            source: 'gemini-api',
            clientOptions: { apiKey },
        });
    }

    if (candidates.length === 0) {
        return {
            models: [createDefaultCodegenModelOption()],
            defaultModel: DEFAULT_GEMINI_MODEL,
            source: 'fallback',
            warning: 'No Google Cloud project or Gemini API key configured. Using default offline model.',
        };
    }

    let lastErrorMessage = '';
    for (const candidate of candidates) {
        try {
            const ai = options?.createClient
                ? options.createClient(candidate.clientOptions)
                : new GoogleGenAI(candidate.clientOptions as ConstructorParameters<typeof GoogleGenAI>[0]);
            const listConfig: { queryBase: boolean; abortSignal?: AbortSignal } = {
                queryBase: true,
                ...(options?.abortSignal ? { abortSignal: options.abortSignal } : {}),
            };
            const pager = await ai.models.list({ config: listConfig });
            const rawItems: unknown[] = [];
            if (Array.isArray(pager)) {
                rawItems.push(...pager);
            } else if (
                pager &&
                typeof (pager as unknown as AsyncIterable<unknown>)[Symbol.asyncIterator] === 'function'
            ) {
                for await (const item of pager as unknown as AsyncIterable<unknown>) {
                    rawItems.push(item);
                }
            } else if (
                pager &&
                typeof (pager as unknown as Iterable<unknown>)[Symbol.iterator] === 'function'
            ) {
                for (const item of pager as unknown as Iterable<unknown>) {
                    rawItems.push(item);
                }
            }

            const models = filterAndRankCodegenModels(rawItems);
            return {
                models,
                defaultModel: resolveDefaultCodegenModel(models),
                source: candidate.source,
            };
        } catch (err: unknown) {
            lastErrorMessage = err instanceof Error ? err.message : String(err);
        }
    }

    return {
        models: [createDefaultCodegenModelOption()],
        defaultModel: DEFAULT_GEMINI_MODEL,
        source: 'fallback',
        warning: lastErrorMessage || 'Failed to list Gemini models. Using default offline model.',
    };
}
