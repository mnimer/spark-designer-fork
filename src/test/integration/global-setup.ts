/**
 * Vitest Global Setup for Integration Tests.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Clears the integration test report cache at the start of every Vitest integration
 * run, initializes the shared cache root (`pytest-venv`, `pip`, `ivy2`, and Temurin
 * `jdk-21` auto-detection), and populates `GOOGLE_CLOUD_PROJECT` when unset.
 */

import * as path from 'path';
import { clearReportCache } from './test-reporter';
import { getGcloudProject } from './codegen-runner';
import {
    SHARED_CACHE_ENV_VAR,
    SPARK_IVY_CACHE_ENV_VAR,
    SharedTestCachePaths,
    ensureSharedTestCacheDirs,
    resolveSparkJavaHome,
} from './sharedTestCache';
import { buildChildEnv, resolvePython } from './pytest-runner';

/** Options for {@link bootstrapIntegrationTestCache}. */
export interface BootstrapSharedCacheOptions {
    /** Optional shared cache root override for isolated tests. */
    cacheRoot?: string;
    /** Target environment map mutated with shared cache paths. Defaults to `process.env`. */
    env?: Record<string, string | undefined>;
    /** Base Python interpreter used when creating `<cacheRoot>/pytest-venv`. */
    python?: string;
    /** When false, skips creating `<cacheRoot>/pytest-venv`. Defaults to true. */
    createVenv?: boolean;
}

/**
 * Ensures the shared cache directories exist, exports `PIP_CACHE_DIR` and `SPARK_IVY_CACHE_DIR`,
 * auto-resolves `<cacheRoot>/jdk-21` when host Java is unsupported, and initializes `<cacheRoot>/pytest-venv`.
 *
 * @param options Optional cache root, environment map, and Python overrides.
 * @returns Resolved shared cache directory paths.
 */
export async function bootstrapIntegrationTestCache(
    options: BootstrapSharedCacheOptions = {}
): Promise<SharedTestCachePaths> {
    const targetEnv = options.env ?? process.env;
    const cachePaths = ensureSharedTestCacheDirs(options.cacheRoot, targetEnv);

    if (!targetEnv[SHARED_CACHE_ENV_VAR]) {
        targetEnv[SHARED_CACHE_ENV_VAR] = cachePaths.rootDir;
    }
    targetEnv.PIP_CACHE_DIR = cachePaths.pipCacheDir;
    targetEnv[SPARK_IVY_CACHE_ENV_VAR] = cachePaths.ivyCacheDir;

    const resolvedJava = resolveSparkJavaHome({ cacheRoot: cachePaths.rootDir, env: targetEnv });
    if (resolvedJava.javaHome && resolvedJava.fromSharedCache) {
        targetEnv.JAVA_HOME = resolvedJava.javaHome;
        const javaBin = path.join(resolvedJava.javaHome, 'bin');
        targetEnv.PATH = targetEnv.PATH ? `${javaBin}${path.delimiter}${targetEnv.PATH}` : javaBin;
    }

    const shouldCreateVenv = options.createVenv ?? !targetEnv.SPARK_PYTEST_PYTHON;
    if (shouldCreateVenv) {
        const basePython = options.python ?? targetEnv.SPARK_PYTEST_PYTHON ?? 'python3';
        const probeEnv = buildChildEnv(basePython, targetEnv, undefined, {
            cacheRoot: cachePaths.rootDir,
        }).env;
        await resolvePython(
            {
                python: basePython,
                venvDir: cachePaths.venvDir,
                createVenv: true,
                cacheRoot: cachePaths.rootDir,
            },
            probeEnv
        );
    }

    return cachePaths;
}

/**
 * Global setup hook for Vitest integration tests.
 * Clears cached test reports, prepares shared test caches, and resolves `GOOGLE_CLOUD_PROJECT` if unset.
 *
 * @returns Promise that resolves when setup is complete.
 */
export default async function setup(): Promise<void> {
    clearReportCache();
    await bootstrapIntegrationTestCache();
    if (!process.env.GOOGLE_CLOUD_PROJECT) {
        const project = await getGcloudProject();
        if (project) {
            process.env.GOOGLE_CLOUD_PROJECT = project;
        }
    }
}
