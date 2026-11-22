/**
 * Shared test cache helper for integration, E2E, and offline pytest suites.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Centralizes reusable cache paths under one root directory so integration runs share
 * a single Python virtual environment, pip download cache, Spark Ivy repository, and
 * Temurin JDK 21 installation instead of duplicating multi-hundred-megabyte artifacts.
 * Also cleans per-run scratch directories inside generated repositories while protecting
 * the shared cache root from deletion.
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

/** Environment variable that overrides the default shared cache root directory. */
export const SHARED_CACHE_ENV_VAR = 'SPARK_DESIGNER_CACHE_ROOT';

/** Environment variable that supplies the shared Spark Ivy cache path to child sessions. */
export const SPARK_IVY_CACHE_ENV_VAR = 'SPARK_IVY_CACHE_DIR';

/** Highest Java major version supported by local Apache Spark 4.0 test sessions. */
export const MAX_SPARK_SUPPORTED_JAVA_MAJOR = 21;

/** Lowest Java major version supported by Apache Spark 4.x runtimes. */
export const MIN_SPARK_SUPPORTED_JAVA_MAJOR = 17;

/** Per-run directory names removed from a generated project directory after each test run. */
export const PER_RUN_REPO_ARTIFACTS: readonly string[] = [
    '.venv',
    '.spark-ivy',
    '.pytest_cache',
    '.xdg-cache',
    '.cache',
];

/** Resolved directory layout under the shared test cache root. */
export interface SharedTestCachePaths {
    /** Root cache directory (`~/.cache/spark-pipeline-builder` by default). */
    rootDir: string;
    /** Shared Python virtual environment (`<rootDir>/pytest-venv`). */
    venvDir: string;
    /** Shared pip download and wheel cache (`<rootDir>/pip`). */
    pipCacheDir: string;
    /** Shared Apache Spark Ivy JAR cache (`<rootDir>/ivy2`). */
    ivyCacheDir: string;
    /** Shared Eclipse Temurin JDK 21 directory (`<rootDir>/jdk-21`). */
    jdkCacheDir: string;
}

/** Result of resolving a Spark-compatible `JAVA_HOME` path. */
export interface ResolvedJavaHome {
    /** Selected `JAVA_HOME` directory, if one is configured or auto-detected. */
    javaHome?: string;
    /** True when `javaHome` came from the shared `<cacheRoot>/jdk-21` cache. */
    fromSharedCache: boolean;
}

/**
 * Resolves the shared cache root directory from an explicit override, environment, or `~/.cache`.
 *
 * @param cacheRootOverride Optional directory override for unit tests or callers.
 * @param env Environment map inspected for `SPARK_DESIGNER_CACHE_ROOT`.
 * @returns Absolute path of the shared cache root directory.
 */
export function resolveSharedCacheRoot(
    cacheRootOverride?: string,
    env: Record<string, string | undefined> = process.env
): string {
    const explicit = cacheRootOverride?.trim();
    if (explicit) {
        return path.resolve(explicit);
    }
    const fromEnv = env[SHARED_CACHE_ENV_VAR]?.trim() || process.env[SHARED_CACHE_ENV_VAR]?.trim();
    if (fromEnv) {
        return path.resolve(fromEnv);
    }
    return path.join(os.homedir(), '.cache', 'spark-pipeline-builder');
}

/**
 * Computes all shared cache subdirectories under the resolved cache root.
 *
 * @param cacheRootOverride Optional directory override for unit tests.
 * @param env Environment map inspected when `cacheRootOverride` is omitted.
 * @returns Shared cache paths for virtualenv, pip, Ivy, and JDK 21.
 */
export function getSharedTestCachePaths(
    cacheRootOverride?: string,
    env: Record<string, string | undefined> = process.env
): SharedTestCachePaths {
    const rootDir = resolveSharedCacheRoot(cacheRootOverride, env);
    return {
        rootDir,
        venvDir: path.join(rootDir, 'pytest-venv'),
        pipCacheDir: path.join(rootDir, 'pip'),
        ivyCacheDir: path.join(rootDir, 'ivy2'),
        jdkCacheDir: path.join(rootDir, 'jdk-21'),
    };
}

/**
 * Safely reads an optional `fs` method without throwing when a unit test partially mocks `fs`.
 *
 * @param methodName Name of the synchronous `fs` helper.
 * @returns Bound function if exported on `fs`, or undefined when omitted by a partial mock.
 */
function getOptionalFsMethod<K extends keyof typeof fs>(methodName: K): (typeof fs)[K] | undefined {
    try {
        const fn = fs[methodName];
        return typeof fn === 'function' ? fn : undefined;
    } catch {
        return undefined;
    }
}

/**
 * Creates the shared cache root, pip cache, and Spark Ivy cache directories if absent.
 * Never deletes or modifies existing `jdk-21`, `pytest-venv`, `pip`, or `ivy2` entries.
 *
 * @param cacheRootOverride Optional directory override for unit tests.
 * @param env Environment map inspected when `cacheRootOverride` is omitted.
 * @returns Resolved shared cache paths.
 */
export function ensureSharedTestCacheDirs(
    cacheRootOverride?: string,
    env: Record<string, string | undefined> = process.env
): SharedTestCachePaths {
    const paths = getSharedTestCachePaths(cacheRootOverride, env);
    const mkdirSyncFn = getOptionalFsMethod('mkdirSync');
    if (mkdirSyncFn) {
        mkdirSyncFn(paths.rootDir, { recursive: true });
        mkdirSyncFn(paths.pipCacheDir, { recursive: true });
        mkdirSyncFn(paths.ivyCacheDir, { recursive: true });
    }
    return paths;
}

/**
 * Parses a JDK version string such as `21.0.13.1`, `27.0.1`, or `1.8.0_392` into a major integer.
 *
 * @param rawVersion Raw version string from a JDK `release` file or folder name.
 * @returns Parsed major version number, or undefined when unparseable.
 */
export function parseJavaMajorVersion(rawVersion: string): number | undefined {
    const trimmed = rawVersion.trim().replace(/^["']|["']$/g, '');
    if (!trimmed) {
        return undefined;
    }
    const normalized = trimmed.startsWith('1.') ? trimmed.slice(2) : trimmed;
    const head = normalized.split(/[.+\-_]/)[0] ?? '';
    const major = Number.parseInt(head, 10);
    return Number.isFinite(major) && major > 0 ? major : undefined;
}

/**
 * Reads the JDK major version from `<javaHome>/release` or directory naming conventions.
 *
 * @param javaHome Candidate `JAVA_HOME` directory.
 * @returns Detected major version, or undefined when no `java` executable or version is found.
 */
export function readJdkMajorVersion(javaHome: string): number | undefined {
    const existsSyncFn = getOptionalFsMethod('existsSync');
    if (!javaHome || !existsSyncFn) {
        return undefined;
    }
    const javaBinName = process.platform === 'win32' ? 'java.exe' : 'java';
    if (!existsSyncFn(path.join(javaHome, 'bin', javaBinName))) {
        return undefined;
    }
    const releaseFile = path.join(javaHome, 'release');
    const readFileSyncFn = getOptionalFsMethod('readFileSync');
    if (readFileSyncFn && existsSyncFn(releaseFile)) {
        try {
            const content = readFileSyncFn(releaseFile, 'utf8');
            const match = /^JAVA_VERSION="([^"]+)"/m.exec(content);
            if (match?.[1]) {
                const parsed = parseJavaMajorVersion(match[1]);
                if (parsed !== undefined) {
                    return parsed;
                }
            }
        } catch {
            // Fall through to directory name heuristic.
        }
    }
    const baseName = path.basename(javaHome);
    const dirMatch = /(?:jdk|java|temurin|openjdk)[-_]?(\d+)/i.exec(baseName);
    if (dirMatch?.[1]) {
        return parseJavaMajorVersion(dirMatch[1]);
    }
    return undefined;
}

/**
 * Searches `<cacheRoot>/jdk-21` for an installed Eclipse Temurin JDK 21 (or JDK 17) home directory.
 *
 * @param cacheRootOverride Optional cache root override for unit tests.
 * @param env Environment map inspected when `cacheRootOverride` is omitted.
 * @returns Absolute `JAVA_HOME` inside `<cacheRoot>/jdk-21`, or undefined if none exists.
 */
export function findCachedJdk21Home(
    cacheRootOverride?: string,
    env: Record<string, string | undefined> = process.env
): string | undefined {
    const existsSyncFn = getOptionalFsMethod('existsSync');
    const readdirSyncFn = getOptionalFsMethod('readdirSync');
    if (!existsSyncFn || !readdirSyncFn) {
        return undefined;
    }
    const { jdkCacheDir } = getSharedTestCachePaths(cacheRootOverride, env);
    if (!existsSyncFn(jdkCacheDir)) {
        return undefined;
    }

    const javaBinName = process.platform === 'win32' ? 'java.exe' : 'java';
    const candidates: string[] = [jdkCacheDir];
    try {
        const entries = readdirSyncFn(jdkCacheDir)
            .filter((name) => !name.startsWith('.'))
            .sort();
        for (const entry of entries) {
            const full = path.join(jdkCacheDir, entry);
            candidates.push(full);
            candidates.push(path.join(full, 'Contents', 'Home'));
        }
    } catch {
        return undefined;
    }

    const withJavaBin = candidates.filter((dir) => existsSyncFn(path.join(dir, 'bin', javaBinName)));
    for (const dir of withJavaBin) {
        if (readJdkMajorVersion(dir) === 21) {
            return dir;
        }
    }
    for (const dir of withJavaBin) {
        const major = readJdkMajorVersion(dir);
        if (major === undefined || (major >= MIN_SPARK_SUPPORTED_JAVA_MAJOR && major <= MAX_SPARK_SUPPORTED_JAVA_MAJOR)) {
            return dir;
        }
    }
    return undefined;
}

/**
 * Inspects the first `java` binary on `PATH` and returns its JDK major version when detectable.
 *
 * @param pathValue `PATH` string separated by `path.delimiter`.
 * @returns Host JDK major version, or undefined when `java` is absent or unversioned.
 */
export function detectHostJavaMajorFromPath(pathValue?: string): number | undefined {
    const existsSyncFn = getOptionalFsMethod('existsSync');
    if (!pathValue || !existsSyncFn) {
        return undefined;
    }
    const realpathSyncFn = getOptionalFsMethod('realpathSync');
    const javaBinName = process.platform === 'win32' ? 'java.exe' : 'java';
    for (const rawDir of pathValue.split(path.delimiter)) {
        const dir = rawDir.trim();
        if (!dir) {
            continue;
        }
        const candidateBin = path.join(dir, javaBinName);
        if (!existsSyncFn(candidateBin)) {
            continue;
        }
        let resolvedBin = candidateBin;
        if (realpathSyncFn) {
            try {
                resolvedBin = realpathSyncFn(candidateBin);
            } catch {
                resolvedBin = candidateBin;
            }
        }
        const inferredHome = path.dirname(path.dirname(resolvedBin));
        return readJdkMajorVersion(inferredHome);
    }
    return undefined;
}

/**
 * Resolves a Spark-compatible `JAVA_HOME`, falling back to `<cacheRoot>/jdk-21` when `JAVA_HOME`
 * is unset (and host Java is absent or newer than JDK 21) or when `JAVA_HOME` exceeds JDK 21.
 *
 * @param options Optional cache root override and environment map.
 * @param options.cacheRoot Explicit cache root directory override.
 * @param options.env Environment map holding `JAVA_HOME` and `PATH`.
 * @returns Selected `JAVA_HOME` and whether it came from `<cacheRoot>/jdk-21`.
 */
export function resolveSparkJavaHome(options: {
    cacheRoot?: string;
    env?: Record<string, string | undefined>;
} = {}): ResolvedJavaHome {
    const env = options.env ?? process.env;
    const configured = env.JAVA_HOME?.trim();
    if (configured) {
        const configuredMajor = readJdkMajorVersion(configured);
        if (
            configuredMajor !== undefined &&
            (configuredMajor > MAX_SPARK_SUPPORTED_JAVA_MAJOR ||
                configuredMajor < MIN_SPARK_SUPPORTED_JAVA_MAJOR)
        ) {
            const cached = findCachedJdk21Home(options.cacheRoot, env);
            if (cached) {
                return { javaHome: cached, fromSharedCache: true };
            }
        }
        return { javaHome: configured, fromSharedCache: false };
    }

    const hostMajor = detectHostJavaMajorFromPath(env.PATH);
    if (
        hostMajor === undefined ||
        hostMajor > MAX_SPARK_SUPPORTED_JAVA_MAJOR ||
        hostMajor < MIN_SPARK_SUPPORTED_JAVA_MAJOR
    ) {
        const cached = findCachedJdk21Home(options.cacheRoot, env);
        if (cached) {
            return { javaHome: cached, fromSharedCache: true };
        }
    }
    return { javaHome: undefined, fromSharedCache: false };
}

/**
 * Returns true when `candidatePath` equals `parentDir` or resolves inside `parentDir`.
 *
 * @param candidatePath Path to test.
 * @param parentDir Potential ancestor directory.
 * @returns True if `candidatePath` is `parentDir` or a subpath beneath it.
 */
export function isSameOrSubpath(candidatePath: string, parentDir: string): boolean {
    const rel = path.relative(path.resolve(parentDir), path.resolve(candidatePath));
    return rel === '' || (!rel.startsWith('..') && !path.isAbsolute(rel));
}

/**
 * Deletes per-run `.venv`, `.spark-ivy`, `.pytest_cache`, and XDG scratch directories inside
 * a generated project directory while refusing to touch the shared cache root.
 *
 * @param repoDir Generated repository directory.
 * @param options Optional cache root and extra per-run scratch directories inside `repoDir`.
 * @param options.cacheRoot Shared cache root protected from deletion.
 * @param options.extraScratchDirs Additional per-run scratch paths inside `repoDir`.
 * @param options.env Environment map used to resolve the shared cache root when omitted.
 * @returns Absolute paths of per-run directories that existed and were deleted.
 */
export function cleanupPerRunRepoArtifacts(
    repoDir: string,
    options: {
        cacheRoot?: string;
        extraScratchDirs?: readonly string[];
        env?: Record<string, string | undefined>;
    } = {}
): string[] {
    const existsSyncFn = getOptionalFsMethod('existsSync');
    const rmSyncFn = getOptionalFsMethod('rmSync');
    if (!existsSyncFn || !rmSyncFn) {
        return [];
    }
    const repo = path.resolve(repoDir);
    const cacheRoot = resolveSharedCacheRoot(options.cacheRoot, options.env);
    const seen = new Set<string>();
    const candidates: string[] = [];

    for (const relName of PER_RUN_REPO_ARTIFACTS) {
        const target = path.join(repo, relName);
        if (!seen.has(target)) {
            seen.add(target);
            candidates.push(target);
        }
    }

    for (const extra of options.extraScratchDirs ?? []) {
        const trimmed = extra.trim();
        if (!trimmed) {
            continue;
        }
        const resolvedExtra = path.resolve(trimmed);
        if (isSameOrSubpath(resolvedExtra, repo) && resolvedExtra !== repo && !seen.has(resolvedExtra)) {
            seen.add(resolvedExtra);
            candidates.push(resolvedExtra);
        }
    }

    const removed: string[] = [];
    for (const target of candidates) {
        if (target === repo) {
            continue;
        }
        if (isSameOrSubpath(target, cacheRoot) || isSameOrSubpath(cacheRoot, target)) {
            continue;
        }
        if (existsSyncFn(target)) {
            rmSyncFn(target, { recursive: true, force: true });
            removed.push(target);
        }
    }
    return removed;
}
