/**
 * Read-Only Check Runner
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Runs fast deterministic checks on generated PySpark and Spark Declarative Pipelines (SDP)
 * files without making any model calls. The optional reviewer (`CodeReviewer.ts`) passes
 * these check results to the review model as evidence.
 *
 * Imperative mode checks:
 * 1. `required_files`: verifies `requirements.txt`, `src/pipeline.py`, `tests/test_pipeline.py`,
 *    and every runner template file exist.
 * 2. `leftover_tokens`: verifies no `{{...}}` placeholder or redaction sentinel remains.
 * 3. `python_syntax`: runs `python3 -m py_compile` on every generated Python file.
 * 4. `pipeline_contract`: verifies `src/pipeline.py` defines `read_sources`, `transform`,
 *    and `write_sinks`.
 * 5. `test_imports`: verifies `tests/test_pipeline.py` imports `src.pipeline`.
 * 6. `pytest_collect`: runs `python3 -m pytest --collect-only -q` when `pytest` is installed.
 *
 * SDP mode checks (when `request.paradigm === 'sdp'` or SDP spec files exist):
 * 1. `required_files`: requires `requirements.txt`, single `spark-pipeline.yml`,
 *    at least one `transformations/*.py` module, `src/transforms.py`, `tests/test_transforms.py`,
 *    and `request.runnerFiles`.
 * 2. `sdp_spec_contract`: validates `spark-pipeline.yml` allowed keys, `storage` URI scheme,
 *    plain `catalog` identifier, string `configuration` values, static-setting ban, and
 *    `libraries` glob syntax.
 * 3. `pipeline_contract`: scans `transformations/*.py` for banned `SparkSession.builder`,
 *    session mutations, writers, eager actions, `.schema`/`.columns` property inspection,
 *    and Databricks-only APIs.
 * 4. `test_imports`: verifies `tests/test_transforms.py` imports `src.transforms` or `transforms`.
 * 5. `sdp_dry_run`: runs `spark-pipelines dry-run --spec spark-pipeline.yml` when
 *    `spark-pipelines` and a compatible local Spark 4.2 environment exist.
 *
 * Architectural notes:
 * - Checks never modify generated code.
 * - Missing host tools (`python3`, `pytest`, or `spark-pipelines`) mark checks `skipped`.
 * - The command executor is injectable so unit tests run deterministically offline.
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFile } from 'child_process';
import * as YAML from 'yaml';
import { REDACTION_PLACEHOLDER } from '../credentialRedaction';
import {
    LANGUAGE_BUILD_FILES,
    MODEL_FACING_PLACEHOLDERS,
    READ_ONLY_CHECK_LIMITS,
    SkillLanguage,
    SkillParadigm,
} from './codegenConfig';

/** One generated file matching the structured output schema. */
export interface GeneratedFile {
    /** Project-relative path with forward slashes. */
    filename: string;
    /** Complete file text. */
    content: string;
    /** Model-requested executable bit (informational). */
    executable?: boolean;
}

/** Identifiers of the read-only checks. */
export const CHECK_IDS = {
    requiredFiles: 'required_files',
    leftoverTokens: 'leftover_tokens',
    pythonSyntax: 'python_syntax',
    sdpSpecContract: 'sdp_spec_contract',
    pipelineContract: 'pipeline_contract',
    testImports: 'test_imports',
    pytestCollect: 'pytest_collect',
    sdpDryRun: 'sdp_dry_run',
} as const;

/** A check identifier. */
export type CheckId = (typeof CHECK_IDS)[keyof typeof CHECK_IDS];

/** Outcome status of one check. */
export type CheckStatus = 'passed' | 'failed' | 'skipped';

/** Outcome of one check. */
export interface CheckResult {
    /** Which check ran. */
    check: CheckId;
    /** Pass, fail, or skip status. */
    status: CheckStatus;
    /** Evidence (truncated) or skip reason. */
    output: string;
}

/** Result of running one external command. */
export interface CommandOutcome {
    /** Process exit code (-1 when it could not start). */
    exitCode: number;
    /** Captured stdout. */
    stdout: string;
    /** Captured stderr. */
    stderr: string;
    /** True when the executable could not be started (ENOENT). */
    notFound?: boolean;
}

/** Environment variable map passed to spawned check commands. */
export type CheckProcessEnv = Record<string, string | undefined>;

/** Execution options passed to {@link CommandExecutor}. */
export interface CommandExecutorOptions {
    /** Working directory of the spawned process. */
    cwd: string;
    /** Wall-clock timeout in milliseconds. */
    timeoutMs: number;
    /** Optional environment variables passed to the child process. */
    env?: CheckProcessEnv;
}

/** Executes an external command. Injectable for unit tests. */
export type CommandExecutor = (
    command: string,
    args: string[],
    options: CommandExecutorOptions
) => Promise<CommandOutcome>;

/** Default executor backed by `child_process.execFile`. */
export const defaultCommandExecutor: CommandExecutor = (command, args, options) =>
    new Promise((resolve) => {
        execFile(
            command,
            args,
            { cwd: options.cwd, timeout: options.timeoutMs, env: options.env ?? process.env },
            (error, stdout, stderr) => {
                const err = error as (Error & { code?: unknown }) | null;
                if (err?.code === 'ENOENT') {
                    resolve({ exitCode: -1, stdout: '', stderr: String(err.message), notFound: true });
                    return;
                }
                const exitCode = err ? (typeof err.code === 'number' ? err.code : 1) : 0;
                resolve({ exitCode, stdout: String(stdout ?? ''), stderr: String(stderr || err?.message || '') });
            }
        );
    });

/** Input of one check run. */
export interface CheckRequest {
    /** Generated files. */
    files: GeneratedFile[];
    /** Pipeline language. */
    language: SkillLanguage;
    /** Pipeline base name (file stem, for example `orders`). */
    baseName: string;
    /** Runner files that must exist (template file names of the selected runner skills). */
    runnerFiles: readonly string[];
    /** Optional pipeline paradigm (`imperative`, `sdp`, or `notebook`). Auto-detects when omitted. */
    paradigm?: SkillParadigm;
    /** Optional shared cache root directory override for pip and Ivy caches. */
    cacheRoot?: string;
}

/** Per-run artifact directories cleaned from check workspaces after execution. */
export const CHECK_RUN_ARTIFACT_DIRS = [
    '.venv',
    '.spark-ivy',
    '.pytest_cache',
    '.xdg-cache',
    '.cache',
] as const;

/** One static violation found in an SDP spec or transformation module. */
export interface SdpStaticViolation {
    /** Error code matching Apache Spark 4.2 SDP diagnostics. */
    code: string;
    /** Project-relative file path. */
    file: string;
    /** Optional 1-indexed line number. */
    line?: number;
    /** Optional trimmed source line evidence. */
    evidence?: string;
    /** Human-readable defect description. */
    message: string;
}

/** Marker appended to truncated output. */
const TRUNCATION_MARKER = '\n...[truncated]';

/** Pattern of a leftover placeholder. */
const PLACEHOLDER_PATTERN = /\{\{[A-Za-z_][\w]*\}\}/g;

/** Known model-facing placeholders that must never remain in SDP YAML specs. */
const KNOWN_CODEGEN_PLACEHOLDERS: ReadonlySet<string> = new Set([
    '{{spark_version}}',
    ...Object.values(MODEL_FACING_PLACEHOLDERS),
]);

/** Spark redaction placeholder checked alongside `REDACTION_PLACEHOLDER`. */
const SPARK_REDACTION_PLACEHOLDER = '__REDACTED_BY_SPARK_DESIGNER__';

/** Required 3-phase functions in `src/pipeline.py` for imperative mode. */
export const REQUIRED_PIPELINE_FUNCTIONS = ['read_sources', 'transform', 'write_sinks'] as const;

/** Allowed top-level keys in `spark-pipeline.yml` (Apache Spark 4.2 `PipelineSpec`). */
export const ALLOWED_SDP_SPEC_KEYS: ReadonlySet<string> = new Set([
    'name',
    'storage',
    'catalog',
    'database',
    'schema',
    'configuration',
    'libraries',
]);

/** Plain Spark identifier pattern for SDP `catalog` aliases. */
const PLAIN_SPARK_IDENTIFIER_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** Jinja template variable expression allowed in Managed Data Pipelines SDP specs. */
const JINJA_TEMPLATE_VALUE_REGEX = /^\{\{\s*[A-Za-z_][A-Za-z0-9_.]*\s*\}\}$/;

/** URI with an explicit scheme such as `gs://bucket/path`. */
const EXPLICIT_URI_SCHEME_REGEX = /^[A-Za-z][A-Za-z0-9+.-]*:\/\/[^\s]+$/;

/** Normalizes a generated relative path to forward slashes without leading `./`. */
function normalizeRelPath(filename: string): string {
    return filename.replace(/\\/g, '/').replace(/^\.\//, '');
}

/**
 * Returns true if a Spark configuration key is a static session or cluster setting
 * forbidden inside `spark-pipeline.yml` `configuration`.
 *
 * @param key Spark configuration property name.
 * @returns True when setting `key` in `spark-pipeline.yml` triggers `CANNOT_MODIFY_STATIC_CONFIG`.
 */
export function isStaticSdpSparkConfigKey(key: string): boolean {
    const trimmed = key.trim();
    return (
        trimmed === 'spark.sql.extensions' ||
        trimmed === 'spark.jars' ||
        trimmed === 'spark.jars.packages' ||
        trimmed.startsWith('spark.jars.') ||
        trimmed === 'spark.sql.catalogImplementation' ||
        trimmed.startsWith('spark.driver.') ||
        trimmed.startsWith('spark.executor.') ||
        trimmed.startsWith('spark.dynamicAllocation.') ||
        trimmed === 'spark.files' ||
        trimmed === 'spark.archives' ||
        trimmed === 'spark.master' ||
        trimmed === 'spark.submit.deployMode'
    );
}

/**
 * Determines whether a set of generated files represents a Spark Declarative Pipeline project.
 *
 * @param files Generated files.
 * @param paradigm Optional explicit paradigm override.
 * @returns True when SDP validation rules apply.
 */
export function isSdpGeneratedProject(
    files: readonly Pick<GeneratedFile, 'filename'>[],
    paradigm?: SkillParadigm
): boolean {
    if (paradigm === 'sdp') {
        return true;
    }
    if (paradigm === 'imperative' || paradigm === 'notebook') {
        return false;
    }
    for (const file of files) {
        const norm = normalizeRelPath(file.filename);
        if (
            norm === 'spark-pipeline.yml' ||
            norm === 'spark-pipeline.yaml' ||
            norm === 'sdp_stage.pipeline.yaml' ||
            norm === 'run_sdp_local.sh' ||
            norm === 'run_sdp_dataproc.sh' ||
            /^transformations\/.+\.(?:py|sql)$/.test(norm)
        ) {
            return true;
        }
    }
    return false;
}

/**
 * Determines whether a set of generated files represents a Jupyter Notebook (`.ipynb`) project.
 *
 * @param files Generated files.
 * @param paradigm Optional explicit paradigm override.
 * @returns True when Notebook validation rules apply.
 */
export function isNotebookGeneratedProject(
    files: readonly Pick<GeneratedFile, 'filename'>[],
    paradigm?: SkillParadigm
): boolean {
    if (paradigm === 'notebook') {
        return true;
    }
    if (paradigm === 'imperative' || paradigm === 'sdp') {
        return false;
    }
    return files.some((file) => normalizeRelPath(file.filename).toLowerCase().endsWith('.ipynb'));
}

/**
 * Validates a generated Jupyter Notebook (`.ipynb`) JSON payload against nbformat v4 rules.
 *
 * @param notebookFilename Notebook filename for diagnostics.
 * @param content Raw notebook JSON text.
 * @returns List of validation error strings (empty when valid).
 */
export function validateNotebookIpynb(notebookFilename: string, content: string): string[] {
    const errors: string[] = [];
    let parsed: unknown;
    try {
        parsed = JSON.parse(content);
    } catch (err) {
        return [`${notebookFilename}: invalid JSON (${(err as Error).message}).`];
    }

    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        return [`${notebookFilename}: root must be a JSON object.`];
    }

    const doc = parsed as Record<string, unknown>;
    if (doc.nbformat !== 4) {
        errors.push(`${notebookFilename}: nbformat must be 4 (found ${String(doc.nbformat)}).`);
    }
    if (!Array.isArray(doc.cells) || doc.cells.length === 0) {
        errors.push(`${notebookFilename}: cells must be a non-empty array.`);
        return errors;
    }

    let codeCellCount = 0;
    const allowedCellTypes = new Set(['markdown', 'code', 'raw']);
    for (let idx = 0; idx < doc.cells.length; idx++) {
        const cell = doc.cells[idx];
        if (!cell || typeof cell !== 'object' || Array.isArray(cell)) {
            errors.push(`${notebookFilename}: cell[${idx}] must be an object.`);
            continue;
        }
        const cellObj = cell as Record<string, unknown>;
        if (typeof cellObj.cell_type !== 'string' || !allowedCellTypes.has(cellObj.cell_type)) {
            errors.push(`${notebookFilename}: cell[${idx}] has invalid cell_type '${String(cellObj.cell_type)}'.`);
        }
        const validSource =
            typeof cellObj.source === 'string' ||
            (Array.isArray(cellObj.source) && cellObj.source.every((line) => typeof line === 'string'));
        if (!validSource) {
            errors.push(`${notebookFilename}: cell[${idx}] source must be a string or list of strings.`);
        }
        if (cellObj.cell_type === 'code') {
            codeCellCount += 1;
            if (!Array.isArray(cellObj.outputs)) {
                errors.push(`${notebookFilename}: code cell[${idx}] must define an outputs array.`);
            }
        }
    }

    if (codeCellCount === 0) {
        errors.push(`${notebookFilename}: notebook must contain at least one code cell.`);
    }

    return errors;
}

/**
 * Validates `spark-pipeline.yml` (or `spark-pipeline.yaml`) content against Apache Spark 4.2 SDP rules.
 *
 * @param specFilename Spec filename for diagnostics.
 * @param content Raw YAML text.
 * @returns List of SDP spec violations (empty when valid).
 */
export function validateSdpSpecYaml(specFilename: string, content: string): SdpStaticViolation[] {
    const violations: SdpStaticViolation[] = [];
    let parsed: unknown;
    try {
        parsed = YAML.parse(content);
    } catch (err) {
        violations.push({
            code: 'PIPELINE_SPEC_INVALID_YAML',
            file: specFilename,
            message: `PIPELINE_SPEC_INVALID_YAML: ${specFilename} is not valid YAML (${(err as Error).message}).`,
        });
        return violations;
    }

    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        violations.push({
            code: 'PIPELINE_SPEC_INVALID_YAML',
            file: specFilename,
            message: `PIPELINE_SPEC_INVALID_YAML: ${specFilename} root must be a YAML mapping.`,
        });
        return violations;
    }

    const doc = parsed as Record<string, unknown>;

    // 1. Top-level key allowlist.
    for (const key of Object.keys(doc)) {
        if (!ALLOWED_SDP_SPEC_KEYS.has(key)) {
            violations.push({
                code: 'PIPELINE_SPEC_UNEXPECTED_FIELD',
                file: specFilename,
                evidence: key,
                message: `PIPELINE_SPEC_UNEXPECTED_FIELD: Unexpected top-level key '${key}' in ${specFilename}. Allowed keys: name, storage, catalog, database, schema, configuration, libraries.`,
            });
        }
    }

    // 2. Required storage field with explicit URI scheme.
    if (doc.storage === undefined || doc.storage === null || String(doc.storage).trim() === '') {
        violations.push({
            code: 'PIPELINE_SPEC_MISSING_REQUIRED_FIELD',
            file: specFilename,
            message: `PIPELINE_SPEC_MISSING_REQUIRED_FIELD: Required field 'storage' is missing in ${specFilename}.`,
        });
    } else if (typeof doc.storage !== 'string') {
        violations.push({
            code: 'PIPELINE_STORAGE_ROOT_INVALID',
            file: specFilename,
            message: `PIPELINE_STORAGE_ROOT_INVALID: 'storage' in ${specFilename} must be a URI string.`,
        });
    } else {
        const storageVal = doc.storage.trim();
        if (!JINJA_TEMPLATE_VALUE_REGEX.test(storageVal) && !EXPLICIT_URI_SCHEME_REGEX.test(storageVal)) {
            violations.push({
                code: 'PIPELINE_STORAGE_ROOT_INVALID',
                file: specFilename,
                evidence: storageVal,
                message: `PIPELINE_STORAGE_ROOT_INVALID: 'storage' value '${storageVal}' in ${specFilename} must include an explicit URI scheme (for example gs://bucket/path).`,
            });
        }
    }

    // 3. Optional catalog field must be a plain identifier unless templated.
    if (doc.catalog !== undefined && doc.catalog !== null) {
        if (typeof doc.catalog !== 'string' || doc.catalog.trim() === '') {
            violations.push({
                code: 'INVALID_IDENTIFIER',
                file: specFilename,
                message: `INVALID_IDENTIFIER: 'catalog' in ${specFilename} must be a non-empty plain identifier string.`,
            });
        } else {
            const catalogVal = doc.catalog.trim();
            if (
                !JINJA_TEMPLATE_VALUE_REGEX.test(catalogVal) &&
                !PLAIN_SPARK_IDENTIFIER_REGEX.test(catalogVal)
            ) {
                violations.push({
                    code: 'INVALID_IDENTIFIER',
                    file: specFilename,
                    evidence: catalogVal,
                    message: `INVALID_IDENTIFIER: 'catalog' value '${catalogVal}' in ${specFilename} must be a plain Spark identifier (letters, digits, and underscores only).`,
                });
            }
        }
    }

    // 4. Optional configuration mapping must contain string-only values and zero static Spark keys.
    if (doc.configuration !== undefined && doc.configuration !== null) {
        if (typeof doc.configuration !== 'object' || Array.isArray(doc.configuration)) {
            violations.push({
                code: 'PIPELINE_SPEC_INVALID_CONFIGURATION',
                file: specFilename,
                message: `PIPELINE_SPEC_INVALID_CONFIGURATION: 'configuration' in ${specFilename} must be a string mapping.`,
            });
        } else {
            const configObj = doc.configuration as Record<string, unknown>;
            for (const [cfgKey, cfgVal] of Object.entries(configObj)) {
                if (typeof cfgVal !== 'string') {
                    violations.push({
                        code: 'PIPELINE_SPEC_DICT_VALUE_NOT_STRING',
                        file: specFilename,
                        evidence: `${cfgKey}: ${String(cfgVal)}`,
                        message: `PIPELINE_SPEC_DICT_VALUE_NOT_STRING: configuration['${cfgKey}'] in ${specFilename} must be a string (found ${typeof cfgVal}).`,
                    });
                }
                if (isStaticSdpSparkConfigKey(cfgKey)) {
                    violations.push({
                        code: 'CANNOT_MODIFY_STATIC_CONFIG',
                        file: specFilename,
                        evidence: cfgKey,
                        message: `CANNOT_MODIFY_STATIC_CONFIG: Static Spark setting '${cfgKey}' is forbidden in ${specFilename} configuration. Move it to runner execution_config.properties or --conf.`,
                    });
                }
            }
        }
    }

    // 5. Libraries globs must use folder/** or explicit .py/.sql paths (never transformations/*.py).
    if (!Array.isArray(doc.libraries) || doc.libraries.length === 0) {
        violations.push({
            code: 'PIPELINE_SPEC_MISSING_LIBRARIES',
            file: specFilename,
            message: `PIPELINE_SPEC_MISSING_LIBRARIES: 'libraries' in ${specFilename} must declare at least one glob include entry.`,
        });
    } else {
        for (const item of doc.libraries) {
            const globObj =
                item && typeof item === 'object' && !Array.isArray(item)
                    ? (item as Record<string, unknown>).glob
                    : undefined;
            const includeVal =
                globObj && typeof globObj === 'object' && !Array.isArray(globObj)
                    ? (globObj as Record<string, unknown>).include
                    : undefined;
            if (typeof includeVal !== 'string' || includeVal.trim() === '') {
                violations.push({
                    code: 'PIPELINE_SPEC_INVALID_GLOB_PATTERN',
                    file: specFilename,
                    message: `PIPELINE_SPEC_INVALID_GLOB_PATTERN: Each entry in 'libraries' must specify glob.include in ${specFilename}.`,
                });
                continue;
            }
            const trimmedInc = includeVal.trim();
            const hasWildcard = /[*?]/.test(trimmedInc);
            if (hasWildcard) {
                const validFolderRecursiveGlob =
                    trimmedInc.endsWith('/**') && !/[*?]/.test(trimmedInc.slice(0, -3));
                if (!validFolderRecursiveGlob) {
                    violations.push({
                        code: 'PIPELINE_SPEC_INVALID_GLOB_PATTERN',
                        file: specFilename,
                        evidence: trimmedInc,
                        message: `PIPELINE_SPEC_INVALID_GLOB_PATTERN: Glob '${trimmedInc}' in ${specFilename} is invalid. Use folder/** (such as transformations/**) or an explicit file path.`,
                    });
                }
            } else if (!trimmedInc.endsWith('.py') && !trimmedInc.endsWith('.sql')) {
                violations.push({
                    code: 'PIPELINE_UNSUPPORTED_DEFINITIONS_FILE_EXTENSION',
                    file: specFilename,
                    evidence: trimmedInc,
                    message: `PIPELINE_UNSUPPORTED_DEFINITIONS_FILE_EXTENSION: Library path '${trimmedInc}' in ${specFilename} must end with .py or .sql.`,
                });
            }
        }
    }

    return violations;
}

/**
 * Strips Python comments and triple-quoted docstrings while preserving exact line count.
 *
 * @param source Raw Python source text.
 * @returns Array of sanitized code lines indexed from 0.
 */
export function stripPythonCommentsAndDocstrings(source: string): string[] {
    const rawLines = source.split('\n');
    const cleaned: string[] = [];
    let inTriple: '"""' | "'''" | null = null;

    for (const rawLine of rawLines) {
        let out = '';
        let i = 0;
        let singleQuote: '"' | "'" | null = null;

        while (i < rawLine.length) {
            if (inTriple) {
                const closeIdx = rawLine.indexOf(inTriple, i);
                if (closeIdx === -1) {
                    i = rawLine.length;
                } else {
                    i = closeIdx + 3;
                    inTriple = null;
                }
                continue;
            }

            if (singleQuote) {
                const ch = rawLine[i];
                out += ch;
                if (ch === '\\' && i + 1 < rawLine.length) {
                    out += rawLine[i + 1];
                    i += 2;
                    continue;
                }
                if (ch === singleQuote) {
                    singleQuote = null;
                }
                i += 1;
                continue;
            }

            const tripleCandidate = rawLine.slice(i, i + 3);
            if (tripleCandidate === '"""' || tripleCandidate === "'''") {
                inTriple = tripleCandidate;
                i += 3;
                continue;
            }

            const ch = rawLine[i];
            if (ch === '#') {
                break;
            }
            if (ch === '"' || ch === "'") {
                singleQuote = ch;
            }
            out += ch;
            i += 1;
        }

        cleaned.push(out);
    }

    return cleaned;
}

/** Per-line SDP transformation module rule. */
interface SdpLineRule {
    code: string;
    regex: RegExp;
    message: string;
    transformationsOnly?: boolean;
}

const SDP_PYTHON_LINE_RULES: readonly SdpLineRule[] = [
    {
        code: 'SDP_BANNED_SESSION_BUILDER',
        regex: /\bSparkSession\s*\.\s*builder\b/,
        message: 'SparkSession.builder is forbidden in SDP modules. Use the injected spark session or SparkSession.active().',
        transformationsOnly: true,
    },
    {
        code: 'SESSION_MUTATION_IN_DECLARATIVE_PIPELINE',
        regex: /\bspark\s*\.\s*conf\s*\.\s*set\s*\(/,
        message: 'SESSION_MUTATION_IN_DECLARATIVE_PIPELINE: spark.conf.set() is forbidden in SDP transformations. Use spark-pipeline.yml configuration or decorator spark_conf.',
        transformationsOnly: true,
    },
    {
        code: 'SESSION_MUTATION_IN_DECLARATIVE_PIPELINE',
        regex: /\bspark\s*\.\s*catalog\s*\.\s*(?:setCurrentCatalog|setCurrentDatabase)\s*\(/,
        message: 'SESSION_MUTATION_IN_DECLARATIVE_PIPELINE: spark.catalog.setCurrentCatalog/setCurrentDatabase is forbidden in SDP transformations.',
        transformationsOnly: true,
    },
    {
        code: 'SESSION_MUTATION_IN_DECLARATIVE_PIPELINE',
        regex: /\.\s*create(?:Global)?(?:OrReplace)?TempView\s*\(/,
        message: 'SESSION_MUTATION_IN_DECLARATIVE_PIPELINE: createOrReplaceTempView is forbidden in SDP transformations. Declare @dp.temporary_view instead.',
        transformationsOnly: true,
    },
    {
        code: 'SESSION_MUTATION_IN_DECLARATIVE_PIPELINE',
        regex: /\bspark\s*\.\s*udf\s*\.\s*register\s*\(/,
        message: 'SESSION_MUTATION_IN_DECLARATIVE_PIPELINE: spark.udf.register() is forbidden in SDP transformations. Apply F.udf or @pandas_udf directly in DataFrame expressions.',
        transformationsOnly: true,
    },
    {
        code: 'ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION',
        regex: /\.\s*(?:write|writeStream|writeTo)\b/,
        message: 'ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION: Imperative writers (.write, .writeStream, .writeTo) are forbidden inside SDP transformations.',
        transformationsOnly: true,
    },
    {
        code: 'ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION',
        regex: /\.\s*(?:save|saveAsTable|toTable|start|collect|show|toPandas)\s*\(/,
        message: 'ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION: Eager actions and table writers (.save, .saveAsTable, .toTable, .start, .collect, .show, .toPandas) are forbidden inside SDP transformations.',
        transformationsOnly: true,
    },
    {
        code: 'ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION',
        regex: /(?<!\b(?:F|functions))\.\s*count\s*\(\s*\)/,
        message: 'ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION: Eager DataFrame .count() is forbidden inside SDP transformations.',
        transformationsOnly: true,
    },
    {
        code: 'ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION',
        regex: /\.\s*schema\b(?!\s*\()/,
        message: 'ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION: Reading DataFrame .schema is forbidden inside SDP dataset functions.',
        transformationsOnly: true,
    },
    {
        code: 'ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION',
        regex: /\.\s*columns\b(?!\s*=)/,
        message: 'ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION: Reading DataFrame .columns is forbidden inside SDP dataset functions.',
        transformationsOnly: true,
    },
    {
        code: 'SDP_BANNED_DATABRICKS_API',
        regex: /^\s*import\s+dlt\b|\bfrom\s+dlt\s+import\b|\bfrom\s+pyspark(?:\.pipelines)?\s+import\s+[^#\n]*\bdlt\b/,
        message: 'SDP_BANNED_DATABRICKS_API: Databricks dlt module does not exist in Apache Spark 4.2. Import from pyspark import pipelines as dp.',
    },
    {
        code: 'SDP_BANNED_DATABRICKS_API',
        regex: /@\s*(?:dp|dlt)\s*\.\s*view\b/,
        message: 'SDP_BANNED_DATABRICKS_API: @dp.view does not exist in Apache Spark 4.2. Use @dp.temporary_view.',
    },
    {
        code: 'SDP_BANNED_DATABRICKS_API',
        regex: /@\s*(?:dp|dlt)\s*\.\s*expect(?:_[A-Za-z0-9_]+)?\b/,
        message: 'SDP_BANNED_DATABRICKS_API: @dp.expect* decorators do not exist in OSS Apache Spark 4.2. Implement filter-based clean and quarantine datasets.',
    },
    {
        code: 'SDP_BANNED_DATABRICKS_API',
        regex: /\b(?:dp|dlt)\s*\.\s*apply_changes\b/,
        message: 'SDP_BANNED_DATABRICKS_API: dp.apply_changes does not exist in Apache Spark 4.2. Use dp.create_auto_cdc_flow.',
    },
    {
        code: 'AUTOCDC_SCD2_NOT_SUPPORTED',
        regex: /\bstored_as_scd_type\s*=\s*["']?2["']?(?!\d)/,
        message: 'AUTOCDC_SCD2_NOT_SUPPORTED: OSS Apache Spark 4.2 Auto CDC supports SCD Type 1 (stored_as_scd_type=1) only.',
    },
    {
        code: 'SDP_BANNED_DATABRICKS_API',
        regex: /\.\s*format\s*\(\s*["']cloudFiles["']\s*\)/i,
        message: 'SDP_BANNED_DATABRICKS_API: Databricks Auto Loader format("cloudFiles") does not exist in Apache Spark 4.2. Use standard spark.readStream.format(...).',
    },
];

/**
 * Scans generated SDP Python files (`transformations/*.py` and non-test `src/*.py`) for
 * banned session mutations, eager DataFrame actions, writers, and Databricks-only APIs.
 *
 * @param files Generated files.
 * @returns List of SDP Python violations (empty when clean).
 */
export function scanSdpPythonViolations(files: readonly GeneratedFile[]): SdpStaticViolation[] {
    const violations: SdpStaticViolation[] = [];

    for (const file of files) {
        const norm = normalizeRelPath(file.filename);
        if (!norm.endsWith('.py')) {
            continue;
        }
        const base = path.posix.basename(norm);
        const isTest =
            norm.startsWith('tests/') ||
            norm.includes('/tests/') ||
            base.startsWith('test_') ||
            base.endsWith('_test.py') ||
            base === 'conftest.py';
        if (isTest) {
            continue;
        }

        const isTransformationFile = norm.startsWith('transformations/');
        const rawLines = file.content.split('\n');
        const cleanedLines = stripPythonCommentsAndDocstrings(file.content);

        for (const rule of SDP_PYTHON_LINE_RULES) {
            if (rule.transformationsOnly && !isTransformationFile) {
                continue;
            }
            for (let idx = 0; idx < cleanedLines.length; idx++) {
                if (rule.regex.test(cleanedLines[idx])) {
                    violations.push({
                        code: rule.code,
                        file: file.filename,
                        line: idx + 1,
                        evidence: rawLines[idx].trim().slice(0, 160),
                        message: `${file.filename}:${idx + 1}: ${rule.message}`,
                    });
                    break;
                }
            }
        }

        // Multi-line check for `@dp.append_flow(..., once=...)`.
        const cleanedJoined = cleanedLines.join('\n');
        const appendFlowOnceRegex = /@\s*(?:dp|dlt)\s*\.\s*append_flow\s*\(([^)]*)\)/g;
        let match: RegExpExecArray | null;
        while ((match = appendFlowOnceRegex.exec(cleanedJoined)) !== null) {
            if (/\bonce\s*=/.test(match[1])) {
                const prefix = cleanedJoined.slice(0, match.index);
                const lineNum = prefix.split('\n').length;
                violations.push({
                    code: 'SDP_BANNED_DATABRICKS_API',
                    file: file.filename,
                    line: lineNum,
                    evidence: rawLines[lineNum - 1]?.trim().slice(0, 160),
                    message: `${file.filename}:${lineNum}: SDP_BANNED_DATABRICKS_API: @dp.append_flow does not accept 'once=' in Apache Spark 4.2.`,
                });
                break;
            }
        }

        // V2.8: Validate sdp_extensions.expectations (@dq.expect*) decorator placement in transformations/*.py.
        if (isTransformationFile) {
            for (let idx = 0; idx < cleanedLines.length; idx++) {
                const trimmed = cleanedLines[idx].trim();
                if (!trimmed.startsWith('@')) {
                    continue;
                }
                const isDqExpect = /^@\s*dq\s*\.\s*expect(?:_[A-Za-z0-9_]+)?\b/.test(trimmed);
                const isDpAppendFlow = /^@\s*dp\s*\.\s*append_flow\b/.test(trimmed);
                if (!isDqExpect && !isDpAppendFlow) {
                    continue;
                }
                // Inspect consecutive decorators in the same stack before the next `def`.
                for (let nextIdx = idx + 1; nextIdx < cleanedLines.length; nextIdx++) {
                    const nextTrimmed = cleanedLines[nextIdx].trim();
                    if (nextTrimmed === '') {
                        continue;
                    }
                    if (/^def\s+/.test(nextTrimmed)) {
                        break;
                    }
                    if (!nextTrimmed.startsWith('@')) {
                        // Continuation line of a multi-line decorator argument list.
                        continue;
                    }
                    if (isDqExpect && /^@\s*dp\s*\.\s*(?:table|materialized_view|temporary_view)\b/.test(nextTrimmed)) {
                        violations.push({
                            code: 'SDP_EXPECTATIONS_DECORATOR_ORDER',
                            file: file.filename,
                            line: idx + 1,
                            evidence: rawLines[idx].trim().slice(0, 160),
                            message: `${file.filename}:${idx + 1}: SDP_EXPECTATIONS_DECORATOR_ORDER: Place @dq.expect* decorators BELOW @dp.table, @dp.materialized_view, or @dp.temporary_view.`,
                        });
                        break;
                    }
                    if (
                        (isDqExpect && /^@\s*dp\s*\.\s*append_flow\b/.test(nextTrimmed)) ||
                        (isDpAppendFlow && /^@\s*dq\s*\.\s*expect(?:_[A-Za-z0-9_]+)?\b/.test(nextTrimmed))
                    ) {
                        violations.push({
                            code: 'SDP_EXPECTATIONS_ON_APPEND_FLOW',
                            file: file.filename,
                            line: idx + 1,
                            evidence: rawLines[idx].trim().slice(0, 160),
                            message: `${file.filename}:${idx + 1}: SDP_EXPECTATIONS_ON_APPEND_FLOW: @dq.expect* decorators cannot be applied to @dp.append_flow.`,
                        });
                        break;
                    }
                }
            }
        }
    }

    return violations;
}

/**
 * Strips SQL line (`--`) and block comments while preserving line numbers.
 *
 * @param source Raw SQL file text.
 * @returns Sanitized SQL string with comments blanked.
 */
function stripSqlComments(source: string): string {
    const withoutBlocks = source.replace(/\/\*[\s\S]*?\*\//g, (block) =>
        block.replace(/[^\n]/g, ' ')
    );
    return withoutBlocks.replace(/--[^\n]*/g, '');
}

/**
 * Scans generated SDP SQL transformation files (`transformations/*.sql`) against Apache Spark 4.2
 * `SqlGraphRegistrationContext` rules (`V2.7`).
 *
 * @param files Generated files.
 * @returns List of SDP SQL violations (empty when valid).
 */
export function scanSdpSqlViolations(files: readonly GeneratedFile[]): SdpStaticViolation[] {
    const violations: SdpStaticViolation[] = [];

    for (const file of files) {
        const norm = normalizeRelPath(file.filename);
        if (!/^transformations\/.+\.sql$/i.test(norm)) {
            continue;
        }
        const rawLines = file.content.split('\n');
        const cleaned = stripSqlComments(file.content);
        const cleanedLines = cleaned.split('\n');

        const lineRules: ReadonlyArray<{ code: string; regex: RegExp; message: string }> = [
            {
                code: 'PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR',
                regex: /\bCREATE\s+(?:SCHEMA|NAMESPACE|DATABASE)\b/i,
                message: 'PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR: CREATE SCHEMA/NAMESPACE is forbidden in SDP .sql files. Create namespaces in runner setup scripts instead.',
            },
            {
                code: 'SDP_SQL_EXPECT_UNSUPPORTED',
                regex: /\bCONSTRAINT\s+[A-Za-z_][A-Za-z0-9_]*\s+EXPECT\b/i,
                message: 'SDP_SQL_EXPECT_UNSUPPORTED: Databricks SQL CONSTRAINT ... EXPECT does not exist in OSS Apache Spark 4.2. Use WHERE filters or Python sdp_extensions.expectations.',
            },
            {
                code: 'SDP_SQL_CLUSTER_BY_UNSUPPORTED',
                regex: /\bCLUSTER\s+BY\b/i,
                message: 'SDP_SQL_CLUSTER_BY_UNSUPPORTED: CLUSTER BY fails SQL registration in Apache Spark 4.2 SDP. Use Python cluster_by= instead.',
            },
            {
                code: 'SDP_SQL_PIVOT_UNSUPPORTED',
                regex: /\bPIVOT\s*\(/i,
                message: 'SDP_SQL_PIVOT_UNSUPPORTED: PIVOT is not supported in SDP SQL files. Use Python .pivot(col, [...]) instead.',
            },
        ];

        for (const rule of lineRules) {
            for (let idx = 0; idx < cleanedLines.length; idx++) {
                if (rule.regex.test(cleanedLines[idx])) {
                    violations.push({
                        code: rule.code,
                        file: file.filename,
                        line: idx + 1,
                        evidence: rawLines[idx]?.trim().slice(0, 160),
                        message: `${file.filename}:${idx + 1}: ${rule.message}`,
                    });
                    break;
                }
            }
        }

        // Check CREATE FLOW statements for mandatory `INSERT INTO <table> BY NAME` and banned OVERWRITE.
        const flowRegex = /\bCREATE\s+FLOW\b[\s\S]*?(?:;|$)/gi;
        let flowMatch: RegExpExecArray | null;
        while ((flowMatch = flowRegex.exec(cleaned)) !== null) {
            const stmt = flowMatch[0];
            const lineNum = cleaned.slice(0, flowMatch.index).split('\n').length;
            if (/\bINSERT\s+OVERWRITE\b/i.test(stmt)) {
                violations.push({
                    code: 'SDP_SQL_FLOW_OVERWRITE_UNSUPPORTED',
                    file: file.filename,
                    line: lineNum,
                    evidence: rawLines[lineNum - 1]?.trim().slice(0, 160),
                    message: `${file.filename}:${lineNum}: SDP_SQL_FLOW_OVERWRITE_UNSUPPORTED: CREATE FLOW does not support INSERT OVERWRITE in Apache Spark 4.2 SDP.`,
                });
            } else if (/\bINSERT\s+INTO\b/i.test(stmt) && !/\bBY\s+NAME\b/i.test(stmt)) {
                violations.push({
                    code: 'SDP_SQL_FLOW_REQUIRES_BY_NAME',
                    file: file.filename,
                    line: lineNum,
                    evidence: rawLines[lineNum - 1]?.trim().slice(0, 160),
                    message: `${file.filename}:${lineNum}: SDP_SQL_FLOW_REQUIRES_BY_NAME: CREATE FLOW ... INSERT INTO must specify BY NAME in Apache Spark 4.2 SDP.`,
                });
            }
        }
    }

    return violations;
}

/**
 * Truncates check output to the configured limit.
 *
 * @param text Raw output.
 * @returns The text, cut at `maxOutputChars` with a marker.
 */
export function truncateCheckOutput(text: string): string {
    const limit = READ_ONLY_CHECK_LIMITS.maxOutputChars;
    return text.length <= limit ? text : text.slice(0, limit) + TRUNCATION_MARKER;
}

/**
 * Returns the canonical PySpark main and test source paths (`src/pipeline.py`, `tests/test_pipeline.py`).
 *
 * @param _language Pipeline language.
 * @param _baseName Pipeline base name.
 * @returns `[main, test]` relative paths.
 */
export function expectedSourceFiles(_language: SkillLanguage, _baseName: string): [string, string] {
    return ['src/pipeline.py', 'tests/test_pipeline.py'];
}

/**
 * Resolves the generated main pipeline Python file (`src/pipeline.py` or `src/<baseName>.py`).
 *
 * @param files Generated files.
 * @param baseName Pipeline base name.
 * @returns Matching main pipeline file, if present.
 */
function findMainPipelineFile(files: GeneratedFile[], baseName: string): GeneratedFile | undefined {
    return (
        files.find((f) => normalizeRelPath(f.filename) === 'src/pipeline.py') ??
        files.find((f) => normalizeRelPath(f.filename) === `src/${baseName}.py`) ??
        files.find((f) => normalizeRelPath(f.filename) === 'pipeline.py')
    );
}

/**
 * Resolves the generated unit test Python file (`tests/test_pipeline.py` or `src/<baseName>_test.py`).
 *
 * @param files Generated files.
 * @param baseName Pipeline base name.
 * @returns Matching test file, if present.
 */
function findTestPipelineFile(files: GeneratedFile[], baseName: string): GeneratedFile | undefined {
    return (
        files.find((f) => normalizeRelPath(f.filename) === 'tests/test_pipeline.py') ??
        files.find((f) => normalizeRelPath(f.filename) === `tests/test_${baseName}.py`) ??
        files.find((f) => normalizeRelPath(f.filename) === `src/${baseName}_test.py`) ??
        files.find((f) => normalizeRelPath(f.filename) === 'src/pipeline_test.py')
    );
}

/**
 * Resolves the SDP test module (`tests/test_transforms.py`).
 *
 * @param files Generated files.
 * @returns Matching SDP test module, if present.
 */
function findSdpTestTransformsFile(files: GeneratedFile[]): GeneratedFile | undefined {
    return (
        files.find((f) => normalizeRelPath(f.filename) === 'tests/test_transforms.py') ??
        files.find((f) => normalizeRelPath(f.filename) === 'test_transforms.py')
    );
}

/**
 * Materializes generated files into a temporary workspace directory safely.
 *
 * @param workDir Temporary workspace root.
 * @param candidateFiles Generated files to write.
 * @returns Relative paths of files written inside `workDir`.
 */
async function materializeWorkspaceFiles(workDir: string, candidateFiles: GeneratedFile[]): Promise<string[]> {
    const written: string[] = [];
    for (const file of candidateFiles) {
        const target = path.resolve(workDir, file.filename);
        if (!target.startsWith(workDir + path.sep)) {
            continue;
        }
        await fs.promises.mkdir(path.dirname(target), { recursive: true });
        await fs.promises.writeFile(target, file.content, 'utf-8');
        written.push(path.relative(workDir, target).split(path.sep).join('/'));
    }
    return written;
}

/**
 * Returns true if `spark-pipelines dry-run` output indicates a missing local runtime,
 * incompatible host JDK, Spark version below 4.2, or unreachable external catalog rather than
 * a generated project error.
 *
 * @param combined Combined stdout and stderr text.
 * @returns True when the check should be marked `skipped`.
 */
export function isSdpDryRunEnvironmentSkip(combined: string): boolean {
    return (
        /JAVA_HOME is not set/i.test(combined) ||
        /\bjava:\s*(?:command\s+)?not found/i.test(combined) ||
        /UnsupportedClassVersionError/i.test(combined) ||
        /getSubject is not supported/i.test(combined) ||
        /No module named ['"]?pyspark\.pipelines['"]?/i.test(combined) ||
        /No module named ['"]?zstandard['"]?/i.test(combined) ||
        /PACKAGE_NOT_INSTALLED[\s\S]*zstandard/i.test(combined) ||
        /iceberg-spark-runtime-4\.2/i.test(combined) ||
        /ClassNotFoundException:\s*org\.apache\.iceberg/i.test(combined) ||
        /UnknownHostException/i.test(combined) ||
        /biglake\.googleapis\.com/i.test(combined)
    );
}

/**
 * Resolves the shared cache root used for reusable `pip` and Spark `ivy2` caches during checks.
 *
 * @param cacheRootOverride Optional cache root override for tests.
 * @param env Environment map inspected when `cacheRootOverride` is omitted.
 * @returns Absolute path of the shared cache root directory.
 */
export function resolveCheckCacheRoot(
    cacheRootOverride?: string,
    env: CheckProcessEnv = process.env
): string {
    const explicit = cacheRootOverride?.trim();
    if (explicit) {
        return path.resolve(explicit);
    }
    const fromEnv = env.SPARK_DESIGNER_CACHE_ROOT?.trim() || process.env.SPARK_DESIGNER_CACHE_ROOT?.trim();
    if (fromEnv) {
        return path.resolve(fromEnv);
    }
    return path.join(os.homedir(), '.cache', 'spark-pipeline-builder');
}

/**
 * Builds the isolated child environment for `python_syntax`, `pytest_collect`, and `sdp_dry_run` checks.
 * Routes `XDG_CACHE_HOME` to a per-run scratch folder inside `workDir` and points `PIP_CACHE_DIR`
 * and Spark Ivy at the shared cache root.
 *
 * @param workDir Temporary check workspace directory.
 * @param options Optional shared cache root and parent environment overrides.
 * @param options.cacheRoot Shared cache root directory override.
 * @param options.baseEnv Parent environment copied into the child environment.
 * @returns Child environment map, per-run XDG scratch path, and resolved shared cache root.
 */
export function buildCheckCommandEnv(
    workDir: string,
    options: { cacheRoot?: string; baseEnv?: CheckProcessEnv } = {}
): { env: CheckProcessEnv; xdgScratchDir: string; cacheRoot: string } {
    const baseEnv = options.baseEnv ?? process.env;
    const cacheRoot = resolveCheckCacheRoot(options.cacheRoot, baseEnv);
    const pipCacheDir = path.join(cacheRoot, 'pip');
    const ivyCacheDir = path.join(cacheRoot, 'ivy2');
    const xdgScratchDir = path.join(path.resolve(workDir), '.xdg-cache');

    const env: CheckProcessEnv = { ...baseEnv };
    env.XDG_CACHE_HOME = xdgScratchDir;
    env.PIP_CACHE_DIR = pipCacheDir;
    env.SPARK_DESIGNER_CACHE_ROOT = cacheRoot;
    env.SPARK_IVY_CACHE_DIR = ivyCacheDir;
    env.PYTHONDONTWRITEBYTECODE = '1';
    env.PYTEST_ADDOPTS = env.PYTEST_ADDOPTS
        ? `${env.PYTEST_ADDOPTS} -p no:cacheprovider`
        : '-p no:cacheprovider';
    if (!env.PYSPARK_SUBMIT_ARGS) {
        const escapedIvy = ivyCacheDir.replace(/\\/g, '/').replace(/"/g, '\\"');
        env.PYSPARK_SUBMIT_ARGS = `--conf "spark.jars.ivy=${escapedIvy}" pyspark-shell`;
    }
    return { env, xdgScratchDir, cacheRoot };
}

/**
 * Returns true when `candidate` equals `parentDir` or resolves inside `parentDir`.
 *
 * @param candidate Path to inspect.
 * @param parentDir Potential ancestor directory.
 * @returns True if `candidate` is equal to or nested under `parentDir`.
 */
function isSameOrInsideDir(candidate: string, parentDir: string): boolean {
    const rel = path.relative(path.resolve(parentDir), path.resolve(candidate));
    return rel === '' || (!rel.startsWith('..') && !path.isAbsolute(rel));
}

/**
 * Recursively collects `__pycache__` directories under `dir` without following symlinks.
 *
 * @param dir Root directory to scan.
 * @param rootBoundary Workspace root boundary that traversal must stay inside.
 * @returns Matching `__pycache__` directory paths.
 */
async function collectPycacheDirs(dir: string, rootBoundary: string): Promise<string[]> {
    const found: string[] = [];
    let entries: fs.Dirent[];
    try {
        entries = await fs.promises.readdir(dir, { withFileTypes: true });
    } catch {
        return found;
    }
    for (const entry of entries) {
        if (entry.isSymbolicLink() || !entry.isDirectory()) {
            continue;
        }
        const full = path.join(dir, entry.name);
        if (!isSameOrInsideDir(full, rootBoundary)) {
            continue;
        }
        if (entry.name === '__pycache__') {
            found.push(full);
            continue;
        }
        const nested = await collectPycacheDirs(full, rootBoundary);
        found.push(...nested);
    }
    return found;
}

/**
 * Removes per-run `.venv`, `.spark-ivy`, `.pytest_cache`, `__pycache__`, and XDG scratch
 * directories from a check workspace while protecting the shared cache root from deletion.
 *
 * @param projectDir Check workspace directory.
 * @param options Cleanup options controlling shared cache protection and root removal.
 * @param options.cacheRoot Shared cache root directory that must never be deleted.
 * @param options.xdgScratchDir Optional per-run XDG scratch folder to remove.
 * @param options.removeProjectDir When true, removes `projectDir` itself after cleaning artifacts.
 * @returns List of per-run artifact paths deleted before optional workspace removal.
 */
export async function cleanupCheckWorkspaceArtifacts(
    projectDir: string,
    options: { cacheRoot?: string; xdgScratchDir?: string; removeProjectDir?: boolean } = {}
): Promise<string[]> {
    const root = path.resolve(projectDir);
    const cacheRoot = resolveCheckCacheRoot(options.cacheRoot);
    const removed: string[] = [];

    if (!fs.existsSync(root)) {
        return removed;
    }

    const targets = new Set<string>();
    for (const name of CHECK_RUN_ARTIFACT_DIRS) {
        targets.add(path.join(root, name));
    }
    if (options.xdgScratchDir) {
        const resolvedXdg = path.resolve(options.xdgScratchDir);
        if (resolvedXdg !== root) {
            targets.add(resolvedXdg);
        }
    }
    for (const pycacheDir of await collectPycacheDirs(root, root)) {
        targets.add(pycacheDir);
    }

    for (const target of targets) {
        if (target === root) {
            continue;
        }
        if (isSameOrInsideDir(target, cacheRoot) || isSameOrInsideDir(cacheRoot, target)) {
            continue;
        }
        if (fs.existsSync(target)) {
            await fs.promises.rm(target, { recursive: true, force: true });
            removed.push(target);
        }
    }

    if (
        options.removeProjectDir &&
        !isSameOrInsideDir(root, cacheRoot) &&
        !isSameOrInsideDir(cacheRoot, root)
    ) {
        await fs.promises.rm(root, { recursive: true, force: true });
    }
    return removed;
}

/**
 * Runs read-only verification checks over generated PySpark or SDP files.
 */
export class ReadOnlyCheckRunner {
    /**
     * Creates the runner.
     *
     * @param executor Command executor (tests inject a fake).
     * @param pythonCommand Python interpreter for `py_compile` and `pytest`.
     * @param sparkPipelinesCommand CLI binary for `spark-pipelines dry-run` in SDP mode.
     * @param cacheRoot Optional shared cache root directory override.
     * @param baseEnv Optional parent environment map copied into check commands.
     */
    constructor(
        private readonly executor: CommandExecutor = defaultCommandExecutor,
        private readonly pythonCommand: string = READ_ONLY_CHECK_LIMITS.pythonCommand,
        private readonly sparkPipelinesCommand = 'spark-pipelines',
        private readonly cacheRoot?: string,
        private readonly baseEnv: CheckProcessEnv = process.env
    ) {}

    /**
     * Runs every check in a stable order for the detected paradigm.
     *
     * @param request Files, language, base name, runner files, and optional paradigm.
     * @returns One result per check.
     */
    public async run(request: CheckRequest): Promise<CheckResult[]> {
        if (isSdpGeneratedProject(request.files, request.paradigm)) {
            return this.runSdpChecks(request);
        }
        if (isNotebookGeneratedProject(request.files, request.paradigm)) {
            return this.runNotebookChecks(request);
        }

        const syntaxAndPytest = await this.checkPythonSyntaxAndPytest(request.files, false, request.cacheRoot);
        return [
            this.checkRequiredFiles(request),
            this.checkLeftoverTokens(request.files, false),
            syntaxAndPytest.syntaxResult,
            this.checkPipelineContract(request),
            this.checkTestImports(request),
            syntaxAndPytest.pytestResult,
        ];
    }

    /**
     * Runs all read-only checks for a Jupyter Notebook (`.ipynb`) project.
     *
     * @param request Check request.
     * @returns Ordered notebook check results.
     */
    private async runNotebookChecks(request: CheckRequest): Promise<CheckResult[]> {
        const syntaxAndPytest = await this.checkPythonSyntaxAndPytest(request.files, false, request.cacheRoot);
        return [
            this.checkNotebookRequiredFiles(request),
            this.checkLeftoverTokens(request.files, false),
            syntaxAndPytest.syntaxResult,
            this.checkNotebookContract(request.files),
            this.checkSdpTestImports(request.files),
            syntaxAndPytest.pytestResult,
        ];
    }

    /**
     * Runs all read-only checks for a Spark Declarative Pipeline (SDP) project.
     *
     * @param request Check request.
     * @returns Ordered SDP check results.
     */
    private async runSdpChecks(request: CheckRequest): Promise<CheckResult[]> {
        const execResults = await this.checkPythonSyntaxAndPytest(request.files, true, request.cacheRoot);
        const dryRunResult: CheckResult = execResults.dryRunResult ?? {
            check: CHECK_IDS.sdpDryRun,
            status: 'skipped',
            output: 'No spark-pipeline.yml generated.',
        };
        return [
            this.checkSdpRequiredFiles(request),
            this.checkLeftoverTokens(request.files, true),
            execResults.syntaxResult,
            this.checkSdpSpecContract(request.files),
            this.checkSdpPipelineContract(request.files),
            this.checkSdpTestImports(request.files),
            execResults.pytestResult,
            dryRunResult,
        ];
    }

    /**
     * Checks notebook required files (`requirements.txt`, `pipeline.ipynb`,
     * `src/transforms.py`, `tests/test_transforms.py`, and runner files).
     *
     * @param request Check request.
     * @returns Check result.
     */
    private checkNotebookRequiredFiles(request: CheckRequest): CheckResult {
        const normalizedPaths = request.files.map((file) => normalizeRelPath(file.filename));
        const present = new Set(normalizedPaths);
        const missing: string[] = [];

        const buildFile = LANGUAGE_BUILD_FILES[request.language];
        if (buildFile && !present.has(buildFile)) {
            missing.push(buildFile);
        }

        const hasNotebook = normalizedPaths.some((p) => p.toLowerCase().endsWith('.ipynb'));
        if (!hasNotebook) {
            missing.push('pipeline.ipynb');
        }
        if (!present.has('src/transforms.py')) {
            missing.push('src/transforms.py');
        }
        if (!present.has('tests/test_transforms.py')) {
            missing.push('tests/test_transforms.py');
        }

        for (const runnerFile of request.runnerFiles) {
            if (!present.has(normalizeRelPath(runnerFile))) {
                missing.push(runnerFile);
            }
        }

        return missing.length === 0
            ? { check: CHECK_IDS.requiredFiles, status: 'passed', output: '' }
            : { check: CHECK_IDS.requiredFiles, status: 'failed', output: `Missing file(s): ${missing.join(', ')}` };
    }

    /**
     * Validates generated `.ipynb` JSON structure and pure `src/transforms.py` helpers.
     *
     * @param files Generated files.
     * @returns Check result.
     */
    private checkNotebookContract(files: GeneratedFile[]): CheckResult {
        const notebookFiles = files.filter((f) => normalizeRelPath(f.filename).toLowerCase().endsWith('.ipynb'));
        if (notebookFiles.length === 0) {
            return {
                check: CHECK_IDS.pipelineContract,
                status: 'skipped',
                output: 'No .ipynb notebook generated.',
            };
        }

        const errors: string[] = [];
        for (const nb of notebookFiles) {
            errors.push(...validateNotebookIpynb(nb.filename, nb.content));
        }

        const transformsHelper = files.find((f) => normalizeRelPath(f.filename) === 'src/transforms.py');
        if (transformsHelper) {
            const helperCleaned = stripPythonCommentsAndDocstrings(transformsHelper.content).join('\n');
            if (!/^\s*def\s+[A-Za-z_][A-Za-z0-9_]*\s*\(/m.test(helperCleaned)) {
                errors.push('src/transforms.py must define at least one pure transformation helper function.');
            }
        }

        return errors.length === 0
            ? { check: CHECK_IDS.pipelineContract, status: 'passed', output: '' }
            : {
                  check: CHECK_IDS.pipelineContract,
                  status: 'failed',
                  output: truncateCheckOutput(errors.join('\n')),
              };
    }

    /**
     * Checks that `requirements.txt`, `src/pipeline.py`, `tests/test_pipeline.py`, and
     * runner files exist in imperative mode.
     *
     * @param request Check request.
     * @returns Check result.
     */
    private checkRequiredFiles(request: CheckRequest): CheckResult {
        const present = new Set(request.files.map((file) => normalizeRelPath(file.filename)));
        const missing: string[] = [];

        const buildFile = LANGUAGE_BUILD_FILES[request.language];
        if (buildFile && !present.has(buildFile)) {
            missing.push(buildFile);
        }

        if (!findMainPipelineFile(request.files, request.baseName)) {
            missing.push('src/pipeline.py');
        }
        if (!findTestPipelineFile(request.files, request.baseName)) {
            missing.push('tests/test_pipeline.py');
        }

        for (const runnerFile of request.runnerFiles) {
            if (!present.has(normalizeRelPath(runnerFile))) {
                missing.push(runnerFile);
            }
        }

        return missing.length === 0
            ? { check: CHECK_IDS.requiredFiles, status: 'passed', output: '' }
            : { check: CHECK_IDS.requiredFiles, status: 'failed', output: `Missing file(s): ${missing.join(', ')}` };
    }

    /**
     * Checks SDP required files (`requirements.txt`, single `spark-pipeline.yml`,
     * `transformations/*.py`, `src/transforms.py`, `tests/test_transforms.py`, and runner files).
     *
     * @param request Check request.
     * @returns Check result.
     */
    private checkSdpRequiredFiles(request: CheckRequest): CheckResult {
        const normalizedPaths = request.files.map((file) => normalizeRelPath(file.filename));
        const present = new Set(normalizedPaths);
        const missing: string[] = [];
        const errors: string[] = [];

        const buildFile = LANGUAGE_BUILD_FILES[request.language];
        if (buildFile && !present.has(buildFile)) {
            missing.push(buildFile);
        }

        const hasYml = present.has('spark-pipeline.yml');
        const hasYaml = present.has('spark-pipeline.yaml');
        if (hasYml && hasYaml) {
            errors.push(
                'MULTIPLE_PIPELINE_SPEC_FILES_FOUND: Keep only one pipeline spec file (spark-pipeline.yml or spark-pipeline.yaml), never both.'
            );
        } else if (!hasYml && !hasYaml) {
            missing.push('spark-pipeline.yml');
        }

        const hasPythonTransformation = normalizedPaths.some(
            (p) => /^transformations\/.+\.py$/.test(p) && !p.endsWith('/__init__.py')
        );
        const hasSqlTransformation = normalizedPaths.some((p) => /^transformations\/.+\.sql$/i.test(p));
        if (!hasPythonTransformation && !hasSqlTransformation) {
            missing.push('transformations/*.py or transformations/*.sql');
        }

        if (hasPythonTransformation) {
            if (!present.has('src/transforms.py')) {
                missing.push('src/transforms.py');
            }
            if (!present.has('tests/test_transforms.py')) {
                missing.push('tests/test_transforms.py');
            }
        }

        for (const runnerFile of request.runnerFiles) {
            if (!present.has(normalizeRelPath(runnerFile))) {
                missing.push(runnerFile);
            }
        }

        if (missing.length > 0) {
            errors.push(`Missing file(s): ${missing.join(', ')}`);
        }

        return errors.length === 0
            ? { check: CHECK_IDS.requiredFiles, status: 'passed', output: '' }
            : { check: CHECK_IDS.requiredFiles, status: 'failed', output: truncateCheckOutput(errors.join('\n')) };
    }

    /**
     * Checks that no codegen placeholder or redaction sentinel remains in any file.
     * In SDP YAML spec files, Managed Data Pipeline Jinja variables are permitted while
     * known codegen placeholders (`{{pipeline_name}}`, `{{spark_version}}`, etc.) fail.
     *
     * @param files Generated files.
     * @param isSdp True when checking an SDP project.
     * @returns Check result.
     */
    private checkLeftoverTokens(files: GeneratedFile[], isSdp: boolean): CheckResult {
        const problems: string[] = [];
        for (const file of files) {
            const norm = normalizeRelPath(file.filename);
            const isSdpYamlSpec =
                isSdp &&
                (norm === 'spark-pipeline.yml' ||
                    norm === 'spark-pipeline.yaml' ||
                    norm.endsWith('.pipeline.yaml'));

            const rawMatches = file.content.match(PLACEHOLDER_PATTERN) ?? [];
            const tokens = new Set(
                isSdpYamlSpec
                    ? rawMatches.filter((tok) => KNOWN_CODEGEN_PLACEHOLDERS.has(tok))
                    : rawMatches
            );
            if (file.content.includes(REDACTION_PLACEHOLDER)) {
                tokens.add(REDACTION_PLACEHOLDER);
            }
            if (file.content.includes(SPARK_REDACTION_PLACEHOLDER)) {
                tokens.add(SPARK_REDACTION_PLACEHOLDER);
            }
            if (tokens.size > 0) {
                problems.push(`${file.filename}: ${[...tokens].join(', ')}`);
            }
        }
        return problems.length === 0
            ? { check: CHECK_IDS.leftoverTokens, status: 'passed', output: '' }
            : { check: CHECK_IDS.leftoverTokens, status: 'failed', output: truncateCheckOutput(problems.join('\n')) };
    }

    /**
     * Validates `spark-pipeline.yml` (or `spark-pipeline.yaml`) in SDP mode.
     *
     * @param files Generated files.
     * @returns Check result.
     */
    private checkSdpSpecContract(files: GeneratedFile[]): CheckResult {
        const specFiles = files.filter((f) => {
            const norm = normalizeRelPath(f.filename);
            return norm === 'spark-pipeline.yml' || norm === 'spark-pipeline.yaml';
        });

        if (specFiles.length === 0) {
            return {
                check: CHECK_IDS.sdpSpecContract,
                status: 'skipped',
                output: 'No spark-pipeline.yml generated.',
            };
        }

        if (specFiles.length > 1) {
            return {
                check: CHECK_IDS.sdpSpecContract,
                status: 'failed',
                output: 'MULTIPLE_PIPELINE_SPEC_FILES_FOUND: Project contains both spark-pipeline.yml and spark-pipeline.yaml.',
            };
        }

        const spec = specFiles[0];
        const violations = validateSdpSpecYaml(spec.filename, spec.content);
        return violations.length === 0
            ? { check: CHECK_IDS.sdpSpecContract, status: 'passed', output: '' }
            : {
                  check: CHECK_IDS.sdpSpecContract,
                  status: 'failed',
                  output: truncateCheckOutput(violations.map((v) => v.message).join('\n')),
              };
    }

    /**
     * Verifies `transformations/*.py` dataset declarations, pure `src/transforms.py` helpers,
     * and absence of banned session mutations, writers, actions, and Databricks APIs in SDP mode.
     *
     * @param files Generated files.
     * @returns Check result.
     */
    private checkSdpPipelineContract(files: GeneratedFile[]): CheckResult {
        const pyTransformationFiles = files.filter((f) => {
            const norm = normalizeRelPath(f.filename);
            return /^transformations\/.+\.py$/.test(norm) && !norm.endsWith('/__init__.py');
        });
        const sqlTransformationFiles = files.filter((f) => {
            const norm = normalizeRelPath(f.filename);
            return /^transformations\/.+\.sql$/i.test(norm);
        });
        if (pyTransformationFiles.length === 0 && sqlTransformationFiles.length === 0) {
            return {
                check: CHECK_IDS.pipelineContract,
                status: 'skipped',
                output: 'No transformations/*.py or transformations/*.sql modules generated.',
            };
        }

        const errors: string[] = [];

        const hasDpRegistration = pyTransformationFiles.some((f) => {
            const cleaned = stripPythonCommentsAndDocstrings(f.content).join('\n');
            return (
                /@\s*dp\s*\.\s*(?:table|materialized_view|temporary_view|append_flow)\b/.test(cleaned) ||
                /\bdp\s*\.\s*create_(?:streaming_table|auto_cdc_flow|sink)\s*\(/.test(cleaned)
            );
        });
        const hasSqlRegistration = sqlTransformationFiles.some((f) => {
            const cleaned = stripSqlComments(f.content);
            return /\bCREATE\s+(?:STREAMING\s+TABLE|MATERIALIZED\s+VIEW|TEMPORARY\s+VIEW|VIEW|FLOW)\b/i.test(
                cleaned
            );
        });
        if (!hasDpRegistration && !hasSqlRegistration) {
            errors.push(
                'transformations/* does not declare any Python @dp.* / dp.create_* or SQL CREATE STREAMING TABLE / MATERIALIZED VIEW / TEMPORARY VIEW / FLOW dataset.'
            );
        }

        const transformsHelper = files.find((f) => normalizeRelPath(f.filename) === 'src/transforms.py');
        if (transformsHelper) {
            const helperCleaned = stripPythonCommentsAndDocstrings(transformsHelper.content).join('\n');
            if (!/^\s*def\s+[A-Za-z_][A-Za-z0-9_]*\s*\(/m.test(helperCleaned)) {
                errors.push('src/transforms.py must define at least one pure transformation helper function.');
            }
        }

        for (const v of scanSdpPythonViolations(files)) {
            errors.push(v.message);
        }
        for (const v of scanSdpSqlViolations(files)) {
            errors.push(v.message);
        }

        return errors.length === 0
            ? { check: CHECK_IDS.pipelineContract, status: 'passed', output: '' }
            : {
                  check: CHECK_IDS.pipelineContract,
                  status: 'failed',
                  output: truncateCheckOutput(errors.join('\n')),
              };
    }

    /**
     * Verifies that `src/pipeline.py` defines `read_sources`, `transform`, and `write_sinks`.
     *
     * @param request Check request.
     * @returns Check result.
     */
    private checkPipelineContract(request: CheckRequest): CheckResult {
        const mainFile = findMainPipelineFile(request.files, request.baseName);
        if (!mainFile) {
            return {
                check: CHECK_IDS.pipelineContract,
                status: 'skipped',
                output: 'No src/pipeline.py generated.',
            };
        }

        const missingFns = REQUIRED_PIPELINE_FUNCTIONS.filter((fnName) => {
            const fnRegex = new RegExp(`^\\s*def\\s+${fnName}\\s*\\(`, 'm');
            return !fnRegex.test(mainFile.content);
        });

        return missingFns.length === 0
            ? { check: CHECK_IDS.pipelineContract, status: 'passed', output: '' }
            : {
                  check: CHECK_IDS.pipelineContract,
                  status: 'failed',
                  output: `${mainFile.filename} is missing required testable entrypoint function(s): ${missingFns.join(', ')}.`,
              };
    }

    /**
     * Checks that the Python test module imports the main module (`src.pipeline` or `src.<baseName>`).
     *
     * @param request Check request.
     * @returns Check result.
     */
    private checkTestImports(request: CheckRequest): CheckResult {
        const test = findTestPipelineFile(request.files, request.baseName);
        if (!test) {
            return { check: CHECK_IDS.testImports, status: 'skipped', output: 'No tests/test_pipeline.py generated.' };
        }
        const escapedBase = request.baseName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const moduleGroup = `(?:pipeline|${escapedBase})`;
        const pattern = new RegExp(
            `^\\s*(from\\s+(src\\.)?${moduleGroup}\\s+import|import\\s+(src\\.)?${moduleGroup}\\b)`,
            'm'
        );
        return pattern.test(test.content)
            ? { check: CHECK_IDS.testImports, status: 'passed', output: '' }
            : {
                  check: CHECK_IDS.testImports,
                  status: 'failed',
                  output: `${test.filename} does not import the main module 'src.pipeline'.`,
              };
    }

    /**
     * Checks that `tests/test_transforms.py` imports `src.transforms` or `transforms` in SDP mode.
     *
     * @param files Generated files.
     * @returns Check result.
     */
    private checkSdpTestImports(files: GeneratedFile[]): CheckResult {
        const test = findSdpTestTransformsFile(files);
        if (!test) {
            return {
                check: CHECK_IDS.testImports,
                status: 'skipped',
                output: 'No tests/test_transforms.py generated.',
            };
        }
        const pattern =
            /^\s*(?:from\s+(?:src\.)?transforms\s+import|from\s+src\s+import\s+[^#\n]*\btransforms\b|import\s+(?:src\.)?transforms\b)/m;
        return pattern.test(test.content)
            ? { check: CHECK_IDS.testImports, status: 'passed', output: '' }
            : {
                  check: CHECK_IDS.testImports,
                  status: 'failed',
                  output: `${test.filename} does not import the pure helper module 'src.transforms'.`,
              };
    }

    /**
     * Byte-compiles every Python file with `python3 -m py_compile`, runs
     * `python3 -m pytest --collect-only -q`, and (in SDP mode) runs
     * `spark-pipelines dry-run --spec <specFile>` when `spark-pipelines` is available.
     *
     * @param files Generated files.
     * @param includeSdpDryRun True when running in SDP mode.
     * @param requestCacheRoot Optional shared cache root override from the check request.
     * @returns Syntax, pytest collection, and optional SDP dry-run outcomes.
     */
    private async checkPythonSyntaxAndPytest(
        files: GeneratedFile[],
        includeSdpDryRun: boolean,
        requestCacheRoot?: string
    ): Promise<{ syntaxResult: CheckResult; pytestResult: CheckResult; dryRunResult?: CheckResult }> {
        const pythonFiles = files.filter((file) => file.filename.endsWith('.py'));
        const specFile = includeSdpDryRun
            ? files.find((f) => {
                  const norm = normalizeRelPath(f.filename);
                  return norm === 'spark-pipeline.yml' || norm === 'spark-pipeline.yaml';
              })
            : undefined;

        if (pythonFiles.length === 0 && !specFile) {
            return {
                syntaxResult: {
                    check: CHECK_IDS.pythonSyntax,
                    status: 'skipped',
                    output: 'No Python files generated.',
                },
                pytestResult: {
                    check: CHECK_IDS.pytestCollect,
                    status: 'skipped',
                    output: 'No Python files generated.',
                },
                ...(includeSdpDryRun
                    ? {
                          dryRunResult: {
                              check: CHECK_IDS.sdpDryRun,
                              status: 'skipped',
                              output: 'No spark-pipeline.yml generated.',
                          },
                      }
                    : {}),
            };
        }

        const workDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'spark-codegen-checks-'));
        const { env: checkEnv, xdgScratchDir, cacheRoot } = buildCheckCommandEnv(workDir, {
            cacheRoot: requestCacheRoot ?? this.cacheRoot,
            baseEnv: this.baseEnv,
        });
        try {
            await fs.promises.mkdir(xdgScratchDir, { recursive: true });
            // 1. Materialize files safely inside workDir.
            const filesToWrite = includeSdpDryRun ? files : pythonFiles;
            const writtenAll = await materializeWorkspaceFiles(workDir, filesToWrite);
            const writtenPy = writtenAll.filter((rel) => rel.endsWith('.py'));

            let syntaxResult: CheckResult;
            let pytestResult: CheckResult;

            if (writtenPy.length === 0) {
                const skipMsg = pythonFiles.length === 0 ? 'No Python files generated.' : 'No safe Python files generated.';
                syntaxResult = { check: CHECK_IDS.pythonSyntax, status: 'skipped', output: skipMsg };
                pytestResult = { check: CHECK_IDS.pytestCollect, status: 'skipped', output: skipMsg };
            } else {
                // 2. Run python3 -m py_compile. A missing interpreter skips rather than fails.
                const compileOutcome = await this.executor(this.pythonCommand, ['-m', 'py_compile', ...writtenPy], {
                    cwd: workDir,
                    timeoutMs: READ_ONLY_CHECK_LIMITS.timeoutMs,
                    env: checkEnv,
                });
                if (compileOutcome.notFound) {
                    const msg = `Python interpreter '${this.pythonCommand}' not found.`;
                    syntaxResult = { check: CHECK_IDS.pythonSyntax, status: 'skipped', output: msg };
                    pytestResult = { check: CHECK_IDS.pytestCollect, status: 'skipped', output: msg };
                } else {
                    const compileOutput = truncateCheckOutput(
                        [compileOutcome.stdout, compileOutcome.stderr].filter(Boolean).join('\n').trim()
                    );
                    syntaxResult = {
                        check: CHECK_IDS.pythonSyntax,
                        status: compileOutcome.exitCode === 0 ? 'passed' : 'failed',
                        output: compileOutput,
                    };

                    // 3. Run python3 -m pytest --collect-only -q when test files exist.
                    const hasTestFile = writtenPy.some(
                        (rel) =>
                            rel.startsWith('tests/') ||
                            rel.endsWith('_test.py') ||
                            path.basename(rel).startsWith('test_')
                    );
                    if (!hasTestFile) {
                        pytestResult = {
                            check: CHECK_IDS.pytestCollect,
                            status: 'skipped',
                            output: 'No pytest test files generated.',
                        };
                    } else {
                        const pytestOutcome = await this.executor(
                            this.pythonCommand,
                            ['-m', 'pytest', '--collect-only', '-q'],
                            {
                                cwd: workDir,
                                timeoutMs: READ_ONLY_CHECK_LIMITS.timeoutMs,
                                env: checkEnv,
                            }
                        );
                        const pytestCombined = [pytestOutcome.stdout, pytestOutcome.stderr]
                            .filter(Boolean)
                            .join('\n')
                            .trim();
                        if (
                            pytestOutcome.notFound ||
                            /No module named ['"]?pytest['"]?/i.test(pytestCombined) ||
                            /ModuleNotFoundError:\s*No module named ['"]?pyspark['"]?/i.test(pytestCombined)
                        ) {
                            pytestResult = {
                                check: CHECK_IDS.pytestCollect,
                                status: 'skipped',
                                output: truncateCheckOutput(
                                    pytestCombined || 'pytest or pyspark module not installed in local environment.'
                                ),
                            };
                        } else {
                            pytestResult = {
                                check: CHECK_IDS.pytestCollect,
                                status: pytestOutcome.exitCode === 0 ? 'passed' : 'failed',
                                output: truncateCheckOutput(pytestCombined),
                            };
                        }
                    }
                }
            }

            if (!includeSdpDryRun) {
                return { syntaxResult, pytestResult };
            }

            // 4. Optional spark-pipelines dry-run when SDP spec is materialized.
            const dryRunResult = await this.runOptionalSdpDryRun(workDir, specFile, writtenAll, checkEnv);
            return { syntaxResult, pytestResult, dryRunResult };
        } finally {
            await cleanupCheckWorkspaceArtifacts(workDir, {
                cacheRoot,
                xdgScratchDir,
                removeProjectDir: true,
            });
        }
    }

    /**
     * Executes `spark-pipelines dry-run --spec <specFile>` when `spark-pipelines` is available.
     *
     * @param workDir Materialized temporary project directory.
     * @param specFile SDP spec file candidate, if present.
     * @param writtenFiles Materialized relative file paths.
     * @param checkEnv Isolated child environment for the dry-run process.
     * @returns Dry-run check result (`passed`, `failed`, or `skipped`).
     */
    private async runOptionalSdpDryRun(
        workDir: string,
        specFile: GeneratedFile | undefined,
        writtenFiles: readonly string[],
        checkEnv: CheckProcessEnv
    ): Promise<CheckResult> {
        if (!specFile) {
            return {
                check: CHECK_IDS.sdpDryRun,
                status: 'skipped',
                output: 'No spark-pipeline.yml generated.',
            };
        }
        const specRel = normalizeRelPath(specFile.filename);
        if (!writtenFiles.includes(specRel)) {
            return {
                check: CHECK_IDS.sdpDryRun,
                status: 'skipped',
                output: 'No safe spark-pipeline.yml generated.',
            };
        }

        const outcome = await this.executor(this.sparkPipelinesCommand, ['dry-run', '--spec', specRel], {
            cwd: workDir,
            timeoutMs: READ_ONLY_CHECK_LIMITS.timeoutMs,
            env: checkEnv,
        });
        const combined = [outcome.stdout, outcome.stderr].filter(Boolean).join('\n').trim();
        if (outcome.notFound) {
            return {
                check: CHECK_IDS.sdpDryRun,
                status: 'skipped',
                output: `CLI '${this.sparkPipelinesCommand}' not installed on PATH.`,
            };
        }
        if (outcome.exitCode !== 0 && isSdpDryRunEnvironmentSkip(combined)) {
            return {
                check: CHECK_IDS.sdpDryRun,
                status: 'skipped',
                output: truncateCheckOutput(combined || 'Local Spark 4.2 environment unavailable for dry-run.'),
            };
        }
        return {
            check: CHECK_IDS.sdpDryRun,
            status: outcome.exitCode === 0 ? 'passed' : 'failed',
            output: truncateCheckOutput(combined),
        };
    }
}
