/**
 * Spark Architecture Evaluator Sub-Agent (`SparkArchitectureAgent`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Audits generated PySpark 4.x and Spark Declarative Pipelines (SDP, Apache Spark 4.2+)
 * repositories for Spark architecture defects.
 * - Imperative mode checks Catalyst optimization rules, `read_sources` / pure
 *   `transform(dfs, config)` / `write_sinks` separation (decision D3), `.cache()` and
 *   `.unpersist()` pairing, `foreachBatch` idempotency, watermark placement (decision D8),
 *   and Managed Spark classpath rules.
 * - SDP mode checks `spark-pipeline.yml` spec hygiene, dataset-function purity inside
 *   `transformations/*.py` (no `SparkSession.builder`, no session mutations, no writers,
 *   no eager actions, no `.schema`/`.columns` property inspection), pure helper isolation
 *   in `src/transforms.py`, query-type alignment (`@dp.table` vs `@dp.materialized_view`),
 *   `SdpExecutionPlan` dataset coverage, and absence of Databricks-only APIs.
 *
 * Architectural decisions:
 * - Two layers. A deterministic static layer runs first and emits high-precision findings.
 *   An LLM layer then audits the semantic rules that regular expressions cannot prove.
 * - The static layer, prompt builder, and response parser are pure functions covered
 *   by offline unit tests without a model client.
 * - If the LLM call fails, the agent keeps the static findings and marks the result as failed.
 *   It never invents a synthetic finding for an infrastructure error.
 * - Test files (`tests/`, `test_*.py`, `*_test.py`) are exempt from driver-collection
 *   and UDF rules because tests may collect small fixtures.
 */

import * as YAML from 'yaml';
import { GoogleGenAI } from '@google/genai';
import {
    isSdpGeneratedProject,
    scanSdpPythonViolations,
    stripPythonCommentsAndDocstrings,
    validateSdpSpecYaml,
} from '../../../codegen/ReadOnlyCheckRunner';
import { DEFAULT_GEMINI_MODEL } from '../../../core/constants';
import type { DagEdge, DagNode } from '../../../shared/dagValidator';
import { buildSdpPlan, type SdpExecutionPlan } from '../../../shared/sdpPlanner';
import { DEFAULT_SPARK_VERSION, type PipelineParadigm } from '../../../shared/types';
import { EvaluatorFinding, FindingSeverity, SpecialistAgentResult, generateContentWithRetry } from './types';
import type { GeneratedFileCandidate } from './deterministic-gate';
import { formatFilesWithLineNumbers } from './format-utils';

/** Agent name used as the key in `MultiAgentEvaluationResult.specialistResults`. */
export const SPARK_ARCHITECTURE_AGENT_NAME = 'SparkArchitectureAgent';

/** Finding categories that this agent may emit. */
export const SPARK_ARCHITECTURE_CATEGORIES = ['ARCHITECTURE', 'IDIOMATIC_SPARK'] as const;

/** Category type that this agent may emit. */
export type SparkArchitectureCategory = (typeof SPARK_ARCHITECTURE_CATEGORIES)[number];

/** Runner scripts that submit to Managed Spark (Dataproc Cluster and Serverless Spark). */
const MANAGED_SPARK_RUNNER_SCRIPTS = [
    'run_dataproc_cluster.sh',
    'run_serverless_spark.sh',
    'run_sdp_dataproc.sh',
];

/** Connectors that Managed Spark runtimes pre-install. A second copy causes classpath conflicts. */
const MANAGED_SPARK_PREINSTALLED_CONNECTORS = [
    'spark-bigquery',
    'iceberg-spark-runtime',
    'gcs-connector',
    'hadoop-aws',
    'hadoop-azure',
];

/** Body of one Python function extracted from source text. */
export interface PythonFunctionBody {
    /** 1-indexed line number of the `def` line. */
    defLine: number;
    /** Parameter text between the parentheses of the `def` line when on one line. */
    params: string;
    /** Body lines with their 1-indexed line numbers. */
    lines: { line: number; text: string }[];
}

/** Deterministic facts that the static layer extracts for the LLM prompt. */
export interface SparkArchitectureSignals {
    /** Main pipeline file name, or undefined when no non-test Python file exists. */
    pipelineFile?: string;
    /** True when the main pipeline file defines `read_sources`. */
    hasReadSources: boolean;
    /** True when the main pipeline file defines `transform`. */
    hasTransform: boolean;
    /** True when the main pipeline file defines `write_sinks`. */
    hasWriteSinks: boolean;
    /** True when any non-test Python file reads a stream (`readStream`). */
    isStreaming: boolean;
    /** True when any non-test Python file calls `withWatermark`. */
    usesWatermark: boolean;
    /** True when any non-test Python file calls `foreachBatch`. */
    usesForeachBatch: boolean;
    /** Number of `.cache()` and `.persist(` calls in non-test Python files. */
    cacheCalls: number;
    /** Number of `.unpersist(` calls in non-test Python files. */
    unpersistCalls: number;
    /** Number of row-at-a-time Python UDF declarations in non-test Python files. */
    pythonUdfCount: number;
    /** Optional SDP paradigm marker populated when evaluating SDP projects. */
    paradigm?: PipelineParadigm;
    /** Optional SDP spec filename when evaluating SDP projects. */
    sdpSpecFile?: string;
    /** Optional count of SDP transformation modules under `transformations/`. */
    sdpTransformationFiles?: number;
    /** Optional flag indicating `src/transforms.py` exists with pure helper functions. */
    hasPureTransformsModule?: boolean;
}

/** Options controlling Spark architecture evaluation and static checks. */
export interface SparkArchitectureCheckOptions {
    /** Explicit paradigm override (`imperative` or `sdp`). */
    paradigm?: PipelineParadigm;
    /** Optional pipeline YAML text used to build an `SdpExecutionPlan` in SDP mode. */
    yamlContent?: string;
}

/** Parsed and normalized LLM response of the Spark architecture agent. */
export interface SparkArchitectureResponse {
    passed: boolean;
    summary: string;
    findings: EvaluatorFinding[];
}

/**
 * Resolves the active paradigm from explicit options, pipeline YAML, or repository files.
 *
 * @param files - Generated files.
 * @param yamlContent - Optional pipeline YAML string.
 * @param explicit - Optional explicit paradigm override.
 * @returns Resolved `PipelineParadigm`.
 */
export function resolveSparkArchitectureParadigm(
    files: readonly Pick<GeneratedFileCandidate, 'filename'>[],
    yamlContent?: string,
    explicit?: PipelineParadigm
): PipelineParadigm {
    if (explicit === 'sdp' || explicit === 'imperative') {
        return explicit;
    }
    if (yamlContent && yamlContent.trim()) {
        try {
            const parsed = YAML.parse(yamlContent) as Record<string, unknown> | null;
            const pCfg =
                parsed && typeof parsed.pipelineConfig === 'object' && parsed.pipelineConfig !== null
                    ? (parsed.pipelineConfig as Record<string, unknown>)
                    : undefined;
            if (parsed?.paradigm === 'sdp' || pCfg?.paradigm === 'sdp') {
                return 'sdp';
            }
            if (parsed?.paradigm === 'imperative' || pCfg?.paradigm === 'imperative') {
                return 'imperative';
            }
        } catch {
            // Fall through to file-based auto-detection.
        }
    }
    return isSdpGeneratedProject(files) ? 'sdp' : 'imperative';
}

/**
 * Builds an `SdpExecutionPlan` from pipeline YAML when valid nodes exist.
 *
 * @param yamlContent - Raw pipeline YAML string.
 * @returns Computed `SdpExecutionPlan`, or null when YAML is empty or malformed.
 */
function tryBuildSdpPlanFromYaml(yamlContent?: string): SdpExecutionPlan | null {
    if (!yamlContent || !yamlContent.trim()) {
        return null;
    }
    try {
        const parsed = YAML.parse(yamlContent) as Record<string, unknown> | null;
        if (!parsed || typeof parsed !== 'object' || !Array.isArray(parsed.nodes) || parsed.nodes.length === 0) {
            return null;
        }
        const isRec = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null;
        const nodes: DagNode[] = (parsed.nodes as unknown[])
            .filter((n): n is Record<string, unknown> => isRec(n) && n.id !== undefined && n.id !== null)
            .map((n) => ({ ...n, id: String(n.id) }));
        const edges: DagEdge[] = (Array.isArray(parsed.edges) ? (parsed.edges as unknown[]) : [])
            .filter(isRec)
            .map((e) => ({ ...e, source: String(e.source ?? ''), target: String(e.target ?? '') }));
        const jobConfig = isRec(parsed.jobConfig) ? parsed.jobConfig : undefined;
        return buildSdpPlan(nodes, edges, jobConfig);
    } catch {
        return null;
    }
}

/**
 * Returns true when a generated file is a Python test file.
 *
 * @param filename - Generated file path.
 * @returns True for `tests/` files, `test_*.py`, and `*_test.py`.
 */
export function isPythonTestFile(filename: string): boolean {
    const normalized = filename.replace(/\\/g, '/');
    const base = normalized.split('/').pop() ?? normalized;
    return /(^|\/)tests?\//.test(normalized) || /^test_.*\.py$/.test(base) || /_test\.py$/.test(base);
}

/**
 * Returns the non-test Python files that hold pipeline code.
 *
 * @param files - All generated files.
 * @returns Non-test `.py` files without `__init__.py`.
 */
export function getPipelineSourceFiles(files: GeneratedFileCandidate[]): GeneratedFileCandidate[] {
    return files.filter(
        (f) => f.filename.endsWith('.py') && !f.filename.endsWith('__init__.py') && !isPythonTestFile(f.filename)
    );
}

/**
 * Selects the main pipeline file. Prefers `src/pipeline.py` (decision D3).
 *
 * @param files - All generated files.
 * @returns The main pipeline file, or undefined when no candidate exists.
 */
export function selectMainPipelineFile(files: GeneratedFileCandidate[]): GeneratedFileCandidate | undefined {
    const sources = getPipelineSourceFiles(files);
    return (
        sources.find((f) => f.filename.replace(/\\/g, '/').endsWith('src/pipeline.py')) ??
        sources.find((f) => /(^|\/)pipeline\.py$/.test(f.filename.replace(/\\/g, '/'))) ??
        sources[0]
    );
}

/**
 * Extracts the body of a top-level or nested Python function by indentation.
 *
 * @param source - Python source text.
 * @param name - Function name.
 * @returns The body, or null when the function does not exist.
 */
export function extractPythonFunctionBody(source: string, name: string): PythonFunctionBody | null {
    const lines = source.split('\n');
    const defPattern = new RegExp(`^(\\s*)(?:async\\s+)?def\\s+${name}\\s*\\(([^)]*)`);
    for (let i = 0; i < lines.length; i++) {
        const match = defPattern.exec(lines[i]);
        if (!match) {
            continue;
        }
        const defIndent = match[1].length;
        const body: { line: number; text: string }[] = [];
        for (let j = i + 1; j < lines.length; j++) {
            const text = lines[j];
            if (text.trim() === '') {
                continue;
            }
            const indent = text.length - text.trimStart().length;
            // NOTE: A non-blank line at or below the def indentation ends the function.
            if (indent <= defIndent) {
                break;
            }
            body.push({ line: j + 1, text });
        }
        return { defLine: i + 1, params: match[2].trim(), lines: body };
    }
    return null;
}

/**
 * Returns the line without a trailing `#` comment. Keeps `#` inside simple quotes.
 *
 * @param text - One source line.
 * @returns Code part of the line.
 */
function stripPythonComment(text: string): string {
    let quote: string | null = null;
    for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        if (quote) {
            if (ch === '\\') {
                i++;
            } else if (ch === quote) {
                quote = null;
            }
        } else if (ch === '"' || ch === "'") {
            quote = ch;
        } else if (ch === '#') {
            return text.slice(0, i);
        }
    }
    return text;
}

/**
 * Finds every line in a file that matches a pattern. Ignores Python comments in `.py` files.
 *
 * @param file - File to scan.
 * @param pattern - Pattern to test per line.
 * @returns Matching 1-indexed lines with trimmed evidence.
 */
function findLines(file: GeneratedFileCandidate, pattern: RegExp): { line: number; evidence: string }[] {
    const isPython = file.filename.endsWith('.py');
    const hits: { line: number; evidence: string }[] = [];
    file.content.split('\n').forEach((raw, idx) => {
        const code = isPython ? stripPythonComment(raw) : raw;
        if (pattern.test(code)) {
            hits.push({ line: idx + 1, evidence: raw.trim() });
        }
    });
    return hits;
}

/** Row-at-a-time Python UDF declarations (`@udf`, `udf(`, `F.udf(`). Excludes `pandas_udf`. */
const PYTHON_UDF_PATTERN = /(?:^\s*@(?:F\.|functions\.)?udf\b(?!_))|(?:(?<![\w.])(?:F\.|functions\.)?udf\s*\()|(?:\b(?:F|functions)\.udf\s*\()/;
/** Driver collection calls banned in pipeline code. */
const DRIVER_COLLECT_PATTERN = /\.(collect|toPandas)\s*\(\s*\)/;
/** RDD and SparkContext APIs that fail on Spark Connect and break Catalyst pushdown. */
const RDD_PATTERN = /\.(rdd|sparkContext)\b/;
/** I/O calls that make `transform(dfs, config)` or `src/transforms.py` impure. */
const TRANSFORM_IO_PATTERN = /\bspark\.(read|readStream)\b|\bspark\.table\s*\(|\.(write|writeStream|writeTo)\b|\.(saveAsTable|insertInto)\s*\(/;
/** Cache calls. */
const CACHE_PATTERN = /\.(cache\s*\(\s*\)|persist\s*\()/g;
/** Unpersist calls. */
const UNPERSIST_PATTERN = /\.unpersist\s*\(/g;
/** Delta writes (append or overwrite) that need `txnVersion` inside `foreachBatch`. */
const DELTA_WRITE_PATTERN = /format\(\s*['"]delta['"]\s*\)/;
/** Event-time operations that need a watermark on a stream. */
const EVENT_TIME_STATEFUL_PATTERN = /\b(?:F\.|functions\.)?(window|session_window)\s*\(|\.dropDuplicatesWithinWatermark\s*\(/;
/** Scala 2.11 or 2.12 Maven coordinates or JAR file names. */
const SCALA_212_PATTERN = /[\w.-]+_2\.1[12][:-][\w.-]+/;
/** Spark 3.x connector artifacts. */
const SPARK3_ARTIFACT_PATTERN = /org\.apache\.spark:spark-[\w-]+_2\.1\d:3\.|iceberg-spark-runtime-3\.\d|hudi-spark3(?:\.\d)?-bundle|spark-3\.\d-bigquery|elasticsearch-spark-3\d|delta-core_2\.1\d/;
/** Classpath configuration inside Python code. */
const PYTHON_JAR_CONFIG_PATTERN = /['"]spark\.jars(?:\.packages)?['"]/;

/**
 * Counts all matches of a global pattern across files.
 *
 * @param files - Files to scan.
 * @param pattern - Global pattern.
 * @returns Number of matches outside Python comments.
 */
function countMatches(files: GeneratedFileCandidate[], pattern: RegExp): number {
    let total = 0;
    for (const file of files) {
        for (const raw of file.content.split('\n')) {
            const code = stripPythonComment(raw);
            total += (code.match(pattern) ?? []).length;
        }
    }
    return total;
}

/**
 * Collects deterministic facts about the generated repository for the LLM prompt.
 *
 * @param files - All generated files.
 * @param paradigm - Optional explicit paradigm override.
 * @returns Static signals.
 */
export function collectSparkArchitectureSignals(
    files: GeneratedFileCandidate[],
    paradigm?: PipelineParadigm
): SparkArchitectureSignals {
    const sources = getPipelineSourceFiles(files);
    const main = selectMainPipelineFile(files);
    const anyLine = (pattern: RegExp): boolean => sources.some((f) => findLines(f, pattern).length > 0);
    const resolvedParadigm = resolveSparkArchitectureParadigm(files, undefined, paradigm);

    const base: SparkArchitectureSignals = {
        pipelineFile: main?.filename,
        hasReadSources: !!main && extractPythonFunctionBody(main.content, 'read_sources') !== null,
        hasTransform: !!main && extractPythonFunctionBody(main.content, 'transform') !== null,
        hasWriteSinks: !!main && extractPythonFunctionBody(main.content, 'write_sinks') !== null,
        isStreaming: anyLine(/\.readStream\b/),
        usesWatermark: anyLine(/\.withWatermark\s*\(/),
        usesForeachBatch: anyLine(/\.foreachBatch\s*\(/),
        cacheCalls: countMatches(sources, CACHE_PATTERN),
        unpersistCalls: countMatches(sources, UNPERSIST_PATTERN),
        pythonUdfCount: sources.reduce((n, f) => n + findLines(f, PYTHON_UDF_PATTERN).length, 0),
    };

    if (resolvedParadigm === 'sdp') {
        const specFile = files.find((f) => {
            const norm = f.filename.replace(/\\/g, '/').replace(/^\.\//, '');
            return norm === 'spark-pipeline.yml' || norm === 'spark-pipeline.yaml';
        });
        const transformModules = files.filter((f) => {
            const norm = f.filename.replace(/\\/g, '/').replace(/^\.\//, '');
            return /^transformations\/.+\.py$/.test(norm) && !norm.endsWith('/__init__.py');
        });
        const pureHelper = files.find(
            (f) => f.filename.replace(/\\/g, '/').replace(/^\.\//, '') === 'src/transforms.py'
        );
        return {
            ...base,
            paradigm: 'sdp',
            sdpSpecFile: specFile?.filename,
            sdpTransformationFiles: transformModules.length,
            hasPureTransformsModule:
                !!pureHelper && /^\s*def\s+[A-Za-z_][A-Za-z0-9_]*\s*\(/m.test(pureHelper.content),
        };
    }

    return base;
}

/**
 * Creates a static finding with a stable id.
 *
 * @param rule - Rule suffix for the id.
 * @param index - 1-indexed occurrence number.
 * @param fields - Finding fields.
 * @returns The finding.
 */
function staticFinding(
    rule: string,
    index: number,
    fields: Omit<EvaluatorFinding, 'id'>
): EvaluatorFinding {
    return { id: `SPARK-ARCH-${rule}-${index}`, ...fields };
}

/**
 * Runs deterministic Catalyst and Managed Spark classpath checks shared by both paradigms.
 *
 * @param files - All generated files.
 * @param sources - Non-test Python source files.
 * @returns Shared Catalyst and classpath findings.
 */
function runSharedCatalystAndClasspathChecks(
    files: GeneratedFileCandidate[],
    sources: GeneratedFileCandidate[]
): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];

    let collectIdx = 0;
    let rddIdx = 0;
    let udfIdx = 0;
    for (const file of sources) {
        for (const hit of findLines(file, DRIVER_COLLECT_PATTERN)) {
            findings.push(staticFinding('DRIVER-COLLECT', ++collectIdx, {
                category: 'IDIOMATIC_SPARK',
                severity: 'MAJOR',
                file: file.filename,
                line: hit.line,
                evidence: hit.evidence,
                description: 'Pipeline code collects a DataFrame to the driver.',
                rationale: 'Driver collection on large data causes driver OOM. Keep data in lazy Catalyst DataFrame operations. Only tests may collect small fixtures.',
            }));
        }
        for (const hit of findLines(file, RDD_PATTERN)) {
            findings.push(staticFinding('RDD-API', ++rddIdx, {
                category: 'IDIOMATIC_SPARK',
                severity: 'MAJOR',
                file: file.filename,
                line: hit.line,
                evidence: hit.evidence,
                description: 'Pipeline code uses the RDD or SparkContext API.',
                rationale: 'RDD and SparkContext APIs break Catalyst pushdown and fail on Spark Connect and Serverless Spark 4.x.',
            }));
        }
        for (const hit of findLines(file, PYTHON_UDF_PATTERN)) {
            findings.push(staticFinding('PYTHON-UDF', ++udfIdx, {
                category: 'IDIOMATIC_SPARK',
                severity: 'MINOR',
                file: file.filename,
                line: hit.line,
                evidence: hit.evidence,
                description: 'Pipeline code declares a row-at-a-time Python UDF.',
                rationale: 'Python UDFs block Catalyst optimization and pickle every row. Use built-in pyspark.sql.functions, or a vectorized pandas_udf when no built-in exists.',
            }));
        }
    }

    let jarIdx = 0;
    for (const file of files) {
        if (file.filename.toLowerCase().endsWith('.md')) {
            continue;
        }
        for (const hit of findLines(file, SCALA_212_PATTERN)) {
            findings.push(staticFinding('CLASSPATH', ++jarIdx, {
                category: 'ARCHITECTURE',
                severity: 'CRITICAL',
                file: file.filename,
                line: hit.line,
                evidence: hit.evidence,
                description: 'Repository references a Scala 2.11 or 2.12 artifact.',
                rationale: 'Spark 4.x runs on Scala 2.13. A Scala 2.12 JAR fails at class load time on every runtime.',
            }));
        }
        for (const hit of findLines(file, SPARK3_ARTIFACT_PATTERN)) {
            findings.push(staticFinding('CLASSPATH', ++jarIdx, {
                category: 'ARCHITECTURE',
                severity: 'CRITICAL',
                file: file.filename,
                line: hit.line,
                evidence: hit.evidence,
                description: 'Repository references a Spark 3.x connector artifact.',
                rationale: 'Spark 3.x connectors are binary-incompatible with Spark 4.x. Use the Spark 4.0 / Scala 2.13 artifact from the version matrix.',
            }));
        }
        const base = file.filename.replace(/\\/g, '/').split('/').pop() ?? file.filename;
        if (MANAGED_SPARK_RUNNER_SCRIPTS.includes(base)) {
            for (const connector of MANAGED_SPARK_PREINSTALLED_CONNECTORS) {
                const pattern = new RegExp(`(packages|jars).*${connector.replace(/[-.]/g, '\\$&')}`, 'i');
                for (const hit of findLines(file, pattern)) {
                    findings.push(staticFinding('CLASSPATH', ++jarIdx, {
                        category: 'ARCHITECTURE',
                        severity: 'MAJOR',
                        file: file.filename,
                        line: hit.line,
                        evidence: hit.evidence,
                        description: `Managed Spark runner adds pre-installed connector \`${connector}\` to the classpath.`,
                        rationale: 'Dataproc and Serverless Spark pre-install this connector. A duplicate JAR causes classpath conflicts.',
                    }));
                }
            }
        }
    }
    for (const file of sources) {
        for (const hit of findLines(file, PYTHON_JAR_CONFIG_PATTERN)) {
            findings.push(staticFinding('CLASSPATH', ++jarIdx, {
                category: 'ARCHITECTURE',
                severity: 'MAJOR',
                file: file.filename,
                line: hit.line,
                evidence: hit.evidence,
                description: 'Pipeline code sets spark.jars or spark.jars.packages.',
                rationale: 'Runner scripts deliver connector JARs with --jars, --packages, or a spark.jars.packages property. Hardcoded coordinates in pipeline code break on Managed Spark.',
            }));
        }
    }

    return findings;
}

/**
 * Runs deterministic Spark Declarative Pipelines (SDP) architecture checks.
 *
 * @param files - All generated files.
 * @param yamlContent - Optional input `.spark` YAML to verify `SdpExecutionPlan` coverage.
 * @returns High-precision SDP architecture findings.
 */
export function runSdpArchitectureStaticChecks(
    files: GeneratedFileCandidate[],
    yamlContent?: string
): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];
    const sources = getPipelineSourceFiles(files);

    // 1. Validate spark-pipeline.yml schema and static-setting ban.
    const specFiles = files.filter((f) => {
        const norm = f.filename.replace(/\\/g, '/').replace(/^\.\//, '');
        return norm === 'spark-pipeline.yml' || norm === 'spark-pipeline.yaml';
    });
    if (specFiles.length > 1) {
        findings.push(staticFinding('SDP-SPEC', 1, {
            category: 'ARCHITECTURE',
            severity: 'CRITICAL',
            file: specFiles[1].filename,
            description: 'MULTIPLE_PIPELINE_SPEC_FILES_FOUND: Project contains both spark-pipeline.yml and spark-pipeline.yaml.',
            rationale: 'Keep exactly one SDP spec file at the repository root.',
        }));
    } else if (specFiles.length === 1) {
        const specViolations = validateSdpSpecYaml(specFiles[0].filename, specFiles[0].content);
        specViolations.forEach((v, idx) => {
            findings.push(staticFinding('SDP-SPEC', idx + 1, {
                category: 'ARCHITECTURE',
                severity: 'CRITICAL',
                file: v.file,
                evidence: v.evidence,
                description: v.message,
                rationale: 'spark-pipeline.yml must declare only allowed keys, string configuration values, and zero static Spark settings.',
            }));
        });
    }

    // 2. Scan transformation and helper Python modules for SDP purity and Databricks API bans.
    const pyViolations = scanSdpPythonViolations(files);
    pyViolations.forEach((v, idx) => {
        findings.push(staticFinding('SDP-PURITY', idx + 1, {
            category: 'ARCHITECTURE',
            severity: 'CRITICAL',
            file: v.file,
            line: v.line,
            evidence: v.evidence,
            description: v.message,
            rationale: 'SDP dataset functions must return lazy DataFrames without mutating session state, executing actions, or calling Databricks-only APIs.',
        }));
    });

    // 3. Verify src/transforms.py remains pure and performs no external I/O or @dp registration.
    const helperFile = files.find(
        (f) => f.filename.replace(/\\/g, '/').replace(/^\.\//, '') === 'src/transforms.py'
    );
    if (helperFile) {
        let helperIoIdx = 0;
        for (const hit of findLines(helperFile, TRANSFORM_IO_PATTERN)) {
            findings.push(staticFinding('SDP-HELPER-IO', ++helperIoIdx, {
                category: 'ARCHITECTURE',
                severity: 'MAJOR',
                file: helperFile.filename,
                line: hit.line,
                evidence: hit.evidence,
                description: 'src/transforms.py performs Spark table or storage I/O.',
                rationale: 'src/transforms.py must hold pure DataFrame helper functions so tests/test_transforms.py runs offline with in-memory DataFrames.',
            }));
        }
    }

    // 4. Check @dp.materialized_view vs @dp.table query-type alignment inside transformations/*.py.
    const transformationFiles = files.filter((f) => {
        const norm = f.filename.replace(/\\/g, '/').replace(/^\.\//, '');
        return /^transformations\/.+\.py$/.test(norm) && !norm.endsWith('/__init__.py');
    });
    let flowTypeIdx = 0;
    for (const tfFile of transformationFiles) {
        const cleanedLines = stripPythonCommentsAndDocstrings(tfFile.content);
        for (let i = 0; i < cleanedLines.length; i++) {
            const line = cleanedLines[i];
            const isMvDecorator = /@\s*dp\s*\.\s*materialized_view\b/.test(line);
            const isTableDecorator = /@\s*dp\s*\.\s*table\b/.test(line);
            if (!isMvDecorator && !isTableDecorator) {
                continue;
            }
            // Find the decorated def line and inspect its body.
            let defName: string | undefined;
            for (let j = i + 1; j < Math.min(i + 15, cleanedLines.length); j++) {
                const defMatch = /^\s*def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(/.exec(cleanedLines[j]);
                if (defMatch) {
                    defName = defMatch[1];
                    break;
                }
            }
            if (!defName) {
                continue;
            }
            const body = extractPythonFunctionBody(cleanedLines.join('\n'), defName);
            if (!body) {
                continue;
            }
            const bodyText = body.lines.map((l) => l.text).join('\n');
            if (isMvDecorator && /\.readStream\b/.test(bodyText)) {
                findings.push(staticFinding('SDP-QUERY-TYPE', ++flowTypeIdx, {
                    category: 'ARCHITECTURE',
                    severity: 'CRITICAL',
                    file: tfFile.filename,
                    line: body.defLine,
                    evidence: `def ${defName}(...)`,
                    description: `INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW: Materialized view '${defName}' reads a stream via readStream.`,
                    rationale: 'Materialized views in SDP must return batch DataFrames read via spark.read.',
                }));
            } else if (isTableDecorator && /\bspark\s*\.\s*read\b(?!Stream)/.test(bodyText) && !/\.readStream\b/.test(bodyText)) {
                findings.push(staticFinding('SDP-QUERY-TYPE', ++flowTypeIdx, {
                    category: 'ARCHITECTURE',
                    severity: 'CRITICAL',
                    file: tfFile.filename,
                    line: body.defLine,
                    evidence: `def ${defName}(...)`,
                    description: `INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE: Streaming table '${defName}' reads a batch relation via spark.read without readStream.`,
                    rationale: 'Streaming tables (@dp.table) in SDP must return streaming DataFrames read via spark.readStream.',
                }));
            }
        }
    }

    // 5. Check SdpExecutionPlan compatibility when pipeline YAML is available.
    const sdpPlan = tryBuildSdpPlanFromYaml(yamlContent);
    if (sdpPlan && !sdpPlan.isCompatible) {
        sdpPlan.blockingIssues.forEach((issue, idx) => {
            findings.push(staticFinding('SDP-PLAN', idx + 1, {
                category: 'ARCHITECTURE',
                severity: 'CRITICAL',
                file: 'pipeline.spark',
                description: `${issue.code}: ${issue.message}`,
                rationale: 'Pipeline graph violates an Apache Spark 4.2 Spark Declarative Pipelines compatibility ceiling.',
            }));
        });
    }

    // 6. Run shared Catalyst and classpath checks.
    findings.push(...runSharedCatalystAndClasspathChecks(files, sources));

    return findings;
}

/**
 * Runs the deterministic Spark architecture checks for Imperative or SDP repositories.
 *
 * @param files - All generated files.
 * @param options - Optional paradigm override and pipeline YAML context.
 * @returns High-precision findings.
 */
export function runSparkArchitectureStaticChecks(
    files: GeneratedFileCandidate[],
    options?: SparkArchitectureCheckOptions
): EvaluatorFinding[] {
    const paradigm = resolveSparkArchitectureParadigm(files, options?.yamlContent, options?.paradigm);
    if (paradigm === 'sdp') {
        return runSdpArchitectureStaticChecks(files, options?.yamlContent);
    }

    const findings: EvaluatorFinding[] = [];
    const sources = getPipelineSourceFiles(files);
    const main = selectMainPipelineFile(files);

    // 1. and 2. D3 separation and pure transform.
    if (main) {
        const missing = ['read_sources', 'transform', 'write_sinks'].filter(
            (fn) => extractPythonFunctionBody(main.content, fn) === null
        );
        if (missing.length > 0) {
            findings.push(staticFinding('SEPARATION', 1, {
                category: 'ARCHITECTURE',
                severity: 'MAJOR',
                file: main.filename,
                description: `Pipeline file does not define ${missing.map((m) => `\`${m}\``).join(', ')}.`,
                rationale: 'Decision D3 requires read_sources(spark, config), pure transform(dfs, config), and write_sinks(dfs, config) so tests verify transformations offline.',
            }));
        }
        const transformBody = extractPythonFunctionBody(main.content, 'transform');
        if (transformBody) {
            if (!/^\s*dfs\b/.test(transformBody.params) && transformBody.params !== '') {
                findings.push(staticFinding('TRANSFORM-SIGNATURE', 1, {
                    category: 'ARCHITECTURE',
                    severity: 'MAJOR',
                    file: main.filename,
                    line: transformBody.defLine,
                    evidence: `def transform(${transformBody.params})`,
                    description: 'transform() does not take the DataFrame dictionary `dfs` as its first parameter.',
                    rationale: 'Decision D3 fixes the signature transform(dfs, config) so tests pass in-memory DataFrames.',
                }));
            }
            const ioLines = transformBody.lines.filter((l) => TRANSFORM_IO_PATTERN.test(stripPythonComment(l.text)));
            ioLines.forEach((l, idx) => findings.push(staticFinding('TRANSFORM-IO', idx + 1, {
                category: 'ARCHITECTURE',
                severity: 'MAJOR',
                file: main.filename,
                line: l.line,
                evidence: l.text.trim(),
                description: 'transform() performs source or sink I/O.',
                rationale: 'transform(dfs, config) must be pure. Reads belong in read_sources() and writes belong in write_sinks().',
            })));
        }
    }

    // 3. to 5. and 9. to 10. Shared Catalyst and Managed Spark classpath rules.
    const sharedFindings = runSharedCatalystAndClasspathChecks(files, sources);
    const catalystFindings = sharedFindings.filter((f) => f.category === 'IDIOMATIC_SPARK');
    const classpathFindings = sharedFindings.filter((f) => f.category !== 'IDIOMATIC_SPARK');
    findings.push(...catalystFindings);

    // 6. Cache and unpersist pairing.
    const signals = collectSparkArchitectureSignals(files, 'imperative');
    if (signals.cacheCalls > 0 && signals.unpersistCalls === 0) {
        const first = sources.map((f) => ({ f, hits: findLines(f, /\.(cache\s*\(\s*\)|persist\s*\()/) })).find((x) => x.hits.length > 0);
        findings.push(staticFinding('CACHE-UNPERSIST', 1, {
            category: 'ARCHITECTURE',
            severity: 'MAJOR',
            file: first?.f.filename ?? signals.pipelineFile ?? 'src/pipeline.py',
            line: first?.hits[0].line,
            evidence: first?.hits[0].evidence,
            description: `Pipeline caches ${signals.cacheCalls} DataFrame(s) but never calls .unpersist().`,
            rationale: 'Cached DataFrames hold executor memory until the session ends. Call .unpersist() in write_sinks() after the last consumer sink writes.',
        }));
    }

    // 7. foreachBatch idempotency.
    let txnIdx = 0;
    for (const file of sources) {
        if (findLines(file, /\.foreachBatch\s*\(/).length === 0 || findLines(file, DELTA_WRITE_PATTERN).length === 0) {
            continue;
        }
        const hasVersion = findLines(file, /['"]txnVersion['"]/).length > 0;
        const hasAppId = findLines(file, /['"]txnAppId['"]/).length > 0;
        if (!hasVersion || !hasAppId) {
            const missing = [!hasVersion ? 'txnVersion' : '', !hasAppId ? 'txnAppId' : ''].filter(Boolean).join(' and ');
            findings.push(staticFinding('FOREACHBATCH-TXN', ++txnIdx, {
                category: 'ARCHITECTURE',
                severity: 'MAJOR',
                file: file.filename,
                line: findLines(file, /\.foreachBatch\s*\(/)[0].line,
                description: `foreachBatch writes Delta without ${missing}.`,
                rationale: 'A retried micro-batch writes duplicate rows. Set .option("txnVersion", str(batch_id)) and .option("txnAppId", txn_app_id) on each Delta write.',
            }));
        }
        for (const hit of findLines(file, /['"]txnVersion['"]\s*,\s*(?:['"][^'"]*['"]|\d)/)) {
            findings.push(staticFinding('FOREACHBATCH-TXN', ++txnIdx, {
                category: 'ARCHITECTURE',
                severity: 'MAJOR',
                file: file.filename,
                line: hit.line,
                evidence: hit.evidence,
                description: 'txnVersion is a constant instead of the micro-batch id.',
                rationale: 'A constant txnVersion makes Delta skip every batch after the first. Bind txnVersion to str(batch_id).',
            }));
        }
    }

    // 8. Watermark presence for streaming event-time state.
    if (signals.isStreaming && !signals.usesWatermark) {
        let wmIdx = 0;
        for (const file of sources) {
            const hit = findLines(file, EVENT_TIME_STATEFUL_PATTERN)[0];
            if (hit) {
                findings.push(staticFinding('WATERMARK', ++wmIdx, {
                    category: 'ARCHITECTURE',
                    severity: 'MAJOR',
                    file: file.filename,
                    line: hit.line,
                    evidence: hit.evidence,
                    description: 'Streaming event-time window or deduplication runs without withWatermark().',
                    rationale: 'Decision D8 requires .withWatermark(event_time_col, delay) before stateful operations. Without it, state grows without bound and append output never finalizes.',
                }));
            }
        }
    }

    findings.push(...classpathFindings);
    return findings;
}

/**
 * Builds the Spark architecture LLM prompt for Imperative PySpark or Spark Declarative Pipelines.
 *
 * @param language - Target language label.
 * @param yamlContent - Original pipeline YAML.
 * @param files - All generated files.
 * @param staticFindings - Findings from the static layer.
 * @param signals - Static signals.
 * @param sparkVersion - Target Apache Spark version.
 * @param paradigm - Optional paradigm override (`imperative` or `sdp`).
 * @returns Prompt text.
 */
export function buildSparkArchitecturePrompt(
    language: string,
    yamlContent: string,
    files: GeneratedFileCandidate[],
    staticFindings: EvaluatorFinding[],
    signals: SparkArchitectureSignals,
    sparkVersion: string = DEFAULT_SPARK_VERSION,
    paradigm?: PipelineParadigm
): string {
    const resolvedParadigm = resolveSparkArchitectureParadigm(
        files,
        yamlContent,
        paradigm ?? signals.paradigm
    );

    if (resolvedParadigm === 'sdp') {
        const sdpPlan = tryBuildSdpPlanFromYaml(yamlContent);
        const sdpPlanSummary = sdpPlan
            ? JSON.stringify(
                  {
                      isCompatible: sdpPlan.isCompatible,
                      datasets: sdpPlan.datasets,
                      warnings: sdpPlan.warnings,
                  },
                  null,
                  2
              )
            : 'not available';

        return `You are the Principal Spark Declarative Pipelines (SDP) Architecture Evaluator Agent.
Your responsibility is auditing a generated Apache Spark 4.2+ Spark Declarative Pipelines (\`pyspark.pipelines\` / \`spark-pipeline.yml\`) repository for SDP graph correctness, dataset-function purity, and Catalyst optimization defects.

=== TARGET LANGUAGE & PARADIGM ===
${language} (Apache Spark 4.2+ Spark Declarative Pipelines)
==================================

=== INPUT PIPELINE YAML ===
${yamlContent}
===========================

=== DETERMINISTIC SDP EXECUTION PLAN ===
${sdpPlanSummary}
========================================

=== GENERATED SOURCE FILES & ARTIFACTS (LINE NUMBERED) ===
${formatFilesWithLineNumbers(files)}
==========================================================

=== STATIC ANALYSIS SIGNALS (DETERMINISTIC) ===
${JSON.stringify(signals, null, 2)}

=== STATIC FINDINGS ALREADY RECORDED (DO NOT REPEAT) ===
${JSON.stringify(staticFindings.map((f) => ({ id: f.id, file: f.file, line: f.line, description: f.description })), null, 2)}
========================================================

Audit the code against the following Spark Declarative Pipelines Architecture Checklist:
1. **Dataset Type & Flow Semantics:**
   - Streaming tables (\`@dp.table\`, \`dp.create_streaming_table\` + \`@dp.append_flow\`) read streaming inputs via \`spark.readStream\`.
   - Materialized views (\`@dp.materialized_view\`) read batch inputs via \`spark.read\` and never use \`readStream\`.
   - Auto CDC SCD Type 1 flows declare \`dp.create_streaming_table\` first and register \`dp.create_auto_cdc_flow(..., stored_as_scd_type=1)\` with unqualified column names.
   - External sinks use \`dp.create_sink(name, format, options)\` paired with \`@dp.append_flow(target=name)\`.
2. **Dataset Function Purity & Testable Helpers:**
   - \`transformations/*.py\` binds \`spark = SparkSession.active()\` (or uses injected \`spark\`) and never calls \`SparkSession.builder\`, \`spark.conf.set\`, \`createOrReplaceTempView\`, \`spark.udf.register\`, writers (\`.write\`, \`.writeStream\`, \`.writeTo\`, \`saveAsTable\`), eager actions (\`.collect()\`, \`.count()\`, \`.show()\`, \`.toPandas()\`), or \`.schema\` / \`.columns\`.
   - \`src/transforms.py\` isolates pure DataFrame transformation helpers tested offline in \`tests/test_transforms.py\`.
3. **OSS Apache Spark 4.2 API Fidelity:**
   - Zero Databricks DLT APIs (\`dlt\`, \`@dp.view\`, \`@dp.expect*\`, \`dp.apply_changes\`, \`append_flow(once=...)\`, \`stored_as_scd_type=2\`, \`format("cloudFiles")\`).
4. **Spec & Catalog Rules (\`spark-pipeline.yml\`):**
   - Only allowed top-level keys (\`name\`, \`storage\`, \`catalog\`, \`database\`, \`schema\`, \`configuration\`, \`libraries\`).
   - \`configuration\` values are strings and contain zero static Spark keys (\`spark.jars*\`, \`spark.sql.extensions\`, \`spark.driver.*\`, \`spark.executor.*\`). Static properties belong in \`sdp_stage.pipeline.yaml\` \`execution_config.properties\` or \`run_sdp_dataproc.sh\` \`--conf\`.
   - File streams specify an explicit \`.schema(...)\`. Streaming aggregations, deduplication, and stream-stream joins apply \`.withWatermark(...)\`.
5. **Catalyst Optimization Rules:**
   - Built-in \`pyspark.sql.functions\` over row-at-a-time Python UDFs, early filter/projection pushdown, and single-pass \`.withColumns({...})\`.

Instructions:
- Report only NEW defects that the static findings above do not cover.
- Cite the exact 1-indexed line number and verbatim evidence from the line-numbered listing.
- Use category \`ARCHITECTURE\` for SDP dataset, spec, purity, streaming, and classpath defects. Use \`IDIOMATIC_SPARK\` for Catalyst optimization defects.
- Severity: CRITICAL for runtime failure or data loss or duplication, MAJOR for a correctness risk or performance anti-pattern, MINOR for style.
- Set \`passed\` to false if any CRITICAL or MAJOR defect exists.`;
    }

    return `You are the Principal Spark Architecture Evaluator Agent.
Your responsibility is auditing a generated Apache Spark ${sparkVersion} (PySpark 4.x DataFrame, Catalyst SQL, and Structured Streaming) repository for architecture and Catalyst optimization defects.

=== TARGET LANGUAGE ===
${language} (PySpark)
=======================

=== INPUT PIPELINE YAML ===
${yamlContent}
===========================

=== GENERATED SOURCE FILES & ARTIFACTS (LINE NUMBERED) ===
${formatFilesWithLineNumbers(files)}
==========================================================

=== STATIC ANALYSIS SIGNALS (DETERMINISTIC) ===
${JSON.stringify(signals, null, 2)}

=== STATIC FINDINGS ALREADY RECORDED (DO NOT REPEAT) ===
${JSON.stringify(staticFindings.map((f) => ({ id: f.id, file: f.file, line: f.line, description: f.description })), null, 2)}
========================================================

Audit the code against the following Spark Architecture Checklist:
1. **Catalyst Optimization Rules:**
   - Use built-in \`pyspark.sql.functions\` instead of Python \`@udf\` whenever a built-in exists. Use \`pandas_udf\` / \`mapInPandas\` only for logic with no built-in.
   - Write filters as column expressions that Catalyst can push down to the source (no Python UDF or \`.rdd\` filters). Filter and project early.
   - No \`.collect()\`, \`.toPandas()\`, \`.take()\`, or eager \`.count()\` on large data in pipeline code. No terminal actions inside loops.
   - Use \`F.broadcast\` only for small dimension tables. Use \`withColumns\` for multi-column projections.
2. **read_sources / transform / write_sinks Separation (Decision D3):**
   - \`src/pipeline.py\` defines \`read_sources(spark, config)\`, a pure \`transform(dfs, config)\`, and \`write_sinks(dfs, config)\`.
   - \`transform\` performs no reads, writes, \`.start()\`, or session creation.
3. **Cache Discipline:**
   - Cache only DataFrames with batch fan-out (\`ExecutionPlan.cachedNodeIds\`). Pair each \`.cache()\` / \`.persist()\` with an \`.unpersist()\` after the last consumer sink writes.
4. **foreachBatch Idempotency:**
   - Delta writes inside \`foreachBatch\` set \`.option("txnVersion", str(batch_id))\` and \`.option("txnAppId", ...)\`. Other sinks use deterministic upsert keys.
   - Group multiple streaming sinks into one \`foreachBatch\` with one checkpoint where the ExecutionPlan says so.
5. **Watermark Placement (Decision D8):**
   - Apply \`.withWatermark(event_time_col, delay)\` right after the event-time column exists and before every stateful operation (windowed aggregation, \`dropDuplicatesWithinWatermark\`, stream-stream join, \`transformWithState\`).
   - Stream-stream outer joins need watermarks on both sides and an event-time range condition.
6. **Managed Spark Classpath Rules:**
   - No Spark 3.x or Scala 2.12 JARs. Spark 4.x needs Scala 2.13 artifacts.
   - Runner scripts deliver connector JARs with \`--jars\`, \`--packages\`, or \`spark.jars.packages\`. Pipeline code never hardcodes Maven coordinates.
   - Never add pre-installed connectors (\`spark-bigquery\`, \`iceberg-spark-runtime\`, \`gcs-connector\`) on Dataproc or Serverless Spark.

Instructions:
- Report only NEW defects that the static findings above do not cover.
- Cite the exact 1-indexed line number and verbatim evidence from the line-numbered listing.
- Use category \`ARCHITECTURE\` for structure, caching, streaming, and classpath defects. Use \`IDIOMATIC_SPARK\` for Catalyst optimization defects.
- Severity: CRITICAL for runtime failure or data loss or duplication, MAJOR for a correctness risk or a significant performance anti-pattern, MINOR for style.
- Set \`passed\` to false if any CRITICAL or MAJOR defect exists.`;
}

/** JSON response schema of the Spark architecture agent. */
export const SPARK_ARCHITECTURE_RESPONSE_SCHEMA = {
    type: 'object',
    properties: {
        passed: { type: 'boolean', description: 'True if no CRITICAL or MAJOR Spark architecture defect exists' },
        summary: { type: 'string', description: 'Spark architecture assessment summary' },
        findings: {
            type: 'array',
            items: {
                type: 'object',
                properties: {
                    id: { type: 'string' },
                    category: { type: 'string', enum: [...SPARK_ARCHITECTURE_CATEGORIES] },
                    severity: { type: 'string', enum: ['CRITICAL', 'MAJOR', 'MINOR'] },
                    file: { type: 'string' },
                    line: { type: 'integer' },
                    evidence: { type: 'string' },
                    description: { type: 'string' },
                    rationale: { type: 'string' },
                },
                required: ['id', 'category', 'severity', 'file', 'description', 'rationale'],
            },
        },
    },
    required: ['passed', 'summary', 'findings'],
};

/**
 * Parses and normalizes the LLM response.
 *
 * Normalization rules:
 * - Unknown categories become `ARCHITECTURE`. Unknown severities become `MAJOR`.
 * - Findings without a description are dropped.
 * - Missing or duplicate ids get a unique `SPARK-ARCH-LLM-<n>` id.
 *
 * @param text - Raw response text.
 * @returns Normalized response.
 * @throws Error when the text is empty, is not JSON, or is not a JSON object.
 */
export function parseSparkArchitectureResponse(text: string | undefined | null): SparkArchitectureResponse {
    if (!text || !text.trim()) {
        throw new Error(`Empty response from ${SPARK_ARCHITECTURE_AGENT_NAME}`);
    }
    let parsed: unknown;
    try {
        parsed = JSON.parse(text);
    } catch (err) {
        throw new Error(`Invalid JSON from ${SPARK_ARCHITECTURE_AGENT_NAME}: ${(err as Error).message}`);
    }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        throw new Error(`Invalid response shape from ${SPARK_ARCHITECTURE_AGENT_NAME}: expected a JSON object`);
    }
    const obj = parsed as Record<string, unknown>;
    const rawFindings = Array.isArray(obj.findings) ? obj.findings : [];
    const seen = new Set<string>();
    const findings: EvaluatorFinding[] = [];
    rawFindings.forEach((raw, idx) => {
        if (!raw || typeof raw !== 'object') {
            return;
        }
        const f = raw as Record<string, unknown>;
        const description = typeof f.description === 'string' ? f.description.trim() : '';
        if (!description) {
            return;
        }
        let id = typeof f.id === 'string' && f.id.trim() ? f.id.trim() : `SPARK-ARCH-LLM-${idx + 1}`;
        if (seen.has(id)) {
            id = `${id}-${idx + 1}`;
        }
        seen.add(id);
        const category: SparkArchitectureCategory = (SPARK_ARCHITECTURE_CATEGORIES as readonly string[]).includes(f.category as string)
            ? (f.category as SparkArchitectureCategory)
            : 'ARCHITECTURE';
        const severity: FindingSeverity = ['CRITICAL', 'MAJOR', 'MINOR'].includes(f.severity as string)
            ? (f.severity as FindingSeverity)
            : 'MAJOR';
        const finding: EvaluatorFinding = {
            id,
            category,
            severity,
            file: typeof f.file === 'string' && f.file ? f.file : 'unknown',
            description,
            rationale: typeof f.rationale === 'string' ? f.rationale : '',
        };
        if (typeof f.line === 'number' && Number.isInteger(f.line) && f.line > 0) {
            finding.line = f.line;
        }
        if (typeof f.evidence === 'string' && f.evidence) {
            finding.evidence = f.evidence;
        }
        findings.push(finding);
    });
    return {
        passed: obj.passed === true,
        summary: typeof obj.summary === 'string' ? obj.summary : '',
        findings,
    };
}

/**
 * Merges static and LLM findings. Drops LLM findings whose id repeats a static id.
 *
 * @param staticFindings - Static findings.
 * @param llmFindings - LLM findings.
 * @returns Merged findings, static first.
 */
export function mergeArchitectureFindings(
    staticFindings: EvaluatorFinding[],
    llmFindings: EvaluatorFinding[]
): EvaluatorFinding[] {
    const ids = new Set(staticFindings.map((f) => f.id));
    return [...staticFindings, ...llmFindings.filter((f) => !ids.has(f.id))];
}

/**
 * Returns true when findings contain a CRITICAL or MAJOR defect.
 *
 * @param findings - Findings to check.
 * @returns True when a blocking finding exists.
 */
function hasBlockingFinding(findings: EvaluatorFinding[]): boolean {
    return findings.some((f) => f.severity === 'CRITICAL' || f.severity === 'MAJOR');
}

/**
 * Runs the Spark Architecture Evaluator sub-agent (`SparkArchitectureAgent`).
 *
 * @param ai - Initialized GoogleGenAI client.
 * @param language - Target language label.
 * @param yamlContent - Original pipeline YAML.
 * @param files - All generated files.
 * @param options - Optional paradigm override.
 * @param options.paradigm - Explicit paradigm override.
 * @returns Specialist evaluation result.
 */
export async function evaluateSparkArchitecture(
    ai: GoogleGenAI,
    language: 'Java' | 'Python',
    yamlContent: string,
    files: GeneratedFileCandidate[],
    options?: { paradigm?: PipelineParadigm }
): Promise<SpecialistAgentResult> {
    const paradigm = resolveSparkArchitectureParadigm(files, yamlContent, options?.paradigm);
    const staticFindings = runSparkArchitectureStaticChecks(files, { paradigm, yamlContent });
    const signals = collectSparkArchitectureSignals(files, paradigm);
    const sparkVersion = paradigm === 'sdp' ? '4.2' : DEFAULT_SPARK_VERSION;
    const prompt = buildSparkArchitecturePrompt(
        language,
        yamlContent,
        files,
        staticFindings,
        signals,
        sparkVersion,
        paradigm
    );

    try {
        const response = await generateContentWithRetry(ai, {
            model: DEFAULT_GEMINI_MODEL,
            contents: prompt,
            config: {
                responseMimeType: 'application/json',
                responseSchema: SPARK_ARCHITECTURE_RESPONSE_SCHEMA,
                temperature: 0.1,
            },
        });
        const parsed = parseSparkArchitectureResponse(response?.text);
        const findings = mergeArchitectureFindings(staticFindings, parsed.findings);
        return {
            agentName: SPARK_ARCHITECTURE_AGENT_NAME,
            passed: parsed.passed && !hasBlockingFinding(findings),
            summary: parsed.summary,
            findings,
        };
    } catch (err) {
        // NOTE: Keep the real static findings. Do not invent a finding for the infrastructure error.
        return {
            agentName: SPARK_ARCHITECTURE_AGENT_NAME,
            passed: false,
            summary: `Subagent infrastructure/API error: ${(err as Error).message}. Static checks only.`,
            findings: staticFindings,
        };
    }
}
