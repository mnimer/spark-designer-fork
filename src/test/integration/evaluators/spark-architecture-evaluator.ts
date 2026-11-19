/**
 * Spark Architecture Evaluator Sub-Agent (`SparkArchitectureAgent`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Audits generated PySpark 4.x repositories for Spark architecture defects.
 * The agent checks Catalyst optimization rules, the `read_sources` / pure
 * `transform(dfs, config)` / `write_sinks` separation (decision D3), `.cache()` and
 * `.unpersist()` pairing, `foreachBatch` idempotency, watermark placement (decision D8),
 * and Managed Spark classpath rules.
 *
 * Architectural decisions:
 * - Two layers. A deterministic static layer runs first and emits high-precision findings.
 *   An LLM layer then audits the semantic rules that regular expressions cannot prove.
 * - The static layer, the prompt builder, and the response parser are pure functions.
 *   Unit tests cover them offline without a model client.
 * - If the LLM call fails, the agent keeps the static findings and marks the result as failed.
 *   It never invents a synthetic finding for an infrastructure error.
 * - Test files (`tests/`, `test_*.py`, `*_test.py`) are exempt from the driver-collection
 *   and UDF rules, because the skills allow `.collect()` on small test fixtures.
 */

import { GoogleGenAI } from '@google/genai';
import { DEFAULT_GEMINI_MODEL } from '../../../constants';
import { DEFAULT_SPARK_VERSION } from '../../../shared/types';
import { EvaluatorFinding, FindingSeverity, SpecialistAgentResult, generateContentWithRetry } from './types';
import { GeneratedFileCandidate } from './deterministic-gate';
import { formatFilesWithLineNumbers } from './format-utils';

/** Agent name used as the key in `MultiAgentEvaluationResult.specialistResults`. */
export const SPARK_ARCHITECTURE_AGENT_NAME = 'SparkArchitectureAgent';

/** Finding categories that this agent may emit. */
export const SPARK_ARCHITECTURE_CATEGORIES = ['ARCHITECTURE', 'IDIOMATIC_SPARK'] as const;

/** Category type that this agent may emit. */
export type SparkArchitectureCategory = (typeof SPARK_ARCHITECTURE_CATEGORIES)[number];

/** Runner scripts that submit to Managed Spark (Dataproc Cluster and Serverless Spark). */
const MANAGED_SPARK_RUNNER_SCRIPTS = ['run_dataproc_cluster.sh', 'run_serverless_spark.sh'];

/** Connectors that Managed Spark runtimes pre-install. A second copy causes classpath conflicts. */
const MANAGED_SPARK_PREINSTALLED_CONNECTORS = [
    'spark-bigquery',
    'iceberg-spark-runtime',
    'gcs-connector',
    'hadoop-aws',
    'hadoop-azure',
];

/** A body of one Python function extracted from source text. */
export interface PythonFunctionBody {
    /** 1-indexed line number of the `def` line. */
    defLine: number;
    /** Parameter text between the parentheses of the `def` line, when it is on one line. */
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
}

/** Parsed and normalized LLM response of the Spark architecture agent. */
export interface SparkArchitectureResponse {
    passed: boolean;
    summary: string;
    findings: EvaluatorFinding[];
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
        f => f.filename.endsWith('.py') && !f.filename.endsWith('__init__.py') && !isPythonTestFile(f.filename)
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
        sources.find(f => f.filename.replace(/\\/g, '/').endsWith('src/pipeline.py')) ??
        sources.find(f => /(^|\/)pipeline\.py$/.test(f.filename.replace(/\\/g, '/'))) ??
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
/** Driver collection calls that the Catalyst skill bans in pipeline code. */
const DRIVER_COLLECT_PATTERN = /\.(collect|toPandas)\s*\(\s*\)/;
/** RDD and SparkContext APIs that fail on Spark Connect and break Catalyst pushdown. */
const RDD_PATTERN = /\.(rdd|sparkContext)\b/;
/** I/O calls that make `transform(dfs, config)` impure. */
const TRANSFORM_IO_PATTERN = /\bspark\.(read|readStream)\b|\bspark\.table\s*\(|\.(write|writeStream)\b|\.(saveAsTable|insertInto|writeTo)\s*\(/;
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
 * @returns Static signals.
 */
export function collectSparkArchitectureSignals(files: GeneratedFileCandidate[]): SparkArchitectureSignals {
    const sources = getPipelineSourceFiles(files);
    const main = selectMainPipelineFile(files);
    const anyLine = (pattern: RegExp): boolean => sources.some(f => findLines(f, pattern).length > 0);
    return {
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
 * Runs the deterministic Spark architecture checks.
 *
 * Rules:
 * 1. D3 separation: the main file defines `read_sources`, `transform`, and `write_sinks`.
 * 2. Pure transform: `transform` performs no reads or writes.
 * 3. No driver collection (`.collect()`, `.toPandas()`) in pipeline code.
 * 4. No RDD or SparkContext APIs in pipeline code.
 * 5. Row-at-a-time Python UDFs are flagged for built-in replacement (MINOR).
 * 6. Every `.cache()` / `.persist()` needs an `.unpersist()`.
 * 7. Delta writes inside `foreachBatch` set `txnVersion` and `txnAppId`. `txnVersion` is not a constant.
 * 8. Streaming event-time windows and deduplication need `withWatermark`.
 * 9. No Scala 2.12 or Spark 3.x JARs. No duplicate pre-installed connectors on Managed Spark.
 * 10. No classpath configuration in Python code. Runner scripts deliver JARs.
 *
 * @param files - All generated files.
 * @returns High-precision findings.
 */
export function runSparkArchitectureStaticChecks(files: GeneratedFileCandidate[]): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];
    const sources = getPipelineSourceFiles(files);
    const main = selectMainPipelineFile(files);

    // 1. and 2. D3 separation and pure transform.
    if (main) {
        const missing = ['read_sources', 'transform', 'write_sinks'].filter(
            fn => extractPythonFunctionBody(main.content, fn) === null
        );
        if (missing.length > 0) {
            findings.push(staticFinding('SEPARATION', 1, {
                category: 'ARCHITECTURE',
                severity: 'MAJOR',
                file: main.filename,
                description: `Pipeline file does not define ${missing.map(m => `\`${m}\``).join(', ')}.`,
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
            const ioLines = transformBody.lines.filter(l => TRANSFORM_IO_PATTERN.test(stripPythonComment(l.text)));
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

    // 3. to 5. Catalyst rules on pipeline code.
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

    // 6. Cache and unpersist pairing.
    const signals = collectSparkArchitectureSignals(files);
    if (signals.cacheCalls > 0 && signals.unpersistCalls === 0) {
        const first = sources.map(f => ({ f, hits: findLines(f, /\.(cache\s*\(\s*\)|persist\s*\()/) })).find(x => x.hits.length > 0);
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

    // 9. and 10. Managed Spark classpath rules.
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
 * Builds the Spark architecture LLM prompt.
 *
 * @param language - Target language label.
 * @param yamlContent - Original pipeline YAML.
 * @param files - All generated files.
 * @param staticFindings - Findings from the static layer.
 * @param signals - Static signals.
 * @param sparkVersion - Target Apache Spark version.
 * @returns Prompt text.
 */
export function buildSparkArchitecturePrompt(
    language: string,
    yamlContent: string,
    files: GeneratedFileCandidate[],
    staticFindings: EvaluatorFinding[],
    signals: SparkArchitectureSignals,
    sparkVersion: string = DEFAULT_SPARK_VERSION
): string {
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
${JSON.stringify(staticFindings.map(f => ({ id: f.id, file: f.file, line: f.line, description: f.description })), null, 2)}
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
    const ids = new Set(staticFindings.map(f => f.id));
    return [...staticFindings, ...llmFindings.filter(f => !ids.has(f.id))];
}

/**
 * Returns true when findings contain a CRITICAL or MAJOR defect.
 *
 * @param findings - Findings to check.
 * @returns True when a blocking finding exists.
 */
function hasBlockingFinding(findings: EvaluatorFinding[]): boolean {
    return findings.some(f => f.severity === 'CRITICAL' || f.severity === 'MAJOR');
}

/**
 * Runs the Spark Architecture Evaluator sub-agent (`SparkArchitectureAgent`).
 *
 * @param ai - Initialized GoogleGenAI client.
 * @param language - Target language label.
 * @param yamlContent - Original pipeline YAML.
 * @param files - All generated files.
 * @returns Specialist evaluation result.
 */
export async function evaluateSparkArchitecture(
    ai: GoogleGenAI,
    language: 'Java' | 'Python',
    yamlContent: string,
    files: GeneratedFileCandidate[]
): Promise<SpecialistAgentResult> {
    const staticFindings = runSparkArchitectureStaticChecks(files);
    const signals = collectSparkArchitectureSignals(files);
    const prompt = buildSparkArchitecturePrompt(language, yamlContent, files, staticFindings, signals);

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
