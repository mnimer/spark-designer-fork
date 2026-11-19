/**
 * Packaging, Dependency, and Deployability Evaluator Sub-Agent.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Audits the packaging of a generated PySpark 4.x repository: `requirements.txt`, the
 * `Dockerfile`, and the `run_local.sh`, `run_dataproc_cluster.sh`, and `run_serverless_spark.sh`
 * runner scripts, together with the connector artifacts that the ExecutionPlan requires.
 *
 * Architectural decisions:
 * - Runs deterministic audits before the LLM call:
 *   - `requirements.txt`: a `pyspark` pin on the local Spark minor line, `pytest`, every ExecutionPlan pip package,
 *     and no leftover Apache Beam dependency.
 *   - Runner scripts: the correct gcloud or spark-submit command, `--region`, `--cluster`,
 *     `--version` with a valid Serverless Spark runtime, `--jars`, `--packages` or
 *     `spark.jars.packages`, `--properties`, `set -euo pipefail`, and no Beam runner flags.
 *   - Connector artifacts: ExecutionPlan JARs, Maven packages, and Spark properties reach the
 *     target runner script. Pre-installed Managed Spark connectors are never passed via packages.
 *     Scala 2.12 or Spark 3.x connector builds are rejected.
 * - Merges deterministic findings with the LLM findings. Any deterministic CRITICAL or MAJOR
 *   finding fails the agent.
 */

import { GoogleGenAI } from '@google/genai';
import { DEFAULT_GEMINI_MODEL } from '../../../constants';
import { DEFAULT_SPARK_VERSION } from '../../../shared/types';
import { RUNTIME_PROFILES, type PipelineRuntimeArtifacts, type SparkRunnerType } from '../../../spark/versionMatrix';
import { EvaluatorFinding, SpecialistAgentResult, generateContentWithRetry } from './types';
import type { GeneratedFileCandidate } from './deterministic-gate';
import { formatFilesWithLineNumbers } from './format-utils';
import { buildSpecExecutionPlan, normalizeRepoPath } from './pipeline-spec-evaluator';

/**
 * Runner script filename for each runtime runner type.
 */
export const RUNNER_SCRIPT_BY_TYPE: Readonly<Record<SparkRunnerType, string>> = {
    Local: 'run_local.sh',
    DataprocCluster: 'run_dataproc_cluster.sh',
    ServerlessSpark: 'run_serverless_spark.sh'
};

/**
 * Valid Serverless Spark runtime versions that ship Apache Spark 4.x.
 */
export const SERVERLESS_SPARK4_RUNTIME_VERSIONS: readonly string[] = Object.values(RUNTIME_PROFILES)
    .filter(p => p.runner === 'ServerlessSpark')
    .map(p => p.runtimeVersion);

/**
 * Normalizes a Python distribution name per PEP 503 (lowercase, runs of `-_.` become `-`).
 *
 * @param name - Raw distribution name.
 * @returns Normalized name.
 */
export function normalizePipName(name: string): string {
    return name.trim().toLowerCase().replace(/[-_.]+/g, '-');
}

/**
 * One parsed requirement line.
 */
export interface ParsedRequirement {
    /** Normalized distribution name without extras. */
    name: string;
    /** Version specifier text, for example `>=4.0.0,<5`. Empty if unpinned. */
    specifier: string;
    /** 1-indexed line number in `requirements.txt`. */
    line: number;
    /** Raw line text. */
    raw: string;
}

/**
 * Parses `requirements.txt` content into requirement entries.
 * Comments, blank lines, and pip options (`-r`, `--index-url`) are skipped.
 *
 * @param content - Raw `requirements.txt` content.
 * @returns Parsed requirements.
 */
export function parseRequirements(content: string): ParsedRequirement[] {
    const result: ParsedRequirement[] = [];
    content.split('\n').forEach((rawLine, idx) => {
        const line = rawLine.replace(/\s+#.*$/, '').trim();
        if (!line || line.startsWith('#') || line.startsWith('-')) {
            return;
        }
        const withoutMarker = line.split(';')[0].trim();
        const match = /^([A-Za-z0-9][A-Za-z0-9._-]*)(\[[^\]]*\])?\s*(.*)$/.exec(withoutMarker);
        if (!match) {
            return;
        }
        result.push({
            name: normalizePipName(match[1]),
            specifier: match[3].replace(/\s+/g, ''),
            line: idx + 1,
            raw: rawLine.trim()
        });
    });
    return result;
}

/**
 * Decides whether a `pyspark` version specifier pins the Spark 4.x line.
 *
 * Logic flow:
 * - `==4.*` / `===4.*` / `~=4.*`: accepted.
 * - A lower bound (`>=` or `>`) must be 4.x. An upper bound (`<` or `<=`) must not be at or below 4.0.
 * - An empty or unrecognized specifier is unpinned.
 *
 * @param specifier - Version specifier text without spaces.
 * @returns 'ok', 'unpinned', or a reason string for a wrong major version.
 */
export function classifyPysparkSpecifier(specifier: string): 'ok' | 'unpinned' | string {
    if (!specifier) {
        return 'unpinned';
    }
    const clauses = specifier.split(',').filter(c => c.length > 0);
    let hasLowerOrExact = false;
    for (const clause of clauses) {
        const m = /^(===|==|~=|>=|<=|!=|>|<)\s*(\d+)(?:\.([\d*]+))?/.exec(clause);
        if (!m) {
            // NOTE: Direct URL references and other forms do not pin a version range.
            return 'unpinned';
        }
        const [, op, majorText, minorText] = m;
        const major = Number(majorText);
        const minor = minorText && minorText !== '*' ? Number(minorText) : 0;
        if (op === '==' || op === '===' || op === '~=' || op === '>=' || op === '>') {
            hasLowerOrExact = true;
            if (major !== 4) {
                return `pins major version ${major} via '${clause}'`;
            }
        } else if (op === '<' && (major < 4 || (major === 4 && minor === 0))) {
            return `upper bound '${clause}' excludes Spark 4.x`;
        } else if (op === '<=' && major < 4) {
            return `upper bound '${clause}' excludes Spark 4.x`;
        }
    }
    return hasLowerOrExact ? 'ok' : 'unpinned';
}

/**
 * Splits a PEP 440 release string into numeric parts. A trailing `.*` is dropped.
 *
 * @param version - Release text such as `4.0.1` or `4.0.*`.
 * @returns Numeric release parts.
 */
function releaseParts(version: string): number[] {
    return version.replace(/\.\*$/, '').split('.').map(part => Number.parseInt(part, 10)).filter(n => !Number.isNaN(n));
}

/**
 * Decides whether a `pyspark` specifier keeps pip on one Spark minor line.
 *
 * Logic flow:
 * - The local Maven connector coordinates target one Spark minor (for example 4.0).
 * - If the specifier lets pip install a newer minor, local `--packages` no longer match.
 * - `==4.0.*`, `==4.0.1`, `~=4.0.1`, `<4.1`, and `<=4.0.2` cap the line. `>=4.0.0` and `<5` do not.
 *
 * @param specifier - Version specifier text without spaces.
 * @param minorLine - Spark minor line, for example `4.0`.
 * @returns True if every allowed version is on the minor line.
 */
export function pysparkPinnedToMinor(specifier: string, minorLine: string = DEFAULT_SPARK_VERSION): boolean {
    const [lineMajor, lineMinor] = releaseParts(minorLine);
    const onLine = (parts: number[]): boolean => parts[0] === lineMajor && (parts[1] ?? 0) === lineMinor;
    let capped = false;
    for (const clause of specifier.split(',').filter(c => c.length > 0)) {
        const m = /^(===|==|~=|>=|<=|!=|>|<)([\d.*]+)$/.exec(clause);
        if (!m) {
            return false;
        }
        const [, op, version] = m;
        const parts = releaseParts(version);
        if (op === '==' || op === '===') {
            if (!onLine(parts) || parts.length < 2) {
                return false;
            }
            capped = true;
        } else if (op === '~=') {
            if (!onLine(parts) || parts.length < 3) {
                return false;
            }
            capped = true;
        } else if (op === '>=' || op === '>') {
            if (parts[0] !== lineMajor || (parts[1] ?? 0) > lineMinor) {
                return false;
            }
        } else if (op === '<') {
            const [major, minor = 0, patch = 0] = parts;
            if (major === lineMajor && ((minor === lineMinor + 1 && patch === 0) || minor === lineMinor)) {
                capped = true;
            }
        } else if (op === '<=' && onLine(parts)) {
            capped = true;
        }
    }
    return capped;
}

/**
 * Audits `requirements.txt` for the PySpark 4.x repository contract.
 *
 * @param files - All generated files.
 * @param pipPackages - Optional ExecutionPlan pip packages that must be declared.
 * @returns Packaging findings.
 */
export function auditRequirementsTxt(files: GeneratedFileCandidate[], pipPackages: readonly string[] = []): EvaluatorFinding[] {
    const reqFile = files.find(f => normalizeRepoPath(f.filename) === 'requirements.txt');
    if (!reqFile) {
        return [{
            id: 'PKG-REQ-MISSING',
            category: 'PACKAGING',
            severity: 'CRITICAL',
            file: 'requirements.txt',
            description: 'requirements.txt is missing',
            rationale: 'The Dockerfile, run_local.sh, and offline tests install dependencies from requirements.txt.'
        }];
    }

    const findings: EvaluatorFinding[] = [];
    const file = reqFile.filename;
    const reqs = parseRequirements(reqFile.content);
    const byName = new Map<string, ParsedRequirement>();
    for (const r of reqs) {
        if (!byName.has(r.name)) byName.set(r.name, r);
    }

    const pyspark = byName.get('pyspark');
    if (!pyspark) {
        findings.push({
            id: 'PKG-REQ-PYSPARK-MISSING',
            category: 'PACKAGING',
            severity: 'CRITICAL',
            file,
            description: 'requirements.txt does not declare pyspark',
            rationale: `Local runs and tests need pyspark ${DEFAULT_SPARK_VERSION}.x installed from requirements.txt.`
        });
    } else {
        const verdict = classifyPysparkSpecifier(pyspark.specifier);
        if (verdict === 'unpinned') {
            findings.push({
                id: 'PKG-REQ-PYSPARK-UNPINNED',
                category: 'PACKAGING',
                severity: 'MAJOR',
                file,
                line: pyspark.line,
                evidence: pyspark.raw,
                description: 'pyspark is not pinned to the 4.x line',
                rationale: `Pin pyspark to the Spark ${DEFAULT_SPARK_VERSION} line (for example pyspark==${DEFAULT_SPARK_VERSION}.*) so local runs match the local Maven connector coordinates.`
            });
        } else if (verdict === 'ok' && !pysparkPinnedToMinor(pyspark.specifier)) {
            findings.push({
                id: 'PKG-REQ-PYSPARK-MINOR',
                category: 'PACKAGING',
                severity: 'MAJOR',
                file,
                line: pyspark.line,
                evidence: pyspark.raw,
                description: `pyspark is not pinned to the Spark ${DEFAULT_SPARK_VERSION} minor line`,
                rationale: `Pin pyspark==${DEFAULT_SPARK_VERSION}.* so pip does not install a newer Spark minor. The local Maven connector coordinates target Spark ${DEFAULT_SPARK_VERSION}.`
            });
        } else if (verdict !== 'ok') {
            findings.push({
                id: 'PKG-REQ-PYSPARK-VERSION',
                category: 'PACKAGING',
                severity: 'CRITICAL',
                file,
                line: pyspark.line,
                evidence: pyspark.raw,
                description: `pyspark requirement ${verdict}`,
                rationale: 'The generated code targets Spark 4.x APIs. A Spark 3.x or 5.x pin breaks local runs and tests.'
            });
        }
    }

    if (!byName.has('pytest')) {
        findings.push({
            id: 'PKG-REQ-PYTEST-MISSING',
            category: 'PACKAGING',
            severity: 'MAJOR',
            file,
            description: 'requirements.txt does not declare pytest',
            rationale: 'tests/test_pipeline.py runs with pytest (pytest>=8.0.0).'
        });
    }

    const beam = reqs.find(r => r.name === 'apache-beam');
    if (beam) {
        findings.push({
            id: 'PKG-REQ-BEAM-LEFTOVER',
            category: 'PACKAGING',
            severity: 'CRITICAL',
            file,
            line: beam.line,
            evidence: beam.raw,
            description: 'requirements.txt declares apache-beam',
            rationale: 'The Spark designer generates PySpark repositories. An Apache Beam dependency is a Beam-era leftover.'
        });
    }

    for (const pkg of pipPackages) {
        const parsedPkg = parseRequirements(pkg)[0];
        if (parsedPkg && !byName.has(parsedPkg.name)) {
            findings.push({
                id: `PKG-REQ-PIP-${parsedPkg.name}`,
                category: 'PACKAGING',
                severity: 'MAJOR',
                file,
                description: `requirements.txt does not declare ExecutionPlan pip package '${pkg}'`,
                rationale: 'Every package in ExecutionPlan.runtimeArtifacts.pipPackages must be installed for the connector code to import.'
            });
        }
    }

    return findings;
}

/**
 * One required token check for a runner script.
 */
interface RunnerTokenRule {
    id: string;
    regex: RegExp;
    severity: EvaluatorFinding['severity'];
    description: string;
    rationale: string;
}

const RUNNER_RULES: Readonly<Record<string, readonly RunnerTokenRule[]>> = {
    'run_local.sh': [
        { id: 'SUBMIT', regex: /\bspark-submit\b|python3?\s+-m\s+src\.pipeline/, severity: 'CRITICAL', description: 'does not launch the pipeline with spark-submit or python3 -m src.pipeline', rationale: 'run_local.sh must start src/pipeline.py on a local PySpark 4.x session.' },
        { id: 'JARS', regex: /--jars\b/, severity: 'MAJOR', description: 'does not forward connector JARs with --jars', rationale: 'Local runs need custom connector JARs on the classpath.' },
        { id: 'PACKAGES', regex: /--packages\b/, severity: 'MAJOR', description: 'does not forward Maven connector packages with --packages', rationale: 'Local runs resolve connectors such as spark-bigquery from Maven with --packages.' },
        { id: 'VENV', regex: /-m\s+venv\b/, severity: 'MAJOR', description: 'does not bootstrap a Python virtual environment', rationale: 'run_local.sh must create .venv and install requirements.txt so local runs need no manual pip steps.' },
        { id: 'JDK', regex: /\bJAVA_HOME\b/, severity: 'MAJOR', description: 'does not select a JDK 17 or 21 through JAVA_HOME', rationale: 'PySpark 4.0 fails on JDK 25. run_local.sh must select or install JDK 17 or 21.' },
        { id: 'IVY', regex: /spark\.jars\.ivySettings\b/, severity: 'MAJOR', description: 'does not pass a Maven Central spark.jars.ivySettings file', rationale: 'POM-only ~/.m2 entries break the default Ivy chain. A Central-only Ivy settings file resolves --packages reliably.' }
    ],
    'run_dataproc_cluster.sh': [
        { id: 'SUBMIT', regex: /dataproc\s+jobs\s+submit\s+pyspark\b/, severity: 'CRITICAL', description: 'does not call gcloud dataproc jobs submit pyspark', rationale: 'Dataproc Cluster submission uses gcloud dataproc jobs submit pyspark.' },
        { id: 'CLUSTER', regex: /--cluster\b/, severity: 'CRITICAL', description: 'does not pass --cluster', rationale: 'gcloud dataproc jobs submit requires the target cluster name.' },
        { id: 'REGION', regex: /--region\b/, severity: 'MAJOR', description: 'does not pass --region', rationale: 'Dataproc jobs are regional. Pass --region from the REGION variable.' },
        { id: 'JARS', regex: /--jars\b/, severity: 'MAJOR', description: 'does not forward connector JARs with --jars', rationale: 'Connector JARs such as Spanner or Bigtable must reach the driver and executors.' },
        { id: 'PROPERTIES', regex: /--properties\b/, severity: 'MAJOR', description: 'does not forward Spark properties with --properties', rationale: 'Spark and Dataproc properties, including spark.jars.packages, travel through --properties.' }
    ],
    'run_serverless_spark.sh': [
        { id: 'SUBMIT', regex: /dataproc\s+batches\s+submit\s+pyspark\b/, severity: 'CRITICAL', description: 'does not call gcloud dataproc batches submit pyspark', rationale: 'Serverless Spark submission uses gcloud dataproc batches submit pyspark.' },
        { id: 'REGION', regex: /--region\b/, severity: 'MAJOR', description: 'does not pass --region', rationale: 'Serverless Spark batches are regional. Pass --region from the REGION variable.' },
        { id: 'VERSION', regex: /--version\b/, severity: 'MAJOR', description: 'does not pass the runtime --version', rationale: `Pin a Spark 4.x Serverless runtime (${SERVERLESS_SPARK4_RUNTIME_VERSIONS.join(' or ')}) with --version.` },
        { id: 'JARS', regex: /--jars\b/, severity: 'MAJOR', description: 'does not forward connector JARs with --jars', rationale: 'Connector JARs such as Spanner or Bigtable must reach the batch classpath.' },
        { id: 'PROPERTIES', regex: /--properties\b/, severity: 'MAJOR', description: 'does not forward Spark properties with --properties', rationale: 'Serverless Spark sets the tier, spark.jars.packages, and other properties through --properties.' }
    ]
};

/**
 * Removes full-line shell comments so that comment text does not satisfy token checks.
 *
 * @param content - Shell script content.
 * @returns Content with comment lines blanked. The shebang line is kept.
 */
function stripShellComments(content: string): string {
    return content
        .split('\n')
        .map((line, idx) => (idx > 0 && line.trimStart().startsWith('#') ? '' : line))
        .join('\n');
}

/**
 * Extracts literal Serverless Spark runtime versions from a runner script.
 * Matches `--version=4.0`, `--version 4.0`, and `RUNTIME_VERSION="${RUNTIME_VERSION:-4.0}"` defaults.
 *
 * @param content - Shell script content without comments.
 * @returns Literal version strings found.
 */
export function extractServerlessRuntimeVersions(content: string): string[] {
    const versions: string[] = [];
    const patterns = [
        /--version(?:=|\s+)["']?(\d+\.\d+)\b/g,
        /RUNTIME_VERSION:-(\d+\.\d+)\b/g,
        /^\s*RUNTIME_VERSION=["']?(\d+\.\d+)\b/gm
    ];
    for (const re of patterns) {
        let m: RegExpExecArray | null;
        while ((m = re.exec(content)) !== null) {
            versions.push(m[1]);
        }
    }
    return Array.from(new Set(versions));
}

/**
 * Audits the three runner scripts for required gcloud and spark-submit flags.
 *
 * @param files - All generated files.
 * @returns Packaging findings.
 */
export function auditRunnerScripts(files: GeneratedFileCandidate[]): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];

    for (const [scriptName, rules] of Object.entries(RUNNER_RULES)) {
        const script = files.find(f => normalizeRepoPath(f.filename) === scriptName);
        if (!script) {
            findings.push({
                id: `PKG-RUNNER-MISSING-${scriptName}`,
                category: 'PACKAGING',
                severity: 'CRITICAL',
                file: scriptName,
                description: `${scriptName} is missing`,
                rationale: 'Decision D3 requires run_local.sh, run_dataproc_cluster.sh, and run_serverless_spark.sh.'
            });
            continue;
        }
        const code = stripShellComments(script.content);

        for (const rule of rules) {
            if (!rule.regex.test(code)) {
                findings.push({
                    id: `PKG-RUNNER-${rule.id}-${scriptName}`,
                    category: 'PACKAGING',
                    severity: rule.severity,
                    file: script.filename,
                    description: `${scriptName} ${rule.description}`,
                    rationale: rule.rationale
                });
            }
        }

        if (!/\bset\s+-eu(?:o\s+|\s+-o\s+)pipefail\b/.test(code)) {
            findings.push({
                id: `PKG-RUNNER-STRICT-${scriptName}`,
                category: 'PACKAGING',
                severity: 'MINOR',
                file: script.filename,
                description: `${scriptName} does not enable set -euo pipefail`,
                rationale: 'Strict mode stops the runner on the first failed command or unset variable.'
            });
        }

        if (scriptName !== 'run_local.sh' && /src\/pipeline\.py/.test(code) === false) {
            findings.push({
                id: `PKG-RUNNER-ENTRYPOINT-${scriptName}`,
                category: 'PACKAGING',
                severity: 'CRITICAL',
                file: script.filename,
                description: `${scriptName} does not submit src/pipeline.py`,
                rationale: 'The managed runners must submit the generated src/pipeline.py entry point.'
            });
        }

        const beamMatch = /DataflowRunner|DirectRunner|--runner\b|apache_beam/.exec(code);
        if (beamMatch) {
            findings.push({
                id: `PKG-RUNNER-BEAM-${scriptName}`,
                category: 'PACKAGING',
                severity: 'CRITICAL',
                file: script.filename,
                evidence: beamMatch[0],
                description: `${scriptName} contains the Beam-era runner token '${beamMatch[0]}'`,
                rationale: 'Spark runner scripts must not reference Apache Beam runners or flags.'
            });
        }

        const hardcodedRegion = /--region(?:=|\s+)["']?([a-z]+-[a-z]+\d)\b/.exec(code);
        if (hardcodedRegion) {
            findings.push({
                id: `PKG-RUNNER-REGION-HARDCODED-${scriptName}`,
                category: 'PACKAGING',
                severity: 'MINOR',
                file: script.filename,
                evidence: hardcodedRegion[0],
                description: `${scriptName} hardcodes --region ${hardcodedRegion[1]}`,
                rationale: 'Read the region from the REGION variable so the Deployment tab setting applies.'
            });
        }

        if (scriptName === 'run_serverless_spark.sh') {
            for (const version of extractServerlessRuntimeVersions(code)) {
                if (!SERVERLESS_SPARK4_RUNTIME_VERSIONS.includes(version)) {
                    findings.push({
                        id: `PKG-RUNNER-RUNTIME-VERSION-${version}`,
                        category: 'PACKAGING',
                        severity: 'CRITICAL',
                        file: script.filename,
                        evidence: version,
                        description: `run_serverless_spark.sh uses Serverless runtime version ${version}, which does not ship Spark 4.x`,
                        rationale: `Valid Spark 4.x Serverless runtimes are ${SERVERLESS_SPARK4_RUNTIME_VERSIONS.join(' and ')}.`
                    });
                }
            }
        }
    }

    return findings;
}

/**
 * Audits ExecutionPlan connector artifacts against the target runner script and all runner scripts.
 *
 * Logic flow:
 * - Resolve the target runner script from `runtimeArtifacts.profile.runner`.
 * - Every GCS JAR, Maven package, and Spark property key must appear in the target script.
 * - Cluster and serverless scripts must not pass pre-installed Managed Spark connectors via packages.
 * - No runner script or Python file may reference Scala 2.12 or Spark 3.x connector builds.
 *
 * @param files - All generated files.
 * @param artifacts - ExecutionPlan runtime artifacts.
 * @returns Packaging findings.
 */
export function auditConnectorArtifacts(files: GeneratedFileCandidate[], artifacts: PipelineRuntimeArtifacts): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];
    const targetScriptName = RUNNER_SCRIPT_BY_TYPE[artifacts.profile.runner];
    const targetScript = files.find(f => normalizeRepoPath(f.filename) === targetScriptName);

    if (targetScript) {
        const code = targetScript.content;
        const required: { kind: string; value: string; needle: string }[] = [
            ...artifacts.gcsJars.map(j => ({ kind: 'connector JAR', value: j, needle: j })),
            ...artifacts.mavenPackages.map(p => ({ kind: 'Maven package', value: p, needle: p })),
            ...Object.keys(artifacts.sparkProperties).map(k => ({ kind: 'Spark property', value: k, needle: k }))
        ];
        for (const item of required) {
            if (!code.includes(item.needle)) {
                findings.push({
                    id: `PKG-ARTIFACT-${item.value}`,
                    category: 'PACKAGING',
                    severity: 'MAJOR',
                    file: targetScript.filename,
                    description: `${targetScriptName} does not supply ExecutionPlan ${item.kind} '${item.value}'`,
                    rationale: `Runtime profile ${artifacts.profile.id} needs every ExecutionPlan runtime artifact on the classpath or in the Spark configuration.`
                });
            }
        }
    }

    for (const scriptName of ['run_dataproc_cluster.sh', 'run_serverless_spark.sh']) {
        const script = files.find(f => normalizeRepoPath(f.filename) === scriptName);
        if (!script) continue;
        const lines = script.content.split('\n');
        lines.forEach((line, idx) => {
            if (line.trimStart().startsWith('#') || !/--packages\b|spark\.jars\.packages|SPARK_PACKAGES/.test(line)) {
                return;
            }
            for (const connector of ['spark-bigquery', 'iceberg-spark-runtime', 'gcs-connector']) {
                if (line.includes(connector)) {
                    findings.push({
                        id: `PKG-PREINSTALLED-${connector}-${scriptName}`,
                        category: 'PACKAGING',
                        severity: 'MAJOR',
                        file: script.filename,
                        line: idx + 1,
                        evidence: line.trim().substring(0, 160),
                        description: `${scriptName} passes the pre-installed Managed Spark connector '${connector}' via packages`,
                        rationale: 'Dataproc and Serverless Spark already ship this connector. A second copy causes classpath conflicts.'
                    });
                }
            }
        });
    }

    for (const file of files) {
        const name = normalizeRepoPath(file.filename);
        if (!name.endsWith('.sh') && !name.endsWith('.py') && name !== 'Dockerfile') continue;
        const lines = file.content.split('\n');
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i];
            if (line.trimStart().startsWith('#')) continue;
            const legacy = /[\w.-]+_2\.12\b[\w.:-]*|(?:spark|runtime)-3\.\d+[\w.:-]*/.exec(line);
            if (legacy) {
                findings.push({
                    id: `PKG-LEGACY-CLASSPATH-${file.filename}`,
                    category: 'PACKAGING',
                    severity: 'CRITICAL',
                    file: file.filename,
                    line: i + 1,
                    evidence: legacy[0],
                    description: `Scala 2.12 or Spark 3.x connector artifact '${legacy[0]}' detected`,
                    rationale: 'Spark 4.x runtimes use Scala 2.13. Spark 3.x or Scala 2.12 builds fail with binary incompatibility errors.'
                });
                break;
            }
        }
    }

    return findings;
}

/**
 * Runs every deterministic packaging audit.
 *
 * @param files - All generated files.
 * @param artifacts - Optional ExecutionPlan runtime artifacts.
 * @returns Packaging findings.
 */
export function runDeterministicPackagingAudit(
    files: GeneratedFileCandidate[],
    artifacts?: PipelineRuntimeArtifacts | null
): EvaluatorFinding[] {
    return [
        ...auditRequirementsTxt(files, artifacts?.pipPackages ?? []),
        ...auditRunnerScripts(files),
        ...(artifacts ? auditConnectorArtifacts(files, artifacts) : [])
    ];
}

/**
 * Runs the Packaging and Deployability Evaluator sub-agent.
 *
 * @param ai - Initialized GoogleGenAI client.
 * @param language - Target language. The Spark designer generates PySpark only.
 * @param files - All generated files.
 * @param yamlContent - Optional `.spark` pipeline YAML. If present, ExecutionPlan artifacts are audited.
 * @returns Specialist evaluation result.
 */
export async function evaluatePackagingAndDeployability(
    ai: GoogleGenAI,
    language: 'Java' | 'Python',
    files: GeneratedFileCandidate[],
    yamlContent?: string
): Promise<SpecialistAgentResult> {
    const sparkVersion = DEFAULT_SPARK_VERSION;
    const plan = yamlContent ? buildSpecExecutionPlan(yamlContent) : null;
    const artifacts = plan?.runtimeArtifacts ?? null;
    const deterministicFindings = runDeterministicPackagingAudit(files, artifacts);

    const artifactText = artifacts
        ? [
            `Runtime profile: ${artifacts.profile.id} (runner ${artifacts.profile.runner}, Spark ${artifacts.profile.sparkVersion}, Scala ${artifacts.profile.scalaVersion})`,
            `GCS JARs: ${artifacts.gcsJars.join(', ') || 'none'}`,
            `Maven packages: ${artifacts.mavenPackages.join(', ') || 'none'}`,
            `Pip packages: ${artifacts.pipPackages.join(', ') || 'none'}`,
            `Pre-installed connectors (never pass via packages): ${artifacts.preinstalledConnectors.join(', ') || 'none'}`,
            `Spark properties: ${Object.entries(artifacts.sparkProperties).map(([k, v]) => `${k}=${v}`).join(', ') || 'none'}`
        ].join('\n')
        : 'not available';

    const prompt = `You are the Principal Packaging & Runtime Deployability Evaluator Agent.
Your responsibility is auditing a generated Apache Spark 4.x PySpark repository for deployment readiness on Local PySpark, Dataproc Clusters, and Serverless Spark.

=== TARGET LANGUAGE ===
${language} (PySpark 4.x)
=======================

=== AUTHORITATIVE APACHE SPARK RELEASE GROUNDING ===
The current target Apache Spark runtime version is ${sparkVersion} (PySpark 4.x, Scala 2.13).
Because your training cutoff may predate Apache Spark ${sparkVersion}, DO NOT flag Apache Spark version ${sparkVersion} as "non-existent", "unreleased", or "hallucinated".
Valid Serverless Spark 4.x runtime versions: ${SERVERLESS_SPARK4_RUNTIME_VERSIONS.join(', ')}.
==================================================

=== EXECUTIONPLAN RUNTIME ARTIFACTS ===
${artifactText}
=======================================

=== DETERMINISTIC PRE-AUDIT FINDINGS (already confirmed, do not repeat) ===
${deterministicFindings.length > 0 ? deterministicFindings.map(f => `- [${f.severity}] ${f.id}: ${f.description}`).join('\n') : 'none'}
==========================================================================

=== GENERATED SOURCE FILES & ARTIFACTS (LINE NUMBERED) ===
${formatFilesWithLineNumbers(files)}
==========================================================

Audit the code against the following Packaging & Deployment Checklist:
1. **requirements.txt:**
   - Is \`pyspark\` pinned to the Spark ${DEFAULT_SPARK_VERSION} minor line (for example \`pyspark==${DEFAULT_SPARK_VERSION}.*\`) and \`pytest\` declared?
   - Are all third-party imported libraries and ExecutionPlan pip packages included with realistic version constraints?
2. **Runner Scripts Integrity:**
   - Does \`run_local.sh\` run spark-submit (or python3 -m src.pipeline) and a pytest mode, forwarding \`--jars\` and \`--packages\`, after it bootstraps \`.venv\` from \`requirements.txt\`, selects a JDK 17 or 21, and passes a Maven Central \`spark.jars.ivySettings\` file?
   - Does \`run_dataproc_cluster.sh\` call \`gcloud dataproc jobs submit pyspark src/pipeline.py\` with \`--cluster\`, \`--region\`, \`--jars\`, and \`--properties\`?
   - Does \`run_serverless_spark.sh\` call \`gcloud dataproc batches submit pyspark src/pipeline.py\` with \`--region\`, \`--version\`, \`--deps-bucket\`, \`--jars\`, and \`--properties\`?
   - Are connector JARs and Maven packages from the ExecutionPlan supplied, with no pre-installed connector passed via packages?
   - Do scripts use \`set -euo pipefail\` and quote every variable expansion?
3. **Dockerfile & Container Alignment:**
   - Does the Dockerfile run as the non-root UID/GID 1099 user required by Serverless Spark?
   - Does it install requirements.txt and copy src/ into the WORKDIR that the runners expect?

Severity Guidelines:
- CRITICAL: Wrong pyspark major version, runner script submitting a non-existent entry point, missing gcloud submit command, Scala 2.12 or Spark 3.x connector builds.
- MAJOR: Missing --region, --version, --jars, --properties, or ExecutionPlan connector artifacts.
- MINOR: Non-standard formatting or missing comments.`;

    const schema = {
        type: "object",
        properties: {
            passed: {
                type: "boolean",
                description: "True if packaging and dependencies are valid and deployable"
            },
            summary: {
                type: "string",
                description: "Packaging assessment summary"
            },
            findings: {
                type: "array",
                items: {
                    type: "object",
                    properties: {
                        id: { type: "string" },
                        category: { type: "string", enum: ["PACKAGING", "SYNTAX"] },
                        severity: { type: "string", enum: ["CRITICAL", "MAJOR", "MINOR"] },
                        file: { type: "string" },
                        line: { type: "integer" },
                        evidence: { type: "string" },
                        description: { type: "string" },
                        rationale: { type: "string" }
                    },
                    required: ["id", "category", "severity", "file", "description", "rationale"]
                }
            }
        },
        required: ["passed", "summary", "findings"]
    };

    const response = await generateContentWithRetry(ai, {
        model: DEFAULT_GEMINI_MODEL,
        contents: prompt,
        config: {
            responseMimeType: 'application/json',
            responseSchema: schema,
            temperature: 0.1
        }
    });

    if (!response.text) {
        throw new Error('Empty response from PackagingAndDeployabilityAgent');
    }

    const parsed = JSON.parse(response.text);
    const blocking = deterministicFindings.filter(f => f.severity === 'CRITICAL' || f.severity === 'MAJOR');
    const summary = deterministicFindings.length > 0
        ? `${parsed.summary} Deterministic audit: ${deterministicFindings.length} finding(s).`
        : parsed.summary;
    return {
        agentName: 'PackagingAndDeployabilityAgent',
        passed: Boolean(parsed.passed) && blocking.length === 0,
        summary,
        findings: [...deterministicFindings, ...(parsed.findings || [])]
    };
}
