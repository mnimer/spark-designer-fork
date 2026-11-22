/**
 * Packaging, Dependency, and Deployability Evaluator Sub-Agent.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Audits the packaging of generated PySpark 4.x and Spark Declarative Pipelines
 * (SDP, Apache Spark 4.2+) repositories:
 * - Imperative mode audits `requirements.txt` (`pyspark==4.0.*`), `Dockerfile`,
 *   `run_local.sh`, `run_dataproc_cluster.sh`, and `run_serverless_spark.sh`, together
 *   with `ExecutionPlan` connector artifacts.
 * - SDP mode audits `requirements.txt` (`pyspark` 4.2+ pin), `run_sdp_local.sh`
 *   (`spark-pipelines`), `run_sdp_dataproc.sh` (Dataproc 4.0 driver wrapper and
 *   `CREATE NAMESPACE IF NOT EXISTS`), and `sdp_stage.pipeline.yaml` (Managed Data
 *   Pipelines `sdp` stage keeping static Spark properties in `execution_config`).
 *
 * Architectural decisions:
 * - Runs deterministic audits before the LLM call and merges deterministic findings
 *   with LLM findings. Any deterministic CRITICAL or MAJOR finding fails the agent.
 */

import * as YAML from 'yaml';
import { GoogleGenAI } from '@google/genai';
import { isSdpGeneratedProject } from '../../../codegen/ReadOnlyCheckRunner';
import { DEFAULT_GEMINI_MODEL } from '../../../core/constants';
import { DEFAULT_SPARK_VERSION, type PipelineParadigm } from '../../../shared/types';
import { RUNTIME_PROFILES, type PipelineRuntimeArtifacts, type SparkRunnerType } from '../../../spark/versionMatrix';
import { EvaluatorFinding, SpecialistAgentResult, generateContentWithRetry } from './types';
import type { GeneratedFileCandidate } from './deterministic-gate';
import { formatFilesWithLineNumbers } from './format-utils';
import {
    buildSpecExecutionPlan,
    buildSpecSdpExecutionPlan,
    normalizeRepoPath,
    parsePipelineSpec,
} from './pipeline-spec-evaluator';

/**
 * Runner script filename for each imperative runtime runner type.
 */
export const RUNNER_SCRIPT_BY_TYPE: Readonly<Record<SparkRunnerType, string>> = {
    Local: 'run_local.sh',
    DataprocCluster: 'run_dataproc_cluster.sh',
    ServerlessSpark: 'run_serverless_spark.sh',
};

/**
 * Valid Serverless Spark runtime versions that ship Apache Spark 4.x.
 */
export const SERVERLESS_SPARK4_RUNTIME_VERSIONS: readonly string[] = Object.values(RUNTIME_PROFILES)
    .filter((p) => p.runner === 'ServerlessSpark')
    .map((p) => p.runtimeVersion);

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
            raw: rawLine.trim(),
        });
    });
    return result;
}

/**
 * Decides whether a `pyspark` version specifier pins the Spark 4.x line.
 *
 * @param specifier - Version specifier text without spaces.
 * @returns 'ok', 'unpinned', or a reason string for a wrong major version.
 */
export function classifyPysparkSpecifier(specifier: string): 'ok' | 'unpinned' | string {
    if (!specifier) {
        return 'unpinned';
    }
    const clauses = specifier.split(',').filter((c) => c.length > 0);
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
    return version
        .replace(/\.\*$/, '')
        .split('.')
        .map((part) => Number.parseInt(part, 10))
        .filter((n) => !Number.isNaN(n));
}

/**
 * Decides whether a `pyspark` specifier keeps pip on one Spark minor line.
 *
 * @param specifier - Version specifier text without spaces.
 * @param minorLine - Spark minor line, for example `4.0` or `4.2`.
 * @returns True if every allowed version is on the minor line.
 */
export function pysparkPinnedToMinor(specifier: string, minorLine: string = DEFAULT_SPARK_VERSION): boolean {
    const [lineMajor, lineMinor] = releaseParts(minorLine);
    const onLine = (parts: number[]): boolean => parts[0] === lineMajor && (parts[1] ?? 0) === lineMinor;
    let capped = false;
    for (const clause of specifier.split(',').filter((c) => c.length > 0)) {
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
 * Decides whether a `pyspark` specifier allows Apache Spark 4.2 or newer for SDP projects.
 *
 * @param specifier - Version specifier text without spaces.
 * @returns True when the specifier requires at least Spark 4.2 and does not exclude 4.2.
 */
export function pysparkSupportsSdp42(specifier: string): boolean {
    if (!specifier) {
        return false;
    }
    let has42LowerOrExact = false;
    for (const clause of specifier.split(',').filter((c) => c.length > 0)) {
        const m = /^(===|==|~=|>=|<=|!=|>|<)([\d.*]+)$/.exec(clause);
        if (!m) {
            return false;
        }
        const [, op, version] = m;
        const [major = 0, minor = 0] = releaseParts(version);
        if (op === '==' || op === '===' || op === '~=') {
            if (major !== 4 || minor < 2) {
                return false;
            }
            has42LowerOrExact = true;
        } else if (op === '>=' || op === '>') {
            if (major !== 4 || minor < 2) {
                return false;
            }
            has42LowerOrExact = true;
        } else if (op === '<') {
            if (major < 4 || (major === 4 && minor <= 2)) {
                return false;
            }
        } else if (op === '<=') {
            if (major < 4 || (major === 4 && minor < 2)) {
                return false;
            }
        }
    }
    return has42LowerOrExact;
}

/**
 * Audits `requirements.txt` for the PySpark 4.x or SDP 4.2+ repository contract.
 *
 * @param files - All generated files.
 * @param pipPackages - Optional ExecutionPlan pip packages that must be declared.
 * @param options - Optional paradigm override (`imperative` or `sdp`).
 * @param options.paradigm - Explicit paradigm override.
 * @returns Packaging findings.
 */
export function auditRequirementsTxt(
    files: GeneratedFileCandidate[],
    pipPackages: readonly string[] = [],
    options?: { paradigm?: PipelineParadigm }
): EvaluatorFinding[] {
    const reqFile = files.find((f) => normalizeRepoPath(f.filename) === 'requirements.txt');
    if (!reqFile) {
        return [{
            id: 'PKG-REQ-MISSING',
            category: 'PACKAGING',
            severity: 'CRITICAL',
            file: 'requirements.txt',
            description: 'requirements.txt is missing',
            rationale: 'Runner scripts and offline tests install dependencies from requirements.txt.',
        }];
    }

    const paradigm: PipelineParadigm = isSdpGeneratedProject(files, options?.paradigm) ? 'sdp' : 'imperative';
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
            rationale: `Local runs and tests need pyspark ${paradigm === 'sdp' ? '4.2' : DEFAULT_SPARK_VERSION}.x installed from requirements.txt.`,
        });
    } else if (paradigm === 'sdp') {
        const verdict = classifyPysparkSpecifier(pyspark.specifier);
        if (verdict === 'unpinned') {
            findings.push({
                id: 'PKG-REQ-PYSPARK-UNPINNED',
                category: 'PACKAGING',
                severity: 'MAJOR',
                file,
                line: pyspark.line,
                evidence: pyspark.raw,
                description: 'pyspark is not pinned to Spark 4.2+',
                rationale: 'Spark Declarative Pipelines requires Apache Spark 4.2+ (for example pyspark==4.2.* or pyspark>=4.2.0,<4.3).',
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
                rationale: 'SDP repositories require Apache Spark 4.2+ (pyspark.pipelines).',
            });
        } else if (!pysparkSupportsSdp42(pyspark.specifier)) {
            findings.push({
                id: 'PKG-REQ-SDP-PYSPARK-42',
                category: 'PACKAGING',
                severity: 'CRITICAL',
                file,
                line: pyspark.line,
                evidence: pyspark.raw,
                description: 'pyspark requirement does not require Apache Spark 4.2+ for SDP',
                rationale: 'pyspark.pipelines exists only in Apache Spark 4.2 and newer (pin pyspark==4.2.* or >=4.2.0,<4.3).',
            });
        }
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
                rationale: `Pin pyspark to the Spark ${DEFAULT_SPARK_VERSION} line (for example pyspark==${DEFAULT_SPARK_VERSION}.*) so local runs match the local Maven connector coordinates.`,
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
                rationale: `Pin pyspark==${DEFAULT_SPARK_VERSION}.* so pip does not install a newer Spark minor. The local Maven connector coordinates target Spark ${DEFAULT_SPARK_VERSION}.`,
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
                rationale: 'The generated code targets Spark 4.x APIs. A Spark 3.x or 5.x pin breaks local runs and tests.',
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
            rationale: `${paradigm === 'sdp' ? 'tests/test_transforms.py' : 'tests/test_pipeline.py'} runs with pytest (pytest>=8.0.0).`,
        });
    }

    const beam = reqs.find((r) => r.name === normalizePipName('apache_beam'));
    if (beam) {
        findings.push({
            id: 'PKG-REQ-BEAM-LEFTOVER',
            category: 'PACKAGING',
            severity: 'CRITICAL',
            file,
            line: beam.line,
            evidence: beam.raw,
            description: 'requirements.txt declares legacy Beam SDK dependency',
            rationale: 'The Spark designer generates PySpark repositories rather than legacy Beam packages.',
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
                rationale: 'Every package in runtimeArtifacts.pipPackages must be installed for the connector code to import.',
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
        { id: 'IVY', regex: /spark\.jars\.ivySettings\b/, severity: 'MAJOR', description: 'does not pass a Maven Central spark.jars.ivySettings file', rationale: 'POM-only ~/.m2 entries break the default Ivy chain. A Central-only Ivy settings file resolves --packages reliably.' },
    ],
    'run_dataproc_cluster.sh': [
        { id: 'SUBMIT', regex: /dataproc\s+jobs\s+submit\s+pyspark\b/, severity: 'CRITICAL', description: 'does not call gcloud dataproc jobs submit pyspark', rationale: 'Dataproc Cluster submission uses gcloud dataproc jobs submit pyspark.' },
        { id: 'CLUSTER', regex: /--cluster\b/, severity: 'CRITICAL', description: 'does not pass --cluster', rationale: 'gcloud dataproc jobs submit requires the target cluster name.' },
        { id: 'REGION', regex: /--region\b/, severity: 'MAJOR', description: 'does not pass --region', rationale: 'Dataproc jobs are regional. Pass --region from the REGION variable.' },
        { id: 'JARS', regex: /--jars\b/, severity: 'MAJOR', description: 'does not forward connector JARs with --jars', rationale: 'Connector JARs such as Spanner or Bigtable must reach the driver and executors.' },
        { id: 'PROPERTIES', regex: /--properties\b/, severity: 'MAJOR', description: 'does not forward Spark properties with --properties', rationale: 'Spark and Dataproc properties, including spark.jars.packages, travel through --properties.' },
    ],
    'run_serverless_spark.sh': [
        { id: 'SUBMIT', regex: /dataproc\s+batches\s+submit\s+pyspark\b/, severity: 'CRITICAL', description: 'does not call gcloud dataproc batches submit pyspark', rationale: 'Serverless Spark submission uses gcloud dataproc batches submit pyspark.' },
        { id: 'REGION', regex: /--region\b/, severity: 'MAJOR', description: 'does not pass --region', rationale: 'Serverless Spark batches are regional. Pass --region from the REGION variable.' },
        { id: 'VERSION', regex: /--version\b/, severity: 'MAJOR', description: 'does not pass the runtime --version', rationale: `Pin a Spark 4.x Serverless runtime (${SERVERLESS_SPARK4_RUNTIME_VERSIONS.join(' or ')}) with --version.` },
        { id: 'JARS', regex: /--jars\b/, severity: 'MAJOR', description: 'does not forward connector JARs with --jars', rationale: 'Connector JARs such as Spanner or Bigtable must reach the batch classpath.' },
        { id: 'PROPERTIES', regex: /--properties\b/, severity: 'MAJOR', description: 'does not forward Spark properties with --properties', rationale: 'Serverless Spark sets the tier, spark.jars.packages, and other properties through --properties.' },
    ],
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
 *
 * @param content - Shell script content without comments.
 * @returns Literal version strings found.
 */
export function extractServerlessRuntimeVersions(content: string): string[] {
    const versions: string[] = [];
    const patterns = [
        /--version(?:=|\s+)["']?(\d+\.\d+)\b/g,
        /RUNTIME_VERSION:-(\d+\.\d+)\b/g,
        /^\s*RUNTIME_VERSION=["']?(\d+\.\d+)\b/gm,
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
 * Audits the three imperative runner scripts for required gcloud and spark-submit flags.
 *
 * @param files - All generated files.
 * @returns Packaging findings.
 */
export function auditRunnerScripts(files: GeneratedFileCandidate[]): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];

    for (const [scriptName, rules] of Object.entries(RUNNER_RULES)) {
        const script = files.find((f) => normalizeRepoPath(f.filename) === scriptName);
        if (!script) {
            findings.push({
                id: `PKG-RUNNER-MISSING-${scriptName}`,
                category: 'PACKAGING',
                severity: 'CRITICAL',
                file: scriptName,
                description: `${scriptName} is missing`,
                rationale: 'Decision D3 requires run_local.sh, run_dataproc_cluster.sh, and run_serverless_spark.sh.',
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
                    rationale: rule.rationale,
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
                rationale: 'Strict mode stops the runner on the first failed command or unset variable.',
            });
        }

        if (scriptName !== 'run_local.sh' && /src\/pipeline\.py/.test(code) === false) {
            findings.push({
                id: `PKG-RUNNER-ENTRYPOINT-${scriptName}`,
                category: 'PACKAGING',
                severity: 'CRITICAL',
                file: script.filename,
                description: `${scriptName} does not submit src/pipeline.py`,
                rationale: 'The managed runners must submit the generated src/pipeline.py entry point.',
            });
        }

        const beamMatch = /--runner\b|apache_beam/.exec(code);
        if (beamMatch) {
            findings.push({
                id: `PKG-RUNNER-BEAM-${scriptName}`,
                category: 'PACKAGING',
                severity: 'CRITICAL',
                file: script.filename,
                evidence: beamMatch[0],
                description: `${scriptName} contains legacy non-Spark runner token '${beamMatch[0]}'`,
                rationale: 'Spark runner scripts must invoke PySpark or gcloud Dataproc commands without legacy runner flags.',
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
                rationale: 'Read the region from the REGION variable so the Deployment tab setting applies.',
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
                        rationale: `Valid Spark 4.x Serverless runtimes are ${SERVERLESS_SPARK4_RUNTIME_VERSIONS.join(' and ')}.`,
                    });
                }
            }
        }
    }

    return findings;
}

/**
 * Audits SDP runner scripts (`run_sdp_local.sh`, `run_sdp_dataproc.sh`) and Managed Data
 * Pipeline stage YAML (`sdp_stage.pipeline.yaml`) for Apache Spark 4.2+ SDP packaging rules.
 *
 * @param files - All generated files.
 * @returns Packaging findings for SDP launch artifacts.
 */
export function auditSdpRunnerScriptsAndStageYaml(files: GeneratedFileCandidate[]): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];

    const localScript = files.find((f) => normalizeRepoPath(f.filename) === 'run_sdp_local.sh');
    if (!localScript) {
        findings.push({
            id: 'PKG-SDP-RUNNER-MISSING-run_sdp_local.sh',
            category: 'PACKAGING',
            severity: 'CRITICAL',
            file: 'run_sdp_local.sh',
            description: 'run_sdp_local.sh is missing',
            rationale: 'SDP repositories require run_sdp_local.sh to run spark-pipelines dry-run and run over local Spark Connect.',
        });
    } else {
        const code = stripShellComments(localScript.content);
        if (!/\bspark-pipelines\b|pyspark\.pipelines\.cli|pyspark\/pipelines\/cli\.py/.test(code)) {
            findings.push({
                id: 'PKG-SDP-LOCAL-CLI',
                category: 'PACKAGING',
                severity: 'CRITICAL',
                file: localScript.filename,
                description: 'run_sdp_local.sh does not invoke spark-pipelines',
                rationale: 'Local SDP execution uses spark-pipelines dry-run or run with --spec spark-pipeline.yml.',
            });
        }
        if (!/--spec\b/.test(code)) {
            findings.push({
                id: 'PKG-SDP-LOCAL-SPEC',
                category: 'PACKAGING',
                severity: 'MAJOR',
                file: localScript.filename,
                description: 'run_sdp_local.sh does not pass --spec to spark-pipelines',
                rationale: 'Pass --spec spark-pipeline.yml explicitly when running spark-pipelines.',
            });
        }
        if (!/\bset\s+-eu(?:o\s+|\s+-o\s+)pipefail\b/.test(code)) {
            findings.push({
                id: 'PKG-SDP-RUNNER-STRICT-run_sdp_local.sh',
                category: 'PACKAGING',
                severity: 'MINOR',
                file: localScript.filename,
                description: 'run_sdp_local.sh does not enable set -euo pipefail',
                rationale: 'Strict shell mode stops the runner on the first failed command.',
            });
        }
    }

    const dataprocScript = files.find((f) => normalizeRepoPath(f.filename) === 'run_sdp_dataproc.sh');
    if (!dataprocScript) {
        findings.push({
            id: 'PKG-SDP-RUNNER-MISSING-run_sdp_dataproc.sh',
            category: 'PACKAGING',
            severity: 'CRITICAL',
            file: 'run_sdp_dataproc.sh',
            description: 'run_sdp_dataproc.sh is missing',
            rationale: 'SDP repositories require run_sdp_dataproc.sh for standalone Dataproc 4.0 validation and runs.',
        });
    } else {
        const code = stripShellComments(dataprocScript.content);
        if (!/SparkPipelines|pyspark\/pipelines\/cli\.py|run_sdp_on_dataproc|dataproc\s+jobs\s+submit/.test(code)) {
            findings.push({
                id: 'PKG-SDP-DATAPROC-SUBMIT',
                category: 'PACKAGING',
                severity: 'CRITICAL',
                file: dataprocScript.filename,
                description: 'run_sdp_dataproc.sh does not submit an SDP Dataproc 4.0 job via SparkPipelines or gcloud dataproc jobs submit',
                rationale: 'Standalone Dataproc 4.0 runs must launch SparkPipelines without conflicting yarn master flags.',
            });
        }
        if (!/CREATE\s+NAMESPACE\s+IF\s+NOT\s+EXISTS|--create_namespace/i.test(code)) {
            findings.push({
                id: 'PKG-SDP-DATAPROC-NAMESPACE',
                category: 'PACKAGING',
                severity: 'MAJOR',
                file: dataprocScript.filename,
                description: 'run_sdp_dataproc.sh does not prepare the target namespace (CREATE NAMESPACE IF NOT EXISTS)',
                rationale: 'SDP does not create target catalog namespaces automatically (SCHEMA_NOT_FOUND).',
            });
        }
    }

    const stageYaml = files.find((f) => {
        const norm = normalizeRepoPath(f.filename);
        return norm === 'sdp_stage.pipeline.yaml' || /^pipelines\/.+\.pipeline\.ya?ml$/.test(norm);
    });
    if (!stageYaml) {
        findings.push({
            id: 'PKG-SDP-STAGE-YAML-MISSING',
            category: 'PACKAGING',
            severity: 'CRITICAL',
            file: 'sdp_stage.pipeline.yaml',
            description: 'sdp_stage.pipeline.yaml is missing',
            rationale: 'Managed Data Pipelines sdp stage configuration is the primary GCP production deployment artifact.',
        });
    } else {
        try {
            const parsed = YAML.parse(stageYaml.content);
            if (!parsed || typeof parsed !== 'object' || !stageYaml.content.includes('spark-pipeline.yml')) {
                findings.push({
                    id: 'PKG-SDP-STAGE-YAML-SPEC-PATH',
                    category: 'PACKAGING',
                    severity: 'MAJOR',
                    file: stageYaml.filename,
                    description: `${stageYaml.filename} does not reference spark-pipeline.yml`,
                    rationale: 'Managed Data Pipeline sdp stages must reference spark-pipeline.yml via spec_path.',
                });
            }
        } catch (err) {
            findings.push({
                id: 'PKG-SDP-STAGE-YAML-PARSE',
                category: 'SYNTAX',
                severity: 'CRITICAL',
                file: stageYaml.filename,
                description: `${stageYaml.filename} is invalid YAML: ${(err as Error).message}`,
                rationale: 'sdp_stage.pipeline.yaml must parse as valid YAML.',
            });
        }
    }

    for (const file of files) {
        const name = normalizeRepoPath(file.filename);
        if (!name.endsWith('.sh') && !name.endsWith('.py') && !name.endsWith('.yaml') && !name.endsWith('.yml')) {
            continue;
        }
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
                    rationale: 'Spark 4.2 runtimes use Scala 2.13. Spark 3.x or Scala 2.12 builds fail at class load time.',
                });
                break;
            }
        }
    }

    return findings;
}

/**
 * Audits ExecutionPlan connector artifacts against the target runner script and all runner scripts.
 *
 * @param files - All generated files.
 * @param artifacts - ExecutionPlan runtime artifacts.
 * @returns Packaging findings.
 */
export function auditConnectorArtifacts(
    files: GeneratedFileCandidate[],
    artifacts: PipelineRuntimeArtifacts
): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];
    const targetScriptName = RUNNER_SCRIPT_BY_TYPE[artifacts.profile.runner];
    const targetScript = files.find((f) => normalizeRepoPath(f.filename) === targetScriptName);

    if (targetScript) {
        const code = targetScript.content;
        const required: { kind: string; value: string; needle: string }[] = [
            ...artifacts.gcsJars.map((j) => ({ kind: 'connector JAR', value: j, needle: j })),
            ...artifacts.mavenPackages.map((p) => ({ kind: 'Maven package', value: p, needle: p })),
            ...Object.keys(artifacts.sparkProperties).map((k) => ({ kind: 'Spark property', value: k, needle: k })),
        ];
        for (const item of required) {
            if (!code.includes(item.needle)) {
                findings.push({
                    id: `PKG-ARTIFACT-${item.value}`,
                    category: 'PACKAGING',
                    severity: 'MAJOR',
                    file: targetScript.filename,
                    description: `${targetScriptName} does not supply ExecutionPlan ${item.kind} '${item.value}'`,
                    rationale: `Runtime profile ${artifacts.profile.id} needs every ExecutionPlan runtime artifact on the classpath or in the Spark configuration.`,
                });
            }
        }
    }

    for (const scriptName of ['run_dataproc_cluster.sh', 'run_serverless_spark.sh']) {
        const script = files.find((f) => normalizeRepoPath(f.filename) === scriptName);
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
                        rationale: 'Dataproc and Serverless Spark already ship this connector. A second copy causes classpath conflicts.',
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
                    rationale: 'Spark 4.x runtimes use Scala 2.13. Spark 3.x or Scala 2.12 builds fail with binary incompatibility errors.',
                });
                break;
            }
        }
    }

    return findings;
}

/**
 * Runs every deterministic packaging audit for Imperative or SDP repositories.
 *
 * @param files - All generated files.
 * @param artifacts - Optional runtime artifacts.
 * @param options - Optional paradigm override (`imperative` or `sdp`).
 * @param options.paradigm - Explicit paradigm override.
 * @returns Packaging findings.
 */
export function runDeterministicPackagingAudit(
    files: GeneratedFileCandidate[],
    artifacts?: PipelineRuntimeArtifacts | null,
    options?: { paradigm?: PipelineParadigm }
): EvaluatorFinding[] {
    const paradigm: PipelineParadigm = isSdpGeneratedProject(files, options?.paradigm) ? 'sdp' : 'imperative';
    if (paradigm === 'sdp') {
        return [
            ...auditRequirementsTxt(files, artifacts?.pipPackages ?? [], { paradigm: 'sdp' }),
            ...auditSdpRunnerScriptsAndStageYaml(files),
        ];
    }

    return [
        ...auditRequirementsTxt(files, artifacts?.pipPackages ?? [], { paradigm: 'imperative' }),
        ...auditRunnerScripts(files),
        ...(artifacts ? auditConnectorArtifacts(files, artifacts) : []),
    ];
}

/**
 * Runs the Packaging and Deployability Evaluator sub-agent.
 *
 * @param ai - Initialized GoogleGenAI client.
 * @param language - Target language. The Spark designer generates PySpark only.
 * @param files - All generated files.
 * @param yamlContent - Optional pipeline YAML. If present, runtime artifacts are audited.
 * @returns Specialist evaluation result.
 */
export async function evaluatePackagingAndDeployability(
    ai: GoogleGenAI,
    language: 'Java' | 'Python',
    files: GeneratedFileCandidate[],
    yamlContent?: string
): Promise<SpecialistAgentResult> {
    const parsedSpec = yamlContent ? parsePipelineSpec(yamlContent) : undefined;
    const paradigm: PipelineParadigm = isSdpGeneratedProject(files, parsedSpec?.paradigm) ? 'sdp' : 'imperative';
    const isSdp = paradigm === 'sdp';
    const sparkVersion = isSdp ? '4.2' : DEFAULT_SPARK_VERSION;

    const imperativePlan = !isSdp && yamlContent ? buildSpecExecutionPlan(yamlContent) : null;
    const sdpPlan = isSdp && yamlContent ? buildSpecSdpExecutionPlan(yamlContent) : null;
    const artifacts = imperativePlan?.runtimeArtifacts ?? sdpPlan?.runtimeArtifacts ?? null;
    const deterministicFindings = runDeterministicPackagingAudit(files, artifacts, { paradigm });

    const artifactText = artifacts
        ? [
              `Runtime profile: ${artifacts.profile.id} (runner ${artifacts.profile.runner}, Spark ${artifacts.profile.sparkVersion}, Scala ${artifacts.profile.scalaVersion})`,
              `GCS JARs: ${artifacts.gcsJars.join(', ') || 'none'}`,
              `Maven packages: ${artifacts.mavenPackages.join(', ') || 'none'}`,
              `Pip packages: ${artifacts.pipPackages.join(', ') || 'none'}`,
              `Pre-installed connectors (never pass via packages): ${artifacts.preinstalledConnectors.join(', ') || 'none'}`,
              `Spark properties: ${Object.entries(artifacts.sparkProperties).map(([k, v]) => `${k}=${v}`).join(', ') || 'none'}`,
          ].join('\n')
        : 'not available';

    const checklistSection = isSdp
        ? `Audit the code against the following SDP Packaging & Deployment Checklist:
1. **requirements.txt:**
   - Is \`pyspark\` pinned to Apache Spark 4.2+ (for example \`pyspark==4.2.*\` or \`pyspark>=4.2.0,<4.3\`) and \`pytest\` declared?
   - Are driver requirements (such as \`zstandard>=0.25.0\` when targeting Dataproc 4.0) and connector pip packages included?
2. **SDP Runner Scripts & Managed Stage YAML:**
   - Does \`run_sdp_local.sh\` invoke \`spark-pipelines dry-run --spec spark-pipeline.yml\` or \`spark-pipelines run --spec spark-pipeline.yml\` over local Spark Connect (\`--remote "local[*]"\`), passing static settings via \`--conf\` and supporting \`SDP_REFRESH\` / \`SDP_FULL_REFRESH\` / \`SDP_FULL_REFRESH_ALL\`?
   - Does \`run_sdp_dataproc.sh\` target Dataproc image \`4.0\` (\`--optional-components=ICEBERG\`), run \`CREATE NAMESPACE IF NOT EXISTS <catalog>.<database>\`, strip conflicting \`spark.master=yarn\` / \`MASTER\` settings, and forward \`--conf\` static settings?
   - Does \`sdp_stage.pipeline.yaml\` define a Managed Data Pipelines \`sdp\` stage pointing \`spec_path\` at \`spark-pipeline.yml\` with static Spark properties and JAR URIs placed in \`execution_config\` (never inside \`spark-pipeline.yml\`)?`
        : `Audit the code against the following Packaging & Deployment Checklist:
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
   - Does it install requirements.txt and copy src/ into the WORKDIR that the runners expect?`;

    const prompt = `You are the Principal Packaging & Runtime Deployability Evaluator Agent.
Your responsibility is auditing a generated ${isSdp ? 'Apache Spark 4.2+ Spark Declarative Pipelines (SDP)' : 'Apache Spark 4.x PySpark'} repository for deployment readiness on Local Spark, Dataproc Clusters, and Serverless Spark.

=== TARGET LANGUAGE ===
${language} (${isSdp ? 'Apache Spark 4.2+ SDP' : 'PySpark 4.x'})
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
${deterministicFindings.length > 0 ? deterministicFindings.map((f) => `- [${f.severity}] ${f.id}: ${f.description}`).join('\n') : 'none'}
==========================================================================

=== GENERATED SOURCE FILES & ARTIFACTS (LINE NUMBERED) ===
${formatFilesWithLineNumbers(files)}
==========================================================

${checklistSection}

Severity Guidelines:
- CRITICAL: Wrong pyspark major/SDP version, runner script submitting a non-existent entry point, missing submit command, Scala 2.12 or Spark 3.x connector builds.
- MAJOR: Missing --region, --version, --jars, --properties, namespace creation, or connector artifacts.
- MINOR: Non-standard formatting or missing comments.`;

    const schema = {
        type: 'object',
        properties: {
            passed: {
                type: 'boolean',
                description: 'True if packaging and dependencies are valid and deployable',
            },
            summary: {
                type: 'string',
                description: 'Packaging assessment summary',
            },
            findings: {
                type: 'array',
                items: {
                    type: 'object',
                    properties: {
                        id: { type: 'string' },
                        category: { type: 'string', enum: ['PACKAGING', 'SYNTAX'] },
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

    const response = await generateContentWithRetry(ai, {
        model: DEFAULT_GEMINI_MODEL,
        contents: prompt,
        config: {
            responseMimeType: 'application/json',
            responseSchema: schema,
            temperature: 0.1,
        },
    });

    if (!response.text) {
        throw new Error('Empty response from PackagingAndDeployabilityAgent');
    }

    const parsed = JSON.parse(response.text);
    const blocking = deterministicFindings.filter((f) => f.severity === 'CRITICAL' || f.severity === 'MAJOR');
    const summary =
        deterministicFindings.length > 0
            ? `${parsed.summary} Deterministic audit: ${deterministicFindings.length} finding(s).`
            : parsed.summary;
    return {
        agentName: 'PackagingAndDeployabilityAgent',
        passed: Boolean(parsed.passed) && blocking.length === 0,
        summary,
        findings: [...deterministicFindings, ...(parsed.findings || [])],
    };
}
