/**
 * Deterministic Pre-Flight Gate for Generated PySpark Repository Evaluation.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Provides fast and fully deterministic ground-truth checks on a generated PySpark 4.x
 * repository before the LLM evaluator agents run. This stops LLM hallucination on
 * basic syntax validity, broken runner scripts, missing deliverables, banned Spark APIs,
 * and obvious credential leaks.
 *
 * Architectural decisions:
 * - Compiles every generated Python file with the host interpreter (`python3 -m py_compile`).
 * - Checks shell scripts (`*.sh`) with `bash -n` and requires a standard bash shebang.
 * - Child processes receive argument arrays only. The gate never builds shell command strings.
 * - Validates JSON artifacts with strict `JSON.parse`.
 * - Audits the locked D3 repository layout: `src/pipeline.py`, `tests/test_pipeline.py`,
 *   `requirements.txt`, `Dockerfile`, `run_local.sh`, `run_dataproc_cluster.sh`,
 *   `run_serverless_spark.sh`, and `README.md`.
 * - Verifies that `src/pipeline.py` declares the testable `transform(dfs, config)` function
 *   together with `read_sources` and `write_sinks`.
 * - Scans for banned Spark patterns: a hardcoded local master in cluster or serverless
 *   execution paths, and JVM gateway access (`spark._jsc`, `spark._jvm`).
 * - Scans for unreplaced template placeholders and credential redaction sentinels.
 * - Scans all generated files for plaintext secrets (cloud keys, tokens, private keys,
 *   and hardcoded password, secret, or token literals).
 * - Findings from this gate use severity 'CRITICAL' for blocking defects so the Arbiter
 *   applies a hard veto.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFile } from 'child_process';
import { DeterministicGateResult, EvaluatorFinding } from './types';

/**
 * One generated file in the code generation payload.
 */
export interface GeneratedFileCandidate {
    filename: string;
    content: string;
    executable?: boolean;
}

/**
 * Deterministic gate result extended with the Spark-specific structure and banned-pattern checks.
 * The extra fields are additive, so the value still satisfies `DeterministicGateResult`.
 */
export interface SparkDeterministicGateResult extends DeterministicGateResult {
    /** Findings for a missing or malformed `transform(dfs, config)` / `read_sources` / `write_sinks` layout. */
    structureFindings: EvaluatorFinding[];
    /** Findings for banned Spark APIs or a hardcoded local master in cluster paths. */
    bannedPatternFindings: EvaluatorFinding[];
}

/**
 * Required files of the generated PySpark repository (locked decision D3).
 */
export const REQUIRED_SPARK_DELIVERABLES: readonly string[] = [
    'src/pipeline.py',
    'tests/test_pipeline.py',
    'requirements.txt',
    'Dockerfile',
    'run_local.sh',
    'run_dataproc_cluster.sh',
    'run_serverless_spark.sh',
    'README.md'
];

/**
 * Runner scripts that submit the pipeline to a Dataproc Cluster or Serverless Spark.
 */
export const CLUSTER_RUNNER_SCRIPTS: readonly string[] = [
    'run_dataproc_cluster.sh',
    'run_serverless_spark.sh'
];

/**
 * Normalizes a generated filename to forward slashes without a leading `./`.
 *
 * @param filename - Raw filename from the generation payload.
 * @returns Normalized relative filename.
 */
export function normalizeGeneratedPath(filename: string): string {
    return filename.replace(/\\/g, '/').replace(/^\.\//, '');
}

/**
 * Returns true if the file is a Python unit test module or pytest fixture module.
 *
 * @param filename - Generated filename.
 * @returns True for `tests/` files, `test_*.py`, `*_test.py`, and `conftest.py`.
 */
export function isPythonTestFile(filename: string): boolean {
    const normalized = normalizeGeneratedPath(filename);
    const base = path.posix.basename(normalized);
    return normalized.startsWith('tests/')
        || normalized.includes('/tests/')
        || /^test_.*\.py$/.test(base)
        || /_test\.py$/.test(base)
        || base === 'conftest.py';
}

/**
 * Runs a syntax checker command on a temporary copy of a file.
 *
 * @param command - Executable name.
 * @param argsPrefix - Arguments placed before the temporary file path.
 * @param tmpPrefix - Prefix for the temporary directory.
 * @param fallbackName - Base name to use when the filename has none.
 * @param label - Checker label for error messages.
 * @param filename - Display filename.
 * @param content - File contents.
 * @returns Error string if the check fails, or null if it passes.
 */
function runSyntaxCheck(
    command: string,
    argsPrefix: string[],
    tmpPrefix: string,
    fallbackName: string,
    label: string,
    filename: string,
    content: string
): Promise<string | null> {
    return new Promise((resolve) => {
        let tmpDir: string;
        try {
            tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), tmpPrefix));
        } catch (mkErr: unknown) {
            resolve(`Could not verify ${label} syntax due to I/O error: ${(mkErr as Error).message}`);
            return;
        }
        const safeBaseName = path.basename(filename) || fallbackName;
        const tmpFile = path.join(tmpDir, safeBaseName);
        const cleanup = (): void => {
            try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* NOTE: Ignore cleanup errors. */ }
        };

        try {
            fs.writeFileSync(tmpFile, content, 'utf-8');
            execFile(command, [...argsPrefix, tmpFile], { timeout: 10000 }, (err, _stdout, stderr) => {
                cleanup();
                if (err) {
                    const message = String(stderr ?? '').trim() || err.message;
                    resolve(`${label} syntax check failed for ${filename}: ${message}`);
                } else {
                    resolve(null);
                }
            });
        } catch (writeErr: unknown) {
            cleanup();
            resolve(`Could not verify ${label} syntax due to I/O error: ${(writeErr as Error).message}`);
        }
    });
}

/**
 * Compiles a Python source string with `python3 -m py_compile`.
 *
 * @param filename - Relative or display filename.
 * @param content - Python source code.
 * @returns Error string if a syntax error occurs, or null if valid.
 */
export function checkPythonSyntax(filename: string, content: string): Promise<string | null> {
    return runSyntaxCheck('python3', ['-m', 'py_compile'], 'spark-eval-py-', 'pipeline.py', 'Python', filename, content);
}

/**
 * Checks a shell script with `bash -n`.
 *
 * @param filename - Shell script filename.
 * @param content - Script contents.
 * @returns Error string if a syntax error occurs, or null if valid.
 */
export function checkBashSyntax(filename: string, content: string): Promise<string | null> {
    return runSyntaxCheck('bash', ['-n'], 'spark-eval-sh-', 'script.sh', 'Bash', filename, content);
}

/**
 * Validates that a JSON file parses correctly.
 *
 * @param filename - JSON filename.
 * @param content - JSON contents.
 * @returns Error string if invalid, or null if valid.
 */
export function checkJsonSyntax(filename: string, content: string): string | null {
    try {
        JSON.parse(content);
        return null;
    } catch (err: unknown) {
        return `JSON parse error in ${filename}: ${(err as Error).message}`;
    }
}

/**
 * Scans for unreplaced template placeholders and redaction sentinels across all generated files.
 *
 * @param files - All generated files.
 * @returns Evaluator findings for unreplaced tokens.
 */
export function scanForUnreplacedPlaceholders(files: GeneratedFileCandidate[]): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];
    // NOTE: The (?<!\{) and (?!\}) guards keep Python f-string escaped braces such as
    // f"DRIVER={{{driver}}};" from matching as unreplaced {{...}} template placeholders.
    const placeholderRegex = /(?<!\{)\{\{[a-zA-Z0-9_]+\}\}(?!\})|__REDACTED_BY_(?:SPARK|BEAM)_DESIGNER__/g;

    for (const file of files) {
        const matches = file.content.match(placeholderRegex);
        if (matches && matches.length > 0) {
            const unique = Array.from(new Set(matches));
            findings.push({
                id: `PLACEHOLDER-${file.filename}`,
                category: 'SYNTAX',
                severity: 'CRITICAL',
                file: file.filename,
                description: `Unreplaced template placeholder(s) detected: ${unique.join(', ')}`,
                rationale: 'Generated code must not contain un-interpolated template placeholders or redaction markers.'
            });
        }
    }

    return findings;
}

/**
 * Validates that shell runner scripts have the executable flag and a standard bash shebang.
 *
 * @param files - All generated files.
 * @returns Evaluator findings for shell script issues.
 */
export function checkShellPermissionsAndShebang(files: GeneratedFileCandidate[]): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];
    const shellFiles = files.filter(f => f.filename.endsWith('.sh'));

    for (const sh of shellFiles) {
        if (sh.executable === false) {
            findings.push({
                id: `SHELL-EXEC-FLAG-${sh.filename}`,
                category: 'PACKAGING',
                severity: 'MAJOR',
                file: sh.filename,
                description: `Shell script '${sh.filename}' is marked with executable: false`,
                rationale: 'Shell runner scripts must specify executable: true in candidate manifest.'
            });
        }

        const firstLine = sh.content.split('\n')[0].trim();
        if (!firstLine.startsWith('#!/bin/bash') && !firstLine.startsWith('#!/usr/bin/env bash')) {
            findings.push({
                id: `SHELL-SHEBANG-${sh.filename}`,
                category: 'PACKAGING',
                severity: 'MAJOR',
                file: sh.filename,
                description: `Shell script '${sh.filename}' lacks standard bash shebang (found '${firstLine.substring(0, 30)}')`,
                rationale: 'Runner scripts must begin with #!/usr/bin/env bash or #!/bin/bash.'
            });
        }
    }

    return findings;
}

/**
 * Audits deliverable completeness against the locked D3 PySpark repository layout.
 *
 * @param files - All generated files.
 * @returns Descriptions of missing deliverables.
 */
export function checkDeliverableCompleteness(files: GeneratedFileCandidate[]): string[] {
    const filenames = new Set(files.map(f => normalizeGeneratedPath(f.filename)));
    return REQUIRED_SPARK_DELIVERABLES
        .filter(req => !filenames.has(req))
        .map(req => `Missing mandatory deliverable: ${req}`);
}

/**
 * Removes Python comment lines so that commented-out code does not satisfy structure checks.
 *
 * @param content - Python source code.
 * @returns Source code with full-line comments blanked.
 */
function stripPythonCommentLines(content: string): string {
    return content
        .split('\n')
        .map(line => (line.trimStart().startsWith('#') ? '' : line))
        .join('\n');
}

/**
 * Splits a Python parameter list at top-level commas.
 * Commas inside brackets, such as `dict[str, DataFrame]`, do not split.
 *
 * @param params - Raw parameter list text without the outer parentheses.
 * @returns Trimmed, non-empty parameter declarations.
 */
export function splitTopLevelParams(params: string): string[] {
    const parts: string[] = [];
    let depth = 0;
    let current = '';
    for (const c of params) {
        if (c === '[' || c === '(' || c === '{') depth++;
        else if (c === ']' || c === ')' || c === '}') depth = Math.max(0, depth - 1);
        if (c === ',' && depth === 0) {
            parts.push(current.trim());
            current = '';
        } else {
            current += c;
        }
    }
    parts.push(current.trim());
    return parts.filter(p => p.length > 0);
}

/**
 * Verifies the mandatory 3-phase testable layout of `src/pipeline.py`.
 *
 * Logic flow:
 * - If `src/pipeline.py` is absent, return no findings. The deliverable audit reports it.
 * - Require a module-level `def transform(dfs, ...)` whose first parameter is `dfs`.
 * - Require a second `config` parameter on `transform`.
 * - Require module-level `read_sources` and `write_sinks` functions.
 *
 * @param files - All generated files.
 * @returns Evaluator findings for structure defects.
 */
export function checkTransformSignature(files: GeneratedFileCandidate[]): EvaluatorFinding[] {
    const pipelineFile = files.find(f => normalizeGeneratedPath(f.filename) === 'src/pipeline.py');
    if (!pipelineFile) {
        return [];
    }

    const findings: EvaluatorFinding[] = [];
    const code = stripPythonCommentLines(pipelineFile.content);

    const transformMatch = /^def\s+transform\s*\(\s*dfs\b([^)]*)\)/m.exec(code);
    if (!transformMatch) {
        findings.push({
            id: 'STRUCT-TRANSFORM-SIGNATURE',
            category: 'ARCHITECTURE',
            severity: 'CRITICAL',
            file: pipelineFile.filename,
            description: 'src/pipeline.py does not declare a module-level transform(dfs, config) function',
            rationale: 'Decision D3 requires a pure transform(dfs, config) function so tests/test_pipeline.py can verify transformations offline.'
        });
    } else if (splitTopLevelParams(`dfs${transformMatch[1]}`)[1]?.split(/[:=]/)[0].trim() !== 'config') {
        findings.push({
            id: 'STRUCT-TRANSFORM-CONFIG-PARAM',
            category: 'ARCHITECTURE',
            severity: 'MAJOR',
            file: pipelineFile.filename,
            evidence: transformMatch[0].replace(/\s+/g, ' ').substring(0, 120),
            description: 'transform(dfs, ...) does not take config as its second parameter',
            rationale: 'Decision D3 fixes the signature transform(dfs, config) so generated tests can pass a test configuration.'
        });
    }

    for (const fnName of ['read_sources', 'write_sinks']) {
        const fnRegex = new RegExp(`^def\\s+${fnName}\\s*\\(`, 'm');
        if (!fnRegex.test(code)) {
            findings.push({
                id: `STRUCT-${fnName.toUpperCase()}`,
                category: 'ARCHITECTURE',
                severity: 'MAJOR',
                file: pipelineFile.filename,
                description: `src/pipeline.py does not declare a module-level ${fnName}() function`,
                rationale: 'Decision D3 separates I/O into read_sources(spark, config) and write_sinks(dfs, config) around the pure transform.'
            });
        }
    }

    return findings;
}

/**
 * One banned-pattern rule applied line by line to matching generated files.
 */
interface BannedPatternRule {
    ruleId: string;
    regex: RegExp;
    description: string;
    rationale: string;
    category: EvaluatorFinding['category'];
    appliesTo: (normalizedFilename: string) => boolean;
}

const LOCAL_MASTER_PY_REGEX = /\.master\(\s*[rfbu]?["']local\b/i;
const LOCAL_MASTER_SH_REGEX = /--master(?:=|\s+)["']?local\b|spark\.master=["']?local\b|\.master\(\s*["']local\b/i;

/**
 * Returns true if the file is a non-test Python module that runs on a cluster.
 *
 * @param normalizedFilename - Normalized generated filename.
 * @returns True for Python files outside the test suite.
 */
function isDeployedPythonFile(normalizedFilename: string): boolean {
    return normalizedFilename.endsWith('.py') && !isPythonTestFile(normalizedFilename);
}

const BANNED_PATTERN_RULES: readonly BannedPatternRule[] = [
    {
        ruleId: 'BANNED-LOCAL-MASTER',
        regex: LOCAL_MASTER_PY_REGEX,
        description: 'Hardcoded SparkSession.builder.master("local...") in a module that runs on Dataproc or Serverless Spark',
        rationale: 'A hardcoded local master overrides the Dataproc Cluster and Serverless Spark resource managers. Set the master only in run_local.sh or test fixtures.',
        category: 'ARCHITECTURE',
        appliesTo: isDeployedPythonFile
    },
    {
        ruleId: 'BANNED-LOCAL-MASTER',
        regex: LOCAL_MASTER_SH_REGEX,
        description: 'Hardcoded local master in a Dataproc Cluster or Serverless Spark runner script',
        rationale: 'Cluster and serverless runner scripts must let the managed resource manager choose the master.',
        category: 'PACKAGING',
        appliesTo: (name) => CLUSTER_RUNNER_SCRIPTS.includes(path.posix.basename(name))
    },
    {
        ruleId: 'BANNED-JVM-GATEWAY',
        regex: /\._(?:jsc|jvm)\b/,
        description: 'Private JVM gateway access (spark._jsc / spark._jvm) detected',
        rationale: 'Private Py4J gateway handles are unavailable in Spark Connect and Serverless Spark sessions, and they bypass the public PySpark API.',
        category: 'ARCHITECTURE',
        appliesTo: (name) => name.endsWith('.py')
    }
];

/**
 * Scans generated files for banned Spark patterns.
 *
 * Logic flow:
 * - Scan each applicable file line by line so findings carry 1-indexed line numbers.
 * - Skip full-line comments so that guidance such as `# Never use spark._jsc` does not match.
 * - Report each rule at most once per file, on the first matching line.
 *
 * @param files - All generated files.
 * @returns Evaluator findings for banned patterns.
 */
export function scanForBannedSparkPatterns(files: GeneratedFileCandidate[]): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];

    for (const file of files) {
        const normalized = normalizeGeneratedPath(file.filename);
        const lines = file.content.split('\n');

        for (const rule of BANNED_PATTERN_RULES) {
            if (!rule.appliesTo(normalized)) {
                continue;
            }
            for (let i = 0; i < lines.length; i++) {
                const line = lines[i];
                if (line.trimStart().startsWith('#')) {
                    continue;
                }
                if (rule.regex.test(line)) {
                    findings.push({
                        id: `${rule.ruleId}-${file.filename}`,
                        category: rule.category,
                        severity: 'CRITICAL',
                        file: file.filename,
                        line: i + 1,
                        evidence: line.trim().substring(0, 160),
                        description: rule.description,
                        rationale: rule.rationale
                    });
                    break;
                }
            }
        }
    }

    return findings;
}

/**
 * One plaintext secret detection pattern.
 */
interface SecretPattern {
    regex: RegExp;
    description: string;
    ruleId: string;
    appliesTo?: (normalizedFilename: string) => boolean;
}

// NOTE: The negative lookahead rejects environment lookups, shell expansions, and documented placeholders.
const SAFE_VALUE_LOOKAHEAD = '(?!\\$|\\{|<|%s|your[_-]|projects/|redacted|__REDACTED)';
const CREDENTIAL_KEY = '(?:password|passwd|pwd|secret|secret[._-]?key|client[._-]?secret|api[._-]?key|access[._-]?key|auth[._-]?token|access[._-]?token|token)';

const SECRET_PATTERNS: readonly SecretPattern[] = [
    {
        regex: /AKIA[0-9A-Z]{16}/,
        description: 'Hardcoded AWS Access Key ID detected',
        ruleId: 'SEC-AWS-KEY'
    },
    {
        regex: /-----BEGIN\s+(?:[A-Z0-9_-]+\s+)*PRIVATE\s+KEY-----/,
        description: 'Hardcoded Private Cryptographic Key detected',
        ruleId: 'SEC-PRIV-KEY'
    },
    {
        regex: /AIzaSy[0-9A-Za-z_-]{33}/,
        description: 'Hardcoded Google API Key detected',
        ruleId: 'SEC-GCP-API-KEY'
    },
    {
        regex: /GOCSPX-[a-zA-Z0-9_-]{28}/,
        description: 'Hardcoded Google OAuth Client Secret detected',
        ruleId: 'SEC-GCP-OAUTH-SECRET'
    },
    {
        regex: /ya29\.[0-9A-Za-z_-]{20,}/,
        description: 'Hardcoded Google OAuth access token detected',
        ruleId: 'SEC-GCP-ACCESS-TOKEN'
    },
    {
        regex: /ghp_[a-zA-Z0-9]{36}/,
        description: 'Hardcoded GitHub Personal Access Token detected',
        ruleId: 'SEC-GITHUB-PAT'
    },
    {
        regex: /xox[baprs]-[0-9A-Za-z-]{10,}/,
        description: 'Hardcoded Slack token detected',
        ruleId: 'SEC-SLACK-TOKEN'
    },
    {
        regex: /jdbc:[a-zA-Z0-9_-]+:\/\/[^\s"'`]+[?&;]password=(?!(\$\{|\$|%s|<|YOUR_))([^\s"'`&;]{4,})/,
        description: 'Plaintext password embedded in JDBC connection URL string',
        ruleId: 'SEC-JDBC-PW'
    },
    {
        // NOTE: Matches `password = "..."`, `"password": "..."`, and `.option("password", "...")`.
        regex: new RegExp(
            `["']?\\b[A-Za-z0-9_.-]*${CREDENTIAL_KEY}["']?\\s*[:=,]\\s*["']${SAFE_VALUE_LOOKAHEAD}[^"'\\s]{4,}["']`,
            'i'
        ),
        description: 'Hardcoded password, secret, key, or token literal detected',
        ruleId: 'SEC-CREDENTIAL-LITERAL',
        appliesTo: (name) => !name.toLowerCase().endsWith('.md')
    },
    {
        // NOTE: Matches unquoted shell or Dockerfile assignments such as `export DB_PASSWORD=hunter22`.
        regex: /^\s*(?:export\s+|ENV\s+)?[A-Z0-9_]*(?:PASSWORD|SECRET|TOKEN|API_KEY|ACCESS_KEY)[A-Z0-9_]*=(?!["'$])[^\s#]{4,}/m,
        description: 'Hardcoded credential assignment in shell script or Dockerfile detected',
        ruleId: 'SEC-SHELL-CREDENTIAL',
        appliesTo: (name) => name.endsWith('.sh') || path.posix.basename(name) === 'Dockerfile'
    }
];

/**
 * Scans generated files for credential leaks or plaintext secrets.
 *
 * @param files - All generated files in the payload.
 * @returns Security findings.
 */
export function scanForPlaintextSecrets(files: GeneratedFileCandidate[]): EvaluatorFinding[] {
    const findings: EvaluatorFinding[] = [];

    for (const file of files) {
        const normalized = normalizeGeneratedPath(file.filename);
        const isTestFile = isPythonTestFile(normalized);

        for (const pattern of SECRET_PATTERNS) {
            if (pattern.appliesTo && !pattern.appliesTo(normalized)) {
                continue;
            }
            const match = pattern.regex.exec(file.content);
            if (!match) {
                continue;
            }
            const matchedText = match[0].toLowerCase();
            if (isTestFile && /test|mock|dummy|fake/.test(matchedText)) {
                continue;
            }

            const line = file.content.substring(0, match.index).split('\n').length;
            findings.push({
                id: `${pattern.ruleId}-${file.filename}`,
                category: 'SECURITY',
                severity: 'CRITICAL',
                file: file.filename,
                line,
                description: pattern.description,
                rationale: `Found match '${match[0].trim().substring(0, 10)}...' which indicates a plaintext secret committed into code or artifacts.`
            });
        }
    }

    return findings;
}

/**
 * Runs the deterministic pre-flight gate on a generated PySpark repository.
 *
 * @param files - All generated files.
 * @param language - Target language. Only 'Python' (PySpark) is supported by decision D3.
 * @returns Combined deterministic gate result.
 */
export async function runDeterministicGate(
    files: GeneratedFileCandidate[],
    language: 'Java' | 'Python' = 'Python'
): Promise<SparkDeterministicGateResult> {
    const syntaxErrors: string[] = [];
    const shellErrors: string[] = [];
    const jsonErrors: string[] = [];

    if (language !== 'Python') {
        syntaxErrors.push(`Unsupported target language '${language}': the Spark designer generates PySpark repositories only (decision D3).`);
    }

    // 1. Compile Python files and check shell scripts concurrently.
    const pyFiles = files.filter(f => f.filename.endsWith('.py'));
    const shFiles = files.filter(f => f.filename.endsWith('.sh'));

    const [pyResults, shResults] = await Promise.all([
        Promise.all(pyFiles.map(pyFile => checkPythonSyntax(pyFile.filename, pyFile.content))),
        Promise.all(shFiles.map(sh => checkBashSyntax(sh.filename, sh.content)))
    ]);

    for (const err of pyResults) {
        if (err) syntaxErrors.push(err);
    }
    for (const err of shResults) {
        if (err) shellErrors.push(err);
    }

    // 2. Parse JSON files.
    for (const jsonFile of files.filter(f => f.filename.endsWith('.json'))) {
        const err = checkJsonSyntax(jsonFile.filename, jsonFile.content);
        if (err) jsonErrors.push(err);
    }

    // 3. Audit the D3 deliverables only for a full generation payload (3 or more files).
    const missingDeliverables = files.length >= 3 ? checkDeliverableCompleteness(files) : [];

    // 4. Run the static scanners.
    const structureFindings = checkTransformSignature(files);
    const bannedPatternFindings = scanForBannedSparkPatterns(files);
    const placeholderFindings = scanForUnreplacedPlaceholders(files);
    const shellFindings = checkShellPermissionsAndShebang(files);
    const secretFindings = scanForPlaintextSecrets(files);

    const allGateFindings = [
        ...secretFindings,
        ...bannedPatternFindings,
        ...structureFindings,
        ...placeholderFindings,
        ...shellFindings
    ];

    const allSyntaxErrors = [
        ...syntaxErrors,
        ...shellErrors,
        ...jsonErrors
    ];

    return {
        syntaxValid: allSyntaxErrors.length === 0,
        syntaxErrors: allSyntaxErrors,
        secretScanPassed: allGateFindings.filter(f => f.severity === 'CRITICAL').length === 0,
        secretFindings: allGateFindings,
        placeholderErrors: placeholderFindings.map(f => f.description),
        jsonErrors,
        shellErrors,
        missingDeliverables,
        structureFindings,
        bannedPatternFindings
    };
}
