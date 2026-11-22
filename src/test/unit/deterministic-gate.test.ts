/**
 * Unit Tests for the Spark Deterministic Pre-Flight Gate.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies the offline deterministic checks that run before the LLM evaluator agents:
 * py_compile and bash -n invocation with argument arrays, JSON parsing, D3 deliverable
 * audit, transform(dfs, config) signature detection, banned Spark pattern scanning,
 * placeholder scanning, shell shebang checks, and plaintext secret detection.
 * The tests mock child_process, so they never start python3 or bash.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

const { execFileMock } = vi.hoisted(() => ({ execFileMock: vi.fn() }));

vi.mock('child_process', async (importOriginal) => {
    const actual = await importOriginal<typeof import('child_process')>();
    return { ...actual, execFile: execFileMock };
});

import {
    GeneratedFileCandidate,
    REQUIRED_SPARK_DELIVERABLES,
    checkBashSyntax,
    checkDeliverableCompleteness,
    checkJsonSyntax,
    checkPythonSyntax,
    checkShellPermissionsAndShebang,
    checkTransformSignature,
    isPythonTestFile,
    normalizeGeneratedPath,
    runDeterministicGate,
    scanForBannedSparkPatterns,
    scanForPlaintextSecrets,
    scanForUnreplacedPlaceholders,
    splitTopLevelParams
} from '../integration/evaluators/deterministic-gate';

type ExecCallback = (err: Error | null, stdout: string, stderr: string) => void;

/**
 * Makes execFile succeed for every call.
 */
function mockExecSuccess(): void {
    execFileMock.mockImplementation((_cmd: string, _args: string[], _opts: unknown, cb: ExecCallback) => {
        cb(null, '', '');
    });
}

const VALID_PIPELINE = [
    'from typing import Any',
    'from pyspark.sql import DataFrame, SparkSession',
    '',
    'def read_sources(spark: SparkSession, config: dict[str, Any]) -> dict[str, DataFrame]:',
    '    return {"src": spark.read.parquet(config["path"])}',
    '',
    'def transform(dfs: dict[str, DataFrame], config: dict[str, Any]) -> dict[str, DataFrame]:',
    '    dfs["out"] = dfs["src"].filter("amount > 0")',
    '    return dfs',
    '',
    'def write_sinks(dfs: dict[str, DataFrame], config: dict[str, Any]) -> None:',
    '    dfs["out"].write.mode("overwrite").parquet(config["out"])',
    '',
    'def main() -> None:',
    '    spark = SparkSession.builder.appName("pipeline").getOrCreate()',
    '    config = {}',
    '    write_sinks(transform(read_sources(spark, config), config), config)',
    ''
].join('\n');

const VALID_TEST = [
    'import pytest',
    'from pyspark.sql import SparkSession',
    'from src.pipeline import transform',
    '',
    '@pytest.fixture(scope="module")',
    'def spark():',
    '    return SparkSession.builder.master("local[2]").appName("unit-tests").getOrCreate()',
    ''
].join('\n');

/**
 * Builds a complete D3 repository payload with optional overrides.
 *
 * @param overrides - Files that replace or extend the defaults, keyed by filename.
 * @returns Generated file candidates.
 */
function buildRepo(overrides: Record<string, string> = {}): GeneratedFileCandidate[] {
    const defaults: Record<string, string> = {
        'src/pipeline.py': VALID_PIPELINE,
        'tests/test_pipeline.py': VALID_TEST,
        'requirements.txt': 'pyspark>=4.0.0,<5\npytest>=8.0.0\n',
        'Dockerfile': 'FROM debian:12-slim\nUSER 1099:1099\n',
        'run_local.sh': '#!/usr/bin/env bash\nset -euo pipefail\nspark-submit --master "local[*]" src/pipeline.py\n',
        'run_dataproc_cluster.sh': '#!/usr/bin/env bash\nset -euo pipefail\ngcloud dataproc jobs submit pyspark src/pipeline.py --region="${REGION}"\n',
        'run_serverless_spark.sh': '#!/usr/bin/env bash\nset -euo pipefail\ngcloud dataproc batches submit pyspark src/pipeline.py --version="${RUNTIME_VERSION:-4.0}"\n',
        'README.md': '# Pipeline\n'
    };
    const merged = { ...defaults, ...overrides };
    return Object.entries(merged).map(([filename, content]) => ({
        filename,
        content,
        executable: filename.endsWith('.sh') ? true : false
    }));
}

beforeEach(() => {
    execFileMock.mockReset();
    mockExecSuccess();
});

describe('Path helpers', () => {
    it('normalizes backslashes and leading ./ (normal condition)', () => {
        expect(normalizeGeneratedPath('.\\src\\pipeline.py')).toBe('src/pipeline.py');
        expect(normalizeGeneratedPath('./tests/test_pipeline.py')).toBe('tests/test_pipeline.py');
    });

    it('classifies Python test modules and fixtures (boundary condition)', () => {
        expect(isPythonTestFile('tests/test_pipeline.py')).toBe(true);
        expect(isPythonTestFile('pkg/tests/helpers.py')).toBe(true);
        expect(isPythonTestFile('conftest.py')).toBe(true);
        expect(isPythonTestFile('src/pipeline_test.py')).toBe(true);
        expect(isPythonTestFile('src/pipeline.py')).toBe(false);
        expect(isPythonTestFile('src/contest.py')).toBe(false);
    });

    it('splits parameters only at top-level commas', () => {
        expect(splitTopLevelParams('dfs: dict[str, DataFrame], config: dict[str, Any]'))
            .toEqual(['dfs: dict[str, DataFrame]', 'config: dict[str, Any]']);
        expect(splitTopLevelParams('dfs,\n    config,\n')).toEqual(['dfs', 'config']);
        expect(splitTopLevelParams('')).toEqual([]);
    });
});

describe('checkPythonSyntax', () => {
    it('invokes python3 -m py_compile with an argument array and returns null on success (normal condition)', async () => {
        const err = await checkPythonSyntax('src/pipeline.py', VALID_PIPELINE);
        expect(err).toBeNull();
        expect(execFileMock).toHaveBeenCalledTimes(1);
        const [cmd, args, opts] = execFileMock.mock.calls[0];
        expect(cmd).toBe('python3');
        expect(args.slice(0, 2)).toEqual(['-m', 'py_compile']);
        expect(args[2]).toMatch(/spark-eval-py-.*pipeline\.py$/);
        expect(opts).toMatchObject({ timeout: 10000 });
    });

    it('returns the compiler stderr on syntax errors (failure condition)', async () => {
        execFileMock.mockImplementation((_c: string, _a: string[], _o: unknown, cb: ExecCallback) => {
            cb(new Error('exit 1'), '', '  SyntaxError: invalid syntax (pipeline.py, line 2)  ');
        });
        const err = await checkPythonSyntax('src/pipeline.py', 'def broken(:\n');
        expect(err).toBe('Python syntax check failed for src/pipeline.py: SyntaxError: invalid syntax (pipeline.py, line 2)');
    });

    it('falls back to the error message when stderr is empty (boundary condition)', async () => {
        execFileMock.mockImplementation((_c: string, _a: string[], _o: unknown, cb: ExecCallback) => {
            cb(new Error('spawn python3 ENOENT'), '', '');
        });
        const err = await checkPythonSyntax('', 'x = 1');
        expect(err).toContain('spawn python3 ENOENT');
        expect(execFileMock.mock.calls[0][1][2]).toMatch(/pipeline\.py$/);
    });

    it('never passes file content through a shell string (security condition)', async () => {
        await checkPythonSyntax('src/$(rm -rf ~).py', 'x = 1');
        const [cmd, args] = execFileMock.mock.calls[0];
        expect(cmd).toBe('python3');
        expect(Array.isArray(args)).toBe(true);
        expect(args.join(' ')).not.toContain('x = 1');
    });
});

describe('checkBashSyntax', () => {
    it('invokes bash -n with an argument array (normal condition)', async () => {
        expect(await checkBashSyntax('run_local.sh', '#!/usr/bin/env bash\necho ok\n')).toBeNull();
        const [cmd, args] = execFileMock.mock.calls[0];
        expect(cmd).toBe('bash');
        expect(args[0]).toBe('-n');
        expect(args[1]).toMatch(/run_local\.sh$/);
    });

    it('reports bash syntax errors (failure condition)', async () => {
        execFileMock.mockImplementation((_c: string, _a: string[], _o: unknown, cb: ExecCallback) => {
            cb(new Error('exit 2'), '', 'syntax error near unexpected token `fi`');
        });
        const err = await checkBashSyntax('run_local.sh', 'if then fi');
        expect(err).toContain('Bash syntax check failed for run_local.sh');
        expect(err).toContain('unexpected token');
    });
});

describe('checkJsonSyntax', () => {
    it('accepts valid JSON and rejects malformed JSON', () => {
        expect(checkJsonSyntax('config.json', '{"a": 1}')).toBeNull();
        expect(checkJsonSyntax('config.json', '{"a": }')).toContain('JSON parse error in config.json');
        expect(checkJsonSyntax('empty.json', '')).toContain('JSON parse error');
    });
});

describe('checkDeliverableCompleteness', () => {
    it('returns no gaps for a complete D3 repository (normal condition)', () => {
        expect(checkDeliverableCompleteness(buildRepo())).toEqual([]);
    });

    it('lists every D3 deliverable for an empty payload (boundary condition)', () => {
        const missing = checkDeliverableCompleteness([]);
        expect(missing).toHaveLength(REQUIRED_SPARK_DELIVERABLES.length);
        expect(missing).toContain('Missing mandatory deliverable: run_serverless_spark.sh');
    });

    it('flags legacy non-Spark layouts as incomplete (failure condition)', () => {
        const legacyRepo: GeneratedFileCandidate[] = [
            { filename: 'src/wordcount.py', content: 'import apache_beam' },
            { filename: 'src/wordcount_test.py', content: '' },
            { filename: 'run_legacy.sh', content: '' },
            { filename: 'metadata.json', content: '{}' }
        ];
        const missing = checkDeliverableCompleteness(legacyRepo);
        expect(missing).toContain('Missing mandatory deliverable: src/pipeline.py');
        expect(missing).toContain('Missing mandatory deliverable: tests/test_pipeline.py');
        expect(missing).toContain('Missing mandatory deliverable: run_dataproc_cluster.sh');
    });

    it('accepts ./ prefixed filenames', () => {
        const repo = buildRepo().map(f => ({ ...f, filename: `./${f.filename}` }));
        expect(checkDeliverableCompleteness(repo)).toEqual([]);
    });
});

describe('checkTransformSignature', () => {
    it('accepts the D3 read_sources / transform / write_sinks layout (normal condition)', () => {
        expect(checkTransformSignature(buildRepo())).toEqual([]);
    });

    it('accepts a multi-line transform signature with a trailing comma (boundary condition)', () => {
        const code = 'def read_sources(spark, config):\n    pass\n\ndef transform(\n    dfs: dict[str, DataFrame],\n    config: dict[str, Any],\n) -> dict:\n    return dfs\n\ndef write_sinks(dfs, config):\n    pass\n';
        expect(checkTransformSignature([{ filename: 'src/pipeline.py', content: code }])).toEqual([]);
    });

    it('returns no findings when src/pipeline.py is absent (boundary condition)', () => {
        expect(checkTransformSignature([{ filename: 'README.md', content: '' }])).toEqual([]);
    });

    it('flags a missing transform(dfs) function as CRITICAL (failure condition)', () => {
        const code = 'def read_sources(spark, config):\n    pass\n\ndef run(df):\n    return df\n\ndef write_sinks(dfs, config):\n    pass\n';
        const findings = checkTransformSignature([{ filename: 'src/pipeline.py', content: code }]);
        expect(findings).toHaveLength(1);
        expect(findings[0].id).toBe('STRUCT-TRANSFORM-SIGNATURE');
        expect(findings[0].severity).toBe('CRITICAL');
        expect(findings[0].category).toBe('ARCHITECTURE');
    });

    it('ignores a commented-out or indented transform definition (failure condition)', () => {
        const code = '# def transform(dfs, config):\nclass P:\n    def transform(dfs, config):\n        pass\ndef read_sources(spark, config):\n    pass\ndef write_sinks(dfs, config):\n    pass\n';
        const findings = checkTransformSignature([{ filename: 'src/pipeline.py', content: code }]);
        expect(findings.map(f => f.id)).toEqual(['STRUCT-TRANSFORM-SIGNATURE']);
    });

    it('flags a transform whose first parameter is not dfs (failure condition)', () => {
        const code = 'def read_sources(spark, config):\n    pass\ndef transform(df, config):\n    return df\ndef write_sinks(dfs, config):\n    pass\n';
        const findings = checkTransformSignature([{ filename: 'src/pipeline.py', content: code }]);
        expect(findings[0].id).toBe('STRUCT-TRANSFORM-SIGNATURE');
    });

    it('flags a missing config parameter and missing I/O functions as MAJOR', () => {
        const code = 'def transform(dfs):\n    return dfs\n';
        const findings = checkTransformSignature([{ filename: 'src/pipeline.py', content: code }]);
        expect(findings.map(f => f.id)).toEqual([
            'STRUCT-TRANSFORM-CONFIG-PARAM',
            'STRUCT-READ_SOURCES',
            'STRUCT-WRITE_SINKS'
        ]);
        expect(findings.every(f => f.severity === 'MAJOR')).toBe(true);
        expect(findings[0].evidence).toBe('def transform(dfs)');
    });
});

describe('scanForBannedSparkPatterns', () => {
    it('returns no findings for a clean repository (normal condition)', () => {
        expect(scanForBannedSparkPatterns(buildRepo())).toEqual([]);
    });

    it('allows a local master in run_local.sh and in test fixtures (boundary condition)', () => {
        const files = buildRepo({
            'tests/conftest.py': 'spark = SparkSession.builder.master("local[2]").getOrCreate()\n',
            // NOTE: The offline pytest runner writes this root conftest into the generated repository.
            'conftest.py': 'import pytest\nfrom pyspark.sql import SparkSession\n\n@pytest.fixture(scope="session")\ndef spark():\n    return SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()\n'
        });
        expect(scanForBannedSparkPatterns(files)).toEqual([]);
    });

    it('allows an environment-driven master in src/pipeline.py (boundary condition)', () => {
        const code = 'builder = SparkSession.builder\nif "SPARK_MASTER" in os.environ:\n    builder = builder.master(os.environ["SPARK_MASTER"])\n';
        expect(scanForBannedSparkPatterns([{ filename: 'src/pipeline.py', content: code }])).toEqual([]);
    });

    it('flags SparkSession.builder.master("local[*]") in src/pipeline.py with a line number (failure condition)', () => {
        const code = 'from pyspark.sql import SparkSession\n\nspark = SparkSession.builder.master("local[*]").getOrCreate()\n';
        const findings = scanForBannedSparkPatterns([{ filename: 'src/pipeline.py', content: code }]);
        expect(findings).toHaveLength(1);
        expect(findings[0].id).toBe('BANNED-LOCAL-MASTER-src/pipeline.py');
        expect(findings[0].severity).toBe('CRITICAL');
        expect(findings[0].line).toBe(3);
        expect(findings[0].evidence).toContain('.master("local[*]")');
    });

    it('flags a chained builder with single quotes on a continuation line (failure condition)', () => {
        const code = 'spark = (\n    SparkSession.builder\n    .master(\'local\')\n    .getOrCreate()\n)\n';
        const findings = scanForBannedSparkPatterns([{ filename: 'src/helpers/session.py', content: code }]);
        expect(findings[0].line).toBe(3);
    });

    it('flags a local master in cluster and serverless runner scripts (failure condition)', () => {
        const files: GeneratedFileCandidate[] = [
            { filename: 'run_dataproc_cluster.sh', content: '#!/usr/bin/env bash\ngcloud dataproc jobs submit pyspark src/pipeline.py --properties=spark.master=local[4]\n' },
            { filename: 'run_serverless_spark.sh', content: '#!/usr/bin/env bash\nspark-submit --master local[*] src/pipeline.py\n' },
            { filename: 'run_local.sh', content: '#!/usr/bin/env bash\nspark-submit --master local[*] src/pipeline.py\n' }
        ];
        const findings = scanForBannedSparkPatterns(files);
        expect(findings.map(f => f.file).sort()).toEqual(['run_dataproc_cluster.sh', 'run_serverless_spark.sh']);
        expect(findings.every(f => f.category === 'PACKAGING')).toBe(true);
    });

    it('flags spark._jsc and spark._jvm access but ignores comment lines (failure condition)', () => {
        const code = '# Never use spark._jsc here.\nhconf = spark._jsc.hadoopConfiguration()\n';
        const findings = scanForBannedSparkPatterns([{ filename: 'src/pipeline.py', content: code }]);
        expect(findings).toHaveLength(1);
        expect(findings[0].id).toBe('BANNED-JVM-GATEWAY-src/pipeline.py');
        expect(findings[0].line).toBe(2);

        const jvm = scanForBannedSparkPatterns([{ filename: 'tests/test_pipeline.py', content: 'x = spark.sparkContext._jvm.java.lang.System\n' }]);
        expect(jvm).toHaveLength(1);
    });

    it('does not flag identifiers that only contain _jsc as a substring', () => {
        const code = 'my_jsc_value = 1\nspark._jscope = None\n';
        expect(scanForBannedSparkPatterns([{ filename: 'src/pipeline.py', content: code }])).toEqual([]);
    });
});

describe('scanForUnreplacedPlaceholders', () => {
    it('flags template tokens and both redaction sentinels', () => {
        const findings = scanForUnreplacedPlaceholders([
            { filename: 'src/pipeline.py', content: 'name = "{{pipeline_name}}"\npw = "__REDACTED_BY_SPARK_DESIGNER__"\n' },
            { filename: 'run_local.sh', content: 'SECRET=__REDACTED_BY_BEAM_DESIGNER__' }
        ]);
        expect(findings).toHaveLength(2);
        expect(findings[0].description).toContain('{{pipeline_name}}');
        expect(findings[0].description).toContain('__REDACTED_BY_SPARK_DESIGNER__');
        expect(findings[0].severity).toBe('CRITICAL');
    });

    it('ignores Python f-string escaped braces (boundary condition)', () => {
        expect(scanForUnreplacedPlaceholders([
            { filename: 'src/pipeline.py', content: 'conn = f"DRIVER={{{driver}}};"\n' }
        ])).toEqual([]);
    });
});

describe('checkShellPermissionsAndShebang', () => {
    it('accepts executable scripts with a bash shebang', () => {
        expect(checkShellPermissionsAndShebang(buildRepo())).toEqual([]);
    });

    it('flags a non-executable script with a sh shebang', () => {
        const findings = checkShellPermissionsAndShebang([
            { filename: 'run_local.sh', content: '#!/bin/sh\necho hi', executable: false }
        ]);
        expect(findings.map(f => f.id)).toEqual(['SHELL-EXEC-FLAG-run_local.sh', 'SHELL-SHEBANG-run_local.sh']);
    });
});

describe('scanForPlaintextSecrets', () => {
    it('returns no findings for environment and Secret Manager lookups (normal condition)', () => {
        const code = [
            'password = os.environ["DB_PASSWORD"]',
            'token = os.environ.get("API_TOKEN", "")',
            'df = spark.read.format("jdbc").option("password", password).load()',
            'config = {"passwordSecret": "projects/p/secrets/db-pass/versions/latest"}',
            'url = "jdbc:postgresql://h:5432/db?user=u&password=${DB_PASSWORD}"'
        ].join('\n');
        const shell = '#!/usr/bin/env bash\nDB_PASSWORD="${DB_PASSWORD:-}"\nexport API_TOKEN="$(gcloud auth print-access-token)"\n';
        expect(scanForPlaintextSecrets([
            { filename: 'src/pipeline.py', content: code },
            { filename: 'run_local.sh', content: shell }
        ])).toEqual([]);
    });

    it('detects cloud keys and tokens (failure condition)', () => {
        const files: GeneratedFileCandidate[] = [
            { filename: 'src/a.py', content: 'k = "AKIAIOSFODNN7EXAMPLE"' },
            { filename: 'src/b.py', content: `k = "AIzaSy${'A'.repeat(33)}"` },
            { filename: 'src/c.py', content: `t = "ya29.${'a'.repeat(30)}"` },
            { filename: 'src/d.py', content: '-----BEGIN RSA PRIVATE KEY-----' },
            { filename: 'src/e.py', content: `g = "ghp_${'a'.repeat(36)}"` }
        ];
        const ids = scanForPlaintextSecrets(files).map(f => f.id);
        expect(ids).toEqual([
            'SEC-AWS-KEY-src/a.py',
            'SEC-GCP-API-KEY-src/b.py',
            'SEC-GCP-ACCESS-TOKEN-src/c.py',
            'SEC-PRIV-KEY-src/d.py',
            'SEC-GITHUB-PAT-src/e.py'
        ]);
    });

    it('detects hardcoded password, secret, and token literals with line numbers (failure condition)', () => {
        const code = 'import os\n\ndf = spark.read.format("jdbc").option("password", "hunter22").load()\n';
        const findings = scanForPlaintextSecrets([{ filename: 'src/pipeline.py', content: code }]);
        expect(findings).toHaveLength(1);
        expect(findings[0].id).toBe('SEC-CREDENTIAL-LITERAL-src/pipeline.py');
        expect(findings[0].line).toBe(3);
        expect(findings[0].category).toBe('SECURITY');

        const dictLiteral = scanForPlaintextSecrets([{ filename: 'src/pipeline.py', content: 'cfg = {"client_secret": "s3cr3tValue"}' }]);
        expect(dictLiteral.map(f => f.id)).toEqual(['SEC-CREDENTIAL-LITERAL-src/pipeline.py']);

        const sparkConf = scanForPlaintextSecrets([{ filename: 'src/pipeline.py', content: '.config("spark.hadoop.fs.s3a.secret.key", "abcd1234")' }]);
        expect(sparkConf).toHaveLength(1);
    });

    it('detects unquoted shell and Dockerfile credential assignments (failure condition)', () => {
        const findings = scanForPlaintextSecrets([
            { filename: 'run_dataproc_cluster.sh', content: '#!/usr/bin/env bash\nexport DB_PASSWORD=hunter22\n' },
            { filename: 'Dockerfile', content: 'FROM debian\nENV API_TOKEN=abcdef123\n' }
        ]);
        expect(findings.map(f => f.id)).toEqual([
            'SEC-SHELL-CREDENTIAL-run_dataproc_cluster.sh',
            'SEC-SHELL-CREDENTIAL-Dockerfile'
        ]);
    });

    it('detects plaintext JDBC URL passwords', () => {
        const findings = scanForPlaintextSecrets([
            { filename: 'src/pipeline.py', content: 'url = "jdbc:mysql://db:3306/x?user=a&password=Sup3rS3cret"' }
        ]);
        expect(findings.map(f => f.id)).toContain('SEC-JDBC-PW-src/pipeline.py');
    });

    it('skips mock credentials in test files and generic literals in markdown (boundary condition)', () => {
        expect(scanForPlaintextSecrets([
            { filename: 'tests/test_pipeline.py', content: 'cfg = {"password": "mock-password"}' },
            { filename: 'README.md', content: 'Set `password = "example-pass"` in your shell.' }
        ])).toEqual([]);
        expect(scanForPlaintextSecrets([
            { filename: 'tests/test_pipeline.py', content: 'cfg = {"password": "realProdPass"}' }
        ])).toHaveLength(1);
    });

    it('ignores short or empty values (boundary condition)', () => {
        expect(scanForPlaintextSecrets([
            { filename: 'src/pipeline.py', content: 'password = ""\ntoken = "abc"\n' }
        ])).toEqual([]);
    });
});

describe('runDeterministicGate', () => {
    it('passes a clean D3 repository (normal condition)', async () => {
        const res = await runDeterministicGate(buildRepo(), 'Python');
        expect(res.syntaxValid).toBe(true);
        expect(res.secretScanPassed).toBe(true);
        expect(res.missingDeliverables).toEqual([]);
        expect(res.structureFindings).toEqual([]);
        expect(res.bannedPatternFindings).toEqual([]);
        // NOTE: 2 Python files plus 3 shell scripts reach the mocked child process.
        expect(execFileMock).toHaveBeenCalledTimes(5);
        expect(execFileMock.mock.calls.filter(c => c[0] === 'python3')).toHaveLength(2);
        expect(execFileMock.mock.calls.filter(c => c[0] === 'bash')).toHaveLength(3);
    });

    it('defaults the language to Python and skips the deliverable audit for small payloads (boundary condition)', async () => {
        const res = await runDeterministicGate([{ filename: 'src/pipeline.py', content: VALID_PIPELINE }]);
        expect(res.syntaxValid).toBe(true);
        expect(res.missingDeliverables).toEqual([]);
    });

    it('rejects a Java target language (failure condition)', async () => {
        const res = await runDeterministicGate(buildRepo(), 'Java');
        expect(res.syntaxValid).toBe(false);
        expect(res.syntaxErrors[0]).toContain("Unsupported target language 'Java'");
    });

    it('fails the gate on banned patterns, structure defects, and syntax errors (failure condition)', async () => {
        execFileMock.mockImplementation((cmd: string, _a: string[], _o: unknown, cb: ExecCallback) => {
            if (cmd === 'python3') {
                cb(new Error('exit 1'), '', 'SyntaxError');
            } else {
                cb(null, '', '');
            }
        });
        const broken = 'spark = SparkSession.builder.master("local[*]").getOrCreate()\nconf = spark._jsc.hadoopConfiguration()\n';
        const res = await runDeterministicGate(buildRepo({ 'src/pipeline.py': broken, 'bad.json': '{' }), 'Python');

        expect(res.syntaxValid).toBe(false);
        expect(res.syntaxErrors.some(e => e.startsWith('Python syntax check failed for src/pipeline.py'))).toBe(true);
        expect(res.jsonErrors).toHaveLength(1);
        expect(res.secretScanPassed).toBe(false);
        expect(res.bannedPatternFindings.map(f => f.id)).toEqual([
            'BANNED-LOCAL-MASTER-src/pipeline.py',
            'BANNED-JVM-GATEWAY-src/pipeline.py'
        ]);
        expect(res.structureFindings.map(f => f.id)).toContain('STRUCT-TRANSFORM-SIGNATURE');
        expect(res.secretFindings).toEqual(expect.arrayContaining([
            ...res.bannedPatternFindings,
            ...res.structureFindings
        ]));
    });

    it('reports missing deliverables for an incomplete full payload', async () => {
        const repo = buildRepo().filter(f => f.filename !== 'run_serverless_spark.sh');
        const res = await runDeterministicGate(repo);
        expect(res.missingDeliverables).toEqual(['Missing mandatory deliverable: run_serverless_spark.sh']);
    });
});
