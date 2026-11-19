/**
 * Unit Tests for the Packaging & Deployability Evaluator.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies the deterministic packaging audits for generated PySpark 4.x repositories:
 * requirements parsing and the pyspark 4.x pin, runner script flags for the local,
 * Dataproc Cluster, and Serverless Spark runners, ExecutionPlan connector artifacts,
 * and the PackagingAndDeployabilityAgent merge logic. The LLM client is mocked, and the
 * runner fixtures are the bundled runner templates.
 */

import * as fs from 'fs';
import * as path from 'path';
import { describe, it, expect, vi } from 'vitest';
import {
    RUNNER_SCRIPT_BY_TYPE,
    SERVERLESS_SPARK4_RUNTIME_VERSIONS,
    auditConnectorArtifacts,
    auditRequirementsTxt,
    auditRunnerScripts,
    classifyPysparkSpecifier,
    pysparkPinnedToMinor,
    evaluatePackagingAndDeployability,
    extractServerlessRuntimeVersions,
    normalizePipName,
    parseRequirements,
    runDeterministicPackagingAudit
} from '../integration/evaluators/packaging-evaluator';
import { GeneratedFileCandidate } from '../integration/evaluators/deterministic-gate';
import { RUNTIME_PROFILES, type PipelineRuntimeArtifacts } from '../../spark/versionMatrix';

const TEMPLATE_DIR = path.resolve(__dirname, '../../../resources/templates/pyspark');

/**
 * Reads one bundled runner template.
 *
 * @param name - Template filename.
 * @returns Template content.
 */
function template(name: string): string {
    return fs.readFileSync(path.join(TEMPLATE_DIR, name), 'utf-8');
}

/**
 * Builds a generated repository from the bundled templates with optional overrides.
 *
 * @param overrides - Files that replace or extend the defaults. A null value removes the file.
 * @returns Generated file candidates.
 */
function buildRepo(overrides: Record<string, string | null> = {}): GeneratedFileCandidate[] {
    const defaults: Record<string, string> = {
        'requirements.txt': 'pyspark==4.0.*\npytest>=8.0.0\n',
        'run_local.sh': template('run_local.sh'),
        'run_dataproc_cluster.sh': template('run_dataproc_cluster.sh'),
        'run_serverless_spark.sh': template('run_serverless_spark.sh'),
        'Dockerfile': template('Dockerfile')
    };
    const merged: Record<string, string | null> = { ...defaults, ...overrides };
    return Object.entries(merged)
        .filter((entry): entry is [string, string] => entry[1] !== null)
        .map(([filename, content]) => ({ filename, content }));
}

/**
 * Builds runtime artifacts for a profile.
 *
 * @param profileId - Runtime profile identifier.
 * @param overrides - Fields to override.
 * @returns Runtime artifacts.
 */
function artifacts(profileId: keyof typeof RUNTIME_PROFILES, overrides: Partial<PipelineRuntimeArtifacts> = {}): PipelineRuntimeArtifacts {
    return {
        profile: RUNTIME_PROFILES[profileId],
        tier: 'standard',
        gcsJars: [],
        mavenPackages: [],
        pipPackages: [],
        sparkProperties: {},
        pythonDataSources: [],
        preinstalledConnectors: [],
        ...overrides
    };
}

describe('requirements parsing', () => {
    it('normalizes distribution names per PEP 503', () => {
        expect(normalizePipName('Google_Cloud.PubSub')).toBe('google-cloud-pubsub');
        expect(normalizePipName('  PySpark ')).toBe('pyspark');
    });

    it('parses names, extras, specifiers, markers, and line numbers (normal condition)', () => {
        const reqs = parseRequirements('# deps\npyspark[connect] >= 4.0.0, <5  # engine\n\n-r base.txt\npytest>=8.0.0 ; python_version >= "3.10"\nrequests\n');
        expect(reqs).toEqual([
            { name: 'pyspark', specifier: '>=4.0.0,<5', line: 2, raw: 'pyspark[connect] >= 4.0.0, <5  # engine' },
            { name: 'pytest', specifier: '>=8.0.0', line: 5, raw: 'pytest>=8.0.0 ; python_version >= "3.10"' },
            { name: 'requests', specifier: '', line: 6, raw: 'requests' }
        ]);
    });

    it('returns no entries for empty or comment-only content (boundary condition)', () => {
        expect(parseRequirements('')).toEqual([]);
        expect(parseRequirements('# nothing\n   \n--index-url https://pypi.org/simple\n')).toEqual([]);
    });
});

describe('classifyPysparkSpecifier', () => {
    it('accepts 4.x pins (normal condition)', () => {
        for (const spec of ['>=4.0.0,<5', '==4.0.1', '~=4.0', '==4.*', '>=4.1', '>4.0,<=4.2', '>=4.0,!=4.0.0']) {
            expect(classifyPysparkSpecifier(spec)).toBe('ok');
        }
    });

    it('reports unpinned, upper-bound-only, and direct references as unpinned (boundary condition)', () => {
        expect(classifyPysparkSpecifier('')).toBe('unpinned');
        expect(classifyPysparkSpecifier('<5')).toBe('unpinned');
        expect(classifyPysparkSpecifier('@https://example.com/pyspark.tar.gz')).toBe('unpinned');
    });

    it('rejects Spark 3.x and 5.x pins and bounds that exclude 4.x (failure condition)', () => {
        expect(classifyPysparkSpecifier('==3.5.1')).toContain('pins major version 3');
        expect(classifyPysparkSpecifier('>=3.5')).toContain('pins major version 3');
        expect(classifyPysparkSpecifier('>=5.0')).toContain('pins major version 5');
        expect(classifyPysparkSpecifier('>=4.0,<4.0')).toContain("upper bound '<4.0'");
        expect(classifyPysparkSpecifier('>=4.0,<=3.9')).toContain("upper bound '<=3.9'");
    });
});

describe('pysparkPinnedToMinor', () => {
    it('accepts pins that stay on the Spark 4.0 minor line (normal condition)', () => {
        for (const spec of ['==4.0.*', '==4.0.1', '===4.0.1', '~=4.0.1', '>=4.0.0,<4.1', '>=4.0,<4.1.0', '>=4.0.1,<=4.0.9', '>=4.0,<4.0.5']) {
            expect(pysparkPinnedToMinor(spec, '4.0'), spec).toBe(true);
        }
    });

    it('rejects open ranges and wider compatible-release pins (boundary condition)', () => {
        for (const spec of ['', '>=4.0.0', '>=4.0.0,<5', '~=4.0', '==4.*', '>=4.0,<4.1.1', '>=4.0,!=4.0.0']) {
            expect(pysparkPinnedToMinor(spec, '4.0'), spec).toBe(false);
        }
    });

    it('rejects other minors and malformed clauses (failure condition)', () => {
        for (const spec of ['==4.1.*', '~=4.1.0', '>=4.1,<4.2', '==3.5.1', '@https://example.com/pyspark.tar.gz']) {
            expect(pysparkPinnedToMinor(spec, '4.0'), spec).toBe(false);
        }
        expect(pysparkPinnedToMinor('==4.1.*', '4.1')).toBe(true);
    });
});

describe('auditRequirementsTxt', () => {
    it('accepts a pinned pyspark 4.x with pytest (normal condition)', () => {
        expect(auditRequirementsTxt(buildRepo())).toEqual([]);
    });

    it('reports a missing requirements file as CRITICAL (failure condition)', () => {
        const findings = auditRequirementsTxt(buildRepo({ 'requirements.txt': null }));
        expect(findings.map(f => f.id)).toEqual(['PKG-REQ-MISSING']);
        expect(findings[0].severity).toBe('CRITICAL');
    });

    it('reports missing pyspark, missing pytest, and an apache-beam leftover (failure condition)', () => {
        const findings = auditRequirementsTxt(buildRepo({ 'requirements.txt': 'apache_beam[gcp]==2.60.0\n' }));
        expect(findings.map(f => [f.id, f.severity])).toEqual([
            ['PKG-REQ-PYSPARK-MISSING', 'CRITICAL'],
            ['PKG-REQ-PYTEST-MISSING', 'MAJOR'],
            ['PKG-REQ-BEAM-LEFTOVER', 'CRITICAL']
        ]);
        expect(findings[2].line).toBe(1);
    });

    it('reports an unpinned pyspark as MAJOR and a 3.x pin as CRITICAL with evidence', () => {
        const unpinned = auditRequirementsTxt(buildRepo({ 'requirements.txt': 'pytest\npyspark\n' }));
        expect(unpinned.map(f => f.id)).toEqual(['PKG-REQ-PYSPARK-UNPINNED']);
        expect(unpinned[0].line).toBe(2);

        const old = auditRequirementsTxt(buildRepo({ 'requirements.txt': 'pyspark==3.5.1\npytest\n' }));
        expect(old[0].id).toBe('PKG-REQ-PYSPARK-VERSION');
        expect(old[0].severity).toBe('CRITICAL');
        expect(old[0].evidence).toBe('pyspark==3.5.1');
    });

    it('reports an open-ended pyspark range as MAJOR because pip installs a newer Spark minor (failure condition)', () => {
        const findings = auditRequirementsTxt(buildRepo({ 'requirements.txt': 'pyspark>=4.0.0\npytest>=8.0.0\n' }));
        expect(findings.map(f => [f.id, f.severity])).toEqual([['PKG-REQ-PYSPARK-MINOR', 'MAJOR']]);
        expect(findings[0].evidence).toBe('pyspark>=4.0.0');
        expect(findings[0].rationale).toContain('pyspark==4.0.*');
    });

    it('requires every ExecutionPlan pip package by normalized name (boundary condition)', () => {
        const files = buildRepo({ 'requirements.txt': 'pyspark==4.0.1\npytest\nGoogle_Cloud_PubSub>=2.0\n' });
        const findings = auditRequirementsTxt(files, ['google-cloud-pubsub>=2.21.0', 'redis']);
        expect(findings.map(f => f.id)).toEqual(['PKG-REQ-PIP-redis']);
    });
});

describe('auditRunnerScripts', () => {
    it('accepts the bundled runner templates (normal condition)', () => {
        expect(auditRunnerScripts(buildRepo())).toEqual([]);
    });

    it('reports missing runner scripts as CRITICAL (failure condition)', () => {
        const ids = auditRunnerScripts(buildRepo({ 'run_local.sh': null, 'run_serverless_spark.sh': null })).map(f => f.id);
        expect(ids).toEqual(['PKG-RUNNER-MISSING-run_local.sh', 'PKG-RUNNER-MISSING-run_serverless_spark.sh']);
    });

    it('reports missing cluster flags and ignores flags that only appear in comments (failure condition)', () => {
        const script = [
            '#!/usr/bin/env bash',
            'set -euo pipefail',
            '# Use --region, --jars, and --properties here.',
            'gcloud dataproc jobs submit pyspark src/pipeline.py --cluster="${CLUSTER_NAME}"',
            ''
        ].join('\n');
        const findings = auditRunnerScripts(buildRepo({ 'run_dataproc_cluster.sh': script }));
        expect(findings.map(f => [f.id, f.severity])).toEqual([
            ['PKG-RUNNER-REGION-run_dataproc_cluster.sh', 'MAJOR'],
            ['PKG-RUNNER-JARS-run_dataproc_cluster.sh', 'MAJOR'],
            ['PKG-RUNNER-PROPERTIES-run_dataproc_cluster.sh', 'MAJOR']
        ]);
    });

    it('reports a wrong submit command, a missing entry point, Beam tokens, and no strict mode', () => {
        const script = '#!/usr/bin/env bash\npython3 -m pipeline --runner=DataflowRunner --region=us-central1 --version=4.0 --jars=a --properties=b\n';
        const findings = auditRunnerScripts(buildRepo({ 'run_serverless_spark.sh': script }));
        expect(findings.map(f => [f.id, f.severity])).toEqual([
            ['PKG-RUNNER-SUBMIT-run_serverless_spark.sh', 'CRITICAL'],
            ['PKG-RUNNER-STRICT-run_serverless_spark.sh', 'MINOR'],
            ['PKG-RUNNER-ENTRYPOINT-run_serverless_spark.sh', 'CRITICAL'],
            ['PKG-RUNNER-BEAM-run_serverless_spark.sh', 'CRITICAL'],
            ['PKG-RUNNER-REGION-HARDCODED-run_serverless_spark.sh', 'MINOR']
        ]);
        expect(findings[3].evidence).toBe('--runner');
    });

    it('reports a Serverless runtime version without Spark 4.x as CRITICAL (failure condition)', () => {
        const script = template('run_serverless_spark.sh').replace('RUNTIME_VERSION:-4.0', 'RUNTIME_VERSION:-2.2');
        const findings = auditRunnerScripts(buildRepo({ 'run_serverless_spark.sh': script }));
        expect(findings.map(f => f.id)).toEqual(['PKG-RUNNER-RUNTIME-VERSION-2.2']);
        expect(findings[0].severity).toBe('CRITICAL');
    });

    it('requires --jars and --packages forwarding in run_local.sh', () => {
        const script = '#!/usr/bin/env bash\nset -euo pipefail\nexec spark-submit src/pipeline.py "$@"\n';
        const ids = auditRunnerScripts(buildRepo({ 'run_local.sh': script })).map(f => f.id);
        expect(ids).toEqual([
            'PKG-RUNNER-JARS-run_local.sh',
            'PKG-RUNNER-PACKAGES-run_local.sh',
            'PKG-RUNNER-VENV-run_local.sh',
            'PKG-RUNNER-JDK-run_local.sh',
            'PKG-RUNNER-IVY-run_local.sh'
        ]);
    });

    it('accepts the bundled run_local.sh with venv, JDK, and Ivy bootstrap (normal condition)', () => {
        expect(auditRunnerScripts(buildRepo())).toEqual([]);
    });

    it('reports a run_local.sh that drops the venv, JDK, and Ivy bootstrap as MAJOR (failure condition)', () => {
        const script = '#!/usr/bin/env bash\nset -euo pipefail\nexec spark-submit --jars "${SPARK_JARS}" --packages "${SPARK_PACKAGES}" src/pipeline.py "$@"\n';
        const findings = auditRunnerScripts(buildRepo({ 'run_local.sh': script }));
        expect(findings.map(f => [f.id, f.severity])).toEqual([
            ['PKG-RUNNER-VENV-run_local.sh', 'MAJOR'],
            ['PKG-RUNNER-JDK-run_local.sh', 'MAJOR'],
            ['PKG-RUNNER-IVY-run_local.sh', 'MAJOR']
        ]);
    });

    it('accepts set -eu -o pipefail as strict mode (boundary condition)', () => {
        const script = template('run_local.sh').replace('set -euo pipefail', 'set -eu -o pipefail');
        expect(auditRunnerScripts(buildRepo({ 'run_local.sh': script }))).toEqual([]);
    });
});

describe('extractServerlessRuntimeVersions', () => {
    it('extracts flag values and env defaults, deduplicated', () => {
        expect(extractServerlessRuntimeVersions('--version=4.0\nRUNTIME_VERSION="${RUNTIME_VERSION:-4.0}"\n--version 3.0\nRUNTIME_VERSION=2.2\n'))
            .toEqual(['4.0', '3.0', '2.2']);
    });

    it('returns nothing when the version comes only from a variable (boundary condition)', () => {
        expect(extractServerlessRuntimeVersions('"--version=${RUNTIME_VERSION}"')).toEqual([]);
    });

    it('derives the valid versions from the runtime profiles', () => {
        expect([...SERVERLESS_SPARK4_RUNTIME_VERSIONS].sort()).toEqual(['3.0', '4.0']);
        expect(RUNNER_SCRIPT_BY_TYPE.ServerlessSpark).toBe('run_serverless_spark.sh');
    });
});

describe('auditConnectorArtifacts', () => {
    const SPANNER_JAR = 'gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar';
    const KAFKA_PKG = 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0';

    it('accepts artifacts supplied as defaults in the target runner script (normal condition)', () => {
        const script = template('run_serverless_spark.sh')
            .replace('SPARK_JARS="${SPARK_JARS:-}"', `SPARK_JARS="\${SPARK_JARS:-${SPANNER_JAR}}"`)
            .replace('SPARK_PACKAGES="${SPARK_PACKAGES:-}"', `SPARK_PACKAGES="\${SPARK_PACKAGES:-${KAFKA_PKG}}"`)
            .replace('SPARK_PROPERTIES="${SPARK_PROPERTIES:-}"', 'SPARK_PROPERTIES="${SPARK_PROPERTIES:-spark.sql.ansi.enabled=true}"');
        const art = artifacts('serverless_4.0', {
            gcsJars: [SPANNER_JAR],
            mavenPackages: [KAFKA_PKG],
            sparkProperties: { 'spark.sql.ansi.enabled': 'true' }
        });
        expect(auditConnectorArtifacts(buildRepo({ 'run_serverless_spark.sh': script }), art)).toEqual([]);
    });

    it('checks only the runner script of the target profile (boundary condition)', () => {
        const art = artifacts('cluster_3.0', { gcsJars: [SPANNER_JAR] });
        const serverless = template('run_serverless_spark.sh').replace('SPARK_JARS="${SPARK_JARS:-}"', `SPARK_JARS="\${SPARK_JARS:-${SPANNER_JAR}}"`);
        const findings = auditConnectorArtifacts(buildRepo({ 'run_serverless_spark.sh': serverless }), art);
        expect(findings.map(f => [f.id, f.file])).toEqual([[`PKG-ARTIFACT-${SPANNER_JAR}`, 'run_dataproc_cluster.sh']]);
    });

    it('reports missing JARs, packages, and properties (failure condition)', () => {
        const art = artifacts('local', {
            gcsJars: [SPANNER_JAR],
            mavenPackages: [KAFKA_PKG],
            sparkProperties: { 'spark.sql.session.timeZone': 'UTC' }
        });
        const findings = auditConnectorArtifacts(buildRepo(), art);
        expect(findings.map(f => f.id)).toEqual([
            `PKG-ARTIFACT-${SPANNER_JAR}`,
            `PKG-ARTIFACT-${KAFKA_PKG}`,
            'PKG-ARTIFACT-spark.sql.session.timeZone'
        ]);
        expect(findings.every(f => f.file === 'run_local.sh' && f.severity === 'MAJOR')).toBe(true);
    });

    it('reports pre-installed Managed Spark connectors passed via packages (failure condition)', () => {
        const script = template('run_dataproc_cluster.sh').replace(
            'SPARK_PACKAGES="${SPARK_PACKAGES:-}"',
            'SPARK_PACKAGES="${SPARK_PACKAGES:-com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.42.1}"'
        );
        const findings = auditConnectorArtifacts(buildRepo({ 'run_dataproc_cluster.sh': script }), artifacts('cluster_3.0'));
        expect(findings.map(f => f.id)).toEqual(['PKG-PREINSTALLED-spark-bigquery-run_dataproc_cluster.sh']);
        expect(findings[0].line).toBeGreaterThan(1);
    });

    it('allows the BigQuery Maven package in run_local.sh (boundary condition)', () => {
        const script = template('run_local.sh').replace(
            'SPARK_PACKAGES="${SPARK_PACKAGES:-}"',
            'SPARK_PACKAGES="${SPARK_PACKAGES:-com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.42.1}"'
        );
        expect(auditConnectorArtifacts(buildRepo({ 'run_local.sh': script }), artifacts('local'))).toEqual([]);
    });

    it('reports Scala 2.12 and Spark 3.x connector builds once per file (failure condition)', () => {
        const files = buildRepo({
            'src/pipeline.py': '# org.example_2.12 in a comment is ignored\nspark.conf.set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")\n',
            'run_local.sh': template('run_local.sh').replace('SPARK_JARS="${SPARK_JARS:-}"', 'SPARK_JARS="${SPARK_JARS:-gs://lib/spark-3.5-spanner-1.0.jar}"')
        });
        const findings = auditConnectorArtifacts(files, artifacts('cluster_3.0'));
        expect(findings.map(f => [f.id, f.severity])).toEqual([
            ['PKG-LEGACY-CLASSPATH-run_local.sh', 'CRITICAL'],
            ['PKG-LEGACY-CLASSPATH-src/pipeline.py', 'CRITICAL']
        ]);
        expect(findings[1].line).toBe(2);
    });
});

describe('runDeterministicPackagingAudit', () => {
    it('returns no findings for the templates without artifacts (normal condition)', () => {
        expect(runDeterministicPackagingAudit(buildRepo())).toEqual([]);
        expect(runDeterministicPackagingAudit(buildRepo(), null)).toEqual([]);
    });

    it('passes ExecutionPlan pip packages to the requirements audit', () => {
        const ids = runDeterministicPackagingAudit(buildRepo(), artifacts('serverless_4.0', { pipPackages: ['redis'] })).map(f => f.id);
        expect(ids).toEqual(['PKG-REQ-PIP-redis']);
    });
});

describe('evaluatePackagingAndDeployability', () => {
    /**
     * Builds a mocked GoogleGenAI client that returns one JSON payload.
     *
     * @param payload - Model response object or raw text.
     * @returns Mock client.
     */
    const mockClient = (payload: unknown) => {
        const generateContent = vi.fn().mockResolvedValue({
            text: typeof payload === 'string' ? payload : JSON.stringify(payload)
        });
        const client = { models: { generateContent } };
        return client as typeof client & Parameters<typeof evaluatePackagingAndDeployability>[0];
    };

    const SPANNER_YAML = `
nodes:
  - id: src
    data: { label: Orders, type: SOURCE, spark_transform: ParquetSource }
  - id: out
    data: { label: Spanner, type: SINK, spark_transform: SpannerSink }
edges:
  - { source: src, target: out }
jobConfig: { runner: DataprocCluster }
`;

    it('passes when the model and the deterministic audits are clean (normal condition)', async () => {
        const ai = mockClient({ passed: true, summary: 'Deployable.', findings: [] });
        const result = await evaluatePackagingAndDeployability(ai, 'Python', buildRepo());
        expect(result.agentName).toBe('PackagingAndDeployabilityAgent');
        expect(result.passed).toBe(true);
        expect(result.findings).toEqual([]);
        expect(result.summary).toBe('Deployable.');
        const request = ai.models.generateContent.mock.calls[0][0];
        expect(request.contents).toContain('run_serverless_spark.sh');
        expect(request.contents).toContain('EXECUTIONPLAN RUNTIME ARTIFACTS ===\nnot available');
        expect(request.contents).not.toMatch(/Dataflow|pom\.xml|metadata\.json|cloudbuild/);
    });

    it('audits ExecutionPlan connector JARs when the YAML is supplied (failure condition)', async () => {
        const ai = mockClient({ passed: true, summary: 'Deployable.', findings: [] });
        const result = await evaluatePackagingAndDeployability(ai, 'Python', buildRepo(), SPANNER_YAML);
        expect(result.passed).toBe(false);
        expect(result.findings.map(f => f.id)).toEqual(['PKG-ARTIFACT-gs://spark-lib/spanner/spark-4.0-spanner-1.5.0.jar']);
        expect(result.summary).toBe('Deployable. Deterministic audit: 1 finding(s).');
        const request = ai.models.generateContent.mock.calls[0][0];
        expect(request.contents).toContain('Runtime profile: cluster_3.0');
        expect(request.contents).toContain('[MAJOR] PKG-ARTIFACT-');
    });

    it('keeps passing with only MINOR deterministic findings and appends model findings (boundary condition)', async () => {
        const ai = mockClient({
            passed: true,
            summary: 'Minor nit.',
            findings: [{ id: 'PKG-1', category: 'PACKAGING', severity: 'MINOR', file: 'Dockerfile', description: 'No comment', rationale: 'Style' }]
        });
        const local = template('run_local.sh').replace('set -euo pipefail', '');
        const result = await evaluatePackagingAndDeployability(ai, 'Python', buildRepo({ 'run_local.sh': local }));
        expect(result.passed).toBe(true);
        expect(result.findings.map(f => f.id)).toEqual(['PKG-RUNNER-STRICT-run_local.sh', 'PKG-1']);
    });

    it('fails when the model fails even if the deterministic audits are clean', async () => {
        const ai = mockClient({ passed: false, summary: 'Broken Dockerfile.', findings: [] });
        const result = await evaluatePackagingAndDeployability(ai, 'Python', buildRepo(), 'not: [valid');
        expect(result.passed).toBe(false);
    });

    it('throws when the model returns empty text (failure condition)', async () => {
        await expect(evaluatePackagingAndDeployability(mockClient(''), 'Python', buildRepo()))
            .rejects.toThrow('Empty response from PackagingAndDeployabilityAgent');
    });
});
