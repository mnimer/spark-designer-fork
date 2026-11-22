/**
 * Unit Tests for the Spark Architecture Evaluator Sub-Agent (`SparkArchitectureAgent`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies the deterministic static checks (D3 separation, pure transform, Catalyst rules,
 * cache pairing, foreachBatch idempotency, watermarks, Managed Spark classpath), the prompt
 * builder, the response parser, and the agent entry point with a mocked model client.
 * All tests run offline.
 */

import { describe, it, expect, vi } from 'vitest';
import { GoogleGenAI } from '@google/genai';
import {
    SPARK_ARCHITECTURE_AGENT_NAME,
    buildSparkArchitecturePrompt,
    collectSparkArchitectureSignals,
    evaluateSparkArchitecture,
    extractPythonFunctionBody,
    getPipelineSourceFiles,
    isPythonTestFile,
    mergeArchitectureFindings,
    parseSparkArchitectureResponse,
    runSparkArchitectureStaticChecks,
    selectMainPipelineFile,
} from '../integration/evaluators/spark-architecture-evaluator';
import { GeneratedFileCandidate } from '../integration/evaluators/deterministic-gate';
import { EvaluatorFinding } from '../integration/evaluators/types';
import { CLEAN_PIPELINE_PY, cleanSparkFiles } from './helpers/sparkPipelineFixtures';

/**
 * Replaces the content of one generated file.
 *
 * @param filename - File to replace.
 * @param content - New content.
 * @returns Clean repository with one changed file.
 */
function withFile(filename: string, content: string): GeneratedFileCandidate[] {
    const files = cleanSparkFiles().filter(f => f.filename !== filename);
    return [...files, { filename, content }];
}

/**
 * Returns the ids of the static findings with a rule prefix.
 *
 * @param findings - Findings.
 * @param rule - Rule name after `SPARK-ARCH-`.
 * @returns Matching findings.
 */
function byRule(findings: EvaluatorFinding[], rule: string): EvaluatorFinding[] {
    return findings.filter(f => f.id.startsWith(`SPARK-ARCH-${rule}-`));
}

describe('file classification helpers', () => {
    it('classifies Python test files (normal and boundary conditions)', () => {
        expect(isPythonTestFile('tests/test_pipeline.py')).toBe(true);
        expect(isPythonTestFile('src/pipeline_test.py')).toBe(true);
        expect(isPythonTestFile('test_utils.py')).toBe(true);
        expect(isPythonTestFile('src\\tests\\conftest.py')).toBe(true);
        expect(isPythonTestFile('src/pipeline.py')).toBe(false);
        expect(isPythonTestFile('src/contest.py')).toBe(false);
    });

    it('selects src/pipeline.py as the main pipeline file and ignores tests and __init__.py', () => {
        const files: GeneratedFileCandidate[] = [
            { filename: 'src/__init__.py', content: '' },
            { filename: 'src/helpers.py', content: 'x = 1' },
            { filename: 'src/pipeline.py', content: 'y = 2' },
            { filename: 'tests/test_pipeline.py', content: 'z = 3' },
        ];
        expect(getPipelineSourceFiles(files).map(f => f.filename)).toEqual(['src/helpers.py', 'src/pipeline.py']);
        expect(selectMainPipelineFile(files)?.filename).toBe('src/pipeline.py');
    });

    it('returns undefined when no pipeline source exists (extreme condition)', () => {
        expect(selectMainPipelineFile([{ filename: 'README.md', content: '# x' }])).toBeUndefined();
        expect(selectMainPipelineFile([])).toBeUndefined();
    });
});

describe('extractPythonFunctionBody', () => {
    it('extracts the indented body and parameters of a function (simple condition)', () => {
        const body = extractPythonFunctionBody(CLEAN_PIPELINE_PY, 'transform');
        expect(body?.params).toContain('dfs: dict[str, DataFrame]');
        expect(body?.lines.map(l => l.text.trim())[0]).toBe('"""Applies pure transformations."""');
        expect(body?.lines.some(l => l.text.includes('def write_sinks'))).toBe(false);
    });

    it('keeps blank lines inside the body and stops at dedent (boundary condition)', () => {
        const src = 'def f(a):\n    x = 1\n\n    y = 2\nz = 3\n';
        const body = extractPythonFunctionBody(src, 'f');
        expect(body?.defLine).toBe(1);
        expect(body?.lines.map(l => l.line)).toEqual([2, 4]);
    });

    it('does not match a function whose name only shares a prefix (failure condition)', () => {
        expect(extractPythonFunctionBody('def transform_rows(x):\n    pass\n', 'transform')).toBeNull();
        expect(extractPythonFunctionBody('', 'transform')).toBeNull();
    });
});

describe('runSparkArchitectureStaticChecks', () => {
    it('reports zero findings for a clean D3 repository (simple condition)', () => {
        expect(runSparkArchitectureStaticChecks(cleanSparkFiles())).toEqual([]);
    });

    it('reports zero findings for an empty repository (extreme condition)', () => {
        expect(runSparkArchitectureStaticChecks([])).toEqual([]);
    });

    describe('D3 separation and pure transform', () => {
        it('flags missing read_sources and write_sinks', () => {
            const findings = runSparkArchitectureStaticChecks(
                withFile('src/pipeline.py', 'def transform(dfs, config):\n    return dfs\n')
            );
            const sep = byRule(findings, 'SEPARATION');
            expect(sep).toHaveLength(1);
            expect(sep[0].severity).toBe('MAJOR');
            expect(sep[0].description).toContain('`read_sources`');
            expect(sep[0].description).toContain('`write_sinks`');
            expect(sep[0].description).not.toContain('`transform`');
        });

        it('flags a transform signature without dfs as the first parameter', () => {
            const src = CLEAN_PIPELINE_PY.replace('def transform(dfs: dict[str, DataFrame], config', 'def transform(spark, config');
            const sig = byRule(runSparkArchitectureStaticChecks(withFile('src/pipeline.py', src)), 'TRANSFORM-SIGNATURE');
            expect(sig).toHaveLength(1);
            expect(sig[0].evidence).toContain('def transform(spark, config');
        });

        it('flags reads and writes inside transform with exact line numbers', () => {
            const src = CLEAN_PIPELINE_PY.replace(
                '    return {"enriched": enriched}',
                '    lookup = spark.read.table("dim")\n    enriched.write.saveAsTable("out")\n    return {"enriched": enriched}'
            );
            const io = byRule(runSparkArchitectureStaticChecks(withFile('src/pipeline.py', src)), 'TRANSFORM-IO');
            expect(io).toHaveLength(2);
            expect(io[0].line).toBe(17);
            expect(io[0].evidence).toBe('lookup = spark.read.table("dim")');
        });

        it('flags spark.table() reads and writeTo() writes inside transform', () => {
            const src = CLEAN_PIPELINE_PY.replace(
                '    return {"enriched": enriched}',
                '    dim = spark.table("dim")\n    enriched.writeTo("cat.db.out").append()\n    return {"enriched": enriched}'
            );
            expect(byRule(runSparkArchitectureStaticChecks(withFile('src/pipeline.py', src)), 'TRANSFORM-IO')).toHaveLength(2);
        });

        it('ignores I/O mentioned only in a comment inside transform (boundary condition)', () => {
            const src = CLEAN_PIPELINE_PY.replace(
                '    return {"enriched": enriched}',
                '    # never call spark.read here\n    return {"enriched": enriched}'
            );
            expect(byRule(runSparkArchitectureStaticChecks(withFile('src/pipeline.py', src)), 'TRANSFORM-IO')).toEqual([]);
        });
    });

    describe('Catalyst optimization rules', () => {
        it('flags collect() and toPandas() in pipeline code but not in tests', () => {
            const src = CLEAN_PIPELINE_PY.replace(
                '    return {"enriched": enriched}',
                '    rows = orders.collect()\n    pdf = orders.toPandas()\n    return {"enriched": enriched}'
            );
            const collect = byRule(runSparkArchitectureStaticChecks(withFile('src/pipeline.py', src)), 'DRIVER-COLLECT');
            expect(collect.map(f => f.evidence)).toEqual(['rows = orders.collect()', 'pdf = orders.toPandas()']);
            expect(collect.every(f => f.category === 'IDIOMATIC_SPARK' && f.severity === 'MAJOR')).toBe(true);
            // The clean fixture test file collects a small fixture and stays clean.
            expect(byRule(runSparkArchitectureStaticChecks(cleanSparkFiles()), 'DRIVER-COLLECT')).toEqual([]);
        });

        it('flags RDD and SparkContext usage', () => {
            const src = CLEAN_PIPELINE_PY.replace(
                '    return {"enriched": enriched}',
                '    counts = orders.rdd.map(lambda r: r)\n    sc = spark.sparkContext\n    return {"enriched": enriched}'
            );
            expect(byRule(runSparkArchitectureStaticChecks(withFile('src/pipeline.py', src)), 'RDD-API')).toHaveLength(2);
        });

        it('flags row-at-a-time Python UDFs as MINOR and ignores pandas_udf', () => {
            const src = [
                'from pyspark.sql.functions import udf, pandas_udf',
                '@udf("string")',
                'def upper(s):',
                '    return s.upper()',
                'lower = F.udf(lambda s: s.lower())',
                '@pandas_udf("double")',
                'def plus_one(v):',
                '    return v + 1',
                'spark.udf.register("x", upper)',
            ].join('\n');
            const udf = byRule(runSparkArchitectureStaticChecks(withFile('src/helpers.py', src)), 'PYTHON-UDF');
            expect(udf.map(f => f.line)).toEqual([2, 5]);
            expect(udf.every(f => f.severity === 'MINOR')).toBe(true);
        });
    });

    describe('cache and unpersist pairing', () => {
        it('flags cache() without unpersist()', () => {
            const src = CLEAN_PIPELINE_PY.replace('        dfs["enriched"].unpersist()', '        pass');
            const cache = byRule(runSparkArchitectureStaticChecks(withFile('src/pipeline.py', src)), 'CACHE-UNPERSIST');
            expect(cache).toHaveLength(1);
            expect(cache[0].line).toBe(16);
            expect(cache[0].description).toContain('1 DataFrame(s)');
        });

        it('accepts persist() paired with unpersist() (boundary condition)', () => {
            const src = CLEAN_PIPELINE_PY.replace('.cache()', '.persist(StorageLevel.MEMORY_AND_DISK)');
            expect(byRule(runSparkArchitectureStaticChecks(withFile('src/pipeline.py', src)), 'CACHE-UNPERSIST')).toEqual([]);
        });
    });

    describe('foreachBatch idempotency', () => {
        const base = [
            'def write_micro_batch(batch_df, batch_id):',
            '    (batch_df.write.format("delta")',
            '        .mode("append")',
            'TXN',
            '        .save(path))',
            'query = stream_df.writeStream.foreachBatch(write_micro_batch).start()',
        ].join('\n');

        it('accepts txnVersion bound to batch_id with txnAppId', () => {
            const src = base.replace('TXN', '        .option("txnVersion", str(batch_id)).option("txnAppId", app_id)');
            expect(byRule(runSparkArchitectureStaticChecks(withFile('src/sinks.py', src)), 'FOREACHBATCH-TXN')).toEqual([]);
        });

        it('flags a Delta foreachBatch write without txnVersion and txnAppId', () => {
            const txn = byRule(runSparkArchitectureStaticChecks(withFile('src/sinks.py', base.replace('TXN', ''))), 'FOREACHBATCH-TXN');
            expect(txn).toHaveLength(1);
            expect(txn[0].description).toContain('txnVersion and txnAppId');
            expect(txn[0].line).toBe(6);
        });

        it('flags a constant txnVersion', () => {
            const src = base.replace('TXN', '        .option("txnVersion", "1").option("txnAppId", app_id)');
            const txn = byRule(runSparkArchitectureStaticChecks(withFile('src/sinks.py', src)), 'FOREACHBATCH-TXN');
            expect(txn).toHaveLength(1);
            expect(txn[0].description).toContain('constant');
        });

        it('ignores foreachBatch without a Delta write (boundary condition)', () => {
            const src = base.replace('format("delta")', 'format("jdbc")').replace('TXN', '');
            expect(byRule(runSparkArchitectureStaticChecks(withFile('src/sinks.py', src)), 'FOREACHBATCH-TXN')).toEqual([]);
        });
    });

    describe('watermark presence', () => {
        const streaming = [
            'events = spark.readStream.format("kafka").load()',
            'counts = events.groupBy(F.window("ts", "5 minutes")).count()',
        ].join('\n');

        it('flags a streaming window without withWatermark', () => {
            const wm = byRule(runSparkArchitectureStaticChecks(withFile('src/stream.py', streaming)), 'WATERMARK');
            expect(wm).toHaveLength(1);
            expect(wm[0].line).toBe(2);
        });

        it('accepts a streaming window with withWatermark', () => {
            const src = streaming.replace('events.groupBy', 'events.withWatermark("ts", "10 minutes").groupBy');
            expect(byRule(runSparkArchitectureStaticChecks(withFile('src/stream.py', src)), 'WATERMARK')).toEqual([]);
        });

        it('ignores batch windows (boundary condition)', () => {
            const src = streaming.replace('readStream', 'read');
            expect(byRule(runSparkArchitectureStaticChecks(withFile('src/stream.py', src)), 'WATERMARK')).toEqual([]);
        });
    });

    describe('Managed Spark classpath rules', () => {
        it('flags Scala 2.12 artifacts as CRITICAL in runner scripts', () => {
            const runner = 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:4.0.0 src/pipeline.py\n';
            const cp = byRule(runSparkArchitectureStaticChecks(withFile('run_local.sh', runner)), 'CLASSPATH');
            expect(cp).toHaveLength(1);
            expect(cp[0].severity).toBe('CRITICAL');
            expect(cp[0].description).toContain('Scala 2.11 or 2.12');
        });

        it('flags Spark 3.x connector artifacts as CRITICAL', () => {
            const runner = 'spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.5.0 src/pipeline.py\n';
            const cp = byRule(runSparkArchitectureStaticChecks(withFile('run_local.sh', runner)), 'CLASSPATH');
            expect(cp).toHaveLength(1);
            expect(cp[0].description).toContain('Spark 3.x');
        });

        it('flags pre-installed connectors on Managed Spark runners only', () => {
            const runner = 'gcloud dataproc batches submit pyspark src/pipeline.py --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.42.1.jar\n';
            const managed = byRule(runSparkArchitectureStaticChecks(withFile('run_serverless_spark.sh', runner)), 'CLASSPATH');
            expect(managed).toHaveLength(1);
            expect(managed[0].description).toContain('spark-bigquery');
            expect(byRule(runSparkArchitectureStaticChecks(withFile('run_local.sh', runner)), 'CLASSPATH')).toEqual([]);
        });

        it('flags spark.jars.packages set inside pipeline code', () => {
            const src = CLEAN_PIPELINE_PY + 'spark = SparkSession.builder.config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0").getOrCreate()\n';
            const cp = byRule(runSparkArchitectureStaticChecks(withFile('src/pipeline.py', src)), 'CLASSPATH');
            expect(cp).toHaveLength(1);
            expect(cp[0].severity).toBe('MAJOR');
        });

        it('ignores incompatible coordinates mentioned in markdown docs (boundary condition)', () => {
            const readme = 'Do not use `delta-core_2.12:2.4.0` on Spark 4.\n';
            expect(byRule(runSparkArchitectureStaticChecks(withFile('README.md', readme)), 'CLASSPATH')).toEqual([]);
        });
    });
});

describe('collectSparkArchitectureSignals', () => {
    it('summarizes a clean batch repository', () => {
        expect(collectSparkArchitectureSignals(cleanSparkFiles())).toEqual({
            pipelineFile: 'src/pipeline.py',
            hasReadSources: true,
            hasTransform: true,
            hasWriteSinks: true,
            isStreaming: false,
            usesWatermark: false,
            usesForeachBatch: false,
            cacheCalls: 1,
            unpersistCalls: 1,
            pythonUdfCount: 0,
        });
    });

    it('returns empty signals without a pipeline file (extreme condition)', () => {
        const signals = collectSparkArchitectureSignals([]);
        expect(signals.pipelineFile).toBeUndefined();
        expect(signals.hasTransform).toBe(false);
        expect(signals.cacheCalls).toBe(0);
    });
});

describe('buildSparkArchitecturePrompt', () => {
    it('embeds the checklist, the Spark version, line-numbered files, and static findings', () => {
        const files = cleanSparkFiles();
        const staticFindings: EvaluatorFinding[] = [{
            id: 'SPARK-ARCH-X-1', category: 'ARCHITECTURE', severity: 'MAJOR', file: 'src/pipeline.py', description: 'static defect', rationale: 'r',
        }];
        const prompt = buildSparkArchitecturePrompt('Python', 'nodes: []', files, staticFindings, collectSparkArchitectureSignals(files), '4.0.1');
        expect(prompt).toContain('Spark Architecture Evaluator Agent');
        expect(prompt).toContain('Apache Spark 4.0.1');
        expect(prompt).toContain('--- FILE: src/pipeline.py ---');
        expect(prompt).toContain('SPARK-ARCH-X-1');
        expect(prompt).toContain('txnVersion');
        expect(prompt).toContain('withWatermark');
        expect(prompt).toContain('Scala 2.12');
        expect(prompt).toContain('transform(dfs, config)');
    });
});

describe('parseSparkArchitectureResponse', () => {
    it('parses a valid response (simple condition)', () => {
        const parsed = parseSparkArchitectureResponse(JSON.stringify({
            passed: false,
            summary: 'one defect',
            findings: [{ id: 'A-1', category: 'IDIOMATIC_SPARK', severity: 'MINOR', file: 'src/pipeline.py', line: 4, evidence: 'x', description: 'd', rationale: 'r' }],
        }));
        expect(parsed.passed).toBe(false);
        expect(parsed.findings).toEqual([{ id: 'A-1', category: 'IDIOMATIC_SPARK', severity: 'MINOR', file: 'src/pipeline.py', line: 4, evidence: 'x', description: 'd', rationale: 'r' }]);
    });

    it('normalizes unknown categories, severities, missing ids, duplicate ids, and bad lines (boundary condition)', () => {
        const parsed = parseSparkArchitectureResponse(JSON.stringify({
            passed: 'yes',
            findings: [
                { category: 'IDIOMATIC_BEAM', severity: 'BLOCKER', description: 'a', line: 0 },
                { id: 'D', description: 'b', line: 2.5 },
                { id: 'D', description: 'c' },
                { id: 'E', description: '   ' },
                null,
            ],
        }));
        expect(parsed.passed).toBe(false);
        expect(parsed.summary).toBe('');
        expect(parsed.findings.map(f => f.id)).toEqual(['SPARK-ARCH-LLM-1', 'D', 'D-3']);
        expect(parsed.findings[0].category).toBe('ARCHITECTURE');
        expect(parsed.findings[0].severity).toBe('MAJOR');
        expect(parsed.findings[0].file).toBe('unknown');
        expect(parsed.findings[0].line).toBeUndefined();
        expect(parsed.findings[1].line).toBeUndefined();
    });

    it('throws on empty, invalid JSON, and non-object responses (failure condition)', () => {
        expect(() => parseSparkArchitectureResponse(undefined)).toThrow('Empty response');
        expect(() => parseSparkArchitectureResponse('  ')).toThrow('Empty response');
        expect(() => parseSparkArchitectureResponse('{not json')).toThrow('Invalid JSON');
        expect(() => parseSparkArchitectureResponse('[1,2]')).toThrow('expected a JSON object');
        expect(() => parseSparkArchitectureResponse('null')).toThrow('expected a JSON object');
    });
});

describe('mergeArchitectureFindings', () => {
    it('keeps static findings first and drops LLM duplicates by id', () => {
        const a: EvaluatorFinding = { id: 'S-1', category: 'ARCHITECTURE', severity: 'MAJOR', file: 'f', description: 'static', rationale: '' };
        const dup: EvaluatorFinding = { ...a, description: 'llm copy' };
        const b: EvaluatorFinding = { ...a, id: 'L-1', description: 'llm' };
        expect(mergeArchitectureFindings([a], [dup, b]).map(f => f.description)).toEqual(['static', 'llm']);
        expect(mergeArchitectureFindings([], [])).toEqual([]);
    });
});

describe('evaluateSparkArchitecture (mocked model client)', () => {
    /**
     * Builds a mock GoogleGenAI client.
     *
     * @param impl - generateContent implementation.
     * @returns Mock client and its spy.
     */
    function mockAi(impl: (args: { config: Record<string, unknown> }) => Promise<unknown>): { ai: GoogleGenAI; spy: ReturnType<typeof vi.fn> } {
        const spy = vi.fn().mockImplementation(impl);
        return { ai: { models: { generateContent: spy } } as unknown as GoogleGenAI, spy };
    }

    it('passes a clean repository when the model reports no findings', async () => {
        const { ai, spy } = mockAi(async () => ({ text: JSON.stringify({ passed: true, summary: 'clean', findings: [] }) }));
        const result = await evaluateSparkArchitecture(ai, 'Python', 'nodes: []', cleanSparkFiles());
        expect(result).toEqual({ agentName: SPARK_ARCHITECTURE_AGENT_NAME, passed: true, summary: 'clean', findings: [] });
        expect(spy).toHaveBeenCalledTimes(1);
        const args = spy.mock.calls[0][0];
        expect(args.config.responseMimeType).toBe('application/json');
        expect(args.config.responseSchema.properties.findings.items.properties.category.enum).toEqual(['ARCHITECTURE', 'IDIOMATIC_SPARK']);
    });

    it('fails when a static MAJOR finding exists even if the model passes the code', async () => {
        const { ai } = mockAi(async () => ({ text: JSON.stringify({ passed: true, summary: 'model saw nothing', findings: [] }) }));
        const files = withFile('src/pipeline.py', CLEAN_PIPELINE_PY.replace('        dfs["enriched"].unpersist()', '        pass'));
        const result = await evaluateSparkArchitecture(ai, 'Python', 'nodes: []', files);
        expect(result.passed).toBe(false);
        expect(result.findings.map(f => f.id)).toEqual(['SPARK-ARCH-CACHE-UNPERSIST-1']);
    });

    it('merges model findings after static findings', async () => {
        const { ai } = mockAi(async () => ({
            text: JSON.stringify({
                passed: false,
                summary: 'broadcast misuse',
                findings: [{ id: 'LLM-BCAST-1', category: 'IDIOMATIC_SPARK', severity: 'MAJOR', file: 'src/pipeline.py', description: 'Broadcast of a large fact table', rationale: 'OOM' }],
            }),
        }));
        const result = await evaluateSparkArchitecture(ai, 'Python', 'nodes: []', cleanSparkFiles());
        expect(result.passed).toBe(false);
        expect(result.findings.map(f => f.id)).toEqual(['LLM-BCAST-1']);
    });

    it('keeps static findings and fails when the model call rejects (failure condition)', async () => {
        const { ai } = mockAi(async () => { throw new Error('500 Internal'); });
        const files = withFile('run_local.sh', 'spark-submit --packages io.delta:delta-core_2.12:2.4.0 src/pipeline.py\n');
        const result = await evaluateSparkArchitecture(ai, 'Python', 'nodes: []', files);
        expect(result.passed).toBe(false);
        expect(result.summary).toContain('500 Internal');
        expect(result.findings.length).toBeGreaterThan(0);
        expect(result.findings.every(f => f.id.startsWith('SPARK-ARCH-CLASSPATH-'))).toBe(true);
    });

    it('fails without inventing findings when the model returns empty text (failure condition)', async () => {
        const { ai } = mockAi(async () => ({ text: '' }));
        const result = await evaluateSparkArchitecture(ai, 'Python', 'nodes: []', cleanSparkFiles());
        expect(result.passed).toBe(false);
        expect(result.findings).toEqual([]);
        expect(result.summary).toContain('Empty response');
    });
});
