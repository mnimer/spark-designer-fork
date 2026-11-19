/**
 * Integration Suites & Bundled Samples Topology Unit Test
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Validates that every `.spark` pipeline file under:
 * - src/test/integration/sources/
 * - src/test/integration/sinks/
 * - src/test/integration/steps/
 * - src/test/integration/e2e/
 * - resources/samples/
 *
 * adheres to topological DAG rules via Kahn's algorithm with zero cycles, zero orphans,
 * valid edges, zero validation warnings, SOURCE -> STEP* -> SINK shape, and `spark_transform` metadata.
 */

import { describe, it, expect, afterAll } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as yaml from 'yaml';
import { validateDagTopology, DagNode, DagEdge } from '../../dagValidator';
import { PIPELINE_FILE_EXT } from '../../core/constants';

interface SparkFileContent {
    nodes: DagNode[];
    edges: DagEdge[];
    runtimeVariables?: { key: string; value: string }[];
    jobConfig?: Record<string, unknown>;
    pipelineConfig?: Record<string, unknown>;
}

/**
 * Recursively locates all `.spark` pipeline files within a directory.
 *
 * @param dir The root directory to search.
 * @returns Sorted absolute file paths of discovered `.spark` files.
 */
function getSparkFiles(dir: string): string[] {
    if (!fs.existsSync(dir)) return [];
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    let files: string[] = [];
    for (const e of entries) {
        const full = path.join(dir, e.name);
        if (e.isDirectory()) {
            files = files.concat(getSparkFiles(full));
        } else if (e.isFile() && e.name.endsWith(PIPELINE_FILE_EXT)) {
            files.push(full);
        }
    }
    return files.sort();
}

describe('getSparkFiles discovery helper', () => {
    const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'spark-suites-'));
    afterAll(() => fs.rmSync(tmpRoot, { recursive: true, force: true }));

    it('finds nested .spark files and ignores other extensions', () => {
        fs.mkdirSync(path.join(tmpRoot, 'sources', 'kafka'), { recursive: true });
        fs.writeFileSync(path.join(tmpRoot, 'sources', 'kafka', 'kafka-source.spark'), 'nodes: []');
        fs.writeFileSync(path.join(tmpRoot, 'sources', 'legacy.yaml'), 'nodes: []');
        fs.writeFileSync(path.join(tmpRoot, 'sources', 'notes.md'), '# x');
        const found = getSparkFiles(path.join(tmpRoot, 'sources'));
        expect(found.map(f => path.basename(f))).toEqual(['kafka-source.spark']);
    });

    it('returns an empty list for a missing directory (extreme condition)', () => {
        expect(getSparkFiles(path.join(tmpRoot, 'does-not-exist'))).toEqual([]);
    });
});

describe('Integration Suites DAG Topology and Pattern Verification', () => {
    const integrationDir = path.resolve(__dirname, '../integration');
    const samplesDir = path.resolve(__dirname, '../../../resources/samples');
    const e2eFiles = getSparkFiles(path.join(integrationDir, 'e2e'));
    const sampleFiles = getSparkFiles(samplesDir);
    const suiteFiles = [
        ...getSparkFiles(path.join(integrationDir, 'sources')),
        ...getSparkFiles(path.join(integrationDir, 'sinks')),
        ...getSparkFiles(path.join(integrationDir, 'steps')),
        ...e2eFiles,
        ...sampleFiles,
    ];

    it('discovers at least 6 E2E .spark pipelines and all 4 bundled sample .spark pipelines', () => {
        expect(e2eFiles.length).toBeGreaterThanOrEqual(6);
        expect(sampleFiles.length).toBe(4);
    });

    it('never ships unexpected non-Spark pipeline suites next to the Spark suites', () => {
        const allowedExts = new Set(['.spark', '.md', '.ts']);
        const unexpected: string[] = [];
        const walk = (dir: string): void => {
            if (!fs.existsSync(dir)) return;
            for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
                if (e.name.startsWith('.')) continue;
                const full = path.join(dir, e.name);
                if (e.isDirectory()) walk(full);
                else if (!allowedExts.has(path.extname(e.name))) unexpected.push(full);
            }
        };
        walk(integrationDir);
        walk(samplesDir);
        expect(unexpected).toEqual([]);
    });

    it('validates every .spark file passes Kahn algorithm with 0 cycles and 0 orphans', () => {
        for (const filePath of suiteFiles) {
            const parsed = yaml.parse(fs.readFileSync(filePath, 'utf-8')) as SparkFileContent;
            expect(parsed, `Failed to parse YAML: ${filePath}`).toBeDefined();
            expect(Array.isArray(parsed.nodes), `Nodes missing in ${filePath}`).toBe(true);
            expect(Array.isArray(parsed.edges), `Edges missing in ${filePath}`).toBe(true);

            const result = validateDagTopology(parsed.nodes, parsed.edges, {
                jobConfig: parsed.jobConfig,
            });
            expect(result.isValid, `DAG invalid in ${filePath}: ${result.errors.join(', ')}`).toBe(true);
            expect(result.errors, `DAG errors in ${filePath}`).toEqual([]);
            expect(result.warnings, `DAG warnings in ${filePath}`).toEqual([]);
            expect(result.hasCycles, `Cycle detected in ${filePath}`).toBe(false);
            expect(result.orphanNodeIds.length, `Orphan nodes in ${filePath}`).toBe(0);
        }
    });

    it('enforces SOURCE -> STEP* -> SINK shape with a spark_transform on every node', () => {
        for (const filePath of suiteFiles) {
            const parsed = yaml.parse(fs.readFileSync(filePath, 'utf-8')) as SparkFileContent;
            const types = parsed.nodes.map(n => n.data?.type);
            expect(types, `No SOURCE node in ${filePath}`).toContain('SOURCE');
            expect(types, `No SINK node in ${filePath}`).toContain('SINK');
            for (const node of parsed.nodes) {
                expect(node.data?.spark_transform, `Missing spark_transform on ${node.id} in ${filePath}`).toBeTruthy();
            }
            const targets = new Set(parsed.edges.map(e => e.target));
            const sources = new Set(parsed.edges.map(e => e.source));
            for (const node of parsed.nodes) {
                if (node.data?.type === 'SOURCE') {
                    expect(targets.has(node.id), `SOURCE ${node.id} has an inbound edge in ${filePath}`).toBe(false);
                }
                if (node.data?.type === 'SINK') {
                    expect(sources.has(node.id), `SINK ${node.id} has an outbound edge in ${filePath}`).toBe(false);
                }
            }
        }
    });
});
