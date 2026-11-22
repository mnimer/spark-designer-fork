/**
 * Discovery Handler Tests (IO68 Bigtable, IO74 Cloud SQL, projects)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Exercises the gcloud-backed discovery handlers added for the Community &
 * Cross-Language connectors end to end with a mocked `execFile`: the exact CLI
 * arguments each handler builds (including the optional `--project` derived from a
 * fully-qualified instance identifier), the response command/key posted back to the
 * webview, the mapped values, and error propagation from the CLI.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { execFile, ExecFileException } from 'child_process';

let mockConfigValues: Record<string, string | undefined> = {};

vi.mock('vscode', async () => (await import('./helpers/vscodeMock')).createVscodeMock({
    workspace: {
        workspaceFolders: [],
        getConfiguration: vi.fn((section?: string) => ({
            get: (key: string) => {
                const fullKey = section ? `${section}.${key}` : key;
                return mockConfigValues[fullKey] ?? mockConfigValues[key];
            }
        }))
    }
}));

vi.mock('child_process', () => {
    return {
        execFile: vi.fn()
    };
});

vi.mock('../../codegen/modelDiscovery', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../codegen/modelDiscovery')>();
    return {
        ...actual,
        discoverCodegenModels: vi.fn(actual.discoverCodegenModels),
    };
});

import { DEFAULT_GEMINI_MODEL } from '../../core/constants';
import {
    MessageContext,
    handleMessages,
    handleGetBigtableInstances,
    handleGetBigtableTables,
    handleGetSqlInstances,
    handleGetSqlDatabases,
    handleGetProjects,
    handleGetDataprocClusters,
    handleGetSessionTemplates,
    handleGetBigLakeCatalogs,
    handleGetDatasets,
    handleGetTables,
    handleGetStorageBuckets,
    handleGetSubnetworks,
    handleGetRegions,
    handleGetCodegenModels,
    handleGetSecrets,
    fetchDataprocClusters,
    fetchSessionTemplates,
    fetchBigLakeCatalogs,
    fetchDatasets,
    fetchTables,
    fetchBuckets,
    fetchStorageBuckets,
    fetchSubnetworks,
    fetchRegions,
    fetchSecrets,
    fetchCodegenModels,
    discoverCodegenModels,
    createDefaultCodegenModelOption,
    mapDataprocClusters,
    mapSessionTemplates,
    mapBigLakeCatalogs,
    mapRegions,
} from '../../extension';

type Posted = Record<string, any>;

/** Configures execFile to succeed with the given JSON and records the invoked args. */
function mockCliSuccess(output: unknown): { calls: string[][] } {
    const calls: string[][] = [];
    (execFile as any).mockImplementation((_bin: string, args: string[], opts: any, cb: any) => {
        calls.push(args);
        const callback = typeof opts === 'function' ? opts : cb;
        callback(null, JSON.stringify(output), '');
    });
    return { calls };
}

/**
 * Creates a mock MessageContext and an array collecting posted messages.
 *
 * @returns An object containing the mock context and the posted message list.
 */
function makeCtx(): { ctx: MessageContext; posted: Posted[] } {
    const posted: Posted[] = [];
    const ctx: MessageContext = {
        context: {} as any,
        webview: {} as any,
        postToWebview: (msg: Posted) => { posted.push(msg); return true; }
    };
    return { ctx, posted };
}

describe('Discovery handlers (Bigtable, Cloud SQL, projects)', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockConfigValues = { 'sparkDesigner.gcloudPath': '/mock/bin/gcloud' };
        vi.spyOn(console, 'log').mockImplementation(() => undefined);
        vi.spyOn(console, 'error').mockImplementation(() => undefined);
    });

    it('getBigtableInstances lists instances and posts short IDs', async () => {
        const { calls } = mockCliSuccess([
            { name: 'projects/p/instances/inst-a', displayName: 'A' },
            { name: 'projects/p/instances/inst-b' }
        ]);
        const { ctx, posted } = makeCtx();

        await handleGetBigtableInstances(ctx, { command: 'getBigtableInstances' });

        expect(calls).toEqual([['bigtable', 'instances', 'list', '--format=json']]);
        expect(posted).toHaveLength(1);
        expect(posted[0].command).toBe('bigtableInstancesLoaded');
        expect(posted[0].bigtableInstances).toEqual(['inst-a', 'inst-b']);
        expect(posted[0].error).toBeUndefined();
    });

    it('getBigtableTables passes a short instance via --instances=', async () => {
        const { calls } = mockCliSuccess([{ name: 'projects/p/instances/my-inst/tables/orders' }]);
        const { ctx, posted } = makeCtx();

        await handleGetBigtableTables(ctx, { command: 'getBigtableTables', instance: 'my-inst' });

        expect(calls).toEqual([['bigtable', 'instances', 'tables', 'list', '--instances=my-inst', '--format=json']]);
        expect(posted[0]).toEqual(expect.objectContaining({ command: 'bigtableTablesLoaded', bigtableTables: ['orders'] }));
    });

    it('getBigtableTables derives --project from a fully-qualified instance name', async () => {
        const { calls } = mockCliSuccess([]);
        const { ctx, posted } = makeCtx();

        await handleGetBigtableTables(ctx, { command: 'getBigtableTables', instance: 'projects/p1/instances/my-inst' });

        expect(calls).toEqual([
            ['bigtable', 'instances', 'tables', 'list', '--instances=my-inst', '--format=json', '--project=p1']
        ]);
        expect(posted[0].bigtableTables).toEqual([]);
    });

    it('getSqlInstances posts instance connection names', async () => {
        const { calls } = mockCliSuccess([
            { name: 'db1', project: 'p', region: 'us-central1', connectionName: 'p:us-central1:db1' },
            { name: 'db2', project: 'p', region: 'europe-west1' }
        ]);
        const { ctx, posted } = makeCtx();

        await handleGetSqlInstances(ctx, { command: 'getSqlInstances' });

        expect(calls).toEqual([['sql', 'instances', 'list', '--format=json']]);
        expect(posted[0]).toEqual(expect.objectContaining({
            command: 'sqlInstancesLoaded',
            sqlInstances: ['p:us-central1:db1', 'p:europe-west1:db2']
        }));
    });

    it('getSqlDatabases accepts a connection name and splits it into --instance/--project', async () => {
        const { calls } = mockCliSuccess([{ name: 'postgres' }, { name: 'app' }]);
        const { ctx, posted } = makeCtx();

        await handleGetSqlDatabases(ctx, { command: 'getSqlDatabases', instance: 'p:us-central1:db1' });

        expect(calls).toEqual([['sql', 'databases', 'list', '--instance=db1', '--format=json', '--project=p']]);
        expect(posted[0]).toEqual(expect.objectContaining({ command: 'sqlDatabasesLoaded', sqlDatabases: ['postgres', 'app'] }));
    });

    it('getSqlDatabases accepts a bare instance name without --project', async () => {
        const { calls } = mockCliSuccess([{ name: 'app' }]);
        const { ctx } = makeCtx();

        await handleGetSqlDatabases(ctx, { command: 'getSqlDatabases', instance: 'db1' });

        expect(calls).toEqual([['sql', 'databases', 'list', '--instance=db1', '--format=json']]);
    });

    it('getSqlDatabases rejects an unsafe identifier without invoking gcloud', async () => {
        const { calls } = mockCliSuccess([]);
        const { ctx, posted } = makeCtx();

        await handleGetSqlDatabases(ctx, { command: 'getSqlDatabases', instance: 'db1 --impersonate-service-account=x' });

        expect(calls).toEqual([]);
        expect(posted[0].error).toContain('Invalid or missing Cloud SQL instance identifier');
    });

    it('getProjects posts project IDs', async () => {
        const { calls } = mockCliSuccess([
            { projectId: 'proj-a', name: 'Project A', projectNumber: '1' },
            { projectId: 'proj-b', name: 'Project B', projectNumber: '2' }
        ]);
        const { ctx, posted } = makeCtx();

        await handleGetProjects(ctx, { command: 'getProjects' });

        expect(calls).toEqual([['projects', 'list', '--format=json']]);
        expect(posted[0]).toEqual(expect.objectContaining({ command: 'projectsLoaded', projects: ['proj-a', 'proj-b'] }));
    });

    it('surfaces gcloud stderr as an error with an empty list under the handler key', async () => {
        (execFile as any).mockImplementation((_bin: string, _args: string[], opts: any, cb: any) => {
            const callback = typeof opts === 'function' ? opts : cb;
            const err: ExecFileException = new Error('Command failed');
            err.code = 1;
            callback(err, '', 'ERROR: (gcloud.sql.instances.list) PERMISSION_DENIED');
        });
        const { ctx, posted } = makeCtx();

        await handleGetSqlInstances(ctx, { command: 'getSqlInstances' });

        expect(posted).toHaveLength(1);
        expect(posted[0].command).toBe('sqlInstancesLoaded');
        expect(posted[0].sqlInstances).toEqual([]);
        expect(posted[0].error).toContain('Failed to list sqlInstances via gcloud');
        expect(posted[0].error).toContain('PERMISSION_DENIED');
    });
});

describe('Phase 4 Live GCP Asset Discovery (D1–D6)', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockConfigValues = { 'sparkDesigner.gcloudPath': '/mock/bin/gcloud' };
        vi.spyOn(console, 'log').mockImplementation(() => undefined);
        vi.spyOn(console, 'error').mockImplementation(() => undefined);
    });

    it('exports fetch* aliases matching their handleGet* counterparts', () => {
        expect(fetchDataprocClusters).toBe(handleGetDataprocClusters);
        expect(fetchSessionTemplates).toBe(handleGetSessionTemplates);
        expect(fetchBigLakeCatalogs).toBe(handleGetBigLakeCatalogs);
        expect(fetchDatasets).toBe(handleGetDatasets);
        expect(fetchTables).toBe(handleGetTables);
        expect(fetchBuckets).toBe(handleGetStorageBuckets);
        expect(fetchStorageBuckets).toBe(handleGetStorageBuckets);
        expect(fetchSubnetworks).toBe(handleGetSubnetworks);
        expect(fetchRegions).toBe(handleGetRegions);
        expect(fetchSecrets).toBe(handleGetSecrets);
        expect(fetchCodegenModels).toBe(handleGetCodegenModels);
    });

    describe('D1 — fetchDataprocClusters / handleGetDataprocClusters', () => {
        it('lists RUNNING Dataproc clusters with default region us-central1', async () => {
            const { calls } = mockCliSuccess([
                {
                    clusterName: 'spark-prod-cluster',
                    status: { state: 'RUNNING' },
                    labels: { 'goog-dataproc-location': 'us-central1' },
                },
                {
                    name: 'projects/p1/regions/us-central1/clusters/spark-analytics',
                    status: 'RUNNING',
                    region: 'projects/p1/regions/us-central1',
                },
                { status: { state: 'RUNNING' } },
            ]);
            const { ctx, posted } = makeCtx();

            await fetchDataprocClusters(ctx, { command: 'getDataprocClusters' });

            expect(calls).toEqual([
                [
                    'dataproc',
                    'clusters',
                    'list',
                    '--region=us-central1',
                    '--filter=status.state = RUNNING',
                    '--limit=100',
                    '--format=json',
                ],
            ]);
            expect(posted).toHaveLength(1);
            expect(posted[0].command).toBe('dataprocClustersLoaded');
            expect(posted[0].clusters).toEqual([
                {
                    name: 'spark-prod-cluster',
                    value: 'spark-prod-cluster',
                    status: 'RUNNING',
                    region: 'us-central1',
                },
                {
                    name: 'spark-analytics',
                    value: 'spark-analytics',
                    status: 'RUNNING',
                    region: 'us-central1',
                },
            ]);
            expect(posted[0].error).toBeUndefined();
        });

        it('passes explicit region and validated projectId to gcloud', async () => {
            const { calls } = mockCliSuccess([{ clusterName: 'eu-cluster', status: { state: 'RUNNING' } }]);
            const { ctx, posted } = makeCtx();

            await handleGetDataprocClusters(ctx, {
                command: 'getDataprocClusters',
                region: 'europe-west1',
                projectId: 'my-spark-proj',
            });

            expect(calls).toEqual([
                [
                    'dataproc',
                    'clusters',
                    'list',
                    '--region=europe-west1',
                    '--filter=status.state = RUNNING',
                    '--limit=100',
                    '--format=json',
                    '--project=my-spark-proj',
                ],
            ]);
            expect(posted[0].clusters).toEqual([
                { name: 'eu-cluster', value: 'eu-cluster', status: 'RUNNING' },
            ]);
        });

        it('rejects invalid region identifiers without invoking gcloud', async () => {
            const { calls } = mockCliSuccess([]);
            const { ctx, posted } = makeCtx();

            await handleGetDataprocClusters(ctx, {
                command: 'getDataprocClusters',
                region: 'us-central1; rm -rf /',
            });

            expect(calls).toEqual([]);
            expect(posted).toHaveLength(1);
            expect(posted[0].command).toBe('dataprocClustersLoaded');
            expect(posted[0].clusters).toEqual([]);
            expect(posted[0].error).toContain("Invalid region identifier: 'us-central1; rm -rf /'");
        });

        it('mapDataprocClusters returns empty array for non-array input', () => {
            expect(mapDataprocClusters(null)).toEqual([]);
            expect(mapDataprocClusters({})).toEqual([]);
        });
    });

    describe('D2 — fetchSessionTemplates / handleGetSessionTemplates', () => {
        it('lists session templates and extracts BigLake Iceberg catalog metadata from runtimeConfig.properties', async () => {
            const { calls } = mockCliSuccess([
                {
                    name: 'projects/p1/locations/us-central1/sessionTemplates/lakehouse-tpl',
                    description: 'Interactive Spark Connect with BigLake',
                    runtimeConfig: {
                        properties: {
                            'spark.sql.defaultCatalog': 'biglake_cat',
                            'spark.sql.catalog.biglake_cat': 'org.apache.iceberg.spark.SparkCatalog',
                            'spark.sql.catalog.biglake_cat.warehouse': 'gs://corp-iceberg-warehouse/prod',
                            'spark.sql.catalog.secondary_cat.warehouse': 'gs://corp-iceberg-warehouse/staging',
                        },
                    },
                },
                {
                    name: 'projects/p1/locations/us-central1/sessionTemplates/basic-tpl',
                },
            ]);
            const { ctx, posted } = makeCtx();

            await fetchSessionTemplates(ctx, {
                command: 'getSessionTemplates',
                location: 'us-central1',
                projectId: 'p1',
            });

            expect(calls).toEqual([
                [
                    'beta',
                    'dataproc',
                    'session-templates',
                    'list',
                    '--location=us-central1',
                    '--limit=50',
                    '--format=json',
                    '--project=p1',
                ],
            ]);
            expect(posted).toHaveLength(1);
            expect(posted[0].command).toBe('sessionTemplatesLoaded');
            expect(posted[0].sessionTemplates).toEqual([
                {
                    name: 'lakehouse-tpl',
                    value: 'lakehouse-tpl',
                    fullName: 'projects/p1/locations/us-central1/sessionTemplates/lakehouse-tpl',
                    description: 'Interactive Spark Connect with BigLake',
                    defaultCatalog: 'biglake_cat',
                    catalogNames: ['biglake_cat', 'secondary_cat'],
                    warehouse: 'gs://corp-iceberg-warehouse/prod',
                },
                {
                    name: 'basic-tpl',
                    value: 'basic-tpl',
                    fullName: 'projects/p1/locations/us-central1/sessionTemplates/basic-tpl',
                },
            ]);
        });

        it('rejects invalid location without invoking gcloud', async () => {
            const { calls } = mockCliSuccess([]);
            const { ctx, posted } = makeCtx();

            await handleGetSessionTemplates(ctx, {
                command: 'getSessionTemplates',
                location: 'invalid_location!',
            });

            expect(calls).toEqual([]);
            expect(posted).toHaveLength(1);
            expect(posted[0].command).toBe('sessionTemplatesLoaded');
            expect(posted[0].sessionTemplates).toEqual([]);
            expect(posted[0].error).toContain("Invalid location identifier: 'invalid_location!'");
        });

        it('mapSessionTemplates handles non-array and empty entries', () => {
            expect(mapSessionTemplates(undefined)).toEqual([]);
            expect(mapSessionTemplates([{}, { name: '' }])).toEqual([]);
        });
    });

    describe('D3 — fetchBigLakeCatalogs / handleGetBigLakeCatalogs', () => {
        it('lists BigLake Iceberg catalogs and normalizes resource names and GCS bucket warehouses', async () => {
            const { calls } = mockCliSuccess([
                {
                    name: 'projects/p1/locations/global/catalogs/lakehouse_prod',
                    default_location: 'gs://lakehouse-prod-warehouse',
                    catalog_type: 'BIGLAKE',
                },
                {
                    name: 'gs://my-warehouse-bucket/',
                },
            ]);
            const { ctx, posted } = makeCtx();

            await fetchBigLakeCatalogs(ctx, { command: 'getBigLakeCatalogs', projectId: 'p1' });

            expect(calls).toEqual([
                ['biglake', 'iceberg', 'catalogs', 'list', '--format=json', '--project=p1'],
            ]);
            expect(posted).toHaveLength(1);
            expect(posted[0].command).toBe('bigLakeCatalogsLoaded');
            expect(posted[0].catalogs).toEqual([
                {
                    name: 'lakehouse_prod',
                    id: 'lakehouse_prod',
                    fullName: 'projects/p1/locations/global/catalogs/lakehouse_prod',
                    value: 'lakehouse_prod',
                    warehouse: 'gs://lakehouse-prod-warehouse',
                    catalogType: 'BIGLAKE',
                },
                {
                    name: 'my-warehouse-bucket',
                    id: 'my-warehouse-bucket',
                    fullName: 'gs://my-warehouse-bucket/',
                    value: 'my-warehouse-bucket',
                    warehouse: 'gs://my-warehouse-bucket',
                    catalogType: 'biglake',
                },
            ]);
        });

        it('mapBigLakeCatalogs handles string items and non-array inputs', () => {
            expect(mapBigLakeCatalogs(null)).toEqual([]);
            expect(mapBigLakeCatalogs(['my-cat'])).toEqual([
                {
                    name: 'my-cat',
                    id: 'my-cat',
                    fullName: 'my-cat',
                    value: 'my-cat',
                    warehouse: 'gs://my-cat',
                    catalogType: 'biglake',
                },
            ]);
        });
    });

    describe('D4 — fetchDatasets and fetchTables (bq ls --format=json)', () => {
        it('fetchDatasets lists BigQuery datasets and extracts projectId', async () => {
            const { calls } = mockCliSuccess([
                { datasetReference: { datasetId: 'raw_events', projectId: 'analytics-p1' } },
                { datasetReference: { datasetId: 'curated_marts', projectId: 'analytics-p1' } },
            ]);
            const { ctx, posted } = makeCtx();

            await fetchDatasets(ctx, { command: 'getDatasets' });

            expect(calls).toEqual([['ls', '--format=json']]);
            expect(posted).toHaveLength(1);
            expect(posted[0].command).toBe('datasetsLoaded');
            expect(posted[0].datasets).toEqual(['raw_events', 'curated_marts']);
            expect(posted[0].projectId).toBe('analytics-p1');
        });

        it('fetchTables lists BigQuery tables for a valid dataset and rejects invalid datasets', async () => {
            const { calls } = mockCliSuccess([
                { tableReference: { tableId: 'orders' } },
                { tableReference: { tableId: 'customers' } },
            ]);
            const { ctx, posted } = makeCtx();

            await fetchTables(ctx, { command: 'getTables', dataset: 'raw_events' });

            expect(calls).toEqual([['ls', '--format=json', 'raw_events']]);
            expect(posted[0]).toEqual(
                expect.objectContaining({
                    command: 'tablesLoaded',
                    tables: ['orders', 'customers'],
                })
            );

            // Reject invalid dataset without invoking bq
            const { calls: badCalls } = mockCliSuccess([]);
            const { ctx: badCtx, posted: badPosted } = makeCtx();
            await fetchTables(badCtx, { command: 'getTables', dataset: 'raw_events; drop table' });
            expect(badCalls).toEqual([]);
            expect(badPosted[0].error).toContain('Invalid or missing dataset identifier');
        });
    });

    describe('D5 — fetchBuckets, fetchSubnetworks, and fetchRegions', () => {
        it('fetchBuckets lists Cloud Storage buckets with gs:// and trailing slashes stripped', async () => {
            const { calls } = mockCliSuccess([
                { name: 'gs://spark-staging-bucket/' },
                { id: 'spark-warehouse-bucket' },
            ]);
            const { ctx, posted } = makeCtx();

            await fetchBuckets(ctx, { command: 'getStorageBuckets' });

            expect(calls).toEqual([['storage', 'buckets', 'list', '--format=json']]);
            expect(posted[0]).toEqual(
                expect.objectContaining({
                    command: 'storageBucketsLoaded',
                    buckets: ['spark-staging-bucket', 'spark-warehouse-bucket'],
                })
            );
        });

        it('fetchSubnetworks lists VPC subnetworks with region and relative value path', async () => {
            const { calls } = mockCliSuccess([
                {
                    name: 'spark-subnet',
                    region: 'https://www.googleapis.com/compute/v1/projects/p1/regions/us-central1',
                    network: 'https://www.googleapis.com/compute/v1/projects/p1/global/networks/default',
                },
            ]);
            const { ctx, posted } = makeCtx();

            await fetchSubnetworks(ctx, { command: 'getSubnetworks' });

            expect(calls).toEqual([['compute', 'networks', 'subnets', 'list', '--format=json']]);
            expect(posted[0]).toEqual(
                expect.objectContaining({
                    command: 'subnetworksLoaded',
                    subnetworks: [
                        {
                            name: 'spark-subnet',
                            region: 'us-central1',
                            network: 'default',
                            value: 'regions/us-central1/subnetworks/spark-subnet',
                        },
                    ],
                })
            );
        });

        it('fetchRegions lists Compute Engine regions', async () => {
            const { calls } = mockCliSuccess([
                { name: 'us-central1' },
                { name: 'projects/p1/regions/europe-west1' },
            ]);
            const { ctx, posted } = makeCtx();

            await fetchRegions(ctx, { command: 'getRegions' });

            expect(calls).toEqual([['compute', 'regions', 'list', '--format=json']]);
            expect(posted[0]).toEqual(
                expect.objectContaining({
                    command: 'regionsLoaded',
                    regions: ['us-central1', 'europe-west1'],
                })
            );
            expect(mapRegions(null)).toEqual([]);
        });
    });

    describe('D6 — fetchSecrets (gcloud secrets list)', () => {
        it('fetchSecrets lists Secret Manager secrets normalized to short IDs', async () => {
            const { calls } = mockCliSuccess([
                { name: 'projects/12345/secrets/jdbc-password' },
                { name: 'projects/12345/secrets/aws-secret-key/' },
            ]);
            const { ctx, posted } = makeCtx();

            await fetchSecrets(ctx, { command: 'getSecrets' });

            expect(calls).toEqual([['secrets', 'list', '--format=json']]);
            expect(posted[0]).toEqual(
                expect.objectContaining({
                    command: 'secretsLoaded',
                    secrets: [
                        {
                            name: 'jdbc-password',
                            id: 'jdbc-password',
                            fullName: 'projects/12345/secrets/jdbc-password',
                            value: 'jdbc-password',
                        },
                        {
                            name: 'aws-secret-key',
                            id: 'aws-secret-key',
                            fullName: 'projects/12345/secrets/aws-secret-key/',
                            value: 'aws-secret-key',
                        },
                    ],
                })
            );
        });
    });

    describe('MOD1 — fetchCodegenModels / handleGetCodegenModels', () => {
        it('forwards a validated projectId and posts ranked models and defaultModel', async () => {
            const rankedModels = [
                {
                    id: DEFAULT_GEMINI_MODEL,
                    displayName: 'Gemini 4 Hydrogen',
                    tier: 'flash' as const,
                    version: '4.0',
                    isDefault: true,
                },
                {
                    id: 'gemini-3.8-flash',
                    displayName: 'Gemini 3.8 Flash',
                    tier: 'flash' as const,
                    version: '3.8',
                },
            ];
            vi.mocked(discoverCodegenModels).mockResolvedValueOnce({
                models: rankedModels,
                defaultModel: DEFAULT_GEMINI_MODEL,
                source: 'vertexai',
            });
            const { ctx, posted } = makeCtx();

            await fetchCodegenModels(ctx, {
                command: 'getCodegenModels',
                projectId: '  my-spark-proj  ',
            });

            expect(discoverCodegenModels).toHaveBeenCalledWith({
                projectId: 'my-spark-proj',
            });
            expect(posted).toEqual([
                {
                    command: 'codegenModelsLoaded',
                    models: rankedModels,
                    defaultModel: 'gemini-4-hydrogen',
                    source: 'vertexai',
                },
            ]);
        });

        it('ignores placeholder, blank, and invalid projectId values before discovery', async () => {
            for (const rawProjectId of ['YOUR_PROJECT_ID', '   ', 'bad;proj`id', 42]) {
                vi.mocked(discoverCodegenModels).mockResolvedValueOnce({
                    models: [createDefaultCodegenModelOption()],
                    defaultModel: DEFAULT_GEMINI_MODEL,
                    source: 'vertexai',
                });
                const { ctx, posted } = makeCtx();

                await handleGetCodegenModels(ctx, {
                    command: 'getCodegenModels',
                    projectId: rawProjectId,
                });

                expect(discoverCodegenModels).toHaveBeenLastCalledWith({
                    projectId: undefined,
                });
                expect(posted[0]).toEqual(
                    expect.objectContaining({
                        command: 'codegenModelsLoaded',
                        defaultModel: DEFAULT_GEMINI_MODEL,
                    })
                );
            }
        });

        it('returns offline DEFAULT_GEMINI_MODEL fallback and debugLog warning when offline or empty', async () => {
            vi.mocked(discoverCodegenModels).mockResolvedValueOnce({
                models: [],
                defaultModel: DEFAULT_GEMINI_MODEL,
                source: 'fallback',
                warning: 'Offline catalog fallback activated.',
            });
            const { ctx, posted } = makeCtx();

            await handleGetCodegenModels(ctx, { command: 'getCodegenModels' });

            expect(posted).toEqual([
                {
                    command: 'codegenModelsLoaded',
                    models: [
                        {
                            id: 'gemini-4-hydrogen',
                            displayName: 'Gemini 4 Hydrogen',
                            tier: 'flash',
                            version: '4.0',
                            isDefault: true,
                        },
                    ],
                    defaultModel: 'gemini-4-hydrogen',
                    source: 'fallback',
                    debugLog: 'Offline catalog fallback activated.',
                },
            ]);
        });

        it('echoes requestParams when dispatched through handleMessages (F11 correlation)', async () => {
            vi.mocked(discoverCodegenModels).mockResolvedValueOnce({
                models: [createDefaultCodegenModelOption()],
                defaultModel: DEFAULT_GEMINI_MODEL,
                source: 'vertexai',
            });
            const posted: Posted[] = [];
            const mockWebview = {
                postMessage: vi.fn((msg: Posted) => {
                    posted.push(msg);
                    return true;
                }),
                onDidReceiveMessage: vi.fn(),
            };
            const dispatch = handleMessages(mockWebview as never, { subscriptions: [] } as never);

            await dispatch({ command: 'getCodegenModels', projectId: 'my-spark-proj' });

            expect(posted).toHaveLength(1);
            expect(posted[0]).toEqual(
                expect.objectContaining({
                    command: 'codegenModelsLoaded',
                    requestParams: { projectId: 'my-spark-proj' },
                })
            );
        });
    });
});
