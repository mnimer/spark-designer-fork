/** @fileoverview Provides data mapping and parsing utilities for Cloud CLI outputs.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

export interface SecretManagerItem {
    name: string;
    id: string;
    fullName: string;
    value: string;
}

/**
 * Safely narrows an unknown JSON value to a plain property record.
 *
 * @param value Candidate JSON value
 * @returns Plain object record or undefined when non-object
 */
function asRecord(value: unknown): Record<string, unknown> | undefined {
    return value !== null && typeof value === 'object'
        ? (value as Record<string, unknown>)
        : undefined;
}

/**
 * Extracts the short secret ID from a Secret Manager fully-qualified resource path or secret name.
 * Handles paths like "projects/123/secrets/my-secret", names with trailing slashes, or simple IDs.
 * 
 * @param fullName Full resource path (e.g. projects/.../secrets/my-secret) or secret name
 * @returns The short secret ID without project path prefix
 */
export function toShortSecretId(fullName?: string | null): string {
    if (!fullName) {
        return '';
    }
    // NOTE: Strip any trailing slashes to properly parse paths such as 'projects/.../secrets/my-secret/'
    const trimmed = fullName.trim().replace(/\/+$/, '');
    if (!trimmed) {
        return '';
    }
    const parts = trimmed.split('/');
    return parts[parts.length - 1] || '';
}

/**
 * Maps already-parsed `gcloud secrets list --format=json` output into normalized
 * SecretManagerItem objects.
 *
 * Guarantees short secret IDs reach the webview: `name`, `id` and `value` all carry the
 * short ID, and only `fullName` retains the fully-qualified resource path. Any consumer
 * therefore reads a usable ID whichever field it happens to pick (finding M1/M20).
 *
 * NOTE: This is deliberately separate from parseSecretManagerOutput. The generic
 * `listAssets` helper has already JSON.parse'd the CLI output by the time it calls its
 * mapper, so it needs the mapping over parsed JSON. Keeping one shared implementation
 * means the logic covered by src/test/secrets.test.ts is the logic that actually runs --
 * an inline duplicate here would recreate exactly the M1 failure mode of a correct,
 * tested code path that never executes.
 *
 * @param json Parsed JSON from `gcloud secrets list --format=json`
 * @returns Array of SecretManagerItem objects containing short IDs and full resource names
 */
export function mapSecretManagerItems(json: unknown): SecretManagerItem[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json.map((entry: unknown) => {
        const s = asRecord(entry);
        const rawName = typeof s?.name === 'string' ? s.name : '';
        const shortName = toShortSecretId(rawName) || rawName;
        return {
            name: shortName,
            id: shortName,
            fullName: rawName,
            value: shortName
        };
    });
}

/**
 * Parses raw JSON output from `gcloud secrets list --format=json` into normalized
 * SecretManagerItem objects. Thin wrapper over mapSecretManagerItems for callers that
 * hold the raw stdout string rather than parsed JSON.
 *
 * @param stdout JSON string from gcloud secrets list command
 * @returns Array of SecretManagerItem objects containing short IDs and full resource names
 */
export function parseSecretManagerOutput(stdout: string): SecretManagerItem[] {
    return mapSecretManagerItems(JSON.parse(stdout));
}

/**
 * Returns the last non-empty '/'-separated segment of a resource name
 * (e.g. `projects/p/instances/i` -> `i`), or '' for non-strings.
 *
 * @param value Resource name from gcloud JSON output
 * @returns The trailing path segment
 */
function lastPathSegment(value: unknown): string {
    if (typeof value !== 'string') {
        return '';
    }
    const parts = value.split('/').filter(Boolean);
    return parts.length ? parts[parts.length - 1] : '';
}

/**
 * Maps `gcloud bigtable instances list --format=json` to short instance IDs.
 *
 * @param json Parsed gcloud JSON output
 * @returns Short Bigtable instance IDs
 */
export function mapBigtableInstances(json: unknown): string[] {
    return Array.isArray(json)
        ? json.map((i: unknown) => lastPathSegment(asRecord(i)?.name)).filter(Boolean)
        : [];
}

/**
 * Maps `gcloud bigtable instances tables list --format=json` to short table IDs.
 *
 * @param json Parsed gcloud JSON output
 * @returns Short Bigtable table IDs
 */
export function mapBigtableTables(json: unknown): string[] {
    return Array.isArray(json)
        ? json.map((t: unknown) => lastPathSegment(asRecord(t)?.name)).filter(Boolean)
        : [];
}

/**
 * Maps `gcloud sql instances list --format=json` to instance connection names
 * (`project:region:instance`), which is what the Cloud SQL connectors expect.
 * Falls back to composing the name from project/region/name when connectionName is absent.
 *
 * @param json Parsed gcloud JSON output
 * @returns Cloud SQL instance connection names
 */
export function mapSqlInstances(json: unknown): string[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json.map((entry: unknown) => {
        const i = asRecord(entry);
        if (typeof i?.connectionName === 'string' && i.connectionName) {
            return i.connectionName;
        }
        return i?.project && i?.region && i?.name
            ? `${String(i.project)}:${String(i.region)}:${String(i.name)}`
            : '';
    }).filter(Boolean);
}

/**
 * Maps `gcloud sql databases list --format=json` to database names.
 *
 * @param json Parsed gcloud JSON output
 * @returns Database names
 */
export function mapSqlDatabases(json: unknown): string[] {
    return Array.isArray(json)
        ? json
              .map((d: unknown) => asRecord(d)?.name)
              .filter((name): name is string => typeof name === 'string' && Boolean(name))
        : [];
}

/**
 * Maps `gcloud projects list --format=json` to project IDs.
 *
 * @param json Parsed gcloud JSON output
 * @returns Project IDs
 */
export function mapProjects(json: unknown): string[] {
    return Array.isArray(json)
        ? json
              .map((p: unknown) => asRecord(p)?.projectId)
              .filter((id): id is string => typeof id === 'string' && Boolean(id))
        : [];
}

/**
 * Parses a Bigtable instance identifier sent from the webview. Accepts a short
 * instance ID or `projects/{project}/instances/{instance}`. The charset is strict
 * (no ',' so a caller cannot smuggle a multi-instance list into `--instances=`),
 * while length is left to the API to validate.
 *
 * @param raw Value of `message.instance`
 * @returns `{ instance, project? }`, or null when missing or invalid
 */
export function parseBigtableInstanceParam(raw: unknown): { instance: string; project?: string } | null {
    if (typeof raw !== 'string') {
        return null;
    }
    const v = raw.trim();
    const full = /^projects\/([a-z0-9.:-]+)\/instances\/([a-z][a-z0-9-]*)$/i.exec(v);
    if (full) {
        return { project: full[1], instance: full[2] };
    }
    return /^[a-z][a-z0-9-]*$/i.test(v) ? { instance: v } : null;
}

/**
 * Parses a Cloud SQL instance identifier sent from the webview. Accepts a bare
 * instance name or an instance connection name `[domain:]project:region:instance`.
 *
 * @param raw Value of `message.instance`
 * @returns `{ instance, project? }`, or null when missing or invalid
 */
export function parseSqlInstanceParam(raw: unknown): { instance: string; project?: string } | null {
    if (typeof raw !== 'string') {
        return null;
    }
    const m = /^(?:([a-z0-9.:-]+):([a-z0-9-]+):)?([a-z][a-z0-9-]*)$/i.exec(raw.trim());
    if (!m) {
        return null;
    }
    return m[1] ? { project: m[1], instance: m[3] } : { instance: m[3] };
}

/**
 * Maps `gcloud healthcare datasets list --format=json` to dataset resource names.
 *
 * @param json Parsed JSON array from gcloud healthcare datasets list
 * @returns Array of non-empty dataset resource name strings
 */
export function mapHealthcareDatasetItems(json: unknown): string[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json
        .map((entry: unknown) => {
            const name = asRecord(entry)?.name;
            return typeof name === 'string' ? name.trim() : '';
        })
        .filter((name: string) => name.length > 0);
}

/** Normalized Dataproc cluster item returned to the webview. */
export interface DataprocClusterItem {
    name: string;
    status?: string;
    region?: string;
    value?: string;
}

/**
 * Maps `gcloud dataproc clusters list --format=json` output into normalized
 * DataprocClusterItem entries.
 *
 * @param json Parsed JSON array from gcloud dataproc clusters list
 * @returns Array of normalized DataprocClusterItem entries
 */
export function mapDataprocClusters(json: unknown): DataprocClusterItem[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json
        .map((entry: unknown): DataprocClusterItem | null => {
            const c = asRecord(entry);
            const rawClusterName =
                typeof c?.clusterName === 'string' && c.clusterName.trim()
                    ? c.clusterName.trim()
                    : lastPathSegment(c?.name);
            if (!rawClusterName) {
                return null;
            }
            const statusRecord = asRecord(c?.status);
            const status =
                typeof statusRecord?.state === 'string' && statusRecord.state.trim()
                    ? statusRecord.state.trim()
                    : typeof c?.status === 'string' && c.status.trim()
                        ? c.status.trim()
                        : undefined;
            const labelsRecord = asRecord(c?.labels);
            const labelLoc = labelsRecord?.['goog-dataproc-location'];
            const rawRegion =
                typeof c?.region === 'string' && c.region.trim()
                    ? c.region.trim()
                    : typeof labelLoc === 'string' && labelLoc.trim()
                        ? labelLoc.trim()
                        : undefined;
            const region = rawRegion ? lastPathSegment(rawRegion) || rawRegion : undefined;
            return {
                name: rawClusterName,
                value: rawClusterName,
                ...(status ? { status } : {}),
                ...(region ? { region } : {}),
            };
        })
        .filter((item): item is DataprocClusterItem => item !== null);
}

/** Normalized Dataproc Serverless session template item returned to the webview. */
export interface SessionTemplateItem {
    name: string;
    value: string;
    fullName: string;
    description?: string;
    defaultCatalog?: string;
    catalogNames?: string[];
    warehouse?: string;
}

/**
 * Maps `gcloud beta dataproc session-templates list --format=json` output into
 * normalized SessionTemplateItem entries, including any BigLake Iceberg catalog
 * properties defined under `runtimeConfig.properties`.
 *
 * @param json Parsed JSON array from gcloud beta dataproc session-templates list
 * @returns Array of normalized SessionTemplateItem entries
 */
export function mapSessionTemplates(json: unknown): SessionTemplateItem[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json
        .map((entry: unknown): SessionTemplateItem | null => {
            const t = asRecord(entry);
            const rawName = typeof t?.name === 'string' ? t.name.trim() : '';
            const shortName = lastPathSegment(rawName);
            if (!shortName) {
                return null;
            }
            const description =
                typeof t?.description === 'string' && t.description.trim()
                    ? t.description.trim()
                    : undefined;

            let defaultCatalog: string | undefined;
            const catalogSet = new Set<string>();
            const warehouseByCatalog: Record<string, string> = {};

            const runtimeConfig = asRecord(t?.runtimeConfig);
            const props = asRecord(runtimeConfig?.properties);
            if (props) {
                const rawDefaultCatalog = props['spark.sql.defaultCatalog'];
                if (typeof rawDefaultCatalog === 'string' && rawDefaultCatalog.trim()) {
                    defaultCatalog = rawDefaultCatalog.trim();
                    catalogSet.add(defaultCatalog);
                }
                for (const [key, val] of Object.entries(props)) {
                    const match = /^spark\.sql\.catalog\.([^.]+)(?:\.(.+))?$/.exec(key);
                    if (match) {
                        const catName = match[1];
                        const propSuffix = match[2];
                        catalogSet.add(catName);
                        if (propSuffix === 'warehouse' && typeof val === 'string' && val.trim()) {
                            warehouseByCatalog[catName] = val.trim();
                        }
                    }
                }
            }

            const catalogNames = catalogSet.size > 0 ? [...catalogSet] : undefined;
            const warehouse =
                (defaultCatalog && warehouseByCatalog[defaultCatalog]) ||
                Object.values(warehouseByCatalog)[0] ||
                undefined;

            return {
                name: shortName,
                value: shortName,
                fullName: rawName,
                ...(description ? { description } : {}),
                ...(defaultCatalog ? { defaultCatalog } : {}),
                ...(catalogNames ? { catalogNames } : {}),
                ...(warehouse ? { warehouse } : {}),
            };
        })
        .filter((item): item is SessionTemplateItem => item !== null);
}

/** Normalized BigLake Iceberg catalog item returned to the webview. */
export interface BigLakeCatalogItem {
    name: string;
    id: string;
    fullName: string;
    value: string;
    warehouse: string;
    catalogType?: string;
}

/**
 * Maps `gcloud biglake iceberg catalogs list --format=json` (or GCS bucket objects)
 * into normalized BigLakeCatalogItem entries.
 *
 * @param json Parsed JSON array from gcloud biglake iceberg catalogs list
 * @returns Array of normalized BigLakeCatalogItem entries
 */
export function mapBigLakeCatalogs(json: unknown): BigLakeCatalogItem[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json
        .map((item: unknown): BigLakeCatalogItem | null => {
            const rec = asRecord(item);
            const rawName =
                typeof item === 'string'
                    ? item.trim()
                    : typeof rec?.name === 'string'
                        ? rec.name.trim()
                        : typeof rec?.id === 'string'
                            ? rec.id.trim()
                            : '';
            if (!rawName) {
                return null;
            }
            const stripped = rawName.replace(/^gs:\/\//, '').replace(/\/+$/, '');
            const shortId = lastPathSegment(stripped);
            if (!shortId) {
                return null;
            }
            const warehouse =
                (typeof rec?.warehouse === 'string' && rec.warehouse.trim()) ||
                (typeof rec?.default_location === 'string' && rec.default_location.trim()) ||
                (typeof rec?.defaultLocation === 'string' && rec.defaultLocation.trim()) ||
                `gs://${shortId}`;
            const catalogType =
                (typeof rec?.catalog_type === 'string' && rec.catalog_type.trim()) ||
                (typeof rec?.catalogType === 'string' && rec.catalogType.trim()) ||
                'biglake';
            return {
                name: shortId,
                id: shortId,
                fullName: rawName || shortId,
                value: shortId,
                warehouse,
                catalogType,
            };
        })
        .filter((item): item is BigLakeCatalogItem => item !== null);
}

/**
 * Maps `gcloud compute regions list --format=json` to region names.
 *
 * @param json Parsed JSON array from gcloud compute regions list
 * @returns Array of region name strings
 */
export function mapRegions(json: unknown): string[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json
        .map((r: unknown) =>
            typeof r === 'string' ? lastPathSegment(r.trim()) : lastPathSegment(asRecord(r)?.name)
        )
        .filter(Boolean);
}
