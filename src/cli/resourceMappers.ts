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
export function mapSecretManagerItems(json: any): SecretManagerItem[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json.map((s: any) => {
        const rawName = s?.name || '';
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
export function mapBigtableInstances(json: any): string[] {
    return Array.isArray(json) ? json.map((i: any) => lastPathSegment(i?.name)).filter(Boolean) : [];
}

/**
 * Maps `gcloud bigtable instances tables list --format=json` to short table IDs.
 *
 * @param json Parsed gcloud JSON output
 * @returns Short Bigtable table IDs
 */
export function mapBigtableTables(json: any): string[] {
    return Array.isArray(json) ? json.map((t: any) => lastPathSegment(t?.name)).filter(Boolean) : [];
}

/**
 * Maps `gcloud sql instances list --format=json` to instance connection names
 * (`project:region:instance`), which is what the Cloud SQL connectors expect.
 * Falls back to composing the name from project/region/name when connectionName is absent.
 *
 * @param json Parsed gcloud JSON output
 * @returns Cloud SQL instance connection names
 */
export function mapSqlInstances(json: any): string[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json.map((i: any) => {
        if (typeof i?.connectionName === 'string' && i.connectionName) {
            return i.connectionName;
        }
        return i?.project && i?.region && i?.name ? `${i.project}:${i.region}:${i.name}` : '';
    }).filter(Boolean);
}

/**
 * Maps `gcloud sql databases list --format=json` to database names.
 *
 * @param json Parsed gcloud JSON output
 * @returns Database names
 */
export function mapSqlDatabases(json: any): string[] {
    return Array.isArray(json) ? json.map((d: any) => d?.name).filter(Boolean) : [];
}

/**
 * Maps `gcloud projects list --format=json` to project IDs.
 *
 * @param json Parsed gcloud JSON output
 * @returns Project IDs
 */
export function mapProjects(json: any): string[] {
    return Array.isArray(json) ? json.map((p: any) => p?.projectId).filter(Boolean) : [];
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
export function mapHealthcareDatasetItems(json: any): string[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json
        .map((d: any) => (typeof d?.name === 'string' ? d.name.trim() : ''))
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
export function mapDataprocClusters(json: any): DataprocClusterItem[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json
        .map((c: any): DataprocClusterItem | null => {
            const rawClusterName =
                typeof c?.clusterName === 'string' && c.clusterName.trim()
                    ? c.clusterName.trim()
                    : lastPathSegment(c?.name);
            if (!rawClusterName) {
                return null;
            }
            const status =
                typeof c?.status?.state === 'string' && c.status.state.trim()
                    ? c.status.state.trim()
                    : typeof c?.status === 'string' && c.status.trim()
                        ? c.status.trim()
                        : undefined;
            const rawRegion =
                typeof c?.region === 'string' && c.region.trim()
                    ? c.region.trim()
                    : typeof c?.labels?.['goog-dataproc-location'] === 'string' &&
                        c.labels['goog-dataproc-location'].trim()
                        ? c.labels['goog-dataproc-location'].trim()
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
export function mapSessionTemplates(json: any): SessionTemplateItem[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json
        .map((t: any): SessionTemplateItem | null => {
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

            const props = t?.runtimeConfig?.properties;
            if (props && typeof props === 'object') {
                if (
                    typeof props['spark.sql.defaultCatalog'] === 'string' &&
                    props['spark.sql.defaultCatalog'].trim()
                ) {
                    defaultCatalog = props['spark.sql.defaultCatalog'].trim();
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
export function mapBigLakeCatalogs(json: any): BigLakeCatalogItem[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json
        .map((item: any): BigLakeCatalogItem | null => {
            const rawName =
                typeof item === 'string'
                    ? item.trim()
                    : typeof item?.name === 'string'
                        ? item.name.trim()
                        : typeof item?.id === 'string'
                            ? item.id.trim()
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
                (typeof item?.warehouse === 'string' && item.warehouse.trim()) ||
                (typeof item?.default_location === 'string' && item.default_location.trim()) ||
                (typeof item?.defaultLocation === 'string' && item.defaultLocation.trim()) ||
                `gs://${shortId}`;
            const catalogType =
                (typeof item?.catalog_type === 'string' && item.catalog_type.trim()) ||
                (typeof item?.catalogType === 'string' && item.catalogType.trim()) ||
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
export function mapRegions(json: any): string[] {
    if (!Array.isArray(json)) {
        return [];
    }
    return json
        .map((r: any) => (typeof r === 'string' ? lastPathSegment(r.trim()) : lastPathSegment(r?.name)))
        .filter(Boolean);
}
