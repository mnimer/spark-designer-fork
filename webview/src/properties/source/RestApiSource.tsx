/**
 * @fileoverview REST API Source Properties Component (EXT1).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for batch HTTP and REST API Source nodes
 * (`RestApiSource`) in Apache Spark 4.x pipelines backed by Spark 4.0 Python DataSourceReader.
 *
 * Architectural Decisions:
 * - Validates non-empty HTTP or HTTPS endpoint URLs and Catalyst DDL output schemas.
 * - Exposes conditional pagination controls for offset/limit, cursor, and RFC 5988 link-header modes.
 * - Configures parallel range partitioning bounds for multi-executor REST extraction.
 * - Integrates HTTP authentication schemes with Google Cloud Secret Manager pointers.
 * - Places custom headers, request body, timeout, and retry settings in a CollapsibleSection
 *   and renders SystemInstructionsField as the final direct child of the root flex column.
 */

import React, { useState } from 'react';
import { Sliders } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField } from '../../components/common/SelectField';
import type { SelectFieldOption } from '../../components/common/SelectField';
import {
  SecretManagerAuthFields,
  type AuthModeOption,
} from '../../components/common/SecretManagerAuthFields';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Supported HTTP request verbs for RestApiSource. */
export type RestApiHttpMethod = 'GET' | 'POST';

/** Supported pagination strategies for RestApiSource. */
export type RestApiPaginationType = 'none' | 'offset_limit' | 'cursor' | 'link_header';

/** Supported HTTP authentication schemes for RestApiSource. */
export type RestApiAuthScheme = 'none' | 'bearer' | 'api_key' | 'basic';

const HTTP_METHOD_OPTIONS: SelectFieldOption[] = [
  { value: 'GET', label: 'GET' },
  { value: 'POST', label: 'POST' },
];

const PAGINATION_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'none', label: 'None (Single Request)' },
  { value: 'offset_limit', label: 'Offset / Limit' },
  { value: 'cursor', label: 'Cursor / Next Token' },
  { value: 'link_header', label: 'RFC 5988 Link Header (rel="next")' },
];

const AUTH_SCHEME_OPTIONS: SelectFieldOption[] = [
  { value: 'none', label: 'None (Public Endpoint)' },
  { value: 'bearer', label: 'Bearer Token (Authorization: Bearer)' },
  { value: 'api_key', label: 'API Key Header' },
  { value: 'basic', label: 'HTTP Basic Auth (username:password)' },
];

const REST_AUTH_MODE_OPTIONS: AuthModeOption[] = [
  { value: 'default', label: 'No Secret Manager Credential' },
  { value: 'secretManager', label: 'Google Cloud Secret Manager' },
];

/**
 * Validates that an endpoint URL is non-empty and uses the HTTP or HTTPS protocol.
 *
 * @param raw Raw endpoint URL value entered by the user.
 * @returns Validation error message, or null when the URL is valid.
 */
function validateEndpointUrl(raw: string): string | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return 'Endpoint URL is required.';
  }
  try {
    const parsed = new URL(trimmed);
    if ((parsed.protocol !== 'http:' && parsed.protocol !== 'https:') || !parsed.hostname) {
      return 'Endpoint URL must start with http:// or https:// and include a hostname.';
    }
    return null;
  } catch {
    return 'Endpoint URL must start with http:// or https:// and include a hostname.';
  }
}

/**
 * Validates an optional positive integer field when a value is present.
 *
 * @param raw Raw numeric input value.
 * @param label Human-readable field label for error messages.
 * @param min Minimum allowed integer value.
 * @returns Validation error message, or null when valid or blank.
 */
function validateOptionalInt(
  raw: string | number | undefined,
  label: string,
  min: number
): string | null {
  if (raw === undefined || raw === null || String(raw).trim() === '') {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < min) {
    return `${label} must be an integer greater than or equal to ${min}.`;
  }
  return null;
}

/** Node data shape accepted by {@link RestApiSourceProperties}. */
export interface RestApiSourceNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      endpointUrl?: string;
      httpMethod?: RestApiHttpMethod;
      schemaDDL?: string;
      recordsJsonPath?: string;
      paginationType?: RestApiPaginationType;
      pageSize?: number | string;
      maxPages?: number | string;
      offsetParam?: string;
      limitParam?: string;
      cursorParam?: string;
      nextCursorJsonPath?: string;
      numPartitions?: number | string;
      partitionParam?: string;
      lowerBound?: number | string;
      upperBound?: number | string;
      authScheme?: RestApiAuthScheme;
      authHeaderName?: string;
      authMode?: string;
      secretName?: string;
      requestHeaders?: string;
      requestBody?: string;
      timeoutSeconds?: number | string;
      maxRetries?: number | string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link RestApiSourceProperties} component. */
export interface RestApiSourcePropertiesProps {
  node: RestApiSourceNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the property configuration form for batch HTTP and REST API Spark 4.0 DataSource nodes.
 *
 * @param props Component props containing the graph node and property change callback.
 * @returns Controlled React form element.
 */
export const RestApiSourceProperties: React.FC<RestApiSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const endpointUrl = String(properties.endpointUrl ?? '');
  const httpMethod: RestApiHttpMethod = properties.httpMethod || 'GET';
  const schemaDDL = String(properties.schemaDDL ?? '');
  const recordsJsonPath = String(properties.recordsJsonPath ?? '');
  const paginationType: RestApiPaginationType = properties.paginationType || 'none';
  const pageSize = properties.pageSize ?? 100;
  const maxPages = properties.maxPages ?? 100;
  const offsetParam = String(properties.offsetParam ?? 'offset');
  const limitParam = String(properties.limitParam ?? 'limit');
  const cursorParam = String(properties.cursorParam ?? 'cursor');
  const nextCursorJsonPath = String(properties.nextCursorJsonPath ?? '');
  const numPartitions = properties.numPartitions ?? 1;
  const partitionParam = String(properties.partitionParam ?? '');
  const lowerBound = properties.lowerBound ?? '';
  const upperBound = properties.upperBound ?? '';
  const authScheme: RestApiAuthScheme = properties.authScheme || 'none';
  const authHeaderName = String(properties.authHeaderName ?? '');
  const authMode = String(properties.authMode ?? 'default');
  const secretName = String(properties.secretName ?? '');
  const requestHeaders = String(properties.requestHeaders ?? '');
  const requestBody = String(properties.requestBody ?? '');
  const timeoutSeconds = properties.timeoutSeconds ?? 30;
  const maxRetries = properties.maxRetries ?? 3;

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

  const endpointUrlError = validateEndpointUrl(endpointUrl);
  const schemaDDLError = schemaDDL.trim().length === 0 ? 'Output Schema (DDL) is required.' : null;
  const pageSizeError =
    paginationType !== 'none' ? validateOptionalInt(pageSize, 'Page Size', 1) : null;
  const maxPagesError =
    paginationType !== 'none' ? validateOptionalInt(maxPages, 'Max Pages', 1) : null;
  const numPartitionsError = validateOptionalInt(numPartitions, 'Number of Partitions', 1);

  const hasBothBounds =
    String(lowerBound).trim() !== '' &&
    String(upperBound).trim() !== '' &&
    !Number.isNaN(Number(lowerBound)) &&
    !Number.isNaN(Number(upperBound));
  const partitionBoundsError =
    hasBothBounds && Number(lowerBound) > Number(upperBound)
      ? 'Lower Bound must be less than or equal to Upper Bound.'
      : null;

  const secretError =
    authMode === 'secretManager' && !secretName.trim()
      ? 'Secret Manager Secret is required when Secret Manager is selected.'
      : null;

  const systemInstructions = resolveSystemInstructions(node);

  const handleFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    onChange({
      ...properties,
      [name]: value,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Endpoint URL and HTTP Method */}
      <PropertyField
        id="restApiEndpointUrl"
        name="endpointUrl"
        label="Endpoint URL"
        required="Endpoint URL is required."
        tooltip="Target HTTP or HTTPS REST API endpoint URL queried by the Spark 4.0 Python DataSourceReader."
        value={endpointUrl}
        onChange={handleFieldChange}
        placeholder="https://api.example.com/v1/orders"
        error={endpointUrlError}
        mono={true}
      />

      <SelectField
        id="restApiHttpMethod"
        name="httpMethod"
        label="HTTP Method"
        tooltip="HTTP verb sent for each page or partition request (GET or POST)."
        value={httpMethod}
        options={HTTP_METHOD_OPTIONS}
        onChange={handleFieldChange}
      />

      {/* Output Catalyst DDL Schema and Records JSON Path */}
      <PropertyField
        id="restApiSchemaDDL"
        name="schemaDDL"
        type="textarea"
        rows={3}
        label="Output Schema (DDL)"
        required="Output Schema (DDL) is required."
        tooltip="Explicit Spark Catalyst DDL schema parsed via StructType.fromDDL(schemaDDL), such as id STRING, created_at TIMESTAMP, amount DOUBLE."
        value={schemaDDL}
        onChange={handleFieldChange}
        placeholder="id STRING, created_at TIMESTAMP, amount DOUBLE, status STRING"
        error={schemaDDLError}
        mono={true}
      />

      <PropertyField
        id="restApiRecordsJsonPath"
        name="recordsJsonPath"
        label="Records JSON Path"
        tooltip="Dotted path to the list of record objects inside the JSON response body (for example data.items). Leave blank when the response root is already a JSON array."
        value={recordsJsonPath}
        onChange={handleFieldChange}
        placeholder="data.items"
        mono={true}
      />

      {/* Pagination Configuration */}
      <div className="flex flex-col gap-4 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)] flex items-center gap-2">
          <span>Pagination</span>
          <InfoTooltip text="Configure how RestApiBatchReader iterates through multi-page REST API responses." />
        </span>

        <SelectField
          id="restApiPaginationType"
          name="paginationType"
          label="Pagination Mode"
          tooltip="Select single-request mode, offset/limit parameters, cursor token extraction, or RFC 5988 Link rel='next' header traversal."
          value={paginationType}
          options={PAGINATION_TYPE_OPTIONS}
          onChange={handleFieldChange}
        />

        {paginationType !== 'none' && (
          <div className="grid grid-cols-2 gap-4">
            <PropertyField
              id="restApiPageSize"
              name="pageSize"
              type="number"
              min={1}
              label="Page Size"
              tooltip="Maximum number of records requested per HTTP page."
              value={pageSize}
              onChange={handleFieldChange}
              error={pageSizeError}
              mono={true}
            />

            <PropertyField
              id="restApiMaxPages"
              name="maxPages"
              type="number"
              min={1}
              label="Max Pages"
              tooltip="Safety upper bound on pages fetched per partition so runaway loops cannot hang an executor."
              value={maxPages}
              onChange={handleFieldChange}
              error={maxPagesError}
              mono={true}
            />
          </div>
        )}

        {paginationType === 'offset_limit' && (
          <div className="grid grid-cols-2 gap-4">
            <PropertyField
              id="restApiOffsetParam"
              name="offsetParam"
              label="Offset Parameter"
              tooltip="Query parameter name carrying the zero-based record offset (for example offset)."
              value={offsetParam}
              onChange={handleFieldChange}
              placeholder="offset"
              mono={true}
            />

            <PropertyField
              id="restApiLimitParam"
              name="limitParam"
              label="Limit Parameter"
              tooltip="Query parameter name carrying the page size limit (for example limit)."
              value={limitParam}
              onChange={handleFieldChange}
              placeholder="limit"
              mono={true}
            />
          </div>
        )}

        {paginationType === 'cursor' && (
          <div className="grid grid-cols-2 gap-4">
            <PropertyField
              id="restApiCursorParam"
              name="cursorParam"
              label="Cursor Parameter"
              tooltip="Query parameter name carrying the continuation cursor token on subsequent requests."
              value={cursorParam}
              onChange={handleFieldChange}
              placeholder="cursor"
              mono={true}
            />

            <PropertyField
              id="restApiNextCursorJsonPath"
              name="nextCursorJsonPath"
              label="Next Cursor JSON Path"
              tooltip="Dotted JSON path in the response body containing the next page cursor token (for example meta.next_cursor)."
              value={nextCursorJsonPath}
              onChange={handleFieldChange}
              placeholder="meta.next_cursor"
              mono={true}
            />
          </div>
        )}
      </div>

      {/* Parallel Range Partitioning */}
      <div className="flex flex-col gap-4 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)] flex items-center gap-2">
          <span>Parallel Range Partitioning</span>
          <InfoTooltip text="Split numeric or epoch bounds across multiple Spark InputPartitions so executors fetch disjoint slices in parallel." />
        </span>

        <div className="grid grid-cols-2 gap-4">
          <PropertyField
            id="restApiNumPartitions"
            name="numPartitions"
            type="number"
            min={1}
            label="Number of Partitions"
            tooltip="Number of Spark InputPartitions emitted by partitions(). Set above 1 with range bounds to parallelize reads."
            value={numPartitions}
            onChange={handleFieldChange}
            error={numPartitionsError}
            mono={true}
          />

          <PropertyField
            id="restApiPartitionParam"
            name="partitionParam"
            label="Partition Parameter"
            tooltip="Base query parameter name split across numeric range slices (for example id or updated_epoch)."
            value={partitionParam}
            onChange={handleFieldChange}
            placeholder="id"
            mono={true}
          />
        </div>

        <div className="grid grid-cols-2 gap-4">
          <PropertyField
            id="restApiLowerBound"
            name="lowerBound"
            type="number"
            label="Lower Bound"
            tooltip="Inclusive lower integer bound for parallel partition range splitting."
            value={lowerBound}
            onChange={handleFieldChange}
            invalid={Boolean(partitionBoundsError)}
            mono={true}
          />

          <PropertyField
            id="restApiUpperBound"
            name="upperBound"
            type="number"
            label="Upper Bound"
            tooltip="Inclusive upper integer bound for parallel partition range splitting."
            value={upperBound}
            onChange={handleFieldChange}
            invalid={Boolean(partitionBoundsError)}
            mono={true}
          />
        </div>

        {partitionBoundsError && (
          <span className="text-xs text-red-400" role="alert">
            {partitionBoundsError}
          </span>
        )}
      </div>

      {/* HTTP Authentication and Secret Manager */}
      <div className="flex flex-col gap-4 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)] flex items-center gap-2">
          <span>Authentication</span>
          <InfoTooltip text="Resolve API tokens or credentials once on the Spark driver from Google Cloud Secret Manager before launching executor tasks." />
        </span>

        <SelectField
          id="restApiAuthScheme"
          name="authScheme"
          label="HTTP Auth Scheme"
          tooltip="Select how the resolved Secret Manager credential is attached to outgoing HTTP requests."
          value={authScheme}
          options={AUTH_SCHEME_OPTIONS}
          onChange={handleFieldChange}
        />

        {authScheme !== 'none' && (
          <PropertyField
            id="restApiAuthHeaderName"
            name="authHeaderName"
            label="Auth Header Name"
            tooltip="Custom HTTP header name for authentication. Defaults to X-API-Key for api_key or Authorization for bearer and basic."
            value={authHeaderName}
            onChange={handleFieldChange}
            placeholder={authScheme === 'api_key' ? 'X-API-Key' : 'Authorization'}
            mono={true}
          />
        )}

        <SecretManagerAuthFields
          idPrefix="restApiSource"
          datalistId="rest-api-source-secrets-list"
          authMode={authMode}
          secretName={secretName}
          serviceName="REST API"
          authOptions={REST_AUTH_MODE_OPTIONS}
          authModeTooltip="Choose whether to resolve REST API credentials from Google Cloud Secret Manager once on the driver."
          secretNameTooltip="Secret Manager secret ID or version resource path storing the token, API key, or username:password credential."
          secretPlaceholder="projects/my-project/secrets/rest-api-key/versions/latest"
          secretError={secretError}
          onChange={handleFieldChange}
        />
      </div>

      {/* Custom Headers, Body, Timeout, and Retries */}
      <CollapsibleSection
        id="restApiAdvancedSection"
        title="Custom Headers, Body & Retries"
        icon={<Sliders className="w-4 h-4 text-[var(--vscode-text-muted)]" />}
        isOpen={isAdvancedOpen}
        onToggle={() => setIsAdvancedOpen((prev) => !prev)}
      >
        <PropertyField
          id="restApiRequestHeaders"
          name="requestHeaders"
          type="textarea"
          rows={2}
          label="Custom Headers (JSON)"
          tooltip="Optional JSON object of static non-secret HTTP request headers merged onto the executor Session."
          value={requestHeaders}
          onChange={handleFieldChange}
          placeholder='{"Accept": "application/json", "X-Client": "spark"}'
          mono={true}
        />

        <PropertyField
          id="restApiRequestBody"
          name="requestBody"
          type="textarea"
          rows={3}
          label="Request Body (POST)"
          tooltip="Optional JSON or text body payload sent when HTTP Method is set to POST."
          value={requestBody}
          onChange={handleFieldChange}
          placeholder='{"status": "ACTIVE"}'
          mono={true}
        />

        <div className="grid grid-cols-2 gap-4">
          <PropertyField
            id="restApiTimeoutSeconds"
            name="timeoutSeconds"
            type="number"
            min={1}
            label="Timeout (Seconds)"
            tooltip="Per-request HTTP socket and read timeout in seconds on executor tasks."
            value={timeoutSeconds}
            onChange={handleFieldChange}
            mono={true}
          />

          <PropertyField
            id="restApiMaxRetries"
            name="maxRetries"
            type="number"
            min={0}
            label="Max Retries (429 / 5xx)"
            tooltip="Maximum exponential backoff retries on HTTP 429 rate limits and 5xx server errors."
            value={maxRetries}
            onChange={handleFieldChange}
            mono={true}
          />
        </div>
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="restApiSourceInstructions"
        value={systemInstructions}
        tooltip="Custom instructions appended to the Gemini prompt when generating PySpark code for this REST API source."
        placeholder="e.g. Normalize nested line_items arrays and filter out test orders."
        helperText="Additional guidance appended to the prompt when synthesizing PySpark code."
        wrapperClassName="flex flex-col gap-2 pt-2 border-t border-[var(--vscode-border-light)]"
        onChange={(value) =>
          onChange({
            ...properties,
            system_instructions: value,
          })
        }
      />
    </div>
  );
};
