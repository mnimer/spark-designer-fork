/**
 * @fileoverview Shared multi-cloud storage backend configuration component for Spark file sources and sinks.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders a unified storage backend selector, URI path input with Google Cloud Storage
 * bucket autocomplete when GCS is active, optional file glob and recursive lookup controls,
 * and Secret Manager authentication fields for Amazon S3 and Azure Storage backends.
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { InfoTooltip } from '../InfoTooltip';
import { PropertyField } from './PropertyField';
import { SelectField, type SelectFieldOption } from './SelectField';
import { StoragePathField } from './StoragePathField';
import { SecretManagerAuthFields, type AuthModeOption } from './SecretManagerAuthFields';

/**
 * Supported multi-cloud and local filesystem schemes for Spark file connectors.
 */
export type StorageBackendType = 'gcs' | 's3a' | 'abfss' | 'wasbs' | 'hdfs' | 'local';

/**
 * Dropdown options for selecting a Spark file storage backend.
 */
export const STORAGE_BACKEND_OPTIONS: SelectFieldOption[] = [
  { value: 'gcs', label: 'Google Cloud Storage (gs://)' },
  { value: 's3a', label: 'Amazon S3 (s3a://)' },
  { value: 'abfss', label: 'Azure ADLS Gen2 (abfss://)' },
  { value: 'wasbs', label: 'Azure Blob Storage (wasbs://)' },
  { value: 'hdfs', label: 'Hadoop HDFS (hdfs://)' },
  { value: 'local', label: 'Local Filesystem (file://)' },
];

const S3_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'default', label: 'Default AWS Credentials Chain (IAM Role / Instance Profile)' },
  { value: 'secretManager', label: 'Google Cloud Secret Manager (AWS Access / Secret Key)' },
];

const AZURE_AUTH_OPTIONS: AuthModeOption[] = [
  { value: 'default', label: 'Default Azure Credentials (Managed Identity / Spark Conf)' },
  { value: 'secretManager', label: 'Google Cloud Secret Manager (Account Key / SAS / Service Principal)' },
];

const DEFAULT_PATH_PLACEHOLDERS: Record<StorageBackendType, string> = {
  gcs: 'gs://my-bucket/data/',
  s3a: 's3a://my-bucket/data/',
  abfss: 'abfss://container@account.dfs.core.windows.net/data/',
  wasbs: 'wasbs://container@account.blob.core.windows.net/data/',
  hdfs: 'hdfs://namenode:8020/data/',
  local: 'file:///var/data/ or /local/path/',
};

/**
 * Props for the {@link StorageBackendFields} component.
 */
export interface StorageBackendFieldsProps {
  /** Node properties dictionary owned by the parent form. */
  properties: Record<string, unknown>;
  /** Callback invoked with the updated node properties object. */
  onChange: (updated: Record<string, unknown>) => void;
  /** Unique prefix applied to DOM element IDs. */
  idPrefix: string;
  /** Property key storing the target URI or path. Defaults to 'path'. */
  pathKey?: string;
  /** Custom label for the URI or path field. */
  pathLabel?: string;
  /** Custom placeholder for the URI or path field. */
  pathPlaceholder?: string;
  /** Whether to render pathGlobFilter and recursiveFileLookup controls. Defaults to true. */
  showGlobAndRecursive?: boolean;
}

/**
 * Infers the Spark storage backend identifier from a URI scheme or path prefix.
 * Defaults to 'gcs' when the URI is empty or carries no recognized scheme.
 *
 * @param uri Raw URI or file path string.
 * @returns Inferred StorageBackendType value.
 */
export function inferStorageBackend(uri?: string): StorageBackendType {
  const trimmed = (uri ?? '').trim().toLowerCase();
  if (!trimmed) {
    return 'gcs';
  }
  if (trimmed.startsWith('gs://')) {
    return 'gcs';
  }
  if (trimmed.startsWith('s3a://') || trimmed.startsWith('s3://') || trimmed.startsWith('s3n://')) {
    return 's3a';
  }
  if (trimmed.startsWith('abfss://') || trimmed.startsWith('abfs://')) {
    return 'abfss';
  }
  if (trimmed.startsWith('wasbs://') || trimmed.startsWith('wasb://')) {
    return 'wasbs';
  }
  if (trimmed.startsWith('hdfs://')) {
    return 'hdfs';
  }
  if (
    trimmed.startsWith('file://') ||
    trimmed.startsWith('/') ||
    trimmed.startsWith('./') ||
    trimmed.startsWith('../')
  ) {
    return 'local';
  }
  return 'gcs';
}

/**
 * Resolves the current path string from the configured key or legacy fallback keys.
 *
 * @param properties Node properties dictionary.
 * @param pathKey Primary property key for the path.
 * @returns Current path string value.
 */
export function resolveStoragePathValue(
  properties: Record<string, unknown>,
  pathKey: string = 'path'
): string {
  if (properties[pathKey] !== undefined && properties[pathKey] !== null) {
    return String(properties[pathKey]);
  }
  if (properties.path !== undefined && properties.path !== null) {
    return String(properties.path);
  }
  if (properties.filePattern !== undefined && properties.filePattern !== null) {
    return String(properties.filePattern);
  }
  if (properties.uri !== undefined && properties.uri !== null) {
    return String(properties.uri);
  }
  return '';
}

/**
 * Renders shared storage backend selection, URI path input, file discovery filters,
 * and cloud authentication controls for Spark file connectors.
 *
 * @param props Component configuration properties.
 * @returns Rendered storage backend field group.
 */
export const StorageBackendFields: React.FC<StorageBackendFieldsProps> = ({
  properties,
  onChange,
  idPrefix,
  pathKey = 'path',
  pathLabel = 'Storage URI / Path',
  pathPlaceholder,
  showGlobAndRecursive = true,
}) => {
  const currentPath = resolveStoragePathValue(properties, pathKey);
  const configuredBackend = properties.storageBackend as StorageBackendType | undefined;
  const effectiveBackend: StorageBackendType =
    configuredBackend && STORAGE_BACKEND_OPTIONS.some((opt) => opt.value === configuredBackend)
      ? configuredBackend
      : inferStorageBackend(currentPath);

  const pathGlobFilter =
    typeof properties.pathGlobFilter === 'string' ? properties.pathGlobFilter : '';
  const recursiveFileLookup = Boolean(properties.recursiveFileLookup);
  const authMode =
    typeof properties.authMode === 'string' && properties.authMode ? properties.authMode : 'default';
  const secretName =
    typeof properties.secretName === 'string' ? properties.secretName : '';

  const effectivePlaceholder =
    pathPlaceholder || DEFAULT_PATH_PLACEHOLDERS[effectiveBackend] || DEFAULT_PATH_PLACEHOLDERS.gcs;
  const isPathInvalid = currentPath.trim().length === 0;
  const pathErrorMessage = `${pathLabel} is required.`;

  const handleFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const isCheckbox = type === 'checkbox';
    const checked = isCheckbox ? (e.target as HTMLInputElement).checked : undefined;

    onChange({
      ...properties,
      [name]: isCheckbox ? checked : value,
    });
  };

  const requiresCloudAuth =
    effectiveBackend === 's3a' || effectiveBackend === 'abfss' || effectiveBackend === 'wasbs';

  return (
    <div className="flex flex-col gap-4">
      {/* Storage Backend Selector */}
      <SelectField
        id={`${idPrefix}StorageBackend`}
        name="storageBackend"
        label="Storage Backend"
        tooltip="Select the target cloud object store or filesystem protocol for Spark file I/O."
        value={effectiveBackend}
        options={STORAGE_BACKEND_OPTIONS}
        onChange={handleFieldChange}
      />

      {/* URI / Path Input (uses StoragePathField for GCS bucket autocomplete) */}
      {effectiveBackend === 'gcs' ? (
        <StoragePathField
          id={`${idPrefix}Path`}
          name={pathKey}
          label={pathLabel}
          tooltip="Target Cloud Storage URI or glob pattern (for example, gs://bucket/path/). Required."
          value={currentPath}
          onChange={handleFieldChange}
          placeholder={effectivePlaceholder}
          errorMessage={pathErrorMessage}
          helperText={
            !isPathInvalid
              ? 'Target URI or glob pattern in Google Cloud Storage (gs://).'
              : undefined
          }
        />
      ) : (
        <PropertyField
          id={`${idPrefix}Path`}
          name={pathKey}
          label={pathLabel}
          tooltip="Target storage URI, directory path, or glob pattern. Required."
          required={pathErrorMessage}
          aria-required={true}
          aria-invalid={isPathInvalid}
          invalid={isPathInvalid}
          error={isPathInvalid ? pathErrorMessage : null}
          value={currentPath}
          onChange={handleFieldChange}
          placeholder={effectivePlaceholder}
          mono={true}
          helperText={
            !isPathInvalid
              ? `Target URI or file path using the ${effectiveBackend} storage backend.`
              : undefined
          }
        />
      )}

      {/* Optional Glob Filter and Recursive Lookup Controls */}
      {showGlobAndRecursive && (
        <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
          <PropertyField
            id={`${idPrefix}PathGlobFilter`}
            name="pathGlobFilter"
            label="Path Glob Filter (pathGlobFilter)"
            tooltip="Optional glob pattern to filter files by filename (for example, *.parquet or *.csv) while preserving partition discovery."
            value={pathGlobFilter}
            onChange={handleFieldChange}
            placeholder="e.g. *.parquet, *.json, part-*"
            mono={true}
          />

          <label
            htmlFor={`${idPrefix}RecursiveFileLookup`}
            className="flex items-center gap-2 cursor-pointer"
          >
            <input
              id={`${idPrefix}RecursiveFileLookup`}
              type="checkbox"
              name="recursiveFileLookup"
              checked={recursiveFileLookup}
              onChange={handleFieldChange}
              className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
            />
            <span className="text-xs text-[var(--vscode-text-secondary)]">
              Recursive File Lookup (recursiveFileLookup)
            </span>
            <InfoTooltip text="Recursively scan nested subdirectories for input files. Disables Spark partition column inference when enabled." />
          </label>
        </div>
      )}

      {/* Cloud Storage Authentication via Secret Manager for S3 and Azure */}
      {requiresCloudAuth && (
        <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
          <SecretManagerAuthFields
            idPrefix={idPrefix}
            authMode={authMode}
            secretName={secretName}
            serviceName={
              effectiveBackend === 's3a'
                ? 'Amazon S3'
                : effectiveBackend === 'abfss'
                  ? 'Azure ADLS Gen2'
                  : 'Azure Blob Storage'
            }
            authOptions={effectiveBackend === 's3a' ? S3_AUTH_OPTIONS : AZURE_AUTH_OPTIONS}
            secretPlaceholder={
              effectiveBackend === 's3a'
                ? 'e.g. aws-s3-credentials or projects/.../secrets/...'
                : 'e.g. azure-storage-credentials or projects/.../secrets/...'
            }
            authModeTooltip="Select how Spark authenticates to the cloud storage backend. Use Google Cloud Secret Manager to avoid hardcoding storage keys."
            secretNameTooltip="Secret Manager secret identifier storing cloud storage credentials fetched at runtime via get_secret()."
            onChange={handleFieldChange}
          />
        </div>
      )}
    </div>
  );
};
