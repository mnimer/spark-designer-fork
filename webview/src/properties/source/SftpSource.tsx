/**
 * @fileoverview Standalone SFTP Source Properties Component (EXT2).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for standalone batch SFTP Source nodes
 * (`SftpSource`) in Apache Spark 4.x pipelines backed by Spark 4.0 Python DataSourceReader
 * and Paramiko SSH transport.
 *
 * Architectural Decisions:
 * - Validates required SFTP host, port range (1..65535, default 22), username, remote path,
 *   and explicit Catalyst DDL schema.
 * - Exposes format-specific parsing controls for CSV, JSON, Parquet, Text, and XML files.
 * - Integrates SSH private key or password authentication modes with Google Cloud Secret Manager.
 * - Groups strict host key verification, known_hosts path, and parallel file partition count in
 *   a CollapsibleSection and anchors SystemInstructionsField at the bottom of the root flex column.
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

/** Supported remote file formats for SftpSource. */
export type SftpFileFormat = 'csv' | 'json' | 'parquet' | 'text' | 'xml';

/** Supported SSH authentication mechanisms for SftpSource. */
export type SftpAuthType = 'ssh_key' | 'password';

const FILE_FORMAT_OPTIONS: SelectFieldOption[] = [
  { value: 'csv', label: 'CSV (Delimited Text)' },
  { value: 'json', label: 'JSON / JSON Lines' },
  { value: 'parquet', label: 'Apache Parquet (PyArrow)' },
  { value: 'text', label: 'Plain Text (Line per Row)' },
  { value: 'xml', label: 'XML (Row Tag Extraction)' },
];

const AUTH_TYPE_OPTIONS: SelectFieldOption[] = [
  { value: 'ssh_key', label: 'SSH Private Key (PEM)' },
  { value: 'password', label: 'SSH Password' },
];

const SFTP_AUTH_MODE_OPTIONS: AuthModeOption[] = [
  { value: 'default', label: 'Environment Variable (SFTP_AUTH_SECRET)' },
  { value: 'secretManager', label: 'Google Cloud Secret Manager' },
];

/**
 * Validates that an SFTP port number is an integer between 1 and 65535.
 *
 * @param raw Raw port value from node properties.
 * @returns Validation error message, or null when valid.
 */
function validatePort(raw: number | string | undefined): string | null {
  if (raw === undefined || raw === null || String(raw).trim() === '') {
    return 'Port must be an integer between 1 and 65535.';
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
    return 'Port must be an integer between 1 and 65535.';
  }
  return null;
}

/**
 * Validates an optional positive integer count when a value is entered.
 *
 * @param raw Raw numeric input value.
 * @param label Field label for error reporting.
 * @returns Validation error message, or null when valid or empty.
 */
function validatePositiveInt(raw: number | string | undefined, label: string): string | null {
  if (raw === undefined || raw === null || String(raw).trim() === '') {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 1) {
    return `${label} must be an integer greater than or equal to 1.`;
  }
  return null;
}

/** Node data shape accepted by {@link SftpSourceProperties}. */
export interface SftpSourceNode {
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      host?: string;
      port?: number | string;
      username?: string;
      remotePath?: string;
      filePattern?: string;
      fileFormat?: SftpFileFormat;
      csvHeader?: boolean;
      csvDelimiter?: string;
      jsonMultiLine?: boolean;
      xmlRowTag?: string;
      schemaDDL?: string;
      authType?: SftpAuthType;
      authMode?: string;
      secretName?: string;
      strictHostKeyChecking?: boolean;
      knownHostsPath?: string;
      numPartitions?: number | string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/** Props for the {@link SftpSourceProperties} component. */
export interface SftpSourcePropertiesProps {
  node: SftpSourceNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Renders the property configuration form for standalone batch SFTP Spark 4.0 DataSource nodes.
 *
 * @param props Component props containing the graph node and property change callback.
 * @returns Controlled React form element.
 */
export const SftpSourceProperties: React.FC<SftpSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};

  const host = String(properties.host ?? '');
  const port = properties.port ?? 22;
  const username = String(properties.username ?? '');
  const remotePath = String(properties.remotePath ?? '');
  const filePattern = String(properties.filePattern ?? '');
  const fileFormat: SftpFileFormat = properties.fileFormat || 'csv';
  const csvHeader = properties.csvHeader ?? true;
  const csvDelimiter = String(properties.csvDelimiter ?? ',');
  const jsonMultiLine = properties.jsonMultiLine ?? false;
  const xmlRowTag = String(properties.xmlRowTag ?? 'row');
  const schemaDDL = String(properties.schemaDDL ?? '');
  const authType: SftpAuthType = properties.authType || 'ssh_key';
  const authMode = String(properties.authMode ?? 'default');
  const secretName = String(properties.secretName ?? '');
  const strictHostKeyChecking = properties.strictHostKeyChecking ?? true;
  const knownHostsPath = String(properties.knownHostsPath ?? '');
  const numPartitions = properties.numPartitions ?? 4;

  const [isAdvancedOpen, setIsAdvancedOpen] = useState<boolean>(false);

  const hostTrimmed = host.trim();
  const hostError =
    hostTrimmed.length === 0
      ? 'Host is required.'
      : /\s/.test(hostTrimmed)
        ? 'Host cannot contain whitespace.'
        : null;
  const portError = validatePort(port);
  const usernameError = username.trim().length === 0 ? 'Username is required.' : null;
  const remotePathError = remotePath.trim().length === 0 ? 'Remote Path is required.' : null;
  const schemaDDLError = schemaDDL.trim().length === 0 ? 'Output Schema (DDL) is required.' : null;
  const xmlRowTagError =
    fileFormat === 'xml' && xmlRowTag.trim().length === 0
      ? 'XML Row Tag is required when File Format is XML.'
      : null;
  const numPartitionsError = validatePositiveInt(numPartitions, 'Number of Partitions');
  const secretError =
    authMode === 'secretManager' && !secretName.trim()
      ? 'Secret Manager Secret is required when Secret Manager is selected.'
      : null;

  const systemInstructions = resolveSystemInstructions(node);

  const handleFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const nextValue =
      type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    onChange({
      ...properties,
      [name]: nextValue,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* SFTP Server Host, Port, and Username */}
      <div className="grid grid-cols-3 gap-4">
        <div className="col-span-2">
          <PropertyField
            id="sftpHost"
            name="host"
            label="SFTP Host"
            required="Host is required."
            tooltip="Hostname or IP address of the remote SFTP server."
            value={host}
            onChange={handleFieldChange}
            placeholder="sftp.partner.example.com"
            error={hostError}
            mono={true}
          />
        </div>

        <PropertyField
          id="sftpPort"
          name="port"
          type="number"
          min={1}
          max={65535}
          label="Port"
          required="Port is required."
          tooltip="SSH / SFTP TCP port between 1 and 65535 (defaults to 22)."
          value={port}
          onChange={handleFieldChange}
          error={portError}
          mono={true}
        />
      </div>

      <PropertyField
        id="sftpUsername"
        name="username"
        label="Username"
        required="Username is required."
        tooltip="SSH account username used when opening the Paramiko SFTP session."
        value={username}
        onChange={handleFieldChange}
        placeholder="spark_ingest"
        error={usernameError}
        mono={true}
      />

      {/* Remote Path and Glob Filter */}
      <div className="grid grid-cols-2 gap-4">
        <PropertyField
          id="sftpRemotePath"
          name="remotePath"
          label="Remote Path"
          required="Remote Path is required."
          tooltip="Remote directory or file path on the SFTP server (for example /incoming/orders)."
          value={remotePath}
          onChange={handleFieldChange}
          placeholder="/incoming/orders"
          error={remotePathError}
          mono={true}
        />

        <PropertyField
          id="sftpFilePattern"
          name="filePattern"
          label="File Glob Pattern"
          tooltip="Optional fnmatch glob pattern filtering filenames inside Remote Path (defaults to * when blank)."
          value={filePattern}
          onChange={handleFieldChange}
          placeholder="*.csv"
          mono={true}
        />
      </div>

      {/* File Format and Format-Specific Parsing Controls */}
      <div className="flex flex-col gap-4 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <SelectField
          id="sftpFileFormat"
          name="fileFormat"
          label="File Format"
          tooltip="Format parser applied on executors after streaming remote bytes over SFTP."
          value={fileFormat}
          options={FILE_FORMAT_OPTIONS}
          onChange={handleFieldChange}
        />

        {fileFormat === 'csv' && (
          <div className="flex flex-col gap-4">
            <label htmlFor="sftpCsvHeader" className="flex items-center gap-2 cursor-pointer">
              <input
                id="sftpCsvHeader"
                type="checkbox"
                name="csvHeader"
                checked={csvHeader}
                onChange={handleFieldChange}
                className="w-4 h-4 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)]"
              />
              <span className="text-sm font-medium text-[var(--vscode-text-primary)]">
                First Row Contains Header
              </span>
              <InfoTooltip text="Map CSV columns by header name with csv.DictReader when enabled, or by positional field order when disabled." />
            </label>

            <PropertyField
              id="sftpCsvDelimiter"
              name="csvDelimiter"
              label="CSV Delimiter"
              tooltip="Single-character field delimiter for CSV parsing (defaults to comma)."
              value={csvDelimiter}
              onChange={handleFieldChange}
              placeholder=","
              mono={true}
            />
          </div>
        )}

        {fileFormat === 'json' && (
          <label htmlFor="sftpJsonMultiLine" className="flex items-center gap-2 cursor-pointer">
            <input
              id="sftpJsonMultiLine"
              type="checkbox"
              name="jsonMultiLine"
              checked={jsonMultiLine}
              onChange={handleFieldChange}
              className="w-4 h-4 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)]"
            />
            <span className="text-sm font-medium text-[var(--vscode-text-primary)]">
              Multi-Line JSON Payload
            </span>
            <InfoTooltip text="Enable when each remote file contains a pretty-printed JSON array or object instead of newline-delimited JSON." />
          </label>
        )}

        {fileFormat === 'xml' && (
          <PropertyField
            id="sftpXmlRowTag"
            name="xmlRowTag"
            label="XML Row Tag"
            required="XML Row Tag is required when File Format is XML."
            tooltip="Repeating XML element tag extracted as one DataFrame row (for example row or record)."
            value={xmlRowTag}
            onChange={handleFieldChange}
            placeholder="row"
            error={xmlRowTagError}
            mono={true}
          />
        )}
      </div>

      {/* Explicit Output Catalyst DDL Schema */}
      <PropertyField
        id="sftpSchemaDDL"
        name="schemaDDL"
        type="textarea"
        rows={3}
        label="Output Schema (DDL)"
        required="Output Schema (DDL) is required."
        tooltip="Explicit Spark Catalyst DDL schema parsed via StructType.fromDDL(schemaDDL)."
        value={schemaDDL}
        onChange={handleFieldChange}
        placeholder="invoice_id STRING, customer_id STRING, total STRING"
        error={schemaDDLError}
        mono={true}
      />

      {/* SSH Authentication and Secret Manager */}
      <div className="flex flex-col gap-4 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)] flex items-center gap-2">
          <span>SSH Authentication</span>
          <InfoTooltip text="Resolve the SSH private key PEM or password once on the Spark driver from Google Cloud Secret Manager." />
        </span>

        <SelectField
          id="sftpAuthType"
          name="authType"
          label="SSH Auth Type"
          tooltip="Authenticate with a PEM-encoded SSH private key (RSA, Ed25519, ECDSA) or an SSH password."
          value={authType}
          options={AUTH_TYPE_OPTIONS}
          onChange={handleFieldChange}
        />

        <SecretManagerAuthFields
          idPrefix="sftpSource"
          datalistId="sftp-source-secrets-list"
          authMode={authMode}
          secretName={secretName}
          serviceName="SFTP"
          authOptions={SFTP_AUTH_MODE_OPTIONS}
          authModeTooltip="Choose whether to fetch the SSH private key or password from Google Cloud Secret Manager once on the driver."
          secretNameTooltip="Secret Manager secret ID or version resource path storing the SSH private key PEM or password."
          secretPlaceholder="projects/my-project/secrets/sftp-private-key/versions/latest"
          secretError={secretError}
          onChange={handleFieldChange}
        />
      </div>

      {/* Host Key Verification and Parallel Sharding */}
      <CollapsibleSection
        id="sftpAdvancedSection"
        title="Host Key Verification & Sharding"
        icon={<Sliders className="w-4 h-4 text-[var(--vscode-text-muted)]" />}
        isOpen={isAdvancedOpen}
        onToggle={() => setIsAdvancedOpen((prev) => !prev)}
      >
        <label
          htmlFor="sftpStrictHostKeyChecking"
          className="flex items-center gap-2 cursor-pointer"
        >
          <input
            id="sftpStrictHostKeyChecking"
            type="checkbox"
            name="strictHostKeyChecking"
            checked={strictHostKeyChecking}
            onChange={handleFieldChange}
            className="w-4 h-4 rounded border-[var(--vscode-border)] bg-[var(--vscode-input-bg)]"
          />
          <span className="text-sm font-medium text-[var(--vscode-text-primary)]">
            Strict Host Key Checking
          </span>
          <InfoTooltip text="Verify the remote SSH server host key against known_hosts before opening the SFTP channel." />
        </label>

        <PropertyField
          id="sftpKnownHostsPath"
          name="knownHostsPath"
          label="Known Hosts File Path"
          tooltip="Path to the OpenSSH known_hosts file on driver and executor nodes (defaults to ~/.ssh/known_hosts when blank)."
          value={knownHostsPath}
          onChange={handleFieldChange}
          placeholder="/etc/ssh/ssh_known_hosts"
          mono={true}
        />

        <PropertyField
          id="sftpNumPartitions"
          name="numPartitions"
          type="number"
          min={1}
          label="Number of Partitions"
          tooltip="Maximum number of Spark InputPartitions across which matched remote SFTP files are sharded."
          value={numPartitions}
          onChange={handleFieldChange}
          error={numPartitionsError}
          mono={true}
        />
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="sftpSourceInstructions"
        value={systemInstructions}
        tooltip="Custom instructions appended to the Gemini prompt when generating PySpark code for this SFTP source."
        placeholder="e.g. Cast numeric string columns after reading CSV files from /incoming."
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
