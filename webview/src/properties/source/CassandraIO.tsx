/**
 * Cassandra Source Properties Component (SRC34)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Cassandra Source nodes
 * (Spark 4.0 Python DataSource via cassandra-driver / .format("org.apache.spark.sql.cassandra")).
 *
 * Architectural Decisions:
 * - Batch-only source configuring contactPoints, port (default 9042), localDc,
 *   keyspace, table, cqlFilter, SecretManagerAuthFields, and SystemInstructionsField.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SecretManagerAuthFields, type SecretItem } from '../../components/common/SecretManagerAuthFields';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

export type { SecretItem };

/** Props for the {@link CassandraIOSourceProperties} component. */
export interface CassandraIOSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        contactPoints?: string;
        hosts?: string;
        port?: number | string;
        localDc?: string;
        keyspace?: string;
        table?: string;
        cqlFilter?: string;
        query?: string;
        authMode?: string;
        secretName?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: any;
      };
    };
  };
  onChange: (properties: any) => void;
}

/**
 * Configures properties for Apache Spark Cassandra batch source nodes.
 */
export const CassandraIOSourceProperties: React.FC<CassandraIOSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const contactPoints = properties.contactPoints ?? properties.hosts ?? '';
  const port = properties.port !== undefined && properties.port !== '' ? properties.port : 9042;
  const localDc = properties.localDc || '';
  const keyspace = properties.keyspace || '';
  const table = properties.table || '';
  const cqlFilter = properties.cqlFilter ?? properties.query ?? '';
  const authMode = properties.authMode || 'default';
  const secretName = properties.secretName || '';

  const isContactPointsEmpty = !String(contactPoints).trim();
  const isKeyspaceEmpty = !keyspace.trim();
  const isTableEmpty = !table.trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next = { ...properties, [name]: updatedValue };
    if (name === 'contactPoints') {
      delete next.hosts;
    }
    if (name === 'cqlFilter') {
      delete next.query;
    }
    onChange(next);
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark 4.0 Cassandra Source</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          cassandra-driver · BATCH
        </span>
      </div>

      {/* Cluster Connection */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Cassandra Cluster Connection</span>
          <InfoTooltip text="Configures contactPoints, native CQL port (9042), and local datacenter." />
        </label>

        <PropertyField
          id="cassandraSourceContactPoints"
          name="contactPoints"
          label="Contact Points (contactPoints)"
          required="Contact points are required"
          tooltip="Comma-separated list of Cassandra seed node hostnames or IP addresses."
          value={contactPoints}
          invalid={isContactPointsEmpty}
          error={isContactPointsEmpty ? 'Contact points are required' : null}
          placeholder="10.0.0.1,10.0.0.2"
          onChange={handleChange}
          mono={true}
        />

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="cassandraSourcePort"
            name="port"
            label="Port"
            type="number"
            tooltip="Native CQL transport port (default 9042)."
            value={port}
            placeholder="9042"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="cassandraSourceLocalDc"
            name="localDc"
            label="Local Datacenter (localDc)"
            tooltip="Local datacenter name for DCAwareRoundRobinPolicy."
            value={localDc}
            placeholder="datacenter1"
            onChange={handleChange}
            mono={true}
          />
        </div>
      </div>

      {/* Keyspace, Table & CQL Filter */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="cassandraSourceKeyspace"
            name="keyspace"
            label="Keyspace"
            required="Keyspace is required"
            tooltip="Target Cassandra keyspace name."
            value={keyspace}
            invalid={isKeyspaceEmpty}
            error={isKeyspaceEmpty ? 'Keyspace is required' : null}
            placeholder="telemetry"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="cassandraSourceTable"
            name="table"
            label="Table"
            required="Table is required"
            tooltip="Target Cassandra table name."
            value={table}
            invalid={isTableEmpty}
            error={isTableEmpty ? 'Table is required' : null}
            placeholder="device_events"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="cassandraSourceCqlFilter"
          name="cqlFilter"
          label="CQL Filter Predicate (Optional)"
          tooltip="Optional CQL WHERE predicate pushed down to token-range queries."
          value={cqlFilter}
          placeholder="event_type = 'ALERT'"
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Secret Manager Authentication */}
      <SecretManagerAuthFields
        idPrefix="cassandraSource"
        serviceName="Apache Cassandra"
        authMode={authMode}
        secretName={secretName}
        datalistId="cassandra-source-secret-options"
        secretPlaceholder="e.g. cassandra-credentials-secret"
        onChange={handleChange}
      />

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="cassandraSourceInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Cassandra source code generation..."
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

export const CassandraSourceProperties = CassandraIOSourceProperties;
