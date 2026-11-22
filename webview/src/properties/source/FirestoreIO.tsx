/**
 * Firestore Source Properties Component (SRC33)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Google Cloud Firestore Source nodes
 * in Apache Spark 4.x pipelines using the Spark 4.0 Python DataSourceReader API
 * (google-cloud-firestore).
 *
 * Architectural Decisions:
 * - Batch-only Python DataSourceReader reading documents from a Firestore collection path.
 * - Configures projectId, databaseId (default '(default)'), collectionPath, filterJson,
 *   tabular SchemaEditor, and SystemInstructionsField.
 */

import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { SchemaEditor, resolveSchemaFields, type SchemaField } from '../../components/common/SchemaEditor';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/** Props for the {@link FirestoreIOSourceProperties} component. */
export interface FirestoreIOSourcePropertiesProps {
  node: {
    data: {
      label?: string;
      spark_transform?: string;
      system_instructions?: string;
      special_instructions?: string;
      properties?: {
        projectId?: string;
        databaseId?: string;
        collectionPath?: string;
        collectionId?: string;
        filterJson?: string;
        fieldFilters?: string;
        schemaFields?: SchemaField[];
        schema?: string;
        schemaString?: string;
        system_instructions?: string;
        special_instructions?: string;
        [key: string]: unknown;
      };
    };
  };
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Configures properties for Apache Spark Firestore batch source nodes (Python DataSourceReader).
 */
export const FirestoreIOSourceProperties: React.FC<FirestoreIOSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node?.data?.properties || {};

  const projectId = properties.projectId || '';
  const databaseId = properties.databaseId || '(default)';
  const collectionPath = properties.collectionPath ?? properties.collectionId ?? '';
  const filterJson = properties.filterJson ?? properties.fieldFilters ?? '';
  const schemaFields = React.useMemo(
    () =>
      resolveSchemaFields({
        schemaFields: properties.schemaFields,
        schema: properties.schema,
        schemaString: properties.schemaString,
      }),
    [properties.schemaFields, properties.schema, properties.schemaString]
  );

  const isProjectEmpty = !projectId.trim();
  const isCollectionEmpty = !String(collectionPath).trim();

  const systemInstructions = resolveSystemInstructions(node);

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>
  ) => {
    const { name, value, type } = e.target;
    const updatedValue = type === 'checkbox' ? (e.target as HTMLInputElement).checked : value;
    const next = { ...properties, [name]: updatedValue };
    if (name === 'collectionPath') {
      delete next.collectionId;
    }
    if (name === 'filterJson') {
      delete next.fieldFilters;
    }
    onChange(next);
  };

  const handleSchemaChange = (data: {
    schemaFields: SchemaField[];
    schema: string;
    schemaString: string;
  }) => {
    onChange({
      ...properties,
      schemaFields: data.schemaFields,
      schema: data.schema,
      schemaString: data.schemaString,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4 text-[var(--vscode-text-primary)] text-sm">
      {/* Connector Header Badge */}
      <div className="flex items-center justify-between p-2.5 rounded bg-slate-800/40 border border-slate-700/60 text-xs">
        <span className="font-semibold text-slate-200">Spark 4.0 Python DataSourceReader (Firestore)</span>
        <span className="px-2 py-0.5 rounded bg-blue-500/20 text-blue-300 font-mono text-[11px]">
          google-cloud-firestore · BATCH
        </span>
      </div>

      {/* Firestore Target Coordinates */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Firestore Collection Coordinates</span>
          <InfoTooltip text="Configure GCP Project ID, Firestore Database ID, and target collection path." />
        </label>

        <div className="grid grid-cols-2 gap-2">
          <PropertyField
            id="firestoreSourceProjectId"
            name="projectId"
            label="GCP Project ID"
            required="Project ID is required"
            tooltip="Google Cloud Project ID hosting Firestore."
            value={projectId}
            invalid={isProjectEmpty}
            error={isProjectEmpty ? 'Project ID is required' : null}
            placeholder="my-gcp-project"
            onChange={handleChange}
            mono={true}
          />
          <PropertyField
            id="firestoreSourceDatabaseId"
            name="databaseId"
            label="Database ID"
            tooltip="Firestore database ID (defaults to '(default)')."
            value={databaseId}
            placeholder="(default)"
            onChange={handleChange}
            mono={true}
          />
        </div>

        <PropertyField
          id="firestoreSourceCollectionPath"
          name="collectionPath"
          label="Collection Path"
          required="Collection path is required"
          tooltip="Firestore collection or subcollection path (for example users or orgs/acme/orders)."
          value={collectionPath}
          invalid={isCollectionEmpty}
          error={isCollectionEmpty ? 'Collection path is required' : null}
          placeholder="users"
          onChange={handleChange}
          mono={true}
        />

        <PropertyField
          id="firestoreSourceFilterJson"
          name="filterJson"
          label="Predicate Filters (JSON)"
          type="textarea"
          rows={3}
          tooltip="Optional JSON array of Firestore FieldFilter predicates."
          value={filterJson}
          placeholder='[{"field": "status", "op": "==", "value": "active"}]'
          onChange={handleChange}
          mono={true}
        />
      </div>

      {/* Output DataFrame Schema Editor */}
      <div className="flex flex-col gap-2 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border-light)]">
        <label className="flex items-center gap-1.5 text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          <span>Output DataFrame Schema</span>
          <InfoTooltip text="Defines the StructType schema returned by the Spark 4.0 Python FirestoreDataSource." />
        </label>
        <SchemaEditor
          fields={schemaFields}
          schemaTitle="Firestore Document Schema (JSON)"
          stringTitle="Spark StructType Schema String"
          stringPlaceholder="doc_id:STRING:REQUIRED, status:STRING, created_at:TIMESTAMP"
          jsonTextareaId="firestoreSchemaJsonTextarea"
          stringTextareaId="firestoreSchemaStringTextarea"
          onChange={handleSchemaChange}
        />
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="firestoreSourceInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for Spark Firestore Python DataSourceReader code generation..."
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

export const FirestoreSourceProperties = FirestoreIOSourceProperties;
