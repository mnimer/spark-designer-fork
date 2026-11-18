/**
 * @fileoverview ORC Source (SRC14 OrcSource) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark ORC source nodes
 * using spark.read.orc(uri) or spark.readStream.format("orc").load(uri).
 * Embeds StorageBackendFields, mergeSchema, maxFilesPerTrigger (for streaming),
 * SchemaEditor, and SystemInstructionsField.
 */

/* eslint-disable @typescript-eslint/no-explicit-any, react-refresh/only-export-components */
import React, { useState } from 'react';
import { FileCode } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { CollapsibleSection } from '../../components/common/CollapsibleSection';
import { StorageBackendFields } from '../../components/common/StorageBackendFields';
import {
  SchemaEditor,
  resolveSchemaFields,
  type SchemaField,
} from '../../components/common/SchemaEditor';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

const READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'BATCH (spark.read.orc)' },
  { value: 'streaming', label: 'STREAM (spark.readStream.format("orc"))' },
];

/**
 * React Flow node shape for the ORC source panel.
 */
export interface OrcSourceNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      readMode?: 'batch' | 'streaming';
      storageBackend?: string;
      path?: string;
      filePattern?: string;
      uri?: string;
      pathGlobFilter?: string;
      recursiveFileLookup?: boolean;
      mergeSchema?: boolean;
      maxFilesPerTrigger?: string | number;
      schema?: string;
      schemaString?: string;
      schemaFields?: SchemaField[];
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: any;
    };
  };
}

/**
 * Props for the {@link OrcSourceProperties} component.
 */
export interface OrcSourcePropertiesProps {
  node: OrcSourceNode;
  onChange: (properties: any) => void;
}

/**
 * Dedicated property panel for Apache Spark OrcSource nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered ORC source property panel.
 */
export const OrcSourceProperties: React.FC<OrcSourcePropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};
  const readMode = properties.readMode === 'streaming' ? 'streaming' : 'batch';
  const mergeSchema = Boolean(properties.mergeSchema);
  const maxFilesPerTrigger = properties.maxFilesPerTrigger ?? '';
  const schemaFields: SchemaField[] = resolveSchemaFields(properties);
  const systemInstructions = resolveSystemInstructions(node);

  const [isSchemaOpen, setIsSchemaOpen] = useState<boolean>(true);

  const handleChange = (
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

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Read Mode Selector */}
      <SelectField
        id="orcSourceReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select batch mode (spark.read.orc) or Structured Streaming mode (spark.readStream.format('orc'))."
        value={readMode}
        options={READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Shared Multi-Cloud Storage Backend Configuration */}
      <StorageBackendFields
        properties={properties}
        onChange={onChange}
        idPrefix="orcSource"
        pathLabel="ORC URI / Path"
        pathPlaceholder="gs://my-bucket/data/*.orc"
      />

      {/* ORC Reader Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          ORC Reader Options
        </span>

        <label htmlFor="orcSourceMergeSchema" className="flex items-center gap-2 cursor-pointer">
          <input
            id="orcSourceMergeSchema"
            type="checkbox"
            name="mergeSchema"
            checked={mergeSchema}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Merge Schema Across Files (mergeSchema)
          </span>
          <InfoTooltip text="Merges schemas across all ORC files when columns evolve across partitions." />
        </label>

        {readMode === 'streaming' && (
          <PropertyField
            id="orcSourceMaxFilesPerTrigger"
            type="number"
            name="maxFilesPerTrigger"
            label="Max Files Per Trigger (maxFilesPerTrigger)"
            tooltip="Maximum number of new ORC files to process in each streaming micro-batch trigger."
            value={maxFilesPerTrigger}
            onChange={handleChange}
            placeholder="e.g. 100"
            min="1"
            mono={true}
          />
        )}
      </div>

      {/* Schema Editor Section */}
      <CollapsibleSection
        title="DataFrame Schema (StructType)"
        icon={<FileCode className="w-4 h-4 text-emerald-400" />}
        isOpen={isSchemaOpen}
        onToggle={() => setIsSchemaOpen(!isSchemaOpen)}
      >
        <div className="mt-2">
          <SchemaEditor
            fields={schemaFields}
            schemaTitle="Spark StructType Schema (Required for Streaming, Optional for Batch)"
            jsonTextareaId="orcSourceSchema"
            stringTextareaId="orcSourceSchemaString"
            onChange={({
              schemaFields: nextFields,
              schema: nextSchema,
              schemaString: nextStr,
            }) => {
              onChange({
                ...properties,
                schemaFields: nextFields,
                schema: nextSchema,
                schemaString: nextStr,
              });
            }}
          />
        </div>
      </CollapsibleSection>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="orcSourceInstructions"
        value={systemInstructions}
        placeholder="e.g. Use native vectorized ORC reader and project specific columns."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark ORC reader code."
        helperText="Additional guidance appended to the prompt when generating PySpark code."
        wrapperClassName="pt-2 border-t border-[var(--vscode-border-light)]"
        textareaClassName="h-24"
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

export const OrcIOSourceProperties = OrcSourceProperties;
