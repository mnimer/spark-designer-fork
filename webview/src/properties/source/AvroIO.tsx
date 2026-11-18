/**
 * @fileoverview Avro Source (SRC15 AvroSource / AvroIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Avro source nodes
 * using spark.read.format("avro") or spark.readStream.format("avro").
 * Embeds StorageBackendFields, avroSchema JSON reader schema textarea,
 * ignoreExtension toggle, maxFilesPerTrigger (for streaming), SchemaEditor,
 * and SystemInstructionsField.
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

/**
 * Sample Avro reader schema JSON string.
 */
export const SAMPLE_AVRO_SCHEMA = JSON.stringify(
  {
    type: 'record',
    name: 'SampleRecord',
    namespace: 'com.example.avro',
    fields: [
      { name: 'id', type: 'string' },
      { name: 'name', type: 'string' },
      { name: 'timestamp', type: 'long' },
      { name: 'active', type: 'boolean', default: true },
    ],
  },
  null,
  2
);

const READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'BATCH (spark.read.format("avro"))' },
  { value: 'streaming', label: 'STREAM (spark.readStream.format("avro"))' },
];

/**
 * React Flow node shape for the Avro source panel.
 */
export interface AvroIOSourceNode {
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
      avroSchema?: string;
      ignoreExtension?: boolean;
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
 * Props for the {@link AvroIOSourceProperties} component.
 */
export interface AvroIOSourcePropertiesProps {
  node: AvroIOSourceNode;
  onChange: (properties: any) => void;
}

/**
 * Dedicated property panel for Apache Spark AvroSource / AvroIO source nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered Avro source property panel.
 */
export const AvroIOSourceProperties: React.FC<AvroIOSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data.properties || {};
  const readMode = properties.readMode === 'streaming' ? 'streaming' : 'batch';
  const avroSchema = properties.avroSchema || '';
  const ignoreExtension = Boolean(properties.ignoreExtension);
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
        id="avroSourceReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select batch mode (spark.read.format('avro')) or Structured Streaming mode (spark.readStream.format('avro'))."
        value={readMode}
        options={READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Shared Multi-Cloud Storage Backend Configuration */}
      <StorageBackendFields
        properties={properties}
        onChange={onChange}
        idPrefix="avroSource"
        pathLabel="Avro URI / Path"
        pathPlaceholder="gs://my-bucket/data/*.avro"
      />

      {/* Avro Reader Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          Avro Reader Options (.format(&quot;avro&quot;))
        </span>

        <PropertyField
          id="avroSourceAvroSchema"
          type="textarea"
          name="avroSchema"
          label="Avro Reader Schema JSON (avroSchema)"
          tooltip="Optional evolved Avro JSON schema string passed to .option('avroSchema', ...)."
          value={avroSchema}
          onChange={handleChange}
          placeholder={SAMPLE_AVRO_SCHEMA}
          rows={4}
          mono={true}
        />

        <label
          htmlFor="avroSourceIgnoreExtension"
          className="flex items-center gap-2 cursor-pointer"
        >
          <input
            id="avroSourceIgnoreExtension"
            type="checkbox"
            name="ignoreExtension"
            checked={ignoreExtension}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Ignore File Extension (ignoreExtension)
          </span>
          <InfoTooltip text="Reads all files in the path regardless of whether they end with .avro." />
        </label>

        {readMode === 'streaming' && (
          <PropertyField
            id="avroSourceMaxFilesPerTrigger"
            type="number"
            name="maxFilesPerTrigger"
            label="Max Files Per Trigger (maxFilesPerTrigger)"
            tooltip="Maximum number of new Avro files to process in each streaming micro-batch trigger."
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
            schemaTitle="Spark StructType Schema"
            jsonTextareaId="avroSourceSchemaJson"
            stringTextareaId="avroSourceSchemaString"
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
        id="avroSourceSystemInstructions"
        value={systemInstructions}
        placeholder="e.g. Project evolved Avro fields using the provided reader schema."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark Avro reader code."
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

export const AvroSourceProperties = AvroIOSourceProperties;
