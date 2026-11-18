/**
 * @fileoverview Parquet Source (SRC13 ParquetSource / ParquetIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Parquet source nodes
 * using spark.read.parquet(uri) or spark.readStream.format("parquet").load(uri).
 * Embeds the shared StorageBackendFields component for multi-cloud URI configuration,
 * schema merging, datetime/INT96 rebase modes, streaming trigger limits, and schema definition.
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
 * Supported Spark Parquet datetime and INT96 rebase modes.
 */
export const PARQUET_REBASE_MODES: SelectFieldOption[] = [
  { value: 'EXCEPTION', label: 'EXCEPTION (Fail on ambiguous ancient dates)' },
  { value: 'CORRECTED', label: 'CORRECTED (Load Proleptic Gregorian verbatim)' },
  { value: 'LEGACY', label: 'LEGACY (Rebase Julian to Gregorian calendar)' },
];

const READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'BATCH (spark.read.parquet)' },
  { value: 'streaming', label: 'STREAM (spark.readStream.format("parquet"))' },
];

/**
 * React Flow node shape for the Parquet source panel.
 */
export interface ParquetIOSourceNode {
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
      datetimeRebaseMode?: 'EXCEPTION' | 'CORRECTED' | 'LEGACY';
      int96RebaseMode?: 'EXCEPTION' | 'CORRECTED' | 'LEGACY';
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
 * Props for the {@link ParquetIOSourceProperties} component.
 */
export interface ParquetIOSourcePropertiesProps {
  node: ParquetIOSourceNode;
  onChange: (properties: any) => void;
}

/**
 * Dedicated property panel for Apache Spark ParquetSource / ParquetIO source nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered Parquet source property panel.
 */
export const ParquetIOSourceProperties: React.FC<ParquetIOSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data.properties || {};
  const readMode = properties.readMode === 'streaming' ? 'streaming' : 'batch';
  const mergeSchema = Boolean(properties.mergeSchema);
  const datetimeRebaseMode = properties.datetimeRebaseMode || 'EXCEPTION';
  const int96RebaseMode = properties.int96RebaseMode || 'EXCEPTION';
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
        id="parquetSourceReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select batch mode (spark.read.parquet) or Structured Streaming mode (spark.readStream.format('parquet'))."
        value={readMode}
        options={READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Shared Multi-Cloud Storage Backend Configuration */}
      <StorageBackendFields
        properties={properties}
        onChange={onChange}
        idPrefix="parquetSource"
        pathLabel="Parquet URI / Path"
        pathPlaceholder="gs://my-bucket/data/*.parquet"
      />

      {/* Parquet Reader Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          Parquet Reader Options
        </span>

        <label
          htmlFor="parquetSourceMergeSchema"
          className="flex items-center gap-2 cursor-pointer"
        >
          <input
            id="parquetSourceMergeSchema"
            type="checkbox"
            name="mergeSchema"
            checked={mergeSchema}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Merge Schema Across Partitions (mergeSchema)
          </span>
          <InfoTooltip text="Merges schemas across all Parquet part-files when columns evolve across partitions." />
        </label>

        <SelectField
          id="parquetSourceDatetimeRebaseMode"
          name="datetimeRebaseMode"
          label="Datetime Rebase Mode (datetimeRebaseMode)"
          tooltip="Controls rebasing of DATE and TIMESTAMP values between Julian and Proleptic Gregorian calendars."
          value={datetimeRebaseMode}
          options={PARQUET_REBASE_MODES}
          onChange={handleChange}
        />

        <SelectField
          id="parquetSourceInt96RebaseMode"
          name="int96RebaseMode"
          label="INT96 Rebase Mode (int96RebaseMode)"
          tooltip="Controls rebasing of legacy INT96 timestamp values between Julian and Proleptic Gregorian calendars."
          value={int96RebaseMode}
          options={PARQUET_REBASE_MODES}
          onChange={handleChange}
        />

        {readMode === 'streaming' && (
          <PropertyField
            id="parquetSourceMaxFilesPerTrigger"
            type="number"
            name="maxFilesPerTrigger"
            label="Max Files Per Trigger (maxFilesPerTrigger)"
            tooltip="Maximum number of new Parquet files to process in each streaming micro-batch trigger."
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
            jsonTextareaId="parquetSourceSchema"
            stringTextareaId="parquetSourceSchemaString"
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
        id="parquetSourceInstructions"
        value={systemInstructions}
        placeholder="e.g. Filter partition columns before join to trigger partition pruning."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark Parquet reader code."
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

export const ParquetSourceProperties = ParquetIOSourceProperties;
