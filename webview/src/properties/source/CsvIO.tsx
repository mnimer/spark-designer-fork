/**
 * @fileoverview CSV Source (SRC16 CsvSource / CsvIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark CSV source nodes
 * using spark.read.csv(uri) or spark.readStream.format("csv").load(uri).
 * Embeds StorageBackendFields, CSV dialect options (header, inferSchema, delimiter,
 * quote, escape, nullValue, multiLine), malformed record handling (mode and
 * columnNameOfCorruptRecord), maxFilesPerTrigger (for streaming), SchemaEditor,
 * and SystemInstructionsField.
 */

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
 * Supported Spark CSV parsing modes for malformed records.
 */
const CSV_PARSE_MODES: SelectFieldOption[] = [
  { value: 'PERMISSIVE', label: 'PERMISSIVE (Capture malformed rows in _corrupt_record)' },
  { value: 'DROPMALFORMED', label: 'DROPMALFORMED (Drop malformed rows silently)' },
  { value: 'FAILFAST', label: 'FAILFAST (Abort job immediately on malformed row)' },
];

const READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'BATCH (spark.read.csv)' },
  { value: 'streaming', label: 'STREAM (spark.readStream.format("csv"))' },
];

/**
 * React Flow node shape for the CSV source panel.
 */
export interface CsvIOSourceNode {
  data: {
    label?: string;
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
      header?: boolean | string;
      hasHeader?: boolean;
      inferSchema?: boolean;
      delimiter?: string;
      quote?: string;
      quoteChar?: string;
      escape?: string;
      nullValue?: string;
      nullString?: string;
      mode?: 'PERMISSIVE' | 'DROPMALFORMED' | 'FAILFAST';
      parseMode?: 'PERMISSIVE' | 'DROPMALFORMED' | 'FAILFAST';
      columnNameOfCorruptRecord?: string;
      multiLine?: boolean;
      maxFilesPerTrigger?: string | number;
      schema?: string;
      schemaString?: string;
      schemaFields?: SchemaField[];
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: unknown;
    };
  };
}

/**
 * Props for the {@link CsvIOSourceProperties} component.
 */
export interface CsvIOSourcePropertiesProps {
  node: CsvIOSourceNode;
  onChange: (properties: Record<string, unknown>) => void;
}

/**
 * Dedicated property panel for Apache Spark CsvSource / CsvIO source nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered CSV source property panel.
 */
export const CsvIOSourceProperties: React.FC<CsvIOSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data.properties || {};
  const readMode = properties.readMode === 'streaming' ? 'streaming' : 'batch';
  const header =
    properties.header === undefined
      ? properties.hasHeader ?? true
      : properties.header === 'false'
        ? false
        : Boolean(properties.header);
  const inferSchema = properties.inferSchema ?? true;
  const delimiter = properties.delimiter ?? ',';
  const quote = properties.quote ?? properties.quoteChar ?? '"';
  const escape = properties.escape ?? '\\';
  const nullValue = properties.nullValue ?? properties.nullString ?? '';
  const mode = properties.mode || properties.parseMode || 'PERMISSIVE';
  const columnNameOfCorruptRecord =
    properties.columnNameOfCorruptRecord ?? '_corrupt_record';
  const multiLine = Boolean(properties.multiLine);
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
        id="csvSourceReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select batch mode (spark.read.csv) or Structured Streaming mode (spark.readStream.format('csv'))."
        value={readMode}
        options={READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Shared Multi-Cloud Storage Backend Configuration */}
      <StorageBackendFields
        properties={properties}
        onChange={onChange}
        idPrefix="csvSource"
        pathLabel="CSV URI / Path"
        pathPlaceholder="gs://my-bucket/data/*.csv"
      />

      {/* CSV Dialect & Parsing Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          CSV Dialect &amp; Parsing Options
        </span>

        <label htmlFor="csvSourceHeader" className="flex items-center gap-2 cursor-pointer">
          <input
            id="csvSourceHeader"
            type="checkbox"
            name="header"
            checked={header}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            First Row Is Header (header=true)
          </span>
          <InfoTooltip text="Uses the first line of each CSV file as column names." />
        </label>

        <label htmlFor="csvSourceInferSchema" className="flex items-center gap-2 cursor-pointer">
          <input
            id="csvSourceInferSchema"
            type="checkbox"
            name="inferSchema"
            checked={inferSchema}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Infer Column Types Automatically (inferSchema=true)
          </span>
          <InfoTooltip text="Infers input column types automatically from data values when an explicit schema is not provided." />
        </label>

        <label htmlFor="csvSourceMultiLine" className="flex items-center gap-2 cursor-pointer">
          <input
            id="csvSourceMultiLine"
            type="checkbox"
            name="multiLine"
            checked={multiLine}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Multi-Line Records (multiLine)
          </span>
          <InfoTooltip text="Allows quoted CSV fields to span multiple lines within a file." />
        </label>

        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="csvSourceDelimiter"
            name="delimiter"
            label="Delimiter (sep)"
            tooltip="Character separating fields in each row (default ',')."
            value={delimiter}
            onChange={handleChange}
            placeholder=","
            mono={true}
          />
          <PropertyField
            id="csvSourceQuote"
            name="quote"
            label="Quote Character (quote)"
            tooltip={'Character used to quote fields containing delimiters (default \'"\').'}
            value={quote}
            onChange={handleChange}
            placeholder='"'
            mono={true}
          />
        </div>

        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="csvSourceEscape"
            name="escape"
            label="Escape Character (escape)"
            tooltip="Character used to escape quote characters inside quoted fields (default '\\')."
            value={escape}
            onChange={handleChange}
            placeholder="\"
            mono={true}
          />
          <PropertyField
            id="csvSourceNullValue"
            name="nullValue"
            label="Null Value (nullValue)"
            tooltip="String representation of a null value in CSV data (for example, NULL or \\N)."
            value={nullValue}
            onChange={handleChange}
            placeholder="e.g. NULL or \N"
            mono={true}
          />
        </div>

        <SelectField
          id="csvSourceMode"
          name="mode"
          label="Parse Mode (mode)"
          tooltip="Controls how Spark handles malformed CSV rows during ingestion."
          value={mode}
          options={CSV_PARSE_MODES}
          onChange={handleChange}
        />

        {mode === 'PERMISSIVE' && (
          <PropertyField
            id="csvSourceCorruptRecordCol"
            name="columnNameOfCorruptRecord"
            label="Corrupt Record Column (columnNameOfCorruptRecord)"
            tooltip="Column name storing raw unparsed strings when mode is PERMISSIVE."
            value={columnNameOfCorruptRecord}
            onChange={handleChange}
            placeholder="_corrupt_record"
            mono={true}
          />
        )}

        {readMode === 'streaming' && (
          <PropertyField
            id="csvSourceMaxFilesPerTrigger"
            type="number"
            name="maxFilesPerTrigger"
            label="Max Files Per Trigger (maxFilesPerTrigger)"
            tooltip="Maximum number of new CSV files to process in each streaming micro-batch trigger."
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
            schemaTitle="Spark StructType Schema (Overrides inferSchema when populated)"
            jsonTextareaId="csvSourceSchema"
            stringTextareaId="csvSourceSchemaString"
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
        id="csvSourceInstructions"
        value={systemInstructions}
        placeholder="e.g. Parse ISO-8601 timestamps and route _corrupt_record rows to DLQ."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark CSV reader code."
        helperText="Additional guidance appended to the prompt when generating PySpark code."
        textareaClassName="h-24"
        wrapperClassName="pt-2 border-t border-[var(--vscode-border-light)]"
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

export const CsvSourceProperties = CsvIOSourceProperties;
