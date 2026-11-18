/**
 * @fileoverview JSON Source (SRC17 JsonSource / JsonIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark JSON source nodes
 * using spark.read.json(uri) or spark.readStream.format("json").load(uri).
 * Embeds StorageBackendFields, multiLine, primitivesAsString, allowComments,
 * mode, columnNameOfCorruptRecord, maxFilesPerTrigger (for streaming),
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

/**
 * Supported Spark JSON parsing modes for malformed records.
 */
export const JSON_PARSE_MODES: SelectFieldOption[] = [
  { value: 'PERMISSIVE', label: 'PERMISSIVE (Capture malformed rows in _corrupt_record)' },
  { value: 'DROPMALFORMED', label: 'DROPMALFORMED (Drop malformed rows silently)' },
  { value: 'FAILFAST', label: 'FAILFAST (Abort job immediately on malformed row)' },
];

const READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'BATCH (spark.read.json)' },
  { value: 'streaming', label: 'STREAM (spark.readStream.format("json"))' },
];

/**
 * React Flow node shape for the JSON source panel.
 */
export interface JsonIOSourceNode {
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
      multiLine?: boolean;
      primitivesAsString?: boolean;
      allowComments?: boolean;
      mode?: 'PERMISSIVE' | 'DROPMALFORMED' | 'FAILFAST';
      columnNameOfCorruptRecord?: string;
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
 * Props for the {@link JsonIOSourceProperties} component.
 */
export interface JsonIOSourcePropertiesProps {
  node: JsonIOSourceNode;
  onChange: (properties: any) => void;
}

/**
 * Dedicated property panel for Apache Spark JsonSource / JsonIO source nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered JSON source property panel.
 */
export const JsonIOSourceProperties: React.FC<JsonIOSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data.properties || {};
  const readMode = properties.readMode === 'streaming' ? 'streaming' : 'batch';
  const multiLine = Boolean(properties.multiLine);
  const primitivesAsString = Boolean(properties.primitivesAsString);
  const allowComments = Boolean(properties.allowComments);
  const mode = properties.mode || 'PERMISSIVE';
  const columnNameOfCorruptRecord =
    properties.columnNameOfCorruptRecord ?? '_corrupt_record';
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
        id="jsonSourceReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select batch mode (spark.read.json) or Structured Streaming mode (spark.readStream.format('json'))."
        value={readMode}
        options={READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Shared Multi-Cloud Storage Backend Configuration */}
      <StorageBackendFields
        properties={properties}
        onChange={onChange}
        idPrefix="jsonSource"
        pathLabel="JSON URI / Path"
        pathPlaceholder="gs://my-bucket/data/*.json"
      />

      {/* JSON Reader Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          JSON Parsing Options
        </span>

        <label htmlFor="jsonSourceMultiLine" className="flex items-center gap-2 cursor-pointer">
          <input
            id="jsonSourceMultiLine"
            type="checkbox"
            name="multiLine"
            checked={multiLine}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Multi-Line JSON Objects (multiLine)
          </span>
          <InfoTooltip text="Parses whole-file pretty-printed JSON objects or arrays instead of one JSON object per line (JSONL)." />
        </label>

        <label
          htmlFor="jsonSourcePrimitivesAsString"
          className="flex items-center gap-2 cursor-pointer"
        >
          <input
            id="jsonSourcePrimitivesAsString"
            type="checkbox"
            name="primitivesAsString"
            checked={primitivesAsString}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Infer Primitives as String (primitivesAsString)
          </span>
          <InfoTooltip text="Infers all primitive values (numbers, booleans) as StringType during schema inference." />
        </label>

        <label
          htmlFor="jsonSourceAllowComments"
          className="flex items-center gap-2 cursor-pointer"
        >
          <input
            id="jsonSourceAllowComments"
            type="checkbox"
            name="allowComments"
            checked={allowComments}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Allow Java/C++ Comments (allowComments)
          </span>
          <InfoTooltip text="Ignores Java and C++ style comments inside JSON records." />
        </label>

        <SelectField
          id="jsonSourceMode"
          name="mode"
          label="Parse Mode (mode)"
          tooltip="Controls how Spark handles corrupt or malformed JSON records."
          value={mode}
          options={JSON_PARSE_MODES}
          onChange={handleChange}
        />

        {mode === 'PERMISSIVE' && (
          <PropertyField
            id="jsonSourceCorruptRecordCol"
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
            id="jsonSourceMaxFilesPerTrigger"
            type="number"
            name="maxFilesPerTrigger"
            label="Max Files Per Trigger (maxFilesPerTrigger)"
            tooltip="Maximum number of new JSON files to process in each streaming micro-batch trigger."
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
            schemaTitle="Spark StructType Schema (Avoids full-scan schema inference pass)"
            jsonTextareaId="jsonSourceSchema"
            stringTextareaId="jsonSourceSchemaString"
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
        id="jsonSourceInstructions"
        value={systemInstructions}
        placeholder="e.g. Provide explicit StructType and route _corrupt_record rows to DLQ."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark JSON reader code."
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

export const JsonSourceProperties = JsonIOSourceProperties;
