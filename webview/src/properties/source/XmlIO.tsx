/**
 * @fileoverview XML Source (SRC19 XmlSource / XmlIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.0 built-in XML source nodes
 * using spark.read.format("xml") or spark.readStream.format("xml").
 * Embeds StorageBackendFields, rowTag (required), valueTag (default '_VALUE'),
 * attributePrefix (default '_'), mode, columnNameOfCorruptRecord, maxFilesPerTrigger
 * (for streaming), SchemaEditor, and SystemInstructionsField.
 */

/* eslint-disable @typescript-eslint/no-explicit-any, react-refresh/only-export-components */
import React, { useState } from 'react';
import { FileCode } from 'lucide-react';
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
 * Supported Spark XML parsing modes for malformed records.
 */
export const XML_PARSE_MODES: SelectFieldOption[] = [
  { value: 'PERMISSIVE', label: 'PERMISSIVE (Capture malformed rows in _corrupt_record)' },
  { value: 'DROPMALFORMED', label: 'DROPMALFORMED (Drop malformed rows silently)' },
  { value: 'FAILFAST', label: 'FAILFAST (Abort job immediately on malformed row)' },
];

const READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'BATCH (spark.read.format("xml"))' },
  { value: 'streaming', label: 'STREAM (spark.readStream.format("xml"))' },
];

/**
 * React Flow node shape for the XML source panel.
 */
export interface XmlIOSourceNode {
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
      rowTag?: string;
      recordElement?: string;
      valueTag?: string;
      attributePrefix?: string;
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
 * Props for the {@link XmlIOSourceProperties} component.
 */
export interface XmlIOSourcePropertiesProps {
  node: XmlIOSourceNode;
  onChange: (properties: any) => void;
}

/**
 * Dedicated property panel for Apache Spark 4.0 XmlSource / XmlIO source nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered XML source property panel.
 */
export const XmlIOSourceProperties: React.FC<XmlIOSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data.properties || {};
  const readMode = properties.readMode === 'streaming' ? 'streaming' : 'batch';
  const rowTag = properties.rowTag ?? properties.recordElement ?? '';
  const valueTag = properties.valueTag ?? '_VALUE';
  const attributePrefix = properties.attributePrefix ?? '_';
  const mode = properties.mode || 'PERMISSIVE';
  const columnNameOfCorruptRecord =
    properties.columnNameOfCorruptRecord ?? '_corrupt_record';
  const maxFilesPerTrigger = properties.maxFilesPerTrigger ?? '';
  const schemaFields: SchemaField[] = resolveSchemaFields(properties);
  const systemInstructions = resolveSystemInstructions(node);

  const [isSchemaOpen, setIsSchemaOpen] = useState<boolean>(true);
  const isRowTagInvalid = !rowTag.trim();

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
        id="xmlSourceReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select batch mode (spark.read.format('xml')) or Structured Streaming mode (spark.readStream.format('xml'))."
        value={readMode}
        options={READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Shared Multi-Cloud Storage Backend Configuration */}
      <StorageBackendFields
        properties={properties}
        onChange={onChange}
        idPrefix="xmlSource"
        pathLabel="XML URI / Path"
        pathPlaceholder="gs://my-bucket/data/*.xml"
      />

      {/* Spark 4.0 Built-in XML Reader Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          Spark 4.0 XML Options (.format(&quot;xml&quot;))
        </span>

        <PropertyField
          id="xmlSourceRowTag"
          name="rowTag"
          label="Row Tag (rowTag)"
          required="Row Tag is required."
          aria-required={true}
          aria-invalid={isRowTagInvalid}
          invalid={isRowTagInvalid}
          error={isRowTagInvalid ? 'Row Tag is required.' : null}
          tooltip="XML element tag name treated as an individual DataFrame row (for example, book, item, or record). Required."
          value={rowTag}
          onChange={handleChange}
          placeholder="e.g. item, record, or book"
          mono={true}
        />

        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="xmlSourceValueTag"
            name="valueTag"
            label="Value Tag (valueTag)"
            tooltip="Column name used for the element text value when an XML element also carries attributes (default '_VALUE')."
            value={valueTag}
            onChange={handleChange}
            placeholder="_VALUE"
            mono={true}
          />
          <PropertyField
            id="xmlSourceAttributePrefix"
            name="attributePrefix"
            label="Attribute Prefix (attributePrefix)"
            tooltip="Prefix prepended to XML attribute names to distinguish them from child elements (default '_')."
            value={attributePrefix}
            onChange={handleChange}
            placeholder="_"
            mono={true}
          />
        </div>

        <SelectField
          id="xmlSourceMode"
          name="mode"
          label="Parse Mode (mode)"
          tooltip="Controls how Spark handles malformed XML elements."
          value={mode}
          options={XML_PARSE_MODES}
          onChange={handleChange}
        />

        {mode === 'PERMISSIVE' && (
          <PropertyField
            id="xmlSourceCorruptRecordCol"
            name="columnNameOfCorruptRecord"
            label="Corrupt Record Column (columnNameOfCorruptRecord)"
            tooltip="Column name storing raw unparsed XML strings when mode is PERMISSIVE."
            value={columnNameOfCorruptRecord}
            onChange={handleChange}
            placeholder="_corrupt_record"
            mono={true}
          />
        )}

        {readMode === 'streaming' && (
          <PropertyField
            id="xmlSourceMaxFilesPerTrigger"
            type="number"
            name="maxFilesPerTrigger"
            label="Max Files Per Trigger (maxFilesPerTrigger)"
            tooltip="Maximum number of new XML files to process in each streaming micro-batch trigger."
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
            jsonTextareaId="xmlSourceSchema"
            stringTextareaId="xmlSourceSchemaString"
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
        id="xmlSourceInstructions"
        value={systemInstructions}
        placeholder="e.g. Extract nested XML attributes and route _corrupt_record rows to DLQ."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark XML reader code."
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

export const XmlSourceProperties = XmlIOSourceProperties;
