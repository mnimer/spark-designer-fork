/**
 * @fileoverview TFRecord Source (SRC21 TFRecordSource / TFRecordIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the batch-only property configuration panel for Apache Spark TFRecord source nodes
 * using spark.read.format("tfrecords").option("recordType", ...).load(uri).
 * Embeds StorageBackendFields, recordType ('Example' | 'SequenceExample'),
 * SchemaEditor, and SystemInstructionsField.
 */

/* eslint-disable @typescript-eslint/no-explicit-any, react-refresh/only-export-components */
import React, { useState } from 'react';
import { FileCode } from 'lucide-react';
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
 * Supported TensorFlow protobuf record types for Spark TFRecord connector.
 */
export const TFRECORD_RECORD_TYPES: SelectFieldOption[] = [
  { value: 'Example', label: 'Example (tf.train.Example flat feature map)' },
  { value: 'SequenceExample', label: 'SequenceExample (tf.train.SequenceExample sequential features)' },
];

/**
 * React Flow node shape for the TFRecord source panel.
 */
export interface TFRecordIOSourceNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      storageBackend?: string;
      path?: string;
      filePattern?: string;
      uri?: string;
      pathGlobFilter?: string;
      recursiveFileLookup?: boolean;
      recordType?: 'Example' | 'SequenceExample';
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
 * Props for the {@link TFRecordIOSourceProperties} component.
 */
export interface TFRecordIOSourcePropertiesProps {
  node: TFRecordIOSourceNode;
  onChange: (properties: any) => void;
}

/**
 * Dedicated batch-only property panel for Apache Spark TFRecordSource / TFRecordIO source nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered TFRecord source property panel.
 */
export const TFRecordIOSourceProperties: React.FC<TFRecordIOSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data.properties || {};
  const recordType =
    properties.recordType === 'SequenceExample' ? 'SequenceExample' : 'Example';
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
      {/* Shared Multi-Cloud Storage Backend Configuration */}
      <StorageBackendFields
        properties={properties}
        onChange={onChange}
        idPrefix="tfrecordSource"
        pathLabel="TFRecord URI / Path"
        pathPlaceholder="gs://my-bucket/training/*.tfrecord"
      />

      {/* TFRecord Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          TFRecord Options (.format(&quot;tfrecords&quot;) — Batch Only)
        </span>

        <SelectField
          id="tfrecordSourceRecordType"
          name="recordType"
          label="TFRecord Protobuf Type (recordType)"
          tooltip="Select the TensorFlow protobuf message type stored in the TFRecord files (default 'Example')."
          value={recordType}
          options={TFRECORD_RECORD_TYPES}
          onChange={handleChange}
        />
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
            jsonTextareaId="tfrecordSourceSchema"
            stringTextareaId="tfrecordSourceSchemaString"
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
        id="tfrecordSourceInstructions"
        value={systemInstructions}
        placeholder="e.g. Load TFRecord feature vectors for Spark ML or XGBoost training."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark TFRecord reader code."
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

export const TFRecordSourceProperties = TFRecordIOSourceProperties;
