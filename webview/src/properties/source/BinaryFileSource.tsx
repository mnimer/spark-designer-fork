/**
 * @fileoverview BinaryFile Source (SRC20 BinaryFileSource) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark BinaryFile source nodes
 * using spark.read.format("binaryFile") or spark.readStream.format("binaryFile").
 * Embeds StorageBackendFields (with pathGlobFilter and recursiveFileLookup),
 * maxFilesPerTrigger (for streaming), and SystemInstructionsField.
 */

/* eslint-disable @typescript-eslint/no-explicit-any, react-refresh/only-export-components */
import React from 'react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { StorageBackendFields } from '../../components/common/StorageBackendFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

const READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'BATCH (spark.read.format("binaryFile"))' },
  { value: 'streaming', label: 'STREAM (spark.readStream.format("binaryFile"))' },
];

/**
 * React Flow node shape for the BinaryFile source panel.
 */
export interface BinaryFileSourceNode {
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
      maxFilesPerTrigger?: string | number;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: any;
    };
  };
}

/**
 * Props for the {@link BinaryFileSourceProperties} component.
 */
export interface BinaryFileSourcePropertiesProps {
  node: BinaryFileSourceNode;
  onChange: (properties: any) => void;
}

/**
 * Dedicated property panel for Apache Spark BinaryFileSource nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered BinaryFile source property panel.
 */
export const BinaryFileSourceProperties: React.FC<BinaryFileSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data.properties || {};
  const readMode = properties.readMode === 'streaming' ? 'streaming' : 'batch';
  const maxFilesPerTrigger = properties.maxFilesPerTrigger ?? '';
  const systemInstructions = resolveSystemInstructions(node);

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
        id="binaryFileSourceReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select batch mode (spark.read.format('binaryFile')) or Structured Streaming mode (spark.readStream.format('binaryFile'))."
        value={readMode}
        options={READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Shared Multi-Cloud Storage Backend Configuration */}
      <StorageBackendFields
        properties={properties}
        onChange={onChange}
        idPrefix="binaryFileSource"
        pathLabel="Binary File URI / Directory"
        pathPlaceholder="gs://my-bucket/images/"
        showGlobAndRecursive={true}
      />

      {/* BinaryFile Output Schema & Streaming Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          BinaryFile Schema &amp; Options (.format(&quot;binaryFile&quot;))
        </span>
        <p className="text-xs text-[var(--vscode-text-secondary)]">
          Produces rows with fixed schema:{' '}
          <code className="font-mono text-emerald-400">
            path: STRING, modificationTime: TIMESTAMP, length: LONG, content: BINARY
          </code>
        </p>

        {readMode === 'streaming' && (
          <PropertyField
            id="binaryFileSourceMaxFilesPerTrigger"
            type="number"
            name="maxFilesPerTrigger"
            label="Max Files Per Trigger (maxFilesPerTrigger)"
            tooltip="Maximum number of binary files to ingest per streaming micro-batch trigger."
            value={maxFilesPerTrigger}
            onChange={handleChange}
            placeholder="e.g. 50"
            min="1"
            mono={true}
          />
        )}
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="binaryFileSourceInstructions"
        value={systemInstructions}
        placeholder="e.g. Pass binary content column to a Pandas UDF or Vertex AI multimodal model."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark binaryFile reader code."
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

export const BinaryFileIOSourceProperties = BinaryFileSourceProperties;
