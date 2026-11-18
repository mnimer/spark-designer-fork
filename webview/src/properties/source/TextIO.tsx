/**
 * @fileoverview Text Source (SRC18 TextSource / TextIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark Text source nodes
 * using spark.read.text(uri) or spark.readStream.format("text").load(uri).
 * Embeds StorageBackendFields, wholetext toggle, lineSep custom separator,
 * maxFilesPerTrigger (for streaming), and SystemInstructionsField.
 */

/* eslint-disable @typescript-eslint/no-explicit-any, react-refresh/only-export-components */
import React from 'react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { StorageBackendFields } from '../../components/common/StorageBackendFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

const READ_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'batch', label: 'BATCH (spark.read.text)' },
  { value: 'streaming', label: 'STREAM (spark.readStream.format("text"))' },
];

/**
 * React Flow node shape for the Text source panel.
 */
export interface TextIOSourceNode {
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
      wholetext?: boolean;
      lineSep?: string;
      maxFilesPerTrigger?: string | number;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: any;
    };
  };
}

/**
 * Props for the {@link TextIOSourceProperties} component.
 */
export interface TextIOSourcePropertiesProps {
  node: TextIOSourceNode;
  onChange: (properties: any) => void;
}

/**
 * Dedicated property panel for Apache Spark TextSource / TextIO source nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered Text source property panel.
 */
export const TextIOSourceProperties: React.FC<TextIOSourcePropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties = node.data.properties || {};
  const readMode = properties.readMode === 'streaming' ? 'streaming' : 'batch';
  const wholetext = Boolean(properties.wholetext);
  const lineSep = properties.lineSep ?? properties.delimiter ?? '';
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
        id="textSourceReadMode"
        name="readMode"
        label="Read Mode"
        tooltip="Select batch mode (spark.read.text) or Structured Streaming mode (spark.readStream.format('text'))."
        value={readMode}
        options={READ_MODE_OPTIONS}
        onChange={handleChange}
      />

      {/* Shared Multi-Cloud Storage Backend Configuration */}
      <StorageBackendFields
        properties={properties}
        onChange={onChange}
        idPrefix="textSource"
        pathLabel="Text URI / Path"
        pathPlaceholder="gs://my-bucket/logs/*.txt"
      />

      {/* Text Reader Options */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          Text Reader Options (Emits value: STRING)
        </span>

        <label htmlFor="textSourceWholetext" className="flex items-center gap-2 cursor-pointer">
          <input
            id="textSourceWholetext"
            type="checkbox"
            name="wholetext"
            checked={wholetext}
            onChange={handleChange}
            className="rounded bg-[var(--vscode-input-bg)] border-[var(--vscode-border)] text-blue-500 focus:ring-0 focus:ring-offset-0"
          />
          <span className="text-xs text-[var(--vscode-text-secondary)]">
            Read Entire File as Single Row (wholetext)
          </span>
          <InfoTooltip text="Reads each complete file into a single DataFrame row instead of splitting by line." />
        </label>

        {!wholetext && (
          <PropertyField
            id="textSourceLineSep"
            name="lineSep"
            label="Line Separator (lineSep)"
            tooltip="Custom line separator character or sequence. Leave blank to cover \\r, \\r\\n, and \\n."
            value={lineSep}
            onChange={handleChange}
            placeholder="\n (Default covers \r, \r\n, \n)"
            mono={true}
          />
        )}

        {readMode === 'streaming' && (
          <PropertyField
            id="textSourceMaxFilesPerTrigger"
            type="number"
            name="maxFilesPerTrigger"
            label="Max Files Per Trigger (maxFilesPerTrigger)"
            tooltip="Maximum number of new text files to process in each streaming micro-batch trigger."
            value={maxFilesPerTrigger}
            onChange={handleChange}
            placeholder="e.g. 100"
            min="1"
            mono={true}
          />
        )}
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="textSourceSystemInstructions"
        value={systemInstructions}
        placeholder="e.g. Extract regex groups from the value column after reading."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark Text reader code."
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

export const TextSourceProperties = TextIOSourceProperties;
