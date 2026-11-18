/**
 * @fileoverview CreateDataFrameSource (SRC38 CreateDataFrameSource / Create) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the batch-only property editor for Apache Spark synthetic and in-memory
 * DataFrame creation nodes. Supports three generation modes:
 * - 'rows': spark.createDataFrame(rows, schema) with inline JSON rows and SchemaEditor
 * - 'range': spark.range(start, end, step, numPartitions)
 * - 'faker': Synthetic row generator with rowCount, fakerSpec, and SchemaEditor
 */

/* eslint-disable @typescript-eslint/no-explicit-any, react-refresh/only-export-components */
import React from 'react';
import { Plus, Trash2 } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import {
  SchemaEditor,
  resolveSchemaFields,
  type SchemaField,
} from '../../components/common/SchemaEditor';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported synthetic DataFrame generation modes for CreateDataFrameSource.
 */
export type CreateGenerationMode = 'rows' | 'range' | 'faker';

/**
 * Dropdown options for selecting the synthetic DataFrame generation strategy.
 */
export const GENERATION_MODE_OPTIONS: SelectFieldOption[] = [
  { value: 'rows', label: 'In-Memory Rows (spark.createDataFrame(rows, schema))' },
  { value: 'range', label: 'Numeric Range (spark.range(start, end, step, numPartitions))' },
  { value: 'faker', label: 'Synthetic Faker Dataset (rowCount + fakerSpec)' },
];

/**
 * Props for the {@link CreateProperties} component.
 */
export interface CreatePropertiesProps {
  node: any;
  onChange: (properties: any) => void;
}

/**
 * Normalizes raw elements or rows from node properties into a string array.
 *
 * @param raw Raw elements or rows property value.
 * @returns Non-empty array of stringified row items.
 */
export function normalizeElements(raw: any): string[] {
  if (Array.isArray(raw)) {
    if (raw.length === 0) {
      return [''];
    }
    return raw.map((el) => {
      if (typeof el === 'string') {
        return el;
      }
      if (typeof el === 'object' && el !== null) {
        try {
          return JSON.stringify(el);
        } catch {
          return String(el);
        }
      }
      return String(el ?? '');
    });
  }

  if (typeof raw === 'string') {
    const trimmed = raw.trim();
    if (!trimmed) {
      return [''];
    }
    if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
      try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) {
          return normalizeElements(parsed);
        }
      } catch {
        // Treat malformed JSON array string as a single raw item
      }
    }
    return [raw];
  }

  if (raw !== undefined && raw !== null) {
    return [String(raw)];
  }

  return [''];
}

/**
 * Dedicated property panel for CreateDataFrameSource / Create source nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered CreateDataFrameSource property panel.
 */
export const CreateProperties: React.FC<CreatePropertiesProps> = ({ node, onChange }) => {
  const properties = node?.data?.properties || {};
  const generationMode: CreateGenerationMode =
    properties.generationMode === 'range' || properties.generationMode === 'faker'
      ? properties.generationMode
      : 'rows';

  const rawRowsSource =
    properties.elements !== undefined ? properties.elements : properties.rows;
  const elements: string[] = normalizeElements(rawRowsSource);
  const isRowsInvalid =
    generationMode === 'rows' &&
    (elements.length === 0 || elements.every((el) => typeof el !== 'string' || !el.trim()));

  const rangeStart = properties.rangeStart ?? 0;
  const rangeEnd = properties.rangeEnd ?? 1000;
  const rangeStep = properties.rangeStep ?? 1;
  const numPartitions = properties.numPartitions ?? '';

  const rowCount = properties.rowCount ?? 100;
  const fakerSpec = properties.fakerSpec ?? '';

  const schemaFields: SchemaField[] = resolveSchemaFields(properties);
  const systemInstructions = resolveSystemInstructions(node);

  const handleFieldChange = (
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

  const updateElements = (newElements: string[]) => {
    onChange({
      ...properties,
      elements: newElements,
      rows: newElements,
    });
  };

  const handleElementChange = (index: number, value: string) => {
    const newElements = [...elements];
    newElements[index] = value;
    updateElements(newElements);
  };

  const addElement = () => {
    updateElements([...elements, '']);
  };

  const removeElement = (index: number) => {
    if (elements.length <= 1) {
      updateElements(['']);
    } else {
      updateElements(elements.filter((_, i) => i !== index));
    }
  };

  const handleSchemaChange = ({
    schemaFields: nextFields,
    schema: nextSchema,
    schemaString: nextStr,
  }: {
    schemaFields: SchemaField[];
    schema: string;
    schemaString: string;
  }) => {
    onChange({
      ...properties,
      schemaFields: nextFields,
      schema: nextSchema,
      schemaString: nextStr,
    });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* Generation Mode Selector */}
      <SelectField
        id="createGenerationMode"
        name="generationMode"
        label="Data Generation Mode"
        tooltip="Choose how Spark synthesizes the batch DataFrame: inline rows (spark.createDataFrame), numeric sequence (spark.range), or Faker synthetic rows."
        value={generationMode}
        options={GENERATION_MODE_OPTIONS}
        onChange={handleFieldChange}
      />

      {/* Mode 1: In-Memory Rows (spark.createDataFrame) */}
      {generationMode === 'rows' && (
        <div className="flex flex-col gap-3">
          <div className="flex justify-between items-center">
            <label className="flex items-center text-xs font-bold uppercase text-slate-500">
              <span>In-Memory Elements / Rows</span>
              <span className="text-red-400 ml-1 font-bold" title="Required">
                *
              </span>
              <InfoTooltip text="JSON objects or row values instantiated directly into a DataFrame via spark.createDataFrame(rows, schema). At least one element is required." />
            </label>
          </div>

          <div className="flex flex-col gap-3 max-h-[320px] overflow-y-auto pr-1">
            {elements.map((element, index) => {
              const isRowEmpty = !element.trim();
              return (
                <div
                  key={index}
                  className={`flex gap-2 items-start bg-slate-800/40 p-2 rounded border ${
                    isRowsInvalid && isRowEmpty ? 'border-red-500/80' : 'border-slate-700/50'
                  }`}
                >
                  <textarea
                    value={element}
                    aria-required="true"
                    aria-invalid={isRowsInvalid && isRowEmpty}
                    onChange={(e) => handleElementChange(index, e.target.value)}
                    placeholder={`Element ${index + 1} (e.g. {"id": 1, "name": "alice"})`}
                    rows={1}
                    className={`flex-1 bg-[#252836] text-white p-2 rounded border text-sm font-mono resize-y focus:outline-none ${
                      isRowsInvalid && isRowEmpty
                        ? 'border-red-500/80 focus:border-red-500'
                        : 'border-slate-700 focus:border-blue-500'
                    }`}
                  />
                  <button
                    type="button"
                    onClick={() => removeElement(index)}
                    className="text-red-400 hover:text-red-300 hover:bg-red-950/30 p-1.5 rounded transition-all self-center"
                    title="Remove Element"
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              );
            })}
          </div>

          {isRowsInvalid && (
            <span className="text-xs text-red-400" role="alert">
              At least one non-empty element is required.
            </span>
          )}

          <button
            type="button"
            onClick={addElement}
            className="flex items-center justify-center gap-1 text-xs bg-blue-600 hover:bg-blue-700 text-white px-3 py-2 rounded transition-colors font-semibold w-full mt-1"
          >
            <Plus className="w-3.5 h-3.5" /> Add Item
          </button>

          <SchemaEditor
            fields={schemaFields}
            schemaTitle="Spark StructType Schema"
            jsonTextareaId="createSourceSchema"
            stringTextareaId="createSourceSchemaString"
            onChange={handleSchemaChange}
          />
        </div>
      )}

      {/* Mode 2: Numeric Range (spark.range) */}
      {generationMode === 'range' && (
        <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
          <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            Spark Range Parameters (spark.range)
          </span>
          <div className="grid grid-cols-2 gap-3">
            <PropertyField
              id="createRangeStart"
              type="number"
              name="rangeStart"
              label="Range Start (start)"
              tooltip="Inclusive starting integer value for spark.range (default 0)."
              value={rangeStart}
              onChange={handleFieldChange}
              placeholder="0"
              mono={true}
            />
            <PropertyField
              id="createRangeEnd"
              type="number"
              name="rangeEnd"
              label="Range End (end)"
              tooltip="Exclusive ending integer value for spark.range (default 1000)."
              value={rangeEnd}
              onChange={handleFieldChange}
              placeholder="1000"
              mono={true}
            />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <PropertyField
              id="createRangeStep"
              type="number"
              name="rangeStep"
              label="Range Step (step)"
              tooltip="Step increment between generated integer values (default 1)."
              value={rangeStep}
              onChange={handleFieldChange}
              placeholder="1"
              mono={true}
            />
            <PropertyField
              id="createNumPartitions"
              type="number"
              name="numPartitions"
              label="Partitions (numPartitions)"
              tooltip="Optional number of Spark partitions for the generated DataFrame."
              value={numPartitions}
              onChange={handleFieldChange}
              placeholder="Default parallelism"
              min="1"
              mono={true}
            />
          </div>
        </div>
      )}

      {/* Mode 3: Synthetic Faker Generator */}
      {generationMode === 'faker' && (
        <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
          <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
            Synthetic Faker Configuration
          </span>

          <PropertyField
            id="createRowCount"
            type="number"
            name="rowCount"
            label="Row Count (rowCount)"
            tooltip="Number of synthetic rows to generate (default 100)."
            value={rowCount}
            onChange={handleFieldChange}
            placeholder="100"
            min="1"
            mono={true}
          />

          <PropertyField
            id="createFakerSpec"
            type="textarea"
            name="fakerSpec"
            label="Faker Provider / Field Spec (fakerSpec)"
            tooltip="JSON mapping or provider instructions mapping column names to Faker providers (for example, name, email, date_time_this_year)."
            value={fakerSpec}
            onChange={handleFieldChange}
            placeholder={'{\n  "user_id": "uuid4",\n  "email": "email",\n  "country": "country_code"\n}'}
            rows={4}
            mono={true}
          />

          <SchemaEditor
            fields={schemaFields}
            schemaTitle="Spark StructType Schema"
            jsonTextareaId="createFakerSchema"
            stringTextareaId="createFakerSchemaString"
            onChange={handleSchemaChange}
          />
        </div>
      )}

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="createSystemInstructions"
        value={systemInstructions}
        placeholder="Enter custom instructions for spark.createDataFrame or spark.range code generation..."
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

export const CreateDataFrameSourceProperties = CreateProperties;
