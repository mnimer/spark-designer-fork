/**
 * @fileoverview Property configuration panel for the Apache Spark ParsePayload step (F.from_json / from_avro / F.from_csv / F.from_xml).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

/* eslint-disable react-refresh/only-export-components */
import React from 'react';
import { AlertTriangle, FileCode, Package } from 'lucide-react';
import { InfoTooltip } from '../../components/InfoTooltip';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported Spark 4.x structured payload serialization formats.
 */
export type PayloadFormat = 'json' | 'avro' | 'csv' | 'xml';

/**
 * Persisted properties for the Spark `ParsePayload` step node.
 */
export interface ParsePayloadStepProperties {
  format?: PayloadFormat;
  sourceColumn?: string;
  schemaDDL?: string;
  flattenFields?: boolean;
  targetColumn?: string;
  rowTag?: string;
  corruptRecordColumn?: string;
  keepSourceColumn?: boolean;
  system_instructions?: string;
  special_instructions?: string;
  codeGenerationPrompt?: string;
  prompt?: string;
  [key: string]: unknown;
}

/**
 * React Flow node shape accepted by {@link ParsePayloadProperties}.
 */
export interface ParsePayloadNode {
  id?: string;
  data: {
    label?: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: ParsePayloadStepProperties;
  };
}

/**
 * Props for the {@link ParsePayloadProperties} component.
 */
export interface ParsePayloadPropertiesProps {
  node: ParsePayloadNode;
  onChange: (properties: ParsePayloadStepProperties) => void;
}

const PAYLOAD_FORMAT_OPTIONS: SelectFieldOption[] = [
  { value: 'json', label: 'JSON (F.from_json)' },
  { value: 'avro', label: 'Avro (from_avro)' },
  { value: 'csv', label: 'CSV (F.from_csv)' },
  { value: 'xml', label: 'XML (F.from_xml — Spark 4.0 Native)' },
];

const VALID_PAYLOAD_FORMATS = new Set<string>(['json', 'avro', 'csv', 'xml']);

/**
 * Normalizes a raw payload format string into a valid {@link PayloadFormat}.
 *
 * @param raw Raw `format` property value from YAML or UI state.
 * @returns Validated lowercase payload format defaulting to `'json'`.
 */
export function normalizePayloadFormat(raw: unknown): PayloadFormat {
  if (typeof raw === 'string') {
    const lower = raw.trim().toLowerCase();
    if (VALID_PAYLOAD_FORMATS.has(lower)) {
      return lower as PayloadFormat;
    }
  }
  return 'json';
}

/**
 * Returns a contextual schema placeholder matching the selected payload format.
 *
 * @param format Active {@link PayloadFormat}.
 * @returns Example Spark DDL or Avro JSON schema snippet.
 */
export function getPayloadSchemaPlaceholder(format: PayloadFormat): string {
  if (format === 'avro') {
    return '{"type":"record","name":"OrderEvent","fields":[{"name":"order_id","type":"string"},{"name":"amount","type":"double"}]}';
  }
  return 'order_id STRING, customer_id STRING, amount DOUBLE, event_timestamp TIMESTAMP';
}

/**
 * Renders the property configuration panel for the Apache Spark ParsePayload step.
 *
 * @param props Selected ParsePayload node and property update callback.
 * @returns The rendered ParsePayload configuration form.
 */
export const ParsePayloadProperties: React.FC<ParsePayloadPropertiesProps> = ({
  node,
  onChange,
}) => {
  const properties: ParsePayloadStepProperties = node?.data?.properties || {};
  const format: PayloadFormat = normalizePayloadFormat(properties.format);
  const sourceColumn =
    typeof properties.sourceColumn === 'string' ? properties.sourceColumn : 'value';
  const schemaDDL = typeof properties.schemaDDL === 'string' ? properties.schemaDDL : '';
  const flattenFields =
    typeof properties.flattenFields === 'boolean' ? properties.flattenFields : true;
  const targetColumn =
    typeof properties.targetColumn === 'string' ? properties.targetColumn : 'parsed_payload';
  const rowTag = typeof properties.rowTag === 'string' ? properties.rowTag : '';
  const corruptRecordColumn =
    typeof properties.corruptRecordColumn === 'string'
      ? properties.corruptRecordColumn
      : '_corrupt_record';
  const keepSourceColumn =
    typeof properties.keepSourceColumn === 'boolean' ? properties.keepSourceColumn : false;
  const systemInstructions = resolveSystemInstructions(node);

  const isSourceColumnInvalid = sourceColumn.trim().length === 0;
  const isSchemaInvalid = schemaDDL.trim().length === 0;
  const isTargetColumnInvalid = !flattenFields && targetColumn.trim().length === 0;
  const isCorruptColumnInvalid = corruptRecordColumn.trim().length === 0;

  const commitPatch = (patch: Partial<ParsePayloadStepProperties>) => {
    onChange({
      ...properties,
      format,
      sourceColumn,
      schemaDDL,
      flattenFields,
      corruptRecordColumn,
      keepSourceColumn,
      ...patch,
    });
  };

  const handleFormatChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const nextFormat = normalizePayloadFormat(e.target.value);
    const patch: Partial<ParsePayloadStepProperties> = { format: nextFormat };
    if (nextFormat === 'xml' && !rowTag) {
      patch.rowTag = 'record';
    }
    commitPatch(patch);
  };

  const handleTextFieldChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    const { name, value } = e.target;
    commitPatch({ [name]: value });
  };

  const handleFlattenToggle = (e: React.ChangeEvent<HTMLInputElement>) => {
    const nextFlatten = e.target.checked;
    const patch: Partial<ParsePayloadStepProperties> = { flattenFields: nextFlatten };
    if (!nextFlatten) {
      patch.targetColumn = targetColumn;
    }
    commitPatch(patch);
  };

  const handleKeepSourceToggle = (e: React.ChangeEvent<HTMLInputElement>) => {
    commitPatch({ keepSourceColumn: e.target.checked });
  };

  const handleSystemInstructionsChange = (value: string) => {
    commitPatch({ system_instructions: value });
  };

  return (
    <div className="flex-1 flex flex-col gap-4">
      {/* 1. Payload Format & Source Column */}
      <div className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]">
        <div className="flex items-center gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)]">
          <FileCode className="w-4 h-4 text-blue-400 shrink-0" />
          <span>Payload Deserializer</span>
          <InfoTooltip text="Deserialize raw Kafka, Pub/Sub, Kinesis, or string columns into typed Spark StructType fields." />
        </div>

        <SelectField
          id="parsePayloadFormat"
          name="format"
          label="Payload Format"
          tooltip="Select F.from_json, pyspark.sql.avro.functions.from_avro, F.from_csv, or Spark 4.0 native F.from_xml."
          value={format}
          options={PAYLOAD_FORMAT_OPTIONS}
          onChange={handleFormatChange}
        />

        {format === 'avro' && (
          <div
            data-testid="parse-payload-avro-note"
            className="flex items-start gap-2 p-2 rounded border border-blue-500/40 bg-blue-500/10 text-xs text-[var(--vscode-text-secondary)]"
          >
            <Package className="w-4 h-4 text-blue-400 shrink-0 mt-0.5" />
            <span>
              Avro deserialization attaches{' '}
              <code className="font-mono text-[var(--vscode-text-primary)]">
                org.apache.spark:spark-avro_2.13
              </code>{' '}
              matching the selected Spark runtime profile.
            </span>
          </div>
        )}

        <PropertyField
          id="parsePayloadSourceColumn"
          name="sourceColumn"
          label="Source Column"
          required="Required"
          tooltip="Upstream binary or UTF-8 string column holding the serialized payload, such as value or data."
          invalid={isSourceColumnInvalid}
          error={isSourceColumnInvalid ? 'Source column is required.' : null}
          value={sourceColumn}
          onChange={handleTextFieldChange}
          placeholder="value"
        />

        {format === 'xml' && (
          <PropertyField
            id="parsePayloadRowTag"
            name="rowTag"
            label="XML Row Tag"
            tooltip="Optional XML element tag passed via rowTag option in F.from_xml()."
            value={rowTag}
            onChange={handleTextFieldChange}
            placeholder="record"
          />
        )}

        <PropertyField
          id="parsePayloadSchemaDDL"
          name="schemaDDL"
          type="textarea"
          rows={4}
          label={format === 'avro' ? 'Avro JSON Schema' : 'Target Schema (DDL / StructType)'}
          required="Required"
          tooltip={
            format === 'avro'
              ? 'Explicit Avro JSON schema string passed to from_avro(col, jsonFormatSchema).'
              : 'Explicit Spark SQL DDL schema (or JSON StructType) passed to F.from_json, F.from_csv, or F.from_xml.'
          }
          invalid={isSchemaInvalid}
          error={isSchemaInvalid ? 'Explicit payload schema is required.' : null}
          value={schemaDDL}
          onChange={handleTextFieldChange}
          placeholder={getPayloadSchemaPlaceholder(format)}
        />
      </div>

      {/* 2. Output Struct Flattening & Corrupt Record DLQ Settings */}
      <div
        data-testid="parse-payload-output-section"
        className="flex flex-col gap-3 p-3 rounded-md bg-[var(--vscode-subcard-bg)] border border-[var(--vscode-border)]"
      >
        <label
          htmlFor="parsePayloadFlattenFields"
          className="flex items-center justify-between gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)] cursor-pointer"
        >
          <span className="flex items-center gap-1.5">
            <span>Flatten Struct Fields</span>
            <InfoTooltip text="When enabled, expands parsed struct fields (parsed.*) into top-level DataFrame columns." />
          </span>
          <input
            id="parsePayloadFlattenFields"
            name="flattenFields"
            type="checkbox"
            checked={flattenFields}
            onChange={handleFlattenToggle}
            className="accent-blue-500 w-4 h-4 cursor-pointer"
          />
        </label>

        {!flattenFields && (
          <PropertyField
            id="parsePayloadTargetColumn"
            name="targetColumn"
            label="Target Struct Column"
            required="Required"
            tooltip="Output struct column name storing the parsed payload when Flatten Struct Fields is disabled."
            invalid={isTargetColumnInvalid}
            error={isTargetColumnInvalid ? 'Target struct column is required.' : null}
            value={targetColumn}
            onChange={handleTextFieldChange}
            placeholder="parsed_payload"
          />
        )}

        <label
          htmlFor="parsePayloadKeepSource"
          className="flex items-center justify-between gap-2 text-xs font-bold uppercase text-[var(--vscode-text-primary)] cursor-pointer"
        >
          <span className="flex items-center gap-1.5">
            <span>Keep Source Column</span>
            <InfoTooltip text="Retain the raw serialized source column on valid output rows alongside parsed fields." />
          </span>
          <input
            id="parsePayloadKeepSource"
            name="keepSourceColumn"
            type="checkbox"
            checked={keepSourceColumn}
            onChange={handleKeepSourceToggle}
            className="accent-blue-500 w-4 h-4 cursor-pointer"
          />
        </label>

        <PropertyField
          id="parsePayloadCorruptColumn"
          name="corruptRecordColumn"
          label="Corrupt Record Column"
          required="Required"
          tooltip="PERMISSIVE mode column storing unparseable payloads for dead-letter routing via the error handle."
          invalid={isCorruptColumnInvalid}
          error={isCorruptColumnInvalid ? 'Corrupt record column is required.' : null}
          value={corruptRecordColumn}
          onChange={handleTextFieldChange}
          placeholder="_corrupt_record"
        />

        <div
          data-testid="parse-payload-dlq-banner"
          className="flex items-start gap-2 p-2 rounded border border-[var(--vscode-border)] bg-[var(--vscode-card-bg)] text-xs text-[var(--vscode-text-muted)] leading-relaxed"
        >
          <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0 mt-0.5" />
          <span>
            Connect the <code className="font-mono text-[var(--vscode-text-primary)]">error</code>{' '}
            output handle to route malformed records where{' '}
            <code className="font-mono text-[var(--vscode-text-primary)]">
              {corruptRecordColumn || '_corrupt_record'} IS NOT NULL
            </code>{' '}
            to a dead-letter sink.
          </span>
        </div>
      </div>

      <SystemInstructionsField
        id="parsePayloadSystemInstructions"
        value={systemInstructions}
        tooltip="Custom prompt guidance appended when generating PySpark code for this ParsePayload step."
        placeholder="Enter custom instructions for ParsePayload code generation..."
        onChange={handleSystemInstructionsChange}
      />
    </div>
  );
};
