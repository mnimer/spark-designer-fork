/**
 * @fileoverview XML Sink (SNK17 XmlSink / XmlIO) property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Renders the property configuration panel for Apache Spark 4.0 built-in XML sink nodes
 * using df.write.format("xml").save(uri) and foreachBatch for Structured Streaming.
 * Embeds the shared StorageBackendFields component alongside Spark 4.x saveMode,
 * compression codecs, partitionBy columns, maxRecordsPerFile, rootTag, rowTag,
 * declaration, nullValue, attributePrefix, valueTag, and SystemInstructionsField.
 */

/* eslint-disable @typescript-eslint/no-explicit-any, react-refresh/only-export-components */
import React from 'react';
import { PropertyField } from '../../components/common/PropertyField';
import { SelectField, type SelectFieldOption } from '../../components/common/SelectField';
import { StorageBackendFields } from '../../components/common/StorageBackendFields';
import { SystemInstructionsField } from '../../components/common/SystemInstructionsField';
import { resolveSystemInstructions } from '../../utils/systemInstructions';

/**
 * Supported Spark 4.x batch DataFrameWriter save modes.
 */
export const XML_SAVE_MODES: SelectFieldOption[] = [
  { value: 'append', label: 'append (Append rows to existing path)' },
  { value: 'overwrite', label: 'overwrite (Replace existing data)' },
  { value: 'errorifexists', label: 'errorifexists (Fail if destination exists)' },
  { value: 'ignore', label: 'ignore (Skip write if destination exists)' },
];

/**
 * Supported Spark 4.0 built-in XML writer compression codecs.
 */
export const XML_COMPRESSION_CODECS: SelectFieldOption[] = [
  { value: 'none', label: 'none (Uncompressed XML)' },
  { value: 'gzip', label: 'gzip (.xml.gz)' },
  { value: 'zstd', label: 'zstd (.xml.zst)' },
  { value: 'bzip2', label: 'bzip2 (.xml.bz2)' },
  { value: 'lz4', label: 'lz4 (.xml.lz4)' },
  { value: 'snappy', label: 'snappy (.xml.snappy)' },
];

/**
 * React Flow node shape for the XML sink panel.
 */
export interface XmlIOSinkNode {
  data: {
    label: string;
    spark_transform?: string;
    system_instructions?: string;
    special_instructions?: string;
    properties?: {
      storageBackend?: string;
      path?: string;
      to?: string;
      uri?: string;
      authMode?: string;
      secretName?: string;
      saveMode?: 'append' | 'overwrite' | 'errorifexists' | 'ignore';
      mode?: string;
      compression?: 'none' | 'gzip' | 'zstd' | 'bzip2' | 'lz4' | 'snappy' | string;
      partitionBy?: string | string[];
      maxRecordsPerFile?: string | number;
      rootTag?: string;
      rootElement?: string;
      rowTag?: string;
      declaration?: string;
      nullValue?: string;
      attributePrefix?: string;
      valueTag?: string;
      system_instructions?: string;
      special_instructions?: string;
      [key: string]: any;
    };
  };
}

/**
 * Props for the {@link XmlIOSinkProperties} component.
 */
export interface XmlIOSinkPropertiesProps {
  node: XmlIOSinkNode;
  onChange: (properties: any) => void;
}

/**
 * Normalizes a raw XML compression codec value to a canonical Spark codec name.
 */
function normalizeXmlCompression(raw?: string): string {
  const lower = (raw ?? '').trim().toLowerCase();
  if (!lower || lower === 'uncompressed') {
    return 'none';
  }
  if (XML_COMPRESSION_CODECS.some((opt) => opt.value === lower)) {
    return lower;
  }
  return 'none';
}

/**
 * Formats a partitionBy property value as a comma-separated column list string.
 */
function resolvePartitionByValue(raw: unknown): string {
  if (Array.isArray(raw)) {
    return raw.join(', ');
  }
  return typeof raw === 'string' ? raw : '';
}

/**
 * Dedicated property panel for Apache Spark 4.0 XmlSink / XmlIO sink nodes.
 *
 * @param props Selected node and change callback.
 * @returns Rendered XML sink property panel.
 */
export const XmlIOSinkProperties: React.FC<XmlIOSinkPropertiesProps> = ({ node, onChange }) => {
  const properties = node.data.properties || {};
  const storageProperties =
    properties.path === undefined && typeof properties.to === 'string' && properties.to.length > 0
      ? { ...properties, path: properties.to }
      : properties;

  const rawSaveMode = (properties.saveMode || properties.mode || 'append').toLowerCase();
  const saveMode = XML_SAVE_MODES.some((opt) => opt.value === rawSaveMode) ? rawSaveMode : 'append';
  const compression = normalizeXmlCompression(properties.compression);
  const partitionBy = resolvePartitionByValue(properties.partitionBy);
  const maxRecordsPerFile = properties.maxRecordsPerFile ?? '';
  const rootTag = properties.rootTag ?? properties.rootElement ?? 'ROWS';
  const rowTag = properties.rowTag ?? 'ROW';
  const declaration = properties.declaration ?? '';
  const nullValue = properties.nullValue ?? '';
  const attributePrefix = properties.attributePrefix ?? '_';
  const valueTag = properties.valueTag ?? '_VALUE';
  const systemInstructions = resolveSystemInstructions(node);

  const isRootTagInvalid = !rootTag.trim();
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
      {/* Shared Multi-Cloud Storage Backend Configuration */}
      <StorageBackendFields
        properties={storageProperties}
        onChange={onChange}
        idPrefix="xmlSink"
        pathLabel="Output XML URI / Path"
        pathPlaceholder="gs://my-bucket/output/xml/"
        showGlobAndRecursive={false}
      />

      {/* XML Writer Save Mode, Compression & Partitioning */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          XML Writer Options (Built-in Spark 4.0 .format(&quot;xml&quot;))
        </span>

        <SelectField
          id="xmlSinkSaveMode"
          name="saveMode"
          label="Save Mode (mode)"
          tooltip="Controls how Spark handles existing data at the destination URI in batch writes. Streaming branches execute idempotent micro-batch writes via foreachBatch."
          value={saveMode}
          options={XML_SAVE_MODES}
          onChange={handleChange}
        />

        <SelectField
          id="xmlSinkCompression"
          name="compression"
          label="Compression Codec (compression)"
          tooltip="Compression codec applied to output XML part-files."
          value={compression}
          options={XML_COMPRESSION_CODECS}
          onChange={handleChange}
        />

        <PropertyField
          id="xmlSinkPartitionBy"
          name="partitionBy"
          label="Partition Columns (partitionBy)"
          tooltip="Optional comma-separated column names used to partition output directories in Hive layout (for example, feed_date, category)."
          value={partitionBy}
          onChange={handleChange}
          placeholder="e.g. feed_date, category"
          mono={true}
        />

        <PropertyField
          id="xmlSinkMaxRecordsPerFile"
          type="number"
          name="maxRecordsPerFile"
          label="Max Records Per File (maxRecordsPerFile)"
          tooltip="Optional maximum number of XML row elements written per output part-file."
          value={maxRecordsPerFile}
          onChange={handleChange}
          placeholder="e.g. 100000"
          min="1"
          mono={true}
        />
      </div>

      {/* Spark 4.0 XML Element & Attribute Formatting */}
      <div className="flex flex-col gap-3 p-3 bg-[var(--vscode-subcard-bg)] rounded-md border border-[var(--vscode-border)]">
        <span className="text-xs font-bold uppercase text-[var(--vscode-text-muted)]">
          XML Element &amp; Attribute Structure
        </span>

        <div className="grid grid-cols-2 gap-3">
          <PropertyField
            id="xmlSinkRootTag"
            name="rootTag"
            label="Root Tag (rootTag)"
            required="Root Tag is required."
            aria-required={true}
            aria-invalid={isRootTagInvalid}
            invalid={isRootTagInvalid}
            error={isRootTagInvalid ? 'Root Tag is required.' : null}
            tooltip="Top-level XML element wrapping all rows in each part-file (default 'ROWS')."
            value={rootTag}
            onChange={handleChange}
            placeholder="ROWS"
            mono={true}
          />

          <PropertyField
            id="xmlSinkRowTag"
            name="rowTag"
            label="Row Tag (rowTag)"
            required="Row Tag is required."
            aria-required={true}
            aria-invalid={isRowTagInvalid}
            invalid={isRowTagInvalid}
            error={isRowTagInvalid ? 'Row Tag is required.' : null}
            tooltip="Repeating XML element wrapping each DataFrame row (default 'ROW')."
            value={rowTag}
            onChange={handleChange}
            placeholder="ROW"
            mono={true}
          />
        </div>

        <PropertyField
          id="xmlSinkDeclaration"
          name="declaration"
          label="XML Declaration (declaration)"
          tooltip="XML header declaration content written inside <?xml ... ?> at the top of each file. Leave blank to use the Spark default header."
          value={declaration}
          onChange={handleChange}
          placeholder='version="1.0" encoding="UTF-8" standalone="yes"'
          mono={true}
        />

        <div className="grid grid-cols-3 gap-3">
          <PropertyField
            id="xmlSinkAttributePrefix"
            name="attributePrefix"
            label="Attribute Prefix"
            tooltip="Prefix identifying struct fields or columns written as XML attributes (default '_')."
            value={attributePrefix}
            onChange={handleChange}
            placeholder="_"
            mono={true}
          />

          <PropertyField
            id="xmlSinkValueTag"
            name="valueTag"
            label="Value Tag"
            tooltip="Field name holding text content when an element also carries attributes (default '_VALUE')."
            value={valueTag}
            onChange={handleChange}
            placeholder="_VALUE"
            mono={true}
          />

          <PropertyField
            id="xmlSinkNullValue"
            name="nullValue"
            label="Null Value"
            tooltip="Optional string written for null values (leave blank for Spark default null representation)."
            value={nullValue}
            onChange={handleChange}
            placeholder="null"
            mono={true}
          />
        </div>
      </div>

      {/* Code Generation Instructions */}
      <SystemInstructionsField
        id="xmlSinkInstructions"
        value={systemInstructions}
        placeholder="e.g. Prefix id column as _id so Spark 4.0 serializes it as an XML attribute on each <book> element."
        tooltip="Custom instructions appended to the Gemini prompt when synthesizing PySpark XML writer code."
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

export const XmlSinkProperties = XmlIOSinkProperties;
