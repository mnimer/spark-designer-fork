/**
 * @fileoverview Unit tests for the Apache Spark ParsePayload step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  ParsePayloadProperties,
  normalizePayloadFormat,
  getPayloadSchemaPlaceholder,
} from './ParsePayload';

describe('ParsePayloadProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-parse-payload-1',
    data: {
      label: 'ParsePayload',
      spark_transform: 'ParsePayload',
      properties,
    },
  });

  describe('helpers', () => {
    it('normalizes payload format case-insensitively and defaults to json', () => {
      expect(normalizePayloadFormat('AVRO')).toBe('avro');
      expect(normalizePayloadFormat(' xml ')).toBe('xml');
      expect(normalizePayloadFormat('unknown')).toBe('json');
      expect(normalizePayloadFormat(undefined)).toBe('json');
    });

    it('returns Avro JSON vs Spark DDL schema placeholders', () => {
      expect(getPayloadSchemaPlaceholder('avro')).toContain('"type":"record"');
      expect(getPayloadSchemaPlaceholder('json')).toContain('order_id STRING');
    });
  });

  it('renders defaults (format json, sourceColumn value, flattenFields true, _corrupt_record) and validates empty schema', () => {
    render(<ParsePayloadProperties node={createNode()} onChange={vi.fn()} />);

    expect(screen.getByLabelText(/Payload Format/i, { selector: 'select' })).toHaveValue('json');
    expect(screen.getByLabelText(/^Source Column/i, { selector: 'input' })).toHaveValue('value');
    expect(screen.getByLabelText(/Flatten Struct Fields/i, { selector: 'input' })).toBeChecked();
    expect(screen.getByLabelText(/Keep Source Column/i, { selector: 'input' })).not.toBeChecked();
    expect(screen.getByLabelText(/Corrupt Record Column/i, { selector: 'input' })).toHaveValue(
      '_corrupt_record'
    );
    expect(
      screen.queryByLabelText(/Target Struct Column/i, { selector: 'input' })
    ).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/XML Row Tag/i, { selector: 'input' })).not.toBeInTheDocument();
    expect(screen.getByRole('alert')).toHaveTextContent('Explicit payload schema is required.');
  });

  it('shows spark-avro package note for Avro and rowTag input for XML', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <ParsePayloadProperties
        node={createNode({ schemaDDL: 'id STRING' })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Payload Format/i, { selector: 'select' }), {
      target: { value: 'avro' },
    });
    expect(handleChange).toHaveBeenCalledWith(expect.objectContaining({ format: 'avro' }));

    rerender(
      <ParsePayloadProperties
        node={createNode({ format: 'avro', schemaDDL: '{"type":"record","name":"R","fields":[]}' })}
        onChange={handleChange}
      />
    );
    expect(screen.getByTestId('parse-payload-avro-note')).toHaveTextContent(
      'org.apache.spark:spark-avro_2.13'
    );

    rerender(
      <ParsePayloadProperties
        node={createNode({ format: 'xml', rowTag: 'book', schemaDDL: 'title STRING' })}
        onChange={handleChange}
      />
    );
    const rowTagInput = screen.getByLabelText(/XML Row Tag/i, { selector: 'input' });
    expect(rowTagInput).toHaveValue('book');
    fireEvent.change(rowTagInput, { target: { value: 'item' } });
    expect(handleChange).toHaveBeenCalledWith(expect.objectContaining({ rowTag: 'item' }));
  });

  it('reveals targetColumn when flattenFields is unchecked and toggles keepSourceColumn and instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <ParsePayloadProperties
        node={createNode({ schemaDDL: 'order_id STRING, amount DOUBLE' })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByLabelText(/Flatten Struct Fields/i, { selector: 'input' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        flattenFields: false,
        targetColumn: 'parsed_payload',
      })
    );

    handleChange.mockClear();
    rerender(
      <ParsePayloadProperties
        node={createNode({
          schemaDDL: 'order_id STRING, amount DOUBLE',
          flattenFields: false,
          targetColumn: 'event_struct',
        })}
        onChange={handleChange}
      />
    );

    const targetColInput = screen.getByLabelText(/Target Struct Column/i, { selector: 'input' });
    expect(targetColInput).toHaveValue('event_struct');

    fireEvent.click(screen.getByLabelText(/Keep Source Column/i, { selector: 'input' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        keepSourceColumn: true,
      })
    );

    handleChange.mockClear();
    fireEvent.change(
      screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }),
      { target: { value: 'Preserve Kafka offset metadata alongside parsed JSON.' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Preserve Kafka offset metadata alongside parsed JSON.',
      })
    );
  });
});
