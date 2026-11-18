/**
 * @fileoverview Unit tests for the AvroSource / AvroIO source properties panel (SRC15).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates rendering, readMode switching, StorageBackendFields integration,
 * avroSchema JSON textarea, ignoreExtension toggle, maxFilesPerTrigger,
 * SchemaEditor, and SystemInstructionsField for AvroIOSourceProperties.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { AvroIOSourceProperties, AvroSourceProperties, SAMPLE_AVRO_SCHEMA } from './AvroIO';

describe('AvroIOSourceProperties (SRC15 AvroSource)', () => {
  const mockOnChange = vi.fn();
  let originalVscode: any;

  beforeEach(() => {
    vi.clearAllMocks();
    originalVscode = (window as any).vscode;
    (window as any).vscode = { postMessage: vi.fn() };
  });

  afterEach(() => {
    (window as any).vscode = originalVscode;
  });

  it('exports AvroSourceProperties as an alias for AvroIOSourceProperties', () => {
    expect(AvroSourceProperties).toBe(AvroIOSourceProperties);
  });

  it('renders default batch settings and flags required validation when path is empty', () => {
    const mockNode: any = {
      data: {
        label: 'AvroSource',
        spark_transform: 'AvroSource',
        properties: {},
      },
    };

    const { container } = render(
      <AvroIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#avroSourceReadMode')).toHaveValue('batch');
    const pathInput = container.querySelector('#avroSourcePath') as HTMLInputElement;
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Avro URI / Path is required.')).toBeInTheDocument();
    expect(container.querySelector('#avroSourceMaxFilesPerTrigger')).not.toBeInTheDocument();
  });

  it('renders populated properties including streaming maxFilesPerTrigger, avroSchema, and ignoreExtension', () => {
    const mockNode: any = {
      data: {
        label: 'AvroSource',
        spark_transform: 'AvroSource',
        properties: {
          readMode: 'streaming',
          path: 'gs://my-bucket/events/*.avro',
          avroSchema: SAMPLE_AVRO_SCHEMA,
          ignoreExtension: true,
          maxFilesPerTrigger: 10,
          schemaFields: [{ name: 'id', type: 'STRING', mode: 'REQUIRED' }],
          system_instructions: 'Read Avro container records',
        },
      },
    };

    const { container } = render(
      <AvroIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#avroSourceReadMode')).toHaveValue('streaming');
    expect(container.querySelector('#avroSourcePath')).toHaveValue(
      'gs://my-bucket/events/*.avro'
    );
    expect(container.querySelector('#avroSourceAvroSchema')).toHaveValue(SAMPLE_AVRO_SCHEMA);
    expect(container.querySelector('#avroSourceIgnoreExtension')).toBeChecked();
    expect(container.querySelector('#avroSourceMaxFilesPerTrigger')).toHaveValue(10);
    expect(screen.getByLabelText('Field #1 Name')).toHaveValue('id');
    expect(container.querySelector('#avroSourceSystemInstructions')).toHaveValue(
      'Read Avro container records'
    );
  });

  it('propagates updates for avroSchema, ignoreExtension, maxFilesPerTrigger, SchemaEditor, and system_instructions', () => {
    const mockNode: any = {
      data: {
        properties: {
          readMode: 'streaming',
          path: 'gs://my-bucket/events/*.avro',
        },
      },
    };

    const { container } = render(
      <AvroIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    fireEvent.change(container.querySelector('#avroSourceAvroSchema')!, {
      target: { name: 'avroSchema', value: '{"type":"record","name":"R","fields":[]}' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ avroSchema: '{"type":"record","name":"R","fields":[]}' })
    );

    fireEvent.click(container.querySelector('#avroSourceIgnoreExtension')!);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ ignoreExtension: true }));

    fireEvent.change(container.querySelector('#avroSourceMaxFilesPerTrigger')!, {
      target: { name: 'maxFilesPerTrigger', value: '50' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxFilesPerTrigger: '50' })
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Schema Field/i }));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        schemaFields: [expect.objectContaining({ name: '', type: 'STRING', mode: 'NULLABLE' })],
      })
    );

    fireEvent.change(container.querySelector('#avroSourceSystemInstructions')!, {
      target: { value: 'Custom Avro prompt' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Custom Avro prompt' })
    );
  });
});
