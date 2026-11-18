/**
 * @fileoverview Unit tests for the TFRecordSource / TFRecordIO source properties panel (SRC21).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates batch-only rendering, StorageBackendFields integration,
 * recordType ('Example' | 'SequenceExample') selection, SchemaEditor,
 * and SystemInstructionsField for TFRecordIOSourceProperties.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { TFRecordIOSourceProperties, TFRecordSourceProperties } from './TFRecordIO';

describe('TFRecordIOSourceProperties (SRC21 TFRecordSource)', () => {
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

  it('exports TFRecordSourceProperties as an alias for TFRecordIOSourceProperties', () => {
    expect(TFRecordSourceProperties).toBe(TFRecordIOSourceProperties);
  });

  it('renders batch-only defaults (recordType="Example") and flags required validation when path is empty', () => {
    const mockNode: any = {
      data: {
        label: 'TFRecordSource',
        spark_transform: 'TFRecordSource',
        properties: {},
      },
    };

    const { container } = render(
      <TFRecordIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#tfrecordSourceReadMode')).not.toBeInTheDocument();
    expect(container.querySelector('#tfrecordSourcePath')).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('TFRecord URI / Path is required.')).toBeInTheDocument();
    expect(container.querySelector('#tfrecordSourceRecordType')).toHaveValue('Example');
  });

  it('renders populated properties and propagates changes for path, recordType, SchemaEditor, and system_instructions', () => {
    const mockNode: any = {
      data: {
        label: 'TFRecordSource',
        spark_transform: 'TFRecordSource',
        properties: {
          path: 'gs://my-bucket/training/*.tfrecord',
          recordType: 'SequenceExample',
          schemaFields: [{ name: 'label', type: 'INTEGER', mode: 'REQUIRED' }],
          system_instructions: 'Decode SequenceExample features',
        },
      },
    };

    const { container } = render(
      <TFRecordIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#tfrecordSourcePath')).toHaveValue(
      'gs://my-bucket/training/*.tfrecord'
    );
    expect(container.querySelector('#tfrecordSourceRecordType')).toHaveValue('SequenceExample');
    expect(screen.getByLabelText('Field #1 Name')).toHaveValue('label');
    expect(container.querySelector('#tfrecordSourceInstructions')).toHaveValue(
      'Decode SequenceExample features'
    );

    fireEvent.change(container.querySelector('#tfrecordSourceRecordType')!, {
      target: { name: 'recordType', value: 'Example' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ recordType: 'Example' }));

    fireEvent.click(screen.getByRole('button', { name: /Add Schema Field/i }));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        schemaFields: expect.arrayContaining([
          expect.objectContaining({ name: 'label', type: 'INTEGER', mode: 'REQUIRED' }),
        ]),
      })
    );

    fireEvent.change(container.querySelector('#tfrecordSourceInstructions')!, {
      target: { value: 'Updated TFRecord instructions' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Updated TFRecord instructions' })
    );
  });
});
