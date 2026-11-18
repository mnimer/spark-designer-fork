/**
 * @fileoverview Unit tests for the CsvSource / CsvIO source properties panel (SRC16).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates rendering, readMode switching, StorageBackendFields integration,
 * CSV dialect options (header, inferSchema, delimiter, quote, escape, nullValue,
 * mode, columnNameOfCorruptRecord, multiLine), maxFilesPerTrigger, SchemaEditor,
 * and SystemInstructionsField for CsvIOSourceProperties.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { CsvIOSourceProperties, CsvSourceProperties } from './CsvIO';

describe('CsvIOSourceProperties (SRC16 CsvSource)', () => {
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

  it('exports CsvSourceProperties as an alias for CsvIOSourceProperties', () => {
    expect(CsvSourceProperties).toBe(CsvIOSourceProperties);
  });

  it('renders Spark CSV defaults (header=true, inferSchema=true, delimiter=",", quote="\\"", escape="\\\\", mode="PERMISSIVE", columnNameOfCorruptRecord="_corrupt_record")', () => {
    const mockNode: any = {
      data: {
        label: 'CsvSource',
        spark_transform: 'CsvSource',
        properties: {},
      },
    };

    const { container } = render(
      <CsvIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#csvSourceReadMode')).toHaveValue('batch');
    expect(container.querySelector('#csvSourcePath')).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('CSV URI / Path is required.')).toBeInTheDocument();

    expect(container.querySelector('#csvSourceHeader')).toBeChecked();
    expect(container.querySelector('#csvSourceInferSchema')).toBeChecked();
    expect(container.querySelector('#csvSourceMultiLine')).not.toBeChecked();
    expect(container.querySelector('#csvSourceDelimiter')).toHaveValue(',');
    expect(container.querySelector('#csvSourceQuote')).toHaveValue('"');
    expect(container.querySelector('#csvSourceEscape')).toHaveValue('\\');
    expect(container.querySelector('#csvSourceMode')).toHaveValue('PERMISSIVE');
    expect(container.querySelector('#csvSourceCorruptRecordCol')).toHaveValue('_corrupt_record');
  });

  it('renders populated streaming CSV properties and propagates changes across all fields', () => {
    const mockNode: any = {
      data: {
        label: 'CsvSource',
        spark_transform: 'CsvSource',
        properties: {
          readMode: 'streaming',
          path: 'gs://my-bucket/orders/*.csv',
          header: true,
          inferSchema: false,
          delimiter: '|',
          quote: "'",
          escape: '\\',
          nullValue: 'NULL',
          mode: 'PERMISSIVE',
          columnNameOfCorruptRecord: '_bad_csv',
          multiLine: true,
          maxFilesPerTrigger: 25,
          schemaFields: [{ name: 'order_id', type: 'STRING', mode: 'REQUIRED' }],
          system_instructions: 'Parse pipe-delimited CSV',
        },
      },
    };

    const { container } = render(
      <CsvIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#csvSourceReadMode')).toHaveValue('streaming');
    expect(container.querySelector('#csvSourcePath')).toHaveValue('gs://my-bucket/orders/*.csv');
    expect(container.querySelector('#csvSourceInferSchema')).not.toBeChecked();
    expect(container.querySelector('#csvSourceMultiLine')).toBeChecked();
    expect(container.querySelector('#csvSourceDelimiter')).toHaveValue('|');
    expect(container.querySelector('#csvSourceQuote')).toHaveValue("'");
    expect(container.querySelector('#csvSourceNullValue')).toHaveValue('NULL');
    expect(container.querySelector('#csvSourceCorruptRecordCol')).toHaveValue('_bad_csv');
    expect(container.querySelector('#csvSourceMaxFilesPerTrigger')).toHaveValue(25);

    fireEvent.click(container.querySelector('#csvSourceHeader')!);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ header: false }));

    fireEvent.click(container.querySelector('#csvSourceInferSchema')!);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ inferSchema: true }));

    fireEvent.change(container.querySelector('#csvSourceDelimiter')!, {
      target: { name: 'delimiter', value: ';' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ delimiter: ';' }));

    fireEvent.change(container.querySelector('#csvSourceEscape')!, {
      target: { name: 'escape', value: '/' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ escape: '/' }));

    fireEvent.change(container.querySelector('#csvSourceMode')!, {
      target: { name: 'mode', value: 'FAILFAST' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ mode: 'FAILFAST' }));

    fireEvent.change(container.querySelector('#csvSourceInstructions')!, {
      target: { value: 'Updated CSV instructions' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Updated CSV instructions' })
    );
  });
});
