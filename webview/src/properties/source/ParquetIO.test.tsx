/**
 * @fileoverview Unit tests for the ParquetSource / ParquetIO source properties panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates rendering, readMode switching, StorageBackendFields integration,
 * mergeSchema, datetimeRebaseMode, int96RebaseMode, maxFilesPerTrigger,
 * SchemaEditor, and SystemInstructionsField for ParquetIOSourceProperties.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { VsCodeApi } from '../../bridge/vscodeApi';
import {
  ParquetIOSourceProperties,
  ParquetSourceProperties,
  type ParquetIOSourceNode,
} from './ParquetIO';

describe('ParquetIOSourceProperties (SRC13 ParquetSource)', () => {
  const mockOnChange = vi.fn<(properties: Record<string, unknown>) => void>();
  let originalVscode: VsCodeApi | undefined;

  beforeEach(() => {
    vi.clearAllMocks();
    originalVscode = window.vscode;
    window.vscode = { postMessage: vi.fn() };
  });

  afterEach(() => {
    window.vscode = originalVscode;
  });

  it('exports ParquetSourceProperties as an alias for ParquetIOSourceProperties', () => {
    expect(ParquetSourceProperties).toBe(ParquetIOSourceProperties);
  });

  it('renders default batch settings and flags required validation when path is empty', () => {
    const mockNode: ParquetIOSourceNode = {
      data: {
        label: 'ParquetSource',
        spark_transform: 'ParquetSource',
        properties: {},
      },
    };

    const { container } = render(
      <ParquetIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    const readModeSelect = container.querySelector('#parquetSourceReadMode') as HTMLSelectElement;
    expect(readModeSelect).toHaveValue('batch');

    const pathInput = container.querySelector('#parquetSourcePath') as HTMLInputElement;
    expect(pathInput).toHaveValue('');
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Parquet URI / Path is required.')).toBeInTheDocument();

    expect(container.querySelector('#parquetSourceMaxFilesPerTrigger')).not.toBeInTheDocument();
  });

  it('renders populated properties including streaming maxFilesPerTrigger and rebase modes', () => {
    const mockNode: ParquetIOSourceNode = {
      data: {
        label: 'ParquetSource',
        spark_transform: 'ParquetSource',
        properties: {
          readMode: 'streaming',
          path: 's3a://my-lakehouse/events/*.parquet',
          mergeSchema: true,
          datetimeRebaseMode: 'CORRECTED',
          int96RebaseMode: 'LEGACY',
          maxFilesPerTrigger: 50,
          schemaFields: [{ name: 'event_id', type: 'STRING', mode: 'REQUIRED' }],
          system_instructions: 'Prune date partitions',
        },
      },
    };

    const { container } = render(
      <ParquetIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#parquetSourceReadMode')).toHaveValue('streaming');
    expect(container.querySelector('#parquetSourceStorageBackend')).toHaveValue('s3a');
    expect(container.querySelector('#parquetSourcePath')).toHaveValue(
      's3a://my-lakehouse/events/*.parquet'
    );
    expect(container.querySelector('#parquetSourceMergeSchema')).toBeChecked();
    expect(container.querySelector('#parquetSourceDatetimeRebaseMode')).toHaveValue('CORRECTED');
    expect(container.querySelector('#parquetSourceInt96RebaseMode')).toHaveValue('LEGACY');
    expect(container.querySelector('#parquetSourceMaxFilesPerTrigger')).toHaveValue(50);
    expect(screen.getByLabelText('Field #1 Name')).toHaveValue('event_id');
    expect(container.querySelector('#parquetSourceInstructions')).toHaveValue(
      'Prune date partitions'
    );
  });

  it('propagates changes for readMode, path, mergeSchema, rebase modes, maxFilesPerTrigger, SchemaEditor, and system_instructions', () => {
    const mockNode: ParquetIOSourceNode = {
      data: {
        properties: {
          readMode: 'streaming',
          path: 'gs://bucket/data/',
        },
      },
    };

    const { container } = render(
      <ParquetIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    fireEvent.change(container.querySelector('#parquetSourceReadMode')!, {
      target: { name: 'readMode', value: 'batch' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ readMode: 'batch' }));

    fireEvent.click(container.querySelector('#parquetSourceMergeSchema')!);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ mergeSchema: true }));

    fireEvent.change(container.querySelector('#parquetSourceDatetimeRebaseMode')!, {
      target: { name: 'datetimeRebaseMode', value: 'CORRECTED' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ datetimeRebaseMode: 'CORRECTED' })
    );

    fireEvent.change(container.querySelector('#parquetSourceInt96RebaseMode')!, {
      target: { name: 'int96RebaseMode', value: 'LEGACY' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ int96RebaseMode: 'LEGACY' })
    );

    fireEvent.change(container.querySelector('#parquetSourceMaxFilesPerTrigger')!, {
      target: { name: 'maxFilesPerTrigger', value: '25' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxFilesPerTrigger: '25' })
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Schema Field/i }));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        schemaFields: [expect.objectContaining({ name: '', type: 'STRING', mode: 'NULLABLE' })],
      })
    );

    fireEvent.change(container.querySelector('#parquetSourceInstructions')!, {
      target: { value: 'Use explicit schema' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Use explicit schema' })
    );
  });
});
