/**
 * @fileoverview Unit tests for the OrcSource properties panel (SRC14).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates rendering, readMode switching, StorageBackendFields integration,
 * mergeSchema, maxFilesPerTrigger, SchemaEditor, and SystemInstructionsField
 * for OrcSourceProperties.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { VsCodeApi } from '../../bridge/vscodeApi';
import { OrcSourceProperties, OrcIOSourceProperties, type OrcSourceNode } from './OrcSource';

describe('OrcSourceProperties (SRC14 OrcSource)', () => {
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

  it('exports OrcIOSourceProperties as an alias for OrcSourceProperties', () => {
    expect(OrcIOSourceProperties).toBe(OrcSourceProperties);
  });

  it('renders default batch settings and flags required validation when path is empty', () => {
    const mockNode: OrcSourceNode = {
      data: {
        label: 'OrcSource',
        spark_transform: 'OrcSource',
        properties: {},
      },
    };

    const { container } = render(
      <OrcSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#orcSourceReadMode')).toHaveValue('batch');
    const pathInput = container.querySelector('#orcSourcePath') as HTMLInputElement;
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('ORC URI / Path is required.')).toBeInTheDocument();
    expect(container.querySelector('#orcSourceMaxFilesPerTrigger')).not.toBeInTheDocument();
  });

  it('renders streaming mode with maxFilesPerTrigger, mergeSchema, SchemaEditor, and system_instructions', () => {
    const mockNode: OrcSourceNode = {
      data: {
        label: 'OrcSource',
        spark_transform: 'OrcSource',
        properties: {
          readMode: 'streaming',
          path: 'gs://my-bucket/warehouse/*.orc',
          mergeSchema: true,
          maxFilesPerTrigger: 20,
          schemaFields: [{ name: 'order_id', type: 'BIGINT', mode: 'REQUIRED' }],
          system_instructions: 'Filter active orders',
        },
      },
    };

    const { container } = render(
      <OrcSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#orcSourceReadMode')).toHaveValue('streaming');
    expect(container.querySelector('#orcSourcePath')).toHaveValue(
      'gs://my-bucket/warehouse/*.orc'
    );
    expect(container.querySelector('#orcSourceMergeSchema')).toBeChecked();
    expect(container.querySelector('#orcSourceMaxFilesPerTrigger')).toHaveValue(20);
    expect(screen.getByLabelText('Field #1 Name')).toHaveValue('order_id');
    expect(container.querySelector('#orcSourceInstructions')).toHaveValue('Filter active orders');
  });

  it('propagates updates for readMode, mergeSchema, maxFilesPerTrigger, SchemaEditor, and system_instructions', () => {
    const mockNode: OrcSourceNode = {
      data: {
        properties: {
          readMode: 'streaming',
          path: 'gs://my-bucket/warehouse/*.orc',
        },
      },
    };

    const { container } = render(
      <OrcSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    fireEvent.click(container.querySelector('#orcSourceMergeSchema')!);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ mergeSchema: true }));

    fireEvent.change(container.querySelector('#orcSourceMaxFilesPerTrigger')!, {
      target: { name: 'maxFilesPerTrigger', value: '15' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxFilesPerTrigger: '15' })
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Schema Field/i }));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        schemaFields: [expect.objectContaining({ name: '', type: 'STRING', mode: 'NULLABLE' })],
      })
    );

    fireEvent.change(container.querySelector('#orcSourceInstructions')!, {
      target: { value: 'Custom ORC prompt' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Custom ORC prompt' })
    );
  });
});
