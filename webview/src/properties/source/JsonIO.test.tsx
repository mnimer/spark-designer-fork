/**
 * @fileoverview Unit tests for the JsonSource / JsonIO source properties panel (SRC17).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates rendering, readMode switching, StorageBackendFields integration,
 * multiLine, primitivesAsString, allowComments, mode, columnNameOfCorruptRecord,
 * maxFilesPerTrigger, SchemaEditor, and SystemInstructionsField for JsonIOSourceProperties.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { VsCodeApi } from '../../bridge/vscodeApi';
import { JsonIOSourceProperties, JsonSourceProperties, type JsonIOSourceNode } from './JsonIO';

describe('JsonIOSourceProperties (SRC17 JsonSource)', () => {
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

  it('exports JsonSourceProperties as an alias for JsonIOSourceProperties', () => {
    expect(JsonSourceProperties).toBe(JsonIOSourceProperties);
  });

  it('renders Spark JSON defaults and flags required validation when path is empty', () => {
    const mockNode: JsonIOSourceNode = {
      data: {
        label: 'JsonSource',
        spark_transform: 'JsonSource',
        properties: {},
      },
    };

    const { container } = render(
      <JsonIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#jsonSourceReadMode')).toHaveValue('batch');
    expect(container.querySelector('#jsonSourcePath')).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('JSON URI / Path is required.')).toBeInTheDocument();

    expect(container.querySelector('#jsonSourceMultiLine')).not.toBeChecked();
    expect(container.querySelector('#jsonSourcePrimitivesAsString')).not.toBeChecked();
    expect(container.querySelector('#jsonSourceAllowComments')).not.toBeChecked();
    expect(container.querySelector('#jsonSourceMode')).toHaveValue('PERMISSIVE');
    expect(container.querySelector('#jsonSourceCorruptRecordCol')).toHaveValue('_corrupt_record');
  });

  it('renders populated streaming JSON properties and propagates changes across all fields', () => {
    const mockNode: JsonIOSourceNode = {
      data: {
        label: 'JsonSource',
        spark_transform: 'JsonSource',
        properties: {
          readMode: 'streaming',
          path: 'gs://my-bucket/events/*.json',
          multiLine: true,
          primitivesAsString: true,
          allowComments: true,
          mode: 'PERMISSIVE',
          columnNameOfCorruptRecord: '_malformed_json',
          maxFilesPerTrigger: 40,
          schemaFields: [{ name: 'event_id', type: 'STRING', mode: 'REQUIRED' }],
          system_instructions: 'Parse nested JSON objects',
        },
      },
    };

    const { container } = render(
      <JsonIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#jsonSourceReadMode')).toHaveValue('streaming');
    expect(container.querySelector('#jsonSourcePath')).toHaveValue('gs://my-bucket/events/*.json');
    expect(container.querySelector('#jsonSourceMultiLine')).toBeChecked();
    expect(container.querySelector('#jsonSourcePrimitivesAsString')).toBeChecked();
    expect(container.querySelector('#jsonSourceAllowComments')).toBeChecked();
    expect(container.querySelector('#jsonSourceCorruptRecordCol')).toHaveValue('_malformed_json');
    expect(container.querySelector('#jsonSourceMaxFilesPerTrigger')).toHaveValue(40);

    fireEvent.click(container.querySelector('#jsonSourceMultiLine')!);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ multiLine: false }));

    fireEvent.click(container.querySelector('#jsonSourcePrimitivesAsString')!);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ primitivesAsString: false })
    );

    fireEvent.click(container.querySelector('#jsonSourceAllowComments')!);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ allowComments: false }));

    fireEvent.change(container.querySelector('#jsonSourceMode')!, {
      target: { name: 'mode', value: 'DROPMALFORMED' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ mode: 'DROPMALFORMED' })
    );

    fireEvent.change(container.querySelector('#jsonSourceInstructions')!, {
      target: { value: 'Updated JSON instructions' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Updated JSON instructions' })
    );
  });
});
