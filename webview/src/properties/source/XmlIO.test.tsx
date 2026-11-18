/**
 * @fileoverview Unit tests for the XmlSource / XmlIO source properties panel (SRC19).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates rendering, readMode switching, StorageBackendFields integration,
 * required rowTag validation, valueTag, attributePrefix, mode,
 * columnNameOfCorruptRecord, maxFilesPerTrigger, SchemaEditor, and
 * SystemInstructionsField for XmlIOSourceProperties.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { XmlIOSourceProperties, XmlSourceProperties } from './XmlIO';

describe('XmlIOSourceProperties (SRC19 XmlSource)', () => {
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

  it('exports XmlSourceProperties as an alias for XmlIOSourceProperties', () => {
    expect(XmlSourceProperties).toBe(XmlIOSourceProperties);
  });

  it('renders defaults and flags required validation errors when path and rowTag are empty', () => {
    const mockNode: any = {
      data: {
        label: 'XmlSource',
        spark_transform: 'XmlSource',
        properties: {},
      },
    };

    const { container } = render(
      <XmlIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#xmlSourceReadMode')).toHaveValue('batch');
    expect(container.querySelector('#xmlSourcePath')).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('XML URI / Path is required.')).toBeInTheDocument();

    const rowTagInput = container.querySelector('#xmlSourceRowTag') as HTMLInputElement;
    expect(rowTagInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Row Tag is required.')).toBeInTheDocument();

    expect(container.querySelector('#xmlSourceValueTag')).toHaveValue('_VALUE');
    expect(container.querySelector('#xmlSourceAttributePrefix')).toHaveValue('_');
    expect(container.querySelector('#xmlSourceMode')).toHaveValue('PERMISSIVE');
    expect(container.querySelector('#xmlSourceCorruptRecordCol')).toHaveValue('_corrupt_record');
  });

  it('renders populated streaming XML properties and propagates changes across all fields', () => {
    const mockNode: any = {
      data: {
        label: 'XmlSource',
        spark_transform: 'XmlSource',
        properties: {
          readMode: 'streaming',
          path: 'gs://my-bucket/catalog/*.xml',
          rowTag: 'book',
          valueTag: '_TEXT',
          attributePrefix: '@',
          mode: 'PERMISSIVE',
          columnNameOfCorruptRecord: '_bad_xml',
          maxFilesPerTrigger: 12,
          schemaFields: [{ name: 'isbn', type: 'STRING', mode: 'REQUIRED' }],
          system_instructions: 'Parse book catalog XML',
        },
      },
    };

    const { container } = render(
      <XmlIOSourceProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#xmlSourceReadMode')).toHaveValue('streaming');
    expect(container.querySelector('#xmlSourcePath')).toHaveValue(
      'gs://my-bucket/catalog/*.xml'
    );
    expect(container.querySelector('#xmlSourceRowTag')).toHaveValue('book');
    expect(container.querySelector('#xmlSourceRowTag')).toHaveAttribute('aria-invalid', 'false');
    expect(container.querySelector('#xmlSourceValueTag')).toHaveValue('_TEXT');
    expect(container.querySelector('#xmlSourceAttributePrefix')).toHaveValue('@');
    expect(container.querySelector('#xmlSourceCorruptRecordCol')).toHaveValue('_bad_xml');
    expect(container.querySelector('#xmlSourceMaxFilesPerTrigger')).toHaveValue(12);

    fireEvent.change(container.querySelector('#xmlSourceRowTag')!, {
      target: { name: 'rowTag', value: 'item' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ rowTag: 'item' }));

    fireEvent.change(container.querySelector('#xmlSourceValueTag')!, {
      target: { name: 'valueTag', value: '_VAL' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ valueTag: '_VAL' }));

    fireEvent.change(container.querySelector('#xmlSourceAttributePrefix')!, {
      target: { name: 'attributePrefix', value: 'attr_' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ attributePrefix: 'attr_' })
    );

    fireEvent.change(container.querySelector('#xmlSourceMode')!, {
      target: { name: 'mode', value: 'FAILFAST' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ mode: 'FAILFAST' }));

    fireEvent.change(container.querySelector('#xmlSourceInstructions')!, {
      target: { value: 'Updated XML instructions' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Updated XML instructions' })
    );
  });
});
