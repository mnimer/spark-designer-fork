/**
 * @fileoverview Unit tests for the CsvSink / CsvIO sink properties panel (SNK14).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates default rendering, required URI validation, StorageBackendFields multi-cloud
 * and Secret Manager integration, saveMode, compression codecs, partitionBy, maxRecordsPerFile,
 * CSV dialect options (header, sep, quote, escape, nullValue, emptyValue, quoteAll), and
 * SystemInstructionsField updates.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { VsCodeApi } from '../../bridge/vscodeApi';
import { CsvIOSinkProperties, CsvSinkProperties, type CsvIOSinkNode } from './CsvIO';

describe('CsvIOSinkProperties (SNK14 CsvSink)', () => {
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

  it('exports CsvSinkProperties as an alias for CsvIOSinkProperties', () => {
    expect(CsvSinkProperties).toBe(CsvIOSinkProperties);
  });

  it('renders defaults, omits StreamingSinkSettingsField and NodeDocsCard, and flags empty path', () => {
    const mockNode: CsvIOSinkNode = {
      data: {
        label: 'CsvSink',
        type: 'SINK',
        spark_transform: 'CsvSink',
        executionMode: 'streaming',
        properties: {},
      },
    };

    const { container } = render(
      <CsvIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    const pathInput = container.querySelector('#csvSinkPath') as HTMLInputElement;
    expect(pathInput).toHaveValue('');
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Output CSV URI / Path is required.')).toBeInTheDocument();

    expect(container.querySelector('#csvSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#csvSinkCompression')).toHaveValue('none');
    expect(container.querySelector('#csvSinkHeader')).toBeChecked();
    expect(container.querySelector('#csvSinkQuoteAll')).not.toBeChecked();
    expect(container.querySelector('#csvSinkSep')).toHaveValue(',');
    expect(container.querySelector('#csvSinkQuote')).toHaveValue('"');
    expect(container.querySelector('#csvSinkEscape')).toHaveValue('\\');
    expect(screen.queryByText('Streaming Sink Settings')).not.toBeInTheDocument();
    expect(screen.queryByText('Docs ↗')).not.toBeInTheDocument();
  });

  it('renders populated Azure Blob properties with dialect overrides and compression normalization', () => {
    const mockNode: CsvIOSinkNode = {
      data: {
        label: 'CsvSink',
        spark_transform: 'CsvSink',
        properties: {
          path: 'wasbs://exports@acct.blob.core.windows.net/daily/',
          authMode: 'secretManager',
          secretName: 'azure-blob-secret',
          saveMode: 'overwrite',
          compression: 'GZIP',
          partitionBy: ['country', 'dt'],
          maxRecordsPerFile: 200000,
          header: false,
          quoteAll: true,
          sep: '|',
          quote: "'",
          escape: '!',
          nullValue: 'NULL',
          emptyValue: 'EMPTY',
          system_instructions: 'Coalesce to 4 files per partition',
        },
      },
    };

    const { container } = render(
      <CsvIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#csvSinkStorageBackend')).toHaveValue('wasbs');
    expect(container.querySelector('#csvSinkPath')).toHaveValue(
      'wasbs://exports@acct.blob.core.windows.net/daily/'
    );
    expect(container.querySelector('#csvSinkSaveMode')).toHaveValue('overwrite');
    expect(container.querySelector('#csvSinkCompression')).toHaveValue('gzip');
    expect(container.querySelector('#csvSinkPartitionBy')).toHaveValue('country, dt');
    expect(container.querySelector('#csvSinkMaxRecordsPerFile')).toHaveValue(200000);
    expect(container.querySelector('#csvSinkHeader')).not.toBeChecked();
    expect(container.querySelector('#csvSinkQuoteAll')).toBeChecked();
    expect(container.querySelector('#csvSinkSep')).toHaveValue('|');
    expect(container.querySelector('#csvSinkQuote')).toHaveValue("'");
    expect(container.querySelector('#csvSinkEscape')).toHaveValue('!');
    expect(container.querySelector('#csvSinkNullValue')).toHaveValue('NULL');
    expect(container.querySelector('#csvSinkEmptyValue')).toHaveValue('EMPTY');
    expect(container.querySelector('#csvSinkInstructions')).toHaveValue(
      'Coalesce to 4 files per partition'
    );
  });

  it('propagates changes across all CSV sink fields and system_instructions', () => {
    const mockNode: CsvIOSinkNode = {
      data: {
        properties: {
          path: 'gs://my-bucket/csv/',
        },
      },
    };

    const { container } = render(
      <CsvIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    fireEvent.change(container.querySelector('#csvSinkPath')!, {
      target: { name: 'path', value: 'gs://my-bucket/csv_out/' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ path: 'gs://my-bucket/csv_out/' })
    );

    fireEvent.change(container.querySelector('#csvSinkSaveMode')!, {
      target: { name: 'saveMode', value: 'overwrite' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ saveMode: 'overwrite' }));

    fireEvent.change(container.querySelector('#csvSinkCompression')!, {
      target: { name: 'compression', value: 'zstd' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ compression: 'zstd' }));

    fireEvent.change(container.querySelector('#csvSinkPartitionBy')!, {
      target: { name: 'partitionBy', value: 'region' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ partitionBy: 'region' }));

    fireEvent.change(container.querySelector('#csvSinkMaxRecordsPerFile')!, {
      target: { name: 'maxRecordsPerFile', value: '50000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxRecordsPerFile: '50000' })
    );

    fireEvent.click(container.querySelector('#csvSinkHeader')!);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ header: false }));

    fireEvent.click(container.querySelector('#csvSinkQuoteAll')!);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ quoteAll: true }));

    fireEvent.change(container.querySelector('#csvSinkSep')!, {
      target: { name: 'sep', value: '\t' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ sep: '\t' }));

    fireEvent.change(container.querySelector('#csvSinkQuote')!, {
      target: { name: 'quote', value: "'" },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ quote: "'" }));

    fireEvent.change(container.querySelector('#csvSinkEscape')!, {
      target: { name: 'escape', value: '/' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ escape: '/' }));

    fireEvent.change(container.querySelector('#csvSinkNullValue')!, {
      target: { name: 'nullValue', value: '\\N' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ nullValue: '\\N' }));

    fireEvent.change(container.querySelector('#csvSinkEmptyValue')!, {
      target: { name: 'emptyValue', value: '""' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ emptyValue: '""' }));

    fireEvent.change(container.querySelector('#csvSinkInstructions')!, {
      target: { value: 'Format decimal amounts to 2 places' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Format decimal amounts to 2 places' })
    );
  });
});
