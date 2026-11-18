/**
 * @fileoverview Unit tests for the OrcSink sink properties panel (SNK12).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates default rendering, required URI validation, StorageBackendFields multi-cloud
 * and Secret Manager integration, saveMode, compression codecs, partitionBy,
 * maxRecordsPerFile, bloomFilterColumns, bloomFilterFpp, and SystemInstructionsField.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { OrcSinkProperties, OrcIOSinkProperties } from './OrcSink';

describe('OrcSinkProperties (SNK12 OrcSink)', () => {
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

  it('exports OrcIOSinkProperties as an alias for OrcSinkProperties', () => {
    expect(OrcIOSinkProperties).toBe(OrcSinkProperties);
  });

  it('renders defaults, omits StreamingSinkSettingsField and NodeDocsCard, and flags empty path', () => {
    const mockNode: any = {
      data: {
        label: 'OrcSink',
        type: 'SINK',
        spark_transform: 'OrcSink',
        executionMode: 'streaming',
        properties: {},
      },
    };

    const { container } = render(<OrcSinkProperties node={mockNode} onChange={mockOnChange} />);

    const pathInput = container.querySelector('#orcSinkPath') as HTMLInputElement;
    expect(pathInput).toHaveValue('');
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Output ORC URI / Path is required.')).toBeInTheDocument();

    expect(container.querySelector('#orcSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#orcSinkCompression')).toHaveValue('snappy');
    expect(container.querySelector('#orcSinkBloomFilterFpp')).not.toBeInTheDocument();
    expect(screen.queryByText('Streaming Sink Settings')).not.toBeInTheDocument();
    expect(screen.queryByText('Docs ↗')).not.toBeInTheDocument();
  });

  it('renders populated ADLS Gen2 properties with bloom filter options and Secret Manager auth', () => {
    const mockNode: any = {
      data: {
        label: 'OrcSink',
        spark_transform: 'OrcSink',
        properties: {
          path: 'abfss://warehouse@acct.dfs.core.windows.net/events_orc/',
          authMode: 'secretManager',
          secretName: 'projects/my-proj/secrets/adls-key/versions/latest',
          saveMode: 'overwrite',
          compression: 'zstd',
          partitionBy: ['dt', 'region'],
          maxRecordsPerFile: 750000,
          bloomFilterColumns: 'user_id, event_id',
          bloomFilterFpp: 0.02,
          system_instructions: 'Order rows by user_id before writing',
        },
      },
    };

    const { container } = render(<OrcSinkProperties node={mockNode} onChange={mockOnChange} />);

    expect(container.querySelector('#orcSinkStorageBackend')).toHaveValue('abfss');
    expect(container.querySelector('#orcSinkPath')).toHaveValue(
      'abfss://warehouse@acct.dfs.core.windows.net/events_orc/'
    );
    expect(container.querySelector('#orcSinkSaveMode')).toHaveValue('overwrite');
    expect(container.querySelector('#orcSinkCompression')).toHaveValue('zstd');
    expect(container.querySelector('#orcSinkPartitionBy')).toHaveValue('dt, region');
    expect(container.querySelector('#orcSinkMaxRecordsPerFile')).toHaveValue(750000);
    expect(container.querySelector('#orcSinkBloomFilterColumns')).toHaveValue('user_id, event_id');
    expect(container.querySelector('#orcSinkBloomFilterFpp')).toHaveValue(0.02);
    expect(container.querySelector('#orcSinkInstructions')).toHaveValue(
      'Order rows by user_id before writing'
    );
  });

  it('propagates changes across all ORC writer fields and system_instructions', () => {
    const mockNode: any = {
      data: {
        properties: {
          path: 'gs://my-bucket/orc/',
          bloomFilterColumns: 'event_id',
        },
      },
    };

    const { container } = render(<OrcSinkProperties node={mockNode} onChange={mockOnChange} />);

    fireEvent.change(container.querySelector('#orcSinkPath')!, {
      target: { name: 'path', value: 'gs://my-bucket/orc_v2/' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ path: 'gs://my-bucket/orc_v2/' })
    );

    fireEvent.change(container.querySelector('#orcSinkSaveMode')!, {
      target: { name: 'saveMode', value: 'ignore' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ saveMode: 'ignore' }));

    fireEvent.change(container.querySelector('#orcSinkCompression')!, {
      target: { name: 'compression', value: 'zlib' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ compression: 'zlib' }));

    fireEvent.change(container.querySelector('#orcSinkPartitionBy')!, {
      target: { name: 'partitionBy', value: 'event_date' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionBy: 'event_date' })
    );

    fireEvent.change(container.querySelector('#orcSinkMaxRecordsPerFile')!, {
      target: { name: 'maxRecordsPerFile', value: '100000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxRecordsPerFile: '100000' })
    );

    fireEvent.change(container.querySelector('#orcSinkBloomFilterColumns')!, {
      target: { name: 'bloomFilterColumns', value: 'event_id, account_id' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ bloomFilterColumns: 'event_id, account_id' })
    );

    fireEvent.change(container.querySelector('#orcSinkBloomFilterFpp')!, {
      target: { name: 'bloomFilterFpp', value: '0.01' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ bloomFilterFpp: '0.01' }));

    fireEvent.change(container.querySelector('#orcSinkInstructions')!, {
      target: { value: 'Enable vectorized ORC writer' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Enable vectorized ORC writer' })
    );
  });
});
