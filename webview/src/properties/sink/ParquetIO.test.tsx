/**
 * @fileoverview Unit tests for the ParquetSink / ParquetIO sink properties panel (SNK11).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates default rendering, required URI validation, StorageBackendFields multi-cloud
 * and Secret Manager integration, saveMode, compression codec normalization, partitionBy,
 * maxRecordsPerFile, advanced rebase modes, and SystemInstructionsField updates.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { VsCodeApi } from '../../bridge/vscodeApi';
import {
  ParquetIOSinkProperties,
  ParquetSinkProperties,
  type ParquetIOSinkNode,
} from './ParquetIO';

describe('ParquetIOSinkProperties (SNK11 ParquetSink)', () => {
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

  it('exports ParquetSinkProperties as an alias for ParquetIOSinkProperties', () => {
    expect(ParquetSinkProperties).toBe(ParquetIOSinkProperties);
  });

  it('renders defaults, omits StreamingSinkSettingsField and NodeDocsCard, and flags empty path', () => {
    const mockNode: ParquetIOSinkNode = {
      data: {
        label: 'ParquetSink',
        type: 'SINK',
        spark_transform: 'ParquetSink',
        executionMode: 'streaming',
        properties: {},
      },
    };

    const { container } = render(
      <ParquetIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    const pathInput = container.querySelector('#parquetSinkPath') as HTMLInputElement;
    expect(pathInput).toHaveValue('');
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Output Parquet URI / Path is required.')).toBeInTheDocument();

    expect(container.querySelector('#parquetSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#parquetSinkCompression')).toHaveValue('snappy');
    expect(container.querySelector('#parquetSinkPathGlobFilter')).not.toBeInTheDocument();
    expect(screen.queryByText('Streaming Sink Settings')).not.toBeInTheDocument();
    expect(screen.queryByText('Docs ↗')).not.toBeInTheDocument();
  });

  it('renders populated S3A properties with Secret Manager auth and legacy codec normalization', () => {
    const mockNode: ParquetIOSinkNode = {
      data: {
        label: 'ParquetSink',
        spark_transform: 'ParquetSink',
        properties: {
          path: 's3a://analytics-lake/curated/orders/',
          authMode: 'secretManager',
          secretName: 'projects/my-proj/secrets/aws-creds/versions/latest',
          saveMode: 'overwrite',
          codec: 'ZSTD',
          partitionBy: ['event_date', 'region'],
          maxRecordsPerFile: 500000,
          datetimeRebaseMode: 'CORRECTED',
          int96RebaseMode: 'LEGACY',
          system_instructions: 'Coalesce partitions per date',
        },
      },
    };

    const { container } = render(
      <ParquetIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#parquetSinkStorageBackend')).toHaveValue('s3a');
    expect(container.querySelector('#parquetSinkPath')).toHaveValue(
      's3a://analytics-lake/curated/orders/'
    );
    expect(container.querySelector('#parquetSinkSaveMode')).toHaveValue('overwrite');
    expect(container.querySelector('#parquetSinkCompression')).toHaveValue('zstd');
    expect(container.querySelector('#parquetSinkPartitionBy')).toHaveValue('event_date, region');
    expect(container.querySelector('#parquetSinkMaxRecordsPerFile')).toHaveValue(500000);
    expect(container.querySelector('#parquetSinkInstructions')).toHaveValue(
      'Coalesce partitions per date'
    );

    fireEvent.click(screen.getByText('Advanced Parquet Write Settings'));
    expect(container.querySelector('#parquetSinkDatetimeRebaseMode')).toHaveValue('CORRECTED');
    expect(container.querySelector('#parquetSinkInt96RebaseMode')).toHaveValue('LEGACY');
  });

  it('propagates changes for path, saveMode, compression, partitionBy, maxRecordsPerFile, rebase modes, and system_instructions', () => {
    const mockNode: ParquetIOSinkNode = {
      data: {
        properties: {
          path: 'gs://my-bucket/out/',
        },
      },
    };

    const { container } = render(
      <ParquetIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    fireEvent.change(container.querySelector('#parquetSinkPath')!, {
      target: { name: 'path', value: 'gs://my-bucket/curated/' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ path: 'gs://my-bucket/curated/' })
    );

    fireEvent.change(container.querySelector('#parquetSinkSaveMode')!, {
      target: { name: 'saveMode', value: 'errorifexists' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ saveMode: 'errorifexists' })
    );

    fireEvent.change(container.querySelector('#parquetSinkCompression')!, {
      target: { name: 'compression', value: 'gzip' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ compression: 'gzip' }));

    fireEvent.change(container.querySelector('#parquetSinkPartitionBy')!, {
      target: { name: 'partitionBy', value: 'dt, country' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionBy: 'dt, country' })
    );

    fireEvent.change(container.querySelector('#parquetSinkMaxRecordsPerFile')!, {
      target: { name: 'maxRecordsPerFile', value: '250000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxRecordsPerFile: '250000' })
    );

    fireEvent.click(screen.getByText('Advanced Parquet Write Settings'));
    fireEvent.change(container.querySelector('#parquetSinkDatetimeRebaseMode')!, {
      target: { name: 'datetimeRebaseMode', value: 'CORRECTED' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ datetimeRebaseMode: 'CORRECTED' })
    );

    fireEvent.change(container.querySelector('#parquetSinkInt96RebaseMode')!, {
      target: { name: 'int96RebaseMode', value: 'LEGACY' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ int96RebaseMode: 'LEGACY' })
    );

    fireEvent.change(container.querySelector('#parquetSinkInstructions')!, {
      target: { value: 'Sort within partitions before writing' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Sort within partitions before writing' })
    );
  });
});
