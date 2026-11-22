/**
 * @fileoverview Unit tests for the JsonSink / JsonIO sink properties panel (SNK15).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates default rendering, required URI validation, StorageBackendFields multi-cloud
 * and Secret Manager integration, saveMode, compression codecs, partitionBy, maxRecordsPerFile,
 * ignoreNullFields, timestampFormat, dateFormat, lineSep, and SystemInstructionsField updates.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { VsCodeApi } from '../../bridge/vscodeApi';
import { JsonIOSinkProperties, JsonSinkProperties, type JsonIOSinkNode } from './JsonIO';

describe('JsonIOSinkProperties (SNK15 JsonSink)', () => {
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

  it('exports JsonSinkProperties as an alias for JsonIOSinkProperties', () => {
    expect(JsonSinkProperties).toBe(JsonIOSinkProperties);
  });

  it('renders defaults, omits StreamingSinkSettingsField and NodeDocsCard, and flags empty path', () => {
    const mockNode: JsonIOSinkNode = {
      data: {
        label: 'JsonSink',
        type: 'SINK',
        spark_transform: 'JsonSink',
        executionMode: 'streaming',
        properties: {},
      },
    };

    const { container } = render(
      <JsonIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    const pathInput = container.querySelector('#jsonSinkPath') as HTMLInputElement;
    expect(pathInput).toHaveValue('');
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Output JSON URI / Path is required.')).toBeInTheDocument();

    expect(container.querySelector('#jsonSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#jsonSinkCompression')).toHaveValue('none');
    expect(container.querySelector('#jsonSinkIgnoreNullFields')).toBeChecked();
    expect(screen.queryByText('Streaming Sink Settings')).not.toBeInTheDocument();
    expect(screen.queryByText('Docs ↗')).not.toBeInTheDocument();
  });

  it('renders populated HDFS and formatting properties with legacy compression normalization', () => {
    const mockNode: JsonIOSinkNode = {
      data: {
        label: 'JsonSink',
        spark_transform: 'JsonSink',
        properties: {
          path: 'hdfs://namenode:8020/data/events_json/',
          saveMode: 'overwrite',
          compression: 'ZSTD',
          partitionBy: ['event_date'],
          maxRecordsPerFile: 150000,
          ignoreNullFields: false,
          timestampFormat: "yyyy-MM-dd'T'HH:mm:ss'Z'",
          dateFormat: 'yyyy/MM/dd',
          lineSep: '\\r\\n',
          system_instructions: 'Emit explicit nulls for audit feed',
        },
      },
    };

    const { container } = render(
      <JsonIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#jsonSinkStorageBackend')).toHaveValue('hdfs');
    expect(container.querySelector('#jsonSinkPath')).toHaveValue(
      'hdfs://namenode:8020/data/events_json/'
    );
    expect(container.querySelector('#jsonSinkSaveMode')).toHaveValue('overwrite');
    expect(container.querySelector('#jsonSinkCompression')).toHaveValue('zstd');
    expect(container.querySelector('#jsonSinkPartitionBy')).toHaveValue('event_date');
    expect(container.querySelector('#jsonSinkMaxRecordsPerFile')).toHaveValue(150000);
    expect(container.querySelector('#jsonSinkIgnoreNullFields')).not.toBeChecked();
    expect(container.querySelector('#jsonSinkTimestampFormat')).toHaveValue(
      "yyyy-MM-dd'T'HH:mm:ss'Z'"
    );
    expect(container.querySelector('#jsonSinkDateFormat')).toHaveValue('yyyy/MM/dd');
    expect(container.querySelector('#jsonSinkLineSep')).toHaveValue('\\r\\n');
    expect(container.querySelector('#jsonSinkInstructions')).toHaveValue(
      'Emit explicit nulls for audit feed'
    );
  });

  it('propagates changes across all JSON sink fields and system_instructions', () => {
    const mockNode: JsonIOSinkNode = {
      data: {
        properties: {
          path: 'gs://my-bucket/json/',
        },
      },
    };

    const { container } = render(
      <JsonIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    fireEvent.change(container.querySelector('#jsonSinkPath')!, {
      target: { name: 'path', value: 'gs://my-bucket/json_v2/' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ path: 'gs://my-bucket/json_v2/' })
    );

    fireEvent.change(container.querySelector('#jsonSinkSaveMode')!, {
      target: { name: 'saveMode', value: 'ignore' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ saveMode: 'ignore' }));

    fireEvent.change(container.querySelector('#jsonSinkCompression')!, {
      target: { name: 'compression', value: 'gzip' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ compression: 'gzip' }));

    fireEvent.change(container.querySelector('#jsonSinkPartitionBy')!, {
      target: { name: 'partitionBy', value: 'dt, source' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionBy: 'dt, source' })
    );

    fireEvent.change(container.querySelector('#jsonSinkMaxRecordsPerFile')!, {
      target: { name: 'maxRecordsPerFile', value: '80000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxRecordsPerFile: '80000' })
    );

    fireEvent.click(container.querySelector('#jsonSinkIgnoreNullFields')!);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ ignoreNullFields: false })
    );

    fireEvent.change(container.querySelector('#jsonSinkTimestampFormat')!, {
      target: { name: 'timestampFormat', value: 'yyyy-MM-dd HH:mm:ss' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ timestampFormat: 'yyyy-MM-dd HH:mm:ss' })
    );

    fireEvent.change(container.querySelector('#jsonSinkDateFormat')!, {
      target: { name: 'dateFormat', value: 'yyyyMMdd' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ dateFormat: 'yyyyMMdd' }));

    fireEvent.change(container.querySelector('#jsonSinkLineSep')!, {
      target: { name: 'lineSep', value: '\\n' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ lineSep: '\\n' }));

    fireEvent.change(container.querySelector('#jsonSinkInstructions')!, {
      target: { value: 'Compress output with gzip' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Compress output with gzip' })
    );
  });
});
