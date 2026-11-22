/**
 * @fileoverview Unit tests for the TextSink / TextIO sink properties panel (SNK16).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates default rendering, required URI validation, StorageBackendFields multi-cloud
 * and Secret Manager integration, saveMode, compression codecs, partitionBy, maxRecordsPerFile,
 * single-string-column projection strategies (column, concat_ws, to_json), lineSep, and
 * SystemInstructionsField updates.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { VsCodeApi } from '../../bridge/vscodeApi';
import { TextIOSinkProperties, TextSinkProperties, type TextIOSinkNode } from './TextIO';

describe('TextIOSinkProperties (SNK16 TextSink)', () => {
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

  it('exports TextSinkProperties as an alias for TextIOSinkProperties', () => {
    expect(TextSinkProperties).toBe(TextIOSinkProperties);
  });

  it('renders defaults, omits StreamingSinkSettingsField and NodeDocsCard, and flags empty path', () => {
    const mockNode: TextIOSinkNode = {
      data: {
        label: 'TextSink',
        type: 'SINK',
        spark_transform: 'TextSink',
        executionMode: 'streaming',
        properties: {},
      },
    };

    const { container } = render(
      <TextIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    const pathInput = container.querySelector('#textSinkPath') as HTMLInputElement;
    expect(pathInput).toHaveValue('');
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Output Text URI / Path is required.')).toBeInTheDocument();

    expect(container.querySelector('#textSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#textSinkCompression')).toHaveValue('none');
    expect(container.querySelector('#textSinkSerializationStrategy')).toHaveValue('column');
    expect(container.querySelector('#textSinkValueColumn')).toHaveValue('value');
    expect(screen.queryByText('Streaming Sink Settings')).not.toBeInTheDocument();
    expect(screen.queryByText('Docs ↗')).not.toBeInTheDocument();
  });

  it('renders populated properties including concat_ws strategy and local filesystem path', () => {
    const mockNode: TextIOSinkNode = {
      data: {
        label: 'TextSink',
        spark_transform: 'TextSink',
        properties: {
          path: 'file:///var/log/spark/exported_lines/',
          saveMode: 'overwrite',
          compression: 'GZIP',
          partitionBy: ['log_date'],
          maxRecordsPerFile: 100000,
          serializationStrategy: 'concat_ws',
          concatDelimiter: ' | ',
          lineSep: '\\r\\n',
          system_instructions: 'Format log lines cleanly',
        },
      },
    };

    const { container } = render(
      <TextIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#textSinkStorageBackend')).toHaveValue('local');
    expect(container.querySelector('#textSinkPath')).toHaveValue(
      'file:///var/log/spark/exported_lines/'
    );
    expect(container.querySelector('#textSinkSaveMode')).toHaveValue('overwrite');
    expect(container.querySelector('#textSinkCompression')).toHaveValue('gzip');
    expect(container.querySelector('#textSinkPartitionBy')).toHaveValue('log_date');
    expect(container.querySelector('#textSinkMaxRecordsPerFile')).toHaveValue(100000);
    expect(container.querySelector('#textSinkSerializationStrategy')).toHaveValue('concat_ws');
    expect(container.querySelector('#textSinkConcatDelimiter')).toHaveValue(' | ');
    expect(container.querySelector('#textSinkLineSep')).toHaveValue('\\r\\n');
    expect(container.querySelector('#textSinkInstructions')).toHaveValue(
      'Format log lines cleanly'
    );
  });

  it('propagates changes across all Text sink fields and system_instructions', () => {
    const mockNode: TextIOSinkNode = {
      data: {
        properties: {
          path: 'gs://my-bucket/logs/',
        },
      },
    };

    const { container } = render(
      <TextIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    fireEvent.change(container.querySelector('#textSinkPath')!, {
      target: { name: 'path', value: 'gs://my-bucket/logs_v2/' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ path: 'gs://my-bucket/logs_v2/' })
    );

    fireEvent.change(container.querySelector('#textSinkSaveMode')!, {
      target: { name: 'saveMode', value: 'errorifexists' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ saveMode: 'errorifexists' })
    );

    fireEvent.change(container.querySelector('#textSinkCompression')!, {
      target: { name: 'compression', value: 'zstd' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ compression: 'zstd' }));

    fireEvent.change(container.querySelector('#textSinkPartitionBy')!, {
      target: { name: 'partitionBy', value: 'service, log_date' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionBy: 'service, log_date' })
    );

    fireEvent.change(container.querySelector('#textSinkMaxRecordsPerFile')!, {
      target: { name: 'maxRecordsPerFile', value: '50000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxRecordsPerFile: '50000' })
    );

    fireEvent.change(container.querySelector('#textSinkValueColumn')!, {
      target: { name: 'valueColumn', value: 'raw_line' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ valueColumn: 'raw_line' })
    );

    fireEvent.change(container.querySelector('#textSinkSerializationStrategy')!, {
      target: { name: 'serializationStrategy', value: 'to_json' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ serializationStrategy: 'to_json' })
    );

    fireEvent.change(container.querySelector('#textSinkLineSep')!, {
      target: { name: 'lineSep', value: '\\n' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ lineSep: '\\n' }));

    fireEvent.change(container.querySelector('#textSinkInstructions')!, {
      target: { value: 'Project message column' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Project message column' })
    );
  });
});
