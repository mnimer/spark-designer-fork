/**
 * @fileoverview Unit tests for the TFRecordSink / TFRecordIO sink properties panel (SNK18).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates default rendering, required URI validation, StorageBackendFields multi-cloud
 * and Secret Manager integration, saveMode, compression codecs, partitionBy,
 * maxRecordsPerFile, recordType, writeLocality, and SystemInstructionsField updates.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { VsCodeApi } from '../../bridge/vscodeApi';
import {
  TFRecordIOSinkProperties,
  TFRecordSinkProperties,
  type TFRecordIOSinkNode,
} from './TFRecordIO';

describe('TFRecordIOSinkProperties (SNK18 TFRecordSink)', () => {
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

  it('exports TFRecordSinkProperties as an alias for TFRecordIOSinkProperties', () => {
    expect(TFRecordSinkProperties).toBe(TFRecordIOSinkProperties);
  });

  it('renders defaults, omits StreamingSinkSettingsField and NodeDocsCard, and flags empty path', () => {
    const mockNode: TFRecordIOSinkNode = {
      data: {
        label: 'TFRecordSink',
        type: 'SINK',
        spark_transform: 'TFRecordSink',
        executionMode: 'streaming',
        properties: {},
      },
    };

    const { container } = render(
      <TFRecordIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    const pathInput = container.querySelector('#tfrecordSinkPath') as HTMLInputElement;
    expect(pathInput).toHaveValue('');
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Output TFRecord URI / Path is required.')).toBeInTheDocument();

    expect(container.querySelector('#tfrecordSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#tfrecordSinkCompression')).toHaveValue('none');
    expect(container.querySelector('#tfrecordSinkRecordType')).toHaveValue('Example');
    expect(container.querySelector('#tfrecordSinkWriteLocality')).toHaveValue('distributed');
    expect(screen.queryByText('Streaming Sink Settings')).not.toBeInTheDocument();
    expect(screen.queryByText('Docs ↗')).not.toBeInTheDocument();
  });

  it('renders populated properties including legacy compressionType normalization and SequenceExample', () => {
    const mockNode: TFRecordIOSinkNode = {
      data: {
        label: 'TFRecordSink',
        spark_transform: 'TFRecordSink',
        properties: {
          path: 'gs://vertex-ai-datasets/seq_examples/',
          saveMode: 'overwrite',
          compressionType: 'GZIP',
          partitionBy: ['split'],
          maxRecordsPerFile: 50000,
          recordType: 'SequenceExample',
          writeLocality: 'local',
          system_instructions: 'Convert VectorUDT with vector_to_array first',
        },
      },
    };

    const { container } = render(
      <TFRecordIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#tfrecordSinkStorageBackend')).toHaveValue('gcs');
    expect(container.querySelector('#tfrecordSinkPath')).toHaveValue(
      'gs://vertex-ai-datasets/seq_examples/'
    );
    expect(container.querySelector('#tfrecordSinkSaveMode')).toHaveValue('overwrite');
    expect(container.querySelector('#tfrecordSinkCompression')).toHaveValue('gzip');
    expect(container.querySelector('#tfrecordSinkPartitionBy')).toHaveValue('split');
    expect(container.querySelector('#tfrecordSinkMaxRecordsPerFile')).toHaveValue(50000);
    expect(container.querySelector('#tfrecordSinkRecordType')).toHaveValue('SequenceExample');
    expect(container.querySelector('#tfrecordSinkWriteLocality')).toHaveValue('local');
    expect(container.querySelector('#tfrecordSinkInstructions')).toHaveValue(
      'Convert VectorUDT with vector_to_array first'
    );
  });

  it('propagates changes across all TFRecord sink fields and system_instructions', () => {
    const mockNode: TFRecordIOSinkNode = {
      data: {
        properties: {
          path: 'gs://my-bucket/tfrecords/',
        },
      },
    };

    const { container } = render(
      <TFRecordIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    fireEvent.change(container.querySelector('#tfrecordSinkPath')!, {
      target: { name: 'path', value: 'gs://my-bucket/tfrecords_v2/' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ path: 'gs://my-bucket/tfrecords_v2/' })
    );

    fireEvent.change(container.querySelector('#tfrecordSinkSaveMode')!, {
      target: { name: 'saveMode', value: 'errorifexists' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ saveMode: 'errorifexists' })
    );

    fireEvent.change(container.querySelector('#tfrecordSinkCompression')!, {
      target: { name: 'compression', value: 'deflate' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ compression: 'deflate' })
    );

    fireEvent.change(container.querySelector('#tfrecordSinkPartitionBy')!, {
      target: { name: 'partitionBy', value: 'split, dt' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionBy: 'split, dt' })
    );

    fireEvent.change(container.querySelector('#tfrecordSinkMaxRecordsPerFile')!, {
      target: { name: 'maxRecordsPerFile', value: '25000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxRecordsPerFile: '25000' })
    );

    fireEvent.change(container.querySelector('#tfrecordSinkRecordType')!, {
      target: { name: 'recordType', value: 'SequenceExample' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ recordType: 'SequenceExample' })
    );

    fireEvent.change(container.querySelector('#tfrecordSinkWriteLocality')!, {
      target: { name: 'writeLocality', value: 'local' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ writeLocality: 'local' })
    );

    fireEvent.change(container.querySelector('#tfrecordSinkInstructions')!, {
      target: { value: 'Shuffle rows evenly across shards' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Shuffle rows evenly across shards' })
    );
  });
});
