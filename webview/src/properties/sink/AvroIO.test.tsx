/**
 * @fileoverview Unit tests for the AvroSink / AvroIO sink properties panel (SNK13).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates default rendering, required URI validation, StorageBackendFields multi-cloud
 * and Secret Manager integration, saveMode, Avro compression codecs, partitionBy,
 * maxRecordsPerFile, recordName, recordNamespace, avroSchema, and SystemInstructionsField.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { AvroIOSinkProperties, AvroSinkProperties, SAMPLE_AVRO_WRITER_SCHEMA } from './AvroIO';

describe('AvroIOSinkProperties (SNK13 AvroSink)', () => {
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

  it('exports AvroSinkProperties as an alias for AvroIOSinkProperties', () => {
    expect(AvroSinkProperties).toBe(AvroIOSinkProperties);
  });

  it('renders defaults, omits StreamingSinkSettingsField and NodeDocsCard, and flags empty path', () => {
    const mockNode: any = {
      data: {
        label: 'AvroSink',
        type: 'SINK',
        spark_transform: 'AvroSink',
        executionMode: 'streaming',
        properties: {},
      },
    };

    const { container } = render(
      <AvroIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    const pathInput = container.querySelector('#avroSinkPath') as HTMLInputElement;
    expect(pathInput).toHaveValue('');
    expect(pathInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Output Avro URI / Path is required.')).toBeInTheDocument();

    expect(container.querySelector('#avroSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#avroSinkCompression')).toHaveValue('snappy');
    expect(screen.queryByText('Streaming Sink Settings')).not.toBeInTheDocument();
    expect(screen.queryByText('Docs ↗')).not.toBeInTheDocument();
  });

  it('renders populated properties including legacy codec normalization and S3A Secret Manager auth', () => {
    const mockNode: any = {
      data: {
        label: 'AvroSink',
        spark_transform: 'AvroSink',
        properties: {
          path: 's3a://events-archive/avro/orders/',
          authMode: 'secretManager',
          secretName: 'projects/my-proj/secrets/s3-creds/versions/latest',
          saveMode: 'overwrite',
          codec: 'ZSTANDARD',
          partitionBy: ['dt'],
          maxRecordsPerFile: 250000,
          recordName: 'OrderEvent',
          recordNamespace: 'com.acme.orders',
          avroSchema: SAMPLE_AVRO_WRITER_SCHEMA,
          system_instructions: 'Write evolved Avro records',
        },
      },
    };

    const { container } = render(
      <AvroIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    expect(container.querySelector('#avroSinkStorageBackend')).toHaveValue('s3a');
    expect(container.querySelector('#avroSinkPath')).toHaveValue(
      's3a://events-archive/avro/orders/'
    );
    expect(container.querySelector('#avroSinkSaveMode')).toHaveValue('overwrite');
    expect(container.querySelector('#avroSinkCompression')).toHaveValue('zstandard');
    expect(container.querySelector('#avroSinkPartitionBy')).toHaveValue('dt');
    expect(container.querySelector('#avroSinkMaxRecordsPerFile')).toHaveValue(250000);
    expect(container.querySelector('#avroSinkRecordName')).toHaveValue('OrderEvent');
    expect(container.querySelector('#avroSinkRecordNamespace')).toHaveValue('com.acme.orders');
    expect(container.querySelector('#avroSinkAvroSchema')).toHaveValue(SAMPLE_AVRO_WRITER_SCHEMA);
    expect(container.querySelector('#avroSinkInstructions')).toHaveValue(
      'Write evolved Avro records'
    );
  });

  it('propagates changes for all Avro sink fields and system_instructions', () => {
    const mockNode: any = {
      data: {
        properties: {
          path: 'gs://my-bucket/avro/',
        },
      },
    };

    const { container } = render(
      <AvroIOSinkProperties node={mockNode} onChange={mockOnChange} />
    );

    fireEvent.change(container.querySelector('#avroSinkPath')!, {
      target: { name: 'path', value: 'gs://my-bucket/avro_v2/' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ path: 'gs://my-bucket/avro_v2/' })
    );

    fireEvent.change(container.querySelector('#avroSinkSaveMode')!, {
      target: { name: 'saveMode', value: 'errorifexists' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ saveMode: 'errorifexists' })
    );

    fireEvent.change(container.querySelector('#avroSinkCompression')!, {
      target: { name: 'compression', value: 'deflate' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ compression: 'deflate' })
    );

    fireEvent.change(container.querySelector('#avroSinkPartitionBy')!, {
      target: { name: 'partitionBy', value: 'event_date, region' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionBy: 'event_date, region' })
    );

    fireEvent.change(container.querySelector('#avroSinkMaxRecordsPerFile')!, {
      target: { name: 'maxRecordsPerFile', value: '100000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ maxRecordsPerFile: '100000' })
    );

    fireEvent.change(container.querySelector('#avroSinkRecordName')!, {
      target: { name: 'recordName', value: 'CustomerRecord' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ recordName: 'CustomerRecord' })
    );

    fireEvent.change(container.querySelector('#avroSinkRecordNamespace')!, {
      target: { name: 'recordNamespace', value: 'com.company.crm' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ recordNamespace: 'com.company.crm' })
    );

    fireEvent.change(container.querySelector('#avroSinkAvroSchema')!, {
      target: { name: 'avroSchema', value: SAMPLE_AVRO_WRITER_SCHEMA },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ avroSchema: SAMPLE_AVRO_WRITER_SCHEMA })
    );

    fireEvent.change(container.querySelector('#avroSinkInstructions')!, {
      target: { value: 'Partition by date' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Partition by date' })
    );
  });
});
