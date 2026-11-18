/**
 * @fileoverview Unit tests for KinesisIOSinkProperties (`KinesisSinkProperties`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x KinesisSink configuration fields, PutRecords 500-record batch bounds,
 * FailedRecordCount retry settings, partition key strategies, Secret Manager auth, and cleartext key stripping.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { KinesisIOSinkProperties, KinesisSinkProperties } from './KinesisIO';

describe('KinesisIOSinkProperties', () => {
  it('exports KinesisSinkProperties as an alias for KinesisIOSinkProperties', () => {
    expect(KinesisSinkProperties).toBe(KinesisIOSinkProperties);
  });

  it('renders Spark 4.x KinesisSink fields and omits legacy Beam expansionService inputs', () => {
    render(
      <KinesisIOSinkProperties
        node={{
          data: {
            label: 'Kinesis Sink',
            properties: {
              streamName: 'telemetry-out',
              partitionKeyColumn: 'device_id',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByLabelText(/Stream Name/i)).toHaveValue('telemetry-out');
    expect(screen.getByLabelText(/AWS Region/i)).toHaveValue('us-east-1');
    expect(screen.getByLabelText(/Partition Key Strategy/i)).toHaveValue('column');
    expect(screen.getByLabelText(/Partition Key Column/i)).toHaveValue('device_id');
    expect(screen.getByLabelText(/Payload Format/i)).toHaveValue('json');
    expect(screen.getByLabelText(/Max Batch Records/i)).toHaveValue(500);
    expect(screen.getByLabelText(/FailedRecord Retries/i)).toHaveValue(5);
    expect(screen.queryByLabelText(/Cross-Language Expansion Service/i)).not.toBeInTheDocument();
  });

  it('validates empty streamName, partition key fields, and maxBatchSize exceeding 500', () => {
    const { rerender } = render(
      <KinesisIOSinkProperties
        node={{
          data: {
            label: 'Kinesis Sink',
            properties: {
              streamName: '',
              partitionKeyMode: 'column',
              partitionKeyColumn: '',
              maxBatchSize: 600,
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByText('Stream Name is required.')).toBeInTheDocument();
    expect(screen.getByText('Partition Key Column is required.')).toBeInTheDocument();
    expect(screen.getByText('Batch size must be between 1 and 500.')).toBeInTheDocument();

    rerender(
      <KinesisIOSinkProperties
        node={{
          data: {
            label: 'Kinesis Sink',
            properties: {
              streamName: 'orders-stream',
              partitionKeyMode: 'static',
              staticPartitionKey: '',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByText('Static Partition Key is required.')).toBeInTheDocument();
  });

  it('strips legacy cleartext awsAccessKey and awsSecretKey on update and binds Secret Manager secret', () => {
    const mockOnChange = vi.fn();
    const { container } = render(
      <KinesisIOSinkProperties
        node={{
          data: {
            label: 'Kinesis Sink',
            properties: {
              streamName: 'events',
              partitionKeyMode: 'hash',
              authMode: 'secretManager',
              awsAccessKey: 'AKIA_LEGACY',
              awsSecretKey: 'SECRET_LEGACY',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('input[name="awsAccessKey"]')).not.toBeInTheDocument();
    expect(container.querySelector('input[name="awsSecretKey"]')).not.toBeInTheDocument();

    const secretInput = screen.getByLabelText(/Secret Manager Secret/i);
    fireEvent.change(secretInput, { target: { name: 'secretName', value: 'aws-kinesis-prod' } });

    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secretName: 'aws-kinesis-prod',
      })
    );
    expect(mockOnChange.mock.calls[0][0].awsAccessKey).toBeUndefined();
    expect(mockOnChange.mock.calls[0][0].awsSecretKey).toBeUndefined();
  });

  it('opens Advanced Kinesis Options and updates roleArn, endpointUrl, and system instructions', () => {
    const mockOnChange = vi.fn();
    render(
      <KinesisIOSinkProperties
        node={{
          data: {
            label: 'Kinesis Sink',
            properties: {
              streamName: 'events',
              partitionKeyMode: 'hash',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.click(screen.getByText('Advanced Kinesis Options'));

    fireEvent.change(screen.getByLabelText(/Assume Role ARN/i), {
      target: { name: 'roleArn', value: 'arn:aws:iam::123456789012:role/SinkRole' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ roleArn: 'arn:aws:iam::123456789012:role/SinkRole' })
    );

    fireEvent.change(screen.getByLabelText(/Custom Endpoint URL/i), {
      target: { name: 'endpointUrl', value: 'http://localhost:4566' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ endpointUrl: 'http://localhost:4566' })
    );

    fireEvent.change(
      screen.getByPlaceholderText('Enter custom instructions for Kinesis sink code generation...'),
      { target: { value: 'Log throttled shard counts' } }
    );
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Log throttled shard counts' })
    );
  });
});
