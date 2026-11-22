/**
 * KinesisIO Source Properties Test Suite
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating KinesisIOSourceProperties (`KinesisSourceProperties`) component functionality,
 * required streamName validation, region, initialPosition and conditional initialTimestamp,
 * endpointUrl, maxRecordsPerFetch, SecretManagerAuthFields, and system_instructions.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { KinesisIOSourceProperties, KinesisSourceProperties } from './KinesisIO';

describe('KinesisIOSourceProperties', () => {
  it('exports KinesisSourceProperties as an alias for KinesisIOSourceProperties', () => {
    expect(KinesisSourceProperties).toBe(KinesisIOSourceProperties);
  });

  it('renders streaming-only badge, default fields, and required streamName validation', () => {
    const mockNode = {
      data: {
        label: 'Kinesis',
        properties: {},
      },
    };
    const mockOnChange = vi.fn();

    render(<KinesisIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByText('STREAMING ONLY')).toBeInTheDocument();
    const streamInput = screen.getByLabelText(/Stream Name/i);
    expect(streamInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Stream Name is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/AWS Region/i)).toHaveValue('us-east-1');
    expect(screen.getByLabelText(/Initial Position/i)).toHaveValue('latest');
    expect(screen.queryByLabelText(/Initial Timestamp/i)).not.toBeInTheDocument();
    expect(screen.getByLabelText(/Max Records Per Fetch/i)).toHaveValue(1000);
    expect(screen.getByLabelText(/Custom Endpoint URL/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Code Generation Instructions/i)).toBeInTheDocument();
  });

  it('populates fields and dispatches updates with readMode: streaming', () => {
    const mockNode = {
      data: {
        label: 'Kinesis',
        properties: {
          streamName: 'clickstream',
          region: 'us-west-2',
          initialPosition: 'trim_horizon' as const,
          maxRecordsPerFetch: 2500,
          endpointUrl: 'http://localhost:4566',
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<KinesisIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Stream Name/i)).toHaveValue('clickstream');
    expect(screen.getByLabelText(/AWS Region/i)).toHaveValue('us-west-2');
    expect(screen.getByLabelText(/Initial Position/i)).toHaveValue('trim_horizon');

    fireEvent.change(screen.getByLabelText(/Stream Name/i), {
      target: { name: 'streamName', value: 'orders-stream' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        readMode: 'streaming',
        streamName: 'orders-stream',
      })
    );

    fireEvent.change(screen.getByLabelText(/Custom Endpoint URL/i), {
      target: { name: 'endpointUrl', value: 'https://kinesis.us-west-2.amazonaws.com' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        endpointUrl: 'https://kinesis.us-west-2.amazonaws.com',
      })
    );
  });

  it('conditionally renders and validates initialTimestamp when initialPosition is at_timestamp', () => {
    const mockOnChange = vi.fn();
    render(
      <KinesisIOSourceProperties
        node={{
          data: {
            label: 'Kinesis',
            properties: {
              streamName: 'events',
              initialPosition: 'at_timestamp',
              initialTimestamp: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const tsInput = screen.getByLabelText(/Initial Timestamp/i);
    expect(tsInput).toBeInTheDocument();
    expect(tsInput).toHaveAttribute('aria-invalid', 'true');

    fireEvent.change(tsInput, { target: { name: 'initialTimestamp', value: '2026-03-01T00:00:00Z' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        initialTimestamp: '2026-03-01T00:00:00Z',
      })
    );
  });

  it('supports SecretManagerAuthFields and system_instructions', () => {
    const mockOnChange = vi.fn();
    render(
      <KinesisIOSourceProperties
        node={{
          data: {
            label: 'Kinesis',
            properties: {
              streamName: 'events',
              authMode: 'secretManager',
              secretName: 'aws-creds',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const secretInput = screen.getByLabelText(/Secret Manager Secret/i);
    expect(secretInput).toHaveValue('aws-creds');

    fireEvent.change(secretInput, { target: { name: 'secretName', value: 'new-aws-secret' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secretName: 'new-aws-secret',
      })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i), {
      target: { value: 'Decode UTF-8 JSON payload' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Decode UTF-8 JSON payload',
      })
    );
  });
});
