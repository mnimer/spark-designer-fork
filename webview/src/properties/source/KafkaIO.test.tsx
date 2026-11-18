/**
 * KafkaIO Source Properties Test Suite
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating KafkaIOSourceProperties (`KafkaSourceProperties`) component functionality,
 * readMode switching, subscriptionMode conditional inputs, starting/ending offsets,
 * trigger/partition limits, security and Secret Manager integration, and system instructions.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { KafkaIOSourceProperties, KafkaSourceProperties } from './KafkaIO';

describe('KafkaIOSourceProperties', () => {
  it('exports KafkaSourceProperties as an alias for KafkaIOSourceProperties', () => {
    expect(KafkaSourceProperties).toBe(KafkaIOSourceProperties);
  });

  it('renders default streaming fields with documentation tooltips and placeholders', () => {
    const mockNode = {
      data: {
        label: 'Kafka',
        properties: {},
      },
    };
    const mockOnChange = vi.fn();

    render(<KafkaIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Read Mode/i)).toHaveValue('streaming');
    expect(screen.getByLabelText(/Bootstrap Servers/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Subscription Mode/i)).toHaveValue('subscribe');
    expect(screen.getByLabelText(/Topic\(s\)/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Starting Offsets/i)).toHaveValue('latest');
    expect(screen.queryByLabelText(/Ending Offsets/i)).not.toBeInTheDocument();
    expect(screen.getByLabelText(/Max Offsets Per Trigger/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Min Partitions/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Fail on Data Loss/i)).toBeChecked();
    expect(screen.getByLabelText(/Include Kafka Headers/i)).not.toBeChecked();
    expect(screen.getByLabelText(/Security Protocol/i)).toHaveValue('PLAINTEXT');
    expect(screen.getByLabelText(/Code Generation Instructions/i)).toBeInTheDocument();
  });

  it('renders endingOffsets when readMode is batch', () => {
    const mockNode = {
      data: {
        label: 'Kafka',
        properties: {
          readMode: 'batch' as const,
          bootstrapServers: 'kafka.prod:9092',
          topics: 'orders-topic',
          startingOffsets: 'earliest',
          endingOffsets: 'latest',
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<KafkaIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Read Mode/i)).toHaveValue('batch');
    const endingInput = screen.getByLabelText(/Ending Offsets/i);
    expect(endingInput).toBeInTheDocument();
    expect(endingInput).toHaveValue('latest');

    fireEvent.change(endingInput, { target: { name: 'endingOffsets', value: '{"orders-topic":{"0":500}}' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        endingOffsets: '{"orders-topic":{"0":500}}',
      })
    );
  });

  it('switches subscription inputs for subscribe, subscribePattern, and assign modes', () => {
    const mockOnChange = vi.fn();
    const { rerender } = render(
      <KafkaIOSourceProperties
        node={{ data: { label: 'Kafka', properties: { bootstrapServers: 'localhost:9092', subscriptionMode: 'subscribePattern' } } }}
        onChange={mockOnChange}
      />
    );

    const patternInput = screen.getByLabelText(/Topic Pattern \(Regex\)/i);
    expect(patternInput).toBeInTheDocument();
    expect(patternInput).toHaveAttribute('aria-invalid', 'true');

    fireEvent.change(patternInput, { target: { name: 'topicPattern', value: 'telemetry-.*' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        topicPattern: 'telemetry-.*',
      })
    );

    rerender(
      <KafkaIOSourceProperties
        node={{ data: { label: 'Kafka', properties: { bootstrapServers: 'localhost:9092', subscriptionMode: 'assign' } } }}
        onChange={mockOnChange}
      />
    );

    const assignInput = screen.getByLabelText(/Assigned Partitions \(JSON\)/i);
    expect(assignInput).toBeInTheDocument();
    expect(assignInput).toHaveAttribute('aria-invalid', 'true');

    fireEvent.change(assignInput, { target: { name: 'assignedPartitions', value: '{"events":[0,1]}' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        assignedPartitions: '{"events":[0,1]}',
      })
    );
  });

  it('updates checkboxes, SASL security options, Secret Manager auth, and system_instructions', () => {
    const mockNode = {
      data: {
        label: 'Kafka',
        properties: {
          bootstrapServers: 'localhost:9092',
          topics: 'events',
          securityProtocol: 'SASL_SSL',
          authMode: 'secretManager',
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<KafkaIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    const saslSelect = screen.getByLabelText(/SASL Mechanism/i);
    expect(saslSelect).toBeInTheDocument();
    fireEvent.change(saslSelect, { target: { name: 'saslMechanism', value: 'SCRAM-SHA-256' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        saslMechanism: 'SCRAM-SHA-256',
      })
    );

    const secretInput = screen.getByLabelText(/Secret Manager Secret/i);
    fireEvent.change(secretInput, { target: { name: 'secretName', value: 'kafka-sasl-secret' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secretName: 'kafka-sasl-secret',
      })
    );

    const failCheckbox = screen.getByLabelText(/Fail on Data Loss/i);
    fireEvent.click(failCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        failOnDataLoss: false,
      })
    );

    const headersCheckbox = screen.getByLabelText(/Include Kafka Headers/i);
    fireEvent.click(headersCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        includeHeaders: true,
      })
    );

    const instructionsTextarea = screen.getByPlaceholderText('Enter custom instructions for Kafka source code generation...');
    fireEvent.change(instructionsTextarea, { target: { value: 'Cast binary value to string' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Cast binary value to string',
      })
    );
  });

  it('displays required validation errors when bootstrapServers and topics are empty', () => {
    const mockNode = {
      data: {
        label: 'Kafka',
        properties: {
          bootstrapServers: '',
          topics: '',
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<KafkaIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByText('Bootstrap Servers is required.')).toBeInTheDocument();
    expect(screen.getByText('Topic(s) is required.')).toBeInTheDocument();
  });

  it('toggles Advanced Kafka Options accordion and updates consumerProperties', () => {
    const mockNode = {
      data: {
        label: 'Kafka',
        properties: {},
      },
    };
    const mockOnChange = vi.fn();

    const { container } = render(<KafkaIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    fireEvent.click(screen.getByText('Advanced Kafka Options'));
    const textarea = container.querySelector('#kafkaConsumerProperties') as HTMLTextAreaElement;
    expect(textarea).toBeInTheDocument();

    fireEvent.change(textarea, { target: { name: 'consumerProperties', value: 'kafka.fetch.min.bytes=2048' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        consumerProperties: 'kafka.fetch.min.bytes=2048',
      })
    );
  });
});
