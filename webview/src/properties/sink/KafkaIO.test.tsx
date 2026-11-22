/**
 * @fileoverview Unit tests for KafkaIOSinkProperties (`KafkaSinkProperties`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x KafkaSink configuration fields, envelope column mapping,
 * producer reliability controls, SASL security via Secret Manager, and validation errors.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { KafkaIOSinkProperties, KafkaSinkProperties } from './KafkaIO';

describe('KafkaIOSinkProperties', () => {
  it('exports KafkaSinkProperties as an alias for KafkaIOSinkProperties', () => {
    expect(KafkaSinkProperties).toBe(KafkaIOSinkProperties);
  });

  it('renders Spark 4.x default sink fields and omits legacy Beam serializer classes', () => {
    const mockNode = {
      data: {
        label: 'Kafka Sink',
        properties: {},
      },
    };
    const mockOnChange = vi.fn();

    render(<KafkaIOSinkProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Bootstrap Servers/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/^Topic/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Value Serialization/i)).toHaveValue('json');
    expect(screen.getByLabelText(/Key Column/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Value Column/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Acks \(Durability\)/i)).toHaveValue('all');
    expect(screen.getByLabelText(/Compression Codec/i)).toHaveValue('none');
    expect(screen.getByLabelText(/Enable Idempotent Producer/i)).toBeChecked();
    expect(screen.getByLabelText(/Include Kafka Headers/i)).not.toBeChecked();
    expect(screen.getByLabelText(/Security Protocol/i)).toHaveValue('PLAINTEXT');
    expect(screen.getByLabelText(/Code Generation Instructions/i)).toBeInTheDocument();

    expect(screen.queryByText(/Exactly-Once Semantics \(EOS\)/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/Key Serializer/i)).not.toBeInTheDocument();
  });

  it('displays required validation errors when bootstrapServers and topic are empty', () => {
    const mockNode = {
      data: {
        label: 'Kafka Sink',
        properties: {
          bootstrapServers: '',
          topic: '',
        },
      },
    };

    render(<KafkaIOSinkProperties node={mockNode} onChange={vi.fn()} />);

    expect(screen.getByText('Bootstrap Servers is required.')).toBeInTheDocument();
    expect(screen.getByText('Topic is required.')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('localhost:9092')).toHaveAttribute('aria-invalid', 'true');
  });

  it('requires valueColumn when valueFormat is string or binary and clears topic error when topicColumn is set', () => {
    const mockOnChange = vi.fn();
    render(
      <KafkaIOSinkProperties
        node={{
          data: {
            label: 'Kafka Sink',
            properties: {
              bootstrapServers: 'kafka.internal:9092',
              topic: '',
              topicColumn: 'route_topic',
              valueFormat: 'string',
              valueColumn: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.queryByText('Topic is required.')).not.toBeInTheDocument();
    expect(screen.getByText('Value Column is required.')).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Value Column/i), {
      target: { name: 'valueColumn', value: 'payload_json' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        valueColumn: 'payload_json',
      })
    );
  });

  it('updates headers, compression, SASL security options, Secret Manager secret, and system instructions', () => {
    const mockOnChange = vi.fn();
    render(
      <KafkaIOSinkProperties
        node={{
          data: {
            label: 'Kafka Sink',
            properties: {
              bootstrapServers: 'kafka.prod:9093',
              topic: 'orders-out',
              includeHeaders: true,
              securityProtocol: 'SASL_SSL',
              authMode: 'secretManager',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const headersInput = screen.getByLabelText(/Headers Column/i);
    fireEvent.change(headersInput, { target: { name: 'headersColumn', value: 'record_headers' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ headersColumn: 'record_headers' })
    );

    const compressionSelect = screen.getByLabelText(/Compression Codec/i);
    fireEvent.change(compressionSelect, { target: { name: 'compressionType', value: 'zstd' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ compressionType: 'zstd' })
    );

    const saslSelect = screen.getByLabelText(/SASL Mechanism/i);
    fireEvent.change(saslSelect, { target: { name: 'saslMechanism', value: 'SCRAM-SHA-512' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ saslMechanism: 'SCRAM-SHA-512' })
    );

    const secretInput = screen.getByLabelText(/Secret Manager Secret/i);
    fireEvent.change(secretInput, { target: { name: 'secretName', value: 'kafka-prod-secret' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ secretName: 'kafka-prod-secret' })
    );

    const instructions = screen.getByPlaceholderText(
      'Enter custom instructions for Kafka sink code generation...'
    );
    fireEvent.change(instructions, { target: { value: 'Add event_type header' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Add event_type header' })
    );
  });

  it('opens Advanced Kafka Producer Options and updates dynamic topic, partition, and producer properties', () => {
    const mockOnChange = vi.fn();
    const { container } = render(
      <KafkaIOSinkProperties
        node={{
          data: {
            label: 'Kafka Sink',
            properties: {
              bootstrapServers: 'localhost:9092',
              topic: 'events',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.click(screen.getByText('Advanced Kafka Producer Options'));

    const partitionInput = screen.getByLabelText(/Partition Column/i);
    fireEvent.change(partitionInput, { target: { name: 'partitionColumn', value: 'part_num' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionColumn: 'part_num' })
    );

    const producerTextarea = container.querySelector('#kafkaProducerProperties') as HTMLTextAreaElement;
    fireEvent.change(producerTextarea, {
      target: { name: 'producerProperties', value: 'kafka.linger.ms=10' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ producerProperties: 'kafka.linger.ms=10' })
    );
  });
});
