/**
 * @fileoverview Unit tests for EventHubsSinkProperties (`EventHubsIOSinkProperties`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x Azure Event Hubs sink configuration fields, namespace and hub validation,
 * partition key and payload serialization, SASL_SSL Secret Manager auth, and producer options.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { EventHubsSinkProperties, EventHubsIOSinkProperties } from './EventHubsSink';

describe('EventHubsSinkProperties', () => {
  it('exports EventHubsIOSinkProperties as an alias for EventHubsSinkProperties', () => {
    expect(EventHubsIOSinkProperties).toBe(EventHubsSinkProperties);
  });

  it('renders default Kafka-endpoint fields and Secret Manager auth for Azure Event Hubs', () => {
    render(
      <EventHubsSinkProperties
        node={{
          data: {
            label: 'Event Hubs Sink',
            properties: {},
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByLabelText(/Connector Mode/i)).toHaveValue('kafka_endpoint');
    expect(screen.getByLabelText(/Event Hubs Namespace/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Event Hub Name \(Topic\)/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Value Serialization/i)).toHaveValue('json');
    expect(screen.getByLabelText(/Partition Key Column/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Acks \(Durability\)/i)).toHaveValue('all');
    expect(screen.getByLabelText(/Request Timeout \(ms\)/i)).toHaveValue(60000);
    expect(screen.getByLabelText(/Secret Manager Secret/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Code Generation Instructions/i)).toBeInTheDocument();
  });

  it('displays required validation errors when namespace, hub name, or string valueColumn is empty', () => {
    render(
      <EventHubsSinkProperties
        node={{
          data: {
            label: 'Event Hubs Sink',
            properties: {
              eventHubNamespace: '',
              eventHubName: '',
              valueFormat: 'string',
              valueColumn: '',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByText('Event Hubs Namespace is required.')).toBeInTheDocument();
    expect(screen.getByText('Event Hub Name is required.')).toBeInTheDocument();
    expect(screen.getByText('Value Column is required.')).toBeInTheDocument();
  });

  it('updates namespace, partitionKeyColumn, headers, Secret Manager secret, and system instructions', () => {
    const mockOnChange = vi.fn();
    const { container } = render(
      <EventHubsSinkProperties
        node={{
          data: {
            label: 'Event Hubs Sink',
            properties: {
              eventHubNamespace: 'telemetry-ns',
              eventHubName: 'iot-out',
              includeHeaders: true,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Partition Key Column/i), {
      target: { name: 'partitionKeyColumn', value: 'sensor_id' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionKeyColumn: 'sensor_id' })
    );

    fireEvent.change(screen.getByLabelText(/Headers Column/i), {
      target: { name: 'headersColumn', value: 'eh_props' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ headersColumn: 'eh_props' })
    );

    fireEvent.change(screen.getByLabelText(/Secret Manager Secret/i), {
      target: { name: 'secretName', value: 'eh-conn-secret' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ secretName: 'eh-conn-secret' })
    );

    fireEvent.click(screen.getByText('Advanced Event Hubs Options'));
    const textarea = container.querySelector(
      '#eventHubsSinkProducerProperties'
    ) as HTMLTextAreaElement;
    fireEvent.change(textarea, {
      target: { name: 'producerProperties', value: 'kafka.linger.ms=25' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ producerProperties: 'kafka.linger.ms=25' })
    );

    fireEvent.change(
      screen.getByPlaceholderText('Enter custom instructions for Event Hubs sink code generation...'),
      { target: { value: 'Attach correlation_id header' } }
    );
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Attach correlation_id header' })
    );
  });
});
