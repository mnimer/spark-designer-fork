/**
 * MqttIO Source Properties Test Suite
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating MqttIOSourceProperties (`MqttSourceProperties`) component functionality,
 * required brokerUrl and topic validation, clientId, qos (0 | 1 | 2), cleanSession checkbox,
 * keepAliveSeconds, SecretManagerAuthFields, and system_instructions.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MqttIOSourceProperties, MqttSourceProperties } from './MqttIO';

describe('MqttIOSourceProperties', () => {
  it('exports MqttSourceProperties as an alias for MqttIOSourceProperties', () => {
    expect(MqttSourceProperties).toBe(MqttIOSourceProperties);
  });

  it('renders streaming-only badge, default fields, and validates required brokerUrl and topic', () => {
    const mockNode = {
      data: {
        label: 'MQTT',
        properties: {},
      },
    };
    const mockOnChange = vi.fn();

    render(<MqttIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByText('STREAMING ONLY')).toBeInTheDocument();
    expect(screen.getByLabelText(/Broker URL/i)).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Broker URL is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Subscription Topic \/ Filter/i)).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Subscription topic is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Client ID/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Keep Alive \(s\)/i)).toHaveValue(60);
    expect(screen.getByLabelText(/^QoS/i)).toHaveValue('1');
    expect(screen.getByLabelText(/Clean Session/i)).toBeChecked();
    expect(screen.getByLabelText(/Code Generation Instructions/i)).toBeInTheDocument();
  });

  it('populates fields and dispatches updates for brokerUrl, topic, qos, cleanSession, and keepAliveSeconds', () => {
    const mockNode = {
      data: {
        label: 'MQTT',
        properties: {
          brokerUrl: 'tcp://broker.hivemq.com:1883',
          topic: 'sensors/+/temperature',
          clientId: 'spark-worker-1',
          qos: 2 as const,
          cleanSession: true,
          keepAliveSeconds: 90,
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<MqttIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Broker URL/i)).toHaveValue('tcp://broker.hivemq.com:1883');
    expect(screen.getByLabelText(/Subscription Topic \/ Filter/i)).toHaveValue('sensors/+/temperature');
    expect(screen.getByLabelText(/Client ID/i)).toHaveValue('spark-worker-1');
    expect(screen.getByLabelText(/^QoS/i)).toHaveValue('2');
    expect(screen.getByLabelText(/Keep Alive \(s\)/i)).toHaveValue(90);

    fireEvent.change(screen.getByLabelText(/Broker URL/i), {
      target: { name: 'brokerUrl', value: 'ssl://broker.hivemq.com:8883' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        readMode: 'streaming',
        brokerUrl: 'ssl://broker.hivemq.com:8883',
      })
    );

    fireEvent.change(screen.getByLabelText(/^QoS/i), {
      target: { name: 'qos', value: '0' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        qos: 0,
      })
    );

    fireEvent.click(screen.getByLabelText(/Clean Session/i));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        cleanSession: false,
      })
    );
  });

  it('supports SecretManagerAuthFields and system_instructions updates', () => {
    const mockOnChange = vi.fn();
    render(
      <MqttIOSourceProperties
        node={{
          data: {
            label: 'MQTT',
            properties: {
              brokerUrl: 'tcp://localhost:1883',
              topic: 'iot/#',
              authMode: 'secretManager',
              secretName: 'mqtt-creds',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const secretInput = screen.getByLabelText(/Secret Manager Secret/i);
    expect(secretInput).toHaveValue('mqtt-creds');

    fireEvent.change(secretInput, { target: { name: 'secretName', value: 'mqtt-prod-creds' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secretName: 'mqtt-prod-creds',
      })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i), {
      target: { value: 'Parse payload as JSON' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Parse payload as JSON',
      })
    );
  });
});
