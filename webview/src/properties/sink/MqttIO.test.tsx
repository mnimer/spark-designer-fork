/**
 * @fileoverview Unit tests for MqttIOSinkProperties (`MqttSinkProperties`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x MqttSink configuration fields, wildcard topic warnings,
 * selectable QoS levels, retain and TLS toggles, Secret Manager auth, and cleartext password stripping.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MqttIOSinkProperties, MqttSinkProperties } from './MqttIO';

describe('MqttIOSinkProperties', () => {
  it('exports MqttSinkProperties as an alias for MqttIOSinkProperties', () => {
    expect(MqttSinkProperties).toBe(MqttIOSinkProperties);
  });

  it('renders Spark 4.x MqttSink fields with selectable QoS levels', () => {
    render(
      <MqttIOSinkProperties
        node={{
          data: {
            label: 'MQTT Sink',
            properties: {
              brokerUrl: 'tcp://broker.local:1883',
              topic: 'alerts/temperature',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByLabelText(/Broker URL/i)).toHaveValue('tcp://broker.local:1883');
    expect(screen.getByLabelText(/Publish Topic/i)).toHaveValue('alerts/temperature');
    expect(screen.getByLabelText(/Payload Format/i)).toHaveValue('json');
    expect(screen.getByLabelText(/QoS \(Quality of Service\)/i)).toHaveValue('1');
    expect(screen.getByLabelText(/Client ID Prefix/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Keep Alive \(s\)/i)).toHaveValue(60);
    expect(screen.getByLabelText(/Retain Messages/i)).not.toBeChecked();
    expect(screen.getByLabelText(/Enable TLS Encryption/i)).not.toBeChecked();
  });

  it('displays required errors when brokerUrl and topic are missing and warns on wildcard topics', () => {
    const { rerender } = render(
      <MqttIOSinkProperties
        node={{
          data: {
            label: 'MQTT Sink',
            properties: {
              brokerUrl: '',
              topic: '',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByText('Broker URL is required.')).toBeInTheDocument();
    expect(screen.getByText('Publish topic is required.')).toBeInTheDocument();

    rerender(
      <MqttIOSinkProperties
        node={{
          data: {
            label: 'MQTT Sink',
            properties: {
              brokerUrl: 'ssl://broker.local:8883',
              topic: 'sensors/+/events',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByText(/Wildcard detected:/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Enable TLS Encryption/i)).toBeChecked();
  });

  it('converts qos to number, toggles retain, binds Secret Manager secret, and strips legacy password', () => {
    const mockOnChange = vi.fn();
    const { container } = render(
      <MqttIOSinkProperties
        node={{
          data: {
            label: 'MQTT Sink',
            properties: {
              brokerUrl: 'ssl://broker.local:8883',
              topic: 'devices/commands',
              authMode: 'secretManager',
              password: 'legacy-cleartext-secret',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('input[name="password"]')).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/QoS \(Quality of Service\)/i), {
      target: { name: 'qos', value: '2' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ qos: 2 }));
    expect(mockOnChange.mock.calls[0][0].password).toBeUndefined();

    fireEvent.click(screen.getByLabelText(/Retain Messages/i));
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ retain: true }));

    fireEvent.change(screen.getByLabelText(/Secret Manager Secret/i), {
      target: { name: 'secretName', value: 'mqtt-prod-pwd' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ secretName: 'mqtt-prod-pwd' })
    );

    fireEvent.change(
      screen.getByPlaceholderText('Enter custom instructions for MQTT sink code generation...'),
      { target: { value: 'Publish compact JSON' } }
    );
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Publish compact JSON' })
    );
  });
});
