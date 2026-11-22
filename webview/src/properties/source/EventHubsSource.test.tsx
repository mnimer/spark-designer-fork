/**
 * EventHubsSource Properties Test Suite
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating EventHubsSourceProperties component functionality,
 * readMode and connectorMode selectors, required eventHubNamespace and eventHubName validation,
 * consumerGroup default ($Default), startingOffsets, maxOffsetsPerTrigger, SecretManagerAuthFields,
 * and system_instructions.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { EventHubsSourceProperties } from './EventHubsSource';

describe('EventHubsSourceProperties', () => {
  it('renders default fields and validates required eventHubNamespace and eventHubName', () => {
    const mockNode = {
      data: {
        label: 'Event Hubs Source',
        properties: {},
      },
    };
    const mockOnChange = vi.fn();

    render(<EventHubsSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Read Mode/i)).toHaveValue('streaming');
    expect(screen.getByLabelText(/Connector Mode/i)).toHaveValue('kafka_endpoint');
    expect(screen.getByLabelText(/Event Hubs Namespace/i)).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Event Hubs Namespace is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Event Hub Name \(Topic\)/i)).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Event Hub Name is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Consumer Group/i)).toHaveValue('$Default');
    expect(screen.getByLabelText(/Starting Offsets/i)).toHaveValue('latest');
    expect(screen.getByLabelText(/Max Offsets Per Trigger/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Secret Manager Secret/i)).toBeInTheDocument();
  });

  it('populates fields and dispatches updates for namespace, hub name, connectorMode, and secretName', () => {
    const mockNode = {
      data: {
        label: 'Event Hubs Source',
        properties: {
          readMode: 'batch' as const,
          connectorMode: 'eventhubs' as const,
          eventHubNamespace: 'telemetry-ns',
          eventHubName: 'iot-stream',
          consumerGroup: 'spark-cg',
          startingOffsets: 'earliest',
          maxOffsetsPerTrigger: 5000,
          authMode: 'secretManager',
          secretName: 'eh-conn-str',
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<EventHubsSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Read Mode/i)).toHaveValue('batch');
    expect(screen.getByLabelText(/Connector Mode/i)).toHaveValue('eventhubs');
    expect(screen.getByLabelText(/Event Hubs Namespace/i)).toHaveValue('telemetry-ns');
    expect(screen.getByLabelText(/Event Hub Name \(Topic\)/i)).toHaveValue('iot-stream');
    expect(screen.getByLabelText(/Consumer Group/i)).toHaveValue('spark-cg');
    expect(screen.getByLabelText(/Starting Offsets/i)).toHaveValue('earliest');
    expect(screen.getByLabelText(/Max Offsets Per Trigger/i)).toHaveValue(5000);

    fireEvent.change(screen.getByLabelText(/Event Hubs Namespace/i), {
      target: { name: 'eventHubNamespace', value: 'orders-ns' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        eventHubNamespace: 'orders-ns',
      })
    );

    fireEvent.change(screen.getByLabelText(/Secret Manager Secret/i), {
      target: { name: 'secretName', value: 'orders-eh-secret' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secretName: 'orders-eh-secret',
      })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i), {
      target: { value: 'Parse Event Hubs body as JSON' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Parse Event Hubs body as JSON',
      })
    );
  });
});
