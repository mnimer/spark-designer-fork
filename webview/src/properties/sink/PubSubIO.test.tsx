/**
 * @fileoverview Unit tests for PubSubSinkProperties (`PubSubIOSinkProperties`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x Google Cloud Pub/Sub sink configuration fields, live topic discovery,
 * payload and attribute column mapping, ordering and idempotency keys, and PublisherClient batch settings.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { PubSubSinkProperties, PubSubIOSinkProperties } from './PubSubIO';

describe('PubSubSinkProperties', () => {
  it('exports PubSubIOSinkProperties as an alias for PubSubSinkProperties', () => {
    expect(PubSubIOSinkProperties).toBe(PubSubSinkProperties);
  });

  it('renders topic validation error when empty and default PublisherClient batch settings', () => {
    const mockNode = {
      data: {
        properties: {
          topic: '',
        },
      },
    };
    const mockOnChange = vi.fn();

    const { container } = render(<PubSubSinkProperties node={mockNode} onChange={mockOnChange} />);

    const topicInput = container.querySelector('#pubsubSinkTopic') as HTMLInputElement;
    expect(topicInput).toBeInTheDocument();
    expect(topicInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Topic is required.')).toBeInTheDocument();

    expect(screen.getByLabelText(/GCP Project ID/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Payload Format/i)).toHaveValue('json');
    expect(screen.getByLabelText(/Payload Column/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Attribute Columns/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Ordering Key Column/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Idempotency Key Column/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Enable Message Ordering/i)).not.toBeChecked();
    expect(screen.getByLabelText(/Max Messages/i)).toHaveValue(100);
    expect(screen.getByLabelText(/Max Bytes/i)).toHaveValue(1048576);
    expect(screen.getByLabelText(/Max Latency \(ms\)/i)).toHaveValue(10);
  });

  it('renders provided topic and project values without validation error', () => {
    const mockNode = {
      data: {
        properties: {
          topic: 'projects/my-project/topics/my-sink-topic',
          projectId: 'my-project',
        },
      },
    };

    const { container } = render(<PubSubSinkProperties node={mockNode} onChange={vi.fn()} />);

    const topicInput = container.querySelector('#pubsubSinkTopic') as HTMLInputElement;
    expect(topicInput).toHaveValue('projects/my-project/topics/my-sink-topic');
    expect(topicInput).toHaveAttribute('aria-invalid', 'false');
    expect(screen.queryByText('Topic is required.')).not.toBeInTheDocument();
    expect(screen.getByLabelText(/GCP Project ID/i)).toHaveValue('my-project');
  });

  it('requires payloadColumn when payloadFormat is string or binary', () => {
    render(
      <PubSubSinkProperties
        node={{
          data: {
            properties: {
              topic: 'events-topic',
              payloadFormat: 'string',
              payloadColumn: '',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByText('Payload Column is required.')).toBeInTheDocument();
  });

  it('updates attribute columns, ordering key, batch settings, and system instructions', () => {
    const mockOnChange = vi.fn();

    render(
      <PubSubSinkProperties
        node={{
          data: {
            properties: {
              topic: 'orders-topic',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Attribute Columns/i), {
      target: { name: 'attributeColumns', value: 'event_type,tenant_id' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ attributeColumns: 'event_type,tenant_id' })
    );

    fireEvent.change(screen.getByLabelText(/Ordering Key Column/i), {
      target: { name: 'orderingKeyColumn', value: 'customer_id' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ orderingKeyColumn: 'customer_id' })
    );

    fireEvent.click(screen.getByLabelText(/Enable Message Ordering/i));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ enableMessageOrdering: true })
    );

    fireEvent.change(screen.getByLabelText(/Max Messages/i), {
      target: { name: 'batchMaxMessages', value: '500' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ batchMaxMessages: '500' })
    );

    fireEvent.change(
      screen.getByPlaceholderText('Enter custom instructions for Pub/Sub sink code generation...'),
      { target: { value: 'Include schema version attribute' } }
    );
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Include schema version attribute' })
    );
  });
});
