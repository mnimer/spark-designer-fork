/**
 * PubSub Source Properties Test Suite
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating PubSubProperties (`PubSubSourceProperties`) component functionality,
 * readMode selection, subscription validation, projectId, maxMessagesPerBatch, ackDeadlineSeconds,
 * includeAttributes toggle, and live subscription discovery error surfacing.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import { PubSubProperties, PubSubSourceProperties } from './PubSubIO';

describe('PubSubProperties', () => {
  it('exports PubSubSourceProperties as an alias for PubSubProperties', () => {
    expect(PubSubSourceProperties).toBe(PubSubProperties);
  });

  it('renders Spark 4.0 Pub/Sub source fields and validates required subscription', () => {
    const mockNode = {
      data: {
        properties: {
          subscription: '',
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<PubSubProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Read Mode/i)).toHaveValue('streaming');
    const subInput = screen.getByPlaceholderText('projects/my-project/subscriptions/my-sub');
    expect(subInput).toBeInTheDocument();
    expect(subInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Subscription is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/GCP Project ID/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Max Messages Per Batch/i)).toHaveValue(1000);
    expect(screen.getByLabelText(/Ack Deadline \(s\)/i)).toHaveValue(60);
    expect(screen.getByLabelText(/Include Message Attributes/i)).toBeChecked();
  });

  it('populates fields and dispatches onChange for readMode, subscription, projectId, batch limits, and includeAttributes', () => {
    const mockNode = {
      data: {
        properties: {
          readMode: 'batch' as const,
          subscription: 'projects/my-project/subscriptions/events-sub',
          projectId: 'my-project',
          maxMessagesPerBatch: 500,
          ackDeadlineSeconds: 120,
          includeAttributes: true,
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<PubSubProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Read Mode/i)).toHaveValue('batch');
    expect(screen.getByDisplayValue('projects/my-project/subscriptions/events-sub')).toBeInTheDocument();
    expect(screen.queryByText('Subscription is required.')).not.toBeInTheDocument();

    const projectInput = screen.getByLabelText(/GCP Project ID/i);
    fireEvent.change(projectInput, { target: { name: 'projectId', value: 'analytics-prod' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ projectId: 'analytics-prod' }));

    const maxMessagesInput = screen.getByLabelText(/Max Messages Per Batch/i);
    fireEvent.change(maxMessagesInput, { target: { name: 'maxMessagesPerBatch', value: '2500' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ maxMessagesPerBatch: '2500' }));

    const ackInput = screen.getByLabelText(/Ack Deadline \(s\)/i);
    fireEvent.change(ackInput, { target: { name: 'ackDeadlineSeconds', value: '90' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ ackDeadlineSeconds: '90' }));

    const attributesCheckbox = screen.getByLabelText(/Include Message Attributes/i);
    fireEvent.click(attributesCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ includeAttributes: false }));
  });

  it('surfaces subscriptionsLoaded discovery errors inline', () => {
    const mockNode = {
      data: {
        properties: {
          subscription: 'projects/p/subscriptions/s',
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<PubSubProperties node={mockNode} onChange={mockOnChange} />);

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'subscriptionsLoaded',
            subscriptions: [],
            error: 'Failed to list subscriptions: API not enabled',
          },
        })
      );
    });

    expect(screen.getByText('Failed to list subscriptions: API not enabled')).toBeInTheDocument();
  });
});
