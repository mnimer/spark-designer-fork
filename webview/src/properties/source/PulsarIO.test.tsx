/**
 * PulsarIO Source Properties Test Suite
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating PulsarIOSourceProperties (`PulsarSourceProperties`) component functionality,
 * readMode switching, serviceUrl and topic validation, subscriptionMode options,
 * startingOffsets and conditional batch endingOffsets, allowDifferentTopicSchemas,
 * Secret Manager authentication, and system_instructions.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { PulsarIOSourceProperties, PulsarSourceProperties } from './PulsarIO';

describe('PulsarIOSourceProperties', () => {
  it('exports PulsarSourceProperties as an alias for PulsarIOSourceProperties', () => {
    expect(PulsarSourceProperties).toBe(PulsarIOSourceProperties);
  });

  it('renders default streaming fields and validates required serviceUrl and topic', () => {
    const mockNode = {
      data: {
        label: 'Pulsar Source',
        properties: {},
      },
    };
    const mockOnChange = vi.fn();

    render(<PulsarIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Read Mode/i)).toHaveValue('streaming');
    expect(screen.getByLabelText(/Pulsar Service URL/i)).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Pulsar Service URL is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Pulsar Admin URL/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Subscription Mode/i)).toHaveValue('topic');
    expect(screen.getByLabelText(/Pulsar Topic/i)).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Pulsar topic is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Starting Offsets/i)).toHaveValue('latest');
    expect(screen.queryByLabelText(/Ending Offsets/i)).not.toBeInTheDocument();
    expect(screen.getByLabelText(/Allow Different Topic Schemas/i)).not.toBeChecked();
    expect(screen.getByLabelText(/Code Generation Instructions/i)).toBeInTheDocument();
  });

  it('renders endingOffsets in batch mode and updates fields cleanly', () => {
    const mockNode = {
      data: {
        label: 'Pulsar',
        properties: {
          readMode: 'batch' as const,
          serviceUrl: 'pulsar://pulsar.prod:6650',
          adminUrl: 'http://pulsar.prod:8080',
          subscriptionMode: 'topics' as const,
          topic: 'persistent://public/default/t1,persistent://public/default/t2',
          startingOffsets: 'earliest',
          endingOffsets: 'latest',
          allowDifferentTopicSchemas: true,
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<PulsarIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Read Mode/i)).toHaveValue('batch');
    expect(screen.getByLabelText(/Pulsar Service URL/i)).toHaveValue('pulsar://pulsar.prod:6650');
    expect(screen.getByLabelText(/Pulsar Admin URL/i)).toHaveValue('http://pulsar.prod:8080');
    expect(screen.getByLabelText(/Pulsar Topics \(Comma-Separated\)/i)).toHaveValue(
      'persistent://public/default/t1,persistent://public/default/t2'
    );
    expect(screen.getByLabelText(/Ending Offsets \(Batch\)/i)).toHaveValue('latest');
    expect(screen.getByLabelText(/Allow Different Topic Schemas/i)).toBeChecked();

    fireEvent.change(screen.getByLabelText(/Pulsar Service URL/i), {
      target: { name: 'serviceUrl', value: 'pulsar+ssl://pulsar.prod:6651' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        serviceUrl: 'pulsar+ssl://pulsar.prod:6651',
      })
    );

    fireEvent.click(screen.getByLabelText(/Allow Different Topic Schemas/i));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        allowDifferentTopicSchemas: false,
      })
    );
  });

  it('supports SecretManagerAuthFields and system_instructions updates', () => {
    const mockOnChange = vi.fn();
    render(
      <PulsarIOSourceProperties
        node={{
          data: {
            label: 'Pulsar',
            properties: {
              serviceUrl: 'pulsar://localhost:6650',
              topic: 'persistent://public/default/events',
              authMode: 'secretManager',
              secretName: 'pulsar-jwt',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const secretInput = screen.getByLabelText(/Secret Manager Secret/i);
    expect(secretInput).toHaveValue('pulsar-jwt');

    fireEvent.change(secretInput, { target: { name: 'secretName', value: 'pulsar-prod-jwt' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secretName: 'pulsar-prod-jwt',
      })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i), {
      target: { value: 'Cast value bytes to UTF-8' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Cast value bytes to UTF-8',
      })
    );
  });
});
