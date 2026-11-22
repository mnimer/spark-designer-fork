/**
 * @fileoverview Unit tests for PulsarIOSinkProperties (`PulsarSinkProperties`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x PulsarSink configuration fields, service.url and topic validation,
 * __key and __eventTime mapping, JWT Token and OAuth2 Secret Manager auth, and removal of Beam x-lang fields.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { PulsarIOSinkProperties, PulsarSinkProperties } from './PulsarIO';

describe('PulsarIOSinkProperties', () => {
  it('exports PulsarSinkProperties as an alias for PulsarIOSinkProperties', () => {
    expect(PulsarSinkProperties).toBe(PulsarIOSinkProperties);
  });

  it('renders Spark 4.x PulsarSink fields and omits legacy Beam Expansion Service inputs', () => {
    render(
      <PulsarIOSinkProperties
        node={{
          data: {
            label: 'Pulsar Sink',
            properties: {},
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByLabelText(/Pulsar Service URL/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Pulsar Admin URL/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Destination Pulsar Topic/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Payload Serialization/i)).toHaveValue('json');
    expect(screen.getByLabelText(/Message Key Column \(__key\)/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Event Time Column/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Compression Codec/i)).toHaveValue('NONE');
    expect(screen.getByLabelText(/Code Generation Instructions/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/Expansion Service Mode/i)).not.toBeInTheDocument();
  });

  it('validates missing serviceUrl and malformed Pulsar topic schemes', () => {
    const { rerender } = render(
      <PulsarIOSinkProperties
        node={{
          data: {
            label: 'Pulsar Sink',
            properties: {
              serviceUrl: '',
              topic: '',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByText('Pulsar Service URL is required.')).toBeInTheDocument();
    expect(screen.getByText('Pulsar destination topic is required.')).toBeInTheDocument();

    rerender(
      <PulsarIOSinkProperties
        node={{
          data: {
            label: 'Pulsar Sink',
            properties: {
              serviceUrl: 'pulsar://localhost:6650',
              topic: 'invalid://bad-format',
            },
          },
        }}
        onChange={vi.fn()}
      />
    );

    expect(
      screen.getByText(
        'Use persistent://tenant/namespace/topic or non-persistent://tenant/namespace/topic.'
      )
    ).toBeInTheDocument();
  });

  it('supports JWT token and OAuth2 Secret Manager authentication modes', () => {
    const mockOnChange = vi.fn();
    const { rerender } = render(
      <PulsarIOSinkProperties
        node={{
          data: {
            label: 'Pulsar Sink',
            properties: {
              serviceUrl: 'pulsar+ssl://broker:6651',
              topic: 'persistent://public/default/orders',
              authMode: 'token',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const secretInput = screen.getByLabelText(/Secret Manager Secret/i);
    fireEvent.change(secretInput, { target: { name: 'secretName', value: 'pulsar-jwt-prod' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ secretName: 'pulsar-jwt-prod' })
    );

    rerender(
      <PulsarIOSinkProperties
        node={{
          data: {
            label: 'Pulsar Sink',
            properties: {
              serviceUrl: 'pulsar+ssl://broker:6651',
              topic: 'persistent://public/default/orders',
              authMode: 'oauth2',
              secretName: 'pulsar-oauth-key',
              oauthIssuerUrl: '',
              oauthAudience: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByText('OAuth2 Issuer URL is required.')).toBeInTheDocument();
    expect(screen.getByText('OAuth2 Audience is required.')).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/OAuth2 Issuer URL/i), {
      target: { name: 'oauthIssuerUrl', value: 'https://auth.streamnative.cloud/' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ oauthIssuerUrl: 'https://auth.streamnative.cloud/' })
    );
  });

  it('updates __key column, advanced producer options, and system instructions', () => {
    const mockOnChange = vi.fn();
    render(
      <PulsarIOSinkProperties
        node={{
          data: {
            label: 'Pulsar Sink',
            properties: {
              serviceUrl: 'pulsar://localhost:6650',
              topic: 'persistent://public/default/events',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Message Key Column \(__key\)/i), {
      target: { name: 'keyColumn', value: 'user_id' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ keyColumn: 'user_id' }));

    fireEvent.click(screen.getByText('Advanced Pulsar Producer Options'));

    fireEvent.click(screen.getByLabelText(/Enable Producer Batching/i));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ batchingEnabled: false })
    );

    fireEvent.change(
      screen.getByPlaceholderText('Enter custom instructions for Pulsar sink code generation...'),
      { target: { value: 'Route DLQ events via __topic' } }
    );
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Route DLQ events via __topic' })
    );
  });
});
