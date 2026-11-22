/**
 * @fileoverview Unit Tests for RedisIOSinkProperties Component (SNK32).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates RedisIOSinkProperties rendering, host and keyColumn validation,
 * dataType switching ('hash' | 'json' | 'string' | 'stream'), streamKey validation,
 * ttlSeconds, TLS ssl toggle, pipeline batchSize, SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { RedisIOSinkProperties, RedisSinkProperties } from './RedisIO';

describe('RedisIOSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports RedisSinkProperties alias and validates required host and keyColumn in default hash mode', () => {
    expect(RedisSinkProperties).toBe(RedisIOSinkProperties);
    render(<RedisIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Host is required')).toBeInTheDocument();
    expect(screen.getByText('Key column is required')).toBeInTheDocument();
    expect(screen.getByDisplayValue('6379')).toBeInTheDocument();
    expect(screen.getByDisplayValue('1000')).toBeInTheDocument();
  });

  it('hydrates Redis hash sink properties including keyPrefix, ttlSeconds, ssl, and Secret Manager', () => {
    const node = {
      data: {
        properties: {
          host: '10.0.0.5',
          port: 6380,
          database: 2,
          ssl: true,
          dataType: 'hash' as const,
          keyPrefix: 'profile:',
          keyColumn: 'customer_id',
          ttlSeconds: 3600,
          batchSize: 500,
          authMode: 'secretManager',
          secretName: 'redis-password-secret',
        },
      },
    };

    const { container } = render(<RedisIOSinkProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByDisplayValue('10.0.0.5')).toBeInTheDocument();
    expect(screen.getByDisplayValue('6380')).toBeInTheDocument();
    expect(screen.getByDisplayValue('2')).toBeInTheDocument();
    expect(container.querySelector('#redisSinkSsl')).toBeChecked();
    expect(container.querySelector('#redisSinkDataType')).toHaveValue('hash');
    expect(screen.getByDisplayValue('profile:')).toBeInTheDocument();
    expect(screen.getByDisplayValue('customer_id')).toBeInTheDocument();
    expect(screen.getByDisplayValue('3600')).toBeInTheDocument();
    expect(screen.getByDisplayValue('500')).toBeInTheDocument();
    expect(screen.getByDisplayValue('redis-password-secret')).toBeInTheDocument();
  });

  it('switches across string and stream modes and validates required streamKey', () => {
    const { container, rerender } = render(
      <RedisIOSinkProperties
        node={{
          data: {
            properties: {
              host: '10.0.0.5',
              dataType: 'string',
              keyColumn: 'id',
              valueColumn: 'body',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByDisplayValue('body')).toBeInTheDocument();

    const typeSelect = container.querySelector('#redisSinkDataType') as HTMLSelectElement;
    fireEvent.change(typeSelect, { target: { name: 'dataType', value: 'stream' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ dataType: 'stream' }));

    rerender(
      <RedisIOSinkProperties
        node={{
          data: {
            properties: {
              host: '10.0.0.5',
              dataType: 'stream',
              streamKey: '',
              maxStreamLen: 50000,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByText('Stream key is required in Stream mode')).toBeInTheDocument();
    expect(screen.getByDisplayValue('50000')).toBeInTheDocument();

    const streamInput = container.querySelector('#redisSinkStreamKey') as HTMLInputElement;
    fireEvent.change(streamInput, { target: { name: 'streamKey', value: 'orders:stream' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ streamKey: 'orders:stream' })
    );

    const sysInput = container.querySelector('#redisSinkInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Flush pipeline every 2000 rows' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Flush pipeline every 2000 rows' })
    );
  });
});
