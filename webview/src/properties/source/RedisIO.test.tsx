/**
 * Unit Tests for RedisIOSourceProperties Component (SRC35)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates RedisIOSourceProperties rendering, readMode ('batch' | 'streaming') switching,
 * dataType selector, keyPattern + scanCount (batch) vs streamKey + consumerGroup (streaming),
 * SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { RedisIOSourceProperties, RedisSourceProperties } from './RedisIO';

describe('RedisIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports RedisSourceProperties alias and validates required host and keyPattern in batch mode', () => {
    expect(RedisSourceProperties).toBe(RedisIOSourceProperties);
    render(<RedisIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Host is required')).toBeInTheDocument();
    expect(screen.getByText('Key pattern is required in Batch mode')).toBeInTheDocument();
    expect(screen.getByDisplayValue('6379')).toBeInTheDocument();
  });

  it('hydrates batch properties (host, port, dataType, keyPattern, scanCount) and dispatches updates', () => {
    const node = {
      data: {
        properties: {
          readMode: 'batch' as const,
          host: '10.0.0.5',
          port: 6379,
          dataType: 'json' as const,
          keyPattern: 'session:*',
          scanCount: 500,
        },
      },
    };

    const { container } = render(<RedisIOSourceProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByDisplayValue('10.0.0.5')).toBeInTheDocument();
    expect(screen.getByDisplayValue('session:*')).toBeInTheDocument();
    expect(screen.getByDisplayValue('500')).toBeInTheDocument();
    expect(container.querySelector('#redisSourceDataType')).toHaveValue('json');

    const scanInput = container.querySelector('#redisSourceScanCount') as HTMLInputElement;
    fireEvent.change(scanInput, { target: { name: 'scanCount', value: '2000' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ scanCount: '2000' }));
  });

  it('switches to streaming mode and renders streamKey and consumerGroup', () => {
    const { container } = render(
      <RedisIOSourceProperties
        node={{
          data: {
            properties: {
              readMode: 'streaming',
              host: '10.0.0.5',
              dataType: 'stream',
              streamKey: 'orders:stream',
              consumerGroup: 'cg-analytics',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByDisplayValue('orders:stream')).toBeInTheDocument();
    expect(screen.getByDisplayValue('cg-analytics')).toBeInTheDocument();

    const groupInput = container.querySelector('#redisSourceConsumerGroup') as HTMLInputElement;
    fireEvent.change(groupInput, { target: { name: 'consumerGroup', value: 'cg-v2' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ consumerGroup: 'cg-v2' }));

    const sysInput = container.querySelector('#redisSourceInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Use pipelined HGETALL' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Use pipelined HGETALL' })
    );
  });
});
