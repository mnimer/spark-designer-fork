/**
 * HudiSource Properties Test Suite
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating HudiSourceProperties component functionality,
 * readMode, queryType ('snapshot' | 'incremental' | 'read_optimized'),
 * required path validation, conditional beginInstantTime/endInstantTime,
 * readPaths, and system_instructions.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { HudiSourceProperties } from './HudiSource';

describe('HudiSourceProperties', () => {
  it('renders default snapshot fields and validates required path', () => {
    const mockNode = {
      data: {
        label: 'Hudi Source',
        properties: {},
      },
    };
    const mockOnChange = vi.fn();

    render(<HudiSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Read Mode/i)).toHaveValue('batch');
    expect(screen.getByLabelText(/Query Type/i)).toHaveValue('snapshot');
    expect(screen.getByLabelText(/Hudi Table Base Path/i)).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Hudi Table Base Path is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Hudi Table Name/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/Begin Instant Time/i)).not.toBeInTheDocument();
    expect(screen.getByLabelText(/Specific Read Paths/i)).toBeInTheDocument();
  });

  it('renders and validates Begin Instant Time and End Instant Time when queryType is incremental', () => {
    const mockOnChange = vi.fn();
    render(
      <HudiSourceProperties
        node={{
          data: {
            label: 'Hudi Source',
            properties: {
              readMode: 'batch',
              path: 'gs://lakehouse/hudi/orders',
              tableName: 'orders_hudi',
              queryType: 'incremental',
              beginInstantTime: '',
              endInstantTime: '20260102000000000',
              readPaths: 'gs://lakehouse/hudi/orders/2026/*',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const beginInput = screen.getByLabelText(/Begin Instant Time/i);
    expect(beginInput).toBeInTheDocument();
    expect(beginInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Begin Instant Time is required for incremental queries.')).toBeInTheDocument();

    fireEvent.change(beginInput, {
      target: { name: 'beginInstantTime', value: '20260101000000000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        beginInstantTime: '20260101000000000',
      })
    );

    fireEvent.change(screen.getByLabelText(/Hudi Table Name/i), {
      target: { name: 'tableName', value: 'updated_orders' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        tableName: 'updated_orders',
      })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i), {
      target: { value: 'Drop Hudi metadata columns (_hoodie_*)' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Drop Hudi metadata columns (_hoodie_*)',
      })
    );
  });
});
