/**
 * RateSource Properties Test Suite
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating RateSourceProperties component functionality,
 * rateFormat switching between 'rate' and 'rate-micro-batch', rowsPerSecond,
 * rampUpTime, numPartitions, rowsPerBatch, advanceMillisPerBatch, and system_instructions.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { RateSourceProperties } from './RateSource';

describe('RateSourceProperties', () => {
  it('renders default rate format fields with rowsPerSecond=1', () => {
    const mockNode = {
      data: {
        label: 'Rate Source',
        properties: {},
      },
    };
    const mockOnChange = vi.fn();

    render(<RateSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByText('STREAMING ONLY')).toBeInTheDocument();
    expect(screen.getByLabelText(/Rate Source Format/i)).toHaveValue('rate');
    expect(screen.getByLabelText(/Rows Per Second/i)).toHaveValue(1);
    expect(screen.getByLabelText(/Ramp-Up Time/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Number of Partitions/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/Rows Per Batch/i)).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Rows Per Second/i), {
      target: { name: 'rowsPerSecond', value: '50' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        readMode: 'streaming',
        rowsPerSecond: '50',
      })
    );
  });

  it('renders rowsPerBatch and advanceMillisPerBatch when rateFormat is rate-micro-batch', () => {
    const mockNode = {
      data: {
        label: 'Rate Source',
        properties: {
          rateFormat: 'rate-micro-batch' as const,
          rowsPerBatch: 250,
          advanceMillisPerBatch: 5000,
          numPartitions: 8,
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<RateSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Rate Source Format/i)).toHaveValue('rate-micro-batch');
    expect(screen.getByLabelText(/Rows Per Batch/i)).toHaveValue(250);
    expect(screen.getByLabelText(/Advance Millis Per Batch/i)).toHaveValue(5000);
    expect(screen.getByLabelText(/Number of Partitions/i)).toHaveValue(8);

    fireEvent.change(screen.getByLabelText(/Rows Per Batch/i), {
      target: { name: 'rowsPerBatch', value: '500' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        rowsPerBatch: '500',
      })
    );

    fireEvent.change(screen.getByLabelText(/Advance Millis Per Batch/i), {
      target: { name: 'advanceMillisPerBatch', value: '2000' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        advanceMillisPerBatch: '2000',
      })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i), {
      target: { value: 'Add synthetic JSON column' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Add synthetic JSON column',
      })
    );
  });
});
