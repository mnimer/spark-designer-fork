/**
 * @fileoverview Unit tests for the RepartitionCoalesce step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { RepartitionCoalesceProperties } from './RepartitionCoalesce';

describe('RepartitionCoalesceProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-repart-1',
    data: {
      label: 'Repartition / Coalesce',
      spark_transform: 'RepartitionCoalesce',
      properties,
    },
  });

  it('defaults to repartition with 8 partitions and shows shuffle guidance banner', () => {
    const handleChange = vi.fn();
    render(<RepartitionCoalesceProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByLabelText(/Partitioning Operation/i, { selector: 'select' })).toHaveValue(
      'repartition'
    );
    expect(screen.getByLabelText(/Number of Partitions/i, { selector: 'input' })).toHaveValue(8);
    expect(screen.getByTestId('repartition-shuffle-banner')).toHaveTextContent(
      /Full Cluster Hash \/ Round-Robin Shuffle/i
    );
    expect(screen.getByTestId('repartition-columns-section')).toBeInTheDocument();
  });

  it('hides partitionColumns and clears them when switching to coalesce', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <RepartitionCoalesceProperties
        node={createNode({
          operation: 'repartition',
          numPartitions: 16,
          partitionColumns: ['tenant_id'],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Partitioning Operation/i, { selector: 'select' }), {
      target: { value: 'coalesce' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        operation: 'coalesce',
        numPartitions: 16,
        partitionColumns: [],
      })
    );

    rerender(
      <RepartitionCoalesceProperties
        node={createNode({ operation: 'coalesce', numPartitions: 4 })}
        onChange={handleChange}
      />
    );

    expect(screen.queryByTestId('repartition-columns-section')).not.toBeInTheDocument();
    expect(screen.getByTestId('repartition-shuffle-banner')).toHaveTextContent(
      /Narrow Partition Merge \(Zero Shuffle\)/i
    );
  });

  it('requires at least one partition column for repartitionByRange and supports adding and removing columns', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <RepartitionCoalesceProperties
        node={createNode({
          operation: 'repartitionByRange',
          numPartitions: 12,
          partitionColumns: [],
        })}
        onChange={handleChange}
      />
    );

    expect(
      screen.getByText('At least one partition column is required for repartitionByRange.')
    ).toBeInTheDocument();

    const addInput = screen.getByLabelText(/Partition Columns/i, { selector: 'input' });
    fireEvent.change(addInput, { target: { value: 'event_date, user_id' } });
    fireEvent.click(screen.getByRole('button', { name: /^Add$/i }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        operation: 'repartitionByRange',
        numPartitions: 12,
        partitionColumns: ['event_date', 'user_id'],
      })
    );

    handleChange.mockClear();
    rerender(
      <RepartitionCoalesceProperties
        node={createNode({
          operation: 'repartitionByRange',
          numPartitions: 12,
          partitionColumns: ['event_date', 'user_id'],
        })}
        onChange={handleChange}
      />
    );

    expect(
      screen.queryByText('At least one partition column is required for repartitionByRange.')
    ).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Remove partition column event_date' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        partitionColumns: ['user_id'],
      })
    );
  });

  it('validates positive integer numPartitions and updates SystemInstructionsField', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <RepartitionCoalesceProperties
        node={createNode({ operation: 'coalesce', numPartitions: 0 })}
        onChange={handleChange}
      />
    );

    expect(
      screen.getByText('Number of partitions must be a positive integer.')
    ).toBeInTheDocument();

    rerender(
      <RepartitionCoalesceProperties
        node={createNode({ operation: 'coalesce', numPartitions: 2 })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Number of Partitions/i, { selector: 'input' }), {
      target: { value: '6' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        numPartitions: 6,
      })
    );

    fireEvent.change(
      screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }),
      { target: { value: 'Coalesce to 6 files before GCS Parquet write' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Coalesce to 6 files before GCS Parquet write',
      })
    );
  });
});
