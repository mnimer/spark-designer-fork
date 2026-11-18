/**
 * @fileoverview Unit tests for the Spark WindowOver step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { WindowOverProperties } from './WindowOver';

describe('WindowOverProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-window-1',
    data: {
      label: 'Window Over',
      spark_transform: 'WindowOver',
      properties,
    },
  });

  it('shows single-partition warning when partitionByColumns is empty and hides it once populated', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <WindowOverProperties node={createNode()} onChange={handleChange} />
    );

    expect(screen.getByTestId('window-empty-partition-warning')).toBeInTheDocument();
    expect(screen.getByText(/moves all rows into a single partition/i)).toBeInTheDocument();

    const partInput = screen.getByLabelText(/Partition By Columns/i, { selector: 'input' });
    fireEvent.change(partInput, { target: { value: 'account_id, region' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add Partition Column' }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        partitionByColumns: ['account_id', 'region'],
      })
    );

    rerender(
      <WindowOverProperties
        node={createNode({ partitionByColumns: ['account_id'] })}
        onChange={handleChange}
      />
    );
    expect(screen.queryByTestId('window-empty-partition-warning')).not.toBeInTheDocument();
  });

  it('manages orderByColumns with column name and asc or desc direction', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <WindowOverProperties
        node={createNode({ partitionByColumns: ['account_id'] })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Sort/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        orderByColumns: [{ column: '', direction: 'asc' }],
      })
    );

    rerender(
      <WindowOverProperties
        node={createNode({
          partitionByColumns: ['account_id'],
          orderByColumns: [{ column: 'event_ts', direction: 'asc' }],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText('Order by direction 1'), {
      target: { value: 'desc' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        orderByColumns: [{ column: 'event_ts', direction: 'desc' }],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove order by column 1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        orderByColumns: [],
      })
    );
  });

  it('configures rowsBetween and rangeBetween frame bounds and validates empty bounds', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <WindowOverProperties
        node={createNode({ partitionByColumns: ['account_id'] })}
        onChange={handleChange}
      />
    );

    const frameSelect = screen.getByLabelText(/Window Frame/i, { selector: 'select' });
    expect(frameSelect).toHaveValue('default');

    fireEvent.change(frameSelect, { target: { value: 'rowsBetween' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        frameType: 'rowsBetween',
        frameStart: 'unboundedPreceding',
        frameEnd: 'currentRow',
      })
    );

    rerender(
      <WindowOverProperties
        node={createNode({
          partitionByColumns: ['account_id'],
          frameType: 'rowsBetween',
          frameStart: '-6',
          frameEnd: '   ',
        })}
        onChange={handleChange}
      />
    );

    const startInput = screen.getByLabelText(/Frame Start/i, { selector: 'input' });
    const endInput = screen.getByLabelText(/Frame End/i, { selector: 'input' });
    expect(startInput).toHaveValue('-6');
    expect(endInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Frame end is required.')).toBeInTheDocument();
  });

  it('supports lag, lead, ntile, and cumulative windowMetrics and updates instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <WindowOverProperties
        node={createNode({
          partitionByColumns: ['account_id'],
          windowMetrics: [{ fn: 'row_number', alias: 'row_num' }],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText('Window metric 1 function'), {
      target: { value: 'lag' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        windowMetrics: [
          {
            fn: 'lag',
            sourceColumn: '',
            offset: 1,
            alias: 'row_num',
          },
        ],
      })
    );

    rerender(
      <WindowOverProperties
        node={createNode({
          partitionByColumns: ['account_id'],
          windowMetrics: [
            { fn: 'lag', sourceColumn: 'amount', offset: 1, defaultValue: '0', alias: 'prev_amt' },
          ],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText('Window metric 1 offset'), {
      target: { value: '2' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        windowMetrics: [
          { fn: 'lag', sourceColumn: 'amount', offset: 2, defaultValue: '0', alias: 'prev_amt' },
        ],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Metric/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        windowMetrics: [
          { fn: 'lag', sourceColumn: 'amount', offset: 1, defaultValue: '0', alias: 'prev_amt' },
          { fn: 'sum', sourceColumn: '', alias: 'win_metric_2' },
        ],
      })
    );

    const instructions = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructions, { target: { value: 'Compute 7-day rolling revenue.' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Compute 7-day rolling revenue.',
      })
    );
  });
});
