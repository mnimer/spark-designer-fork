/**
 * @fileoverview Unit tests for the Spark RollupCube step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { RollupCubeProperties } from './RollupCube';

describe('RollupCubeProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-rollup-1',
    data: {
      label: 'Rollup / Cube',
      spark_transform: 'RollupCube',
      properties,
    },
  });

  it('renders rollup mode, default count aggregation, and includeGroupingId enabled by default', () => {
    render(<RollupCubeProperties node={createNode()} onChange={vi.fn()} />);

    expect(screen.getByLabelText(/Grouping Set Mode/i, { selector: 'select' })).toHaveValue(
      'rollup'
    );
    expect(screen.getByText('At least one dimension column is required for Rollup or Cube.')).toBeInTheDocument();

    expect(screen.getByLabelText('Aggregation 1 function')).toHaveValue('count');
    expect(screen.getByLabelText('Aggregation 1 column')).toHaveValue('*');
    expect(screen.getByLabelText('Aggregation 1 alias')).toHaveValue('record_count');

    const groupingCheckbox = screen.getByLabelText(/Include F\.grouping_id\(\) Column/i, {
      selector: 'input',
    });
    expect(groupingCheckbox).toBeChecked();

    const groupingAliasInput = screen.getByLabelText(/Grouping ID Alias/i, { selector: 'input' });
    expect(groupingAliasInput).toHaveValue('grouping_id');
  });

  it('adds and removes dimension columns and shows combinatorial warning in cube mode with 5+ dimensions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <RollupCubeProperties
        node={createNode({
          mode: 'rollup',
          dimensionColumns: ['region', 'country'],
        })}
        onChange={handleChange}
      />
    );

    const dimInput = screen.getByLabelText(/Dimension Columns/i, { selector: 'input' });
    fireEvent.change(dimInput, { target: { value: 'channel' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add Dimension Column' }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'rollup',
        dimensionColumns: ['region', 'country', 'channel'],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove dimension column region' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        dimensionColumns: ['country'],
      })
    );

    rerender(
      <RollupCubeProperties
        node={createNode({
          mode: 'cube',
          dimensionColumns: ['d1', 'd2', 'd3', 'd4', 'd5'],
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByTestId('cube-combinatorial-warning')).toBeInTheDocument();
    expect(screen.getByText(/generates 32 grouping sets/i)).toBeInTheDocument();
  });

  it('adds, edits, and removes aggregation metrics', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <RollupCubeProperties
        node={createNode({
          dimensionColumns: ['region'],
          aggregations: [{ column: 'revenue', fn: 'sum', alias: 'total_revenue' }],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Aggregation/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        aggregations: [
          { column: 'revenue', fn: 'sum', alias: 'total_revenue' },
          { column: '', fn: 'sum', alias: 'agg_2' },
        ],
      })
    );

    rerender(
      <RollupCubeProperties
        node={createNode({
          dimensionColumns: ['region'],
          aggregations: [
            { column: 'revenue', fn: 'sum', alias: 'total_revenue' },
            { column: 'orders', fn: 'avg', alias: 'avg_orders' },
          ],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText('Aggregation 2 alias'), {
      target: { value: 'mean_orders' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        aggregations: [
          { column: 'revenue', fn: 'sum', alias: 'total_revenue' },
          { column: 'orders', fn: 'avg', alias: 'mean_orders' },
        ],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove aggregation 1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        aggregations: [{ column: 'orders', fn: 'avg', alias: 'avg_orders' }],
      })
    );
  });

  it('toggles includeGroupingId, validates empty groupingIdAlias, and updates instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <RollupCubeProperties
        node={createNode({
          dimensionColumns: ['region'],
          includeGroupingId: true,
          groupingIdAlias: '   ',
        })}
        onChange={handleChange}
      />
    );

    const aliasInput = screen.getByLabelText(/Grouping ID Alias/i, { selector: 'input' });
    expect(aliasInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Grouping ID alias is required when enabled.')).toBeInTheDocument();

    const checkbox = screen.getByLabelText(/Include F\.grouping_id\(\) Column/i, {
      selector: 'input',
    });
    fireEvent.click(checkbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        includeGroupingId: false,
      })
    );

    rerender(
      <RollupCubeProperties
        node={createNode({
          dimensionColumns: ['region'],
          includeGroupingId: false,
        })}
        onChange={handleChange}
      />
    );
    expect(screen.queryByLabelText(/Grouping ID Alias/i)).not.toBeInTheDocument();

    const instructions = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructions, { target: { value: 'Label grand total rows as ALL.' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Label grand total rows as ALL.',
      })
    );
  });
});
