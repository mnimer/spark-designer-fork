/**
 * @fileoverview Unit tests for the Spark GroupBy & Aggregate step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  GroupByAggProperties,
  normalizeGroupByColumns,
  normalizeAggregations,
} from './GroupByAgg';

describe('GroupByAggProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-groupby-1',
    data: {
      label: 'GroupBy & Aggregate',
      spark_transform: 'GroupByAgg',
      properties,
    },
  });

  describe('normalization helpers', () => {
    it('normalizes comma-separated strings and arrays into clean string[]', () => {
      expect(normalizeGroupByColumns('region, user_id , ')).toEqual(['region', 'user_id']);
      expect(normalizeGroupByColumns([' region ', '', 'device'])).toEqual(['region', 'device']);
      expect(normalizeGroupByColumns(undefined)).toEqual([]);
    });

    it('provides a default count(*) aggregation when aggregations is undefined', () => {
      expect(normalizeAggregations(undefined)).toEqual([
        { column: '*', fn: 'count', alias: 'record_count' },
      ]);
    });
  });

  it('adds comma-separated grouping columns cleanly as string[] and removes columns', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <GroupByAggProperties node={createNode()} onChange={handleChange} />
    );

    const addInput = screen.getByLabelText(/Group By Columns/i);
    fireEvent.change(addInput, { target: { value: 'region, category' } });
    fireEvent.click(screen.getByRole('button', { name: /^Add$/i }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        groupByColumns: ['region', 'category'],
      })
    );

    handleChange.mockClear();
    rerender(
      <GroupByAggProperties
        node={createNode({ groupByColumns: ['region', 'category'] })}
        onChange={handleChange}
      />
    );

    expect(screen.getByLabelText('Group by column 1')).toHaveValue('region');
    expect(screen.getByLabelText('Group by column 2')).toHaveValue('category');

    fireEvent.click(screen.getByRole('button', { name: 'Remove group by column region' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        groupByColumns: ['category'],
      })
    );
  });

  it('switches Time Window between None, Tumbling, Sliding, and Session with contextual fields', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <GroupByAggProperties node={createNode()} onChange={handleChange} />
    );

    // None by default: no timeColumn or duration fields rendered
    expect(screen.queryByLabelText(/Time Column/i)).not.toBeInTheDocument();

    const windowSelect = screen.getByLabelText(/Window Type/i);
    fireEvent.change(windowSelect, { target: { value: 'Tumbling' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        windowType: 'Tumbling',
        windowDuration: '5 minutes',
      })
    );

    // Render Tumbling state: shows Time Column and Window Duration, not Slide or Session Gap
    rerender(
      <GroupByAggProperties
        node={createNode({ windowType: 'Tumbling', timeColumn: 'event_ts', windowDuration: '5 minutes' })}
        onChange={handleChange}
      />
    );
    expect(screen.getByLabelText(/Time Column/i)).toHaveValue('event_ts');
    expect(screen.getByLabelText(/Window Duration/i)).toHaveValue('5 minutes');
    expect(screen.queryByLabelText(/Slide Duration/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/Session Gap Duration/i)).not.toBeInTheDocument();

    // Render Sliding state: shows Time Column, Window Duration, and Slide Duration
    rerender(
      <GroupByAggProperties
        node={createNode({
          windowType: 'Sliding',
          timeColumn: 'event_ts',
          windowDuration: '10 minutes',
          slideDuration: '2 minutes',
        })}
        onChange={handleChange}
      />
    );
    expect(screen.getByLabelText(/Window Duration/i)).toHaveValue('10 minutes');
    expect(screen.getByLabelText(/Slide Duration/i)).toHaveValue('2 minutes');
    expect(screen.queryByLabelText(/Session Gap Duration/i)).not.toBeInTheDocument();

    // Render Session state: shows Time Column and Session Gap Duration
    rerender(
      <GroupByAggProperties
        node={createNode({
          windowType: 'Session',
          timeColumn: 'event_ts',
          sessionGapDuration: '15 minutes',
        })}
        onChange={handleChange}
      />
    );
    expect(screen.getByLabelText(/Session Gap Duration/i)).toHaveValue('15 minutes');
    expect(screen.queryByLabelText(/Window Duration/i)).not.toBeInTheDocument();
    expect(screen.queryByLabelText(/Slide Duration/i)).not.toBeInTheDocument();
  });

  it('adds, updates, and removes aggregate expressions across all supported PySpark functions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <GroupByAggProperties
        node={createNode({
          groupByColumns: ['region'],
          aggregations: [{ column: 'amount', fn: 'sum', alias: 'total_amount' }],
        })}
        onChange={handleChange}
      />
    );

    const fnSelect = screen.getByLabelText('Aggregation 1 function') as HTMLSelectElement;
    const optionValues = Array.from(fnSelect.options).map((o) => o.value);
    expect(optionValues).toEqual([
      'count',
      'sum',
      'avg',
      'min',
      'max',
      'countDistinct',
      'approx_count_distinct',
      'stddev',
      'variance',
      'first',
      'last',
      'collect_list',
      'collect_set',
    ]);

    fireEvent.change(fnSelect, { target: { value: 'approx_count_distinct' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        aggregations: [
          { column: 'amount', fn: 'approx_count_distinct', alias: 'total_amount' },
        ],
      })
    );

    fireEvent.change(fnSelect, { target: { value: 'countDistinct' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        aggregations: [{ column: 'amount', fn: 'countDistinct', alias: 'total_amount' }],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Aggregation/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        aggregations: [
          { column: 'amount', fn: 'sum', alias: 'total_amount' },
          { column: '', fn: 'sum', alias: 'agg_2' },
        ],
      })
    );

    handleChange.mockClear();
    rerender(
      <GroupByAggProperties
        node={createNode({
          aggregations: [
            { column: 'amount', fn: 'sum', alias: 'total_amount' },
            { column: 'user_id', fn: 'countDistinct', alias: 'unique_users' },
          ],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove aggregation 1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        aggregations: [{ column: 'user_id', fn: 'countDistinct', alias: 'unique_users' }],
      })
    );
  });

  it('toggles flattenWindowStruct when an event-time window is active and supports stddev and variance', () => {
    const handleChange = vi.fn();
    render(
      <GroupByAggProperties
        node={createNode({
          windowType: 'Tumbling',
          timeColumn: 'event_ts',
          windowDuration: '5 minutes',
          flattenWindowStruct: false,
          aggregations: [
            { column: 'latency_ms', fn: 'stddev', alias: 'latency_stddev' },
            { column: 'latency_ms', fn: 'variance', alias: 'latency_variance' },
          ],
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByLabelText('Aggregation 1 function')).toHaveValue('stddev');
    expect(screen.getByLabelText('Aggregation 2 function')).toHaveValue('variance');

    const flattenCheckbox = screen.getByLabelText(/Flatten Window Struct/i, {
      selector: 'input',
    });
    expect(flattenCheckbox).not.toBeChecked();
    fireEvent.click(flattenCheckbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        flattenWindowStruct: true,
      })
    );
  });
});
