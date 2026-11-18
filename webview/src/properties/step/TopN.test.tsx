/**
 * @fileoverview Unit tests for the Spark TopN step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  TopNProperties,
  normalizeOrderByColumns,
  normalizePartitionColumns,
} from './TopN';

describe('TopNProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-topn-1',
    data: {
      label: 'TopN',
      spark_transform: 'TopN',
      properties,
    },
  });

  describe('normalization helpers', () => {
    it('normalizes partitionByColumns from arrays and comma-separated strings', () => {
      expect(normalizePartitionColumns(undefined)).toEqual([]);
      expect(normalizePartitionColumns('region, category , ')).toEqual(['region', 'category']);
      expect(normalizePartitionColumns([' tenant ', '', 'country'])).toEqual([
        'tenant',
        'country',
      ]);
    });

    it('provides a default descending orderByColumns entry when missing', () => {
      expect(normalizeOrderByColumns(undefined)).toEqual([{ column: '', direction: 'desc' }]);
      expect(
        normalizeOrderByColumns([
          { column: 'score', direction: 'desc' },
          { column: 'created_at', direction: 'asc' },
        ])
      ).toEqual([
        { column: 'score', direction: 'desc' },
        { column: 'created_at', direction: 'asc' },
      ]);
    });
  });

  it('defaults to n=10, row_number, keepRankColumn=false, and validates n >= 1', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <TopNProperties node={createNode()} onChange={handleChange} />
    );

    const nInput = screen.getByLabelText(/Top N Rows/i, { selector: 'input' });
    expect(nInput).toHaveValue(10);

    const rankSelect = screen.getByLabelText(/Ranking Function/i, {
      selector: 'select',
    }) as HTMLSelectElement;
    expect(rankSelect.value).toBe('row_number');
    expect(Array.from(rankSelect.options).map((o) => o.value)).toEqual([
      'row_number',
      'rank',
      'dense_rank',
    ]);

    fireEvent.change(nInput, { target: { value: '5' } });
    expect(handleChange).toHaveBeenCalledWith(expect.objectContaining({ n: 5 }));

    rerender(<TopNProperties node={createNode({ n: 0 })} onChange={handleChange} />);
    expect(screen.getByRole('alert')).toHaveTextContent(
      'Top N must be an integer greater than or equal to 1.'
    );
  });

  it('adds and removes partitionByColumns and orderByColumns entries', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <TopNProperties
        node={createNode({
          orderByColumns: [{ column: 'revenue', direction: 'desc' }],
        })}
        onChange={handleChange}
      />
    );

    const partInput = screen.getByLabelText(/Partition By Columns/i, { selector: 'input' });
    fireEvent.change(partInput, { target: { value: 'region, store_id' } });
    fireEvent.click(screen.getByRole('button', { name: /^Add$/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        partitionByColumns: ['region', 'store_id'],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Sort/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        orderByColumns: [
          { column: 'revenue', direction: 'desc' },
          { column: '', direction: 'desc' },
        ],
      })
    );

    handleChange.mockClear();
    rerender(
      <TopNProperties
        node={createNode({
          partitionByColumns: ['region', 'store_id'],
          orderByColumns: [
            { column: 'revenue', direction: 'desc' },
            { column: 'order_id', direction: 'asc' },
          ],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove partition column region' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        partitionByColumns: ['store_id'],
      })
    );

    fireEvent.change(screen.getByLabelText('Order by 2 direction'), {
      target: { value: 'desc' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        orderByColumns: [
          { column: 'revenue', direction: 'desc' },
          { column: 'order_id', direction: 'desc' },
        ],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove order by 1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        orderByColumns: [{ column: 'order_id', direction: 'asc' }],
      })
    );
  });

  it('toggles keepRankColumn, edits rankColumnAlias, and updates Code Generation Instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <TopNProperties node={createNode()} onChange={handleChange} />
    );

    expect(
      screen.queryByLabelText(/Rank Column Alias/i, { selector: 'input' })
    ).not.toBeInTheDocument();

    const keepRankCheckbox = screen.getByLabelText(/Keep Rank Column in Output Schema/i, {
      selector: 'input',
    });
    fireEvent.click(keepRankCheckbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        keepRankColumn: true,
        rankColumnAlias: 'rank_num',
      })
    );

    rerender(
      <TopNProperties
        node={createNode({ keepRankColumn: true, rankColumnAlias: 'rank_num' })}
        onChange={handleChange}
      />
    );

    const aliasInput = screen.getByLabelText(/Rank Column Alias/i, { selector: 'input' });
    expect(aliasInput).toHaveValue('rank_num');
    fireEvent.change(aliasInput, { target: { value: 'sales_rank' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        rankColumnAlias: 'sales_rank',
      })
    );

    const sysTextarea = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(sysTextarea, { target: { value: 'Place NULLs last when sorting' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Place NULLs last when sorting',
      })
    );
  });
});
