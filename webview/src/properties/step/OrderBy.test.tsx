/**
 * @fileoverview Unit tests for the Spark OrderBy step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { OrderByProperties, normalizeSortColumns } from './OrderBy';

describe('OrderByProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-orderby-1',
    data: {
      label: 'Order By',
      spark_transform: 'OrderBy',
      properties,
    },
  });

  it('normalizes empty or malformed sortColumns into valid defaults', () => {
    expect(normalizeSortColumns(undefined)).toEqual([
      { column: '', direction: 'asc', nullsOrder: 'default' },
    ]);
    expect(
      normalizeSortColumns([
        { column: 'score', direction: 'desc', nullsOrder: 'nulls_last' },
        { column: 123, direction: 'invalid', nullsOrder: 'unknown' },
      ])
    ).toEqual([
      { column: 'score', direction: 'desc', nullsOrder: 'nulls_last' },
      { column: '', direction: 'asc', nullsOrder: 'default' },
    ]);
  });

  it('displays empty-column validation alert by default and updates sort column fields', () => {
    const handleChange = vi.fn();
    render(<OrderByProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByRole('alert')).toHaveTextContent(/Sort column name is required/i);

    fireEvent.change(screen.getByLabelText('Sort key 1 column'), {
      target: { value: 'event_ts' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sortColumns: [{ column: 'event_ts', direction: 'asc', nullsOrder: 'default' }],
        withinPartitionsOnly: false,
      })
    );
  });

  it('adds, updates direction and nulls placement, reorders, and removes sort keys', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <OrderByProperties
        node={createNode({
          sortColumns: [{ column: 'region', direction: 'asc', nullsOrder: 'default' }],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Sort Key/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sortColumns: [
          { column: 'region', direction: 'asc', nullsOrder: 'default' },
          { column: '', direction: 'asc', nullsOrder: 'default' },
        ],
      })
    );

    handleChange.mockClear();
    rerender(
      <OrderByProperties
        node={createNode({
          sortColumns: [
            { column: 'region', direction: 'asc', nullsOrder: 'default' },
            { column: 'score', direction: 'desc', nullsOrder: 'nulls_last' },
          ],
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByRole('button', { name: 'Move sort key 1 up' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Move sort key 2 down' })).toBeDisabled();

    fireEvent.click(screen.getByRole('button', { name: 'Move sort key 2 up' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sortColumns: [
          { column: 'score', direction: 'desc', nullsOrder: 'nulls_last' },
          { column: 'region', direction: 'asc', nullsOrder: 'default' },
        ],
      })
    );

    handleChange.mockClear();
    fireEvent.change(screen.getByLabelText('Sort key 1 nulls placement'), {
      target: { value: 'nulls_first' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sortColumns: [
          { column: 'region', direction: 'asc', nullsOrder: 'nulls_first' },
          { column: 'score', direction: 'desc', nullsOrder: 'nulls_last' },
        ],
      })
    );

    handleChange.mockClear();
    fireEvent.click(screen.getByRole('button', { name: 'Remove sort key 1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sortColumns: [{ column: 'score', direction: 'desc', nullsOrder: 'nulls_last' }],
      })
    );
  });

  it('toggles withinPartitionsOnly and updates system instructions', () => {
    const handleChange = vi.fn();
    render(
      <OrderByProperties
        node={createNode({
          sortColumns: [{ column: 'user_id', direction: 'asc', nullsOrder: 'default' }],
          withinPartitionsOnly: false,
        })}
        onChange={handleChange}
      />
    );

    const checkbox = screen.getByLabelText(/Sort Within Partitions Only/i, { selector: 'input' });
    fireEvent.click(checkbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        withinPartitionsOnly: true,
      })
    );

    const instructions = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructions, { target: { value: 'Sort locally before Iceberg write' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Sort locally before Iceberg write',
      })
    );
  });
});
