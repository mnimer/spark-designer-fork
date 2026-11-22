/**
 * @fileoverview Unit tests for the Apache Spark WithColumns step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { WithColumnsProperties, normalizeWithColumns } from './WithColumns';

describe('WithColumnsProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-with-cols-1',
    data: {
      label: 'WithColumns',
      spark_transform: 'WithColumns',
      properties,
    },
  });

  describe('normalizeWithColumns', () => {
    it('returns a single blank row when columns is undefined', () => {
      expect(normalizeWithColumns(undefined)).toEqual([
        { name: '', expression: '', castType: '' },
      ]);
    });

    it('normalizes partial or malformed entries safely', () => {
      expect(
        normalizeWithColumns([
          { name: 'net_amount', expression: 'gross - discount', castType: 'DECIMAL(18, 2)' },
          null,
        ])
      ).toEqual([
        { name: 'net_amount', expression: 'gross - discount', castType: 'DECIMAL(18, 2)' },
        { name: '', expression: '', castType: '' },
      ]);
    });
  });

  it('validates when column name, expression, or both are empty', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <WithColumnsProperties node={createNode()} onChange={handleChange} />
    );

    expect(screen.getByRole('alert')).toHaveTextContent(
      'Column name and expression are required.'
    );

    rerender(
      <WithColumnsProperties
        node={createNode({ columns: [{ name: '   ', expression: 'amount * 2' }] })}
        onChange={handleChange}
      />
    );
    expect(screen.getByRole('alert')).toHaveTextContent('Column name is required.');

    rerender(
      <WithColumnsProperties
        node={createNode({ columns: [{ name: 'doubled', expression: '   ' }] })}
        onChange={handleChange}
      />
    );
    expect(screen.getByRole('alert')).toHaveTextContent('Column expression is required.');

    rerender(
      <WithColumnsProperties
        node={createNode({ columns: [{ name: 'doubled', expression: 'amount * 2' }] })}
        onChange={handleChange}
      />
    );
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('adds, edits, casts, and removes derived column rows and updates system instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <WithColumnsProperties
        node={createNode({
          columns: [{ name: 'net_amount', expression: 'gross - discount', castType: '' }],
        })}
        onChange={handleChange}
      />
    );

    // Select cast type
    const castSelect = screen.getByLabelText('Derived column 1 cast type', {
      selector: 'select',
    });
    fireEvent.change(castSelect, { target: { value: 'DECIMAL(18, 2)' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        columns: [
          { name: 'net_amount', expression: 'gross - discount', castType: 'DECIMAL(18, 2)' },
        ],
      })
    );

    // Add second column
    handleChange.mockClear();
    fireEvent.click(screen.getByRole('button', { name: /Add Column/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        columns: [
          { name: 'net_amount', expression: 'gross - discount', castType: '' },
          { name: '', expression: '', castType: '' },
        ],
      })
    );

    // Remove first column when two exist
    handleChange.mockClear();
    rerender(
      <WithColumnsProperties
        node={createNode({
          columns: [
            { name: 'net_amount', expression: 'gross - discount', castType: 'DECIMAL(18, 2)' },
            { name: 'currency_upper', expression: 'UPPER(currency)', castType: 'STRING' },
          ],
        })}
        onChange={handleChange}
      />
    );
    fireEvent.click(screen.getByRole('button', { name: 'Remove derived column 1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        columns: [{ name: 'currency_upper', expression: 'UPPER(currency)', castType: 'STRING' }],
      })
    );

    // Update system instructions
    handleChange.mockClear();
    fireEvent.change(
      screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }),
      { target: { value: 'Round currency fields to 2 decimal places.' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Round currency fields to 2 decimal places.',
      })
    );
  });
});
