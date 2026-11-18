/**
 * @fileoverview Unit tests for the Apache Spark Select step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SelectProperties, normalizeSelectColumns } from './Select';

describe('SelectProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-select-1',
    data: {
      label: 'Select',
      spark_transform: 'Select',
      properties,
    },
  });

  describe('normalizeSelectColumns', () => {
    it('returns a default wildcard row when raw input is undefined', () => {
      expect(normalizeSelectColumns(undefined)).toEqual([{ expression: '*', alias: '' }]);
    });

    it('normalizes string entries and partial objects into SelectColumnSpec entries', () => {
      expect(
        normalizeSelectColumns(['order_id', { expression: 'customer.email', alias: 'email' }, null])
      ).toEqual([
        { expression: 'order_id', alias: '' },
        { expression: 'customer.email', alias: 'email' },
        { expression: '', alias: '' },
      ]);
    });
  });

  it('switches between select and selectExpr modes', () => {
    const handleChange = vi.fn();
    render(<SelectProperties node={createNode()} onChange={handleChange} />);

    const modeSelect = screen.getByLabelText(/Selection API/i, { selector: 'select' });
    expect(modeSelect).toHaveValue('select');

    fireEvent.change(modeSelect, { target: { value: 'selectExpr' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'selectExpr',
        columns: [{ expression: '*', alias: '' }],
      })
    );
  });

  it('adds, edits, reorders, and removes projection rows', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <SelectProperties
        node={createNode({
          mode: 'select',
          columns: [{ expression: 'order_id', alias: 'id' }],
        })}
        onChange={handleChange}
      />
    );

    // Add a second row
    fireEvent.click(screen.getByRole('button', { name: /Add Column/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        columns: [
          { expression: 'order_id', alias: 'id' },
          { expression: '', alias: '' },
        ],
      })
    );

    handleChange.mockClear();
    rerender(
      <SelectProperties
        node={createNode({
          mode: 'selectExpr',
          columns: [
            { expression: 'order_id', alias: 'id' },
            { expression: 'UPPER(currency)', alias: 'currency_code' },
          ],
        })}
        onChange={handleChange}
      />
    );

    // Edit second row alias
    const secondAlias = screen.getByLabelText('Column 2 alias', { selector: 'input' });
    fireEvent.change(secondAlias, { target: { value: 'iso_currency' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        columns: [
          { expression: 'order_id', alias: 'id' },
          { expression: 'UPPER(currency)', alias: 'iso_currency' },
        ],
      })
    );

    // Move second row up
    handleChange.mockClear();
    fireEvent.click(screen.getByRole('button', { name: 'Move column 2 up' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        columns: [
          { expression: 'UPPER(currency)', alias: 'currency_code' },
          { expression: 'order_id', alias: 'id' },
        ],
      })
    );

    // Remove first row
    handleChange.mockClear();
    fireEvent.click(screen.getByRole('button', { name: 'Remove column 1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        columns: [{ expression: 'UPPER(currency)', alias: 'currency_code' }],
      })
    );
  });

  it('displays validation alerts for empty expressions or empty projection lists and updates system instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <SelectProperties
        node={createNode({
          mode: 'select',
          columns: [{ expression: '   ', alias: 'bad' }],
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByRole('alert')).toHaveTextContent('Column or SQL expression is required.');

    rerender(<SelectProperties node={createNode({ columns: [] })} onChange={handleChange} />);
    expect(screen.getByRole('alert')).toHaveTextContent(
      'Add at least one column or projection expression.'
    );

    const instructionsInput = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructionsInput, {
      target: { value: 'Prune unused audit columns early.' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Prune unused audit columns early.',
      })
    );
  });
});
