/**
 * @fileoverview Unit tests for the Spark SetOperation step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SetOperationProperties, normalizeSetOperation } from './SetOperation';

describe('SetOperationProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-set-op-1',
    data: {
      label: 'SetOperation',
      spark_transform: 'SetOperation',
      properties,
    },
  });

  it('normalizes raw operation strings cleanly', () => {
    expect(normalizeSetOperation(undefined)).toBe('intersect');
    expect(normalizeSetOperation('intersect')).toBe('intersect');
    expect(normalizeSetOperation('intersectAll')).toBe('intersectAll');
    expect(normalizeSetOperation('exceptAll')).toBe('exceptAll');
    expect(normalizeSetOperation('subtract')).toBe('subtract');
    expect(normalizeSetOperation('unknown')).toBe('intersect');
  });

  it('renders all 4 set operations and defaults to intersect with alignColumnsByName enabled', () => {
    const handleChange = vi.fn();
    render(<SetOperationProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByTestId('set-operation-banner')).toHaveTextContent(/Ordered Set Inputs/i);

    const opSelect = screen.getByLabelText(/Set Operation/i, {
      selector: 'select',
    }) as HTMLSelectElement;
    expect(opSelect.value).toBe('intersect');
    expect(Array.from(opSelect.options).map((o) => o.value)).toEqual([
      'intersect',
      'intersectAll',
      'exceptAll',
      'subtract',
    ]);

    const alignCheckbox = screen.getByLabelText(/Align Columns by Name/i, {
      selector: 'input',
    });
    expect(alignCheckbox).toBeChecked();
  });

  it('updates operation, alignColumnsByName, and Code Generation Instructions', () => {
    const handleChange = vi.fn();
    render(<SetOperationProperties node={createNode()} onChange={handleChange} />);

    const opSelect = screen.getByLabelText(/Set Operation/i, { selector: 'select' });
    fireEvent.change(opSelect, { target: { value: 'exceptAll' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        operation: 'exceptAll',
        alignColumnsByName: true,
      })
    );

    const alignCheckbox = screen.getByLabelText(/Align Columns by Name/i, {
      selector: 'input',
    });
    fireEvent.click(alignCheckbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        alignColumnsByName: false,
      })
    );

    const sysTextarea = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(sysTextarea, {
      target: { value: 'Cast numeric IDs to STRING before intersecting' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Cast numeric IDs to STRING before intersecting',
      })
    );
  });
});
