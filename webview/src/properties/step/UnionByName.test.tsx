/**
 * @fileoverview Unit tests for the Spark UnionByName step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { UnionByNameProperties, normalizeUnionMode } from './UnionByName';

describe('UnionByNameProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-union-1',
    data: {
      label: 'UnionByName',
      spark_transform: 'UnionByName',
      properties,
    },
  });

  it('normalizes unionMode values cleanly to by_name or by_position', () => {
    expect(normalizeUnionMode(undefined)).toBe('by_name');
    expect(normalizeUnionMode('by_name')).toBe('by_name');
    expect(normalizeUnionMode('by_position')).toBe('by_position');
    expect(normalizeUnionMode('invalid')).toBe('by_name');
  });

  it('renders multi-input ordering helper note and defaults to by_name with allowMissingColumns true and deduplicate false', () => {
    const handleChange = vi.fn();
    render(<UnionByNameProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByTestId('union-multi-input-banner')).toHaveTextContent(
      /Multi-Input Ordering/i
    );

    const modeSelect = screen.getByLabelText(/Union Strategy/i, {
      selector: 'select',
    }) as HTMLSelectElement;
    expect(modeSelect.value).toBe('by_name');

    const allowMissingCheckbox = screen.getByLabelText(/Allow Missing Columns/i, {
      selector: 'input',
    });
    expect(allowMissingCheckbox).toBeChecked();

    const dedupCheckbox = screen.getByLabelText(/Deduplicate Rows After Union/i, {
      selector: 'input',
    });
    expect(dedupCheckbox).not.toBeChecked();
  });

  it('switches between by_name and by_position and hides allowMissingColumns when positional', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <UnionByNameProperties node={createNode()} onChange={handleChange} />
    );

    const modeSelect = screen.getByLabelText(/Union Strategy/i, { selector: 'select' });
    fireEvent.change(modeSelect, { target: { value: 'by_position' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        unionMode: 'by_position',
        allowMissingColumns: true,
        deduplicate: false,
      })
    );

    rerender(
      <UnionByNameProperties
        node={createNode({ unionMode: 'by_position' })}
        onChange={handleChange}
      />
    );
    expect(
      screen.queryByLabelText(/Allow Missing Columns/i, { selector: 'input' })
    ).not.toBeInTheDocument();
  });

  it('toggles allowMissingColumns, deduplicate, and updates Code Generation Instructions', () => {
    const handleChange = vi.fn();
    render(<UnionByNameProperties node={createNode()} onChange={handleChange} />);

    const allowMissingCheckbox = screen.getByLabelText(/Allow Missing Columns/i, {
      selector: 'input',
    });
    fireEvent.click(allowMissingCheckbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        allowMissingColumns: false,
      })
    );

    const dedupCheckbox = screen.getByLabelText(/Deduplicate Rows After Union/i, {
      selector: 'input',
    });
    fireEvent.click(dedupCheckbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        deduplicate: true,
      })
    );

    const sysTextarea = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(sysTextarea, {
      target: { value: 'Add source_system literal column if needed' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Add source_system literal column if needed',
      })
    );
  });
});
