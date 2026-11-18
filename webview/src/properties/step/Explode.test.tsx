/**
 * @fileoverview Unit tests for the Apache Spark Explode step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ExplodeProperties, normalizeExplodeFunction } from './Explode';

describe('ExplodeProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-explode-1',
    data: {
      label: 'Explode',
      spark_transform: 'Explode',
      properties,
    },
  });

  describe('normalizeExplodeFunction', () => {
    it('accepts all six Spark generator functions and falls back to explode', () => {
      expect(normalizeExplodeFunction('explode_outer')).toBe('explode_outer');
      expect(normalizeExplodeFunction('posexplode')).toBe('posexplode');
      expect(normalizeExplodeFunction('inline_outer')).toBe('inline_outer');
      expect(normalizeExplodeFunction('unknown')).toBe('explode');
      expect(normalizeExplodeFunction(undefined)).toBe('explode');
    });
  });

  it('renders defaults and validates missing sourceColumn', () => {
    render(<ExplodeProperties node={createNode()} onChange={vi.fn()} />);

    const fnSelect = screen.getByLabelText(/Generator Function/i, {
      selector: 'select',
    }) as HTMLSelectElement;
    expect(Array.from(fnSelect.options).map((o) => o.value)).toEqual([
      'explode',
      'explode_outer',
      'posexplode',
      'posexplode_outer',
      'inline',
      'inline_outer',
    ]);
    expect(fnSelect).toHaveValue('explode');
    expect(screen.getByLabelText(/Element Output Alias/i, { selector: 'input' })).toHaveValue(
      'item'
    );
    expect(
      screen.queryByLabelText(/Position Output Alias/i, { selector: 'input' })
    ).not.toBeInTheDocument();
    expect(screen.getByLabelText(/Drop Source Column/i, { selector: 'input' })).not.toBeChecked();
    expect(screen.getByRole('alert')).toHaveTextContent('Source collection column is required.');
  });

  it('shows positionAlias for posexplode variants and struct field guidance for inline variants', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <ExplodeProperties
        node={createNode({ sourceColumn: 'line_items' })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Generator Function/i, { selector: 'select' }), {
      target: { value: 'posexplode_outer' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        explodeFunction: 'posexplode_outer',
        positionAlias: 'pos',
      })
    );

    rerender(
      <ExplodeProperties
        node={createNode({
          explodeFunction: 'posexplode_outer',
          sourceColumn: 'line_items',
          positionAlias: 'idx',
          outputAlias: 'line_item',
        })}
        onChange={handleChange}
      />
    );
    expect(screen.getByLabelText(/Position Output Alias/i, { selector: 'input' })).toHaveValue(
      'idx'
    );
    expect(screen.getByLabelText(/Element Output Alias/i, { selector: 'input' })).toHaveValue(
      'line_item'
    );

    rerender(
      <ExplodeProperties
        node={createNode({
          explodeFunction: 'inline_outer',
          sourceColumn: 'events',
        })}
        onChange={handleChange}
      />
    );
    expect(screen.getByTestId('explode-inline-note')).toHaveTextContent('inline_outer');
    expect(
      screen.queryByLabelText(/Element Output Alias/i, { selector: 'input' })
    ).not.toBeInTheDocument();
  });

  it('toggles dropSourceColumn and updates custom code generation instructions', () => {
    const handleChange = vi.fn();
    render(
      <ExplodeProperties
        node={createNode({ sourceColumn: 'tags', outputAlias: 'tag' })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByLabelText(/Drop Source Column/i, { selector: 'input' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sourceColumn: 'tags',
        outputAlias: 'tag',
        dropSourceColumn: true,
      })
    );

    handleChange.mockClear();
    fireEvent.change(
      screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }),
      { target: { value: 'Trim whitespace on exploded tag strings.' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Trim whitespace on exploded tag strings.',
      })
    );
  });
});
