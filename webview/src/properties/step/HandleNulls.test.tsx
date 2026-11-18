/**
 * @fileoverview Unit tests for the Spark HandleNulls step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  HandleNullsProperties,
  normalizeSubsetColumns,
  normalizeFillMappings,
  normalizeReplaceMappings,
  validateDropThresh,
} from './HandleNulls';

describe('HandleNullsProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-handlenulls-1',
    data: {
      label: 'Handle Nulls',
      spark_transform: 'HandleNulls',
      properties,
    },
  });

  describe('normalization and validation helpers', () => {
    it('normalizes subset columns from comma-separated strings and arrays', () => {
      expect(normalizeSubsetColumns('amount, country , ')).toEqual(['amount', 'country']);
      expect(normalizeSubsetColumns([' user_id ', '', 'region'])).toEqual(['user_id', 'region']);
      expect(normalizeSubsetColumns(undefined)).toEqual([]);
    });

    it('normalizes fill and replace mappings cleanly', () => {
      expect(normalizeFillMappings(undefined)).toEqual([]);
      expect(normalizeFillMappings([{ column: 'amount', value: 0 }])).toEqual([
        { column: 'amount', value: '0' },
      ]);
      expect(normalizeReplaceMappings(undefined)).toEqual([{ fromValue: '', toValue: '' }]);
      expect(normalizeReplaceMappings([{ fromValue: -1, toValue: 0 }])).toEqual([
        { fromValue: '-1', toValue: '0' },
      ]);
    });

    it('validates optional dropThresh positive integer boundaries', () => {
      expect(validateDropThresh(undefined)).toBeNull();
      expect(validateDropThresh('')).toBeNull();
      expect(validateDropThresh(2)).toBeNull();
      expect(validateDropThresh('3')).toBeNull();
      expect(validateDropThresh(0)).toMatch(/positive integer/i);
      expect(validateDropThresh(-2)).toMatch(/positive integer/i);
      expect(validateDropThresh('1.5')).toMatch(/positive integer/i);
    });
  });

  it('adds and removes subset columns cleanly as string[]', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <HandleNullsProperties node={createNode()} onChange={handleChange} />
    );

    const subsetInput = screen.getByLabelText(/Subset Columns/i, { selector: 'input' });
    fireEvent.change(subsetInput, { target: { value: 'amount, country' } });
    fireEvent.click(screen.getByRole('button', { name: /^Add$/i }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        strategy: 'fill',
        subsetColumns: ['amount', 'country'],
      })
    );

    handleChange.mockClear();
    rerender(
      <HandleNullsProperties
        node={createNode({ subsetColumns: ['amount', 'country'] })}
        onChange={handleChange}
      />
    );

    expect(screen.getByLabelText('Subset column 1')).toHaveValue('amount');
    fireEvent.click(screen.getByRole('button', { name: 'Remove subset column amount' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        subsetColumns: ['country'],
      })
    );
  });

  it('configures fill strategy with defaultFillValue, per-column fillMappings, and missing-column validation', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <HandleNullsProperties node={createNode({ strategy: 'fill' })} onChange={handleChange} />
    );

    const defaultFillInput = screen.getByLabelText(/Default Fill Value/i, { selector: 'input' });
    fireEvent.change(defaultFillInput, { target: { value: 'UNKNOWN' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        strategy: 'fill',
        defaultFillValue: 'UNKNOWN',
      })
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Mapping/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        fillMappings: [{ column: '', value: '' }],
      })
    );

    handleChange.mockClear();
    rerender(
      <HandleNullsProperties
        node={createNode({
          strategy: 'fill',
          fillMappings: [{ column: '', value: '0.0' }],
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByRole('alert')).toHaveTextContent(/Column name is required/i);

    fireEvent.change(screen.getByLabelText('Fill mapping 1 column'), {
      target: { value: 'amount' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        fillMappings: [{ column: 'amount', value: '0.0' }],
      })
    );
  });

  it('switches to drop strategy and validates dropHow and dropThresh', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <HandleNullsProperties node={createNode()} onChange={handleChange} />
    );

    const strategySelect = screen.getByLabelText(/^Strategy/i, { selector: 'select' });
    fireEvent.change(strategySelect, { target: { value: 'drop' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        strategy: 'drop',
        dropHow: 'any',
      })
    );

    handleChange.mockClear();
    rerender(
      <HandleNullsProperties
        node={createNode({ strategy: 'drop', dropHow: 'any', dropThresh: 0 })}
        onChange={handleChange}
      />
    );

    expect(screen.getByRole('alert')).toHaveTextContent(/positive integer/i);

    const howSelect = screen.getByLabelText(/Drop Condition/i, { selector: 'select' });
    fireEvent.change(howSelect, { target: { value: 'all' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        dropHow: 'all',
      })
    );

    const threshInput = screen.getByLabelText(/Minimum Non-Null Count/i, { selector: 'input' });
    fireEvent.change(threshInput, { target: { value: '3' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        dropThresh: 3,
      })
    );
  });

  it('switches to replace strategy, edits sentinel mappings, and updates system instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <HandleNullsProperties
        node={createNode({
          strategy: 'replace',
          replaceMappings: [{ fromValue: 'N/A', toValue: 'NULL' }],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Replacement/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        replaceMappings: [
          { fromValue: 'N/A', toValue: 'NULL' },
          { fromValue: '', toValue: '' },
        ],
      })
    );

    handleChange.mockClear();
    rerender(
      <HandleNullsProperties
        node={createNode({
          strategy: 'replace',
          replaceMappings: [
            { fromValue: 'N/A', toValue: 'NULL' },
            { fromValue: '', toValue: '0' },
          ],
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByRole('alert')).toHaveTextContent(/From value is required/i);

    fireEvent.click(screen.getByRole('button', { name: 'Remove replace mapping 2' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        replaceMappings: [{ fromValue: 'N/A', toValue: 'NULL' }],
      })
    );

    const instructionsInput = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructionsInput, { target: { value: 'Preserve NaN on diagnostic cols' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Preserve NaN on diagnostic cols',
      })
    );
  });
});
