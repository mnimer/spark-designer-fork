/**
 * @fileoverview Unit tests for the Spark PivotUnpivot step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { PivotUnpivotProperties } from './PivotUnpivot';

describe('PivotUnpivotProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-pivot-1',
    data: {
      label: 'Pivot / Unpivot',
      spark_transform: 'PivotUnpivot',
      properties,
    },
  });

  it('normalizes comma-separated string properties into clean list items on render', () => {
    render(
      <PivotUnpivotProperties
        node={createNode({
          operation: 'pivot',
          groupByColumns: ' region , , country ',
          pivotColumn: 'quarter',
          pivotValues: 'Q1, Q2',
          aggFunction: 'sum',
          aggColumn: 'revenue',
        })}
        onChange={vi.fn()}
      />
    );

    expect(screen.getByLabelText('Group by column 1')).toHaveValue('region');
    expect(screen.getByLabelText('Group by column 2')).toHaveValue('country');
    expect(screen.getByLabelText('Pivot value 1')).toHaveValue('Q1');
    expect(screen.getByLabelText('Pivot value 2')).toHaveValue('Q2');
  });

  it('renders pivot mode by default with performance tip and validates required pivot fields', () => {
    render(<PivotUnpivotProperties node={createNode()} onChange={vi.fn()} />);

    const opSelect = screen.getByLabelText(/Reshape Operation/i, { selector: 'select' });
    expect(opSelect).toHaveValue('pivot');

    expect(screen.getByTestId('pivot-values-perf-tip')).toBeInTheDocument();
    expect(screen.getByText(/avoids running an eager distinct-values scan/i)).toBeInTheDocument();

    const pivotColInput = screen.getByLabelText(/Pivot Column/i, { selector: 'input' });
    expect(pivotColInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Pivot column is required.')).toBeInTheDocument();

    const aggFnSelect = screen.getByLabelText(/Aggregate Function/i, { selector: 'select' });
    expect(aggFnSelect).toHaveValue('sum');

    const aggColInput = screen.getByLabelText(/Aggregate Column/i, { selector: 'input' });
    expect(aggColInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Aggregate column is required.')).toBeInTheDocument();
  });

  it('adds and removes groupByColumns and explicit pivotValues and updates pivot aggregation', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <PivotUnpivotProperties
        node={createNode({
          operation: 'pivot',
          groupByColumns: ['region'],
          pivotColumn: 'quarter',
          pivotValues: ['Q1', 'Q2'],
          aggFunction: 'sum',
          aggColumn: 'revenue',
        })}
        onChange={handleChange}
      />
    );

    const groupInput = screen.getByLabelText(/Group By Columns/i, { selector: 'input' });
    fireEvent.change(groupInput, { target: { value: 'country, segment' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add Group By Columns' }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        operation: 'pivot',
        groupByColumns: ['region', 'country', 'segment'],
        pivotColumn: 'quarter',
        pivotValues: ['Q1', 'Q2'],
      })
    );

    const valuesInput = screen.getByLabelText(/Explicit Pivot Values/i, { selector: 'input' });
    fireEvent.change(valuesInput, { target: { value: 'Q3, Q4' } });
    fireEvent.keyDown(valuesInput, { key: 'Enter' });

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        pivotValues: ['Q1', 'Q2', 'Q3', 'Q4'],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove pivot value Q1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        pivotValues: ['Q2'],
      })
    );

    rerender(
      <PivotUnpivotProperties
        node={createNode({
          operation: 'pivot',
          pivotColumn: 'quarter',
          aggFunction: 'sum',
          aggColumn: '',
        })}
        onChange={handleChange}
      />
    );

    const aggFnSelect = screen.getByLabelText(/Aggregate Function/i, { selector: 'select' });
    fireEvent.change(aggFnSelect, { target: { value: 'count' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        aggFunction: 'count',
        aggColumn: '*',
      })
    );
  });

  it('switches to unpivot mode with default variable and value column names and edits lists', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <PivotUnpivotProperties node={createNode()} onChange={handleChange} />
    );

    const opSelect = screen.getByLabelText(/Reshape Operation/i, { selector: 'select' });
    fireEvent.change(opSelect, { target: { value: 'unpivot' } });

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        operation: 'unpivot',
        idColumns: [],
        valueColumns: [],
        variableColumnName: 'variable',
        valueColumnName: 'value',
      })
    );

    rerender(
      <PivotUnpivotProperties
        node={createNode({
          operation: 'unpivot',
          idColumns: ['region'],
          valueColumns: ['Q1', 'Q2'],
        })}
        onChange={handleChange}
      />
    );

    const varColInput = screen.getByLabelText(/Variable Column Name/i, { selector: 'input' });
    const valColInput = screen.getByLabelText(/Value Column Name/i, { selector: 'input' });
    expect(varColInput).toHaveValue('variable');
    expect(valColInput).toHaveValue('value');

    fireEvent.change(varColInput, { target: { value: 'quarter' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        operation: 'unpivot',
        variableColumnName: 'quarter',
        valueColumnName: 'value',
      })
    );

    const idColsInput = screen.getByLabelText(/ID Columns/i, { selector: 'input' });
    fireEvent.change(idColsInput, { target: { value: 'product_line' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add ID Columns' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        idColumns: ['region', 'product_line'],
      })
    );
  });

  it('validates empty variableColumnName and valueColumnName in unpivot mode and updates instructions', () => {
    const handleChange = vi.fn();
    render(
      <PivotUnpivotProperties
        node={createNode({
          operation: 'unpivot',
          variableColumnName: '   ',
          valueColumnName: '',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByLabelText(/Variable Column Name/i, { selector: 'input' })).toHaveAttribute(
      'aria-invalid',
      'true'
    );
    expect(screen.getByText('Variable column name is required.')).toBeInTheDocument();

    expect(screen.getByLabelText(/Value Column Name/i, { selector: 'input' })).toHaveAttribute(
      'aria-invalid',
      'true'
    );
    expect(screen.getByText('Value column name is required.')).toBeInTheDocument();

    const instructions = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructions, { target: { value: 'Cast integer columns to double first.' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Cast integer columns to double first.',
      })
    );
  });
});
