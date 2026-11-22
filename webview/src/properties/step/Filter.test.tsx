/**
 * @fileoverview Unit tests for the Apache Spark Filter step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  FilterProperties,
  generateFilterSystemInstruction,
  buildSqlFilterSystemInstruction,
} from './Filter';
import { PropertyForm } from '../../components/PropertyForm';

describe('FilterProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-filter-1',
    data: {
      label: 'Filter',
      spark_transform: 'Filter',
      properties,
    },
  });

  it('renders builder mode by default with column, operator, value, DLQ banner, and system instructions', () => {
    const handleChange = vi.fn();
    render(<FilterProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByLabelText(/Filter Mode/i, { selector: 'select' })).toHaveValue('builder');
    expect(screen.getByLabelText(/Column Name/i, { selector: 'input' })).toBeInTheDocument();
    expect(screen.getByLabelText(/Operator/i, { selector: 'select' })).toHaveValue('==');
    expect(screen.getByLabelText(/Target Value/i, { selector: 'input' })).toBeInTheDocument();
    expect(screen.getByTestId('filter-dlq-banner')).toHaveTextContent('~(cond) | (cond).isNull()');
    expect(
      screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' })
    ).toBeInTheDocument();
    // Empty column in builder mode triggers validation alert
    expect(screen.getByRole('alert')).toHaveTextContent('Column name is required.');
  });

  it('exposes all Spark 4.x builder operators and syncs column and legacy property keys', () => {
    const handleChange = vi.fn();
    const { rerender } = render(<FilterProperties node={createNode()} onChange={handleChange} />);

    const opSelect = screen.getByLabelText(/Operator/i, { selector: 'select' }) as HTMLSelectElement;
    const optionValues = Array.from(opSelect.options).map((opt) => opt.value);
    expect(optionValues).toEqual([
      '==',
      '!=',
      '>',
      '>=',
      '<',
      '<=',
      'contains',
      'startswith',
      'endswith',
      'isin',
      'rlike',
      'is not null',
      'is null',
      'is not empty',
    ]);

    const colInput = screen.getByLabelText(/Column Name/i, { selector: 'input' });
    fireEvent.change(colInput, { target: { value: 'status' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'builder',
        column: 'status',
        property: 'status',
        operator: '==',
      })
    );

    handleChange.mockClear();
    rerender(
      <FilterProperties
        node={createNode({ column: 'country', operator: 'isin', value: 'US, CA' })}
        onChange={handleChange}
      />
    );
    fireEvent.change(screen.getByLabelText(/Operator/i, { selector: 'select' }), {
      target: { value: 'rlike' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        column: 'country',
        property: 'country',
        operator: 'rlike',
        value: 'US, CA',
      })
    );
  });

  it('hides Target Value input for unary operators (is not null, is null, is not empty)', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <FilterProperties
        node={createNode({ column: 'customer_id', operator: 'is not null' })}
        onChange={handleChange}
      />
    );
    expect(screen.queryByLabelText(/Target Value/i, { selector: 'input' })).not.toBeInTheDocument();

    rerender(
      <FilterProperties
        node={createNode({ column: 'deleted_at', operator: 'is null' })}
        onChange={handleChange}
      />
    );
    expect(screen.queryByLabelText(/Target Value/i, { selector: 'input' })).not.toBeInTheDocument();

    rerender(
      <FilterProperties
        node={createNode({ column: 'email', operator: 'is not empty' })}
        onChange={handleChange}
      />
    );
    expect(screen.queryByLabelText(/Target Value/i, { selector: 'input' })).not.toBeInTheDocument();
  });

  it('switches to SQL expression mode, validates empty condition, and updates condition', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <FilterProperties node={createNode({ column: 'status' })} onChange={handleChange} />
    );

    fireEvent.change(screen.getByLabelText(/Filter Mode/i, { selector: 'select' }), {
      target: { value: 'sql' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'sql',
        condition: '',
      })
    );

    handleChange.mockClear();
    rerender(
      <FilterProperties node={createNode({ mode: 'sql', condition: '' })} onChange={handleChange} />
    );
    expect(screen.getByRole('alert')).toHaveTextContent('SQL predicate expression is required.');

    const condTextarea = screen.getByLabelText(/SQL \/ Column Predicate/i, {
      selector: 'textarea',
    });
    fireEvent.change(condTextarea, {
      target: { value: "status = 'ACTIVE' AND amount >= 100.0" },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'sql',
        condition: "status = 'ACTIVE' AND amount >= 100.0",
        system_instructions: buildSqlFilterSystemInstruction(
          "status = 'ACTIVE' AND amount >= 100.0"
        ),
      })
    );
  });

  it('automatically selects SQL mode when legacy or imported node carries a non-empty condition', () => {
    render(
      <FilterProperties
        node={createNode({ condition: 'amount > 0' })}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByLabelText(/Filter Mode/i, { selector: 'select' })).toHaveValue('sql');
    expect(
      screen.getByLabelText(/SQL \/ Column Predicate/i, { selector: 'textarea' })
    ).toHaveValue('amount > 0');
  });

  it('preserves legacy Element target and renders cleanly inside PropertyForm', async () => {
    render(
      <FilterProperties
        node={createNode({ compareTarget: 'Element', property: '' })}
        onChange={vi.fn()}
      />
    );
    expect(screen.queryByLabelText(/Column Name/i, { selector: 'input' })).not.toBeInTheDocument();

    render(<PropertyForm node={createNode()} onChange={vi.fn()} />);
    expect(await screen.findByText('Row Predicate')).toBeInTheDocument();
  });
});

describe('generateFilterSystemInstruction', () => {
  it('generates Spark Column instructions when compareTarget is Column', () => {
    expect(generateFilterSystemInstruction('Column', 'amount', '>=', '100')).toBe(
      "Apply df.filter() where column 'amount' >= '100'."
    );
    expect(generateFilterSystemInstruction('Column', 'user_id', 'is not null', '')).toBe(
      "Apply df.filter(F.col('user_id').isNotNull())."
    );
    expect(generateFilterSystemInstruction('Column', 'deleted_at', 'is null', '')).toBe(
      "Apply df.filter(F.col('deleted_at').isNull())."
    );
    expect(generateFilterSystemInstruction('Column', 'email', 'is not empty', '')).toBe(
      "Apply df.filter(F.col('email').isNotNull() & (F.trim(F.col('email').cast('string')) != ''))."
    );
  });

  it('preserves backwards-compatible output for legacy Message Property, Header Property, and Element callers', () => {
    expect(generateFilterSystemInstruction('Message Property', '', '==', '')).toBe(
      'Create a custom function with a todo comment and return true.'
    );
    expect(generateFilterSystemInstruction('Message Property', 'age', '>', '18')).toBe(
      "Filter elements where message property 'age' > '18'."
    );
    expect(generateFilterSystemInstruction('Header Property', 'token', '!=', 'secret')).toBe(
      "Filter elements where message header 'token' != 'secret'."
    );
    expect(generateFilterSystemInstruction('Element', '', 'is not null', '')).toBe(
      'Filter out elements that are null.'
    );
    expect(generateFilterSystemInstruction('Element', '', 'is not empty', '')).toBe(
      'Filter out elements that are null or empty.'
    );
    expect(generateFilterSystemInstruction('Message Property', 'status', 'contains', 'ERR')).toBe(
      "Filter elements where message property 'status' contains 'ERR'."
    );
  });
});
