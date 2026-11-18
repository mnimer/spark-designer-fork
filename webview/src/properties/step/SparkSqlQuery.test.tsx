/**
 * @fileoverview Unit tests for the SparkSqlQuery step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SparkSqlQueryProperties } from './SparkSqlQuery';

describe('SparkSqlQueryProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-sql-1',
    data: {
      label: 'Spark SQL Query',
      spark_transform: 'SparkSqlQuery',
      properties,
    },
  });

  it('renders default input_0 view alias and validates required sqlQuery when empty', () => {
    const handleChange = vi.fn();
    render(<SparkSqlQueryProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByLabelText('View alias 1 input index', { selector: 'input' })).toHaveValue(0);
    expect(screen.getByLabelText('View alias 1 name', { selector: 'input' })).toHaveValue('input_0');

    const sqlInput = screen.getByLabelText(/SQL Query/i, { selector: 'textarea' });
    expect(sqlInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('SQL query is required.')).toBeInTheDocument();

    fireEvent.change(sqlInput, {
      target: { value: 'SELECT * FROM input_0 WHERE region = :region' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sqlQuery: 'SELECT * FROM input_0 WHERE region = :region',
        inputViewAliases: [{ inputIndex: 0, viewName: 'input_0' }],
      })
    );
  });

  it('adds, edits, validates, and removes upstream temp view aliases', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <SparkSqlQueryProperties
        node={createNode({
          inputViewAliases: [{ inputIndex: 0, viewName: 'orders_view' }],
          sqlQuery: 'SELECT * FROM orders_view',
        })}
        onChange={handleChange}
      />
    );

    // Single alias cannot be removed
    expect(screen.getByRole('button', { name: 'Remove view alias 1' })).toBeDisabled();

    // Add second view alias
    fireEvent.click(screen.getByRole('button', { name: /Add View/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        inputViewAliases: [
          { inputIndex: 0, viewName: 'orders_view' },
          { inputIndex: 1, viewName: 'input_1' },
        ],
      })
    );

    handleChange.mockClear();
    rerender(
      <SparkSqlQueryProperties
        node={createNode({
          inputViewAliases: [
            { inputIndex: 0, viewName: 'orders_view' },
            { inputIndex: 1, viewName: '123-bad-view' },
          ],
          sqlQuery: 'SELECT * FROM orders_view',
        })}
        onChange={handleChange}
      />
    );

    // Invalid SQL view identifier shows alert
    expect(screen.getByLabelText('View alias 2 name', { selector: 'input' })).toHaveAttribute(
      'aria-invalid',
      'true'
    );
    expect(
      screen.getByText(/Temp view name must start with a letter or underscore/i)
    ).toBeInTheDocument();

    // Edit view alias name and input index
    fireEvent.change(screen.getByLabelText('View alias 2 name', { selector: 'input' }), {
      target: { value: 'customers_view' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        inputViewAliases: [
          { inputIndex: 0, viewName: 'orders_view' },
          { inputIndex: 1, viewName: 'customers_view' },
        ],
      })
    );

    // Remove first view alias when two exist
    handleChange.mockClear();
    fireEvent.click(screen.getByRole('button', { name: 'Remove view alias 1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        inputViewAliases: [{ inputIndex: 1, viewName: '123-bad-view' }],
      })
    );
  });

  it('manages Spark 4.x parameterized SQL (:param) bindings and strips leading colons', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <SparkSqlQueryProperties
        node={createNode({ sqlQuery: 'SELECT * FROM input_0 WHERE region = :region' })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Parameter/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sqlParameters: [{ key: '', value: '' }],
      })
    );

    handleChange.mockClear();
    rerender(
      <SparkSqlQueryProperties
        node={createNode({
          sqlQuery: 'SELECT * FROM input_0 WHERE region = :region',
          sqlParameters: [{ key: '', value: '' }],
        })}
        onChange={handleChange}
      />
    );

    // Empty key shows validation alert
    expect(screen.getByText('Parameter name must be a valid SQL identifier.')).toBeInTheDocument();

    // Typing :region strips leading colon automatically
    fireEvent.change(screen.getByLabelText('SQL parameter 1 name', { selector: 'input' }), {
      target: { value: ':region' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sqlParameters: [{ key: 'region', value: '' }],
      })
    );

    handleChange.mockClear();
    rerender(
      <SparkSqlQueryProperties
        node={createNode({
          sqlQuery: 'SELECT * FROM input_0 WHERE region = :region',
          sqlParameters: [{ key: 'region', value: '' }],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText('SQL parameter 1 value', { selector: 'input' }), {
      target: { value: 'US' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sqlParameters: [{ key: 'region', value: 'US' }],
      })
    );

    handleChange.mockClear();
    fireEvent.click(screen.getByRole('button', { name: 'Remove SQL parameter 1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sqlParameters: [],
      })
    );
  });

  it('updates SystemInstructionsField and renders DLQ guidance banner', () => {
    const handleChange = vi.fn();
    render(
      <SparkSqlQueryProperties
        node={createNode({ sqlQuery: 'SELECT 1' })}
        onChange={handleChange}
      />
    );

    expect(screen.getByTestId('sql-dlq-guidance-banner')).toBeInTheDocument();

    const instructionsInput = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructionsInput, {
      target: { value: 'Apply broadcast hint to lookup view' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Apply broadcast hint to lookup view',
      })
    );
  });
});
