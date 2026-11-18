/**
 * @fileoverview Unit tests for the Spark DataQualityAssert step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  DataQualityAssertProperties,
  normalizeExpectations,
  validateExpectation,
} from './DataQualityAssert';

describe('DataQualityAssertProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-dq-1',
    data: {
      label: 'Data Quality Assert',
      spark_transform: 'DataQualityAssert',
      properties,
    },
  });

  describe('normalization and expectation validation helpers', () => {
    it('normalizes missing expectations to a default not_null expectation', () => {
      expect(normalizeExpectations(undefined)).toEqual([
        { name: 'not_null_check_1', column: '', checkType: 'not_null' },
      ]);
    });

    it('validates all 6 expectation check types and range bounds', () => {
      expect(
        validateExpectation({ name: '', column: 'id', checkType: 'not_null' })
      ).toMatch(/Expectation name is required/i);

      expect(
        validateExpectation({ name: 'nn_id', column: '', checkType: 'not_null' })
      ).toMatch(/Target column is required/i);

      expect(
        validateExpectation({ name: 'rng', column: 'amount', checkType: 'range' })
      ).toMatch(/minimum value or a maximum value/i);

      expect(
        validateExpectation({
          name: 'rng_inverted',
          column: 'amount',
          checkType: 'range',
          minValue: '100',
          maxValue: '10',
        })
      ).toMatch(/Minimum value cannot exceed maximum value/i);

      expect(
        validateExpectation({
          name: 'rng_ok',
          column: 'amount',
          checkType: 'range',
          minValue: '0',
          maxValue: '100',
        })
      ).toBeNull();

      expect(
        validateExpectation({ name: 'rx', column: 'email', checkType: 'regex' })
      ).toMatch(/Regex pattern is required/i);

      expect(
        validateExpectation({ name: 'set', column: 'currency', checkType: 'in_set' })
      ).toMatch(/Allowed values list is required/i);

      expect(
        validateExpectation({ name: 'sql', column: '', checkType: 'sql_expr' })
      ).toMatch(/SQL boolean expression is required/i);

      expect(
        validateExpectation({
          name: 'sql_ok',
          column: '',
          checkType: 'sql_expr',
          sqlExpression: 'net <= gross',
        })
      ).toBeNull();
    });
  });

  it('switches failurePolicy between route_to_dlq, drop_invalid, and fail_job', () => {
    const handleChange = vi.fn();
    render(
      <DataQualityAssertProperties
        node={createNode({
          expectations: [{ name: 'id_present', column: 'order_id', checkType: 'not_null' }],
          failurePolicy: 'route_to_dlq',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByTestId('dq-dlq-guidance')).toHaveTextContent(/_failed_rules/i);

    const policySelect = screen.getByLabelText(/Failure Policy/i, { selector: 'select' });
    fireEvent.change(policySelect, { target: { value: 'fail_job' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        failurePolicy: 'fail_job',
      })
    );
  });

  it('renders contextual fields across range, regex, in_set, unique, and sql_expr expectations', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <DataQualityAssertProperties
        node={createNode({
          expectations: [
            { name: 'amount_bounds', column: 'amount', checkType: 'range', minValue: '0', maxValue: '500' },
          ],
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByLabelText('Expectation 1 min value')).toHaveValue('0');
    expect(screen.getByLabelText('Expectation 1 max value')).toHaveValue('500');

    rerender(
      <DataQualityAssertProperties
        node={createNode({
          expectations: [
            { name: 'email_pattern', column: 'email', checkType: 'regex', regexPattern: '^.+@.+$' },
            { name: 'currency_set', column: 'currency', checkType: 'in_set', allowedValues: 'USD, EUR' },
            { name: 'order_unique', column: 'order_id', checkType: 'unique' },
            { name: 'margin_check', column: '', checkType: 'sql_expr', sqlExpression: 'net <= gross' },
          ],
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByLabelText('Expectation 1 regex pattern')).toHaveValue('^.+@.+$');
    expect(screen.getByLabelText('Expectation 2 allowed values')).toHaveValue('USD, EUR');
    expect(screen.getByText(/Window\.partitionBy/i)).toBeInTheDocument();
    expect(screen.getByLabelText('Expectation 4 SQL expression')).toHaveValue('net <= gross');
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('adds and removes expectation cards and updates system instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <DataQualityAssertProperties
        node={createNode({
          expectations: [{ name: 'id_not_null', column: 'order_id', checkType: 'not_null' }],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Expectation/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        expectations: [
          { name: 'id_not_null', column: 'order_id', checkType: 'not_null' },
          { name: 'expectation_2', column: '', checkType: 'not_null' },
        ],
      })
    );

    handleChange.mockClear();
    rerender(
      <DataQualityAssertProperties
        node={createNode({
          expectations: [
            { name: 'id_not_null', column: 'order_id', checkType: 'not_null' },
            { name: 'expectation_2', column: '', checkType: 'not_null' },
          ],
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByRole('alert')).toHaveTextContent(/Target column is required/i);

    fireEvent.click(screen.getByRole('button', { name: 'Remove expectation 2' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        expectations: [{ name: 'id_not_null', column: 'order_id', checkType: 'not_null' }],
      })
    );

    const instructions = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructions, { target: { value: 'Include ingestion_ts in DLQ payload' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Include ingestion_ts in DLQ payload',
      })
    );
  });
});
