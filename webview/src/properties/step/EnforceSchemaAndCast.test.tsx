/**
 * @fileoverview Unit tests for the Spark EnforceSchemaAndCast step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { EnforceSchemaAndCastProperties } from './EnforceSchemaAndCast';

describe('EnforceSchemaAndCastProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-enforce-schema-1',
    data: {
      label: 'Enforce Schema & Cast',
      spark_transform: 'EnforceSchemaAndCast',
      properties,
    },
  });

  it('renders banners, defaults, and required validation alert when schemaDDL is empty', () => {
    const handleChange = vi.fn();
    render(<EnforceSchemaAndCastProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByTestId('enforce-contract-banner')).toHaveTextContent(/StructType\.fromDDL/i);
    expect(screen.getByTestId('enforce-dlq-banner')).toHaveTextContent(/_error/i);
    expect(screen.getByRole('alert')).toHaveTextContent(/Target Spark SQL DDL schema is required/i);

    const ddlInput = screen.getByLabelText(/Target Schema DDL/i, { selector: 'textarea' });
    expect(ddlInput).toHaveAttribute('aria-invalid', 'true');

    fireEvent.change(ddlInput, {
      target: { value: 'order_id BIGINT NOT NULL, amount DECIMAL(18, 2)' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        schemaDDL: 'order_id BIGINT NOT NULL, amount DECIMAL(18, 2)',
        driftPolicy: 'drop_extra',
        castMode: 'try_cast_dlq',
      })
    );
  });

  it('clears validation error and renders custom date and timestamp formats when configured', () => {
    const handleChange = vi.fn();
    render(
      <EnforceSchemaAndCastProperties
        node={createNode({
          schemaDDL: 'order_id BIGINT NOT NULL, order_date DATE, event_ts TIMESTAMP',
          dateFormat: 'MM/dd/yyyy',
          timestampFormat: 'yyyy-MM-dd HH:mm:ss',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.queryByRole('alert')).not.toBeInTheDocument();

    const dateInput = screen.getByLabelText(/^Date Format/i, { selector: 'input' });
    const tsInput = screen.getByLabelText(/^Timestamp Format/i, { selector: 'input' });
    expect(dateInput).toHaveValue('MM/dd/yyyy');
    expect(tsInput).toHaveValue('yyyy-MM-dd HH:mm:ss');

    fireEvent.change(dateInput, { target: { value: 'yyyyMMdd' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        dateFormat: 'yyyyMMdd',
      })
    );
  });

  it('switches driftPolicy and castMode options and toggles allowMissingNullable under fail policy', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <EnforceSchemaAndCastProperties
        node={createNode({
          schemaDDL: 'id BIGINT NOT NULL',
          driftPolicy: 'drop_extra',
          castMode: 'try_cast_dlq',
        })}
        onChange={handleChange}
      />
    );

    expect(
      screen.queryByLabelText(/Allow Missing Nullable Columns/i, { selector: 'input' })
    ).not.toBeInTheDocument();

    const driftSelect = screen.getByLabelText(/Schema Drift Policy/i, { selector: 'select' });
    fireEvent.change(driftSelect, { target: { value: 'fail' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        driftPolicy: 'fail',
      })
    );

    handleChange.mockClear();
    rerender(
      <EnforceSchemaAndCastProperties
        node={createNode({
          schemaDDL: 'id BIGINT NOT NULL',
          driftPolicy: 'fail',
          allowMissingNullable: false,
          castMode: 'try_cast_null',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByTestId('enforce-dlq-banner')).toHaveTextContent(
      /Malformed values coerce to NULL/i
    );

    const allowMissingCheckbox = screen.getByLabelText(/Allow Missing Nullable Columns/i, {
      selector: 'input',
    });
    fireEvent.click(allowMissingCheckbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        allowMissingNullable: true,
      })
    );

    handleChange.mockClear();
    const castSelect = screen.getByLabelText(/Cast Safety Mode/i, { selector: 'select' });
    fireEvent.change(castSelect, { target: { value: 'ansi_strict' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        castMode: 'ansi_strict',
      })
    );

    rerender(
      <EnforceSchemaAndCastProperties
        node={createNode({
          schemaDDL: 'id BIGINT NOT NULL',
          driftPolicy: 'add_nulls',
          castMode: 'ansi_strict',
        })}
        onChange={handleChange}
      />
    );
    expect(screen.getByTestId('enforce-dlq-banner')).toHaveTextContent(/Strict ANSI mode aborts/i);
  });

  it('adds, edits, and removes nonNullableColumns and updates system instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <EnforceSchemaAndCastProperties
        node={createNode({
          schemaDDL: 'order_id BIGINT, customer_id STRING, amount DOUBLE',
          nonNullableColumns: ['order_id'],
        })}
        onChange={handleChange}
      />
    );

    const addInput = screen.getByLabelText(/Non-Nullable Columns/i, { selector: 'input' });
    fireEvent.change(addInput, { target: { value: 'customer_id, order_id, amount' } });
    fireEvent.click(screen.getByRole('button', { name: /Add Column/i }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        nonNullableColumns: ['order_id', 'customer_id', 'amount'],
      })
    );

    handleChange.mockClear();
    rerender(
      <EnforceSchemaAndCastProperties
        node={createNode({
          schemaDDL: 'order_id BIGINT, customer_id STRING, amount DOUBLE',
          nonNullableColumns: ['order_id', 'customer_id'],
        })}
        onChange={handleChange}
      />
    );

    const secondColInput = screen.getByLabelText('Non-nullable column 2');
    fireEvent.change(secondColInput, { target: { value: 'account_id' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        nonNullableColumns: ['order_id', 'account_id'],
      })
    );

    handleChange.mockClear();
    fireEvent.click(screen.getByRole('button', { name: 'Remove non-nullable column customer_id' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        nonNullableColumns: ['order_id'],
      })
    );

    handleChange.mockClear();
    const addInputAgain = screen.getByLabelText(/Non-Nullable Columns/i, { selector: 'input' });
    fireEvent.change(addInputAgain, { target: { value: 'event_ts' } });
    fireEvent.keyDown(addInputAgain, { key: 'Enter' });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        nonNullableColumns: ['order_id', 'customer_id', 'event_ts'],
      })
    );

    handleChange.mockClear();
    const instructions = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructions, { target: { value: 'Attach contract version tag on DLQ rows' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Attach contract version tag on DLQ rows',
      })
    );
  });
});
