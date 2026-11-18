/**
 * @fileoverview Unit tests for the Spark StatefulTransform step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { StatefulTransformProperties } from './StatefulTransform';

describe('StatefulTransformProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-stateful-1',
    data: {
      label: 'Stateful Transform',
      spark_transform: 'StatefulTransform',
      properties,
    },
  });

  it('renders transformWithState defaults, RocksDB banner, and validates required keys and schemas', () => {
    render(<StatefulTransformProperties node={createNode()} onChange={vi.fn()} />);

    expect(screen.getByTestId('stateful-rocksdb-banner')).toBeInTheDocument();
    expect(screen.getByText(/RocksDBStateStoreProvider/i)).toBeInTheDocument();

    expect(screen.getByLabelText(/Stateful API Mode/i, { selector: 'select' })).toHaveValue(
      'transformWithState'
    );
    expect(screen.getByLabelText(/State Primitive/i, { selector: 'select' })).toHaveValue(
      'ValueState'
    );
    expect(screen.getByLabelText(/Time Mode/i, { selector: 'select' })).toHaveValue('None');
    expect(screen.getByLabelText(/Output Mode/i, { selector: 'select' })).toHaveValue('Update');

    expect(screen.getByText('At least one grouping key column is required.')).toBeInTheDocument();
    expect(screen.getByText('State schema DDL is required.')).toBeInTheDocument();
    expect(screen.getByText('Output schema DDL is required.')).toBeInTheDocument();
  });

  it('adds and removes grouping key columns and switches between ValueState, ListState, and MapState', () => {
    const handleChange = vi.fn();
    render(
      <StatefulTransformProperties
        node={createNode({
          keyColumns: ['user_id', 'tenant_id'],
          stateSchemaDDL: 'cnt BIGINT',
          outputSchemaDDL: 'user_id STRING, cnt BIGINT',
        })}
        onChange={handleChange}
      />
    );

    const keyInput = screen.getByLabelText(/Grouping Key Columns/i, { selector: 'input' });
    fireEvent.change(keyInput, { target: { value: 'session_id' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add Key Column' }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        keyColumns: ['user_id', 'tenant_id', 'session_id'],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove key column user_id' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        keyColumns: ['tenant_id'],
      })
    );

    fireEvent.change(screen.getByLabelText(/State Primitive/i, { selector: 'select' }), {
      target: { value: 'MapState' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        statePrimitive: 'MapState',
      })
    );
  });

  it('hides statePrimitive in applyInPandasWithState mode and shows EventTime watermark hint', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <StatefulTransformProperties
        node={createNode({
          apiMode: 'applyInPandasWithState',
          keyColumns: ['user_id'],
          timeMode: 'EventTime',
          outputMode: 'Append',
          stateSchemaDDL: 'cnt BIGINT',
          outputSchemaDDL: 'user_id STRING, cnt BIGINT',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.queryByLabelText(/State Primitive/i)).not.toBeInTheDocument();
    expect(screen.getByTestId('stateful-event-time-hint')).toBeInTheDocument();

    const ttlInput = screen.getByLabelText(/State TTL Duration/i, { selector: 'input' });
    fireEvent.change(ttlInput, { target: { value: '24 hours' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        apiMode: 'applyInPandasWithState',
        ttlDuration: '24 hours',
      })
    );

    rerender(
      <StatefulTransformProperties
        node={createNode({
          keyColumns: ['user_id'],
          stateSchemaDDL: 'cnt BIGINT',
          outputSchemaDDL: 'user_id STRING, cnt BIGINT',
        })}
        onChange={handleChange}
      />
    );

    const outSchemaInput = screen.getByLabelText(/Output Schema DDL/i, { selector: 'textarea' });
    fireEvent.change(outSchemaInput, {
      target: { value: 'user_id STRING, alert_flag BOOLEAN' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        outputSchemaDDL: 'user_id STRING, alert_flag BOOLEAN',
      })
    );

    const instructions = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructions, {
      target: { value: 'Emit fraud alert after 5 failed logins within 10 minutes.' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Emit fraud alert after 5 failed logins within 10 minutes.',
      })
    );
  });
});
