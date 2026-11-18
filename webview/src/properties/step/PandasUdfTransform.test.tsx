/**
 * @fileoverview Unit tests for the PandasUdfTransform step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { PandasUdfTransformProperties } from './PandasUdfTransform';

describe('PandasUdfTransformProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-pandas-1',
    data: {
      label: 'Pandas UDF Transform',
      spark_transform: 'PandasUdfTransform',
      properties,
    },
  });

  it('defaults to pandas_udf with vectorized_transform functionName and validates required fields', () => {
    const handleChange = vi.fn();
    render(<PandasUdfTransformProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByLabelText(/UDF Execution Mode/i, { selector: 'select' })).toHaveValue(
      'pandas_udf'
    );
    expect(screen.getByLabelText(/Python Function Name/i, { selector: 'input' })).toHaveValue(
      'vectorized_transform'
    );

    // Output column and Return Schema DDL are required in pandas_udf mode
    expect(screen.getByText('Output column is required for @pandas_udf mode.')).toBeInTheDocument();
    expect(screen.getByText('Return schema DDL is required.')).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Output Column/i, { selector: 'input' }), {
      target: { value: 'z_score' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        udfMode: 'pandas_udf',
        functionName: 'vectorized_transform',
        outputColumn: 'z_score',
      })
    );
  });

  it('switches between pandas_udf, mapInPandas, and mapInArrow and hides outputColumn for batch modes', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <PandasUdfTransformProperties
        node={createNode({
          udfMode: 'pandas_udf',
          outputColumn: 'score',
          returnSchemaDDL: 'double',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByLabelText(/Output Column/i, { selector: 'input' })).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/UDF Execution Mode/i, { selector: 'select' }), {
      target: { value: 'mapInArrow' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        udfMode: 'mapInArrow',
      })
    );

    rerender(
      <PandasUdfTransformProperties
        node={createNode({
          udfMode: 'mapInArrow',
          returnSchemaDDL: 'id string, upper_code string',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.queryByLabelText(/Output Column/i, { selector: 'input' })).not.toBeInTheDocument();
    expect(
      screen.getByLabelText(/Return Schema \(Spark DDL\)/i, { selector: 'input' })
    ).toHaveValue('id string, upper_code string');
  });

  it('adds comma-separated inputColumns and removes columns cleanly', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <PandasUdfTransformProperties
        node={createNode({ returnSchemaDDL: 'double', outputColumn: 'total' })}
        onChange={handleChange}
      />
    );

    const colAddInput = screen.getByLabelText(/Input Columns/i, { selector: 'input' });
    fireEvent.change(colAddInput, { target: { value: 'price, quantity' } });
    fireEvent.click(screen.getByRole('button', { name: /^Add$/i }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        inputColumns: ['price', 'quantity'],
      })
    );

    handleChange.mockClear();
    rerender(
      <PandasUdfTransformProperties
        node={createNode({
          inputColumns: ['price', 'quantity'],
          returnSchemaDDL: 'double',
          outputColumn: 'total',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByLabelText('Input column 1')).toHaveValue('price');
    expect(screen.getByLabelText('Input column 2')).toHaveValue('quantity');

    fireEvent.click(screen.getByRole('button', { name: 'Remove input column price' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        inputColumns: ['quantity'],
      })
    );
  });

  it('validates functionName and optional positive integer maxRecordsPerBatch', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <PandasUdfTransformProperties
        node={createNode({
          functionName: '99-invalid',
          outputColumn: 'out',
          returnSchemaDDL: 'double',
          maxRecordsPerBatch: '0',
        })}
        onChange={handleChange}
      />
    );

    expect(
      screen.getByText('Function name must be a valid Python identifier.')
    ).toBeInTheDocument();
    expect(
      screen.getByText('Max records per batch must be a positive integer.')
    ).toBeInTheDocument();

    rerender(
      <PandasUdfTransformProperties
        node={createNode({
          functionName: 'scale_batch',
          outputColumn: 'out',
          returnSchemaDDL: 'double',
        })}
        onChange={handleChange}
      />
    );

    fireEvent.change(
      screen.getByLabelText(/Max Records Per Arrow Batch/i, { selector: 'input' }),
      { target: { value: '4096' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        maxRecordsPerBatch: 4096,
      })
    );

    const sysInstructions = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(sysInstructions, { target: { value: 'Compute rolling z-score' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Compute rolling z-score',
      })
    );
  });
});
