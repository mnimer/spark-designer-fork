/**
 * @fileoverview Unit tests for the Spark NestedStructArrayOps step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { NestedStructArrayOpsProperties } from './NestedStructArrayOps';

describe('NestedStructArrayOpsProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-nested-ops-1',
    data: {
      label: 'Nested Struct & Array Ops',
      spark_transform: 'NestedStructArrayOps',
      properties,
    },
  });

  it('renders flatten_structs mode by default and validates separator and maxDepth >= 1', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <NestedStructArrayOpsProperties node={createNode()} onChange={handleChange} />
    );

    expect(screen.getByTestId('nested-ops-banner')).toHaveTextContent(/Zero-Shuffle/i);
    expect(screen.getByTestId('flatten-structs-section')).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();

    const maxDepthInput = screen.getByLabelText(/Max Recursion Depth/i, { selector: 'input' });
    expect(maxDepthInput).toHaveValue(3);

    fireEvent.change(maxDepthInput, { target: { value: '0' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        operationMode: 'flatten_structs',
        maxDepth: 0,
      })
    );

    rerender(
      <NestedStructArrayOpsProperties
        node={createNode({ operationMode: 'flatten_structs', maxDepth: 0, separator: '' })}
        onChange={handleChange}
      />
    );

    const alerts = screen.getAllByRole('alert');
    expect(alerts).toHaveLength(2);
    expect(alerts[0]).toHaveTextContent(/Field separator is required/i);
    expect(alerts[1]).toHaveTextContent(/Max depth must be an integer >= 1/i);
  });

  it('adds, edits, and removes structColumns and toggles dropOriginalStructs in flatten_structs mode', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <NestedStructArrayOpsProperties
        node={createNode({
          operationMode: 'flatten_structs',
          structColumns: ['customer'],
          separator: '__',
          maxDepth: 2,
          dropOriginalStructs: true,
        })}
        onChange={handleChange}
      />
    );

    const addInput = screen.getByLabelText(/Struct Columns Filter/i, { selector: 'input' });
    fireEvent.change(addInput, { target: { value: 'shipping, billing' } });
    fireEvent.click(screen.getByRole('button', { name: /Add Struct/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        structColumns: ['customer', 'shipping', 'billing'],
      })
    );

    handleChange.mockClear();
    rerender(
      <NestedStructArrayOpsProperties
        node={createNode({
          operationMode: 'flatten_structs',
          structColumns: ['customer', 'shipping'],
          dropOriginalStructs: true,
        })}
        onChange={handleChange}
      />
    );

    fireEvent.change(screen.getByLabelText('Struct column 2'), {
      target: { value: 'billing_addr' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        structColumns: ['customer', 'billing_addr'],
      })
    );

    handleChange.mockClear();
    fireEvent.click(screen.getByRole('button', { name: 'Remove struct column customer' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        structColumns: ['shipping'],
      })
    );

    handleChange.mockClear();
    const dropCheckbox = screen.getByLabelText(/Drop Original Struct Columns/i, {
      selector: 'input',
    });
    fireEvent.click(dropCheckbox);
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        dropOriginalStructs: false,
      })
    );
  });

  it('switches to pack_struct mode, validates required fields, and edits sourceColumns', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <NestedStructArrayOpsProperties node={createNode()} onChange={handleChange} />
    );

    const modeSelect = screen.getByLabelText(/Operation Mode/i, { selector: 'select' });
    fireEvent.change(modeSelect, { target: { value: 'pack_struct' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        operationMode: 'pack_struct',
      })
    );

    handleChange.mockClear();
    rerender(
      <NestedStructArrayOpsProperties
        node={createNode({ operationMode: 'pack_struct' })}
        onChange={handleChange}
      />
    );

    expect(screen.getByTestId('pack-struct-section')).toBeInTheDocument();
    const alerts = screen.getAllByRole('alert');
    expect(alerts).toHaveLength(2);
    expect(alerts[0]).toHaveTextContent(/Output struct column name is required/i);
    expect(alerts[1]).toHaveTextContent(/At least one source column is required/i);

    const outColInput = screen.getByLabelText(/Output Struct Column/i, { selector: 'input' });
    fireEvent.change(outColInput, { target: { value: 'address_struct' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        outputStructColumn: 'address_struct',
      })
    );

    handleChange.mockClear();
    const addSourceInput = screen.getByLabelText(/Source Columns to Pack/i, { selector: 'input' });
    fireEvent.change(addSourceInput, { target: { value: 'street, city' } });
    fireEvent.keyDown(addSourceInput, { key: 'Enter' });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sourceColumns: ['street', 'city'],
      })
    );

    handleChange.mockClear();
    rerender(
      <NestedStructArrayOpsProperties
        node={createNode({
          operationMode: 'pack_struct',
          outputStructColumn: 'address_struct',
          sourceColumns: ['street', 'city'],
          dropPackedSourceColumns: false,
        })}
        onChange={handleChange}
      />
    );

    expect(screen.queryByRole('alert')).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText('Pack source column 1'), {
      target: { value: 'line_1' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sourceColumns: ['line_1', 'city'],
      })
    );

    handleChange.mockClear();
    fireEvent.click(screen.getByRole('button', { name: 'Remove pack source column city' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sourceColumns: ['street'],
      })
    );

    handleChange.mockClear();
    fireEvent.click(
      screen.getByLabelText(/Drop Packed Source Columns/i, { selector: 'input' })
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        dropPackedSourceColumns: true,
      })
    );
  });

  it('manages array_higher_order rules across transform, filter, exists, forall, and aggregate', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <NestedStructArrayOpsProperties
        node={createNode({
          operationMode: 'array_higher_order',
          arrayRules: [
            {
              sourceArrayColumn: 'tags',
              targetColumn: 'clean_tags',
              functionType: 'transform',
              lambdaExpression: 'x -> lower(trim(x))',
            },
          ],
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByTestId('array-hof-section')).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();

    // Add a second rule
    fireEvent.click(screen.getByRole('button', { name: /Add Array Rule/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        arrayRules: [
          {
            sourceArrayColumn: 'tags',
            targetColumn: 'clean_tags',
            functionType: 'transform',
            lambdaExpression: 'x -> lower(trim(x))',
          },
          {
            sourceArrayColumn: '',
            targetColumn: '',
            functionType: 'transform',
            lambdaExpression: 'x -> x',
          },
        ],
      })
    );

    // Switch rule 1 to aggregate and verify aggregate extra fields & validation
    handleChange.mockClear();
    fireEvent.change(screen.getByLabelText('Array rule 1 function type'), {
      target: { value: 'aggregate' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        arrayRules: [
          expect.objectContaining({
            functionType: 'aggregate',
            initialValueExpr: '',
          }),
        ],
      })
    );

    handleChange.mockClear();
    rerender(
      <NestedStructArrayOpsProperties
        node={createNode({
          operationMode: 'array_higher_order',
          arrayRules: [
            {
              sourceArrayColumn: 'prices',
              targetColumn: 'total_price',
              functionType: 'aggregate',
              lambdaExpression: '(acc, x) -> acc + x',
              initialValueExpr: '',
            },
            {
              sourceArrayColumn: 'prices',
              targetColumn: 'positive_prices',
              functionType: 'filter',
              lambdaExpression: 'x -> x > 0',
            },
            {
              sourceArrayColumn: 'prices',
              targetColumn: 'has_high',
              functionType: 'exists',
              lambdaExpression: 'x -> x >= 100',
            },
            {
              sourceArrayColumn: 'prices',
              targetColumn: 'all_valid',
              functionType: 'forall',
              lambdaExpression: 'x -> x IS NOT NULL',
            },
          ],
        })}
        onChange={handleChange}
      />
    );

    // Initial value is empty on rule 1 -> alert rendered
    expect(screen.getByRole('alert')).toHaveTextContent(
      /Initial accumulator expression is required for aggregate/i
    );

    fireEvent.change(screen.getByLabelText('Array rule 1 initial value'), {
      target: { value: 'CAST(0.0 AS DOUBLE)' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        arrayRules: expect.arrayContaining([
          expect.objectContaining({
            functionType: 'aggregate',
            initialValueExpr: 'CAST(0.0 AS DOUBLE)',
          }),
        ]),
      })
    );

    handleChange.mockClear();
    fireEvent.change(screen.getByLabelText('Array rule 1 finish lambda'), {
      target: { value: 'acc -> round(acc, 2)' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        arrayRules: expect.arrayContaining([
          expect.objectContaining({
            finishLambdaExpr: 'acc -> round(acc, 2)',
          }),
        ]),
      })
    );

    // Remove rule 4
    handleChange.mockClear();
    fireEvent.click(screen.getByRole('button', { name: 'Remove array rule 4' }));
    expect(handleChange.mock.calls[0][0].arrayRules).toHaveLength(3);

    // Update system instructions
    handleChange.mockClear();
    const instructions = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(instructions, { target: { value: 'Preserve null arrays as empty arrays' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Preserve null arrays as empty arrays',
      })
    );
  });
});
