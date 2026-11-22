/**
 * @fileoverview Unit tests for the CustomTransform step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { CustomTransformProperties } from './CustomTransform';

describe('CustomTransformProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-custom-1',
    data: {
      label: 'Custom Transform',
      spark_transform: 'CustomTransform',
      properties,
    },
  });

  it('renders defaults for functionName and preserveExistingColumns', () => {
    const handleChange = vi.fn();
    render(<CustomTransformProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByLabelText(/Helper Function Name/i, { selector: 'input' })).toHaveValue(
      'custom_transform'
    );
    expect(
      screen.getByLabelText(/Output Schema Hint/i, { selector: 'input' })
    ).toHaveValue('');
    expect(
      screen.getByLabelText(/Preserve Existing Columns/i, { selector: 'input' })
    ).toBeChecked();
    expect(screen.getByTestId('custom-transform-catalyst-banner')).toBeInTheDocument();
    expect(screen.getByTestId('custom-transform-dlq-banner')).toBeInTheDocument();
  });

  it('validates invalid Python functionName and updates outputSchemaHint and preserveExistingColumns', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <CustomTransformProperties
        node={createNode({ functionName: 'invalid-name!' })}
        onChange={handleChange}
      />
    );

    expect(
      screen.getByText('Function name must be a valid Python identifier.')
    ).toBeInTheDocument();

    rerender(
      <CustomTransformProperties
        node={createNode({ functionName: 'enrich_orders', preserveExistingColumns: true })}
        onChange={handleChange}
      />
    );

    fireEvent.change(
      screen.getByLabelText(/Output Schema Hint/i, { selector: 'input' }),
      { target: { value: 'order_id string, net_total double' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        functionName: 'enrich_orders',
        outputSchemaHint: 'order_id string, net_total double',
        preserveExistingColumns: true,
      })
    );

    handleChange.mockClear();
    fireEvent.click(screen.getByLabelText(/Preserve Existing Columns/i, { selector: 'input' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        functionName: 'enrich_orders',
        preserveExistingColumns: false,
      })
    );
  });

  it('updates prominent SystemInstructionsField prompt configuration', () => {
    const handleChange = vi.fn();
    render(<CustomTransformProperties node={createNode()} onChange={handleChange} />);

    const promptTextarea = screen.getByLabelText(
      /Custom Transformation Prompt & Instructions/i,
      { selector: 'textarea' }
    );
    expect(promptTextarea).toHaveAttribute('rows', '6');

    fireEvent.change(promptTextarea, {
      target: { value: 'Compute net_revenue = gross_revenue - tax_amount and trim currency' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        functionName: 'custom_transform',
        preserveExistingColumns: true,
        system_instructions:
          'Compute net_revenue = gross_revenue - tax_amount and trim currency',
      })
    );
  });
});
