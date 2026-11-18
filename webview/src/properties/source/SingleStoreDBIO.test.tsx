/**
 * Unit Tests for SingleStoreDBIOSourceProperties Component (SRC31)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates SingleStoreDBIOSourceProperties rendering, ddlEndpoint/database/table/query
 * validation, enableParallelRead toggle, SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SingleStoreDBIOSourceProperties, SingleStoreSourceProperties } from './SingleStoreDBIO';

describe('SingleStoreDBIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports SingleStoreSourceProperties alias and validates required fields when empty', () => {
    expect(SingleStoreSourceProperties).toBe(SingleStoreDBIOSourceProperties);
    render(<SingleStoreDBIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('DDL endpoint host:port is required')).toBeInTheDocument();
    expect(screen.getByText('Database name is required')).toBeInTheDocument();
    expect(screen.getByText('Table name is required in Table mode')).toBeInTheDocument();
  });

  it('hydrates ddlEndpoint, database, table, and enableParallelRead and dispatches updates', () => {
    const node = {
      data: {
        properties: {
          ddlEndpoint: 'singlestore.prod:3306',
          database: 'iot_metrics',
          table: 'sensor_readings',
          enableParallelRead: true,
        },
      },
    };

    const { container } = render(
      <SingleStoreDBIOSourceProperties node={node} onChange={mockOnChange} />
    );

    expect(screen.getByDisplayValue('singlestore.prod:3306')).toBeInTheDocument();
    expect(screen.getByDisplayValue('iot_metrics')).toBeInTheDocument();
    expect(screen.getByDisplayValue('sensor_readings')).toBeInTheDocument();

    const parallelCheckbox = container.querySelector(
      '#singleStoreSourceEnableParallelRead'
    ) as HTMLInputElement;
    expect(parallelCheckbox).toBeChecked();

    fireEvent.click(parallelCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ enableParallelRead: false })
    );
  });

  it('switches to query mode and updates query and system_instructions', () => {
    const { container } = render(
      <SingleStoreDBIOSourceProperties
        node={{
          data: {
            properties: {
              ddlEndpoint: '127.0.0.1:3306',
              database: 'db',
              readTargetMode: 'query',
              query: 'SELECT * FROM sensor_readings',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const queryInput = container.querySelector('#singleStoreSourceQuery') as HTMLTextAreaElement;
    expect(queryInput).toHaveValue('SELECT * FROM sensor_readings');

    fireEvent.change(queryInput, { target: { name: 'query', value: 'SELECT id FROM sensor_readings' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ query: 'SELECT id FROM sensor_readings' })
    );

    const sysInput = container.querySelector('#singleStoreSourceInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Read from leaf nodes' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Read from leaf nodes' })
    );
  });
});
