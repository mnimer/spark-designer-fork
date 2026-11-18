/**
 * Unit Tests for SnowflakeIOSourceProperties Component (SRC28)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates SnowflakeIOSourceProperties rendering, sfURL/sfWarehouse/sfDatabase/sfSchema/sfRole,
 * dbtable vs query switching, SecretManagerAuthFields integration, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SnowflakeIOSourceProperties, SnowflakeSourceProperties } from './SnowflakeIO';

describe('SnowflakeIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports SnowflakeSourceProperties alias and validates required fields when empty', () => {
    expect(SnowflakeSourceProperties).toBe(SnowflakeIOSourceProperties);
    render(<SnowflakeIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('sfURL is required')).toBeInTheDocument();
    expect(screen.getByText('sfWarehouse is required')).toBeInTheDocument();
    expect(screen.getByText('sfDatabase is required')).toBeInTheDocument();
    expect(screen.getByText('dbtable is required in Table mode')).toBeInTheDocument();
  });

  it('hydrates sfURL, sfWarehouse, sfDatabase, sfSchema, sfRole, and dbtable and dispatches updates', () => {
    const node = {
      data: {
        properties: {
          sfURL: 'xy12345.us-central1.gcp.snowflakecomputing.com',
          sfWarehouse: 'COMPUTE_WH',
          sfDatabase: 'ANALYTICS_DB',
          sfSchema: 'PUBLIC',
          sfRole: 'DATA_ENG',
          dbtable: 'ORDERS',
          secretName: 'sf-secret',
        },
      },
    };

    const { container } = render(<SnowflakeIOSourceProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByDisplayValue('xy12345.us-central1.gcp.snowflakecomputing.com')).toBeInTheDocument();
    expect(screen.getByDisplayValue('COMPUTE_WH')).toBeInTheDocument();
    expect(screen.getByDisplayValue('ANALYTICS_DB')).toBeInTheDocument();
    expect(screen.getByDisplayValue('PUBLIC')).toBeInTheDocument();
    expect(screen.getByDisplayValue('DATA_ENG')).toBeInTheDocument();
    expect(screen.getByDisplayValue('ORDERS')).toBeInTheDocument();
    expect(screen.getByDisplayValue('sf-secret')).toBeInTheDocument();

    const roleInput = container.querySelector('#snowflakeSourceSfRole') as HTMLInputElement;
    fireEvent.change(roleInput, { target: { name: 'sfRole', value: 'ACCOUNTADMIN' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ sfRole: 'ACCOUNTADMIN' }));
  });

  it('switches to query mode and updates query and system_instructions', () => {
    const { container } = render(
      <SnowflakeIOSourceProperties
        node={{
          data: {
            properties: {
              sfURL: 'xy12345.snowflakecomputing.com',
              sfWarehouse: 'WH',
              sfDatabase: 'DB',
              readTargetMode: 'query',
              query: 'SELECT * FROM ORDERS',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const queryInput = container.querySelector('#snowflakeSourceQuery') as HTMLTextAreaElement;
    expect(queryInput).toHaveValue('SELECT * FROM ORDERS');

    fireEvent.change(queryInput, { target: { name: 'query', value: 'SELECT ID FROM ORDERS' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ query: 'SELECT ID FROM ORDERS' })
    );

    const sysInput = container.querySelector('#snowflakeSourceSystemInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Enable pushdown' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Enable pushdown' })
    );
  });
});
