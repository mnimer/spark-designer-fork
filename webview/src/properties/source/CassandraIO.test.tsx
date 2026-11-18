/**
 * Unit Tests for CassandraIOSourceProperties Component (SRC34)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates CassandraIOSourceProperties rendering, contactPoints/port/localDc/keyspace/table/cqlFilter,
 * SecretManagerAuthFields integration, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { CassandraIOSourceProperties, CassandraSourceProperties } from './CassandraIO';

describe('CassandraIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports CassandraSourceProperties alias and validates required fields when empty', () => {
    expect(CassandraSourceProperties).toBe(CassandraIOSourceProperties);
    render(<CassandraIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Contact points are required')).toBeInTheDocument();
    expect(screen.getByText('Keyspace is required')).toBeInTheDocument();
    expect(screen.getByText('Table is required')).toBeInTheDocument();
    expect(screen.getByDisplayValue('9042')).toBeInTheDocument();
  });

  it('hydrates contactPoints, port, localDc, keyspace, table, and cqlFilter and dispatches updates', () => {
    const node = {
      data: {
        properties: {
          contactPoints: '10.0.0.1,10.0.0.2',
          port: 9042,
          localDc: 'dc-central1',
          keyspace: 'telemetry',
          table: 'device_events',
          cqlFilter: "status = 'ACTIVE'",
        },
      },
    };

    const { container } = render(<CassandraIOSourceProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByDisplayValue('10.0.0.1,10.0.0.2')).toBeInTheDocument();
    expect(screen.getByDisplayValue('dc-central1')).toBeInTheDocument();
    expect(screen.getByDisplayValue('telemetry')).toBeInTheDocument();
    expect(screen.getByDisplayValue('device_events')).toBeInTheDocument();
    expect(screen.getByDisplayValue("status = 'ACTIVE'")).toBeInTheDocument();

    const filterInput = container.querySelector('#cassandraSourceCqlFilter') as HTMLInputElement;
    fireEvent.change(filterInput, { target: { name: 'cqlFilter', value: "region = 'us'" } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ cqlFilter: "region = 'us'" })
    );

    const sysInput = container.querySelector('#cassandraSourceInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Split by token ring' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Split by token ring' })
    );
  });
});
