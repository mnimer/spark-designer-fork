/**
 * Unit Tests for ClickHouseIOSourceProperties Component (SRC30)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates ClickHouseIOSourceProperties rendering, host/jdbcUrl/port/database,
 * table vs query mode, partitionColumn, numPartitions, SecretManagerAuthFields,
 * and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ClickHouseIOSourceProperties, ClickHouseSourceProperties } from './ClickHouseIO';

describe('ClickHouseIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports ClickHouseSourceProperties alias and validates required fields when empty', () => {
    expect(ClickHouseSourceProperties).toBe(ClickHouseIOSourceProperties);
    render(<ClickHouseIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Host or JDBC URL is required')).toBeInTheDocument();
    expect(screen.getByText('Table name is required in Table mode')).toBeInTheDocument();
  });

  it('hydrates host, port, jdbcUrl, database, table, partitionColumn, and numPartitions', () => {
    const node = {
      data: {
        properties: {
          host: 'ch.internal',
          port: 8123,
          jdbcUrl: 'jdbc:clickhouse://ch.internal:8123/analytics',
          database: 'analytics',
          table: 'events',
          partitionColumn: 'event_id',
          numPartitions: 16,
        },
      },
    };

    const { container } = render(<ClickHouseIOSourceProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByDisplayValue('ch.internal')).toBeInTheDocument();
    expect(screen.getByDisplayValue('8123')).toBeInTheDocument();
    expect(screen.getByDisplayValue('jdbc:clickhouse://ch.internal:8123/analytics')).toBeInTheDocument();
    expect(screen.getByDisplayValue('analytics')).toBeInTheDocument();
    expect(screen.getByDisplayValue('events')).toBeInTheDocument();
    expect(screen.getByDisplayValue('event_id')).toBeInTheDocument();
    expect(screen.getByDisplayValue('16')).toBeInTheDocument();

    const partInput = container.querySelector('#clickhouseSourcePartitionColumn') as HTMLInputElement;
    fireEvent.change(partInput, { target: { name: 'partitionColumn', value: 'user_id' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ partitionColumn: 'user_id' }));
  });

  it('switches to query mode and updates query and system_instructions', () => {
    const { container } = render(
      <ClickHouseIOSourceProperties
        node={{
          data: {
            properties: {
              host: 'ch.internal',
              readTargetMode: 'query',
              query: 'SELECT * FROM events',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const queryInput = container.querySelector('#clickhouseSourceQuery') as HTMLTextAreaElement;
    expect(queryInput).toHaveValue('SELECT * FROM events');

    fireEvent.change(queryInput, { target: { name: 'query', value: 'SELECT id FROM events' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ query: 'SELECT id FROM events' }));

    const sysInput = container.querySelector('#clickhouseSourceInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Use ClickHouse Spark V2' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Use ClickHouse Spark V2' })
    );
  });
});
