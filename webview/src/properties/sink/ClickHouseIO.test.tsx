/**
 * Unit Tests for ClickHouseIOSinkProperties Component (SNK27)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates ClickHouseIOSinkProperties rendering, required endpoint/table validation,
 * MergeTree tiny-batch warning (< 1000) and default batchsize=50000, isolationLevel=NONE,
 * rewriteBatchedStatements and asyncInsert toggles, overwrite truncate toggle, legacy alias
 * cleanup, SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ClickHouseIOSinkProperties, ClickHouseSinkProperties } from './ClickHouseIO';

describe('ClickHouseIOSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports ClickHouseSinkProperties alias, validates required fields, and sets MergeTree defaults', () => {
    expect(ClickHouseSinkProperties).toBe(ClickHouseIOSinkProperties);

    const { container } = render(
      <ClickHouseIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    expect(screen.getByText('Host or JDBC URL is required')).toBeInTheDocument();
    expect(screen.getByText('Target table name is required')).toBeInTheDocument();
    expect(container.querySelector('#clickhouseSinkBatchsize')).toHaveValue(50000);
    expect(container.querySelector('#clickhouseSinkIsolationLevel')).toHaveValue('NONE');
    expect(container.querySelector('#clickhouseSinkRewriteBatched')).toBeChecked();
    expect(container.querySelector('#clickhouseSinkAsyncInsert')).not.toBeChecked();
  });

  it('warns when batchsize is tiny (< 1000) or when jdbcUrl lacks jdbc:clickhouse: prefix', () => {
    const { rerender } = render(
      <ClickHouseIOSinkProperties
        node={{
          data: {
            properties: {
              jdbcUrl: 'http://ch-host:8123/default',
              table: 'events',
              batchsize: 250,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText(/Warning: ClickHouse JDBC URL should start with "jdbc:clickhouse:"/i)
    ).toBeInTheDocument();
    expect(
      screen.getByText(/Warning: Tiny batch sizes \(< 1000\) create excessive MergeTree parts/i)
    ).toBeInTheDocument();

    rerender(
      <ClickHouseIOSinkProperties
        node={{
          data: {
            properties: {
              jdbcUrl: 'jdbc:clickhouse://ch-host:8443/default?ssl=true',
              table: 'events',
              batchsize: 50000,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('hydrates legacy aliases and dispatches updates for saveMode, asyncInsert, auth, and instructions', () => {
    const { container } = render(
      <ClickHouseIOSinkProperties
        node={{
          data: {
            properties: {
              url: 'jdbc:clickhouse://ch-prod:8443/analytics?ssl=true',
              tableName: 'metrics_rollup',
              maxInsertBlockSize: 25000,
              saveMode: 'overwrite',
              authMode: 'secretManager',
              passwordSecret: 'ch-sink-secret',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#clickhouseSinkJdbcUrl')).toHaveValue(
      'jdbc:clickhouse://ch-prod:8443/analytics?ssl=true'
    );
    expect(container.querySelector('#clickhouseSinkTable')).toHaveValue('metrics_rollup');
    expect(container.querySelector('#clickhouseSinkBatchsize')).toHaveValue(25000);
    expect(container.querySelector('#clickhouseSinkTruncate')).toBeChecked();
    expect(container.querySelector('#clickhouseSinkSecretName')).toHaveValue('ch-sink-secret');

    fireEvent.change(container.querySelector('#clickhouseSinkTable') as HTMLInputElement, {
      target: { name: 'table', value: 'metrics_v2' },
    });
    const updated = mockOnChange.mock.calls[0][0];
    expect(updated.table).toBe('metrics_v2');
    expect(updated.tableName).toBeUndefined();

    fireEvent.click(container.querySelector('#clickhouseSinkAsyncInsert') as HTMLInputElement);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ asyncInsert: true }));

    fireEvent.change(
      container.querySelector('#clickhouseSinkInstructions') as HTMLTextAreaElement,
      { target: { value: 'Partition inserts by toYYYYMM(event_time)' } }
    );
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Partition inserts by toYYYYMM(event_time)',
      })
    );
  });
});
