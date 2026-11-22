/**
 * @fileoverview Unit Tests for CassandraIOSinkProperties Component (SNK31).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates CassandraIOSinkProperties rendering, contactPoints/keyspace/table validation,
 * connectorBackend ('python_driver' | 'jvm_connector') and consistencyLevel selection,
 * ttlColumn and writetimeColumn configuration, SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { CassandraIOSinkProperties, CassandraSinkProperties } from './CassandraIO';

describe('CassandraIOSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports CassandraSinkProperties alias and validates required contactPoints, keyspace, and table', () => {
    expect(CassandraSinkProperties).toBe(CassandraIOSinkProperties);
    render(
      <CassandraIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    expect(screen.getByText('Contact points are required')).toBeInTheDocument();
    expect(screen.getByText('Keyspace is required')).toBeInTheDocument();
    expect(screen.getByText('Table is required')).toBeInTheDocument();
    expect(screen.getByDisplayValue('9042')).toBeInTheDocument();
    expect(screen.getByDisplayValue('100')).toBeInTheDocument();
  });

  it('hydrates Cassandra sink properties including TTL, writetime, consistencyLevel, and backend', () => {
    const node = {
      data: {
        properties: {
          contactPoints: '10.0.0.1,10.0.0.2',
          port: 9042,
          localDc: 'dc-west1',
          ssl: true,
          keyspace: 'telemetry',
          table: 'device_rollups',
          connectorBackend: 'jvm_connector' as const,
          consistencyLevel: 'QUORUM' as const,
          ttlColumn: '86400',
          writetimeColumn: 'event_micros',
          concurrency: 200,
          username: 'cql_writer',
          authMode: 'secretManager',
          secretName: 'cassandra-pass-secret',
        },
      },
    };

    const { container } = render(
      <CassandraIOSinkProperties node={node} onChange={mockOnChange} />
    );

    expect(screen.getByDisplayValue('10.0.0.1,10.0.0.2')).toBeInTheDocument();
    expect(screen.getByDisplayValue('dc-west1')).toBeInTheDocument();
    expect(container.querySelector('#cassandraSinkSsl')).toBeChecked();
    expect(screen.getByDisplayValue('telemetry')).toBeInTheDocument();
    expect(screen.getByDisplayValue('device_rollups')).toBeInTheDocument();
    expect(container.querySelector('#cassandraSinkConnectorBackend')).toHaveValue('jvm_connector');
    expect(container.querySelector('#cassandraSinkConsistencyLevel')).toHaveValue('QUORUM');
    expect(screen.getByDisplayValue('86400')).toBeInTheDocument();
    expect(screen.getByDisplayValue('event_micros')).toBeInTheDocument();
    expect(screen.getByDisplayValue('200')).toBeInTheDocument();
    expect(screen.getByDisplayValue('cql_writer')).toBeInTheDocument();
    expect(screen.getByDisplayValue('cassandra-pass-secret')).toBeInTheDocument();
  });

  it('dispatches updates for connectorBackend, consistencyLevel, ttlColumn, and writetimeColumn', () => {
    const { container } = render(
      <CassandraIOSinkProperties
        node={{
          data: {
            properties: {
              contactPoints: '10.0.0.1',
              keyspace: 'ks',
              table: 'tbl',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const backendSelect = container.querySelector(
      '#cassandraSinkConnectorBackend'
    ) as HTMLSelectElement;
    fireEvent.change(backendSelect, {
      target: { name: 'connectorBackend', value: 'jvm_connector' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ connectorBackend: 'jvm_connector' })
    );

    const consistencySelect = container.querySelector(
      '#cassandraSinkConsistencyLevel'
    ) as HTMLSelectElement;
    fireEvent.change(consistencySelect, {
      target: { name: 'consistencyLevel', value: 'LOCAL_ONE' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ consistencyLevel: 'LOCAL_ONE' })
    );

    const ttlInput = container.querySelector('#cassandraSinkTtlColumn') as HTMLInputElement;
    fireEvent.change(ttlInput, { target: { name: 'ttlColumn', value: 'ttl_sec' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ ttlColumn: 'ttl_sec' }));

    const writetimeInput = container.querySelector(
      '#cassandraSinkWritetimeColumn'
    ) as HTMLInputElement;
    fireEvent.change(writetimeInput, { target: { name: 'writetimeColumn', value: 'ts_micros' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ writetimeColumn: 'ts_micros' })
    );

    const instructionsInput = container.querySelector(
      '#cassandraSinkInstructions'
    ) as HTMLTextAreaElement;
    fireEvent.change(instructionsInput, {
      target: { value: 'Partition writes by token key' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Partition writes by token key' })
    );
  });
});
