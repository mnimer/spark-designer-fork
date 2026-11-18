/**
 * Unit Tests for SingleStoreDBIOSinkProperties Component (SNK28)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates SingleStoreDBIOSinkProperties rendering, required ddlEndpoint/database/table
 * validation, writeFormat switching (singlestore vs jdbc fallback), loadDataCompression,
 * overwriteBehavior (truncate, dropAndCreate, merge), onDuplicateKeySQL upsert clause,
 * legacy alias cleanup, SecretManagerAuthFields, and system instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SingleStoreDBIOSinkProperties, SingleStoreSinkProperties } from './SingleStoreDBIO';

describe('SingleStoreDBIOSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports SingleStoreSinkProperties alias and validates required ddlEndpoint, database, and table', () => {
    expect(SingleStoreSinkProperties).toBe(SingleStoreDBIOSinkProperties);

    const { container } = render(
      <SingleStoreDBIOSinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    expect(screen.getByText('DDL endpoint host:port is required')).toBeInTheDocument();
    expect(screen.getByText('Database name is required')).toBeInTheDocument();
    expect(screen.getByText('Destination table is required')).toBeInTheDocument();
    expect(container.querySelector('#singleStoreSinkWriteFormat')).toHaveValue('singlestore');
    expect(container.querySelector('#singleStoreSinkSaveMode')).toHaveValue('append');
    expect(container.querySelector('#singleStoreSinkOverwriteBehavior')).toHaveValue('truncate');
    expect(container.querySelector('#singleStoreSinkLoadDataCompression')).toHaveValue('LZ4');
  });

  it('updates overwriteBehavior, loadDataCompression, and onDuplicateKeySQL in native singlestore mode', () => {
    const { container } = render(
      <SingleStoreDBIOSinkProperties
        node={{
          data: {
            properties: {
              ddlEndpoint: 'svc-s2.internal:3306',
              database: 'telemetry_db',
              table: 'sensor_rollups',
              saveMode: 'overwrite',
              overwriteBehavior: 'merge',
              loadDataCompression: 'GZip',
              onDuplicateKeySQL: 'metric_val = VALUES(metric_val)',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#singleStoreSinkOverwriteBehavior')).toHaveValue('merge');
    expect(container.querySelector('#singleStoreSinkLoadDataCompression')).toHaveValue('GZip');
    expect(container.querySelector('#singleStoreSinkOnDuplicateKeySQL')).toHaveValue(
      'metric_val = VALUES(metric_val)'
    );

    fireEvent.change(
      container.querySelector('#singleStoreSinkOverwriteBehavior') as HTMLSelectElement,
      { target: { name: 'overwriteBehavior', value: 'dropAndCreate' } }
    );
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ overwriteBehavior: 'dropAndCreate' })
    );

    fireEvent.change(
      container.querySelector('#singleStoreSinkOnDuplicateKeySQL') as HTMLTextAreaElement,
      {
        target: {
          name: 'onDuplicateKeySQL',
          value: 'metric_val = VALUES(metric_val), ts = VALUES(ts)',
        },
      }
    );
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        onDuplicateKeySQL: 'metric_val = VALUES(metric_val), ts = VALUES(ts)',
      })
    );
  });

  it('switches to jdbc fallback format, cleans legacy endpoint/passwordSecret aliases, and updates instructions', () => {
    const { container, rerender } = render(
      <SingleStoreDBIOSinkProperties
        node={{
          data: {
            properties: {
              endpoint: 'legacy-s2.internal:3306',
              database: 'iot_db',
              table: 'events',
              authMode: 'secretManager',
              passwordSecret: 's2-sink-pw',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#singleStoreSinkDdlEndpoint')).toHaveValue(
      'legacy-s2.internal:3306'
    );
    expect(container.querySelector('#singleStoreSinkSecretName')).toHaveValue('s2-sink-pw');

    fireEvent.change(container.querySelector('#singleStoreSinkDdlEndpoint') as HTMLInputElement, {
      target: { name: 'ddlEndpoint', value: 's2-prod.internal:3306' },
    });
    const updated = mockOnChange.mock.calls[0][0];
    expect(updated.ddlEndpoint).toBe('s2-prod.internal:3306');
    expect(updated.endpoint).toBeUndefined();

    rerender(
      <SingleStoreDBIOSinkProperties
        node={{
          data: {
            properties: {
              ddlEndpoint: 's2-prod.internal:3306',
              database: 'iot_db',
              table: 'events',
              writeFormat: 'jdbc',
              batchsize: 8000,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#singleStoreSinkBatchsize')).toHaveValue(8000);
    expect(container.querySelector('#singleStoreSinkOnDuplicateKeySQL')).not.toBeInTheDocument();

    fireEvent.change(
      container.querySelector('#singleStoreSinkInstructions') as HTMLTextAreaElement,
      { target: { value: 'Use LZ4 compression for parallel leaf ingest' } }
    );
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Use LZ4 compression for parallel leaf ingest',
      })
    );
  });
});
