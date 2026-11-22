/**
 * @fileoverview Unit tests for HCatalogIOSinkProperties (`HiveSinkProperties`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x `HiveSink` form rendering, `HiveSinkProperties` alias export,
 * required `table` validation, optional `metastoreUri` Thrift format warning,
 * `writeMethod` (`saveAsTable` vs `insertInto`), `saveMode`, `fileFormat`,
 * `partitionCols`, `dynamicPartitionOverwrite`, advanced bucketing and external path fields,
 * and `system_instructions`.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { HCatalogIOSinkProperties, HiveSinkProperties } from './HCatalogIO';

describe('HCatalogIOSinkProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports HiveSinkProperties as an alias for HCatalogIOSinkProperties', () => {
    expect(HiveSinkProperties).toBe(HCatalogIOSinkProperties);
  });

  it('renders defaults with database=default and validates required Table without requiring Metastore URI', () => {
    render(
      <HCatalogIOSinkProperties
        node={{ data: { label: 'Hive Sink', spark_transform: 'HiveSink', properties: {} } }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByText('BATCH / FOREACHBATCH')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('default')).toHaveValue('default');
    expect(screen.getByText('Table is required')).toBeInTheDocument();
    expect(screen.getByLabelText(/Write Method/i, { selector: 'select' })).toHaveValue('saveAsTable');
    expect(screen.getByLabelText(/Save Mode/i, { selector: 'select' })).toHaveValue('append');
    expect(screen.getByLabelText(/Table Storage Format/i, { selector: 'select' })).toHaveValue('parquet');
  });

  it('displays warning when non-empty Metastore URI does not start with thrift://', () => {
    render(
      <HCatalogIOSinkProperties
        node={{
          data: {
            properties: {
              table: 'daily_aggregates',
              metastoreUri: 'http://localhost:9083',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText(/Warning: Hive Metastore URIs typically start with thrift:\/\//i)
    ).toBeInTheDocument();
  });

  it('hides Table Storage Format in insertInto mode and exposes dynamicPartitionOverwrite in overwrite mode', () => {
    render(
      <HCatalogIOSinkProperties
        node={{
          data: {
            properties: {
              database: 'sales_db',
              table: 'daily_aggregates',
              writeMethod: 'insertInto',
              saveMode: 'overwrite',
              dynamicPartitionOverwrite: true,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.queryByLabelText(/Table Storage Format/i, { selector: 'select' })).not.toBeInTheDocument();
    const dynamicToggle = screen.getByLabelText(/Dynamic Partition Overwrite/i, { selector: 'input' });
    expect(dynamicToggle).toBeChecked();
  });

  it('dispatches updates for database, table, partitionCols, bucketing options, and system_instructions', () => {
    render(
      <HCatalogIOSinkProperties
        node={{
          data: {
            properties: {
              table: 'daily_aggregates',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.change(screen.getByPlaceholderText('default'), {
      target: { name: 'database', value: 'analytics' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ database: 'analytics' }));

    fireEvent.change(screen.getByPlaceholderText('ds, region'), {
      target: { name: 'partitionCols', value: 'event_date, country' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionCols: 'event_date, country' })
    );

    fireEvent.click(screen.getByRole('button', { name: /Bucketing, External Path & Advanced Settings/i }));

    fireEvent.change(screen.getByPlaceholderText('32'), {
      target: { name: 'numBuckets', value: '64' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ numBuckets: '64' }));

    fireEvent.change(screen.getByPlaceholderText('customer_id'), {
      target: { name: 'bucketCols', value: 'user_id' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ bucketCols: 'user_id' }));

    fireEvent.change(screen.getByPlaceholderText('event_timestamp'), {
      target: { name: 'sortCols', value: 'created_at' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ sortCols: 'created_at' }));

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }), {
      target: { value: 'Compact partitions to 4 files before saveAsTable' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Compact partitions to 4 files before saveAsTable',
      })
    );
  });
});
