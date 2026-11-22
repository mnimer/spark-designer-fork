/**
 * Unit Tests for HCatalogIOSourceProperties (`HiveSourceProperties`) Component
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates HCatalogIOSourceProperties (`HiveSourceProperties`) form rendering, hydration from properties,
 * input change dispatches for database, table, filterPredicate, and metastoreUri,
 * Thrift URI format warning, advanced settings collapse, and code generation instructions.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { HCatalogIOSourceProperties, HiveSourceProperties } from './HCatalogIO';

describe('HCatalogIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports HiveSourceProperties as an alias for HCatalogIOSourceProperties', () => {
    expect(HiveSourceProperties).toBe(HCatalogIOSourceProperties);
  });

  it('renders default database=default and alerts when required Table field is empty', () => {
    const node = {
      data: {
        label: 'Hive Source',
        properties: {},
      },
    };

    render(<HCatalogIOSourceProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByText('BATCH ONLY (spark.table)')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('default')).toHaveValue('default');
    expect(screen.getByPlaceholderText('my_table')).toBeInTheDocument();
    expect(screen.getByText('Table is required')).toBeInTheDocument();
  });

  it('displays warning when Metastore URI does not start with thrift://', () => {
    const node = {
      data: {
        label: 'Hive Source',
        properties: {
          table: 'events',
          metastoreUri: 'http://metastore.example.com:9083',
        },
      },
    };

    render(<HCatalogIOSourceProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByText(/Warning: Hive Metastore URIs typically start with thrift:\/\//i)).toBeInTheDocument();
  });

  it('hydrates form fields and dispatches updates for database, table, filterPredicate, and metastoreUri', () => {
    const node = {
      data: {
        label: 'Hive Source',
        properties: {
          database: 'warehouse',
          table: 'events_log',
          filterPredicate: "ds = '2026-09-23'",
          metastoreUri: 'thrift://hive-meta.corp:9083',
          partitionCols: 'ds, hr',
          configProperties: 'spark.sql.hive.metastorePartitionPruning=true',
          system_instructions: 'Extract web events from Hive table',
        },
      },
    };

    render(<HCatalogIOSourceProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByPlaceholderText('default')).toHaveValue('warehouse');
    expect(screen.getByPlaceholderText('my_table')).toHaveValue('events_log');
    expect(screen.getByPlaceholderText(/ds = '2026-09-23'/i)).toHaveValue("ds = '2026-09-23'");
    expect(screen.getByPlaceholderText('thrift://localhost:9083')).toHaveValue('thrift://hive-meta.corp:9083');

    fireEvent.change(screen.getByPlaceholderText('default'), {
      target: { name: 'database', value: 'analytics' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        readMode: 'batch',
        database: 'analytics',
      })
    );

    fireEvent.change(screen.getByPlaceholderText('my_table'), {
      target: { name: 'table', value: 'user_actions' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'user_actions',
      })
    );

    fireEvent.change(screen.getByPlaceholderText(/ds = '2026-09-23'/i), {
      target: { name: 'filterPredicate', value: 'year = 2026' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        filterPredicate: 'year = 2026',
        filter: 'year = 2026',
      })
    );

    fireEvent.change(screen.getByPlaceholderText('thrift://localhost:9083'), {
      target: { name: 'metastoreUri', value: 'thrift://prod-meta:9083' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        metastoreUri: 'thrift://prod-meta:9083',
      })
    );

    fireEvent.click(screen.getByRole('button', { name: /Advanced Hive Settings/i }));
    expect(screen.getByPlaceholderText(/ds, region/i)).toHaveValue('ds, hr');

    fireEvent.change(screen.getByPlaceholderText(/Read partitioned Hive table/i), {
      target: { value: 'Updated prompt for HiveSource' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Updated prompt for HiveSource',
      })
    );
  });
});
