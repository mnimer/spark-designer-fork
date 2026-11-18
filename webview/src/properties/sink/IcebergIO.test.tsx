/**
 * @fileoverview Unit tests for IcebergIOSinkProperties (`IcebergSinkProperties`).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x `IcebergSink` form rendering, BigLake REST catalog discovery,
 * write operations (`append`, `createOrReplace`, `overwritePartitions`, `merge`),
 * merge key and condition validation, partition specs, fanout toggle, table properties,
 * Secret Manager authentication, and system instructions.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  IcebergIOSinkProperties,
  IcebergSinkProperties,
  ICEBERG_CATALOG_TYPES,
  ICEBERG_WRITE_OPERATION_OPTIONS,
} from './IcebergIO';

describe('IcebergIOSinkProperties', () => {
  let originalVscode: unknown;
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    originalVscode = (window as unknown as Record<string, unknown>).vscode;
  });

  afterEach(() => {
    (window as unknown as Record<string, unknown>).vscode = originalVscode;
  });

  it('exports IcebergSinkProperties alias and all required catalog and write operation options', () => {
    expect(IcebergSinkProperties).toBe(IcebergIOSinkProperties);
    expect(ICEBERG_CATALOG_TYPES.map((o) => o.value)).toEqual(
      expect.arrayContaining(['biglake', 'rest', 'hive', 'hadoop'])
    );
    expect(ICEBERG_WRITE_OPERATION_OPTIONS.map((o) => o.value)).toEqual([
      'append',
      'createOrReplace',
      'overwritePartitions',
      'merge',
    ]);
  });

  it('renders default values and flags validation errors when table and warehouseLocation are empty', () => {
    const { container } = render(
      <IcebergIOSinkProperties
        node={{ data: { label: 'Iceberg Sink', spark_transform: 'IcebergSink', properties: {} } }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#icebergSinkWriteOperation')).toHaveValue('append');
    expect(container.querySelector('#icebergSinkCatalogType')).toHaveValue('biglake');

    const tableInput = container.querySelector('#icebergSinkTable') as HTMLInputElement;
    expect(tableInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Table is required.')).toBeInTheDocument();

    const warehouseInput = container.querySelector('#icebergSinkWarehouseLocation') as HTMLInputElement;
    expect(warehouseInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Warehouse Location is required.')).toBeInTheDocument();
  });

  it('validates merge keys and merge condition when writeOperation is merge', () => {
    const { container, rerender } = render(
      <IcebergIOSinkProperties
        node={{
          data: {
            properties: {
              table: 'orders',
              warehouseLocation: 'gs://lakehouse/warehouse',
              writeOperation: 'merge',
              mergeKeys: '',
              mergeCondition: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText('Provide Merge Key Columns or a Custom Merge ON Condition when Write Operation is merge.')
    ).toBeInTheDocument();

    const mergeKeysInput = container.querySelector('#icebergSinkMergeKeys') as HTMLInputElement;
    fireEvent.change(mergeKeysInput, {
      target: { name: 'mergeKeys', value: 'order_id, tenant_id' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mergeKeys: 'order_id, tenant_id',
      })
    );

    rerender(
      <IcebergIOSinkProperties
        node={{
          data: {
            properties: {
              table: 'orders',
              warehouseLocation: 'gs://lakehouse/warehouse',
              writeOperation: 'merge',
              mergeCondition: 'target.order_id = source.order_id',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.queryByText('Provide Merge Key Columns or a Custom Merge ON Condition when Write Operation is merge.')
    ).not.toBeInTheDocument();
  });

  it('conditionally renders catalogUri for biglake, rest, and hive, and hides it for hadoop', () => {
    const { container, rerender } = render(
      <IcebergIOSinkProperties
        node={{ data: { properties: { catalogType: 'biglake' } } }}
        onChange={mockOnChange}
      />
    );
    expect(container.querySelector('#icebergSinkCatalogUri')).toBeInTheDocument();

    rerender(
      <IcebergIOSinkProperties
        node={{ data: { properties: { catalogType: 'rest' } } }}
        onChange={mockOnChange}
      />
    );
    expect(container.querySelector('#icebergSinkCatalogUri')).toBeInTheDocument();

    rerender(
      <IcebergIOSinkProperties
        node={{ data: { properties: { catalogType: 'hive' } } }}
        onChange={mockOnChange}
      />
    );
    expect(container.querySelector('#icebergSinkCatalogUri')).toBeInTheDocument();

    rerender(
      <IcebergIOSinkProperties
        node={{ data: { properties: { catalogType: 'hadoop' } } }}
        onChange={mockOnChange}
      />
    );
    expect(container.querySelector('#icebergSinkCatalogUri')).not.toBeInTheDocument();
  });

  it('dispatches updates for writeOperation, namespace, partitionFields, fanoutEnabled, tableProperties, and system_instructions', () => {
    const { container } = render(
      <IcebergIOSinkProperties
        node={{
          data: {
            properties: {
              table: 'events',
              warehouseLocation: 'gs://lakehouse/warehouse',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.change(container.querySelector('#icebergSinkWriteOperation') as HTMLSelectElement, {
      target: { name: 'writeOperation', value: 'overwritePartitions' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ writeOperation: 'overwritePartitions' })
    );

    fireEvent.change(container.querySelector('#icebergSinkNamespace') as HTMLInputElement, {
      target: { name: 'namespace', value: 'analytics' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ namespace: 'analytics' })
    );

    fireEvent.change(container.querySelector('#icebergSinkPartitionFields') as HTMLInputElement, {
      target: { name: 'partitionFields', value: 'days(event_time), region' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        partitionFields: 'days(event_time), region',
        partitionSpec: 'days(event_time), region',
      })
    );

    fireEvent.click(container.querySelector('#icebergSinkFanoutEnabled') as HTMLInputElement);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ fanoutEnabled: true })
    );

    fireEvent.click(screen.getByText('Table & Write Options'));
    fireEvent.change(container.querySelector('#icebergSinkTableProperties') as HTMLTextAreaElement, {
      target: { name: 'tableProperties', value: 'write.format.default=parquet' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ tableProperties: 'write.format.default=parquet' })
    );

    fireEvent.change(container.querySelector('#icebergSinkInstructions') as HTMLTextAreaElement, {
      target: { value: 'Partition by event day and enable zstd' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Partition by event day and enable zstd' })
    );
  });

  it('populates BigLake catalog datalist via useAssetLoader and surfaces catalog errors', () => {
    const mockPostMessage = vi.fn();
    (window as unknown as Record<string, unknown>).vscode = { postMessage: mockPostMessage };

    const { container } = render(
      <IcebergIOSinkProperties
        node={{
          data: {
            properties: {
              table: 'analytics.events_out',
              warehouseLocation: 'gs://lakehouse/warehouse',
              catalogType: 'biglake',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(mockPostMessage).toHaveBeenCalledWith({ command: 'getBigLakeCatalogs' });

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'bigLakeCatalogsLoaded',
            catalogs: [
              {
                name: 'prod_biglake',
                id: 'prod_biglake',
                value: 'prod_biglake',
                warehouse: 'gs://prod-warehouse',
              },
            ],
          },
        })
      );
    });

    const datalist = container.querySelector('#iceberg-sink-catalogs-list');
    expect(datalist?.querySelector('option[value="prod_biglake"]')).toBeInTheDocument();

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'bigLakeCatalogsLoaded',
            catalogs: [],
            error: 'BigLake API not enabled',
          },
        })
      );
    });

    expect(screen.getByText(/Failed to load BigLake catalogs: BigLake API not enabled/i)).toBeInTheDocument();
  });

  it('handles SecretManagerAuthFields interaction', () => {
    const { container } = render(
      <IcebergIOSinkProperties
        node={{
          data: {
            properties: {
              table: 'events',
              warehouseLocation: 'gs://lakehouse/warehouse',
              authMode: 'secretManager',
              secretName: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    const secretInput = container.querySelector('#icebergSinkSecretName') as HTMLInputElement;
    expect(secretInput).toBeInTheDocument();
    fireEvent.change(secretInput, {
      target: { name: 'secretName', value: 'iceberg-rest-token' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ secretName: 'iceberg-rest-token' })
    );
  });
});
