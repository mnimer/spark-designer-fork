/**
 * IcebergIO Source Properties Test Suite
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating IcebergIOSourceProperties (`IcebergSourceProperties`) component functionality,
 * readMode switching (batch vs streaming incremental options), form change callbacks, validation states
 * for required fields (table and warehouseLocation), mutually exclusive time-travel validation,
 * conditional catalogUri rendering across all seven catalog types (`biglake`, `rest`, `hive`, `hadoop`,
 * `glue`, `nessie`, `bigquery`), exact option parity with `IcebergSink`, BigLake catalog discovery (D3),
 * Secret Manager authentication, and code generation instructions.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  IcebergIOSourceProperties,
  IcebergSourceProperties,
  ICEBERG_CATALOG_TYPES,
  type IcebergIOSourceNode,
} from './IcebergIO';
import { ICEBERG_CATALOG_TYPES as SINK_ICEBERG_CATALOG_TYPES } from '../sink/IcebergIO';

describe('IcebergIOSourceProperties', () => {
  let originalVscode: unknown;
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    originalVscode = (window as unknown as Record<string, unknown>).vscode;
  });

  afterEach(() => {
    (window as unknown as Record<string, unknown>).vscode = originalVscode;
  });

  it('exports IcebergSourceProperties as an alias and maintains exact catalog option parity with IcebergSink', () => {
    expect(IcebergSourceProperties).toBe(IcebergIOSourceProperties);
    expect(ICEBERG_CATALOG_TYPES.map((opt) => opt.value)).toEqual([
      'biglake',
      'rest',
      'hive',
      'hadoop',
      'glue',
      'nessie',
      'bigquery',
    ]);
    expect(ICEBERG_CATALOG_TYPES).toEqual(SINK_ICEBERG_CATALOG_TYPES);
  });

  it('renders with empty properties and flags validation error for missing table and warehouseLocation', () => {
    const mockNode: IcebergIOSourceNode = {
      data: {
        label: 'IcebergIO',
        spark_transform: 'IcebergIO',
        properties: {},
      },
    };

    const { container } = render(<IcebergIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(container.querySelector('#icebergSourceReadMode')).toHaveValue('batch');

    const tableInput = container.querySelector('#icebergSourceTable') as HTMLInputElement;
    expect(tableInput).toBeInTheDocument();
    expect(tableInput).toHaveValue('');
    expect(tableInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Table is required.')).toBeInTheDocument();

    const warehouseInput = container.querySelector('#icebergSourceWarehouseLocation') as HTMLInputElement;
    expect(warehouseInput).toBeInTheDocument();
    expect(warehouseInput).toHaveValue('');
    expect(warehouseInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Warehouse Location is required.')).toBeInTheDocument();
  });

  it('renders with populated properties and no validation error', () => {
    const mockNode: IcebergIOSourceNode = {
      data: {
        label: 'IcebergIO',
        spark_transform: 'IcebergIO',
        properties: {
          readMode: 'batch',
          namespace: 'lakehouse',
          table: 'lakehouse.events',
          catalogName: 'lakehouse',
          catalogType: 'rest',
          warehouseLocation: 'gs://lakehouse/iceberg/warehouse',
          catalogUri: 'http://rest-catalog:8181',
          snapshotId: '1234567890123456789',
          filterExpression: "status = 'ACTIVE'",
          columnProjection: 'id, timestamp, status',
          configProperties: 'io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO',
          system_instructions: 'Custom Iceberg reader instructions',
        },
      },
    };

    const { container } = render(<IcebergIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    const tableInput = container.querySelector('#icebergSourceTable') as HTMLInputElement;
    const namespaceInput = container.querySelector('#icebergSourceNamespace') as HTMLInputElement;
    const catalogNameInput = container.querySelector('#icebergSourceCatalogName') as HTMLInputElement;
    const catalogTypeSelect = container.querySelector('#icebergSourceCatalogType') as HTMLSelectElement;
    const warehouseInput = container.querySelector('#icebergSourceWarehouseLocation') as HTMLInputElement;
    const catalogUriInput = container.querySelector('#icebergSourceCatalogUri') as HTMLInputElement;
    const snapshotInput = container.querySelector('#icebergSourceSnapshotId') as HTMLInputElement;
    const filterInput = container.querySelector('#icebergSourceFilterExpression') as HTMLInputElement;
    const projectionInput = container.querySelector('#icebergSourceColumnProjection') as HTMLInputElement;
    const instructionsTextarea = container.querySelector('#icebergSourceInstructions') as HTMLTextAreaElement;

    expect(namespaceInput).toHaveValue('lakehouse');
    expect(tableInput).toHaveValue('lakehouse.events');
    expect(tableInput).toHaveAttribute('aria-invalid', 'false');
    expect(catalogNameInput).toHaveValue('lakehouse');
    expect(catalogTypeSelect).toHaveValue('rest');
    expect(warehouseInput).toHaveValue('gs://lakehouse/iceberg/warehouse');
    expect(warehouseInput).toHaveAttribute('aria-invalid', 'false');
    expect(catalogUriInput).toHaveValue('http://rest-catalog:8181');
    expect(snapshotInput).toHaveValue('1234567890123456789');
    expect(filterInput).toHaveValue("status = 'ACTIVE'");
    expect(projectionInput).toHaveValue('id, timestamp, status');
    expect(instructionsTextarea).toHaveValue('Custom Iceberg reader instructions');

    expect(screen.queryByText('Table is required.')).not.toBeInTheDocument();
    expect(screen.queryByText('Warehouse Location is required.')).not.toBeInTheDocument();
  });

  it('renders streaming incremental scan options when readMode is streaming', () => {
    const mockNode: IcebergIOSourceNode = {
      data: {
        properties: {
          readMode: 'streaming',
          table: 'analytics.events',
          warehouseLocation: 'gs://lakehouse/warehouse',
          streamingFromTimestamp: '1704067200000',
          streamingSkipDeleteSnapshots: true,
          streamingSkipOverwriteSnapshots: false,
        },
      },
    };

    render(<IcebergIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Streaming From Timestamp/i)).toHaveValue('1704067200000');
    expect(screen.getByLabelText(/Skip Delete Snapshots/i)).toBeChecked();
    expect(screen.getByLabelText(/Skip Overwrite Snapshots/i)).not.toBeChecked();

    fireEvent.click(screen.getByLabelText(/Skip Overwrite Snapshots/i));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        streamingSkipOverwriteSnapshots: true,
      })
    );
  });

  it('validates on/off for table and warehouseLocation independently', () => {
    const mockNodeWithTable: IcebergIOSourceNode = {
      data: {
        properties: {
          table: 'lakehouse.events',
        },
      },
    };
    const { rerender } = render(<IcebergIOSourceProperties node={mockNodeWithTable} onChange={mockOnChange} />);
    expect(screen.queryByText('Table is required.')).not.toBeInTheDocument();
    expect(screen.getByText('Warehouse Location is required.')).toBeInTheDocument();

    const mockNodeWithWarehouse: IcebergIOSourceNode = {
      data: {
        properties: {
          warehouseLocation: 'gs://lakehouse/iceberg/warehouse',
        },
      },
    };
    rerender(<IcebergIOSourceProperties node={mockNodeWithWarehouse} onChange={mockOnChange} />);
    expect(screen.getByText('Table is required.')).toBeInTheDocument();
    expect(screen.queryByText('Warehouse Location is required.')).not.toBeInTheDocument();
  });

  it('conditionally renders catalogUri for biglake, rest, hive, and nessie, and hides it with hints for hadoop, glue, and bigquery', () => {
    const mockNodeBigLake: IcebergIOSourceNode = {
      data: {
        properties: {
          catalogType: 'biglake',
        },
      },
    };
    const { container, rerender } = render(
      <IcebergIOSourceProperties node={mockNodeBigLake} onChange={mockOnChange} />
    );
    const bigLakeUriInput = container.querySelector('#icebergSourceCatalogUri') as HTMLInputElement;
    expect(bigLakeUriInput).toBeInTheDocument();
    expect(bigLakeUriInput.placeholder).toBe(
      'https://biglake.googleapis.com/iceberg/v1beta/restcatalog'
    );

    const mockNodeRest: IcebergIOSourceNode = {
      data: {
        properties: {
          catalogType: 'rest',
          catalogUri: 'http://iceberg-rest:8181',
        },
      },
    };
    rerender(<IcebergIOSourceProperties node={mockNodeRest} onChange={mockOnChange} />);
    expect(container.querySelector('#icebergSourceCatalogUri')).toBeInTheDocument();

    const mockNodeHive: IcebergIOSourceNode = {
      data: {
        properties: {
          catalogType: 'hive',
          catalogUri: 'thrift://metastore:9083',
        },
      },
    };
    rerender(<IcebergIOSourceProperties node={mockNodeHive} onChange={mockOnChange} />);
    const hiveUriInput = container.querySelector('#icebergSourceCatalogUri') as HTMLInputElement;
    expect(hiveUriInput).toBeInTheDocument();
    expect(hiveUriInput.placeholder).toBe('thrift://metastore:9083');

    const mockNodeNessie: IcebergIOSourceNode = {
      data: {
        properties: {
          catalogType: 'nessie',
        },
      },
    };
    rerender(<IcebergIOSourceProperties node={mockNodeNessie} onChange={mockOnChange} />);
    const nessieUriInput = container.querySelector('#icebergSourceCatalogUri') as HTMLInputElement;
    expect(nessieUriInput).toBeInTheDocument();
    expect(nessieUriInput.placeholder).toBe('http://nessie:19120/api/v1');
    expect(screen.getByText('NessieCatalog')).toBeInTheDocument();
    expect(screen.getByText(/Project Nessie API server/i)).toBeInTheDocument();

    const mockNodeHadoop: IcebergIOSourceNode = {
      data: {
        properties: {
          catalogType: 'hadoop',
        },
      },
    };
    rerender(<IcebergIOSourceProperties node={mockNodeHadoop} onChange={mockOnChange} />);
    expect(container.querySelector('#icebergSourceCatalogUri')).not.toBeInTheDocument();

    const mockNodeGlue: IcebergIOSourceNode = {
      data: {
        properties: {
          catalogType: 'glue',
        },
      },
    };
    rerender(<IcebergIOSourceProperties node={mockNodeGlue} onChange={mockOnChange} />);
    expect(container.querySelector('#icebergSourceCatalogUri')).not.toBeInTheDocument();
    expect(screen.getByText('GlueCatalog')).toBeInTheDocument();
    expect(screen.getByText('S3FileIO')).toBeInTheDocument();

    const mockNodeBigQuery: IcebergIOSourceNode = {
      data: {
        properties: {
          catalogType: 'bigquery',
        },
      },
    };
    rerender(<IcebergIOSourceProperties node={mockNodeBigQuery} onChange={mockOnChange} />);
    expect(container.querySelector('#icebergSourceCatalogUri')).not.toBeInTheDocument();
    expect(screen.getByText('BigQueryMetastoreCatalog')).toBeInTheDocument();
    expect(screen.getByText('GCSFileIO')).toBeInTheDocument();
  });

  it('validates mutually exclusive time-travel: neither, snapshot only, timestamp only, and both', () => {
    const baseNode: IcebergIOSourceNode = {
      data: {
        properties: {},
      },
    };

    const { container, rerender } = render(<IcebergIOSourceProperties node={baseNode} onChange={mockOnChange} />);
    expect(screen.queryByText(/Time travel parameters are mutually exclusive/i)).not.toBeInTheDocument();
    expect(container.querySelector('#icebergSourceSnapshotId')).toHaveAttribute('aria-invalid', 'false');
    expect(container.querySelector('#icebergSourceAsOfTimestamp')).toHaveAttribute('aria-invalid', 'false');

    const nodeWithSnapshot: IcebergIOSourceNode = {
      data: {
        properties: {
          snapshotId: '1234567890',
        },
      },
    };
    rerender(<IcebergIOSourceProperties node={nodeWithSnapshot} onChange={mockOnChange} />);
    expect(screen.queryByText(/Time travel parameters are mutually exclusive/i)).not.toBeInTheDocument();

    const nodeWithTimestamp: IcebergIOSourceNode = {
      data: {
        properties: {
          asOfTimestamp: '2024-01-01T00:00:00Z',
        },
      },
    };
    rerender(<IcebergIOSourceProperties node={nodeWithTimestamp} onChange={mockOnChange} />);
    expect(screen.queryByText(/Time travel parameters are mutually exclusive/i)).not.toBeInTheDocument();

    const nodeWithBoth: IcebergIOSourceNode = {
      data: {
        properties: {
          snapshotId: '1234567890',
          asOfTimestamp: '2024-01-01T00:00:00Z',
        },
      },
    };
    rerender(<IcebergIOSourceProperties node={nodeWithBoth} onChange={mockOnChange} />);
    expect(
      screen.getByText('Cannot specify both Snapshot ID and As Of Timestamp. Time travel parameters are mutually exclusive.')
    ).toBeInTheDocument();
    expect(container.querySelector('#icebergSourceSnapshotId')).toHaveAttribute('aria-invalid', 'true');
    expect(container.querySelector('#icebergSourceAsOfTimestamp')).toHaveAttribute('aria-invalid', 'true');
  });

  it('handles field changes for table, catalogName, catalogType (glue, nessie, bigquery), catalogUri, snapshotId, and asOfTimestamp', () => {
    const mockNode: IcebergIOSourceNode = {
      data: {
        properties: {
          catalogType: 'rest',
        },
      },
    };

    const { container } = render(<IcebergIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    fireEvent.change(container.querySelector('#icebergSourceTable') as HTMLInputElement, {
      target: { name: 'table', value: 'default.orders' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ table: 'default.orders' }));

    fireEvent.change(container.querySelector('#icebergSourceCatalogName') as HTMLInputElement, {
      target: { name: 'catalogName', value: 'prod_iceberg' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ catalogName: 'prod_iceberg' }));

    const catalogSelect = container.querySelector('#icebergSourceCatalogType') as HTMLSelectElement;
    for (const nextType of ['hive', 'glue', 'nessie', 'bigquery']) {
      fireEvent.change(catalogSelect, {
        target: { name: 'catalogType', value: nextType },
      });
      expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ catalogType: nextType }));
    }

    fireEvent.change(container.querySelector('#icebergSourceCatalogUri') as HTMLInputElement, {
      target: { name: 'catalogUri', value: 'http://rest-server:8181' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ catalogUri: 'http://rest-server:8181' }));

    fireEvent.change(container.querySelector('#icebergSourceSnapshotId') as HTMLInputElement, {
      target: { name: 'snapshotId', value: '9876543210' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ snapshotId: '9876543210' }));

    fireEvent.change(container.querySelector('#icebergSourceAsOfTimestamp') as HTMLInputElement, {
      target: { name: 'asOfTimestamp', value: '2024-06-01T12:00:00Z' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ asOfTimestamp: '2024-06-01T12:00:00Z' }));
  });

  it('handles accordion toggling and configProperties input in advanced configuration', () => {
    const mockNode: IcebergIOSourceNode = {
      data: {
        properties: {},
      },
    };

    const { container } = render(<IcebergIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    fireEvent.click(screen.getByText('Advanced & Catalog Configuration'));

    const configPropertiesTextarea = container.querySelector('#icebergSourceConfigProperties') as HTMLTextAreaElement;
    expect(configPropertiesTextarea).toBeInTheDocument();

    fireEvent.change(configPropertiesTextarea, {
      target: { name: 'configProperties', value: 'io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        configProperties: 'io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO',
      })
    );
  });

  it('handles SecretManagerAuthFields interaction and system_instructions updates', () => {
    const mockNode: IcebergIOSourceNode = {
      data: {
        properties: {
          authMode: 'secretManager',
          secretName: '',
        },
      },
    };

    const { container } = render(<IcebergIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    const secretInput = container.querySelector('#icebergSourceSecretName') as HTMLInputElement;
    expect(secretInput).toBeInTheDocument();

    fireEvent.change(secretInput, {
      target: { name: 'secretName', value: 'iceberg-catalog-key' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secretName: 'iceberg-catalog-key',
      })
    );

    const instructionsTextarea = container.querySelector('#icebergSourceInstructions') as HTMLTextAreaElement;
    fireEvent.change(instructionsTextarea, {
      target: { value: 'Read Iceberg records and apply pushdown projection' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Read Iceberg records and apply pushdown projection',
      })
    );
  });

  it('includes biglake in ICEBERG_CATALOG_TYPES and populates BigLake catalog datalist via useAssetLoader (D3)', () => {
    expect(ICEBERG_CATALOG_TYPES.some((opt) => opt.value === 'biglake')).toBe(true);

    const mockPostMessage = vi.fn();
    (window as unknown as Record<string, unknown>).vscode = { postMessage: mockPostMessage };

    const mockNode: IcebergIOSourceNode = {
      data: {
        properties: {
          table: 'analytics.events',
          warehouseLocation: 'gs://lakehouse/warehouse',
          catalogType: 'biglake',
        },
      },
    };

    const { container } = render(<IcebergIOSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(mockPostMessage).toHaveBeenCalledWith({ command: 'getBigLakeCatalogs' });

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'bigLakeCatalogsLoaded',
            catalogs: [
              {
                name: 'lakehouse_catalog',
                id: 'lakehouse_catalog',
                value: 'lakehouse_catalog',
                warehouse: 'gs://lakehouse_catalog',
              },
              'projects/my-proj/locations/us-central1/catalogs/analytics_cat',
            ],
          },
        })
      );
    });

    const datalist = container.querySelector('#iceberg-source-catalogs-list');
    expect(datalist).toBeInTheDocument();
    expect(datalist?.querySelector('option[value="lakehouse_catalog"]')).toBeInTheDocument();
    expect(datalist?.querySelector('option[value="analytics_cat"]')).toBeInTheDocument();

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'bigLakeCatalogsLoaded',
            catalogs: [],
            error: 'PERMISSION_DENIED: biglake.catalogs.list',
          },
        })
      );
    });

    expect(screen.getByText(/Failed to load BigLake catalogs: PERMISSION_DENIED/i)).toBeInTheDocument();
  });
});
