/**
 * @fileoverview Unit Tests for BigQuerySinkProperties Component (SNK19).
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies Spark BigQuerySinkProperties rendering, live GCP dataset, table, and
 * storage bucket discovery, direct vs indirect writeMethod switching, saveMode,
 * partitioning, clustering validation, schema evolution toggles, BigQuerySchemaEditor
 * integration, and stale-closure protection.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import { BigQuerySinkProperties } from './BigQueryIO';
import { classifyKey } from '../../../../src/credentialRedaction';

describe('BigQuerySinkProperties (SNK19)', () => {
  let postMessageSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.clearAllMocks();
    postMessageSpy = vi.fn();
    (window as unknown as { vscode?: { postMessage: typeof postMessageSpy } }).vscode = {
      postMessage: postMessageSpy,
    };
  });

  afterEach(() => {
    delete (window as unknown as { vscode?: unknown }).vscode;
  });

  it('renders Spark BigQuery defaults, validates required table, and requests datasets', () => {
    const mockOnChange = vi.fn();
    const { container } = render(
      <BigQuerySinkProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    expect(screen.getByText('Spark BigQuery Storage Write API')).toBeInTheDocument();
    const tableInput = container.querySelector('#bigQuerySinkTable') as HTMLInputElement;
    expect(tableInput).toHaveAttribute('placeholder', 'my_table');
    expect(tableInput).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Table is required.')).toBeInTheDocument();

    const writeMethodSelect = container.querySelector(
      '#bigQuerySinkWriteMethod'
    ) as HTMLSelectElement;
    expect(writeMethodSelect.value).toBe('direct');

    const saveModeSelect = container.querySelector('#bigQuerySinkSaveMode') as HTMLSelectElement;
    expect(saveModeSelect.value).toBe('append');

    expect(postMessageSpy).toHaveBeenCalledWith({ command: 'getDatasets' });
  });

  it('clears table when dataset changes and updates table value', () => {
    const mockOnChange = vi.fn();
    const { container } = render(
      <BigQuerySinkProperties
        node={{ data: { properties: { dataset: 'analytics', table: 'old_orders' } } }}
        onChange={mockOnChange}
      />
    );

    const datasetInput = container.querySelector('#bigQuerySinkDataset') as HTMLInputElement;
    fireEvent.change(datasetInput, { target: { name: 'dataset', value: 'warehouse' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ dataset: 'warehouse', table: '' })
    );

    const tableInput = container.querySelector('#bigQuerySinkTable') as HTMLInputElement;
    fireEvent.change(tableInput, { target: { name: 'table', value: 'fact_orders' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ dataset: 'analytics', table: 'fact_orders' })
    );
  });

  it('toggles indirect writeMethod, validates temporaryGcsBucket, and loads bucket suggestions', () => {
    const mockOnChange = vi.fn();
    const { container, rerender } = render(
      <BigQuerySinkProperties
        node={{ data: { properties: { table: 'fact_sales', writeMethod: 'direct' } } }}
        onChange={mockOnChange}
      />
    );

    expect(container.querySelector('#bigQuerySinkTemporaryGcsBucket')).not.toBeInTheDocument();

    const writeMethodSelect = container.querySelector(
      '#bigQuerySinkWriteMethod'
    ) as HTMLSelectElement;
    fireEvent.change(writeMethodSelect, { target: { name: 'writeMethod', value: 'indirect' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ writeMethod: 'indirect' })
    );

    rerender(
      <BigQuerySinkProperties
        node={{ data: { properties: { table: 'fact_sales', writeMethod: 'indirect' } } }}
        onChange={mockOnChange}
      />
    );

    expect(postMessageSpy).toHaveBeenCalledWith({ command: 'getStorageBuckets' });
    expect(screen.getByText('Temporary GCS bucket is required for indirect writes.')).toBeInTheDocument();

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'storageBucketsLoaded',
            buckets: ['spark-staging-us', 'spark-staging-eu'],
          },
        })
      );
    });

    const options = Array.from(
      document.querySelectorAll('#bq-sink-temp-bucket-options option')
    ).map((opt) => opt.getAttribute('value'));
    expect(options).toEqual(['spark-staging-us', 'spark-staging-eu']);

    const bucketInput = container.querySelector(
      '#bigQuerySinkTemporaryGcsBucket'
    ) as HTMLInputElement;
    fireEvent.change(bucketInput, {
      target: { name: 'temporaryGcsBucket', value: 'spark-staging-us' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ temporaryGcsBucket: 'spark-staging-us' })
    );
  });

  it('configures saveMode, partitioning, clustering validation, and schema evolution toggles', () => {
    const emitted: Array<Record<string, unknown>> = [];
    const mockOnChange = vi.fn((p: Record<string, unknown>) => emitted.push(p));

    const { container, rerender } = render(
      <BigQuerySinkProperties
        node={{
          data: {
            properties: {
              table: 'events',
              saveMode: 'overwrite',
              partitionField: 'event_date',
              partitionType: 'MONTH',
              clusteredFields: 'c1,c2,c3,c4,c5',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText('BigQuery supports at most 4 clustered columns per table')
    ).toBeInTheDocument();

    rerender(
      <BigQuerySinkProperties
        node={{
          data: {
            properties: {
              table: 'events',
              clusteredFields: 'bad col',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );
    expect(
      screen.getByText('Enter comma-separated column names without spaces, e.g. customer_id,region')
    ).toBeInTheDocument();

    fireEvent.change(container.querySelector('#bigQuerySinkSaveMode')!, {
      target: { name: 'saveMode', value: 'ignore' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ saveMode: 'ignore' }));

    fireEvent.change(container.querySelector('#bigQuerySinkPartitionField')!, {
      target: { name: 'partitionField', value: 'created_at' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionField: 'created_at' })
    );

    fireEvent.change(container.querySelector('#bigQuerySinkPartitionType')!, {
      target: { name: 'partitionType', value: 'HOUR' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ partitionType: 'HOUR' }));

    fireEvent.change(container.querySelector('#bigQuerySinkClusteredFields')!, {
      target: { name: 'clusteredFields', value: 'customer_id,region' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ clusteredFields: 'customer_id,region' })
    );

    fireEvent.click(container.querySelector('#bigQuerySinkAllowFieldAddition')!);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ allowFieldAddition: true })
    );

    fireEvent.click(container.querySelector('#bigQuerySinkAllowFieldRelaxation')!);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ allowFieldRelaxation: true })
    );

    for (const payload of emitted) {
      for (const key of Object.keys(payload)) {
        expect(['safe', 'reference']).toContain(classifyKey(key));
      }
    }
  });

  it('preserves latest user changes when datasetsLoaded message arrives (stale closure guard)', () => {
    const mockOnChange = vi.fn();
    const { rerender } = render(
      <BigQuerySinkProperties
        node={{ data: { properties: { dataset: 'initial_ds', table: 'initial_tbl' } } }}
        onChange={mockOnChange}
      />
    );

    rerender(
      <BigQuerySinkProperties
        node={{
          data: {
            properties: {
              dataset: 'initial_ds',
              table: 'updated_tbl',
              writeMethod: 'direct',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'datasetsLoaded',
            datasets: ['initial_ds', 'other_ds'],
            projectId: 'my-spark-project',
          },
        })
      );
    });

    expect(mockOnChange).toHaveBeenCalledWith({
      dataset: 'initial_ds',
      table: 'updated_tbl',
      writeMethod: 'direct',
      projectId: 'my-spark-project',
    });
  });

  it('renders dataset and table discovery errors as alerts', () => {
    render(
      <BigQuerySinkProperties
        node={{ data: { properties: { dataset: 'analytics', table: 'orders' } } }}
        onChange={vi.fn()}
      />
    );

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'datasetsLoaded',
            datasets: [],
            error: 'Failed to authenticate to BigQuery',
          },
        })
      );
    });
    expect(screen.getByRole('alert')).toHaveTextContent('Failed to authenticate to BigQuery');

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'tablesLoaded',
            dataset: 'analytics',
            tables: [],
            error: 'Dataset not found: analytics',
          },
        })
      );
    });
    expect(screen.getByText('Dataset not found: analytics')).toBeInTheDocument();
  });

  it('toggles BigQuerySchemaEditor based on createDisposition and updates system_instructions', () => {
    const mockOnChange = vi.fn();
    const { container, rerender } = render(
      <BigQuerySinkProperties
        node={{ data: { properties: { table: 'orders', createDisposition: 'CREATE_NEVER' } } }}
        onChange={mockOnChange}
      />
    );

    expect(screen.queryByText('Table Schema (Optional Override)')).not.toBeInTheDocument();

    rerender(
      <BigQuerySinkProperties
        node={{ data: { properties: { table: 'orders', createDisposition: 'CREATE_IF_NEEDED' } } }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByText('Table Schema (Optional Override)')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /Add Schema Field/i }));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        schemaFields: [{ name: '', type: 'STRING', mode: 'NULLABLE', description: '' }],
      })
    );

    const instructions = container.querySelector(
      '#bigQuerySinkSystemInstructions'
    ) as HTMLTextAreaElement;
    fireEvent.change(instructions, {
      target: { value: 'Partition by order_date and cluster by region' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Partition by order_date and cluster by region',
      })
    );
  });
});
