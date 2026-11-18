/**
 * BigQuery Source Properties Unit Tests (SRC22)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Verifies the Spark BigQuerySourceProperties component for .format("bigquery"),
 * including readModeType switching ('table' vs 'query'), live dataset/table discovery,
 * viewsEnabled and materializationDataset options, filter pushdown, and maxParallelism.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, fireEvent, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import { BigQuerySourceProperties } from './BigQueryIO';

describe('BigQuerySourceProperties', () => {
  it('renders dataset, table, filter pushdown, and maxParallelism in table mode', () => {
    const mockNode = {
      data: {
        properties: {
          readModeType: 'table' as const,
          dataset: 'test-dataset',
          table: 'test-table',
          filter: "status = 'ACTIVE'",
          maxParallelism: 32,
        },
      },
    };
    const mockOnChange = vi.fn();

    const { container } = render(<BigQuerySourceProperties node={mockNode} onChange={mockOnChange} />);

    const modeSelect = container.querySelector('#bqSourceReadModeType') as HTMLSelectElement;
    const datasetInput = container.querySelector('#bqSourceDataset') as HTMLInputElement;
    const tableInput = container.querySelector('#bqSourceTable') as HTMLInputElement;
    const filterInput = container.querySelector('#bqSourceFilter') as HTMLInputElement;
    const maxParallelismInput = container.querySelector('#bqSourceMaxParallelism') as HTMLInputElement;

    expect(modeSelect).toHaveValue('table');
    expect(datasetInput).toHaveValue('test-dataset');
    expect(tableInput).toHaveValue('test-table');
    expect(filterInput).toHaveValue("status = 'ACTIVE'");
    expect(maxParallelismInput).toHaveValue(32);
  });

  it('calls onChange and resets table when dataset input changes', () => {
    const mockNode = {
      data: {
        properties: {
          dataset: 'old-dataset',
          table: 'old-table',
        },
      },
    };
    const mockOnChange = vi.fn();

    const { container } = render(<BigQuerySourceProperties node={mockNode} onChange={mockOnChange} />);

    const datasetInput = container.querySelector('#bqSourceDataset') as HTMLInputElement;
    fireEvent.change(datasetInput, { target: { name: 'dataset', value: 'new-dataset' } });

    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ dataset: 'new-dataset', table: '' })
    );
  });

  it('switches to query mode and configures query, viewsEnabled, and materializationDataset', () => {
    const mockNode = {
      data: {
        properties: {
          readModeType: 'query' as const,
          query: 'SELECT * FROM `proj.ds.tbl`',
          viewsEnabled: true,
          materializationDataset: 'temp_ds',
        },
      },
    };
    const mockOnChange = vi.fn();

    const { container } = render(<BigQuerySourceProperties node={mockNode} onChange={mockOnChange} />);

    const queryInput = container.querySelector('#bqSourceQuery') as HTMLTextAreaElement;
    const viewsCheckbox = container.querySelector('#bqSourceViewsEnabled') as HTMLInputElement;
    const matInput = container.querySelector('#bqSourceMaterializationDataset') as HTMLInputElement;

    expect(queryInput).toHaveValue('SELECT * FROM `proj.ds.tbl`');
    expect(viewsCheckbox).toBeChecked();
    expect(matInput).toHaveValue('temp_ds');

    fireEvent.change(queryInput, { target: { name: 'query', value: 'SELECT id FROM `proj.ds.tbl`' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ query: 'SELECT id FROM `proj.ds.tbl`' })
    );

    fireEvent.click(viewsCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ viewsEnabled: false })
    );

    fireEvent.change(matInput, { target: { name: 'materializationDataset', value: 'new_mat_ds' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ materializationDataset: 'new_mat_ds' })
    );
  });

  it('updates filter pushdown, maxParallelism, and system_instructions', () => {
    const mockNode = {
      data: {
        properties: {
          readModeType: 'table' as const,
          table: 'orders',
        },
      },
    };
    const mockOnChange = vi.fn();

    const { container } = render(<BigQuerySourceProperties node={mockNode} onChange={mockOnChange} />);

    const filterInput = container.querySelector('#bqSourceFilter') as HTMLInputElement;
    fireEvent.change(filterInput, { target: { name: 'filter', value: 'amount > 50' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ filter: 'amount > 50' }));

    const parallelismInput = container.querySelector('#bqSourceMaxParallelism') as HTMLInputElement;
    fireEvent.change(parallelismInput, { target: { name: 'maxParallelism', value: '64' } });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ maxParallelism: '64' }));

    const sysInput = container.querySelector('#bqSourceSystemInstructions') as HTMLTextAreaElement;
    fireEvent.change(sysInput, { target: { value: 'Push down date partition filter' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Push down date partition filter' })
    );
  });

  it('preserves latest user changes when datasetsLoaded message arrives', () => {
    const initialNode = {
      data: {
        properties: {
          dataset: 'initial-dataset',
          table: 'initial-table',
        },
      },
    };
    const mockOnChange = vi.fn();

    const { rerender } = render(<BigQuerySourceProperties node={initialNode} onChange={mockOnChange} />);

    const updatedNode = {
      data: {
        properties: {
          dataset: 'initial-dataset',
          table: 'user-updated-table',
          filter: 'id > 0',
        },
      },
    };
    rerender(<BigQuerySourceProperties node={updatedNode} onChange={mockOnChange} />);

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'datasetsLoaded',
            datasets: ['dataset-a', 'dataset-b'],
            projectId: 'new-project-id',
          },
        })
      );
    });

    expect(mockOnChange).toHaveBeenCalledWith({
      dataset: 'initial-dataset',
      table: 'user-updated-table',
      filter: 'id > 0',
      projectId: 'new-project-id',
    });
  });

  it('does not trigger onChange if datasetsLoaded message has no projectId', () => {
    const mockNode = {
      data: {
        properties: {
          dataset: 'analytics',
          table: 'events',
        },
      },
    };
    const mockOnChange = vi.fn();

    render(<BigQuerySourceProperties node={mockNode} onChange={mockOnChange} />);

    act(() => {
      window.dispatchEvent(
        new MessageEvent('message', {
          data: {
            command: 'datasetsLoaded',
            datasets: ['analytics', 'monitoring'],
          },
        })
      );
    });

    expect(mockOnChange).not.toHaveBeenCalled();
  });
});
