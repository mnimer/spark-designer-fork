/**
 * @fileoverview Unit tests for HudiSinkProperties.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x `HudiSink` form rendering, required `path`, `tableName`,
 * `recordKeyField`, and conditional `precombineField` validation, `tableType`
 * (`COPY_ON_WRITE` vs `MERGE_ON_READ`), `operation` (`upsert`, `insert`, `bulk_insert`, `delete`),
 * `partitionPathField`, `hiveStylePartitioning`, advanced options, and `system_instructions`.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { HudiSinkProperties } from './HudiSink';

describe('HudiSinkProperties', () => {
  it('renders defaults and validates required path, tableName, recordKeyField, and upsert precombineField', () => {
    const mockOnChange = vi.fn();
    render(
      <HudiSinkProperties
        node={{ data: { label: 'Hudi Sink', spark_transform: 'HudiSink', properties: {} } }}
        onChange={mockOnChange}
      />
    );

    const tableTypeSelect = screen.getByLabelText(/Table Type/i, { selector: 'select' }) as HTMLSelectElement;
    const operationSelect = screen.getByLabelText(/Write Operation/i, { selector: 'select' }) as HTMLSelectElement;
    expect(Array.from(tableTypeSelect.options).map((o) => o.value)).toEqual([
      'COPY_ON_WRITE',
      'MERGE_ON_READ',
    ]);
    expect(Array.from(operationSelect.options).map((o) => o.value)).toEqual([
      'upsert',
      'insert',
      'bulk_insert',
      'delete',
    ]);

    expect(tableTypeSelect).toHaveValue('COPY_ON_WRITE');
    expect(operationSelect).toHaveValue('upsert');
    expect(screen.getByText('Hudi Table Base Path is required.')).toBeInTheDocument();
    expect(screen.getByText('Hudi Table Name is required.')).toBeInTheDocument();
    expect(screen.getByText('Record Key Field is required.')).toBeInTheDocument();
    expect(screen.getByText('Precombine Field is required for upsert.')).toBeInTheDocument();
  });

  it('does not require precombineField when operation is bulk_insert', () => {
    const mockOnChange = vi.fn();
    render(
      <HudiSinkProperties
        node={{
          data: {
            properties: {
              path: 'gs://lakehouse/hudi/orders',
              tableName: 'orders_hudi',
              tableType: 'MERGE_ON_READ',
              operation: 'bulk_insert',
              recordKeyField: 'order_id',
              precombineField: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByLabelText(/Table Type/i, { selector: 'select' })).toHaveValue('MERGE_ON_READ');
    expect(screen.getByLabelText(/Write Operation/i, { selector: 'select' })).toHaveValue('bulk_insert');
    expect(screen.queryByText('Precombine Field is required for upsert.')).not.toBeInTheDocument();
  });

  it('dispatches updates for Hudi writer fields, hiveStylePartitioning, advanced options, and system_instructions', () => {
    const mockOnChange = vi.fn();
    render(
      <HudiSinkProperties
        node={{
          data: {
            properties: {
              path: 'gs://lakehouse/hudi/orders',
              tableName: 'orders_hudi',
              recordKeyField: 'order_id',
              precombineField: 'updated_at',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Table Type/i), {
      target: { name: 'tableType', value: 'MERGE_ON_READ' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ tableType: 'MERGE_ON_READ' })
    );

    fireEvent.change(screen.getByLabelText(/Partition Path Field/i, { selector: 'input' }), {
      target: { name: 'partitionPathField', value: 'event_date, region' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionPathField: 'event_date, region' })
    );

    fireEvent.click(screen.getByLabelText(/Hive-Style Partitioning/i));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ hiveStylePartitioning: true })
    );

    fireEvent.click(screen.getByText('Advanced Hudi Write Options'));
    fireEvent.change(screen.getByLabelText(/Write Shuffle Parallelism/i, { selector: 'input' }), {
      target: { name: 'writeParallelism', value: '128' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ writeParallelism: '128' })
    );

    fireEvent.change(screen.getByLabelText(/Additional Hudi Options/i, { selector: 'textarea' }), {
      target: { name: 'hoodieProperties', value: 'hoodie.cleaner.commits.retained=5' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ hoodieProperties: 'hoodie.cleaner.commits.retained=5' })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i), {
      target: { value: 'Enable inline compaction every 5 commits' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Enable inline compaction every 5 commits' })
    );
  });
});
