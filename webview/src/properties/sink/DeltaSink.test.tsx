/**
 * @fileoverview Unit tests for DeltaSinkProperties.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x `DeltaSink` form rendering, `targetMode` (`path` vs `table`),
 * `writeMode` (`append`, `overwrite`, `merge`), `replaceWhere`, `overwriteSchema`,
 * `mergeKeys` and `mergeCondition` validation, `mergeSchema`, `partitionBy`,
 * `txnAppId`, `optimizeWrite`, and `system_instructions`.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { DeltaSinkProperties } from './DeltaSink';

describe('DeltaSinkProperties', () => {
  it('renders default append mode with path input and validates required path', () => {
    const mockOnChange = vi.fn();
    render(
      <DeltaSinkProperties
        node={{ data: { label: 'Delta Sink', spark_transform: 'DeltaSink', properties: {} } }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByLabelText(/Write Mode/i)).toHaveValue('append');
    expect(screen.getByLabelText(/Target Location Mode/i)).toHaveValue('path');
    expect(screen.getByLabelText(/Delta Table Path/i)).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Delta Table Path is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Merge Schema Evolution/i)).not.toBeChecked();
  });

  it('validates tableName in table mode and exposes replaceWhere and overwriteSchema in overwrite mode', () => {
    const mockOnChange = vi.fn();
    render(
      <DeltaSinkProperties
        node={{
          data: {
            properties: {
              targetMode: 'table',
              tableName: '',
              writeMode: 'overwrite',
              replaceWhere: "event_date >= '2026-01-01'",
              overwriteSchema: true,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByText('Delta Table Name is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Replace Where Predicate/i)).toHaveValue("event_date >= '2026-01-01'");
    expect(screen.getByLabelText(/Overwrite Schema/i)).toBeChecked();

    fireEvent.change(screen.getByLabelText(/Delta Table Name/i), {
      target: { name: 'tableName', value: 'analytics.events_gold' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ tableName: 'analytics.events_gold' })
    );
  });

  it('validates merge keys and merge condition when writeMode is merge', () => {
    const mockOnChange = vi.fn();
    const { rerender } = render(
      <DeltaSinkProperties
        node={{
          data: {
            properties: {
              path: 'gs://lakehouse/delta/accounts',
              writeMode: 'merge',
              mergeKeys: '',
              mergeCondition: '',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText('Provide Merge Key Columns or a Custom Merge Condition when Write Mode is merge.')
    ).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Merge Key Columns/i, { selector: 'input' }), {
      target: { name: 'mergeKeys', value: 'account_id' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ mergeKeys: 'account_id' })
    );

    rerender(
      <DeltaSinkProperties
        node={{
          data: {
            properties: {
              path: 'gs://lakehouse/delta/accounts',
              writeMode: 'merge',
              mergeKeys: 'account_id',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.queryByText('Provide Merge Key Columns or a Custom Merge Condition when Write Mode is merge.')
    ).not.toBeInTheDocument();
  });

  it('dispatches updates for partitionBy, mergeSchema, advanced idempotency options, and system_instructions', () => {
    const mockOnChange = vi.fn();
    render(
      <DeltaSinkProperties
        node={{
          data: {
            properties: {
              path: 'gs://lakehouse/delta/events',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Partition Columns/i), {
      target: { name: 'partitionBy', value: 'event_date, region' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ partitionBy: 'event_date, region' })
    );

    fireEvent.click(screen.getByLabelText(/Merge Schema Evolution/i));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ mergeSchema: true })
    );

    fireEvent.click(screen.getByText('Advanced & Idempotency Options'));
    fireEvent.change(screen.getByLabelText(/Transaction App ID/i), {
      target: { name: 'txnAppId', value: 'orders_stream_v1' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ txnAppId: 'orders_stream_v1' })
    );

    fireEvent.click(screen.getByLabelText(/Optimize Write Bin-Packing/i));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ optimizeWrite: true })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i), {
      target: { value: 'Z-order by customer_id after batch load' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Z-order by customer_id after batch load' })
    );
  });
});
