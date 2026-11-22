/**
 * @fileoverview Unit tests for the Spark MergeCdcScd step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  MergeCdcScdProperties,
  normalizeCdcFormat,
  normalizeColumnList,
  normalizeDeleteValuesText,
  normalizeScdType,
} from './MergeCdcScd';

describe('MergeCdcScdProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-merge-cdc-scd-1',
    data: {
      label: 'Merge CDC / SCD',
      spark_transform: 'MergeCdcScd',
      properties,
    },
  });

  describe('normalization helpers', () => {
    it('normalizes scdType, cdcFormat, column lists, and deleteValues cleanly', () => {
      expect(normalizeScdType(undefined)).toBe('scd1');
      expect(normalizeScdType('scd1')).toBe('scd1');
      expect(normalizeScdType('scd2')).toBe('scd2');
      expect(normalizeScdType('invalid')).toBe('scd1');

      expect(normalizeCdcFormat(undefined)).toBe('debezium');
      expect(normalizeCdcFormat('delta_cdf')).toBe('delta_cdf');
      expect(normalizeCdcFormat('custom_op')).toBe('custom_op');
      expect(normalizeCdcFormat('upsert_only')).toBe('upsert_only');
      expect(normalizeCdcFormat('unknown')).toBe('debezium');

      expect(normalizeColumnList(undefined)).toEqual([]);
      expect(normalizeColumnList('customer_id, tenant_id , ')).toEqual([
        'customer_id',
        'tenant_id',
      ]);
      expect(normalizeColumnList([' id ', '', 'region'])).toEqual(['id', 'region']);

      expect(normalizeDeleteValuesText(undefined)).toBe('D, DELETE');
      expect(normalizeDeleteValuesText(['d', 'del'])).toBe('d, del');
      expect(normalizeDeleteValuesText('X, REMOVE')).toBe('X, REMOVE');
    });
  });

  it('renders dual-input banner, defaults to scd1, and validates missing keyColumns and sequenceColumn', () => {
    const handleChange = vi.fn();
    render(<MergeCdcScdProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByTestId('merge-cdc-scd-handle-banner')).toHaveTextContent(
      /CDC Stream & Target Snapshot Inputs/i
    );
    expect(screen.queryByTestId('merge-cdc-scd2-section')).not.toBeInTheDocument();

    const scdSelect = screen.getByLabelText(/SCD Strategy/i, {
      selector: 'select',
    }) as HTMLSelectElement;
    expect(scdSelect.value).toBe('scd1');

    expect(screen.getByText('At least one primary key column is required.')).toBeInTheDocument();
    expect(screen.getByText('Sequence column is required.')).toBeInTheDocument();

    const seqInput = screen.getByLabelText(/Sequence \/ Event Timestamp Column/i, {
      selector: 'input',
    });
    fireEvent.change(seqInput, { target: { name: 'sequenceColumn', value: 'updated_at' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        sequenceColumn: 'updated_at',
      })
    );
  });

  it('adds, edits, and removes primary key columns and optional tracked columns', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <MergeCdcScdProperties
        node={createNode({
          keyColumns: ['id'],
          sequenceColumn: 'updated_at',
        })}
        onChange={handleChange}
      />
    );

    expect(
      screen.queryByText('At least one primary key column is required.')
    ).not.toBeInTheDocument();

    // Edit existing key column
    const keyRowInput = screen.getByLabelText('Key column 1');
    expect(keyRowInput).toHaveValue('id');
    fireEvent.change(keyRowInput, { target: { value: 'customer_id' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        keyColumns: ['customer_id'],
      })
    );

    // Add composite key columns via input + Add Key button
    const addKeyInput = screen.getByLabelText(/Primary Key Columns/i, { selector: 'input' });
    fireEvent.change(addKeyInput, { target: { value: 'tenant_id, region_id' } });
    fireEvent.click(screen.getByRole('button', { name: /Add Key/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        keyColumns: ['id', 'tenant_id', 'region_id'],
      })
    );

    // Remove a key column
    handleChange.mockClear();
    rerender(
      <MergeCdcScdProperties
        node={createNode({
          keyColumns: ['customer_id', 'tenant_id'],
          sequenceColumn: 'updated_at',
          trackedColumns: ['tier'],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove key column customer_id' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        keyColumns: ['tenant_id'],
      })
    );

    // Add and edit tracked columns via Enter key and inline input
    const addTrackedInput = screen.getByLabelText(/Tracked Value Columns/i, {
      selector: 'input',
    });
    fireEvent.change(addTrackedInput, { target: { value: 'email' } });
    fireEvent.keyDown(addTrackedInput, { key: 'Enter' });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        trackedColumns: ['tier', 'email'],
      })
    );

    const trackedRowInput = screen.getByLabelText('Tracked column 1');
    fireEvent.change(trackedRowInput, { target: { value: 'plan_tier' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        trackedColumns: ['plan_tier'],
      })
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove tracked column tier' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        trackedColumns: [],
      })
    );
  });

  it('switches CDC format modes and validates custom_op operationColumn and deleteValues', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <MergeCdcScdProperties
        node={createNode({
          keyColumns: ['customer_id'],
          sequenceColumn: 'updated_at',
          cdcFormat: 'debezium',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.queryByTestId('merge-cdc-custom-op-section')).not.toBeInTheDocument();

    const cdcSelect = screen.getByLabelText(/CDC Operation Format/i, {
      selector: 'select',
    });
    fireEvent.change(cdcSelect, { target: { value: 'custom_op' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        cdcFormat: 'custom_op',
      })
    );

    rerender(
      <MergeCdcScdProperties
        node={createNode({
          keyColumns: ['customer_id'],
          sequenceColumn: 'updated_at',
          cdcFormat: 'custom_op',
          operationColumn: '',
          deleteValues: '',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByTestId('merge-cdc-custom-op-section')).toBeInTheDocument();
    expect(screen.getByText('Operation column is required.')).toBeInTheDocument();
    expect(screen.getByText('Delete marker values are required.')).toBeInTheDocument();

    const opColInput = screen.getByLabelText(/Operation Column/i, { selector: 'input' });
    fireEvent.change(opColInput, { target: { name: 'operationColumn', value: 'cdc_flag' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        operationColumn: 'cdc_flag',
      })
    );

    const deleteValsInput = screen.getByLabelText(/Delete Marker Values/i, {
      selector: 'input',
    });
    fireEvent.change(deleteValsInput, {
      target: { name: 'deleteValues', value: 'DEL, PURGE' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        deleteValues: 'DEL, PURGE',
      })
    );
  });

  it('switches between scd1 and scd2 and updates SCD Type 2 versioning fields, actionColumn, and system_instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <MergeCdcScdProperties
        node={createNode({
          scdType: 'scd1',
          keyColumns: ['customer_id'],
          sequenceColumn: 'updated_at',
        })}
        onChange={handleChange}
      />
    );

    const scdSelect = screen.getByLabelText(/SCD Strategy/i, { selector: 'select' });
    fireEvent.change(scdSelect, { target: { value: 'scd2' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        scdType: 'scd2',
      })
    );

    rerender(
      <MergeCdcScdProperties
        node={createNode({
          scdType: 'scd2',
          keyColumns: ['customer_id'],
          sequenceColumn: 'updated_at',
        })}
        onChange={handleChange}
      />
    );

    expect(screen.getByTestId('merge-cdc-scd2-section')).toBeInTheDocument();

    const effFromInput = screen.getByLabelText(/Effective From Column/i, {
      selector: 'input',
    });
    expect(effFromInput).toHaveValue('effective_from');
    fireEvent.change(effFromInput, {
      target: { name: 'effectiveFromColumn', value: 'valid_from' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        effectiveFromColumn: 'valid_from',
      })
    );

    const effToInput = screen.getByLabelText(/Effective To Column/i, {
      selector: 'input',
    });
    expect(effToInput).toHaveValue('effective_to');
    fireEvent.change(effToInput, {
      target: { name: 'effectiveToColumn', value: 'valid_to' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        effectiveToColumn: 'valid_to',
      })
    );

    const isCurrentInput = screen.getByLabelText(/Is Current Column/i, {
      selector: 'input',
    });
    expect(isCurrentInput).toHaveValue('is_current');
    fireEvent.change(isCurrentInput, {
      target: { name: 'isCurrentColumn', value: 'active_flag' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        isCurrentColumn: 'active_flag',
      })
    );

    const openEndSelect = screen.getByLabelText(/Open-End Active Value/i, {
      selector: 'select',
    });
    expect(openEndSelect).toHaveValue('null');
    fireEvent.change(openEndSelect, { target: { value: '9999-12-31' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        openEndValue: '9999-12-31',
      })
    );

    const actionColInput = screen.getByLabelText(/Staging Action Column/i, {
      selector: 'input',
    });
    expect(actionColInput).toHaveValue('_scd_action');
    fireEvent.change(actionColInput, {
      target: { name: 'actionColumn', value: '_merge_op' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        actionColumn: '_merge_op',
      })
    );

    const sysTextarea = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(sysTextarea, {
      target: { value: 'Sort tie-breakers by lsn desc' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Sort tie-breakers by lsn desc',
      })
    );
  });
});
