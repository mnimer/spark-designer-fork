/**
 * @fileoverview Unit tests for the Apache Spark RenameDropColumns step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  RenameDropColumnsProperties,
  normalizeRenameMappings,
  normalizeDropColumns,
} from './RenameDropColumns';

describe('RenameDropColumnsProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-rename-drop-1',
    data: {
      label: 'RenameDropColumns',
      spark_transform: 'RenameDropColumns',
      properties,
    },
  });

  describe('normalization helpers', () => {
    it('normalizes renameMappings and dropColumns cleanly', () => {
      expect(normalizeRenameMappings(undefined)).toEqual([]);
      expect(normalizeRenameMappings([{ from: 'old_id', to: 'new_id' }, null])).toEqual([
        { from: 'old_id', to: 'new_id' },
        { from: '', to: '' },
      ]);

      expect(normalizeDropColumns(' _tmp_a, _tmp_b , ')).toEqual(['_tmp_a', '_tmp_b']);
      expect(normalizeDropColumns([' col1 ', '', 'col2'])).toEqual(['col1', 'col2']);
      expect(normalizeDropColumns(undefined)).toEqual([]);
    });
  });

  it('adds, edits, validates, and removes column rename mappings', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <RenameDropColumnsProperties node={createNode()} onChange={handleChange} />
    );

    fireEvent.click(screen.getByRole('button', { name: /Add Rename/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        renameMappings: [{ from: '', to: '' }],
        dropColumns: [],
      })
    );

    handleChange.mockClear();
    rerender(
      <RenameDropColumnsProperties
        node={createNode({ renameMappings: [{ from: 'cust_id', to: '' }] })}
        onChange={handleChange}
      />
    );
    expect(screen.getByRole('alert')).toHaveTextContent(
      'Both source and target column names are required.'
    );

    const toInput = screen.getByLabelText('Rename 1 target column', { selector: 'input' });
    fireEvent.change(toInput, { target: { value: 'customer_id' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        renameMappings: [{ from: 'cust_id', to: 'customer_id' }],
      })
    );

    handleChange.mockClear();
    rerender(
      <RenameDropColumnsProperties
        node={createNode({
          renameMappings: [
            { from: 'cust_id', to: 'customer_id' },
            { from: 'ord_ts', to: 'order_timestamp' },
          ],
        })}
        onChange={handleChange}
      />
    );
    fireEvent.click(screen.getByRole('button', { name: 'Remove rename mapping 1' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        renameMappings: [{ from: 'ord_ts', to: 'order_timestamp' }],
      })
    );
  });

  it('adds comma-separated drop columns, deduplicates entries, removes columns, and updates instructions', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <RenameDropColumnsProperties node={createNode()} onChange={handleChange} />
    );

    const dropInput = screen.getByLabelText(/Drop Columns/i, { selector: 'input' });
    fireEvent.change(dropInput, { target: { value: 'raw_payload, _debug_flag, raw_payload' } });
    fireEvent.click(screen.getByRole('button', { name: /Add Drop/i }));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        dropColumns: ['raw_payload', '_debug_flag'],
      })
    );

    handleChange.mockClear();
    rerender(
      <RenameDropColumnsProperties
        node={createNode({ dropColumns: ['raw_payload', '_debug_flag'] })}
        onChange={handleChange}
      />
    );

    expect(screen.getByLabelText('Drop column 1', { selector: 'input' })).toHaveValue(
      'raw_payload'
    );
    fireEvent.click(screen.getByRole('button', { name: 'Remove drop column raw_payload' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        dropColumns: ['_debug_flag'],
      })
    );

    handleChange.mockClear();
    fireEvent.change(
      screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }),
      { target: { value: 'Drop PII columns before downstream sinks.' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Drop PII columns before downstream sinks.',
      })
    );
  });
});
