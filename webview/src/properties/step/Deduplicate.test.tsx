/**
 * @fileoverview Unit tests for the Spark Deduplicate step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import {
  DeduplicateProperties,
  normalizeDeduplicateStrategy,
  normalizeSubsetColumns,
} from './Deduplicate';

describe('DeduplicateProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-dedup-1',
    data: {
      label: 'Deduplicate',
      spark_transform: 'Deduplicate',
      properties,
    },
  });

  describe('normalization helpers', () => {
    it('normalizes strategy values cleanly with drop_duplicates default', () => {
      expect(normalizeDeduplicateStrategy(undefined)).toBe('drop_duplicates');
      expect(normalizeDeduplicateStrategy('distinct_all')).toBe('distinct_all');
      expect(normalizeDeduplicateStrategy('drop_duplicates')).toBe('drop_duplicates');
      expect(normalizeDeduplicateStrategy('within_watermark')).toBe('within_watermark');
      expect(normalizeDeduplicateStrategy('keep_latest')).toBe('keep_latest');
      expect(normalizeDeduplicateStrategy('invalid')).toBe('drop_duplicates');
    });

    it('normalizes subsetColumns from arrays and comma-separated strings', () => {
      expect(normalizeSubsetColumns(undefined)).toEqual([]);
      expect(normalizeSubsetColumns('event_id, user_id , ')).toEqual(['event_id', 'user_id']);
      expect(normalizeSubsetColumns([' id ', '', 'tenant'])).toEqual(['id', 'tenant']);
    });
  });

  it('renders streaming watermark guidance banner and all 4 deduplication strategies', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <DeduplicateProperties node={createNode()} onChange={handleChange} />
    );

    expect(screen.getByTestId('deduplicate-watermark-banner')).toHaveTextContent(
      /dropDuplicatesWithinWatermark/i
    );

    const strategySelect = screen.getByLabelText(/Deduplication Strategy/i, {
      selector: 'select',
    }) as HTMLSelectElement;
    expect(strategySelect.value).toBe('drop_duplicates');
    expect(Array.from(strategySelect.options).map((o) => o.value)).toEqual([
      'distinct_all',
      'drop_duplicates',
      'within_watermark',
      'keep_latest',
    ]);

    fireEvent.change(strategySelect, { target: { value: 'distinct_all' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        strategy: 'distinct_all',
      })
    );

    rerender(
      <DeduplicateProperties
        node={createNode({ strategy: 'distinct_all' })}
        onChange={handleChange}
      />
    );
    expect(screen.queryByTestId('deduplicate-subset-section')).not.toBeInTheDocument();
    expect(screen.queryByTestId('deduplicate-keep-latest-section')).not.toBeInTheDocument();
  });

  it('adds and removes subsetColumns for drop_duplicates and within_watermark', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <DeduplicateProperties
        node={createNode({ strategy: 'within_watermark' })}
        onChange={handleChange}
      />
    );

    const subsetInput = screen.getByLabelText(/Key Subset Columns/i, { selector: 'input' });
    fireEvent.change(subsetInput, { target: { value: 'event_id, source_id' } });
    fireEvent.click(screen.getByRole('button', { name: /^Add$/i }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        strategy: 'within_watermark',
        subsetColumns: ['event_id', 'source_id'],
      })
    );

    handleChange.mockClear();
    rerender(
      <DeduplicateProperties
        node={createNode({
          strategy: 'within_watermark',
          subsetColumns: ['event_id', 'source_id'],
        })}
        onChange={handleChange}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: 'Remove subset column event_id' }));
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        subsetColumns: ['source_id'],
      })
    );
  });

  it('validates and updates orderByColumn and orderDirection when strategy is keep_latest', () => {
    const handleChange = vi.fn();
    render(
      <DeduplicateProperties
        node={createNode({
          strategy: 'keep_latest',
          subsetColumns: [],
          orderByColumn: '',
        })}
        onChange={handleChange}
      />
    );

    const alerts = screen.getAllByRole('alert');
    expect(alerts).toHaveLength(2);

    const orderColInput = screen.getByLabelText(/Order By Column/i, { selector: 'input' });
    fireEvent.change(orderColInput, { target: { value: 'updated_ts' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        orderByColumn: 'updated_ts',
      })
    );

    const dirSelect = screen.getByLabelText(/Order Direction/i, { selector: 'select' });
    fireEvent.change(dirSelect, { target: { value: 'asc' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        orderDirection: 'asc',
      })
    );

    const sysTextarea = screen.getByLabelText(/Code Generation Instructions/i, {
      selector: 'textarea',
    });
    fireEvent.change(sysTextarea, { target: { value: 'Tie-break by ingestion_ts desc' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Tie-break by ingestion_ts desc',
      })
    );
  });
});
