/**
 * @fileoverview Unit tests for the CachePersist step property configuration panel.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { CachePersistProperties } from './CachePersist';

describe('CachePersistProperties', () => {
  const createNode = (properties: Record<string, unknown> = {}) => ({
    id: 'step-cache-1',
    data: {
      label: 'Cache / Persist',
      spark_transform: 'CachePersist',
      properties,
    },
  });

  it('defaults to cache mode with unpersistAfterDownstream enabled and hides StorageLevel', () => {
    const handleChange = vi.fn();
    render(<CachePersistProperties node={createNode()} onChange={handleChange} />);

    expect(screen.getByLabelText(/Persistence Mode/i, { selector: 'select' })).toHaveValue('cache');
    expect(screen.queryByLabelText(/Storage Level/i, { selector: 'select' })).not.toBeInTheDocument();
    expect(
      screen.getByLabelText(/Unpersist After Downstream Sinks/i, { selector: 'input' })
    ).toBeChecked();
    expect(screen.getByTestId('cache-persist-guidance-banner')).toBeInTheDocument();
  });

  it('switches to persist mode and exposes all five Spark 4.x StorageLevel constants', () => {
    const handleChange = vi.fn();
    const { rerender } = render(
      <CachePersistProperties node={createNode()} onChange={handleChange} />
    );

    fireEvent.change(screen.getByLabelText(/Persistence Mode/i, { selector: 'select' }), {
      target: { value: 'persist' },
    });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'persist',
        storageLevel: 'MEMORY_AND_DISK',
        unpersistAfterDownstream: true,
      })
    );

    rerender(
      <CachePersistProperties
        node={createNode({ mode: 'persist', storageLevel: 'MEMORY_AND_DISK' })}
        onChange={handleChange}
      />
    );

    const levelSelect = screen.getByLabelText(/Storage Level/i, {
      selector: 'select',
    }) as HTMLSelectElement;
    const optionValues = Array.from(levelSelect.options).map((o) => o.value);
    expect(optionValues).toEqual([
      'MEMORY_AND_DISK',
      'MEMORY_ONLY',
      'DISK_ONLY',
      'MEMORY_AND_DISK_DESER',
      'OFF_HEAP',
    ]);

    fireEvent.change(levelSelect, { target: { value: 'DISK_ONLY' } });
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'persist',
        storageLevel: 'DISK_ONLY',
      })
    );
  });

  it('toggles unpersistAfterDownstream and updates SystemInstructionsField', () => {
    const handleChange = vi.fn();
    render(
      <CachePersistProperties
        node={createNode({ mode: 'persist', storageLevel: 'OFF_HEAP' })}
        onChange={handleChange}
      />
    );

    fireEvent.click(
      screen.getByLabelText(/Unpersist After Downstream Sinks/i, { selector: 'input' })
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: 'persist',
        storageLevel: 'OFF_HEAP',
        unpersistAfterDownstream: false,
      })
    );

    fireEvent.change(
      screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }),
      { target: { value: 'Materialize before fan-out to Iceberg and BigQuery' } }
    );
    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Materialize before fan-out to Iceberg and BigQuery',
      })
    );
  });
});
