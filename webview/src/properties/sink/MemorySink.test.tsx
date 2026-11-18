/**
 * @fileoverview Unit tests for MemorySinkProperties.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x `MemorySink` form rendering, driver memory warning banner,
 * required and SQL-identifier validation on `queryName` / `viewName`, synchronized
 * `queryName` + `viewName` updates, `globalTempView` toggle, and `system_instructions`.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemorySinkProperties } from './MemorySink';

describe('MemorySinkProperties', () => {
  it('renders driver memory warning and flags required validation when queryName is empty', () => {
    const mockOnChange = vi.fn();
    render(
      <MemorySinkProperties
        node={{ data: { label: 'Memory Sink', spark_transform: 'MemorySink', properties: {} } }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByRole('note')).toHaveTextContent(/buffers all emitted rows in Spark driver memory/i);
    expect(screen.getByText('Query / View Name is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Register as Global Temp View/i, { selector: 'input' })).not.toBeChecked();
  });

  it('rejects invalid SQL identifiers and accepts valid viewName fallback hydration', () => {
    const mockOnChange = vi.fn();
    const { rerender } = render(
      <MemorySinkProperties
        node={{
          data: {
            properties: {
              queryName: '123-invalid!',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(
      screen.getByText(
        'Must start with a letter or underscore and contain only letters, digits, or underscores.'
      )
    ).toBeInTheDocument();

    rerender(
      <MemorySinkProperties
        node={{
          data: {
            properties: {
              viewName: 'valid_metrics_table',
              globalTempView: true,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByLabelText(/Query \/ Temp View Name/i, { selector: 'input' })).toHaveValue(
      'valid_metrics_table'
    );
    expect(screen.getByText('global_temp.valid_metrics_table')).toBeInTheDocument();
  });

  it('synchronizes queryName and viewName on change and updates globalTempView and system_instructions', () => {
    const mockOnChange = vi.fn();
    render(
      <MemorySinkProperties
        node={{ data: { properties: {} } }}
        onChange={mockOnChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Query \/ Temp View Name/i, { selector: 'input' }), {
      target: { name: 'queryName', value: 'live_counts' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        queryName: 'live_counts',
        viewName: 'live_counts',
      })
    );

    fireEvent.click(screen.getByLabelText(/Register as Global Temp View/i, { selector: 'input' }));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ globalTempView: true })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }), {
      target: { value: 'Expose aggregated anomaly counts in SQL' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Expose aggregated anomaly counts in SQL' })
    );
  });
});
