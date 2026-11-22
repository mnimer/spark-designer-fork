/**
 * @fileoverview Unit tests for ConsoleSinkProperties.
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates Spark 4.x `ConsoleSink` form rendering, default `numRows` (`20`),
 * `truncate` (`true`), and `logLevel` (`INFO`), positive row count validation,
 * field update dispatches, and `system_instructions`.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ConsoleSinkProperties } from './ConsoleSink';

describe('ConsoleSinkProperties', () => {
  it('renders defaults (numRows=20, truncate=true, logLevel=INFO)', () => {
    const mockOnChange = vi.fn();
    render(
      <ConsoleSinkProperties
        node={{ data: { label: 'Console Sink', spark_transform: 'ConsoleSink', properties: {} } }}
        onChange={mockOnChange}
      />
    );

    const logLevelSelect = screen.getByLabelText(/Log Level/i, { selector: 'select' }) as HTMLSelectElement;
    expect(Array.from(logLevelSelect.options).map((o) => o.value)).toEqual([
      'INFO',
      'DEBUG',
      'WARNING',
      'ERROR',
    ]);
    expect(screen.getByLabelText(/Max Preview Rows/i, { selector: 'input' })).toHaveValue(20);
    expect(logLevelSelect).toHaveValue('INFO');
    expect(screen.getByLabelText(/Truncate Long Strings/i, { selector: 'input' })).toBeChecked();
  });

  it('flags validation error when numRows is less than 1 or empty', () => {
    const mockOnChange = vi.fn();
    render(
      <ConsoleSinkProperties
        node={{
          data: {
            properties: {
              numRows: 0,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByText('Enter at least 1 row.')).toBeInTheDocument();
  });

  it('dispatches updates for numRows, logLevel, truncate, and system_instructions', () => {
    const mockOnChange = vi.fn();
    render(
      <ConsoleSinkProperties
        node={{ data: { properties: {} } }}
        onChange={mockOnChange}
      />
    );

    fireEvent.change(screen.getByLabelText(/Max Preview Rows/i, { selector: 'input' }), {
      target: { name: 'numRows', value: '50' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ numRows: '50' }));

    fireEvent.change(screen.getByLabelText(/Log Level/i, { selector: 'select' }), {
      target: { name: 'logLevel', value: 'WARNING' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ logLevel: 'WARNING' }));

    fireEvent.click(screen.getByLabelText(/Truncate Long Strings/i, { selector: 'input' }));
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ truncate: false }));

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i, { selector: 'textarea' }), {
      target: { value: 'Include partition count in debug summary' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ system_instructions: 'Include partition count in debug summary' })
    );
  });
});
