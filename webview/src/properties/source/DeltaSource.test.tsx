/**
 * DeltaSource Properties Test Suite
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Unit tests validating DeltaSourceProperties component functionality,
 * readMode (batch vs streaming), sourceMode (path vs table), time travel mutual exclusivity,
 * readChangeFeed toggle, startingVersion/startingTimestamp, maxFilesPerTrigger,
 * ignoreChanges/ignoreDeletes, and system_instructions.
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { DeltaSourceProperties } from './DeltaSource';

describe('DeltaSourceProperties', () => {
  it('renders default batch mode with path input and validates required path', () => {
    const mockNode = {
      data: {
        label: 'Delta Source',
        properties: {},
      },
    };
    const mockOnChange = vi.fn();

    render(<DeltaSourceProperties node={mockNode} onChange={mockOnChange} />);

    expect(screen.getByLabelText(/Read Mode/i)).toHaveValue('batch');
    expect(screen.getByLabelText(/Source Location Mode/i)).toHaveValue('path');
    expect(screen.getByLabelText(/Delta Table Path/i)).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByText('Delta Table Path is required.')).toBeInTheDocument();
    expect(screen.getByLabelText(/Version As Of/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Timestamp As Of/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Read Change Data Feed \(CDF\)/i)).not.toBeChecked();
  });

  it('switches to table mode and validates tableName, plus flags conflict when both time-travel fields are set', () => {
    const mockOnChange = vi.fn();
    render(
      <DeltaSourceProperties
        node={{
          data: {
            label: 'Delta Source',
            properties: {
              readMode: 'batch',
              sourceMode: 'table',
              tableName: '',
              versionAsOf: 10,
              timestampAsOf: '2026-01-01T00:00:00Z',
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByText('Delta Table Name is required.')).toBeInTheDocument();
    expect(screen.getByText('Cannot specify both Version As Of and Timestamp As Of.')).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Delta Table Name/i), {
      target: { name: 'tableName', value: 'analytics.orders' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        tableName: 'analytics.orders',
      })
    );
  });

  it('renders streaming options and Change Data Feed controls when readMode is streaming', () => {
    const mockOnChange = vi.fn();
    render(
      <DeltaSourceProperties
        node={{
          data: {
            label: 'Delta Source',
            properties: {
              readMode: 'streaming',
              path: 'gs://lakehouse/delta/events',
              readChangeFeed: true,
              startingVersion: '0',
              maxFilesPerTrigger: 500,
              maxBytesPerTrigger: '1g',
              ignoreChanges: true,
              ignoreDeletes: false,
            },
          },
        }}
        onChange={mockOnChange}
      />
    );

    expect(screen.getByLabelText(/Read Change Data Feed \(CDF\)/i)).toBeChecked();
    expect(screen.getByLabelText(/Starting Version/i)).toHaveValue('0');
    expect(screen.getByLabelText(/Max Files Per Trigger/i)).toHaveValue(500);
    expect(screen.getByLabelText(/Max Bytes Per Trigger/i)).toHaveValue('1g');
    expect(screen.getByLabelText(/Ignore Changes/i)).toBeChecked();
    expect(screen.getByLabelText(/Ignore Deletes/i)).not.toBeChecked();

    fireEvent.click(screen.getByLabelText(/Ignore Deletes/i));
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        ignoreDeletes: true,
      })
    );

    fireEvent.change(screen.getByLabelText(/Code Generation Instructions/i), {
      target: { value: 'Filter _change_type == insert' },
    });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({
        system_instructions: 'Filter _change_type == insert',
      })
    );
  });
});
