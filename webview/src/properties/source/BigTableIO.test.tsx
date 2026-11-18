/**
 * Unit Tests for BigTableIOSourceProperties Component (SRC24)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates BigTableIOSourceProperties form rendering, projectId/instanceId/tableId/catalog
 * required validation, pushDownRowKeyFilters toggle, and system instruction updates.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { BigTableIOSourceProperties, BigtableSourceProperties } from './BigTableIO';

describe('BigTableIOSourceProperties', () => {
  const mockOnChange = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('exports BigtableSourceProperties alias and validates required fields when empty', () => {
    expect(BigtableSourceProperties).toBe(BigTableIOSourceProperties);
    render(<BigTableIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Project ID is required')).toBeInTheDocument();
    expect(screen.getByText('Instance ID is required')).toBeInTheDocument();
    expect(screen.getByText('Table ID is required')).toBeInTheDocument();
    expect(screen.getByText('Catalog JSON schema mapping is required')).toBeInTheDocument();
  });

  it('hydrates properties and dispatches updates for tableId, catalog, and pushDownRowKeyFilters', () => {
    const node = {
      data: {
        properties: {
          projectId: 'my-gcp-project',
          instanceId: 'bt-prod',
          tableId: 'events',
          catalog: '{"table":{"name":"events"}}',
          pushDownRowKeyFilters: true,
        },
      },
    };

    const { container } = render(<BigTableIOSourceProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByDisplayValue('my-gcp-project')).toBeInTheDocument();
    expect(screen.getByDisplayValue('bt-prod')).toBeInTheDocument();
    expect(screen.getByDisplayValue('events')).toBeInTheDocument();
    expect(screen.getByDisplayValue('{"table":{"name":"events"}}')).toBeInTheDocument();

    const pushDownCheckbox = container.querySelector('#bigtableSourcePushDownRowKeyFilters') as HTMLInputElement;
    expect(pushDownCheckbox).toBeChecked();

    fireEvent.click(pushDownCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ pushDownRowKeyFilters: false })
    );

    const tableInput = container.querySelector('#bigtableSourceTableId') as HTMLInputElement;
    fireEvent.change(tableInput, { target: { name: 'tableId', value: 'sessions' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ tableId: 'sessions', table: 'sessions' })
    );

    const catalogInput = container.querySelector('#bigtableSourceCatalog') as HTMLTextAreaElement;
    fireEvent.change(catalogInput, { target: { name: 'catalog', value: '{"table":{"name":"sessions"}}' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ catalog: '{"table":{"name":"sessions"}}' })
    );
  });

  it('updates system_instructions when instructions textarea changes', () => {
    const { container } = render(
      <BigTableIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    const textarea = container.querySelector('#bigtableSourceInstructions') as HTMLTextAreaElement;
    fireEvent.change(textarea, { target: { value: 'Filter by rowkey prefix' } });
    expect(mockOnChange).toHaveBeenCalledWith({
      system_instructions: 'Filter by rowkey prefix',
    });
  });
});
