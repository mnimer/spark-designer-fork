/**
 * Unit Tests for SpannerIOSourceProperties Component (SRC23)
 *
 * Author: Mike Nimer <mikenimer@google.com>
 *
 * Purpose:
 * Validates SpannerIOSourceProperties form rendering, projectId/instanceId/databaseId validation,
 * table vs query mode switching, enableDataBoost toggle, timestampBound input, and system instructions.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { SpannerIOSourceProperties, SpannerSourceProperties, DEFAULT_SPANNER_JAR_URI } from './SpannerIO';

describe('SpannerIOSourceProperties', () => {
  const mockOnChange = vi.fn();
  let postMessageSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.clearAllMocks();
    postMessageSpy = vi.fn();
    (window as any).vscode = { postMessage: postMessageSpy };
  });

  afterEach(() => {
    delete (window as any).vscode;
  });

  it('exports SpannerSourceProperties alias and displays Spanner connector JAR URI', () => {
    expect(SpannerSourceProperties).toBe(SpannerIOSourceProperties);
    render(<SpannerIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);
    expect(screen.getByText(DEFAULT_SPANNER_JAR_URI)).toBeInTheDocument();
  });

  it('renders required Project ID, Instance ID, Database ID, and Table fields and alerts when empty', () => {
    render(<SpannerIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />);

    expect(screen.getByText('Project ID is required')).toBeInTheDocument();
    expect(screen.getByText('Spanner instance ID is required')).toBeInTheDocument();
    expect(screen.getByText('Spanner database ID is required')).toBeInTheDocument();
    expect(screen.getByText('Table name is required in Table mode')).toBeInTheDocument();
  });

  it('hydrates values and dispatches changes for projectId, instanceId, databaseId, table, enableDataBoost, and timestampBound', () => {
    const node = {
      data: {
        properties: {
          projectId: 'my-project',
          instanceId: 'my-instance',
          databaseId: 'my-database',
          readTargetMode: 'table' as const,
          table: 'Singers',
          enableDataBoost: true,
          timestampBound: '2026-01-01T00:00:00Z',
        },
      },
    };

    const { container } = render(<SpannerIOSourceProperties node={node} onChange={mockOnChange} />);

    expect(screen.getByDisplayValue('my-project')).toBeInTheDocument();
    expect(screen.getByDisplayValue('my-instance')).toBeInTheDocument();
    expect(screen.getByDisplayValue('my-database')).toBeInTheDocument();
    expect(screen.getByDisplayValue('Singers')).toBeInTheDocument();
    expect(screen.getByDisplayValue('2026-01-01T00:00:00Z')).toBeInTheDocument();

    const dataBoostCheckbox = container.querySelector('#spannerSourceEnableDataBoost') as HTMLInputElement;
    expect(dataBoostCheckbox).toBeChecked();

    fireEvent.click(dataBoostCheckbox);
    expect(mockOnChange).toHaveBeenCalledWith(expect.objectContaining({ enableDataBoost: false }));

    const tsInput = container.querySelector('#spannerSourceTimestampBound') as HTMLInputElement;
    fireEvent.change(tsInput, { target: { name: 'timestampBound', value: '2026-06-01T00:00:00Z' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ timestampBound: '2026-06-01T00:00:00Z' })
    );
  });

  it('switches to SQL query mode and validates query field', () => {
    const node = {
      data: {
        properties: {
          projectId: 'p',
          instanceId: 'i',
          databaseId: 'd',
          readTargetMode: 'query' as const,
          query: '',
        },
      },
    };

    const { container } = render(<SpannerIOSourceProperties node={node} onChange={mockOnChange} />);
    expect(screen.getByText('SQL query is required in Query mode')).toBeInTheDocument();

    const queryInput = container.querySelector('#spannerSourceQuery') as HTMLTextAreaElement;
    fireEvent.change(queryInput, { target: { name: 'query', value: 'SELECT * FROM Singers' } });
    expect(mockOnChange).toHaveBeenCalledWith(
      expect.objectContaining({ query: 'SELECT * FROM Singers' })
    );
  });

  it('updates system_instructions when instructions textarea changes', () => {
    const { container } = render(
      <SpannerIOSourceProperties node={{ data: { properties: {} } }} onChange={mockOnChange} />
    );

    const textarea = container.querySelector('#spannerSourceSystemInstructions') as HTMLTextAreaElement;
    fireEvent.change(textarea, { target: { value: 'Use Spanner Data Boost with strong reads' } });
    expect(mockOnChange).toHaveBeenCalledWith({
      system_instructions: 'Use Spanner Data Boost with strong reads',
    });
  });
});
